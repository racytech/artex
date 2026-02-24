/*
 * Persistent ART - Iterator (Sorted Key Enumeration)
 *
 * Stack-based depth-first traversal that yields leaves in lexicographic
 * key order. Each next() call resets the TLS arena and reloads nodes from
 * their page references, so no persistent page pinning is needed.
 *
 * The iterator captures a snapshot of the committed root at creation time,
 * providing a consistent view even under concurrent writes.
 */

#include "data_art.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

// Forward declarations from data_art_core.c
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_reset_arena(void);

// Forward declaration from data_art_overflow.c
extern bool data_art_read_overflow_value(data_art_tree_t *tree,
                                          const data_art_leaf_t *leaf, void *out_buf);

// ============================================================================
// Iterator State
// ============================================================================

#define ITER_MAX_DEPTH 64

typedef struct {
    node_ref_t node_ref;
    int child_idx;        // Next child index to try (-1 means node not yet visited)
} iter_stack_frame_t;

struct data_art_iterator {
    data_art_tree_t *tree;
    node_ref_t root;

    iter_stack_frame_t stack[ITER_MAX_DEPTH];
    int depth;
    bool started;
    bool done;

    // Current leaf data (malloc'd, survives arena reset)
    uint8_t *current_key;
    void    *current_value;
    size_t   current_key_len;
    size_t   current_value_len;
};

// ============================================================================
// Internal: Get next child reference from a loaded node
// ============================================================================

// Given a loaded node and the current scan position (*child_idx),
// find the next valid child in key-sorted order.
// Advances *child_idx past the returned child.
// Returns NULL_NODE_REF when no more children.
static node_ref_t get_next_child_ref(const void *node, int *child_idx) {
    uint8_t type = *(const uint8_t *)node;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            if (*child_idx < n->num_children) {
                int i = *child_idx;
                *child_idx = i + 1;
                return (node_ref_t){.page_id = n->child_page_ids[i],
                                    .offset  = n->child_offsets[i]};
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            if (*child_idx < n->num_children) {
                int i = *child_idx;
                *child_idx = i + 1;
                return (node_ref_t){.page_id = n->child_page_ids[i],
                                    .offset  = n->child_offsets[i]};
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            while (*child_idx < 256) {
                int byte = *child_idx;
                *child_idx = byte + 1;
                uint8_t slot = n->keys[byte];
                if (slot != NODE48_EMPTY) {
                    return (node_ref_t){.page_id = n->child_page_ids[slot],
                                        .offset  = n->child_offsets[slot]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            while (*child_idx < 256) {
                int byte = *child_idx;
                *child_idx = byte + 1;
                if (n->child_page_ids[byte] != 0) {
                    return (node_ref_t){.page_id = n->child_page_ids[byte],
                                        .offset  = n->child_offsets[byte]};
                }
            }
            return NULL_NODE_REF;
        }
        default:
            return NULL_NODE_REF;
    }
}

// ============================================================================
// Internal: Copy leaf data to malloc'd buffers
// ============================================================================

static bool copy_leaf_data(data_art_iterator_t *iter, const data_art_leaf_t *leaf) {
    // Free previous data
    free(iter->current_key);
    free(iter->current_value);
    iter->current_key = NULL;
    iter->current_value = NULL;

    // Copy key
    iter->current_key_len = leaf->key_len;
    iter->current_key = malloc(leaf->key_len);
    if (!iter->current_key) return false;
    memcpy(iter->current_key, leaf->data, leaf->key_len);

    // Copy value
    iter->current_value_len = leaf->value_len;
    if (leaf->value_len == 0) {
        iter->current_value = NULL;
        return true;
    }

    iter->current_value = malloc(leaf->value_len);
    if (!iter->current_value) {
        free(iter->current_key);
        iter->current_key = NULL;
        return false;
    }

    if (leaf->flags & LEAF_FLAG_OVERFLOW) {
        if (!data_art_read_overflow_value(iter->tree, leaf, iter->current_value)) {
            free(iter->current_key);
            free(iter->current_value);
            iter->current_key = NULL;
            iter->current_value = NULL;
            return false;
        }
    } else {
        memcpy(iter->current_value, leaf->data + leaf->key_len, leaf->value_len);
    }

    return true;
}

// ============================================================================
// Public API
// ============================================================================

data_art_iterator_t *data_art_iterator_create(data_art_tree_t *tree) {
    if (!tree) return NULL;

    data_art_iterator_t *iter = calloc(1, sizeof(*iter));
    if (!iter) return NULL;

    iter->tree = tree;

    // Capture committed root atomically for consistent snapshot
    uint64_t root_page = atomic_load_explicit(&tree->committed_root_page_id,
                                               memory_order_acquire);
    iter->root = (node_ref_t){.page_id = root_page, .offset = 0};

    iter->depth = -1;
    iter->done = node_ref_is_null(iter->root);

    return iter;
}

bool data_art_iterator_next(data_art_iterator_t *iter) {
    if (!iter || iter->done) return false;

    // Reset TLS arena for this traversal step
    data_art_reset_arena();

    // First call: push root onto stack
    if (!iter->started) {
        iter->started = true;
        iter->depth = 0;
        iter->stack[0].node_ref = iter->root;
        iter->stack[0].child_idx = 0;
    }

    // DFS traversal to find next leaf
    while (iter->depth >= 0) {
        iter_stack_frame_t *frame = &iter->stack[iter->depth];

        // Load the node at current stack level
        const void *node = data_art_load_node(iter->tree, frame->node_ref);
        if (!node) {
            LOG_ERROR("Iterator: failed to load node at page=%lu offset=%u",
                      frame->node_ref.page_id, frame->node_ref.offset);
            iter->done = true;
            return false;
        }

        uint8_t type = *(const uint8_t *)node;

        // Leaf node: yield it
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;

            // Skip deleted leaves (xmax != 0 means superseded/deleted)
            if (leaf->xmax != 0) {
                iter->depth--;
                continue;
            }

            if (!copy_leaf_data(iter, leaf)) {
                iter->done = true;
                return false;
            }

            // Pop leaf from stack so next call continues with parent
            iter->depth--;
            return true;
        }

        // Inner node: get next child (get_next_child_ref advances child_idx)
        node_ref_t child_ref = get_next_child_ref(node, &frame->child_idx);

        if (!node_ref_is_null(child_ref)) {
            // Push child and descend
            iter->depth++;
            if (iter->depth >= ITER_MAX_DEPTH) {
                LOG_ERROR("Iterator: stack overflow (depth >= %d)", ITER_MAX_DEPTH);
                iter->done = true;
                return false;
            }
            iter->stack[iter->depth].node_ref = child_ref;
            iter->stack[iter->depth].child_idx = 0;
        } else {
            // No more children, backtrack
            iter->depth--;
        }
    }

    // Traversal complete
    iter->done = true;
    return false;
}

const uint8_t *data_art_iterator_key(const data_art_iterator_t *iter, size_t *key_len) {
    if (!iter || iter->done || !iter->current_key) return NULL;
    if (key_len) *key_len = iter->current_key_len;
    return iter->current_key;
}

const void *data_art_iterator_value(const data_art_iterator_t *iter, size_t *value_len) {
    if (!iter || iter->done || !iter->current_value) return NULL;
    if (value_len) *value_len = iter->current_value_len;
    return iter->current_value;
}

bool data_art_iterator_done(const data_art_iterator_t *iter) {
    if (!iter) return true;
    return iter->done;
}

void data_art_iterator_destroy(data_art_iterator_t *iter) {
    if (!iter) return;
    free(iter->current_key);
    free(iter->current_value);
    free(iter);
}
