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
extern void data_art_rdlock(data_art_tree_t *tree);
extern void data_art_rdunlock(data_art_tree_t *tree);

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

    // Prefix filter (NULL = full iteration)
    uint8_t *prefix;
    size_t   prefix_len;
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
    uint32_t root_off = atomic_load_explicit(&tree->committed_root_offset,
                                              memory_order_relaxed);
    iter->root = (node_ref_t){.page_id = root_page, .offset = root_off};

    iter->depth = -1;
    iter->done = node_ref_is_null(iter->root);

    return iter;
}

bool data_art_iterator_next(data_art_iterator_t *iter) {
    if (!iter || iter->done) return false;

    // Hold write_lock rdlock to coordinate with in-place mutation writers,
    // and resize_lock rdlock for zero-copy mmap reads.
    pthread_rwlock_rdlock(&iter->tree->write_lock);
    data_art_rdlock(iter->tree);

    // Reset TLS arena (harmless when using zero-copy, needed for overflow fallback)
    data_art_reset_arena();

    // First call: push root onto stack
    if (!iter->started) {
        iter->started = true;
        iter->depth = 0;
        iter->stack[0].node_ref = iter->root;
        iter->stack[0].child_idx = 0;
    }

    bool result = false;

    // DFS traversal to find next leaf
    while (iter->depth >= 0) {
        iter_stack_frame_t *frame = &iter->stack[iter->depth];

        // Load the node at current stack level
        const void *node = data_art_load_node(iter->tree, frame->node_ref);
        if (!node) {
            LOG_ERROR("Iterator: failed to load node at page=%lu offset=%u",
                      frame->node_ref.page_id, frame->node_ref.offset);
            iter->done = true;
            goto out;
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
                goto out;
            }

            // Prefix check: stop if key no longer matches prefix
            if (iter->prefix) {
                if (iter->current_key_len < iter->prefix_len ||
                    memcmp(iter->current_key, iter->prefix, iter->prefix_len) != 0) {
                    iter->done = true;
                    free(iter->current_key);
                    free(iter->current_value);
                    iter->current_key = NULL;
                    iter->current_value = NULL;
                    iter->depth--;
                    goto out;
                }
            }

            // Pop leaf from stack so next call continues with parent
            iter->depth--;
            result = true;
            goto out;
        }

        // Inner node: get next child (get_next_child_ref advances child_idx)
        node_ref_t child_ref = get_next_child_ref(node, &frame->child_idx);

        if (!node_ref_is_null(child_ref)) {
            // Push child and descend
            iter->depth++;
            if (iter->depth >= ITER_MAX_DEPTH) {
                LOG_ERROR("Iterator: stack overflow (depth >= %d)", ITER_MAX_DEPTH);
                iter->done = true;
                goto out;
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

out:
    data_art_rdunlock(iter->tree);
    pthread_rwlock_unlock(&iter->tree->write_lock);
    return result;
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
    free(iter->prefix);
    free(iter);
}

// ============================================================================
// Prefix Iteration
// ============================================================================

data_art_iterator_t *data_art_iterator_create_prefix(
        data_art_tree_t *tree, const uint8_t *prefix, size_t prefix_len) {
    if (!tree || !prefix || prefix_len == 0) {
        // No prefix = full iteration
        return data_art_iterator_create(tree);
    }

    data_art_iterator_t *iter = data_art_iterator_create(tree);
    if (!iter) return NULL;

    // Store prefix for boundary checking in next()
    iter->prefix = malloc(prefix_len);
    if (!iter->prefix) {
        data_art_iterator_destroy(iter);
        return NULL;
    }
    memcpy(iter->prefix, prefix, prefix_len);
    iter->prefix_len = prefix_len;

    // Seek to first key >= prefix
    if (!data_art_iterator_seek(iter, prefix, prefix_len)) {
        // No key >= prefix exists
        iter->done = true;
        return iter;
    }

    // Seek succeeded — the prefix check in next() already validated the match
    // (seek calls next() internally, which runs the prefix filter)
    return iter;
}

// ============================================================================
// Seek: Position iterator at first key >= target
// ============================================================================

// All inner nodes share the same layout for the first 14 bytes:
//   type(1) + num_children(1) + partial_len(1) + padding(1) + partial(10)
// Use node4 as the generic accessor.
static inline uint8_t node_partial_len(const void *node) {
    return ((const data_art_node4_t *)node)->partial_len;
}
static inline const uint8_t *node_partial(const void *node) {
    return ((const data_art_node4_t *)node)->partial;
}

// Find child with key >= byte. Sets *child_idx for get_next_child_ref() continuation.
// Returns the child ref (NULL_NODE_REF if none), and sets *exact = true if key == byte.
static node_ref_t find_child_ge(const void *node, uint8_t byte,
                                int *child_idx, bool *exact) {
    uint8_t type = *(const uint8_t *)node;
    *exact = false;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] >= byte) {
                    *exact = (n->keys[i] == byte);
                    *child_idx = i + 1;  // parent continues after this child
                    return (node_ref_t){.page_id = n->child_page_ids[i],
                                        .offset  = n->child_offsets[i]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] >= byte) {
                    *exact = (n->keys[i] == byte);
                    *child_idx = i + 1;
                    return (node_ref_t){.page_id = n->child_page_ids[i],
                                        .offset  = n->child_offsets[i]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            for (int b = byte; b < 256; b++) {
                uint8_t slot = n->keys[b];
                if (slot != NODE48_EMPTY) {
                    *exact = (b == byte);
                    *child_idx = b + 1;  // continue scanning from next byte
                    return (node_ref_t){.page_id = n->child_page_ids[slot],
                                        .offset  = n->child_offsets[slot]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            for (int b = byte; b < 256; b++) {
                if (n->child_page_ids[b] != 0) {
                    *exact = (b == byte);
                    *child_idx = b + 1;
                    return (node_ref_t){.page_id = n->child_page_ids[b],
                                        .offset  = n->child_offsets[b]};
                }
            }
            return NULL_NODE_REF;
        }
        default:
            return NULL_NODE_REF;
    }
}

bool data_art_iterator_seek(data_art_iterator_t *iter,
                            const uint8_t *key, size_t key_len) {
    if (!iter || !key) return false;

    // Reset iterator state
    data_art_reset_arena();
    free(iter->current_key);
    free(iter->current_value);
    iter->current_key = NULL;
    iter->current_value = NULL;
    iter->current_key_len = 0;
    iter->current_value_len = 0;
    iter->started = true;
    iter->done = false;
    iter->depth = -1;

    if (node_ref_is_null(iter->root)) {
        iter->done = true;
        return false;
    }

    // Push root
    iter->depth = 0;
    iter->stack[0].node_ref = iter->root;
    iter->stack[0].child_idx = 0;

    size_t key_depth = 0;  // bytes of seek key consumed

    // Hold write_lock rdlock + resize_lock for the descent phase
    pthread_rwlock_rdlock(&iter->tree->write_lock);
    data_art_rdlock(iter->tree);

    // Targeted descent: set up stack so next() finds first leaf >= key
    while (iter->depth >= 0) {
        iter_stack_frame_t *frame = &iter->stack[iter->depth];
        const void *node = data_art_load_node(iter->tree, frame->node_ref);
        if (!node) {
            iter->done = true;
            data_art_rdunlock(iter->tree);
            pthread_rwlock_unlock(&iter->tree->write_lock);
            return false;
        }

        uint8_t type = *(const uint8_t *)node;

        // Leaf: compare with seek key
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;

            // Skip deleted leaves
            if (leaf->xmax != 0) {
                iter->depth--;
                break;  // let next() find the next valid leaf
            }

            // Compare leaf key with seek key
            size_t cmp_len = leaf->key_len < key_len ? leaf->key_len : key_len;
            int cmp = memcmp(leaf->data, key, cmp_len);
            if (cmp == 0) cmp = (int)leaf->key_len - (int)key_len;

            if (cmp >= 0) {
                // leaf >= seek key: this is our target
                // Leave it on the stack so next() yields it
                break;
            } else {
                // leaf < seek key: pop, let parent advance
                iter->depth--;
                break;
            }
        }

        // Inner node: compare partial prefix
        uint8_t plen = node_partial_len(node);
        const uint8_t *partial = node_partial(node);
        bool prefix_mismatch = false;

        for (uint8_t i = 0; i < plen; i++) {
            if (key_depth >= key_len) {
                frame->child_idx = 0;
                prefix_mismatch = true;
                break;
            }
            if (key[key_depth] < partial[i]) {
                frame->child_idx = 0;
                prefix_mismatch = true;
                break;
            }
            if (key[key_depth] > partial[i]) {
                iter->depth--;
                prefix_mismatch = true;
                break;
            }
            key_depth++;
        }

        if (prefix_mismatch)
            break;

        // Prefix matched fully. Find child for next key byte.
        if (key_depth >= key_len) {
            frame->child_idx = 0;
            break;
        }

        uint8_t byte = key[key_depth];
        key_depth++;

        bool exact_child;
        node_ref_t child = find_child_ge(node, byte,
                                         &frame->child_idx, &exact_child);

        if (node_ref_is_null(child)) {
            iter->depth--;
            break;
        }

        if (exact_child) {
            iter->depth++;
            if (iter->depth >= ITER_MAX_DEPTH) {
                iter->done = true;
                data_art_rdunlock(iter->tree);
                pthread_rwlock_unlock(&iter->tree->write_lock);
                return false;
            }
            iter->stack[iter->depth].node_ref = child;
            iter->stack[iter->depth].child_idx = 0;
        } else {
            iter->depth++;
            if (iter->depth >= ITER_MAX_DEPTH) {
                iter->done = true;
                data_art_rdunlock(iter->tree);
                pthread_rwlock_unlock(&iter->tree->write_lock);
                return false;
            }
            iter->stack[iter->depth].node_ref = child;
            iter->stack[iter->depth].child_idx = 0;
            break;
        }
    }

    // Release locks before calling next() which acquires its own
    data_art_rdunlock(iter->tree);
    pthread_rwlock_unlock(&iter->tree->write_lock);

    // Now advance to the first leaf >= target
    return data_art_iterator_next(iter);
}
