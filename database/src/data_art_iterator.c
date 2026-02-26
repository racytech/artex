/*
 * Persistent ART - Iterator (Sorted Key Enumeration)
 *
 * Stack-based depth-first traversal that yields leaves in lexicographic
 * key order. Locks are acquired at creation time and held for the entire
 * scan lifetime (released on destroy or scan completion) enabling zero-copy
 * mmap reads. Key/value buffers are pre-allocated and reused across calls.
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
    bool scanning;          // true while scan-lifetime locks are held

    // Pre-allocated buffers (reused across next() calls)
    uint8_t *current_key;
    void    *current_value;
    size_t   current_key_len;
    size_t   current_value_len;
    size_t   value_buf_cap;  // allocated capacity of current_value

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
                return n->children[i];
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            if (*child_idx < n->num_children) {
                int i = *child_idx;
                *child_idx = i + 1;
                return n->children[i];
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
                    return n->children[slot];
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            while (*child_idx < 256) {
                int byte = *child_idx;
                *child_idx = byte + 1;
                if (n->children[byte] != 0) {
                    return n->children[byte];
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
    // Copy key into pre-allocated buffer
    iter->current_key_len = iter->tree->key_size;
    memcpy(iter->current_key, leaf_key(leaf), iter->tree->key_size);

    // Copy value (grow buffer if needed)
    iter->current_value_len = leaf->value_len;
    if (leaf->value_len == 0) {
        return true;
    }

    if (leaf->value_len > iter->value_buf_cap) {
        size_t new_cap = leaf->value_len;
        void *new_buf = realloc(iter->current_value, new_cap);
        if (!new_buf) return false;
        iter->current_value = new_buf;
        iter->value_buf_cap = new_cap;
    }

    if (leaf->flags & LEAF_FLAG_OVERFLOW) {
        if (!data_art_read_overflow_value(iter->tree, leaf, iter->current_value)) {
            return false;
        }
    } else {
        memcpy(iter->current_value, leaf_key(leaf) + iter->tree->key_size, leaf->value_len);
    }

    return true;
}

// ============================================================================
// Public API
// ============================================================================

static void end_scan(data_art_iterator_t *iter);

data_art_iterator_t *data_art_iterator_create(data_art_tree_t *tree) {
    if (!tree) return NULL;

    data_art_iterator_t *iter = calloc(1, sizeof(*iter));
    if (!iter) return NULL;

    iter->tree = tree;

    // Acquire scan-lifetime locks and capture root atomically.
    // Locks held until end_scan() (on destroy or scan completion).
    // This closes the race where a writer could release/reuse pages
    // between root capture and first next() call.
    pthread_rwlock_rdlock(&tree->write_lock);
    data_art_rdlock(tree);
    iter->scanning = true;
    iter->root = atomic_load_explicit(&tree->committed_root, memory_order_acquire);

    iter->depth = -1;
    iter->done = node_ref_is_null(iter->root);

    // Pre-allocate key buffer (fixed size, reused across all next() calls)
    iter->current_key = malloc(tree->key_size);
    if (!iter->current_key) {
        end_scan(iter);
        free(iter);
        return NULL;
    }

    return iter;
}

// Release scan-lifetime locks (called when scan ends or iterator destroyed)
static void end_scan(data_art_iterator_t *iter) {
    if (iter->scanning) {
        iter->scanning = false;
        data_art_rdunlock(iter->tree);
        pthread_rwlock_unlock(&iter->tree->write_lock);
    }
}

bool data_art_iterator_next(data_art_iterator_t *iter) {
    if (!iter || iter->done) return false;

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

        // Load the node at current stack level (zero-copy: direct mmap pointer)
        const void *node = data_art_load_node(iter->tree, frame->node_ref);
        if (!node) {
            LOG_ERROR("Iterator: failed to load node at page=%lu offset=%u",
                      node_ref_page_id(frame->node_ref), node_ref_offset(frame->node_ref));
            iter->done = true;
            end_scan(iter);
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
                end_scan(iter);
                return false;
            }

            // Prefix check: stop if key no longer matches prefix
            if (iter->prefix) {
                if (iter->current_key_len < iter->prefix_len ||
                    memcmp(iter->current_key, iter->prefix, iter->prefix_len) != 0) {
                    iter->done = true;
                    iter->depth--;
                    end_scan(iter);
                    return false;
                }
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
                end_scan(iter);
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
    end_scan(iter);
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
    end_scan(iter);
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
                    return n->children[i];
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
                    return n->children[i];
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
                    return n->children[slot];
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            for (int b = byte; b < 256; b++) {
                if (n->children[b] != 0) {
                    *exact = (b == byte);
                    *child_idx = b + 1;
                    return n->children[b];
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

    // Reset iterator state (keep buffers allocated)
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

    // Targeted descent: set up stack so next() finds first leaf >= key
    while (iter->depth >= 0) {
        iter_stack_frame_t *frame = &iter->stack[iter->depth];
        const void *node = data_art_load_node(iter->tree, frame->node_ref);
        if (!node) {
            iter->done = true;
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
            size_t leaf_key_len = iter->tree->key_size;
            size_t cmp_len = leaf_key_len < key_len ? leaf_key_len : key_len;
            int cmp = memcmp(leaf_key(leaf), key, cmp_len);
            if (cmp == 0) cmp = (int)leaf_key_len - (int)key_len;

            if (cmp >= 0) {
                // leaf >= seek key: this is our target
                break;
            } else {
                // leaf < seek key: pop, let parent advance
                iter->depth--;
                break;
            }
        }

        // Path compression: check compressed prefix
        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            const uint8_t *partial = node_partial(node);
            // Compare prefix with seek key
            size_t i;
            for (i = 0; i < plen && key_depth + i < key_len; i++) {
                if (key[key_depth + i] < partial[i]) {
                    // seek key < prefix → first leaf in this subtree is >= seek key
                    // Position at first child and let next() find the leaf
                    frame->child_idx = 0;
                    goto seek_done;
                }
                if (key[key_depth + i] > partial[i]) {
                    // seek key > prefix → entire subtree is < seek key
                    // Backtrack to parent
                    iter->depth--;
                    goto seek_done;
                }
            }
            // All compared bytes match
            if (i < plen) {
                // seek key exhausted within prefix → first leaf here is >= seek key
                frame->child_idx = 0;
                goto seek_done;
            }
            key_depth += plen;
        }

        // Find child for next key byte.
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
                return false;
            }
            iter->stack[iter->depth].node_ref = child;
            iter->stack[iter->depth].child_idx = 0;
        } else {
            iter->depth++;
            if (iter->depth >= ITER_MAX_DEPTH) {
                iter->done = true;
                return false;
            }
            iter->stack[iter->depth].node_ref = child;
            iter->stack[iter->depth].child_idx = 0;
            break;
        }
    }

seek_done:
    // Now advance to the first leaf >= target
    return data_art_iterator_next(iter);
}
