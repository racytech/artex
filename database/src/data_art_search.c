/*
 * Persistent ART - Search Operations
 *
 * Implements tree lookup/search:
 * - data_art_get / data_art_get_snapshot (public API)
 * - data_art_contains
 * - Internal traversal: find_child, leaf_matches
 * - MVCC visibility checks (CoW model — no version chain walk)
 *
 * Search operations are fully lock-free. Readers load the committed root
 * atomically and traverse immutable CoW pages without holding any lock.
 */

#include "data_art.h"
#include "mvcc.h"
#include "logger.h"

#include <stdlib.h>
#include <stdio.h>

// Debug: thread-local trace flag for diagnosing snapshot isolation violations.
// When non-zero, data_art_get_internal prints each traversal step to stderr.
__thread int tls_search_trace = 0;
#include <string.h>

// Forward declarations - functions from data_art_core.c
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_reset_arena(void);
extern void data_art_rdlock(data_art_tree_t *tree);
extern void data_art_rdunlock(data_art_tree_t *tree);

// Forward declaration - overflow value reader from data_art_overflow.c
extern bool data_art_read_overflow_value(data_art_tree_t *tree,
                                          const data_art_leaf_t *leaf, void *out_buf);

// ============================================================================
// Lock-Free Read Helper
// ============================================================================

// Read the committed root atomically (no lock needed).
static inline node_ref_t data_art_read_committed_root(data_art_tree_t *tree) {
    return atomic_load_explicit(&tree->committed_root, memory_order_acquire);
}

// ============================================================================
// Helper Functions - Node Navigation
// ============================================================================

/**
 * Find child node reference by byte key
 */
node_ref_t find_child(data_art_tree_t *tree, node_ref_t node_ref, uint8_t byte) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("find_child: failed to load node at page=%lu offset=%u",
                  node_ref_page_id(node_ref), node_ref_offset(node_ref));
        return NULL_NODE_REF;
    }

    uint8_t type = *(const uint8_t *)node;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    return n->children[i];
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    return n->children[i];
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            uint8_t idx = n->keys[byte];
            if (idx == 255) return NULL_NODE_REF;
            return n->children[idx];
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            if (n->children[byte] == 0) return NULL_NODE_REF;
            return n->children[byte];
        }
        default:
            return NULL_NODE_REF;
    }
}


/**
 * Check if leaf matches key
 */
bool leaf_matches(const data_art_leaf_t *leaf, const uint8_t *key, size_t key_len) {
    return memcmp(leaf_key(leaf), key, key_len) == 0;
}

// ============================================================================
// Core Operations - Search
// ============================================================================

// Internal get with snapshot support.
// If out_buf != NULL, copies value into it (no malloc). Otherwise, malloc's a buffer.
static const void *data_art_get_internal(data_art_tree_t *tree, node_ref_t root,
                         const uint8_t *key, size_t key_len,
                         size_t *value_len, mvcc_snapshot_t *snapshot, uint64_t snapshot_txn_id,
                         void *out_buf, size_t out_buf_size) {
    if (!tree || !key) {
        LOG_ERROR("Invalid parameters");
        return NULL;
    }

    // Reset thread-local arena — each get operation starts fresh
    data_art_reset_arena();

    // Validate key size matches tree's configured size
    if (key_len != tree->key_size) {
        LOG_ERROR("Key size mismatch: expected %zu bytes, got %zu bytes",
                  tree->key_size, key_len);
        return NULL;
    }

    node_ref_t current = root;

    if (node_ref_is_null(current)) {
        if (tls_search_trace) fprintf(stderr, "  TRACE: root is NULL\n");
        return NULL;  // Empty tree
    }

    if (tls_search_trace) {
        fprintf(stderr, "  TRACE: start traversal root=page=%lu off=%u key=",
                node_ref_page_id(root), node_ref_offset(root));
        for (size_t ki = 0; ki < key_len && ki < 20; ki++)
            fprintf(stderr, "%02x", key[ki]);
        fprintf(stderr, "\n");
    }

    size_t depth = 0;

    while (!node_ref_is_null(current)) {
        const void *node = data_art_load_node(tree, current);
        if (!node) {
            LOG_ERROR("Failed to load node at page=%lu, offset=%u",
                     node_ref_page_id(current), node_ref_offset(current));
            return NULL;
        }

        uint8_t type = *(const uint8_t *)node;

        if (tls_search_trace) {
            if (type <= DATA_NODE_256) {
                uint8_t nc = ((const uint8_t *)node)[1];
                fprintf(stderr, "  TRACE: depth=%zu node type=%u nchildren=%u page=%lu off=%u\n",
                        depth, type, nc, node_ref_page_id(current), node_ref_offset(current));
            }
        }

        // Path compression: check compressed prefix before child dispatch
        if (type <= DATA_NODE_256) {
            uint8_t plen = node_partial_len(node);
            if (plen > 0) {
                if (depth + plen > key_len ||
                    memcmp(node_partial(node), key + depth, plen) != 0) {
                    if (tls_search_trace) {
                        fprintf(stderr, "  TRACE: depth=%zu prefix mismatch (plen=%u)\n", depth, plen);
                    }
                    return NULL;  // Key doesn't match compressed prefix
                }
                depth += plen;
            }
        }

        // Check if leaf
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
            if (tls_search_trace) {
                fprintf(stderr, "  TRACE: depth=%zu LEAF page=%lu off=%u xmin=%lu xmax=%lu vlen=%u\n",
                        depth, node_ref_page_id(current), node_ref_offset(current),
                        leaf->xmin, leaf->xmax, leaf->value_len);
            }
            LOG_DEBUG("Found LEAF at page=%lu: key_size=%zu, value_len=%u, flags=0x%02x, xmin=%lu, xmax=%lu",
                      node_ref_page_id(current), tree->key_size, leaf->value_len, leaf->flags, leaf->xmin, leaf->xmax);

            // MVCC visibility check — CoW tree model
            //
            // In our CoW model, each snapshot captures its own root and traverses
            // an immutable tree.  The leaf at this position IS the correct version
            // for the reader's tree.  We do NOT walk prev_version chains because:
            //
            // 1. Non-snapshot reads follow the committed root.  The leaf here is
            //    the latest committed version.  If xmax != 0, the key is deleted.
            //
            // 2. Snapshot reads follow the snapshot's captured root.  CoW ensures
            //    the snapshot's tree still has the leaf that was current at snapshot
            //    creation time.  Use mvcc_is_visible on this leaf directly.
            //
            // Walking prev_version would incorrectly "resurface" old versions
            // whose xmax was never set (old versions are kept for structural
            // reasons but are superseded in the tree).
            {
                bool visible = true;
                if (snapshot && tree->mvcc_manager) {
                    visible = mvcc_is_visible(tree->mvcc_manager, snapshot,
                                              leaf->xmin, leaf->xmax, snapshot_txn_id);
                    if (tls_search_trace && !visible) {
                        fprintf(stderr, "  TRACE: leaf NOT visible (xmin=%lu xmax=%lu)\n",
                                leaf->xmin, leaf->xmax);
                    }
                } else if (leaf->xmax != 0) {
                    visible = false;
                    if (tls_search_trace) {
                        fprintf(stderr, "  TRACE: leaf deleted (xmax=%lu), not visible\n", leaf->xmax);
                    }
                }

                if (!visible) {
                    LOG_DEBUG("Leaf not visible: xmin=%lu xmax=%lu", leaf->xmin, leaf->xmax);
                    return NULL;
                }
            }

            // Debug: Verify the leaf structure makes sense
            if (leaf->value_len > 1024) {  // Suspiciously large
                LOG_DEBUG("CORRUPTION DETECTED: leaf at page=%lu offset=%u has suspiciously large value_len=%u, key_size=%zu",
                          node_ref_page_id(current), node_ref_offset(current), leaf->value_len, tree->key_size);
                LOG_DEBUG("Leaf dump: type=%u flags=0x%02x, overflow_page=%lu",
                          leaf->type, leaf->flags, leaf_overflow_page(leaf));
            }

            if (leaf_matches(leaf, key, key_len)) {
                if (tls_search_trace) {
                    fprintf(stderr, "  TRACE: leaf MATCHES, returning value_len=%u\n", leaf->value_len);
                }
                if (value_len) {
                    *value_len = leaf->value_len;
                }
                LOG_TRACE("Leaf matches! Returning value_len=%u", leaf->value_len);

                // Copy value into caller's buffer or malloc'd buffer
                void *value_copy;
                if (out_buf) {
                    if (leaf->value_len > out_buf_size) {
                        LOG_ERROR("Value too large for buffer: %u > %zu", leaf->value_len, out_buf_size);
                        return NULL;
                    }
                    value_copy = out_buf;
                } else {
                    value_copy = malloc(leaf->value_len);
                    if (!value_copy) {
                        LOG_ERROR("Failed to allocate memory for value");
                        return NULL;
                    }
                }

                // Handle overflow if needed
                if (leaf->flags & LEAF_FLAG_OVERFLOW) {
                    if (!data_art_read_overflow_value(tree, leaf, value_copy)) {
                        if (!out_buf) free(value_copy);
                        LOG_ERROR("Failed to read overflow value");
                        return NULL;
                    }
                    return value_copy;
                }

                // Copy inline value
                memcpy(value_copy, leaf_key(leaf) + tree->key_size, leaf->value_len);
                return value_copy;
            }
            if (tls_search_trace) {
                fprintf(stderr, "  TRACE: leaf does NOT match key\n");
            }
            return NULL;
        }

        // Get next byte to search
        // Fixed-size keys optimization: check exhaustion only when needed
        uint8_t byte;
        if (depth < key_len) {
            byte = key[depth];
            depth++;  // Move past the byte we just used for lookup
        } else {
            // Key exhausted, look for NULL byte child (leaf level)
            byte = 0x00;
        }

        current = find_child(tree, current, byte);
        if (tls_search_trace && node_ref_is_null(current)) {
            fprintf(stderr, "  TRACE: depth=%zu child for byte=0x%02x NOT FOUND → NULL\n", depth, byte);
        }
    }

    return NULL;  // Not found
}

// Public API: Get with snapshot — zero-copy reads via operation-scoped rdlock.
// Holds write_lock as rdlock to coordinate with in-place mutation writers.
// Holds resize_lock as rdlock for direct mmap pointer access (zero-copy).
const void *data_art_get_snapshot(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                                   size_t *value_len, data_art_snapshot_t *snapshot) {
    if (!tree) return NULL;

    pthread_rwlock_rdlock(&tree->write_lock);
    data_art_rdlock(tree);

    node_ref_t root;
    const void *result;
    if (snapshot) {
        root = snapshot->root;
        result = data_art_get_internal(tree, root, key, key_len, value_len,
                                        snapshot->mvcc_snapshot, snapshot->txn_id,
                                        NULL, 0);
    } else {
        root = data_art_read_committed_root(tree);
        result = data_art_get_internal(tree, root, key, key_len, value_len,
                                        NULL, 0, NULL, 0);
    }

    data_art_rdunlock(tree);
    pthread_rwlock_unlock(&tree->write_lock);
    return result;
}

// Legacy API: Get without snapshot (reads latest committed)
const void *data_art_get(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                         size_t *value_len) {
    return data_art_get_snapshot(tree, key, key_len, value_len, NULL);
}

// Zero-alloc get: copies value into caller-supplied buffer (no malloc)
bool data_art_get_into(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                       void *value_buf, size_t buf_size, size_t *value_len) {
    if (!tree) return false;

    pthread_rwlock_rdlock(&tree->write_lock);
    data_art_rdlock(tree);

    node_ref_t root = data_art_read_committed_root(tree);
    const void *result = data_art_get_internal(tree, root, key, key_len, value_len,
                                                NULL, 0, value_buf, buf_size);

    data_art_rdunlock(tree);
    pthread_rwlock_unlock(&tree->write_lock);
    return result != NULL;
}

bool data_art_contains(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    size_t len;
    const void *val = data_art_get(tree, key, key_len, &len);
    if (val) {
        free((void *)val);
        return true;
    }
    return false;
}
