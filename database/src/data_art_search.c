/*
 * Persistent ART - Search Operations
 *
 * Implements tree lookup/search:
 * - data_art_get / data_art_get_snapshot (public API)
 * - data_art_contains
 * - Internal traversal: find_child, leaf_matches
 * - Version chain walk with MVCC visibility checks
 *
 * Search operations are fully lock-free. Readers load the committed root
 * atomically and traverse immutable CoW pages without holding any lock.
 */

#include "data_art.h"
#include "mvcc.h"
#include "logger.h"

#include <stdlib.h>
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
        LOG_INFO("find_child: failed to load node at page=%lu offset=%u",
                 node_ref_page_id(node_ref), node_ref_offset(node_ref));
        return NULL_NODE_REF;
    }

    uint8_t type = *(const uint8_t *)node;
    LOG_INFO("find_child: looking for byte=0x%02x in node type=%d at page=%lu",
             byte, type, node_ref_page_id(node_ref));

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            // Debug: log available keys
            char keys_str[32] = {0};
            for (int i = 0; i < n->num_children && i < 4; i++) {
                char temp[8];
                snprintf(temp, sizeof(temp), "0x%02x ", n->keys[i]);
                strcat(keys_str, temp);
            }
            LOG_INFO("find_child NODE_4: looking for 0x%02x, available keys: %s", byte, keys_str);

            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    return n->children[i];
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            // Debug: log available keys
            char keys_str[128] = {0};
            for (int i = 0; i < n->num_children && i < 16; i++) {
                char temp[8];
                snprintf(temp, sizeof(temp), "0x%02x ", n->keys[i]);
                strcat(keys_str, temp);
            }
            LOG_INFO("find_child NODE_16: looking for 0x%02x, num_children=%d, available keys: %s",
                     byte, n->num_children, keys_str);

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
            if (idx == 255) return NULL_NODE_REF;  // Empty slot
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

// Internal get with snapshot support
static const void *data_art_get_internal(data_art_tree_t *tree, node_ref_t root,
                         const uint8_t *key, size_t key_len,
                         size_t *value_len, mvcc_snapshot_t *snapshot, uint64_t snapshot_txn_id) {
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
        return NULL;  // Empty tree
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

        // Check if leaf
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
            LOG_DEBUG("Found LEAF at page=%lu: key_size=%zu, value_len=%u, flags=0x%02x, xmin=%lu, xmax=%lu",
                      node_ref_page_id(current), tree->key_size, leaf->value_len, leaf->flags, leaf->xmin, leaf->xmax);

            // Walk version chain to find visible version
            node_ref_t version_ref = current;
            const data_art_leaf_t *visible_leaf = NULL;
            int chain_length = 0;
            const int MAX_CHAIN_LENGTH = 1000;  // Prevent infinite loops

            LOG_TRACE("Starting version chain walk from page=%lu offset=%u", node_ref_page_id(version_ref), node_ref_offset(version_ref));

            // Start with the leaf we already loaded
            const data_art_leaf_t *candidate = leaf;

            while (chain_length < MAX_CHAIN_LENGTH) {
                if (!candidate || candidate->type != DATA_NODE_LEAF) {
                    LOG_ERROR("Invalid version chain: candidate=%p, type=%u",
                              (void*)candidate,
                              candidate ? candidate->type : 255);
                    break;
                }

                LOG_TRACE("Checking version chain[%d]: page=%lu offset=%u xmin=%lu xmax=%lu",
                         chain_length, node_ref_page_id(version_ref), node_ref_offset(version_ref), candidate->xmin, candidate->xmax);

                // MVCC visibility check
                bool visible = true;
                if (snapshot && tree->mvcc_manager) {
                    visible = mvcc_is_visible(tree->mvcc_manager, snapshot,
                                              candidate->xmin, candidate->xmax, snapshot_txn_id);
                    if (!visible) {
                        LOG_TRACE("Version not visible to snapshot (xmin=%lu, xmax=%lu)",
                                 candidate->xmin, candidate->xmax);
                    }
                } else if (candidate->xmax != 0) {
                    // No snapshot but leaf is deleted - not visible
                    visible = false;
                    LOG_TRACE("Version is deleted (xmax=%lu), not visible", candidate->xmax);
                }

                if (visible) {
                    visible_leaf = candidate;
                    LOG_DEBUG("Found visible version at chain[%d]: xmin=%lu xmax=%lu",
                             chain_length, candidate->xmin, candidate->xmax);
                    break;
                }

                // Move to previous version
                if (node_ref_is_null(candidate->prev_version)) {
                    LOG_DEBUG("End of version chain (no prev_version)");
                    break;
                }

                version_ref = candidate->prev_version;
                chain_length++;

                // Load next version in chain
                candidate = (const data_art_leaf_t *)data_art_load_node(tree, version_ref);
            }

            if (chain_length >= MAX_CHAIN_LENGTH) {
                LOG_ERROR("Version chain too long (>%d), possible corruption", MAX_CHAIN_LENGTH);
                return NULL;
            }

            if (!visible_leaf) {
                LOG_DEBUG("No visible version found in chain (length=%d)", chain_length);
                return NULL;  // No visible version in chain
            }

            leaf = visible_leaf;  // Use the visible version

            // Debug: Verify the leaf structure makes sense
            if (leaf->value_len > 1024) {  // Suspiciously large
                LOG_ERROR("CORRUPTION DETECTED: leaf at page=%lu offset=%u has suspiciously large value_len=%u, key_size=%zu",
                          node_ref_page_id(current), node_ref_offset(current), leaf->value_len, tree->key_size);
                LOG_ERROR("Leaf dump: type=%u flags=0x%02x, overflow_page=%lu",
                          leaf->type, leaf->flags, leaf_overflow_page(leaf));
            }

            if (leaf_matches(leaf, key, key_len)) {
                if (value_len) {
                    *value_len = leaf->value_len;
                }
                LOG_TRACE("Leaf matches! Returning value_len=%u", leaf->value_len);

                // Allocate buffer for value (caller must free)
                void *value_copy = malloc(leaf->value_len);
                if (!value_copy) {
                    LOG_ERROR("Failed to allocate memory for value");
                    return NULL;
                }

                // Handle overflow if needed
                if (leaf->flags & LEAF_FLAG_OVERFLOW) {
                    // Read full value from overflow chain
                    if (!data_art_read_overflow_value(tree, leaf, value_copy)) {
                        free(value_copy);
                        LOG_ERROR("Failed to read overflow value");
                        return NULL;
                    }
                    return value_copy;
                }

                // Copy inline value
                memcpy(value_copy, leaf_key(leaf) + tree->key_size, leaf->value_len);
                return value_copy;
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
        if (node_ref_is_null(current)) {
            // Debug: child not found
            char key_str[64];
            snprintf(key_str, sizeof(key_str), "%.*s", (int)(key_len < 40 ? key_len : 40), key);
            LOG_INFO("Child lookup failed: key='%s', depth=%zu, byte=0x%02x('%c')",
                     key_str, depth, byte, (byte >= 32 && byte < 127) ? byte : '?');
        } else {
            LOG_INFO("Child found, advancing to page=%lu offset=%u",
                     node_ref_page_id(current), node_ref_offset(current));
        }
    }

    LOG_INFO("Exited search loop, current is null, returning NULL");
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
                                        snapshot->mvcc_snapshot, snapshot->txn_id);
    } else {
        root = data_art_read_committed_root(tree);
        result = data_art_get_internal(tree, root, key, key_len, value_len, NULL, 0);
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

bool data_art_contains(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    size_t len;
    return data_art_get(tree, key, key_len, &len) != NULL;
}
