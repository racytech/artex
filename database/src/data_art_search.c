/*
 * Persistent ART - Search Operations
 *
 * Implements tree lookup/search:
 * - data_art_get / data_art_get_snapshot (public API)
 * - data_art_contains
 * - Internal traversal: find_child, check_prefix, leaf_matches
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

// Forward declaration - overflow value reader from data_art_overflow.c
extern bool data_art_read_overflow_value(data_art_tree_t *tree,
                                          const data_art_leaf_t *leaf, void *out_buf);

// ============================================================================
// Lock-Free Read Helper
// ============================================================================

// Read the committed root atomically (no lock needed).
static inline node_ref_t data_art_read_committed_root(data_art_tree_t *tree) {
    uint64_t page_id = atomic_load_explicit(&tree->committed_root_page_id,
                                             memory_order_acquire);
    return (node_ref_t){.page_id = page_id, .offset = 0};
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
                 node_ref.page_id, node_ref.offset);
        return NULL_NODE_REF;
    }

    uint8_t type = *(const uint8_t *)node;
    LOG_INFO("find_child: looking for byte=0x%02x in node type=%d at page=%lu",
             byte, type, node_ref.page_id);

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
                    return (node_ref_t){.page_id = n->child_page_ids[i],
                                       .offset = n->child_offsets[i]};
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
                    return (node_ref_t){.page_id = n->child_page_ids[i],
                                       .offset = n->child_offsets[i]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            uint8_t idx = n->keys[byte];
            if (idx == 255) return NULL_NODE_REF;  // Empty slot
            return (node_ref_t){.page_id = n->child_page_ids[idx],
                               .offset = n->child_offsets[idx]};
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            uint64_t page_id = n->child_page_ids[byte];
            if (page_id == 0) return NULL_NODE_REF;
            return (node_ref_t){.page_id = page_id,
                               .offset = n->child_offsets[byte]};
        }
        default:
            return NULL_NODE_REF;
    }
}

/**
 * Check prefix match (path compression) with lazy expansion support
 * For search operations, we use optimistic matching for lazy expansion
 */
int check_prefix(data_art_tree_t *tree, node_ref_t node_ref,
                       const void *node, const uint8_t *key,
                       size_t key_len, size_t depth) {
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t partial_len = node_bytes[2];  // Offset of partial_len field
    const uint8_t *partial = node_bytes + 4;  // Offset of partial array

    int max_cmp = (partial_len < 10) ? partial_len : 10;

    // Check inline portion
    for (int i = 0; i < max_cmp; i++) {
        if (depth + i >= key_len) return i;
        if (partial[i] != key[depth + i]) return i;
    }

    // For lazy expansion (partial_len > 10):
    // We optimistically assume the remaining bytes match.
    // The final leaf comparison will verify the full key.
    // This avoids the cost of traversing to a leaf just for prefix verification.
    return partial_len;
}

/**
 * Check if leaf matches key
 */
bool leaf_matches(const data_art_leaf_t *leaf, const uint8_t *key, size_t key_len) {
    if (leaf->key_len != key_len) {
        return false;
    }
    return memcmp(leaf->data, key, key_len) == 0;
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
                     current.page_id, current.offset);
            return NULL;
        }

        uint8_t type = *(const uint8_t *)node;

        // Check if leaf
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
            LOG_DEBUG("Found LEAF at page=%lu: key_len=%u, value_len=%u, flags=0x%02x, xmin=%lu, xmax=%lu",
                      current.page_id, leaf->key_len, leaf->value_len, leaf->flags, leaf->xmin, leaf->xmax);

            // Walk version chain to find visible version
            node_ref_t version_ref = current;
            const data_art_leaf_t *visible_leaf = NULL;
            int chain_length = 0;
            const int MAX_CHAIN_LENGTH = 1000;  // Prevent infinite loops

            LOG_TRACE("Starting version chain walk from page=%lu offset=%u", version_ref.page_id, version_ref.offset);

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
                         chain_length, version_ref.page_id, version_ref.offset, candidate->xmin, candidate->xmax);

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
                LOG_ERROR("CORRUPTION DETECTED: leaf at page=%lu offset=%u has suspiciously large value_len=%u, key_len=%u",
                          current.page_id, current.offset, leaf->value_len, leaf->key_len);
                LOG_ERROR("Leaf dump: type=%u flags=0x%02x, overflow_page=%lu, inline_data_len=%u",
                          leaf->type, leaf->flags, leaf->overflow_page, leaf->inline_data_len);
                // Dump raw bytes of entire leaf header
                const uint8_t *raw = (const uint8_t *)leaf;
                LOG_ERROR("Raw bytes [0-23]: %02x %02x %02x %02x | %02x %02x %02x %02x | %02x %02x %02x %02x | %02x %02x %02x %02x %02x %02x %02x %02x | %02x %02x %02x %02x",
                          raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                          raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
                          raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22], raw[23]);
                LOG_ERROR("Field breakdown: type@0=%02x flags@1=%02x pad@2-3=%02x%02x | key_len@4-7=%02x%02x%02x%02x | value_len@8-11=%02x%02x%02x%02x | overflow@12-19=%02x%02x%02x%02x%02x%02x%02x%02x | inline_len@20-23=%02x%02x%02x%02x",
                          raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                          raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
                          raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22], raw[23]);
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
                memcpy(value_copy, leaf->data + leaf->key_len, leaf->value_len);
                return value_copy;
            }
            return NULL;
        }

        // Check compressed path
        const uint8_t *node_bytes = (const uint8_t *)node;
        uint8_t partial_len = node_bytes[2];

        if (partial_len > 0) {
            LOG_INFO("Checking prefix: node at page=%lu has partial_len=%u, current depth=%zu",
                     current.page_id, partial_len, depth);

            int prefix_match = check_prefix(tree, current, node, key, key_len, depth);

            // Check inline portion (up to 10 bytes)
            int expected_match = (partial_len < 10) ? partial_len : 10;

            LOG_INFO("Prefix check result: prefix_match=%d, expected_match=%d",
                     prefix_match, expected_match);

            if (prefix_match < expected_match) {
                // Show what mismatched - use hex for clarity
                char key_hex[64];
                char node_hex[64];
                int show_bytes = (expected_match < 10) ? expected_match : 10;

                for (int i = 0; i < show_bytes; i++) {
                    if (depth + i < key_len) {
                        snprintf(key_hex + i*3, 4, "%02x ", key[depth + i]);
                    } else {
                        snprintf(key_hex + i*3, 4, "?? ");
                    }
                    snprintf(node_hex + i*3, 4, "%02x ", node_bytes[4 + i]);
                }

                LOG_INFO("PREFIX MISMATCH at depth=%zu (matched %d/%d bytes)",
                         depth, prefix_match, expected_match);
                LOG_INFO("  Key bytes:  %s", key_hex);
                LOG_INFO("  Node bytes: %s", node_hex);

                return NULL;  // Prefix mismatch in inline portion
            }

            // For lazy expansion (partial_len > 10), verify full prefix via leaf
            if (partial_len > 10) {
                // Need to check remaining bytes - traverse to any leaf
                // But we'll do this optimistically and verify at the leaf level
                // The leaf comparison will catch any mismatch in bytes 10+
            }

            depth += partial_len;  // Skip the full compressed path
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
                     current.page_id, current.offset);
        }
    }

    LOG_INFO("Exited search loop, current is null, returning NULL");
    return NULL;  // Not found
}

// Public API: Get with snapshot — LOCK-FREE
// Full CoW ensures all pages on the committed root's path are immutable.
// Readers load the committed root atomically and traverse without any lock.
const void *data_art_get_snapshot(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                                   size_t *value_len, data_art_snapshot_t *snapshot) {
    node_ref_t root;
    if (snapshot) {
        // Use the root captured at snapshot creation time for consistent reads
        root = (node_ref_t){.page_id = snapshot->root_page_id, .offset = 0};
        return data_art_get_internal(tree, root, key, key_len, value_len,
                                      snapshot->mvcc_snapshot, snapshot->txn_id);
    } else {
        // No snapshot: read latest committed root
        root = data_art_read_committed_root(tree);
        return data_art_get_internal(tree, root, key, key_len, value_len, NULL, 0);
    }
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
