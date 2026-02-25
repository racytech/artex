/*
 * Persistent ART - Full Tree Operations
 * 
 * Complete implementation of recursive insert and delete with:
 * - Node growth (4→16→48→256)
 * - Node shrinking (256→48→16→4)
 * - Path compression handling
 * - Prefix splitting
 * 
 * This file extends data_art_core.c with the full ART algorithms.
 */

#include "data_art.h"
#include "mvcc.h"
#include "txn_buffer.h"
#include "db_error.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>

// Forward declarations (from data_art_core.c)
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                                  const void *node, size_t size);
extern void data_art_reset_arena(void);
extern void data_art_publish_root(data_art_tree_t *tree);
extern void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);

// From data_art_core.c
extern size_t get_node_size(data_art_node_type_t node_type);

// Forward declarations (from data_art_overflow.c)
extern uint64_t data_art_write_overflow_value(data_art_tree_t *tree, const void *value,
                                                size_t value_len, size_t already_written);

// Forward declarations (from data_art_node_ops.c)
extern node_ref_t data_art_add_child(data_art_tree_t *tree, node_ref_t node_ref,
                                      uint8_t byte, node_ref_t child_ref);
extern node_ref_t data_art_add_child_inplace(data_art_tree_t *tree, node_ref_t node_ref,
                                              uint8_t byte, node_ref_t child_ref);

// Forward declarations (from data_art_core.c) — in-place mutation & partial write helpers
extern void *data_art_lock_node_mut(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_unlock_node_mut(data_art_tree_t *tree);
extern bool data_art_write_partial(data_art_tree_t *tree, node_ref_t ref,
                                    size_t node_offset, const void *data, size_t len);
extern bool data_art_copy_node(data_art_tree_t *tree, node_ref_t dst, node_ref_t src, size_t size);

// Thread-local flag: skip MVCC version chain machinery when no snapshots active.
// Set by data_art_insert() / data_art_delete() before calling recursive functions.
static __thread bool tls_skip_mvcc = false;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Find any leaf descendant of a node (for lazy expansion prefix verification)
 * Recursively traverses down the first available child until reaching a leaf
 */
static const data_art_leaf_t* find_any_leaf(data_art_tree_t *tree, node_ref_t node_ref) {
    if (node_ref_is_null(node_ref)) {
        return NULL;
    }
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return NULL;
    }
    
    uint8_t type = *(const uint8_t *)node;
    
    // If it's a leaf, return it
    if (type == DATA_NODE_LEAF) {
        return (const data_art_leaf_t *)node;
    }
    
    // Otherwise, recurse to first child
    node_ref_t child_ref = NULL_NODE_REF;
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            if (n->num_children > 0) {
                child_ref = n->children[0];
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            if (n->num_children > 0) {
                child_ref = n->children[0];
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            // Find first non-empty slot
            for (int i = 0; i < 256; i++) {
                if (n->keys[i] != NODE48_EMPTY) {
                    uint8_t idx = n->keys[i];
                    child_ref = n->children[idx];
                    break;
                }
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            // Find first non-null child
            for (int i = 0; i < 256; i++) {
                if (n->children[i] != 0) {
                    child_ref = n->children[i];
                    break;
                }
            }
            break;
        }
    }
    
    return find_any_leaf(tree, child_ref);
}

/**
 * Check if leaf matches key
 */
static bool leaf_matches_key(const data_art_leaf_t *leaf,
                              const uint8_t *key, size_t key_len) {
    return memcmp(leaf->data, key, key_len) == 0;
}

/**
 * Check prefix match (path compression) with lazy expansion support
 * Returns number of matching bytes
 */
static int check_prefix_match(data_art_tree_t *tree, node_ref_t node_ref,
                              const void *node, const uint8_t *key, 
                              size_t key_len, size_t depth) {
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t partial_len = node_bytes[2];  // Offset of partial_len field
    const uint8_t *partial = node_bytes + 4;  // Offset of partial array
    
    int max_cmp = (partial_len < 10) ? partial_len : 10;
    
    // Check inline portion first
    // Fixed-size keys: depth + partial_len won't exceed key_len during normal traversal
    for (int i = 0; i < max_cmp; i++) {
        if (partial[i] != key[depth + i]) return i;
    }
    
    // For lazy expansion (partial_len > 10):
    // Find any leaf and verify the full prefix against it
    if (partial_len > 10) {
        // Fast-path: if remaining bytes would exceed key length, can't match
        // This is rare for Ethereum keys but avoids expensive leaf lookup
        if (depth + partial_len > key_len) {
            return max_cmp;  // Mismatch beyond key boundary
        }
        
        const data_art_leaf_t *leaf = find_any_leaf(tree, node_ref);
        if (leaf) {
            // Verify remaining bytes (from index 10 to partial_len)
            // Fixed-size keys: both keys have same length, simplify comparison
            for (int i = 10; i < partial_len; i++) {
                if (leaf->data[depth + i] != key[depth + i]) return i;
            }
        }
    }
    
    return partial_len;
}

/**
 * Find child node reference by byte key
 */
static node_ref_t find_child_ref(data_art_tree_t *tree, node_ref_t node_ref, uint8_t byte) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return NULL_NODE_REF;
    }
    
    uint8_t type = *(const uint8_t *)node;
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            LOG_DEBUG("[FIND_CHILD] NODE_4 at page=%lu: looking for byte=0x%02x, num_children=%u",
                     node_ref_page_id(node_ref), byte, n->num_children);
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    LOG_DEBUG("[FIND_CHILD] Found child at index %d → page=%lu", i, node_ref_page_id(n->children[i]));
                    return n->children[i];
                }
            }
            LOG_DEBUG("[FIND_CHILD] Child byte=0x%02x NOT FOUND in NODE_4 at page=%lu", byte, node_ref_page_id(node_ref));
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
            if (idx == NODE48_EMPTY) return NULL_NODE_REF;
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
 * Create a new leaf node
 */
static node_ref_t alloc_leaf(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len) {
    // Calculate leaf size
    size_t total_data = key_len + value_len;
    bool needs_overflow = total_data > MAX_INLINE_DATA;
    
    size_t inline_data_len = needs_overflow ? 
        (key_len + (MAX_INLINE_DATA - key_len)) : total_data;
    
    size_t leaf_size = sizeof(data_art_leaf_t) + inline_data_len;
    
    // Allocate leaf node
    node_ref_t leaf_ref = data_art_alloc_node(tree, leaf_size);
    if (node_ref_is_null(leaf_ref)) {
        LOG_ERROR("Failed to allocate leaf node");
        return NULL_NODE_REF;
    }
    
    // Create leaf structure
    data_art_leaf_t *leaf = malloc(leaf_size);
    if (!leaf) {
        LOG_ERROR("Failed to allocate temporary leaf");
        return NULL_NODE_REF;
    }
    
    leaf->type = DATA_NODE_LEAF;
    leaf->flags = needs_overflow ? LEAF_FLAG_OVERFLOW : LEAF_FLAG_NONE;
    leaf->value_len = value_len;
    leaf->overflow_page = 0;
    leaf->inline_data_len = inline_data_len;
    
    // Set MVCC version fields
    leaf->xmin = tree->current_txn_id > 0 ? tree->current_txn_id : 1;
    leaf->xmax = 0;  // Not deleted
    leaf->prev_version = NULL_NODE_REF;  // No older version (new insert)
    
    // Copy key
    memcpy(leaf->data, key, key_len);
    LOG_DEBUG("[ALLOC_LEAF] Copied key: %zu bytes at offset 0", key_len);
    
    // Copy value (inline portion)
    if (needs_overflow) {
        size_t inline_value_size = inline_data_len - key_len;
        memcpy(leaf->data + key_len, value, inline_value_size);
        LOG_DEBUG("[ALLOC_LEAF] Copied inline value: %zu bytes at offset %zu (overflow mode)", inline_value_size, key_len);
        
        // Write overflow pages
        uint64_t overflow_page = data_art_write_overflow_value(tree, value, 
                                                                 value_len, 
                                                                 inline_value_size);
        if (overflow_page == 0) {
            LOG_ERROR("Failed to write overflow value");
            free(leaf);
            return NULL_NODE_REF;
        }
        leaf->overflow_page = overflow_page;
    } else {
        memcpy(leaf->data + key_len, value, value_len);
        LOG_DEBUG("[ALLOC_LEAF] Copied full value: %zu bytes at offset %zu (inline mode)", value_len, key_len);
    }
    
    // Write leaf to disk
    LOG_TRACE("Writing leaf: page=%lu, value_len=%u, leaf_size=%zu",
              node_ref_page_id(leaf_ref), leaf->value_len, leaf_size);
    LOG_DEBUG("[WRITE_LEAF] BEFORE write: page=%lu offset=%u | type=%u flags=0x%02x value_len=%u overflow_page=%lu inline_data_len=%u",
              node_ref_page_id(leaf_ref), node_ref_offset(leaf_ref), leaf->type, leaf->flags, leaf->value_len, leaf->overflow_page, leaf->inline_data_len);
    if (!data_art_write_node(tree, leaf_ref, leaf, leaf_size)) {
        LOG_ERROR("Failed to write leaf node");
        free(leaf);
        return NULL_NODE_REF;
    }
    LOG_DEBUG("[WRITE_LEAF] AFTER write: page=%lu offset=%u | value_len=%u (should be unchanged)",
              node_ref_page_id(leaf_ref), node_ref_offset(leaf_ref), leaf->value_len);
    LOG_TRACE("Leaf written successfully: page=%lu, value_len=%u",
              node_ref_page_id(leaf_ref), leaf->value_len);
    
    free(leaf);
    return leaf_ref;
}

// ============================================================================
// Node Operations - Add/Remove Child
// ============================================================================

/**
 * Replace a child in a node (when child reference changes due to split/growth)
 * This modifies the node in place by rewriting it
 */
static node_ref_t replace_child_in_node(data_art_tree_t *tree, node_ref_t node_ref,
                                         uint8_t byte, node_ref_t old_child_ref,
                                         node_ref_t new_child_ref, bool inplace) {
    if (node_ref_is_null(node_ref) || node_ref_equals(old_child_ref, new_child_ref)) {
        return node_ref;  // Nothing to do
    }

    if (inplace) {
        // In-place mutation: write only the child ref directly in mmap.
        // Returns same node_ref — no CoW propagation upward.
        void *mut = data_art_lock_node_mut(tree, node_ref);
        uint8_t type = *(uint8_t *)mut;
        bool found = false;

        switch (type) {
            case DATA_NODE_4: {
                data_art_node4_t *m = (data_art_node4_t *)mut;
                for (int i = 0; i < m->num_children; i++) {
                    if (m->keys[i] == byte) {
                        m->children[i] = new_child_ref;
                        found = true;
                        break;
                    }
                }
                break;
            }
            case DATA_NODE_16: {
                data_art_node16_t *m = (data_art_node16_t *)mut;
                for (int i = 0; i < m->num_children; i++) {
                    if (m->keys[i] == byte) {
                        m->children[i] = new_child_ref;
                        found = true;
                        break;
                    }
                }
                break;
            }
            case DATA_NODE_48: {
                data_art_node48_t *m = (data_art_node48_t *)mut;
                uint8_t idx = m->keys[byte];
                if (idx != 255) {
                    m->children[idx] = new_child_ref;
                    found = true;
                }
                break;
            }
            case DATA_NODE_256: {
                data_art_node256_t *m = (data_art_node256_t *)mut;
                if (m->children[byte] != 0) {
                    m->children[byte] = new_child_ref;
                    found = true;
                }
                break;
            }
        }
        data_art_unlock_node_mut(tree);

        if (!found) {
            LOG_ERROR("[REPLACE_CHILD] inplace: byte=0x%02x not found in type=%u page=%lu",
                     byte, type, node_ref_page_id(node_ref));
            return NULL_NODE_REF;
        }
        tree->inplace_mutations++;
        return node_ref;  // Same ref — no propagation
    }

    // Optimized CoW path: alloc new slot, mmap-to-mmap copy, patch changed bytes.
    // No malloc/free needed.
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return NULL_NODE_REF;
    }

    uint8_t type = *(const uint8_t *)node;
    size_t node_size = get_node_size(type);

    // Find the child index and compute byte offset for the patch
    size_t child_off = 0;
    bool found = false;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    child_off = offsetof(data_art_node4_t, children) + i * sizeof(uint64_t);
                    found = true;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    child_off = offsetof(data_art_node16_t, children) + i * sizeof(uint64_t);
                    found = true;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            uint8_t idx = n->keys[byte];
            if (idx != 255) {
                child_off = offsetof(data_art_node48_t, children) + idx * sizeof(uint64_t);
                found = true;
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            if (n->children[byte] != 0) {
                child_off = offsetof(data_art_node256_t, children) + byte * sizeof(uint64_t);
                found = true;
            }
            break;
        }
        default:
            LOG_ERROR("Invalid node type for child replacement: %d", type);
            return NULL_NODE_REF;
    }

    if (!found) {
        LOG_ERROR("[REPLACE_CHILD] FAILED: Child byte=0x%02x not found in node type=%u at page=%lu",
                 byte, type, node_ref_page_id(node_ref));
        return NULL_NODE_REF;
    }

    // Allocate new slot, copy old node, patch the 8 changed bytes
    node_ref_t new_ref = data_art_alloc_node_hint(tree, node_size, node_ref_page_id(node_ref));
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("[REPLACE_CHILD] FAILED: Could not allocate new page for CoW");
        return NULL_NODE_REF;
    }

    data_art_copy_node(tree, new_ref, node_ref, node_size);
    data_art_write_partial(tree, new_ref, child_off, &new_child_ref, sizeof(uint64_t));

    data_art_release_page(tree, node_ref);

    LOG_DEBUG("[REPLACE_CHILD] CoW: byte=0x%02x old_parent=%lu → new_parent=%lu (child %lu → %lu)",
             byte, node_ref_page_id(node_ref), node_ref_page_id(new_ref),
             node_ref_page_id(old_child_ref), node_ref_page_id(new_child_ref));

    return new_ref;
}

/**
 * Add child to node (with automatic growth)
 * Returns new node reference (may be different if node grew)
 */
static node_ref_t add_child_to_node(data_art_tree_t *tree, node_ref_t node_ref,
                                     uint8_t byte, node_ref_t child_ref, bool inplace) {
    if (node_ref_is_null(node_ref) || node_ref_is_null(child_ref)) {
        return node_ref;
    }

    if (inplace) {
        return data_art_add_child_inplace(tree, node_ref, byte, child_ref);
    }
    return data_art_add_child(tree, node_ref, byte, child_ref);
}

// ============================================================================
// Recursive Insert
// ============================================================================

/**
 * Recursive insert with full ART logic
 */
static node_ref_t insert_recursive(data_art_tree_t *tree, node_ref_t node_ref,
                                    const uint8_t *key, size_t key_len, size_t depth,
                                    const void *value, size_t value_len,
                                    bool *inserted, bool inplace) {
    // Debug: Print key being inserted
    if (depth == 0) {
        char key_str[256];
        size_t copy_len = key_len < 255 ? key_len : 255;
        memcpy(key_str, key, copy_len);
        key_str[copy_len] = '\0';
        LOG_INFO("=== INSERTING KEY: %s (len=%zu) ===", key_str, key_len);
    }
    
    // Base case: create new leaf
    if (node_ref_is_null(node_ref)) {
        *inserted = true;
        LOG_INFO("  depth=%zu: Creating new leaf", depth);
        return alloc_leaf(tree, key, key_len, value, value_len);
    }
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node during insert");
        return node_ref;
    }
    
    uint8_t type = *(const uint8_t *)node;
    LOG_INFO("  depth=%zu: node_type=%u at page=%lu offset=%u",
             depth, type, node_ref_page_id(node_ref), node_ref_offset(node_ref));
    
    // If it's a leaf, check for match or split
    if (type == DATA_NODE_LEAF) {
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
        
        // Update existing leaf
        if (leaf_matches_key(leaf, key, key_len)) {
            *inserted = false;  // Updated, not inserted
            LOG_DEBUG("[UPDATE_LEAF] Updating leaf at page=%lu (old value_len=%u, xmin=%lu, xmax=%lu)",
                     node_ref_page_id(node_ref), leaf->value_len, leaf->xmin, leaf->xmax);
            
            // Save old leaf information
            uint64_t old_overflow_page = leaf->overflow_page;
            bool had_overflow = (leaf->flags & LEAF_FLAG_OVERFLOW) != 0;
            node_ref_t old_leaf_ref = node_ref;
            
            // Create new version of the leaf
            node_ref_t new_leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
            if (node_ref_is_null(new_leaf_ref)) {
                LOG_ERROR("Failed to allocate new leaf version");
                return node_ref;
            }

            if (tls_skip_mvcc) {
                // Fast path: no snapshots active, skip version chain & xmax marking.
                // Release old leaf immediately since no snapshot can reference it.
                data_art_release_page(tree, old_leaf_ref);
                LOG_DEBUG("[UPDATE_LEAF] skip_mvcc: replaced leaf page=%lu -> page=%lu",
                         node_ref_page_id(old_leaf_ref), node_ref_page_id(new_leaf_ref));
                return new_leaf_ref;
            }

            // Full MVCC path: link new version to old version (version chain)
            data_art_leaf_t *new_leaf = (data_art_leaf_t *)data_art_load_node(tree, new_leaf_ref);
            if (!new_leaf) {
                LOG_ERROR("Failed to load newly created leaf");
                return node_ref;
            }

            // Create a modifiable copy
            size_t new_leaf_size = sizeof(data_art_leaf_t) + new_leaf->inline_data_len;
            data_art_leaf_t *new_leaf_copy = malloc(new_leaf_size);
            if (!new_leaf_copy) {
                LOG_ERROR("Failed to allocate memory for leaf copy");
                return node_ref;
            }
            memcpy(new_leaf_copy, new_leaf, new_leaf_size);

            // Set version chain link
            new_leaf_copy->prev_version = old_leaf_ref;

            // Write back the modified new leaf
            if (!data_art_write_node(tree, new_leaf_ref, new_leaf_copy, new_leaf_size)) {
                LOG_ERROR("Failed to write version chain link");
                free(new_leaf_copy);
                return node_ref;
            }
            free(new_leaf_copy);

            // Mark old version as superseded (set xmax) for MVCC version chains
            if (tree->mvcc_manager) {
                size_t old_leaf_size = sizeof(data_art_leaf_t) + leaf->inline_data_len;
                data_art_leaf_t *old_leaf_copy = malloc(old_leaf_size);
                if (old_leaf_copy) {
                    memcpy(old_leaf_copy, leaf, old_leaf_size);
                    old_leaf_copy->xmax = tree->current_txn_id;
                    data_art_write_node(tree, old_leaf_ref, old_leaf_copy, old_leaf_size);
                    free(old_leaf_copy);
                    LOG_DEBUG("[UPDATE_LEAF] Marked old version (page=%lu) as superseded: xmax=%lu",
                             node_ref_page_id(old_leaf_ref), tree->current_txn_id);
                }
            } else {
                LOG_DEBUG("[UPDATE_LEAF] Physical update: old version (page=%lu) kept for version chain",
                         node_ref_page_id(old_leaf_ref));
            }

            LOG_DEBUG("[UPDATE_LEAF] Created new version at page=%lu (new value_len=%zu) -> prev_version=page=%lu",
                     node_ref_page_id(new_leaf_ref), value_len, node_ref_page_id(old_leaf_ref));

            return new_leaf_ref;
        }
        
        // Keys differ - need to create a new node to hold both leaves
        *inserted = true;
        
        // IMPORTANT: Copy the existing key data NOW before any other operations
        // because data_art_load_node may use a static buffer that gets overwritten
        const uint8_t *existing_key = leaf->data;
        size_t existing_key_len = tree->key_size;
        
        uint8_t existing_key_copy[256];  // Temporary buffer for existing key
        if (existing_key_len > sizeof(existing_key_copy)) {
            LOG_ERROR("Existing key too long: %zu", existing_key_len);
            return node_ref;
        }
        memcpy(existing_key_copy, existing_key, existing_key_len);
        
        // Calculate longest common prefix between the two keys
        // IMPORTANT: Start comparison from current depth, not from 0!
        size_t common_prefix_len = 0;
        size_t max_compare_from_depth = (key_len - depth < existing_key_len - depth) ? 
                                         (key_len - depth) : (existing_key_len - depth);
        
        while (common_prefix_len < max_compare_from_depth && 
               existing_key_copy[depth + common_prefix_len] == key[depth + common_prefix_len]) {
            common_prefix_len++;
        }
        
        // Create new NODE_4 to hold both leaves
        node_ref_t new_node_ref = data_art_alloc_node(tree, sizeof(data_art_node4_t));
        if (node_ref_is_null(new_node_ref)) {
            LOG_ERROR("Failed to allocate NODE_4 for leaf split");
            return node_ref;
        }
        
        data_art_node4_t new_node;
        memset(&new_node, 0, sizeof(new_node));
        new_node.type = DATA_NODE_4;
        new_node.num_children = 0;
        
        // Set compressed path (up to 10 bytes)
        size_t prefix_to_store = (common_prefix_len > 10) ? 10 : common_prefix_len;
        new_node.partial_len = common_prefix_len;
        if (prefix_to_store > 0) {
            memcpy(new_node.partial, key + depth, prefix_to_store);
        }
        
        // Write the new node
        if (!data_art_write_node(tree, new_node_ref, &new_node, sizeof(new_node))) {
            LOG_ERROR("Failed to write NODE_4");
            return node_ref;
        }
        
        // Create new leaf for the key we're inserting
        node_ref_t new_leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
        if (node_ref_is_null(new_leaf_ref)) {
            LOG_ERROR("Failed to create new leaf");
            return node_ref;
        }
        
        // Determine the bytes where the keys differ
        // Fixed-size keys: both keys have same length, split point must be valid
        size_t split_pos = depth + common_prefix_len;
        uint8_t existing_byte = (split_pos < existing_key_len) ? 
                                 existing_key_copy[split_pos] : 0x00;
        uint8_t new_byte = (split_pos < key_len) ? key[split_pos] : 0x00;
        
        // Sanity check: keys must differ at split point (duplicate keys caught earlier)
        if (existing_byte == new_byte) {
            LOG_ERROR("BUG: Keys don't differ at split point (depth=%zu, common_prefix=%zu)", 
                     depth, common_prefix_len);
            LOG_ERROR("  Both bytes are 0x%02x - duplicate key not caught by leaf_matches_key()", 
                     existing_byte);
            return node_ref;
        }
        
        // Add both leaves as children to the new node (always CoW — fresh node)
        new_node_ref = add_child_to_node(tree, new_node_ref, existing_byte, node_ref, false);
        if (node_ref_is_null(new_node_ref)) {
            LOG_ERROR("Failed to add existing leaf to new node");
            return node_ref;
        }

        new_node_ref = add_child_to_node(tree, new_node_ref, new_byte, new_leaf_ref, false);
        if (node_ref_is_null(new_node_ref)) {
            LOG_ERROR("Failed to add new leaf to new node");
            return node_ref;
        }
        
        return new_node_ref;
    }
    
    // Check compressed path (prefix)
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t partial_len = node_bytes[2];
    
    LOG_INFO("  depth=%zu: checking prefix, partial_len=%u", depth, partial_len);
    
    if (partial_len > 0) {
        // IMPORTANT: Make a copy of the node BEFORE calling check_prefix_match,
        // because check_prefix_match may call find_any_leaf which overwrites temp_page
        size_t node_size = get_node_size(type);
        void *node_copy = malloc(node_size);
        if (!node_copy) {
            LOG_ERROR("Failed to allocate memory for node copy before prefix check");
            return node_ref;
        }
        memcpy(node_copy, node, node_size);
        
        int prefix_match = check_prefix_match(tree, node_ref, node, key, key_len, depth);
        
        LOG_INFO("  depth=%zu: prefix_match=%d/%u", depth, prefix_match, partial_len);
        
        if (prefix_match < partial_len) {
            // Prefix mismatch - need to split the node at the mismatch point
            *inserted = true;
            
            LOG_INFO("  depth=%zu: PREFIX SPLIT needed at position %d", depth, prefix_match);
            
            // Create new NODE_4 to become the parent
            node_ref_t new_parent_ref = data_art_alloc_node(tree, sizeof(data_art_node4_t));
            if (node_ref_is_null(new_parent_ref)) {
                free(node_copy);
                LOG_ERROR("Failed to allocate NODE_4 for prefix split");
                return node_ref;
            }
            
            data_art_node4_t new_parent;
            memset(&new_parent, 0, sizeof(new_parent));
            new_parent.type = DATA_NODE_4;
            new_parent.num_children = 0;
            new_parent.partial_len = prefix_match;  // Common portion
            
            // Debug: show what the original partial prefix was
            LOG_INFO("  Original partial prefix (%u bytes): '%.*s'", 
                     partial_len, partial_len < 10 ? partial_len : 10, node_bytes + 4);
            
            // Copy prefix from the KEY, not from old node's partial array
            // (old node's partial may only have first 10 bytes if partial_len > 10)
            size_t prefix_to_store = (prefix_match > 10) ? 10 : prefix_match;
            if (prefix_to_store > 0) {
                memcpy(new_parent.partial, key + depth, prefix_to_store);
                LOG_INFO("  New parent prefix (%zu bytes): '%.*s'", 
                         prefix_to_store, (int)prefix_to_store, new_parent.partial);
            }
            
            // Write new parent
            if (!data_art_write_node(tree, new_parent_ref, &new_parent, sizeof(new_parent))) {
                free(node_copy);
                LOG_ERROR("Failed to write new parent node");
                return node_ref;
            }
            
            // Update old node's prefix (remove the common part)
            // Use node_copy since the original 'node' pointer may be invalid
            void *modified_node = malloc(node_size);
            if (!modified_node) {
                free(node_copy);
                LOG_ERROR("Failed to allocate memory for node modification");
                return node_ref;
            }
            memcpy(modified_node, node_copy, node_size);
            free(node_copy);  // Done with node_copy
            
            // Log old node structure before modification
            if (type == DATA_NODE_4) {
                const data_art_node4_t *old_n4 = (const data_art_node4_t *)modified_node;
                LOG_INFO("  OLD NODE_4 before modification: num_children=%u, partial_len=%u", 
                         old_n4->num_children, old_n4->partial_len);
                for (int i = 0; i < old_n4->num_children; i++) {
                    LOG_INFO("    child[%d]: key=0x%02x, page=%lu, offset=%u",
                             i, old_n4->keys[i], node_ref_page_id(old_n4->children[i]), node_ref_offset(old_n4->children[i]));
                }
            }
            
            // *** CRITICAL: Get the discriminating byte BEFORE modifying the node! ***
            // Get the byte where the old node diverges
            // For lazy expansion: if prefix_match >= 10, we need to get the byte from a leaf
            // since the partial array only stores first 10 bytes
            uint8_t old_byte;
            if (prefix_match < 10 && partial_len <= 10) {
                // Can read from original partial array (before we modify it)
                const uint8_t *partial = node_bytes + 4;
                old_byte = partial[prefix_match];
                LOG_INFO("  Getting old_byte from original partial[%d] = 0x%02x ('%c')", 
                         prefix_match, old_byte, old_byte >= 32 && old_byte < 127 ? old_byte : '?');
            } else {
                // Need to read from a leaf descendant (lazy expansion case)
                const data_art_leaf_t *leaf = find_any_leaf(tree, node_ref);
                if (leaf && depth + prefix_match < tree->key_size) {
                    old_byte = leaf->data[depth + prefix_match];

                    // Debug: show what leaf we got and what byte
                    char leaf_key[256];
                    size_t copy_len = tree->key_size < 255 ? tree->key_size : 255;
                    memcpy(leaf_key, leaf->data, copy_len);
                    leaf_key[copy_len] = '\0';
                    LOG_INFO("  Getting old_byte from leaf key '%s' at position %zu = 0x%02x ('%c')",
                             leaf_key, depth + prefix_match, old_byte,
                             old_byte >= 32 && old_byte < 127 ? old_byte : '?');
                } else {
                    // Fallback: use NULL byte
                    old_byte = 0x00;
                    LOG_INFO("  Using fallback old_byte = 0x00");
                }
            }
            
            uint8_t *mod_bytes = (uint8_t *)modified_node;
            uint8_t remaining_prefix_len = partial_len - prefix_match - 1;
            mod_bytes[2] = remaining_prefix_len;  // Update partial_len
            
            LOG_INFO("  PREFIX SPLIT: partial_len %u -> new_parent %u, modified_node %u", 
                     partial_len, prefix_match, remaining_prefix_len);
            
            // Reconstruct the remaining prefix from the OLD node's prefix
            // After split: parent has [0..prefix_match-1], byte at prefix_match becomes discriminator,
            // old node keeps [prefix_match+1..partial_len-1]
            if (remaining_prefix_len > 0) {
                size_t bytes_to_copy = (remaining_prefix_len > 10) ? 10 : remaining_prefix_len;
                
                // Copy from old node's prefix, NOT from the new key being inserted!
                if (prefix_match + 1 < 10 && prefix_match + 1 + bytes_to_copy <= 10 && partial_len <= 10) {
                    // Old node had non-lazy prefix, copy from partial array
                    memmove(mod_bytes + 4, node_bytes + 4 + prefix_match + 1, bytes_to_copy);
                } else {
                    // Old node had lazy expansion - need to get bytes from a leaf in old subtree
                    const data_art_leaf_t *leaf = find_any_leaf(tree, node_ref);
                    if (leaf && depth + prefix_match + 1 + bytes_to_copy <= tree->key_size) {
                        memcpy(mod_bytes + 4, leaf->data + depth + prefix_match + 1, bytes_to_copy);
                    }
                }
            }
            
            // Allocate new page for modified old node (hint = old page for same-page COW)
            node_ref_t modified_node_ref = data_art_alloc_node_hint(tree, node_size, node_ref_page_id(node_ref));
            if (node_ref_is_null(modified_node_ref)) {
                free(modified_node);
                LOG_ERROR("Failed to allocate page for modified node");
                return node_ref;
            }
            
            if (!data_art_write_node(tree, modified_node_ref, modified_node, node_size)) {
                free(modified_node);
                LOG_ERROR("Failed to write modified node");
                return node_ref;
            }
            free(modified_node);
            
            LOG_INFO("  Adding modified_node (page=%lu) as child with key=0x%02x to new_parent (page=%lu)",
                     node_ref_page_id(modified_node_ref), old_byte, node_ref_page_id(new_parent_ref));
            
            // Add modified old node as child (always CoW — fresh parent node)
            new_parent_ref = add_child_to_node(tree, new_parent_ref, old_byte, modified_node_ref, false);
            if (node_ref_is_null(new_parent_ref)) {
                LOG_ERROR("Failed to add modified node to new parent");
                return node_ref;
            }
            
            // Create leaf for new key and add it
            node_ref_t new_leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
            if (node_ref_is_null(new_leaf_ref)) {
                LOG_ERROR("Failed to create new leaf");
                return new_parent_ref;
            }
            
            uint8_t new_byte = (depth + prefix_match < key_len) ? 
                               key[depth + prefix_match] : 0x00;
            
            LOG_INFO("  Creating new_leaf for key 149, will add as child with key=0x%02x ('%c')", 
                     new_byte, new_byte >= 32 && new_byte < 127 ? new_byte : '?');
            
            new_parent_ref = add_child_to_node(tree, new_parent_ref, new_byte, new_leaf_ref, false);
            if (node_ref_is_null(new_parent_ref)) {
                LOG_ERROR("Failed to add new leaf to new parent");
                return new_parent_ref;
            }
            
            return new_parent_ref;
        }
        
        depth += partial_len;
    }
    
    // Determine which byte to insert for
    // Fixed-size keys: simplified byte extraction
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    LOG_INFO("  depth=%zu: looking for child with byte=0x%02x", depth, byte);
    
    // Find child for this byte
    node_ref_t child_ref = find_child_ref(tree, node_ref, byte);
    
    if (!node_ref_is_null(child_ref)) {
        LOG_INFO("  depth=%zu: child found at page=%lu offset=%u, recursing...",
                 depth, node_ref_page_id(child_ref), node_ref_offset(child_ref));
        // Child exists - recurse
        node_ref_t new_child_ref = insert_recursive(tree, child_ref, key, key_len,
                                                     depth + 1, value, value_len,
                                                     inserted, inplace);

        // If child changed (due to split or growth), update parent's reference
        if (!node_ref_equals(new_child_ref, child_ref)) {
            LOG_DEBUG("  [UPDATE_PARENT] depth=%zu: child changed from page=%lu to page=%lu, updating parent at page=%lu",
                     depth, node_ref_page_id(child_ref), node_ref_page_id(new_child_ref), node_ref_page_id(node_ref));
            node_ref_t new_node_ref = replace_child_in_node(tree, node_ref, byte,
                                                             child_ref, new_child_ref, inplace);
            if (node_ref_is_null(new_node_ref)) {
                LOG_ERROR("  [UPDATE_PARENT] FAILED to update child reference in parent");
                return node_ref;
            }
            return new_node_ref;  // With inplace: same ref, no further propagation
        }

        return node_ref;
    } else {
        // No child - add new leaf here
        LOG_INFO("  depth=%zu: no child found, creating new leaf for byte=0x%02x", depth, byte);
        *inserted = true;
        node_ref_t leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
        if (!node_ref_is_null(leaf_ref)) {
            LOG_INFO("  depth=%zu: adding leaf at page=%lu as child", depth, node_ref_page_id(leaf_ref));
            node_ref_t new_node_ref = add_child_to_node(tree, node_ref, byte, leaf_ref, inplace);
            LOG_INFO("  depth=%zu: after adding child, node ref: page=%lu offset=%u",
                     depth, node_ref_page_id(new_node_ref), node_ref_offset(new_node_ref));
            return new_node_ref;
        }
    }
    
    return node_ref;
}

// ============================================================================
// Public API Implementation
// ============================================================================

/**
 * Full recursive insert implementation with node growth
 */
bool data_art_insert(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                     const void *value, size_t value_len) {
    if (!tree || !key || !value) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree, key, or value is NULL");
        return false;
    }

    // Validate key size matches tree's configured size
    if (key_len != tree->key_size) {
        DB_ERROR(DB_ERROR_INVALID_ARG,
            "key size mismatch: expected %zu, got %zu", tree->key_size, key_len);
        return false;
    }
    
    // If in transaction, buffer the operation instead of applying immediately
    if (tree->txn_buffer) {
        return txn_buffer_add_insert(tree->txn_buffer, key, key_len, value, value_len);
    }
    
    // Reset thread-local arena — each insert starts fresh
    data_art_reset_arena();

    // Acquire write lock to serialize all write operations (one writer at a time)
    pthread_rwlock_wrlock(&tree->write_lock);

    // Auto-commit MVCC transaction setup
    uint64_t auto_txn_id = 0;
    bool auto_commit = (tree->current_txn_id == 0);
    bool skip_mvcc = false;

    if (auto_commit) {
        if (tree->mvcc_manager &&
            mvcc_has_active_snapshots(tree->mvcc_manager)) {
            // Snapshots active: full MVCC required for visibility correctness
            if (!mvcc_begin_txn(tree->mvcc_manager, &auto_txn_id)) {
                pthread_rwlock_unlock(&tree->write_lock);
                DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "failed to begin auto-commit txn");
                return false;
            }
            tree->current_txn_id = auto_txn_id;
        } else {
            // No snapshots (or no MVCC manager): skip full MVCC machinery,
            // use monotonic version counter as xmin for the leaf.
            skip_mvcc = true;
            tree->version++;
            tree->current_txn_id = tree->version;
        }
    }

    tls_skip_mvcc = skip_mvcc;
    bool inplace = skip_mvcc;  // In-place mutation when no snapshots active

    bool inserted = false;
    node_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                            value, value_len, &inserted, inplace);

    if (!node_ref_is_null(new_root)) {
        tree->root = new_root;

        if (inserted) {
            tree->size++;
        }

        // Commit MVCC transaction if we used the full machinery
        if (auto_commit && !skip_mvcc && tree->mvcc_manager) {
            mvcc_commit_txn(tree->mvcc_manager, auto_txn_id);
        }
        tree->current_txn_id = 0;

        // Publish new root for readers
        data_art_publish_root(tree);

        pthread_rwlock_unlock(&tree->write_lock);
        return true;
    }

    // Failed to insert - abort auto-commit transaction if we started one
    if (auto_commit && !skip_mvcc && tree->mvcc_manager) {
        mvcc_abort_txn(tree->mvcc_manager, auto_txn_id);
    }
    tree->current_txn_id = 0;

    pthread_rwlock_unlock(&tree->write_lock);
    if (db_get_last_error() == DB_OK) {
        DB_ERROR(DB_ERROR_IO, "recursive insert failed");
    }
    return false;
}

// ============================================================================
// Internal Insert (for optimized commit path)
// ============================================================================

/**
 * Internal insert — called from commit path with write_lock already held.
 * No locking, no auto-commit MVCC, no WAL logging, no root publication.
 * Caller is responsible for: write_lock, MVCC txn, WAL logging, root publish.
 */
bool data_art_insert_internal(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len) {
    if (!tree || !key || !value) return false;
    if (key_len != tree->key_size) return false;

    data_art_reset_arena();

    // Internal path is called from commit_txn under write_lock.
    // Snapshots may be active during explicit transactions, so use CoW (no inplace).
    bool inserted = false;
    node_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                            value, value_len, &inserted, false);
    if (node_ref_is_null(new_root)) return false;

    tree->root = new_root;
    if (inserted) tree->size++;
    return true;
}
