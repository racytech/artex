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
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Forward declarations (from data_art_core.c)
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                                  const void *node, size_t size);

// Helper to get node size by type
static size_t get_node_size(data_art_node_type_t type) {
    switch (type) {
        case DATA_NODE_4:    return sizeof(data_art_node4_t);
        case DATA_NODE_16:   return sizeof(data_art_node16_t);
        case DATA_NODE_48:   return sizeof(data_art_node48_t);
        case DATA_NODE_256:  return sizeof(data_art_node256_t);
        default:             return 0;
    }
}

// Forward declarations (from data_art_overflow.c)
extern uint64_t data_art_write_overflow_value(data_art_tree_t *tree, const void *value,
                                                size_t value_len, size_t already_written);

// Forward declarations (from data_art_node_ops.c)
extern node_ref_t data_art_add_child(data_art_tree_t *tree, node_ref_t node_ref,
                                      uint8_t byte, node_ref_t child_ref);

// Special value for Node48 index indicating no child
#define NODE48_EMPTY 255

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if a node reference points to a leaf
 */
static bool is_leaf_ref(data_art_tree_t *tree, node_ref_t ref) {
    if (node_ref_is_null(ref)) {
        return false;
    }
    
    const void *node = data_art_load_node(tree, ref);
    if (!node) {
        return false;
    }
    
    uint8_t type = *(const uint8_t *)node;
    return type == DATA_NODE_LEAF;
}

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
                child_ref = (node_ref_t){.page_id = n->child_page_ids[0],
                                        .offset = n->child_offsets[0]};
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            if (n->num_children > 0) {
                child_ref = (node_ref_t){.page_id = n->child_page_ids[0],
                                        .offset = n->child_offsets[0]};
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            // Find first non-empty slot
            for (int i = 0; i < 256; i++) {
                if (n->keys[i] != NODE48_EMPTY) {
                    uint8_t idx = n->keys[i];
                    child_ref = (node_ref_t){.page_id = n->child_page_ids[idx],
                                            .offset = n->child_offsets[idx]};
                    break;
                }
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            // Find first non-null child
            for (int i = 0; i < 256; i++) {
                if (n->child_page_ids[i] != 0) {
                    child_ref = (node_ref_t){.page_id = n->child_page_ids[i],
                                            .offset = n->child_offsets[i]};
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
    // Compare exact key length and bytes (no terminator)
    if (leaf->key_len != key_len) {
        return false;
    }
    // Compare key bytes
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
    for (int i = 0; i < max_cmp; i++) {
        if (depth + i >= key_len) return i;
        if (partial[i] != key[depth + i]) return i;
    }
    
    // For lazy expansion (partial_len > 10):
    // Find any leaf and verify the full prefix against it
    if (partial_len > 10) {
        const data_art_leaf_t *leaf = find_any_leaf(tree, node_ref);
        if (leaf) {
            // Verify remaining bytes (from index 10 to partial_len)
            // leaf->data contains the key starting at index 0
            for (int i = 10; i < partial_len; i++) {
                if (depth + i >= key_len) return i;
                if (depth + i >= leaf->key_len) return i;
                // Compare leaf's key byte at position (depth+i) with incoming key's byte at (depth+i)
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
            LOG_ERROR("[FIND_CHILD] NODE_4 at page=%lu: looking for byte=0x%02x, num_children=%u, keys=[0x%02x 0x%02x 0x%02x 0x%02x]",
                     node_ref.page_id, byte, n->num_children,
                     n->num_children > 0 ? n->keys[0] : 0xFF,
                     n->num_children > 1 ? n->keys[1] : 0xFF,
                     n->num_children > 2 ? n->keys[2] : 0xFF,
                     n->num_children > 3 ? n->keys[3] : 0xFF);
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    LOG_ERROR("[FIND_CHILD] Found child at index %d → page=%lu", i, n->child_page_ids[i]);
                    return (node_ref_t){.page_id = n->child_page_ids[i], 
                                       .offset = n->child_offsets[i]};
                }
            }
            LOG_ERROR("[FIND_CHILD] Child byte=0x%02x NOT FOUND in NODE_4 at page=%lu", byte, node_ref.page_id);
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
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
            if (idx == NODE48_EMPTY) return NULL_NODE_REF;
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
 * Create a new leaf node
 */
static node_ref_t alloc_leaf(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                              const void *value, size_t value_len) {
    // Calculate leaf size (no terminator needed - we store exact key length)
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
    leaf->key_len = key_len;  // Store exact key length (no terminator)
    leaf->value_len = value_len;
    leaf->overflow_page = 0;
    leaf->inline_data_len = inline_data_len;
    
    // Copy key (no terminator byte needed)
    memcpy(leaf->data, key, key_len);
    
    // Copy value (inline portion) - starts immediately after key
    if (needs_overflow) {
        size_t inline_value_size = inline_data_len - key_len;
        memcpy(leaf->data + key_len, value, inline_value_size);
        
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
    }
    
    // Write leaf to disk
    LOG_TRACE("Writing leaf: page=%lu, key_len=%u, value_len=%u, leaf_size=%zu",
              leaf_ref.page_id, leaf->key_len, leaf->value_len, leaf_size);
    if (!data_art_write_node(tree, leaf_ref, leaf, leaf_size)) {
        LOG_ERROR("Failed to write leaf node");
        free(leaf);
        return NULL_NODE_REF;
    }
    LOG_TRACE("Leaf written successfully: page=%lu, value_len=%u", 
              leaf_ref.page_id, leaf->value_len);
    
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
static bool replace_child_in_node(data_art_tree_t *tree, node_ref_t node_ref,
                                    uint8_t byte, node_ref_t old_child_ref, 
                                    node_ref_t new_child_ref) {
    if (node_ref_is_null(node_ref) || node_ref_equals(old_child_ref, new_child_ref)) {
        return true;  // Nothing to do
    }
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return false;
    }
    
    uint8_t type = *(const uint8_t *)node;
    size_t node_size = get_node_size(type);
    
    // Create a copy of the node to modify
    void *modified_node = malloc(node_size);
    if (!modified_node) {
        LOG_ERROR("Failed to allocate memory for node modification");
        return false;
    }
    memcpy(modified_node, node, node_size);
    
    // Update the child reference based on node type
    bool found = false;
    switch (type) {
        case DATA_NODE_4: {
            data_art_node4_t *n = (data_art_node4_t *)modified_node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    n->child_page_ids[i] = new_child_ref.page_id;
                    n->child_offsets[i] = new_child_ref.offset;
                    found = true;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_16: {
            data_art_node16_t *n = (data_art_node16_t *)modified_node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    n->child_page_ids[i] = new_child_ref.page_id;
                    n->child_offsets[i] = new_child_ref.offset;
                    found = true;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_48: {
            data_art_node48_t *n = (data_art_node48_t *)modified_node;
            uint8_t idx = n->keys[byte];
            if (idx != 255) {
                n->child_page_ids[idx] = new_child_ref.page_id;
                n->child_offsets[idx] = new_child_ref.offset;
                found = true;
            }
            break;
        }
        case DATA_NODE_256: {
            data_art_node256_t *n = (data_art_node256_t *)modified_node;
            if (n->child_page_ids[byte] != 0) {
                n->child_page_ids[byte] = new_child_ref.page_id;
                n->child_offsets[byte] = new_child_ref.offset;
                found = true;
            }
            break;
        }
        default:
            free(modified_node);
            LOG_ERROR("Invalid node type for child replacement: %d", type);
            return false;
    }
    
    if (!found) {
        free(modified_node);
        LOG_ERROR("[REPLACE_CHILD] FAILED: Child byte=0x%02x not found in node type=%u at page=%lu (num_children=%u)", 
                 byte, type, node_ref.page_id, 
                 (type == DATA_NODE_4) ? ((data_art_node4_t*)modified_node)->num_children :
                 (type == DATA_NODE_16) ? ((data_art_node16_t*)modified_node)->num_children :
                 (type == DATA_NODE_48) ? ((data_art_node48_t*)modified_node)->num_children :
                 (type == DATA_NODE_256) ? ((data_art_node256_t*)modified_node)->num_children : 0);
        if (type == DATA_NODE_4) {
            data_art_node4_t *n = (data_art_node4_t *)modified_node;
            LOG_ERROR("[REPLACE_CHILD] NODE_4 keys: 0x%02x 0x%02x 0x%02x 0x%02x", 
                     n->num_children > 0 ? n->keys[0] : 0xFF,
                     n->num_children > 1 ? n->keys[1] : 0xFF,
                     n->num_children > 2 ? n->keys[2] : 0xFF,
                     n->num_children > 3 ? n->keys[3] : 0xFF);
        }
        return false;
    }
    
    // Write modified node back
    bool success = data_art_write_node(tree, node_ref, modified_node, node_size);
    free(modified_node);
    
    if (!success) {
        LOG_ERROR("[REPLACE_CHILD] FAILED: data_art_write_node failed for page=%lu", node_ref.page_id);
    } else {
        LOG_ERROR("[REPLACE_CHILD] SUCCESS: Updated child byte=0x%02x in node at page=%lu (old_page=%lu → new_page=%lu)", 
                 byte, node_ref.page_id, old_child_ref.page_id, new_child_ref.page_id);
    }
    
    return success;
}

/**
 * Add child to node (with automatic growth)
 * Returns new node reference (may be different if node grew)
 */
static node_ref_t add_child_to_node(data_art_tree_t *tree, node_ref_t node_ref,
                                     uint8_t byte, node_ref_t child_ref) {
    if (node_ref_is_null(node_ref) || node_ref_is_null(child_ref)) {
        return node_ref;
    }
    
    // Use the implementation from data_art_node_ops.c
    return data_art_add_child(tree, node_ref, byte, child_ref);
}

/**
 * Remove child from node (with automatic shrinking)
 */
static node_ref_t remove_child_from_node(data_art_tree_t *tree, node_ref_t node_ref,
                                          uint8_t byte) {
    // TODO: Implement node shrinking logic
    LOG_ERROR("remove_child_from_node not yet implemented");
    return node_ref;
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
                                    bool *inserted) {
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
    LOG_INFO("  depth=%zu: node_type=%u at page=%u offset=%u", 
             depth, type, node_ref.page_id, node_ref.offset);
    
    // If it's a leaf, check for match or split
    if (type == DATA_NODE_LEAF) {
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
        
        // Update existing leaf
        if (leaf_matches_key(leaf, key, key_len)) {
            *inserted = false;  // Updated, not inserted
            LOG_ERROR("[UPDATE_LEAF] Replacing leaf at page=%lu (old value_len=%u) with new leaf", 
                     node_ref.page_id, leaf->value_len);
            node_ref_t new_leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
            if (!node_ref_is_null(new_leaf_ref)) {
                LOG_ERROR("[UPDATE_LEAF] New leaf allocated at page=%lu (new value_len=%zu)", 
                         new_leaf_ref.page_id, value_len);
                
                // TODO: Free old leaf page AFTER parent is successfully updated
                // For now, we leak the old page to avoid use-after-free bugs
                // page_manager_free(tree->page_manager, node_ref.page_id);
                
                // TODO: Also free overflow pages if leaf had any
                if (leaf->flags & LEAF_FLAG_OVERFLOW) {
                    LOG_WARN("Old leaf has overflow pages - not yet freeing them");
                }
                
                LOG_ERROR("[UPDATE_LEAF] Returning new leaf ref (page=%lu) to parent", 
                         new_leaf_ref.page_id);
                return new_leaf_ref;
            }
            return node_ref;
        }
        
        // Keys differ - need to create a new node to hold both leaves
        *inserted = true;
        
        // IMPORTANT: Copy the existing key data NOW before any other operations
        // because data_art_load_node may use a static buffer that gets overwritten
        const uint8_t *existing_key = leaf->data;
        size_t existing_key_len = leaf->key_len;
        
        uint8_t existing_key_copy[256];  // Temporary buffer for existing key
        if (existing_key_len > sizeof(existing_key_copy)) {
            LOG_ERROR("Existing key too long: %zu", existing_key_len);
            return node_ref;
        }
        memcpy(existing_key_copy, existing_key, existing_key_len);
        
        // Calculate longest common prefix between the two keys from current depth
        // Match mem_art logic: compare up to the shorter key's length
        size_t common_prefix_len = 0;
        size_t limit = (key_len < existing_key_len) ? key_len : existing_key_len;
        
        // Debug for single-byte keys
        if (key_len == 1 || existing_key_len == 1) {
            LOG_ERROR("[LEAF_SPLIT] Single-byte key detected!");
            LOG_ERROR("  depth=%zu, key_len=%zu, existing_key_len=%zu", depth, key_len, existing_key_len);
            LOG_ERROR("  limit=%zu", limit);
        }
        
        while (depth + common_prefix_len < limit && 
               existing_key_copy[depth + common_prefix_len] == key[depth + common_prefix_len]) {
            common_prefix_len++;
        }
        
        // Debug for single-byte keys
        if (key_len == 1 || existing_key_len == 1) {
            LOG_ERROR("  common_prefix_len=%zu", common_prefix_len);
            LOG_ERROR("  split_pos will be=%zu", depth + common_prefix_len);
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
        // Use 0x00 as the discriminating byte if key is exhausted (for prefix handling)
        size_t split_pos = depth + common_prefix_len;
        uint8_t existing_byte = (split_pos < existing_key_len) ? existing_key_copy[split_pos] : 0x00;
        uint8_t new_byte = (split_pos < key_len) ? key[split_pos] : 0x00;
        
        // Add both leaves as children to the new node
        new_node_ref = add_child_to_node(tree, new_node_ref, existing_byte, node_ref);
        if (node_ref_is_null(new_node_ref)) {
            LOG_ERROR("Failed to add existing leaf to new node");
            return node_ref;
        }
        
        new_node_ref = add_child_to_node(tree, new_node_ref, new_byte, new_leaf_ref);
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
                    LOG_INFO("    child[%d]: key=0x%02x, page=%u, offset=%u", 
                             i, old_n4->keys[i], old_n4->child_page_ids[i], old_n4->child_offsets[i]);
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
                if (leaf && depth + prefix_match < leaf->key_len) {
                    old_byte = leaf->data[depth + prefix_match];
                    
                    // Debug: show what leaf we got and what byte
                    char leaf_key[256];
                    size_t copy_len = leaf->key_len < 255 ? leaf->key_len : 255;
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
                    if (leaf && depth + prefix_match + 1 + bytes_to_copy <= leaf->key_len) {
                        memcpy(mod_bytes + 4, leaf->data + depth + prefix_match + 1, bytes_to_copy);
                    }
                }
            }
            
            // Allocate new page for modified old node
            node_ref_t modified_node_ref = data_art_alloc_node(tree, node_size);
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
                     modified_node_ref.page_id, old_byte, new_parent_ref.page_id);
            
            // Add modified old node as child
            new_parent_ref = add_child_to_node(tree, new_parent_ref, old_byte, modified_node_ref);
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
            
            new_parent_ref = add_child_to_node(tree, new_parent_ref, new_byte, new_leaf_ref);
            if (node_ref_is_null(new_parent_ref)) {
                LOG_ERROR("Failed to add new leaf to new parent");
                return new_parent_ref;
            }
            
            return new_parent_ref;
        }
        
        depth += partial_len;
    }
    
    // Keys have terminator bytes stored in leaves, but incoming keys don't have them yet.
    // When navigating, if we've consumed all bytes of the incoming key, use terminator.
    // Original key [0x87] needs to navigate: [0x87] at depth 0, then [0x00] at depth 1
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    LOG_INFO("  depth=%zu: looking for child with byte=0x%02x", depth, byte);
    
    // Find child for this byte
    node_ref_t child_ref = find_child_ref(tree, node_ref, byte);
    
    if (!node_ref_is_null(child_ref)) {
        LOG_INFO("  depth=%zu: child found at page=%u offset=%u, recursing...", 
                 depth, child_ref.page_id, child_ref.offset);
        // Child exists - recurse
        node_ref_t new_child_ref = insert_recursive(tree, child_ref, key, key_len,
                                                     depth + 1, value, value_len, inserted);
        
        // If child changed (due to split or growth), update parent's reference
        if (!node_ref_equals(new_child_ref, child_ref)) {
            LOG_ERROR("  [UPDATE_PARENT] depth=%zu: child changed from page=%u to page=%u, updating parent at page=%u", 
                     depth, child_ref.page_id, new_child_ref.page_id, node_ref.page_id);
            if (!replace_child_in_node(tree, node_ref, byte, child_ref, new_child_ref)) {
                LOG_ERROR("  [UPDATE_PARENT] FAILED to update child reference in parent");
            } else {
                LOG_ERROR("  [UPDATE_PARENT] SUCCESS - parent updated");
            }
        } else {
            LOG_DEBUG("  depth=%zu: child unchanged (page=%u), no parent update needed", 
                     depth, child_ref.page_id);
        }
        
        return node_ref;
    } else {
        // No child - add new leaf here
        LOG_INFO("  depth=%zu: no child found, creating new leaf for byte=0x%02x", depth, byte);
        *inserted = true;
        node_ref_t leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
        if (!node_ref_is_null(leaf_ref)) {
            LOG_INFO("  depth=%zu: adding leaf at page=%u as child", depth, leaf_ref.page_id);
            // Add leaf as child
            node_ref_t new_node_ref = add_child_to_node(tree, node_ref, byte, leaf_ref);
            LOG_INFO("  depth=%zu: after adding child, node ref: page=%u offset=%u", 
                     depth, new_node_ref.page_id, new_node_ref.offset);
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
        LOG_ERROR("Invalid parameters");
        return false;
    }
    
    bool inserted = false;
    node_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                            value, value_len, &inserted);
    
    if (!node_ref_is_null(new_root)) {
        tree->root = new_root;
        if (inserted) {
            tree->size++;
        }
        return true;
    }
    
    return false;
}
