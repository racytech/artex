/**
 * Persistent ART - Delete Operations
 * 
 * Handles recursive deletion with node shrinking:
 * - NODE_256 -> NODE_48 (at 48 children)
 * - NODE_48 -> NODE_16 (at 16 children)
 * - NODE_16 -> NODE_4 (at 4 children)
 * - NODE_4 -> collapse to single child or leaf
 */

#include "data_art.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Forward declarations - functions we need from data_art_core.c and other files
// These need to be made non-static in their respective files
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern bool leaf_matches(const data_art_leaf_t *leaf, const uint8_t *key, size_t key_len);
extern int check_prefix(data_art_tree_t *tree, node_ref_t node_ref, const void *node,
                       const uint8_t *key, size_t key_len, size_t depth);
extern node_ref_t find_child(data_art_tree_t *tree, node_ref_t node_ref, uint8_t byte);
extern size_t get_node_size(data_art_node_type_t node_type);
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref, const void *data, size_t size);

// Forward declarations
static node_ref_t delete_recursive(data_art_tree_t *tree, node_ref_t node_ref,
                                   const uint8_t *key, size_t key_len, size_t depth,
                                   bool *deleted);
static node_ref_t remove_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, bool *did_remove);
static node_ref_t update_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, node_ref_t new_child_ref);
static node_ref_t try_shrink_node(data_art_tree_t *tree, node_ref_t node_ref);// ============================================================================
// Public API
// ============================================================================

bool data_art_delete(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key) {
        LOG_ERROR("Invalid parameters");
        return false;
    }
    
    if (node_ref_is_null(tree->root)) {
        return false;  // Empty tree, nothing to delete
    }
    
    bool deleted = false;
    node_ref_t new_root = delete_recursive(tree, tree->root, key, key_len, 0, &deleted);
    
    if (deleted) {
        tree->root = new_root;
        tree->size--;
        LOG_DEBUG("Deleted key, new size: %zu", tree->size);
    }
    
    return deleted;
}

// ============================================================================
// Recursive Delete
// ============================================================================

static node_ref_t delete_recursive(data_art_tree_t *tree, node_ref_t node_ref,
                                   const uint8_t *key, size_t key_len, size_t depth,
                                   bool *deleted) {
    *deleted = false;
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node at page=%lu, offset=%u", 
                 node_ref.page_id, node_ref.offset);
        return node_ref;
    }
    
    uint8_t type = *(const uint8_t *)node;
    
    // If we've reached a leaf, check if it matches
    if (type == DATA_NODE_LEAF) {
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
        
        if (leaf_matches(leaf, key, key_len)) {
            *deleted = true;
            // Return NULL to indicate this node should be removed
            return NULL_NODE_REF;
        }
        
        // Not a match, nothing to delete
        return node_ref;
    }
    
    // It's an inner node, check prefix
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t partial_len = node_bytes[2];
    
    if (partial_len > 0) {
        int prefix_match = check_prefix(tree, node_ref, node, key, key_len, depth);
        
        // Check inline portion (up to 10 bytes)
        int expected_match = (partial_len < 10) ? partial_len : 10;
        if (prefix_match < expected_match) {
            // Prefix mismatch, key doesn't exist
            return node_ref;
        }
        
        // For lazy expansion, we'll verify at leaf level
        depth += partial_len;
    }
    
    // Get next byte to search
    if (depth >= key_len) {
        // Key exhausted, look for NULL byte child
        node_ref_t child_ref = find_child(tree, node_ref, 0x00);
        if (node_ref_is_null(child_ref)) {
            return node_ref;  // No NULL child, key doesn't exist
        }
        
        // Recursively delete in child
        node_ref_t new_child = delete_recursive(tree, child_ref, key, key_len, depth, deleted);
        
        if (*deleted) {
            if (node_ref_is_null(new_child)) {
                // Child was removed, remove it from this node
                bool did_remove;
                node_ref_t updated = remove_child(tree, node_ref, 0x00, &did_remove);
                
                // Try to shrink the node if needed
                return try_shrink_node(tree, updated);
            } else {
                // Child was updated, update the pointer
                return update_child(tree, node_ref, 0x00, new_child);
            }
        }
        
        return node_ref;
    }
    
    uint8_t byte = key[depth];
    node_ref_t child_ref = find_child(tree, node_ref, byte);
    
    if (node_ref_is_null(child_ref)) {
        // Child doesn't exist, key not in tree
        return node_ref;
    }
    
    // Recursively delete in child
    node_ref_t new_child = delete_recursive(tree, child_ref, key, key_len, depth + 1, deleted);
    
    if (*deleted) {
        if (node_ref_is_null(new_child)) {
            // Child was removed, remove it from this node
            bool did_remove;
            node_ref_t updated = remove_child(tree, node_ref, byte, &did_remove);
            
            // Try to shrink the node if needed
            return try_shrink_node(tree, updated);
        } else {
            // Child was updated but not removed
            return update_child(tree, node_ref, byte, new_child);
        }
    }
    
    return node_ref;
}

// ============================================================================
// Update Child Pointer in Node
// ============================================================================

static node_ref_t update_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, node_ref_t new_child_ref) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node for update_child");
        return node_ref;
    }
    
    uint8_t type = *(const uint8_t *)node;
    size_t node_size = get_node_size(type);
    
    // Create a copy of the node
    void *new_node = malloc(node_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for updated node");
        return node_ref;
    }
    
    memcpy(new_node, node, node_size);
    
    // Update the child pointer based on node type
    switch (type) {
        case DATA_NODE_4: {
            data_art_node4_t *n4 = (data_art_node4_t *)new_node;
            for (int i = 0; i < n4->num_children; i++) {
                if (n4->keys[i] == byte) {
                    n4->child_page_ids[i] = new_child_ref.page_id;
                    n4->child_offsets[i] = new_child_ref.offset;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_16: {
            data_art_node16_t *n16 = (data_art_node16_t *)new_node;
            for (int i = 0; i < n16->num_children; i++) {
                if (n16->keys[i] == byte) {
                    n16->child_page_ids[i] = new_child_ref.page_id;
                    n16->child_offsets[i] = new_child_ref.offset;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_48: {
            data_art_node48_t *n48 = (data_art_node48_t *)new_node;
            int index = n48->keys[byte];
            if (index != 255) {  // 255 means empty slot
                n48->child_page_ids[index] = new_child_ref.page_id;
                n48->child_offsets[index] = new_child_ref.offset;
            }
            break;
        }
        case DATA_NODE_256: {
            data_art_node256_t *n256 = (data_art_node256_t *)new_node;
            n256->child_page_ids[byte] = new_child_ref.page_id;
            n256->child_offsets[byte] = new_child_ref.offset;
            break;
        }
        default:
            LOG_ERROR("Cannot update child on node type %d", type);
            free(new_node);
            return node_ref;
    }
    
    // Allocate new page and write the node
    node_ref_t new_ref = data_art_alloc_node(tree, node_size);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate page for updated node");
        free(new_node);
        return node_ref;
    }
    
    if (!data_art_write_node(tree, new_ref, new_node, node_size)) {
        LOG_ERROR("Failed to write updated node");
        free(new_node);
        return node_ref;
    }
    
    free(new_node);
    return new_ref;
}

// ============================================================================
// Remove Child from Node
// ============================================================================

static node_ref_t remove_child(data_art_tree_t *tree, node_ref_t node_ref, 
                               uint8_t byte, bool *did_remove) {
    *did_remove = false;
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node");
        return node_ref;
    }
    
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t type = node_bytes[0];
    uint8_t num_children = node_bytes[1];
    
    // Find the child to remove
    int child_pos = -1;
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n4 = (const data_art_node4_t *)node;
            for (int i = 0; i < num_children; i++) {
                if (n4->keys[i] == byte) {
                    child_pos = i;
                    break;
                }
            }
            break;
        }
        
        case DATA_NODE_16: {
            const data_art_node16_t *n16 = (const data_art_node16_t *)node;
            for (int i = 0; i < num_children; i++) {
                if (n16->keys[i] == byte) {
                    child_pos = i;
                    break;
                }
            }
            break;
        }
        
        case DATA_NODE_48: {
            const data_art_node48_t *n48 = (const data_art_node48_t *)node;
            uint8_t pos = n48->keys[byte];
            if (pos != 0xFF) {
                child_pos = pos;
            }
            break;
        }
        
        case DATA_NODE_256: {
            const data_art_node256_t *n256 = (const data_art_node256_t *)node;
            
            if (n256->child_page_ids[byte] == 0 && n256->child_offsets[byte] == 0) {
                // Child doesn't exist
                return node_ref;
            }
            
            child_pos = byte;
            break;
        }
        
        default:
            LOG_ERROR("Invalid node type: %d", type);
            return node_ref;
    }
    
    if (child_pos == -1) {
        // Child not found
        return node_ref;
    }
    
    // Create a new node without this child
    size_t node_size = get_node_size(type);
    void *new_node = malloc(node_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for new node");
        return node_ref;
    }
    
    memcpy(new_node, node, node_size);
    uint8_t *new_bytes = (uint8_t *)new_node;
    
    // Decrease child count
    new_bytes[1] = num_children - 1;
    
    // Remove the child based on node type
    switch (type) {
        case DATA_NODE_4: {
            data_art_node4_t *n4 = (data_art_node4_t *)new_node;
            
            // Shift keys and children to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n4->keys[i] = n4->keys[i + 1];
                n4->child_page_ids[i] = n4->child_page_ids[i + 1];
                n4->child_offsets[i] = n4->child_offsets[i + 1];
            }
            
            // Clear the last slot
            n4->keys[num_children - 1] = 0;
            n4->child_page_ids[num_children - 1] = 0;
            n4->child_offsets[num_children - 1] = 0;
            break;
        }
        
        case DATA_NODE_16: {
            data_art_node16_t *n16 = (data_art_node16_t *)new_node;
            
            // Shift keys and children to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n16->keys[i] = n16->keys[i + 1];
                n16->child_page_ids[i] = n16->child_page_ids[i + 1];
                n16->child_offsets[i] = n16->child_offsets[i + 1];
            }
            
            // Clear the last slot
            n16->keys[num_children - 1] = 0;
            n16->child_page_ids[num_children - 1] = 0;
            n16->child_offsets[num_children - 1] = 0;
            break;
        }
        
        case DATA_NODE_48: {
            data_art_node48_t *n48 = (data_art_node48_t *)new_node;
            
            // Mark index slot as empty
            n48->keys[byte] = 0xFF;
            
            // Shift children down to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n48->child_page_ids[i] = n48->child_page_ids[i + 1];
                n48->child_offsets[i] = n48->child_offsets[i + 1];
            }
            
            // Clear the last slot
            n48->child_page_ids[num_children - 1] = 0;
            n48->child_offsets[num_children - 1] = 0;
            
            // Update all index entries that pointed after the removed position
            for (int i = 0; i < 256; i++) {
                if (n48->keys[i] != 0xFF && n48->keys[i] > child_pos) {
                    n48->keys[i]--;
                }
            }
            break;
        }
        
        case DATA_NODE_256: {
            data_art_node256_t *n256 = (data_art_node256_t *)new_node;
            
            // Simply zero out the child pointer
            n256->child_page_ids[byte] = 0;
            n256->child_offsets[byte] = 0;
            break;
        }
    }
    
    // Allocate new page for modified node
    node_ref_t new_ref = data_art_alloc_node(tree, node_size);
    if (node_ref_is_null(new_ref)) {
        free(new_node);
        LOG_ERROR("Failed to allocate page for modified node");
        return node_ref;
    }
    
    if (!data_art_write_node(tree, new_ref, new_node, node_size)) {
        free(new_node);
        LOG_ERROR("Failed to write modified node");
        return node_ref;
    }
    
    free(new_node);
    *did_remove = true;
    
    return new_ref;
}

// ============================================================================
// Node Shrinking
// ============================================================================

static node_ref_t try_shrink_node(data_art_tree_t *tree, node_ref_t node_ref) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return node_ref;
    }
    
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t type = node_bytes[0];
    uint8_t num_children = node_bytes[1];
    
    // Determine if we should shrink
    bool should_shrink = false;
    uint8_t new_type = type;
    
    switch (type) {
        case DATA_NODE_256:
            if (num_children <= 48) {
                should_shrink = true;
                new_type = DATA_NODE_48;
            }
            break;
            
        case DATA_NODE_48:
            if (num_children <= 16) {
                should_shrink = true;
                new_type = DATA_NODE_16;
            }
            break;
            
        case DATA_NODE_16:
            if (num_children <= 4) {
                should_shrink = true;
                new_type = DATA_NODE_4;
            }
            break;
            
        case DATA_NODE_4:
            if (num_children == 1) {
                // Special case: collapse to single child
                // If the child is a leaf, return it
                // If the child is a node, we might be able to merge prefixes
                uint8_t *children = (uint8_t *)node_bytes + 16 + 4;
                
                node_ref_t child_ref;
                memcpy(&child_ref.page_id, children, 8);
                memcpy(&child_ref.offset, children + 8, 4);
                
                const void *child_node = data_art_load_node(tree, child_ref);
                if (child_node) {
                    uint8_t child_type = *(const uint8_t *)child_node;
                    
                    if (child_type == DATA_NODE_LEAF) {
                        // Return the leaf directly
                        return child_ref;
                    } else {
                        // Try to merge prefixes (path compression)
                        // TODO: Implement prefix merging
                        // For now, keep the NODE_4
                        return node_ref;
                    }
                }
            }
            break;
    }
    
    if (!should_shrink) {
        return node_ref;
    }
    
    // Shrink the node
    LOG_DEBUG("Shrinking node from type %d to type %d", type, new_type);
    
    // Allocate new smaller node
    size_t new_size = get_node_size(new_type);
    void *new_node = calloc(1, new_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for shrunk node");
        return node_ref;
    }
    
    uint8_t *new_bytes = (uint8_t *)new_node;
    
    // Copy header
    new_bytes[0] = new_type;
    new_bytes[1] = num_children;
    new_bytes[2] = node_bytes[2];  // partial_len
    new_bytes[3] = node_bytes[3];  // flags
    
    // Copy prefix
    memcpy(new_bytes + 4, node_bytes + 4, 10);
    
    // Copy children based on conversion
    if (type == DATA_NODE_256 && new_type == DATA_NODE_48) {
        // NODE_256 -> NODE_48
        uint8_t *new_index = new_bytes + 16;
        uint8_t *new_children = new_bytes + 16 + 256;
        
        memset(new_index, 0xFF, 256);
        
        int pos = 0;
        for (int i = 0; i < 256; i++) {
            uint64_t page_id;
            memcpy(&page_id, node_bytes + 16 + i * 12, 8);
            
            if (page_id != 0) {
                new_index[i] = pos;
                memcpy(new_children + pos * 12, node_bytes + 16 + i * 12, 12);
                pos++;
            }
        }
    } else if (type == DATA_NODE_48 && new_type == DATA_NODE_16) {
        // NODE_48 -> NODE_16
        const uint8_t *old_index = node_bytes + 16;
        const uint8_t *old_children = node_bytes + 16 + 256;
        
        uint8_t *new_keys = new_bytes + 16;
        uint8_t *new_children = new_bytes + 16 + 16;
        
        int pos = 0;
        for (int i = 0; i < 256; i++) {
            if (old_index[i] != 0xFF) {
                new_keys[pos] = i;
                memcpy(new_children + pos * 12, old_children + old_index[i] * 12, 12);
                pos++;
            }
        }
    } else if (type == DATA_NODE_16 && new_type == DATA_NODE_4) {
        // NODE_16 -> NODE_4
        const uint8_t *old_keys = node_bytes + 16;
        const uint8_t *old_children = node_bytes + 16 + 16;
        
        uint8_t *new_keys = new_bytes + 16;
        uint8_t *new_children = new_bytes + 16 + 4;
        
        for (int i = 0; i < num_children; i++) {
            new_keys[i] = old_keys[i];
            memcpy(new_children + i * 12, old_children + i * 12, 12);
        }
    }
    
    // Allocate page for new node
    node_ref_t new_ref = data_art_alloc_node(tree, new_size);
    if (node_ref_is_null(new_ref)) {
        free(new_node);
        LOG_ERROR("Failed to allocate page for shrunk node");
        return node_ref;
    }
    
    if (!data_art_write_node(tree, new_ref, new_node, new_size)) {
        free(new_node);
        LOG_ERROR("Failed to write shrunk node");
        return node_ref;
    }
    
    free(new_node);
    
    LOG_DEBUG("Successfully shrunk node from type %d to type %d", type, new_type);
    
    return new_ref;
}
