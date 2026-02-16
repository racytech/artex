/*
 * Persistent ART - Core Operations
 * 
 * Implements the main tree operations for the disk-backed adaptive radix tree:
 * - Tree lifecycle (create, destroy)
 * - Basic operations (insert, get, delete, contains)
 * - Node management (alloc, load, write, CoW)
 * 
 * This file handles the core ART logic, while data_art_overflow.c handles
 * overflow pages, versioning, persistence, and statistics.
 */

#include "data_art.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

// ============================================================================
// Helper Functions - Node Size Calculation
// ============================================================================

/**
 * Get the fixed size of a node based on its type
 */
size_t get_node_size(data_art_node_type_t type) {
    switch (type) {
        case DATA_NODE_4:   return sizeof(data_art_node4_t);
        case DATA_NODE_16:  return sizeof(data_art_node16_t);
        case DATA_NODE_48:  return sizeof(data_art_node48_t);
        case DATA_NODE_256: return sizeof(data_art_node256_t);
        case DATA_NODE_OVERFLOW: return sizeof(data_art_overflow_t);
        case DATA_NODE_LEAF: 
            // Leaf size is variable, caller must specify
            LOG_ERROR("get_node_size called for LEAF without size");
            return 0;
        default:
            LOG_ERROR("Unknown node type: %d", type);
            return 0;
    }
}

// ============================================================================
// Tree Lifecycle
// ============================================================================

data_art_tree_t *data_art_create(page_manager_t *page_manager,
                                   buffer_pool_t *buffer_pool) {
    if (!page_manager) {
        LOG_ERROR("page_manager cannot be NULL");
        return NULL;
    }
    
    data_art_tree_t *tree = calloc(1, sizeof(data_art_tree_t));
    if (!tree) {
        LOG_ERROR("Failed to allocate tree structure");
        return NULL;
    }
    
    tree->page_manager = page_manager;
    tree->buffer_pool = buffer_pool;
    tree->root = NULL_NODE_REF;
    tree->version = 1;
    tree->size = 0;
    tree->cow_enabled = false;
    tree->active_versions = NULL;
    tree->num_active_versions = 0;
    
    // Initialize statistics
    tree->nodes_allocated = 0;
    tree->nodes_copied = 0;
    tree->cache_hits = 0;
    tree->cache_misses = 0;
    tree->overflow_pages_allocated = 0;
    tree->overflow_chain_reads = 0;
    
    LOG_INFO("Created persistent ART tree (buffer_pool=%s)", 
             buffer_pool ? "enabled" : "disabled");
    
    return tree;
}

void data_art_destroy(data_art_tree_t *tree) {
    if (!tree) {
        return;
    }
    
    // Free active versions array if allocated
    if (tree->active_versions) {
        free(tree->active_versions);
    }
    
    // Note: We do NOT delete pages from disk - they persist
    // Use data_art_drop() if you want to remove all data
    
    LOG_INFO("Destroyed ART tree (size=%zu, nodes=%lu)", 
             tree->size, tree->nodes_allocated);
    
    free(tree);
}

// ============================================================================
// Basic Tree Operations - Inline Helpers
// ============================================================================

size_t data_art_size(const data_art_tree_t *tree) {
    return tree ? tree->size : 0;
}

bool data_art_is_empty(const data_art_tree_t *tree) {
    return tree ? node_ref_is_null(tree->root) : true;
}

// ============================================================================
// Node Management - Allocation
// ============================================================================

/**
 * Simple allocation strategy: allocate one node per page
 * 
 * TODO: Implement page-level free space tracking for packing multiple
 * small nodes into a single page. For now, we use 1 node = 1 page for
 * simplicity and to avoid fragmentation.
 */
node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL_NODE_REF;
    }
    
    // Page data size is PAGE_SIZE - PAGE_HEADER_SIZE
    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    
    if (size == 0 || size > max_size) {
        LOG_ERROR("Invalid node size: %zu (max=%zu)", size, max_size);
        return NULL_NODE_REF;
    }
    
    // Allocate a new page for this node
    uint64_t page_id = page_manager_alloc(tree->page_manager, size);
    if (page_id == 0) {
        LOG_ERROR("Failed to allocate page for node");
        return NULL_NODE_REF;
    }
    
    tree->nodes_allocated++;
    
    // Node starts at offset 0 in the page
    node_ref_t ref = {.page_id = page_id, .offset = 0};
    
    return ref;
}

// ============================================================================
// Node Management - Load/Write
// ============================================================================

const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL;
    }
    
    if (node_ref_is_null(ref)) {
        return NULL;
    }
    
    // Use buffer pool if available for caching
    if (tree->buffer_pool) {
        page_t *page = buffer_pool_get(tree->buffer_pool, ref.page_id);
        if (!page) {
            LOG_ERROR("Failed to load page %lu from buffer pool", ref.page_id);
            tree->cache_misses++;
            return NULL;
        }
        tree->cache_hits++;
        
        // Return pointer to node at offset within page data
        return page->data + ref.offset;
    }
    
    // Fallback: direct page manager access
    // Note: We need to allocate a temporary page buffer since page_manager_read
    // writes into the provided buffer, not returns a pointer
    static __thread page_t temp_page;  // Thread-local to avoid races
    page_result_t result = page_manager_read(tree->page_manager, ref.page_id, &temp_page);
    if (result != PAGE_SUCCESS) {
        LOG_ERROR("Failed to read page %lu", ref.page_id);
        tree->cache_misses++;
        return NULL;
    }
    
    tree->cache_misses++;
    
    // Return pointer to node at offset
    // WARNING: This pointer is only valid until next data_art_load_node call in this thread
    return temp_page.data + ref.offset;
}

bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                         const void *node, size_t size) {
    if (!tree || !node) {
        LOG_ERROR("Invalid parameters: tree=%p, node=%p", (void*)tree, node);
        return false;
    }
    
    if (node_ref_is_null(ref)) {
        LOG_ERROR("Cannot write to NULL node reference");
        return false;
    }
    
    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    if (size > max_size) {
        LOG_ERROR("Node size %zu exceeds page capacity %zu", size, max_size);
        return false;
    }
    
    // Get or create page
    page_t *page = NULL;
    bool page_is_temp = false;
    
    if (tree->buffer_pool) {
        page = buffer_pool_get(tree->buffer_pool, ref.page_id);
        if (!page) {
            LOG_ERROR("Failed to get page %lu from buffer pool", ref.page_id);
            return false;
        }
    } else {
        // For newly allocated pages, we need to initialize them first
        static __thread page_t temp_page;
        page_result_t result = page_manager_read(tree->page_manager, ref.page_id, &temp_page);
        if (result != PAGE_SUCCESS) {
            // Page doesn't exist yet - initialize it
            memset(&temp_page, 0, sizeof(temp_page));
            temp_page.header.page_id = ref.page_id;
        }
        page = &temp_page;
        page_is_temp = true;
    }
    
    // Copy node data to page at offset
    memcpy(page->data + ref.offset, node, size);
    
    // Compute checksum before writing
    page_compute_checksum(page);
    
    // Mark page as dirty or write directly
    if (tree->buffer_pool) {
        buffer_pool_mark_dirty(tree->buffer_pool, ref.page_id);
    } else {
        // Direct write through page manager
        page_result_t result = page_manager_write(tree->page_manager, page);
        if (result != PAGE_SUCCESS) {
            LOG_ERROR("Failed to write page %lu", ref.page_id);
            return false;
        }
        // Sync to ensure data is persisted
        page_manager_sync(tree->page_manager);
    }
    
    return true;
}

// ============================================================================
// Copy-on-Write (CoW) Support
// ============================================================================

node_ref_t data_art_cow_node(data_art_tree_t *tree, node_ref_t ref) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL_NODE_REF;
    }
    
    if (node_ref_is_null(ref)) {
        return NULL_NODE_REF;
    }
    
    // Load the original node
    const void *original = data_art_load_node(tree, ref);
    if (!original) {
        LOG_ERROR("Failed to load node for CoW");
        return NULL_NODE_REF;
    }
    
    // Determine node type and size
    uint8_t type = *(const uint8_t *)original;
    size_t size = 0;
    
    if (type == DATA_NODE_LEAF) {
        // Leaf nodes have variable size
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)original;
        size = sizeof(data_art_leaf_t) + leaf->inline_data_len;
    } else {
        size = get_node_size(type);
        if (size == 0) {
            LOG_ERROR("Failed to determine size for node type %d", type);
            return NULL_NODE_REF;
        }
    }
    
    // Allocate new node
    node_ref_t new_ref = data_art_alloc_node(tree, size);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate node for CoW copy");
        return NULL_NODE_REF;
    }
    
    // Copy data to new node
    if (!data_art_write_node(tree, new_ref, original, size)) {
        LOG_ERROR("Failed to write CoW copy");
        // TODO: Free allocated node
        return NULL_NODE_REF;
    }
    
    tree->nodes_copied++;
    
    return new_ref;
}

// ============================================================================
// Helper Functions - Node Navigation
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
 * Find any leaf descendant of a node (for lazy expansion prefix verification)
 */
static const data_art_leaf_t* find_any_leaf_for_prefix(data_art_tree_t *tree, node_ref_t node_ref) {
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
                if (n->keys[i] != 255) {  // NODE48_EMPTY
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
    
    return find_any_leaf_for_prefix(tree, child_ref);
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

const void *data_art_get(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                         size_t *value_len) {
    if (!tree || !key) {
        LOG_ERROR("Invalid parameters");
        return NULL;
    }
    
    // Log search start
    char key_str[64];
    snprintf(key_str, sizeof(key_str), "%.*s", (int)(key_len < 50 ? key_len : 50), key);
    LOG_INFO(">>> SEARCHING for key: '%s' (len=%zu)", key_str, key_len);
    
    if (node_ref_is_null(tree->root)) {
        LOG_INFO(">>> Tree is empty, returning NULL");
        return NULL;  // Empty tree
    }
    
    node_ref_t current = tree->root;
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
            if (leaf_matches(leaf, key, key_len)) {
                if (value_len) {
                    *value_len = leaf->value_len;
                }
                
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
        if (depth >= key_len) {
            // Key exhausted, look for NULL byte child
            current = find_child(tree, current, 0x00);
        } else {
            uint8_t byte = key[depth];
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
            depth++;  // Move past the byte we just used for lookup
        }
    }
    
    LOG_INFO("Exited search loop, current is null, returning NULL");
    return NULL;  // Not found
}

bool data_art_contains(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    size_t len;
    return data_art_get(tree, key, key_len, &len) != NULL;
}

// ============================================================================
// NOTE: Insert and Delete operations are implemented in separate files:
// - data_art_insert.c (recursive insert with node growth)
// - data_art_delete.c (recursive delete with node shrinking - TODO)
// 
// Node growth/shrinking operations are in:
// - data_art_node_ops.c (add_child, remove_child, node transitions)
// ============================================================================
