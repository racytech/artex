#include "art.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/**
 * ART Implementation
 * 
 * Node structure details:
 * - Node4: 4 keys + 4 child pointers (sorted by key for binary search)
 * - Node16: 16 keys + 16 child pointers (sorted by key for SIMD-friendly search)
 * - Node48: 256-byte index + 48 child pointers (index maps byte to child slot)
 * - Node256: 256 child pointers (direct indexing, no key array needed)
 * - Leaf: stores full key + value
 */

// Maximum children per node type
#define NODE4_MAX 4
#define NODE16_MAX 16
#define NODE48_MAX 48
#define NODE256_MAX 256

// Special value for Node48 index indicating no child
#define NODE48_EMPTY 255

/**
 * Node4: Up to 4 children
 * Memory: ~48 bytes (4 keys + 4 pointers)
 */
typedef struct {
    uint8_t keys[NODE4_MAX];           // Sorted array of key bytes
    art_node_t *children[NODE4_MAX];   // Corresponding child pointers
} art_node4_t;

/**
 * Node16: Up to 16 children
 * Memory: ~144 bytes (16 keys + 16 pointers)
 */
typedef struct {
    uint8_t keys[NODE16_MAX];          // Sorted array of key bytes
    art_node_t *children[NODE16_MAX];  // Corresponding child pointers
} art_node16_t;

/**
 * Node48: Up to 48 children with indexed lookup
 * Memory: ~640 bytes (256-byte index + 48 pointers)
 */
typedef struct {
    uint8_t index[256];                // Maps key byte to child index (255 = empty)
    art_node_t *children[NODE48_MAX];  // Child pointers
} art_node48_t;

/**
 * Node256: Up to 256 children with direct indexing
 * Memory: ~2KB (256 pointers)
 */
typedef struct {
    art_node_t *children[NODE256_MAX]; // Direct array of child pointers
} art_node256_t;

/**
 * Leaf node structure - allocated separately with variable size
 */
typedef struct {
    art_node_type_t type;  // Always NODE_LEAF
    uint32_t key_len;
    uint32_t value_len;
    uint8_t data[];  // Flexible array: key bytes followed by value bytes
} art_leaf_node_t;

/**
 * Inner node structure - all inner nodes use this structure
 */
struct art_node {
    art_node_type_t type;  // Type discriminator
    uint32_t num_children;
    uint32_t partial_len;
    uint8_t partial[10];
    union {
        art_node4_t node4;
        art_node16_t node16;
        art_node48_t node48;
        art_node256_t node256;
    };
};

/**
 * Iterator internal state
 */
typedef struct {
    const uint8_t *key;
    size_t key_len;
    const void *value;
    size_t value_len;
    bool done;
    bool started;  // Track if iteration has begun
    // Stack for depth-first traversal
    struct {
        art_node_t *node;
        int child_idx;
    } stack[64];  // Max depth
    int depth;
} art_iterator_state_t;

// Forward declarations of internal functions
static art_node_t *alloc_node(art_node_type_t type);
static void free_node(art_node_t *node);
static art_node_t *alloc_leaf(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len);
static bool is_leaf(const art_node_t *node);
static art_node_t *insert_recursive(art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth,
                                     const void *value, size_t value_len,
                                     bool *inserted);
static art_node_t *delete_recursive(art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth, bool *deleted);
static const void *search(const art_node_t *node, const uint8_t *key,
                          size_t key_len, size_t depth, size_t *value_len);
static int check_prefix(const art_node_t *node, const uint8_t *key,
                        size_t key_len, size_t depth);
static art_node_t *add_child(art_node_t *node, uint8_t byte, art_node_t *child);
static void remove_child(art_node_t *node, uint8_t byte, art_node_t **child_out);
static art_node_t *find_child(const art_node_t *node, uint8_t byte);

//==============================================================================
// Public API Implementation
//==============================================================================

bool art_tree_init(art_tree_t *tree) {
    if (!tree) return false;
    tree->root = NULL;
    tree->size = 0;
    return true;
}

void art_tree_destroy(art_tree_t *tree) {
    if (!tree) return;
    if (tree->root) {
        free_node(tree->root);
        tree->root = NULL;
    }
    tree->size = 0;
}

bool art_insert(art_tree_t *tree, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len) {
    if (!tree || !key || key_len == 0) return false;
    
    LOG_DB_DEBUG("art_insert: key_len=%zu", key_len);
    
    bool inserted = false;
    tree->root = insert_recursive(tree->root, key, key_len, 0, value, value_len, &inserted);
    
    if (inserted) {
        tree->size++;
        LOG_DB_DEBUG("art_insert: new entry inserted, size now %zu", tree->size);
    } else {
        LOG_DB_DEBUG("art_insert: existing entry updated");
    }
    
    return tree->root != NULL;
}

const void *art_get(const art_tree_t *tree, const uint8_t *key, size_t key_len,
                    size_t *value_len) {
    if (!tree || !key || key_len == 0) return NULL;
    return search(tree->root, key, key_len, 0, value_len);
}

bool art_delete(art_tree_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key || key_len == 0) return false;
    
    bool deleted = false;
    tree->root = delete_recursive(tree->root, key, key_len, 0, &deleted);
    
    if (deleted) {
        tree->size--;
    }
    
    return deleted;
}

bool art_contains(const art_tree_t *tree, const uint8_t *key, size_t key_len) {
    return art_get(tree, key, key_len, NULL) != NULL;
}

size_t art_size(const art_tree_t *tree) {
    return tree ? tree->size : 0;
}

bool art_is_empty(const art_tree_t *tree) {
    return tree ? (tree->size == 0) : true;
}

//==============================================================================
// Iterator Implementation
//==============================================================================

art_iterator_t *art_iterator_create(const art_tree_t *tree) {
    if (!tree) return NULL;
    
    art_iterator_t *iter = malloc(sizeof(art_iterator_t));
    if (!iter) return NULL;
    
    art_iterator_state_t *state = malloc(sizeof(art_iterator_state_t));
    if (!state) {
        free(iter);
        return NULL;
    }
    
    iter->tree = (art_tree_t *)tree;
    iter->internal = state;
    
    memset(state, 0, sizeof(art_iterator_state_t));
    state->done = (tree->root == NULL);
    state->depth = -1;
    
    return iter;
}

bool art_iterator_next(art_iterator_t *iter) {
    if (!iter || !iter->internal) return false;
    
    art_iterator_state_t *state = (art_iterator_state_t *)iter->internal;
    if (state->done) return false;
    
    art_node_t *node;
    
    // First call - initialize stack with root
    if (!state->started) {
        state->started = true;
        if (!iter->tree->root) {
            state->done = true;
            return false;
        }
        state->depth = 0;
        state->stack[0].node = iter->tree->root;
        state->stack[0].child_idx = -1;
    }
    
    // Depth-first traversal to find next leaf
    while (state->depth >= 0) {
        node = state->stack[state->depth].node;
        
        // If this is a leaf, return it
        if (is_leaf(node)) {
            art_leaf_node_t *leaf = (art_leaf_node_t *)node;
            state->key = leaf->data;
            state->key_len = leaf->key_len;
            state->value = leaf->data + leaf->key_len;
            state->value_len = leaf->value_len;
            
            // Pop this leaf from stack for next iteration
            state->depth--;
            return true;
        }
        
        // Inner node - find next child to visit
        int *child_idx = &state->stack[state->depth].child_idx;
        (*child_idx)++;
        
        art_node_t *next_child = NULL;
        
        // Get next child based on node type
        switch (node->type) {
            case NODE_4:
                if (*child_idx < (int)node->num_children) {
                    next_child = node->node4.children[*child_idx];
                }
                break;
            case NODE_16:
                if (*child_idx < (int)node->num_children) {
                    next_child = node->node16.children[*child_idx];
                }
                break;
            case NODE_48:
                // Find next non-empty child
                while (*child_idx < 256) {
                    uint8_t idx = node->node48.index[*child_idx];
                    if (idx != NODE48_EMPTY) {
                        next_child = node->node48.children[idx];
                        break;
                    }
                    (*child_idx)++;
                }
                break;
            case NODE_256:
                // Find next non-NULL child
                while (*child_idx < 256) {
                    next_child = node->node256.children[*child_idx];
                    if (next_child) break;
                    (*child_idx)++;
                }
                break;
            default:
                break;
        }
        
        if (next_child) {
            // Push child onto stack and descend
            state->depth++;
            if (state->depth >= 64) {
                // Stack overflow - tree too deep
                state->done = true;
                return false;
            }
            state->stack[state->depth].node = next_child;
            state->stack[state->depth].child_idx = -1;
        } else {
            // No more children at this level, backtrack
            state->depth--;
        }
    }
    
    // Traversal complete
    state->done = true;
    return false;
}

const uint8_t *art_iterator_key(const art_iterator_t *iter, size_t *key_len) {
    if (!iter || !iter->internal) return NULL;
    
    art_iterator_state_t *state = (art_iterator_state_t *)iter->internal;
    if (state->done) return NULL;
    
    if (key_len) *key_len = state->key_len;
    return state->key;
}

const void *art_iterator_value(const art_iterator_t *iter, size_t *value_len) {
    if (!iter || !iter->internal) return NULL;
    
    art_iterator_state_t *state = (art_iterator_state_t *)iter->internal;
    if (state->done) return NULL;
    
    if (value_len) *value_len = state->value_len;
    return state->value;
}

bool art_iterator_done(const art_iterator_t *iter) {
    if (!iter || !iter->internal) return true;
    
    art_iterator_state_t *state = (art_iterator_state_t *)iter->internal;
    return state->done;
}

void art_iterator_destroy(art_iterator_t *iter) {
    if (!iter) return;
    if (iter->internal) {
        free(iter->internal);
    }
    free(iter);
}

void art_foreach(const art_tree_t *tree, art_callback_t callback, void *user_data) {
    if (!tree || !callback) return;
    
    art_iterator_t *iter = art_iterator_create(tree);
    if (!iter) return;
    
    while (art_iterator_next(iter)) {
        size_t key_len, value_len;
        const uint8_t *key = art_iterator_key(iter, &key_len);
        const void *value = art_iterator_value(iter, &value_len);
        
        if (key && value) {
            if (!callback(key, key_len, value, value_len, user_data)) {
                break;  // Callback returned false, stop iteration
            }
        }
    }
    
    art_iterator_destroy(iter);
}

//==============================================================================
// Internal Helper Functions
//==============================================================================

static art_node_t *alloc_node(art_node_type_t type) {
    // Always allocate enough for the full node structure
    // This includes the type field + union with header + specific node type
    art_node_t *node = calloc(1, sizeof(art_node_t));
    if (!node) return NULL;
    
    node->type = type;
    node->num_children = 0;
    node->partial_len = 0;
    
    // Initialize Node48 index array
    if (type == NODE_48) {
        memset(node->node48.index, NODE48_EMPTY, 256);
    }
    
    return node;
}

static void free_node(art_node_t *node) {
    if (!node) return;
    
    if (is_leaf(node)) {
        free(node);
        return;
    }
    
    // Recursively free children
    switch (node->type) {
        case NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                free_node(node->node4.children[i]);
            }
            break;
        }
        case NODE_16: {
            for (int i = 0; i < node->num_children; i++) {
                free_node(node->node16.children[i]);
            }
            break;
        }
        case NODE_48: {
            for (int i = 0; i < NODE48_MAX; i++) {
                if (node->node48.children[i]) {
                    free_node(node->node48.children[i]);
                }
            }
            break;
        }
        case NODE_256: {
            for (int i = 0; i < NODE256_MAX; i++) {
                if (node->node256.children[i]) {
                    free_node(node->node256.children[i]);
                }
            }
            break;
        }
        default:
            break;
    }
    
    free(node);
}

static art_node_t *alloc_leaf(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len) {
    size_t total_size = sizeof(art_leaf_node_t) + key_len + value_len;
    art_leaf_node_t *leaf = malloc(total_size);
    if (!leaf) return NULL;
    
    leaf->type = NODE_LEAF;
    leaf->key_len = key_len;
    leaf->value_len = value_len;
    
    // Copy key and value into data array
    memcpy(leaf->data, key, key_len);
    memcpy(leaf->data + key_len, value, value_len);
    
    return (art_node_t *)leaf;
}

static bool is_leaf(const art_node_t *node) {
    return node && node->type == NODE_LEAF;
}

static const uint8_t *leaf_key(const art_node_t *node) {
    const art_leaf_node_t *leaf = (const art_leaf_node_t *)node;
    return leaf->data;
}

static const void *leaf_value(const art_node_t *node, size_t *value_len) {
    const art_leaf_node_t *leaf = (const art_leaf_node_t *)node;
    if (value_len) *value_len = leaf->value_len;
    return leaf->data + leaf->key_len;
}

static bool leaf_matches(const art_node_t *node, const uint8_t *key, size_t key_len) {
    const art_leaf_node_t *leaf = (const art_leaf_node_t *)node;
    if (leaf->key_len != key_len) return false;
    return memcmp(leaf_key(node), key, key_len) == 0;
}

/**
 * Check how many bytes of the compressed path match the key
 * Returns the number of matching bytes, or prefix length if all match
 * For lazy expansion (partial_len > 10), may need to check beyond stored bytes
 */
static int check_prefix(const art_node_t *node, const uint8_t *key,
                        size_t key_len, size_t depth) {
    int max_cmp = node->partial_len < 10 ? node->partial_len : 10;
    int idx;
    
    for (idx = 0; idx < max_cmp; idx++) {
        if (depth + idx >= key_len) return idx;
        if (node->partial[idx] != key[depth + idx]) return idx;
    }
    
    // If we've checked all stored bytes and partial_len > 10,
    // we need to verify the remaining bytes match by checking a leaf
    if (node->partial_len > 10) {
        LOG_DB_TRACE("check_prefix: lazy expansion, need to check beyond stored 10 bytes (partial_len=%u)",
                     node->partial_len);
        // Find any leaf under this node
        art_node_t *current = (art_node_t *)node;
        int max_depth = 64;  // Safety limit
        while (!is_leaf(current) && max_depth-- > 0) {
            art_node_t *next = NULL;
            if (current->type == NODE_4 && current->num_children > 0) {
                next = current->node4.children[0];
            } else if (current->type == NODE_16 && current->num_children > 0) {
                next = current->node16.children[0];
            } else if (current->type == NODE_48) {
                for (int i = 0; i < NODE48_MAX; i++) {
                    if (current->node48.children[i]) {
                        next = current->node48.children[i];
                        break;
                    }
                }
            } else if (current->type == NODE_256) {
                for (int i = 0; i < NODE256_MAX; i++) {
                    if (current->node256.children[i]) {
                        next = current->node256.children[i];
                        break;
                    }
                }
            }
            
            if (!next) {
                // No child found - shouldn't happen in valid tree
                LOG_DB_ERROR("check_prefix: no child found during leaf traversal!");
                return idx;
            }
            current = next;
        }
        
        if (!is_leaf(current)) {
            LOG_DB_ERROR("check_prefix: max depth reached without finding leaf!");
            return idx;
        }
        
        // Compare the remaining bytes with the leaf's key
        const uint8_t *leaf_key_bytes = leaf_key(current);
        LOG_DB_TRACE("check_prefix: comparing bytes %d to %u with leaf key", idx, node->partial_len);
        for (; idx < node->partial_len; idx++) {
            if (depth + idx >= key_len) {
                LOG_DB_TRACE("check_prefix: key exhausted at idx=%d", idx);
                return idx;
            }
            if (leaf_key_bytes[depth + idx] != key[depth + idx]) {
                LOG_DB_TRACE("check_prefix: mismatch at idx=%d (leaf=0x%02X, key=0x%02X)",
                            idx, leaf_key_bytes[depth + idx], key[depth + idx]);
                return idx;
            }
        }
        LOG_DB_TRACE("check_prefix: full match up to idx=%d", idx);
    }
    
    return idx;
}

static const void *search(const art_node_t *node, const uint8_t *key,
                          size_t key_len, size_t depth, size_t *value_len) {
    if (!node) {
        LOG_DB_TRACE("search: NULL node at depth=%zu", depth);
        return NULL;
    }
    
    LOG_DB_TRACE("search: depth=%zu, key_len=%zu, node_type=%d", depth, key_len, node->type);
    
    // Check if it's a leaf
    if (is_leaf(node)) {
        bool matches = leaf_matches(node, key, key_len);
        LOG_DB_TRACE("search: leaf node, matches=%d", matches);
        if (matches) {
            return leaf_value(node, value_len);
        }
        return NULL;
    }
    
    // Check compressed path
    if (node->partial_len > 0) {
        int prefix_len = check_prefix(node, key, key_len, depth);
        // check_prefix returns the full match length (including lazy expansion for >10 bytes)
        // so we compare against the full partial_len
        LOG_DB_TRACE("search: compressed path check, partial_len=%u, prefix_match=%d",
                     node->partial_len, prefix_len);
        if (prefix_len != node->partial_len) {
            LOG_DB_TRACE("search: prefix mismatch, returning NULL");
            return NULL;  // Prefix mismatch
        }
        depth += node->partial_len;
        LOG_DB_TRACE("search: after compressed path, depth=%zu", depth);
    }
    
    // Determine which byte to look for (NULL byte if key is consumed)
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    LOG_DB_TRACE("search: looking for child at byte=0x%02X (depth=%zu, key_len=%zu)",
                 byte, depth, key_len);
    
    // Find child for this byte
    art_node_t *child = find_child(node, byte);
    if (!child) {
        LOG_DB_TRACE("search: no child found for byte=0x%02X", byte);
        return NULL;
    }
    LOG_DB_TRACE("search: found child, recursing");
    
    // If we've consumed the key, the child should be our leaf
    if (depth >= key_len) {
        if (is_leaf(child) && leaf_matches(child, key, key_len)) {
            return leaf_value(child, value_len);
        }
        return NULL;
    }
    
    return search(child, key, key_len, depth + 1, value_len);
}

static art_node_t *find_child(const art_node_t *node, uint8_t byte) {
    switch (node->type) {
        case NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node4.keys[i] == byte) {
                    return node->node4.children[i];
                }
            }
            return NULL;
        }
        case NODE_16: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node16.keys[i] == byte) {
                    return node->node16.children[i];
                }
            }
            return NULL;
        }
        case NODE_48: {
            uint8_t idx = node->node48.index[byte];
            if (idx == NODE48_EMPTY) return NULL;
            return node->node48.children[idx];
        }
        case NODE_256: {
            return node->node256.children[byte];
        }
        default:
            return NULL;
    }
}

static art_node_t *insert_recursive(art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth,
                                     const void *value, size_t value_len,
                                     bool *inserted) {
    LOG_DB_TRACE("insert_recursive: depth=%zu, key_len=%zu, node=%p", depth, key_len, (void*)node);
    
    // Base case: create new leaf
    if (!node) {
        LOG_DB_DEBUG("insert_recursive: creating new leaf at depth=%zu", depth);
        *inserted = true;
        return (art_node_t *)alloc_leaf(key, key_len, value, value_len);
    }
    
    LOG_DB_TRACE("insert_recursive: node_type=%d", node->type);
    
    // If it's a leaf, check for match or split
    if (is_leaf(node)) {
        // Update existing leaf
        if (leaf_matches(node, key, key_len)) {
            // Replace value
            art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
            if (new_leaf) {
                free(node);
                *inserted = false;  // Updated, not inserted
                return new_leaf;
            }
            return node;
        }
        
        // Keys differ - need to create a new node to hold both leaves
        *inserted = true;
        art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
        if (!new_leaf) return node;
        
        // Find longest common prefix between the two full keys
        const uint8_t *leaf_key_bytes = leaf_key(node);
        const art_leaf_node_t *leaf = (const art_leaf_node_t *)node;
        size_t leaf_key_len = leaf->key_len;
        
        // Calculate common prefix from current depth
        size_t limit = (key_len < leaf_key_len) ? key_len : leaf_key_len;
        size_t prefix_len = 0;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == leaf_key_bytes[depth + prefix_len]) {
            prefix_len++;
        }
        
        // Create new node to hold both leaves
        art_node_t *new_node = alloc_node(NODE_4);
        if (!new_node) {
            free(new_leaf);
            return node;
        }
        
        // Set compressed path (common prefix)
        new_node->partial_len = prefix_len;
        if (prefix_len > 0) {
            int copy_len = (prefix_len < 10) ? prefix_len : 10;
            memcpy(new_node->partial, key + depth, copy_len);
        }
        
        // Calculate new depth after compressed path
        size_t new_depth = depth + prefix_len;
        
        // Add both leaves as children with their distinguishing bytes
        // Use NULL byte (0x00) as terminator if key is fully consumed
        uint8_t new_key_byte = (new_depth < key_len) ? key[new_depth] : 0x00;
        uint8_t old_key_byte = (new_depth < leaf_key_len) ? leaf_key_bytes[new_depth] : 0x00;
        
        new_node = add_child(new_node, new_key_byte, new_leaf);
        new_node = add_child(new_node, old_key_byte, node);
        
        return new_node;
    }
    
    // Check compressed path
    if (node->partial_len > 0) {
        int prefix_len = check_prefix(node, key, key_len, depth);
        
        // Prefix mismatch - need to split the node
        if (prefix_len < node->partial_len) {
            LOG_DB_DEBUG("insert_recursive: prefix mismatch at depth=%zu, prefix_len=%d, node->partial_len=%u",
                        depth, prefix_len, node->partial_len);
            *inserted = true;
            
            // Save the discriminating byte BEFORE modifying the partial array
            uint8_t old_byte = node->partial[prefix_len];
            LOG_DB_DEBUG("insert_recursive: splitting node, old_byte=0x%02X", old_byte);
            
            art_node_t *new_node = alloc_node(NODE_4);
            if (!new_node) return node;
            
            // New node gets the matching prefix
            new_node->partial_len = prefix_len;
            int copy_len = (prefix_len < 10) ? prefix_len : 10;
            memcpy(new_node->partial, node->partial, copy_len);
            
            // Calculate the new partial length for the old node
            uint32_t new_partial_len = node->partial_len - (prefix_len + 1);
            
            // Adjust old node's prefix
            // If the old partial was > 10 bytes and remains > 10 bytes after split,
            // we need to get the bytes from a leaf since we only stored first 10
            if (node->partial_len > 10 && new_partial_len > 10) {
                // Find a leaf to get the full path
                // We'll need to traverse down to find any leaf
                art_node_t *current = node;
                while (!is_leaf(current)) {
                    // Find first non-null child
                    if (current->type == NODE_4) {
                        current = current->node4.children[0];
                    } else if (current->type == NODE_16) {
                        current = current->node16.children[0];
                    } else if (current->type == NODE_48) {
                        for (int i = 0; i < NODE48_MAX; i++) {
                            if (current->node48.children[i]) {
                                current = current->node48.children[i];
                                break;
                            }
                        }
                    } else { // NODE_256
                        for (int i = 0; i < NODE256_MAX; i++) {
                            if (current->node256.children[i]) {
                                current = current->node256.children[i];
                                break;
                            }
                        }
                    }
                }
                
                // Get the full key from the leaf
                const uint8_t *leaf_key_bytes = leaf_key(current);
                
                // Copy the new compressed path from the leaf
                // Start from: depth + prefix_len + 1 (skip matched prefix + discriminating byte)
                size_t start = depth + prefix_len + 1;
                copy_len = (new_partial_len < 10) ? new_partial_len : 10;
                memcpy(node->partial, leaf_key_bytes + start, copy_len);
            } else {
                // Simple case: just shift the partial path
                memmove(node->partial, node->partial + prefix_len + 1,
                        (new_partial_len < 10) ? new_partial_len : 10);
            }
            node->partial_len = new_partial_len;
            
            // Create leaf for new key
            art_node_t *leaf = alloc_leaf(key, key_len, value, value_len);
            if (!leaf) {
                free(new_node);
                return node;
            }
            
            // Calculate discriminating byte for new key
            uint8_t new_byte = (depth + prefix_len < key_len) ? key[depth + prefix_len] : 0x00;
            
            // Add both as children
            new_node = add_child(new_node, old_byte, node);
            new_node = add_child(new_node, new_byte, (art_node_t *)leaf);
            
            return new_node;
        }
        
        depth += node->partial_len;
    }
    
    // Determine which byte to insert for (NULL byte if key is consumed)
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    // Find child for this byte
    art_node_t *child = find_child(node, byte);
    
    if (child) {
        // Child exists
        if (depth >= key_len) {
            // Key is consumed, child should be a leaf - update it
            if (is_leaf(child) && leaf_matches(child, key, key_len)) {
                // Update existing leaf
                art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
                if (new_leaf) {
                    remove_child(node, byte, NULL);
                    node = add_child(node, byte, new_leaf);
                    free(child);
                    *inserted = false;  // Updated, not inserted
                }
            }
            return node;
        }
        // Recurse with remaining key
        art_node_t *new_child = insert_recursive(child, key, key_len, depth + 1,
                                                  value, value_len, inserted);
        if (new_child != child) {
            // Child was replaced - update parent
            remove_child(node, byte, NULL);
            node = add_child(node, byte, new_child);
        }
    } else {
        // No child - create new leaf
        *inserted = true;
        art_node_t *leaf = alloc_leaf(key, key_len, value, value_len);
        if (leaf) {
            node = add_child(node, byte, leaf);
        }
    }
    
    return node;
}

static art_node_t *add_child(art_node_t *node, uint8_t byte, art_node_t *child) {
    LOG_DB_TRACE("add_child: node_type=%d, byte=0x%02X, num_children=%u",
                 node->type, byte, node->num_children);
    switch (node->type) {
        case NODE_4: {
            if (node->num_children < NODE4_MAX) {
                // Find insertion position to keep sorted
                int i;
                for (i = 0; i < node->num_children; i++) {
                    if (byte < node->node4.keys[i]) break;
                }
                // Shift elements
                if (i < node->num_children) {
                    memmove(&node->node4.keys[i + 1], &node->node4.keys[i], node->num_children - i);
                    memmove(&node->node4.children[i + 1], &node->node4.children[i],
                            (node->num_children - i) * sizeof(art_node_t *));
                }
                node->node4.keys[i] = byte;
                node->node4.children[i] = child;
                node->num_children++;
                LOG_DB_TRACE("add_child: added to Node4 at position %d, num_children now %u", i, node->num_children);
                return node;
            }
            
            // Grow to Node16
            art_node_t *new_node = alloc_node(NODE_16);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, 10);
            
            // Copy children
            memcpy(new_node->node16.keys, node->node4.keys, NODE4_MAX);
            memcpy(new_node->node16.children, node->node4.children, NODE4_MAX * sizeof(art_node_t *));
            new_node->num_children = NODE4_MAX;
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case NODE_16: {
            if (node->num_children < NODE16_MAX) {
                // Find insertion position
                int i;
                for (i = 0; i < node->num_children; i++) {
                    if (byte < node->node16.keys[i]) break;
                }
                // Shift elements
                if (i < node->num_children) {
                    memmove(&node->node16.keys[i + 1], &node->node16.keys[i], node->num_children - i);
                    memmove(&node->node16.children[i + 1], &node->node16.children[i],
                            (node->num_children - i) * sizeof(art_node_t *));
                }
                node->node16.keys[i] = byte;
                node->node16.children[i] = child;
                node->num_children++;
                return node;
            }
            
            // Grow to Node48
            art_node_t *new_node = alloc_node(NODE_48);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, 10);
            
            // Copy children
            for (int i = 0; i < NODE16_MAX; i++) {
                new_node->node48.index[node->node16.keys[i]] = i;
                new_node->node48.children[i] = node->node16.children[i];
            }
            new_node->num_children = NODE16_MAX;
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case NODE_48: {
            if (node->num_children < NODE48_MAX) {
                // Find free slot
                int slot;
                for (slot = 0; slot < NODE48_MAX; slot++) {
                    if (!node->node48.children[slot]) break;
                }
                node->node48.index[byte] = slot;
                node->node48.children[slot] = child;
                node->num_children++;
                return node;
            }
            
            // Grow to Node256
            art_node_t *new_node = alloc_node(NODE_256);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, 10);
            
            // Copy children
            for (int i = 0; i < 256; i++) {
                if (node->node48.index[i] != NODE48_EMPTY) {
                    new_node->node256.children[i] = node->node48.children[node->node48.index[i]];
                }
            }
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case NODE_256: {
            node->node256.children[byte] = child;
            return node;
        }
        
        default:
            return node;
    }
}

static void remove_child(art_node_t *node, uint8_t byte, art_node_t **child_out) {
    switch (node->type) {
        case NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node4.keys[i] == byte) {
                    if (child_out) *child_out = node->node4.children[i];
                    // Shift remaining elements
                    memmove(&node->node4.keys[i], &node->node4.keys[i + 1], node->num_children - i - 1);
                    memmove(&node->node4.children[i], &node->node4.children[i + 1],
                            (node->num_children - i - 1) * sizeof(art_node_t *));
                    node->num_children--;
                    return;
                }
            }
            break;
        }
        case NODE_16: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node16.keys[i] == byte) {
                    if (child_out) *child_out = node->node16.children[i];
                    memmove(&node->node16.keys[i], &node->node16.keys[i + 1], node->num_children - i - 1);
                    memmove(&node->node16.children[i], &node->node16.children[i + 1],
                            (node->num_children - i - 1) * sizeof(art_node_t *));
                    node->num_children--;
                    return;
                }
            }
            break;
        }
        case NODE_48: {
            uint8_t idx = node->node48.index[byte];
            if (idx != NODE48_EMPTY) {
                if (child_out) *child_out = node->node48.children[idx];
                node->node48.children[idx] = NULL;
                node->node48.index[byte] = NODE48_EMPTY;
                node->num_children--;
            }
            break;
        }
        case NODE_256: {
            if (child_out) *child_out = node->node256.children[byte];
            node->node256.children[byte] = NULL;
            break;
        }
        default:
            break;
    }
}

static art_node_t *delete_recursive(art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth, bool *deleted) {
    if (!node) return NULL;
    
    // If it's a leaf, check if it matches
    if (is_leaf(node)) {
        if (leaf_matches(node, key, key_len)) {
            *deleted = true;
            free(node);
            return NULL;
        }
        return node;
    }
    
    // Check compressed path
    if (node->partial_len > 0) {
        int prefix_len = check_prefix(node, key, key_len, depth);
        if (prefix_len != (node->partial_len < 10 ? node->partial_len : 10)) {
            return node;  // Prefix mismatch - key not found
        }
        depth += node->partial_len;
    }
    
    // Determine which byte to look for (NULL byte if key is consumed)
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    // Find and delete from child
    art_node_t *child = find_child(node, byte);
    if (!child) return node;
    
    // If key is consumed, child should be the leaf to delete
    if (depth >= key_len) {
        if (is_leaf(child) && leaf_matches(child, key, key_len)) {
            *deleted = true;
            free(child);
            remove_child(node, byte, NULL);
            // TODO: Implement node shrinking if needed
            return node;
        }
        return node;
    }
    
    art_node_t *new_child = delete_recursive(child, key, key_len, depth + 1, deleted);
    
    if (new_child != child) {
        if (!new_child) {
            // Child was deleted
            remove_child(node, byte, NULL);
            
            // TODO: Implement node shrinking (Node16->Node4, Node48->Node16, Node256->Node48)
            // For now, just keep the node as-is
        } else {
            // Child was replaced
            remove_child(node, byte, NULL);
            node = add_child(node, byte, new_child);
        }
    }
    
    return node;
}
