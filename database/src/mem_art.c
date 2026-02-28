#include "mem_art.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <emmintrin.h>  // SSE2: _mm_cmpeq_epi8, _mm_set1_epi8, _mm_loadu_si128

/**
 * In-Memory ART Implementation
 * 
 * *** MEMORY-ONLY - NO PERSISTENCE ***
 * This implementation uses malloc/free for all allocations.
 * All data is stored in RAM and lost on process termination.
 * 
 * Node structure details:
 * - Node4: 4 keys + 4 child pointers (sorted by key for binary search)
 * - Node16: 16 keys + 16 child pointers (sorted by key for SIMD-friendly search)
 * - Node48: 256-byte index + 48 child pointers (index maps byte to child slot)
 * - Node256: 256 child pointers (direct indexing, no key array needed)
 * - Leaf: stores full key + value
 * 
 * All pointers are direct memory addresses (not page references).
 * For persistent, disk-backed version, see data_art.c instead.
 *
 * TODO: Replace malloc/free with bump arena allocator.
 * mem_art serves dual purpose: write buffer (accumulate dirty state, merge into
 * compact_art) and EVM state cache (SLOAD/SSTORE/BALANCE lookups during execution).
 * Same tree handles both — dirty writes and reads in one place.
 * It's on the hot path for every state-touching opcode. Arena allocation gives:
 *   - Better cache locality (nodes packed in contiguous memory)
 *   - Faster allocation (pointer bump vs malloc)
 *   - Faster destroy (free arena chunks vs recursive tree walk)
 *   - Less overhead (no 16-32B malloc metadata per allocation)
 * Bump arena works because the tree resets per block — no individual frees needed.
 */

// Maximum children per node type
#define NODE4_MAX 4
#define NODE16_MAX 16
#define NODE48_MAX 48
#define NODE256_MAX 256

// Maximum prefix bytes stored inline (matches persistent ART)
#define MAX_PREFIX 32

// Special value for Node48 index indicating no child
#define NODE48_EMPTY 255

/**
 * Node4: Up to 4 children
 * Memory: ~48 bytes (4 keys + 4 pointers)
 */
typedef struct {
    uint8_t keys[NODE4_MAX];           // Sorted array of key bytes
    mem_art_node_t *children[NODE4_MAX];   // Corresponding child pointers
} mem_node4_t;

/**
 * Node16: Up to 16 children
 * Memory: ~144 bytes (16 keys + 16 pointers)
 */
typedef struct {
    uint8_t keys[NODE16_MAX];          // Sorted array of key bytes
    mem_art_node_t *children[NODE16_MAX];  // Corresponding child pointers
} mem_node16_t;

/**
 * Node48: Up to 48 children with indexed lookup
 * Memory: ~640 bytes (256-byte index + 48 pointers)
 */
typedef struct {
    uint8_t index[256];                // Maps key byte to child index (255 = empty)
    mem_art_node_t *children[NODE48_MAX];  // Child pointers
} mem_node48_t;

/**
 * Node256: Up to 256 children with direct indexing
 * Memory: ~2KB (256 pointers)
 */
typedef struct {
    mem_art_node_t *children[NODE256_MAX]; // Direct array of child pointers
} mem_node256_t;

/**
 * Leaf node structure - allocated separately with variable size
 */
typedef struct {
    mem_art_node_type_t type;  // Always MEM_NODE_LEAF
    uint32_t key_len;
    uint32_t value_len;
    uint8_t data[];  // Flexible array: key bytes followed by value bytes
} mem_leaf_node_t;

/**
 * Inner node structure - all inner nodes use this structure
 */
struct mem_art_node {
    mem_art_node_type_t type;  // Type discriminator
    uint32_t num_children;
    uint32_t partial_len;
    uint8_t partial[MAX_PREFIX];
    union {
        mem_node4_t node4;
        mem_node16_t node16;
        mem_node48_t node48;
        mem_node256_t node256;
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
        mem_art_node_t *node;
        int child_idx;
    } stack[64];  // Max depth
    int depth;
} mem_iterator_state_t;

// Forward declarations of internal functions
static mem_art_node_t *alloc_node(mem_art_node_type_t type);
static void free_node(mem_art_node_t *node);
static mem_art_node_t *alloc_leaf(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len);
static bool is_leaf(const mem_art_node_t *node);
static mem_art_node_t *insert_recursive(mem_art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth,
                                     const void *value, size_t value_len,
                                     bool *inserted);
static mem_art_node_t *delete_recursive(mem_art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth, bool *deleted);
static const void *search(const mem_art_node_t *node, const uint8_t *key,
                          size_t key_len, size_t depth, size_t *value_len);
static int check_prefix(const mem_art_node_t *node, const uint8_t *key,
                        size_t key_len, size_t depth);
static mem_art_node_t *add_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t *child);
static mem_art_node_t *remove_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t **child_out);
static mem_art_node_t *find_child(const mem_art_node_t *node, uint8_t byte);
static void replace_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t *new_child);

//==============================================================================
// Public API Implementation
//==============================================================================

bool mem_art_init(mem_art_t *tree) {
    if (!tree) return false;
    tree->root = NULL;
    tree->size = 0;
    return true;
}

void mem_art_destroy(mem_art_t *tree) {
    if (!tree) return;
    if (tree->root) {
        free_node(tree->root);
        tree->root = NULL;
    }
    tree->size = 0;
}

bool mem_art_insert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len) {
    if (!tree || !key || key_len == 0) return false;

    bool inserted = false;
    tree->root = insert_recursive(tree->root, key, key_len, 0, value, value_len, &inserted);
    
    if (inserted) {
        tree->size++;
    }

    return tree->root != NULL;
}

const void *mem_art_get(const mem_art_t *tree, const uint8_t *key, size_t key_len,
                    size_t *value_len) {
    if (!tree || !key || key_len == 0) return NULL;
    return search(tree->root, key, key_len, 0, value_len);
}

bool mem_art_delete(mem_art_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key || key_len == 0) return false;

    bool deleted = false;
    tree->root = delete_recursive(tree->root, key, key_len, 0, &deleted);

    if (deleted) {
        tree->size--;
    }
    
    return deleted;
}

bool mem_art_contains(const mem_art_t *tree, const uint8_t *key, size_t key_len) {
    return mem_art_get(tree, key, key_len, NULL) != NULL;
}

size_t mem_art_size(const mem_art_t *tree) {
    return tree ? tree->size : 0;
}

bool mem_art_is_empty(const mem_art_t *tree) {
    return tree ? (tree->size == 0) : true;
}

//==============================================================================
// Iterator Implementation
//==============================================================================

mem_art_iterator_t *mem_art_iterator_create(const mem_art_t *tree) {
    if (!tree) return NULL;
    
    mem_art_iterator_t *iter = malloc(sizeof(mem_art_iterator_t));
    if (!iter) return NULL;
    
    mem_iterator_state_t *state = malloc(sizeof(mem_iterator_state_t));
    if (!state) {
        free(iter);
        return NULL;
    }
    
    iter->tree = (mem_art_t *)tree;
    iter->internal = state;
    
    memset(state, 0, sizeof(mem_iterator_state_t));
    state->done = (tree->root == NULL);
    state->depth = -1;
    
    return iter;
}

bool mem_art_iterator_next(mem_art_iterator_t *iter) {
    if (!iter || !iter->internal) return false;
    
    mem_iterator_state_t *state = (mem_iterator_state_t *)iter->internal;
    if (state->done) return false;
    
    mem_art_node_t *node;
    
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
            mem_leaf_node_t *leaf = (mem_leaf_node_t *)node;
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
        
        mem_art_node_t *next_child = NULL;
        
        // Get next child based on node type
        switch (node->type) {
            case MEM_NODE_4:
                if (*child_idx < (int)node->num_children) {
                    next_child = node->node4.children[*child_idx];
                }
                break;
            case MEM_NODE_16:
                if (*child_idx < (int)node->num_children) {
                    next_child = node->node16.children[*child_idx];
                }
                break;
            case MEM_NODE_48:
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
            case MEM_NODE_256:
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

const uint8_t *mem_art_iterator_key(const mem_art_iterator_t *iter, size_t *key_len) {
    if (!iter || !iter->internal) return NULL;
    
    mem_iterator_state_t *state = (mem_iterator_state_t *)iter->internal;
    if (state->done) return NULL;
    
    if (key_len) *key_len = state->key_len;
    return state->key;
}

const void *mem_art_iterator_value(const mem_art_iterator_t *iter, size_t *value_len) {
    if (!iter || !iter->internal) return NULL;
    
    mem_iterator_state_t *state = (mem_iterator_state_t *)iter->internal;
    if (state->done) return NULL;
    
    if (value_len) *value_len = state->value_len;
    return state->value;
}

bool mem_art_iterator_done(const mem_art_iterator_t *iter) {
    if (!iter || !iter->internal) return true;
    
    mem_iterator_state_t *state = (mem_iterator_state_t *)iter->internal;
    return state->done;
}

void mem_art_iterator_destroy(mem_art_iterator_t *iter) {
    if (!iter) return;
    if (iter->internal) {
        free(iter->internal);
    }
    free(iter);
}

void mem_art_foreach(const mem_art_t *tree, mem_art_callback_t callback, void *user_data) {
    if (!tree || !callback) return;
    
    mem_art_iterator_t *iter = mem_art_iterator_create(tree);
    if (!iter) return;
    
    while (mem_art_iterator_next(iter)) {
        size_t key_len, value_len;
        const uint8_t *key = mem_art_iterator_key(iter, &key_len);
        const void *value = mem_art_iterator_value(iter, &value_len);
        
        if (key && value) {
            if (!callback(key, key_len, value, value_len, user_data)) {
                break;  // Callback returned false, stop iteration
            }
        }
    }
    
    mem_art_iterator_destroy(iter);
}

//==============================================================================
// Internal Helper Functions
//==============================================================================

// Header size: type + num_children + partial_len + partial[]
#define NODE_HEADER_SIZE (offsetof(mem_art_node_t, node4))

static mem_art_node_t *alloc_node(mem_art_node_type_t type) {
    // Allocate only what's needed for the specific node type
    size_t size;
    switch (type) {
        case MEM_NODE_4:   size = NODE_HEADER_SIZE + sizeof(mem_node4_t);   break;
        case MEM_NODE_16:  size = NODE_HEADER_SIZE + sizeof(mem_node16_t);  break;
        case MEM_NODE_48:  size = NODE_HEADER_SIZE + sizeof(mem_node48_t);  break;
        case MEM_NODE_256: size = NODE_HEADER_SIZE + sizeof(mem_node256_t); break;
        default:       return NULL;
    }

    mem_art_node_t *node = malloc(size);
    if (!node) return NULL;

    node->type = type;
    node->num_children = 0;
    node->partial_len = 0;

    // Only zero the type-specific part
    switch (type) {
        case MEM_NODE_4:
            memset(&node->node4, 0, sizeof(mem_node4_t));
            break;
        case MEM_NODE_16:
            memset(&node->node16, 0, sizeof(mem_node16_t));
            break;
        case MEM_NODE_48:
            memset(node->node48.index, NODE48_EMPTY, 256);
            memset(node->node48.children, 0, sizeof(node->node48.children));
            break;
        case MEM_NODE_256:
            memset(&node->node256, 0, sizeof(mem_node256_t));
            break;
        default:
            break;
    }

    return node;
}

static void free_node(mem_art_node_t *node) {
    if (!node) return;
    
    if (is_leaf(node)) {
        free(node);
        return;
    }
    
    // Recursively free children
    switch (node->type) {
        case MEM_NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                free_node(node->node4.children[i]);
            }
            break;
        }
        case MEM_NODE_16: {
            for (int i = 0; i < node->num_children; i++) {
                free_node(node->node16.children[i]);
            }
            break;
        }
        case MEM_NODE_48: {
            for (int i = 0; i < NODE48_MAX; i++) {
                if (node->node48.children[i]) {
                    free_node(node->node48.children[i]);
                }
            }
            break;
        }
        case MEM_NODE_256: {
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

static mem_art_node_t *alloc_leaf(const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len) {
    size_t total_size = sizeof(mem_leaf_node_t) + key_len + value_len;
    mem_leaf_node_t *leaf = malloc(total_size);
    if (!leaf) return NULL;
    
    leaf->type = MEM_NODE_LEAF;
    leaf->key_len = key_len;
    leaf->value_len = value_len;
    
    // Copy key and value into data array
    memcpy(leaf->data, key, key_len);
    memcpy(leaf->data + key_len, value, value_len);
    
    return (mem_art_node_t *)leaf;
}

static bool is_leaf(const mem_art_node_t *node) {
    return node && node->type == MEM_NODE_LEAF;
}

static const uint8_t *leaf_key(const mem_art_node_t *node) {
    const mem_leaf_node_t *leaf = (const mem_leaf_node_t *)node;
    return leaf->data;
}

static const void *leaf_value(const mem_art_node_t *node, size_t *value_len) {
    const mem_leaf_node_t *leaf = (const mem_leaf_node_t *)node;
    if (value_len) *value_len = leaf->value_len;
    return leaf->data + leaf->key_len;
}

static bool leaf_matches(const mem_art_node_t *node, const uint8_t *key, size_t key_len) {
    const mem_leaf_node_t *leaf = (const mem_leaf_node_t *)node;
    if (leaf->key_len != key_len) return false;
    return memcmp(leaf_key(node), key, key_len) == 0;
}

/**
 * Check how many bytes of the compressed path match the key.
 * Returns the number of matching bytes.
 * With MAX_PREFIX=32, the full prefix is always stored inline — no lazy expansion.
 */
static int check_prefix(const mem_art_node_t *node, const uint8_t *key,
                        size_t key_len, size_t depth) {
    int max_cmp = (int)node->partial_len;
    if (depth + max_cmp > (int)key_len) {
        max_cmp = (int)key_len - (int)depth;
    }

    // Fast path: full match via memcmp
    if (memcmp(node->partial, key + depth, max_cmp) == 0) {
        return max_cmp;
    }

    // Slow path: find exact mismatch position
    for (int idx = 0; idx < max_cmp; idx++) {
        if (node->partial[idx] != key[depth + idx]) return idx;
    }
    return max_cmp;
}

static const void *search(const mem_art_node_t *node, const uint8_t *key,
                          size_t key_len, size_t depth, size_t *value_len) {
    while (node) {
        if (is_leaf(node)) {
            if (leaf_matches(node, key, key_len)) {
                return leaf_value(node, value_len);
            }
            return NULL;
        }

        // Check compressed path
        if (node->partial_len > 0) {
            int prefix_len = check_prefix(node, key, key_len, depth);
            if (prefix_len != (int)node->partial_len) {
                return NULL;
            }
            depth += node->partial_len;
        }

        // Find child for next byte
        uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
        mem_art_node_t *child = find_child(node, byte);
        if (!child) return NULL;

        // If key is consumed, child must be the matching leaf
        if (depth >= key_len) {
            if (is_leaf(child) && leaf_matches(child, key, key_len)) {
                return leaf_value(child, value_len);
            }
            return NULL;
        }

        node = child;
        depth++;
    }
    return NULL;
}

static mem_art_node_t *find_child(const mem_art_node_t *node, uint8_t byte) {
    switch (node->type) {
        case MEM_NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node4.keys[i] == byte) {
                    return node->node4.children[i];
                }
            }
            return NULL;
        }
        case MEM_NODE_16: {
            // SSE: compare all 16 keys in parallel
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)node->node16.keys));
            int mask = _mm_movemask_epi8(cmp) & ((1 << node->num_children) - 1);
            if (mask) {
                return node->node16.children[__builtin_ctz(mask)];
            }
            return NULL;
        }
        case MEM_NODE_48: {
            uint8_t idx = node->node48.index[byte];
            if (idx == NODE48_EMPTY) return NULL;
            return node->node48.children[idx];
        }
        case MEM_NODE_256: {
            return node->node256.children[byte];
        }
        default:
            return NULL;
    }
}

/**
 * Replace a child pointer in-place (no add/remove, no size change).
 * Used when a recursive operation returns a different pointer for the same key byte.
 */
static void replace_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t *new_child) {
    switch (node->type) {
        case MEM_NODE_4:
            for (uint32_t i = 0; i < node->num_children; i++) {
                if (node->node4.keys[i] == byte) {
                    node->node4.children[i] = new_child;
                    return;
                }
            }
            break;
        case MEM_NODE_16: {
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)node->node16.keys));
            int mask = _mm_movemask_epi8(cmp) & ((1 << node->num_children) - 1);
            if (mask) {
                node->node16.children[__builtin_ctz(mask)] = new_child;
            }
            break;
        }
        case MEM_NODE_48: {
            uint8_t idx = node->node48.index[byte];
            if (idx != NODE48_EMPTY) {
                node->node48.children[idx] = new_child;
            }
            break;
        }
        case MEM_NODE_256:
            node->node256.children[byte] = new_child;
            break;
        default:
            break;
    }
}

static mem_art_node_t *insert_recursive(mem_art_node_t *node, const uint8_t *key,
                                     size_t key_len, size_t depth,
                                     const void *value, size_t value_len,
                                     bool *inserted) {
    // Base case: create new leaf
    if (!node) {
        *inserted = true;
        return (mem_art_node_t *)alloc_leaf(key, key_len, value, value_len);
    }

    // If it's a leaf, check for match or split
    if (is_leaf(node)) {
        // Update existing leaf
        if (leaf_matches(node, key, key_len)) {
            // Replace value
            mem_art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
            if (new_leaf) {
                free(node);
                *inserted = false;  // Updated, not inserted
                return new_leaf;
            }
            return node;
        }
        
        // Keys differ - need to create a new node to hold both leaves
        *inserted = true;
        mem_art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
        if (!new_leaf) return node;
        
        // Find longest common prefix between the two full keys
        const uint8_t *leaf_key_bytes = leaf_key(node);
        const mem_leaf_node_t *leaf = (const mem_leaf_node_t *)node;
        size_t leaf_key_len = leaf->key_len;
        
        // Calculate common prefix from current depth
        size_t limit = (key_len < leaf_key_len) ? key_len : leaf_key_len;
        size_t prefix_len = 0;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == leaf_key_bytes[depth + prefix_len]) {
            prefix_len++;
        }
        
        // Create new node to hold both leaves
        mem_art_node_t *new_node = alloc_node(MEM_NODE_4);
        if (!new_node) {
            free(new_leaf);
            return node;
        }
        
        // Set compressed path (common prefix)
        new_node->partial_len = prefix_len;
        if (prefix_len > 0) {
            memcpy(new_node->partial, key + depth, prefix_len);
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
        if (prefix_len < (int)node->partial_len) {
            *inserted = true;

            // Discriminating byte for old node — always in partial[]
            uint8_t old_byte = node->partial[prefix_len];

            mem_art_node_t *new_node = alloc_node(MEM_NODE_4);
            if (!new_node) return node;

            // New node gets the matching prefix
            new_node->partial_len = prefix_len;
            memcpy(new_node->partial, node->partial, prefix_len);

            // Shift old node's prefix: skip matched prefix + discriminating byte
            uint32_t new_partial_len = node->partial_len - (prefix_len + 1);
            memmove(node->partial, node->partial + prefix_len + 1, new_partial_len);
            node->partial_len = new_partial_len;

            // Create leaf for new key
            mem_art_node_t *leaf = alloc_leaf(key, key_len, value, value_len);
            if (!leaf) {
                free(new_node);
                return node;
            }

            uint8_t new_byte = (depth + prefix_len < key_len) ? key[depth + prefix_len] : 0x00;

            new_node = add_child(new_node, old_byte, node);
            new_node = add_child(new_node, new_byte, (mem_art_node_t *)leaf);

            return new_node;
        }
        
        depth += node->partial_len;
    }
    
    // Determine which byte to insert for (NULL byte if key is consumed)
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    // Find child for this byte
    mem_art_node_t *child = find_child(node, byte);
    
    if (child) {
        // Child exists
        if (depth >= key_len) {
            // Key is consumed, child should be a leaf - update it
            if (is_leaf(child) && leaf_matches(child, key, key_len)) {
                mem_art_node_t *new_leaf = alloc_leaf(key, key_len, value, value_len);
                if (new_leaf) {
                    replace_child(node, byte, new_leaf);
                    free(child);
                    *inserted = false;
                }
            }
            return node;
        }
        // Recurse with remaining key
        mem_art_node_t *new_child = insert_recursive(child, key, key_len, depth + 1,
                                                  value, value_len, inserted);
        if (new_child != child) {
            replace_child(node, byte, new_child);
        }
    } else {
        // No child - create new leaf
        *inserted = true;
        mem_art_node_t *leaf = alloc_leaf(key, key_len, value, value_len);
        if (leaf) {
            node = add_child(node, byte, leaf);
        }
    }
    
    return node;
}

static mem_art_node_t *add_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t *child) {
    switch (node->type) {
        case MEM_NODE_4: {
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
                            (node->num_children - i) * sizeof(mem_art_node_t *));
                }
                node->node4.keys[i] = byte;
                node->node4.children[i] = child;
                node->num_children++;
                return node;
            }
            
            // Grow to Node16
            mem_art_node_t *new_node = alloc_node(MEM_NODE_16);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, node->partial_len);
            
            // Copy children
            memcpy(new_node->node16.keys, node->node4.keys, NODE4_MAX);
            memcpy(new_node->node16.children, node->node4.children, NODE4_MAX * sizeof(mem_art_node_t *));
            new_node->num_children = NODE4_MAX;
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case MEM_NODE_16: {
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
                            (node->num_children - i) * sizeof(mem_art_node_t *));
                }
                node->node16.keys[i] = byte;
                node->node16.children[i] = child;
                node->num_children++;
                return node;
            }
            
            // Grow to Node48
            mem_art_node_t *new_node = alloc_node(MEM_NODE_48);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, node->partial_len);
            
            // Initialize index array to NODE48_EMPTY
            memset(new_node->node48.index, NODE48_EMPTY, 256);
            
            // Copy children
            for (int i = 0; i < NODE16_MAX; i++) {
                new_node->node48.index[node->node16.keys[i]] = i;
                new_node->node48.children[i] = node->node16.children[i];
            }
            new_node->num_children = NODE16_MAX;
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case MEM_NODE_48: {
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
            mem_art_node_t *new_node = alloc_node(MEM_NODE_256);
            if (!new_node) return node;
            
            // Copy header
            new_node->partial_len = node->partial_len;
            memcpy(new_node->partial, node->partial, node->partial_len);
            
            // Copy children
            for (int i = 0; i < 256; i++) {
                if (node->node48.index[i] != NODE48_EMPTY) {
                    new_node->node256.children[i] = node->node48.children[node->node48.index[i]];
                }
            }
            new_node->num_children = node->num_children;  // Preserve count
            
            free(node);
            return add_child(new_node, byte, child);
        }
        
        case MEM_NODE_256: {
            if (!node->node256.children[byte]) {
                node->num_children++;
            }
            node->node256.children[byte] = child;
            return node;
        }
        
        default:
            return node;
    }
}

static mem_art_node_t *remove_child(mem_art_node_t *node, uint8_t byte, mem_art_node_t **child_out) {
    if (!node) return NULL;
    
    switch (node->type) {
        case MEM_NODE_4: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node4.keys[i] == byte) {
                    if (child_out) *child_out = node->node4.children[i];
                    // Shift remaining elements
                    memmove(&node->node4.keys[i], &node->node4.keys[i + 1], node->num_children - i - 1);
                    memmove(&node->node4.children[i], &node->node4.children[i + 1],
                            (node->num_children - i - 1) * sizeof(mem_art_node_t *));
                    node->num_children--;
                    return node;  // MEM_NODE_4 cannot shrink further
                }
            }
            break;
        }
        case MEM_NODE_16: {
            for (int i = 0; i < node->num_children; i++) {
                if (node->node16.keys[i] == byte) {
                    if (child_out) *child_out = node->node16.children[i];
                    memmove(&node->node16.keys[i], &node->node16.keys[i + 1], node->num_children - i - 1);
                    memmove(&node->node16.children[i], &node->node16.children[i + 1],
                            (node->num_children - i - 1) * sizeof(mem_art_node_t *));
                    node->num_children--;
                    
                    // Shrink MEM_NODE_16 → MEM_NODE_4 if children <= 4
                    if (node->num_children <= NODE4_MAX) {
                        mem_art_node_t *new_node = alloc_node(MEM_NODE_4);
                        if (new_node) {
                            new_node->num_children = node->num_children;
                            new_node->partial_len = node->partial_len;
                            memcpy(new_node->partial, node->partial, node->partial_len);
                            memcpy(new_node->node4.keys, node->node16.keys, node->num_children);
                            memcpy(new_node->node4.children, node->node16.children, 
                                   node->num_children * sizeof(mem_art_node_t *));
                            free(node);
                            return new_node;
                        }
                    }
                    return node;
                }
            }
            break;
        }
        case MEM_NODE_48: {
            uint8_t idx = node->node48.index[byte];
            if (idx != NODE48_EMPTY) {
                if (child_out) *child_out = node->node48.children[idx];
                node->node48.children[idx] = NULL;
                node->node48.index[byte] = NODE48_EMPTY;
                node->num_children--;
                
                // Shrink MEM_NODE_48 → MEM_NODE_16 if children <= 16
                if (node->num_children <= NODE16_MAX) {
                    mem_art_node_t *new_node = alloc_node(MEM_NODE_16);
                    if (new_node) {
                        new_node->num_children = 0;
                        new_node->partial_len = node->partial_len;
                        memcpy(new_node->partial, node->partial, node->partial_len);
                        // Copy non-null children
                        for (int i = 0; i < NODE256_MAX; i++) {
                            if (node->node48.index[i] != NODE48_EMPTY) {
                                uint8_t child_idx = node->node48.index[i];
                                new_node->node16.keys[new_node->num_children] = (uint8_t)i;
                                new_node->node16.children[new_node->num_children] = node->node48.children[child_idx];
                                new_node->num_children++;
                            }
                        }
                        free(node);
                        return new_node;
                    }
                }
                return node;
            }
            break;
        }
        case MEM_NODE_256: {
            if (child_out) *child_out = node->node256.children[byte];
            node->node256.children[byte] = NULL;
            node->num_children--;
            
            // Shrink MEM_NODE_256 → MEM_NODE_48 if children <= 48
            if (node->num_children <= NODE48_MAX) {
                mem_art_node_t *new_node = alloc_node(MEM_NODE_48);
                if (new_node) {
                    new_node->num_children = 0;
                    new_node->partial_len = node->partial_len;
                    memcpy(new_node->partial, node->partial, node->partial_len);
                    // Initialize index array
                    memset(new_node->node48.index, NODE48_EMPTY, NODE256_MAX);
                    // Copy non-null children
                    for (int i = 0; i < NODE256_MAX; i++) {
                        if (node->node256.children[i]) {
                            new_node->node48.index[i] = (uint8_t)new_node->num_children;
                            new_node->node48.children[new_node->num_children] = node->node256.children[i];
                            new_node->num_children++;
                        }
                    }
                    free(node);
                    return new_node;
                }
            }
            break;
        }
        default:
            break;
    }
    return node;
}

static mem_art_node_t *delete_recursive(mem_art_node_t *node, const uint8_t *key,
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
        if (prefix_len != (int)node->partial_len) {
            return node;  // Prefix mismatch - key not found
        }
        depth += node->partial_len;
    }
    
    // Determine which byte to look for (NULL byte if key is consumed)
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    
    // Find and delete from child
    mem_art_node_t *child = find_child(node, byte);
    if (!child) return node;
    
    // If key is consumed, child should be the leaf to delete
    if (depth >= key_len) {
        if (is_leaf(child) && leaf_matches(child, key, key_len)) {
            *deleted = true;
            free(child);
            node = remove_child(node, byte, NULL);  // node might shrink
            return node;
        }
        return node;
    }
    
    mem_art_node_t *new_child = delete_recursive(child, key, key_len, depth + 1, deleted);

    if (new_child != child) {
        if (!new_child) {
            // Child was deleted
            node = remove_child(node, byte, NULL);  // node might shrink
        } else {
            // Child was replaced (e.g., node split) — update pointer in-place
            replace_child(node, byte, new_child);
        }
    }

    // Path collapse: if Node4 has exactly 1 child, merge with that child
    if (!is_leaf(node) && node->type == MEM_NODE_4 && node->num_children == 1) {
        mem_art_node_t *only_child = node->node4.children[0];
        uint8_t only_byte = node->node4.keys[0];

        if (is_leaf(only_child)) {
            // Single leaf child — just return the leaf, free the Node4
            free(node);
            return only_child;
        }

        // Merge prefixes: node->partial + discriminating byte + child->partial
        uint32_t new_len = node->partial_len + 1 + only_child->partial_len;
        if (new_len <= MAX_PREFIX) {
            // Shift child's partial right to make room
            memmove(only_child->partial + node->partial_len + 1,
                    only_child->partial, only_child->partial_len);
            // Copy parent's partial
            memcpy(only_child->partial, node->partial, node->partial_len);
            // Insert discriminating byte
            only_child->partial[node->partial_len] = only_byte;
            only_child->partial_len = new_len;
            free(node);
            return only_child;
        }
        // If merged prefix would exceed MAX_PREFIX, keep as-is
    }

    return node;
}
