#ifndef MEM_ART_H
#define MEM_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * In-Memory Adaptive Radix Tree (ART) Implementation
 * 
 * DESIGNED FOR FIXED-SIZE KEYS (e.g., 20-byte addresses, 32-byte hashes)
 * This implementation does NOT support variable-length keys or prefix relationships.
 * All keys must have the same length (e.g., Ethereum: 20 bytes for addresses, 32 bytes for storage keys).
 * For variable-length key support, see branch: feature/values-in-internal-nodes
 * 
 * *** MEMORY-ONLY - NO PERSISTENCE ***
 * This implementation uses malloc/free and stores all data in RAM.
 * Data is lost when the process terminates.
 * For disk-backed persistent storage, see data_art.h instead.
 * 
 * A space-efficient, cache-friendly ordered key-value trie with O(k) operations
 * where k is the key length.
 * 
 * Features:
 * - Adaptive node sizes: 4, 16, 48, 256 children
 * - Path compression for single-child chains
 * - Ordered iteration support
 * - Memory-efficient for sparse and dense datasets
 * - Fast: Direct memory pointers, zero persistence overhead
 * - Simple: Single-threaded, no durability guarantees
 * 
 * Use Cases:
 * - Small datasets that fit entirely in RAM
 * - Temporary data structures during computation
 * - Testing and development baseline
 * - Buffer pool cache (used internally by persistent ART)
 * 
 * References:
 * - "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases"
 *   by Viktor Leis et al., ICDE 2013
 */

// Forward declarations
typedef struct mem_art mem_art_t;
typedef struct mem_art_node mem_art_node_t;
typedef struct mem_art_iterator mem_art_iterator_t;

/**
 * Node types in the ART structure
 */
typedef enum {
    MEM_NODE_4   = 0,  // Up to 4 children
    MEM_NODE_16  = 1,  // Up to 16 children
    MEM_NODE_48  = 2,  // Up to 48 children (indexed)
    MEM_NODE_256 = 3,  // Up to 256 children (direct array)
    MEM_NODE_LEAF = 4  // Leaf node containing key-value pair
} mem_art_node_type_t;

/**
 * ART tree structure
 */
struct mem_art {
    mem_art_node_t *root;     // Root node of the tree
    size_t size;          // Number of key-value pairs in the tree
};

/**
 * Iterator for ordered traversal of the tree
 */
struct mem_art_iterator {
    mem_art_t *tree;     // Reference to the tree
    void *internal;       // Internal iterator state (opaque)
};

/**
 * Initialize an empty ART tree
 * 
 * @param tree Pointer to the tree structure to initialize
 * @return true on success, false on failure
 */
bool mem_art_init(mem_art_t *tree);

/**
 * Destroy an ART tree and free all resources
 * 
 * @param tree Pointer to the tree to destroy
 */
void mem_art_destroy(mem_art_t *tree);

/**
 * Insert or update a key-value pair in the tree
 * 
 * @param tree Pointer to the tree
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @param value Pointer to the value to store
 * @param value_len Length of the value in bytes
 * @return true on success, false on failure (e.g., allocation error)
 */
bool mem_art_insert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len);

/**
 * Retrieve a value by key
 * 
 * @param tree Pointer to the tree
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @param value_len Output parameter for the value length (can be NULL)
 * @return Pointer to the value if found, NULL if not found
 */
const void *mem_art_get(const mem_art_t *tree, const uint8_t *key, size_t key_len,
                    size_t *value_len);

/**
 * Delete a key-value pair from the tree
 * 
 * @param tree Pointer to the tree
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @return true if the key was found and deleted, false if not found
 */
bool mem_art_delete(mem_art_t *tree, const uint8_t *key, size_t key_len);

/**
 * Check if a key exists in the tree
 * 
 * @param tree Pointer to the tree
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @return true if the key exists, false otherwise
 */
bool mem_art_contains(const mem_art_t *tree, const uint8_t *key, size_t key_len);

/**
 * Get the number of key-value pairs in the tree
 * 
 * @param tree Pointer to the tree
 * @return Number of entries
 */
size_t mem_art_size(const mem_art_t *tree);

/**
 * Check if the tree is empty
 * 
 * @param tree Pointer to the tree
 * @return true if empty, false otherwise
 */
bool mem_art_is_empty(const mem_art_t *tree);

/**
 * Create an iterator for ordered traversal
 * 
 * The iterator traverses keys in lexicographic order.
 * 
 * @param tree Pointer to the tree to iterate
 * @return Pointer to iterator, or NULL on allocation failure
 */
mem_art_iterator_t *mem_art_iterator_create(const mem_art_t *tree);

/**
 * Move iterator to the next key-value pair
 * 
 * @param iter Pointer to the iterator
 * @return true if moved to next item, false if end of iteration
 */
bool mem_art_iterator_next(mem_art_iterator_t *iter);

/**
 * Get the current key from the iterator
 * 
 * @param iter Pointer to the iterator
 * @param key_len Output parameter for key length (can be NULL)
 * @return Pointer to key bytes, or NULL if iterator is at end
 */
const uint8_t *mem_art_iterator_key(const mem_art_iterator_t *iter, size_t *key_len);

/**
 * Get the current value from the iterator
 * 
 * @param iter Pointer to the iterator
 * @param value_len Output parameter for value length (can be NULL)
 * @return Pointer to value, or NULL if iterator is at end
 */
const void *mem_art_iterator_value(const mem_art_iterator_t *iter, size_t *value_len);

/**
 * Check if iterator has reached the end
 * 
 * @param iter Pointer to the iterator
 * @return true if at end, false otherwise
 */
bool mem_art_iterator_done(const mem_art_iterator_t *iter);

/**
 * Destroy an iterator and free resources
 * 
 * @param iter Pointer to the iterator to destroy
 */
void mem_art_iterator_destroy(mem_art_iterator_t *iter);

/**
 * Callback function type for iteration
 * 
 * @param key Pointer to key bytes
 * @param key_len Length of the key
 * @param value Pointer to value
 * @param value_len Length of the value
 * @param user_data User-provided context pointer
 * @return true to continue iteration, false to stop
 */
typedef bool (*mem_art_callback_t)(const uint8_t *key, size_t key_len,
                                const void *value, size_t value_len,
                                void *user_data);

/**
 * Iterate over all key-value pairs with a callback function
 * 
 * Keys are visited in lexicographic order.
 * 
 * @param tree Pointer to the tree
 * @param callback Function to call for each key-value pair
 * @param user_data User-provided context pointer passed to callback
 */
void mem_art_foreach(const mem_art_t *tree, mem_art_callback_t callback, void *user_data);

#endif // MEM_ART_H
