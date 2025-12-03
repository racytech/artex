#ifndef MPT_H
#define MPT_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Hash-only Merkle Patricia Trie (MPT) Implementation
 * 
 * This is a memory-efficient MPT that stores only hashes at intermediate nodes,
 * using an ART (Adaptive Radix Tree) as the storage backend for actual key-value data.
 * 
 * Key Features:
 * - Hash-only nodes: Branch and Extension nodes store only child hashes
 * - ART storage: Actual key-value pairs stored in ART for O(k) access
 * - Merkle proofs: Generate and verify inclusion proofs
 * - State root: Cryptographic commitment to entire state
 * 
 * Architecture:
 * - Keys are hashed with Keccak256 and converted to 64 nibbles
 * - MPT uses nibbles for path traversal (16-way branching)
 * - Node types: Branch (16 children), Extension (compressed path), Leaf (terminal)
 * - All nodes are hashed with keccak256(rlp(node_contents))
 */

#define MPT_HASH_SIZE 32        // Keccak256 produces 32-byte hashes
#define MPT_NIBBLES_PER_BYTE 2  // Each byte = 2 nibbles
#define MPT_MAX_KEY_NIBBLES 64  // 32 bytes = 64 nibbles

/**
 * MPT node types
 */
typedef enum {
    MPT_NODE_BRANCH = 0,    // 16 children + optional value
    MPT_NODE_EXTENSION = 1, // Compressed path with single child
    MPT_NODE_LEAF = 2       // Terminal node with value
} mpt_node_type_t;

/**
 * Hash representation (32 bytes)
 */
typedef struct {
    uint8_t bytes[MPT_HASH_SIZE];
} mpt_hash_t;

/**
 * MPT node structure (hash-only)
 * Only stores hashes, not actual data
 */
typedef struct {
    mpt_node_type_t type;
    
    union {
        // Branch node: 16 child hashes + optional value hash
        struct {
            mpt_hash_t children[16];  // Child hashes (empty = all zeros)
            mpt_hash_t value_hash;     // Hash of value if present
            bool has_value;
        } branch;
        
        // Extension node: compressed path + single child hash
        struct {
            uint8_t *path;            // Nibble path (shared prefix)
            size_t path_len;          // Length in nibbles
            mpt_hash_t child_hash;    // Hash of single child
        } extension;
        
        // Leaf node: key suffix + value hash
        struct {
            uint8_t *key_suffix;      // Remaining key nibbles
            size_t suffix_len;        // Length in nibbles
            mpt_hash_t value_hash;    // Hash of the value
        } leaf;
    };
} mpt_node_t;

/**
 * MPT state - combines ART storage with Merkle tree structure
 */
typedef struct {
    void *art_tree;           // ART tree for actual key-value storage (opaque)
    mpt_hash_t root_hash;     // Current state root hash
    size_t size;              // Number of entries
} mpt_state_t;

/**
 * Merkle proof structure
 */
typedef struct {
    uint8_t *key;             // Key being proven (original bytes, not nibbles)
    size_t key_len;
    uint8_t *value;           // Value at the key
    size_t value_len;
    mpt_hash_t *proof_nodes;  // Array of sibling hashes along the path
    size_t proof_len;         // Number of proof nodes
    mpt_hash_t root_hash;     // Expected root hash
} mpt_proof_t;

/**
 * Initialize an empty MPT state
 * 
 * @param state Pointer to the state structure to initialize
 * @return true on success, false on failure
 */
bool mpt_init(mpt_state_t *state);

/**
 * Destroy an MPT state and free all resources
 * 
 * @param state Pointer to the state to destroy
 */
void mpt_destroy(mpt_state_t *state);

/**
 * Insert or update a key-value pair and recompute state root
 * 
 * @param state Pointer to the MPT state
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @param value Pointer to the value to store
 * @param value_len Length of the value in bytes
 * @return true on success, false on failure
 */
bool mpt_insert(mpt_state_t *state, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len);

/**
 * Retrieve a value by key
 * 
 * @param state Pointer to the MPT state
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @param value_len Output parameter for the value length (can be NULL)
 * @return Pointer to the value if found, NULL if not found
 */
const void *mpt_get(const mpt_state_t *state, const uint8_t *key, size_t key_len,
                    size_t *value_len);

/**
 * Delete a key-value pair and recompute state root
 * 
 * @param state Pointer to the MPT state
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @return true if key was found and deleted, false if not found
 */
bool mpt_delete(mpt_state_t *state, const uint8_t *key, size_t key_len);

/**
 * Get the current state root hash
 * 
 * @param state Pointer to the MPT state
 * @return Pointer to the root hash (32 bytes)
 */
const mpt_hash_t *mpt_root(const mpt_state_t *state);

/**
 * Generate a Merkle proof for a key
 * 
 * @param state Pointer to the MPT state
 * @param key Byte array key
 * @param key_len Length of the key in bytes
 * @param proof Output pointer to the proof structure
 * @return true if proof generated successfully, false if key not found
 */
bool mpt_prove(const mpt_state_t *state, const uint8_t *key, size_t key_len,
               mpt_proof_t **proof);

/**
 * Verify a Merkle proof
 * 
 * @param proof Pointer to the proof to verify
 * @return true if proof is valid, false otherwise
 */
bool mpt_verify_proof(const mpt_proof_t *proof);

/**
 * Free a proof structure
 * 
 * @param proof Pointer to the proof to free
 */
void mpt_proof_free(mpt_proof_t *proof);

/**
 * Get the number of key-value pairs in the MPT
 * 
 * @param state Pointer to the MPT state
 * @return Number of entries
 */
size_t mpt_size(const mpt_state_t *state);

/**
 * Check if the MPT is empty
 * 
 * @param state Pointer to the MPT state
 * @return true if empty, false otherwise
 */
bool mpt_is_empty(const mpt_state_t *state);

#endif // MPT_H
