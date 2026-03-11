#ifndef MEM_MPT_H
#define MEM_MPT_H

/**
 * In-Memory Batch MPT Root Computation (testing only)
 *
 * Computes the Ethereum MPT state root from a set of key-value pairs in one
 * pass. O(n log n) -- sort + recursive trie build with stack-allocated RLP.
 *
 * This is used exclusively by the integration test runner to verify post-state
 * roots without requiring a disk-backed mpt_store. For production chain replay,
 * use mpt_store (incremental, persistent) instead.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "hash.h"

/**
 * Batch entry for root computation.
 * Keys must be 32 bytes (keccak256 hashes of addresses/storage keys).
 */
typedef struct {
    uint8_t key[32];           // 32-byte key (already hashed)
    const uint8_t *value;      // Pointer to RLP-encoded value (caller-managed)
    size_t value_len;          // Length of value data
} mpt_batch_entry_t;

/**
 * Compute MPT root hash from a batch of key-value pairs in one pass.
 *
 * Entries are sorted internally by key. The entries array may be reordered.
 *
 * @param entries Array of batch entries (may be NULL if count == 0)
 * @param count   Number of entries
 * @param out_root Output 32-byte root hash
 * @return true on success
 */
bool mpt_compute_root_batch(mpt_batch_entry_t *entries, size_t count,
                            hash_t *out_root);

/**
 * Batch entry for unsecured trie (variable-length raw keys).
 * Used for transaction tries and receipt tries where keys are
 * RLP-encoded indices (not keccak256-hashed).
 */
typedef struct {
    const uint8_t *key;        // Raw key bytes (NOT hashed)
    size_t         key_len;    // Key length in bytes
    const uint8_t *value;      // Pointer to RLP-encoded value (caller-managed)
    size_t         value_len;  // Length of value data
} mpt_unsecured_entry_t;

/**
 * Compute MPT root hash from unsecured (raw-key) entries.
 *
 * Same algorithm as mpt_compute_root_batch but keys are used directly
 * (not pre-hashed). Supports variable-length keys up to 32 bytes.
 *
 * @param entries Array of entries (may be reordered internally)
 * @param count   Number of entries
 * @param out_root Output 32-byte root hash
 * @return true on success
 */
bool mpt_compute_root_unsecured(mpt_unsecured_entry_t *entries,
                                 size_t count, hash_t *out_root);

#endif // MEM_MPT_H
