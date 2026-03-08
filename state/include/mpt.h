#ifndef MPT_H
#define MPT_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "hash.h"

/**
 * Merkle Patricia Trie — Batch Root Computation
 *
 * Computes the Ethereum MPT state root from a set of key-value pairs in one
 * pass. O(n log n) — sort + recursive trie build with stack-allocated RLP.
 * No ART intermediary, no per-insert rebuilds.
 *
 * Used to verify our EVM state against block headers during chain replay.
 */

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

#endif // MPT_H
