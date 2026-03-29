#ifndef MPT_TRIE_H
#define MPT_TRIE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * MPT Trie — In-Memory Merkle Patricia Trie (structure only).
 *
 * Native tree with direct child pointers. No serialization during
 * trie walks — nodes are C structs linked by pointers.
 *
 * Leaf nodes store only the VALUE HASH (32 bytes), not the actual
 * value data. Values live in flat_state. At checkpoint time the
 * caller provides the RLP-encoded value for each dirty leaf so the
 * trie can compute hashes bottom-up.
 *
 * Lifecycle per checkpoint:
 *   1. mpt_trie_insert(key, value_rlp, len) for each dirty key
 *      — inserts/updates the leaf, stores value hash, marks path dirty
 *   2. mpt_trie_remove(key) for deleted keys
 *   3. mpt_trie_root_hash(out) — walks dirty paths bottom-up,
 *      computes keccak hashes, returns 32-byte state root
 *
 * Shared mode: multiple tries (per-account storage) share structure
 * via copy-on-write. Modifying a shared node creates a private copy.
 *
 * No compact_art, no arena, no RLP storage. Just pointers.
 */

typedef struct mpt_trie mpt_trie_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create empty trie. Returns NULL on failure. */
mpt_trie_t *mpt_trie_create(void);

/** Free all nodes. */
void mpt_trie_destroy(mpt_trie_t *t);

/** Reset to empty. Faster than destroy + create. */
void mpt_trie_reset(mpt_trie_t *t);

/* =========================================================================
 * Mutations
 * ========================================================================= */

/**
 * Insert or update a leaf.
 * key:       32-byte trie key (keccak256 of address or slot).
 * value_rlp: RLP-encoded leaf value. Trie stores keccak256(value_rlp)
 *            internally — the data itself is NOT kept.
 * value_len: length of value_rlp.
 *
 * Restructures the trie immediately (no batching needed).
 * Marks affected path as dirty for next root_hash computation.
 * Returns true on success.
 */
bool mpt_trie_insert(mpt_trie_t *t, const uint8_t key[32],
                      const uint8_t *value_rlp, size_t value_len);

/**
 * Remove a leaf. No-op if key doesn't exist.
 * Restructures trie (collapse branch→extension, etc).
 * Marks path dirty. Returns true on success.
 */
bool mpt_trie_remove(mpt_trie_t *t, const uint8_t key[32]);

/* =========================================================================
 * Root Hash
 * ========================================================================= */

/**
 * Compute the Merkle root hash.
 * Walks only dirty paths — clean subtrees use cached hashes.
 * O(dirty_nodes * depth), not O(total_nodes).
 *
 * After this call, all nodes are marked clean until the next mutation.
 */
void mpt_trie_root_hash(mpt_trie_t *t, uint8_t out[32]);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint64_t node_count;      /** Total nodes in trie */
    uint64_t leaf_count;      /** Leaf nodes only */
    uint64_t dirty_count;     /** Nodes needing rehash */
    uint64_t memory_bytes;    /** Approximate heap usage */
} mpt_trie_stats_t;

mpt_trie_stats_t mpt_trie_stats(const mpt_trie_t *t);

#endif /* MPT_TRIE_H */
