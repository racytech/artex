#ifndef ART_MPT_H
#define ART_MPT_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "compact_art.h"
#include "art_iface.h"

/**
 * ART→MPT — Incremental Ethereum MPT root hash from compact_art.
 *
 * Wraps a compact_art tree (owned by flat_state) and computes MPT root
 * hashes incrementally: only dirty paths are rehashed, clean subtrees
 * return cached 32-byte hashes in O(1).
 *
 * Does NOT own the compact_art — just holds a pointer + side hash cache.
 *
 * Usage:
 *   art_mpt_t *am = art_mpt_create(tree, encoder, ctx);
 *   // ... mutate via art_mpt_insert / art_mpt_delete ...
 *   art_mpt_root_hash(am, root_out);  // incremental
 *   art_mpt_destroy(am);
 */

typedef struct art_mpt art_mpt_t;

/**
 * Callback to produce RLP-encoded value for an MPT leaf.
 * key:      32-byte leaf key.
 * leaf_val: pointer to compact_art leaf value data.
 * val_size: size of leaf value data.
 * rlp_out:  buffer to write RLP (max 1024 bytes).
 * Returns RLP length, or 0 on failure.
 */
typedef uint32_t (*art_mpt_value_encode_t)(const uint8_t *key,
                                            const void *leaf_val,
                                            uint32_t val_size,
                                            uint8_t *rlp_out,
                                            void *user_ctx);

/**
 * Create an art_mpt context over any ART tree via the abstract interface.
 * The tree is NOT owned — caller manages its lifetime.
 */
art_mpt_t *art_mpt_create_iface(art_iface_t iface,
                                  art_mpt_value_encode_t encode, void *ctx);

/**
 * Convenience: create over a compact_art tree (backward compatible).
 */
art_mpt_t *art_mpt_create(compact_art_t *tree,
                            art_mpt_value_encode_t encode, void *ctx);

/** Free the hash cache. Does NOT destroy the compact_art tree. */
void art_mpt_destroy(art_mpt_t *am);

/** Disable hash caching. Useful for small tries where cache overhead
 *  exceeds the benefit. Cache array is never allocated. */
void art_mpt_set_no_cache(art_mpt_t *am, bool disable);

/** Get hash cache memory usage (entries × sizeof(hash_entry_t)). */
size_t art_mpt_cache_bytes(const art_mpt_t *am);
size_t art_mpt_cache_cap(const art_mpt_t *am);

/**
 * Insert or update a key in the underlying compact_art, then mark
 * the path from root to key as dirty (invalidate cached hashes).
 */
bool art_mpt_insert(art_mpt_t *am, const uint8_t key[32],
                     const void *value, uint32_t value_size);

/**
 * Delete a key from the underlying compact_art, then mark
 * the path as dirty.
 */
bool art_mpt_delete(art_mpt_t *am, const uint8_t key[32]);

/**
 * Compute MPT root hash incrementally.
 * Only rehashes nodes whose subtree changed since the last call.
 * Clean subtrees return cached hashes in O(1).
 */
void art_mpt_root_hash(art_mpt_t *am, uint8_t out[32]);

/**
 * Invalidate all cached hashes. Forces full recomputation on next root_hash.
 * Use after external modifications to the compact_art (e.g., bulk load).
 */
void art_mpt_invalidate_all(art_mpt_t *am);

/** Stats */
typedef struct {
    uint64_t cache_hits;      /** Nodes with valid cached hash (skipped) */
    uint64_t cache_misses;    /** Nodes recomputed this root_hash call */
    uint64_t invalidations;   /** Nodes invalidated since last root_hash */
} art_mpt_stats_t;

art_mpt_stats_t art_mpt_get_stats(const art_mpt_t *am);
void art_mpt_reset_stats(art_mpt_t *am);

/**
 * Compute MPT root hash for a subtree identified by a key prefix.
 * The MPT is computed over the key suffix (key[prefix_len..key_size]).
 *
 * Enables per-account storage roots from a single 64-byte-keyed compact_art:
 *   prefix = addr_hash[32], MPT key = slot_hash[32].
 *
 * Uses the same incremental caching as art_mpt_root_hash.
 * Returns EMPTY_ROOT if the subtree is empty or prefix not found.
 */
void art_mpt_subtree_hash(art_mpt_t *am,
                            const uint8_t *prefix, uint32_t prefix_len,
                            uint8_t out[32]);

/**
 * Non-incremental: compute full MPT hash without persistent context.
 * Creates a temporary context, computes, destroys. For tests/one-shot use.
 */
void art_mpt_root_hash_full(const compact_art_t *tree,
                              art_mpt_value_encode_t encode, void *ctx,
                              uint8_t out[32]);

#endif /* ART_MPT_H */
