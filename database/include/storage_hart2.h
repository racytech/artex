#ifndef STORAGE_HART2_H
#define STORAGE_HART2_H

/**
 * Storage Hart v2 — ART storage trie backed by hart_pool slab allocator.
 *
 * Same public API as storage_hart.h (v1), but the pool type is now
 * hart_pool_t (single MAP_ANONYMOUS allocator, slab-chain per hart) and
 * the per-hart state embeds hart_slab_t + a per-size-class intra-hart
 * freelist instead of arena + per-type freelists.
 *
 * Migration: include this header instead of storage_hart.h. Callers pass
 * hart_pool_t* where they used to pass storage_hart_pool_t*. All function
 * names are unchanged (storage_hart_get / put / del / clear / foreach /
 * root_hash / mark_dirty / invalidate / reserve / count / empty).
 *
 * Node refs are now 64-bit pool byte offsets. The high bit (0x8000...)
 * encodes the leaf flag. Internal only — not serialized.
 *
 * SLOAD warming invariant: refs are NOT stable identities across slot
 * delete+recreate. Warming cache keys must derive from (resource_idx,
 * hashed_key), not from refs.
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "hart_pool.h"

/* =========================================================================
 * Per-hart state — embedded in resource_t.
 * ========================================================================= */

/* Intra-hart node freelists — one head per node type (same scheme as v1).
 * Fixed-size-per-type avoids padding waste: a leaf is stored at its native
 * 64 bytes, a node4 at its native 72 bytes, etc. Cross-type reuse is lost
 * (a freed leaf can't serve a node4 alloc) in exchange for ~90 GB saved
 * on leaf padding at mainnet scale. Each freed slot stores the next ref
 * in its first 8 bytes. */
typedef struct {
    hart_slab_t       slab;        /* active slab + chain */
    uint64_t          root_ref;    /* ART root (pool ref, with leaf bit if leaf) */
    uint32_t          count;       /* live leaf count */
    uint32_t          _pad;
    hart_pool_ref_t   free_leaf;
    hart_pool_ref_t   free_node4;
    hart_pool_ref_t   free_node16;
    hart_pool_ref_t   free_node48;
    hart_pool_ref_t   free_node256;
} storage_hart_t;

#define STORAGE_HART_INIT {HART_SLAB_INIT, 0, 0, 0, 0, 0, 0, 0, 0}

/* =========================================================================
 * Operations — identical signatures to v1 (pool type changed).
 * ========================================================================= */

bool storage_hart_get(const hart_pool_t *pool,
                      const storage_hart_t *sh,
                      const uint8_t key[32], uint8_t val[32]);

void storage_hart_reserve(hart_pool_t *pool, storage_hart_t *sh,
                          uint32_t expected_entries);

bool storage_hart_put(hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32], const uint8_t val[32]);

void storage_hart_del(hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32]);

void storage_hart_clear(hart_pool_t *pool, storage_hart_t *sh);

/* =========================================================================
 * Root computation (MPT hash)
 * ========================================================================= */

typedef uint32_t (*storage_hart_encode_t)(const uint8_t key[32],
                                          const void *leaf_val,
                                          uint8_t *rlp_out, void *ctx);

void storage_hart_root_hash(hart_pool_t *pool,
                            storage_hart_t *sh,
                            storage_hart_encode_t encode,
                            void *ctx, uint8_t out[32]);

void storage_hart_invalidate(hart_pool_t *pool, storage_hart_t *sh);

void storage_hart_mark_dirty(hart_pool_t *pool, storage_hart_t *sh,
                             const uint8_t key[32]);

/* =========================================================================
 * Iteration
 * ========================================================================= */

typedef bool (*storage_hart_iter_cb)(const uint8_t key[32],
                                     const uint8_t val[32], void *ctx);

void storage_hart_foreach(const hart_pool_t *pool,
                          const storage_hart_t *sh,
                          storage_hart_iter_cb cb, void *ctx);

static inline uint32_t storage_hart_count(const storage_hart_t *sh) {
    return sh ? sh->count : 0;
}

static inline bool storage_hart_empty(const storage_hart_t *sh) {
    return !sh || sh->count == 0;
}

#endif /* STORAGE_HART2_H */
