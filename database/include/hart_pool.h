#ifndef HART_POOL_H
#define HART_POOL_H

/**
 * hart_pool — single-pool slab allocator for hart-style tries.
 *
 * Single MAP_ANONYMOUS | MAP_PRIVATE region that hands out fixed-size slabs
 * (512 B, 1 KB, 2 KB, 4 KB, 8 KB). Per-account slab chains eliminate
 * relocation-on-grow: a hart that needs more space just appends a new slab;
 * old slabs are never moved. On account destruct, the whole slab chain
 * returns to the pool's size-class freelist for reuse.
 *
 * Individual ART nodes live inside slabs and are recycled via a per-size-class
 * freelist kept on each `storage_hart_t` (16 classes, 128 B step).
 *
 * Slot identity for SLOAD warming:
 *   Slabs are never moved. A node's ref is stable for the node's lifetime.
 *   However, warming-cache keys must still derive from (resource_idx,
 *   hashed_key), NOT from pool refs — because when a slot is deleted and a
 *   new slot is later created, the freelist may reuse the old ref value
 *   for the new slot.
 *
 * See docs/storage_hart_pool_design.md for rationale and benchmarks.
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* =========================================================================
 * Types
 * ========================================================================= */

typedef struct hart_pool hart_pool_t;

/* Byte offset into the pool. 0 reserved as NULL/"none". */
typedef uint64_t hart_pool_ref_t;

#define HART_POOL_REF_NULL ((hart_pool_ref_t)0)

/* Per-hart slab state (embedded in storage_hart_t / acct_index hart). */
typedef struct {
    hart_pool_ref_t  head_ref;     /* active slab — bump target */
    uint32_t         head_used;    /* bytes consumed in head slab */
    uint32_t         head_cap;     /* head slab capacity */
    hart_pool_ref_t  chain_ref;    /* previous closed slab (linked list) */
} hart_slab_t;

#define HART_SLAB_INIT {0, 0, 0, 0}

/* =========================================================================
 * Pool lifecycle
 * ========================================================================= */

/**
 * Create a fresh pool (MAP_ANONYMOUS | MAP_PRIVATE, no backing file).
 * Returns NULL on failure.
 */
hart_pool_t *hart_pool_create(void);

/** Close and free. */
void hart_pool_destroy(hart_pool_t *p);

/* =========================================================================
 * Allocation — slab-aware
 * ========================================================================= */

/**
 * Allocate `bytes` for the given hart's slab chain.
 * - Bumps inside the active slab if it fits.
 * - Otherwise closes the active slab (chained), reserves a new larger slab
 *   from the pool's size-class freelist or bump cursor, and bumps once.
 *
 * Returns HART_POOL_REF_NULL on failure. `*out_cap` receives the size
 * actually allocated (may be larger than `bytes` due to slab bumping).
 *
 * NOTE: this function allocates slab-internal space. For node allocations
 * that want freelist recycling, check `storage_hart_t.free_class[]` first;
 * only call here on freelist miss.
 */
hart_pool_ref_t hart_pool_alloc(hart_pool_t *p, hart_slab_t *slab,
                                 uint32_t bytes, uint32_t *out_cap);

/**
 * Free the entire slab chain of a hart (head + all chained slabs back onto
 * the pool's size-class freelist). Used on account destruct / storage clear.
 */
void hart_pool_free_slabs(hart_pool_t *p, hart_slab_t *slab);

/* =========================================================================
 * Pointer resolution
 * ========================================================================= */

/**
 * Resolve a ref to a stable pointer.
 *
 * Pointer validity:
 *   - Valid until the next call that may grow the pool
 *     (hart_pool_alloc may trigger mremap with MREMAP_MAYMOVE).
 *   - For short inner loops (e.g. ART walk), resolve once and reuse.
 *   - For code paths that allocate mid-walk, re-resolve after every alloc.
 */
void *hart_pool_ptr(const hart_pool_t *p, hart_pool_ref_t ref);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint64_t mapped;         /* virtual bytes currently mapped */
    uint64_t used;           /* bump cursor */
    uint64_t free_bytes;     /* bytes on slab-class freelists */
    uint32_t slab_count;     /* live slabs (excluding freelist) */
    uint32_t slabs_per_class[5];  /* live slabs, per size class */
    uint32_t free_per_class[5];   /* free slabs, per size class */
} hart_pool_stats_t;

hart_pool_stats_t hart_pool_stats(const hart_pool_t *p);

/* =========================================================================
 * Tunables
 * ========================================================================= */

/* Slab sizes: 5 geometric classes from 512 B to 8 KB. */
#define HART_SLAB_MIN             512U
#define HART_SLAB_MAX             8192U
#define HART_SLAB_CLASSES         5U   /* 512, 1K, 2K, 4K, 8K */
#define HART_SLAB_INITIAL         HART_SLAB_MIN

/* Intra-hart node freelist — lives in storage_hart_t.free_class[]. Used for
 * recycling ART nodes inside a slab chain. 128 B step, 16 classes covering
 * up to 2 KB. A freed node4 (~64 B) and a leaf alloc (~80 B) both land in
 * class 0, giving cross-type reuse within one account. */
#define HART_NODE_STEP            128U
#define HART_NODE_CLASSES         16U   /* 128, 256, ..., 2048 */

/* Pool grows in 64 MB steps via mremap(MREMAP_MAYMOVE). */
#define HART_POOL_INITIAL_MAP     (1ULL << 20)          /* 1 MB */
#define HART_POOL_GROW_STEP       (64ULL * 1024 * 1024) /* 64 MB */

#endif /* HART_POOL_H */
