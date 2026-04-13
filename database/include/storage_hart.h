#ifndef STORAGE_HART_H
#define STORAGE_HART_H

/**
 * Storage Hart — mmap-backed per-account Adaptive Radix Trie for storage slots.
 *
 * Combines the ART trie structure from hashed_art.c with a shared mmap file
 * pool. Each account gets its own trie (arena region) within the shared file.
 * OS page cache manages hot/cold — no LRU, no eviction logic.
 *
 * Features:
 *   - O(1) read/write per slot (ART trie walk)
 *   - 32-byte hash cache per inner node (incremental root computation)
 *   - Per-account arena freelist (recycles deleted nodes)
 *   - Pool-level region freelist (recycles freed account arenas)
 *   - Zero-copy save: arena IS the persistence — just sync the file
 *   - Load: mmap the file, arenas are immediately usable
 */

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* =========================================================================
 * Pool — shared mmap file for all per-account storage arenas
 * ========================================================================= */

typedef struct storage_hart_pool storage_hart_pool_t;

/** Create a new pool file. Truncates if exists. */
storage_hart_pool_t *storage_hart_pool_create(const char *path);

/** Open an existing pool file. Returns NULL if not found/corrupt. */
storage_hart_pool_t *storage_hart_pool_open(const char *path);

/** Close and free. Does NOT remove the file. */
void storage_hart_pool_destroy(storage_hart_pool_t *pool);

/** Flush header to disk. */
void storage_hart_pool_sync(storage_hart_pool_t *pool);

/* =========================================================================
 * Per-account handle — lightweight, stored in resource_t
 * ========================================================================= */

typedef struct {
    uint64_t arena_offset;  /* byte offset of arena in pool file (0 = none) */
    uint64_t arena_used;    /* bytes used in arena */
    uint64_t arena_cap;     /* allocated capacity (bytes) */
    uint32_t count;         /* number of live entries (leaves) */
    uint32_t root_ref;      /* ref to root node within arena (0 = empty) */
    /* Per-type freelist heads (arena-relative offsets, 0 = empty) */
    uint32_t free_node4;
    uint32_t free_node16;
    uint32_t free_node48;
    uint32_t free_node256;
    uint32_t free_leaf;
} storage_hart_t;

#define STORAGE_HART_INIT {0}

/* =========================================================================
 * Operations
 * ========================================================================= */

/**
 * Get a storage slot value. Returns true if found.
 * key: 32-byte slot hash (keccak256 of slot index)
 * val: output 32-byte value (written only if found)
 */
bool storage_hart_get(const storage_hart_pool_t *pool,
                      const storage_hart_t *sh,
                      const uint8_t key[32], uint8_t val[32]);

/**
 * Pre-allocate arena for expected number of entries.
 * Call before bulk loading (e.g. state_load) to avoid repeated arena growth.
 * Estimates ~90 bytes per entry for the ART trie overhead.
 */
void storage_hart_reserve(storage_hart_pool_t *pool, storage_hart_t *sh,
                          uint32_t expected_entries);

/**
 * Set a storage slot value. Inserts or updates.
 * Returns true on success. May grow the arena (relocate in pool).
 */
bool storage_hart_put(storage_hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32], const uint8_t val[32]);

/**
 * Delete a storage slot. No-op if key doesn't exist.
 * Deleted node goes to per-account freelist.
 */
void storage_hart_del(storage_hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32]);

/**
 * Clear all storage for an account. Frees the arena region back to pool.
 * Resets sh to empty state.
 */
void storage_hart_clear(storage_hart_pool_t *pool, storage_hart_t *sh);

/* =========================================================================
 * Root computation (MPT hash)
 * ========================================================================= */

/**
 * Encode callback for leaf values (same signature as hart_encode_t).
 * Called for each leaf during root hash computation.
 */
typedef uint32_t (*storage_hart_encode_t)(const uint8_t key[32],
                                          const void *leaf_val,
                                          uint8_t *rlp_out, void *ctx);

/**
 * Compute MPT root hash. Walks only dirty paths (incremental).
 * Clean subtrees reuse cached hash. Same algorithm as hart_root_hash.
 */
void storage_hart_root_hash(storage_hart_pool_t *pool,
                            storage_hart_t *sh,
                            storage_hart_encode_t encode,
                            void *ctx, uint8_t out[32]);

/**
 * Mark all nodes dirty — forces full recomputation on next root_hash.
 */
void storage_hart_invalidate(storage_hart_pool_t *pool, storage_hart_t *sh);

/**
 * Mark the path to a specific key as dirty.
 */
void storage_hart_mark_dirty(storage_hart_pool_t *pool, storage_hart_t *sh,
                             const uint8_t key[32]);

/* =========================================================================
 * Iteration
 * ========================================================================= */

/**
 * Iterate all key-value pairs. Calls cb for each leaf.
 * Used by state_save to write inline entries.
 */
typedef bool (*storage_hart_iter_cb)(const uint8_t key[32],
                                     const uint8_t val[32], void *ctx);
void storage_hart_foreach(const storage_hart_pool_t *pool,
                          const storage_hart_t *sh,
                          storage_hart_iter_cb cb, void *ctx);

/**
 * Return the number of entries.
 */
static inline uint32_t storage_hart_count(const storage_hart_t *sh) {
    return sh ? sh->count : 0;
}

/**
 * Check if empty.
 */
static inline bool storage_hart_empty(const storage_hart_t *sh) {
    return !sh || sh->count == 0;
}

/* =========================================================================
 * Pool stats
 * ========================================================================= */

typedef struct {
    uint64_t data_size;       /* bytes allocated in pool */
    uint64_t free_bytes;      /* bytes on pool freelist */
    uint64_t file_size;       /* actual file size */
} storage_hart_pool_stats_t;

storage_hart_pool_stats_t storage_hart_pool_stats(const storage_hart_pool_t *pool);

#endif /* STORAGE_HART_H */
