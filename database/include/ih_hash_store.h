#ifndef IH_HASH_STORE_H
#define IH_HASH_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "hash.h"
#include "intermediate_hashes.h"  /* ih_cursor_t */

/**
 * Intermediate Hash Table — hash_store Backend
 *
 * Same algorithm as intermediate_hashes.c (Erigon-style MPT commitment)
 * but backed by hash_store instead of compact_art.
 *
 * Advantages over compact_art backend:
 *   - Zero RAM (mmap'd file-backed, only hot pages resident)
 *   - Persistent (survives restart, no rebuild needed on clean shutdown)
 *   - Unified sync path (hash_store_sync alongside data stores)
 *
 * Trade-off: no ordered iteration, so stale entries from structural
 * changes (extension splits, branch collapses) remain in the store.
 * They are unreachable (parent child_depths always reflects current
 * structure) and waste only disk space.
 */

typedef struct ihs_state ihs_state_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create a new IH store in the given directory. Returns NULL on failure. */
ihs_state_t *ihs_create(const char *dir);

/** Open an existing IH store from directory. Returns NULL on failure. */
ihs_state_t *ihs_open(const char *dir);

/** Close hash_store and free all resources. */
void ihs_destroy(ihs_state_t *ih);

/** Flush hash_store to disk (msync). */
void ihs_sync(ihs_state_t *ih);

/* =========================================================================
 * Full Build
 * ========================================================================= */

/**
 * Build the full MPT from sorted 32-byte keys and raw values.
 * Clears any previous state before building.
 */
hash_t ihs_build(ihs_state_t *ih,
                  const uint8_t *const *keys,
                  const uint8_t *const *values,
                  const uint16_t *value_lens,
                  size_t count);

/**
 * Build MPT from variable-length keys (for Ethereum test vectors).
 */
hash_t ihs_build_varlen(ihs_state_t *ih,
                         const uint8_t *const *keys,
                         const size_t *key_lens,
                         const uint8_t *const *values,
                         const size_t *value_lens,
                         size_t count);

/* =========================================================================
 * Incremental Update
 * ========================================================================= */

/**
 * Incremental MPT update for a block's dirty keys.
 * Uses ih_cursor_t (defined in intermediate_hashes.h) for data access.
 */
hash_t ihs_update(ihs_state_t *ih,
                   const uint8_t *const *dirty_keys,
                   const uint8_t *const *dirty_vals,
                   const size_t *dirty_vlens,
                   size_t count,
                   ih_cursor_t *cursor);

/* =========================================================================
 * Accessors
 * ========================================================================= */

/** Get current root hash (no recomputation). */
hash_t ihs_root(const ihs_state_t *ih);

/** Number of intermediate hash entries stored. */
size_t ihs_entry_count(const ihs_state_t *ih);

#endif /* IH_HASH_STORE_H */
