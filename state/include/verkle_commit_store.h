#ifndef VERKLE_COMMIT_STORE_H
#define VERKLE_COMMIT_STORE_H

#include "verkle.h"
#include "hash_store.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Commitment Store — hash_store-backed persistence for commitment points.
 *
 * Persists banderwagon commitment points so they survive restarts,
 * avoiding expensive full MSM recomputation on reload.
 *
 * Single hash_store with 32-byte keys:
 *   Leaf key:     [0x00 || stem (31B)]           → value: C1||C2||commitment (96B)
 *   Internal key: [depth+1 || path_prefix (31B)]  → value: commitment (32B)
 *
 * hash_store config: key_size=32, slot_size=144 (max_value=102, fits 96B leaf).
 */

typedef struct {
    hash_store_t *store;   /* owned */
} verkle_commit_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create a new commitment store in the given directory. */
verkle_commit_store_t *vcs_create(const char *dir, uint64_t shard_capacity);

/** Open an existing commitment store from directory. */
verkle_commit_store_t *vcs_open(const char *dir);

/** Close and free. Does not remove files. */
void vcs_destroy(verkle_commit_store_t *cs);

/* =========================================================================
 * Leaf Commitments (C1, C2, leaf commitment — 3 serialized points)
 * ========================================================================= */

/** Store leaf commitments for a given stem. */
bool vcs_put_leaf(verkle_commit_store_t *cs,
                  const uint8_t stem[31],
                  const banderwagon_point_t *c1,
                  const banderwagon_point_t *c2,
                  const banderwagon_point_t *commitment);

/** Load leaf commitments for a given stem. Returns false if not found. */
bool vcs_get_leaf(const verkle_commit_store_t *cs,
                  const uint8_t stem[31],
                  banderwagon_point_t *c1,
                  banderwagon_point_t *c2,
                  banderwagon_point_t *commitment);

/* =========================================================================
 * Internal Node Commitments (1 serialized point)
 * ========================================================================= */

/** Store internal node commitment. path_prefix is `depth` bytes long. */
bool vcs_put_internal(verkle_commit_store_t *cs,
                      int depth,
                      const uint8_t *path_prefix,
                      const banderwagon_point_t *commitment);

/** Load internal node commitment. Returns false if not found. */
bool vcs_get_internal(const verkle_commit_store_t *cs,
                      int depth,
                      const uint8_t *path_prefix,
                      banderwagon_point_t *commitment);

/* =========================================================================
 * Tree Flush
 * ========================================================================= */

/** Walk the in-memory tree and persist all commitments. */
bool vcs_flush_tree(verkle_commit_store_t *cs, const verkle_tree_t *vt);

/* =========================================================================
 * Durability
 * ========================================================================= */

/** Flush to disk (calls hash_store_sync). */
void vcs_sync(verkle_commit_store_t *cs);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_COMMIT_STORE_H */
