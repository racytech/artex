#ifndef VERKLE_COMMIT_STORE_H
#define VERKLE_COMMIT_STORE_H

#include "disk_table.h"
#include "banderwagon.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Commitment Store — mmap'd disk_table persistence for commitment points.
 *
 * Persists banderwagon commitment points so they survive restarts,
 * avoiding expensive full MSM recomputation on reload.
 * Uses disk_table (mmap'd hash table) for O(1) lookups with no syscall overhead.
 *
 * Two disk_table tables with 32-byte keys:
 *   Leaf store:     key=[0x00 || stem (31B)]          → record: C1||C2||commitment (96B)
 *   Internal store: key=[depth+1 || path_prefix (31B)] → record: commitment (32B)
 *
 * Files created inside the given directory:
 *   dir/leaves.idx    — leaf disk_table (key=32, record=96)
 *   dir/internals.idx — internal disk_table (key=32, record=32)
 */

typedef struct {
    disk_table_t *leaf_store;      /* key=32, record=96 (C1+C2+commitment) */
    disk_table_t *internal_store;  /* key=32, record=32 (commitment) */
} verkle_commit_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create a new commitment store in the given directory. */
verkle_commit_store_t *vcs_create(const char *dir);

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
 * Durability
 * ========================================================================= */

/** Flush to disk (calls disk_table_sync on both stores). */
void vcs_sync(verkle_commit_store_t *cs);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_COMMIT_STORE_H */
