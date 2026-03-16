#ifndef ART_EXECUTOR_VERKLE_BUILDER_H
#define ART_EXECUTOR_VERKLE_BUILDER_H

#include "state_history.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * Verkle Builder: background verkle state construction from block diffs
 *
 * Consumes block_diff_t from its own SPSC ring (independent of state_history).
 * Converts account/storage diffs to verkle keys via Pedersen hash,
 * then applies them to verkle_flat via begin_block/set/commit_block.
 *
 * Architecture:
 *   block_executor (producer) → SPSC ring → builder thread → verkle_flat
 *
 * The executor never blocks: if the ring is full, the diff is dropped.
 * ========================================================================= */

typedef struct verkle_builder verkle_builder_t;

/* ── Lifecycle ────────────────────────────────────────────────────────── */

/**
 * Create verkle builder. Opens/creates verkle_flat stores at the given dirs.
 * Spawns builder thread. Returns NULL on failure.
 *
 * @param value_dir   Directory for verkle_flat value store
 * @param commit_dir  Directory for verkle_flat commitment store
 */
verkle_builder_t *verkle_builder_create(const char *value_dir,
                                         const char *commit_dir);

/**
 * Open existing verkle builder (resumes from persisted verkle_flat state).
 */
verkle_builder_t *verkle_builder_open(const char *value_dir,
                                       const char *commit_dir);

/** Stop builder thread, flush remaining diffs, sync stores, free resources. */
void verkle_builder_destroy(verkle_builder_t *vb);

/* ── Producer API (called from block executor, non-blocking) ──────────── */

/**
 * Push a block diff to the builder's ring. Non-blocking: drops if full.
 * The builder takes ownership of diff->accounts and diff->storage arrays.
 */
void verkle_builder_push(verkle_builder_t *vb, const block_diff_t *diff);

/* ── Query API ────────────────────────────────────────────────────────── */

/** Get the last block number successfully committed to verkle. */
uint64_t verkle_builder_last_block(const verkle_builder_t *vb);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_VERKLE_BUILDER_H */
