#ifndef VERKLE_FLAT_H
#define VERKLE_FLAT_H

#include "verkle_commit_store.h"
#include "art_store.h"
#include "disk_hash.h"
#include "banderwagon.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle Flat Updater — Disk-Backed Commitment Updates
 *
 * Replaces the in-memory verkle tree for block execution.
 * RAM usage: O(block_changes) instead of O(total_state).
 *
 * Disk-backed stores (all disk_hash):
 *   - Value store:      disk_hash (key=32B, record=32B) — all state values
 *   - Commitment store: verkle_commit_store — leaf (C1,C2,commit) + internal
 *   - Slot store:       disk_hash — maps (depth,path,slot) → occupant stem
 *
 * Per-block flow:
 *   1. begin_block()
 *   2. set() calls buffer changes + record undo
 *   3. commit_block() groups by stem, updates commitments incrementally
 *   4. revert_block() restores old values + commitments from undo log
 */

/* =========================================================================
 * Data Structures
 * ========================================================================= */

/** Buffered change within a block. */
typedef struct {
    uint8_t key[32];
    uint8_t new_value[32];
} vf_change_t;

/** Value undo entry — records old value for revert. */
typedef struct {
    uint8_t key[32];
    uint8_t old_value[32];
    bool    had_value;
} vf_undo_t;

/** Commitment undo entry — records old commitment/slot store data for revert. */
typedef struct {
    uint8_t  cs_key[32];      /* store key (commit or slot format) */
    uint8_t  old_data[96];    /* max: 96 bytes for leaf, 32 for internal/slot */
    uint8_t  data_len;        /* 0 = didn't exist, 96 = leaf, 32 = internal/slot */
    uint8_t  store_id;        /* 0 = leaf, 1 = internal, 2 = slot */
} vf_commit_undo_t;

/** Block marker — tracks array offsets for revert. */
typedef struct {
    uint32_t change_start;
    uint32_t undo_start;
    uint32_t commit_undo_start;
    uint64_t block_number;
} vf_block_t;

/** Main handle. */
typedef struct {
    /* Stores (owned) */
    disk_hash_t             *value_store;
    verkle_commit_store_t   *commit_store;
    art_store_t             *slot_store;    /* (depth,path,slot) → occupant stem */

    /* Current block changes */
    vf_change_t  *changes;
    uint32_t      change_count;
    uint32_t      change_cap;

    /* Value undo log */
    vf_undo_t    *undos;
    uint32_t      undo_count;
    uint32_t      undo_cap;

    /* Commitment undo log */
    vf_commit_undo_t *commit_undos;
    uint32_t          cu_count;
    uint32_t          cu_cap;

    /* Block tracking */
    vf_block_t   *blocks;
    uint32_t      block_count;
    uint32_t      block_cap;
    bool          block_active;
} verkle_flat_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create a new flat updater with fresh stores. */
verkle_flat_t *verkle_flat_create(const char *value_dir,
                                  const char *commit_dir);

/** Open existing flat updater from persisted stores. */
verkle_flat_t *verkle_flat_open(const char *value_dir,
                                const char *commit_dir);

/** Destroy and free all resources. */
void verkle_flat_destroy(verkle_flat_t *vf);

/* =========================================================================
 * Block Operations
 * ========================================================================= */

/** Begin a new block. Must call before set(). */
bool verkle_flat_begin_block(verkle_flat_t *vf, uint64_t block_number);

/** Record a key-value set within the current block.
 *  Buffers the change + records old value in undo log. */
bool verkle_flat_set(verkle_flat_t *vf,
                     const uint8_t key[32],
                     const uint8_t value[32]);

/** Look up a key's current value.
 *  Checks in-flight changes first, then value store. */
bool verkle_flat_get(const verkle_flat_t *vf,
                     const uint8_t key[32],
                     uint8_t value[32]);

/** Commit the block: apply all changes, update commitments.
 *  Groups changes by stem, incremental update for existing leaves,
 *  full MSM for new leaves, bottom-up propagation for internals. */
bool verkle_flat_commit_block(verkle_flat_t *vf);

/** Revert the current (or most recent committed) block.
 *  Restores values + commitments from undo logs. */
bool verkle_flat_revert_block(verkle_flat_t *vf);

/** Discard undo entries for blocks up to (and including) up_to_block. */
void verkle_flat_trim(verkle_flat_t *vf, uint64_t up_to_block);

/* =========================================================================
 * Root
 * ========================================================================= */

/** Get the state root hash (serialized root commitment, 32 bytes). */
void verkle_flat_root_hash(const verkle_flat_t *vf, uint8_t out[32]);

/* =========================================================================
 * Durability
 * ========================================================================= */

/** Flush all stores to disk. */
void verkle_flat_sync(verkle_flat_t *vf);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_FLAT_H */
