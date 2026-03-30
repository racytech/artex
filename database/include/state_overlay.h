#ifndef STATE_OVERLAY_H
#define STATE_OVERLAY_H

#include <stdint.h>
#include <stdbool.h>
#include "uint256.h"
#include "address.h"
#include "hash.h"

/**
 * State Overlay — unified in-memory state backed by flat_store.
 *
 * Replaces mem_art + flat_state with a single layer:
 *   - Account/storage lookups go through flat_store's overlay (in-memory)
 *   - Writes create/update overlay entries (dirty-tracked)
 *   - Journal for snapshot/revert (EVM call depth)
 *   - Per-tx commit (EIP-2200 originals, flag clearing)
 *   - Dirty iteration for checkpoint (compute_mpt_root)
 *
 * Thread safety: NONE. Single-threaded EVM execution only.
 */

typedef struct state_overlay state_overlay_t;
typedef struct flat_state flat_state_t;
typedef struct code_store code_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_overlay_t *state_overlay_create(flat_state_t *fs, code_store_t *cs);
void             state_overlay_destroy(state_overlay_t *so);

/** Set or change the flat_state backend. Creates/destroys tries as needed. */
void             state_overlay_set_flat_state(state_overlay_t *so, flat_state_t *fs);

/* =========================================================================
 * Account — read
 * ========================================================================= */

bool      state_overlay_exists(state_overlay_t *so, const address_t *addr);
bool      state_overlay_is_empty(state_overlay_t *so, const address_t *addr);
uint64_t  state_overlay_get_nonce(state_overlay_t *so, const address_t *addr);
uint256_t state_overlay_get_balance(state_overlay_t *so, const address_t *addr);
hash_t    state_overlay_get_code_hash(state_overlay_t *so, const address_t *addr);
uint32_t  state_overlay_get_code_size(state_overlay_t *so, const address_t *addr);
bool      state_overlay_get_code(state_overlay_t *so, const address_t *addr,
                                  uint8_t *out, uint32_t *out_len);
const uint8_t *state_overlay_get_code_ptr(state_overlay_t *so, const address_t *addr,
                                           uint32_t *out_len);

/* =========================================================================
 * Account — write (journaled)
 * ========================================================================= */

void state_overlay_set_nonce(state_overlay_t *so, const address_t *addr, uint64_t nonce);
void state_overlay_set_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *balance);
void state_overlay_add_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *amount);
bool state_overlay_sub_balance(state_overlay_t *so, const address_t *addr,
                                const uint256_t *amount);
void state_overlay_set_code(state_overlay_t *so, const address_t *addr,
                             const uint8_t *code, uint32_t len);
void state_overlay_set_code_hash(state_overlay_t *so, const address_t *addr,
                                  const hash_t *code_hash);

/* =========================================================================
 * Storage (journaled)
 * ========================================================================= */

uint256_t state_overlay_get_storage(state_overlay_t *so, const address_t *addr,
                                     const uint256_t *key);
uint256_t state_overlay_get_committed_storage(state_overlay_t *so, const address_t *addr,
                                               const uint256_t *key);
void      state_overlay_set_storage(state_overlay_t *so, const address_t *addr,
                                     const uint256_t *key, const uint256_t *value);

/** Combined SLOAD: load + warm check in one pass. */
uint256_t state_overlay_sload(state_overlay_t *so, const address_t *addr,
                               const uint256_t *key, bool *was_warm);
/** Combined SSTORE lookup: load + warm check + current/original in one pass. */
void      state_overlay_sstore_lookup(state_overlay_t *so, const address_t *addr,
                                       const uint256_t *key,
                                       uint256_t *current, uint256_t *original,
                                       bool *was_warm);

/* =========================================================================
 * Account lifecycle (journaled)
 * ========================================================================= */

void state_overlay_create_account(state_overlay_t *so, const address_t *addr);
void state_overlay_self_destruct(state_overlay_t *so, const address_t *addr);
void state_overlay_mark_existed(state_overlay_t *so, const address_t *addr);

bool state_overlay_is_self_destructed(state_overlay_t *so, const address_t *addr);
bool state_overlay_is_created(state_overlay_t *so, const address_t *addr);
bool state_overlay_has_storage(state_overlay_t *so, const address_t *addr);

/* =========================================================================
 * Access lists (EIP-2929)
 * ========================================================================= */

bool state_overlay_warm_address(state_overlay_t *so, const address_t *addr);
bool state_overlay_warm_slot(state_overlay_t *so, const address_t *addr,
                              const uint256_t *key);
bool state_overlay_is_address_warm(const state_overlay_t *so, const address_t *addr);
bool state_overlay_is_slot_warm(const state_overlay_t *so, const address_t *addr,
                                 const uint256_t *key);

/* =========================================================================
 * Transient storage (EIP-1153)
 * ========================================================================= */

uint256_t state_overlay_tload(state_overlay_t *so, const address_t *addr,
                               const uint256_t *key);
void      state_overlay_tstore(state_overlay_t *so, const address_t *addr,
                                const uint256_t *key, const uint256_t *value);

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

uint32_t state_overlay_snapshot(state_overlay_t *so);
void     state_overlay_revert(state_overlay_t *so, uint32_t snap_id);

/* =========================================================================
 * Commit
 * ========================================================================= */

/** Per-tx commit: set originals, clear tx flags, handle self-destruct/CREATE. */
void state_overlay_commit_tx(state_overlay_t *so);

/** Per-block commit: clear block flags, set EIP-2200 originals. */
void state_overlay_commit(state_overlay_t *so);

/** Begin a new block. */
void state_overlay_begin_block(state_overlay_t *so, uint64_t block_number);

/** Set EIP-161 pruning mode. */
void state_overlay_set_prune_empty(state_overlay_t *so, bool enabled);

/* =========================================================================
 * Checkpoint (root computation support)
 * ========================================================================= */

/**
 * Flush dirty state to flat_store and compute MPT root.
 * This is the single function that replaces the 7-step compute_mpt_root.
 */
hash_t state_overlay_compute_mpt_root(state_overlay_t *so, bool prune_empty);

/** Evict clean overlay entries to free memory. */
void state_overlay_evict(state_overlay_t *so);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    size_t   overlay_accounts;
    size_t   overlay_slots;
    uint64_t flat_acct_count;
    uint64_t flat_stor_count;
    size_t   flat_acct_mem;
    size_t   flat_stor_mem;
    double   root_stor_ms;
    double   root_acct_ms;
    size_t   root_dirty_count;
} state_overlay_stats_t;

state_overlay_stats_t state_overlay_get_stats(const state_overlay_t *so);

/* =========================================================================
 * History diff support
 * ========================================================================= */

#ifdef ENABLE_HISTORY
struct block_diff_t;
void state_overlay_collect_block_diff(state_overlay_t *so, struct block_diff_t *out);
void state_overlay_apply_diff_bulk(state_overlay_t *so, const struct block_diff_t *diff);
#endif

/* =========================================================================
 * Debug / Dump
 * ========================================================================= */

void state_overlay_dump_alloc_json(state_overlay_t *so, const char *path);
size_t state_overlay_collect_addresses(state_overlay_t *so, address_t *out, size_t max);
size_t state_overlay_collect_storage_keys(state_overlay_t *so, const address_t *addr,
                                           uint256_t *out, size_t max);

#endif /* STATE_OVERLAY_H */
