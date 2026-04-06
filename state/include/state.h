#ifndef STATE_H
#define STATE_H

/**
 * State — single in-memory Ethereum world state.
 *
 * All accounts in a flat vector indexed by hart (hashed ART).
 * Per-account storage in separate harts.
 * Account trie for MPT root computation.
 * Journal for snapshot/revert.
 * No disk on hot path.
 */

#include "account.h"
#include "mem_art.h"
#include "hashed_art.h"
#include "hash.h"
#include "address.h"
#include "uint256.h"
#include <stdint.h>
#include <stdbool.h>

typedef struct code_store code_store_t;
typedef struct state state_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

state_t *state_create(code_store_t *cs);
void     state_destroy(state_t *s);

/* Load/save full state to file (graceful shutdown / restart).
 * state_save stores the given state_root in the header.
 * state_load returns the stored root via out_root (if non-NULL). */
bool state_load(state_t *s, const char *path, hash_t *out_root);
bool state_save(const state_t *s, const char *path, const hash_t *state_root);

/* =========================================================================
 * Account access
 * ========================================================================= */

/* Get or create account. Touches last_access_block. */
account_t *state_get_account(state_t *s, const address_t *addr);

bool     state_exists(state_t *s, const address_t *addr);
bool     state_is_empty(state_t *s, const address_t *addr);
uint64_t state_get_nonce(state_t *s, const address_t *addr);
uint256_t state_get_balance(state_t *s, const address_t *addr);

void state_set_nonce(state_t *s, const address_t *addr, uint64_t nonce);
void state_set_balance(state_t *s, const address_t *addr, const uint256_t *bal);
void state_add_balance(state_t *s, const address_t *addr, const uint256_t *amount);
bool state_sub_balance(state_t *s, const address_t *addr, const uint256_t *amount);

/* =========================================================================
 * Code
 * ========================================================================= */

void state_set_code(state_t *s, const address_t *addr,
                    const uint8_t *code, uint32_t len);
const uint8_t *state_get_code(state_t *s, const address_t *addr, uint32_t *out_len);
uint32_t state_get_code_size(state_t *s, const address_t *addr);
hash_t   state_get_code_hash(state_t *s, const address_t *addr);

/* =========================================================================
 * Storage
 * ========================================================================= */

uint256_t state_get_storage(state_t *s, const address_t *addr, const uint256_t *key);
void      state_set_storage(state_t *s, const address_t *addr,
                            const uint256_t *key, const uint256_t *value);
bool      state_has_storage(state_t *s, const address_t *addr);

/* SLOAD/SSTORE combined lookups (EIP-2200 gas calculation) */
uint256_t state_sload(state_t *s, const address_t *addr,
                      const uint256_t *key, bool *was_warm);
void      state_sstore_lookup(state_t *s, const address_t *addr,
                              const uint256_t *key,
                              uint256_t *current, uint256_t *original,
                              bool *was_warm);

/* =========================================================================
 * Account lifecycle
 * ========================================================================= */

void state_create_account(state_t *s, const address_t *addr);
void state_self_destruct(state_t *s, const address_t *addr);

/* =========================================================================
 * Snapshot / Revert
 * ========================================================================= */

uint32_t state_snapshot(state_t *s);
void     state_revert(state_t *s, uint32_t snap);

/* =========================================================================
 * Transaction / Block commit
 * ========================================================================= */

/* Per-tx: process self-destructs, clear warm sets, reset tx flags */
void state_commit_tx(state_t *s);

/* Per-block: reset block flags, clear originals */
void state_commit_block(state_t *s);

/* Set current block number (for LRU tracking) */
void state_begin_block(state_t *s, uint64_t block_number);
uint64_t state_get_block(const state_t *s);

/* EIP-161: enable empty account pruning post-Spurious Dragon */
void state_set_prune_empty(state_t *s, bool enabled);

/* Clear all dirty flags from prestate setup (for test runner) */
void state_clear_prestate_dirty(state_t *s);

/* =========================================================================
 * EIP-2929 warm/cold access
 * ========================================================================= */

void state_mark_addr_warm(state_t *s, const address_t *addr);
bool state_is_addr_warm(const state_t *s, const address_t *addr);
void state_mark_storage_warm(state_t *s, const address_t *addr, const uint256_t *key);
bool state_is_storage_warm(const state_t *s, const address_t *addr, const uint256_t *key);

/* =========================================================================
 * EIP-1153 transient storage
 * ========================================================================= */

uint256_t state_tload(state_t *s, const address_t *addr, const uint256_t *key);
void      state_tstore(state_t *s, const address_t *addr,
                       const uint256_t *key, const uint256_t *value);

/* =========================================================================
 * MPT root computation (per-block checkpoint)
 * ========================================================================= */

hash_t state_compute_root(state_t *s, bool prune_empty);
hash_t state_compute_root_ex(state_t *s, bool prune_empty, bool compute_hash);
void   state_finalize_block(state_t *s, bool prune_empty);

/* Force full recomputation on next compute_root — invalidates all cached
 * hashes in account index and all storage harts. Use at snapshot time. */
void   state_invalidate_all(state_t *s);

/* Number of tracked dead accounts (phantoms + destructed + pruned). */
uint32_t state_dead_count(const state_t *s);

/* dump-prestate: access tracking + collection.
 * Enable tracking before the target block, then collect after execution. */
void   state_enable_access_tracking(state_t *s);
void   state_disable_access_tracking(state_t *s);
size_t state_collect_dirty_addresses(const state_t *s, address_t *out, size_t max);
size_t state_collect_accessed_storage_keys(const state_t *s,
                                           const address_t *addr,
                                           uint256_t *out, size_t max);

/* Compact: rebuild accounts vector and acct_index with only EXISTED accounts.
 * Eliminates dead/phantom entries and reclaims mem_art arena space.
 * Call at checkpoint boundaries when blk_dirty is empty. */
void state_compact(state_t *s);

/* Cold storage eviction — evict inactive storage harts to disk. */
void     state_set_evict_path(state_t *s, const char *dir);
void     state_set_evict_threshold(state_t *s, uint64_t blocks);
void     state_set_evict_budget(state_t *s, size_t bytes);
uint32_t state_evict_cold_storage(state_t *s);
void     state_compact_evict_file(state_t *s);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint32_t account_count;
    uint32_t storage_account_count;
    uint32_t stor_in_memory;         /* storage harts currently in RAM */
    uint32_t stor_evicted;           /* storage harts on disk (eviction file) */
    /* Memory breakdown */
    size_t   acct_vec_bytes;         /* accounts vector: count * 80 */
    size_t   res_vec_bytes;          /* resource vector: res_count * 104 */
    size_t   acct_arena_bytes;       /* acct_index hart arena (includes hash cache) */
    size_t   stor_arena_bytes;       /* all storage hart arenas total */
    size_t   total_tracked;          /* sum of above */
} state_stats_t;

state_stats_t state_get_stats(const state_t *s);

#endif /* STATE_H */
