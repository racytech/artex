#ifndef STATE_H
#define STATE_H

/**
 * State — single in-memory Ethereum world state.
 *
 * All accounts in a flat vector indexed by mem_art.
 * Per-account storage in separate mem_arts.
 * Account trie for MPT root computation.
 * Journal for snapshot/revert.
 * No disk on hot path.
 */

#include "account.h"
#include "mem_art.h"
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

/* Load/save full state to file (graceful shutdown / restart) */
bool state_load(state_t *s, const char *path);
bool state_save(const state_t *s, const char *path);

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

/* Number of tracked dead accounts (phantoms + destructed + pruned). */
uint32_t state_dead_count(const state_t *s);

/* Compact: rebuild accounts vector and acct_index with only EXISTED accounts.
 * Eliminates dead/phantom entries and reclaims mem_art arena space.
 * Call at checkpoint boundaries when blk_dirty is empty. */
void state_compact(state_t *s);

/* =========================================================================
 * Stats
 * ========================================================================= */

typedef struct {
    uint32_t account_count;
    uint32_t storage_account_count;  /* accounts with storage mem_art */
    size_t   memory_used;            /* approximate RSS contribution */
    size_t   arena_used;             /* acct_index arena bytes in use */
    size_t   arena_cap;              /* acct_index arena capacity */
    size_t   mpt_cache_bytes;        /* acct_index art_mpt hash cache size */
    size_t   mpt_cache_cap;          /* acct_index art_mpt hash cache entries */
} state_stats_t;

state_stats_t state_get_stats(const state_t *s);

#endif /* STATE_H */
