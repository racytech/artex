#ifndef EVM_STATE_H
#define EVM_STATE_H

#include <stdint.h>
#include <stdbool.h>
#include "uint256.h"
#include "address.h"
#include "hash.h"

/* Forward declaration — code_store lifecycle is managed by caller */
typedef struct code_store code_store_t;

/**
 * EVM State — Typed, in-memory state interface backed by flat_state.
 *
 * Architecture:
 *   - Account cache (mem_art by address_t): load-on-demand from flat_state
 *   - Storage cache (mem_art by addr[20]+slot[32]): load-on-demand
 *   - Journal (dynamic array): snapshot = journal position, revert = undo
 *   - Access lists (mem_art sets): EIP-2929 warm/cold tracking
 *   - Transient storage (mem_art): EIP-1153 per-transaction storage
 *
 * Lifecycle:
 *   1. evm_state_create() — create for block execution
 *   2. Execute transactions (get/set nonce, balance, storage, etc.)
 *   3. Use snapshot/revert for transaction boundaries
 *   4. evm_state_compute_mpt_root(es) — compute state root
 *   5. evm_state_destroy(es) — free in-memory caches
 *
 * Opaque handle — struct defined in evm_state.c.
 */

typedef struct evm_state evm_state_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create EVM state.
 * cs: if non-NULL, enables read-through for contract bytecode (not owned).
 */
evm_state_t *evm_state_create(code_store_t *cs);

/** Destroy EVM state and free all in-memory caches. */
void evm_state_destroy(evm_state_t *es);

/**
 * Mark state to discard pending writes on destroy.
 */
void evm_state_discard_pending(evm_state_t *es);

/**
 * Flush deferred MPT writes to disk. Call at checkpoint time.
 */
void evm_state_flush(evm_state_t *es);

/**
 * Enable/disable batch mode. In batch mode, per-block verkle flush
 * is skipped — block_dirty flags accumulate across blocks.
 * Call evm_state_flush_verkle() at checkpoint time to flush.
 */
void evm_state_set_batch_mode(evm_state_t *es, bool enabled);

/** EIP-161: when enabled, touched empty accounts are pruned from state. */
void evm_state_set_prune_empty(evm_state_t *es, bool enabled);

/** Set flat state backend for O(1) disk-backed lookups (optional, not owned). */
typedef struct flat_state flat_state_t;
void evm_state_set_flat_state(evm_state_t *es, flat_state_t *fs);
flat_state_t *evm_state_get_flat_state(const evm_state_t *es);

/**
 * Flush accumulated block-dirty state to verkle backing store.
 * Call at checkpoint boundaries when in batch mode.
 * Clears block_dirty flags after flush.
 */
void evm_state_flush_verkle(evm_state_t *es);

/**
 * Evict all cached accounts and storage slots.
 * Call ONLY after compute_mpt_root / flush_verkle (all dirty flags cleared,
 * data on disk). Read-through cache will reload entries on demand.
 */
void evm_state_evict_cache(evm_state_t *es);

// ============================================================================
// Account Existence
// ============================================================================

/** Prefetch account ART path into CPU cache (non-blocking hint). */
void evm_state_prefetch_account(evm_state_t *es, const address_t *addr);

/** Check if account exists (has been touched or loaded from disk). */
bool evm_state_exists(evm_state_t *es, const address_t *addr);

/** EIP-161: account is empty if nonce=0, balance=0, no code. */
bool evm_state_is_empty(evm_state_t *es, const address_t *addr);

// ============================================================================
// Nonce
// ============================================================================

uint64_t evm_state_get_nonce(evm_state_t *es, const address_t *addr);
void     evm_state_set_nonce(evm_state_t *es, const address_t *addr, uint64_t nonce);
void     evm_state_increment_nonce(evm_state_t *es, const address_t *addr);

// ============================================================================
// Balance
// ============================================================================

uint256_t evm_state_get_balance(evm_state_t *es, const address_t *addr);
void      evm_state_set_balance(evm_state_t *es, const address_t *addr,
                                const uint256_t *balance);
void      evm_state_add_balance(evm_state_t *es, const address_t *addr,
                                const uint256_t *amount);
/** Returns false if insufficient balance (no change made). */
bool      evm_state_sub_balance(evm_state_t *es, const address_t *addr,
                                const uint256_t *amount);

// ============================================================================
// Code
// ============================================================================

hash_t   evm_state_get_code_hash(evm_state_t *es, const address_t *addr);
/** Set code_hash directly (for state reconstruction from history diffs). */
void     evm_state_set_code_hash(evm_state_t *es, const address_t *addr,
                                  const hash_t *code_hash);
uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr);
bool     evm_state_get_code(evm_state_t *es, const address_t *addr,
                            uint8_t *out, uint32_t *out_len);
/** Return pointer to cached code bytes (valid for lifetime of evm_state). */
const uint8_t *evm_state_get_code_ptr(evm_state_t *es, const address_t *addr,
                                       uint32_t *out_len);
void     evm_state_set_code(evm_state_t *es, const address_t *addr,
                            const uint8_t *code, uint32_t len);

// ============================================================================
// Storage
// ============================================================================

uint256_t evm_state_get_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key);
/** Get committed (original/pre-transaction) storage value. For EIP-2200. */
uint256_t evm_state_get_committed_storage(evm_state_t *es, const address_t *addr,
                                          const uint256_t *key);
/** Combined SLOAD: ensure_slot + warm check in one pass. */
uint256_t evm_state_sload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key, bool *was_warm);
/** Combined SSTORE lookup: ensure_slot + warm check + current/original in one pass. */
void      evm_state_sstore_lookup(evm_state_t *es, const address_t *addr,
                                   const uint256_t *key,
                                   uint256_t *current, uint256_t *original,
                                   bool *was_warm);
void      evm_state_set_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key, const uint256_t *value);

/** EIP-7610: Check if account has any non-zero storage (for CREATE collision detection). */
bool evm_state_has_storage(evm_state_t *es, const address_t *addr);

// ============================================================================
// Account Creation (CREATE / CREATE2)
// ============================================================================

/** Create or reset account: nonce=0, balance=0, no code, clear storage. */
void evm_state_create_account(evm_state_t *es, const address_t *addr);
void evm_state_mark_existed(evm_state_t *es, const address_t *addr);
void evm_state_clear_prestate_dirty(evm_state_t *es);

// ============================================================================
// Self-Destruct
// ============================================================================

void evm_state_self_destruct(evm_state_t *es, const address_t *addr);
bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr);

/** EIP-6780: Check if account was created in current transaction. */
bool evm_state_is_created(evm_state_t *es, const address_t *addr);

// ============================================================================
// Snapshot / Revert
// ============================================================================

/**
 * Commit current state as the "original" state for EIP-2200.
 * Call after pre-state setup, before transaction execution.
 * Sets original = current for all cached storage slots and clears the journal.
 */
void evm_state_commit(evm_state_t *es);

/**
 * Per-transaction commit for block execution.
 * Call after each transaction_execute() in a block:
 *   - Processes self-destructed accounts (zeros balance/nonce/code/storage)
 *   - Commits remaining accounts (existed=true, clears created/dirty flags)
 *   - Sets original = current for storage (EIP-2200)
 *   - Clears journal, access lists, and transient storage
 */
void evm_state_commit_tx(evm_state_t *es);

/**
 * Begin a new block.
 * Resets per-block witness gas state (EIP-4762) and opens a block
 * in the backing verkle store (required for flat backend).
 */
void evm_state_begin_block(evm_state_t *es, uint64_t block_number);

/** Take snapshot. Returns journal position for later revert. */
uint32_t evm_state_snapshot(evm_state_t *es);

/** Revert to snapshot. Undoes all changes since snap_id. */
void evm_state_revert(evm_state_t *es, uint32_t snap_id);

// ============================================================================
// Access Lists (EIP-2929)
// ============================================================================

/** Mark address as warm. Returns true if it was already warm. */
bool evm_state_warm_address(evm_state_t *es, const address_t *addr);

/** Mark storage slot as warm. Returns true if it was already warm. */
bool evm_state_warm_slot(evm_state_t *es, const address_t *addr,
                         const uint256_t *key);

bool evm_state_is_address_warm(const evm_state_t *es, const address_t *addr);
bool evm_state_is_slot_warm(const evm_state_t *es, const address_t *addr,
                            const uint256_t *key);

// ============================================================================
// Transient Storage (EIP-1153, Cancun+)
// ============================================================================

uint256_t evm_state_tload(evm_state_t *es, const address_t *addr,
                           const uint256_t *key);
void      evm_state_tstore(evm_state_t *es, const address_t *addr,
                            const uint256_t *key, const uint256_t *value);

// ============================================================================
// Finalize
// ============================================================================

/**
 * Flush all dirty state to backing store.
 * - Writes dirty account fields (nonce, balance)
 * - Writes dirty storage slots
 * - Writes new code
 * - Zeros self-destructed accounts
 */
bool evm_state_finalize(evm_state_t *es);

// ============================================================================
// State Root
// ============================================================================

/**
 * Compute MPT (Merkle Patricia Trie) state root from flat_state.
 * @param prune_empty  If true (EIP-161+), exclude empty accounts from the trie.
 */
hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty);
hash_t evm_state_compute_mpt_root(evm_state_t *es, bool prune_empty);

/**
 * Prune ALL empty accounts from flat_state.
 * Empty = nonce 0, balance 0, no code, no storage.
 * Called at EIP-161 transition. Can also be called for maintenance.
 */
void evm_state_prune_empty_accounts(evm_state_t *es);
// ============================================================================
// Cache / Store Statistics
// ============================================================================

typedef struct {
    /* In-memory cache (mem_art) */
    size_t   cache_accounts;      /* entries in account cache */
    size_t   cache_slots;         /* entries in storage slot cache */
    size_t   cache_arena_bytes;   /* total arena bytes (accounts + storage) */

    /* Flat state (persistent, compact_art + mmap data files) */
    uint64_t flat_acct_count;     /* accounts in flat_state */
    uint64_t flat_stor_count;     /* storage slots in flat_state */
    size_t   flat_acct_mem;       /* account compact_art memory (bytes) */
    size_t   flat_stor_mem;       /* storage compact_art memory (bytes) */

    /* Code store */
    uint64_t code_count;
    uint64_t code_cache_hits;
    uint64_t code_cache_misses;
    uint32_t code_cache_count;
    uint32_t code_cache_capacity;

    /* Root computation timing (filled by compute_mpt_root) */
    double   root_stor_ms;
    double   root_acct_ms;
    size_t   root_dirty_count;

    /* Flat state lookup stats (reset per checkpoint window) */
    uint64_t flat_acct_hit;
    uint64_t flat_acct_miss;
    uint64_t flat_stor_hit;
    uint64_t flat_stor_miss;

    /* Checkpoint timing (filled by sync, not evm_state) */
    double   evict_ms;
    double   wait_flush_ms;
    double   exec_ms;        /* block_execute time this window (ms) */
} evm_state_stats_t;

evm_state_stats_t evm_state_get_stats(const evm_state_t *es);

#ifdef ENABLE_DEBUG
void evm_state_print_mpt_stats(evm_state_t *es);
void evm_state_debug_dump(evm_state_t *es);
void evm_state_dump_mpt(evm_state_t *es, const char *path);
#endif

/**
 * Dump all cached accounts and storage as a geth-compatible alloc.json.
 * Writes to the given path. Only dumps accounts that existed.
 */
void evm_state_dump_alloc_json(evm_state_t *es, const char *path);

/**
 * Collect all cached account addresses into a flat array.
 * Returns number of addresses written. Caller provides buffer.
 */
size_t evm_state_collect_addresses(evm_state_t *es, address_t *out, size_t max_count);

/**
 * Collect all cached storage slot keys for a given address.
 * Writes uint256_t slot keys to out. Returns count.
 */
size_t evm_state_collect_storage_keys(evm_state_t *es, const address_t *addr,
                                       uint256_t *out, size_t max_count);

#ifdef ENABLE_HISTORY
/**
 * Collect per-block state diffs from dirty accounts and storage slots.
 * Must be called after finalize() but before compute_state_root_ex().
 * Fills caller-provided block_diff_t. Caller owns allocated arrays.
 */
struct block_diff_t;
void evm_state_collect_block_diff(evm_state_t *es, struct block_diff_t *out);

/**
 * Apply a block diff directly to cached state, bypassing journaling.
 * For fast bulk reconstruction from history. No commit/finalize needed.
 * Only marks mpt_dirty — caller computes root at the end.
 */
void evm_state_apply_diff_bulk(evm_state_t *es, const struct block_diff_t *out);

/**
 * Debug dump: write pre_alloc.json and post_alloc.json to dir.
 * pre_alloc uses original_* fields, post_alloc uses current values.
 */
void evm_state_dump_debug(evm_state_t *es, const char *dir);
#endif

#endif // EVM_STATE_H
