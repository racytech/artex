#ifndef EVM_STATE_H
#define EVM_STATE_H

#include <stdint.h>
#include <stdbool.h>
#include "../../common/include/uint256.h"
#include "../../common/include/address.h"
#include "../../common/include/hash.h"
#include "state_db.h"

/**
 * EVM State — Typed, in-memory state interface above state_db.
 *
 * Architecture:
 *   - Account cache (mem_art by address_t): load-on-demand from state_db
 *   - Storage cache (mem_art by addr[20]+slot[32]): load-on-demand
 *   - Journal (dynamic array): snapshot = journal position, revert = undo
 *   - Access lists (mem_art sets): EIP-2929 warm/cold tracking
 *
 * Lifecycle:
 *   1. evm_state_create(sdb) — create for block execution
 *   2. Execute transactions (get/set nonce, balance, storage, etc.)
 *   3. Use snapshot/revert for transaction boundaries
 *   4. evm_state_finalize(es) — flush dirty state to state_db
 *   5. evm_state_destroy(es) — free in-memory caches
 *   6. Caller controls sdb_merge/sdb_checkpoint timing
 *
 * Opaque handle — struct defined in evm_state.c.
 */

typedef struct evm_state evm_state_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create EVM state over an existing state_db. sdb is NOT owned.
 * @param state_mpt_path    Path for persistent state MPT file, or NULL for
 *                          ephemeral mode (rebuilds trie from scratch each call).
 *                          When set, the MPT survives between blocks — only dirty
 *                          accounts are re-inserted per compute_state_root call.
 * @param storage_mpt_path  Path for persistent shared storage MPT (64-byte keys:
 *                          keccak(addr)[32] || keccak(slot)[32]), or NULL for
 *                          ephemeral mode. When set, only dirty slots are updated
 *                          per block; per-account storage roots use mpt_subtree_root.
 */
evm_state_t *evm_state_create(state_db_t *sdb, const char *state_mpt_path,
                               const char *storage_mpt_path);

/** Destroy EVM state and free all in-memory caches. */
void evm_state_destroy(evm_state_t *es);

// ============================================================================
// Account Existence
// ============================================================================

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
uint32_t evm_state_get_code_size(evm_state_t *es, const address_t *addr);
bool     evm_state_get_code(evm_state_t *es, const address_t *addr,
                            uint8_t *out, uint32_t *out_len);
void     evm_state_set_code(evm_state_t *es, const address_t *addr,
                            const uint8_t *code, uint32_t len);

// ============================================================================
// Storage
// ============================================================================

uint256_t evm_state_get_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key);
void      evm_state_set_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key, const uint256_t *value);

// ============================================================================
// Account Creation (CREATE / CREATE2)
// ============================================================================

/** Create or reset account: nonce=0, balance=0, no code, clear storage. */
void evm_state_create_account(evm_state_t *es, const address_t *addr);

// ============================================================================
// Self-Destruct
// ============================================================================

void evm_state_self_destruct(evm_state_t *es, const address_t *addr);
bool evm_state_is_self_destructed(evm_state_t *es, const address_t *addr);

// ============================================================================
// Snapshot / Revert
// ============================================================================

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
// Finalize
// ============================================================================

/**
 * Flush all dirty state to state_db.
 * - Encodes and writes dirty accounts via sdb_put
 * - Writes dirty storage slots via sdb_put_storage
 * - Writes new code via sdb_put_code
 * - Deletes self-destructed accounts
 * Does NOT call sdb_merge — caller controls merge timing.
 */
bool evm_state_finalize(evm_state_t *es);

// ============================================================================
// State Root
// ============================================================================

/**
 * Compute Ethereum MPT state root from current in-memory state.
 *
 * Persistent mode (state_mpt_path was set at create):
 *   Only dirty/created accounts are inserted into the persistent MPT.
 *   mpt_root walks only dirty paths — O(dirty × depth).
 *
 * Ephemeral mode (state_mpt_path was NULL):
 *   Rebuilds the entire state trie from scratch each call.
 *
 * Storage roots are always computed per-account from evm_state cache (ephemeral).
 */
hash_t evm_state_compute_state_root(evm_state_t *es);

/**
 * Compute state root with explicit EIP-161 control.
 * @param prune_empty  true = EIP-161+ (skip empty accounts), false = pre-EIP-161
 */
hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty);

#endif // EVM_STATE_H
