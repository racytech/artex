#ifndef EVM_STATE_H
#define EVM_STATE_H

#include <stdint.h>
#include <stdbool.h>
#include "uint256.h"
#include "address.h"
#include "hash.h"
#include "verkle_state.h"
#include "witness_gas.h"
#include "mpt_store.h"

/**
 * EVM State — Typed, in-memory state interface above verkle_state.
 *
 * Architecture:
 *   - Account cache (mem_art by address_t): load-on-demand from verkle_state
 *   - Storage cache (mem_art by addr[20]+slot[32]): load-on-demand
 *   - Journal (dynamic array): snapshot = journal position, revert = undo
 *   - Access lists (mem_art sets): EIP-2929 warm/cold tracking
 *   - Transient storage (mem_art): EIP-1153 per-transaction storage
 *
 * Lifecycle:
 *   1. evm_state_create(vs) — create for block execution
 *   2. Execute transactions (get/set nonce, balance, storage, etc.)
 *   3. Use snapshot/revert for transaction boundaries
 *   4. evm_state_finalize(es) — flush dirty state to verkle_state
 *   5. evm_state_compute_state_root_ex(es) — get verkle root
 *   6. evm_state_destroy(es) — free in-memory caches
 *
 * Opaque handle — struct defined in evm_state.c.
 */

typedef struct evm_state evm_state_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create EVM state over an existing verkle_state. vs is NOT owned.
 * mpt_path: if non-NULL, enables persistent incremental MPT for state root
 * computation. If NULL, falls back to batch root rebuild (slow at scale).
 */
evm_state_t *evm_state_create(verkle_state_t *vs, const char *mpt_path);

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
void      evm_state_set_storage(evm_state_t *es, const address_t *addr,
                                const uint256_t *key, const uint256_t *value);

/** EIP-7610: Check if account has any non-zero storage (for CREATE collision detection). */
bool evm_state_has_storage(evm_state_t *es, const address_t *addr);

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
// Witness Gas (EIP-4762, Verkle+)
// ============================================================================

/** Verkle witness gas access event. Returns gas to charge.
 *  key must be a 32-byte verkle tree key (key[0:31] = stem). */
uint64_t evm_state_witness_gas_access(evm_state_t *es,
                                       const uint8_t key[32],
                                       bool is_write,
                                       bool value_was_empty);

// ============================================================================
// Finalize
// ============================================================================

/**
 * Flush all dirty state to verkle_state.
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
 * Compute verkle state root from the backing verkle_state.
 * Must call evm_state_finalize() first to flush dirty state.
 * @param prune_empty  Ignored for verkle (kept for API compatibility)
 */
hash_t evm_state_compute_state_root_ex(evm_state_t *es, bool prune_empty);

/**
 * Compute MPT (Merkle Patricia Trie) state root from the in-memory caches.
 * Used for pre-Verkle block validation against block headers.
 * Must be called AFTER evm_state_compute_state_root_ex() (which flushes
 * dirty state and sets existed flags).
 * @param prune_empty  If true (EIP-161+), exclude empty accounts from the trie.
 */
hash_t evm_state_compute_mpt_root(evm_state_t *es, bool prune_empty);
void evm_state_debug_dump(evm_state_t *es);

#endif // EVM_STATE_H
