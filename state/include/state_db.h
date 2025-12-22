/**
 * StateDB - Ethereum World State Database
 *
 * The StateDB is the main interface for managing Ethereum world state.
 * It integrates multiple layers:
 * - State Cache: Fast in-memory account/storage access
 * - Journal: Transaction-level change tracking and rollback
 * - MPT: Merkle Patricia Trie for state root computation
 * - Database: (Future) Persistent storage integration
 *
 * Architecture:
 *   EVM → StateDB → Journal → Cache → Database
 *
 * Usage Pattern:
 * 1. Create StateDB instance
 * 2. Begin transaction (creates snapshot)
 * 3. Execute operations (get/set account, storage, etc.)
 * 4. On success: commit transaction
 * 5. On failure: revert to snapshot
 * 6. Finalize: compute state root, persist to database
 */

#ifndef ART_STATE_DB_H
#define ART_STATE_DB_H

#include "state_cache.h"
#include "state_journal.h"
#include "mpt.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// StateDB Structure
//==============================================================================

/**
 * StateDB - Complete state management system
 */
typedef struct
{
    state_cache_t cache;     // In-memory account/storage cache
    state_journal_t journal; // Transaction change tracking
    mpt_state_t mpt;         // Merkle Patricia Trie for state root
    hash_t state_root;       // Current state root hash
    uint64_t block_number;   // Current block number
    bool in_transaction;     // Whether we're in a transaction

} state_db_t;

//==============================================================================
// StateDB Lifecycle
//==============================================================================

/**
 * Initialize a new StateDB instance
 *
 * @param db StateDB to initialize
 * @return true on success, false on failure
 */
bool state_db_init(state_db_t *db);

/**
 * Destroy StateDB and free all resources
 *
 * @param db StateDB to destroy
 */
void state_db_destroy(state_db_t *db);

/**
 * Reset StateDB to empty state
 * Clears all accounts, storage, and resets state root
 *
 * @param db StateDB to reset
 */
void state_db_reset(state_db_t *db);

//==============================================================================
// Transaction Management
//==============================================================================

/**
 * Begin a new transaction
 * Creates a snapshot that can be reverted
 *
 * @param db StateDB instance
 * @return Snapshot ID, or UINT32_MAX on failure
 */
uint32_t state_db_begin_transaction(state_db_t *db);

/**
 * Commit current transaction
 * Applies all changes to the cache
 *
 * @param db StateDB instance
 * @return true on success, false on failure
 */
bool state_db_commit_transaction(state_db_t *db);

/**
 * Finalize transaction (EIP-161)
 * Deletes all empty accounts that were touched during transaction.
 * An account is empty if balance=0, nonce=0, and no code.
 * Should be called before commit_transaction.
 *
 * @param db StateDB instance
 * @return true on success, false on failure
 */
bool state_db_finalize_transaction(state_db_t *db);

/**
 * Revert to a previous snapshot
 * Rolls back all changes since the snapshot
 *
 * @param db StateDB instance
 * @param snapshot_id Snapshot ID from state_db_begin_transaction
 * @return true on success, false on failure
 */
bool state_db_revert_to_snapshot(state_db_t *db, uint32_t snapshot_id);

//==============================================================================
// Account Operations
//==============================================================================

/**
 * Check if an account exists
 *
 * @param db StateDB instance
 * @param addr Account address
 * @return true if account exists, false otherwise
 */
bool state_db_exist(const state_db_t *db, const address_t *addr);

/**
 * Check if an account is empty (zero balance, zero nonce, no code)
 *
 * @param db StateDB instance
 * @param addr Account address
 * @return true if empty, false otherwise
 */
bool state_db_empty(const state_db_t *db, const address_t *addr);

/**
 * Get account balance
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param balance Output parameter for balance
 * @return true on success, false if account doesn't exist
 */
bool state_db_get_balance(const state_db_t *db, const address_t *addr, uint256_t *balance);

/**
 * Set account balance
 * Records change in journal for potential rollback
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param balance New balance
 * @return true on success, false on failure
 */
bool state_db_set_balance(state_db_t *db, const address_t *addr, const uint256_t *balance);

/**
 * Add to account balance
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param amount Amount to add
 * @return true on success, false on failure
 */
bool state_db_add_balance(state_db_t *db, const address_t *addr, const uint256_t *amount);

/**
 * Subtract from account balance
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param amount Amount to subtract
 * @return true on success, false if insufficient balance
 */
bool state_db_sub_balance(state_db_t *db, const address_t *addr, const uint256_t *amount);

/**
 * Get account nonce
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param nonce Output parameter for nonce
 * @return true on success, false if account doesn't exist
 */
bool state_db_get_nonce(const state_db_t *db, const address_t *addr, uint64_t *nonce);

/**
 * Set account nonce
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param nonce New nonce value
 * @return true on success, false on failure
 */
bool state_db_set_nonce(state_db_t *db, const address_t *addr, uint64_t nonce);

/**
 * Get account code hash
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param code_hash Output parameter for code hash
 * @return true on success, false if account doesn't exist
 */
bool state_db_get_code_hash(const state_db_t *db, const address_t *addr, hash_t *code_hash);

/**
 * Set account code hash
 *
 * @param db StateDB instance
 * @param addr Account address
 * @param code_hash New code hash
 * @return true on success, false on failure
 */
bool state_db_set_code_hash(state_db_t *db, const address_t *addr, const hash_t *code_hash);

/**
 * Get contract code
 *
 * @param db StateDB instance
 * @param addr Contract address
 * @param code Output parameter for code pointer (caller must NOT free)
 * @param code_size Output parameter for code size
 * @return true on success, false if account doesn't exist or has no code
 */
bool state_db_get_code(const state_db_t *db, const address_t *addr, 
                       const uint8_t **code, size_t *code_size);

/**
 * Set contract code
 *
 * @param db StateDB instance
 * @param addr Contract address
 * @param code Contract bytecode (will be copied)
 * @param code_size Size of bytecode
 * @return true on success, false on failure
 */
bool state_db_set_code(state_db_t *db, const address_t *addr,
                       const uint8_t *code, size_t code_size);

/**
 * Create a new account
 * Fails if account already exists
 *
 * @param db StateDB instance
 * @param addr Account address
 * @return true on success, false if account exists
 */
bool state_db_create_account(state_db_t *db, const address_t *addr);

/**
 * Delete an account (SELFDESTRUCT)
 *
 * @param db StateDB instance
 * @param addr Account address
 * @return true on success, false on failure
 */
bool state_db_suicide(state_db_t *db, const address_t *addr);

//==============================================================================
// Storage Operations
//==============================================================================

/**
 * Get storage value at key
 *
 * @param db StateDB instance
 * @param addr Contract address
 * @param key Storage key
 * @param value Output parameter for storage value
 * @return true on success, false if account doesn't exist
 */
bool state_db_get_state(const state_db_t *db, const address_t *addr,
                        const uint256_t *key, uint256_t *value);

/**
 * Set storage value at key
 *
 * @param db StateDB instance
 * @param addr Contract address
 * @param key Storage key
 * @param value New storage value
 * @return true on success, false on failure
 */
bool state_db_set_state(state_db_t *db, const address_t *addr,
                        const uint256_t *key, const uint256_t *value);

//==============================================================================
// State Root Operations
//==============================================================================

/**
 * Compute and update the state root
 * This is computationally expensive - only call when needed
 * (typically at end of block execution)
 *
 * @param db StateDB instance
 * @return true on success, false on failure
 */
bool state_db_compute_state_root(state_db_t *db);

/**
 * Get current state root
 *
 * @param db StateDB instance
 * @param root Output parameter for state root
 */
void state_db_get_state_root(const state_db_t *db, hash_t *root);

/**
 * Set state root (used when loading from database)
 *
 * @param db StateDB instance
 * @param root New state root
 */
void state_db_set_state_root(state_db_t *db, const hash_t *root);

//==============================================================================
// Block Context
//==============================================================================

/**
 * Get current block number
 *
 * @param db StateDB instance
 * @return Block number
 */
uint64_t state_db_get_block_number(const state_db_t *db);

/**
 * Set current block number
 *
 * @param db StateDB instance
 * @param block_number New block number
 */
void state_db_set_block_number(state_db_t *db, uint64_t block_number);

//==============================================================================
// Utilities
//==============================================================================

/**
 * Get number of accounts in cache
 *
 * @param db StateDB instance
 * @return Number of cached accounts
 */
size_t state_db_num_accounts(const state_db_t *db);

/**
 * Get number of dirty (modified) accounts
 *
 * @param db StateDB instance
 * @return Number of dirty accounts
 */
size_t state_db_num_dirty(const state_db_t *db);

/**
 * Get number of journal entries
 *
 * @param db StateDB instance
 * @return Number of journal entries
 */
size_t state_db_journal_size(const state_db_t *db);

#ifdef __cplusplus
}
#endif

#endif /* ART_STATE_DB_H */
