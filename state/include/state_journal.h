/**
 * State Journal - Transaction-level change tracking and rollback
 *
 * The journal layer sits above the state cache and provides:
 * - Recording of all state modifications during execution
 * - Snapshot/revert support for EVM operations
 * - Rollback capability for failed transactions
 * - Change batching for efficient commits
 *
 * Architecture:
 *   EVM → Journal → State Cache → ART → Persistent DB
 *
 * During transaction execution:
 * - All changes are recorded in the journal
 * - Original values are preserved for rollback
 * - Changes can be reverted via snapshots
 *
 * On transaction success:
 * - Journal commits changes to state cache
 * - Journal is cleared for next transaction
 *
 * On transaction failure:
 * - Journal reverts to snapshot (or beginning)
 * - State cache remains unchanged
 */

#ifndef ART_STATE_JOURNAL_H
#define ART_STATE_JOURNAL_H

#include "state_cache.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Journal Entry Types
//==============================================================================

/**
 * Types of state changes that can be journaled
 */
typedef enum
{
    JOURNAL_ACCOUNT_CREATED,  // Account created (was non-existent)
    JOURNAL_ACCOUNT_MODIFIED, // Account modified (balance/nonce/etc changed)
    JOURNAL_ACCOUNT_DELETED,  // Account deleted
    JOURNAL_STORAGE_CHANGED,  // Storage slot modified
    JOURNAL_CODE_CHANGED,     // Contract code set
} journal_entry_type_t;

/**
 * A single journal entry recording one state change
 */
typedef struct
{
    journal_entry_type_t type;
    address_t address;

    // Original values (for rollback)
    union
    {
        struct
        {
            uint256_t balance;
            uint64_t nonce;
            hash_t code_hash;
            hash_t storage_root;
            bool existed; // Did account exist before?
        } account;

        struct
        {
            uint256_t key;
            uint256_t old_value;
        } storage;

        struct
        {
            hash_t old_code_hash;
        } code;
    } old_state;

} journal_entry_t;

/**
 * Snapshot represents the state of the journal at a point in time
 * Used for EVM's REVERT operation
 */
typedef struct
{
    uint32_t id;
    size_t journal_length; // Number of entries at snapshot time
} journal_snapshot_t;

/**
 * State Journal - tracks uncommitted changes
 */
typedef struct
{
    journal_entry_t *entries;      // Dynamic array of journal entries
    size_t num_entries;            // Current number of entries
    size_t entries_capacity;       // Allocated capacity
    journal_snapshot_t *snapshots; // Stack of snapshots
    size_t num_snapshots;          // Number of snapshots
    size_t snapshots_capacity;     // Allocated capacity for snapshots
    state_cache_t *cache;          // Reference to underlying state cache

} state_journal_t;

//==============================================================================
// Journal Lifecycle
//==============================================================================

/**
 * Initialize a new journal
 *
 * @param journal Journal to initialize
 * @param cache State cache to operate on
 * @return true on success, false on failure
 */
bool state_journal_init(state_journal_t *journal, state_cache_t *cache);

/**
 * Destroy journal and free resources
 * Does NOT modify the state cache
 *
 * @param journal Journal to destroy
 */
void state_journal_destroy(state_journal_t *journal);

/**
 * Reset journal to empty state
 * Discards all entries and snapshots
 *
 * @param journal Journal to reset
 */
void state_journal_reset(state_journal_t *journal);

//==============================================================================
// Snapshot Management
//==============================================================================

/**
 * Create a snapshot of current journal state
 * Used by EVM for REVERT operations
 *
 * @param journal Journal to snapshot
 * @return Snapshot ID, or UINT32_MAX on failure
 */
uint32_t state_journal_snapshot(state_journal_t *journal);

/**
 * Revert journal to a previous snapshot
 * Discards all entries added after the snapshot
 * Does NOT modify the state cache
 *
 * @param journal Journal to revert
 * @param snapshot_id ID returned by state_journal_snapshot
 * @return true on success, false if snapshot_id is invalid
 */
bool state_journal_revert_to_snapshot(state_journal_t *journal, uint32_t snapshot_id);

//==============================================================================
// State Modification Tracking
//==============================================================================

/**
 * Record account creation
 * Called when a new account is created
 *
 * @param journal Journal to record in
 * @param addr Address of created account
 * @return true on success, false on failure
 */
bool state_journal_account_created(state_journal_t *journal, const address_t *addr);

/**
 * Record account modification
 * Called before modifying an existing account
 * Preserves old state for rollback
 *
 * @param journal Journal to record in
 * @param addr Address of account
 * @param old_account Account state before modification
 * @return true on success, false on failure
 */
bool state_journal_account_modified(state_journal_t *journal,
                                    const address_t *addr,
                                    const account_object_t *old_account);

/**
 * Record account deletion
 * Called before deleting an account
 *
 * @param journal Journal to record in
 * @param addr Address of account to delete
 * @param old_account Account state before deletion
 * @return true on success, false on failure
 */
bool state_journal_account_deleted(state_journal_t *journal,
                                    const address_t *addr,
                                    const account_object_t *old_account);

/**
 * Record storage change
 * Called before modifying a storage slot
 *
 * @param journal Journal to record in
 * @param addr Address of contract
 * @param key Storage slot key
 * @param old_value Value before modification
 * @return true on success, false on failure
 */
bool state_journal_storage_changed(state_journal_t *journal,
                                    const address_t *addr,
                                    const uint256_t *key,
                                    const uint256_t *old_value);

//==============================================================================
// Journal Commit/Rollback
//==============================================================================

/**
 * Commit all journal changes to the state cache
 * After commit, journal is reset to empty
 * This makes changes "permanent" in the cache layer
 *
 * @param journal Journal to commit
 * @return true on success, false on failure
 */
bool state_journal_commit(state_journal_t *journal);

/**
 * Rollback all journal changes
 * Reverts state cache to state before any journal entries
 * After rollback, journal is reset to empty
 *
 * @param journal Journal to rollback
 * @return true on success, false on failure
 */
bool state_journal_rollback(state_journal_t *journal);

//==============================================================================
// Utilities
//==============================================================================

/**
 * Get number of journal entries
 *
 * @param journal Journal to query
 * @return Number of entries
 */
size_t state_journal_size(const state_journal_t *journal);

/**
 * Get number of active snapshots
 *
 * @param journal Journal to query
 * @return Number of snapshots
 */
size_t state_journal_snapshot_count(const state_journal_t *journal);

#ifdef __cplusplus
}
#endif

#endif /* ART_STATE_JOURNAL_H */
