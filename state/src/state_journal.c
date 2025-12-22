/**
 * State Journal Implementation
 */

#include "state_journal.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Constants
//==============================================================================

#define INITIAL_ENTRIES_CAPACITY 64
#define INITIAL_SNAPSHOTS_CAPACITY 8

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Ensure journal has capacity for at least one more entry
 */
static bool ensure_entry_capacity(state_journal_t *journal)
{
    if (journal->num_entries >= journal->entries_capacity)
    {
        size_t new_capacity = journal->entries_capacity == 0 ? 
                              INITIAL_ENTRIES_CAPACITY : 
                              journal->entries_capacity * 2;
        journal_entry_t *new_entries = realloc(journal->entries,
                                               new_capacity * sizeof(journal_entry_t));
        if (!new_entries)
        {
            LOG_STATE_ERROR("ensure_entry_capacity: failed to allocate memory");
            return false;
        }
        journal->entries = new_entries;
        journal->entries_capacity = new_capacity;
    }
    return true;
}

/**
 * Ensure journal has capacity for at least one more snapshot
 */
static bool ensure_snapshot_capacity(state_journal_t *journal)
{
    if (journal->num_snapshots >= journal->snapshots_capacity)
    {
        size_t new_capacity = journal->snapshots_capacity == 0 ? 
                              INITIAL_SNAPSHOTS_CAPACITY : 
                              journal->snapshots_capacity * 2;
        journal_snapshot_t *new_snapshots = realloc(journal->snapshots,
                                                    new_capacity * sizeof(journal_snapshot_t));
        if (!new_snapshots)
        {
            LOG_STATE_ERROR("ensure_snapshot_capacity: failed to allocate memory");
            return false;
        }
        journal->snapshots = new_snapshots;
        journal->snapshots_capacity = new_capacity;
    }
    return true;
}

/**
 * Add a journal entry
 */
static bool add_entry(state_journal_t *journal, const journal_entry_t *entry)
{
    if (!ensure_entry_capacity(journal))
        return false;

    journal->entries[journal->num_entries++] = *entry;
    return true;
}

//==============================================================================
// Journal Lifecycle
//==============================================================================

bool state_journal_init(state_journal_t *journal, state_cache_t *cache)
{
    if (!journal || !cache)
        return false;

    journal->entries = malloc(INITIAL_ENTRIES_CAPACITY * sizeof(journal_entry_t));
    if (!journal->entries)
    {
        LOG_STATE_ERROR("state_journal_init: failed to allocate entries");
        return false;
    }

    journal->snapshots = malloc(INITIAL_SNAPSHOTS_CAPACITY * sizeof(journal_snapshot_t));
    if (!journal->snapshots)
    {
        free(journal->entries);
        LOG_STATE_ERROR("state_journal_init: failed to allocate snapshots");
        return false;
    }

    journal->num_entries = 0;
    journal->entries_capacity = INITIAL_ENTRIES_CAPACITY;
    journal->num_snapshots = 0;
    journal->snapshots_capacity = INITIAL_SNAPSHOTS_CAPACITY;
    journal->cache = cache;

    LOG_STATE_DEBUG("state_journal_init: journal initialized");
    return true;
}

void state_journal_destroy(state_journal_t *journal)
{
    if (!journal)
        return;

    free(journal->entries);
    free(journal->snapshots);

    journal->entries = NULL;
    journal->snapshots = NULL;
    journal->num_entries = 0;
    journal->entries_capacity = 0;
    journal->num_snapshots = 0;
    journal->snapshots_capacity = 0;
    journal->cache = NULL;

    LOG_STATE_DEBUG("state_journal_destroy: journal destroyed");
}

void state_journal_reset(state_journal_t *journal)
{
    if (!journal)
        return;

    journal->num_entries = 0;
    journal->num_snapshots = 0;

    LOG_STATE_DEBUG("state_journal_reset: journal reset to empty state");
}

//==============================================================================
// Snapshot Management
//==============================================================================

uint32_t state_journal_snapshot(state_journal_t *journal)
{
    if (!journal)
        return UINT32_MAX;

    if (!ensure_snapshot_capacity(journal))
        return UINT32_MAX;

    uint32_t snapshot_id = (uint32_t)journal->num_snapshots;
    journal_snapshot_t snapshot = {
        .id = snapshot_id,
        .journal_length = journal->num_entries};

    journal->snapshots[journal->num_snapshots++] = snapshot;

    LOG_STATE_DEBUG("state_journal_snapshot: created snapshot %u at journal length %zu",
                    snapshot_id, journal->num_entries);
    return snapshot_id;
}

bool state_journal_revert_to_snapshot(state_journal_t *journal, uint32_t snapshot_id)
{
    if (!journal)
        return false;

    if (snapshot_id >= journal->num_snapshots)
    {
        LOG_STATE_ERROR("state_journal_revert_to_snapshot: invalid snapshot_id %u", snapshot_id);
        return false;
    }

    journal_snapshot_t *snapshot = &journal->snapshots[snapshot_id];
    size_t target_length = snapshot->journal_length;

    // CRITICAL: Roll back all entries added after this snapshot BEFORE discarding them
    // Otherwise the cache will have stale state from reverted operations
    for (size_t i = journal->num_entries; i > target_length; i--)
    {
        journal_entry_t *entry = &journal->entries[i - 1];

        switch (entry->type)
        {
        case JOURNAL_ACCOUNT_CREATED:
        {
            // Account was created after snapshot, so mark it as non-existent
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->exists = false;
                account->deleted = true;
                account->dirty = false;
            }
            break;
        }

        case JOURNAL_ACCOUNT_MODIFIED:
        {
            // Restore old account state from before this modification
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->balance = entry->old_state.account.balance;
                account->nonce = entry->old_state.account.nonce;
                account->code_hash = entry->old_state.account.code_hash;
                account->storage_root = entry->old_state.account.storage_root;
                account->exists = entry->old_state.account.existed;
                account->dirty = false;
                account->deleted = false;
            }
            break;
        }

        case JOURNAL_ACCOUNT_DELETED:
        {
            // Account was deleted after snapshot, so restore it
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->balance = entry->old_state.account.balance;
                account->nonce = entry->old_state.account.nonce;
                account->code_hash = entry->old_state.account.code_hash;
                account->storage_root = entry->old_state.account.storage_root;
                account->exists = entry->old_state.account.existed;
                account->dirty = false;
                account->deleted = false;
            }
            break;
        }

        case JOURNAL_STORAGE_CHANGED:
        {
            // Restore old storage value
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                state_cache_set_storage(journal->cache, account, &entry->old_state.storage.key,
                                        &entry->old_state.storage.old_value);
            }
            break;
        }

        case JOURNAL_CODE_CHANGED:
        {
            // Code changes rollback not yet implemented
            // For now, just restore code_hash
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->code_hash = entry->old_state.code.old_code_hash;
            }
            break;
        }
        }
    }

    // Now discard the rolled-back entries
    journal->num_entries = target_length;

    // Discard all snapshots created after this one
    journal->num_snapshots = snapshot_id;

    LOG_STATE_DEBUG("state_journal_revert_to_snapshot: reverted to snapshot %u, "
                    "journal length now %zu",
                    snapshot_id, journal->num_entries);
    return true;
}

//==============================================================================
// State Modification Tracking
//==============================================================================

bool state_journal_account_created(state_journal_t *journal, const address_t *addr)
{
    if (!journal || !addr)
        return false;

    journal_entry_t entry = {
        .type = JOURNAL_ACCOUNT_CREATED,
        .address = *addr,
        .old_state.account.existed = false};

    bool result = add_entry(journal, &entry);
    if (result)
    {
        LOG_STATE_DEBUG("state_journal_account_created: recorded account creation");
    }
    return result;
}

bool state_journal_account_modified(state_journal_t *journal,
                                    const address_t *addr,
                                    const account_object_t *old_account)
{
    if (!journal || !addr || !old_account)
        return false;

    journal_entry_t entry = {
        .type = JOURNAL_ACCOUNT_MODIFIED,
        .address = *addr,
        .old_state.account = {
            .balance = old_account->balance,
            .nonce = old_account->nonce,
            .code_hash = old_account->code_hash,
            .storage_root = old_account->storage_root,
            .existed = old_account->exists}};

    bool result = add_entry(journal, &entry);
    if (result)
    {
        LOG_STATE_DEBUG("state_journal_account_modified: recorded account modification");
    }
    return result;
}

bool state_journal_account_deleted(state_journal_t *journal,
                                   const address_t *addr,
                                   const account_object_t *old_account)
{
    if (!journal || !addr || !old_account)
        return false;

    journal_entry_t entry = {
        .type = JOURNAL_ACCOUNT_DELETED,
        .address = *addr,
        .old_state.account = {
            .balance = old_account->balance,
            .nonce = old_account->nonce,
            .code_hash = old_account->code_hash,
            .storage_root = old_account->storage_root,
            .existed = old_account->exists}};

    bool result = add_entry(journal, &entry);
    if (result)
    {
        LOG_STATE_DEBUG("state_journal_account_deleted: recorded account deletion");
    }
    return result;
}

bool state_journal_storage_changed(state_journal_t *journal,
                                   const address_t *addr,
                                   const uint256_t *key,
                                   const uint256_t *old_value)
{
    if (!journal || !addr || !key || !old_value)
        return false;

    journal_entry_t entry = {
        .type = JOURNAL_STORAGE_CHANGED,
        .address = *addr,
        .old_state.storage = {
            .key = *key,
            .old_value = *old_value}};

    bool result = add_entry(journal, &entry);
    if (result)
    {
        LOG_STATE_DEBUG("state_journal_storage_changed: recorded storage change");
    }
    return result;
}

//==============================================================================
// Journal Commit/Rollback
//==============================================================================

bool state_journal_commit(state_journal_t *journal)
{
    if (!journal || !journal->cache)
        return false;

    // Journal entries are already applied to the cache
    // Commit just means "keep these changes"
    // So we just clear the journal

    size_t num_committed = journal->num_entries;
    state_journal_reset(journal);

    LOG_STATE_DEBUG("state_journal_commit: committed %zu entries", num_committed);
    return true;
}

bool state_journal_rollback(state_journal_t *journal)
{
    if (!journal || !journal->cache)
        return false;

    // Rollback: apply journal entries in reverse to undo changes
    for (size_t i = journal->num_entries; i > 0; i--)
    {
        journal_entry_t *entry = &journal->entries[i - 1];

        switch (entry->type)
        {
        case JOURNAL_ACCOUNT_CREATED:
        {
            // Account was created during transaction, so mark it as non-existent
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->exists = false;
                account->deleted = true;
                account->dirty = false;
            }
            break;
        }

        case JOURNAL_ACCOUNT_MODIFIED:
        {
            // Restore old account state
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->balance = entry->old_state.account.balance;
                account->nonce = entry->old_state.account.nonce;
                account->code_hash = entry->old_state.account.code_hash;
                account->storage_root = entry->old_state.account.storage_root;
                account->exists = entry->old_state.account.existed;
                account->dirty = false;
                account->deleted = false;
            }
            break;
        }

        case JOURNAL_ACCOUNT_DELETED:
        {
            // Account was deleted, so restore it
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                account->balance = entry->old_state.account.balance;
                account->nonce = entry->old_state.account.nonce;
                account->code_hash = entry->old_state.account.code_hash;
                account->storage_root = entry->old_state.account.storage_root;
                account->exists = entry->old_state.account.existed;
                account->dirty = false;
                account->deleted = false;
            }
            break;
        }

        case JOURNAL_STORAGE_CHANGED:
        {
            // Restore old storage value
            account_object_t *account = state_cache_get_account(journal->cache, &entry->address);
            if (account)
            {
                state_cache_set_storage(journal->cache, account, &entry->old_state.storage.key,
                                        &entry->old_state.storage.old_value);
            }
            break;
        }

        case JOURNAL_CODE_CHANGED:
        {
            // Code changes not yet implemented
            break;
        }
        }
    }
    
    size_t num_rolled_back = journal->num_entries;
    state_journal_reset(journal);

    LOG_STATE_DEBUG("state_journal_rollback: rolled back %zu entries", num_rolled_back);
    return true;
}

//==============================================================================
// Utilities
//==============================================================================

size_t state_journal_size(const state_journal_t *journal)
{
    return journal ? journal->num_entries : 0;
}

size_t state_journal_snapshot_count(const state_journal_t *journal)
{
    return journal ? journal->num_snapshots : 0;
}
