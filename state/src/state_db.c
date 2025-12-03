/**
 * StateDB Implementation
 */

#include "state_db.h"
#include "hash.h"
#include "logger.h"
#include "rlp.h"
#include "art.h"
#include <string.h>
#include <stdlib.h>

//==============================================================================
// StateDB Lifecycle
//==============================================================================

bool state_db_init(state_db_t *db)
{
    if (!db)
        return false;

    // Initialize cache
    if (!state_cache_init(&db->cache))
    {
        LOG_STATE_ERROR("state_db_init: failed to initialize cache");
        return false;
    }

    // Initialize journal
    if (!state_journal_init(&db->journal, &db->cache))
    {
        state_cache_destroy(&db->cache);
        LOG_STATE_ERROR("state_db_init: failed to initialize journal");
        return false;
    }

    // Initialize MPT
    if (!mpt_init(&db->mpt))
    {
        state_journal_destroy(&db->journal);
        state_cache_destroy(&db->cache);
        LOG_STATE_ERROR("state_db_init: failed to initialize MPT");
        return false;
    }

    // Initialize state root to empty trie
    memset(&db->state_root, 0, sizeof(hash_t));

    db->block_number = 0;
    db->in_transaction = false;

    LOG_STATE_INFO("state_db_init: StateDB initialized");
    return true;
}

void state_db_destroy(state_db_t *db)
{
    if (!db)
        return;

    mpt_destroy(&db->mpt);
    state_journal_destroy(&db->journal);
    state_cache_destroy(&db->cache);

    memset(db, 0, sizeof(state_db_t));

    LOG_STATE_INFO("state_db_destroy: StateDB destroyed");
}

void state_db_reset(state_db_t *db)
{
    if (!db)
        return;

    state_journal_reset(&db->journal);
    state_cache_reset(&db->cache);
    mpt_destroy(&db->mpt);
    mpt_init(&db->mpt);

    memset(&db->state_root, 0, sizeof(hash_t));
    db->block_number = 0;
    db->in_transaction = false;

    LOG_STATE_DEBUG("state_db_reset: StateDB reset to empty state");
}

//==============================================================================
// Transaction Management
//==============================================================================

uint32_t state_db_begin_transaction(state_db_t *db)
{
    if (!db)
        return UINT32_MAX;

    uint32_t snapshot_id = state_journal_snapshot(&db->journal);
    if (snapshot_id != UINT32_MAX)
    {
        db->in_transaction = true;
        LOG_STATE_DEBUG("state_db_begin_transaction: snapshot %u created", snapshot_id);
    }

    return snapshot_id;
}

bool state_db_commit_transaction(state_db_t *db)
{
    if (!db)
        return false;

    bool result = state_journal_commit(&db->journal);
    if (result)
    {
        db->in_transaction = false;
        LOG_STATE_DEBUG("state_db_commit_transaction: transaction committed");
    }

    return result;
}

bool state_db_revert_to_snapshot(state_db_t *db, uint32_t snapshot_id)
{
    if (!db)
        return false;

    bool result = state_journal_revert_to_snapshot(&db->journal, snapshot_id);
    if (result)
    {
        // If we reverted to snapshot 0, we're out of transaction
        if (state_journal_snapshot_count(&db->journal) == 0)
        {
            db->in_transaction = false;
        }
        LOG_STATE_DEBUG("state_db_revert_to_snapshot: reverted to snapshot %u", snapshot_id);
    }

    return result;
}

//==============================================================================
// Account Operations
//==============================================================================

bool state_db_exist(const state_db_t *db, const address_t *addr)
{
    if (!db || !addr)
        return false;

    return state_cache_has_account(&db->cache, addr);
}

bool state_db_empty(const state_db_t *db, const address_t *addr)
{
    if (!db || !addr)
        return true;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists)
        return true;

    // Empty if: balance is 0, nonce is 0, and no code
    uint256_t zero = uint256_from_uint64(0);
    bool has_code = !hash_is_zero(&account->code_hash);

    return uint256_eq(&account->balance, &zero) &&
           account->nonce == 0 &&
           !has_code;
}

bool state_db_get_balance(const state_db_t *db, const address_t *addr, uint256_t *balance)
{
    if (!db || !addr || !balance)
        return false;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists)
        return false;

    *balance = account->balance;
    return true;
}

bool state_db_set_balance(state_db_t *db, const address_t *addr, const uint256_t *balance)
{
    if (!db || !addr || !balance)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account)
        return false;

    // Record old state in journal if this is the first modification
    if (!account->dirty && account->exists)
    {
        if (!state_journal_account_modified(&db->journal, addr, account))
            return false;
    }

    account->balance = *balance;
    account->exists = true;
    state_cache_mark_dirty(&db->cache, account);

    return true;
}

bool state_db_add_balance(state_db_t *db, const address_t *addr, const uint256_t *amount)
{
    if (!db || !addr || !amount)
        return false;

    uint256_t current_balance;
    if (!state_db_get_balance(db, addr, &current_balance))
    {
        current_balance = uint256_from_uint64(0);
    }

    uint256_t new_balance = uint256_add(&current_balance, amount);
    return state_db_set_balance(db, addr, &new_balance);
}

bool state_db_sub_balance(state_db_t *db, const address_t *addr, const uint256_t *amount)
{
    if (!db || !addr || !amount)
        return false;

    uint256_t current_balance;
    if (!state_db_get_balance(db, addr, &current_balance))
        return false;

    // Check for underflow
    if (uint256_lt(&current_balance, amount))
        return false;

    uint256_t new_balance = uint256_sub(&current_balance, amount);
    return state_db_set_balance(db, addr, &new_balance);
}

bool state_db_get_nonce(const state_db_t *db, const address_t *addr, uint64_t *nonce)
{
    if (!db || !addr || !nonce)
        return false;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists)
        return false;

    *nonce = account->nonce;
    return true;
}

bool state_db_set_nonce(state_db_t *db, const address_t *addr, uint64_t nonce)
{
    if (!db || !addr)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account)
        return false;

    // Record old state in journal if this is the first modification
    if (!account->dirty && account->exists)
    {
        if (!state_journal_account_modified(&db->journal, addr, account))
            return false;
    }

    account->nonce = nonce;
    account->exists = true;
    state_cache_mark_dirty(&db->cache, account);

    return true;
}

bool state_db_get_code_hash(const state_db_t *db, const address_t *addr, hash_t *code_hash)
{
    if (!db || !addr || !code_hash)
        return false;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists)
        return false;

    *code_hash = account->code_hash;
    return true;
}

bool state_db_set_code_hash(state_db_t *db, const address_t *addr, const hash_t *code_hash)
{
    if (!db || !addr || !code_hash)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account)
        return false;

    // Record old state in journal if this is the first modification
    if (!account->dirty && account->exists)
    {
        if (!state_journal_account_modified(&db->journal, addr, account))
            return false;
    }

    account->code_hash = *code_hash;
    account->exists = true;
    state_cache_mark_dirty(&db->cache, account);

    return true;
}

bool state_db_get_code(const state_db_t *db, const address_t *addr,
                       const uint8_t **code, size_t *code_size)
{
    if (!db || !addr || !code || !code_size)
        return false;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists || !account->code || account->code_size == 0)
    {
        *code = NULL;
        *code_size = 0;
        return false;
    }

    *code = account->code;
    *code_size = account->code_size;
    return true;
}

bool state_db_set_code(state_db_t *db, const address_t *addr,
                       const uint8_t *code, size_t code_size)
{
    if (!db || !addr)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account)
        return false;

    // Record old state in journal if this is the first modification
    if (!account->dirty && account->exists)
    {
        if (!state_journal_account_modified(&db->journal, addr, account))
            return false;
    }

    // Free old code if exists
    if (account->code)
    {
        free(account->code);
        account->code = NULL;
        account->code_size = 0;
    }

    // Copy new code
    if (code && code_size > 0)
    {
        account->code = malloc(code_size);
        if (!account->code)
            return false;
        
        memcpy(account->code, code, code_size);
        account->code_size = code_size;

        // Also update code hash
        account->code_hash = hash_keccak256(code, code_size);
    }
    else
    {
        // No code - set to empty code hash
        account->code_hash = HASH_EMPTY_CODE;
    }

    account->exists = true;
    state_cache_mark_dirty(&db->cache, account);

    return true;
}

bool state_db_create_account(state_db_t *db, const address_t *addr)
{
    if (!db || !addr)
        return false;

    // Check if account already exists
    if (state_db_exist(db, addr))
    {
        LOG_STATE_WARN("state_db_create_account: account already exists");
        return false;
    }

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account)
        return false;

    // Record creation in journal
    if (!state_journal_account_created(&db->journal, addr))
        return false;

    // Initialize account
    account->balance = uint256_from_uint64(0);
    account->nonce = 0;
    account->code_hash = HASH_EMPTY_CODE;
    account->storage_root = HASH_EMPTY_STORAGE;
    account->exists = true;

    state_cache_mark_dirty(&db->cache, account);

    LOG_STATE_DEBUG("state_db_create_account: account created");
    return true;
}

bool state_db_suicide(state_db_t *db, const address_t *addr)
{
    if (!db || !addr)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account || !account->exists)
        return false;

    // Record deletion in journal
    if (!state_journal_account_deleted(&db->journal, addr, account))
        return false;

    // Delete account
    bool result = state_cache_delete_account(&db->cache, addr);

    if (result)
    {
        LOG_STATE_DEBUG("state_db_suicide: account deleted");
    }

    return result;
}

//==============================================================================
// Storage Operations
//==============================================================================

bool state_db_get_state(const state_db_t *db, const address_t *addr,
                        const uint256_t *key, uint256_t *value)
{
    if (!db || !addr || !key || !value)
        return false;

    account_object_t *account = state_cache_get_account((state_cache_t *)&db->cache, addr);
    if (!account || !account->exists)
        return false;

    return state_cache_get_storage(account, key, value);
}

bool state_db_set_state(state_db_t *db, const address_t *addr,
                        const uint256_t *key, const uint256_t *value)
{
    if (!db || !addr || !key || !value)
        return false;

    account_object_t *account = state_cache_get_account(&db->cache, addr);
    if (!account || !account->exists)
        return false;

    // Get old value for journal
    uint256_t old_value;
    if (!state_cache_get_storage(account, key, &old_value))
    {
        old_value = uint256_from_uint64(0);
    }

    // Record change in journal
    if (!state_journal_storage_changed(&db->journal, addr, key, &old_value))
        return false;

    // Set new value
    return state_cache_set_storage(&db->cache, account, key, value);
}

//==============================================================================
// State Root Operations
//==============================================================================

/**
 * Compute storage root for an account's storage trie
 */
static bool compute_storage_root(account_object_t *account)
{
    if (!account)
        return false;

    // If no storage, use empty trie root
    if (!account->storage_cache)
    {
        account->storage_root = HASH_EMPTY_STORAGE;
        return true;
    }

    // Create MPT for storage
    mpt_state_t storage_mpt = {0};
    if (!mpt_init(&storage_mpt))
    {
        return false;
    }

    // Iterate through storage and insert into MPT
    art_tree_t *storage_tree = (art_tree_t *)account->storage_cache;
    art_iterator_t *iter = art_iterator_create(storage_tree);
    if (!iter)
    {
        mpt_destroy(&storage_mpt);
        return false;
    }

    bool success = true;
    while (art_iterator_next(iter))
    {
        size_t key_len, value_len;
        const uint8_t *key = art_iterator_key(iter, &key_len);
        const void *value = art_iterator_value(iter, &value_len);

        if (key && value && key_len == 32 && value_len == sizeof(uint256_t))
        {
            const uint256_t *storage_value = (const uint256_t *)value;
            
            // Skip zero values (they don't go into the storage trie)
            uint256_t zero = uint256_from_uint64(0);
            if (uint256_eq(storage_value, &zero))
                continue;

            // RLP encode the storage value (remove leading zeros)
            uint8_t value_bytes[32];
            uint256_to_bytes(storage_value, value_bytes);
            
            int start = 0;
            while (start < 32 && value_bytes[start] == 0) start++;
            
            if (start < 32)  // Non-zero value
            {
                // Ethereum uses secure trie: hash the key with Keccak256
                hash_t key_hash = hash_keccak256(key, 32);
                
                // RLP encode the value bytes directly
                // For single byte strings, RLP just uses the byte if < 0x80
                // Otherwise it's 0x80+len followed by bytes
                uint8_t rlp_buffer[34];  // Max: 1 byte prefix + 1 byte length + 32 bytes data
                size_t rlp_len;
                size_t value_len = 32 - start;
                
                if (value_len == 1 && value_bytes[start] < 0x80) {
                    // Single byte < 0x80 encodes as itself
                    rlp_buffer[0] = value_bytes[start];
                    rlp_len = 1;
                } else if (value_len <= 55) {
                    // Short string: 0x80 + len, then data
                    rlp_buffer[0] = 0x80 + value_len;
                    memcpy(rlp_buffer + 1, value_bytes + start, value_len);
                    rlp_len = 1 + value_len;
                } else {
                    // Long string (shouldn't happen for uint256, but handle it)
                    rlp_buffer[0] = 0xb7 + 1;  // 0xb7 + length of length
                    rlp_buffer[1] = value_len;
                    memcpy(rlp_buffer + 2, value_bytes + start, value_len);
                    rlp_len = 2 + value_len;
                }
                
                // DEBUG: Print first few insertions
                static int debug_count = 0;
                if (debug_count < 3) {
                    printf("    [DEBUG] Storage insert %d: raw_key=", debug_count);
                    for (int i = 0; i < 32; i++) printf("%02x", key[i]);
                    printf(", key_hash=");
                    for (int i = 0; i < 8; i++) printf("%02x", key_hash.bytes[i]);
                    printf("..., value_rlp=");
                    for (size_t i = 0; i < rlp_len; i++) printf("%02x", rlp_buffer[i]);
                    printf("\n");
                    debug_count++;
                }
                
                // Insert RLP-encoded value
                if (!mpt_insert(&storage_mpt, key_hash.bytes, 32, 
                               rlp_buffer, rlp_len))
                {
                    success = false;
                    break;
                }
            }
        }
    }

    art_iterator_destroy(iter);

    if (success)
    {
        const mpt_hash_t *root = mpt_root(&storage_mpt);
        if (root)
        {
            memcpy(account->storage_root.bytes, root->bytes, 32);
        }
        else
        {
            success = false;
        }
    }

    mpt_destroy(&storage_mpt);
    return success;
}

/**
 * Encode account for MPT storage
 * RLP format: [nonce, balance, storage_root, code_hash]
 */
static bool encode_account(account_object_t *account, uint8_t **out_data, size_t *out_len)
{
    if (!account || !out_data || !out_len)
        return false;

    // Create RLP list: [nonce, balance, storage_root, code_hash]
    rlp_item_t *list = rlp_list_new();
    if (!list)
        return false;

    // 1. Nonce
    rlp_list_append(list, rlp_uint64(account->nonce));

    // 2. Balance (uint256 as big-endian bytes, remove leading zeros)
    uint8_t balance_bytes[32];
    uint256_to_bytes(&account->balance, balance_bytes);
    
    int start = 0;
    while (start < 32 && balance_bytes[start] == 0) start++;
    
    if (start == 32) {
        rlp_list_append(list, rlp_string(NULL, 0));
    } else {
        rlp_list_append(list, rlp_string(balance_bytes + start, 32 - start));
    }

    // 3. Storage root (32 bytes)
    rlp_list_append(list, rlp_string(account->storage_root.bytes, 32));

    // 4. Code hash (32 bytes)
    rlp_list_append(list, rlp_string(account->code_hash.bytes, 32));

    // Encode to bytes
    bytes_t encoded = rlp_encode(list);
    rlp_item_free(list);

    if (encoded.data == NULL || encoded.len == 0)
    {
        return false;
    }

    *out_data = encoded.data;
    *out_len = encoded.len;
    return true;
}

bool state_db_compute_state_root(state_db_t *db)
{
    if (!db)
        return false;

    // Clear and rebuild MPT with all existing accounts
    mpt_destroy(&db->mpt);
    if (!mpt_init(&db->mpt))
    {
        LOG_STATE_ERROR("state_db_compute_state_root: failed to reinit MPT");
        return false;
    }

    // Iterate through all accounts and insert into MPT
    state_cache_iterator_t *iter = state_cache_iterator_create(&db->cache);
    if (!iter)
    {
        LOG_STATE_ERROR("state_db_compute_state_root: failed to create iterator");
        return false;
    }

    size_t account_count = 0;
    bool success = true;

    printf("\nDEBUG: Computing state root from accounts:\n");
    printf("========================================\n");

    while (state_cache_iterator_next(iter))
    {
        const address_t *addr = state_cache_iterator_address(iter);
        const account_object_t *const_account = state_cache_iterator_account(iter);
        account_object_t *account = (account_object_t *)const_account;

        if (!addr || !account || !account->exists)
            continue;

        // Debug: Print account details
        printf("Account %zu: 0x", account_count);
        for (int i = 0; i < 20; i++) printf("%02x", addr->bytes[i]);
        printf("\n  Nonce: %lu\n", account->nonce);
        printf("  Balance: ");
        char *balance_hex = uint256_to_hex(&account->balance);
        printf("%s\n", balance_hex ? balance_hex : "NULL");
        free(balance_hex);
        
        printf("  Code hash: 0x");
        for (int i = 0; i < 32; i++) printf("%02x", account->code_hash.bytes[i]);
        printf("\n");
        
        printf("  Code size: %zu bytes\n", account->code_size);

        // Count storage entries
        size_t storage_count = 0;
        if (account->storage_cache)
        {
            art_tree_t *storage_tree = (art_tree_t *)account->storage_cache;
            art_iterator_t *storage_iter = art_iterator_create(storage_tree);
            if (storage_iter)
            {
                while (art_iterator_next(storage_iter))
                {
                    size_t key_len, value_len;
                    const uint8_t *key = art_iterator_key(storage_iter, &key_len);
                    const void *value = art_iterator_value(storage_iter, &value_len);
                    
                    if (key && value && key_len == 32 && value_len == sizeof(uint256_t))
                    {
                        const uint256_t *storage_value = (const uint256_t *)value;
                        uint256_t zero = uint256_from_uint64(0);
                        if (!uint256_eq(storage_value, &zero))
                        {
                            if (storage_count < 20)  // Print first 20 storage slots
                            {
                                printf("    Storage[");
                                for (int i = 0; i < 32; i++) printf("%02x", key[i]);
                                printf("] = ");
                                char *val_hex = uint256_to_hex(storage_value);
                                printf("%s\n", val_hex ? val_hex : "NULL");
                                free(val_hex);
                            }
                            storage_count++;
                        }
                    }
                }
                art_iterator_destroy(storage_iter);
            }
        }
        printf("  Storage entries: %zu\n", storage_count);

        // Compute storage root before encoding
        if (!compute_storage_root(account))
        {
            LOG_STATE_ERROR("state_db_compute_state_root: failed to compute storage root");
            success = false;
            break;
        }

        printf("  Storage root: 0x");
        for (int i = 0; i < 32; i++) printf("%02x", account->storage_root.bytes[i]);
        printf("\n");

        // Encode account to RLP
        uint8_t *encoded_account = NULL;
        size_t encoded_len = 0;
        if (!encode_account(account, &encoded_account, &encoded_len))
        {
            LOG_STATE_ERROR("state_db_compute_state_root: failed to encode account");
            success = false;
            break;
        }

        printf("  RLP encoded: %zu bytes\n", encoded_len);

        // Ethereum uses secure trie: hash address with Keccak256
        hash_t addr_hash = hash_keccak256(addr->bytes, 20);
        
        if (!mpt_insert(&db->mpt, addr_hash.bytes, 32, encoded_account, encoded_len))
        {
            LOG_STATE_ERROR("state_db_compute_state_root: failed to insert into MPT");
            free(encoded_account);
            success = false;
            break;
        }

        free(encoded_account);
        account_count++;
        printf("\n");
    }

    state_cache_iterator_destroy(iter);

    if (!success)
    {
        return false;
    }

    // Get MPT root and store it
    const mpt_hash_t *root = mpt_root(&db->mpt);
    if (root)
    {
        memcpy(db->state_root.bytes, root->bytes, 32);
        printf("========================================\n");
        printf("Total accounts: %zu\n", account_count);
        printf("Computed MPT root: 0x");
        for (int i = 0; i < 32; i++) printf("%02x", root->bytes[i]);
        printf("\n");
        LOG_STATE_DEBUG("state_db_compute_state_root: computed root from %zu accounts", account_count);
    }
    else
    {
        LOG_STATE_ERROR("state_db_compute_state_root: failed to get MPT root");
        return false;
    }

    return true;
}

void state_db_get_state_root(const state_db_t *db, hash_t *root)
{
    if (!db || !root)
        return;

    *root = db->state_root;
}

void state_db_set_state_root(state_db_t *db, const hash_t *root)
{
    if (!db || !root)
        return;

    db->state_root = *root;
}

//==============================================================================
// Block Context
//==============================================================================

uint64_t state_db_get_block_number(const state_db_t *db)
{
    return db ? db->block_number : 0;
}

void state_db_set_block_number(state_db_t *db, uint64_t block_number)
{
    if (!db)
        return;

    db->block_number = block_number;
}

//==============================================================================
// Utilities
//==============================================================================

size_t state_db_num_accounts(const state_db_t *db)
{
    return db ? state_cache_size(&db->cache) : 0;
}

size_t state_db_num_dirty(const state_db_t *db)
{
    return db ? state_cache_dirty_count(&db->cache) : 0;
}

size_t state_db_journal_size(const state_db_t *db)
{
    return db ? state_journal_size(&db->journal) : 0;
}
