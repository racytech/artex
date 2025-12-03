/**
 * State Cache Implementation
 *
 * In-memory cache for Ethereum account state using ART trees.
 * Provides fast lookups and dirty tracking for state modifications.
 */

#include "state_cache.h"
#include "art.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

/**
 * Iterator structure
 */
struct state_cache_iterator
{
    art_iterator_t *art_iter;
};

//==============================================================================
// Internal Helper Functions
//==============================================================================

/**
 * Create a new empty account object
 */
static account_object_t *create_account_object(const address_t *addr)
{
    account_object_t *account = calloc(1, sizeof(account_object_t));
    if (!account)
        return NULL;

    memcpy(&account->address, addr, sizeof(address_t));
    uint256_zero(&account->balance);
    account->nonce = 0;
    account->code_hash = HASH_EMPTY_CODE;
    account->storage_root = HASH_EMPTY_STORAGE;

    account->code = NULL;
    account->code_size = 0;

    account->exists = false;
    account->dirty = false;
    account->deleted = false;
    account->storage_dirty = false;

    // Initialize storage cache
    art_tree_t *storage = malloc(sizeof(art_tree_t));
    if (!storage)
    {
        free(account);
        return NULL;
    }

    if (!art_tree_init(storage))
    {
        free(storage);
        free(account);
        return NULL;
    }

    account->storage_cache = storage;
    return account;
}

/**
 * Destroy an account object and free resources
 */
static void destroy_account_object(account_object_t *account)
{
    if (!account)
        return;

    if (account->storage_cache)
    {
        art_tree_t *storage = (art_tree_t *)account->storage_cache;
        art_tree_destroy(storage);
        free(storage);
    }

    // Free contract code if exists
    if (account->code)
    {
        free(account->code);
    }

    free(account);
}

//==============================================================================
// Public API Implementation
//==============================================================================

bool state_cache_init(state_cache_t *cache)
{
    if (!cache)
    {
        return false;
    }

    art_tree_t *tree = malloc(sizeof(art_tree_t));
    if (!tree)
    {
        LOG_STATE_ERROR("state_cache_init: failed to allocate tree");
        return false;
    }

    if (!art_tree_init(tree))
    {
        free(tree);
        LOG_STATE_ERROR("state_cache_init: art_tree_init failed");
        return false;
    }

    cache->accounts = tree;
    cache->num_accounts = 0;
    cache->num_dirty = 0;

    LOG_STATE_DEBUG("state_cache_init: cache initialized, cache->accounts=%p", cache->accounts);
    return true;
}

void state_cache_destroy(state_cache_t *cache)
{
    if (!cache || !cache->accounts)
        return;

    art_tree_t *tree = (art_tree_t *)cache->accounts;

    // Destroy all account objects
    art_iterator_t *iter = art_iterator_create(tree);
    if (iter)
    {
        while (art_iterator_next(iter))
        {
            size_t value_len;
            void *value = art_iterator_value(iter, &value_len);
            if (value && value_len == sizeof(account_object_t *))
            {
                account_object_t *account;
                memcpy(&account, value, sizeof(account_object_t *));
                if (account)
                {
                    destroy_account_object(account);
                }
            }
        }
        art_iterator_destroy(iter);
    }

    art_tree_destroy(tree);
    free(tree);

    cache->accounts = NULL;
    cache->num_accounts = 0;
    cache->num_dirty = 0;

    LOG_STATE_DEBUG("state_cache_destroy: cache destroyed");
}

account_object_t *state_cache_get_account(state_cache_t *cache, const address_t *addr)
{
    if (!cache || !addr)
        return NULL;

    art_tree_t *tree = (art_tree_t *)cache->accounts;

    if (!tree)
    {
        LOG_STATE_ERROR("state_cache_get_account: tree is NULL, cache=%p, cache->accounts=%p",
                        (void *)cache, cache->accounts);
        return NULL;
    }

    // Try to find existing account
    size_t value_len;
    const void *value = art_get(tree, addr->bytes, sizeof(address_t), &value_len);

    if (value && value_len == sizeof(account_object_t *))
    {
        // Retrieve the stored pointer
        account_object_t *account;
        memcpy(&account, value, sizeof(account_object_t *));
        LOG_STATE_DEBUG("state_cache_get_account: found existing account");
        return account;
    }

    // Account doesn't exist - create new account object
    account_object_t *account = create_account_object(addr);
    if (!account)
    {
        LOG_STATE_ERROR("state_cache_get_account: failed to create account object");
        return NULL;
    }

    // Allocate memory to store the pointer value
    account_object_t **ptr_storage = malloc(sizeof(account_object_t *));
    if (!ptr_storage)
    {
        destroy_account_object(account);
        LOG_STATE_ERROR("state_cache_get_account: failed to allocate ptr_storage");
        return NULL;
    }
    *ptr_storage = account;

    LOG_STATE_DEBUG("state_cache_get_account: attempting to insert, tree=%p", (void *)tree);

    // Store the pointer in ART
    if (!art_insert(tree, addr->bytes, sizeof(address_t), ptr_storage, sizeof(account_object_t *)))
    {
        free(ptr_storage);
        destroy_account_object(account);
        LOG_STATE_ERROR("state_cache_get_account: failed to insert account into cache");
        return NULL;
    }

    // ART made a copy, so we can free our temp storage
    free(ptr_storage);

    cache->num_accounts++;

    LOG_STATE_DEBUG("state_cache_get_account: created new account in cache");
    return account;
}

bool state_cache_has_account(const state_cache_t *cache, const address_t *addr)
{
    if (!cache || !addr)
        return false;

    art_tree_t *tree = (art_tree_t *)cache->accounts;
    size_t value_len;
    void *value = art_get(tree, addr->bytes, sizeof(address_t), &value_len);

    if (value && value_len == sizeof(account_object_t *))
    {
        account_object_t *account;
        memcpy(&account, value, sizeof(account_object_t *));
        return (account && account->exists && !account->deleted);
    }

    return false;
}

void state_cache_mark_dirty(state_cache_t *cache, account_object_t *account)
{
    if (!cache || !account)
        return;

    if (!account->dirty)
    {
        account->dirty = true;
        cache->num_dirty++;
        LOG_STATE_DEBUG("state_cache_mark_dirty: account marked dirty");
    }
}

bool state_cache_delete_account(state_cache_t *cache, const address_t *addr)
{
    if (!cache || !addr)
        return false;

    account_object_t *account = state_cache_get_account(cache, addr);
    if (!account)
        return false;

    if (!account->deleted)
    {
        account->deleted = true;
        account->exists = false;
        state_cache_mark_dirty(cache, account);
        LOG_STATE_DEBUG("state_cache_delete_account: account marked for deletion");
    }

    return true;
}

bool state_cache_get_storage(const account_object_t *account, const uint256_t *key,
                             uint256_t *value)
{
    if (!account || !key || !value)
        return false;

    art_tree_t *storage = (art_tree_t *)account->storage_cache;
    if (!storage)
        return false;

    // Convert key to big-endian bytes for lookup
    uint8_t key_bytes[32];
    uint256_to_bytes(key, key_bytes);

    size_t value_len;
    const uint256_t *stored_value = (const uint256_t *)art_get(storage, key_bytes,
                                                               32, &value_len);

    if (!stored_value)
    {
        // Slot is empty (return zero)
        uint256_zero(value);
        return false;
    }

    memcpy(value, stored_value, sizeof(uint256_t));
    return true;
}

bool state_cache_set_storage(state_cache_t *cache, account_object_t *account,
                             const uint256_t *key, const uint256_t *value)
{
    if (!cache || !account || !key || !value)
        return false;

    art_tree_t *storage = (art_tree_t *)account->storage_cache;
    if (!storage)
        return false;

    // Convert key to big-endian bytes for storage
    uint8_t key_bytes[32];
    uint256_to_bytes(key, key_bytes);

    // Insert/update storage slot
    if (!art_insert(storage, key_bytes, 32,
                    value, sizeof(uint256_t)))
    {
        LOG_STATE_ERROR("state_cache_set_storage: failed to insert storage slot");
        return false;
    }

    // Mark account and storage as dirty
    account->storage_dirty = true;
    state_cache_mark_dirty(cache, account);

    LOG_STATE_DEBUG("state_cache_set_storage: storage slot updated");
    return true;
}

size_t state_cache_size(const state_cache_t *cache)
{
    return cache ? cache->num_accounts : 0;
}

size_t state_cache_dirty_count(const state_cache_t *cache)
{
    return cache ? cache->num_dirty : 0;
}

void state_cache_clear_dirty(state_cache_t *cache)
{
    if (!cache || !cache->accounts)
        return;

    art_tree_t *tree = (art_tree_t *)cache->accounts;
    art_iterator_t *iter = art_iterator_create(tree);

    if (iter)
    {
        while (art_iterator_next(iter))
        {
            size_t value_len;
            void *value = art_iterator_value(iter, &value_len);
            if (value && value_len == sizeof(account_object_t *))
            {
                account_object_t *account;
                memcpy(&account, value, sizeof(account_object_t *));
                if (account)
                {
                    account->dirty = false;
                    account->storage_dirty = false;
                }
            }
        }
        art_iterator_destroy(iter);
    }

    cache->num_dirty = 0;
    LOG_STATE_DEBUG("state_cache_clear_dirty: all dirty flags cleared");
}

void state_cache_reset(state_cache_t *cache)
{
    if (!cache || !cache->accounts)
        return;

    // Destroy and reinitialize
    state_cache_destroy(cache);
    state_cache_init(cache);

    LOG_STATE_DEBUG("state_cache_reset: cache reset to empty state");
}

//==============================================================================
// Iterator Implementation
//==============================================================================

state_cache_iterator_t *state_cache_iterator_create(const state_cache_t *cache)
{
    if (!cache || !cache->accounts)
        return NULL;

    state_cache_iterator_t *iter = malloc(sizeof(state_cache_iterator_t));
    if (!iter)
        return NULL;

    art_tree_t *tree = (art_tree_t *)cache->accounts;
    iter->art_iter = art_iterator_create(tree);

    if (!iter->art_iter)
    {
        free(iter);
        return NULL;
    }

    return iter;
}

bool state_cache_iterator_next(state_cache_iterator_t *iter)
{
    if (!iter || !iter->art_iter)
        return false;
    return art_iterator_next(iter->art_iter);
}

const account_object_t *state_cache_iterator_account(const state_cache_iterator_t *iter)
{
    if (!iter || !iter->art_iter)
        return NULL;

    size_t value_len;
    void *value = art_iterator_value(iter->art_iter, &value_len);

    if (value && value_len == sizeof(account_object_t *))
    {
        account_object_t *account;
        memcpy(&account, value, sizeof(account_object_t *));
        return account;
    }

    return NULL;
}

const address_t *state_cache_iterator_address(const state_cache_iterator_t *iter)
{
    if (!iter || !iter->art_iter)
        return NULL;

    size_t key_len;
    const uint8_t *key = art_iterator_key(iter->art_iter, &key_len);

    if (key && key_len == 20)
    {
        return (const address_t *)key;
    }

    return NULL;
}

void state_cache_iterator_destroy(state_cache_iterator_t *iter)
{
    if (!iter)
        return;

    if (iter->art_iter)
    {
        art_iterator_destroy(iter->art_iter);
    }

    free(iter);
}
