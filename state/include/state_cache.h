#ifndef STATE_CACHE_H
#define STATE_CACHE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "address.h"
#include "uint256.h"
#include "hash.h"

/**
 * State Cache Layer
 *
 * Provides in-memory caching of account states with dirty tracking.
 * Built on top of ART for efficient address-based lookups.
 *
 * Features:
 * - Fast account lookups by address (O(k) where k=20 bytes)
 * - Dirty flag tracking for modified accounts
 * - Delete marking for account removal
 * - Storage slot caching per account
 * - Ordered iteration for MPT root computation
 */

/**
 * Account object - cached representation of an Ethereum account
 */
typedef struct
{
    address_t address;   // 20-byte account address
    uint256_t balance;   // Account balance in wei
    uint64_t nonce;      // Transaction nonce
    hash_t code_hash;    // Hash of contract code (empty for EOA)
    hash_t storage_root; // Root of account storage trie

    // Contract code
    uint8_t *code;       // Contract bytecode (NULL for EOA)
    size_t code_size;    // Size of contract code

    // Cache metadata
    bool exists;  // True if account exists in state
    bool dirty;   // True if modified since last commit
    bool deleted; // True if marked for deletion

    // Storage cache
    void *storage_cache; // ART tree: storage_key -> uint256_t value
    bool storage_dirty;  // True if any storage slots modified
} account_object_t;

/**
 * State cache - main cache structure
 */
typedef struct
{
    void *accounts;      // ART tree: address -> account_object_t*
    size_t num_accounts; // Number of cached accounts
    size_t num_dirty;    // Number of dirty accounts
} state_cache_t;

/**
 * Initialize a new state cache
 *
 * @param cache Pointer to cache structure to initialize
 * @return true on success, false on failure
 */
bool state_cache_init(state_cache_t *cache);

/**
 * Destroy state cache and free all resources
 *
 * @param cache Pointer to cache to destroy
 */
void state_cache_destroy(state_cache_t *cache);

/**
 * Get or create an account object
 * If account doesn't exist in cache, creates a new empty account
 *
 * @param cache Pointer to state cache
 * @param addr Account address
 * @return Pointer to account object, or NULL on error
 */
account_object_t *state_cache_get_account(state_cache_t *cache, const address_t *addr);

/**
 * Check if an account exists in the cache
 *
 * @param cache Pointer to state cache
 * @param addr Account address
 * @return true if account exists, false otherwise
 */
bool state_cache_has_account(const state_cache_t *cache, const address_t *addr);

/**
 * Mark an account as dirty (modified)
 *
 * @param cache Pointer to state cache
 * @param account Pointer to account object to mark dirty
 */
void state_cache_mark_dirty(state_cache_t *cache, account_object_t *account);

/**
 * Delete an account from the cache
 * Marks account as deleted but keeps in cache for journal rollback
 *
 * @param cache Pointer to state cache
 * @param addr Account address
 * @return true if account was found and marked deleted
 */
bool state_cache_delete_account(state_cache_t *cache, const address_t *addr);

/**
 * Get a storage value for an account
 *
 * @param account Pointer to account object
 * @param key Storage key (32 bytes)
 * @param value Output pointer for storage value
 * @return true if value found, false if slot is empty
 */
bool state_cache_get_storage(const account_object_t *account, const uint256_t *key,
                             uint256_t *value);

/**
 * Set a storage value for an account
 *
 * @param cache Pointer to state cache
 * @param account Pointer to account object
 * @param key Storage key (32 bytes)
 * @param value Storage value (32 bytes)
 * @return true on success, false on failure
 */
bool state_cache_set_storage(state_cache_t *cache, account_object_t *account,
                             const uint256_t *key, const uint256_t *value);

/**
 * Get number of cached accounts
 *
 * @param cache Pointer to state cache
 * @return Number of accounts in cache
 */
size_t state_cache_size(const state_cache_t *cache);

/**
 * Get number of dirty (modified) accounts
 *
 * @param cache Pointer to state cache
 * @return Number of dirty accounts
 */
size_t state_cache_dirty_count(const state_cache_t *cache);

/**
 * Clear all dirty flags (called after commit)
 *
 * @param cache Pointer to state cache
 */
void state_cache_clear_dirty(state_cache_t *cache);

/**
 * Reset cache to empty state
 * Removes all accounts and frees memory
 *
 * @param cache Pointer to state cache
 */
void state_cache_reset(state_cache_t *cache);

/**
 * Cache iterator for traversing accounts
 */
typedef struct state_cache_iterator state_cache_iterator_t;

/**
 * Create iterator for cache traversal
 *
 * @param cache Pointer to state cache
 * @return Iterator pointer, or NULL on error
 */
state_cache_iterator_t *state_cache_iterator_create(const state_cache_t *cache);

/**
 * Move iterator to next account
 *
 * @param iter Iterator pointer
 * @return true if next account exists, false if end reached
 */
bool state_cache_iterator_next(state_cache_iterator_t *iter);

/**
 * Get current account from iterator
 *
 * @param iter Iterator pointer
 * @return Pointer to current account object, or NULL
 */
const account_object_t *state_cache_iterator_account(const state_cache_iterator_t *iter);

/**
 * Get current address from iterator
 *
 * @param iter Iterator pointer
 * @return Pointer to current address, or NULL
 */
const address_t *state_cache_iterator_address(const state_cache_iterator_t *iter);

/**
 * Destroy iterator
 *
 * @param iter Iterator pointer to destroy
 */
void state_cache_iterator_destroy(state_cache_iterator_t *iter);

#endif // STATE_CACHE_H
