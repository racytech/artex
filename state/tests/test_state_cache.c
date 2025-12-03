/**
 * State Cache Unit Tests
 */

#include "state_cache.h"
#include "logger.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>

static void test_cache_init_destroy(void) {
    printf("Test: Cache init/destroy... ");
    
    state_cache_t cache = {0};
    assert(state_cache_init(&cache));
    assert(state_cache_size(&cache) == 0);
    assert(state_cache_dirty_count(&cache) == 0);
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_get_account(void) {
    printf("Test: Get account... ");
    
    state_cache_t cache = {0};
    bool init_result = state_cache_init(&cache);
    assert(init_result);
    
    address_t addr = {0};
    addr.bytes[0] = 0x12;
    addr.bytes[19] = 0x34;
    
    // Get non-existent account (should create new)
    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(account != NULL);
    assert(!account->exists);
    assert(!account->dirty);
    assert(account->nonce == 0);
    assert(state_cache_size(&cache) == 1);
    
    // Get same account again (should return existing)
    account_object_t *account2 = state_cache_get_account(&cache, &addr);
    assert(account2 == account); // Same pointer
    assert(state_cache_size(&cache) == 1); // Still 1 account
    (void)account2; // Suppress warning
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_mark_dirty(void) {
    printf("Test: Mark account dirty... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    address_t addr = {0};
    addr.bytes[0] = 0xAA;
    
    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(!account->dirty);
    assert(state_cache_dirty_count(&cache) == 0);
    
    // Mark dirty
    state_cache_mark_dirty(&cache, account);
    assert(account->dirty);
    assert(state_cache_dirty_count(&cache) == 1);
    
    // Mark dirty again (should not increase count)
    state_cache_mark_dirty(&cache, account);
    assert(state_cache_dirty_count(&cache) == 1);
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_account_balance(void) {
    printf("Test: Account balance operations... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    address_t addr = {0};
    addr.bytes[0] = 0xBB;
    
    account_object_t *account = state_cache_get_account(&cache, &addr);
    
    // Check initial balance is zero
    assert(uint256_is_zero(&account->balance));
    
    // Set balance
    uint256_t balance = uint256_from_uint64(1000000);
    account->balance = balance;
    account->exists = true;
    state_cache_mark_dirty(&cache, account);
    
    assert(!uint256_is_zero(&account->balance));
    assert(account->dirty);
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_storage_operations(void) {
    printf("Test: Storage operations... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    address_t addr = {0};
    addr.bytes[0] = 0xCC;
    
    account_object_t *account = state_cache_get_account(&cache, &addr);
    
    // Set storage slot
    uint256_t key = uint256_from_uint64(42);
    uint256_t value = uint256_from_uint64(9999);
    
    assert(state_cache_set_storage(&cache, account, &key, &value));
    assert(account->storage_dirty);
    assert(account->dirty);
    
    // Get storage slot
    uint256_t retrieved;
    assert(state_cache_get_storage(account, &key, &retrieved));
    assert(uint256_eq(&value, &retrieved));
    
    // Get non-existent slot (should return zero)
    uint256_t key2 = uint256_from_uint64(100);
    assert(!state_cache_get_storage(account, &key2, &retrieved));
    assert(uint256_is_zero(&retrieved));
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_delete_account(void) {
    printf("Test: Delete account... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    address_t addr = {0};
    addr.bytes[0] = 0xDD;
    
    account_object_t *account = state_cache_get_account(&cache, &addr);
    account->exists = true;
    
    assert(state_cache_has_account(&cache, &addr));
    
    // Delete account
    assert(state_cache_delete_account(&cache, &addr));
    assert(account->deleted);
    assert(!account->exists);
    assert(account->dirty);
    assert(!state_cache_has_account(&cache, &addr));
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_multiple_accounts(void) {
    printf("Test: Multiple accounts... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    // Create 10 accounts
    for (int i = 0; i < 10; i++) {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;
        
        account_object_t *account = state_cache_get_account(&cache, &addr);
        account->balance = uint256_from_uint64(i * 1000);
        account->nonce = i;
        account->exists = true;
        state_cache_mark_dirty(&cache, account);
    }
    
    assert(state_cache_size(&cache) == 10);
    assert(state_cache_dirty_count(&cache) == 10);
    
    // Verify accounts
    for (int i = 0; i < 10; i++) {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;
        
        assert(state_cache_has_account(&cache, &addr));
        account_object_t *account = state_cache_get_account(&cache, &addr);
        assert(account->nonce == (uint64_t)i);
    }
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_iterator(void) {
    printf("Test: Cache iterator... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    // Create 5 accounts
    for (int i = 0; i < 5; i++) {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;
        account_object_t *account = state_cache_get_account(&cache, &addr);
        account->exists = true;
    }
    
    // Iterate and count
    state_cache_iterator_t *iter = state_cache_iterator_create(&cache);
    assert(iter != NULL);
    
    int count = 0;
    while (state_cache_iterator_next(iter)) {
        const account_object_t *account = state_cache_iterator_account(iter);
        assert(account != NULL);
        assert(account->exists);
        count++;
    }
    
    assert(count == 5);
    
    state_cache_iterator_destroy(iter);
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

static void test_clear_dirty(void) {
    printf("Test: Clear dirty flags... ");
    
    state_cache_t cache = {0};
    bool result = state_cache_init(&cache);
    assert(result);
    
    // Create and dirty some accounts
    for (int i = 0; i < 3; i++) {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;
        account_object_t *account = state_cache_get_account(&cache, &addr);
        state_cache_mark_dirty(&cache, account);
    }
    
    assert(state_cache_dirty_count(&cache) == 3);
    
    // Clear all dirty flags
    state_cache_clear_dirty(&cache);
    assert(state_cache_dirty_count(&cache) == 0);
    
    // Verify accounts still exist but not dirty
    for (int i = 0; i < 3; i++) {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;
        account_object_t *account = state_cache_get_account(&cache, &addr);
        assert(!account->dirty);
    }
    
    state_cache_destroy(&cache);
    printf("PASSED\n");
}

int main(void) {
    printf("=== State Cache Test Suite ===\n\n");
    
    log_init(LOG_LEVEL_DEBUG, stdout);
    
    test_cache_init_destroy();
    test_get_account();
    test_mark_dirty();
    test_account_balance();
    test_storage_operations();
    test_delete_account();
    test_multiple_accounts();
    test_iterator();
    test_clear_dirty();
    
    printf("\n=== All tests passed! ===\n");
    return 0;
}
