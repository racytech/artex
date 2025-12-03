/**
 * State Journal Tests
 */

#include "state_journal.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

//==============================================================================
// Test Helpers
//==============================================================================

static void print_test_header(const char *name)
{
    printf("Test: %s... ", name);
}

static void print_test_pass(void)
{
    printf("PASSED\n");
}

//==============================================================================
// Tests
//==============================================================================

static void test_journal_init_destroy(void)
{
    print_test_header("Journal init/destroy");

    state_cache_t cache = {0};
    assert(state_cache_init(&cache));

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    assert(journal.cache == &cache);
    assert(journal.entries != NULL);
    assert(journal.snapshots != NULL);
    assert(journal.num_entries == 0);
    assert(journal.num_snapshots == 0);

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_snapshot_and_revert(void)
{
    print_test_header("Snapshot and revert");

    state_cache_t cache = {0};
    assert(state_cache_init(&cache));

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Create snapshot 0
    uint32_t snap0 = state_journal_snapshot(&journal);
    assert(snap0 == 0);
    assert(state_journal_snapshot_count(&journal) == 1);

    // Add some entries
    address_t addr1 = {0};
    addr1.bytes[0] = 0x01;
    assert(state_journal_account_created(&journal, &addr1));
    assert(state_journal_size(&journal) == 1);

    // Create snapshot 1
    uint32_t snap1 = state_journal_snapshot(&journal);
    assert(snap1 == 1);
    assert(state_journal_snapshot_count(&journal) == 2);

    // Add more entries
    address_t addr2 = {0};
    addr2.bytes[0] = 0x02;
    assert(state_journal_account_created(&journal, &addr2));
    assert(state_journal_size(&journal) == 2);

    // Revert to snapshot 1
    assert(state_journal_revert_to_snapshot(&journal, snap1));
    assert(state_journal_size(&journal) == 1);
    assert(state_journal_snapshot_count(&journal) == 1);

    // Revert to snapshot 0
    assert(state_journal_revert_to_snapshot(&journal, snap0));
    assert(state_journal_size(&journal) == 0);
    assert(state_journal_snapshot_count(&journal) == 0);

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_account_created_rollback(void)
{
    print_test_header("Account created rollback");

    state_cache_t cache = {0};
    bool init_result = state_cache_init(&cache);
    printf("result=%d, cache.accounts=%p\n", init_result, cache.accounts);
    assert(init_result);

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Create an account in the cache
    address_t addr = {0};
    addr.bytes[0] = 0xAA;

    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(account != NULL);

    // Record creation in journal
    assert(state_journal_account_created(&journal, &addr));

    // Set account properties
    account->balance = uint256_from_uint64(100);
    account->nonce = 1;
    account->exists = true;

    // Verify account exists
    assert(state_cache_has_account(&cache, &addr));

    // Rollback
    assert(state_journal_rollback(&journal));

    // Account should be deleted
    assert(!state_cache_has_account(&cache, &addr));

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_account_modified_rollback(void)
{
    print_test_header("Account modified rollback");

    state_cache_t cache = {0};
    bool init_result = state_cache_init(&cache);
    printf("result=%d, cache.accounts=%p\n", init_result, cache.accounts);
    assert(init_result);

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Create an account with initial state
    address_t addr = {0};
    addr.bytes[0] = 0xBB;

    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(account != NULL);

    uint256_t original_balance = uint256_from_uint64(500);
    uint64_t original_nonce = 5;

    account->balance = original_balance;
    account->nonce = original_nonce;
    account->exists = true;

    // Save old state to journal before modification
    assert(state_journal_account_modified(&journal, &addr, account));

    // Modify account
    account->balance = uint256_from_uint64(1000);
    account->nonce = 10;

    // Verify modifications
    account = state_cache_get_account(&cache, &addr);
    assert(uint256_eq(&account->balance, &uint256_from_uint64(1000)));
    assert(account->nonce == 10);

    // Rollback
    assert(state_journal_rollback(&journal));

    // Account should be restored to original state
    account = state_cache_get_account(&cache, &addr);
    assert(uint256_eq(&account->balance, &original_balance));
    assert(account->nonce == original_nonce);

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_storage_changed_rollback(void)
{
    print_test_header("Storage changed rollback");

    state_cache_t cache = {0};
    bool init_result = state_cache_init(&cache);
    assert(init_result);

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Create an account
    address_t addr = {0};
    addr.bytes[0] = 0xCC;

    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(account != NULL);
    account->exists = true;

    // Set initial storage value
    uint256_t key = uint256_from_uint64(42);
    uint256_t old_value = uint256_from_uint64(100);
    assert(state_cache_set_storage(&cache, account, &key, &old_value));

    // Record old value in journal
    assert(state_journal_storage_changed(&journal, &addr, &key, &old_value));

    // Change storage
    uint256_t new_value = uint256_from_uint64(200);
    assert(state_cache_set_storage(&cache, account, &key, &new_value));

    // Verify new value
    uint256_t retrieved;
    assert(state_cache_get_storage(account, &key, &retrieved));
    assert(uint256_eq(&retrieved, &new_value));

    // Rollback
    assert(state_journal_rollback(&journal));

    // Storage should be restored
    assert(state_cache_get_storage(account, &key, &retrieved));
    assert(uint256_eq(&retrieved, &old_value));

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_multiple_snapshots(void)
{
    print_test_header("Multiple snapshots");

    state_cache_t cache = {0};
    assert(state_cache_init(&cache));

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    address_t addr = {0};
    addr.bytes[0] = 0xDD;

    account_object_t *account = state_cache_get_account(&cache, &addr);
    account->balance = uint256_from_uint64(0);
    account->exists = true;

    // Snapshot 0: balance = 0
    uint32_t snap0 = state_journal_snapshot(&journal);

    // Modify and record
    assert(state_journal_account_modified(&journal, &addr, account));
    account->balance = uint256_from_uint64(100);

    // Snapshot 1: balance = 100
    uint32_t snap1 = state_journal_snapshot(&journal);

    // Modify again
    assert(state_journal_account_modified(&journal, &addr, account));
    account->balance = uint256_from_uint64(200);

    // Snapshot 2: balance = 200
    uint32_t snap2 = state_journal_snapshot(&journal);

    // Modify again
    assert(state_journal_account_modified(&journal, &addr, account));
    account->balance = uint256_from_uint64(300);

    // Verify current balance
    assert(uint256_eq(&account->balance, &uint256_from_uint64(300)));

    // Revert to snapshot 2
    assert(state_journal_revert_to_snapshot(&journal, snap2));
    assert(state_journal_size(&journal) == 2); // snap0 entry + snap1 entry

    // Revert to snapshot 1
    assert(state_journal_revert_to_snapshot(&journal, snap1));
    assert(state_journal_size(&journal) == 1); // snap0 entry only

    // Revert to snapshot 0
    assert(state_journal_revert_to_snapshot(&journal, snap0));
    assert(state_journal_size(&journal) == 0);

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_commit(void)
{
    print_test_header("Journal commit");

    state_cache_t cache = {0};
    assert(state_cache_init(&cache));

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Create account and record
    address_t addr = {0};
    addr.bytes[0] = 0xEE;

    account_object_t *account = state_cache_get_account(&cache, &addr);
    assert(state_journal_account_created(&journal, &addr));

    account->balance = uint256_from_uint64(999);
    account->exists = true;

    // Journal has 1 entry
    assert(state_journal_size(&journal) == 1);

    // Commit
    assert(state_journal_commit(&journal));

    // Journal should be empty after commit
    assert(state_journal_size(&journal) == 0);
    assert(state_journal_snapshot_count(&journal) == 0);

    // But account should still exist in cache
    assert(state_cache_has_account(&cache, &addr));
    account = state_cache_get_account(&cache, &addr);
    assert(uint256_eq(&account->balance, &uint256_from_uint64(999)));

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

static void test_reset(void)
{
    print_test_header("Journal reset");

    state_cache_t cache = {0};
    assert(state_cache_init(&cache));

    state_journal_t journal = {0};
    assert(state_journal_init(&journal, &cache));

    // Add entries and snapshots
    address_t addr = {0};
    assert(state_journal_account_created(&journal, &addr));
    assert(state_journal_snapshot(&journal) == 0);
    assert(state_journal_account_created(&journal, &addr));

    assert(state_journal_size(&journal) == 2);
    assert(state_journal_snapshot_count(&journal) == 1);

    // Reset
    state_journal_reset(&journal);

    assert(state_journal_size(&journal) == 0);
    assert(state_journal_snapshot_count(&journal) == 0);

    state_journal_destroy(&journal);
    state_cache_destroy(&cache);

    print_test_pass();
}

//==============================================================================
// Main
//==============================================================================

int main(void)
{
    printf("\n=== State Journal Test Suite ===\n");

    test_journal_init_destroy();
    test_snapshot_and_revert();
    test_account_created_rollback();
    test_account_modified_rollback();
    test_storage_changed_rollback();
    test_multiple_snapshots();
    test_commit();
    test_reset();

    printf("\n=== All tests passed! ===\n");
    return 0;
}
