/**
 * StateDB Tests
 */

#include "state_db.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

//==============================================================================
// Test Helpers
//==============================================================================

static void print_test_header(const char *name)
{
    printf("Test: %s... ", name);
    fflush(stdout);
}

static void print_test_pass(void)
{
    printf("PASSED\n");
}

//==============================================================================
// Tests
//==============================================================================

static void test_state_db_init_destroy(void)
{
    print_test_header("StateDB init/destroy");

    state_db_t db = {0};
    assert(state_db_init(&db));

    assert(state_db_get_block_number(&db) == 0);
    assert(state_db_num_accounts(&db) == 0);
    assert(state_db_num_dirty(&db) == 0);

    state_db_destroy(&db);

    print_test_pass();
}

static void test_create_account(void)
{
    print_test_header("Create account");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0x12;
    addr.bytes[1] = 0x34;

    // Account shouldn't exist initially
    assert(!state_db_exist(&db, &addr));

    // Create account
    assert(state_db_create_account(&db, &addr));

    // Now it should exist
    assert(state_db_exist(&db, &addr));

    // Should be empty (zero balance, zero nonce, no code)
    assert(state_db_empty(&db, &addr));

    // Can't create same account twice
    assert(!state_db_create_account(&db, &addr));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_balance_operations(void)
{
    print_test_header("Balance operations");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xAA;

    // Create account
    assert(state_db_create_account(&db, &addr));

    // Initial balance should be 0
    uint256_t balance;
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(0)));

    // Set balance
    uint256_t new_balance = uint256_from_uint64(1000);
    assert(state_db_set_balance(&db, &addr, &new_balance));

    // Verify balance
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &new_balance));

    // Add to balance
    uint256_t amount = uint256_from_uint64(500);
    assert(state_db_add_balance(&db, &addr, &amount));

    // Should be 1500
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(1500)));

    // Subtract from balance
    uint256_t sub_amount = uint256_from_uint64(300);
    assert(state_db_sub_balance(&db, &addr, &sub_amount));

    // Should be 1200
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(1200)));

    // Can't subtract more than balance
    uint256_t too_much = uint256_from_uint64(2000);
    assert(!state_db_sub_balance(&db, &addr, &too_much));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_nonce_operations(void)
{
    print_test_header("Nonce operations");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xBB;

    assert(state_db_create_account(&db, &addr));

    // Initial nonce should be 0
    uint64_t nonce;
    assert(state_db_get_nonce(&db, &addr, &nonce));
    assert(nonce == 0);

    // Set nonce
    assert(state_db_set_nonce(&db, &addr, 42));

    // Verify nonce
    assert(state_db_get_nonce(&db, &addr, &nonce));
    assert(nonce == 42);

    state_db_destroy(&db);

    print_test_pass();
}

static void test_storage_operations(void)
{
    print_test_header("Storage operations");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xCC;

    assert(state_db_create_account(&db, &addr));

    // Set storage
    uint256_t key = uint256_from_uint64(10);
    uint256_t value = uint256_from_uint64(99);
    assert(state_db_set_state(&db, &addr, &key, &value));

    // Get storage
    uint256_t retrieved;
    assert(state_db_get_state(&db, &addr, &key, &retrieved));
    assert(uint256_eq(&retrieved, &value));

    // Update storage
    uint256_t new_value = uint256_from_uint64(777);
    assert(state_db_set_state(&db, &addr, &key, &new_value));

    assert(state_db_get_state(&db, &addr, &key, &retrieved));
    assert(uint256_eq(&retrieved, &new_value));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_transaction_commit(void)
{
    print_test_header("Transaction commit");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xDD;

    // Begin transaction
    uint32_t snap = state_db_begin_transaction(&db);
    assert(snap != UINT32_MAX);

    // Create account and modify it
    assert(state_db_create_account(&db, &addr));
    assert(state_db_set_balance(&db, &addr, &uint256_from_uint64(500)));

    // Commit transaction
    assert(state_db_commit_transaction(&db));

    // Changes should persist
    assert(state_db_exist(&db, &addr));
    uint256_t balance;
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(500)));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_transaction_revert(void)
{
    print_test_header("Transaction revert");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xEE;

    // Create account with initial balance
    assert(state_db_create_account(&db, &addr));
    assert(state_db_set_balance(&db, &addr, &uint256_from_uint64(100)));

    // Begin transaction
    uint32_t snap = state_db_begin_transaction(&db);
    assert(snap != UINT32_MAX);

    // Modify balance
    assert(state_db_set_balance(&db, &addr, &uint256_from_uint64(500)));

    // Verify modification
    uint256_t balance;
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(500)));

    // Revert transaction
    assert(state_db_revert_to_snapshot(&db, snap));

    // Balance should be restored to original
    assert(state_db_get_balance(&db, &addr, &balance));
    assert(uint256_eq(&balance, &uint256_from_uint64(100)));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_suicide(void)
{
    print_test_header("Account suicide");

    state_db_t db = {0};
    assert(state_db_init(&db));

    address_t addr = {0};
    addr.bytes[0] = 0xFF;

    // Create account
    assert(state_db_create_account(&db, &addr));
    assert(state_db_set_balance(&db, &addr, &uint256_from_uint64(1000)));

    // Account exists
    assert(state_db_exist(&db, &addr));

    // Suicide account
    assert(state_db_suicide(&db, &addr));

    // Account should not exist anymore
    assert(!state_db_exist(&db, &addr));

    state_db_destroy(&db);

    print_test_pass();
}

static void test_multiple_accounts(void)
{
    print_test_header("Multiple accounts");

    state_db_t db = {0};
    assert(state_db_init(&db));

    // Create 10 accounts
    for (int i = 0; i < 10; i++)
    {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;

        assert(state_db_create_account(&db, &addr));
        assert(state_db_set_balance(&db, &addr, &uint256_from_uint64(i * 100)));
        assert(state_db_set_nonce(&db, &addr, i));
    }

    assert(state_db_num_accounts(&db) == 10);

    // Verify all accounts
    for (int i = 0; i < 10; i++)
    {
        address_t addr = {0};
        addr.bytes[0] = (uint8_t)i;

        assert(state_db_exist(&db, &addr));

        uint256_t balance;
        assert(state_db_get_balance(&db, &addr, &balance));
        assert(uint256_eq(&balance, &uint256_from_uint64(i * 100)));

        uint64_t nonce;
        assert(state_db_get_nonce(&db, &addr, &nonce));
        assert(nonce == (uint64_t)i);
    }

    state_db_destroy(&db);

    print_test_pass();
}

//==============================================================================
// Main
//==============================================================================

int main(void)
{
    printf("\n=== StateDB Test Suite ===\n");

    test_state_db_init_destroy();
    test_create_account();
    test_balance_operations();
    test_nonce_operations();
    test_storage_operations();
    test_transaction_commit();
    test_transaction_revert();
    test_suicide();
    test_multiple_accounts();

    printf("\n=== All tests passed! ===\n");
    return 0;
}
