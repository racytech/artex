/**
 * State History Tests
 *
 * Tests:
 *  1. Create/destroy lifecycle
 *  2. Direct push + readback (serialize/deserialize roundtrip)
 *  3. Multiple blocks + range query
 *  4. Integration: evm_state → capture → readback
 */

#include "state_history.h"
#include "evm_state.h"
#include "uint256.h"
#include "address.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

static const char *TEST_DIR = "/tmp/test_state_history";

static void cleanup_test_dir(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    (void)system(cmd);
}

/* =========================================================================
 * Test 1: Basic lifecycle — create and destroy without any writes
 * ========================================================================= */

static void test_lifecycle(void) {
    TEST("lifecycle");
    cleanup_test_dir();

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "create returned NULL");

    uint64_t first, last;
    ASSERT(!state_history_range(sh, &first, &last), "empty range should return false");

    state_history_destroy(sh);

    /* Verify files were created */
    char path[256];
    struct stat st;
    snprintf(path, sizeof(path), "%s/state_history.dat", TEST_DIR);
    ASSERT(stat(path, &st) == 0, "dat file not created");
    snprintf(path, sizeof(path), "%s/state_history.idx", TEST_DIR);
    ASSERT(stat(path, &st) == 0, "idx file not created");

    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Test 2: Direct push via state_history_capture with a manually constructed
 *         evm_state that has dirty accounts and slots
 * ========================================================================= */

static void test_direct_roundtrip(void) {
    TEST("direct roundtrip");
    cleanup_test_dir();

    /* Create evm_state with no backing store */
    evm_state_t *es = evm_state_create(NULL, NULL, NULL);
    ASSERT(es != NULL, "evm_state_create");

    /* Set up an account: start a block, set values */
    evm_state_begin_block(es, 100);

    address_t addr1;
    memset(addr1.bytes, 0, 20);
    addr1.bytes[19] = 0x01;

    address_t addr2;
    memset(addr2.bytes, 0, 20);
    addr2.bytes[19] = 0x02;

    /* Modify account 1: nonce and balance */
    evm_state_set_nonce(es, &addr1, 5);
    uint256_t bal1 = uint256_from_uint64(1000000);
    evm_state_set_balance(es, &addr1, &bal1);

    /* Modify account 2: just balance */
    uint256_t bal2 = uint256_from_uint64(2000000);
    evm_state_set_balance(es, &addr2, &bal2);

    /* Set storage on addr1 */
    uint256_t slot = uint256_from_uint64(42);
    uint256_t val  = uint256_from_uint64(12345);
    evm_state_set_storage(es, &addr1, &slot, &val);

    /* Commit tx (makes dirty for block) */
    evm_state_commit_tx(es);

    /* Finalize (flushes to backing store, keeps block_dirty flags) */
    evm_state_finalize(es);

    /* Create state history and capture */
    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    state_history_capture(sh, es, 100);

    /* Destroy to flush consumer thread */
    state_history_destroy(sh);

    /* Reopen and read back */
    sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "reopen state_history");

    uint64_t first, last;
    ASSERT(state_history_range(sh, &first, &last), "range should succeed");
    ASSERT(first == 100, "first block should be 100");
    ASSERT(last == 100, "last block should be 100");

    block_diff_t diff;
    ASSERT(state_history_get_diff(sh, 100, &diff), "get_diff block 100");
    ASSERT(diff.block_number == 100, "block_number mismatch");
    ASSERT(diff.account_count >= 2, "expected at least 2 account diffs");
    ASSERT(diff.storage_count >= 1, "expected at least 1 storage diff");

    /* Verify we can find addr1 in the account diffs */
    bool found_addr1 = false;
    for (uint32_t i = 0; i < diff.account_count; i++) {
        if (memcmp(diff.accounts[i].addr.bytes, addr1.bytes, 20) == 0) {
            found_addr1 = true;
            ASSERT(diff.accounts[i].new_nonce == 5, "addr1 new_nonce should be 5");
            ASSERT(uint256_is_equal(&diff.accounts[i].new_balance, &bal1),
                   "addr1 new_balance mismatch");
            /* Original values should be zero (new account) */
            ASSERT(diff.accounts[i].old_nonce == 0, "addr1 old_nonce should be 0");
            break;
        }
    }
    ASSERT(found_addr1, "addr1 not found in account diffs");

    /* Verify storage diff */
    bool found_slot = false;
    for (uint32_t i = 0; i < diff.storage_count; i++) {
        if (memcmp(diff.storage[i].addr.bytes, addr1.bytes, 20) == 0 &&
            uint256_is_equal(&diff.storage[i].slot, &slot)) {
            found_slot = true;
            ASSERT(uint256_is_equal(&diff.storage[i].new_value, &val),
                   "slot new_value mismatch");
            uint256_t zero = uint256_from_uint64(0);
            ASSERT(uint256_is_equal(&diff.storage[i].old_value, &zero),
                   "slot old_value should be 0");
            break;
        }
    }
    ASSERT(found_slot, "storage slot not found in diffs");

    block_diff_free(&diff);
    state_history_destroy(sh);
    evm_state_destroy(es);
    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Test 3: Multiple blocks — write several blocks, read them all back
 * ========================================================================= */

static void test_multiple_blocks(void) {
    TEST("multiple blocks");
    cleanup_test_dir();

    evm_state_t *es = evm_state_create(NULL, NULL, NULL);
    ASSERT(es != NULL, "evm_state_create");

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    address_t addr;
    memset(addr.bytes, 0, 20);
    addr.bytes[19] = 0xAA;

    /* Execute 10 blocks, each incrementing the balance */
    for (uint64_t bn = 1; bn <= 10; bn++) {
        evm_state_begin_block(es, bn);

        uint256_t bal = uint256_from_uint64(bn * 1000);
        evm_state_set_balance(es, &addr, &bal);

        evm_state_commit_tx(es);
        evm_state_finalize(es);

        state_history_capture(sh, es, bn);

        /* Commit block (clears dirty flags for next block) */
        evm_state_commit(es);
    }

    /* Flush everything */
    state_history_destroy(sh);

    /* Reopen and verify */
    sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "reopen");

    uint64_t first, last;
    ASSERT(state_history_range(sh, &first, &last), "range");
    ASSERT(first == 1, "first should be 1");
    ASSERT(last == 10, "last should be 10");

    /* Verify each block's diff */
    for (uint64_t bn = 1; bn <= 10; bn++) {
        block_diff_t diff;
        ASSERT(state_history_get_diff(sh, bn, &diff), "get_diff");
        ASSERT(diff.block_number == bn, "block_number");

        /* Find our account */
        bool found = false;
        for (uint32_t i = 0; i < diff.account_count; i++) {
            if (memcmp(diff.accounts[i].addr.bytes, addr.bytes, 20) == 0) {
                found = true;
                uint256_t expected_new = uint256_from_uint64(bn * 1000);
                ASSERT(uint256_is_equal(&diff.accounts[i].new_balance, &expected_new),
                       "new_balance mismatch");
                if (bn == 1) {
                    uint256_t zero = uint256_from_uint64(0);
                    ASSERT(uint256_is_equal(&diff.accounts[i].old_balance, &zero),
                           "block 1 old_balance should be 0");
                } else {
                    uint256_t expected_old = uint256_from_uint64((bn - 1) * 1000);
                    ASSERT(uint256_is_equal(&diff.accounts[i].old_balance, &expected_old),
                           "old_balance mismatch");
                }
                break;
            }
        }
        ASSERT(found, "account not found");
        block_diff_free(&diff);
    }

    /* Query for non-existent block should fail */
    block_diff_t diff;
    ASSERT(!state_history_get_diff(sh, 0, &diff), "block 0 should not exist");
    ASSERT(!state_history_get_diff(sh, 11, &diff), "block 11 should not exist");

    state_history_destroy(sh);
    evm_state_destroy(es);
    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Test 4: No-change block produces empty diff
 * ========================================================================= */

static void test_empty_diff(void) {
    TEST("empty diff");
    cleanup_test_dir();

    evm_state_t *es = evm_state_create(NULL, NULL, NULL);
    ASSERT(es != NULL, "evm_state_create");

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    /* Block with no state changes */
    evm_state_begin_block(es, 50);
    evm_state_finalize(es);
    state_history_capture(sh, es, 50);

    state_history_destroy(sh);

    /* Read back */
    sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "reopen");

    block_diff_t diff;
    ASSERT(state_history_get_diff(sh, 50, &diff), "get_diff");
    ASSERT(diff.block_number == 50, "block_number");
    ASSERT(diff.account_count == 0, "should have 0 account diffs");
    ASSERT(diff.storage_count == 0, "should have 0 storage diffs");
    block_diff_free(&diff);

    state_history_destroy(sh);
    evm_state_destroy(es);
    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("State History Tests\n");

    test_lifecycle();
    test_direct_roundtrip();
    test_multiple_blocks();
    test_empty_diff();

    printf("\n  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
