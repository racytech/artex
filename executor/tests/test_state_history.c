/**
 * State History Tests (v3 — grouped, new-values-only)
 *
 * Tests:
 *  1. Create/destroy lifecycle
 *  2. Direct push + readback (serialize/deserialize roundtrip)
 *  3. Multiple blocks + range query
 *  4. Empty diff block
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

/* Helper: find a group by address */
static const addr_diff_t *find_group(const block_diff_t *diff,
                                      const address_t *addr) {
    for (uint16_t i = 0; i < diff->group_count; i++) {
        if (memcmp(diff->groups[i].addr.bytes, addr->bytes, 20) == 0)
            return &diff->groups[i];
    }
    return NULL;
}

/* Helper: find a slot within a group */
static const slot_diff_t *find_slot(const addr_diff_t *g,
                                     const uint256_t *slot) {
    for (uint16_t i = 0; i < g->slot_count; i++) {
        if (uint256_is_equal(&g->slots[i].slot, slot))
            return &g->slots[i];
    }
    return NULL;
}

/* =========================================================================
 * Test 1: Basic lifecycle
 * ========================================================================= */

static void test_lifecycle(void) {
    TEST("lifecycle");
    cleanup_test_dir();

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "create returned NULL");

    uint64_t first, last;
    ASSERT(!state_history_range(sh, &first, &last), "empty range should return false");

    state_history_destroy(sh);

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
 * Test 2: Direct capture + readback (roundtrip)
 * ========================================================================= */

static void test_direct_roundtrip(void) {
    TEST("direct roundtrip");
    cleanup_test_dir();

    evm_state_t *es = evm_state_create(NULL);
    ASSERT(es != NULL, "evm_state_create");

    evm_state_begin_block(es, 100);

    address_t addr1;
    memset(addr1.bytes, 0, 20);
    addr1.bytes[19] = 0x01;

    address_t addr2;
    memset(addr2.bytes, 0, 20);
    addr2.bytes[19] = 0x02;

    /* Modify account 1: nonce + balance + storage */
    evm_state_set_nonce(es, &addr1, 5);
    uint256_t bal1 = uint256_from_uint64(1000000);
    evm_state_set_balance(es, &addr1, &bal1);

    uint256_t slot = uint256_from_uint64(42);
    uint256_t val  = uint256_from_uint64(12345);
    evm_state_set_storage(es, &addr1, &slot, &val);

    /* Modify account 2: just balance */
    uint256_t bal2 = uint256_from_uint64(2000000);
    evm_state_set_balance(es, &addr2, &bal2);

    evm_state_commit_tx(es);
    evm_state_finalize(es);

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    state_history_capture(sh, es, 100);
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
    ASSERT(diff.group_count >= 2, "expected at least 2 groups");

    /* Verify addr1 group */
    const addr_diff_t *g1 = find_group(&diff, &addr1);
    ASSERT(g1 != NULL, "addr1 not found in groups");
    ASSERT(g1->field_mask & FIELD_NONCE, "addr1 should have FIELD_NONCE");
    ASSERT(g1->nonce == 5, "addr1 nonce should be 5");
    ASSERT(g1->field_mask & FIELD_BALANCE, "addr1 should have FIELD_BALANCE");
    ASSERT(uint256_is_equal(&g1->balance, &bal1), "addr1 balance mismatch");

    /* Verify addr1 storage slot */
    ASSERT(g1->slot_count >= 1, "addr1 should have at least 1 slot");
    const slot_diff_t *sd = find_slot(g1, &slot);
    ASSERT(sd != NULL, "slot 42 not found");
    ASSERT(uint256_is_equal(&sd->value, &val), "slot value mismatch");

    /* Verify addr2 group */
    const addr_diff_t *g2 = find_group(&diff, &addr2);
    ASSERT(g2 != NULL, "addr2 not found in groups");
    ASSERT(g2->field_mask & FIELD_BALANCE, "addr2 should have FIELD_BALANCE");
    ASSERT(uint256_is_equal(&g2->balance, &bal2), "addr2 balance mismatch");
    ASSERT(!(g2->field_mask & FIELD_NONCE), "addr2 should not have FIELD_NONCE");
    ASSERT(g2->slot_count == 0, "addr2 should have 0 slots");

    block_diff_free(&diff);
    state_history_destroy(sh);
    evm_state_destroy(es);
    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Test 3: Multiple blocks
 * ========================================================================= */

static void test_multiple_blocks(void) {
    TEST("multiple blocks");
    cleanup_test_dir();

    evm_state_t *es = evm_state_create(NULL);
    ASSERT(es != NULL, "evm_state_create");

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    address_t addr;
    memset(addr.bytes, 0, 20);
    addr.bytes[19] = 0xAA;

    for (uint64_t bn = 1; bn <= 10; bn++) {
        evm_state_begin_block(es, bn);

        uint256_t bal = uint256_from_uint64(bn * 1000);
        evm_state_set_balance(es, &addr, &bal);

        evm_state_commit_tx(es);
        evm_state_finalize(es);
        state_history_capture(sh, es, bn);
        evm_state_commit(es);
    }

    state_history_destroy(sh);

    /* Reopen and verify */
    sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "reopen");

    uint64_t first, last;
    ASSERT(state_history_range(sh, &first, &last), "range");
    ASSERT(first == 1, "first should be 1");
    ASSERT(last == 10, "last should be 10");

    for (uint64_t bn = 1; bn <= 10; bn++) {
        block_diff_t diff;
        ASSERT(state_history_get_diff(sh, bn, &diff), "get_diff");
        ASSERT(diff.block_number == bn, "block_number");

        const addr_diff_t *g = find_group(&diff, &addr);
        ASSERT(g != NULL, "account not found");
        ASSERT(g->field_mask & FIELD_BALANCE, "should have FIELD_BALANCE");

        uint256_t expected = uint256_from_uint64(bn * 1000);
        ASSERT(uint256_is_equal(&g->balance, &expected), "balance mismatch");

        block_diff_free(&diff);
    }

    block_diff_t diff;
    ASSERT(!state_history_get_diff(sh, 0, &diff), "block 0 should not exist");
    ASSERT(!state_history_get_diff(sh, 11, &diff), "block 11 should not exist");

    state_history_destroy(sh);
    evm_state_destroy(es);
    cleanup_test_dir();
    PASS();
}

/* =========================================================================
 * Test 4: Empty diff
 * ========================================================================= */

static void test_empty_diff(void) {
    TEST("empty diff");
    cleanup_test_dir();

    evm_state_t *es = evm_state_create(NULL);
    ASSERT(es != NULL, "evm_state_create");

    state_history_t *sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "state_history_create");

    evm_state_begin_block(es, 50);
    evm_state_finalize(es);
    state_history_capture(sh, es, 50);

    state_history_destroy(sh);

    sh = state_history_create(TEST_DIR);
    ASSERT(sh != NULL, "reopen");

    block_diff_t diff;
    ASSERT(state_history_get_diff(sh, 50, &diff), "get_diff");
    ASSERT(diff.block_number == 50, "block_number");
    ASSERT(diff.group_count == 0, "should have 0 groups");
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
    printf("State History Tests (v3)\n");

    test_lifecycle();
    test_direct_roundtrip();
    test_multiple_blocks();
    test_empty_diff();

    printf("\n  Results: %d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
