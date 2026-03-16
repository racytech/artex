/**
 * Targeted tests for evm_state.c functional changes.
 * Tests each behavioral change individually to verify correctness.
 */

#include "evm_state.h"
#ifdef ENABLE_VERKLE
#include "verkle_state.h"
#endif
#include "uint256.h"
#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

static int tests_passed = 0;
static int tests_failed = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: %s (line %d)\n", msg, __LINE__); \
        tests_failed++; \
        goto cleanup; \
    } \
} while(0)

#define PASS(msg) do { \
    printf("  OK: %s\n", msg); \
    tests_passed++; \
} while(0)

static address_t make_addr(uint8_t byte) {
    address_t a;
    memset(a.bytes, 0, 20);
    a.bytes[19] = byte;
    return a;
}

/* Create state + track verkle_state for proper cleanup */
typedef struct {
#ifdef ENABLE_VERKLE
    verkle_state_t *vs;
#endif
    evm_state_t *es;
} test_state_t;

static test_state_t make_state(void) {
    test_state_t ts;
#ifdef ENABLE_VERKLE
    ts.vs = verkle_state_create_flat("/tmp/test_evm_audit_vf", "/tmp/test_evm_audit_vc");
    ts.es = evm_state_create(ts.vs,
#else
    ts.es = evm_state_create(NULL,
#endif
#ifdef ENABLE_MPT
        "/tmp/test_evm_state_audit_mpt",
#else
        NULL,
#endif
        NULL  /* no code_store for tests */
    );
    return ts;
}

static void free_state(test_state_t *ts) {
    /* evm_state_destroy may access vs internally, so destroy es first */
    if (ts->es) {
        evm_state_destroy(ts->es);
        ts->es = NULL;
    }
#ifdef ENABLE_VERKLE
    if (ts->vs) {
        verkle_state_destroy(ts->vs);
        ts->vs = NULL;
    }
#endif
}

/* =========================================================================
 * Test 1: evm_state_exists() — dirty flag makes account "exist"
 *
 * Change: existed || created || dirty || block_dirty
 * Purpose: CALL to new address should make it "exist" within the same tx
 *          so repeated CALLs don't re-charge 25000 new-account gas.
 * ========================================================================= */
static void test_exists_dirty_flag(void) {
    printf("\nTest 1: evm_state_exists — dirty flag\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    address_t addr = make_addr(0x42);

    /* Fresh account — should NOT exist */
    bool exists_before = evm_state_exists(es, &addr);
    CHECK(!exists_before, "new address should not exist initially");

    /* add_balance(addr, 0) — this is what CALL does to "touch" an address */
    uint256_t zero = UINT256_ZERO_INIT;
    evm_state_add_balance(es, &addr, &zero);

    /* Now it should exist (dirty=true from set_balance) */
    bool exists_after = evm_state_exists(es, &addr);
    CHECK(exists_after, "account should exist after add_balance(0) — dirty flag");

    PASS("dirty flag makes account exist within tx");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 1b: Revert undoes dirty flag for existence
 *
 * BUG FOUND: block_dirty is NOT reverted by JOURNAL_BALANCE.
 * After revert, block_dirty stays true → evm_state_exists() returns true.
 * ========================================================================= */
static void test_exists_revert_undoes_dirty(void) {
    printf("\nTest 1b: Revert undoes dirty flag for existence\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    address_t addr2 = make_addr(0x43);
    uint256_t zero = UINT256_ZERO_INIT;

    uint32_t snap = evm_state_snapshot(es);
    evm_state_add_balance(es, &addr2, &zero);
    CHECK(evm_state_exists(es, &addr2), "addr2 should exist after touch");
    evm_state_revert(es, snap);
    bool exists_reverted = evm_state_exists(es, &addr2);
    CHECK(!exists_reverted,
          "addr2 should NOT exist after revert "
          "(BUG: block_dirty not reverted by JOURNAL_BALANCE)");
    PASS("revert properly undoes dirty/block_dirty for existence");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 2: evm_state_exists() — block_dirty survives commit_tx
 *
 * Purpose: Account touched in tx1 should still "exist" in tx2 of the same
 *          block (Frontier: empty objects persist across txs).
 * ========================================================================= */
static void test_exists_block_dirty_survives_commit(void) {
    printf("\nTest 2: evm_state_exists — block_dirty survives commit_tx\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0x50);

    /* Touch account with a value transfer, then commit tx */
    uint256_t one_wei = uint256_from_uint64(1);
    evm_state_add_balance(es, &addr, &one_wei);
    CHECK(evm_state_exists(es, &addr), "exists during tx");

    evm_state_commit_tx(es);

    /* After commit_tx: dirty=false, but existed should be true
     * (non-empty account gets existed=true in commit_tx_account_cb) */
    bool exists_after_commit = evm_state_exists(es, &addr);
    CHECK(exists_after_commit,
           "non-empty account should still exist after commit_tx (existed=true)");

    PASS("non-empty account persists across commit_tx");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 2b: Empty account + block_dirty survives commit_tx (Frontier)
 * ========================================================================= */
static void test_empty_account_block_dirty_survives_commit(void) {
    printf("\nTest 2b: Empty account block_dirty survives commit_tx\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);

    address_t empty_addr = make_addr(0x51);
    uint256_t zero = UINT256_ZERO_INIT;
    evm_state_add_balance(es, &empty_addr, &zero);
    CHECK(evm_state_exists(es, &empty_addr), "empty touched account exists in tx");

    evm_state_commit_tx(es);

    /* After commit_tx for empty account:
     * - dirty cleared, existed NOT set (empty, EIP-161 logic)
     * - block_dirty should still be true → account exists */
    bool empty_exists = evm_state_exists(es, &empty_addr);
    CHECK(empty_exists,
          "empty touched account should still exist via block_dirty after commit_tx");

    PASS("empty account persists via block_dirty across commit_tx (Frontier)");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 3: Journal saves/restores block_dirty on CREATE revert
 *
 * Purpose: If CREATE fails and reverts, block_dirty must be restored to
 *          its pre-CREATE value.
 * ========================================================================= */
static void test_create_revert_restores_block_dirty(void) {
    printf("\nTest 3: CREATE revert restores block_dirty\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0x60);

    CHECK(!evm_state_exists(es, &addr), "account should not exist before CREATE");

    uint32_t snap = evm_state_snapshot(es);
    evm_state_create_account(es, &addr);
    CHECK(evm_state_exists(es, &addr), "account should exist after CREATE");

    evm_state_revert(es, snap);
    bool exists_after_revert = evm_state_exists(es, &addr);
    CHECK(!exists_after_revert,
          "account should NOT exist after CREATE revert (block_dirty restored)");

    PASS("CREATE revert properly restores block_dirty=false");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 3b: CREATE revert preserves pre-existing block_dirty
 * ========================================================================= */
static void test_create_revert_preserves_existing_block_dirty(void) {
    printf("\nTest 3b: CREATE revert preserves pre-existing block_dirty\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr2 = make_addr(0x61);

    uint256_t one = uint256_from_uint64(1);
    evm_state_add_balance(es, &addr2, &one);  /* dirty=true, block_dirty=true */
    evm_state_commit_tx(es);                   /* dirty=false, existed=true */

    /* Now simulate CREATE on same address in a new tx, then revert */
    uint32_t snap2 = evm_state_snapshot(es);
    evm_state_create_account(es, &addr2);
    evm_state_revert(es, snap2);

    /* block_dirty was true before CREATE, should still be true */
    CHECK(evm_state_exists(es, &addr2),
          "previously-touched account should still exist after CREATE revert");

    PASS("CREATE revert preserves pre-existing block_dirty=true");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 4: Self-destruct sets block_dirty=false in commit_tx
 * ========================================================================= */
static void test_selfdestruct_clears_block_dirty(void) {
    printf("\nTest 4: Self-destruct clears block_dirty in commit_tx\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0x70);

    uint256_t funds = uint256_from_uint64(1000);
    evm_state_add_balance(es, &addr, &funds);
    CHECK(evm_state_exists(es, &addr), "account should exist after funding");

    evm_state_self_destruct(es, &addr);
    evm_state_commit_tx(es);

    /* After commit: account should NOT exist */
    bool exists_after = evm_state_exists(es, &addr);
    CHECK(!exists_after,
          "self-destructed account should not exist after commit_tx");

    uint256_t bal = evm_state_get_balance(es, &addr);
    CHECK(uint256_is_zero(&bal), "self-destructed account balance should be 0");

    PASS("self-destruct zeroes account and clears block_dirty");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 5: Self-destruct clears storage block_dirty too
 * ========================================================================= */
static void test_selfdestruct_clears_storage_block_dirty(void) {
    printf("\nTest 5: Self-destruct clears storage block_dirty\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0x80);
    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t val = uint256_from_uint64(42);
    uint256_t funds = uint256_from_uint64(1000);

    evm_state_add_balance(es, &addr, &funds);
    evm_state_set_storage(es, &addr, &slot0, &val);

    uint256_t got = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_eq(&got, &val), "storage should be set before self-destruct");

    evm_state_self_destruct(es, &addr);
    evm_state_commit_tx(es);

    uint256_t got2 = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_is_zero(&got2), "storage should be zeroed after self-destruct commit");

    PASS("self-destruct zeroes storage and clears block_dirty");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 6: flush sets existed for block_dirty accounts
 * ========================================================================= */
static void test_flush_sets_existed(void) {
    printf("\nTest 6: compute_state_root_ex sets existed for block_dirty accounts\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0x90);

    uint256_t funds = uint256_from_uint64(1000);
    evm_state_add_balance(es, &addr, &funds);
    evm_state_commit_tx(es);

    evm_state_finalize(es);
    hash_t root = evm_state_compute_state_root_ex(es, false);

    CHECK(evm_state_exists(es, &addr),
          "account should still exist after compute_state_root_ex");

#ifdef ENABLE_VERKLE
    hash_t zero_root = hash_zero();
    CHECK(memcmp(root.bytes, zero_root.bytes, 32) != 0,
          "state root should not be zero after flushing an account");
#endif

    PASS("flush_all_accounts_cb correctly processes block_dirty accounts");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 7: prune_empty propagation — Frontier vs EIP-161
 * ========================================================================= */
static void test_prune_empty_frontier(void) {
    printf("\nTest 7a: prune_empty=false (Frontier): empty account flushed\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xA0);

    uint256_t zero = UINT256_ZERO_INIT;
    evm_state_add_balance(es, &addr, &zero);
    evm_state_commit_tx(es);

    evm_state_finalize(es);
    evm_state_compute_state_root_ex(es, false);

#ifdef ENABLE_VERKLE
    /* With prune_empty=false, block_dirty was cleared, but existed should
     * have been set to true by flush_all_accounts_cb */
    CHECK(evm_state_exists(es, &addr),
          "empty account should exist after flush with prune_empty=false");

    PASS("prune_empty=false: empty touched account is flushed (Frontier)");
#else
    /* Without verkle, flush is a no-op; block_dirty cleared, account not promoted */
    (void)addr;
    PASS("prune_empty=false: skipped (no verkle flush)");
#endif

cleanup:
    free_state(&ts);
}

static void test_prune_empty_eip161(void) {
    printf("\nTest 7b: prune_empty=true (EIP-161): empty account pruned\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xA1);

    uint256_t zero = UINT256_ZERO_INIT;
    evm_state_add_balance(es, &addr, &zero);
    evm_state_commit_tx(es);

    evm_state_finalize(es);
    evm_state_compute_state_root_ex(es, true);

    /* With prune_empty=true, the empty account should be skipped,
     * existed stays false, block_dirty cleared */
    bool exists = evm_state_exists(es, &addr);
    CHECK(!exists,
          "empty account should NOT exist after flush with prune_empty=true");

    PASS("prune_empty=true: empty touched account is pruned (EIP-161)");

cleanup:
    free_state(&ts);
}

#ifdef ENABLE_MPT
/* =========================================================================
 * Test 8: MPT root consistency
 * ========================================================================= */
static void test_mpt_root_consistency(void) {
    printf("\nTest 8: MPT root computation consistency\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);

    address_t a1 = make_addr(0xB0);
    address_t a2 = make_addr(0xB1);
    uint256_t bal1 = uint256_from_uint64(1000);
    uint256_t bal2 = uint256_from_uint64(2000);
    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t val1 = uint256_from_uint64(42);

    evm_state_add_balance(es, &a1, &bal1);
    evm_state_add_balance(es, &a2, &bal2);
    evm_state_set_storage(es, &a1, &slot0, &val1);

    evm_state_commit_tx(es);
    evm_state_finalize(es);
    evm_state_compute_state_root_ex(es, false);

    hash_t root1 = evm_state_compute_mpt_root(es, false);
    hash_t root2 = evm_state_compute_mpt_root(es, false);

    hash_t zero_hash = hash_zero();
    CHECK(memcmp(root1.bytes, zero_hash.bytes, 32) != 0,
          "MPT root should not be zero with accounts");
    CHECK(memcmp(root1.bytes, root2.bytes, 32) == 0,
          "MPT root should be deterministic");

    PASS("MPT root is non-zero and deterministic");

cleanup:
    free_state(&ts);
}
#endif /* ENABLE_MPT */

/* =========================================================================
 * Test 9: Storage visible across transactions
 * ========================================================================= */
static void test_storage_across_txs(void) {
    printf("\nTest 9: Storage visibility across transactions\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xC0);
    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t slot1 = uint256_from_uint64(1);
    uint256_t val1 = uint256_from_uint64(100);
    uint256_t val2 = uint256_from_uint64(200);
    uint256_t funds = uint256_from_uint64(1000);

    evm_state_add_balance(es, &addr, &funds);
    evm_state_set_storage(es, &addr, &slot0, &val1);
    evm_state_commit_tx(es);

    uint256_t got = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_eq(&got, &val1), "storage from tx1 should be visible in tx2");

    evm_state_set_storage(es, &addr, &slot1, &val2);
    evm_state_commit_tx(es);

    uint256_t got0 = evm_state_get_storage(es, &addr, &slot0);
    uint256_t got1 = evm_state_get_storage(es, &addr, &slot1);
    CHECK(uint256_eq(&got0, &val1), "slot0 should persist after tx2");
    CHECK(uint256_eq(&got1, &val2), "slot1 should be set after tx2");

    PASS("storage values are correctly visible across transactions");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 10: Snapshot/revert with storage
 * ========================================================================= */
static void test_snapshot_revert_storage(void) {
    printf("\nTest 10: Snapshot/revert with storage\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xD0);
    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t val1 = uint256_from_uint64(100);
    uint256_t val2 = uint256_from_uint64(200);
    uint256_t funds = uint256_from_uint64(1000);

    evm_state_add_balance(es, &addr, &funds);
    evm_state_set_storage(es, &addr, &slot0, &val1);

    uint32_t snap = evm_state_snapshot(es);
    evm_state_set_storage(es, &addr, &slot0, &val2);

    uint256_t got_during = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_eq(&got_during, &val2), "storage should be modified in snapshot");

    evm_state_revert(es, snap);

    uint256_t got_after = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_eq(&got_after, &val1), "storage should be reverted after revert");

    evm_state_commit_tx(es);
    uint256_t got_committed = evm_state_get_storage(es, &addr, &slot0);
    CHECK(uint256_eq(&got_committed, &val1), "storage should persist after commit");

    PASS("snapshot/revert does not corrupt storage");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 11: Balance revert restores block_dirty
 *
 * This tests that JOURNAL_BALANCE saves/restores block_dirty.
 * If block_dirty is NOT restored, reverted accounts appear "existing".
 * ========================================================================= */
static void test_balance_revert_restores_block_dirty(void) {
    printf("\nTest 11: Balance revert restores block_dirty\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xE0);
    uint256_t one = uint256_from_uint64(1);

    /* Account doesn't exist */
    CHECK(!evm_state_exists(es, &addr), "should not exist initially");

    /* Snapshot, transfer value, verify exists, revert */
    uint32_t snap = evm_state_snapshot(es);
    evm_state_add_balance(es, &addr, &one);
    CHECK(evm_state_exists(es, &addr), "should exist after value transfer");

    evm_state_revert(es, snap);

    /* After revert: balance is 0, dirty should be restored to false,
     * block_dirty should be restored to false.
     * BUG: if JOURNAL_BALANCE doesn't save/restore block_dirty,
     * exists() returns true via block_dirty. */
    bool exists = evm_state_exists(es, &addr);
    CHECK(!exists,
          "account should NOT exist after revert "
          "(JOURNAL_BALANCE must restore block_dirty)");

    PASS("balance revert restores block_dirty");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Test 12: Nonce revert restores block_dirty
 * ========================================================================= */
static void test_nonce_revert_restores_block_dirty(void) {
    printf("\nTest 12: Nonce revert restores block_dirty\n");

    test_state_t ts = make_state();
    evm_state_t *es = ts.es;
    evm_state_begin_block(es, 1);
    address_t addr = make_addr(0xE1);

    CHECK(!evm_state_exists(es, &addr), "should not exist initially");

    uint32_t snap = evm_state_snapshot(es);
    evm_state_set_nonce(es, &addr, 1);
    CHECK(evm_state_exists(es, &addr), "should exist after nonce set");

    evm_state_revert(es, snap);

    bool exists = evm_state_exists(es, &addr);
    CHECK(!exists,
          "account should NOT exist after nonce revert "
          "(JOURNAL_NONCE must restore block_dirty)");

    PASS("nonce revert restores block_dirty");

cleanup:
    free_state(&ts);
}

/* =========================================================================
 * Main
 * ========================================================================= */
int main(void) {
    printf("=== EVM State Audit Tests ===\n");

    test_exists_dirty_flag();
    test_exists_revert_undoes_dirty();
    test_exists_block_dirty_survives_commit();
    test_empty_account_block_dirty_survives_commit();
    test_create_revert_restores_block_dirty();
    test_create_revert_preserves_existing_block_dirty();
    test_selfdestruct_clears_block_dirty();
    test_selfdestruct_clears_storage_block_dirty();
    test_flush_sets_existed();
    test_prune_empty_frontier();
    test_prune_empty_eip161();
#ifdef ENABLE_MPT
    test_mpt_root_consistency();
#endif
    test_storage_across_txs();
    test_snapshot_revert_storage();
    test_balance_revert_restores_block_dirty();
    test_nonce_revert_restores_block_dirty();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
