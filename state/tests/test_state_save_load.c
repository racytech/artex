/**
 * Test state_save / state_load round-trip.
 *
 * Sets up a known state with accounts, code, and storage,
 * computes root, saves to file, loads into fresh state,
 * recomputes root, verifies they match.
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include "code_store.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define TEST_FILE "/tmp/test_state_save_load.bin"

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

static address_t make_addr(uint8_t seed) {
    address_t a = {0};
    for (int i = 0; i < 20; i++) a.bytes[i] = seed + (uint8_t)i;
    return a;
}

static int test_basic_round_trip(void) {
    printf("test_basic_round_trip:\n");

    evm_state_t *es = evm_state_create(NULL);
    if (!es) { printf("  FAIL: create\n"); return 1; }

    /* Setup 5 accounts with varying properties */
    address_t a1 = make_addr(0x10);
    address_t a2 = make_addr(0x20);
    address_t a3 = make_addr(0x30);
    address_t a4 = make_addr(0x40);
    address_t a5 = make_addr(0x50);

    /* a1: nonce + balance */
    evm_state_set_nonce(es, &a1, 42);
    uint256_t bal1 = uint256_from_uint64(1000000);
    evm_state_set_balance(es, &a1, &bal1);
    evm_state_mark_existed(es, &a1);

    /* a2: just balance */
    uint256_t bal2 = uint256_from_uint64(999);
    evm_state_set_balance(es, &a2, &bal2);
    evm_state_mark_existed(es, &a2);

    /* a3: nonce + storage */
    evm_state_set_nonce(es, &a3, 7);
    evm_state_mark_existed(es, &a3);
    uint256_t skey1 = uint256_from_uint64(1);
    uint256_t sval1 = uint256_from_uint64(0xDEADBEEF);
    evm_state_set_storage(es, &a3, &skey1, &sval1);
    uint256_t skey2 = uint256_from_uint64(2);
    uint256_t sval2 = uint256_from_uint64(0xCAFEBABE);
    evm_state_set_storage(es, &a3, &skey2, &sval2);

    /* a4: large balance (>8 bytes) + storage */
    uint8_t big_bal[32] = {0};
    big_bal[0] = 0x01; big_bal[15] = 0xFF; big_bal[31] = 0x01;
    uint256_t bal4 = uint256_from_bytes(big_bal, 32);
    evm_state_set_balance(es, &a4, &bal4);
    evm_state_set_nonce(es, &a4, 100);
    evm_state_mark_existed(es, &a4);
    for (uint64_t i = 0; i < 10; i++) {
        uint256_t sk = uint256_from_uint64(i + 100);
        uint256_t sv = uint256_from_uint64((i + 1) * 1111);
        evm_state_set_storage(es, &a4, &sk, &sv);
    }

    /* a5: nonce only (empty-ish but existed) */
    evm_state_set_nonce(es, &a5, 1);
    evm_state_mark_existed(es, &a5);

    /* Commit and compute root */
    evm_state_commit(es);
    evm_state_commit_tx(es);
    hash_t root1 = evm_state_compute_mpt_root(es, false);
    print_hash("original root", root1.bytes);

    /* Save */
    state_t *st = evm_state_get_state(es);
    state_begin_block(st, 12345);
    if (!state_save(st, TEST_FILE, &root1)) {
        printf("  FAIL: state_save\n");
        evm_state_destroy(es);
        return 1;
    }
    state_stats_t ss1 = state_get_stats(st);
    printf("  saved: %u accounts\n", ss1.account_count);
    evm_state_destroy(es);

    /* Load into fresh state */
    evm_state_t *es2 = evm_state_create(NULL);
    if (!es2) { printf("  FAIL: create2\n"); return 1; }
    state_t *st2 = evm_state_get_state(es2);
    hash_t loaded_root;
    if (!state_load(st2, TEST_FILE, &loaded_root)) {
        printf("  FAIL: state_load\n");
        evm_state_destroy(es2);
        return 1;
    }
    state_stats_t ss2 = state_get_stats(st2);
    printf("  loaded: %u accounts\n", ss2.account_count);

    /* Verify account data */
    int errors = 0;

    /* a1: nonce=42, balance=1000000 */
    if (state_get_nonce(st2, &a1) != 42) { printf("  FAIL: a1 nonce\n"); errors++; }
    uint256_t rb1 = state_get_balance(st2, &a1);
    if (!uint256_eq(&rb1, &bal1)) { printf("  FAIL: a1 balance\n"); errors++; }

    /* a3: storage */
    uint256_t rs1 = state_get_storage(st2, &a3, &skey1);
    if (!uint256_eq(&rs1, &sval1)) { printf("  FAIL: a3 storage[1]\n"); errors++; }
    uint256_t rs2 = state_get_storage(st2, &a3, &skey2);
    if (!uint256_eq(&rs2, &sval2)) { printf("  FAIL: a3 storage[2]\n"); errors++; }

    /* a4: 10 storage slots */
    for (uint64_t i = 0; i < 10; i++) {
        uint256_t sk = uint256_from_uint64(i + 100);
        uint256_t expected = uint256_from_uint64((i + 1) * 1111);
        uint256_t got = state_get_storage(st2, &a4, &sk);
        if (!uint256_eq(&got, &expected)) {
            printf("  FAIL: a4 storage[%lu]\n", i + 100);
            errors++;
        }
    }

    /* Mark all as existed + dirty for root computation */
    address_t addrs[] = {a1, a2, a3, a4, a5};
    for (int i = 0; i < 5; i++) {
        account_t *a = state_get_account(st2, &addrs[i]);
        if (a) {
            acct_set_flag(a, ACCT_EXISTED | ACCT_DIRTY | ACCT_IN_BLK_DIRTY);
            /* Re-mark storage dirty if present */
            if (state_has_storage(st2, &addrs[i]))
                acct_set_flag(a, ACCT_STORAGE_DIRTY);
        }
    }

    /* Compute root from loaded state */
    hash_t root2 = state_compute_root(st2, false);
    print_hash("loaded root ", root2.bytes);

    if (memcmp(root1.bytes, root2.bytes, 32) != 0) {
        printf("  FAIL: roots don't match!\n");
        errors++;
    } else {
        printf("  OK: roots match\n");
    }

    evm_state_destroy(es2);
    unlink(TEST_FILE);
    return errors;
}

static int test_empty_state(void) {
    printf("\ntest_empty_state:\n");

    evm_state_t *es = evm_state_create(NULL);
    state_t *st = evm_state_get_state(es);
    state_begin_block(st, 0);

    if (!state_save(st, TEST_FILE, NULL)) {
        printf("  FAIL: save empty\n");
        evm_state_destroy(es);
        return 1;
    }

    evm_state_t *es2 = evm_state_create(NULL);
    state_t *st2 = evm_state_get_state(es2);
    hash_t loaded_root;
    if (!state_load(st2, TEST_FILE, &loaded_root)) {
        printf("  FAIL: load empty\n");
        evm_state_destroy(es);
        evm_state_destroy(es2);
        return 1;
    }

    state_stats_t ss = state_get_stats(st2);
    if (ss.account_count != 0) {
        printf("  FAIL: expected 0 accounts, got %u\n", ss.account_count);
        evm_state_destroy(es);
        evm_state_destroy(es2);
        return 1;
    }

    printf("  OK: empty state round-trip\n");
    evm_state_destroy(es);
    evm_state_destroy(es2);
    unlink(TEST_FILE);
    return 0;
}

/**
 * Test with many accounts + storage to exercise bitmap growth.
 * Uses invalidate_all + compute_mpt_root (the real chain_replay flow).
 */
static int test_large_round_trip(void) {
    printf("\ntest_large_round_trip:\n");

    evm_state_t *es = evm_state_create(NULL);
    if (!es) { printf("  FAIL: create\n"); return 1; }

    state_t *st = evm_state_get_state(es);
    state_begin_block(st, 99999);

    /* Create 2000 accounts, 500 with storage (5 slots each) */
    address_t addrs[2000];
    for (int i = 0; i < 2000; i++) {
        addrs[i] = (address_t){0};
        addrs[i].bytes[0] = (uint8_t)(i >> 8);
        addrs[i].bytes[1] = (uint8_t)(i & 0xFF);

        evm_state_set_nonce(es, &addrs[i], (uint64_t)(i + 1));
        uint256_t bal = uint256_from_uint64((uint64_t)(i + 1) * 1000);
        evm_state_set_balance(es, &addrs[i], &bal);
        evm_state_mark_existed(es, &addrs[i]);

        /* Every 4th account gets storage */
        if (i % 4 == 0) {
            for (int j = 0; j < 5; j++) {
                uint256_t sk = uint256_from_uint64((uint64_t)(i * 100 + j));
                uint256_t sv = uint256_from_uint64((uint64_t)(i * 100 + j + 1));
                evm_state_set_storage(es, &addrs[i], &sk, &sv);
            }
        }
    }

    /* Compute root through the real path */
    evm_state_commit(es);
    evm_state_clear_prestate_dirty(es);
    evm_state_invalidate_all(es);
    hash_t root1 = evm_state_compute_mpt_root(es, false);
    print_hash("original root", root1.bytes);

    state_stats_t ss1 = state_get_stats(st);
    printf("  accounts=%u resources=%u\n", ss1.account_count, ss1.storage_account_count);

    /* Save */
    if (!state_save(st, TEST_FILE, &root1)) {
        printf("  FAIL: save\n");
        evm_state_destroy(es);
        return 1;
    }
    evm_state_destroy(es);

    /* Load into fresh state */
    evm_state_t *es2 = evm_state_create(NULL);
    state_t *st2 = evm_state_get_state(es2);
    hash_t loaded_root;
    if (!state_load(st2, TEST_FILE, &loaded_root)) {
        printf("  FAIL: load\n");
        evm_state_destroy(es2);
        return 1;
    }

    state_stats_t ss2 = state_get_stats(st2);
    printf("  loaded: accounts=%u resources=%u\n",
           ss2.account_count, ss2.storage_account_count);

    /* Recompute root through invalidate_all + compute_mpt_root (chain_replay path) */
    evm_state_invalidate_all(es2);
    hash_t root2 = evm_state_compute_mpt_root(es2, false);
    print_hash("loaded root ", root2.bytes);

    int errors = 0;
    if (memcmp(root1.bytes, root2.bytes, 32) != 0) {
        printf("  FAIL: roots don't match!\n");
        errors++;
    } else {
        printf("  OK: roots match\n");
    }

    /* Verify sample data */
    for (int i = 0; i < 2000; i += 100) {
        if (state_get_nonce(st2, &addrs[i]) != (uint64_t)(i + 1)) {
            printf("  FAIL: acct %d nonce\n", i);
            errors++;
        }
        if (i % 4 == 0) {
            uint256_t sk = uint256_from_uint64((uint64_t)(i * 100));
            uint256_t expected = uint256_from_uint64((uint64_t)(i * 100 + 1));
            uint256_t got = state_get_storage(st2, &addrs[i], &sk);
            if (!uint256_eq(&got, &expected)) {
                printf("  FAIL: acct %d storage\n", i);
                errors++;
            }
        }
    }

    /* Execute a block after load — verify state works for execution */
    evm_state_begin_block(es2, 100000);
    uint256_t new_bal = uint256_from_uint64(999999);
    evm_state_set_balance(es2, &addrs[0], &new_bal);
    evm_state_commit_tx(es2);
    evm_state_invalidate_all(es2);
    hash_t root3 = evm_state_compute_mpt_root(es2, false);
    if (memcmp(root2.bytes, root3.bytes, 32) == 0) {
        printf("  FAIL: root unchanged after modification\n");
        errors++;
    } else {
        printf("  OK: root changed after post-load modification\n");
    }

    evm_state_destroy(es2);
    unlink(TEST_FILE);
    return errors;
}

int main(void) {
    int errors = 0;
    errors += test_basic_round_trip();
    errors += test_empty_state();
    errors += test_large_round_trip();
    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
