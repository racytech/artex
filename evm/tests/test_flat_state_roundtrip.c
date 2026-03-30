/**
 * Targeted tests for flat_state persistence correctness.
 *
 * Tests:
 * 1. Round-trip: compute root in-memory, flush, reopen, compute again → must match
 * 2. flush_deferred dedup: put same key N times, flush, reopen → correct count
 * 3. Stale slots after CREATE: SLOAD + CREATE → old slot must not appear
 * 4. EIP-161 boundary: pre-SD and post-SD touched empty accounts
 * 5. Evict + reopen round-trip: evict_cache → reopen → root must match
 */

#include "evm_state.h"
#include "flat_state.h"
#include "flat_store.h"
#include "compact_art.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define FLAT_PATH "/dev/shm/test_roundtrip_flat"
#define ASSERT(cond, msg) do { \
    if (!(cond)) { fprintf(stderr, "FAIL [%s:%d] %s\n", __func__, __LINE__, msg); fails++; } \
    else { passes++; } \
} while (0)

static int passes = 0, fails = 0;

static void cleanup(void) {
    remove(FLAT_PATH "_acct.art");
    remove(FLAT_PATH "_stor.art");
}

static address_t make_addr(uint8_t seed) {
    address_t a = {0};
    a.bytes[19] = seed;
    return a;
}

/* =========================================================================
 * Test 1: Basic round-trip — in-memory root must match reopened root
 * ========================================================================= */
static void test_roundtrip_basic(void) {
    cleanup();
    printf("  test_roundtrip_basic...\n");

    evm_state_t *es = evm_state_create(NULL);
    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(es && fs, "create state");
    evm_state_set_flat_state(es, fs);

    /* Populate state */
    for (int i = 1; i <= 100; i++) {
        address_t a = make_addr(i);
        uint256_t bal = uint256_from_uint64(i * 1000);
        evm_state_set_balance(es, &a, &bal);
        evm_state_set_nonce(es, &a, i);
        evm_state_mark_existed(es, &a);
    }

    /* Set some storage */
    address_t a1 = make_addr(1);
    for (int i = 0; i < 10; i++) {
        uint256_t key = uint256_from_uint64(i);
        uint256_t val = uint256_from_uint64(i + 100);
        evm_state_set_storage(es, &a1, &key, &val);
    }

    evm_state_commit(es);

    /* Compute root in-memory */
    hash_t root1 = evm_state_compute_mpt_root(es, false);
    ASSERT(memcmp(root1.bytes, ((hash_t){0}).bytes, 32) != 0, "root1 non-zero");

    /* Evict (flushes all to flat_state + disk) */
    evm_state_evict_cache(es);

    /* Destroy and reopen */
    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);

    /* Reopen */
    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen flat_state");
    if (!fs) return;

    uint64_t acct_count = flat_state_account_count(fs);
    printf("    reopened: %lu accounts\n", (unsigned long)acct_count);
    ASSERT(acct_count == 100, "100 accounts after reopen");

    /* Compute root from reopened flat_state */
    es = evm_state_create(NULL);
    evm_state_set_flat_state(es, fs);

    /* Load all accounts into cache (ensure_account) */
    for (int i = 1; i <= 100; i++) {
        address_t a = make_addr(i);
        evm_state_get_balance(es, &a);
        evm_state_mark_existed(es, &a);
    }
    /* Load storage */
    for (int i = 0; i < 10; i++) {
        uint256_t key = uint256_from_uint64(i);
        evm_state_get_storage(es, &a1, &key);
    }

    evm_state_commit(es);
    hash_t root2 = evm_state_compute_mpt_root(es, false);

    printf("    root1: ");
    for (int i = 0; i < 8; i++) printf("%02x", root1.bytes[i]);
    printf("...\n    root2: ");
    for (int i = 0; i < 8; i++) printf("%02x", root2.bytes[i]);
    printf("...\n");

    ASSERT(memcmp(root1.bytes, root2.bytes, 32) == 0, "root1 == root2 (round-trip)");

    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 2: flush_deferred dedup — same key put N times
 * ========================================================================= */
static void test_flush_deferred_dedup(void) {
    cleanup();
    printf("  test_flush_deferred_dedup...\n");

    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(fs != NULL, "create flat_state");
    if (!fs) return;

    /* Put same account 10 times with different balances */
    uint8_t addr_hash[32] = {0};
    addr_hash[0] = 0xAA;
    for (int i = 0; i < 10; i++) {
        flat_account_record_t rec = {0};
        rec.nonce = i + 1;
        flat_state_put_account(fs, addr_hash, &rec);
    }

    /* Flush + destroy */
    flat_store_flush_deferred(flat_state_account_store(fs));
    flat_state_destroy(fs);

    /* Reopen and check */
    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen");
    if (!fs) { cleanup(); return; }

    uint64_t count = flat_state_account_count(fs);
    printf("    accounts after 10 puts of same key: %lu\n", (unsigned long)count);
    ASSERT(count == 1, "exactly 1 account (deduped)");

    /* Verify latest value */
    flat_account_record_t rec;
    bool found = flat_state_get_account(fs, addr_hash, &rec);
    ASSERT(found, "account found");
    ASSERT(rec.nonce == 10, "nonce == 10 (latest put)");

    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 3: Multiple flush cycles — put, flush, put again, flush, reopen
 * ========================================================================= */
static void test_multi_flush_roundtrip(void) {
    cleanup();
    printf("  test_multi_flush_roundtrip...\n");

    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(fs != NULL, "create");
    if (!fs) return;

    flat_store_t *store = flat_state_account_store(fs);
    uint8_t key[32] = {0};
    key[0] = 0xBB;

    /* Cycle 1: put + flush */
    flat_account_record_t rec1 = {0};
    rec1.nonce = 100;
    flat_state_put_account(fs, key, &rec1);
    flat_store_flush_deferred(store);

    /* Cycle 2: put same key + flush */
    flat_account_record_t rec2 = {0};
    rec2.nonce = 200;
    flat_state_put_account(fs, key, &rec2);
    flat_store_flush_deferred(store);

    /* Cycle 3: put same key + flush */
    flat_account_record_t rec3 = {0};
    rec3.nonce = 300;
    flat_state_put_account(fs, key, &rec3);
    flat_store_flush_deferred(store);

    flat_state_destroy(fs);

    /* Reopen */
    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen");
    if (!fs) { cleanup(); return; }

    uint64_t count = flat_state_account_count(fs);
    printf("    accounts after 3 flush cycles: %lu\n", (unsigned long)count);
    ASSERT(count == 1, "exactly 1 account");

    flat_account_record_t rec;
    ASSERT(flat_state_get_account(fs, key, &rec), "found");
    printf("    nonce: %lu (expected 300)\n", (unsigned long)rec.nonce);
    ASSERT(rec.nonce == 300, "nonce == 300 (latest)");

    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 4: Many accounts — flush, reopen, count must match
 * ========================================================================= */
static void test_bulk_roundtrip(void) {
    cleanup();
    printf("  test_bulk_roundtrip...\n");

    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(fs != NULL, "create");
    if (!fs) return;

    /* Write 10000 accounts */
    for (int i = 0; i < 10000; i++) {
        uint8_t key[32] = {0};
        key[0] = (i >> 24) & 0xFF;
        key[1] = (i >> 16) & 0xFF;
        key[2] = (i >> 8) & 0xFF;
        key[3] = i & 0xFF;
        flat_account_record_t rec = {0};
        rec.nonce = i + 1;
        flat_state_put_account(fs, key, &rec);
    }

    flat_store_flush_deferred(flat_state_account_store(fs));

    /* Now overwrite half of them */
    for (int i = 0; i < 5000; i++) {
        uint8_t key[32] = {0};
        key[0] = (i >> 24) & 0xFF;
        key[1] = (i >> 16) & 0xFF;
        key[2] = (i >> 8) & 0xFF;
        key[3] = i & 0xFF;
        flat_account_record_t rec = {0};
        rec.nonce = i + 10001;
        flat_state_put_account(fs, key, &rec);
    }

    flat_store_flush_deferred(flat_state_account_store(fs));
    flat_state_destroy(fs);

    /* Reopen */
    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen");
    if (!fs) { cleanup(); return; }

    uint64_t count = flat_state_account_count(fs);
    printf("    accounts: %lu (expected 10000)\n", (unsigned long)count);
    ASSERT(count == 10000, "10000 accounts");

    /* Verify updated values */
    for (int i = 0; i < 5000; i++) {
        uint8_t key[32] = {0};
        key[0] = (i >> 24) & 0xFF;
        key[1] = (i >> 16) & 0xFF;
        key[2] = (i >> 8) & 0xFF;
        key[3] = i & 0xFF;
        flat_account_record_t rec;
        if (flat_state_get_account(fs, key, &rec)) {
            ASSERT(rec.nonce == (uint64_t)(i + 10001), "updated nonce");
        } else {
            ASSERT(0, "account missing");
        }
    }

    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 5: evm_state full cycle — multiple checkpoints with evict
 * ========================================================================= */
static void test_checkpoint_evict_roundtrip(void) {
    cleanup();
    printf("  test_checkpoint_evict_roundtrip...\n");

    evm_state_t *es = evm_state_create(NULL);
    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(es && fs, "create");
    evm_state_set_flat_state(es, fs);

    /* Checkpoint 1: create 50 accounts */
    for (int i = 1; i <= 50; i++) {
        address_t a = make_addr(i);
        uint256_t bal = uint256_from_uint64(i * 100);
        evm_state_set_balance(es, &a, &bal);
        evm_state_mark_existed(es, &a);
    }
    evm_state_commit(es);
    hash_t root1 = evm_state_compute_mpt_root(es, false);
    evm_state_evict_cache(es);

    /* Checkpoint 2: modify 25 accounts, add 25 new ones */
    for (int i = 1; i <= 25; i++) {
        address_t a = make_addr(i);
        uint256_t bal = uint256_from_uint64(i * 200);
        evm_state_set_balance(es, &a, &bal);
    }
    for (int i = 51; i <= 75; i++) {
        address_t a = make_addr(i);
        uint256_t bal = uint256_from_uint64(i * 100);
        evm_state_set_balance(es, &a, &bal);
        evm_state_mark_existed(es, &a);
    }
    evm_state_commit(es);
    hash_t root2 = evm_state_compute_mpt_root(es, false);
    evm_state_evict_cache(es);

    ASSERT(memcmp(root1.bytes, root2.bytes, 32) != 0, "root changed after modifications");

    /* Destroy and reopen */
    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);

    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen");
    if (!fs) { cleanup(); return; }

    uint64_t count = flat_state_account_count(fs);
    printf("    accounts after 2 checkpoints: %lu (expected 75)\n", (unsigned long)count);
    ASSERT(count == 75, "75 accounts");

    /* Recompute root from reopened state */
    es = evm_state_create(NULL);
    evm_state_set_flat_state(es, fs);

    /* Load all accounts */
    for (int i = 1; i <= 75; i++) {
        address_t a = make_addr(i);
        evm_state_get_balance(es, &a);
    }
    evm_state_commit(es);
    hash_t root3 = evm_state_compute_mpt_root(es, false);

    printf("    root2: ");
    for (int i = 0; i < 8; i++) printf("%02x", root2.bytes[i]);
    printf("...\n    root3: ");
    for (int i = 0; i < 8; i++) printf("%02x", root3.bytes[i]);
    printf("...\n");

    ASSERT(memcmp(root2.bytes, root3.bytes, 32) == 0, "root2 == root3 (evict round-trip)");

    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 6: Stale slots after CREATE — SLOAD then CREATE at same address
 *
 * Simulates: Block A loads slot from existing contract.
 *            Block B does CREATE2 at same address (new contract).
 *            At checkpoint, the old slot must NOT appear in storage trie.
 * ========================================================================= */
static void test_stale_slots_after_create(void) {
    cleanup();
    printf("  test_stale_slots_after_create...\n");

    evm_state_t *es = evm_state_create(NULL);
    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(es && fs, "create");
    evm_state_set_flat_state(es, fs);

    address_t addr = make_addr(0x42);

    /* === Checkpoint 1: contract with storage === */
    /* Set up account with balance + storage */
    uint256_t bal = uint256_from_uint64(1000);
    evm_state_set_balance(es, &addr, &bal);
    evm_state_mark_existed(es, &addr);

    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t slot1 = uint256_from_uint64(1);
    uint256_t val100 = uint256_from_uint64(100);
    uint256_t val200 = uint256_from_uint64(200);
    evm_state_set_storage(es, &addr, &slot0, &val100);
    evm_state_set_storage(es, &addr, &slot1, &val200);

    evm_state_commit(es);
    hash_t root1 = evm_state_compute_mpt_root(es, false);
    printf("    root1 (with 2 slots): ");
    for (int i = 0; i < 8; i++) printf("%02x", root1.bytes[i]);
    printf("...\n");

    evm_state_evict_cache(es);

    /* Verify storage is in flat_state */
    hash_t addr_hash = hash_keccak256(addr.bytes, 20);
    hash_t slot0_hash = hash_keccak256((uint8_t *)&slot0, 32);
    uint8_t stor_val[32];
    bool found = flat_state_get_storage(fs, addr_hash.bytes, slot0_hash.bytes, stor_val);
    ASSERT(found, "slot0 in flat_state after checkpoint 1");

    /* === Checkpoint 2: SLOAD old slot, then CREATE at same address === */

    /* Block A: SLOAD reads slot0 (loads into cache from flat_state) */
    uint256_t loaded = evm_state_get_storage(es, &addr, &slot0);
    ASSERT(uint256_eq(&loaded, &val100), "SLOAD returns 100");

    /* Block B: CREATE2 at same address — resets account */
    evm_state_create_account(es, &addr);

    /* New contract writes slot 5 only (not slot 0 or 1) */
    uint256_t slot5 = uint256_from_uint64(5);
    uint256_t val500 = uint256_from_uint64(500);
    evm_state_set_storage(es, &addr, &slot5, &val500);

    /* Set the new contract as existing with some balance */
    uint256_t new_bal = uint256_from_uint64(2000);
    evm_state_set_balance(es, &addr, &new_bal);

    evm_state_commit_tx(es);
    evm_state_commit(es);
    hash_t root2 = evm_state_compute_mpt_root(es, false);
    printf("    root2 (after CREATE, 1 new slot): ");
    for (int i = 0; i < 8; i++) printf("%02x", root2.bytes[i]);
    printf("...\n");

    ASSERT(memcmp(root1.bytes, root2.bytes, 32) != 0, "root changed after CREATE");

    /* The storage root for this account should only have slot5=500.
     * slot0=100 and slot1=200 must NOT be in the storage trie. */

    /* Verify by evicting and reopening */
    evm_state_evict_cache(es);
    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);

    fs = flat_state_open(FLAT_PATH);
    ASSERT(fs != NULL, "reopen");
    if (!fs) { cleanup(); return; }

    es = evm_state_create(NULL);
    evm_state_set_flat_state(es, fs);

    /* Load account and its storage */
    evm_state_get_balance(es, &addr);
    evm_state_get_storage(es, &addr, &slot5);
    evm_state_mark_existed(es, &addr);
    evm_state_commit(es);
    hash_t root3 = evm_state_compute_mpt_root(es, false);
    printf("    root3 (after reopen): ");
    for (int i = 0; i < 8; i++) printf("%02x", root3.bytes[i]);
    printf("...\n");

    ASSERT(memcmp(root2.bytes, root3.bytes, 32) == 0,
           "root2 == root3 (stale slots not in reopened state)");

    /* Check that old slots are NOT in flat_state */
    found = flat_state_get_storage(fs, addr_hash.bytes, slot0_hash.bytes, stor_val);
    ASSERT(!found, "slot0 must NOT be in flat_state after CREATE");

    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Test 7: Stale slots survive evict after CREATE
 *
 * Same as test 6 but adds a THIRD checkpoint after evict to verify
 * stale slots don't reappear.
 * ========================================================================= */
static void test_stale_slots_survive_evict(void) {
    cleanup();
    printf("  test_stale_slots_survive_evict...\n");

    evm_state_t *es = evm_state_create(NULL);
    flat_state_t *fs = flat_state_create(FLAT_PATH);
    ASSERT(es && fs, "create");
    evm_state_set_flat_state(es, fs);

    address_t addr = make_addr(0x55);

    /* Checkpoint 1: contract with storage */
    uint256_t bal = uint256_from_uint64(1000);
    evm_state_set_balance(es, &addr, &bal);
    evm_state_mark_existed(es, &addr);

    uint256_t slot0 = uint256_from_uint64(0);
    uint256_t val100 = uint256_from_uint64(100);
    evm_state_set_storage(es, &addr, &slot0, &val100);

    evm_state_commit(es);
    evm_state_compute_mpt_root(es, false);
    evm_state_evict_cache(es);

    /* Checkpoint 2: SLOAD then CREATE */
    uint256_t loaded = evm_state_get_storage(es, &addr, &slot0);
    ASSERT(uint256_eq(&loaded, &val100), "SLOAD returns 100");

    evm_state_create_account(es, &addr);
    uint256_t slot9 = uint256_from_uint64(9);
    uint256_t val999 = uint256_from_uint64(999);
    evm_state_set_storage(es, &addr, &slot9, &val999);
    uint256_t new_bal = uint256_from_uint64(5000);
    evm_state_set_balance(es, &addr, &new_bal);

    evm_state_commit_tx(es);
    evm_state_commit(es);
    hash_t root_after_create = evm_state_compute_mpt_root(es, false);
    evm_state_evict_cache(es);

    /* Checkpoint 3: just read the account — root must be same */
    evm_state_get_balance(es, &addr);
    evm_state_get_storage(es, &addr, &slot9);

    evm_state_commit(es);
    hash_t root_checkpoint3 = evm_state_compute_mpt_root(es, false);

    printf("    root after CREATE: ");
    for (int i = 0; i < 8; i++) printf("%02x", root_after_create.bytes[i]);
    printf("...\n    root checkpoint3:  ");
    for (int i = 0; i < 8; i++) printf("%02x", root_checkpoint3.bytes[i]);
    printf("...\n");

    ASSERT(memcmp(root_after_create.bytes, root_checkpoint3.bytes, 32) == 0,
           "root stable after evict (stale slot0 not reappearing)");

    /* Verify slot0 is NOT in flat_state */
    hash_t addr_hash = hash_keccak256(addr.bytes, 20);
    hash_t slot0_hash = hash_keccak256((uint8_t *)&slot0, 32);
    uint8_t stor_val[32];
    bool found = flat_state_get_storage(fs, addr_hash.bytes, slot0_hash.bytes, stor_val);
    ASSERT(!found, "slot0 must NOT survive evict");

    evm_state_set_flat_state(es, NULL);
    evm_state_destroy(es);
    flat_state_destroy(fs);
    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */
int main(void) {
    printf("=== Flat State Round-Trip Tests ===\n\n");

    test_roundtrip_basic();
    test_flush_deferred_dedup();
    test_multi_flush_roundtrip();
    test_bulk_roundtrip();
    test_checkpoint_evict_roundtrip();
    test_stale_slots_after_create();
    test_stale_slots_survive_evict();

    printf("\n=== Results: %d passed, %d failed ===\n", passes, fails);
    return fails > 0 ? 1 : 0;
}
