/*
 * MPT Trie Correctness Test
 *
 * Tests mpt_root() correctness: insert/delete cycles, persistence,
 * incremental updates, value updates.
 *
 * Usage: ./test_mpt
 */

#include "../include/mpt.h"
#include "../include/hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#define MPT_PATH "/tmp/test_mpt.dat"

/* ========================================================================
 * SplitMix64 RNG + key/value generation
 * ======================================================================== */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);
    return r;
}

#define SEED 0x4D50545F54455354ULL

static void gen_key(uint8_t key[32], uint64_t index) {
    rng_t rng = rng_create(SEED ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static void gen_value(uint8_t *val, uint8_t *vlen, uint64_t index) {
    rng_t rng = rng_create(SEED ^ (index * 0x9ABCDEF012345678ULL));
    *vlen = 4 + (rng_next(&rng) % 28);  /* 4-31 bytes */
    for (int i = 0; i < *vlen; i += 8) {
        uint64_t r = rng_next(&rng);
        int remain = *vlen - i;
        memcpy(val + i, &r, remain < 8 ? remain : 8);
    }
}

/* ========================================================================
 * Helpers
 * ======================================================================== */

static int failures = 0;

#define CHECK(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("  FAIL [%s:%d]: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        failures++; \
        return; \
    } \
} while(0)

static void print_hash(const char *label, const hash_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 8; i++) printf("%02x", h->bytes[i]);
    printf("...\n");
}

/* ========================================================================
 * Phase 1: Single key (edge case)
 * ======================================================================== */

static void test_single_key(void) {
    printf("\n=== Single Key ===\n");
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    uint8_t key[32], val[4] = {0xDE, 0xAD, 0xBE, 0xEF};
    gen_key(key, 42);
    mpt_put(m, key, val, 4);

    hash_t root = mpt_root(m);
    print_hash("single key root", &root);

    /* Verify non-empty */
    uint8_t empty_rlp = 0x80;
    hash_t empty = hash_keccak256(&empty_rlp, 1);
    CHECK(!hash_equal(&root, &empty), "single key should not be empty root");
    printf("  single key root is non-empty\n");

    /* Delete and check empty */
    mpt_delete(m, key);
    hash_t root_after = mpt_root(m);
    CHECK(hash_equal(&root_after, &empty), "not empty after delete");
    printf("  delete single key -> empty root OK\n");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 2: Batch insert + root consistency
 * ======================================================================== */

static void test_batch_insert(uint32_t n) {
    printf("\n=== Batch Insert: %u keys ===\n", n);
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    for (uint32_t i = 0; i < n; i++) {
        uint8_t key[32], val[32];
        uint8_t vlen;
        gen_key(key, i);
        gen_value(val, &vlen, i);
        mpt_put(m, key, val, vlen);
    }

    hash_t root = mpt_root(m);
    print_hash("root", &root);
    CHECK(mpt_size(m) == n, "size should be %u, got %lu", n, mpt_size(m));

    /* Call mpt_root again — should return same hash (idempotent) */
    hash_t root2 = mpt_root(m);
    CHECK(hash_equal(&root, &root2), "second mpt_root should be identical");

    printf("  size=%lu, nodes=%u, leaves=%u\n",
           mpt_size(m), mpt_nodes(m), mpt_leaves(m));

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 3: Insert + delete cycle
 * ======================================================================== */

static void test_insert_delete(void) {
    printf("\n=== Insert + Delete Cycle ===\n");
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    /* Empty root = keccak(0x80) */
    hash_t empty_root = mpt_root(m);
    uint8_t empty_rlp = 0x80;
    hash_t expected_empty = hash_keccak256(&empty_rlp, 1);
    CHECK(hash_equal(&empty_root, &expected_empty), "empty root mismatch");
    printf("  empty root OK\n");

    /* Insert 100 keys */
    uint8_t keys[100][32];
    uint8_t vals[100][32];
    uint8_t vlens[100];
    for (int i = 0; i < 100; i++) {
        gen_key(keys[i], i);
        gen_value(vals[i], &vlens[i], i);
        mpt_put(m, keys[i], vals[i], vlens[i]);
    }
    hash_t root_100 = mpt_root(m);
    printf("  root after 100 inserts OK (size=%lu)\n", mpt_size(m));
    CHECK(mpt_size(m) == 100, "size should be 100");

    /* Delete all 100 keys */
    for (int i = 0; i < 100; i++)
        mpt_delete(m, keys[i]);

    hash_t root_empty_again = mpt_root(m);
    CHECK(mpt_size(m) == 0, "size should be 0 after deletes");
    CHECK(hash_equal(&root_empty_again, &expected_empty),
          "root should be empty after deleting all keys");
    printf("  root after delete-all = empty root OK\n");

    /* Re-insert same keys — should produce same root */
    for (int i = 0; i < 100; i++)
        mpt_put(m, keys[i], vals[i], vlens[i]);
    hash_t root_100_again = mpt_root(m);
    CHECK(hash_equal(&root_100, &root_100_again),
          "root should match after re-insert");
    printf("  re-insert produces same root OK\n");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 4: Persistence (commit + reopen)
 * ======================================================================== */

static void test_persistence(void) {
    printf("\n=== Persistence ===\n");
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    for (uint32_t i = 0; i < 1000; i++) {
        uint8_t key[32], val[32];
        uint8_t vlen;
        gen_key(key, i);
        gen_value(val, &vlen, i);
        mpt_put(m, key, val, vlen);
    }

    hash_t root_before = mpt_root(m);
    CHECK(mpt_commit(m), "commit failed");
    printf("  committed 1000 keys\n");
    print_hash("root before", &root_before);
    mpt_close(m);

    /* Reopen + verify */
    m = mpt_open(MPT_PATH);
    CHECK(m != NULL, "mpt_open failed");
    CHECK(mpt_size(m) == 1000, "size mismatch after reopen: %lu", mpt_size(m));

    hash_t root_after = mpt_root(m);
    print_hash("root after ", &root_after);
    CHECK(hash_equal(&root_before, &root_after),
          "root hash changed after reopen");
    printf("  root matches after reopen\n");

    /* Insert more + commit again */
    for (uint32_t i = 1000; i < 2000; i++) {
        uint8_t key[32], val[32];
        uint8_t vlen;
        gen_key(key, i);
        gen_value(val, &vlen, i);
        mpt_put(m, key, val, vlen);
    }
    hash_t root_2000 = mpt_root(m);
    CHECK(mpt_commit(m), "second commit failed");
    mpt_close(m);

    /* Reopen again */
    m = mpt_open(MPT_PATH);
    CHECK(m != NULL, "second reopen failed");
    CHECK(mpt_size(m) == 2000, "size should be 2000: %lu", mpt_size(m));
    hash_t root_2000_check = mpt_root(m);
    CHECK(hash_equal(&root_2000, &root_2000_check),
          "root changed after second reopen");
    printf("  2000 keys survived second reopen\n");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 5: Value update (same key, different value)
 * ======================================================================== */

static void test_update_value(void) {
    printf("\n=== Value Update ===\n");
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    /* Insert 50 keys */
    for (uint32_t i = 0; i < 50; i++) {
        uint8_t key[32], val[32];
        uint8_t vlen;
        gen_key(key, i);
        gen_value(val, &vlen, i);
        mpt_put(m, key, val, vlen);
    }
    hash_t root1 = mpt_root(m);

    /* Update first 10 keys with new values */
    for (uint32_t i = 0; i < 10; i++) {
        uint8_t key[32], val[32];
        uint8_t vlen;
        gen_key(key, i);
        gen_value(val, &vlen, i + 1000);  /* different value seed */
        mpt_put(m, key, val, vlen);
    }
    hash_t root2 = mpt_root(m);
    CHECK(!hash_equal(&root1, &root2), "root should change after updates");
    CHECK(mpt_size(m) == 50, "size should still be 50: %lu", mpt_size(m));
    printf("  root changed after value update, size unchanged\n");

    /* Calling mpt_root again should be idempotent */
    hash_t root2b = mpt_root(m);
    CHECK(hash_equal(&root2, &root2b), "mpt_root not idempotent after update");

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 6: Incremental updates (simulate blocks)
 * ======================================================================== */

static void test_incremental(void) {
    printf("\n=== Incremental Updates ===\n");
    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    CHECK(m != NULL, "mpt_create failed");

    /* Build up state over 10 blocks of 1000 keys each */
    hash_t prev_root = mpt_root(m);
    for (int block = 0; block < 10; block++) {
        for (int i = 0; i < 1000; i++) {
            uint32_t idx = block * 1000 + i;
            uint8_t key[32], val[32];
            uint8_t vlen;
            gen_key(key, idx);
            gen_value(val, &vlen, idx);
            mpt_put(m, key, val, vlen);
        }
        hash_t root = mpt_root(m);
        CHECK(!hash_equal(&root, &prev_root), "root unchanged at block %d", block);
        prev_root = root;
    }

    CHECK(mpt_size(m) == 10000, "size should be 10000: %lu", mpt_size(m));
    printf("  10 blocks x 1000 keys = %lu keys\n", mpt_size(m));
    print_hash("final root", &prev_root);

    mpt_close(m);
    unlink(MPT_PATH);
    printf("  PASS\n");
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(void) {
    printf("=== MPT Trie Correctness Tests ===\n");

    test_single_key();
    test_batch_insert(10);
    test_batch_insert(100);
    test_batch_insert(1000);
    test_insert_delete();
    test_persistence();
    test_update_value();
    test_incremental();

    printf("\n========================================\n");
    if (failures == 0)
        printf("ALL TESTS PASSED\n");
    else
        printf("FAILURES: %d\n", failures);
    printf("========================================\n");

    return failures;
}
