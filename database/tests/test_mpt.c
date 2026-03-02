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
 * Phase 7: 64-byte keys (mpt_create_ex)
 * ======================================================================== */

#define MPT_PATH_64 "/tmp/test_mpt_64.dat"

static void gen_key_64(uint8_t key[64], uint64_t prefix_seed, uint64_t suffix_seed) {
    rng_t rng = rng_create(SEED ^ (prefix_seed * 0x517cc1b727220a95ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
    rng = rng_create(SEED ^ (suffix_seed * 0x9ABCDEF012345678ULL));
    for (int i = 32; i < 64; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static void test_64byte_keys(void) {
    printf("\n=== 64-Byte Keys ===\n");
    unlink(MPT_PATH_64);

    mpt_t *m = mpt_create_ex(MPT_PATH_64, 64, 33);
    CHECK(m != NULL, "mpt_create_ex failed");

    /* Insert 100 keys with 33-byte values */
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[64];
        gen_key_64(key, i / 10, i);
        uint8_t val[33];
        uint8_t vlen = 1 + (i % 32);
        memset(val, (uint8_t)(i + 1), vlen);
        mpt_put(m, key, val, vlen);
    }

    CHECK(mpt_size(m) == 100, "size should be 100, got %lu", mpt_size(m));
    hash_t root1 = mpt_root(m);

    /* mpt_get: verify all keys */
    for (uint32_t i = 0; i < 100; i++) {
        uint8_t key[64];
        gen_key_64(key, i / 10, i);
        uint8_t val_out[33];
        uint8_t vlen_out = 0;
        CHECK(mpt_get(m, key, val_out, &vlen_out), "mpt_get failed for key %u", i);
        uint8_t expected_len = 1 + (i % 32);
        CHECK(vlen_out == expected_len, "vlen mismatch: %u vs %u", vlen_out, expected_len);
        for (uint8_t j = 0; j < vlen_out; j++)
            CHECK(val_out[j] == (uint8_t)(i + 1), "value byte mismatch at key %u", i);
    }
    printf("  insert + get 100 keys:     OK\n");

    /* Idempotent root */
    hash_t root1b = mpt_root(m);
    CHECK(hash_equal(&root1, &root1b), "root not idempotent");

    /* Delete 50 keys, verify remaining */
    for (uint32_t i = 0; i < 50; i++) {
        uint8_t key[64];
        gen_key_64(key, i / 10, i);
        mpt_delete(m, key);
    }
    CHECK(mpt_size(m) == 50, "size should be 50 after deletes");
    hash_t root2 = mpt_root(m);
    CHECK(!hash_equal(&root1, &root2), "root should change after deletes");

    /* Verify deleted keys are gone */
    for (uint32_t i = 0; i < 50; i++) {
        uint8_t key[64];
        gen_key_64(key, i / 10, i);
        CHECK(!mpt_get(m, key, NULL, NULL), "deleted key %u should not be found", i);
    }

    /* Verify remaining keys still readable */
    for (uint32_t i = 50; i < 100; i++) {
        uint8_t key[64];
        gen_key_64(key, i / 10, i);
        uint8_t vlen_out = 0;
        CHECK(mpt_get(m, key, NULL, &vlen_out), "remaining key %u not found", i);
    }
    printf("  delete + verify remaining: OK\n");

    /* Persistence */
    CHECK(mpt_commit(m), "commit failed");
    mpt_close(m);

    m = mpt_open(MPT_PATH_64);
    CHECK(m != NULL, "mpt_open failed");
    CHECK(mpt_size(m) == 50, "size mismatch after reopen: %lu", mpt_size(m));
    hash_t root_reopen = mpt_root(m);
    CHECK(hash_equal(&root2, &root_reopen), "root changed after reopen");
    printf("  persistence:               OK\n");

    mpt_close(m);
    unlink(MPT_PATH_64);
    printf("  PASS\n");
}

/* ========================================================================
 * Phase 8: Subtree root (mpt_subtree_root)
 *
 * Verify: subtree root of a shared 64-byte key mpt at a 32-byte prefix
 * equals mpt_root of a standalone 32-byte key mpt with the same suffixes.
 * ======================================================================== */

#define SHARED_MPT_PATH   "/tmp/test_mpt_shared.dat"
#define STAND_A_MPT_PATH  "/tmp/test_mpt_stand_a.dat"
#define STAND_B_MPT_PATH  "/tmp/test_mpt_stand_b.dat"

static void make_rlp_value(uint64_t v, uint8_t *out, uint8_t *out_len) {
    /* Minimal RLP encoding for a small integer (storage value style) */
    if (v == 0) {
        out[0] = 0x80;
        *out_len = 1;
    } else if (v < 0x80) {
        out[0] = (uint8_t)v;
        *out_len = 1;
    } else {
        /* Big-endian encode, trim leading zeros */
        uint8_t be[8];
        for (int i = 7; i >= 0; i--) {
            be[i] = (uint8_t)(v & 0xFF);
            v >>= 8;
        }
        int start = 0;
        while (start < 7 && be[start] == 0) start++;
        uint8_t len = 8 - start;
        out[0] = 0x80 + len;
        memcpy(out + 1, be + start, len);
        *out_len = 1 + len;
    }
}

static void test_subtree_root(void) {
    printf("\n=== Subtree Root ===\n");
    unlink(SHARED_MPT_PATH);
    unlink(STAND_A_MPT_PATH);
    unlink(STAND_B_MPT_PATH);

    /* Two account prefixes (32 bytes each) */
    uint8_t prefix_a[32], prefix_b[32];
    gen_key(prefix_a, 9000);
    gen_key(prefix_b, 9001);

    /* Create shared 64-byte key mpt (max_value=33 for RLP storage values) */
    mpt_t *shared = mpt_create_ex(SHARED_MPT_PATH, 64, 33);
    CHECK(shared != NULL, "mpt_create_ex failed");

    /* Create standalone 32-byte key mpts */
    mpt_t *stand_a = mpt_create(STAND_A_MPT_PATH);
    mpt_t *stand_b = mpt_create(STAND_B_MPT_PATH);
    CHECK(stand_a != NULL, "standalone A create failed");
    CHECK(stand_b != NULL, "standalone B create failed");

    /* Insert 30 storage slots for account A */
    for (int i = 0; i < 30; i++) {
        uint8_t suffix[32];
        gen_key(suffix, 5000 + i);

        uint8_t rlp_val[33];
        uint8_t rlp_len;
        make_rlp_value((uint64_t)(i + 1) * 100, rlp_val, &rlp_len);

        /* Shared: prefix_a || suffix */
        uint8_t key64[64];
        memcpy(key64, prefix_a, 32);
        memcpy(key64 + 32, suffix, 32);
        mpt_put(shared, key64, rlp_val, rlp_len);

        /* Standalone A: just suffix */
        mpt_put(stand_a, suffix, rlp_val, rlp_len);
    }

    /* Insert 20 storage slots for account B */
    for (int i = 0; i < 20; i++) {
        uint8_t suffix[32];
        gen_key(suffix, 6000 + i);

        uint8_t rlp_val[33];
        uint8_t rlp_len;
        make_rlp_value((uint64_t)(i + 1) * 200, rlp_val, &rlp_len);

        uint8_t key64[64];
        memcpy(key64, prefix_b, 32);
        memcpy(key64 + 32, suffix, 32);
        mpt_put(shared, key64, rlp_val, rlp_len);

        mpt_put(stand_b, suffix, rlp_val, rlp_len);
    }

    CHECK(mpt_size(shared) == 50, "shared size should be 50");

    /* Compute subtree roots from shared mpt */
    hash_t subtree_a = mpt_subtree_root(shared, prefix_a, 64);  /* 64 nibbles = 32 bytes */
    hash_t subtree_b = mpt_subtree_root(shared, prefix_b, 64);

    /* Compute standalone roots */
    hash_t root_a = mpt_root(stand_a);
    hash_t root_b = mpt_root(stand_b);

    /* Must match */
    CHECK(hash_equal(&subtree_a, &root_a),
          "subtree root A must equal standalone root A");
    CHECK(hash_equal(&subtree_b, &root_b),
          "subtree root B must equal standalone root B");
    printf("  subtree == standalone:     OK\n");

    /* Different accounts should have different roots */
    CHECK(!hash_equal(&subtree_a, &subtree_b),
          "different accounts should have different roots");
    printf("  A != B:                    OK\n");

    /* Empty prefix should return empty trie hash */
    uint8_t prefix_empty[32];
    gen_key(prefix_empty, 9999);
    hash_t subtree_empty = mpt_subtree_root(shared, prefix_empty, 64);
    uint8_t empty_rlp = 0x80;
    hash_t expected_empty = hash_keccak256(&empty_rlp, 1);
    CHECK(hash_equal(&subtree_empty, &expected_empty),
          "empty prefix should give empty trie hash");
    printf("  empty prefix:              OK\n");

    /* Update a slot in account A, verify root changes */
    {
        uint8_t suffix[32];
        gen_key(suffix, 5000);  /* same slot as first insert */

        uint8_t rlp_val[33];
        uint8_t rlp_len;
        make_rlp_value(99999, rlp_val, &rlp_len);

        uint8_t key64[64];
        memcpy(key64, prefix_a, 32);
        memcpy(key64 + 32, suffix, 32);
        mpt_put(shared, key64, rlp_val, rlp_len);

        mpt_put(stand_a, suffix, rlp_val, rlp_len);
    }

    hash_t subtree_a2 = mpt_subtree_root(shared, prefix_a, 64);
    hash_t root_a2 = mpt_root(stand_a);
    CHECK(!hash_equal(&subtree_a, &subtree_a2), "A root should change after update");
    CHECK(hash_equal(&subtree_a2, &root_a2), "updated subtree A should match standalone A");

    /* Account B should be unchanged */
    hash_t subtree_b2 = mpt_subtree_root(shared, prefix_b, 64);
    CHECK(hash_equal(&subtree_b, &subtree_b2), "B root should be unchanged");
    printf("  incremental update:        OK\n");

    /* Persistence: commit, reopen, verify subtree roots */
    CHECK(mpt_commit(shared), "shared commit failed");
    mpt_close(shared);

    shared = mpt_open(SHARED_MPT_PATH);
    CHECK(shared != NULL, "shared reopen failed");

    hash_t subtree_a3 = mpt_subtree_root(shared, prefix_a, 64);
    hash_t subtree_b3 = mpt_subtree_root(shared, prefix_b, 64);
    CHECK(hash_equal(&subtree_a2, &subtree_a3), "A root should survive reopen");
    CHECK(hash_equal(&subtree_b, &subtree_b3), "B root should survive reopen");
    printf("  persistence:               OK\n");

    mpt_close(shared);
    mpt_close(stand_a);
    mpt_close(stand_b);
    unlink(SHARED_MPT_PATH);
    unlink(STAND_A_MPT_PATH);
    unlink(STAND_B_MPT_PATH);
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
    test_64byte_keys();
    test_subtree_root();

    printf("\n========================================\n");
    if (failures == 0)
        printf("ALL TESTS PASSED\n");
    else
        printf("FAILURES: %d\n", failures);
    printf("========================================\n");

    return failures;
}
