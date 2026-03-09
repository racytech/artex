/*
 * MPT Store Tests — cross-validated against mpt_compute_root_batch().
 *
 * Every test computes the root via mpt_store (incremental) and via
 * mpt_compute_root_batch (batch rebuild), then asserts they match.
 */

#include "mpt_store.h"
#include "mpt.h"
#include "keccak256.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

/* =========================================================================
 * Test harness
 * ========================================================================= */

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
    static void name(void); \
    static void name##_wrapper(void) { \
        tests_run++; \
        printf("  %-50s ", #name); \
        name(); \
        tests_passed++; \
        printf("PASS\n"); \
    } \
    static void name(void)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("FAIL\n    %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        exit(1); \
    } \
} while(0)

#define ASSERT_HASH_EQ(a, b) do { \
    if (memcmp((a), (b), 32) != 0) { \
        printf("FAIL\n    %s:%d: hash mismatch\n", __FILE__, __LINE__); \
        printf("    got:  "); \
        for (int _i = 0; _i < 32; _i++) printf("%02x", (a)[_i]); \
        printf("\n    exp:  "); \
        for (int _i = 0; _i < 32; _i++) printf("%02x", (b)[_i]); \
        printf("\n"); \
        exit(1); \
    } \
} while(0)

/* =========================================================================
 * Helpers
 * ========================================================================= */

static const char *TEST_PATH = "/tmp/test_mpt_store";

static void cleanup_files(void) {
    unlink("/tmp/test_mpt_store.idx");
    unlink("/tmp/test_mpt_store.dat");
    unlink("/tmp/test_mpt_store.compact.idx");
    unlink("/tmp/test_mpt_store.compact.dat");
}

static void make_key(uint8_t key[32], uint64_t seed) {
    /* Generate a deterministic 32-byte key by hashing the seed */
    memset(key, 0, 32);
    memcpy(key, &seed, sizeof(seed));
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, key, 32);
    keccak_final(&ctx, key);
}

/* RLP-encode a simple uint64 value for use as leaf data.
 * Returns length written to `out` (max 9 bytes). */
static size_t rlp_encode_u64(uint64_t val, uint8_t *out) {
    if (val == 0) {
        out[0] = 0x80;
        return 1;
    }
    if (val < 0x80) {
        out[0] = (uint8_t)val;
        return 1;
    }
    /* Big-endian encoding */
    uint8_t be[8];
    size_t len = 0;
    uint64_t tmp = val;
    while (tmp > 0) {
        be[7 - len] = (uint8_t)(tmp & 0xFF);
        tmp >>= 8;
        len++;
    }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + 8 - len, len);
    return 1 + len;
}

/* Batch root helper: compute MPT root from key-value pairs using the
 * existing mpt_compute_root_batch function for cross-validation. */
static void batch_root(uint8_t keys[][32], uint8_t **values,
                       size_t *value_lens, size_t count, uint8_t root[32]) {
    if (count == 0) {
        /* Empty trie root */
        const uint8_t empty_rlp[] = {0x80};
        SHA3_CTX ctx;
        keccak_init(&ctx);
        keccak_update(&ctx, empty_rlp, 1);
        keccak_final(&ctx, root);
        return;
    }

    mpt_batch_entry_t *entries = calloc(count, sizeof(*entries));
    ASSERT(entries);
    for (size_t i = 0; i < count; i++) {
        memcpy(entries[i].key, keys[i], 32);
        entries[i].value     = values[i];
        entries[i].value_len = value_lens[i];
    }

    hash_t h;
    bool ok = mpt_compute_root_batch(entries, count, &h);
    ASSERT(ok);
    memcpy(root, h.bytes, 32);
    free(entries);
}

/* =========================================================================
 * Tests
 * ========================================================================= */

TEST(test_empty_trie) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t root[32];
    mpt_store_root(ms, root);

    /* Should match keccak256(0x80) */
    uint8_t expected[32];
    batch_root(NULL, NULL, NULL, 0, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_single_insert) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t key[32];
    make_key(key, 42);

    uint8_t val[9];
    size_t val_len = rlp_encode_u64(1000, val);

    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key, val, val_len));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    /* Cross-validate */
    uint8_t keys[1][32];
    memcpy(keys[0], key, 32);
    uint8_t *values[] = { val };
    size_t lens[] = { val_len };
    uint8_t expected[32];
    batch_root(keys, values, lens, 1, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_two_inserts_same_batch) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t k1[32], k2[32];
    make_key(k1, 1);
    make_key(k2, 2);

    uint8_t v1[9], v2[9];
    size_t v1_len = rlp_encode_u64(100, v1);
    size_t v2_len = rlp_encode_u64(200, v2);

    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, k1, v1, v1_len));
    ASSERT(mpt_store_update(ms, k2, v2, v2_len));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t keys[2][32];
    memcpy(keys[0], k1, 32); memcpy(keys[1], k2, 32);
    uint8_t *values[] = { v1, v2 };
    size_t lens[] = { v1_len, v2_len };
    uint8_t expected[32];
    batch_root(keys, values, lens, 2, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_two_inserts_separate_batches) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t k1[32], k2[32];
    make_key(k1, 10);
    make_key(k2, 20);

    uint8_t v1[9], v2[9];
    size_t v1_len = rlp_encode_u64(111, v1);
    size_t v2_len = rlp_encode_u64(222, v2);

    /* First batch: insert k1 */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, k1, v1, v1_len));
    ASSERT(mpt_store_commit_batch(ms));

    /* Second batch: insert k2 */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, k2, v2, v2_len));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t keys[2][32];
    memcpy(keys[0], k1, 32); memcpy(keys[1], k2, 32);
    uint8_t *values[] = { v1, v2 };
    size_t lens[] = { v1_len, v2_len };
    uint8_t expected[32];
    batch_root(keys, values, lens, 2, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_update_existing_key) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t key[32];
    make_key(key, 99);

    uint8_t v1[9], v2[9];
    size_t v1_len = rlp_encode_u64(500, v1);
    size_t v2_len = rlp_encode_u64(999, v2);

    /* Insert */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key, v1, v1_len));
    ASSERT(mpt_store_commit_batch(ms));

    /* Update */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key, v2, v2_len));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t keys[1][32];
    memcpy(keys[0], key, 32);
    uint8_t *values[] = { v2 };
    size_t lens[] = { v2_len };
    uint8_t expected[32];
    batch_root(keys, values, lens, 1, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_delete_single) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t key[32];
    make_key(key, 77);

    uint8_t val[9];
    size_t val_len = rlp_encode_u64(42, val);

    /* Insert */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key, val, val_len));
    ASSERT(mpt_store_commit_batch(ms));

    /* Delete */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_delete(ms, key));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    /* Should be empty trie root */
    uint8_t expected[32];
    batch_root(NULL, NULL, NULL, 0, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_delete_one_of_two) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t k1[32], k2[32];
    make_key(k1, 100);
    make_key(k2, 200);

    uint8_t v1[9], v2[9];
    size_t v1_len = rlp_encode_u64(11, v1);
    size_t v2_len = rlp_encode_u64(22, v2);

    /* Insert both */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, k1, v1, v1_len));
    ASSERT(mpt_store_update(ms, k2, v2, v2_len));
    ASSERT(mpt_store_commit_batch(ms));

    /* Delete k1, keep k2 */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_delete(ms, k1));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    /* Cross-validate: should be single-key trie with k2 */
    uint8_t keys[1][32];
    memcpy(keys[0], k2, 32);
    uint8_t *values[] = { v2 };
    size_t lens[] = { v2_len };
    uint8_t expected[32];
    batch_root(keys, values, lens, 1, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_batch_100_keys) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 4096);
    ASSERT(ms);

    #define N 100
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];
    uint8_t *val_ptrs[N];

    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)i);
        lens[i] = rlp_encode_u64((uint64_t)(i + 1) * 100, vals[i]);
        val_ptrs[i] = vals[i];
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(keys, val_ptrs, lens, N, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_incremental_100_keys) {
    /* Insert keys one-by-one in separate batches, verify at each step */
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 4096);
    ASSERT(ms);

    #define N 100
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];
    uint8_t *val_ptrs[N];

    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)(i + 1000));
        lens[i] = rlp_encode_u64((uint64_t)(i + 1), vals[i]);
        val_ptrs[i] = vals[i];

        ASSERT(mpt_store_begin_batch(ms));
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
        ASSERT(mpt_store_commit_batch(ms));
    }

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(keys, val_ptrs, lens, N, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_mixed_insert_update_delete) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 4096);
    ASSERT(ms);

    #define N 50
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];

    /* Insert 50 keys */
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)i);
        lens[i] = rlp_encode_u64((uint64_t)(i + 1), vals[i]);
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    /* Delete even keys, update odd keys with new values */
    uint8_t new_vals[N][9];
    size_t  new_lens[N];
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        if (i % 2 == 0) {
            ASSERT(mpt_store_delete(ms, keys[i]));
        } else {
            new_lens[i] = rlp_encode_u64((uint64_t)((i + 1) * 1000), new_vals[i]);
            ASSERT(mpt_store_update(ms, keys[i], new_vals[i], new_lens[i]));
        }
    }
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    /* Build expected: only odd keys with updated values */
    size_t count = 0;
    uint8_t exp_keys[N][32];
    uint8_t *exp_vals[N];
    size_t   exp_lens[N];
    for (int i = 0; i < N; i++) {
        if (i % 2 == 1) {
            memcpy(exp_keys[count], keys[i], 32);
            exp_vals[count] = new_vals[i];
            exp_lens[count] = new_lens[i];
            count++;
        }
    }

    uint8_t expected[32];
    batch_root(exp_keys, exp_vals, exp_lens, count, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_persistence) {
    cleanup_files();

    uint8_t k1[32], k2[32];
    make_key(k1, 555);
    make_key(k2, 666);

    uint8_t v1[9], v2[9];
    size_t v1_len = rlp_encode_u64(111, v1);
    size_t v2_len = rlp_encode_u64(222, v2);

    uint8_t saved_root[32];

    /* Create and populate */
    {
        mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
        ASSERT(ms);
        ASSERT(mpt_store_begin_batch(ms));
        ASSERT(mpt_store_update(ms, k1, v1, v1_len));
        ASSERT(mpt_store_update(ms, k2, v2, v2_len));
        ASSERT(mpt_store_commit_batch(ms));
        mpt_store_root(ms, saved_root);
        mpt_store_sync(ms);
        mpt_store_destroy(ms);
    }

    /* Reopen and verify root */
    {
        mpt_store_t *ms = mpt_store_open(TEST_PATH);
        ASSERT(ms);
        uint8_t root[32];
        mpt_store_root(ms, root);
        ASSERT_HASH_EQ(root, saved_root);

        /* Insert more data and verify */
        uint8_t k3[32];
        make_key(k3, 777);
        uint8_t v3[9];
        size_t v3_len = rlp_encode_u64(333, v3);

        ASSERT(mpt_store_begin_batch(ms));
        ASSERT(mpt_store_update(ms, k3, v3, v3_len));
        ASSERT(mpt_store_commit_batch(ms));

        mpt_store_root(ms, root);

        uint8_t keys[3][32];
        memcpy(keys[0], k1, 32); memcpy(keys[1], k2, 32); memcpy(keys[2], k3, 32);
        uint8_t *values[] = { v1, v2, v3 };
        size_t lens[] = { v1_len, v2_len, v3_len };
        uint8_t expected[32];
        batch_root(keys, values, lens, 3, expected);
        ASSERT_HASH_EQ(root, expected);

        mpt_store_destroy(ms);
    }

    cleanup_files();
}

TEST(test_compact) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 4096);
    ASSERT(ms);

    #define N 50
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];
    uint8_t *val_ptrs[N];

    /* Insert 50 keys */
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)(i + 2000));
        lens[i] = rlp_encode_u64((uint64_t)(i + 1), vals[i]);
        val_ptrs[i] = vals[i];
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    /* Delete half to create garbage */
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i += 2) {
        ASSERT(mpt_store_delete(ms, keys[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root_before[32];
    mpt_store_root(ms, root_before);

    /* Compact */
    ASSERT(mpt_store_compact(ms));

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);

    /* Root must be unchanged */
    ASSERT_HASH_EQ(root_after, root_before);

    /* Cross-validate against batch */
    size_t count = 0;
    uint8_t exp_keys[N][32];
    uint8_t *exp_vals[N];
    size_t   exp_lens[N];
    for (int i = 1; i < N; i += 2) {
        memcpy(exp_keys[count], keys[i], 32);
        exp_vals[count] = vals[i];
        exp_lens[count] = lens[i];
        count++;
    }
    uint8_t expected[32];
    batch_root(exp_keys, exp_vals, exp_lens, count, expected);
    ASSERT_HASH_EQ(root_after, expected);

    /* Can still insert after compact */
    uint8_t new_key[32];
    make_key(new_key, 9999);
    uint8_t new_val[9];
    size_t new_val_len = rlp_encode_u64(9999, new_val);

    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, new_key, new_val, new_val_len));
    ASSERT(mpt_store_commit_batch(ms));

    /* Verify with updated set */
    memcpy(exp_keys[count], new_key, 32);
    exp_vals[count] = new_val;
    exp_lens[count] = new_val_len;
    count++;

    mpt_store_root(ms, root_after);
    batch_root(exp_keys, exp_vals, exp_lens, count, expected);
    ASSERT_HASH_EQ(root_after, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_discard_batch) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    uint8_t key[32];
    make_key(key, 123);
    uint8_t val[9];
    size_t val_len = rlp_encode_u64(42, val);

    /* Insert one key */
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key, val, val_len));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root_before[32];
    mpt_store_root(ms, root_before);

    /* Begin batch, stage changes, then discard */
    uint8_t key2[32];
    make_key(key2, 456);
    ASSERT(mpt_store_begin_batch(ms));
    ASSERT(mpt_store_update(ms, key2, val, val_len));
    mpt_store_discard_batch(ms);

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);
    ASSERT_HASH_EQ(root_after, root_before);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_batch_1000_keys) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 8192);
    ASSERT(ms);

    #define N 1000
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];
    uint8_t *val_ptrs[N];

    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)(i + 5000));
        lens[i] = rlp_encode_u64((uint64_t)(i + 1) * 7, vals[i]);
        val_ptrs[i] = vals[i];
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(keys, val_ptrs, lens, N, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_16_keys_diverging_at_root) {
    /* 16 keys whose keccak hashes start with different nibbles */
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    /* Find 16 seeds whose keccak hashes start with nibbles 0-F */
    uint8_t keys[16][32];
    uint8_t vals[16][9];
    size_t  lens[16];
    uint8_t *val_ptrs[16];
    bool found[16];
    memset(found, 0, sizeof(found));

    int total_found = 0;
    for (uint64_t seed = 0; total_found < 16 && seed < 100000; seed++) {
        uint8_t k[32];
        make_key(k, seed);
        uint8_t nib = k[0] >> 4;
        if (!found[nib]) {
            found[nib] = true;
            memcpy(keys[total_found], k, 32);
            lens[total_found] = rlp_encode_u64(seed + 1, vals[total_found]);
            val_ptrs[total_found] = vals[total_found];
            total_found++;
        }
    }
    ASSERT(total_found == 16);

    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < 16; i++)
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(keys, val_ptrs, lens, 16, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
}

TEST(test_same_prefix_keys) {
    /* Generate keys that share a long common prefix.
     * Use values >= 32 bytes so leaf RLP is always >= 32 bytes,
     * avoiding inline node differences with the reference impl
     * (mpt.c doesn't implement inlining per Ethereum spec). */
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    #define N 10
    uint8_t keys[N][32];
    uint8_t vals[N][34];  /* 32-byte values wrapped as RLP byte string = 34 bytes */
    size_t  lens[N];
    uint8_t *val_ptrs[N];

    uint8_t base[32];
    make_key(base, 12345);

    for (int i = 0; i < N; i++) {
        memcpy(keys[i], base, 32);
        keys[i][30] = (uint8_t)(i >> 8);
        keys[i][31] = (uint8_t)(i & 0xFF);
        /* Create a 32-byte value (large enough to avoid inline nodes) */
        uint8_t raw[32];
        memset(raw, 0, 32);
        raw[0] = (uint8_t)(i + 1);
        raw[31] = (uint8_t)(i * 7);
        /* RLP-encode as 32-byte string: 0xa0 + 32 bytes = 33 bytes */
        vals[i][0] = 0xa0;
        memcpy(vals[i] + 1, raw, 32);
        lens[i] = 33;
        val_ptrs[i] = vals[i];
    }

    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++)
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(keys, val_ptrs, lens, N, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

TEST(test_delete_all) {
    cleanup_files();
    mpt_store_t *ms = mpt_store_create(TEST_PATH, 1024);
    ASSERT(ms);

    #define N 20
    uint8_t keys[N][32];
    uint8_t vals[N][9];
    size_t  lens[N];

    /* Insert */
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++) {
        make_key(keys[i], (uint64_t)(i + 3000));
        lens[i] = rlp_encode_u64((uint64_t)(i + 1), vals[i]);
        ASSERT(mpt_store_update(ms, keys[i], vals[i], lens[i]));
    }
    ASSERT(mpt_store_commit_batch(ms));

    /* Delete all */
    ASSERT(mpt_store_begin_batch(ms));
    for (int i = 0; i < N; i++)
        ASSERT(mpt_store_delete(ms, keys[i]));
    ASSERT(mpt_store_commit_batch(ms));

    uint8_t root[32];
    mpt_store_root(ms, root);

    uint8_t expected[32];
    batch_root(NULL, NULL, NULL, 0, expected);
    ASSERT_HASH_EQ(root, expected);

    mpt_store_destroy(ms);
    cleanup_files();
    #undef N
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("MPT Store Tests\n");
    printf("================\n");

    test_empty_trie_wrapper();
    test_single_insert_wrapper();
    test_two_inserts_same_batch_wrapper();
    test_two_inserts_separate_batches_wrapper();
    test_update_existing_key_wrapper();
    test_delete_single_wrapper();
    test_delete_one_of_two_wrapper();
    test_batch_100_keys_wrapper();
    test_incremental_100_keys_wrapper();
    test_mixed_insert_update_delete_wrapper();
    test_persistence_wrapper();
    test_compact_wrapper();
    test_discard_batch_wrapper();
    test_batch_1000_keys_wrapper();
    test_16_keys_diverging_at_root_wrapper();
    test_same_prefix_keys_wrapper();
    test_delete_all_wrapper();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
