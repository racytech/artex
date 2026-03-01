#include "../include/nibble_trie.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define TEST_PATH "/tmp/test_stress_nt.dat"
#define TEST_PATH_64 "/tmp/test_stress_nt_64.dat"

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define FAIL(fmt, ...) do { \
    printf("  FAIL [line %d]: " fmt "\n", __LINE__, ##__VA_ARGS__); \
    tests_failed++; \
    return 0; \
} while(0)

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) FAIL(fmt, ##__VA_ARGS__); \
} while(0)

#define RUN_TEST(fn) do { \
    unlink(TEST_PATH); \
    unlink(TEST_PATH_64); \
    tests_run++; \
    printf("%-55s ", #fn); \
    fflush(stdout); \
    struct timespec t0, t1; \
    clock_gettime(CLOCK_MONOTONIC, &t0); \
    int _ok = fn(); \
    clock_gettime(CLOCK_MONOTONIC, &t1); \
    double ms = (t1.tv_sec - t0.tv_sec)*1e3 + \
                (t1.tv_nsec - t0.tv_nsec)/1e6; \
    if (_ok) { printf("PASS  (%.0fms)\n", ms); tests_passed++; } \
    else     { printf("FAIL  (%.0fms)\n", ms); } \
    unlink(TEST_PATH); \
    unlink(TEST_PATH_64); \
} while(0)

/* ========================================================================
 * Deterministic PRNG (xorshift64)
 * ======================================================================== */

static uint64_t rng_state;

static void rng_seed(uint64_t s) { rng_state = s ? s : 1; }

static uint64_t rng_next(void) {
    uint64_t x = rng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    rng_state = x;
    return x;
}

static uint32_t rng_u32(void) { return (uint32_t)(rng_next() >> 32); }

/* ========================================================================
 * Key/value generators (all produce 32-byte arrays)
 * ======================================================================== */

static void key_sequential(uint8_t *key, uint32_t i) {
    memset(key, 0, 32);
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = i & 0xFF;
    key[16] = (i * 7) & 0xFF;
    key[17] = (i * 13) & 0xFF;
    key[31] = (i * 31) & 0xFF;
}

static void key_random(uint8_t *key) {
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next();
        int n = 32 - i < 8 ? 32 - i : 8;
        memcpy(key + i, &r, n);
    }
}

/* Dense prefix: first 24 bytes identical, last 8 vary */
static void key_dense_prefix(uint8_t *key, uint32_t i) {
    memset(key, 0xAA, 24);
    key[24] = (i >> 24) & 0xFF;
    key[25] = (i >> 16) & 0xFF;
    key[26] = (i >> 8) & 0xFF;
    key[27] = i & 0xFF;
    key[28] = (i * 3) & 0xFF;
    key[29] = (i * 7) & 0xFF;
    key[30] = (i * 13) & 0xFF;
    key[31] = (i * 31) & 0xFF;
}

/* Keys differ only at one byte position */
static void key_single_byte_diff(uint8_t *key, uint32_t i, int pos) {
    memset(key, 0x55, 32);
    key[pos] = (uint8_t)i;
}

static void make_val(uint8_t *val, uint32_t i) {
    memset(val, 0, 32);
    uint32_t v = i * 1000 + 42;
    memcpy(val, &v, 4);
    val[16] = (i * 3) & 0xFF;
    val[31] = (i * 17) & 0xFF;
}

/* ========================================================================
 * 1. Sequential insert + verify + delete (50K)
 * ======================================================================== */

static int test_sequential_50k(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 50000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        ASSERT(nt_insert(&t, key, val), "insert i=%d", i);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size=%zu expected=%d",
           nt_size(&t), N);

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "val mismatch i=%d", i);
    }

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty, size=%zu", nt_size(&t));

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) == NULL, "still present i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 2. Random keys (20K)
 * ======================================================================== */

static int test_random_keys_20k(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 20000;
    rng_seed(0xDEADBEEF);

    uint8_t (*keys)[32] = malloc(N * 32);
    ASSERT(keys != NULL, "malloc");

    uint8_t val[32];
    for (int i = 0; i < N; i++) {
        key_random(keys[i]);
        make_val(val, i);
        nt_insert(&t, keys[i], val);
    }

    int found = 0;
    for (int i = 0; i < N; i++) {
        if (nt_get(&t, keys[i]) != NULL) found++;
    }
    ASSERT(found > 0, "no keys found");
    ASSERT(nt_size(&t) == (size_t)found, "size mismatch");

    /* Delete all, verify */
    for (int i = N - 1; i >= 0; i--)
        nt_delete(&t, keys[i]);
    ASSERT(nt_size(&t) == 0, "not empty after delete, size=%zu", nt_size(&t));

    free(keys);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 3. Dense prefix (10K) — stress extension splitting/merging
 * ======================================================================== */

static int test_dense_prefix_10k(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 10000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        key_dense_prefix(key, i);
        make_val(val, i);
        ASSERT(nt_insert(&t, key, val), "insert i=%d", i);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size");

    for (int i = 0; i < N; i++) {
        key_dense_prefix(key, i);
        make_val(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "val i=%d", i);
    }

    for (int i = 0; i < N; i++) {
        key_dense_prefix(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 4. Full nibble fanout — 16 children at depth 0
 * ======================================================================== */

static int test_full_fanout_16(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    /* Insert 16 keys differing only in first nibble */
    for (int n = 0; n < 16; n++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)(n << 4);  /* high nibble of byte 0 */
        make_val(val, n);
        ASSERT(nt_insert(&t, key, val), "insert n=%d", n);
    }
    ASSERT(nt_size(&t) == 16, "size=%zu", nt_size(&t));

    for (int n = 0; n < 16; n++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)(n << 4);
        make_val(val, n);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing n=%d", n);
        ASSERT(memcmp(got, val, 32) == 0, "val n=%d", n);
    }

    /* Delete all */
    for (int n = 0; n < 16; n++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)(n << 4);
        ASSERT(nt_delete(&t, key), "delete n=%d", n);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 5. Multi-level fanout — 256 keys at byte 0 (all nibble combos)
 * ======================================================================== */

static int test_multi_fanout_256(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    for (int b = 0; b < 256; b++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)b;
        make_val(val, b);
        ASSERT(nt_insert(&t, key, val), "insert b=%d", b);
    }
    ASSERT(nt_size(&t) == 256, "size");

    for (int b = 0; b < 256; b++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)b;
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing b=%d", b);
    }

    for (int b = 0; b < 256; b++) {
        memset(key, 0, 32);
        key[0] = (uint8_t)b;
        ASSERT(nt_delete(&t, key), "delete b=%d", b);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 6. Random interleaved insert/delete (50K ops)
 * ======================================================================== */

static int test_random_ops_50k(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    rng_seed(0x12345678);
    const int N = 50000;
    size_t expected_size = 0;

    /* Phase 1: insert N/2 */
    uint8_t (*keys)[32] = malloc((N / 2) * 32);
    ASSERT(keys != NULL, "malloc");
    uint8_t val[32];

    for (int i = 0; i < N / 2; i++) {
        key_random(keys[i]);
        make_val(val, i);
        nt_insert(&t, keys[i], val);
        expected_size++;
    }

    /* Phase 2: interleave insert/delete */
    for (int i = 0; i < N / 2; i++) {
        if (rng_u32() % 3 == 0 && expected_size > 0) {
            /* Delete a random existing key */
            int idx = rng_u32() % (N / 2);
            if (nt_delete(&t, keys[idx]))
                expected_size--;
        } else {
            /* Insert a new random key */
            uint8_t newkey[32];
            key_random(newkey);
            make_val(val, N + i);
            nt_insert(&t, newkey, val);
            expected_size++;
        }
    }

    ASSERT(nt_size(&t) == expected_size, "size=%zu expected=%zu",
           nt_size(&t), expected_size);

    free(keys);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 7. Iterator order after mixed operations
 * ======================================================================== */

static int test_iterator_order(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    rng_seed(0xCAFEBABE);
    const int N = 5000;
    uint8_t val[32];

    for (int i = 0; i < N; i++) {
        uint8_t key[32];
        key_random(key);
        make_val(val, i);
        nt_insert(&t, key, val);
    }

    /* Delete ~30% */
    rng_seed(0xCAFEBABE);
    for (int i = 0; i < N; i++) {
        uint8_t key[32];
        key_random(key);
        if (i % 3 == 0)
            nt_delete(&t, key);
    }

    /* Verify sorted order */
    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    int count = 0;
    uint8_t prev[32];
    memset(prev, 0, 32);

    while (nt_iterator_next(it)) {
        const uint8_t *k = nt_iterator_key(it);
        ASSERT(k != NULL, "null key");
        if (count > 0) {
            ASSERT(memcmp(prev, k, 32) < 0,
                   "order violation at count=%d", count);
        }
        memcpy(prev, k, 32);
        count++;
    }
    ASSERT((size_t)count == nt_size(&t), "iter count=%d size=%zu",
           count, nt_size(&t));

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 8. Commit + reopen (large)
 * ======================================================================== */

static int test_commit_reopen_large(void) {
    const int N = 10000;
    uint8_t key[32], val[32];

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");
        for (int i = 0; i < N; i++) {
            key_sequential(key, i);
            make_val(val, i);
            nt_insert(&t, key, val);
        }
        ASSERT(nt_commit(&t), "commit");
        nt_close(&t);
    }

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH, 32, 32), "reopen");
        ASSERT(nt_size(&t) == (size_t)N, "size=%zu", nt_size(&t));

        for (int i = 0; i < N; i++) {
            key_sequential(key, i);
            make_val(val, i);
            const uint8_t *got = nt_get(&t, key);
            ASSERT(got != NULL, "missing i=%d", i);
            ASSERT(memcmp(got, val, 32) == 0, "val i=%d", i);
        }
        nt_close(&t);
    }

    return 1;
}

/* ========================================================================
 * 9. Multi-commit cycles
 * ======================================================================== */

static int test_multi_commit_cycles(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];
    const int BATCH = 1000;
    const int CYCLES = 5;

    for (int c = 0; c < CYCLES; c++) {
        int base = c * BATCH;
        for (int i = 0; i < BATCH; i++) {
            key_sequential(key, base + i);
            make_val(val, base + i);
            nt_insert(&t, key, val);
        }
        ASSERT(nt_commit(&t), "commit cycle=%d", c);
    }
    ASSERT(nt_size(&t) == (size_t)(CYCLES * BATCH), "size");

    nt_close(&t);

    /* Reopen and verify all */
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "reopen");
    ASSERT(nt_size(&t) == (size_t)(CYCLES * BATCH), "size after reopen");

    for (int i = 0; i < CYCLES * BATCH; i++) {
        key_sequential(key, i);
        make_val(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "val i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 10. Rollback stress
 * ======================================================================== */

static int test_rollback_stress(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    /* Insert 500, commit */
    for (int i = 0; i < 500; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit");

    /* Insert 500 more, rollback */
    for (int i = 500; i < 1000; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == 1000, "size before rollback");
    nt_rollback(&t);
    ASSERT(nt_size(&t) == 500, "size after rollback");

    /* Verify only committed keys exist */
    for (int i = 0; i < 500; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) != NULL, "committed key missing i=%d", i);
    }
    for (int i = 500; i < 1000; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) == NULL, "rolled back key present i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 11. Mass update — insert N, then update all with new values
 * ======================================================================== */

static int test_mass_update(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 5000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after insert");

    /* Update all with new values */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i + 100000);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size unchanged after updates");

    /* Verify new values */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i + 100000);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "old val at i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 12. Seek stress — seek to random positions, verify lower-bound
 * ======================================================================== */

static int test_seek_stress(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    /* Insert every 10th key */
    uint8_t key[32], val[32];
    for (int i = 0; i < 5000; i += 10) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    /* Seek to each gap and verify we land on next */
    for (int i = 1; i < 5000; i += 10) {
        key_sequential(key, i);  /* not inserted */
        bool found = nt_iterator_seek(it, key);
        if (!found) continue;

        const uint8_t *k = nt_iterator_key(it);
        ASSERT(memcmp(k, key, 32) >= 0,
               "seek result < target at i=%d", i);
    }

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 13. Delete all + reinsert
 * ======================================================================== */

static int test_delete_all_reinsert(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 5000;
    uint8_t key[32], val[32];

    /* Insert all */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after insert");

    /* Delete all */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty after delete-all");
    ASSERT(nt_get(&t, key) == NULL, "not empty");

    /* Reinsert all with different values */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i + 50000);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after reinsert");

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i + 50000);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing after reinsert i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "wrong val after reinsert i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 14. Reverse insert, forward delete
 * ======================================================================== */

static int test_reverse_insert_forward_delete(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 10000;
    uint8_t key[32], val[32];

    /* Insert in reverse */
    for (int i = N - 1; i >= 0; i--) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size");

    /* Verify all */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) != NULL, "missing i=%d", i);
    }

    /* Delete forward */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 15. Seek then walk — verify seek+next produces correct sequence
 * ======================================================================== */

static int test_seek_then_walk(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    /* Insert 0, 100, 200, ..., 900 */
    for (int i = 0; i < 1000; i += 100) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    /* Seek to 250 — should land on 300 */
    key_sequential(key, 250);
    ASSERT(nt_iterator_seek(it, key), "seek");

    uint8_t expected[32];
    key_sequential(expected, 300);
    ASSERT(memcmp(nt_iterator_key(it), expected, 32) == 0,
           "seek landed wrong");

    /* Walk: 400, 500, 600, 700, 800, 900 */
    int expect_vals[] = {400, 500, 600, 700, 800, 900};
    for (int i = 0; i < 6; i++) {
        ASSERT(nt_iterator_next(it), "next i=%d", i);
        key_sequential(expected, expect_vals[i]);
        ASSERT(memcmp(nt_iterator_key(it), expected, 32) == 0,
               "walk mismatch at i=%d", i);
    }

    ASSERT(!nt_iterator_next(it), "should be done");
    ASSERT(nt_iterator_done(it), "done flag");

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 16. Last byte differs — stress deep extension splitting
 * ======================================================================== */

static int test_last_byte_diff(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 31);
        make_val(val, i);
        ASSERT(nt_insert(&t, key, val), "insert i=%d", i);
    }
    ASSERT(nt_size(&t) == 256, "size");

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 31);
        ASSERT(nt_get(&t, key) != NULL, "missing i=%d", i);
    }

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 31);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 17. First byte differs — stress shallow branching
 * ======================================================================== */

static int test_first_byte_diff(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 0);
        make_val(val, i);
        ASSERT(nt_insert(&t, key, val), "insert i=%d", i);
    }
    ASSERT(nt_size(&t) == 256, "size");

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 0);
        ASSERT(nt_get(&t, key) != NULL, "missing i=%d", i);
    }

    for (int i = 0; i < 256; i++) {
        key_single_byte_diff(key, i, 0);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 18. Commit + uncommitted interleave
 * ======================================================================== */

static int test_commit_uncommitted_interleave(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key[32], val[32];

    /* Committed batch */
    for (int i = 0; i < 500; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit");

    /* Uncommitted inserts + deletes */
    for (int i = 500; i < 800; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    for (int i = 0; i < 100; i++) {
        key_sequential(key, i);
        nt_delete(&t, key);
    }

    /* Close without commit */
    nt_close(&t);

    /* Reopen: should see committed state (500 keys) */
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "reopen");
    ASSERT(nt_size(&t) == 500, "committed size=%zu", nt_size(&t));

    for (int i = 0; i < 500; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) != NULL, "committed key missing i=%d", i);
    }
    for (int i = 500; i < 800; i++) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) == NULL, "uncommitted key present i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 19. Large scale (200K)
 * ======================================================================== */

static int test_large_200k(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 200000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size");

    /* Verify sample */
    for (int i = 0; i < N; i += 100) {
        key_sequential(key, i);
        make_val(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing i=%d", i);
        ASSERT(memcmp(got, val, 32) == 0, "val i=%d", i);
    }

    /* Delete first half */
    for (int i = 0; i < N / 2; i++) {
        key_sequential(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }
    ASSERT(nt_size(&t) == (size_t)(N / 2), "half size");

    /* Verify second half remains */
    for (int i = N / 2; i < N; i += 100) {
        key_sequential(key, i);
        ASSERT(nt_get(&t, key) != NULL, "missing second half i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 20. Iterator size consistency
 * ======================================================================== */

static int test_iterator_size_consistency(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    rng_seed(0xBEEFCAFE);
    uint8_t key[32], val[32];

    /* Insert random keys */
    for (int i = 0; i < 3000; i++) {
        key_random(key);
        make_val(val, i);
        nt_insert(&t, key, val);
    }

    /* Delete some */
    rng_seed(0xBEEFCAFE);
    for (int i = 0; i < 3000; i++) {
        key_random(key);
        if (i % 4 == 0)
            nt_delete(&t, key);
    }

    /* Iterator count must match size */
    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    int count = 0;
    while (nt_iterator_next(it))
        count++;

    ASSERT((size_t)count == nt_size(&t),
           "iter count=%d size=%zu", count, nt_size(&t));

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 21. Nibble boundary stress — keys differing at every nibble position
 * ======================================================================== */

static int test_nibble_boundary(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t base[32], key[32], val[32];
    memset(base, 0x77, 32);

    /* For each nibble position 0-63, create a key that differs only there */
    for (int nib = 0; nib < 64; nib++) {
        memcpy(key, base, 32);
        int byte_pos = nib / 2;
        if (nib & 1) {
            /* Low nibble */
            key[byte_pos] = (key[byte_pos] & 0xF0) | 0x0A;
        } else {
            /* High nibble */
            key[byte_pos] = (key[byte_pos] & 0x0F) | 0xA0;
        }
        make_val(val, nib);
        ASSERT(nt_insert(&t, key, val), "insert nib=%d", nib);
    }
    /* Also insert the base key itself */
    make_val(val, 999);
    nt_insert(&t, base, val);

    ASSERT(nt_size(&t) == 65, "size=%zu", nt_size(&t));

    /* Verify all */
    for (int nib = 0; nib < 64; nib++) {
        memcpy(key, base, 32);
        int byte_pos = nib / 2;
        if (nib & 1)
            key[byte_pos] = (key[byte_pos] & 0xF0) | 0x0A;
        else
            key[byte_pos] = (key[byte_pos] & 0x0F) | 0xA0;
        ASSERT(nt_get(&t, key) != NULL, "missing nib=%d", nib);
    }
    ASSERT(nt_get(&t, base) != NULL, "missing base");

    /* Delete all */
    for (int nib = 0; nib < 64; nib++) {
        memcpy(key, base, 32);
        int byte_pos = nib / 2;
        if (nib & 1)
            key[byte_pos] = (key[byte_pos] & 0xF0) | 0x0A;
        else
            key[byte_pos] = (key[byte_pos] & 0x0F) | 0xA0;
        ASSERT(nt_delete(&t, key), "delete nib=%d", nib);
    }
    ASSERT(nt_delete(&t, base), "delete base");
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 22. Extension merge chain — build long extensions, then trigger merges
 * ======================================================================== */

static int test_extension_merge_chain(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    uint8_t key_a[32], key_b[32], key_c[32];
    uint8_t val[32];

    /* Three keys: same first 28 bytes, differ at byte 28 */
    memset(key_a, 0x11, 32);
    memset(key_b, 0x11, 32);
    memset(key_c, 0x11, 32);
    key_a[28] = 0xAA;
    key_b[28] = 0xBB;
    key_c[28] = 0xCC;

    make_val(val, 1);
    nt_insert(&t, key_a, val);
    make_val(val, 2);
    nt_insert(&t, key_b, val);
    make_val(val, 3);
    nt_insert(&t, key_c, val);

    ASSERT(nt_size(&t) == 3, "size=3");

    /* Delete key_b — branch at byte 28 goes from 3 children to 2 */
    ASSERT(nt_delete(&t, key_b), "delete b");
    ASSERT(nt_size(&t) == 2, "size=2");
    ASSERT(nt_get(&t, key_a) != NULL, "a present");
    ASSERT(nt_get(&t, key_c) != NULL, "c present");

    /* Delete key_a — branch collapses to 1 child, extension merges */
    ASSERT(nt_delete(&t, key_a), "delete a");
    ASSERT(nt_size(&t) == 1, "size=1");
    ASSERT(nt_get(&t, key_c) != NULL, "c still present");

    /* Delete key_c — tree empty */
    ASSERT(nt_delete(&t, key_c), "delete c");
    ASSERT(nt_size(&t) == 0, "empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 23. Alternating insert/delete at scale — every other key
 * ======================================================================== */

static int test_alternating_insert_delete(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

    const int N = 20000;
    uint8_t key[32], val[32];

    /* Insert all */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        make_val(val, i);
        nt_insert(&t, key, val);
    }

    /* Delete every 3rd, verify remaining after each batch */
    for (int i = 0; i < N; i += 3) {
        key_sequential(key, i);
        ASSERT(nt_delete(&t, key), "delete i=%d", i);
    }

    /* Count via iterator */
    nt_iterator_t *it = nt_iterator_create(&t);
    int count = 0;
    while (nt_iterator_next(it)) count++;
    nt_iterator_destroy(it);

    ASSERT((size_t)count == nt_size(&t),
           "iter=%d size=%zu", count, nt_size(&t));

    /* Verify specific keys */
    for (int i = 0; i < N; i++) {
        key_sequential(key, i);
        if (i % 3 == 0)
            ASSERT(nt_get(&t, key) == NULL, "deleted present i=%d", i);
        else
            ASSERT(nt_get(&t, key) != NULL, "missing i=%d", i);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 24. Commit, delete half, commit, reopen — verify persistence across
 *     structural changes (extension collapses)
 * ======================================================================== */

static int test_commit_delete_commit_reopen(void) {
    const int N = 5000;
    uint8_t key[32], val[32];

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH, 32, 32), "open");

        for (int i = 0; i < N; i++) {
            key_sequential(key, i);
            make_val(val, i);
            nt_insert(&t, key, val);
        }
        ASSERT(nt_commit(&t), "commit 1");

        /* Delete first half */
        for (int i = 0; i < N / 2; i++) {
            key_sequential(key, i);
            nt_delete(&t, key);
        }
        ASSERT(nt_commit(&t), "commit 2");
        nt_close(&t);
    }

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH, 32, 32), "reopen");
        ASSERT(nt_size(&t) == (size_t)(N / 2), "size=%zu", nt_size(&t));

        for (int i = 0; i < N / 2; i++) {
            key_sequential(key, i);
            ASSERT(nt_get(&t, key) == NULL, "deleted still present i=%d", i);
        }
        for (int i = N / 2; i < N; i++) {
            key_sequential(key, i);
            make_val(val, i);
            const uint8_t *got = nt_get(&t, key);
            ASSERT(got != NULL, "missing i=%d", i);
            ASSERT(memcmp(got, val, 32) == 0, "val i=%d", i);
        }
        nt_close(&t);
    }

    return 1;
}

/* ========================================================================
 * Storage slot helpers (64B keys = addr_hash[32] || slot_hash[32], 4B values)
 * ======================================================================== */

static void make_storage_key(uint8_t key[64], uint32_t account, uint32_t slot) {
    /* Simulate keccak256(address): deterministic pseudo-hash from account id */
    memset(key, 0, 64);
    uint64_t seed = (uint64_t)account * 0x9E3779B97F4A7C15ULL;
    for (int i = 0; i < 32; i += 8) {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        int n = 32 - i < 8 ? 32 - i : 8;
        memcpy(key + i, &seed, n);
    }
    /* Simulate keccak256(slot): deterministic pseudo-hash from slot id */
    seed = (uint64_t)slot * 0x517CC1B727220A95ULL;
    for (int i = 0; i < 32; i += 8) {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        int n = 32 - i < 8 ? 32 - i : 8;
        memcpy(key + 32 + i, &seed, n);
    }
}

static void make_slot_ref(uint8_t val[4], uint32_t ref) {
    memcpy(val, &ref, 4);
}

/* ========================================================================
 * 25. Storage slots — multi-account insert + verify (64B keys, 4B values)
 * ======================================================================== */

static int test_storage_64b_basic(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

    const int ACCOUNTS = 50;
    const int SLOTS = 200;
    uint8_t key[64];
    uint8_t val[4];

    /* Insert: 50 accounts × 200 slots = 10K entries */
    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < SLOTS; s++) {
            make_storage_key(key, a, s);
            uint32_t ref = a * SLOTS + s;
            make_slot_ref(val, ref);
            ASSERT(nt_insert(&t, key, val), "insert a=%d s=%d", a, s);
        }
    }
    ASSERT(nt_size(&t) == (size_t)(ACCOUNTS * SLOTS),
           "size=%zu expected=%d", nt_size(&t), ACCOUNTS * SLOTS);

    /* Verify all */
    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < SLOTS; s++) {
            make_storage_key(key, a, s);
            const void *got = nt_get(&t, key);
            ASSERT(got != NULL, "missing a=%d s=%d", a, s);
            uint32_t expected_ref = a * SLOTS + s;
            uint32_t got_ref;
            memcpy(&got_ref, got, 4);
            ASSERT(got_ref == expected_ref,
                   "val mismatch a=%d s=%d got=%u exp=%u",
                   a, s, got_ref, expected_ref);
        }
    }

    /* Delete all */
    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < SLOTS; s++) {
            make_storage_key(key, a, s);
            ASSERT(nt_delete(&t, key), "delete a=%d s=%d", a, s);
        }
    }
    ASSERT(nt_size(&t) == 0, "not empty");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 26. Storage slots — per-account prefix grouping in iteration
 * ======================================================================== */

static int test_storage_64b_sorted_order(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

    const int ACCOUNTS = 20;
    const int SLOTS = 100;
    uint8_t key[64];
    uint8_t val[4];

    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < SLOTS; s++) {
            make_storage_key(key, a, s);
            uint32_t ref = a * SLOTS + s;
            make_slot_ref(val, ref);
            nt_insert(&t, key, val);
        }
    }

    /* Verify sorted order via iterator */
    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    int count = 0;
    uint8_t prev[64];
    memset(prev, 0, 64);

    while (nt_iterator_next(it)) {
        const uint8_t *k = nt_iterator_key(it);
        ASSERT(k != NULL, "null key");
        if (count > 0) {
            ASSERT(memcmp(prev, k, 64) < 0,
                   "order violation at count=%d", count);
        }
        memcpy(prev, k, 64);
        count++;
    }
    ASSERT((size_t)count == nt_size(&t),
           "iter count=%d size=%zu", count, nt_size(&t));

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 27. Storage slots — commit/reopen persistence (64B keys)
 * ======================================================================== */

static int test_storage_64b_commit_reopen(void) {
    const int ACCOUNTS = 30;
    const int SLOTS = 100;
    uint8_t key[64];
    uint8_t val[4];

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

        for (int a = 0; a < ACCOUNTS; a++) {
            for (int s = 0; s < SLOTS; s++) {
                make_storage_key(key, a, s);
                uint32_t ref = a * SLOTS + s;
                make_slot_ref(val, ref);
                nt_insert(&t, key, val);
            }
        }
        ASSERT(nt_commit(&t), "commit");
        nt_close(&t);
    }

    {
        nibble_trie_t t;
        ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "reopen");
        ASSERT(nt_size(&t) == (size_t)(ACCOUNTS * SLOTS),
               "size=%zu expected=%d", nt_size(&t), ACCOUNTS * SLOTS);

        /* Verify all values */
        for (int a = 0; a < ACCOUNTS; a++) {
            for (int s = 0; s < SLOTS; s++) {
                make_storage_key(key, a, s);
                const void *got = nt_get(&t, key);
                ASSERT(got != NULL, "missing a=%d s=%d", a, s);
                uint32_t expected_ref = a * SLOTS + s;
                uint32_t got_ref;
                memcpy(&got_ref, got, 4);
                ASSERT(got_ref == expected_ref,
                       "val a=%d s=%d got=%u exp=%u",
                       a, s, got_ref, expected_ref);
            }
        }
        nt_close(&t);
    }

    return 1;
}

/* ========================================================================
 * 28. Storage slots — mixed ops simulating block execution
 * ======================================================================== */

static int test_storage_64b_mixed_ops(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

    const int ACCOUNTS = 100;
    const int INITIAL_SLOTS = 50;
    uint8_t key[64];
    uint8_t val[4];

    /* Phase 1: insert initial state (100 accounts × 50 slots) */
    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < INITIAL_SLOTS; s++) {
            make_storage_key(key, a, s);
            uint32_t ref = a * 1000 + s;
            make_slot_ref(val, ref);
            nt_insert(&t, key, val);
        }
    }
    ASSERT(nt_commit(&t), "commit initial");
    size_t initial_size = nt_size(&t);
    ASSERT(initial_size == (size_t)(ACCOUNTS * INITIAL_SLOTS), "initial size");

    /* Phase 2: simulate block — update some, delete some, add new */
    rng_seed(0x53544F524147455FULL);
    size_t expected = initial_size;

    /* Update 1000 random existing slots */
    for (int i = 0; i < 1000; i++) {
        int a = rng_u32() % ACCOUNTS;
        int s = rng_u32() % INITIAL_SLOTS;
        make_storage_key(key, a, s);
        uint32_t new_ref = 100000 + i;
        make_slot_ref(val, new_ref);
        nt_insert(&t, key, val);
    }

    /* Delete 500 random slots (some might miss — that's fine) */
    int actual_deletes = 0;
    for (int i = 0; i < 500; i++) {
        int a = rng_u32() % ACCOUNTS;
        int s = rng_u32() % INITIAL_SLOTS;
        make_storage_key(key, a, s);
        if (nt_delete(&t, key)) {
            actual_deletes++;
            expected--;
        }
    }

    /* Add 2000 new slots (slot IDs beyond INITIAL_SLOTS) */
    for (int i = 0; i < 2000; i++) {
        int a = rng_u32() % ACCOUNTS;
        int s = INITIAL_SLOTS + i;
        make_storage_key(key, a, s);
        uint32_t ref = 200000 + i;
        make_slot_ref(val, ref);
        nt_insert(&t, key, val);
        expected++;
    }

    ASSERT(nt_size(&t) == expected,
           "size=%zu expected=%zu", nt_size(&t), expected);

    ASSERT(nt_commit(&t), "commit block");

    /* Rollback should be no-op after commit */
    nt_rollback(&t);
    ASSERT(nt_size(&t) == expected, "size after rollback");

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 29. Storage slots — large scale (50K slots across 500 accounts)
 * ======================================================================== */

static int test_storage_64b_large(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

    const int ACCOUNTS = 500;
    const int SLOTS_PER = 100;
    const int TOTAL = ACCOUNTS * SLOTS_PER;
    uint8_t key[64];
    uint8_t val[4];

    for (int a = 0; a < ACCOUNTS; a++) {
        for (int s = 0; s < SLOTS_PER; s++) {
            make_storage_key(key, a, s);
            uint32_t ref = a * SLOTS_PER + s;
            make_slot_ref(val, ref);
            nt_insert(&t, key, val);
        }
    }
    ASSERT(nt_size(&t) == (size_t)TOTAL, "size=%zu expected=%d",
           nt_size(&t), TOTAL);

    /* Verify random sample */
    rng_seed(0x4C41524745ULL);
    for (int i = 0; i < 5000; i++) {
        int a = rng_u32() % ACCOUNTS;
        int s = rng_u32() % SLOTS_PER;
        make_storage_key(key, a, s);
        const void *got = nt_get(&t, key);
        ASSERT(got != NULL, "missing a=%d s=%d", a, s);
        uint32_t expected_ref = a * SLOTS_PER + s;
        uint32_t got_ref;
        memcpy(&got_ref, got, 4);
        ASSERT(got_ref == expected_ref, "val a=%d s=%d", a, s);
    }

    /* Delete half the accounts, verify the other half */
    for (int a = 0; a < ACCOUNTS / 2; a++) {
        for (int s = 0; s < SLOTS_PER; s++) {
            make_storage_key(key, a, s);
            ASSERT(nt_delete(&t, key), "delete a=%d s=%d", a, s);
        }
    }
    ASSERT(nt_size(&t) == (size_t)(TOTAL / 2), "half size");

    for (int a = ACCOUNTS / 2; a < ACCOUNTS; a++) {
        int s = rng_u32() % SLOTS_PER;
        make_storage_key(key, a, s);
        ASSERT(nt_get(&t, key) != NULL, "missing after half-delete a=%d", a);
    }

    nt_close(&t);
    return 1;
}

/* ========================================================================
 * 30. Storage slots — seek with 64B keys
 * ======================================================================== */

static int test_storage_64b_seek(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH_64, 64, 4), "open");

    /* Insert slots for 10 accounts, 10 slots each */
    uint8_t key[64];
    uint8_t val[4];
    for (int a = 0; a < 10; a++) {
        for (int s = 0; s < 10; s++) {
            make_storage_key(key, a, s);
            uint32_t ref = a * 10 + s;
            make_slot_ref(val, ref);
            nt_insert(&t, key, val);
        }
    }
    ASSERT(nt_size(&t) == 100, "size");

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    /* Seek to each account's first slot, verify we land on or after it */
    for (int a = 0; a < 10; a++) {
        make_storage_key(key, a, 0);
        bool found = nt_iterator_seek(it, key);
        ASSERT(found, "seek a=%d", a);
        const uint8_t *k = nt_iterator_key(it);
        ASSERT(memcmp(k, key, 64) >= 0, "seek result < target a=%d", a);

        /* Walk a few entries */
        int walked = 0;
        while (nt_iterator_next(it) && walked < 5)
            walked++;
    }

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

/* ========================================================================
 * main
 * ======================================================================== */

int main(void) {
    printf("=== Nibble Trie Stress Tests ===\n\n");

    RUN_TEST(test_sequential_50k);
    RUN_TEST(test_random_keys_20k);
    RUN_TEST(test_dense_prefix_10k);
    RUN_TEST(test_full_fanout_16);
    RUN_TEST(test_multi_fanout_256);
    RUN_TEST(test_random_ops_50k);
    RUN_TEST(test_iterator_order);
    RUN_TEST(test_commit_reopen_large);
    RUN_TEST(test_multi_commit_cycles);
    RUN_TEST(test_rollback_stress);
    RUN_TEST(test_mass_update);
    RUN_TEST(test_seek_stress);
    RUN_TEST(test_delete_all_reinsert);
    RUN_TEST(test_reverse_insert_forward_delete);
    RUN_TEST(test_seek_then_walk);
    RUN_TEST(test_last_byte_diff);
    RUN_TEST(test_first_byte_diff);
    RUN_TEST(test_commit_uncommitted_interleave);
    RUN_TEST(test_large_200k);
    RUN_TEST(test_iterator_size_consistency);
    RUN_TEST(test_nibble_boundary);
    RUN_TEST(test_extension_merge_chain);
    RUN_TEST(test_alternating_insert_delete);
    RUN_TEST(test_commit_delete_commit_reopen);

    printf("\n--- Storage Slots (64B keys, 4B values) ---\n\n");

    RUN_TEST(test_storage_64b_basic);
    RUN_TEST(test_storage_64b_sorted_order);
    RUN_TEST(test_storage_64b_commit_reopen);
    RUN_TEST(test_storage_64b_mixed_ops);
    RUN_TEST(test_storage_64b_large);
    RUN_TEST(test_storage_64b_seek);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
