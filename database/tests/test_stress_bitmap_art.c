#include "../include/bitmap_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define TEST_PATH     "/tmp/test_stress_bart.dat"
#define KEY_SIZE      32
#define VALUE_SIZE    4

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
} while(0)

// ============================================================================
// Deterministic PRNG (xorshift64)
// ============================================================================

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

// ============================================================================
// Key generators
// ============================================================================

// Spread bits across 32 bytes (same as basic test)
static void key_sequential(uint8_t *key, uint32_t i) {
    memset(key, 0, KEY_SIZE);
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = i & 0xFF;
    key[16] = (i * 7) & 0xFF;
    key[17] = (i * 13) & 0xFF;
    key[31] = (i * 31) & 0xFF;
}

// Fully random 32-byte key
static void key_random(uint8_t *key) {
    for (int i = 0; i < KEY_SIZE; i += 8) {
        uint64_t r = rng_next();
        int n = KEY_SIZE - i < 8 ? KEY_SIZE - i : 8;
        memcpy(key + i, &r, n);
    }
}

// Dense prefix: first 24 bytes identical, last 8 vary
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

// Keys differ only at one byte position
static void key_single_byte_diff(uint8_t *key, uint32_t i, int pos) {
    memset(key, 0x55, KEY_SIZE);
    key[pos] = (uint8_t)i;
}

// ============================================================================
// 1. Sequential insert + verify + delete (large scale)
// ============================================================================

static int test_sequential_50k(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 50000;

    // Insert all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        ASSERT(bart_insert(&tree, key, &val), "insert i=%d", i);
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size=%zu expected=%d",
           bart_size(&tree), N);

    // Verify all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing key i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "value mismatch i=%d got=%u", i, val);
    }

    // Delete all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        ASSERT(bart_delete(&tree, key), "delete i=%d", i);
    }
    ASSERT(bart_size(&tree) == 0, "tree should be empty, size=%zu",
           bart_size(&tree));

    // Verify all gone
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        ASSERT(bart_get(&tree, key) == NULL, "key still present i=%d", i);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 2. Random keys — insert, verify, delete, verify
// ============================================================================

static int test_random_keys_20k(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 20000;
    rng_seed(0xDEADBEEF);

    // Generate and store keys for later verification
    uint8_t (*keys)[KEY_SIZE] = malloc(N * KEY_SIZE);
    ASSERT(keys != NULL, "malloc");

    for (int i = 0; i < N; i++) {
        key_random(keys[i]);
        uint32_t val = (uint32_t)i;
        ASSERT(bart_insert(&tree, keys[i], &val), "insert i=%d", i);
    }

    // Verify (note: duplicates possible with random keys — last write wins)
    int found = 0;
    for (int i = 0; i < N; i++) {
        if (bart_get(&tree, keys[i]) != NULL) found++;
    }
    ASSERT(found == (int)bart_size(&tree) || found >= (int)bart_size(&tree),
           "found=%d size=%zu", found, bart_size(&tree));

    // Delete all
    for (int i = 0; i < N; i++) {
        bart_delete(&tree, keys[i]);  // may fail for dups, that's ok
    }
    ASSERT(bart_size(&tree) == 0, "should be empty, size=%zu",
           bart_size(&tree));

    free(keys);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 3. Dense prefix keys — stresses long path compression
// ============================================================================

static int test_dense_prefix_10k(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 10000;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_dense_prefix(key, i);
        ASSERT(bart_insert(&tree, key, &val), "insert i=%d", i);
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size=%zu", bart_size(&tree));

    // Verify all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_dense_prefix(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing key i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "value mismatch i=%d", i);
    }

    // Delete odd keys
    for (int i = 1; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        key_dense_prefix(key, i);
        ASSERT(bart_delete(&tree, key), "delete i=%d", i);
    }
    ASSERT(bart_size(&tree) == (size_t)N / 2, "size after delete=%zu",
           bart_size(&tree));

    // Even keys still present
    for (int i = 0; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        key_dense_prefix(key, i);
        ASSERT(bart_get(&tree, key) != NULL, "even key missing i=%d", i);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 4. 256 children at one node (all byte values at one position)
// ============================================================================

static int test_full_fanout_256(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // 256 keys that differ only at byte[15] → one node with 256 children
    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_single_byte_diff(key, i, 15);
        ASSERT(bart_insert(&tree, key, &val), "insert byte=%d", i);
    }
    ASSERT(bart_size(&tree) == 256, "size=%zu", bart_size(&tree));

    // Verify all
    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        key_single_byte_diff(key, i, 15);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing byte=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "value mismatch byte=%d", i);
    }

    // Delete all in reverse
    for (int i = 255; i >= 0; i--) {
        uint8_t key[KEY_SIZE];
        key_single_byte_diff(key, i, 15);
        ASSERT(bart_delete(&tree, key), "delete byte=%d", i);
    }
    ASSERT(bart_size(&tree) == 0, "should be empty");

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 5. Multiple fanout positions (256 children at several depths)
// ============================================================================

static int test_multi_fanout(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Keys that fan out at positions 0, 10, and 20
    int count = 0;
    for (int a = 0; a < 16; a++) {
        for (int b = 0; b < 16; b++) {
            for (int c = 0; c < 16; c++) {
                uint8_t key[KEY_SIZE];
                memset(key, 0, KEY_SIZE);
                key[0] = (uint8_t)a;
                key[10] = (uint8_t)b;
                key[20] = (uint8_t)c;
                uint32_t val = (uint32_t)count;
                ASSERT(bart_insert(&tree, key, &val), "insert %d/%d/%d",
                       a, b, c);
                count++;
            }
        }
    }
    ASSERT(bart_size(&tree) == (size_t)count, "size=%zu expected=%d",
           bart_size(&tree), count);

    // Verify all
    count = 0;
    for (int a = 0; a < 16; a++) {
        for (int b = 0; b < 16; b++) {
            for (int c = 0; c < 16; c++) {
                uint8_t key[KEY_SIZE];
                memset(key, 0, KEY_SIZE);
                key[0] = (uint8_t)a;
                key[10] = (uint8_t)b;
                key[20] = (uint8_t)c;
                const void *got = bart_get(&tree, key);
                ASSERT(got != NULL, "missing %d/%d/%d", a, b, c);
                uint32_t val;
                memcpy(&val, got, 4);
                ASSERT(val == (uint32_t)count, "value mismatch %d/%d/%d",
                       a, b, c);
                count++;
            }
        }
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 6. Interleaved insert + delete (random ops)
// ============================================================================

static int test_random_ops_50k(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    rng_seed(42);
    const int N = 50000;
    const int KEY_SPACE = 20000;  // keys drawn from 0..KEY_SPACE-1

    // Track which keys are present
    uint8_t *present = calloc(KEY_SPACE, 1);
    ASSERT(present != NULL, "malloc");
    size_t expected_size = 0;

    for (int op = 0; op < N; op++) {
        uint32_t idx = rng_u32() % KEY_SPACE;
        uint8_t key[KEY_SIZE];
        key_sequential(key, idx);

        if (rng_u32() % 3 != 0) {
            // Insert (2/3 probability)
            uint32_t val = idx;
            bart_insert(&tree, key, &val);
            if (!present[idx]) {
                present[idx] = 1;
                expected_size++;
            }
        } else {
            // Delete (1/3 probability)
            bart_delete(&tree, key);
            if (present[idx]) {
                present[idx] = 0;
                expected_size--;
            }
        }
    }

    ASSERT(bart_size(&tree) == expected_size,
           "size=%zu expected=%zu", bart_size(&tree), expected_size);

    // Verify every key
    for (int i = 0; i < KEY_SPACE; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        if (present[i]) {
            ASSERT(got != NULL, "should be present i=%d", i);
            uint32_t val;
            memcpy(&val, got, 4);
            ASSERT(val == (uint32_t)i, "value mismatch i=%d got=%u", i, val);
        } else {
            ASSERT(got == NULL, "should be absent i=%d", i);
        }
    }

    free(present);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 7. Iterator count and order after random operations
// ============================================================================

static int test_iterator_order_after_ops(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    rng_seed(12345);
    const int N = 10000;

    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_random(key);
        uint32_t val = (uint32_t)i;
        bart_insert(&tree, key, &val);
    }

    // Delete ~30%
    rng_seed(12345);  // reset to get same keys
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_random(key);
        if (i % 3 == 0) bart_delete(&tree, key);
    }

    size_t expected = bart_size(&tree);

    // Iterate and check order
    bart_iterator_t *it = bart_iterator_create(&tree);
    ASSERT(it != NULL, "iterator create");

    int count = 0;
    uint8_t prev[KEY_SIZE];
    memset(prev, 0, KEY_SIZE);

    while (bart_iterator_next(it)) {
        const uint8_t *k = bart_iterator_key(it);
        ASSERT(k != NULL, "iterator key null at count=%d", count);
        if (count > 0) {
            ASSERT(memcmp(prev, k, KEY_SIZE) < 0,
                   "sort violation at count=%d", count);
        }
        memcpy(prev, k, KEY_SIZE);
        count++;
    }

    ASSERT((size_t)count == expected,
           "iterator count=%d expected=%zu", count, expected);

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 8. Commit + reopen + verify after many operations
// ============================================================================

static int test_commit_reopen_large(void) {
    const int N = 30000;

    // Phase 1: insert and commit
    {
        bitmap_art_t tree;
        ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            uint32_t val = (uint32_t)i;
            key_sequential(key, i);
            bart_insert(&tree, key, &val);
        }

        // Delete even keys
        for (int i = 0; i < N; i += 2) {
            uint8_t key[KEY_SIZE];
            key_sequential(key, i);
            bart_delete(&tree, key);
        }

        ASSERT(bart_commit(&tree), "commit");
        bart_close(&tree);
    }

    // Phase 2: reopen and verify
    {
        bitmap_art_t tree;
        ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
        ASSERT(bart_size(&tree) == (size_t)N / 2,
               "size=%zu expected=%d", bart_size(&tree), N / 2);

        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            key_sequential(key, i);
            const void *got = bart_get(&tree, key);
            if (i % 2 == 0) {
                ASSERT(got == NULL, "even key present i=%d", i);
            } else {
                ASSERT(got != NULL, "odd key missing i=%d", i);
                uint32_t val;
                memcpy(&val, got, 4);
                ASSERT(val == (uint32_t)i, "value mismatch i=%d", i);
            }
        }

        bart_close(&tree);
    }

    return 1;
}

// ============================================================================
// 9. Multiple commit/reopen cycles
// ============================================================================

static int test_multi_commit_cycles(void) {
    const int BATCH = 5000;
    const int CYCLES = 5;

    for (int cycle = 0; cycle < CYCLES; cycle++) {
        bitmap_art_t tree;
        ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open c=%d",
               cycle);

        size_t expected = bart_size(&tree);

        // Insert a batch
        int base = cycle * BATCH;
        for (int i = 0; i < BATCH; i++) {
            uint8_t key[KEY_SIZE];
            uint32_t val = (uint32_t)(base + i);
            key_sequential(key, base + i);
            bart_insert(&tree, key, &val);
        }
        expected += BATCH;

        ASSERT(bart_size(&tree) == expected,
               "size=%zu expected=%zu cycle=%d", bart_size(&tree), expected,
               cycle);
        ASSERT(bart_commit(&tree), "commit cycle=%d", cycle);
        bart_close(&tree);
    }

    // Final verify: all keys from all cycles
    {
        bitmap_art_t tree;
        ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "final open");
        ASSERT(bart_size(&tree) == (size_t)(CYCLES * BATCH),
               "final size=%zu expected=%d", bart_size(&tree),
               CYCLES * BATCH);

        for (int i = 0; i < CYCLES * BATCH; i++) {
            uint8_t key[KEY_SIZE];
            key_sequential(key, i);
            const void *got = bart_get(&tree, key);
            ASSERT(got != NULL, "missing i=%d", i);
            uint32_t val;
            memcpy(&val, got, 4);
            ASSERT(val == (uint32_t)i, "value mismatch i=%d", i);
        }

        bart_close(&tree);
    }

    return 1;
}

// ============================================================================
// 10. Rollback correctness under load
// ============================================================================

static int test_rollback_stress(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Commit 5000 keys
    for (int i = 0; i < 5000; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_commit(&tree), "commit");

    // Insert 5000 more, then rollback
    for (int i = 5000; i < 10000; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_size(&tree) == 10000, "size before rollback=%zu",
           bart_size(&tree));

    bart_rollback(&tree);
    ASSERT(bart_size(&tree) == 5000, "size after rollback=%zu",
           bart_size(&tree));

    // Verify: first 5000 present, next 5000 gone
    for (int i = 0; i < 10000; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        if (i < 5000) {
            ASSERT(bart_get(&tree, key) != NULL, "committed key missing i=%d",
                   i);
        } else {
            ASSERT(bart_get(&tree, key) == NULL,
                   "rolled-back key present i=%d", i);
        }
    }

    // Can still insert after rollback
    for (int i = 5000; i < 7000; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)(i + 100000);
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_size(&tree) == 7000, "size after re-insert=%zu",
           bart_size(&tree));

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 11. Update every key (in-place value replacement)
// ============================================================================

static int test_mass_update(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 10000;

    // Insert all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }

    // Update all with new values
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)(i + 1000000);
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_size(&tree) == (size_t)N,
           "size should be unchanged=%zu", bart_size(&tree));

    // Verify updated values
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)(i + 1000000),
               "update failed i=%d got=%u expected=%u", i, val, i + 1000000);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 12. Seek correctness with many keys
// ============================================================================

static int test_seek_stress(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 5000;

    // Insert even-numbered keys only
    for (int i = 0; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }

    bart_iterator_t *it = bart_iterator_create(&tree);
    ASSERT(it != NULL, "iterator create");

    // Seek to each odd key — should land on next even key
    for (int i = 1; i < N - 1; i += 2) {
        uint8_t seek_key[KEY_SIZE];
        key_sequential(seek_key, i);

        bool found = bart_iterator_seek(it, seek_key);
        ASSERT(found, "seek failed for i=%d", i);

        const uint8_t *got = bart_iterator_key(it);
        ASSERT(got != NULL, "seek key null i=%d", i);

        // The result should be >= seek_key
        ASSERT(memcmp(got, seek_key, KEY_SIZE) >= 0,
               "seek result < target at i=%d", i);
    }

    // Seek to exact keys
    for (int i = 0; i < N; i += 2) {
        uint8_t seek_key[KEY_SIZE];
        key_sequential(seek_key, i);

        bool found = bart_iterator_seek(it, seek_key);
        ASSERT(found, "exact seek failed for i=%d", i);

        const uint8_t *got = bart_iterator_key(it);
        ASSERT(memcmp(got, seek_key, KEY_SIZE) == 0,
               "exact seek mismatch at i=%d", i);
    }

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 13. Delete-all then re-insert (tests tree fully emptied)
// ============================================================================

static int test_delete_all_reinsert(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 10000;

    for (int round = 0; round < 3; round++) {
        // Insert N keys
        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            uint32_t val = (uint32_t)(round * 100000 + i);
            key_sequential(key, i);
            bart_insert(&tree, key, &val);
        }
        ASSERT(bart_size(&tree) == (size_t)N,
               "size after insert round=%d sz=%zu", round, bart_size(&tree));

        // Verify
        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            key_sequential(key, i);
            const void *got = bart_get(&tree, key);
            ASSERT(got != NULL, "missing round=%d i=%d", round, i);
            uint32_t val;
            memcpy(&val, got, 4);
            ASSERT(val == (uint32_t)(round * 100000 + i),
                   "value wrong round=%d i=%d", round, i);
        }

        // Delete all
        for (int i = 0; i < N; i++) {
            uint8_t key[KEY_SIZE];
            key_sequential(key, i);
            ASSERT(bart_delete(&tree, key), "delete round=%d i=%d", round, i);
        }
        ASSERT(bart_size(&tree) == 0, "not empty round=%d sz=%zu",
               round, bart_size(&tree));
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 14. Reverse-order insert and forward-order delete
// ============================================================================

static int test_reverse_insert_forward_delete(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 20000;

    // Insert in reverse
    for (int i = N - 1; i >= 0; i--) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        ASSERT(bart_insert(&tree, key, &val), "insert i=%d", i);
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size=%zu", bart_size(&tree));

    // Verify
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "value mismatch i=%d", i);
    }

    // Delete forward
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        ASSERT(bart_delete(&tree, key), "delete i=%d", i);
    }
    ASSERT(bart_size(&tree) == 0, "not empty");

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 15. Iterator + next after seek (seek then walk forward)
// ============================================================================

static int test_seek_then_walk(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }

    // Collect all keys via full iteration for reference
    uint8_t (*all_keys)[KEY_SIZE] = malloc(N * KEY_SIZE);
    ASSERT(all_keys != NULL, "malloc");

    bart_iterator_t *it = bart_iterator_create(&tree);
    int count = 0;
    while (bart_iterator_next(it)) {
        memcpy(all_keys[count], bart_iterator_key(it), KEY_SIZE);
        count++;
    }
    ASSERT(count == N, "full iter count=%d", count);

    // Seek to the middle key, then walk to the end
    int mid = N / 2;
    bool found = bart_iterator_seek(it, all_keys[mid]);
    ASSERT(found, "seek to mid");
    ASSERT(memcmp(bart_iterator_key(it), all_keys[mid], KEY_SIZE) == 0,
           "seek landed wrong");

    int walk_count = 1;  // already positioned on mid
    while (bart_iterator_next(it)) {
        int expected_idx = mid + walk_count;
        ASSERT(expected_idx < N, "walked past end");
        ASSERT(memcmp(bart_iterator_key(it), all_keys[expected_idx],
                       KEY_SIZE) == 0,
               "walk mismatch at offset=%d", walk_count);
        walk_count++;
    }
    ASSERT(walk_count == N - mid, "walk count=%d expected=%d",
           walk_count, N - mid);

    bart_iterator_destroy(it);
    free(all_keys);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 16. Keys differing at last byte only (max prefix compression)
// ============================================================================

static int test_last_byte_diff(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // 256 keys identical except last byte
    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_single_byte_diff(key, i, KEY_SIZE - 1);
        ASSERT(bart_insert(&tree, key, &val), "insert i=%d", i);
    }
    ASSERT(bart_size(&tree) == 256, "size=%zu", bart_size(&tree));

    // Verify + delete interleaved
    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        key_single_byte_diff(key, i, KEY_SIZE - 1);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "missing i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "value mismatch i=%d", i);
    }

    // Delete in random order
    rng_seed(999);
    uint8_t order[256];
    for (int i = 0; i < 256; i++) order[i] = (uint8_t)i;
    for (int i = 255; i > 0; i--) {
        int j = rng_u32() % (i + 1);
        uint8_t tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }

    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        key_single_byte_diff(key, order[i], KEY_SIZE - 1);
        ASSERT(bart_delete(&tree, key), "delete byte=%d", order[i]);

        // Remaining keys still findable
        for (int j = i + 1; j < 256; j++) {
            uint8_t kk[KEY_SIZE];
            key_single_byte_diff(kk, order[j], KEY_SIZE - 1);
            ASSERT(bart_get(&tree, kk) != NULL,
                   "missing after delete step=%d byte=%d", i, order[j]);
        }
    }
    ASSERT(bart_size(&tree) == 0, "not empty");

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 17. Keys differing at first byte only (no prefix compression)
// ============================================================================

static int test_first_byte_diff(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    for (int i = 0; i < 256; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_single_byte_diff(key, i, 0);
        ASSERT(bart_insert(&tree, key, &val), "insert i=%d", i);
    }
    ASSERT(bart_size(&tree) == 256, "size=%zu", bart_size(&tree));

    // Iterator should produce sorted order
    bart_iterator_t *it = bart_iterator_create(&tree);
    int count = 0;
    uint8_t prev = 0;
    while (bart_iterator_next(it)) {
        const uint8_t *k = bart_iterator_key(it);
        if (count > 0) {
            ASSERT(k[0] > prev, "sort violation at count=%d", count);
        }
        prev = k[0];
        count++;
    }
    ASSERT(count == 256, "iterator count=%d", count);

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// 18. Commit uncommitted mix — data integrity
// ============================================================================

static int test_commit_uncommitted_interleave(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Insert 0..999, commit
    for (int i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_commit(&tree), "commit 1");

    // Insert 1000..1999 (uncommitted), delete 0..499 (uncommitted)
    for (int i = 1000; i < 2000; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        bart_insert(&tree, key, &val);
    }
    for (int i = 0; i < 500; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        bart_delete(&tree, key);
    }
    ASSERT(bart_size(&tree) == 1500, "size=%zu", bart_size(&tree));

    // Close WITHOUT commit
    bart_close(&tree);

    // Reopen — should have only 0..999
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == 1000,
           "size after reopen=%zu expected=1000", bart_size(&tree));

    for (int i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        ASSERT(bart_get(&tree, key) != NULL, "committed key missing i=%d", i);
    }
    for (int i = 1000; i < 2000; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        ASSERT(bart_get(&tree, key) == NULL,
               "uncommitted key present i=%d", i);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 19. Large stress — 200K insert, verify, commit, reopen, verify
// ============================================================================

static int test_large_200k(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 200000;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = (uint32_t)i;
        key_sequential(key, i);
        if (!bart_insert(&tree, key, &val)) {
            FAIL("insert failed at i=%d", i);
        }
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size=%zu", bart_size(&tree));

    // Spot-check 1000 random keys
    rng_seed(777);
    for (int c = 0; c < 1000; c++) {
        int i = rng_u32() % N;
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "spot check missing i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        ASSERT(val == (uint32_t)i, "spot check value i=%d", i);
    }

    ASSERT(bart_commit(&tree), "commit");
    bart_close(&tree);

    // Reopen and full verify
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == (size_t)N, "reopen size=%zu", bart_size(&tree));

    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        key_sequential(key, i);
        const void *got = bart_get(&tree, key);
        if (!got) FAIL("missing after reopen i=%d", i);
        uint32_t val;
        memcpy(&val, got, 4);
        if (val != (uint32_t)i) FAIL("value after reopen i=%d got=%u", i, val);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// 20. Iterator count always matches size
// ============================================================================

static int test_iterator_size_consistency(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    rng_seed(0xCAFE);

    // Run 20 rounds of random ops, check iterator count == size each time
    for (int round = 0; round < 20; round++) {
        int ops = 500;
        for (int op = 0; op < ops; op++) {
            uint32_t idx = rng_u32() % 2000;
            uint8_t key[KEY_SIZE];
            key_sequential(key, idx);
            if (rng_u32() % 2) {
                uint32_t val = idx;
                bart_insert(&tree, key, &val);
            } else {
                bart_delete(&tree, key);
            }
        }

        size_t expected = bart_size(&tree);
        bart_iterator_t *it = bart_iterator_create(&tree);
        int count = 0;
        while (bart_iterator_next(it)) count++;
        bart_iterator_destroy(it);

        ASSERT((size_t)count == expected,
               "round=%d iter=%d size=%zu", round, count, expected);
    }

    bart_close(&tree);
    return 1;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== Bitmap ART Stress Tests ===\n\n");

    RUN_TEST(test_sequential_50k);
    RUN_TEST(test_random_keys_20k);
    RUN_TEST(test_dense_prefix_10k);
    RUN_TEST(test_full_fanout_256);
    RUN_TEST(test_multi_fanout);
    RUN_TEST(test_random_ops_50k);
    RUN_TEST(test_iterator_order_after_ops);
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

    printf("\n%d/%d tests passed", tests_passed, tests_run);
    if (tests_failed > 0) printf(" (%d FAILED)", tests_failed);
    printf("\n");
    return (tests_passed == tests_run) ? 0 : 1;
}
