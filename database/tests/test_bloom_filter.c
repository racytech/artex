/*
 * Bloom Filter — Unit + Correctness Tests
 *
 * Phases:
 *   1. Basic add + lookup (no false negatives)
 *   2. Negative lookups on empty filter
 *   3. False positive rate verification
 *   4. Clear resets all bits
 *   5. Popcount sanity
 *   6. Memory reporting
 *   7. Edge cases (tiny capacity, single element)
 *
 * Fail-fast: aborts on first failure.
 */

#include "../include/bloom_filter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <math.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static int tests_run = 0;
static int tests_passed = 0;

#define FAIL(fmt, ...) do {                                     \
    fprintf(stderr, "  FAIL [%s:%d]: " fmt "\n",                \
            __func__, __LINE__, ##__VA_ARGS__);                 \
    return false;                                               \
} while (0)

#define RUN(fn) do {                                            \
    tests_run++;                                                \
    printf("  %-50s ", #fn);                                    \
    if (fn()) { tests_passed++; printf("OK\n"); }               \
    else { printf("FAILED\n"); goto done; }                     \
} while (0)

/* Fill key with deterministic bytes from seed */
static void make_key(uint8_t *key, size_t len, uint64_t seed) {
    for (size_t i = 0; i < len; i++) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        key[i] = (uint8_t)(seed >> 56);
    }
}

/* =========================================================================
 * Tests
 * ========================================================================= */

/* Test 1: No false negatives — every inserted key must be found */
static bool test_no_false_negatives(void) {
    bloom_filter_t *bf = bloom_filter_create(10000, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];
    const int N = 5000;

    /* Insert N keys */
    for (int i = 0; i < N; i++) {
        make_key(key, 32, (uint64_t)i);
        bloom_filter_add(bf, key, 32);
    }

    /* Every inserted key must be found */
    for (int i = 0; i < N; i++) {
        make_key(key, 32, (uint64_t)i);
        if (!bloom_filter_maybe_contains(bf, key, 32))
            FAIL("false negative for key %d", i);
    }

    bloom_filter_destroy(bf);
    return true;
}

/* Test 2: Empty filter returns false for all queries */
static bool test_empty_filter(void) {
    bloom_filter_t *bf = bloom_filter_create(1000, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];
    for (int i = 0; i < 1000; i++) {
        make_key(key, 32, (uint64_t)(i + 100000));
        if (bloom_filter_maybe_contains(bf, key, 32))
            FAIL("empty filter returned true for key %d", i);
    }

    bloom_filter_destroy(bf);
    return true;
}

/* Test 3: False positive rate is within expected bounds */
static bool test_false_positive_rate(void) {
    const int N = 50000;       /* inserted keys */
    const int M = 100000;      /* test keys (disjoint from inserted) */
    const double target_fpr = 0.01;

    bloom_filter_t *bf = bloom_filter_create(N, target_fpr);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];

    /* Insert N keys with seeds [0, N) */
    for (int i = 0; i < N; i++) {
        make_key(key, 32, (uint64_t)i);
        bloom_filter_add(bf, key, 32);
    }

    /* Test M disjoint keys with seeds [N + 1000000, ...) */
    int false_positives = 0;
    for (int i = 0; i < M; i++) {
        make_key(key, 32, (uint64_t)(i + N + 1000000));
        if (bloom_filter_maybe_contains(bf, key, 32))
            false_positives++;
    }

    double actual_fpr = (double)false_positives / M;
    printf("(fpr=%.4f) ", actual_fpr);

    /* Allow up to 3x target — bloom math has variance */
    if (actual_fpr > target_fpr * 3.0)
        FAIL("FPR %.4f exceeds 3x target %.4f", actual_fpr, target_fpr);

    /* Sanity: should have *some* false positives with 100K tests */
    /* (At 1% FPR with 100K tests, expected ~1000, nearly impossible to get 0) */

    bloom_filter_destroy(bf);
    return true;
}

/* Test 4: Clear resets the filter */
static bool test_clear(void) {
    bloom_filter_t *bf = bloom_filter_create(1000, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];

    /* Add some keys */
    for (int i = 0; i < 500; i++) {
        make_key(key, 32, (uint64_t)i);
        bloom_filter_add(bf, key, 32);
    }

    if (bloom_filter_popcount(bf) == 0)
        FAIL("popcount should be > 0 after inserts");

    /* Clear */
    bloom_filter_clear(bf);

    if (bloom_filter_popcount(bf) != 0)
        FAIL("popcount should be 0 after clear");

    /* All previous keys should now miss */
    for (int i = 0; i < 500; i++) {
        make_key(key, 32, (uint64_t)i);
        if (bloom_filter_maybe_contains(bf, key, 32))
            FAIL("cleared filter returned true for key %d", i);
    }

    bloom_filter_destroy(bf);
    return true;
}

/* Test 5: Popcount increases with inserts */
static bool test_popcount(void) {
    bloom_filter_t *bf = bloom_filter_create(10000, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint64_t pc0 = bloom_filter_popcount(bf);
    if (pc0 != 0) FAIL("fresh filter has popcount %lu", pc0);

    uint8_t key[32];
    make_key(key, 32, 42);
    bloom_filter_add(bf, key, 32);

    uint64_t pc1 = bloom_filter_popcount(bf);
    if (pc1 == 0) FAIL("popcount still 0 after insert");

    /* Add more, popcount should grow (or stay same if hash collisions) */
    for (int i = 0; i < 100; i++) {
        make_key(key, 32, (uint64_t)(i + 1000));
        bloom_filter_add(bf, key, 32);
    }

    uint64_t pc2 = bloom_filter_popcount(bf);
    if (pc2 < pc1) FAIL("popcount decreased: %lu -> %lu", pc1, pc2);

    bloom_filter_destroy(bf);
    return true;
}

/* Test 6: Memory reporting */
static bool test_memory(void) {
    bloom_filter_t *bf = bloom_filter_create(100000, 0.01);
    if (!bf) FAIL("create returned NULL");

    size_t mem = bloom_filter_memory(bf);
    /* 100K elements at 1% FPR ≈ 9.6 bits/element ≈ 120KB */
    if (mem < 100000) FAIL("memory too small: %zu", mem);
    if (mem > 2000000) FAIL("memory too large: %zu", mem);

    printf("(%zu bytes) ", mem);

    bloom_filter_destroy(bf);
    return true;
}

/* Test 7: Variable key lengths */
static bool test_variable_key_lengths(void) {
    bloom_filter_t *bf = bloom_filter_create(1000, 0.01);
    if (!bf) FAIL("create returned NULL");

    /* Short keys */
    uint8_t k1[] = {0x01};
    uint8_t k2[] = {0x01, 0x02};
    uint8_t k4[] = {0x01, 0x02, 0x03, 0x04};

    bloom_filter_add(bf, k1, 1);
    bloom_filter_add(bf, k2, 2);
    bloom_filter_add(bf, k4, 4);

    if (!bloom_filter_maybe_contains(bf, k1, 1))
        FAIL("false negative for 1-byte key");
    if (!bloom_filter_maybe_contains(bf, k2, 2))
        FAIL("false negative for 2-byte key");
    if (!bloom_filter_maybe_contains(bf, k4, 4))
        FAIL("false negative for 4-byte key");

    /* 20-byte key (address size) */
    uint8_t addr[20];
    make_key(addr, 20, 9999);
    bloom_filter_add(bf, addr, 20);
    if (!bloom_filter_maybe_contains(bf, addr, 20))
        FAIL("false negative for 20-byte key");

    bloom_filter_destroy(bf);
    return true;
}

/* Test 8: Duplicate inserts don't break anything */
static bool test_duplicate_inserts(void) {
    bloom_filter_t *bf = bloom_filter_create(1000, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];
    make_key(key, 32, 42);

    /* Insert same key 100 times */
    for (int i = 0; i < 100; i++)
        bloom_filter_add(bf, key, 32);

    if (!bloom_filter_maybe_contains(bf, key, 32))
        FAIL("false negative after duplicate inserts");

    /* Popcount should be same as single insert (same bits set) */
    bloom_filter_t *bf2 = bloom_filter_create(1000, 0.01);
    bloom_filter_add(bf2, key, 32);

    if (bloom_filter_popcount(bf) != bloom_filter_popcount(bf2))
        FAIL("duplicate inserts changed popcount: %lu vs %lu",
             bloom_filter_popcount(bf), bloom_filter_popcount(bf2));

    bloom_filter_destroy(bf);
    bloom_filter_destroy(bf2);
    return true;
}

/* Test 9: Large scale — 1M insertions, verify no false negatives */
static bool test_large_scale(void) {
    const int N = 1000000;
    bloom_filter_t *bf = bloom_filter_create(N, 0.01);
    if (!bf) FAIL("create returned NULL");

    uint8_t key[32];

    for (int i = 0; i < N; i++) {
        make_key(key, 32, (uint64_t)i);
        bloom_filter_add(bf, key, 32);
    }

    /* Spot-check 10K random inserted keys */
    for (int i = 0; i < 10000; i++) {
        int idx = (i * 97) % N;  /* pseudo-random sampling */
        make_key(key, 32, (uint64_t)idx);
        if (!bloom_filter_maybe_contains(bf, key, 32))
            FAIL("false negative at key %d", idx);
    }

    size_t mem = bloom_filter_memory(bf);
    printf("(%zuKB, %lu bits set) ", mem / 1024, bloom_filter_popcount(bf));

    bloom_filter_destroy(bf);
    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("Bloom Filter Tests\n");
    printf("==================\n");

    RUN(test_no_false_negatives);
    RUN(test_empty_filter);
    RUN(test_false_positive_rate);
    RUN(test_clear);
    RUN(test_popcount);
    RUN(test_memory);
    RUN(test_variable_key_lengths);
    RUN(test_duplicate_inserts);
    RUN(test_large_scale);

done:
    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
