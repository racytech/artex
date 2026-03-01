/*
 * Cross-validation: nt_root_hash (tree walk) vs ih_build (sorted-key scan)
 *
 * Both must produce identical Ethereum MPT root hashes for the same data.
 * Tests at multiple scales: 1, 10, 100, 1K, 10K, 100K, 1M keys.
 */

#include "../include/nt_hash.h"
#include "../include/nibble_trie.h"
#include "intermediate_hashes.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

/* ========================================================================
 * Test framework
 * ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("  FAIL [line %d]: " fmt "\n", __LINE__, ##__VA_ARGS__); \
        return 0; \
    } \
} while(0)

#define RUN_TEST(fn) do { \
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
} while(0)

/* ========================================================================
 * RNG (SplitMix64)
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

#define SEED 0x4E54484153480000ULL

/* ========================================================================
 * Key/Value generation
 * ======================================================================== */

static void generate_key(uint8_t key[32], uint64_t index) {
    rng_t rng = rng_create(SEED ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static void generate_value(uint8_t val[32], uint64_t index) {
    rng_t rng = rng_create(SEED ^ (index * 0x9ABCDEF012345678ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(val + i, &r, 8);
    }
}

/* ========================================================================
 * Helpers
 * ======================================================================== */

static void print_hash(const char *label, const hash_t *h) {
    printf("    %s: 0x", label);
    for (int i = 0; i < 4; i++) printf("%02x", h->bytes[i]);
    printf("..");
    for (int i = 28; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1e3 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

/* Sort helper for ih_build (sorts key pointers by key content) */
typedef struct {
    const uint8_t *key;
    const uint8_t *val;
} kv_ptr_t;

static int kv_cmp(const void *a, const void *b) {
    return memcmp(((kv_ptr_t *)a)->key, ((kv_ptr_t *)b)->key, 32);
}

/**
 * Compute ih_build root for a set of key-value pairs.
 * Keys are sorted internally.
 */
static hash_t compute_ih_root(const uint8_t *keys, const uint8_t *values,
                               size_t count) {
    if (count == 0) return HASH_EMPTY_STORAGE;

    /* Build sorted pointer arrays */
    kv_ptr_t *sorted = malloc(count * sizeof(kv_ptr_t));
    for (size_t i = 0; i < count; i++) {
        sorted[i].key = keys + i * 32;
        sorted[i].val = values + i * 32;
    }
    qsort(sorted, count, sizeof(kv_ptr_t), kv_cmp);

    const uint8_t **key_ptrs = malloc(count * sizeof(uint8_t *));
    const uint8_t **val_ptrs = malloc(count * sizeof(uint8_t *));
    uint16_t *vlens = malloc(count * sizeof(uint16_t));

    for (size_t i = 0; i < count; i++) {
        key_ptrs[i] = sorted[i].key;
        val_ptrs[i] = sorted[i].val;
        vlens[i] = 32;
    }

    ih_state_t *ih = ih_create();
    hash_t root = ih_build(ih, key_ptrs, val_ptrs, vlens, count);
    ih_destroy(ih);

    free(sorted);
    free(key_ptrs);
    free(val_ptrs);
    free(vlens);

    return root;
}

/* ========================================================================
 * Cross-validation at a given scale
 * ======================================================================== */

static int cross_validate(size_t n) {
    /* Generate keys and values */
    uint8_t *keys = malloc(n * 32);
    uint8_t *values = malloc(n * 32);
    ASSERT(keys && values, "malloc");

    for (size_t i = 0; i < n; i++) {
        generate_key(keys + i * 32, i);
        generate_value(values + i * 32, i);
    }

    /* Insert into nibble_trie */
    nibble_trie_t t;
    ASSERT(nt_init(&t, 32, 32), "nt_init");

    for (size_t i = 0; i < n; i++)
        ASSERT(nt_insert(&t, keys + i * 32, values + i * 32), "insert i=%zu", i);
    ASSERT(nt_size(&t) == n, "size=%zu expected=%zu", nt_size(&t), n);

    /* Compute root via tree walk */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hash_t nt_root = nt_root_hash(&t);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double nt_ms = elapsed_ms(&t0, &t1);

    /* Compute root via ih_build */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hash_t ih_root = compute_ih_root(keys, values, n);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ih_ms = elapsed_ms(&t0, &t1);

    /* Compare */
    bool match = hash_equal(&nt_root, &ih_root);
    if (!match) {
        print_hash("nt_root", &nt_root);
        print_hash("ih_root", &ih_root);
    }

    printf(" [nt=%.1fms ih=%.1fms] ", nt_ms, ih_ms);

    nt_destroy(&t);
    free(keys);
    free(values);

    ASSERT(match, "roots differ at n=%zu", n);
    return 1;
}

/* ========================================================================
 * Test: empty trie
 * ======================================================================== */

static int test_empty(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 32, 32), "init");
    hash_t root = nt_root_hash(&t);
    ASSERT(hash_equal(&root, &HASH_EMPTY_STORAGE), "empty root");
    nt_destroy(&t);
    return 1;
}

/* ========================================================================
 * Test: single key
 * ======================================================================== */

static int test_single_key(void) {
    return cross_validate(1);
}

/* ========================================================================
 * Scale tests
 * ======================================================================== */

static int test_10(void) { return cross_validate(10); }
static int test_100(void) { return cross_validate(100); }
static int test_1k(void) { return cross_validate(1000); }
static int test_10k(void) { return cross_validate(10000); }
static int test_100k(void) { return cross_validate(100000); }
static int test_1m(void) { return cross_validate(1000000); }

/* ========================================================================
 * Test: delete subset, verify remaining root
 * ======================================================================== */

static int test_after_deletes(void) {
    const size_t N = 10000;
    const size_t DEL = 3000;

    uint8_t *keys = malloc(N * 32);
    uint8_t *values = malloc(N * 32);
    ASSERT(keys && values, "malloc");

    for (size_t i = 0; i < N; i++) {
        generate_key(keys + i * 32, i);
        generate_value(values + i * 32, i);
    }

    /* Insert all into nibble_trie */
    nibble_trie_t t;
    ASSERT(nt_init(&t, 32, 32), "nt_init");

    for (size_t i = 0; i < N; i++)
        nt_insert(&t, keys + i * 32, values + i * 32);

    /* Delete first DEL keys */
    for (size_t i = 0; i < DEL; i++)
        nt_delete(&t, keys + i * 32);

    size_t remaining = N - DEL;
    ASSERT(nt_size(&t) == remaining, "size=%zu expected=%zu",
           nt_size(&t), remaining);

    /* nt_root_hash on trie with deletions */
    hash_t nt_root = nt_root_hash(&t);

    /* ih_build on remaining keys only */
    hash_t ih_root = compute_ih_root(keys + DEL * 32, values + DEL * 32,
                                      remaining);

    bool match = hash_equal(&nt_root, &ih_root);
    if (!match) {
        print_hash("nt_root", &nt_root);
        print_hash("ih_root", &ih_root);
    }

    nt_destroy(&t);
    free(keys);
    free(values);

    ASSERT(match, "roots differ after deletes");
    return 1;
}

/* ========================================================================
 * Test: sequential keys (stress extension paths)
 * ======================================================================== */

static int test_sequential_keys(void) {
    const size_t N = 5000;

    uint8_t *keys = malloc(N * 32);
    uint8_t *values = malloc(N * 32);

    for (size_t i = 0; i < N; i++) {
        memset(keys + i * 32, 0, 32);
        keys[i * 32 + 0] = (i >> 24) & 0xFF;
        keys[i * 32 + 1] = (i >> 16) & 0xFF;
        keys[i * 32 + 2] = (i >> 8) & 0xFF;
        keys[i * 32 + 3] = i & 0xFF;

        memset(values + i * 32, 0, 32);
        uint32_t v = (uint32_t)(i * 1000 + 42);
        memcpy(values + i * 32, &v, 4);
    }

    /* nibble_trie */
    nibble_trie_t t;
    ASSERT(nt_init(&t, 32, 32), "nt_init");
    for (size_t i = 0; i < N; i++)
        nt_insert(&t, keys + i * 32, values + i * 32);

    hash_t nt_root = nt_root_hash(&t);

    /* ih_build */
    hash_t ih_root = compute_ih_root(keys, values, N);

    bool match = hash_equal(&nt_root, &ih_root);
    if (!match) {
        print_hash("nt_root", &nt_root);
        print_hash("ih_root", &ih_root);
    }

    nt_destroy(&t);
    free(keys);
    free(values);

    ASSERT(match, "roots differ for sequential keys");
    return 1;
}

/* ========================================================================
 * main
 * ======================================================================== */

int main(void) {
    printf("=== nibble_trie Root Hash Cross-Validation ===\n\n");

    RUN_TEST(test_empty);
    RUN_TEST(test_single_key);
    RUN_TEST(test_10);
    RUN_TEST(test_100);
    RUN_TEST(test_1k);
    RUN_TEST(test_10k);
    RUN_TEST(test_sequential_keys);
    RUN_TEST(test_after_deletes);
    RUN_TEST(test_100k);
    RUN_TEST(test_1m);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
