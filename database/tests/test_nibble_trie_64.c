/*
 * Quick correctness test for nibble_trie with 64-byte keys.
 * Verifies insert, get, update, delete, iterator, and seek.
 */

#include "../include/nibble_trie.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

static int pass = 0, fail = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL [%s:%d]: %s\n", __FILE__, __LINE__, msg); \
        fail++; return; \
    } \
} while(0)

#define PASS(name) do { pass++; printf("  %-50s PASS\n", name); } while(0)

/* SplitMix64 */
typedef struct { uint64_t state; } rng_t;
static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void make_key64(uint8_t key[64], uint64_t seed, uint64_t idx) {
    rng_t rng = { .state = seed ^ (idx * 0x517cc1b727220a95ULL) };
    rng_next(&rng);
    for (int i = 0; i < 64; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static void make_val(uint8_t val[32], uint64_t seed, uint64_t idx) {
    rng_t rng = { .state = seed ^ (idx * 0x9ABCDEF012345678ULL) };
    rng_next(&rng);
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(val + i, &r, 8);
    }
}

#define SEED 0xDEADBEEF64646464ULL

/* ======================================================================== */

static void test_basic_insert_get(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    uint8_t key[64], val[32];
    make_key64(key, SEED, 0);
    make_val(val, SEED, 0);

    ASSERT(nt_insert(&t, key, val), "insert");
    ASSERT(nt_size(&t) == 1, "size=1");

    const void *got = nt_get(&t, key);
    ASSERT(got != NULL, "get not null");
    ASSERT(memcmp(got, val, 32) == 0, "value matches");

    /* Non-existent key */
    uint8_t bad[64];
    memset(bad, 0xFF, 64);
    ASSERT(nt_get(&t, bad) == NULL, "miss returns null");

    nt_destroy(&t);
    PASS("test_basic_insert_get");
}

static void test_multiple_keys(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    const int N = 1000;
    uint8_t key[64], val[32];

    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        ASSERT(nt_insert(&t, key, val), "insert");
    }
    ASSERT(nt_size(&t) == (size_t)N, "size");

    /* Verify all keys present with correct values */
    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        const void *got = nt_get(&t, key);
        ASSERT(got != NULL, "get");
        ASSERT(memcmp(got, val, 32) == 0, "value match");
    }

    nt_destroy(&t);
    PASS("test_multiple_keys (1K)");
}

static void test_update(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    uint8_t key[64], val[32], val2[32];
    make_key64(key, SEED, 42);
    make_val(val, SEED, 42);
    memset(val2, 0xAA, 32);

    ASSERT(nt_insert(&t, key, val), "insert");
    ASSERT(nt_size(&t) == 1, "size=1 after insert");

    /* Update same key with new value */
    ASSERT(nt_insert(&t, key, val2), "update");
    ASSERT(nt_size(&t) == 1, "size=1 after update");

    const void *got = nt_get(&t, key);
    ASSERT(got != NULL, "get");
    ASSERT(memcmp(got, val2, 32) == 0, "updated value");

    nt_destroy(&t);
    PASS("test_update");
}

static void test_delete(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    const int N = 500;
    uint8_t key[64], val[32];

    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after inserts");

    /* Delete even keys */
    for (int i = 0; i < N; i += 2) {
        make_key64(key, SEED, i);
        ASSERT(nt_delete(&t, key), "delete");
    }
    ASSERT(nt_size(&t) == (size_t)(N / 2), "size after deletes");

    /* Verify even keys gone, odd keys present */
    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        const void *got = nt_get(&t, key);
        if (i % 2 == 0) {
            ASSERT(got == NULL, "deleted key gone");
        } else {
            ASSERT(got != NULL, "remaining key present");
            ASSERT(memcmp(got, val, 32) == 0, "remaining value correct");
        }
    }

    nt_destroy(&t);
    PASS("test_delete");
}

static void test_iterator(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    const int N = 2000;
    uint8_t key[64], val[32];

    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }

    /* Iterate and count */
    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "iterator create");

    uint64_t count = 0;
    const uint8_t *prev_key = NULL;
    uint8_t prev_buf[64];

    while (nt_iterator_next(it)) {
        const uint8_t *k = nt_iterator_key(it);
        ASSERT(k != NULL, "iterator key not null");

        /* Verify sorted order */
        if (prev_key != NULL) {
            ASSERT(memcmp(prev_key, k, 64) < 0, "sorted order");
        }
        memcpy(prev_buf, k, 64);
        prev_key = prev_buf;
        count++;
    }

    ASSERT(count == (uint64_t)N, "iterator count matches");
    nt_iterator_destroy(it);

    nt_destroy(&t);
    PASS("test_iterator (2K, sorted order)");
}

static void test_seek(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    const int N = 1000;
    uint8_t key[64], val[32];

    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }

    /* Seek to a known key */
    make_key64(key, SEED, 500);
    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "iterator create");
    ASSERT(nt_iterator_seek(it, key), "seek found");

    const uint8_t *found = nt_iterator_key(it);
    ASSERT(found != NULL, "seek key not null");
    ASSERT(memcmp(found, key, 64) >= 0, "seek key >= target");

    /* Seek to min key (all zeros) — should find first */
    uint8_t min_key[64];
    memset(min_key, 0, 64);
    nt_iterator_t *it2 = nt_iterator_create(&t);
    ASSERT(nt_iterator_seek(it2, min_key), "seek min");
    ASSERT(nt_iterator_key(it2) != NULL, "seek min key");

    nt_iterator_destroy(it);
    nt_iterator_destroy(it2);
    nt_destroy(&t);
    PASS("test_seek");
}

static void test_stress_50k(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    const int N = 50000;
    uint8_t key[64], val[32];

    /* Insert all */
    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after insert");

    /* Verify all */
    for (int i = 0; i < N; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        const void *got = nt_get(&t, key);
        ASSERT(got != NULL, "get");
        ASSERT(memcmp(got, val, 32) == 0, "value");
    }

    /* Delete half, re-verify */
    for (int i = 0; i < N; i += 2) {
        make_key64(key, SEED, i);
        nt_delete(&t, key);
    }
    ASSERT(nt_size(&t) == (size_t)(N / 2), "size after delete");

    for (int i = 1; i < N; i += 2) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        const void *got = nt_get(&t, key);
        ASSERT(got != NULL, "remaining get");
        ASSERT(memcmp(got, val, 32) == 0, "remaining value");
    }

    /* Re-insert deleted keys with new values */
    uint8_t new_val[32];
    memset(new_val, 0xBB, 32);
    for (int i = 0; i < N; i += 2) {
        make_key64(key, SEED, i);
        nt_insert(&t, key, new_val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "size after reinsert");

    /* Iterator count */
    nt_iterator_t *it = nt_iterator_create(&t);
    uint64_t count = 0;
    while (nt_iterator_next(it)) count++;
    nt_iterator_destroy(it);
    ASSERT(count == (uint64_t)N, "iterator count");

    nt_destroy(&t);
    PASS("test_stress_50k");
}

static void test_clear_reuse(void) {
    nibble_trie_t t;
    ASSERT(nt_init(&t, 64, 32), "init");

    uint8_t key[64], val[32];
    for (int i = 0; i < 100; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == 100, "size=100");

    nt_clear(&t);
    ASSERT(nt_size(&t) == 0, "size=0 after clear");

    /* Reinsert different keys */
    for (int i = 1000; i < 1100; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == 100, "size=100 after reinsert");

    for (int i = 1000; i < 1100; i++) {
        make_key64(key, SEED, i);
        make_val(val, SEED, i);
        const void *got = nt_get(&t, key);
        ASSERT(got != NULL, "get after clear+reinsert");
        ASSERT(memcmp(got, val, 32) == 0, "value after clear+reinsert");
    }

    nt_destroy(&t);
    PASS("test_clear_reuse");
}

int main(void) {
    printf("=== nibble_trie 64B Key Correctness Tests ===\n\n");

    test_basic_insert_get();
    test_multiple_keys();
    test_update();
    test_delete();
    test_iterator();
    test_seek();
    test_stress_50k();
    test_clear_reuse();

    printf("\n%d/%d tests passed\n", pass, pass + fail);
    return fail > 0 ? 1 : 0;
}
