/*
 * Disk Hash — Unit + Correctness Tests
 *
 * Phases:
 *   1. Basic CRUD (single operations)
 *   2. Batch put + batch get
 *   3. Tombstone reuse (delete + re-insert)
 *   4. Persistence (sync → destroy → open → verify)
 *   5. Overflow buckets (tiny capacity → forced overflow)
 *   6. Empty table edge cases
 *   7. Scale correctness (100K keys, mixed operations)
 *
 * Fail-fast: aborts on first failure.
 */

#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <unistd.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

#define DATA_PATH "/tmp/test_disk_hash.dat"
#define KEY_SIZE  32
#define REC_SIZE  32

static int tests_run = 0;
static int tests_passed = 0;

#define FAIL(fmt, ...) do {                                     \
    fprintf(stderr, "  FAIL [%s:%d]: " fmt "\n",                \
            __func__, __LINE__, ##__VA_ARGS__);                 \
    return false;                                               \
} while (0)

#define CHECK(cond, fmt, ...) do {                              \
    if (!(cond)) FAIL(fmt, ##__VA_ARGS__);                      \
} while (0)

#define RUN_TEST(fn) do {                                       \
    tests_run++;                                                \
    printf("  [%2d] %-50s", tests_run, #fn);                    \
    fflush(stdout);                                             \
    unlink(DATA_PATH);                                          \
    if (fn()) { tests_passed++; printf("OK\n"); }               \
    else { printf("FAILED\n"); }                                \
    unlink(DATA_PATH);                                          \
} while (0)

/* splitmix64 PRNG */
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

static void make_key(uint8_t key[KEY_SIZE], uint64_t index) {
    rng_t rng = rng_create(index * 0x517cc1b727220a95ULL);
    for (int i = 0; i < KEY_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        int n = KEY_SIZE - i;
        if (n > 8) n = 8;
        memcpy(key + i, &r, (size_t)n);
    }
}

static void make_record(uint8_t rec[REC_SIZE], uint64_t index) {
    rng_t rng = rng_create(index * 0xA2F5C3D7E1B94068ULL);
    for (int i = 0; i < REC_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        int n = REC_SIZE - i;
        if (n > 8) n = 8;
        memcpy(rec + i, &r, (size_t)n);
    }
}

/* =========================================================================
 * Phase 1: Basic CRUD
 * ========================================================================= */

static bool test_create_destroy(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 100);
    CHECK(dh != NULL, "create failed");
    CHECK(disk_hash_count(dh) == 0, "count should be 0");
    CHECK(disk_hash_capacity(dh) > 0, "capacity should be > 0");
    disk_hash_destroy(dh);
    return true;
}

static bool test_put_get_single(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 100);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];
    make_key(key, 42);
    make_record(rec, 42);

    CHECK(disk_hash_put(dh, key, rec), "put failed");
    CHECK(disk_hash_count(dh) == 1, "count should be 1");
    CHECK(disk_hash_contains(dh, key), "contains should be true");

    CHECK(disk_hash_get(dh, key, got), "get failed");
    CHECK(memcmp(got, rec, REC_SIZE) == 0, "record mismatch");

    disk_hash_destroy(dh);
    return true;
}

static bool test_update(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 100);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec1[REC_SIZE], rec2[REC_SIZE], got[REC_SIZE];
    make_key(key, 1);
    make_record(rec1, 1);
    make_record(rec2, 999);

    CHECK(disk_hash_put(dh, key, rec1), "put1 failed");
    CHECK(disk_hash_put(dh, key, rec2), "put2 (update) failed");
    CHECK(disk_hash_count(dh) == 1, "count should still be 1");

    CHECK(disk_hash_get(dh, key, got), "get failed");
    CHECK(memcmp(got, rec2, REC_SIZE) == 0, "should have updated record");

    disk_hash_destroy(dh);
    return true;
}

static bool test_delete(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 100);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];
    make_key(key, 7);
    make_record(rec, 7);

    CHECK(disk_hash_put(dh, key, rec), "put failed");
    CHECK(disk_hash_delete(dh, key), "delete failed");
    CHECK(disk_hash_count(dh) == 0, "count should be 0 after delete");
    CHECK(!disk_hash_contains(dh, key), "should not contain after delete");
    CHECK(!disk_hash_get(dh, key, got), "get should fail after delete");

    /* Delete non-existent key */
    CHECK(!disk_hash_delete(dh, key), "double delete should fail");

    disk_hash_destroy(dh);
    return true;
}

static bool test_multi_keys(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 1000);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];

    /* Insert 500 keys */
    for (uint64_t i = 0; i < 500; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_put(dh, key, rec), "put %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == 500, "count should be 500");

    /* Verify all 500 */
    for (uint64_t i = 0; i < 500; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got), "get %" PRIu64 " failed", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "record mismatch at %" PRIu64, i);
    }

    /* Verify non-existent key */
    make_key(key, 999999);
    CHECK(!disk_hash_get(dh, key, got), "should not find non-existent");

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 2: Batch Operations
 * ========================================================================= */

static bool test_batch_put_get(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 20000);
    CHECK(dh != NULL, "create failed");

    uint32_t N = 10000;
    uint8_t *keys = malloc((uint64_t)N * KEY_SIZE);
    uint8_t *recs = malloc((uint64_t)N * REC_SIZE);
    uint8_t *got  = malloc((uint64_t)N * REC_SIZE);
    bool    *found = malloc(N * sizeof(bool));
    CHECK(keys && recs && got && found, "alloc failed");

    /* Prepare batch */
    for (uint32_t i = 0; i < N; i++) {
        make_key(keys + (uint64_t)i * KEY_SIZE, i);
        make_record(recs + (uint64_t)i * REC_SIZE, i);
    }

    /* Batch put */
    CHECK(disk_hash_batch_put(dh, keys, recs, N), "batch_put failed");
    CHECK(disk_hash_count(dh) == N, "count should be %u, got %" PRIu64,
          N, disk_hash_count(dh));

    /* Batch get */
    memset(got, 0, (uint64_t)N * REC_SIZE);
    uint32_t found_count = disk_hash_batch_get(dh, keys, got, found, N);
    CHECK(found_count == N, "batch_get found %u / %u", found_count, N);

    for (uint32_t i = 0; i < N; i++) {
        CHECK(found[i], "key %u not found", i);
        uint8_t expected[REC_SIZE];
        make_record(expected, i);
        CHECK(memcmp(got + (uint64_t)i * REC_SIZE, expected, REC_SIZE) == 0,
              "record mismatch at %u", i);
    }

    free(keys); free(recs); free(got); free(found);
    disk_hash_destroy(dh);
    return true;
}

static bool test_batch_get_mixed(void) {
    /* Batch get with some existing and some non-existing keys */
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 1000);
    CHECK(dh != NULL, "create failed");

    /* Insert even-indexed keys only */
    uint8_t key[KEY_SIZE], rec[REC_SIZE];
    for (uint64_t i = 0; i < 200; i += 2) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_put(dh, key, rec), "put %" PRIu64 " failed", i);
    }

    /* Batch get all 200 (half exist, half don't) */
    uint32_t N = 200;
    uint8_t *keys = malloc((uint64_t)N * KEY_SIZE);
    uint8_t *recs_out = malloc((uint64_t)N * REC_SIZE);
    bool *found = malloc(N * sizeof(bool));

    for (uint32_t i = 0; i < N; i++)
        make_key(keys + (uint64_t)i * KEY_SIZE, i);

    uint32_t fc = disk_hash_batch_get(dh, keys, recs_out, found, N);
    CHECK(fc == 100, "expected 100 found, got %u", fc);

    for (uint32_t i = 0; i < N; i++) {
        if (i % 2 == 0) {
            CHECK(found[i], "even key %u should be found", i);
            uint8_t expected[REC_SIZE];
            make_record(expected, i);
            CHECK(memcmp(recs_out + (uint64_t)i * REC_SIZE,
                         expected, REC_SIZE) == 0,
                  "record mismatch at %u", i);
        } else {
            CHECK(!found[i], "odd key %u should not be found", i);
        }
    }

    free(keys); free(recs_out); free(found);
    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 3: Tombstone Reuse
 * ========================================================================= */

static bool test_tombstone_reuse(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 1000);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];

    /* Insert 200 keys */
    for (uint64_t i = 0; i < 200; i++) {
        make_key(key, i);
        make_record(rec, i);
        disk_hash_put(dh, key, rec);
    }
    CHECK(disk_hash_count(dh) == 200, "count should be 200");

    /* Delete first 100 */
    for (uint64_t i = 0; i < 100; i++) {
        make_key(key, i);
        CHECK(disk_hash_delete(dh, key), "delete %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == 100, "count should be 100 after deletes");

    /* Insert 100 new keys (should reuse tombstone slots) */
    for (uint64_t i = 1000; i < 1100; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_put(dh, key, rec), "put %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == 200, "count should be 200 again");

    /* Verify deleted keys are gone */
    for (uint64_t i = 0; i < 100; i++) {
        make_key(key, i);
        CHECK(!disk_hash_get(dh, key, got),
              "deleted key %" PRIu64 " should be gone", i);
    }

    /* Verify surviving original keys */
    for (uint64_t i = 100; i < 200; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got),
              "surviving key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "surviving record %" PRIu64 " mismatch", i);
    }

    /* Verify new keys */
    for (uint64_t i = 1000; i < 1100; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got),
              "new key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "new record %" PRIu64 " mismatch", i);
    }

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 4: Persistence
 * ========================================================================= */

static bool test_persistence(void) {
    uint64_t N = 1000;

    /* Create and populate */
    {
        disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 2000);
        CHECK(dh != NULL, "create failed");

        uint8_t key[KEY_SIZE], rec[REC_SIZE];
        for (uint64_t i = 0; i < N; i++) {
            make_key(key, i);
            make_record(rec, i);
            CHECK(disk_hash_put(dh, key, rec), "put failed at %" PRIu64, i);
        }
        disk_hash_sync(dh);
        disk_hash_destroy(dh);
    }

    /* Reopen and verify */
    {
        disk_hash_t *dh = disk_hash_open(DATA_PATH);
        CHECK(dh != NULL, "open failed");
        CHECK(disk_hash_count(dh) == N,
              "count mismatch: expected %" PRIu64 " got %" PRIu64,
              N, disk_hash_count(dh));

        uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];
        for (uint64_t i = 0; i < N; i++) {
            make_key(key, i);
            make_record(rec, i);
            CHECK(disk_hash_get(dh, key, got),
                  "key %" PRIu64 " not found after reopen", i);
            CHECK(memcmp(got, rec, REC_SIZE) == 0,
                  "record %" PRIu64 " mismatch after reopen", i);
        }

        disk_hash_destroy(dh);
    }

    return true;
}

/* =========================================================================
 * Phase 5: Overflow Buckets
 * ========================================================================= */

static bool test_overflow(void) {
    /* Create with capacity_hint=1 → 1 bucket (62 slots).
     * Insert 100 keys, forcing at least 1 overflow. */
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 1);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];

    for (uint64_t i = 0; i < 100; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_put(dh, key, rec), "put %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == 100, "count should be 100");

    /* Verify all */
    for (uint64_t i = 0; i < 100; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got),
              "key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "record %" PRIu64 " mismatch", i);
    }

    /* Delete some, verify remaining */
    for (uint64_t i = 0; i < 50; i++) {
        make_key(key, i);
        CHECK(disk_hash_delete(dh, key),
              "delete %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == 50, "count should be 50");

    for (uint64_t i = 50; i < 100; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got),
              "surviving key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "surviving record %" PRIu64 " mismatch", i);
    }

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 6: Empty Table Edge Cases
 * ========================================================================= */

static bool test_empty_table(void) {
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE, 100);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], got[REC_SIZE];
    make_key(key, 0);

    CHECK(!disk_hash_get(dh, key, got), "get on empty should fail");
    CHECK(!disk_hash_delete(dh, key), "delete on empty should fail");
    CHECK(!disk_hash_contains(dh, key), "contains on empty should be false");
    CHECK(disk_hash_count(dh) == 0, "count should be 0");

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Phase 7: Scale Correctness
 * ========================================================================= */

static bool test_scale_correctness(void) {
    uint64_t N = 100000;
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, REC_SIZE,
                                        N * 2);
    CHECK(dh != NULL, "create failed");

    uint8_t key[KEY_SIZE], rec[REC_SIZE], got[REC_SIZE];

    /* Insert N keys */
    for (uint64_t i = 0; i < N; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_put(dh, key, rec), "put %" PRIu64 " failed", i);
    }
    CHECK(disk_hash_count(dh) == N,
          "count should be %" PRIu64 ", got %" PRIu64,
          N, disk_hash_count(dh));

    /* Verify all */
    for (uint64_t i = 0; i < N; i++) {
        make_key(key, i);
        make_record(rec, i);
        CHECK(disk_hash_get(dh, key, got),
              "key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "record %" PRIu64 " mismatch", i);
    }

    /* Delete every other key */
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < N; i += 2) {
        make_key(key, i);
        CHECK(disk_hash_delete(dh, key),
              "delete %" PRIu64 " failed", i);
        deleted++;
    }
    CHECK(disk_hash_count(dh) == N - deleted,
          "count after delete: expected %" PRIu64 " got %" PRIu64,
          N - deleted, disk_hash_count(dh));

    /* Update surviving keys with new records */
    for (uint64_t i = 1; i < N; i += 2) {
        make_key(key, i);
        make_record(rec, i + N);  /* different record */
        CHECK(disk_hash_put(dh, key, rec), "update %" PRIu64 " failed", i);
    }

    /* Verify deleted keys are gone */
    for (uint64_t i = 0; i < N; i += 2) {
        make_key(key, i);
        CHECK(!disk_hash_contains(dh, key),
              "deleted key %" PRIu64 " still exists", i);
    }

    /* Verify updated keys */
    for (uint64_t i = 1; i < N; i += 2) {
        make_key(key, i);
        make_record(rec, i + N);
        CHECK(disk_hash_get(dh, key, got),
              "updated key %" PRIu64 " not found", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "updated record %" PRIu64 " mismatch", i);
    }

    /* Persistence: sync, destroy, reopen, verify */
    disk_hash_sync(dh);
    disk_hash_destroy(dh);

    dh = disk_hash_open(DATA_PATH);
    CHECK(dh != NULL, "reopen failed");
    CHECK(disk_hash_count(dh) == N - deleted,
          "count after reopen: expected %" PRIu64 " got %" PRIu64,
          N - deleted, disk_hash_count(dh));

    for (uint64_t i = 1; i < N; i += 2) {
        make_key(key, i);
        make_record(rec, i + N);
        CHECK(disk_hash_get(dh, key, got),
              "key %" PRIu64 " not found after reopen", i);
        CHECK(memcmp(got, rec, REC_SIZE) == 0,
              "record %" PRIu64 " mismatch after reopen", i);
    }

    disk_hash_destroy(dh);
    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("\n=== disk_hash Tests ===\n\n");

    /* Phase 1: Basic CRUD */
    printf("Phase 1: Basic CRUD\n");
    RUN_TEST(test_create_destroy);
    RUN_TEST(test_put_get_single);
    RUN_TEST(test_update);
    RUN_TEST(test_delete);
    RUN_TEST(test_multi_keys);

    /* Phase 2: Batch operations */
    printf("\nPhase 2: Batch Operations\n");
    RUN_TEST(test_batch_put_get);
    RUN_TEST(test_batch_get_mixed);

    /* Phase 3: Tombstone reuse */
    printf("\nPhase 3: Tombstone Reuse\n");
    RUN_TEST(test_tombstone_reuse);

    /* Phase 4: Persistence */
    printf("\nPhase 4: Persistence\n");
    RUN_TEST(test_persistence);

    /* Phase 5: Overflow */
    printf("\nPhase 5: Overflow Buckets\n");
    RUN_TEST(test_overflow);

    /* Phase 6: Edge cases */
    printf("\nPhase 6: Edge Cases\n");
    RUN_TEST(test_empty_table);

    /* Phase 7: Scale */
    printf("\nPhase 7: Scale Correctness (100K keys)\n");
    RUN_TEST(test_scale_correctness);

    printf("\n=== Results: %d / %d passed ===\n\n",
           tests_passed, tests_run);

    return (tests_passed == tests_run) ? 0 : 1;
}
