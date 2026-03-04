/*
 * disk_hash Crash Simulation Test
 *
 * Validates crash safety by forking child processes that mutate aggressively,
 * then killing them mid-operation with SIGKILL. Parent reopens the file and
 * verifies recovery produces a consistent state.
 *
 * Test scenarios:
 *   1. Crash during bulk inserts (dirty flag recovery)
 *   2. Crash during mixed ops (insert/update/delete)
 *   3. Crash during overflow allocation (data-first write order)
 *   4. Repeated crash/recover cycles
 *
 * Usage: ./test_crash_disk_hash [rounds]
 *   Default: 20 rounds per scenario
 */

#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE        32
#define RECORD_SIZE     32
#define DATA_PATH       "/tmp/test_crash_disk_hash.dat"
#define BASELINE_START  500000
#define BASELINE_COUNT  10000

/* =========================================================================
 * Fail-fast
 * ========================================================================= */

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL at %s:%d: " fmt "\n", \
            __FILE__, __LINE__, ##__VA_ARGS__); \
    exit(1); \
} while (0)

/* =========================================================================
 * splitmix64 PRNG
 * ========================================================================= */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

/* =========================================================================
 * Key/record generation (deterministic from index)
 * ========================================================================= */

static void make_key(uint64_t i, uint8_t key[KEY_SIZE]) {
    memset(key, 0, KEY_SIZE);
    /* First 8 bytes: index (for hash). Rest: derived. */
    memcpy(key, &i, 8);
    uint64_t x = i * 0x517cc1b727220a95ULL;
    memcpy(key + 8, &x, 8);
    x = x * 0x6c62272e07bb0142ULL;
    memcpy(key + 16, &x, 8);
    x = x * 0x9e3779b97f4a7c15ULL;
    memcpy(key + 24, &x, 8);
}

static void make_record(uint64_t i, uint64_t version, uint8_t rec[RECORD_SIZE]) {
    memset(rec, 0, RECORD_SIZE);
    memcpy(rec, &i, 8);
    memcpy(rec + 8, &version, 8);
    uint64_t x = i ^ version ^ 0xdeadbeefcafebabeULL;
    memcpy(rec + 16, &x, 8);
    x = ~x;
    memcpy(rec + 24, &x, 8);
}

/* =========================================================================
 * Verify consistency after recovery
 *
 * Opens the file, checks:
 *   1. disk_hash_open succeeds (recovery runs if dirty)
 *   2. Every entry found via get is well-formed
 *   3. entry_count matches actual occupied slots
 * ========================================================================= */

static void verify_consistency(const char *label) {
    disk_hash_t *dh = disk_hash_open(DATA_PATH);
    if (!dh)
        FAIL("[%s] disk_hash_open failed after crash", label);

    uint64_t count = disk_hash_count(dh);

    /* Scan all keys 0..scan_limit, verify each found entry has valid data */
    uint64_t scan_limit = BASELINE_START + BASELINE_COUNT;
    uint64_t found_count = 0;
    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];

    for (uint64_t i = 0; i < scan_limit; i++) {
        make_key(i, key);
        if (disk_hash_get(dh, key, rec)) {
            found_count++;
            /* Verify key index matches record */
            uint64_t stored_idx;
            memcpy(&stored_idx, rec, 8);
            if (stored_idx != i)
                FAIL("[%s] key %" PRIu64 " has wrong record idx %" PRIu64,
                     label, i, stored_idx);
        }
    }

    /* found_count should match disk_hash_count (within scan range) */
    if (found_count != count && count <= scan_limit) {
        FAIL("[%s] found_count %" PRIu64 " != disk_hash_count %" PRIu64,
             label, found_count, count);
    }

    printf("    [%s] OK: count=%" PRIu64 ", found=%" PRIu64 "\n",
           label, count, found_count);

    disk_hash_sync(dh);
    disk_hash_destroy(dh);
}

/* =========================================================================
 * Verify baseline keys survive crash
 *
 * baseline_start..baseline_start+baseline_count must all be present and
 * have version==1 in their records.
 * ========================================================================= */

static void verify_baseline(const char *label, uint64_t baseline_start,
                             uint64_t baseline_count) {
    disk_hash_t *dh = disk_hash_open(DATA_PATH);
    if (!dh) FAIL("[%s] open for baseline check failed", label);

    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];
    uint64_t missing = 0;

    for (uint64_t i = baseline_start; i < baseline_start + baseline_count; i++) {
        make_key(i, key);
        if (!disk_hash_get(dh, key, rec)) {
            missing++;
            if (missing <= 5)
                fprintf(stderr, "  [%s] baseline key %" PRIu64 " MISSING\n", label, i);
            continue;
        }
        uint64_t stored_idx;
        memcpy(&stored_idx, rec, 8);
        if (stored_idx != i)
            FAIL("[%s] baseline key %" PRIu64 " has wrong idx %" PRIu64,
                 label, i, stored_idx);
        uint64_t stored_ver;
        memcpy(&stored_ver, rec + 8, 8);
        if (stored_ver != 1)
            FAIL("[%s] baseline key %" PRIu64 " has wrong version %" PRIu64,
                 label, i, stored_ver);
    }

    if (missing > 0)
        FAIL("[%s] %" PRIu64 "/%" PRIu64 " baseline keys missing",
             label, missing, baseline_count);

    disk_hash_destroy(dh);
}

/* =========================================================================
 * Scenario 1: Crash during bulk inserts
 *
 * Pre-populate baseline keys in a range the child never touches, sync.
 * Child inserts in a separate range without sync.
 * After crash: verify baseline keys survive + consistency.
 * ========================================================================= */

static void test_crash_bulk_insert(int rounds) {
    printf("\n--- Scenario 1: Crash during bulk inserts (%d rounds) ---\n", rounds);

    for (int round = 0; round < rounds; round++) {
        /* Create table and populate baseline keys */
        disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, 50000);
        if (!dh) FAIL("create failed");

        uint8_t key[KEY_SIZE], rec[RECORD_SIZE];
        for (uint64_t i = BASELINE_START; i < BASELINE_START + BASELINE_COUNT; i++) {
            make_key(i, key);
            make_record(i, 1, rec);
            if (!disk_hash_put(dh, key, rec))
                FAIL("baseline insert failed");
        }
        disk_hash_sync(dh);
        disk_hash_destroy(dh);

        pid_t pid = fork();
        if (pid < 0) FAIL("fork failed");

        if (pid == 0) {
            /* Child: insert in range 0..99999 (no overlap with baseline) */
            dh = disk_hash_open(DATA_PATH);
            if (!dh) _exit(1);

            uint8_t k[KEY_SIZE], r[RECORD_SIZE];
            for (uint64_t i = 0; i < 100000; i++) {
                make_key(i, k);
                make_record(i, 1, r);
                disk_hash_put(dh, k, r);
            }
            disk_hash_destroy(dh);
            _exit(0);
        }

        /* Parent: let child run briefly, then kill it */
        rng_t rng = { .state = (uint64_t)round * 0x9e3779b97f4a7c15ULL };
        uint64_t sleep_us = 1000 + (rng_next(&rng) % 50000);  /* 1-51ms */
        usleep((useconds_t)sleep_us);

        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);

        char label[64];
        snprintf(label, sizeof(label), "bulk round %d", round);
        verify_consistency(label);
        verify_baseline(label, BASELINE_START, BASELINE_COUNT);
    }

    printf("  Scenario 1 passed\n");
}

/* =========================================================================
 * Scenario 2: Crash during mixed operations
 *
 * Pre-populate table, then child does random insert/update/delete.
 * Kill child, verify recovery.
 * ========================================================================= */

static void test_crash_mixed_ops(int rounds) {
    printf("\n--- Scenario 2: Crash during mixed ops (%d rounds) ---\n", rounds);

    for (int round = 0; round < rounds; round++) {
        /* Create table with baseline keys (safe range) + working set */
        disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, 50000);
        if (!dh) FAIL("create failed");

        uint8_t key[KEY_SIZE], rec[RECORD_SIZE];

        /* Baseline: 500000..509999 — child never touches these */
        for (uint64_t i = BASELINE_START; i < BASELINE_START + BASELINE_COUNT; i++) {
            make_key(i, key);
            make_record(i, 1, rec);
            if (!disk_hash_put(dh, key, rec))
                FAIL("baseline insert failed");
        }

        /* Working set: 0..9999 — child will mutate these */
        for (uint64_t i = 0; i < 10000; i++) {
            make_key(i, key);
            make_record(i, 1, rec);
            if (!disk_hash_put(dh, key, rec))
                FAIL("initial insert failed");
        }
        disk_hash_sync(dh);
        disk_hash_destroy(dh);

        pid_t pid = fork();
        if (pid < 0) FAIL("fork failed");

        if (pid == 0) {
            /* Child: mixed ops on range 0..20000 only, never touches baseline */
            dh = disk_hash_open(DATA_PATH);
            if (!dh) _exit(1);

            rng_t rng = { .state = (uint64_t)round * 0x517cc1b727220a95ULL };
            for (uint64_t op = 0; op < 50000; op++) {
                uint64_t r = rng_next(&rng);
                uint64_t idx = r % 20000;
                uint8_t k[KEY_SIZE], v[RECORD_SIZE];
                make_key(idx, k);

                uint64_t action = (r >> 32) % 10;
                if (action < 5) {
                    make_record(idx, op + 2, v);
                    disk_hash_put(dh, k, v);
                } else if (action < 8) {
                    make_key(20000 + op, k);
                    make_record(20000 + op, 1, v);
                    disk_hash_put(dh, k, v);
                } else {
                    disk_hash_delete(dh, k);
                }
            }
            disk_hash_destroy(dh);
            _exit(0);
        }

        rng_t rng = { .state = (uint64_t)round * 0x6c62272e07bb0142ULL };
        uint64_t sleep_us = 500 + (rng_next(&rng) % 30000);
        usleep((useconds_t)sleep_us);

        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);

        char label[64];
        snprintf(label, sizeof(label), "mixed round %d", round);
        verify_consistency(label);
        verify_baseline(label, BASELINE_START, BASELINE_COUNT);
    }

    printf("  Scenario 2 passed\n");
}

/* =========================================================================
 * Scenario 3: Crash during overflow allocation
 *
 * Create a small table (few buckets) and insert enough keys to force
 * overflow chains. Kill child during overflow writes.
 * ========================================================================= */

static void test_crash_overflow(int rounds) {
    printf("\n--- Scenario 3: Crash during overflow allocation (%d rounds) ---\n", rounds);

    #define OVF_BASELINE_COUNT 100

    for (int round = 0; round < rounds; round++) {
        /* Small table → forces overflow quickly */
        disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, 100);
        if (!dh) FAIL("create failed");

        /* Baseline keys at 500000..500099 — child inserts 0..9999 only */
        uint8_t key[KEY_SIZE], rec[RECORD_SIZE];
        for (uint64_t i = BASELINE_START; i < BASELINE_START + OVF_BASELINE_COUNT; i++) {
            make_key(i, key);
            make_record(i, 1, rec);
            if (!disk_hash_put(dh, key, rec))
                FAIL("baseline insert failed");
        }
        disk_hash_sync(dh);
        disk_hash_destroy(dh);

        pid_t pid = fork();
        if (pid < 0) FAIL("fork failed");

        if (pid == 0) {
            /* Child: fill way past capacity to trigger many overflows */
            dh = disk_hash_open(DATA_PATH);
            if (!dh) _exit(1);

            uint8_t k[KEY_SIZE], v[RECORD_SIZE];
            for (uint64_t i = 0; i < 10000; i++) {
                make_key(i, k);
                make_record(i, 1, v);
                disk_hash_put(dh, k, v);
            }
            disk_hash_destroy(dh);
            _exit(0);
        }

        /* Kill quickly to hit overflow paths */
        rng_t rng = { .state = (uint64_t)round * 0x94d049bb133111ebULL };
        uint64_t sleep_us = 200 + (rng_next(&rng) % 10000);
        usleep((useconds_t)sleep_us);

        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);

        char label[64];
        snprintf(label, sizeof(label), "overflow round %d", round);
        verify_consistency(label);
        verify_baseline(label, BASELINE_START, OVF_BASELINE_COUNT);
    }

    printf("  Scenario 3 passed\n");
}

/* =========================================================================
 * Scenario 4: Repeated crash/recover cycles
 *
 * Same file survives multiple crash/recover without corruption accumulation.
 * ========================================================================= */

static void test_crash_repeated(int rounds) {
    printf("\n--- Scenario 4: Repeated crash/recover (%d rounds) ---\n", rounds);

    /* Create fresh table */
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, 50000);
    if (!dh) FAIL("create failed");

    /* Insert baseline: 5000 entries */
    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];
    for (uint64_t i = 0; i < 5000; i++) {
        make_key(i, key);
        make_record(i, 1, rec);
        if (!disk_hash_put(dh, key, rec))
            FAIL("baseline insert failed");
    }
    disk_hash_sync(dh);
    disk_hash_destroy(dh);

    for (int round = 0; round < rounds; round++) {
        pid_t pid = fork();
        if (pid < 0) FAIL("fork failed");

        if (pid == 0) {
            /* Child: add some entries, never sync */
            dh = disk_hash_open(DATA_PATH);
            if (!dh) _exit(1);

            rng_t rng = { .state = (uint64_t)round * 0xbf58476d1ce4e5b9ULL };
            uint8_t k[KEY_SIZE], v[RECORD_SIZE];

            for (uint64_t op = 0; op < 20000; op++) {
                uint64_t idx = 5000 + (rng_next(&rng) % 10000);
                make_key(idx, k);
                make_record(idx, (uint64_t)round + 2, v);
                disk_hash_put(dh, k, v);
            }
            disk_hash_destroy(dh);
            _exit(0);
        }

        rng_t rng = { .state = (uint64_t)round * 0x9e3779b97f4a7c15ULL + 0x42 };
        uint64_t sleep_us = 1000 + (rng_next(&rng) % 20000);
        usleep((useconds_t)sleep_us);

        kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);

        char label[64];
        snprintf(label, sizeof(label), "repeated round %d", round);
        verify_consistency(label);

        /* Verify baseline keys survive each crash */
        dh = disk_hash_open(DATA_PATH);
        if (!dh) FAIL("reopen after repeated round %d failed", round);

        uint64_t baseline_found = 0;
        for (uint64_t i = 0; i < 5000; i++) {
            make_key(i, key);
            if (disk_hash_get(dh, key, rec)) {
                uint64_t stored_idx;
                memcpy(&stored_idx, rec, 8);
                if (stored_idx != i)
                    FAIL("baseline key %" PRIu64 " corrupted", i);
                baseline_found++;
            }
        }
        if (baseline_found != 5000)
            FAIL("round %d: baseline entries lost: %" PRIu64 "/5000",
                 round, baseline_found);

        disk_hash_sync(dh);
        disk_hash_destroy(dh);
    }

    printf("  Scenario 4 passed\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char *argv[]) {
    int rounds = 20;
    if (argc > 1) rounds = atoi(argv[1]);
    if (rounds <= 0) rounds = 1;

    printf("=== disk_hash Crash Simulation Test ===\n");
    printf("  rounds per scenario: %d\n", rounds);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    test_crash_bulk_insert(rounds);
    test_crash_mixed_ops(rounds);
    test_crash_overflow(rounds);
    test_crash_repeated(rounds);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec)
                   + (double)(t1.tv_nsec - t0.tv_nsec) * 1e-9;

    /* Clean up */
    unlink(DATA_PATH);

    printf("\n============================================\n");
    printf("ALL CRASH TESTS PASSED\n");
    printf("============================================\n");
    printf("  rounds:    %d per scenario\n", rounds);
    printf("  scenarios: 4\n");
    printf("  wall time: %.1fs\n", elapsed);
    printf("============================================\n");

    return 0;
}
