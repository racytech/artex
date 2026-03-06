/*
 * disk_hash Multi-Threaded Stress Test
 *
 * Validates thread safety with concurrent readers and a single writer.
 *
 *   - Pre-populate table with SAFE_COUNT keys in the "safe range"
 *     (indices SAFE_START..SAFE_START+SAFE_COUNT). These are never
 *     modified by the writer, so readers must always see correct values.
 *
 *   - Writer thread: continuous put/delete on the "hot range"
 *     (indices 0..HOT_RANGE). Runs for DURATION_SEC seconds.
 *
 *   - Reader threads (N): continuous get on safe-range keys.
 *     Every found entry is verified for correctness. Fail-fast on
 *     any data corruption.
 *
 * Usage: ./test_mt_disk_hash [duration_sec] [num_readers]
 *   Default: 5 seconds, 4 reader threads
 */

#include "../include/disk_hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE        32
#define RECORD_SIZE     32
#define DATA_PATH       "/tmp/test_mt_disk_hash.dat"

#define SAFE_START      500000      /* safe range: never touched by writer */
#define SAFE_COUNT      50000
#define HOT_RANGE       100000      /* writer operates on 0..HOT_RANGE-1 */

/* =========================================================================
 * Fail-fast
 * ========================================================================= */

#define FAIL(fmt, ...) do { \
    fprintf(stderr, "FAIL at %s:%d: " fmt "\n", \
            __FILE__, __LINE__, ##__VA_ARGS__); \
    atomic_store(&g_stop, true); \
    exit(1); \
} while (0)

/* =========================================================================
 * Shared state
 * ========================================================================= */

static atomic_bool g_stop = false;

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
 * Key/record generation
 * ========================================================================= */

static void make_key(uint64_t i, uint8_t key[KEY_SIZE]) {
    memset(key, 0, KEY_SIZE);
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

static bool verify_record(uint64_t i, const uint8_t rec[RECORD_SIZE]) {
    uint64_t stored_idx;
    memcpy(&stored_idx, rec, 8);
    return stored_idx == i;
}

/* =========================================================================
 * Writer thread
 * ========================================================================= */

typedef struct {
    disk_hash_t *dh;
    uint64_t     ops;
} writer_ctx_t;

static void *writer_fn(void *arg) {
    writer_ctx_t *ctx = (writer_ctx_t *)arg;
    disk_hash_t *dh = ctx->dh;
    rng_t rng = { .state = 0xdeadbeef12345678ULL };
    uint64_t ops = 0;

    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];

    while (!atomic_load(&g_stop)) {
        uint64_t r = rng_next(&rng);
        uint64_t idx = r % HOT_RANGE;
        uint64_t action = (r >> 32) % 10;

        make_key(idx, key);

        if (action < 7) {
            /* 70% put (insert or update) */
            make_record(idx, ops + 1, rec);
            disk_hash_put(dh, key, rec);
        } else {
            /* 30% delete */
            disk_hash_delete(dh, key);
        }
        ops++;
    }

    ctx->ops = ops;
    return NULL;
}

/* =========================================================================
 * Reader thread
 * ========================================================================= */

typedef struct {
    disk_hash_t *dh;
    int          id;
    uint64_t     ops;
    uint64_t     found;
    uint64_t     errors;
} reader_ctx_t;

static void *reader_fn(void *arg) {
    reader_ctx_t *ctx = (reader_ctx_t *)arg;
    disk_hash_t *dh = ctx->dh;
    rng_t rng = { .state = 0x12345678ULL + (uint64_t)ctx->id * 0x9e3779b97f4a7c15ULL };
    uint64_t ops = 0, found = 0;

    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];

    while (!atomic_load(&g_stop)) {
        /* Read from safe range — must always succeed with correct data */
        uint64_t idx = SAFE_START + (rng_next(&rng) % SAFE_COUNT);
        make_key(idx, key);

        if (disk_hash_get(dh, key, rec)) {
            found++;
            if (!verify_record(idx, rec))
                FAIL("reader %d: safe key %" PRIu64 " has corrupted record", ctx->id, idx);
        } else {
            /* Safe-range key must always exist */
            FAIL("reader %d: safe key %" PRIu64 " not found", ctx->id, idx);
        }

        ops++;

        /* Occasional spot-check on hot range — may or may not exist,
         * but if found must be self-consistent */
        if ((ops & 0xF) == 0) {
            uint64_t hot_idx = rng_next(&rng) % HOT_RANGE;
            make_key(hot_idx, key);
            if (disk_hash_get(dh, key, rec)) {
                if (!verify_record(hot_idx, rec))
                    FAIL("reader %d: hot key %" PRIu64 " has corrupted record",
                         ctx->id, hot_idx);
            }
            ops++;
        }
    }

    ctx->ops = ops;
    ctx->found = found;
    ctx->errors = 0;
    return NULL;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char *argv[]) {
    int duration_sec = 5;
    int num_readers = 4;

    if (argc > 1) duration_sec = atoi(argv[1]);
    if (argc > 2) num_readers = atoi(argv[2]);
    if (duration_sec <= 0) duration_sec = 1;
    if (num_readers <= 0) num_readers = 1;
    if (num_readers > 32) num_readers = 32;

    printf("=== disk_hash Multi-Threaded Stress Test ===\n");
    printf("  duration:  %d seconds\n", duration_sec);
    printf("  readers:   %d threads\n", num_readers);
    printf("  safe keys: %d (indices %d..%d)\n",
           SAFE_COUNT, SAFE_START, SAFE_START + SAFE_COUNT - 1);
    printf("  hot range: 0..%d\n\n", HOT_RANGE - 1);

    /* Create and populate */
    printf("  Populating...\n");
    disk_hash_t *dh = disk_hash_create(DATA_PATH, KEY_SIZE, RECORD_SIZE, 200000);
    if (!dh) FAIL("create failed");

    uint8_t key[KEY_SIZE], rec[RECORD_SIZE];

    /* Safe range */
    for (uint64_t i = SAFE_START; i < SAFE_START + SAFE_COUNT; i++) {
        make_key(i, key);
        make_record(i, 1, rec);
        if (!disk_hash_put(dh, key, rec))
            FAIL("safe insert %" PRIu64 " failed", i);
    }

    /* Hot range initial population */
    for (uint64_t i = 0; i < HOT_RANGE; i++) {
        make_key(i, key);
        make_record(i, 1, rec);
        if (!disk_hash_put(dh, key, rec))
            FAIL("hot insert %" PRIu64 " failed", i);
    }

    disk_hash_sync(dh);
    printf("  Populated: %"PRIu64" entries\n\n", disk_hash_count(dh));

    /* Launch threads */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    writer_ctx_t wctx = { .dh = dh, .ops = 0 };
    pthread_t writer_thread;
    pthread_create(&writer_thread, NULL, writer_fn, &wctx);

    reader_ctx_t *rctx = calloc((size_t)num_readers, sizeof(reader_ctx_t));
    pthread_t *reader_threads = calloc((size_t)num_readers, sizeof(pthread_t));

    for (int i = 0; i < num_readers; i++) {
        rctx[i].dh = dh;
        rctx[i].id = i;
        pthread_create(&reader_threads[i], NULL, reader_fn, &rctx[i]);
    }

    /* Let threads run */
    printf("  Running %d seconds...\n", duration_sec);
    sleep((unsigned)duration_sec);
    atomic_store(&g_stop, true);

    /* Join all threads */
    pthread_join(writer_thread, NULL);
    for (int i = 0; i < num_readers; i++)
        pthread_join(reader_threads[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (double)(t1.tv_sec - t0.tv_sec)
                   + (double)(t1.tv_nsec - t0.tv_nsec) * 1e-9;

    /* Report */
    printf("\n  Writer:  %" PRIu64 " ops (%.0f Kops/s)\n",
           wctx.ops, (double)wctx.ops / elapsed / 1000.0);

    uint64_t total_reader_ops = 0;
    for (int i = 0; i < num_readers; i++) {
        printf("  Reader %d: %" PRIu64 " ops, %" PRIu64 " found\n",
               i, rctx[i].ops, rctx[i].found);
        total_reader_ops += rctx[i].ops;
    }
    printf("  Total reader ops: %" PRIu64 " (%.0f Kops/s)\n",
           total_reader_ops, (double)total_reader_ops / elapsed / 1000.0);

    /* Final verification: all safe-range keys still intact */
    printf("\n  Final verification: safe-range keys...\n");
    uint64_t safe_found = 0;
    for (uint64_t i = SAFE_START; i < SAFE_START + SAFE_COUNT; i++) {
        make_key(i, key);
        if (!disk_hash_get(dh, key, rec))
            FAIL("final check: safe key %" PRIu64 " missing", i);
        if (!verify_record(i, rec))
            FAIL("final check: safe key %" PRIu64 " corrupted", i);
        safe_found++;
    }
    printf("  Safe-range: %" PRIu64 "/%" PRIu64 " verified OK\n",
           safe_found, (uint64_t)SAFE_COUNT);

    /* Cleanup */
    disk_hash_destroy(dh);
    unlink(DATA_PATH);
    free(rctx);
    free(reader_threads);

    printf("\n============================================\n");
    printf("ALL MT TESTS PASSED\n");
    printf("============================================\n");
    printf("  duration:    %.1fs\n", elapsed);
    printf("  writer ops:  %" PRIu64 "\n", wctx.ops);
    printf("  reader ops:  %" PRIu64 "\n", total_reader_ops);
    printf("============================================\n");

    return 0;
}
