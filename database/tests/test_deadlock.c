/*
 * Targeted Deadlock Test
 *
 * Specifically tests lock ordering between write_lock and resize_lock.
 * Each sub-test uses alarm() as a watchdog — if the test hangs for more
 * than TIMEOUT_SEC seconds, SIGALRM fires and reports a deadlock.
 *
 * Sub-tests:
 *   1. Writer + Iterators during mmap resize
 *   2. Writer + Many readers (starvation check)
 *   3. Snapshot create/end + Writer
 *   4. Combined pressure (all lock patterns)
 *
 * Usage:
 *   ./test_deadlock              # run all 4 sub-tests
 */

#include "../include/data_art.h"
#include "../include/logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <signal.h>
#include <sys/stat.h>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#define KEY_SIZE            32
#define VALUE_MAX_LEN       64
#define TIMEOUT_SEC         15      // per sub-test watchdog
#define PREPOPULATE_KEYS    1000    // initial keys before spawning threads
#define WRITER_OPS_PER_TXN  50      // ops per transaction (small = more commits)

// ---------------------------------------------------------------------------
// splitmix64 PRNG
// ---------------------------------------------------------------------------

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline uint32_t rng_range(rng_t *rng, uint32_t min, uint32_t max) {
    return min + (uint32_t)(rng_next(rng) % (uint64_t)(max - min + 1));
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);
    return r;
}

// ---------------------------------------------------------------------------
// Key / value generation
// ---------------------------------------------------------------------------

static void generate_key(uint8_t *key, uint64_t index) {
    uint32_t h = (uint32_t)index * 2654435761u;
    key[0] = (h >> 24) & 0xFF;
    key[1] = (h >> 16) & 0xFF;
    key[2] = (h >> 8)  & 0xFF;
    key[3] = h & 0xFF;
    h = ((uint32_t)index + 1) * 2246822519u;
    key[4] = (h >> 24) & 0xFF;
    key[5] = (h >> 16) & 0xFF;
    key[6] = (h >> 8)  & 0xFF;
    key[7] = h & 0xFF;
    for (int i = 8; i < KEY_SIZE; i++) {
        h = h * 1103515245u + 12345;
        key[i] = (h >> 16) & 0xFF;
    }
}

static size_t generate_value(char *buf, uint64_t index) {
    int n = snprintf(buf, VALUE_MAX_LEN, "v%" PRIu64, index);
    return (size_t)n + 1;
}

// ---------------------------------------------------------------------------
// Watchdog (SIGALRM)
// ---------------------------------------------------------------------------

static volatile sig_atomic_t g_current_test = 0;
static const char *g_test_names[] = {
    "unknown",
    "Writer + Iterators (resize contention)",
    "Writer + Readers (starvation check)",
    "Snapshots + Writer",
    "Combined pressure",
};

static void alarm_handler(int sig) {
    (void)sig;
    const char prefix[] = "\n*** DEADLOCK DETECTED in test: ";
    const char suffix[] = " ***\n";
    write(STDERR_FILENO, prefix, sizeof(prefix) - 1);
    int idx = g_current_test;
    if (idx >= 1 && idx <= 4) {
        const char *name = g_test_names[idx];
        write(STDERR_FILENO, name, strlen(name));
    }
    write(STDERR_FILENO, suffix, sizeof(suffix) - 1);
    _exit(1);
}

// ---------------------------------------------------------------------------
// Shared thread context
// ---------------------------------------------------------------------------

typedef struct {
    data_art_tree_t    *tree;
    volatile bool       stop;
    _Atomic uint64_t    next_key;       // monotonically increasing key index
    _Atomic uint64_t    writer_blocks;  // committed blocks counter
    uint64_t            seed;
} test_ctx_t;

// ---------------------------------------------------------------------------
// Helper: create tree in temp directory, prepopulate
// ---------------------------------------------------------------------------

static char g_tmpdir[256];

static data_art_tree_t *create_test_tree(test_ctx_t *ctx) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/test_deadlock_%d", getpid());
    mkdir(g_tmpdir, 0755);

    char path[512];
    snprintf(path, sizeof(path), "%s/art.dat", g_tmpdir);

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "FAIL: data_art_create failed\n");
        return NULL;
    }

    // Prepopulate so readers/iterators have data from the start
    uint64_t txn_id;
    data_art_begin_txn(tree, &txn_id);
    for (uint64_t i = 0; i < PREPOPULATE_KEYS; i++) {
        uint8_t key[KEY_SIZE];
        char val[VALUE_MAX_LEN];
        generate_key(key, i);
        size_t vlen = generate_value(val, i);
        data_art_insert(tree, key, KEY_SIZE, val, vlen);
    }
    data_art_commit_txn(tree);
    data_art_checkpoint(tree, NULL);

    ctx->tree = tree;
    ctx->stop = false;
    atomic_store(&ctx->next_key, PREPOPULATE_KEYS);
    atomic_store(&ctx->writer_blocks, 0);
    return tree;
}

static void cleanup_test(data_art_tree_t *tree) {
    data_art_destroy(tree);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_tmpdir);
    int ret = system(cmd);
    (void)ret;
}

// ---------------------------------------------------------------------------
// Thread: Writer (continuous txn commit)
// ---------------------------------------------------------------------------

static void *writer_thread(void *arg) {
    test_ctx_t *ctx = (test_ctx_t *)arg;
    while (!ctx->stop) {
        uint64_t txn_id;
        if (!data_art_begin_txn(ctx->tree, &txn_id)) break;

        for (int i = 0; i < WRITER_OPS_PER_TXN; i++) {
            uint64_t idx = atomic_fetch_add(&ctx->next_key, 1);
            uint8_t key[KEY_SIZE];
            char val[VALUE_MAX_LEN];
            generate_key(key, idx);
            size_t vlen = generate_value(val, idx);
            data_art_insert(ctx->tree, key, KEY_SIZE, val, vlen);
        }

        if (!data_art_commit_txn(ctx->tree)) break;
        atomic_fetch_add(&ctx->writer_blocks, 1);
    }
    return NULL;
}

// ---------------------------------------------------------------------------
// Thread: Iterator (tight create/scan/destroy loop)
// ---------------------------------------------------------------------------

static void *iterator_thread(void *arg) {
    test_ctx_t *ctx = (test_ctx_t *)arg;
    uint64_t scans = 0;

    while (!ctx->stop) {
        data_art_iterator_t *iter = data_art_iterator_create(ctx->tree);
        if (!iter) continue;

        int count = 0;
        while (data_art_iterator_next(iter)) {
            count++;
            // Don't do anything with the data — just traverse
        }
        data_art_iterator_destroy(iter);
        scans++;
        (void)count;
    }

    // Store scans in thread return value
    return (void *)scans;
}

// ---------------------------------------------------------------------------
// Thread: Reader (tight data_art_get loop)
// ---------------------------------------------------------------------------

static void *reader_thread(void *arg) {
    test_ctx_t *ctx = (test_ctx_t *)arg;
    rng_t rng = rng_create(ctx->seed ^ (uint64_t)pthread_self());
    uint64_t gets = 0;

    while (!ctx->stop) {
        uint64_t max_key = atomic_load(&ctx->next_key);
        if (max_key == 0) continue;
        uint64_t idx = rng_next(&rng) % max_key;

        uint8_t key[KEY_SIZE];
        generate_key(key, idx);

        size_t vlen;
        const void *val = data_art_get(ctx->tree, key, KEY_SIZE, &vlen);
        if (val) free((void *)val);
        gets++;
    }

    return (void *)gets;
}

// ---------------------------------------------------------------------------
// Thread: Snapshot (tight create/read/end loop)
// ---------------------------------------------------------------------------

static void *snapshot_thread(void *arg) {
    test_ctx_t *ctx = (test_ctx_t *)arg;
    rng_t rng = rng_create(ctx->seed ^ (uint64_t)pthread_self() ^ 0x5A);
    uint64_t cycles = 0;

    while (!ctx->stop) {
        data_art_snapshot_t *snap = data_art_begin_snapshot(ctx->tree);
        if (!snap) continue;

        // Do a few reads within the snapshot
        uint64_t max_key = atomic_load(&ctx->next_key);
        for (int i = 0; i < 5 && max_key > 0; i++) {
            uint64_t idx = rng_next(&rng) % max_key;
            uint8_t key[KEY_SIZE];
            generate_key(key, idx);
            size_t vlen;
            const void *val = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &vlen, snap);
            if (val) free((void *)val);
        }

        data_art_end_snapshot(ctx->tree, snap);
        cycles++;
    }

    return (void *)cycles;
}

// ---------------------------------------------------------------------------
// Thread: Checkpoint
// ---------------------------------------------------------------------------

static void *checkpoint_thread(void *arg) {
    test_ctx_t *ctx = (test_ctx_t *)arg;
    uint64_t ckpts = 0;

    while (!ctx->stop) {
        usleep(100000);  // 100ms between checkpoints
        data_art_checkpoint(ctx->tree, NULL);
        ckpts++;
    }

    return (void *)ckpts;
}

// ---------------------------------------------------------------------------
// Timer helper
// ---------------------------------------------------------------------------

static double elapsed_sec(struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (now.tv_sec - start->tv_sec) + (now.tv_nsec - start->tv_nsec) / 1e9;
}

// ---------------------------------------------------------------------------
// Test 1: Writer + Iterators (resize contention)
// ---------------------------------------------------------------------------

static bool test_writer_iterators(void) {
    test_ctx_t ctx = { .seed = 0xDEAD1001 };
    data_art_tree_t *tree = create_test_tree(&ctx);
    if (!tree) return false;

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    // Spawn threads
    #define T1_ITERS 4
    pthread_t writer, iters[T1_ITERS];
    pthread_create(&writer, NULL, writer_thread, &ctx);
    for (int i = 0; i < T1_ITERS; i++)
        pthread_create(&iters[i], NULL, iterator_thread, &ctx);

    // Run for 10s or until writer has done enough work
    for (int s = 0; s < 10; s++) {
        sleep(1);
        if (atomic_load(&ctx.writer_blocks) >= 400) break;
    }

    ctx.stop = true;
    pthread_join(writer, NULL);
    uint64_t total_scans = 0;
    for (int i = 0; i < T1_ITERS; i++) {
        void *ret;
        pthread_join(iters[i], &ret);
        total_scans += (uint64_t)ret;
    }

    double dt = elapsed_sec(&t0);
    uint64_t blocks = atomic_load(&ctx.writer_blocks);
    printf("      writer: %" PRIu64 " blocks | iterators: %" PRIu64 " scans",
           blocks, total_scans);

    bool pass = blocks >= 10;  // writer must make progress
    printf("%*sPASS (%.1fs)\n", 4, "", dt);

    cleanup_test(tree);
    return pass;
}

// ---------------------------------------------------------------------------
// Test 2: Writer + Many readers (starvation check)
// ---------------------------------------------------------------------------

static bool test_writer_readers(void) {
    test_ctx_t ctx = { .seed = 0xDEAD2002 };
    data_art_tree_t *tree = create_test_tree(&ctx);
    if (!tree) return false;

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    #define T2_READERS 8
    pthread_t writer, readers[T2_READERS];
    pthread_create(&writer, NULL, writer_thread, &ctx);
    for (int i = 0; i < T2_READERS; i++)
        pthread_create(&readers[i], NULL, reader_thread, &ctx);

    sleep(5);
    ctx.stop = true;

    pthread_join(writer, NULL);
    uint64_t total_gets = 0;
    for (int i = 0; i < T2_READERS; i++) {
        void *ret;
        pthread_join(readers[i], &ret);
        total_gets += (uint64_t)ret;
    }

    double dt = elapsed_sec(&t0);
    uint64_t blocks = atomic_load(&ctx.writer_blocks);
    printf("      writer: %" PRIu64 " blocks | readers: %" PRIu64 " gets",
           blocks, total_gets);

    bool pass = blocks >= 100;  // writer must not be starved
    if (!pass) {
        printf("    FAIL (writer starved: only %" PRIu64 " blocks)\n", blocks);
    } else {
        printf("%*sPASS (%.1fs)\n", 4, "", dt);
    }

    cleanup_test(tree);
    return pass;
}

// ---------------------------------------------------------------------------
// Test 3: Snapshots + Writer
// ---------------------------------------------------------------------------

static bool test_snapshots_writer(void) {
    test_ctx_t ctx = { .seed = 0xDEAD3003 };
    data_art_tree_t *tree = create_test_tree(&ctx);
    if (!tree) return false;

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    #define T3_SNAPS 4
    pthread_t writer, snaps[T3_SNAPS];
    pthread_create(&writer, NULL, writer_thread, &ctx);
    for (int i = 0; i < T3_SNAPS; i++)
        pthread_create(&snaps[i], NULL, snapshot_thread, &ctx);

    sleep(5);
    ctx.stop = true;

    pthread_join(writer, NULL);
    uint64_t total_cycles = 0;
    for (int i = 0; i < T3_SNAPS; i++) {
        void *ret;
        pthread_join(snaps[i], &ret);
        total_cycles += (uint64_t)ret;
    }

    double dt = elapsed_sec(&t0);
    uint64_t blocks = atomic_load(&ctx.writer_blocks);
    printf("      writer: %" PRIu64 " blocks | snapshots: %" PRIu64 " cycles",
           blocks, total_cycles);

    bool pass = blocks >= 50;
    printf("%*sPASS (%.1fs)\n", 4, "", dt);

    cleanup_test(tree);
    return pass;
}

// ---------------------------------------------------------------------------
// Test 4: Combined pressure
// ---------------------------------------------------------------------------

static bool test_combined(void) {
    test_ctx_t ctx = { .seed = 0xDEAD4004 };
    data_art_tree_t *tree = create_test_tree(&ctx);
    if (!tree) return false;

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    #define T4_READERS 4
    #define T4_ITERS   2
    #define T4_SNAPS   2
    pthread_t writer;
    pthread_t readers[T4_READERS], iters[T4_ITERS], snaps[T4_SNAPS], ckpt;

    pthread_create(&writer, NULL, writer_thread, &ctx);
    for (int i = 0; i < T4_READERS; i++)
        pthread_create(&readers[i], NULL, reader_thread, &ctx);
    for (int i = 0; i < T4_ITERS; i++)
        pthread_create(&iters[i], NULL, iterator_thread, &ctx);
    for (int i = 0; i < T4_SNAPS; i++)
        pthread_create(&snaps[i], NULL, snapshot_thread, &ctx);
    pthread_create(&ckpt, NULL, checkpoint_thread, &ctx);

    sleep(10);
    ctx.stop = true;

    pthread_join(writer, NULL);
    uint64_t total_gets = 0, total_scans = 0, total_snap_cycles = 0;
    for (int i = 0; i < T4_READERS; i++) {
        void *ret;
        pthread_join(readers[i], &ret);
        total_gets += (uint64_t)ret;
    }
    for (int i = 0; i < T4_ITERS; i++) {
        void *ret;
        pthread_join(iters[i], &ret);
        total_scans += (uint64_t)ret;
    }
    for (int i = 0; i < T4_SNAPS; i++) {
        void *ret;
        pthread_join(snaps[i], &ret);
        total_snap_cycles += (uint64_t)ret;
    }
    void *ckpt_ret;
    pthread_join(ckpt, &ckpt_ret);

    double dt = elapsed_sec(&t0);
    uint64_t blocks = atomic_load(&ctx.writer_blocks);
    printf("      writer: %" PRIu64 " blocks | readers: %" PRIu64
           " | iters: %" PRIu64 " | snaps: %" PRIu64,
           blocks, total_gets, total_scans, total_snap_cycles);

    bool pass = blocks >= 50;
    if (!pass) {
        printf("    FAIL (writer starved: only %" PRIu64 " blocks)\n", blocks);
    } else {
        printf("%*sPASS (%.1fs)\n", 4, "", dt);
    }

    cleanup_test(tree);
    return pass;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(void) {
    // Install watchdog
    struct sigaction sa;
    sa.sa_handler = alarm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGALRM, &sa, NULL);

    printf("=== Deadlock Tests ===\n\n");

    int passed = 0, total = 4;

    // Test 1
    g_current_test = 1;
    printf("[1/%d] Writer + Iterators (resize contention)...\n", total);
    alarm(TIMEOUT_SEC);
    if (test_writer_iterators()) passed++;
    alarm(0);

    // Test 2
    g_current_test = 2;
    printf("[2/%d] Writer + Readers (starvation check)...\n", total);
    alarm(TIMEOUT_SEC);
    if (test_writer_readers()) passed++;
    alarm(0);

    // Test 3
    g_current_test = 3;
    printf("[3/%d] Snapshots + Writer...\n", total);
    alarm(TIMEOUT_SEC);
    if (test_snapshots_writer()) passed++;
    alarm(0);

    // Test 4
    g_current_test = 4;
    printf("[4/%d] Combined pressure...\n", total);
    alarm(TIMEOUT_SEC);
    if (test_combined()) passed++;
    alarm(0);

    printf("\n");
    if (passed == total) {
        printf("All %d deadlock tests PASSED\n", total);
        return 0;
    } else {
        printf("FAILED: %d/%d tests passed\n", passed, total);
        return 1;
    }
}
