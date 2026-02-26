/*
 * Reproducible Loop Stress Test — Ethereum Workload Simulator
 *
 * Runs N iterations, each starting with a fresh tree:
 *   1. Create tree, populate initial state ("block 0")
 *   2. Run concurrent stress: 1 writer + 8 readers + 2 iterators
 *      + 1 checkpoint thread + 1 long-snapshot thread (5 seconds)
 *   3. Stop all threads
 *   4. Verify final state against writer's shadow map
 *   5. Close, reopen, verify persistence
 *   6. Cleanup
 *
 * Every failure prints the seed for exact reproduction:
 *   ./test_stress_loop 1 0x<seed>
 *
 * Usage:
 *   ./test_stress_loop              # 20 iterations, random seed each
 *   ./test_stress_loop 50           # 50 iterations
 *   ./test_stress_loop 1 0xABCD     # 1 iteration with exact seed
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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#define DEFAULT_ITERATIONS       20
#define DEFAULT_DURATION_SEC     5
#define DEFAULT_NUM_KEYS         5000
#define DEFAULT_NUM_READERS      8
#define DEFAULT_NUM_ITERATORS    2
#define KEY_SIZE                 32
#define VALUE_MAX_LEN            64
#define BLOCK_OPS_MIN            50
#define BLOCK_OPS_MAX            200
#define CHECKPOINT_EVERY_BLOCKS  5
#define DELETE_PROBABILITY       30   // percent
#define ABORT_PROBABILITY        5    // percent
#define READS_PER_SNAPSHOT_MIN   10
#define READS_PER_SNAPSHOT_MAX   50
#define LONG_SNAPSHOT_HOLD_MS    2000

// ---------------------------------------------------------------------------
// splitmix64 PRNG — deterministic, platform-independent
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
    rng_next(&r);  // warm up
    return r;
}

// ---------------------------------------------------------------------------
// Global fail-fast state
// ---------------------------------------------------------------------------

static volatile bool g_failed = false;
static volatile uint64_t g_fail_seed = 0;
static volatile int g_fail_iter = -1;


// ---------------------------------------------------------------------------
// Shadow map — writer's ground truth (single-writer, no lock needed)
// ---------------------------------------------------------------------------

typedef struct {
    bool     exists;
    char     value[VALUE_MAX_LEN];
    size_t   value_len;
    uint32_t version;
} shadow_entry_t;

// ---------------------------------------------------------------------------
// Per-thread context
// ---------------------------------------------------------------------------

typedef struct {
    data_art_tree_t    *tree;
    shadow_entry_t     *shadow;
    int                 num_keys;
    volatile bool      *stop;
    uint64_t            master_seed;
    int                 iteration;

    rng_t               rng;
    int                 thread_id;
    char                thread_name[32];

    // stats (thread-local, aggregated after join)
    uint64_t            ops;
    uint64_t            blocks_committed;
    uint64_t            blocks_aborted;
    uint64_t            snapshots_taken;
    uint64_t            isolation_checks;
    uint64_t            iterator_scans;
    uint64_t            checkpoints;
} thread_ctx_t;

// ---------------------------------------------------------------------------
// Fail-fast macro (for thread functions that return void*)
// ---------------------------------------------------------------------------

#define STRESS_ASSERT(cond, fmt, ...) do {                                      \
    if (!(cond)) {                                                              \
        fprintf(stderr,                                                         \
            "\nFAIL [iter=%d seed=0x%016" PRIx64 " thread=%s] " fmt "\n",       \
            ctx->iteration, ctx->master_seed, ctx->thread_name, ##__VA_ARGS__); \
        fflush(stderr);                                                         \
        g_failed = true;                                                        \
        g_fail_seed = ctx->master_seed;                                         \
        g_fail_iter = ctx->iteration;                                           \
        return NULL;                                                            \
    }                                                                           \
} while(0)

// ---------------------------------------------------------------------------
// Key / value generation
// ---------------------------------------------------------------------------

static void generate_key(uint8_t *key, int index) {
    memset(key, 0, KEY_SIZE);
    key[0] = (uint8_t)((index >> 24) & 0xFF);
    key[1] = (uint8_t)((index >> 16) & 0xFF);
    key[2] = (uint8_t)((index >> 8) & 0xFF);
    key[3] = (uint8_t)(index & 0xFF);
}

static size_t generate_value(char *buf, int key_index, uint32_t version) {
    int n = snprintf(buf, VALUE_MAX_LEN, "k%d_v%u", key_index, version);
    return (size_t)n + 1;  // include null terminator
}

// ---------------------------------------------------------------------------
// Thread: Block Writer (1 thread)
//
// Mimics Ethereum block executor: each "block" is a transaction with
// 50-200 mixed insert/delete ops, then commit. Every N blocks, checkpoint.
// ---------------------------------------------------------------------------

typedef struct {
    int    key_index;
    bool   is_delete;
    char   value[VALUE_MAX_LEN];
    size_t value_len;
} pending_op_t;

static void *writer_thread_fn(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    pending_op_t pending[BLOCK_OPS_MAX];

    while (!*ctx->stop && !g_failed) {
        int block_size = (int)rng_range(&ctx->rng, BLOCK_OPS_MIN, BLOCK_OPS_MAX);
        bool should_abort = rng_range(&ctx->rng, 0, 99) < ABORT_PROBABILITY;

        uint64_t txn_id;
        if (!data_art_begin_txn(ctx->tree, &txn_id)) {
            // begin_txn can fail if another txn is active on this thread
            // (shouldn't happen with single writer, but be safe)
            usleep(100);
            continue;
        }

        int pending_count = 0;
        bool success = true;

        for (int i = 0; i < block_size && !*ctx->stop && !g_failed; i++) {
            int key_index = (int)rng_range(&ctx->rng, 0, (uint32_t)(ctx->num_keys - 1));
            uint8_t key[KEY_SIZE];
            generate_key(key, key_index);

            bool is_delete = rng_range(&ctx->rng, 0, 99) < DELETE_PROBABILITY;

            if (is_delete) {
                data_art_delete(ctx->tree, key, KEY_SIZE);
                // delete may "fail" if key doesn't exist — that's fine
            } else {
                // Version comes from shadow (only writer reads/writes it)
                uint32_t ver = ctx->shadow[key_index].version + 1;
                char value[VALUE_MAX_LEN];
                size_t vlen = generate_value(value, key_index, ver);

                if (!data_art_insert(ctx->tree, key, KEY_SIZE, value, vlen)) {
                    success = false;
                    break;
                }

                pending[pending_count].value_len = vlen;
                memcpy(pending[pending_count].value, value, vlen);
            }

            pending[pending_count].key_index = key_index;
            pending[pending_count].is_delete = is_delete;
            pending_count++;
            ctx->ops++;
        }

        if (should_abort || !success) {
            data_art_abort_txn(ctx->tree);
            ctx->blocks_aborted++;
            continue;
        }

        if (!data_art_commit_txn(ctx->tree)) {
            STRESS_ASSERT(false, "commit_txn failed");
        }

        // Commit succeeded — apply pending ops to shadow map
        for (int i = 0; i < pending_count; i++) {
            int ki = pending[i].key_index;
            if (pending[i].is_delete) {
                ctx->shadow[ki].exists = false;
            } else {
                ctx->shadow[ki].exists = true;
                memcpy(ctx->shadow[ki].value, pending[i].value, pending[i].value_len);
                ctx->shadow[ki].value_len = pending[i].value_len;
                ctx->shadow[ki].version++;
            }
        }

        ctx->blocks_committed++;

        // Periodic checkpoint
        if (ctx->blocks_committed % CHECKPOINT_EVERY_BLOCKS == 0) {
            data_art_checkpoint(ctx->tree, NULL);
            ctx->checkpoints++;
        }
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Thread: Reader (8 threads)
//
// Mimics JSON-RPC point lookups. Tight loop — no usleep.
// Core invariant: read the same key twice in the same snapshot, get identical
// results. No cross-thread state needed.
// ---------------------------------------------------------------------------

static void *reader_thread_fn(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        data_art_snapshot_t *snap = data_art_begin_snapshot(ctx->tree);
        if (!snap) continue;
        ctx->snapshots_taken++;

        int nreads = (int)rng_range(&ctx->rng, READS_PER_SNAPSHOT_MIN, READS_PER_SNAPSHOT_MAX);

        for (int i = 0; i < nreads && !*ctx->stop && !g_failed; i++) {
            int key_index = (int)rng_range(&ctx->rng, 0, (uint32_t)(ctx->num_keys - 1));
            uint8_t key[KEY_SIZE];
            generate_key(key, key_index);

            size_t len1, len2;
            const void *val1 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len1, snap);
            const void *val2 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len2, snap);

            if (val1 == NULL && val2 == NULL) {
                // Both NULL — consistent
            } else if (val1 != NULL && val2 != NULL) {
                STRESS_ASSERT(len1 == len2,
                    "isolation: key %d len mismatch %zu vs %zu", key_index, len1, len2);
                STRESS_ASSERT(memcmp(val1, val2, len1) == 0,
                    "isolation: key %d value changed within snapshot", key_index);
            } else {
                STRESS_ASSERT(false,
                    "isolation: key %d existence flipped within snapshot (val1=%p val2=%p)",
                    key_index, val1, val2);
            }

            free((void *)val1);
            free((void *)val2);
            ctx->isolation_checks++;
            ctx->ops++;
        }

        data_art_end_snapshot(ctx->tree, snap);
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Thread: Iterator (2 threads)
//
// Full scans and prefix scans. Checks sorted order invariant.
// ---------------------------------------------------------------------------

static void *iterator_thread_fn(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;
    uint8_t prev_key[KEY_SIZE];

    while (!*ctx->stop && !g_failed) {
        bool prefix_scan = rng_range(&ctx->rng, 0, 99) < 30;
        data_art_iterator_t *it = NULL;
        uint8_t prefix[KEY_SIZE];
        size_t prefix_len = 0;

        if (prefix_scan) {
            // Prefix = first 1-2 bytes of a random key
            int key_index = (int)rng_range(&ctx->rng, 0, (uint32_t)(ctx->num_keys - 1));
            generate_key(prefix, key_index);
            prefix_len = rng_range(&ctx->rng, 1, 2);
            it = data_art_iterator_create_prefix(ctx->tree, prefix, prefix_len);
        } else {
            it = data_art_iterator_create(ctx->tree);
        }

        if (!it) continue;

        bool first = true;
        int count = 0;

        while (data_art_iterator_next(it) && !*ctx->stop && !g_failed) {
            size_t klen;
            const uint8_t *key = data_art_iterator_key(it, &klen);
            if (!key) break;

            STRESS_ASSERT(klen == KEY_SIZE,
                "iterator: unexpected key_len %zu (expected %d)", klen, KEY_SIZE);

            if (!first) {
                STRESS_ASSERT(memcmp(prev_key, key, KEY_SIZE) < 0,
                    "iterator: keys not sorted at entry %d", count);
            }

            if (prefix_scan) {
                STRESS_ASSERT(memcmp(key, prefix, prefix_len) == 0,
                    "iterator: prefix mismatch at entry %d", count);
            }

            memcpy(prev_key, key, KEY_SIZE);
            first = false;
            count++;
        }

        data_art_iterator_destroy(it);
        ctx->iterator_scans++;
        ctx->ops += (uint64_t)count;
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Thread: Checkpoint (1 thread)
//
// Periodically triggers checkpoint, racing with writer's periodic checkpoints.
// ---------------------------------------------------------------------------

static void *checkpoint_thread_fn(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        uint32_t delay_ms = rng_range(&ctx->rng, 200, 500);
        // Sleep in 50ms increments to check stop flag
        for (uint32_t elapsed = 0; elapsed < delay_ms && !*ctx->stop && !g_failed; elapsed += 50) {
            usleep(50000);
        }
        if (*ctx->stop || g_failed) break;

        data_art_checkpoint(ctx->tree, NULL);
        ctx->checkpoints++;
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Thread: Long Snapshot (1 thread)
//
// Holds a snapshot for ~2 seconds, reads a key at start and end, asserts
// identical results. Stresses version chain / GC retention.
// ---------------------------------------------------------------------------

static void *long_snapshot_thread_fn(void *arg) {
    thread_ctx_t *ctx = (thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        data_art_snapshot_t *snap = data_art_begin_snapshot(ctx->tree);
        if (!snap) continue;
        ctx->snapshots_taken++;

        int key_index = (int)rng_range(&ctx->rng, 0, (uint32_t)(ctx->num_keys - 1));
        uint8_t key[KEY_SIZE];
        generate_key(key, key_index);

        // Read at start
        size_t len1;
        const void *val1 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len1, snap);

        // Hold for ~2 seconds
        for (int ms = 0; ms < LONG_SNAPSHOT_HOLD_MS && !*ctx->stop && !g_failed; ms += 100) {
            usleep(100000);
        }

        // Read same key again — must be identical
        size_t len2;
        const void *val2 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len2, snap);

        if (val1 == NULL && val2 == NULL) {
            // consistent
        } else if (val1 != NULL && val2 != NULL) {
            STRESS_ASSERT(len1 == len2,
                "long-snapshot: key %d len mismatch %zu vs %zu", key_index, len1, len2);
            STRESS_ASSERT(memcmp(val1, val2, len1) == 0,
                "long-snapshot: key %d value changed during hold", key_index);
        } else {
            STRESS_ASSERT(false,
                "long-snapshot: key %d existence flipped during hold "
                "(read1=%s, read2=%s)",
                key_index,
                val1 ? (const char *)val1 : "NULL",
                val2 ? (const char *)val2 : "NULL");
        }

        free((void *)val1);
        free((void *)val2);
        ctx->isolation_checks++;

        data_art_end_snapshot(ctx->tree, snap);
    }

    return NULL;
}

// ---------------------------------------------------------------------------
// Verification helpers (called from main thread after all threads joined)
// ---------------------------------------------------------------------------

static bool verify_shadow_map(data_art_tree_t *tree, shadow_entry_t *shadow,
                              int num_keys, const char *phase) {
    int expected_count = 0;
    for (int i = 0; i < num_keys; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);

        size_t len;
        const void *val = data_art_get(tree, key, KEY_SIZE, &len);

        if (shadow[i].exists) {
            if (val == NULL) {
                fprintf(stderr, "VERIFY FAIL [%s]: key %d should exist but get returned NULL\n",
                        phase, i);
                return false;
            }
            if (len != shadow[i].value_len) {
                fprintf(stderr, "VERIFY FAIL [%s]: key %d len %zu != shadow %zu\n",
                        phase, i, len, shadow[i].value_len);
                free((void *)val);
                return false;
            }
            if (memcmp(val, shadow[i].value, len) != 0) {
                fprintf(stderr, "VERIFY FAIL [%s]: key %d value mismatch: got '%.*s' expected '%.*s'\n",
                        phase, i, (int)len, (const char *)val,
                        (int)shadow[i].value_len, shadow[i].value);
                free((void *)val);
                return false;
            }
            free((void *)val);
            expected_count++;
        } else {
            if (val != NULL) {
                fprintf(stderr, "VERIFY FAIL [%s]: key %d should NOT exist but got value '%.*s' (len=%zu)\n",
                        phase, i, (int)len, (const char *)val, len);
                free((void *)val);
                return false;
            }
        }
    }

    size_t tree_size = data_art_size(tree);
    if ((int)tree_size != expected_count) {
        fprintf(stderr, "VERIFY FAIL [%s]: tree size %zu != expected %d\n",
                phase, tree_size, expected_count);
        return false;
    }

    return true;
}

static bool verify_iterator_sorted(data_art_tree_t *tree, int expected_count,
                                   const char *phase) {
    data_art_iterator_t *it = data_art_iterator_create(tree);
    if (!it) {
        fprintf(stderr, "VERIFY FAIL [%s]: iterator_create returned NULL\n", phase);
        return false;
    }

    uint8_t prev_key[KEY_SIZE];
    bool first = true;
    int count = 0;

    while (data_art_iterator_next(it)) {
        size_t klen;
        const uint8_t *key = data_art_iterator_key(it, &klen);
        if (!key) break;

        if (klen != KEY_SIZE) {
            fprintf(stderr, "VERIFY FAIL [%s]: iterator key_len %zu at entry %d\n",
                    phase, klen, count);
            data_art_iterator_destroy(it);
            return false;
        }

        if (!first && memcmp(prev_key, key, KEY_SIZE) >= 0) {
            fprintf(stderr, "VERIFY FAIL [%s]: iterator not sorted at entry %d\n",
                    phase, count);
            data_art_iterator_destroy(it);
            return false;
        }

        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;
    }

    data_art_iterator_destroy(it);

    if (count != expected_count) {
        fprintf(stderr, "VERIFY FAIL [%s]: iterator count %d != expected %d\n",
                phase, count, expected_count);
        return false;
    }

    return true;
}

// ---------------------------------------------------------------------------
// Main — iteration loop
// ---------------------------------------------------------------------------

int main(int argc, char *argv[]) {
    log_set_level(LOG_LEVEL_ERROR);

    int iterations = DEFAULT_ITERATIONS;
    uint64_t user_seed = 0;
    bool seed_provided = false;

    if (argc > 1) {
        iterations = atoi(argv[1]);
        if (iterations <= 0) {
            fprintf(stderr, "Usage: %s [iterations] [seed]\n", argv[0]);
            return 1;
        }
    }
    if (argc > 2) {
        user_seed = strtoull(argv[2], NULL, 0);
        seed_provided = true;
        iterations = 1;
    }

    printf("=== Stress Loop Test ===\n");
    printf("  iterations:  %d\n", iterations);
    printf("  duration:    %d sec/iter\n", DEFAULT_DURATION_SEC);
    printf("  keys:        %d\n", DEFAULT_NUM_KEYS);
    printf("  readers:     %d\n", DEFAULT_NUM_READERS);
    printf("  iterators:   %d\n", DEFAULT_NUM_ITERATORS);
    printf("  seed:        %s\n\n", seed_provided ? "provided" : "random/iter");

    char dir_path[256];
    char art_path[256];
    snprintf(dir_path, sizeof(dir_path), "/tmp/test_stress_loop_%d", getpid());
    snprintf(art_path, sizeof(art_path), "%s/art.dat", dir_path);

    for (int iter = 0; iter < iterations && !g_failed; iter++) {
        // ----- 1. Seed -----
        uint64_t master_seed;
        if (seed_provided) {
            master_seed = user_seed;
        } else {
            master_seed = (uint64_t)time(NULL) ^ ((uint64_t)iter * 0x517cc1b727220a95ULL);
        }

        printf("=== Iteration %d/%d  seed=0x%016" PRIx64 " ===\n", iter + 1, iterations, master_seed);
        fflush(stdout);

        // ----- 2. Fresh tree -----
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir_path, dir_path);
        system(cmd);

        data_art_tree_t *tree = data_art_create(art_path, KEY_SIZE);
        if (!tree) {
            fprintf(stderr, "FAIL: data_art_create returned NULL\n");
            return 1;
        }

        // ----- 3. Shadow map -----
        shadow_entry_t *shadow = calloc(DEFAULT_NUM_KEYS, sizeof(shadow_entry_t));
        if (!shadow) {
            fprintf(stderr, "FAIL: calloc shadow map\n");
            return 1;
        }

        // ----- 4. Populate "block 0" -----
        {
            rng_t init_rng = rng_create(master_seed);
            int init_count = DEFAULT_NUM_KEYS / 2;

            uint64_t txn_id;
            if (!data_art_begin_txn(tree, &txn_id)) {
                fprintf(stderr, "FAIL: begin_txn for init\n");
                free(shadow);
                data_art_destroy(tree);
                return 1;
            }

            for (int i = 0; i < init_count; i++) {
                int ki = (int)rng_range(&init_rng, 0, DEFAULT_NUM_KEYS - 1);
                uint8_t key[KEY_SIZE];
                generate_key(key, ki);

                shadow[ki].version++;
                char value[VALUE_MAX_LEN];
                size_t vlen = generate_value(value, ki, shadow[ki].version);

                if (!data_art_insert(tree, key, KEY_SIZE, value, vlen)) {
                    fprintf(stderr, "FAIL: insert during init (key %d)\n", ki);
                    data_art_abort_txn(tree);
                    free(shadow);
                    data_art_destroy(tree);
                    return 1;
                }

                shadow[ki].exists = true;
                memcpy(shadow[ki].value, value, vlen);
                shadow[ki].value_len = vlen;
            }

            if (!data_art_commit_txn(tree)) {
                fprintf(stderr, "FAIL: commit init txn\n");
                free(shadow);
                data_art_destroy(tree);
                return 1;
            }

            data_art_checkpoint(tree, NULL);

            int init_existing = 0;
            for (int i = 0; i < DEFAULT_NUM_KEYS; i++) {
                if (shadow[i].exists) init_existing++;
            }
            printf("  Populated %d unique keys (from %d random inserts)\n", init_existing, init_count);
        }

        // ----- 5. Thread contexts -----
        volatile bool stop = false;

        // Total: 1 writer + NUM_READERS readers + NUM_ITERATORS iterators + 1 checkpoint + 1 long-snapshot
        int total_threads = 1 + DEFAULT_NUM_READERS + DEFAULT_NUM_ITERATORS + 1 + 1;
        thread_ctx_t *ctxs = calloc((size_t)total_threads, sizeof(thread_ctx_t));
        pthread_t *threads = calloc((size_t)total_threads, sizeof(pthread_t));
        int t = 0;

        // Helper to initialize common fields
        #define INIT_CTX(idx, name_str, seed_offset) do {       \
            ctxs[idx].tree = tree;                              \
            ctxs[idx].shadow = shadow;                          \
            ctxs[idx].num_keys = DEFAULT_NUM_KEYS;              \
            ctxs[idx].stop = &stop;                             \
            ctxs[idx].master_seed = master_seed;                \
            ctxs[idx].iteration = iter;                         \
            ctxs[idx].rng = rng_create(master_seed + (seed_offset)); \
            ctxs[idx].thread_id = idx;                          \
            snprintf(ctxs[idx].thread_name,                     \
                     sizeof(ctxs[idx].thread_name),             \
                     "%s", (name_str));                         \
        } while(0)

        // Writer
        INIT_CTX(t, "writer", 1);
        pthread_create(&threads[t], NULL, writer_thread_fn, &ctxs[t]);
        t++;

        // Readers
        for (int i = 0; i < DEFAULT_NUM_READERS; i++) {
            char name[32];
            snprintf(name, sizeof(name), "reader-%d", i);
            INIT_CTX(t, name, 100 + (uint64_t)i);
            pthread_create(&threads[t], NULL, reader_thread_fn, &ctxs[t]);
            t++;
        }

        // Iterators
        for (int i = 0; i < DEFAULT_NUM_ITERATORS; i++) {
            char name[32];
            snprintf(name, sizeof(name), "iter-%d", i);
            INIT_CTX(t, name, 200 + (uint64_t)i);
            pthread_create(&threads[t], NULL, iterator_thread_fn, &ctxs[t]);
            t++;
        }

        // Checkpoint
        INIT_CTX(t, "checkpoint", 300);
        pthread_create(&threads[t], NULL, checkpoint_thread_fn, &ctxs[t]);
        t++;

        // Long snapshot
        INIT_CTX(t, "long-snap", 400);
        pthread_create(&threads[t], NULL, long_snapshot_thread_fn, &ctxs[t]);
        t++;

        #undef INIT_CTX

        // ----- 6. Run stress phase -----
        for (int s = 0; s < DEFAULT_DURATION_SEC * 10 && !g_failed; s++) {
            usleep(100000);  // 100ms ticks
        }

        stop = true;

        // ----- 7. Join all threads -----
        for (int i = 0; i < total_threads; i++) {
            pthread_join(threads[i], NULL);
        }

        if (g_failed) {
            fprintf(stderr, "\nFAILED during stress phase\n");
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            data_art_destroy(tree);
            return 1;
        }

        // ----- 8. Aggregate stats -----
        uint64_t total_ops = 0, total_blocks = 0, total_aborted = 0;
        uint64_t total_snaps = 0, total_iso = 0, total_iterscans = 0, total_ckpts = 0;
        for (int i = 0; i < total_threads; i++) {
            total_ops += ctxs[i].ops;
            total_blocks += ctxs[i].blocks_committed;
            total_aborted += ctxs[i].blocks_aborted;
            total_snaps += ctxs[i].snapshots_taken;
            total_iso += ctxs[i].isolation_checks;
            total_iterscans += ctxs[i].iterator_scans;
            total_ckpts += ctxs[i].checkpoints;
        }

        // ----- 9. Post-stop verification -----
        printf("  Verifying final state...\n");

        // 9a. Checkpoint + flush
        data_art_checkpoint(tree, NULL);

        // 9b. Shadow map verification
        if (!verify_shadow_map(tree, shadow, DEFAULT_NUM_KEYS, "live")) {
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            data_art_destroy(tree);
            return 1;
        }

        // 9c. Iterator sorted order + count
        int expected_count = 0;
        for (int i = 0; i < DEFAULT_NUM_KEYS; i++) {
            if (shadow[i].exists) expected_count++;
        }

        if (!verify_iterator_sorted(tree, expected_count, "live")) {
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            data_art_destroy(tree);
            return 1;
        }

        // ----- 10. Persistence cycle: close → reopen → verify -----
        printf("  Persistence check: close → reopen → verify...\n");
        data_art_destroy(tree);
        tree = NULL;

        tree = data_art_open(art_path, KEY_SIZE);
        if (!tree) {
            fprintf(stderr, "FAIL: data_art_open returned NULL after close\n");
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            return 1;
        }

        if (!verify_shadow_map(tree, shadow, DEFAULT_NUM_KEYS, "reopen")) {
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            data_art_destroy(tree);
            return 1;
        }

        if (!verify_iterator_sorted(tree, expected_count, "reopen")) {
            fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], master_seed);
            free(ctxs);
            free(threads);
            free(shadow);
            data_art_destroy(tree);
            return 1;
        }

        // ----- 11. Cleanup -----
        data_art_destroy(tree);
        tree = NULL;

        printf("  PASS [iter=%d seed=0x%016" PRIx64 "] "
               "blocks=%"PRIu64"(+%"PRIu64" aborted) ops=%"PRIu64" "
               "snaps=%"PRIu64" iso_checks=%"PRIu64" "
               "iter_scans=%"PRIu64" ckpts=%"PRIu64" "
               "final_keys=%d\n",
               iter + 1, master_seed,
               total_blocks, total_aborted, total_ops,
               total_snaps, total_iso,
               total_iterscans, total_ckpts,
               expected_count);

        free(ctxs);
        free(threads);
        free(shadow);

        // Clean up test directory for next iteration
        snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
        system(cmd);
    }

    if (g_failed) {
        fprintf(stderr, "\nFAILED at iteration %d\n", g_fail_iter);
        fprintf(stderr, "Reproduce: %s 1 0x%016" PRIx64 "\n", argv[0], g_fail_seed);
        return 1;
    }

    printf("\nAll %d iterations PASSED\n", iterations);
    return 0;
}
