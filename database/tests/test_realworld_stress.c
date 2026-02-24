/*
 * Real-World Concurrent Stress Test
 *
 * Simulates Ethereum's access pattern:
 *   - 1 writer thread  (block executor)
 *   - 8 reader threads (JSON-RPC handlers)
 *   - 1 monitor thread (throughput + MVCC stats)
 *
 * Features:
 *   - Seed-based reproducibility (deterministic per-thread operation sequences)
 *   - Deletion tracking with atomics (readers distinguish expected NULL from bugs)
 *   - Snapshot isolation violation detection
 *   - SIGINT handler for graceful early stop
 *
 * Usage: ./test_realworld_stress [seconds] [seed]
 *   defaults: 60 seconds, seed = time(NULL)
 *
 * Pass criteria:
 *   - 0 isolation violations
 *   - Error rate < 0.1%
 */

#include "../include/data_art.h"
#include "../include/logger.h"
#include "../include/page_manager.h"
#include "../include/buffer_pool.h"
#include "../include/wal.h"
#include "../include/mvcc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <stdatomic.h>
#include <inttypes.h>

// ============================================================================
// Configuration
// ============================================================================

#define NUM_KEYS              10000
#define KEY_SIZE              20
#define NUM_READER_THREADS    8
#define DEFAULT_DURATION      60
#define STATS_INTERVAL        5
#define DELETE_PROBABILITY    30      // 30% of batch ops are deletes
#define BATCH_MIN             10
#define BATCH_MAX             50
#define READS_PER_SNAP_MIN   20
#define READS_PER_SNAP_MAX   50
#define PREPOPULATE_COUNT    (NUM_KEYS / 2)

#define TEST_DB_PATH   "test_realworld_stress.db"
#define TEST_WAL_PATH  "test_realworld_stress_wal"

// ============================================================================
// Shared State
// ============================================================================

typedef struct {
    data_art_tree_t *tree;
    volatile bool    stop;
    pthread_mutex_t  stats_lock;

    // Aggregate counters (merged by threads at exit)
    uint64_t total_reads;
    uint64_t total_writes;
    uint64_t total_deletes;
    uint64_t total_snapshots;
    uint64_t null_reads;              // Expected NULLs (deleted keys)
    uint64_t read_errors;             // Unexpected NULLs or failures
    uint64_t write_errors;
    uint64_t isolation_violations;

    // Cross-thread deletion tracking
    _Atomic bool *key_deleted;        // [NUM_KEYS]

    // Timing
    uint64_t start_time_usec;

    // Reproducibility
    unsigned int base_seed;
} test_state_t;

typedef struct {
    test_state_t *state;
    int           thread_id;          // 0..NUM_READER_THREADS-1
} reader_arg_t;

// ============================================================================
// SIGINT Handler
// ============================================================================

static volatile sig_atomic_t g_keep_running = 1;

static void sigint_handler(int signum) {
    (void)signum;
    g_keep_running = 0;
}

// ============================================================================
// Utilities
// ============================================================================

static void generate_key(uint8_t *key, int index) {
    memset(key, 0, KEY_SIZE);
    key[0] = (index >> 24) & 0xFF;
    key[1] = (index >> 16) & 0xFF;
    key[2] = (index >> 8)  & 0xFF;
    key[3] = index & 0xFF;
}

static void generate_value(char *value, size_t size, int key_index, int version) {
    snprintf(value, size, "key_%d_v%d", key_index, version);
}

static uint64_t get_time_usec(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

// ============================================================================
// Tree Setup
// ============================================================================

static data_art_tree_t *create_stress_tree(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", TEST_DB_PATH, TEST_WAL_PATH);
    system(cmd);

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t config = buffer_pool_default_config();
    config.capacity = 10000;
    buffer_pool_t *bp = buffer_pool_create(&config, pm);
    assert(bp != NULL);

    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = 4 * 1024 * 1024;
    wal_t *wal = wal_open(TEST_WAL_PATH, &wal_config);
    assert(wal != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);
    assert(tree != NULL);
    assert(tree->mvcc_manager != NULL);

    return tree;
}

static void prepopulate(data_art_tree_t *tree) {
    printf("Pre-populating %d keys...\n", PREPOPULATE_COUNT);

    uint64_t txn_id;
    assert(data_art_begin_txn(tree, &txn_id));

    for (int i = 0; i < PREPOPULATE_COUNT; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);

        char value[64];
        generate_value(value, sizeof(value), i, 0);

        if (!data_art_insert(tree, key, KEY_SIZE, value, strlen(value) + 1)) {
            fprintf(stderr, "Pre-populate failed at key %d\n", i);
            assert(false);
        }

        if ((i + 1) % 1000 == 0)
            printf("  ... %d/%d\n", i + 1, PREPOPULATE_COUNT);
    }

    assert(data_art_commit_txn(tree));
    printf("Pre-populated %d keys\n", PREPOPULATE_COUNT);
}

// ============================================================================
// Writer Thread
// ============================================================================

static void *writer_thread(void *arg) {
    test_state_t *state = (test_state_t *)arg;
    unsigned int seed = state->base_seed;
    uint64_t local_writes = 0;
    uint64_t local_deletes = 0;
    uint64_t local_errors = 0;
    int version = 1;

    int batch_keys[BATCH_MAX];
    bool batch_is_del[BATCH_MAX];

    while (!state->stop && g_keep_running) {
        uint64_t txn_id;
        if (!data_art_begin_txn(state->tree, &txn_id)) {
            local_errors++;
            usleep(1000);
            continue;
        }

        int batch_size = BATCH_MIN + (rand_r(&seed) % (BATCH_MAX - BATCH_MIN + 1));
        int batch_count = 0;
        bool success = true;

        for (int i = 0; i < batch_size && !state->stop; i++) {
            int key_index = rand_r(&seed) % NUM_KEYS;
            uint8_t key[KEY_SIZE];
            generate_key(key, key_index);

            bool is_delete = (rand_r(&seed) % 100) < DELETE_PROBABILITY;

            if (is_delete) {
                data_art_delete(state->tree, key, KEY_SIZE);
                local_deletes++;
            } else {
                char value[64];
                generate_value(value, sizeof(value), key_index, version++);
                if (!data_art_insert(state->tree, key, KEY_SIZE, value, strlen(value) + 1)) {
                    success = false;
                    break;
                }
                local_writes++;
            }

            batch_keys[batch_count] = key_index;
            batch_is_del[batch_count] = is_delete;
            batch_count++;
        }

        if (success) {
            if (data_art_commit_txn(state->tree)) {
                for (int i = 0; i < batch_count; i++) {
                    atomic_store_explicit(&state->key_deleted[batch_keys[i]],
                                          batch_is_del[i], memory_order_release);
                }
            } else {
                local_errors++;
            }
        } else {
            data_art_abort_txn(state->tree);
            local_errors++;
        }

        // Merge stats after each batch so monitor sees live numbers
        pthread_mutex_lock(&state->stats_lock);
        state->total_writes  += local_writes;
        state->total_deletes += local_deletes;
        state->write_errors  += local_errors;
        pthread_mutex_unlock(&state->stats_lock);
        local_writes = 0;
        local_deletes = 0;
        local_errors = 0;

        usleep(rand_r(&seed) % 1000);
    }

    return NULL;
}

// ============================================================================
// Reader Thread
// ============================================================================

static void *reader_thread(void *arg) {
    reader_arg_t *rarg = (reader_arg_t *)arg;
    test_state_t *state = rarg->state;
    unsigned int seed = state->base_seed + (unsigned int)(rarg->thread_id + 1);

    uint64_t local_reads = 0;
    uint64_t local_null_reads = 0;
    uint64_t local_errors = 0;
    uint64_t local_snapshots = 0;
    uint64_t local_violations = 0;

    while (!state->stop && g_keep_running) {
        data_art_snapshot_t *snapshot = data_art_begin_snapshot(state->tree);
        if (!snapshot) {
            local_errors++;
            usleep(100);
            continue;
        }
        local_snapshots++;

        int reads_in_snapshot = READS_PER_SNAP_MIN +
            (rand_r(&seed) % (READS_PER_SNAP_MAX - READS_PER_SNAP_MIN + 1));

        // Isolation tracking: first key read in this snapshot
        int tracked_key_index = -1;
        char *tracked_value = NULL;
        bool tracked_was_null = false;

        for (int i = 0; i < reads_in_snapshot && !state->stop; i++) {
            int key_index = rand_r(&seed) % NUM_KEYS;
            uint8_t key[KEY_SIZE];
            generate_key(key, key_index);

            size_t read_len;
            const void *read_val = data_art_get_snapshot(
                state->tree, key, KEY_SIZE, &read_len, snapshot);

            if (read_val) {
                local_reads++;

                // Isolation check
                if (key_index == tracked_key_index) {
                    if (tracked_was_null) {
                        fprintf(stderr,
                            "ISOLATION VIOLATION: key %d was NULL, now has value\n",
                            key_index);
                        local_violations++;
                    } else if (tracked_value &&
                               strcmp((const char *)read_val, tracked_value) != 0) {
                        fprintf(stderr,
                            "ISOLATION VIOLATION: key %d value changed within snapshot\n",
                            key_index);
                        local_violations++;
                    }
                } else if (tracked_key_index == -1) {
                    tracked_key_index = key_index;
                    tracked_value = strdup((const char *)read_val);
                    tracked_was_null = false;
                }

                free((void *)read_val);
            } else {
                bool was_deleted = atomic_load_explicit(
                    &state->key_deleted[key_index], memory_order_acquire);
                if (was_deleted) {
                    local_null_reads++;
                } else {
                    local_errors++;
                }

                // Isolation check
                if (key_index == tracked_key_index) {
                    if (!tracked_was_null) {
                        fprintf(stderr,
                            "ISOLATION VIOLATION: key %d had value, now NULL\n",
                            key_index);
                        local_violations++;
                    }
                } else if (tracked_key_index == -1) {
                    tracked_key_index = key_index;
                    tracked_was_null = true;
                }
            }
        }

        free(tracked_value);
        data_art_end_snapshot(state->tree, snapshot);

        // Merge stats after each snapshot so monitor sees live numbers
        pthread_mutex_lock(&state->stats_lock);
        state->total_reads          += local_reads;
        state->null_reads           += local_null_reads;
        state->read_errors          += local_errors;
        state->total_snapshots      += local_snapshots;
        state->isolation_violations += local_violations;
        pthread_mutex_unlock(&state->stats_lock);
        local_reads = 0;
        local_null_reads = 0;
        local_errors = 0;
        local_snapshots = 0;
        local_violations = 0;

        usleep(rand_r(&seed) % 500);
    }

    return NULL;
}

// ============================================================================
// Monitor Thread
// ============================================================================

static void *monitor_thread(void *arg) {
    test_state_t *state = (test_state_t *)arg;
    uint64_t last_reads = 0;
    uint64_t last_writes = 0;
    int elapsed = 0;

    while (!state->stop && g_keep_running) {
        sleep(STATS_INTERVAL);
        if (state->stop || !g_keep_running) break;
        elapsed += STATS_INTERVAL;

        pthread_mutex_lock(&state->stats_lock);
        uint64_t cur_reads   = state->total_reads;
        uint64_t cur_writes  = state->total_writes;
        uint64_t cur_deletes = state->total_deletes;
        uint64_t snapshots   = state->total_snapshots;
        uint64_t null_reads  = state->null_reads;
        uint64_t r_errors    = state->read_errors;
        uint64_t w_errors    = state->write_errors;
        uint64_t violations  = state->isolation_violations;
        pthread_mutex_unlock(&state->stats_lock);

        double read_tps  = (double)(cur_reads - last_reads) / STATS_INTERVAL;
        double write_tps = (double)(cur_writes - last_writes) / STATS_INTERVAL;

        uint64_t active_txns = state->tree->mvcc_manager->txn_map.active_count;
        uint64_t retired     = state->tree->mvcc_manager->gc.retired_count;
        uint64_t epoch       = state->tree->mvcc_manager->gc.global_epoch;

        printf("\n[%3ds] %.0f reads/s | %.0f writes/s\n", elapsed, read_tps, write_tps);
        printf("       ops: %" PRIu64 " reads, %" PRIu64 " null_reads, "
               "%" PRIu64 " writes, %" PRIu64 " deletes\n",
               cur_reads, null_reads, cur_writes, cur_deletes);
        printf("       snapshots: %" PRIu64 " | errors: %" PRIu64 " read, %" PRIu64 " write\n",
               snapshots, r_errors, w_errors);
        printf("       mvcc: %" PRIu64 " active, %" PRIu64 " retired, epoch=%" PRIu64 "\n",
               active_txns, retired, epoch);
        if (violations > 0)
            printf("       ISOLATION VIOLATIONS: %" PRIu64 "\n", violations);

        last_reads  = cur_reads;
        last_writes = cur_writes;
    }

    return NULL;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    log_set_level(LOG_LEVEL_ERROR);

    int duration = DEFAULT_DURATION;
    unsigned int base_seed = (unsigned int)time(NULL);

    if (argc > 1) {
        duration = atoi(argv[1]);
        if (duration <= 0) {
            fprintf(stderr, "Usage: %s [seconds] [seed]\n", argv[0]);
            return 1;
        }
    }
    if (argc > 2)
        base_seed = (unsigned int)atoi(argv[2]);

    signal(SIGINT, sigint_handler);

    printf("=== Real-World Stress Test ===\n");
    printf("  Duration:  %d seconds\n", duration);
    printf("  Seed:      %u\n", base_seed);
    printf("  Writer:    1 thread (batches %d-%d, %d%% deletes)\n",
           BATCH_MIN, BATCH_MAX, DELETE_PROBABILITY);
    printf("  Readers:   %d threads (%d-%d reads/snapshot)\n",
           NUM_READER_THREADS, READS_PER_SNAP_MIN, READS_PER_SNAP_MAX);
    printf("  Key space: %d keys, %d-byte, %d pre-populated\n",
           NUM_KEYS, KEY_SIZE, PREPOPULATE_COUNT);
    printf("\n");

    // Setup
    data_art_tree_t *tree = create_stress_tree();
    prepopulate(tree);

    _Atomic bool *key_deleted = calloc(NUM_KEYS, sizeof(_Atomic bool));
    assert(key_deleted != NULL);

    // Keys beyond PREPOPULATE_COUNT don't exist yet — NULL is expected
    for (int i = PREPOPULATE_COUNT; i < NUM_KEYS; i++)
        atomic_store(&key_deleted[i], true);

    test_state_t state = {
        .tree                = tree,
        .stop                = false,
        .total_reads         = 0,
        .total_writes        = 0,
        .total_deletes       = 0,
        .total_snapshots     = 0,
        .null_reads          = 0,
        .read_errors         = 0,
        .write_errors        = 0,
        .isolation_violations = 0,
        .key_deleted         = key_deleted,
        .start_time_usec     = get_time_usec(),
        .base_seed           = base_seed,
    };
    pthread_mutex_init(&state.stats_lock, NULL);

    // Spawn threads
    pthread_t writer_tid;
    pthread_t reader_tids[NUM_READER_THREADS];
    reader_arg_t reader_args[NUM_READER_THREADS];
    pthread_t monitor_tid;

    pthread_create(&writer_tid, NULL, writer_thread, &state);
    for (int i = 0; i < NUM_READER_THREADS; i++) {
        reader_args[i].state = &state;
        reader_args[i].thread_id = i;
        pthread_create(&reader_tids[i], NULL, reader_thread, &reader_args[i]);
    }
    pthread_create(&monitor_tid, NULL, monitor_thread, &state);

    printf("Running for %d seconds (Ctrl+C to stop early)...\n", duration);

    // Wait for duration, checking SIGINT every second
    for (int i = 0; i < duration && g_keep_running; i++)
        sleep(1);

    state.stop = true;
    printf("\nStopping threads...\n");

    pthread_join(writer_tid, NULL);
    for (int i = 0; i < NUM_READER_THREADS; i++)
        pthread_join(reader_tids[i], NULL);
    pthread_join(monitor_tid, NULL);

    // Final report
    uint64_t duration_usec = get_time_usec() - state.start_time_usec;
    double duration_sec = duration_usec / 1000000.0;
    uint64_t total_lookups = state.total_reads + state.null_reads;

    printf("\n");
    printf("===============================================================\n");
    printf("                     FINAL RESULTS\n");
    printf("===============================================================\n");
    printf("  Duration: %.1fs  |  Seed: %u\n", duration_sec, base_seed);
    printf("\n");
    printf("  Reads:     %" PRIu64 " (%.0f/s) — %" PRIu64 " value, %" PRIu64 " null (deleted)\n",
           total_lookups, total_lookups / duration_sec, state.total_reads, state.null_reads);
    printf("  Writes:    %" PRIu64 " (%.0f/s)\n",
           state.total_writes, state.total_writes / duration_sec);
    printf("  Deletes:   %" PRIu64 " (%.0f/s)\n",
           state.total_deletes, state.total_deletes / duration_sec);
    printf("  Snapshots: %" PRIu64 " (avg %.1f reads/snap)\n",
           state.total_snapshots,
           state.total_snapshots ? (double)total_lookups / state.total_snapshots : 0.0);
    printf("\n");
    printf("  Read errors:  %" PRIu64 " (%.3f%%)\n",
           state.read_errors,
           total_lookups + state.read_errors > 0
               ? 100.0 * state.read_errors / (total_lookups + state.read_errors) : 0.0);
    printf("  Write errors: %" PRIu64 " (%.3f%%)\n",
           state.write_errors,
           state.total_writes + state.write_errors > 0
               ? 100.0 * state.write_errors / (state.total_writes + state.write_errors) : 0.0);
    printf("\n");
    printf("  MVCC: %" PRIu64 " active, %" PRIu64 " retired, epoch=%" PRIu64 "\n",
           tree->mvcc_manager->txn_map.active_count,
           tree->mvcc_manager->gc.retired_count,
           tree->mvcc_manager->gc.global_epoch);
    printf("  Snapshots: %" PRIu64 " created, %" PRIu64 " released\n",
           tree->mvcc_manager->snapshots_created,
           tree->mvcc_manager->snapshots_released);

    // Pass/fail
    uint64_t total_ops = total_lookups + state.total_writes + state.total_deletes;
    uint64_t total_errors = state.read_errors + state.write_errors;
    double error_rate = (total_ops + total_errors) > 0
        ? 100.0 * total_errors / (total_ops + total_errors)
        : 0.0;

    printf("\n");
    printf("  Isolation violations: %" PRIu64 " %s\n",
           state.isolation_violations,
           state.isolation_violations == 0 ? "[PASS]" : "[FAIL]");
    printf("  Error rate: %.3f%% %s\n",
           error_rate,
           error_rate < 0.1 ? "[PASS]" : "[FAIL]");
    printf("===============================================================\n");

    // Cleanup
    pthread_mutex_destroy(&state.stats_lock);
    free(key_deleted);
    data_art_destroy(tree);
    system("rm -rf " TEST_DB_PATH " " TEST_WAL_PATH);

    // Assertions
    if (state.isolation_violations > 0) {
        fprintf(stderr, "FAILED: %" PRIu64 " isolation violations (seed=%u)\n",
                state.isolation_violations, base_seed);
        return 1;
    }
    if (error_rate >= 0.1) {
        fprintf(stderr, "FAILED: error rate %.3f%% >= 0.1%% (seed=%u)\n",
                error_rate, base_seed);
        return 1;
    }

    printf("\nPASSED (seed=%u)\n", base_seed);
    return 0;
}
