/*
 * LMDB Production-Scale Ethereum Block Processing Benchmark
 *
 * Identical workload to test_production_scale.c but targeting LMDB:
 *   - Same key generation (splitmix64 PRNG, 32-byte keys)
 *   - Same value generation (Ethereum distribution: 32B-24KB)
 *   - Same block structure (2000 inserts + 200 deletes)
 *   - Same checkpoint interval (every 100 blocks -> mdb_env_sync)
 *   - Same 4 reader threads with value verification
 *   - Same reporting format for direct comparison
 *
 * Usage:
 *   ./bench_lmdb_production                   # 10M keys, random seed
 *   ./bench_lmdb_production 300               # 300M keys, random seed
 *   ./bench_lmdb_production 500 0xABCD1234    # 500M keys, exact seed
 */

#include <lmdb.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <stdatomic.h>
#include <inttypes.h>
#include <sys/stat.h>

// ============================================================================
// Configuration — MUST match test_production_scale.c exactly
// ============================================================================

#define KEY_SIZE                  32
#define MAX_VALUE_SIZE            24576   // 24 KB (EIP-170 max contract size)
#define DEFAULT_TARGET_MILLIONS   10
#define BLOCK_SIZE                2000
#define DELETE_PER_BLOCK          200
#define CHECKPOINT_EVERY_BLOCKS   100
#define NUM_READERS               4
#define PROGRESS_EVERY_BLOCKS     500
#define READER_BATCH_SIZE         1000

// ============================================================================
// splitmix64 PRNG — identical to test_production_scale.c
// ============================================================================

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static inline rng_t rng_create(uint64_t seed) {
    rng_t r = { .state = seed };
    rng_next(&r);  // warm up
    return r;
}

// ============================================================================
// Key generation — identical to test_production_scale.c
// ============================================================================

static void generate_key(uint8_t key[KEY_SIZE], uint64_t master_seed, uint64_t index) {
    rng_t rng = rng_create(master_seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(key,      &r0, 8);
    memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8);
    memcpy(key + 24, &r3, 8);
}

// ============================================================================
// Value generation — identical to test_production_scale.c
// ============================================================================

static size_t generate_value(uint8_t *buf, uint64_t index) {
    rng_t rng = rng_create(index ^ 0x9E3779B97F4A7C15ULL);

    uint64_t bucket = rng_next(&rng) % 100;
    uint64_t size_rand = rng_next(&rng);
    size_t vlen;

    if (bucket < 60) {
        vlen = 32 + (size_rand % 33);
    } else if (bucket < 85) {
        vlen = 64 + (size_rand % 449);
    } else if (bucket < 95) {
        vlen = 512 + (size_rand % 3585);
    } else {
        vlen = 4096 + (size_rand % 20481);
    }

    for (size_t i = 0; i < vlen; i += 8) {
        uint64_t r = rng_next(&rng);
        size_t remain = vlen - i;
        size_t chunk = remain < 8 ? remain : 8;
        memcpy(buf + i, &r, chunk);
    }

    return vlen;
}

// ============================================================================
// Delete bitmap — identical to test_production_scale.c
// ============================================================================

static uint8_t *g_deleted_bitmap = NULL;

static inline void bitmap_set(uint8_t *bm, uint64_t idx) {
    bm[idx / 8] |= (uint8_t)(1 << (idx % 8));
}

static inline bool bitmap_test(const uint8_t *bm, uint64_t idx) {
    return (bm[idx / 8] & (1 << (idx % 8))) != 0;
}

// ============================================================================
// Shared atomic state
// ============================================================================

static _Atomic uint64_t g_committed_key_count = 0;
static _Atomic uint64_t g_total_deleted = 0;
static _Atomic bool g_error = false;
static _Atomic bool g_writer_done = false;
static _Atomic bool g_error_details_set = false;

static uint64_t g_error_seed;
static uint64_t g_error_key_index;
static char g_error_message[512];

#define FAIL_FAST(seed, key_idx, fmt, ...) do {                                 \
    bool expected = false;                                                       \
    if (atomic_compare_exchange_strong(&g_error_details_set, &expected, true)) { \
        g_error_seed = (seed);                                                   \
        g_error_key_index = (key_idx);                                           \
        snprintf(g_error_message, sizeof(g_error_message),                       \
                 fmt, ##__VA_ARGS__);                                            \
        fprintf(stderr,                                                          \
            "\nFAIL [seed=0x%016" PRIx64 " key_index=%" PRIu64 "] " fmt "\n",   \
            (uint64_t)(seed), (uint64_t)(key_idx), ##__VA_ARGS__);              \
        fflush(stderr);                                                          \
    }                                                                            \
    atomic_store_explicit(&g_error, true, memory_order_release);                 \
} while(0)

// ============================================================================
// LMDB reader context
// ============================================================================

typedef struct {
    MDB_env         *env;
    MDB_dbi          dbi;
    uint64_t         master_seed;
    int              thread_id;
    uint64_t         reads_ok;
    uint64_t         reads_verified;
    uint64_t         reads_not_found;
    uint64_t         reads_skipped_deleted;
} reader_ctx_t;

// ============================================================================
// Reader thread: continuous reads with value verification
// ============================================================================

static void *reader_thread_fn(void *arg) {
    reader_ctx_t *ctx = (reader_ctx_t *)arg;
    rng_t rng = rng_create(ctx->master_seed + 500 + (uint64_t)ctx->thread_id);

    uint8_t key_buf[KEY_SIZE];
    uint8_t expected_buf[MAX_VALUE_SIZE];

    while (!atomic_load_explicit(&g_error, memory_order_acquire) &&
           !atomic_load_explicit(&g_writer_done, memory_order_acquire)) {

        uint64_t committed = atomic_load_explicit(&g_committed_key_count,
                                                   memory_order_acquire);
        if (committed == 0) {
            usleep(1000);
            continue;
        }

        // Open read-only transaction for this batch
        MDB_txn *rtxn;
        int rc = mdb_txn_begin(ctx->env, NULL, MDB_RDONLY, &rtxn);
        if (rc != 0) {
            usleep(1000);
            continue;
        }

        int batch = READER_BATCH_SIZE;
        if ((uint64_t)batch > committed) batch = (int)committed;

        for (int i = 0; i < batch; i++) {
            if (atomic_load_explicit(&g_error, memory_order_acquire) ||
                atomic_load_explicit(&g_writer_done, memory_order_acquire))
                break;

            uint64_t key_index = rng_next(&rng) % committed;

            if (bitmap_test(g_deleted_bitmap, key_index)) {
                ctx->reads_skipped_deleted++;
                continue;
            }

            generate_key(key_buf, ctx->master_seed, key_index);
            size_t expected_len = generate_value(expected_buf, key_index);

            MDB_val mk = { .mv_size = KEY_SIZE, .mv_data = key_buf };
            MDB_val mv;
            rc = mdb_get(rtxn, ctx->dbi, &mk, &mv);

            if (rc == MDB_NOTFOUND) {
                // Key might have been deleted between bitmap check and read
                usleep(100);
                if (bitmap_test(g_deleted_bitmap, key_index)) {
                    ctx->reads_skipped_deleted++;
                    continue;
                }
                ctx->reads_not_found++;
                // Renew txn to get latest snapshot
                mdb_txn_reset(rtxn);
                mdb_txn_renew(rtxn);
                rc = mdb_get(rtxn, ctx->dbi, &mk, &mv);
                if (rc == MDB_NOTFOUND) {
                    if (bitmap_test(g_deleted_bitmap, key_index)) {
                        ctx->reads_skipped_deleted++;
                        continue;
                    }
                    FAIL_FAST(ctx->master_seed, key_index,
                              "reader-%d: key NOT FOUND (committed=%" PRIu64 ")",
                              ctx->thread_id, committed);
                    mdb_txn_abort(rtxn);
                    return NULL;
                }
            }

            if (rc != 0) {
                FAIL_FAST(ctx->master_seed, key_index,
                          "reader-%d: mdb_get error: %s",
                          ctx->thread_id, mdb_strerror(rc));
                mdb_txn_abort(rtxn);
                return NULL;
            }

            ctx->reads_ok++;

            if (mv.mv_size != expected_len) {
                FAIL_FAST(ctx->master_seed, key_index,
                          "reader-%d: value_len=%zu expected=%zu",
                          ctx->thread_id, mv.mv_size, expected_len);
                mdb_txn_abort(rtxn);
                return NULL;
            }

            if (memcmp(mv.mv_data, expected_buf, expected_len) != 0) {
                FAIL_FAST(ctx->master_seed, key_index,
                          "reader-%d: VALUE MISMATCH at key %" PRIu64,
                          ctx->thread_id, key_index);
                mdb_txn_abort(rtxn);
                return NULL;
            }

            ctx->reads_verified++;
        }

        mdb_txn_abort(rtxn);
    }

    return NULL;
}

// ============================================================================
// Helper: get LMDB actual used size (not mmap reservation)
// ============================================================================

static uint64_t lmdb_db_size(MDB_env *env) {
    MDB_envinfo info;
    MDB_stat mstat;
    mdb_env_info(env, &info);
    mdb_env_stat(env, &mstat);
    return (uint64_t)info.me_last_pgno * (uint64_t)mstat.ms_psize;
}

// ============================================================================
// Helper: format elapsed time
// ============================================================================

static void format_elapsed(double secs, char *buf, size_t bufsz) {
    int h = (int)(secs / 3600);
    int m = (int)((secs - h * 3600) / 60);
    int s = (int)(secs - h * 3600 - m * 60);
    if (h > 0)
        snprintf(buf, bufsz, "%dh%02dm%02ds", h, m, s);
    else if (m > 0)
        snprintf(buf, bufsz, "%dm%02ds", m, s);
    else
        snprintf(buf, bufsz, "%ds", s);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    // Parse CLI — identical to test_production_scale.c
    uint64_t target_millions = DEFAULT_TARGET_MILLIONS;
    uint64_t master_seed = 0;
    bool seed_provided = false;

    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) {
            fprintf(stderr, "Usage: %s [target_millions] [0xseed]\n", argv[0]);
            return 1;
        }
    }
    if (argc >= 3) {
        master_seed = strtoull(argv[2], NULL, 16);
        seed_provided = true;
    }
    if (!seed_provided) {
        master_seed = (uint64_t)time(NULL);
    }

    uint64_t target_keys = target_millions * 1000000ULL;

    // Allocate delete bitmap
    size_t bitmap_bytes = (target_keys + 7) / 8;
    g_deleted_bitmap = calloc(1, bitmap_bytes);
    if (!g_deleted_bitmap) {
        fprintf(stderr, "FAIL: cannot allocate delete bitmap (%zu MB)\n",
                bitmap_bytes / (1024 * 1024));
        return 1;
    }

    printf("\n");
    printf("=== LMDB Production-Scale Ethereum Block Test ===\n");
    printf("  target:     %" PRIu64 "M keys (%" PRIu64 " total inserts)\n",
           target_millions, target_keys);
    printf("  seed:       0x%016" PRIx64 "\n", master_seed);
    printf("  key_size:   %d bytes (keccak256 hash)\n", KEY_SIZE);
    printf("  values:     32B-24KB (variable, Ethereum distribution)\n");
    printf("  block_size: %d inserts + %d deletes/block\n", BLOCK_SIZE, DELETE_PER_BLOCK);
    printf("  checkpoint: every %d blocks (mdb_env_sync)\n", CHECKPOINT_EVERY_BLOCKS);
    printf("  readers:    %d threads (MDB_RDONLY transactions)\n", NUM_READERS);
    printf("  flags:      MDB_WRITEMAP | MDB_NOSYNC | MDB_MAPASYNC\n");
    printf("\n");
    fflush(stdout);

    // Create LMDB environment
    char dir_path[256];
    snprintf(dir_path, sizeof(dir_path), "/tmp/bench_lmdb_production_%d", getpid());

    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir_path, dir_path);
    system(cmd);

    MDB_env *env;
    int rc = mdb_env_create(&env);
    if (rc) {
        fprintf(stderr, "FAIL: mdb_env_create: %s\n", mdb_strerror(rc));
        free(g_deleted_bitmap);
        return 1;
    }

    // 200 GB map reservation — large enough for 500M keys with variable values
    mdb_env_set_mapsize(env, 200ULL * 1024 * 1024 * 1024);
    mdb_env_set_maxreaders(env, NUM_READERS + 4);

    // MDB_WRITEMAP: mmap writes (like ART), MDB_NOSYNC: no fsync per commit,
    // MDB_MAPASYNC: async msync (matches ART's sync_file_range hint)
    rc = mdb_env_open(env, dir_path, MDB_WRITEMAP | MDB_NOSYNC | MDB_MAPASYNC, 0664);
    if (rc) {
        fprintf(stderr, "FAIL: mdb_env_open: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        free(g_deleted_bitmap);
        return 1;
    }

    // Open the default unnamed database
    MDB_dbi dbi;
    {
        MDB_txn *txn;
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        if (rc) {
            fprintf(stderr, "FAIL: mdb_txn_begin (init): %s\n", mdb_strerror(rc));
            mdb_env_close(env);
            free(g_deleted_bitmap);
            return 1;
        }
        rc = mdb_dbi_open(txn, NULL, 0, &dbi);
        if (rc) {
            fprintf(stderr, "FAIL: mdb_dbi_open: %s\n", mdb_strerror(rc));
            mdb_txn_abort(txn);
            mdb_env_close(env);
            free(g_deleted_bitmap);
            return 1;
        }
        mdb_txn_commit(txn);
    }

    // Spawn reader threads
    reader_ctx_t reader_ctxs[NUM_READERS];
    pthread_t reader_threads[NUM_READERS];

    for (int i = 0; i < NUM_READERS; i++) {
        reader_ctxs[i] = (reader_ctx_t){
            .env = env,
            .dbi = dbi,
            .master_seed = master_seed,
            .thread_id = i,
        };
        pthread_create(&reader_threads[i], NULL, reader_thread_fn, &reader_ctxs[i]);
    }

    // Run writer on main thread
    struct timespec wall_start, now, last_progress;
    clock_gettime(CLOCK_MONOTONIC, &wall_start);
    last_progress = wall_start;

    uint8_t key_buf[KEY_SIZE];
    uint8_t value_buf[MAX_VALUE_SIZE];

    uint64_t keys_so_far = 0;
    uint64_t total_deleted = 0;
    uint64_t block_num = 0;
    uint64_t checkpoints_done = 0;
    double last_ckpt_ms = 0.0;
    double max_ckpt_ms = 0.0;
    double total_ckpt_ms = 0.0;

    while (keys_so_far < target_keys &&
           !atomic_load_explicit(&g_error, memory_order_acquire)) {

        uint64_t remaining = target_keys - keys_so_far;
        int this_block = (remaining < BLOCK_SIZE) ? (int)remaining : BLOCK_SIZE;

        // Begin write transaction (one per block, like ART)
        MDB_txn *txn;
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        if (rc) {
            FAIL_FAST(master_seed, keys_so_far,
                      "writer: mdb_txn_begin failed: %s", mdb_strerror(rc));
            break;
        }

        // Insert batch
        for (int i = 0; i < this_block; i++) {
            uint64_t key_index = keys_so_far + (uint64_t)i;

            generate_key(key_buf, master_seed, key_index);
            size_t vlen = generate_value(value_buf, key_index);

            MDB_val mk = { .mv_size = KEY_SIZE, .mv_data = key_buf };
            MDB_val mv = { .mv_size = vlen, .mv_data = value_buf };

            rc = mdb_put(txn, dbi, &mk, &mv, 0);
            if (rc) {
                FAIL_FAST(master_seed, key_index,
                          "writer: mdb_put failed: %s", mdb_strerror(rc));
                mdb_txn_abort(txn);
                goto writer_done;
            }
        }

        // Delete batch
        uint64_t keys_after_insert = keys_so_far + (uint64_t)this_block;
        int deletes_this_block = 0;

        if (keys_so_far >= BLOCK_SIZE * 2) {
            rng_t del_rng = rng_create(master_seed ^ (block_num * 0xA2F5C3D7E1B94068ULL));

            for (int d = 0; d < DELETE_PER_BLOCK; d++) {
                uint64_t del_index = rng_next(&del_rng) % keys_so_far;

                if (bitmap_test(g_deleted_bitmap, del_index))
                    continue;

                generate_key(key_buf, master_seed, del_index);

                MDB_val mk = { .mv_size = KEY_SIZE, .mv_data = key_buf };
                rc = mdb_del(txn, dbi, &mk, NULL);
                if (rc == 0) {
                    bitmap_set(g_deleted_bitmap, del_index);
                    deletes_this_block++;
                }
                // MDB_NOTFOUND is OK — same as ART returning false
            }
        }

        // Commit
        rc = mdb_txn_commit(txn);
        if (rc) {
            FAIL_FAST(master_seed, keys_so_far,
                      "writer: mdb_txn_commit failed: %s", mdb_strerror(rc));
            break;
        }

        // Advance watermark
        total_deleted += (uint64_t)deletes_this_block;
        keys_so_far = keys_after_insert;
        atomic_store_explicit(&g_total_deleted, total_deleted, memory_order_release);
        atomic_store_explicit(&g_committed_key_count, keys_so_far,
                              memory_order_release);
        block_num++;

        // Periodic checkpoint (sync)
        if (CHECKPOINT_EVERY_BLOCKS > 0 && block_num % CHECKPOINT_EVERY_BLOCKS == 0) {
            struct timespec ckpt_start, ckpt_end;
            clock_gettime(CLOCK_MONOTONIC, &ckpt_start);

            // Force sync — equivalent to ART's data_art_checkpoint
            mdb_env_sync(env, 1);

            clock_gettime(CLOCK_MONOTONIC, &ckpt_end);
            last_ckpt_ms = (ckpt_end.tv_sec - ckpt_start.tv_sec) * 1000.0
                         + (ckpt_end.tv_nsec - ckpt_start.tv_nsec) / 1e6;
            total_ckpt_ms += last_ckpt_ms;
            if (last_ckpt_ms > max_ckpt_ms) max_ckpt_ms = last_ckpt_ms;
            checkpoints_done++;
        }

        // Progress reporting — same format as test_production_scale.c
        if (block_num % PROGRESS_EVERY_BLOCKS == 0) {
            clock_gettime(CLOCK_MONOTONIC, &now);
            double elapsed = (now.tv_sec - wall_start.tv_sec)
                           + (now.tv_nsec - wall_start.tv_nsec) / 1e9;
            double interval = (now.tv_sec - last_progress.tv_sec)
                            + (now.tv_nsec - last_progress.tv_nsec) / 1e9;
            last_progress = now;

            double overall_kps = keys_so_far / elapsed / 1000.0;
            double interval_kps = ((uint64_t)PROGRESS_EVERY_BLOCKS * BLOCK_SIZE) / interval / 1000.0;

            uint64_t db_bytes = lmdb_db_size(env);
            double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);

            uint64_t live_keys = keys_so_far - total_deleted;

            uint64_t total_reads = 0;
            for (int i = 0; i < NUM_READERS; i++)
                total_reads += reader_ctxs[i].reads_verified;

            printf("  block %8" PRIu64 " | %7.1fM / %.0fM ins | "
                   "live %.1fM (del %.1fM) | "
                   "%.0f Kkeys/s (avg %.0f Kkeys/s) | DB %.2f GB | "
                   "reads %" PRIu64 " | ckpt %" PRIu64
                   " (last %.0fms, max %.0fms, avg %.0fms)\n",
                   block_num,
                   (double)keys_so_far / 1e6,
                   (double)target_keys / 1e6,
                   (double)live_keys / 1e6,
                   (double)total_deleted / 1e6,
                   interval_kps, overall_kps, db_gb,
                   total_reads, checkpoints_done,
                   last_ckpt_ms, max_ckpt_ms,
                   checkpoints_done > 0 ? total_ckpt_ms / checkpoints_done : 0.0);
            fflush(stdout);
        }
    }

writer_done:
    // Stop readers
    atomic_store_explicit(&g_writer_done, true, memory_order_release);
    for (int i = 0; i < NUM_READERS; i++)
        pthread_join(reader_threads[i], NULL);

    struct timespec wall_end;
    clock_gettime(CLOCK_MONOTONIC, &wall_end);
    double wall_secs = (wall_end.tv_sec - wall_start.tv_sec)
                     + (wall_end.tv_nsec - wall_start.tv_nsec) / 1e9;

    // Check for errors
    if (atomic_load(&g_error_details_set)) {
        fprintf(stderr, "\nFAILED -- seed: 0x%016" PRIx64
                " key_index: %" PRIu64 "\n  %s\n",
                g_error_seed, g_error_key_index, g_error_message);
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, master_seed);
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        free(g_deleted_bitmap);
        return 1;
    }

    // Aggregate reader stats
    uint64_t total_reads = 0, total_verified = 0, total_not_found = 0;
    uint64_t total_skipped_deleted = 0;
    for (int i = 0; i < NUM_READERS; i++) {
        total_reads += reader_ctxs[i].reads_ok;
        total_verified += reader_ctxs[i].reads_verified;
        total_not_found += reader_ctxs[i].reads_not_found;
        total_skipped_deleted += reader_ctxs[i].reads_skipped_deleted;
    }

    uint64_t live_keys = keys_so_far - total_deleted;
    uint64_t db_bytes = lmdb_db_size(env);
    double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);

    printf("\n  Writer done: %" PRIu64 " blocks, %" PRIu64 " inserts, "
           "%" PRIu64 " deletes, %" PRIu64 " live keys, "
           "%" PRIu64 " checkpoints\n",
           block_num, keys_so_far, total_deleted, live_keys, checkpoints_done);
    printf("  Readers: %" PRIu64 " reads, %" PRIu64 " verified, "
           "%" PRIu64 " skipped(deleted), %" PRIu64 " transient not-found\n",
           total_reads, total_verified, total_skipped_deleted, total_not_found);

    // Final verification: iterate all keys, check count matches
    printf("\n  Final verification: %" PRIu64 " keys...\n", live_keys);
    fflush(stdout);

    struct timespec vt0, vt1;
    clock_gettime(CLOCK_MONOTONIC, &vt0);

    MDB_txn *vtxn;
    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &vtxn);
    if (rc) {
        fprintf(stderr, "FAIL: verification txn_begin: %s\n", mdb_strerror(rc));
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        free(g_deleted_bitmap);
        return 1;
    }

    MDB_cursor *cursor;
    mdb_cursor_open(vtxn, dbi, &cursor);

    uint64_t count = 0;
    uint8_t prev_key[KEY_SIZE];
    bool first = true;
    MDB_val mk, mv;

    rc = mdb_cursor_get(cursor, &mk, &mv, MDB_FIRST);
    while (rc == 0) {
        if (mk.mv_size != KEY_SIZE) {
            fprintf(stderr, "FAIL: key_len %zu at entry %" PRIu64 "\n", mk.mv_size, count);
            mdb_cursor_close(cursor);
            mdb_txn_abort(vtxn);
            mdb_dbi_close(env, dbi);
            mdb_env_close(env);
            free(g_deleted_bitmap);
            return 1;
        }

        if (!first && memcmp(prev_key, mk.mv_data, KEY_SIZE) >= 0) {
            fprintf(stderr, "FAIL: keys not sorted at entry %" PRIu64 "\n", count);
            mdb_cursor_close(cursor);
            mdb_txn_abort(vtxn);
            mdb_dbi_close(env, dbi);
            mdb_env_close(env);
            free(g_deleted_bitmap);
            return 1;
        }

        memcpy(prev_key, mk.mv_data, KEY_SIZE);
        first = false;
        count++;

        if (count % 10000000 == 0) {
            printf("    ... iterated %" PRIu64 "M keys\n", count / 1000000);
            fflush(stdout);
        }

        rc = mdb_cursor_get(cursor, &mk, &mv, MDB_NEXT);
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(vtxn);

    if (count != live_keys) {
        fprintf(stderr, "FAIL: iterated %" PRIu64 " keys, expected %" PRIu64 "\n",
                count, live_keys);
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        free(g_deleted_bitmap);
        return 1;
    }

    clock_gettime(CLOCK_MONOTONIC, &vt1);
    double vt_secs = (vt1.tv_sec - vt0.tv_sec) + (vt1.tv_nsec - vt0.tv_nsec) / 1e9;

    printf("  Verification PASSED: %" PRIu64 " keys, sorted=OK (%.1fs)\n",
           count, vt_secs);

    // Summary
    char elapsed_str[32];
    format_elapsed(wall_secs, elapsed_str, sizeof(elapsed_str));

    printf("\n");
    printf("  ============================================\n");
    printf("  LMDB BENCHMARK COMPLETE\n");
    printf("  ============================================\n");
    printf("  inserts:     %" PRIu64 " (%.0fM)\n", keys_so_far, keys_so_far / 1e6);
    printf("  deletes:     %" PRIu64 " (%.0fM)\n", total_deleted, total_deleted / 1e6);
    printf("  live keys:   %" PRIu64 " (%.0fM)\n", live_keys, live_keys / 1e6);
    printf("  DB size:     %.2f GB\n", db_gb);
    printf("  wall time:   %s (%.0f Kkeys/sec inserts)\n", elapsed_str,
           (double)keys_so_far / wall_secs / 1000.0);
    printf("  reads:       %" PRIu64 " (%" PRIu64 " verified)\n",
           total_reads, total_verified);
    printf("  blocks:      %" PRIu64 "\n", block_num);
    printf("  checkpoints: %" PRIu64 "\n", checkpoints_done);
    printf("  seed:        0x%016" PRIx64 "\n", master_seed);
    printf("  DB path:     %s\n", dir_path);
    printf("  ============================================\n\n");

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    free(g_deleted_bitmap);
    return 0;
}
