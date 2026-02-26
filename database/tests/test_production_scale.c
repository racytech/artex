/*
 * Production-Scale Ethereum Block Processing Test
 *
 * Simulates real Ethereum workload:
 *   - 1 writer thread (block executor): batched inserts in transactions
 *   - 4 reader threads (RPC/websocket): zero-alloc reads with value verification
 *
 * Key properties:
 *   - 32-byte keys (keccak256 hash size)
 *   - Variable-length values (32 bytes to 24 KB, realistic Ethereum distribution)
 *   - No shadow map: verification via deterministic value regeneration
 *   - Scales to 300-500M keys
 *   - Seed-based reproducibility (splitmix64 PRNG)
 *
 * Usage:
 *   ./test_production_scale                   # 10M keys, random seed
 *   ./test_production_scale 300               # 300M keys, random seed
 *   ./test_production_scale 500 0xABCD1234    # 500M keys, exact seed
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
#include <sys/stat.h>

// ============================================================================
// Configuration
// ============================================================================

#define KEY_SIZE                  32
#define MAX_VALUE_SIZE            24576   // 24 KB (EIP-170 max contract size)
#define DEFAULT_TARGET_MILLIONS   10
#define BLOCK_SIZE                2000    // Inserts per "block" (Ethereum ~2000 txns/block)
#define CHECKPOINT_EVERY_BLOCKS   100
#define NUM_READERS               4
#define PROGRESS_EVERY_BLOCKS     500
#define READER_BATCH_SIZE         1000

// ============================================================================
// splitmix64 PRNG — deterministic, platform-independent
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
// Key generation: 32-byte key from master_seed + index
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
// Value generation: variable-length value from index alone
//
// Distribution (realistic Ethereum):
//   60%  small:   32 -   64 bytes  (storage slots, balances, nonces)
//   25%  medium:  64 -  512 bytes  (account state, receipts)
//   10%  larger: 512 - 4096 bytes  (larger state objects)
//    5%  large: 4096 -24576 bytes  (contract bytecodes)
//
// Returns value length. Depends only on index, not master_seed.
// ============================================================================

static size_t generate_value(uint8_t *buf, uint64_t index) {
    rng_t rng = rng_create(index ^ 0x9E3779B97F4A7C15ULL);

    // Determine size bucket
    uint64_t bucket = rng_next(&rng) % 100;
    uint64_t size_rand = rng_next(&rng);
    size_t vlen;

    if (bucket < 60) {
        // 60%: 32-64 bytes
        vlen = 32 + (size_rand % 33);
    } else if (bucket < 85) {
        // 25%: 64-512 bytes
        vlen = 64 + (size_rand % 449);
    } else if (bucket < 95) {
        // 10%: 512-4096 bytes
        vlen = 512 + (size_rand % 3585);
    } else {
        // 5%: 4096-24576 bytes
        vlen = 4096 + (size_rand % 20481);
    }

    // Fill value bytes
    for (size_t i = 0; i < vlen; i += 8) {
        uint64_t r = rng_next(&rng);
        size_t remain = vlen - i;
        size_t chunk = remain < 8 ? remain : 8;
        memcpy(buf + i, &r, chunk);
    }

    return vlen;
}

// ============================================================================
// FNV-1a hash — for final determinism verification
// ============================================================================

#define FNV_OFFSET_BASIS 0xcbf29ce484222325ULL

static inline uint64_t fnv1a_update(uint64_t hash, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

// ============================================================================
// Shared atomic state
// ============================================================================

static _Atomic uint64_t g_committed_key_count = 0;
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
// Thread contexts
// ============================================================================

typedef struct {
    data_art_tree_t *tree;
    uint64_t         master_seed;
    int              thread_id;
    uint64_t         reads_ok;
    uint64_t         reads_verified;
    uint64_t         reads_not_found;
} reader_ctx_t;

// ============================================================================
// Reader thread: continuous zero-alloc reads with value verification
// ============================================================================

static void *reader_thread_fn(void *arg) {
    reader_ctx_t *ctx = (reader_ctx_t *)arg;
    rng_t rng = rng_create(ctx->master_seed + 500 + (uint64_t)ctx->thread_id);

    uint8_t key_buf[KEY_SIZE];
    uint8_t value_buf[MAX_VALUE_SIZE];
    uint8_t expected_buf[MAX_VALUE_SIZE];

    while (!atomic_load_explicit(&g_error, memory_order_acquire) &&
           !atomic_load_explicit(&g_writer_done, memory_order_acquire)) {

        uint64_t committed = atomic_load_explicit(&g_committed_key_count,
                                                   memory_order_acquire);
        if (committed == 0) {
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

            generate_key(key_buf, ctx->master_seed, key_index);
            size_t expected_len = generate_value(expected_buf, key_index);

            size_t vlen = 0;
            bool found = data_art_get_into(ctx->tree, key_buf, KEY_SIZE,
                                            value_buf, MAX_VALUE_SIZE, &vlen);

            if (!found) {
                ctx->reads_not_found++;
                usleep(100);
                found = data_art_get_into(ctx->tree, key_buf, KEY_SIZE,
                                           value_buf, MAX_VALUE_SIZE, &vlen);
                if (!found) {
                    FAIL_FAST(ctx->master_seed, key_index,
                              "reader-%d: key NOT FOUND (committed=%" PRIu64 ")",
                              ctx->thread_id, committed);
                    return NULL;
                }
            }

            ctx->reads_ok++;

            if (vlen != expected_len) {
                FAIL_FAST(ctx->master_seed, key_index,
                          "reader-%d: value_len=%zu expected=%zu",
                          ctx->thread_id, vlen, expected_len);
                return NULL;
            }

            if (memcmp(value_buf, expected_buf, vlen) != 0) {
                FAIL_FAST(ctx->master_seed, key_index,
                          "reader-%d: VALUE MISMATCH at key %" PRIu64,
                          ctx->thread_id, key_index);
                return NULL;
            }

            ctx->reads_verified++;
        }
    }

    return NULL;
}

// ============================================================================
// Final verification: dual-pass FNV-1a determinism check
// ============================================================================

static bool run_final_verification(data_art_tree_t *tree, uint64_t expected_count) {
    printf("\n  Final verification: %" PRIu64 " keys...\n", expected_count);
    fflush(stdout);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    data_art_checkpoint(tree, NULL);

    size_t tree_size = data_art_size(tree);
    if (tree_size != expected_count) {
        fprintf(stderr, "FAIL: tree size %zu != expected %" PRIu64 "\n",
                tree_size, expected_count);
        return false;
    }

    // Pass 1: iterate all, verify sorted, compute FNV-1a
    data_art_iterator_t *it = data_art_iterator_create(tree);
    if (!it) {
        fprintf(stderr, "FAIL: iterator_create returned NULL\n");
        return false;
    }

    uint64_t count = 0;
    uint64_t hash = FNV_OFFSET_BASIS;
    uint8_t prev_key[KEY_SIZE];
    bool first = true;

    while (data_art_iterator_next(it)) {
        size_t klen;
        const uint8_t *key = data_art_iterator_key(it, &klen);
        if (!key) break;

        if (klen != KEY_SIZE) {
            fprintf(stderr, "FAIL: key_len %zu at entry %" PRIu64 "\n", klen, count);
            data_art_iterator_destroy(it);
            return false;
        }

        if (!first && memcmp(prev_key, key, KEY_SIZE) >= 0) {
            fprintf(stderr, "FAIL: keys not sorted at entry %" PRIu64 "\n", count);
            data_art_iterator_destroy(it);
            return false;
        }

        hash = fnv1a_update(hash, key, KEY_SIZE);
        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;

        if (count % 10000000 == 0) {
            printf("    ... iterated %" PRIu64 "M keys\n", count / 1000000);
            fflush(stdout);
        }
    }
    data_art_iterator_destroy(it);

    if (count != expected_count) {
        fprintf(stderr, "FAIL: iterated %" PRIu64 " keys, expected %" PRIu64 "\n",
                count, expected_count);
        return false;
    }

    // Pass 2: iterate again, verify identical hash (determinism proof)
    data_art_iterator_t *it2 = data_art_iterator_create(tree);
    if (!it2) {
        fprintf(stderr, "FAIL: iterator_create (pass 2) returned NULL\n");
        return false;
    }

    uint64_t count2 = 0;
    uint64_t hash2 = FNV_OFFSET_BASIS;

    while (data_art_iterator_next(it2)) {
        size_t klen;
        const uint8_t *key = data_art_iterator_key(it2, &klen);
        if (!key) break;
        hash2 = fnv1a_update(hash2, key, KEY_SIZE);
        count2++;
    }
    data_art_iterator_destroy(it2);

    if (hash != hash2 || count != count2) {
        fprintf(stderr, "FAIL: determinism violated! "
                "pass1: count=%" PRIu64 " hash=0x%016" PRIx64 " "
                "pass2: count=%" PRIu64 " hash=0x%016" PRIx64 "\n",
                count, hash, count2, hash2);
        return false;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("  Verification PASSED: %" PRIu64 " keys, hash=0x%016" PRIx64
           ", sorted=OK, deterministic=OK (%.1fs)\n", count, hash, elapsed);
    fflush(stdout);

    return true;
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
    log_set_level(LOG_LEVEL_ERROR);

    // Parse CLI
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

    printf("\n");
    printf("=== Production-Scale Ethereum Block Test ===\n");
    printf("  target:     %" PRIu64 "M keys (%" PRIu64 " total)\n",
           target_millions, target_keys);
    printf("  seed:       0x%016" PRIx64 "\n", master_seed);
    printf("  key_size:   %d bytes (keccak256 hash)\n", KEY_SIZE);
    printf("  values:     32B-24KB (variable, Ethereum distribution)\n");
    printf("  block_size: %d inserts/block\n", BLOCK_SIZE);
    printf("  checkpoint: every %d blocks (%d inserts)\n",
           CHECKPOINT_EVERY_BLOCKS, CHECKPOINT_EVERY_BLOCKS * BLOCK_SIZE);
    printf("  readers:    %d threads (zero-alloc data_art_get_into)\n", NUM_READERS);
    printf("\n");
    fflush(stdout);

    // Create tree
    char dir_path[256];
    snprintf(dir_path, sizeof(dir_path), "/tmp/test_production_scale_%d", getpid());
    char art_path[280];
    snprintf(art_path, sizeof(art_path), "%s/art.dat", dir_path);

    char cmd[600];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir_path, dir_path);
    system(cmd);

    data_art_tree_t *tree = data_art_create(art_path, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "FAIL: data_art_create returned NULL\n");
        return 1;
    }

    // Spawn reader threads
    reader_ctx_t reader_ctxs[NUM_READERS];
    pthread_t reader_threads[NUM_READERS];

    for (int i = 0; i < NUM_READERS; i++) {
        reader_ctxs[i] = (reader_ctx_t){
            .tree = tree,
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
    uint64_t block_num = 0;
    uint64_t checkpoints_done = 0;

    while (keys_so_far < target_keys &&
           !atomic_load_explicit(&g_error, memory_order_acquire)) {

        uint64_t remaining = target_keys - keys_so_far;
        int this_block = (remaining < BLOCK_SIZE) ? (int)remaining : BLOCK_SIZE;

        // Begin transaction
        uint64_t txn_id;
        if (!data_art_begin_txn(tree, &txn_id)) {
            FAIL_FAST(master_seed, keys_so_far,
                      "writer: begin_txn failed at block %" PRIu64, block_num);
            break;
        }

        // Insert batch
        for (int i = 0; i < this_block; i++) {
            uint64_t key_index = keys_so_far + (uint64_t)i;

            generate_key(key_buf, master_seed, key_index);
            size_t vlen = generate_value(value_buf, key_index);

            if (!data_art_insert(tree, key_buf, KEY_SIZE, value_buf, vlen)) {
                FAIL_FAST(master_seed, key_index,
                          "writer: insert failed at key %" PRIu64, key_index);
                data_art_abort_txn(tree);
                goto writer_done;
            }
        }

        // Commit
        if (!data_art_commit_txn(tree)) {
            FAIL_FAST(master_seed, keys_so_far,
                      "writer: commit_txn failed at block %" PRIu64, block_num);
            break;
        }

        // Advance watermark
        keys_so_far += (uint64_t)this_block;
        atomic_store_explicit(&g_committed_key_count, keys_so_far,
                              memory_order_release);
        block_num++;

        // Periodic checkpoint
        if (block_num % CHECKPOINT_EVERY_BLOCKS == 0) {
            data_art_checkpoint(tree, NULL);
            checkpoints_done++;
        }

        // Progress reporting
        if (block_num % PROGRESS_EVERY_BLOCKS == 0) {
            clock_gettime(CLOCK_MONOTONIC, &now);
            double elapsed = (now.tv_sec - wall_start.tv_sec)
                           + (now.tv_nsec - wall_start.tv_nsec) / 1e9;
            double interval = (now.tv_sec - last_progress.tv_sec)
                            + (now.tv_nsec - last_progress.tv_nsec) / 1e9;
            last_progress = now;

            double overall_kps = keys_so_far / elapsed / 1000.0;
            double interval_kps = ((uint64_t)PROGRESS_EVERY_BLOCKS * BLOCK_SIZE) / interval / 1000.0;

            uint64_t db_bytes = tree->mmap_storage->mapped_size;
            double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);

            // Aggregate reader stats for display
            uint64_t total_reads = 0;
            for (int i = 0; i < NUM_READERS; i++)
                total_reads += reader_ctxs[i].reads_verified;

            printf("  block %8" PRIu64 " | %7.1fM / %.0fM keys | "
                   "%.0f K/s (avg %.0f K/s) | DB %.2f GB | "
                   "reads %" PRIu64 " | ckpt %" PRIu64 "\n",
                   block_num,
                   (double)keys_so_far / 1e6,
                   (double)target_keys / 1e6,
                   interval_kps, overall_kps, db_gb,
                   total_reads, checkpoints_done);
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
        data_art_destroy(tree);
        return 1;
    }

    // Aggregate reader stats
    uint64_t total_reads = 0, total_verified = 0, total_not_found = 0;
    for (int i = 0; i < NUM_READERS; i++) {
        total_reads += reader_ctxs[i].reads_ok;
        total_verified += reader_ctxs[i].reads_verified;
        total_not_found += reader_ctxs[i].reads_not_found;
    }

    printf("\n  Writer done: %" PRIu64 " blocks, %" PRIu64 " keys, "
           "%" PRIu64 " checkpoints\n",
           block_num, keys_so_far, checkpoints_done);
    printf("  Readers: %" PRIu64 " reads, %" PRIu64 " verified, "
           "%" PRIu64 " transient not-found\n",
           total_reads, total_verified, total_not_found);

    // Final determinism verification
    if (!run_final_verification(tree, target_keys)) {
        fprintf(stderr, "\nFAILED final verification\n");
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, master_seed);
        data_art_destroy(tree);
        return 1;
    }

    // Persistence check: close -> reopen -> verify
    printf("\n  Persistence check: close -> reopen -> verify...\n");
    fflush(stdout);

    data_art_destroy(tree);
    tree = data_art_open(art_path, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "FAIL: data_art_open returned NULL after close\n");
        return 1;
    }

    if (!run_final_verification(tree, target_keys)) {
        fprintf(stderr, "\nFAILED post-reopen verification\n");
        fprintf(stderr, "Reproduce: %s %" PRIu64 " 0x%016" PRIx64 "\n",
                argv[0], target_millions, master_seed);
        data_art_destroy(tree);
        return 1;
    }

    // Summary
    char elapsed_str[32];
    format_elapsed(wall_secs, elapsed_str, sizeof(elapsed_str));

    uint64_t db_bytes = tree->mmap_storage->mapped_size;
    double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);

    printf("\n");
    printf("  ============================================\n");
    printf("  ALL CHECKS PASSED\n");
    printf("  ============================================\n");
    printf("  keys:        %" PRIu64 " (%.0fM)\n", target_keys, target_keys / 1e6);
    printf("  DB size:     %.2f GB\n", db_gb);
    printf("  wall time:   %s (%.0f keys/sec)\n", elapsed_str,
           (double)target_keys / wall_secs);
    printf("  reads:       %" PRIu64 " (%" PRIu64 " verified)\n",
           total_reads, total_verified);
    printf("  blocks:      %" PRIu64 "\n", block_num);
    printf("  checkpoints: %" PRIu64 "\n", checkpoints_done);
    printf("  seed:        0x%016" PRIx64 "\n", master_seed);
    printf("  DB path:     %s\n", dir_path);
    printf("  ============================================\n\n");

    data_art_destroy(tree);
    return 0;
}
