/*
 * Growing Database Stress Test
 *
 * Grows a single tree continuously to a target size (default 10 GB), with
 * concurrent readers, iterators, checkpoint, and long-snapshot threads running
 * throughout.  Every BATCH_SIZE inserts, the writer pauses to run a determinism
 * check: two full iterations must produce the same FNV-1a hash (proving
 * deterministic traversal), sorted order, and correct count.
 *
 * Usage:
 *   ./test_stress_grow                  # grow to 10 GB, random seed
 *   ./test_stress_grow 5                # grow to 5 GB
 *   ./test_stress_grow 10 0xABCD1234    # 10 GB with exact seed
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

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#define DEFAULT_TARGET_GB        10
#define KEY_SIZE                 32
#define VALUE_MAX_LEN            64
#define BATCH_SIZE               5000    // min determinism check interval (inserts)
#define BLOCK_OPS_MIN            50
#define BLOCK_OPS_MAX            200
#define CHECKPOINT_EVERY_BLOCKS  5
#define DELETE_PROBABILITY       30      // percent
#define ABORT_PROBABILITY        5       // percent
#define NUM_READERS              8
#define NUM_ITERATORS            2
#define MAX_KEYS                 (256ULL * 1024 * 1024)  // 256M keys max
#define VERIFY_SAMPLE            500     // keys to spot-check per determinism check
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

// ---------------------------------------------------------------------------
// Bitset — existence tracking (writer-only, no lock needed)
// ---------------------------------------------------------------------------

#define BITSET_WORDS  (MAX_KEYS / 64)

static inline bool bitset_get(const uint64_t *bs, uint64_t idx) {
    return (bs[idx / 64] >> (idx % 64)) & 1;
}
static inline void bitset_set(uint64_t *bs, uint64_t idx) {
    bs[idx / 64] |= (1ULL << (idx % 64));
}
static inline void bitset_clear(uint64_t *bs, uint64_t idx) {
    bs[idx / 64] &= ~(1ULL << (idx % 64));
}

// ---------------------------------------------------------------------------
// FNV-1a hash — for determinism verification
// ---------------------------------------------------------------------------

#define FNV_OFFSET_BASIS 0xcbf29ce484222325ULL

static inline uint64_t fnv1a_update(uint64_t hash, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash ^= data[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

// ---------------------------------------------------------------------------
// Per-thread context
// ---------------------------------------------------------------------------

typedef struct {
    data_art_tree_t    *tree;
    volatile bool      *stop;
    _Atomic uint64_t   *shared_next_key;  // writer's next_key_index, shared for readers
    uint64_t            master_seed;

    rng_t               rng;
    int                 thread_id;
    char                thread_name[32];

    // stats (thread-local, aggregated after join)
    uint64_t            ops;
    uint64_t            snapshots_taken;
    uint64_t            isolation_checks;
    uint64_t            iterator_scans;
    uint64_t            checkpoints;
} grow_thread_ctx_t;

// ---------------------------------------------------------------------------
// Fail-fast macro (for thread functions that return void*)
// ---------------------------------------------------------------------------

#define STRESS_ASSERT(cond, fmt, ...) do {                                      \
    if (!(cond)) {                                                              \
        fprintf(stderr,                                                         \
            "\nFAIL [seed=0x%016" PRIx64 " thread=%s] " fmt "\n",              \
            ctx->master_seed, ctx->thread_name, ##__VA_ARGS__);                 \
        fflush(stderr);                                                         \
        g_failed = true;                                                        \
        return NULL;                                                            \
    }                                                                           \
} while(0)

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

static size_t generate_value(char *buf, uint64_t key_index) {
    int n = snprintf(buf, VALUE_MAX_LEN, "v%" PRIu64, key_index);
    return (size_t)n + 1;  // include null terminator
}

// ---------------------------------------------------------------------------
// Thread: Reader (8 threads)
// ---------------------------------------------------------------------------

static void *reader_thread_fn(void *arg) {
    grow_thread_ctx_t *ctx = (grow_thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        uint64_t max_key = atomic_load_explicit(ctx->shared_next_key, memory_order_relaxed);
        if (max_key == 0) { usleep(1000); continue; }

        data_art_snapshot_t *snap = data_art_begin_snapshot(ctx->tree);
        if (!snap) continue;
        ctx->snapshots_taken++;

        int nreads = (int)rng_range(&ctx->rng, READS_PER_SNAPSHOT_MIN, READS_PER_SNAPSHOT_MAX);

        for (int i = 0; i < nreads && !*ctx->stop && !g_failed; i++) {
            uint64_t key_index = rng_next(&ctx->rng) % max_key;
            uint8_t key[KEY_SIZE];
            generate_key(key, key_index);

            size_t len1, len2;
            const void *val1 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len1, snap);
            const void *val2 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len2, snap);

            if (val1 == NULL && val2 == NULL) {
                // consistent
            } else if (val1 != NULL && val2 != NULL) {
                STRESS_ASSERT(len1 == len2,
                    "isolation: key %" PRIu64 " len mismatch %zu vs %zu", key_index, len1, len2);
                STRESS_ASSERT(memcmp(val1, val2, len1) == 0,
                    "isolation: key %" PRIu64 " value changed within snapshot", key_index);
            } else {
                STRESS_ASSERT(false,
                    "isolation: key %" PRIu64 " existence flipped (val1=%p val2=%p)",
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
// ---------------------------------------------------------------------------

static void *iterator_thread_fn(void *arg) {
    grow_thread_ctx_t *ctx = (grow_thread_ctx_t *)arg;
    uint8_t prev_key[KEY_SIZE];

    while (!*ctx->stop && !g_failed) {
        bool prefix_scan = rng_range(&ctx->rng, 0, 99) < 30;
        data_art_iterator_t *it = NULL;
        uint8_t prefix[KEY_SIZE];
        size_t prefix_len = 0;

        uint64_t max_key = atomic_load_explicit(ctx->shared_next_key, memory_order_relaxed);
        if (max_key == 0) { usleep(1000); continue; }

        if (prefix_scan) {
            uint64_t key_index = rng_next(&ctx->rng) % max_key;
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
// ---------------------------------------------------------------------------

static void *checkpoint_thread_fn(void *arg) {
    grow_thread_ctx_t *ctx = (grow_thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        uint32_t delay_ms = rng_range(&ctx->rng, 200, 500);
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
// ---------------------------------------------------------------------------

static void *long_snapshot_thread_fn(void *arg) {
    grow_thread_ctx_t *ctx = (grow_thread_ctx_t *)arg;

    while (!*ctx->stop && !g_failed) {
        uint64_t max_key = atomic_load_explicit(ctx->shared_next_key, memory_order_relaxed);
        if (max_key == 0) { usleep(1000); continue; }

        data_art_snapshot_t *snap = data_art_begin_snapshot(ctx->tree);
        if (!snap) continue;
        ctx->snapshots_taken++;

        uint64_t key_index = rng_next(&ctx->rng) % max_key;
        uint8_t key[KEY_SIZE];
        generate_key(key, key_index);

        size_t len1;
        const void *val1 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len1, snap);

        for (int ms = 0; ms < LONG_SNAPSHOT_HOLD_MS && !*ctx->stop && !g_failed; ms += 100) {
            usleep(100000);
        }

        size_t len2;
        const void *val2 = data_art_get_snapshot(ctx->tree, key, KEY_SIZE, &len2, snap);

        if (val1 == NULL && val2 == NULL) {
            // consistent
        } else if (val1 != NULL && val2 != NULL) {
            STRESS_ASSERT(len1 == len2,
                "long-snapshot: key %" PRIu64 " len mismatch %zu vs %zu", key_index, len1, len2);
            STRESS_ASSERT(memcmp(val1, val2, len1) == 0,
                "long-snapshot: key %" PRIu64 " value changed during hold", key_index);
        } else {
            STRESS_ASSERT(false,
                "long-snapshot: key %" PRIu64 " existence flipped during hold "
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
// Determinism check — called from main/writer thread
// ---------------------------------------------------------------------------

static bool run_determinism_check(data_art_tree_t *tree, uint64_t live_count,
                                   int check_num, uint64_t target_bytes,
                                   uint64_t master_seed) {
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    data_art_checkpoint(tree, NULL);

    // Pass 1: iterate, verify sorted order, compute hash, count entries
    data_art_iterator_t *it = data_art_iterator_create(tree);
    if (!it) {
        fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: iterator_create returned NULL\n",
                master_seed, check_num);
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
            fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: key_len %zu at entry %" PRIu64 "\n",
                    master_seed, check_num, klen, count);
            data_art_iterator_destroy(it);
            return false;
        }

        if (!first && memcmp(prev_key, key, KEY_SIZE) >= 0) {
            fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: not sorted at entry %" PRIu64 "\n",
                    master_seed, check_num, count);
            data_art_iterator_destroy(it);
            return false;
        }

        hash = fnv1a_update(hash, key, KEY_SIZE);
        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;
    }
    data_art_iterator_destroy(it);

    // Verify count
    if (count != live_count) {
        fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: iter count %" PRIu64
                " != live_count %" PRIu64 "\n", master_seed, check_num, count, live_count);
        return false;
    }

    size_t tree_size = data_art_size(tree);
    if (tree_size != live_count) {
        fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: tree size %zu != live_count %" PRIu64 "\n",
                master_seed, check_num, tree_size, live_count);
        return false;
    }

    // Pass 2: iterate again, verify identical hash (determinism proof)
    data_art_iterator_t *it2 = data_art_iterator_create(tree);
    if (!it2) {
        fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: iterator_create (pass 2) returned NULL\n",
                master_seed, check_num);
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
        fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: determinism violated! "
                "pass1: count=%" PRIu64 " hash=0x%016" PRIx64 " "
                "pass2: count=%" PRIu64 " hash=0x%016" PRIx64 "\n",
                master_seed, check_num, count, hash, count2, hash2);
        return false;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

    uint64_t db_bytes = tree->mmap_storage->mapped_size;
    double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);
    double target_gb = (double)target_bytes / (1024.0 * 1024.0 * 1024.0);

    printf("  CHECK %4d | keys: %12" PRIu64 " | DB: %5.1f GB / %.1f GB | hash: 0x%016" PRIx64 "  (%.1fs)\n",
           check_num, count, db_gb, target_gb, hash, elapsed);
    fflush(stdout);

    return true;
}

// ---------------------------------------------------------------------------
// Helper: pick random existing key from bitset
// ---------------------------------------------------------------------------

static uint64_t pick_random_existing(rng_t *rng, const uint64_t *bitset, uint64_t max_index) {
    for (int attempt = 0; attempt < 20; attempt++) {
        uint64_t idx = rng_next(rng) % max_index;
        if (bitset_get(bitset, idx)) return idx;
    }
    // Linear scan fallback (rare)
    uint64_t start = rng_next(rng) % max_index;
    for (uint64_t i = 0; i < max_index; i++) {
        uint64_t idx = (start + i) % max_index;
        if (bitset_get(bitset, idx)) return idx;
    }
    return UINT64_MAX;  // no existing keys found
}

// ---------------------------------------------------------------------------
// Helper: adaptive check interval
// ---------------------------------------------------------------------------

static uint64_t check_interval(uint64_t live_count) {
    if (live_count < 1000000)  return BATCH_SIZE;          // 5K
    if (live_count < 10000000) return BATCH_SIZE * 10;     // 50K
    return BATCH_SIZE * 100;                               // 500K
}

// ---------------------------------------------------------------------------
// Helper: format elapsed time
// ---------------------------------------------------------------------------

static void format_elapsed(double secs, char *buf, size_t bufsz) {
    int h = (int)(secs / 3600);
    int m = (int)((secs - h * 3600) / 60);
    int s = (int)(secs - h * 3600 - m * 60);
    if (h > 0) {
        snprintf(buf, bufsz, "%dh%02dm%02ds", h, m, s);
    } else if (m > 0) {
        snprintf(buf, bufsz, "%dm%02ds", m, s);
    } else {
        snprintf(buf, bufsz, "%ds", s);
    }
}

// ---------------------------------------------------------------------------
// Main — writer + orchestration
// ---------------------------------------------------------------------------

typedef struct {
    uint64_t key_index;
    bool     is_delete;
} grow_pending_op_t;

int main(int argc, char *argv[]) {
    // ----- Parse CLI -----
    double target_gb = DEFAULT_TARGET_GB;
    uint64_t master_seed = 0;
    bool seed_provided = false;

    if (argc >= 2) {
        target_gb = atof(argv[1]);
        if (target_gb <= 0) {
            fprintf(stderr, "Usage: %s [target_gb] [seed]\n", argv[0]);
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

    uint64_t target_bytes = (uint64_t)(target_gb * 1024.0 * 1024.0 * 1024.0);

    printf("=== Growing Stress Test ===\n");
    printf("  target:   %.1f GB\n", target_gb);
    printf("  seed:     0x%016" PRIx64 "\n", master_seed);
    printf("  key_size: %d\n", KEY_SIZE);
    printf("  batch:    %d inserts (adaptive scaling)\n", BATCH_SIZE);
    printf("  readers:  %d\n", NUM_READERS);
    printf("  iterators: %d\n", NUM_ITERATORS);
    printf("\n");
    fflush(stdout);

    // ----- Create tree -----
    char dir_path[256];
    snprintf(dir_path, sizeof(dir_path), "/tmp/test_stress_grow_%d", getpid());
    char art_path[256];
    snprintf(art_path, sizeof(art_path), "%s/art.dat", dir_path);

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir_path, dir_path);
    system(cmd);

    data_art_tree_t *tree = data_art_create(art_path, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "FAIL: data_art_create returned NULL\n");
        return 1;
    }

    // ----- Allocate bitset -----
    uint64_t *bitset = calloc(BITSET_WORDS, sizeof(uint64_t));
    if (!bitset) {
        fprintf(stderr, "FAIL: calloc bitset (%" PRIu64 " bytes)\n",
                (uint64_t)BITSET_WORDS * 8);
        data_art_destroy(tree);
        return 1;
    }

    // ----- Writer state -----
    _Atomic uint64_t shared_next_key = 0;
    uint64_t next_key_index = 0;
    uint64_t live_count = 0;
    uint64_t total_inserts = 0;
    uint64_t total_deletes = 0;
    uint64_t total_blocks = 0;
    uint64_t total_aborted = 0;
    int check_num = 0;
    rng_t writer_rng = rng_create(master_seed + 1);

    // ----- Spawn background threads -----
    volatile bool stop = false;
    int total_threads = NUM_READERS + NUM_ITERATORS + 1 + 1;  // + checkpoint + long-snap
    grow_thread_ctx_t *ctxs = calloc((size_t)total_threads, sizeof(grow_thread_ctx_t));
    pthread_t *threads = calloc((size_t)total_threads, sizeof(pthread_t));
    int t = 0;

    #define INIT_CTX(idx, name_str, seed_offset) do {           \
        ctxs[idx].tree = tree;                                  \
        ctxs[idx].stop = &stop;                                 \
        ctxs[idx].shared_next_key = &shared_next_key;           \
        ctxs[idx].master_seed = master_seed;                    \
        ctxs[idx].rng = rng_create(master_seed + (seed_offset));\
        ctxs[idx].thread_id = idx;                              \
        snprintf(ctxs[idx].thread_name,                         \
                 sizeof(ctxs[idx].thread_name),                 \
                 "%s", (name_str));                              \
    } while(0)

    for (int i = 0; i < NUM_READERS; i++) {
        char name[32];
        snprintf(name, sizeof(name), "reader-%d", i);
        INIT_CTX(t, name, 100 + (uint64_t)i);
        pthread_create(&threads[t], NULL, reader_thread_fn, &ctxs[t]);
        t++;
    }

    for (int i = 0; i < NUM_ITERATORS; i++) {
        char name[32];
        snprintf(name, sizeof(name), "iter-%d", i);
        INIT_CTX(t, name, 200 + (uint64_t)i);
        pthread_create(&threads[t], NULL, iterator_thread_fn, &ctxs[t]);
        t++;
    }

    INIT_CTX(t, "checkpoint", 300);
    pthread_create(&threads[t], NULL, checkpoint_thread_fn, &ctxs[t]);
    t++;

    INIT_CTX(t, "long-snap", 400);
    pthread_create(&threads[t], NULL, long_snapshot_thread_fn, &ctxs[t]);
    t++;

    #undef INIT_CTX

    struct timespec start_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);

    // ----- Writer loop (main thread) -----
    uint64_t next_check_at = BATCH_SIZE;
    grow_pending_op_t pending[BLOCK_OPS_MAX];

    while (!g_failed) {
        int block_size = (int)rng_range(&writer_rng, BLOCK_OPS_MIN, BLOCK_OPS_MAX);
        bool should_abort = rng_range(&writer_rng, 0, 99) < ABORT_PROBABILITY;

        uint64_t txn_id;
        if (!data_art_begin_txn(tree, &txn_id)) {
            usleep(100);
            continue;
        }

        int pending_count = 0;
        bool success = true;
        uint64_t batch_inserts = 0;

        for (int i = 0; i < block_size && !g_failed; i++) {
            bool is_delete = rng_range(&writer_rng, 0, 99) < DELETE_PROBABILITY
                             && live_count > 0 && next_key_index > 0;

            if (is_delete) {
                uint64_t del_idx = pick_random_existing(&writer_rng, bitset, next_key_index);
                if (del_idx == UINT64_MAX) {
                    // No existing key found, do insert instead
                    is_delete = false;
                } else {
                    // Check for duplicate delete in this block
                    bool dup = false;
                    for (int j = 0; j < pending_count; j++) {
                        if (pending[j].key_index == del_idx && pending[j].is_delete) {
                            dup = true;
                            break;
                        }
                    }
                    if (dup) {
                        is_delete = false;  // fall through to insert
                    } else {
                        uint8_t key[KEY_SIZE];
                        generate_key(key, del_idx);
                        data_art_delete(tree, key, KEY_SIZE);
                        pending[pending_count].key_index = del_idx;
                        pending[pending_count].is_delete = true;
                        pending_count++;
                        continue;
                    }
                }
            }

            if (!is_delete) {
                if (next_key_index >= MAX_KEYS) {
                    fprintf(stderr, "FAIL: max key index reached (%" PRIu64 ")\n", next_key_index);
                    g_failed = true;
                    break;
                }

                uint64_t ki = next_key_index++;
                atomic_store_explicit(&shared_next_key, next_key_index, memory_order_relaxed);

                uint8_t key[KEY_SIZE];
                generate_key(key, ki);
                char value[VALUE_MAX_LEN];
                size_t vlen = generate_value(value, ki);

                if (!data_art_insert(tree, key, KEY_SIZE, value, vlen)) {
                    success = false;
                    break;
                }

                pending[pending_count].key_index = ki;
                pending[pending_count].is_delete = false;
                pending_count++;
                batch_inserts++;
            }
        }

        if (g_failed) {
            data_art_abort_txn(tree);
            break;
        }

        if (should_abort || !success) {
            data_art_abort_txn(tree);
            // Undo next_key_index for freshly allocated indices
            // (simpler: just let them be wasted — bitset won't mark them)
            total_aborted++;
            continue;
        }

        if (!data_art_commit_txn(tree)) {
            fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] commit_txn failed at block %" PRIu64 "\n",
                    master_seed, total_blocks);
            g_failed = true;
            break;
        }

        // Apply pending ops to bitset
        for (int i = 0; i < pending_count; i++) {
            if (pending[i].is_delete) {
                bitset_clear(bitset, pending[i].key_index);
                live_count--;
                total_deletes++;
            } else {
                bitset_set(bitset, pending[i].key_index);
                live_count++;
                total_inserts++;
            }
        }

        total_blocks++;

        // Periodic checkpoint
        if (total_blocks % CHECKPOINT_EVERY_BLOCKS == 0) {
            data_art_checkpoint(tree, NULL);
        }

        // Determinism check at adaptive intervals
        if (total_inserts >= next_check_at) {
            check_num++;
            if (!run_determinism_check(tree, live_count, check_num, target_bytes, master_seed)) {
                g_failed = true;
                break;
            }
            next_check_at = total_inserts + check_interval(live_count);

            // Spot-check: verify random existing keys return non-NULL
            int spot_checked = 0;
            rng_t spot_rng = rng_create(master_seed + check_num * 1000ULL);
            for (int s = 0; s < VERIFY_SAMPLE && next_key_index > 0; s++) {
                uint64_t idx = pick_random_existing(&spot_rng, bitset, next_key_index);
                if (idx == UINT64_MAX) break;
                uint8_t key[KEY_SIZE];
                generate_key(key, idx);
                size_t len;
                const void *val = data_art_get(tree, key, KEY_SIZE, &len);
                if (!val) {
                    fprintf(stderr, "\nFAIL [seed=0x%016" PRIx64 "] CHECK %d: "
                            "spot-check key %" PRIu64 " should exist but got NULL\n",
                            master_seed, check_num, idx);
                    g_failed = true;
                    break;
                }
                free((void *)val);
                spot_checked++;
            }
            if (g_failed) break;

            // Check DB size
            uint64_t db_bytes = tree->mmap_storage->mapped_size;
            if (db_bytes >= target_bytes) {
                printf("\n  Target size reached (%.1f GB >= %.1f GB)\n\n",
                       (double)db_bytes / (1024.0*1024.0*1024.0), target_gb);
                break;
            }
        }
    }

    // ----- Stop background threads -----
    stop = true;
    for (int i = 0; i < total_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    if (g_failed) {
        fprintf(stderr, "\nFAILED — reproduce: %s %.1f 0x%016" PRIx64 "\n",
                argv[0], target_gb, master_seed);
        free(ctxs);
        free(threads);
        free(bitset);
        data_art_destroy(tree);
        snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
        system(cmd);
        return 1;
    }

    // ----- Aggregate stats -----
    uint64_t total_snaps = 0, total_iso = 0, total_iterscans = 0, total_ckpts = 0;
    uint64_t total_ops = 0;
    for (int i = 0; i < total_threads; i++) {
        total_ops += ctxs[i].ops;
        total_snaps += ctxs[i].snapshots_taken;
        total_iso += ctxs[i].isolation_checks;
        total_iterscans += ctxs[i].iterator_scans;
        total_ckpts += ctxs[i].checkpoints;
    }

    // ----- Final verification -----
    printf("  Final verification...\n");
    fflush(stdout);

    data_art_checkpoint(tree, NULL);

    // Final determinism check
    check_num++;
    if (!run_determinism_check(tree, live_count, check_num, target_bytes, master_seed)) {
        fprintf(stderr, "FAILED final determinism check\n");
        free(ctxs); free(threads); free(bitset);
        data_art_destroy(tree);
        return 1;
    }

    // ----- Persistence check -----
    printf("  Persistence check: close → reopen → verify...\n");
    fflush(stdout);

    data_art_destroy(tree);
    tree = NULL;

    tree = data_art_open(art_path, KEY_SIZE);
    if (!tree) {
        fprintf(stderr, "FAIL: data_art_open returned NULL after close\n");
        free(ctxs); free(threads); free(bitset);
        return 1;
    }

    // Verify after reopen
    check_num++;
    if (!run_determinism_check(tree, live_count, check_num, target_bytes, master_seed)) {
        fprintf(stderr, "FAILED post-reopen determinism check\n");
        free(ctxs); free(threads); free(bitset);
        data_art_destroy(tree);
        return 1;
    }

    // ----- Summary -----
    struct timespec end_time;
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    double total_secs = (end_time.tv_sec - start_time.tv_sec)
                      + (end_time.tv_nsec - start_time.tv_nsec) / 1e9;
    char elapsed_str[32];
    format_elapsed(total_secs, elapsed_str, sizeof(elapsed_str));

    uint64_t db_bytes = tree->mmap_storage->mapped_size;
    double db_gb = (double)db_bytes / (1024.0 * 1024.0 * 1024.0);

    printf("\n  DONE | keys: %" PRIu64 " | blocks: %" PRIu64 "(+%" PRIu64 " aborted)"
           " | DB: %.1f GB | checks: %d"
           " | snaps: %" PRIu64 " | iso: %" PRIu64
           " | iter_scans: %" PRIu64 " | ckpts: %" PRIu64
           " | time: %s\n",
           live_count, total_blocks, total_aborted,
           db_gb, check_num,
           total_snaps, total_iso,
           total_iterscans, total_ckpts,
           elapsed_str);

    printf("\n  ALL CHECKS PASSED\n\n");

    // ----- Cleanup -----
    data_art_destroy(tree);
    free(ctxs);
    free(threads);
    free(bitset);

    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
    system(cmd);

    return 0;
}
