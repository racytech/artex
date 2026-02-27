/*
 * Per-Block Merge Benchmark
 *
 * Validates that merging ~5K ops per block stays fast at scale.
 *
 * Two phases:
 *   1. Bulk fill: insert N million keys via large batch merges
 *   2. Per-block merge: simulate 1000 blocks (~5K ops each),
 *      measure merge latency per block
 *
 * Realistic Ethereum block mix: ~60% updates, ~25% inserts, ~15% deletes
 *
 * Reports: min/max/avg/p50/p95/p99 merge latency
 *
 * Usage: ./bench_per_block_merge [prefill_millions] [num_blocks]
 *   Default: 10M keys, 1000 blocks
 */

#include "../include/compact_art.h"
#include "../include/mem_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE       32
#define SLOT_SIZE      64
#define MAX_VALUE_LEN  62
#define VALUE_LEN      32

#define BUF_FLAG_WRITE     0x01
#define BUF_FLAG_TOMBSTONE 0x02

#define OPS_PER_BLOCK      5000
#define BULK_BATCH_SIZE    500000

// ============================================================================
// RNG
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
    rng_next(&r);
    return r;
}

static void generate_key(uint8_t key[KEY_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(key,      &r0, 8);
    memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8);
    memcpy(key + 24, &r3, 8);
}

static void generate_value(uint8_t value[VALUE_LEN], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    for (int i = 0; i < VALUE_LEN; i += 8) {
        uint64_t r = rng_next(&rng);
        int remain = VALUE_LEN - i;
        memcpy(value + i, &r, remain < 8 ? remain : 8);
    }
}

// ============================================================================
// Utilities
// ============================================================================

static size_t get_rss_mb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss_kb = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, "%zu", &rss_kb);
            break;
        }
    }
    fclose(f);
    return rss_kb / 1024;
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// ============================================================================
// State Store (same as PoC)
// ============================================================================

typedef struct {
    int fd;
    uint32_t next_slot;
    uint32_t *free_list;
    uint32_t free_count;
    uint32_t free_cap;
    char path[256];
} state_store_t;

static bool store_init(state_store_t *s, const char *path) {
    snprintf(s->path, sizeof(s->path), "%s", path);
    s->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (s->fd < 0) return false;
    s->next_slot = 0;
    s->free_count = 0;
    s->free_cap = 4096;
    s->free_list = malloc(s->free_cap * sizeof(uint32_t));
    return s->free_list != NULL;
}

static void store_destroy(state_store_t *s) {
    if (s->fd >= 0) {
        close(s->fd);
        unlink(s->path);
    }
    free(s->free_list);
}

static uint32_t store_alloc(state_store_t *s) {
    if (s->free_count > 0)
        return s->free_list[--s->free_count];
    return s->next_slot++;
}

static void store_free_slot(state_store_t *s, uint32_t slot) {
    if (s->free_count >= s->free_cap) {
        s->free_cap *= 2;
        s->free_list = realloc(s->free_list, s->free_cap * sizeof(uint32_t));
    }
    s->free_list[s->free_count++] = slot;
}

static bool store_write(state_store_t *s, uint32_t slot,
                        const void *value, uint16_t len) {
    uint8_t buf[SLOT_SIZE] = {0};
    memcpy(buf, &len, 2);
    memcpy(buf + 2, value, len);
    ssize_t w = pwrite(s->fd, buf, SLOT_SIZE, (off_t)slot * SLOT_SIZE);
    return w == SLOT_SIZE;
}

// ============================================================================
// Data Layer (minimal — just what we need for merge)
// ============================================================================

typedef struct {
    compact_art_t index;
    state_store_t store;
    art_tree_t buffer;
    uint64_t key_count;
} data_layer_t;

static bool dl_init(data_layer_t *dl, const char *path) {
    memset(dl, 0, sizeof(*dl));
    if (!compact_art_init(&dl->index, KEY_SIZE, 4)) return false;
    if (!store_init(&dl->store, path)) {
        compact_art_destroy(&dl->index);
        return false;
    }
    if (!art_tree_init(&dl->buffer)) {
        compact_art_destroy(&dl->index);
        store_destroy(&dl->store);
        return false;
    }
    return true;
}

static void dl_destroy(data_layer_t *dl) {
    compact_art_destroy(&dl->index);
    store_destroy(&dl->store);
    art_tree_destroy(&dl->buffer);
}

static bool dl_buf_put(data_layer_t *dl, const uint8_t key[KEY_SIZE],
                       const void *value, uint16_t len) {
    uint8_t buf[1 + MAX_VALUE_LEN];
    buf[0] = BUF_FLAG_WRITE;
    memcpy(buf + 1, value, len);
    return art_insert(&dl->buffer, key, KEY_SIZE, buf, 1 + len);
}

static bool dl_buf_delete(data_layer_t *dl, const uint8_t key[KEY_SIZE]) {
    uint8_t tombstone = BUF_FLAG_TOMBSTONE;
    return art_insert(&dl->buffer, key, KEY_SIZE, &tombstone, 1);
}

// Merge buffer into index + state.dat. No fdatasync (checkpoint handles that).
static uint64_t dl_merge(data_layer_t *dl) {
    uint64_t count = 0;

    art_iterator_t *iter = art_iterator_create(&dl->buffer);
    if (!iter) return 0;

    while (art_iterator_next(iter)) {
        size_t klen = 0, vlen = 0;
        const uint8_t *key = art_iterator_key(iter, &klen);
        const void *val = art_iterator_value(iter, &vlen);
        if (!key || !val || klen != KEY_SIZE || vlen < 1) continue;

        uint8_t flag = *(const uint8_t *)val;

        if (flag == BUF_FLAG_TOMBSTONE) {
            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                uint32_t slot;
                memcpy(&slot, ref, 4);
                store_free_slot(&dl->store, slot);
                compact_art_delete(&dl->index, key);
                dl->key_count--;
            }
        } else if (flag == BUF_FLAG_WRITE && vlen > 1) {
            const uint8_t *data = (const uint8_t *)val + 1;
            uint16_t data_len = (uint16_t)(vlen - 1);

            const void *ref = compact_art_get(&dl->index, key);
            if (ref) {
                uint32_t slot;
                memcpy(&slot, ref, 4);
                store_write(&dl->store, slot, data, data_len);
            } else {
                uint32_t slot = store_alloc(&dl->store);
                store_write(&dl->store, slot, data, data_len);
                compact_art_insert(&dl->index, key, &slot);
                dl->key_count++;
            }
        }
        count++;
    }

    art_iterator_destroy(iter);
    art_tree_destroy(&dl->buffer);
    art_tree_init(&dl->buffer);
    return count;
}

// ============================================================================
// qsort comparator for latency percentiles
// ============================================================================

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t prefill_millions = 10;
    uint64_t num_blocks = 1000;

    if (argc > 1) {
        prefill_millions = (uint64_t)atoll(argv[1]);
        if (prefill_millions == 0) prefill_millions = 10;
    }
    if (argc > 2) {
        num_blocks = (uint64_t)atoll(argv[2]);
        if (num_blocks == 0) num_blocks = 1000;
    }

    uint64_t prefill = prefill_millions * 1000000ULL;
    uint64_t seed = 0x0000000069a11ab9ULL;

    printf("============================================\n");
    printf("Per-Block Merge Benchmark\n");
    printf("============================================\n");
    printf("  prefill:     %" PRIu64 "M keys\n", prefill_millions);
    printf("  blocks:      %" PRIu64 "\n", num_blocks);
    printf("  ops/block:   %d\n", OPS_PER_BLOCK);
    printf("  block mix:   60%% update, 25%% insert, 15%% delete\n");
    printf("============================================\n\n");

    data_layer_t dl;
    if (!dl_init(&dl, "/tmp/art_bench_merge_state.dat")) {
        fprintf(stderr, "FAIL: dl_init\n");
        return 1;
    }

    // ========================================================================
    // Phase 1: Bulk fill
    // ========================================================================
    printf("--- Phase 1: Bulk fill %" PRIu64 "M keys ---\n", prefill_millions);
    double t_fill_start = now_sec();

    uint8_t key[KEY_SIZE], value[VALUE_LEN];
    uint64_t filled = 0;

    while (filled < prefill) {
        uint64_t batch = BULK_BATCH_SIZE;
        if (filled + batch > prefill) batch = prefill - filled;

        for (uint64_t i = 0; i < batch; i++) {
            generate_key(key, seed, filled + i);
            generate_value(value, seed, filled + i);
            dl_buf_put(&dl, key, value, VALUE_LEN);
        }
        dl_merge(&dl);
        filled += batch;

        double elapsed = now_sec() - t_fill_start;
        printf("  %.0fM / %" PRIu64 "M | %.0f Kk/s | RSS %zu MB\n",
               filled / 1e6, prefill_millions,
               filled / elapsed / 1000.0, get_rss_mb());
        fflush(stdout);
    }

    double t_fill_end = now_sec();
    printf("\n  fill done: %" PRIu64 "M keys in %.1fs (%.0f Kk/s)\n",
           prefill_millions, t_fill_end - t_fill_start,
           prefill / (t_fill_end - t_fill_start) / 1000.0);
    printf("  index size: %zu | RSS: %zu MB\n\n",
           compact_art_size(&dl.index), get_rss_mb());

    // ========================================================================
    // Phase 2: Per-block merge
    // ========================================================================
    printf("--- Phase 2: Per-block merge (%" PRIu64 " blocks × %d ops) ---\n",
           num_blocks, OPS_PER_BLOCK);

    double *latencies = malloc(num_blocks * sizeof(double));
    if (!latencies) {
        fprintf(stderr, "FAIL: malloc latencies\n");
        dl_destroy(&dl);
        return 1;
    }

    rng_t block_rng = rng_create(seed ^ 0xB10CB10CB10CB10CULL);
    uint64_t next_new_key = prefill;

    // Op counts per block: 60% update, 25% insert, 15% delete
    uint64_t updates_per_block = (OPS_PER_BLOCK * 60) / 100;  // 3000
    uint64_t inserts_per_block = (OPS_PER_BLOCK * 25) / 100;  // 1250
    uint64_t deletes_per_block = OPS_PER_BLOCK - updates_per_block - inserts_per_block; // 750

    for (uint64_t b = 0; b < num_blocks; b++) {
        // --- Fill buffer with one block's ops ---

        // Updates: pick random existing keys, write new values
        for (uint64_t i = 0; i < updates_per_block; i++) {
            uint64_t idx = rng_next(&block_rng) % dl.key_count;
            generate_key(key, seed, idx);
            generate_value(value, seed + b + 1, idx);  // new value
            dl_buf_put(&dl, key, value, VALUE_LEN);
        }

        // Inserts: new keys
        for (uint64_t i = 0; i < inserts_per_block; i++) {
            generate_key(key, seed, next_new_key);
            generate_value(value, seed, next_new_key);
            dl_buf_put(&dl, key, value, VALUE_LEN);
            next_new_key++;
        }

        // Deletes: pick random existing keys
        for (uint64_t i = 0; i < deletes_per_block; i++) {
            uint64_t idx = rng_next(&block_rng) % dl.key_count;
            generate_key(key, seed, idx);
            dl_buf_delete(&dl, key);
        }

        // --- Merge (this is what we're measuring) ---
        double t0 = now_sec();
        uint64_t merged = dl_merge(&dl);
        double t1 = now_sec();

        latencies[b] = (t1 - t0) * 1000.0;  // ms

        if ((b + 1) % 100 == 0 || b == 0) {
            printf("  block %4" PRIu64 " | merged %" PRIu64
                   " | %.2f ms | keys %" PRIu64 " | RSS %zu MB\n",
                   b + 1, merged, latencies[b], dl.key_count, get_rss_mb());
            fflush(stdout);
        }
    }

    // ========================================================================
    // Phase 3: Report
    // ========================================================================
    printf("\n--- Results ---\n");

    // Sort for percentiles
    qsort(latencies, num_blocks, sizeof(double), cmp_double);

    double sum = 0;
    for (uint64_t i = 0; i < num_blocks; i++) sum += latencies[i];

    double avg = sum / num_blocks;
    double min = latencies[0];
    double max = latencies[num_blocks - 1];
    double p50 = latencies[num_blocks / 2];
    double p95 = latencies[(uint64_t)(num_blocks * 0.95)];
    double p99 = latencies[(uint64_t)(num_blocks * 0.99)];

    printf("\n");
    printf("  ============================================\n");
    printf("  Per-Block Merge Latency (%" PRIu64 " blocks)\n", num_blocks);
    printf("  ============================================\n");
    printf("  prefill:   %" PRIu64 "M keys\n", prefill_millions);
    printf("  ops/block: %d (60%% upd, 25%% ins, 15%% del)\n", OPS_PER_BLOCK);
    printf("  final keys: %" PRIu64 "\n", dl.key_count);
    printf("  --------------------------------------------\n");
    printf("  min:       %8.2f ms\n", min);
    printf("  avg:       %8.2f ms\n", avg);
    printf("  p50:       %8.2f ms\n", p50);
    printf("  p95:       %8.2f ms\n", p95);
    printf("  p99:       %8.2f ms\n", p99);
    printf("  max:       %8.2f ms\n", max);
    printf("  --------------------------------------------\n");
    printf("  RSS:       %zu MB\n", get_rss_mb());
    printf("  ============================================\n\n");

    free(latencies);
    dl_destroy(&dl);
    return 0;
}
