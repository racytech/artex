/*
 * MPT Root Benchmark — nibble_trie tree walk (nt_root_hash)
 *
 * Simulates block execution with mixed insert/update/delete operations,
 * measures per-block nt_root_hash cost as the trie grows.
 * Cross-validates final root against ih_build (sorted-key scan).
 *
 * Usage: ./bench_nt_hash [target_millions]
 *   Default: 1M keys
 */

#include "../include/nibble_trie.h"
#include "../include/nt_hash.h"
#include "intermediate_hashes.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE        32
#define VALUE_SIZE      32
#define OPS_MIN         5000
#define OPS_MAX         50000
#define STATS_INTERVAL  10

#define MASTER_SEED     0x4E54484153480000ULL   // "NTHASH\0\0"
#define KEY_SEED        (MASTER_SEED ^ 0x4B45595300000000ULL)

// ============================================================================
// Fail-fast
// ============================================================================

#define ASSERT_MSG(cond, fmt, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
                __FILE__, __LINE__, ##__VA_ARGS__); \
        abort(); \
    } \
} while(0)

// ============================================================================
// RNG (SplitMix64)
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

// ============================================================================
// Key/Value Generation
// ============================================================================

static void generate_key(uint8_t key[KEY_SIZE], uint64_t index) {
    rng_t rng = rng_create(KEY_SEED ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key,      &r0, 8); memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8); memcpy(key + 24, &r3, 8);
}

static void generate_value(uint8_t val[VALUE_SIZE], uint64_t seed,
                            uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(val,      &r0, 8); memcpy(val + 8,  &r1, 8);
    memcpy(val + 16, &r2, 8); memcpy(val + 24, &r3, 8);
}

// ============================================================================
// Utilities
// ============================================================================

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

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

static void print_root_short(const hash_t *h) {
    printf("0x");
    for (int i = 0; i < 4; i++) printf("%02x", h->bytes[i]);
    printf("..");
    for (int i = 28; i < 32; i++) printf("%02x", h->bytes[i]);
}

// ============================================================================
// Cross-validation: collect trie into flat arrays for ih_build
// ============================================================================

typedef struct {
    const uint8_t **key_ptrs;
    const uint8_t **val_ptrs;
    uint16_t       *val_lens;
    size_t          count;
} flat_dataset_t;

static flat_dataset_t collect_sorted(const nibble_trie_t *t) {
    size_t n = nt_size(t);
    flat_dataset_t ds = {0};
    if (n == 0) return ds;

    ds.key_ptrs = malloc(n * sizeof(uint8_t *));
    ds.val_ptrs = malloc(n * sizeof(uint8_t *));
    ds.val_lens = malloc(n * sizeof(uint16_t));
    ASSERT_MSG(ds.key_ptrs && ds.val_ptrs && ds.val_lens, "malloc");

    nt_iterator_t *it = nt_iterator_create(t);
    ASSERT_MSG(it != NULL, "nt_iterator_create");

    size_t i = 0;
    while (nt_iterator_next(it) && i < n) {
        ds.key_ptrs[i] = nt_iterator_key(it);
        ds.val_ptrs[i] = nt_iterator_value(it);
        ds.val_lens[i] = VALUE_SIZE;
        i++;
    }
    nt_iterator_destroy(it);

    ds.count = i;
    return ds;
}

static void free_dataset(flat_dataset_t *ds) {
    free(ds->key_ptrs);
    free(ds->val_ptrs);
    free(ds->val_lens);
    memset(ds, 0, sizeof(*ds));
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t target_millions = 1;
    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) target_millions = 1;
    }

    uint64_t target_keys = target_millions * 1000000ULL;

    printf("============================================\n");
    printf("  nt_root_hash Benchmark\n");
    printf("============================================\n");
    printf("  target:     %" PRIu64 "M keys\n", target_millions);
    printf("  values:     %dB fixed\n", VALUE_SIZE);
    printf("  ops/block:  %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  seed:       0x%016" PRIx64 "\n", (uint64_t)MASTER_SEED);
    printf("============================================\n\n");

    nibble_trie_t t;
    ASSERT_MSG(nt_init(&t, KEY_SIZE, VALUE_SIZE), "nt_init");

    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double total_hash_time = 0;
    double min_hash_time = 1e9;
    double max_hash_time = 0;
    hash_t current_root = HASH_EMPTY_STORAGE;

    double t_start = now_sec();

    // ========================================================================
    // Block simulation + per-block nt_root_hash
    // ========================================================================
    printf("--- Per-Block Tree Walk ---\n\n");

    while (next_id < target_keys) {
        rng_t brng = rng_create(MASTER_SEED ^
                                (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        uint8_t key[KEY_SIZE];
        uint8_t val[VALUE_SIZE];

        // --- 70% inserts ---
        uint32_t insert_count = (uint32_t)(ops_count * 0.7);
        for (uint32_t i = 0; i < insert_count; i++) {
            uint64_t kid = next_id++;
            generate_key(key, kid);
            generate_value(val, KEY_SEED, kid);
            nt_insert(&t, key, val);
        }

        // --- 20% updates ---
        uint32_t update_count = (uint32_t)(ops_count * 0.2);
        for (uint32_t i = 0; i < update_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, kid);
            generate_value(val, KEY_SEED ^ (total_blocks + 1), kid);
            nt_insert(&t, key, val);
        }

        // --- 10% deletes ---
        uint32_t delete_count = (uint32_t)(ops_count * 0.1);
        for (uint32_t i = 0; i < delete_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, kid);
            nt_delete(&t, key);
        }

        // --- nt_root_hash ---
        double t_hash0 = now_sec();
        current_root = nt_root_hash(&t);
        double t_hash1 = now_sec();

        double hash_ms = (t_hash1 - t_hash0) * 1000.0;
        total_hash_time += (t_hash1 - t_hash0);
        if (hash_ms < min_hash_time) min_hash_time = hash_ms;
        if (hash_ms > max_hash_time) max_hash_time = hash_ms;

        total_blocks++;

        // --- Stats ---
        bool is_last = (next_id >= target_keys);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            printf("block %5" PRIu64 " | keys %8zu | "
                   "hash %7.1fms | RSS %4zuMB | root ",
                   total_blocks,
                   nt_size(&t),
                   hash_ms,
                   get_rss_mb());
            print_root_short(&current_root);
            printf("\n");
            fflush(stdout);
        }
    }

    double phase_elapsed = now_sec() - t_start;
    double avg_hash = (total_blocks > 0)
        ? (total_hash_time / total_blocks) * 1000.0 : 0;

    printf("\n--- Block Simulation Complete ---\n");
    printf("  blocks:       %" PRIu64 "\n", total_blocks);
    printf("  trie keys:    %zu\n", nt_size(&t));
    printf("  avg hash:     %.2fms\n", avg_hash);
    printf("  min hash:     %.2fms\n", min_hash_time);
    printf("  max hash:     %.2fms\n", max_hash_time);
    printf("  total hash:   %.2fs\n", total_hash_time);
    printf("  time:         %.1fs\n", phase_elapsed);
    printf("  RSS:          %zuMB\n", get_rss_mb());
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&current_root, hex);
        printf("%s\n", hex);
    }
    printf("\n");

    // ========================================================================
    // Cross-validation: ih_build on same data
    // ========================================================================
    printf("--- Cross-Validation: ih_build ---\n");

    printf("  collecting %zu keys...\n", nt_size(&t));
    double t_collect0 = now_sec();
    flat_dataset_t ds = collect_sorted(&t);
    double t_collect1 = now_sec();
    printf("  collected:    %zu keys in %.2fs\n", ds.count, t_collect1 - t_collect0);

    ih_state_t *ih = ih_create();
    ASSERT_MSG(ih != NULL, "ih_create");

    double t_build0 = now_sec();
    hash_t ih_root = ih_build(ih, ds.key_ptrs, ds.val_ptrs,
                               ds.val_lens, ds.count);
    double t_build1 = now_sec();

    printf("  ih_build:     %.2fs (%zu keys)\n", t_build1 - t_build0, ds.count);
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&ih_root, hex);
        printf("%s\n", hex);
    }

    bool roots_match = hash_equal(&current_root, &ih_root);
    printf("  roots match:  %s\n", roots_match ? "YES" : "NO");
    if (!roots_match) {
        printf("  WARNING: nt_root_hash and ih_build roots differ!\n");
    }

    printf("\n--- Summary ---\n");
    printf("  nt_root_hash (avg):  %.2fms\n", avg_hash);
    printf("  ih_build (full):     %.2fs\n", t_build1 - t_build0);
    if (avg_hash > 0) {
        printf("  ih_build/nt_hash:    %.1fx\n",
               (t_build1 - t_build0) * 1000.0 / avg_hash);
    }
    printf("\n");

    // Cleanup
    free_dataset(&ds);
    ih_destroy(ih);
    nt_destroy(&t);

    printf("============================================\n");
    if (roots_match) {
        printf("  BENCHMARK COMPLETE — ROOTS MATCH\n");
    } else {
        printf("  BENCHMARK COMPLETE — ROOT MISMATCH\n");
    }
    printf("============================================\n");

    return roots_match ? 0 : 1;
}
