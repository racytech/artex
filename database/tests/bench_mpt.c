/*
 * MPT Commitment Benchmark — nibble_trie + nt_root_hash
 *
 * Measures per-block incremental MPT root computation using
 * nibble_trie with hash caching (nt_root_hash). No data_layer,
 * no compact_art, no intermediate_hashes update — pure trie walk.
 *
 * Phase A: Per-block nt_root_hash under realistic block simulation
 * Phase B: Full ih_build comparison at final scale (cross-validation)
 *
 * Usage: ./bench_mpt [target_millions]
 *   Default: 1M keys
 */

#include "../include/nibble_trie.h"
#include "../include/nt_hash.h"
#include "../include/intermediate_hashes.h"
#include "../include/hash.h"

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

#define MASTER_SEED 0x4D50544245000000ULL   // "MPTBE\0\0\0"
#define STATE_KEY_SEED (MASTER_SEED ^ 0x5354415445000000ULL)

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

static void generate_key(uint8_t key[KEY_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    uint64_t r0 = rng_next(&rng), r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng), r3 = rng_next(&rng);
    memcpy(key,      &r0, 8); memcpy(key + 8,  &r1, 8);
    memcpy(key + 16, &r2, 8); memcpy(key + 24, &r3, 8);
}

static void generate_value(uint8_t val[VALUE_SIZE], uint64_t seed, uint64_t index) {
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
    printf("  MPT Commitment Benchmark\n");
    printf("============================================\n");
    printf("  target:     %" PRIu64 "M keys\n", target_millions);
    printf("  key_size:   %d bytes\n", KEY_SIZE);
    printf("  value_size: %d bytes\n", VALUE_SIZE);
    printf("  ops/block:  %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  engine:     nibble_trie + nt_root_hash (cached)\n");
    printf("  seed:       0x%016" PRIx64 "\n", (uint64_t)MASTER_SEED);
    printf("============================================\n\n");

    // Create nibble_trie for MPT computation
    nibble_trie_t mpt;
    ASSERT_MSG(nt_init(&mpt, KEY_SIZE, VALUE_SIZE), "nt_init failed");

    // Block simulation state
    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double total_hash_time = 0;
    double min_hash_time = 1e9;
    double max_hash_time = 0;
    hash_t current_root = HASH_EMPTY_STORAGE;

    double t_start = now_sec();

    // ========================================================================
    // Phase A: Per-block nt_root_hash
    // ========================================================================
    printf("--- Phase A: Per-Block nt_root_hash (cached) ---\n\n");

    while (next_id < target_keys) {
        rng_t brng = rng_create(MASTER_SEED ^ (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        uint8_t key[KEY_SIZE];
        uint8_t val[VALUE_SIZE];

        // --- 70% inserts ---
        uint32_t insert_count = (uint32_t)(ops_count * 0.7);
        for (uint32_t i = 0; i < insert_count; i++) {
            uint64_t kid = next_id++;
            generate_key(key, STATE_KEY_SEED, kid);
            generate_value(val, STATE_KEY_SEED, kid);
            nt_insert(&mpt, key, val);
        }

        // --- 20% updates ---
        uint32_t update_count = (uint32_t)(ops_count * 0.2);
        for (uint32_t i = 0; i < update_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, STATE_KEY_SEED, kid);
            generate_value(val, STATE_KEY_SEED ^ (total_blocks + 1), kid);
            nt_insert(&mpt, key, val);
        }

        // --- 10% deletes ---
        uint32_t delete_count = (uint32_t)(ops_count * 0.1);
        for (uint32_t i = 0; i < delete_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, STATE_KEY_SEED, kid);
            nt_delete(&mpt, key);
        }

        total_blocks++;

        // --- MPT root hash ---
        double t_hash0 = now_sec();
        current_root = nt_root_hash(&mpt);
        double t_hash1 = now_sec();
        double hash_ms = (t_hash1 - t_hash0) * 1000.0;

        total_hash_time += (t_hash1 - t_hash0);
        if (hash_ms < min_hash_time) min_hash_time = hash_ms;
        if (hash_ms > max_hash_time) max_hash_time = hash_ms;

        // --- Stats ---
        bool is_last = (next_id >= target_keys);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            double avg_hash = (total_hash_time / total_blocks) * 1000.0;

            double elapsed = now_sec() - t_start;
            double kkeys_s = next_id / elapsed / 1000.0;

            printf("block %5" PRIu64 " | keys %7" PRIu64 " | "
                   "hash %6.1fms | avg %6.1fms | "
                   "%6.0fKk/s | RSS %4zuMB | root ",
                   total_blocks,
                   (uint64_t)nt_size(&mpt),
                   hash_ms,
                   avg_hash,
                   kkeys_s,
                   get_rss_mb());
            print_root_short(&current_root);
            printf("\n");
            fflush(stdout);
        }
    }

    double phase_a_elapsed = now_sec() - t_start;
    double avg_hash = (total_hash_time / total_blocks) * 1000.0;

    printf("\n--- Phase A Complete ---\n");
    printf("  blocks:       %" PRIu64 "\n", total_blocks);
    printf("  trie keys:    %" PRIu64 "\n", (uint64_t)nt_size(&mpt));
    printf("  avg hash:     %.2fms\n", avg_hash);
    printf("  min hash:     %.2fms\n", min_hash_time);
    printf("  max hash:     %.2fms\n", max_hash_time);
    printf("  total hash:   %.2fs\n", total_hash_time);
    printf("  time:         %.1fs\n", phase_a_elapsed);
    printf("  RSS:          %zuMB\n", get_rss_mb());
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&current_root, hex);
        printf("%s\n", hex);
    }
    printf("\n");

    // ========================================================================
    // Phase B: Full ih_build comparison
    // ========================================================================
    printf("--- Phase B: Full ih_build Comparison ---\n");

    // Collect all keys+values from nibble_trie iterator
    size_t n = nt_size(&mpt);
    printf("  collecting %" PRIu64 " keys...\n", (uint64_t)n);

    uint8_t *key_storage = malloc(n * KEY_SIZE);
    uint8_t *val_storage = malloc(n * VALUE_SIZE);
    const uint8_t **key_ptrs = malloc(n * sizeof(uint8_t *));
    const uint8_t **val_ptrs = malloc(n * sizeof(uint8_t *));
    uint16_t *val_lens = malloc(n * sizeof(uint16_t));
    ASSERT_MSG(key_storage && val_storage && key_ptrs && val_ptrs && val_lens,
               "alloc failed");

    double t_collect0 = now_sec();
    nt_iterator_t *it = nt_iterator_create(&mpt);
    ASSERT_MSG(it != NULL, "iterator create failed");

    size_t count = 0;
    while (nt_iterator_next(it) && count < n) {
        const uint8_t *k = nt_iterator_key(it);
        const void *v = nt_iterator_value(it);
        memcpy(key_storage + count * KEY_SIZE, k, KEY_SIZE);
        memcpy(val_storage + count * VALUE_SIZE, v, VALUE_SIZE);
        key_ptrs[count] = key_storage + count * KEY_SIZE;
        val_ptrs[count] = val_storage + count * VALUE_SIZE;
        val_lens[count] = VALUE_SIZE;
        count++;
    }
    nt_iterator_destroy(it);
    double t_collect1 = now_sec();

    printf("  collected:    %zu keys in %.2fs\n", count, t_collect1 - t_collect0);

    // ih_build
    ih_state_t *ih = ih_create();
    ASSERT_MSG(ih != NULL, "ih_create failed");

    double t_build0 = now_sec();
    hash_t build_root = ih_build(ih, key_ptrs, val_ptrs, val_lens, count);
    double t_build1 = now_sec();

    printf("  ih_build:     %.2fs (%zu keys)\n", t_build1 - t_build0, count);
    printf("  ih_entries:   %zu\n", ih_entry_count(ih));
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&build_root, hex);
        printf("%s\n", hex);
    }

    bool roots_match = hash_equal(&current_root, &build_root);
    printf("  roots match:  %s\n", roots_match ? "YES" : "NO");

    // Summary
    printf("\n--- Summary ---\n");
    printf("  nt_root_hash (avg):   %.2fms\n", avg_hash);
    printf("  nt_root_hash (min):   %.2fms\n", min_hash_time);
    printf("  nt_root_hash (max):   %.2fms\n", max_hash_time);
    printf("  ih_build (full):      %.2fs\n", t_build1 - t_build0);
    if (avg_hash > 0) {
        printf("  ih_build/nt_hash:     %.1fx\n",
               (t_build1 - t_build0) * 1000.0 / avg_hash);
    }
    printf("\n");

    // Cleanup
    free(key_storage);
    free(val_storage);
    free(key_ptrs);
    free(val_ptrs);
    free(val_lens);
    ih_destroy(ih);
    nt_destroy(&mpt);

    printf("============================================\n");
    if (roots_match) {
        printf("  BENCHMARK COMPLETE — ROOTS MATCH\n");
    } else {
        printf("  BENCHMARK COMPLETE — ROOT MISMATCH\n");
    }
    printf("============================================\n");

    return roots_match ? 0 : 1;
}
