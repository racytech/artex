/*
 * MPT Commitment Benchmark — Merkle Patricia Trie State Root at Scale
 *
 * Measures:
 *   Phase A: Per-block ih_build from dirty keys under realistic block simulation
 *
 * With hash_store, there is no ordered cursor over all committed keys,
 * so we extract dirty keys from the write buffer before each merge and
 * compute an incremental MPT over the dirty set.
 *
 * Usage: ./bench_mpt [target_millions]
 *   Default: 5M keys
 */

#include "../include/data_layer.h"
#include "../include/intermediate_hashes.h"
#include "../include/hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE        32
#define VALUE_LEN_MIN   32
#define VALUE_LEN_MAX   62   // max value size for hash_store slots
#define OPS_MIN         5000
#define OPS_MAX         50000
#define STATS_INTERVAL  10

#define STATE_DIR  "/tmp/art_bench_mpt_state"

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

static uint16_t generate_value(uint8_t *buf, uint64_t seed, uint64_t index,
                               uint16_t len) {
    rng_t rng = rng_create(seed ^ (index * 0x9c2f0b3a71d8e6f5ULL));
    for (uint16_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(&rng);
        uint16_t remain = len - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
    return len;
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
    uint64_t target_millions = 5;
    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) target_millions = 5;
    }

    uint64_t target_keys = target_millions * 1000000ULL;

    printf("============================================\n");
    printf("  MPT Commitment Benchmark\n");
    printf("============================================\n");
    printf("  target:     %" PRIu64 "M keys\n", target_millions);
    printf("  ops/block:  %d-%d (70/20/10 ins/upd/del)\n", OPS_MIN, OPS_MAX);
    printf("  seed:       0x%016" PRIx64 "\n", (uint64_t)MASTER_SEED);
    printf("============================================\n\n");

    // Clean up
    { char cmd[256]; snprintf(cmd, sizeof(cmd), "rm -rf %s", STATE_DIR); system(cmd); }

    // Create data layer (no code store — focusing on state commitment)
    data_layer_t *dl = dl_create(STATE_DIR, NULL, KEY_SIZE, 128, (1ULL << 24));
    ASSERT_MSG(dl != NULL, "dl_create failed");

    // Create intermediate hash state
    ih_state_t *ih = ih_create();
    ASSERT_MSG(ih != NULL, "ih_create failed");

    // Block simulation state
    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double total_merge_time = 0;
    double total_mpt_time = 0;
    double min_mpt_time = 1e9;
    double max_mpt_time = 0;
    bool ih_initialized = false;
    hash_t current_root = HASH_EMPTY_STORAGE;

    double t_start = now_sec();

    // ========================================================================
    // Phase A: Block simulation + per-block MPT from dirty keys
    // ========================================================================
    printf("--- Phase A: Per-Block Dirty Key MPT ---\n\n");

    while (next_id < target_keys) {
        rng_t brng = rng_create(MASTER_SEED ^ (total_blocks * 0x426C6F636B000000ULL));
        uint32_t ops_count = OPS_MIN + (uint32_t)(rng_next(&brng) %
                             (OPS_MAX - OPS_MIN + 1));

        uint8_t key[KEY_SIZE];
        uint8_t val_buf[VALUE_LEN_MAX];

        // --- 70% inserts ---
        uint32_t insert_count = (uint32_t)(ops_count * 0.7);
        for (uint32_t i = 0; i < insert_count; i++) {
            uint64_t kid = next_id++;
            generate_key(key, STATE_KEY_SEED, kid);
            uint16_t vlen = VALUE_LEN_MIN + (uint16_t)(rng_next(&brng) %
                            (VALUE_LEN_MAX - VALUE_LEN_MIN + 1));
            generate_value(val_buf, STATE_KEY_SEED, kid, vlen);
            dl_put(dl, key, val_buf, vlen);
        }

        // --- 20% updates ---
        uint32_t update_count = (uint32_t)(ops_count * 0.2);
        for (uint32_t i = 0; i < update_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, STATE_KEY_SEED, kid);
            uint16_t vlen = VALUE_LEN_MIN + (uint16_t)(rng_next(&brng) %
                            (VALUE_LEN_MAX - VALUE_LEN_MIN + 1));
            generate_value(val_buf, STATE_KEY_SEED ^ (total_blocks + 1), kid, vlen);
            dl_put(dl, key, val_buf, vlen);
        }

        // --- 10% deletes ---
        uint32_t delete_count = (uint32_t)(ops_count * 0.1);
        for (uint32_t i = 0; i < delete_count && next_id > 0; i++) {
            uint64_t kid = rng_next(&brng) % next_id;
            generate_key(key, STATE_KEY_SEED, kid);
            dl_delete(dl, key);
        }

        // --- Extract dirty keys BEFORE merge ---
        dl_dirty_set_t dirty;
        bool has_dirty = dl_extract_dirty(dl, &dirty);

        // --- Merge ---
        double t_merge0 = now_sec();
        dl_merge(dl);
        double t_merge1 = now_sec();
        total_merge_time += (t_merge1 - t_merge0);
        total_blocks++;

        // --- MPT commitment from dirty keys ---
        double t_mpt0 = now_sec();

        if (has_dirty) {
            // Convert dirty set to ih_build format
            const uint8_t **key_ptrs = malloc(dirty.count * sizeof(uint8_t *));
            const uint8_t **val_ptrs = malloc(dirty.count * sizeof(uint8_t *));
            uint16_t *vlens = malloc(dirty.count * sizeof(uint16_t));
            ASSERT_MSG(key_ptrs && val_ptrs && vlens, "malloc failed");

            size_t live_count = 0;
            for (size_t j = 0; j < dirty.count; j++) {
                if (dirty.values[j] != NULL) {
                    key_ptrs[live_count] = dirty.keys[j];
                    val_ptrs[live_count] = dirty.values[j];
                    vlens[live_count] = (uint16_t)dirty.value_lens[j];
                    live_count++;
                }
            }

            if (live_count > 0) {
                if (!ih_initialized) {
                    // First block: full build from dirty keys
                    current_root = ih_build(ih, key_ptrs, val_ptrs,
                                            vlens, live_count);
                    ih_initialized = true;
                } else {
                    // Subsequent blocks: rebuild from dirty keys
                    // (Without a cursor over all committed keys, we rebuild
                    //  the ih state from this block's dirty keys only.
                    //  This measures trie construction throughput.)
                    ih_state_t *block_ih = ih_create();
                    ASSERT_MSG(block_ih != NULL, "ih_create failed");
                    current_root = ih_build(block_ih, key_ptrs, val_ptrs,
                                            vlens, live_count);
                    ih_destroy(block_ih);
                }
            }

            free(key_ptrs);
            free(val_ptrs);
            free(vlens);
        }

        double t_mpt1 = now_sec();
        double mpt_ms = (t_mpt1 - t_mpt0) * 1000.0;

        // Track min/max (skip first block)
        if (total_blocks > 1) {
            total_mpt_time += (t_mpt1 - t_mpt0);
            if (mpt_ms < min_mpt_time) min_mpt_time = mpt_ms;
            if (mpt_ms > max_mpt_time) max_mpt_time = mpt_ms;
        }

        if (has_dirty) dl_dirty_set_free(&dirty);

        // --- Stats ---
        bool is_last = (next_id >= target_keys);
        if (total_blocks % STATS_INTERVAL == 0 || is_last) {
            dl_stats_t st = dl_stats(dl);
            double avg_merge = (total_merge_time / total_blocks) * 1000.0;

            printf("block %5" PRIu64 " | index %6.2fM | "
                   "merge %5.1fms | mpt %6.1fms | "
                   "ih_entries %6zu | RSS %4zuMB | root ",
                   total_blocks,
                   st.index_keys / 1e6,
                   avg_merge,
                   mpt_ms,
                   ih_entry_count(ih),
                   get_rss_mb());
            print_root_short(&current_root);
            printf("\n");
            fflush(stdout);
        }
    }

    double phase_a_elapsed = now_sec() - t_start;
    dl_stats_t final_stats = dl_stats(dl);
    double avg_mpt = (total_blocks > 1)
        ? (total_mpt_time / (total_blocks - 1)) * 1000.0 : 0;

    printf("\n--- Phase A Complete ---\n");
    printf("  blocks:       %" PRIu64 "\n", total_blocks);
    printf("  index keys:   %" PRIu64 "\n", final_stats.index_keys);
    printf("  ih_entries:   %zu\n", ih_entry_count(ih));
    printf("  avg merge:    %.2fms\n",
           (total_merge_time / total_blocks) * 1000.0);
    printf("  avg mpt:      %.2fms\n", avg_mpt);
    printf("  min mpt:      %.2fms\n", min_mpt_time);
    printf("  max mpt:      %.2fms\n", max_mpt_time);
    printf("  time:         %.1fs\n", phase_a_elapsed);
    printf("  RSS:          %zuMB\n", get_rss_mb());
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&current_root, hex);
        printf("%s\n", hex);
    }
    printf("\n");

    // Cleanup
    ih_destroy(ih);
    dl_destroy(dl);
    { char cmd[256]; snprintf(cmd, sizeof(cmd), "rm -rf %s", STATE_DIR); system(cmd); }

    printf("============================================\n");
    printf("  BENCHMARK COMPLETE\n");
    printf("============================================\n");

    return 0;
}
