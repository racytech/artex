/*
 * MPT Commitment Benchmark — Merkle Patricia Trie State Root at Scale
 *
 * Measures:
 *   Phase A: Per-block ih_update (incremental) under realistic block simulation
 *   Phase B: Full ih_build comparison at final scale
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
#define VALUE_LEN_MAX   62   // STATE_STORE_MAX_VALUE (values in ih must be readable)
#define OPS_MIN         5000
#define OPS_MAX         50000
#define STATS_INTERVAL  10

#define STATE_PATH "/tmp/art_bench_mpt_state.dat"
#define TRIE_PATH  "/tmp/art_bench_mpt_trie.dat"

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
// Full build helper: collect all keys+values from cursor
// ============================================================================

typedef struct {
    uint8_t  *key_storage;     // flat array: count * KEY_SIZE
    uint8_t  *value_storage;   // flat array: count * max_value_len
    uint16_t *value_lens;
    const uint8_t **key_ptrs;
    const uint8_t **value_ptrs;
    size_t count;
} full_dataset_t;

static bool collect_full_dataset(data_layer_t *dl, full_dataset_t *ds) {
    dl_stats_t st = dl_stats(dl);
    uint64_t n = st.index_keys;
    if (n == 0) return false;

    ds->key_storage = malloc((size_t)n * KEY_SIZE);
    ds->value_storage = malloc((size_t)n * VALUE_LEN_MAX);
    ds->value_lens = malloc((size_t)n * sizeof(uint16_t));
    ds->key_ptrs = malloc((size_t)n * sizeof(uint8_t *));
    ds->value_ptrs = malloc((size_t)n * sizeof(uint8_t *));
    if (!ds->key_storage || !ds->value_storage || !ds->value_lens ||
        !ds->key_ptrs || !ds->value_ptrs) {
        free(ds->key_storage); free(ds->value_storage);
        free(ds->value_lens); free(ds->key_ptrs); free(ds->value_ptrs);
        return false;
    }

    dl_cursor_t *cur = dl_cursor_create(dl);
    if (!cur) {
        free(ds->key_storage); free(ds->value_storage);
        free(ds->value_lens); free(ds->key_ptrs); free(ds->value_ptrs);
        return false;
    }
    ih_cursor_t ihc = dl_cursor_as_ih(cur);

    // Seek to start (all-zero key)
    uint8_t zero_key[KEY_SIZE] = {0};
    ihc.seek(ihc.ctx, zero_key, KEY_SIZE);

    size_t count = 0;
    while (ihc.valid(ihc.ctx) && count < (size_t)n) {
        size_t klen, vlen;
        const uint8_t *k = ihc.key(ihc.ctx, &klen);
        const uint8_t *v = ihc.value(ihc.ctx, &vlen);

        if (k && v && vlen > 0) {
            memcpy(ds->key_storage + count * KEY_SIZE, k, KEY_SIZE);
            memcpy(ds->value_storage + count * VALUE_LEN_MAX, v, vlen);
            ds->value_lens[count] = (uint16_t)vlen;
            ds->key_ptrs[count] = ds->key_storage + count * KEY_SIZE;
            ds->value_ptrs[count] = ds->value_storage + count * VALUE_LEN_MAX;
            count++;
        }
        ihc.next(ihc.ctx);
    }

    dl_cursor_destroy(cur);
    ds->count = count;
    return count > 0;
}

static void free_full_dataset(full_dataset_t *ds) {
    free(ds->key_storage);
    free(ds->value_storage);
    free(ds->value_lens);
    free(ds->key_ptrs);
    free(ds->value_ptrs);
    memset(ds, 0, sizeof(*ds));
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
    unlink(STATE_PATH);
    unlink(TRIE_PATH);

    // Create data layer (no code store — focusing on state commitment)
    data_layer_t *dl = dl_create(STATE_PATH, NULL, TRIE_PATH, KEY_SIZE, 4);
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
    // Phase A: Block simulation + per-block ih_update
    // ========================================================================
    printf("--- Phase A: Per-Block Incremental Update ---\n\n");

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

        // --- MPT commitment ---
        double t_mpt0 = now_sec();

        if (!ih_initialized) {
            // First block: full build from all committed keys
            full_dataset_t ds = {0};
            if (collect_full_dataset(dl, &ds)) {
                current_root = ih_build(ih, ds.key_ptrs, ds.value_ptrs,
                                        ds.value_lens, ds.count);
                free_full_dataset(&ds);
            }
            ih_initialized = true;
        } else if (has_dirty) {
            // Incremental update
            dl_cursor_t *cur = dl_cursor_create(dl);
            ASSERT_MSG(cur != NULL, "dl_cursor_create failed");
            ih_cursor_t ihc = dl_cursor_as_ih(cur);

            current_root = ih_update(ih,
                (const uint8_t *const *)dirty.keys,
                (const uint8_t *const *)dirty.values,
                dirty.value_lens, dirty.count, &ihc);

            dl_cursor_destroy(cur);
        }

        double t_mpt1 = now_sec();
        double mpt_ms = (t_mpt1 - t_mpt0) * 1000.0;

        // Skip first block (full build) for min/max tracking
        if (ih_initialized && total_blocks > 1) {
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

    // ========================================================================
    // Phase B: Full build comparison
    // ========================================================================
    printf("--- Phase B: Full Build Comparison ---\n");

    full_dataset_t ds = {0};
    printf("  collecting %6.2fM keys...\n", final_stats.index_keys / 1e6);
    double t_collect0 = now_sec();
    bool ok = collect_full_dataset(dl, &ds);
    double t_collect1 = now_sec();
    ASSERT_MSG(ok, "collect_full_dataset failed");
    printf("  collected:    %zu keys in %.2fs\n", ds.count, t_collect1 - t_collect0);

    ih_state_t *ih2 = ih_create();
    ASSERT_MSG(ih2 != NULL, "ih_create failed");

    double t_build0 = now_sec();
    hash_t build_root = ih_build(ih2, ds.key_ptrs, ds.value_ptrs,
                                 ds.value_lens, ds.count);
    double t_build1 = now_sec();

    printf("  ih_build:     %.2fs (%zu keys)\n", t_build1 - t_build0, ds.count);
    printf("  ih_entries:   %zu\n", ih_entry_count(ih2));
    printf("  root:         ");
    {
        char hex[67];
        hash_to_hex(&build_root, hex);
        printf("%s\n", hex);
    }

    // Verify roots match
    bool roots_match = hash_equal(&current_root, &build_root);
    printf("  roots match:  %s\n", roots_match ? "YES" : "NO");
    if (!roots_match) {
        printf("  WARNING: incremental and full build roots differ!\n");
    }

    printf("\n--- Summary ---\n");
    printf("  ih_build (full):    %.2fs\n", t_build1 - t_build0);
    printf("  ih_update (avg):    %.2fms\n", avg_mpt);
    if (avg_mpt > 0) {
        printf("  speedup:            %.1fx\n",
               (t_build1 - t_build0) * 1000.0 / avg_mpt);
    }
    printf("\n");

    // Cleanup
    free_full_dataset(&ds);
    ih_destroy(ih);
    ih_destroy(ih2);
    dl_destroy(dl);
    unlink(STATE_PATH);
    unlink(TRIE_PATH);

    printf("============================================\n");
    if (roots_match) {
        printf("  BENCHMARK COMPLETE — ROOTS MATCH\n");
    } else {
        printf("  BENCHMARK COMPLETE — ROOT MISMATCH\n");
    }
    printf("============================================\n");

    return roots_match ? 0 : 1;
}
