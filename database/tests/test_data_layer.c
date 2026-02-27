/*
 * Data Layer Module Test
 *
 * Tests the data_layer library API (dl_create, dl_put, dl_get, dl_delete,
 * dl_merge, dl_stats). Same 4-phase structure as the PoC but uses only
 * the public API — no direct access to internals.
 *
 * Four phases:
 *   1. Buffer writes + first merge to disk
 *   2. Mixed reads (buffer hits + disk reads via dl_get)
 *   3. Updates, deletes, inserts + merge
 *   4. Scale test — many merge cycles, verify stable throughput
 *
 * Usage: ./test_data_layer [scale_millions]
 *   Default: 1M keys
 */

#include "../include/data_layer.h"
#include "../include/state_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

// ============================================================================
// Constants
// ============================================================================

#define KEY_SIZE       32
#define VALUE_LEN      32   // simulating storage slot

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
// Phase 1: Buffer Writes + First Merge
// ============================================================================

static bool phase1(data_layer_t *dl, uint64_t seed, uint64_t num_keys,
                   uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 1: Buffer Writes + First Merge\n");
    printf("========================================\n");
    printf("  keys: %" PRIu64 "\n\n", num_keys);

    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Write to buffer
    double t0 = now_sec();
    for (uint64_t i = 0; i < num_keys; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed, i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();

    dl_stats_t st = dl_stats(dl);
    printf("  buffer writes: %" PRIu64 " keys in %.2fs (%.1f Kk/s)\n",
           num_keys, t1 - t0, num_keys / (t1 - t0) / 1000.0);
    printf("  buffer size:   %" PRIu64 " entries\n", st.buffer_entries);
    printf("  RSS:           %zu MB\n", get_rss_mb());

    // Merge
    double t2 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t3 = now_sec();

    st = dl_stats(dl);
    printf("\n  merge: %" PRIu64 " entries in %.3fs (%.1f Kk/s)\n",
           merged, t3 - t2, merged / (t3 - t2) / 1000.0);
    printf("  index keys:    %" PRIu64 "\n", st.index_keys);
    printf("  RSS:           %zu MB\n", get_rss_mb());

    // Verify sample
    uint64_t check = num_keys < 10000 ? num_keys : 10000;
    uint64_t step = num_keys / check;
    uint64_t errors = 0;
    for (uint64_t i = 0; i < num_keys; i += step) {
        generate_key(key, seed, i);
        generate_value(value, seed, i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            errors++;
        } else if (got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }

    if (errors > 0) {
        printf("\n  FAIL: %" PRIu64 " verification errors\n", errors);
        return false;
    }
    printf("\n  verify: %" PRIu64 " keys OK\n", check);
    printf("  Phase 1: PASS\n");

    *next_key_idx = num_keys;
    return true;
}

// ============================================================================
// Phase 2: Mixed Reads (buffer + disk)
// ============================================================================

static bool phase2(data_layer_t *dl, uint64_t seed,
                   uint64_t disk_keys, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 2: Mixed Reads (buffer + disk)\n");
    printf("========================================\n");

    uint64_t new_keys = disk_keys / 5;
    if (new_keys < 1000) new_keys = 1000;
    uint64_t start = *next_key_idx;

    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Write new batch to buffer (don't merge)
    double t0 = now_sec();
    for (uint64_t i = 0; i < new_keys; i++) {
        generate_key(key, seed, start + i);
        generate_value(value, seed, start + i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();
    printf("  buffered %" PRIu64 " new keys in %.2fs\n", new_keys, t1 - t0);

    // Read from full keyspace
    uint64_t total_reads = disk_keys + new_keys;
    if (total_reads > 100000) total_reads = 100000;
    uint64_t found = 0, misses = 0;

    double t2 = now_sec();
    for (uint64_t i = 0; i < total_reads; i++) {
        uint64_t idx;
        if (i % 2 == 0) {
            idx = (i / 2) % disk_keys;
        } else {
            idx = start + ((i / 2) % new_keys);
        }
        generate_key(key, seed, idx);

        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (dl_get(dl, key, got, &got_len)) {
            found++;
            // Verify correctness
            generate_value(value, seed, idx);
            if (got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
                printf("  FAIL: wrong value for key idx %" PRIu64 "\n", idx);
                return false;
            }
        } else {
            misses++;
        }
    }
    double t3 = now_sec();

    printf("  reads:   %" PRIu64 " in %.3fs (%.1f Kk/s)\n",
           total_reads, t3 - t2, total_reads / (t3 - t2) / 1000.0);
    printf("  found:   %" PRIu64 "\n", found);
    printf("  misses:  %" PRIu64 "\n", misses);
    printf("  RSS:     %zu MB\n", get_rss_mb());

    if (misses > 0) {
        printf("\n  FAIL: unexpected misses\n");
        return false;
    }

    // Merge the buffer keys to disk
    double t4 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t5 = now_sec();
    printf("\n  merge: %" PRIu64 " entries in %.3fs\n", merged, t5 - t4);

    printf("  Phase 2: PASS\n");
    *next_key_idx = start + new_keys;
    return true;
}

// ============================================================================
// Phase 3: Updates + Deletes + Inserts + Merge
// ============================================================================

static bool phase3(data_layer_t *dl, uint64_t seed,
                   uint64_t disk_keys, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 3: Updates + Deletes + Inserts\n");
    printf("========================================\n");

    uint64_t num_updates = disk_keys / 5;
    uint64_t num_deletes = disk_keys / 10;
    uint64_t num_inserts = disk_keys / 10;
    if (num_updates < 100) num_updates = 100;
    if (num_deletes < 50) num_deletes = 50;
    if (num_inserts < 50) num_inserts = 50;

    uint64_t new_start = *next_key_idx;
    uint8_t key[KEY_SIZE], value[VALUE_LEN];

    // Updates: rewrite existing keys with new values (different seed)
    double t0 = now_sec();
    for (uint64_t i = 0; i < num_updates; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed + 1, i);
        dl_put(dl, key, value, VALUE_LEN);
    }

    // Deletes
    uint64_t del_start = num_updates;
    for (uint64_t i = 0; i < num_deletes; i++) {
        generate_key(key, seed, del_start + i);
        dl_delete(dl, key);
    }

    // Inserts: new keys
    for (uint64_t i = 0; i < num_inserts; i++) {
        generate_key(key, seed, new_start + i);
        generate_value(value, seed, new_start + i);
        dl_put(dl, key, value, VALUE_LEN);
    }
    double t1 = now_sec();

    dl_stats_t st = dl_stats(dl);
    printf("  updates:  %" PRIu64 "\n", num_updates);
    printf("  deletes:  %" PRIu64 "\n", num_deletes);
    printf("  inserts:  %" PRIu64 "\n", num_inserts);
    printf("  buffer:   %" PRIu64 " entries in %.3fs\n", st.buffer_entries, t1 - t0);

    // Merge
    dl_stats_t before = st;
    double t2 = now_sec();
    uint64_t merged = dl_merge(dl);
    double t3 = now_sec();

    st = dl_stats(dl);
    printf("\n  merge: %" PRIu64 " entries in %.3fs (%.1f Kk/s)\n",
           merged, t3 - t2, merged / (t3 - t2) / 1000.0);
    printf("  index keys: %" PRIu64 " (was %" PRIu64 ")\n",
           st.index_keys, before.index_keys);
    printf("  free slots: %u\n", st.free_slots);
    printf("  RSS: %zu MB\n", get_rss_mb());

    // Verify updates
    uint64_t errors = 0;
    for (uint64_t i = 0; i < num_updates && i < 1000; i++) {
        generate_key(key, seed, i);
        generate_value(value, seed + 1, i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len) ||
            got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " update verification errors\n", errors);
        return false;
    }

    // Verify deletes
    for (uint64_t i = 0; i < num_deletes && i < 1000; i++) {
        generate_key(key, seed, del_start + i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (dl_get(dl, key, got, &got_len)) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " deleted keys still found\n", errors);
        return false;
    }

    // Verify new inserts
    for (uint64_t i = 0; i < num_inserts && i < 1000; i++) {
        generate_key(key, seed, new_start + i);
        generate_value(value, seed, new_start + i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len) ||
            got_len != VALUE_LEN || memcmp(got, value, VALUE_LEN) != 0) {
            errors++;
        }
    }
    if (errors > 0) {
        printf("  FAIL: %" PRIu64 " new insert verification errors\n", errors);
        return false;
    }

    printf("\n  verify: updates OK, deletes OK, inserts OK\n");
    printf("  Phase 3: PASS\n");

    *next_key_idx = new_start + num_inserts;
    return true;
}

// ============================================================================
// Phase 4: Scale Test — Many Merge Cycles
// ============================================================================

static bool phase4(data_layer_t *dl, uint64_t seed,
                   uint64_t total_target, uint64_t *next_key_idx) {
    printf("\n========================================\n");
    printf("Phase 4: Scale Test (many merge cycles)\n");
    printf("========================================\n");

    uint64_t keys_per_cycle = 500000;  // 500K per merge cycle
    uint64_t current = *next_key_idx;
    uint64_t remaining = (total_target > current) ? (total_target - current) : 0;
    uint64_t num_cycles = remaining / keys_per_cycle;
    if (num_cycles < 2) num_cycles = 2;
    if (remaining < keys_per_cycle * 2) {
        keys_per_cycle = remaining / 2;
        if (keys_per_cycle == 0) keys_per_cycle = 1000;
    }

    printf("  target:       %" PRIu64 " total keys\n", total_target);
    printf("  remaining:    %" PRIu64 "\n", remaining);
    printf("  cycles:       %" PRIu64 " x %" PRIu64 " keys\n",
           num_cycles, keys_per_cycle);
    printf("\n");

    uint8_t key[KEY_SIZE], value[VALUE_LEN];
    double total_merge_time = 0;

    for (uint64_t cycle = 0; cycle < num_cycles; cycle++) {
        double t0 = now_sec();
        for (uint64_t i = 0; i < keys_per_cycle; i++) {
            generate_key(key, seed, current + i);
            generate_value(value, seed, current + i);
            dl_put(dl, key, value, VALUE_LEN);
        }
        double t1 = now_sec();

        double t2 = now_sec();
        uint64_t merged = dl_merge(dl);
        double t3 = now_sec();
        total_merge_time += (t3 - t2);

        current += keys_per_cycle;

        dl_stats_t st = dl_stats(dl);
        size_t rss = get_rss_mb();

        printf("  cycle %3" PRIu64 " | %" PRIu64 "k merged | "
               "write %.1f Kk/s | merge %.1f Kk/s (%.3fs) | "
               "index %" PRIu64 "k | RSS %zu MB\n",
               cycle + 1, merged / 1000,
               keys_per_cycle / (t1 - t0) / 1000.0,
               merged / (t3 - t2) / 1000.0, t3 - t2,
               st.index_keys / 1000, rss);
        fflush(stdout);
    }

    // Final verification: sample 10000 random keys
    printf("\n  verifying...");
    fflush(stdout);
    uint64_t check = current < 10000 ? current : 10000;
    uint64_t step = current / check;
    uint64_t errors = 0;
    for (uint64_t i = 0; i < current; i += step) {
        generate_key(key, seed, i);
        uint8_t got[VALUE_LEN];
        uint16_t got_len = 0;
        bool found = dl_get(dl, key, got, &got_len);
        if (found && got_len != VALUE_LEN) {
            errors++;
        }
    }
    printf(" %" PRIu64 " keys sampled, %" PRIu64 " errors\n", check, errors);

    if (errors > 0) {
        printf("  FAIL\n");
        return false;
    }

    dl_stats_t st = dl_stats(dl);
    printf("\n  ============================================\n");
    printf("  Phase 4 Summary\n");
    printf("  ============================================\n");
    printf("  index keys:      %" PRIu64 "\n", st.index_keys);
    printf("  merge cycles:    %" PRIu64 "\n", num_cycles);
    printf("  avg merge time:  %.3fs\n", total_merge_time / num_cycles);
    printf("  total merged:    %" PRIu64 "\n", st.total_merged);
    printf("  free slots:      %u\n", st.free_slots);
    printf("  RSS:             %zu MB\n", get_rss_mb());
    printf("  ============================================\n");
    printf("  Phase 4: PASS\n");

    *next_key_idx = current;
    return true;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t scale_millions = 1;
    if (argc >= 2) {
        scale_millions = (uint64_t)atoll(argv[1]);
        if (scale_millions == 0) {
            fprintf(stderr, "Usage: %s [scale_millions]\n", argv[0]);
            return 1;
        }
    }

    uint64_t total_keys = scale_millions * 1000000ULL;
    uint64_t seed = 0x0000000069a11ab9ULL;
    const char *state_path = "/tmp/art_dl_test_state.dat";

    printf("============================================\n");
    printf("Data Layer Module Test\n");
    printf("============================================\n");
    printf("scale:      %" PRIu64 "M keys\n", scale_millions);
    printf("value size: %d bytes\n", VALUE_LEN);
    printf("slot size:  %d bytes\n", STATE_STORE_SLOT_SIZE);
    printf("RSS:        %zu MB\n", get_rss_mb());

    data_layer_t *dl = dl_create(state_path, KEY_SIZE, 4);
    if (!dl) {
        fprintf(stderr, "FAIL: dl_create\n");
        return 1;
    }

    uint64_t next_key = 0;
    double t_start = now_sec();

    // Phase 1: 10% of keys for initial load
    uint64_t phase1_keys = total_keys / 10;
    if (phase1_keys < 1000) phase1_keys = 1000;

    dl_stats_t st = dl_stats(dl);

    if (!phase1(dl, seed, phase1_keys, &next_key)) {
        dl_destroy(dl);
        unlink(state_path);
        return 1;
    }

    st = dl_stats(dl);
    if (!phase2(dl, seed, st.index_keys, &next_key)) {
        dl_destroy(dl);
        unlink(state_path);
        return 1;
    }

    st = dl_stats(dl);
    if (!phase3(dl, seed, st.index_keys, &next_key)) {
        dl_destroy(dl);
        unlink(state_path);
        return 1;
    }

    if (!phase4(dl, seed, total_keys, &next_key)) {
        dl_destroy(dl);
        unlink(state_path);
        return 1;
    }

    double t_end = now_sec();

    st = dl_stats(dl);
    printf("\n============================================\n");
    printf("ALL PHASES PASSED\n");
    printf("============================================\n");
    printf("total time:  %.1fs\n", t_end - t_start);
    printf("total keys:  %" PRIu64 "\n", st.index_keys);
    printf("total merge: %" PRIu64 "\n", st.total_merged);
    printf("final RSS:   %zu MB\n", get_rss_mb());
    printf("============================================\n");

    dl_destroy(dl);
    unlink(state_path);
    return 0;
}
