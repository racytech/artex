/*
 * compact_art vs mem_art Index Memory Benchmark
 *
 * Measures memory usage of both ART implementations as a pure index:
 *   - 32-byte keys (keccak256)
 *   - 12-byte values (8B file offset + 4B value length)
 *
 * Reports RSS at each milestone for direct comparison.
 *
 * Usage: ./bench_compact_art_index [target_millions] [compact|mem|both]
 *   Default: 10M keys, compact only
 */

#include "../include/compact_art.h"
#include "../include/mem_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

#define KEY_SIZE    32
#define VALUE_SIZE  12   // 8B offset + 4B length

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

// ============================================================================
// Benchmark: compact_art
// ============================================================================

static int bench_compact(uint64_t target, uint64_t seed) {
    printf("\n=== compact_art Index Benchmark ===\n");
    printf("  target:     %" PRIu64 "M keys\n", target / 1000000);
    printf("  key_size:   %d bytes\n", KEY_SIZE);
    printf("  value_size: %d bytes\n", VALUE_SIZE);
    printf("  RSS before: %zu MB\n", get_rss_mb());
    printf("\n");

    compact_art_t tree;
    if (!compact_art_init(&tree, KEY_SIZE, VALUE_SIZE)) {
        fprintf(stderr, "FAIL: compact_art_init\n");
        return 1;
    }

    uint8_t key[KEY_SIZE];
    uint8_t value[VALUE_SIZE];

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;
    uint64_t milestone = 1000000;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);

        uint64_t offset = i * 256;
        uint32_t len = 200;
        memcpy(value, &offset, 8);
        memcpy(value + 8, &len, 4);

        if (!compact_art_insert(&tree, key, value)) {
            fprintf(stderr, "FAIL: insert at %" PRIu64 "\n", i);
            compact_art_destroy(&tree);
            return 1;
        }

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - last.tv_sec) +
                            (t1.tv_nsec - last.tv_nsec) / 1e9;
            double total = (t1.tv_sec - t0.tv_sec) +
                          (t1.tv_nsec - t0.tv_nsec) / 1e9;
            double kps = milestone / elapsed / 1000.0;
            size_t rss = get_rss_mb();
            double bpk = (double)(rss * 1024 * 1024) / (double)(i + 1);

            printf("  %6" PRIu64 "M keys | RSS %5zu MB | %5.0f B/key | "
                   "%.0f Kk/s (avg %.0f Kk/s)\n",
                   (i + 1) / 1000000, rss, bpk,
                   kps, (i + 1) / total / 1000.0);
            fflush(stdout);
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double total = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    size_t final_rss = get_rss_mb();

    // Verify a sample of keys
    printf("\n  Verifying...");
    fflush(stdout);
    uint64_t check_count = target < 1000000 ? target : 1000000;
    uint64_t step = target / check_count;
    for (uint64_t i = 0; i < target; i += step) {
        generate_key(key, seed, i);
        const void *v = compact_art_get(&tree, key);
        if (!v) {
            fprintf(stderr, "\n  FAIL: key %" PRIu64 " not found!\n", i);
            compact_art_destroy(&tree);
            return 1;
        }
        uint64_t expected_offset = i * 256;
        uint64_t got_offset;
        memcpy(&got_offset, v, 8);
        if (got_offset != expected_offset) {
            fprintf(stderr, "\n  FAIL: key %" PRIu64 " wrong value!\n", i);
            compact_art_destroy(&tree);
            return 1;
        }
    }
    printf(" OK (%" PRIu64 " keys verified)\n", check_count);

    printf("\n  ============================================\n");
    printf("  compact_art DONE\n");
    printf("  ============================================\n");
    printf("  keys:        %" PRIu64 "M\n", target / 1000000);
    printf("  RSS:         %zu MB (%.2f GB)\n", final_rss, final_rss / 1024.0);
    printf("  bytes/key:   %.0f B\n",
           (double)(final_rss * 1024 * 1024) / (double)target);
    printf("  time:        %.1fs (%.0f Kkeys/sec)\n",
           total, target / total / 1000.0);
    printf("  tree size:   %zu\n", compact_art_size(&tree));
    printf("  ============================================\n\n");

    compact_art_destroy(&tree);
    return 0;
}

// ============================================================================
// Benchmark: mem_art (baseline)
// ============================================================================

static int bench_mem(uint64_t target, uint64_t seed) {
    printf("\n=== mem_art Index Benchmark (baseline) ===\n");
    printf("  target:     %" PRIu64 "M keys\n", target / 1000000);
    printf("  RSS before: %zu MB\n", get_rss_mb());
    printf("\n");

    art_tree_t tree;
    art_tree_init(&tree);

    uint8_t key[KEY_SIZE];
    uint8_t value[VALUE_SIZE];

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;
    uint64_t milestone = 1000000;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);

        uint64_t offset = i * 256;
        uint32_t len = 200;
        memcpy(value, &offset, 8);
        memcpy(value + 8, &len, 4);

        if (!art_insert(&tree, key, KEY_SIZE, value, VALUE_SIZE)) {
            fprintf(stderr, "FAIL: insert at %" PRIu64 "\n", i);
            art_tree_destroy(&tree);
            return 1;
        }

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - last.tv_sec) +
                            (t1.tv_nsec - last.tv_nsec) / 1e9;
            double total = (t1.tv_sec - t0.tv_sec) +
                          (t1.tv_nsec - t0.tv_nsec) / 1e9;
            double kps = milestone / elapsed / 1000.0;
            size_t rss = get_rss_mb();
            double bpk = (double)(rss * 1024 * 1024) / (double)(i + 1);

            printf("  %6" PRIu64 "M keys | RSS %5zu MB | %5.0f B/key | "
                   "%.0f Kk/s (avg %.0f Kk/s)\n",
                   (i + 1) / 1000000, rss, bpk,
                   kps, (i + 1) / total / 1000.0);
            fflush(stdout);
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double total = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    size_t final_rss = get_rss_mb();

    printf("\n  ============================================\n");
    printf("  mem_art DONE\n");
    printf("  ============================================\n");
    printf("  keys:        %" PRIu64 "M\n", target / 1000000);
    printf("  RSS:         %zu MB (%.2f GB)\n", final_rss, final_rss / 1024.0);
    printf("  bytes/key:   %.0f B\n",
           (double)(final_rss * 1024 * 1024) / (double)target);
    printf("  time:        %.1fs (%.0f Kkeys/sec)\n",
           total, target / total / 1000.0);
    printf("  tree size:   %zu\n", art_size(&tree));
    printf("  ============================================\n\n");

    art_tree_destroy(&tree);
    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char *argv[]) {
    uint64_t target_millions = 10;
    const char *mode = "compact";  // compact, mem, both

    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) {
            fprintf(stderr, "Usage: %s [target_millions] [compact|mem|both]\n",
                    argv[0]);
            return 1;
        }
    }
    if (argc >= 3) {
        mode = argv[2];
    }

    uint64_t target = target_millions * 1000000ULL;
    uint64_t seed = 0x0000000069a11ab9ULL;

    int rc = 0;

    if (strcmp(mode, "compact") == 0 || strcmp(mode, "both") == 0) {
        rc = bench_compact(target, seed);
        if (rc != 0) return rc;
    }

    if (strcmp(mode, "mem") == 0 || strcmp(mode, "both") == 0) {
        rc = bench_mem(target, seed);
        if (rc != 0) return rc;
    }

    return 0;
}
