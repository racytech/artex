/*
 * mem_art Index Memory Benchmark
 *
 * Measures memory usage of in-memory ART as a pure index:
 *   - 32-byte keys (keccak256)
 *   - 12-byte values (8B file offset + 4B value length)
 *
 * Reports RSS at each milestone.
 *
 * Usage: ./bench_mem_art_index [target_millions]
 *   Default: 10M keys
 */

#include "../include/mem_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

#define KEY_SIZE    32
#define VALUE_SIZE  12   // 8B offset + 4B length (index entry)

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

int main(int argc, char *argv[]) {
    uint64_t target_millions = 10;
    if (argc >= 2) {
        target_millions = (uint64_t)atoll(argv[1]);
        if (target_millions == 0) {
            fprintf(stderr, "Usage: %s [target_millions]\n", argv[0]);
            return 1;
        }
    }

    uint64_t target = target_millions * 1000000ULL;
    uint64_t seed = 0x0000000069a11ab9ULL;

    printf("\n=== mem_art Index Memory Benchmark ===\n");
    printf("  target:     %" PRIu64 "M keys\n", target_millions);
    printf("  key_size:   %d bytes\n", KEY_SIZE);
    printf("  value_size: %d bytes (index: offset+length)\n", VALUE_SIZE);
    printf("  RSS before: %zu MB\n", get_rss_mb());
    printf("\n");

    mem_art_t tree;
    mem_art_init(&tree);

    uint8_t key[KEY_SIZE];
    uint8_t value[VALUE_SIZE];
    memset(value, 0, VALUE_SIZE);  // dummy index entry

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    uint64_t milestone = 1000000;  // report every 1M

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, seed, i);

        // Simulate index value: offset = i * 256, length = 200
        uint64_t offset = i * 256;
        uint32_t len = 200;
        memcpy(value, &offset, 8);
        memcpy(value + 8, &len, 4);

        if (!mem_art_insert(&tree, key, KEY_SIZE, value, VALUE_SIZE)) {
            fprintf(stderr, "FAIL: insert failed at %" PRIu64 "\n", i);
            mem_art_destroy(&tree);
            return 1;
        }

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - last.tv_sec) + (t1.tv_nsec - last.tv_nsec) / 1e9;
            double total = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
            double kps = milestone / elapsed / 1000.0;
            size_t rss = get_rss_mb();
            double bytes_per_key = (double)(rss * 1024 * 1024) / (double)(i + 1);

            printf("  %6" PRIu64 "M keys | RSS %5zu MB | %.0f B/key | "
                   "%.0f Kk/s (avg %.0f Kk/s)\n",
                   (i + 1) / 1000000, rss, bytes_per_key,
                   kps, (i + 1) / total / 1000.0);
            fflush(stdout);
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double total = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    size_t final_rss = get_rss_mb();

    printf("\n");
    printf("  ============================================\n");
    printf("  DONE\n");
    printf("  ============================================\n");
    printf("  keys:        %" PRIu64 "M\n", target_millions);
    printf("  RSS:         %zu MB (%.2f GB)\n", final_rss, final_rss / 1024.0);
    printf("  bytes/key:   %.0f B\n", (double)(final_rss * 1024 * 1024) / (double)target);
    printf("  time:        %.1fs (%.0f Kkeys/sec)\n", total, target / total / 1000.0);
    printf("  tree size:   %zu\n", mem_art_size(&tree));
    printf("  ============================================\n\n");

    mem_art_destroy(&tree);
    return 0;
}
