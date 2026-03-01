/*
 * Index Structure Scale Benchmark
 *
 * Compares two implementations:
 *
 *   nibble_trie:  16-way nibble trie (in-memory, arena-allocated)
 *   compact_art:  in-memory ART (no persistence)
 *
 * Workload: 32B keys + 32B values (account index)
 *
 * Usage: ./bench_nibble_trie [target_millions] [mode]
 *   Modes: nt | cart | both
 *   Default: 1M keys, both
 */

#include "../include/nibble_trie.h"
#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>

/* ========================================================================
 * SplitMix64 RNG + key generation
 * ======================================================================== */

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

/* Account key: 32 bytes (simulates keccak256(address)) */
static void generate_key32(uint8_t key[32], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

/* Account value: 32 bytes */
static void generate_val32(uint8_t val[32], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9ABCDEF012345678ULL));
    for (int i = 0; i < 32; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(val + i, &r, 8);
    }
}

/* ========================================================================
 * Helpers
 * ======================================================================== */

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

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1e3 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

#define SEED 0x42424242BEEFCAFEULL

/* ========================================================================
 * Benchmark: nibble_trie — accounts (32B keys, 32B values)
 * ======================================================================== */

static void bench_nt(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== nibble_trie — Accounts (32B key + 32B val) ===\n");
    printf("  target: %" PRIu64 " keys (in-memory, arena-allocated)\n\n", target);

    nibble_trie_t t;
    if (!nt_init(&t, 32)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[32], val[32];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key32(key, SEED, i);
        generate_val32(val, SEED, i);
        nt_insert(&t, key, val);

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double ms = elapsed_ms(&last, &t1);
            double kks = (double)milestone / ms;
            printf("  insert       %8" PRIu64 " %8.0f %8.0f %8zu\n",
                   i + 1, elapsed_ms(&t0, &t1), kks, get_rss_mb());
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);
    printf("  INSERT TOTAL %8" PRIu64 " %8.0f %8.0f %8zu\n\n",
           target, insert_ms, (double)target / insert_ms, get_rss_mb());

    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key32(key, SEED, idx);
        if (nt_get(&t, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, elapsed_ms(&t0, &t1), (double)lookups / elapsed_ms(&t0, &t1),
           "", found);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_iterator_t *it = nt_iterator_create(&t);
    uint64_t iter_count = 0;
    while (nt_iterator_next(it)) iter_count++;
    nt_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, elapsed_ms(&t0, &t1),
           (double)iter_count / elapsed_ms(&t0, &t1));

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key32(key, SEED, i);
        if (nt_delete(&t, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, elapsed_ms(&t0, &t1),
           (double)deleted / elapsed_ms(&t0, &t1));

    printf("\n  Memory (RSS):  %zu MB\n", get_rss_mb());
    printf("  Tree size:     %" PRIu64 " keys\n", (uint64_t)nt_size(&t));

    nt_destroy(&t);
}

/* ========================================================================
 * Benchmark: compact_art — accounts (32B keys, 32B values)
 * ======================================================================== */

static void bench_cart(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== compact_art — Accounts (32B key + 32B val) ===\n");
    printf("  target: %" PRIu64 " keys (in-memory, no persistence)\n\n", target);

    compact_art_t tree;
    if (!compact_art_init(&tree, 32, 32)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[32], val[32];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key32(key, SEED, i);
        generate_val32(val, SEED, i);
        compact_art_insert(&tree, key, val);

        if ((i + 1) % milestone == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double ms = elapsed_ms(&last, &t1);
            double kks = (double)milestone / ms;
            printf("  insert       %8" PRIu64 " %8.0f %8.0f %8zu\n",
                   i + 1, elapsed_ms(&t0, &t1), kks, get_rss_mb());
            last = t1;
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);
    printf("  INSERT TOTAL %8" PRIu64 " %8.0f %8.0f %8zu\n\n",
           target, insert_ms, (double)target / insert_ms, get_rss_mb());

    printf("  COMMIT       %8s %8s %8s %8s (N/A — in-memory)\n",
           "", "", "", "");

    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key32(key, SEED, idx);
        if (compact_art_get(&tree, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, elapsed_ms(&t0, &t1), (double)lookups / elapsed_ms(&t0, &t1),
           "", found);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    compact_art_iterator_t *it = compact_art_iterator_create(&tree);
    uint64_t iter_count = 0;
    while (compact_art_iterator_next(it)) iter_count++;
    compact_art_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, elapsed_ms(&t0, &t1),
           (double)iter_count / elapsed_ms(&t0, &t1));

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key32(key, SEED, i);
        if (compact_art_delete(&tree, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, elapsed_ms(&t0, &t1),
           (double)deleted / elapsed_ms(&t0, &t1));

    printf("\n  Memory (RSS):  %zu MB\n", get_rss_mb());
    printf("  Tree size:     %" PRIu64 " keys\n",
           (uint64_t)compact_art_size(&tree));

    compact_art_destroy(&tree);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char **argv) {
    uint64_t target = 1000000;
    const char *mode = "both";

    if (argc > 1) target = (uint64_t)atol(argv[1]) * 1000000;
    if (argc > 2) mode = argv[2];
    if (target == 0) target = 1000000;

    printf("=== Index Structure Scale Benchmark ===\n");
    printf("  Target: %" PRIu64 " keys (%.1fM)\n", target, (double)target / 1e6);
    printf("  Mode:   %s\n", mode);

    if (strcmp(mode, "nt") == 0 || strcmp(mode, "both") == 0)
        bench_nt(target);
    if (strcmp(mode, "cart") == 0 || strcmp(mode, "both") == 0)
        bench_cart(target);

    printf("\nDone.\n");
    return 0;
}
