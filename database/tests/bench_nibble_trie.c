/*
 * Index Structure Scale Benchmark
 *
 * Compares three implementations:
 *   nibble_trie:  64-byte fixed-slot nibble trie (COW, file-backed)
 *   bitmap_art:   variable-size bitmap node ART (COW, file-backed)
 *   compact_art:  in-memory ART (no persistence)
 *
 * All use 32-byte keys + 32-byte values.
 *
 * Metrics: insert throughput, lookup throughput, delete throughput,
 *          commit latency, iterator scan speed, file/memory size.
 *
 * Usage: ./bench_nibble_trie [target_millions] [nt|bart|cart|both|all]
 *   Default: 1M keys, both (nt+bart)
 *   "all" runs nt + bart + cart
 */

#include "../include/nibble_trie.h"
#include "../include/bitmap_art.h"
#include "../include/compact_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#define KEY_SIZE    32
#define VALUE_SIZE  32

#define NT_PATH     "/tmp/bench_nt.dat"
#define BART_PATH   "/tmp/bench_bart.dat"

/* ========================================================================
 * SplitMix64 RNG + key generation (same as other benchmarks)
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

static void generate_value(uint8_t val[VALUE_SIZE], uint64_t seed, uint64_t index) {
    rng_t rng = rng_create(seed ^ (index * 0x9ABCDEF012345678ULL));
    uint64_t r0 = rng_next(&rng);
    uint64_t r1 = rng_next(&rng);
    uint64_t r2 = rng_next(&rng);
    uint64_t r3 = rng_next(&rng);
    memcpy(val,      &r0, 8);
    memcpy(val + 8,  &r1, 8);
    memcpy(val + 16, &r2, 8);
    memcpy(val + 24, &r3, 8);
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

static size_t file_size_mb(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    /* Actual disk usage (sparse file) */
    return (size_t)(st.st_blocks * 512) / (1024 * 1024);
}

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1e3 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

#define SEED 0x42424242BEEFCAFEULL

/* ========================================================================
 * Benchmark: nibble_trie
 * ======================================================================== */

static void bench_nt(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== nibble_trie Benchmark ===\n");
    printf("  target:     %" PRIu64 " keys\n", target);
    printf("  slot size:  64 bytes (fixed)\n");
    printf("  key+value:  32+32 = 64 bytes\n\n");

    unlink(NT_PATH);
    nibble_trie_t t;
    if (!nt_open(&t, NT_PATH)) {
        printf("  FAILED to open\n");
        return;
    }

    uint8_t key[KEY_SIZE], val[VALUE_SIZE];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    /* --- INSERT --- */
    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, SEED, i);
        generate_value(val, SEED, i);
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

    /* --- COMMIT --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_commit(&t);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double commit_ms = elapsed_ms(&t0, &t1);
    printf("  COMMIT       %8s %8.0f %8s %8zu\n",
           "", commit_ms, "", get_rss_mb());

    /* --- LOOKUP (random sample) --- */
    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;  /* pseudo-random access */
        generate_key(key, SEED, idx);
        if (nt_get(&t, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double lookup_ms = elapsed_ms(&t0, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, lookup_ms, (double)lookups / lookup_ms, "", found);

    /* --- ITERATOR SCAN --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_iterator_t *it = nt_iterator_create(&t);
    uint64_t iter_count = 0;
    while (nt_iterator_next(it)) iter_count++;
    nt_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double iter_ms = elapsed_ms(&t0, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, iter_ms, (double)iter_count / iter_ms);

    /* --- DELETE half --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key(key, SEED, i);
        if (nt_delete(&t, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double delete_ms = elapsed_ms(&t0, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, delete_ms, (double)deleted / delete_ms);

    /* --- COMMIT after delete --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_commit(&t);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double commit2_ms = elapsed_ms(&t0, &t1);
    printf("  COMMIT2      %8s %8.0f\n", "", commit2_ms);

    /* --- FILE SIZE --- */
    size_t disk_mb = file_size_mb(NT_PATH);
    printf("\n  File size (on disk): %zu MB\n", disk_mb);
    printf("  Tree size:           %" PRIu64 " keys\n", (uint64_t)nt_size(&t));

    /* --- REOPEN --- */
    nt_close(&t);
    clock_gettime(CLOCK_MONOTONIC, &t0);
    nt_open(&t, NT_PATH);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double reopen_ms = elapsed_ms(&t0, &t1);
    printf("  Reopen:              %.1f ms (size=%" PRIu64 ")\n",
           reopen_ms, (uint64_t)nt_size(&t));

    nt_close(&t);
    unlink(NT_PATH);
}

/* ========================================================================
 * Benchmark: bitmap_art
 * ======================================================================== */

static void bench_bart(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== bitmap_art Benchmark ===\n");
    printf("  target:     %" PRIu64 " keys\n", target);
    printf("  node:       44 + 4×N bytes (variable)\n");
    printf("  key+value:  32+32 = 64 bytes\n\n");

    unlink(BART_PATH);
    bitmap_art_t tree;
    if (!bart_open(&tree, BART_PATH, KEY_SIZE, VALUE_SIZE)) {
        printf("  FAILED to open\n");
        return;
    }

    uint8_t key[KEY_SIZE], val[VALUE_SIZE];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    /* --- INSERT --- */
    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, SEED, i);
        generate_value(val, SEED, i);
        bart_insert(&tree, key, val);

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

    /* --- COMMIT --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    bart_commit(&tree);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double commit_ms = elapsed_ms(&t0, &t1);
    printf("  COMMIT       %8s %8.0f %8s %8zu\n",
           "", commit_ms, "", get_rss_mb());

    /* --- LOOKUP (random sample) --- */
    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key(key, SEED, idx);
        if (bart_get(&tree, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double lookup_ms = elapsed_ms(&t0, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, lookup_ms, (double)lookups / lookup_ms, "", found);

    /* --- ITERATOR SCAN --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    bart_iterator_t *it = bart_iterator_create(&tree);
    uint64_t iter_count = 0;
    while (bart_iterator_next(it)) iter_count++;
    bart_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double iter_ms = elapsed_ms(&t0, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, iter_ms, (double)iter_count / iter_ms);

    /* --- DELETE half --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key(key, SEED, i);
        if (bart_delete(&tree, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double delete_ms = elapsed_ms(&t0, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, delete_ms, (double)deleted / delete_ms);

    /* --- COMMIT after delete --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    bart_commit(&tree);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double commit2_ms = elapsed_ms(&t0, &t1);
    printf("  COMMIT2      %8s %8.0f\n", "", commit2_ms);

    /* --- FILE SIZE --- */
    size_t disk_mb = file_size_mb(BART_PATH);
    printf("\n  File size (on disk): %zu MB\n", disk_mb);
    printf("  Tree size:           %" PRIu64 " keys\n", (uint64_t)bart_size(&tree));

    /* --- REOPEN --- */
    bart_close(&tree);
    clock_gettime(CLOCK_MONOTONIC, &t0);
    bart_open(&tree, BART_PATH, KEY_SIZE, VALUE_SIZE);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double reopen_ms = elapsed_ms(&t0, &t1);
    printf("  Reopen:              %.1f ms (size=%" PRIu64 ")\n",
           reopen_ms, (uint64_t)bart_size(&tree));

    bart_close(&tree);
    unlink(BART_PATH);
}

/* ========================================================================
 * Benchmark: compact_art (in-memory ART, no persistence)
 * ======================================================================== */

static void bench_cart(uint64_t target) {
    struct timespec t0, t1;

    printf("\n=== compact_art Benchmark ===\n");
    printf("  target:     %" PRIu64 " keys\n", target);
    printf("  node:       variable (Node4/16/32/48/256)\n");
    printf("  key+value:  32+32 = 64 bytes\n");
    printf("  persistence: NONE (in-memory only)\n\n");

    compact_art_t tree;
    if (!compact_art_init(&tree, KEY_SIZE, VALUE_SIZE)) {
        printf("  FAILED to init\n");
        return;
    }

    uint8_t key[KEY_SIZE], val[VALUE_SIZE];
    uint64_t milestone = target < 1000000 ? target / 10 : 1000000;
    if (milestone == 0) milestone = 1;

    /* --- INSERT --- */
    printf("  %-12s %8s %8s %8s %8s\n",
           "Phase", "Keys", "ms", "Kk/s", "RSS MB");
    printf("  %-12s %8s %8s %8s %8s\n",
           "-----", "----", "--", "----", "------");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    struct timespec last = t0;

    for (uint64_t i = 0; i < target; i++) {
        generate_key(key, SEED, i);
        generate_value(val, SEED, i);
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

    /* --- No commit (in-memory) --- */
    printf("  COMMIT       %8s %8s %8s %8s (N/A — in-memory)\n",
           "", "", "", "");

    /* --- LOOKUP (random sample) --- */
    uint64_t lookups = target < 1000000 ? target : 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t found = 0;
    for (uint64_t i = 0; i < lookups; i++) {
        uint64_t idx = (i * 7919) % target;
        generate_key(key, SEED, idx);
        if (compact_art_get(&tree, key) != NULL) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double lookup_ms = elapsed_ms(&t0, &t1);
    printf("  LOOKUP       %8" PRIu64 " %8.0f %8.0f %8s (found=%" PRIu64 ")\n",
           lookups, lookup_ms, (double)lookups / lookup_ms, "", found);

    /* --- ITERATOR SCAN --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    compact_art_iterator_t *it = compact_art_iterator_create(&tree);
    uint64_t iter_count = 0;
    while (compact_art_iterator_next(it)) iter_count++;
    compact_art_iterator_destroy(it);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double iter_ms = elapsed_ms(&t0, &t1);
    printf("  ITERATE      %8" PRIu64 " %8.0f %8.0f\n",
           iter_count, iter_ms, (double)iter_count / iter_ms);

    /* --- DELETE half --- */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t deleted = 0;
    for (uint64_t i = 0; i < target; i += 2) {
        generate_key(key, SEED, i);
        if (compact_art_delete(&tree, key)) deleted++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double delete_ms = elapsed_ms(&t0, &t1);
    printf("  DELETE half  %8" PRIu64 " %8.0f %8.0f\n",
           deleted, delete_ms, (double)deleted / delete_ms);

    /* --- Memory usage --- */
    printf("\n  Memory (RSS):        %zu MB\n", get_rss_mb());
    printf("  Tree size:           %" PRIu64 " keys\n",
           (uint64_t)compact_art_size(&tree));

    compact_art_destroy(&tree);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char **argv) {
    uint64_t target = 1000000;  /* default 1M */
    const char *mode = "both";

    if (argc > 1) target = (uint64_t)atol(argv[1]) * 1000000;
    if (argc > 2) mode = argv[2];
    if (target == 0) target = 1000000;

    printf("=== Index Structure Scale Benchmark ===\n");
    printf("  Target: %" PRIu64 " keys (%.1fM)\n",
           target, (double)target / 1e6);
    printf("  Mode:   %s\n", mode);

    bool run_all = strcmp(mode, "all") == 0;

    if (strcmp(mode, "nt") == 0 || strcmp(mode, "both") == 0 || run_all)
        bench_nt(target);

    if (strcmp(mode, "bart") == 0 || strcmp(mode, "both") == 0 || run_all)
        bench_bart(target);

    if (strcmp(mode, "cart") == 0 || run_all)
        bench_cart(target);

    printf("\nDone.\n");
    return 0;
}
