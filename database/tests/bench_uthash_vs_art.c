/*
 * uthash vs mem_art Benchmark
 *
 * Compares uthash (hash table) against mem_art (arena ART) for the
 * evm_state cache workload: accounts (20B key) and storage (52B key).
 *
 * Operations: insert, lookup, mixed (30% insert + 70% lookup), delete, destroy.
 * Scales: 100, 1K, 10K, 100K, 1M entries.
 *
 * Usage: ./bench_uthash_vs_art
 */

#include "../include/mem_art.h"
#include "uthash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

// ============================================================================
// Configuration
// ============================================================================

#define MAX_KEY_SIZE   52   // addr[20] + slot[32]
#define MAX_VAL_SIZE   80   // cached_account size
#define WARMUP_ITERS    2
#define MEASURE_ITERS   5

// ============================================================================
// RNG (same as other benchmarks)
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
// Key generation
// ============================================================================

static void generate_keys(uint8_t *keys, uint64_t n, int key_size, uint64_t seed) {
    rng_t rng = rng_create(seed);
    for (uint64_t i = 0; i < n; i++) {
        uint8_t *k = keys + i * key_size;
        for (int j = 0; j < key_size; j += 8) {
            uint64_t r = rng_next(&rng);
            int copy = (key_size - j) < 8 ? (key_size - j) : 8;
            memcpy(k + j, &r, copy);
        }
    }
}

static uint8_t dummy_value[MAX_VAL_SIZE];

// ============================================================================
// Timing
// ============================================================================

static double elapsed_us(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1e6 +
           (end->tv_nsec - start->tv_nsec) / 1e3;
}

// ============================================================================
// uthash entry
// ============================================================================

typedef struct {
    uint8_t key[MAX_KEY_SIZE];
    uint8_t value[MAX_VAL_SIZE];
    UT_hash_handle hh;
} uthash_entry_t;

// ============================================================================
// Memory measurement
// ============================================================================

static size_t get_rss_kb(void) {
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
    return rss_kb;
}

// ============================================================================
// uthash benchmarks
// ============================================================================

static double bench_insert_uthash(const uint8_t *keys, uint64_t n,
                                   int key_size, int val_size) {
    uthash_entry_t *table = NULL;
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);

    // Cleanup
    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }
    return us;
}

static double bench_lookup_uthash(const uint8_t *keys, uint64_t n,
                                   int key_size) {
    // Build table first
    uthash_entry_t *table = NULL;
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, MAX_VAL_SIZE);
        HASH_ADD(hh, table, key, key_size, e);
    }

    struct timespec t0, t1;
    volatile int found = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e;
        HASH_FIND(hh, table, keys + i * key_size, key_size, e);
        if (e) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);

    // Cleanup
    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }
    return us;
}

static double bench_mixed_uthash(const uint8_t *keys, uint64_t n,
                                  int key_size, int val_size) {
    uthash_entry_t *table = NULL;
    struct timespec t0, t1;
    volatile int found = 0;

    // Pre-insert 30% so lookups have something to find
    uint64_t insert_count = n * 3 / 10;
    for (uint64_t i = 0; i < insert_count; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);
    // 70% lookups into existing keys
    for (uint64_t i = 0; i < n * 7 / 10; i++) {
        uthash_entry_t *e;
        uint64_t idx = i % insert_count;
        HASH_FIND(hh, table, keys + idx * key_size, key_size, e);
        if (e) found++;
    }
    // 30% inserts of new keys
    for (uint64_t i = insert_count; i < insert_count + n * 3 / 10; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);

    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }
    return us;
}

static double bench_delete_uthash(const uint8_t *keys, uint64_t n,
                                   int key_size, int val_size) {
    // Build table
    uthash_entry_t *table = NULL;
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }

    struct timespec t0, t1;

    // Delete every 5th entry
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i += 5) {
        uthash_entry_t *e;
        HASH_FIND(hh, table, keys + i * key_size, key_size, e);
        if (e) {
            HASH_DEL(table, e);
            free(e);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);

    // Cleanup remaining
    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }
    return us;
}

static double bench_destroy_uthash(const uint8_t *keys, uint64_t n,
                                    int key_size, int val_size) {
    // Build table
    uthash_entry_t *table = NULL;
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }

    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    return elapsed_us(&t0, &t1);
}

// ============================================================================
// mem_art benchmarks
// ============================================================================

static double bench_insert_art(const uint8_t *keys, uint64_t n,
                                int key_size, int val_size) {
    mem_art_t tree;
    mem_art_init(&tree);
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);
    mem_art_destroy(&tree);
    return us;
}

static double bench_lookup_art(const uint8_t *keys, uint64_t n,
                                int key_size, int val_size) {
    mem_art_t tree;
    mem_art_init(&tree);
    for (uint64_t i = 0; i < n; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }

    struct timespec t0, t1;
    volatile int found = 0;
    size_t vlen;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i++) {
        const void *v = mem_art_get(&tree, keys + i * key_size, key_size, &vlen);
        if (v) found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);
    mem_art_destroy(&tree);
    return us;
}

static double bench_mixed_art(const uint8_t *keys, uint64_t n,
                               int key_size, int val_size) {
    mem_art_t tree;
    mem_art_init(&tree);
    struct timespec t0, t1;
    volatile int found = 0;
    size_t vlen;

    uint64_t insert_count = n * 3 / 10;
    for (uint64_t i = 0; i < insert_count; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n * 7 / 10; i++) {
        uint64_t idx = i % insert_count;
        const void *v = mem_art_get(&tree, keys + idx * key_size, key_size, &vlen);
        if (v) found++;
    }
    for (uint64_t i = insert_count; i < insert_count + n * 3 / 10; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);
    mem_art_destroy(&tree);
    return us;
}

static double bench_delete_art(const uint8_t *keys, uint64_t n,
                                int key_size, int val_size) {
    mem_art_t tree;
    mem_art_init(&tree);
    for (uint64_t i = 0; i < n; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }

    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint64_t i = 0; i < n; i += 5) {
        mem_art_delete(&tree, keys + i * key_size, key_size);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double us = elapsed_us(&t0, &t1);
    mem_art_destroy(&tree);
    return us;
}

static double bench_destroy_art(const uint8_t *keys, uint64_t n,
                                 int key_size, int val_size) {
    mem_art_t tree;
    mem_art_init(&tree);
    for (uint64_t i = 0; i < n; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }

    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    mem_art_destroy(&tree);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    return elapsed_us(&t0, &t1);
}

// ============================================================================
// Runner
// ============================================================================

static int cmp_double(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static double median(double *arr, int n) {
    qsort(arr, n, sizeof(double), cmp_double);
    return arr[n / 2];
}

typedef double (*bench_fn)(const uint8_t *keys, uint64_t n,
                            int key_size, int val_size);

static double run_bench(bench_fn fn, const uint8_t *keys, uint64_t n,
                         int key_size, int val_size) {
    // Warmup
    for (int i = 0; i < WARMUP_ITERS; i++)
        fn(keys, n, key_size, val_size);

    // Measure
    double results[MEASURE_ITERS];
    for (int i = 0; i < MEASURE_ITERS; i++)
        results[i] = fn(keys, n, key_size, val_size);

    return median(results, MEASURE_ITERS);
}

// Wrapper for lookup (no val_size param needed, but keep signature consistent)
static double bench_lookup_uthash_wrap(const uint8_t *keys, uint64_t n,
                                       int key_size, int val_size) {
    (void)val_size;
    return bench_lookup_uthash(keys, n, key_size);
}

// ============================================================================
// Format helpers
// ============================================================================

static void fmt_time(char *buf, size_t sz, double us) {
    if (us < 1.0)
        snprintf(buf, sz, "%6.0fns", us * 1000.0);
    else if (us < 1000.0)
        snprintf(buf, sz, "%6.1fus", us);
    else if (us < 1000000.0)
        snprintf(buf, sz, "%6.2fms", us / 1000.0);
    else
        snprintf(buf, sz, "%6.2fs ", us / 1000000.0);
}

// ============================================================================
// Memory comparison
// ============================================================================

static void bench_memory(int key_size, int val_size, const char *label) {
    uint64_t n = 1000000;
    uint8_t *keys = malloc(n * key_size);
    generate_keys(keys, n, key_size, 0x3E3BE4C40000ULL + key_size);

    // Measure uthash
    size_t rss_before = get_rss_kb();
    uthash_entry_t *table = NULL;
    for (uint64_t i = 0; i < n; i++) {
        uthash_entry_t *e = malloc(sizeof(uthash_entry_t));
        memcpy(e->key, keys + i * key_size, key_size);
        memcpy(e->value, dummy_value, val_size);
        HASH_ADD(hh, table, key, key_size, e);
    }
    size_t rss_uthash = get_rss_kb() - rss_before;

    // Cleanup uthash
    uthash_entry_t *cur, *tmp;
    HASH_ITER(hh, table, cur, tmp) {
        HASH_DEL(table, cur);
        free(cur);
    }

    // Measure mem_art
    rss_before = get_rss_kb();
    mem_art_t tree;
    mem_art_init(&tree);
    for (uint64_t i = 0; i < n; i++) {
        mem_art_insert(&tree, keys + i * key_size, key_size,
                       dummy_value, val_size);
    }
    size_t rss_art = get_rss_kb() - rss_before;

    printf("  %-25s uthash: %5zuMB (%3zu B/entry)  mem_art: %5zuMB (%3zu B/entry)\n",
           label,
           rss_uthash / 1024, rss_uthash * 1024 / n,
           rss_art / 1024, rss_art * 1024 / n);

    mem_art_destroy(&tree);
    free(keys);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    memset(dummy_value, 0xAB, MAX_VAL_SIZE);

    printf("\n============================================\n");
    printf("  uthash vs mem_art Benchmark\n");
    printf("============================================\n\n");

    struct {
        int key_size;
        int val_size;
        const char *label;
    } configs[] = {
        { 20, 80, "20-byte keys (account cache)" },
        { 52, 72, "52-byte keys (storage cache)" },
    };

    uint64_t scales[] = { 100, 1000, 10000, 100000, 1000000 };
    const char *scale_labels[] = { "100", "1K", "10K", "100K", "1M" };
    int num_scales = sizeof(scales) / sizeof(scales[0]);

    struct {
        const char *name;
        bench_fn uthash_fn;
        bench_fn art_fn;
    } ops[] = {
        { "insert",  bench_insert_uthash,       bench_insert_art },
        { "lookup",  bench_lookup_uthash_wrap,   bench_lookup_art },
        { "mixed",   bench_mixed_uthash,         bench_mixed_art },
        { "delete",  bench_delete_uthash,        bench_delete_art },
        { "destroy", bench_destroy_uthash,       bench_destroy_art },
    };
    int num_ops = sizeof(ops) / sizeof(ops[0]);

    for (int c = 0; c < 2; c++) {
        int ks = configs[c].key_size;
        int vs = configs[c].val_size;

        printf("--- %s ---\n\n", configs[c].label);

        // Header
        printf("  %5s |", "scale");
        for (int o = 0; o < num_ops; o++)
            printf("      %-7s      |", ops[o].name);
        printf("\n");
        printf("  %5s |", "");
        for (int o = 0; o < num_ops; o++)
            printf(" uthash   mem_art |");
        printf("\n");
        printf("  ------+");
        for (int o = 0; o < num_ops; o++)
            printf("------------------+");
        printf("\n");

        for (int s = 0; s < num_scales; s++) {
            uint64_t n = scales[s];

            // Need 2x keys for mixed (30% pre-insert + 30% new)
            uint64_t key_count = n * 2;
            uint8_t *keys = malloc(key_count * ks);
            generate_keys(keys, key_count, ks, 0xBE4C40000ULL + n + ks);

            printf("  %5s |", scale_labels[s]);

            for (int o = 0; o < num_ops; o++) {
                double t_uthash = run_bench(ops[o].uthash_fn, keys, n, ks, vs);
                double t_art = run_bench(ops[o].art_fn, keys, n, ks, vs);

                char buf_u[16], buf_a[16];
                fmt_time(buf_u, sizeof(buf_u), t_uthash);
                fmt_time(buf_a, sizeof(buf_a), t_art);
                printf(" %s %s |", buf_u, buf_a);
                fflush(stdout);
            }
            printf("\n");

            free(keys);
        }
        printf("\n");
    }

    // Memory comparison
    printf("--- Memory (1M entries) ---\n\n");
    bench_memory(20, 80, "20B keys, 80B values:");
    bench_memory(52, 72, "52B keys, 72B values:");
    printf("\n");

    return 0;
}
