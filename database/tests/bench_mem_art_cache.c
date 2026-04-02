#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mem_art.h"
#include "hash.h"

/*
 * Measure total memory when keeping many mem_arts alive simultaneously.
 * Simulates the hot account cache.
 */

static long get_rss_kb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    long pages = 0;
    fscanf(f, "%*ld %ld", &pages);
    fclose(f);
    return pages * 4; /* 4 KB pages */
}

static void fill_tree(mem_art_t *tree, int slot_count) {
    uint8_t slot_be[32];
    uint8_t value_be[32];
    for (int i = 0; i < slot_count; i++) {
        memset(slot_be, 0, 32);
        slot_be[28] = (i >> 24) & 0xFF;
        slot_be[29] = (i >> 16) & 0xFF;
        slot_be[30] = (i >> 8) & 0xFF;
        slot_be[31] = i & 0xFF;
        hash_t slot_hash = hash_keccak256(slot_be, 32);
        memset(value_be, 0, 32);
        value_be[31] = (i + 1) & 0xFF;
        value_be[30] = ((i + 1) >> 8) & 0xFF;
        mem_art_insert(tree, slot_hash.bytes, 32, value_be, 32);
    }
}

static void bench_cache(int num_accts, int slots_per_acct, size_t init_cap) {
    long rss_before = get_rss_kb();

    mem_art_t *trees = calloc(num_accts, sizeof(mem_art_t));
    if (!trees) { printf("  calloc failed for %d trees\n", num_accts); return; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    size_t total_used = 0;
    size_t total_cap = 0;
    for (int i = 0; i < num_accts; i++) {
        mem_art_init_cap(&trees[i], init_cap);
        fill_tree(&trees[i], slots_per_acct);
        total_used += trees[i].arena_used;
        total_cap += trees[i].arena_cap;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

    long rss_after = get_rss_kb();
    long rss_delta = rss_after - rss_before;

    printf("  %7d accts x %4d slots | used %6zu MB | cap %6zu MB | RSS %6ld MB | overhead %.0f%% | %.0f ms\n",
           num_accts, slots_per_acct,
           total_used / (1024*1024),
           total_cap / (1024*1024),
           rss_delta / 1024,
           100.0 * (1.0 - (double)total_used / (rss_delta * 1024)),
           ms);

    /* Cleanup */
    for (int i = 0; i < num_accts; i++)
        mem_art_destroy(&trees[i]);
    free(trees);
}

int main(void) {
    printf("mem_art cache benchmark — all trees alive simultaneously\n");
    printf("init_cap = 12288 (12 KB)\n\n");

    size_t init_cap = 12288;

    /* Varying account count, fixed 100 slots */
    printf("--- Fixed 100 slots/acct, varying account count ---\n");
    int counts[] = {1000, 5000, 10000, 50000, 100000, 500000, 1000000};
    for (int i = 0; i < 7; i++)
        bench_cache(counts[i], 100, init_cap);

    /* Varying slot count, fixed 10K accounts */
    printf("\n--- Fixed 10,000 accounts, varying slots/acct ---\n");
    int slots[] = {1, 5, 10, 50, 100, 500, 1000, 5000};
    for (int i = 0; i < 8; i++)
        bench_cache(10000, slots[i], init_cap);

    /* Real-world mix: most accounts small, few large */
    printf("\n--- Realistic mix: 10K accts (90%% x 10 slots + 9%% x 100 + 1%% x 1000) ---\n");
    {
        int total = 10000;
        long rss_before = get_rss_kb();
        mem_art_t *trees = calloc(total, sizeof(mem_art_t));
        size_t total_used = 0, total_cap = 0;

        for (int i = 0; i < total; i++) {
            int slots;
            if (i < total * 90 / 100) slots = 10;
            else if (i < total * 99 / 100) slots = 100;
            else slots = 1000;

            mem_art_init_cap(&trees[i], init_cap);
            fill_tree(&trees[i], slots);
            total_used += trees[i].arena_used;
            total_cap += trees[i].arena_cap;
        }

        long rss_after = get_rss_kb();
        long rss_delta = rss_after - rss_before;
        printf("  used %zu MB | cap %zu MB | RSS %ld MB | avg %zu B/acct\n",
               total_used / (1024*1024), total_cap / (1024*1024),
               rss_delta / 1024, total_used / total);

        for (int i = 0; i < total; i++)
            mem_art_destroy(&trees[i]);
        free(trees);
    }

    /* Same mix at 100K and 1M */
    printf("\n--- Realistic mix at scale ---\n");
    int scale_counts[] = {100000, 500000};
    for (int s = 0; s < 2; s++) {
        int total = scale_counts[s];
        long rss_before = get_rss_kb();
        mem_art_t *trees = calloc(total, sizeof(mem_art_t));
        if (!trees) { printf("  calloc failed for %d\n", total); continue; }
        size_t total_used = 0, total_cap = 0;

        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        for (int i = 0; i < total; i++) {
            int slots;
            if (i < total * 90 / 100) slots = 10;
            else if (i < total * 99 / 100) slots = 100;
            else slots = 1000;

            mem_art_init_cap(&trees[i], init_cap);
            fill_tree(&trees[i], slots);
            total_used += trees[i].arena_used;
            total_cap += trees[i].arena_cap;
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

        long rss_after = get_rss_kb();
        long rss_delta = rss_after - rss_before;
        printf("  %7d accts (90/9/1 mix) | used %6zu MB | cap %6zu MB | RSS %6ld MB | avg %zu B/acct | %.0f ms\n",
               total, total_used / (1024*1024), total_cap / (1024*1024),
               rss_delta / 1024, total_used / total, ms);

        for (int i = 0; i < total; i++)
            mem_art_destroy(&trees[i]);
        free(trees);
    }

    return 0;
}
