#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mem_art.h"
#include "hash.h"

/*
 * Measure mem_art memory usage for per-account storage simulation.
 * key = keccak256(slot_be[32]), value = slot_value_be[32]
 */

static void insert_slots(mem_art_t *tree, int slot_count) {
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

static void bench_per_slot(size_t init_cap) {
    printf("\n--- init_cap = %zu ---\n", init_cap);
    printf("  %6s slots | %8s     | %8s     | %s\n", "", "used", "cap", "per_slot");
    printf("  ------------|------------|------------|----------\n");

    int counts[] = {1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 50000};
    int n = sizeof(counts) / sizeof(counts[0]);

    for (int i = 0; i < n; i++) {
        mem_art_t tree;
        mem_art_init_cap(&tree, init_cap);
        insert_slots(&tree, counts[i]);
        double per_slot = counts[i] > 0 ? (double)tree.arena_used / counts[i] : 0;
        printf("  %6d slots | used %8zu | cap %8zu | per_slot %6.1f bytes\n",
               counts[i], tree.arena_used, tree.arena_cap, per_slot);
        mem_art_destroy(&tree);
    }
}

static void bench_aggregate(size_t init_cap, int slots_per_acct) {
    printf("\n--- Aggregate: init_cap=%zu, %d slots/acct ---\n", init_cap, slots_per_acct);

    int acct_counts[] = {1000, 10000, 100000, 1000000};
    for (int a = 0; a < 4; a++) {
        int num_accts = acct_counts[a];
        size_t total_used = 0;
        size_t total_cap = 0;

        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        for (int j = 0; j < num_accts; j++) {
            mem_art_t tree;
            mem_art_init_cap(&tree, init_cap);
            insert_slots(&tree, slots_per_acct);
            total_used += tree.arena_used;
            total_cap += tree.arena_cap;
            mem_art_destroy(&tree);
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ms = (t1.tv_sec - t0.tv_sec) * 1000.0 + (t1.tv_nsec - t0.tv_nsec) / 1e6;

        printf("  %7d accts | used %6zu MB | cap %6zu MB | waste %.0f%% | avg %zu B/acct | %.0f ms\n",
               num_accts,
               total_used / (1024*1024),
               total_cap / (1024*1024),
               100.0 * (1.0 - (double)total_used / total_cap),
               total_used / num_accts, ms);
    }
}

int main(void) {
    printf("mem_art storage benchmark (key=32B slot_hash, value=32B)\n");

    size_t caps[] = {4096, 8192, 12288, 16384, 65536};
    for (int c = 0; c < 5; c++)
        bench_per_slot(caps[c]);

    printf("\n========================================\n");
    printf("Aggregate benchmarks (100 slots/acct)\n");
    printf("========================================\n");

    size_t agg_caps[] = {4096, 12288, 16384, 65536};
    for (int c = 0; c < 4; c++)
        bench_aggregate(agg_caps[c], 100);

    return 0;
}
