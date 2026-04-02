#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mem_art.h"

/*
 * Measure mem_art memory for large account index.
 * Key = addr_hash[32] (random), Value = uint32_t idx.
 */

static long get_rss_kb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    long pages = 0;
    fscanf(f, "%*ld %ld", &pages);
    fclose(f);
    return pages * 4;
}

static uint64_t rng_state = 12345678901ULL;
static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static void rng_bytes(uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++)
        out[i] = (uint8_t)(rng_next() & 0xff);
}

int main(void) {
    printf("mem_art account index benchmark (key=32B, value=4B uint32_t)\n\n");

    mem_art_t tree;
    mem_art_init(&tree);

    long rss_before = get_rss_kb();
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint32_t milestones[] = {
        1000000, 5000000, 10000000, 25000000,
        50000000, 100000000, 200000000, 300000000
    };
    int mi = 0;

    uint8_t key[32];
    uint32_t total = 300000000;

    for (uint32_t i = 0; i < total; i++) {
        rng_bytes(key, 32);
        mem_art_insert(&tree, key, 32, &i, sizeof(uint32_t));

        if (mi < 8 && i + 1 == milestones[mi]) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                        (t1.tv_nsec - t0.tv_nsec) / 1e6;
            long rss_now = get_rss_kb();
            long rss_delta = rss_now - rss_before;

            printf("  %9uM keys | arena_used %6zu MB | arena_cap %6zu MB | RSS %6ld MB | per_key %5.1f B | %.1f s\n",
                   milestones[mi] / 1000000,
                   tree.arena_used / (1024*1024),
                   tree.arena_cap / (1024*1024),
                   rss_delta / 1024,
                   (double)tree.arena_used / milestones[mi],
                   ms / 1000.0);
            mi++;
        }
    }

    mem_art_destroy(&tree);
    return 0;
}
