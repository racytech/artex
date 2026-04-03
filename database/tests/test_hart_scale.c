/**
 * hashed_art scale test — measure arena/RSS at 50M, 100M, 150M keys.
 */
#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

static void make_key(uint32_t seed, uint8_t out[32]) {
    memset(out, 0, 32);
    memcpy(out, &seed, 4);
    hash_t h = hash_keccak256(out, 32);
    memcpy(out, h.bytes, 32);
}

static size_t get_rss_mb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    long pages = 0;
    long dummy;
    if (fscanf(f, "%ld %ld", &dummy, &pages) != 2) pages = 0;
    fclose(f);
    return (size_t)(pages * 4096) / (1024 * 1024);
}

int main(int argc, char **argv) {
    int max_millions = 150;
    if (argc > 1) max_millions = atoi(argv[1]);

    printf("hashed_art scale test: up to %dM keys (value_size=32)\n", max_millions);
    printf("%-12s %12s %12s %12s %12s\n",
           "entries", "arena_MB", "arena_cap_MB", "RSS_MB", "time_s");

    hart_t tree;
    hart_init(&tree, 32);

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int total = max_millions * 1000000;
    int checkpoint = 10000000; /* every 10M */

    for (int i = 0; i < total; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)i, k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree, k, v);

        if ((i + 1) % checkpoint == 0) {
            struct timespec t1;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;

            printf("%-12d %12zu %12zu %12zu %12.1f\n",
                   i + 1,
                   tree.arena_used / (1024 * 1024),
                   tree.arena_cap / (1024 * 1024),
                   get_rss_mb(),
                   elapsed);
            fflush(stdout);
        }
    }

    printf("\nfinal: size=%zu arena=%zu/%zu (%zu MB / %zu MB)\n",
           hart_size(&tree),
           tree.arena_used, tree.arena_cap,
           tree.arena_used / (1024*1024),
           tree.arena_cap / (1024*1024));
    printf("RSS: %zu MB\n", get_rss_mb());

    hart_destroy(&tree);
    return 0;
}
