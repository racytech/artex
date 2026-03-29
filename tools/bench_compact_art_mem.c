/**
 * Measure compact_art memory usage at scale.
 * Inserts N random 32-byte keys with 8-byte values (compact_leaves=true).
 * Reports RSS and pool usage at intervals.
 *
 * Usage: ./bench_compact_art_mem [num_keys_millions]
 */

#include "compact_art.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static size_t get_rss_bytes(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    long pages = 0;
    if (fscanf(f, "%*ld %ld", &pages) != 1) pages = 0;
    fclose(f);
    return (size_t)pages * 4096;
}

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

/* Dummy key_fetch — not needed for insert-only benchmark */
static bool dummy_fetch(const void *value, uint8_t *key_out, void *ctx) {
    (void)value; (void)key_out; (void)ctx;
    return false;
}

int main(int argc, char **argv) {
    uint64_t num_keys = 10000000; /* 10M default */
    if (argc >= 2) num_keys = (uint64_t)atol(argv[1]) * 1000000ULL;

    fprintf(stderr, "compact_art memory benchmark: %lu M keys\n",
            (unsigned long)(num_keys / 1000000));

    compact_art_t tree;
    if (!compact_art_init(&tree, 32, 8, true, dummy_fetch, NULL)) {
        fprintf(stderr, "FAIL: init\n");
        return 1;
    }

    size_t rss0 = get_rss_bytes();
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint64_t value = 0;
    uint64_t report_interval = num_keys / 20;
    if (report_interval < 1000000) report_interval = 1000000;

    fprintf(stderr, "%12s  %10s  %10s  %10s  %10s  %8s\n",
            "keys", "RSS(MB)", "nodes(MB)", "leaves(MB)", "bytes/key", "keys/s");

    for (uint64_t i = 0; i < num_keys; i++) {
        /* Generate key: keccak256(i) — uniform distribution like Ethereum */
        uint8_t key[32];
        keccak((const uint8_t *)&i, sizeof(i), key);

        value = i;
        compact_art_insert(&tree, key, &value);

        if ((i + 1) % report_interval == 0 || i + 1 == num_keys) {
            size_t rss = get_rss_bytes();
            size_t used_rss = rss > rss0 ? rss - rss0 : 0;
            double bytes_per_key = (double)used_rss / (double)(i + 1);

            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            double elapsed = (now.tv_sec - t0.tv_sec) +
                             (now.tv_nsec - t0.tv_nsec) / 1e9;
            double kps = (i + 1) / elapsed;

            fprintf(stderr, "%12lu  %10.1f  %10.1f  %10.1f  %10.1f  %8.0f\n",
                    (unsigned long)(i + 1),
                    (double)used_rss / (1024 * 1024),
                    (double)tree.nodes.used / (1024 * 1024),
                    (double)tree.leaves.used / (1024 * 1024),
                    bytes_per_key, kps / 1000);
        }
    }

    fprintf(stderr, "\nFinal: %lu keys, %zu entries in tree\n",
            (unsigned long)num_keys, compact_art_size(&tree));
    fprintf(stderr, "  node pool: %.1f MB used / %.1f MB reserved\n",
            (double)tree.nodes.used / (1024*1024),
            (double)tree.nodes.reserved / (1024*1024));
    fprintf(stderr, "  leaf pool: %.1f MB used / %.1f MB reserved\n",
            (double)tree.leaves.used / (1024*1024),
            (double)tree.leaves.reserved / (1024*1024));

    compact_art_destroy(&tree);
    return 0;
}
