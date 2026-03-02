/*
 * MPT Trie Benchmark — Per-block commitment latency at scale
 *
 * Measures mpt_put + mpt_root per block as the trie grows.
 *
 * Usage: ./bench_mpt_trie [target_thousands]
 *   Default: 100K keys
 */

#include "../include/mpt.h"
#include "../include/hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>

/* ========================================================================
 * Constants
 * ======================================================================== */

#define KEY_SIZE        32
#define VALUE_LEN_MIN   4
#define VALUE_LEN_MAX   32
#define KEYS_PER_BLOCK  7000

#define MPT_PATH  "/tmp/bench_mpt_trie.dat"

#define MASTER_SEED 0xBE4C484D50545249ULL

/* ========================================================================
 * Fail-fast
 * ======================================================================== */

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n", \
                __FILE__, __LINE__, ##__VA_ARGS__); \
        abort(); \
    } \
} while(0)

/* ========================================================================
 * RNG (SplitMix64)
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

/* ========================================================================
 * Key/Value Generation
 * ======================================================================== */

static void gen_key(uint8_t key[KEY_SIZE], uint64_t index) {
    rng_t rng = rng_create(MASTER_SEED ^ (index * 0x517cc1b727220a95ULL));
    for (int i = 0; i < KEY_SIZE; i += 8) {
        uint64_t r = rng_next(&rng);
        memcpy(key + i, &r, 8);
    }
}

static uint8_t gen_value(uint8_t *buf, uint64_t index) {
    rng_t rng = rng_create(MASTER_SEED ^ (index * 0x9ABCDEF012345678ULL));
    uint8_t vlen = (uint8_t)(VALUE_LEN_MIN +
                   (rng_next(&rng) % (VALUE_LEN_MAX - VALUE_LEN_MIN + 1)));
    for (int i = 0; i < vlen; i += 8) {
        uint64_t r = rng_next(&rng);
        int remain = vlen - i;
        memcpy(buf + i, &r, remain < 8 ? remain : 8);
    }
    return vlen;
}

/* ========================================================================
 * Utilities
 * ======================================================================== */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
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

static void print_root(const hash_t *h) {
    for (int i = 0; i < 4; i++) printf("%02x", h->bytes[i]);
    printf("..");
    for (int i = 28; i < 32; i++) printf("%02x", h->bytes[i]);
}

/* ========================================================================
 * Main
 * ======================================================================== */

int main(int argc, char *argv[]) {
    uint64_t target_thousands = 100;
    if (argc >= 2) {
        target_thousands = (uint64_t)atoll(argv[1]);
        if (target_thousands == 0) target_thousands = 100;
    }
    uint64_t target_keys = target_thousands * 1000ULL;
    uint64_t est_blocks = (target_keys + KEYS_PER_BLOCK - 1) / KEYS_PER_BLOCK;

    printf("============================================\n");
    printf("  MPT Trie Benchmark\n");
    printf("============================================\n");
    printf("  target:       %" PRIu64 "K keys\n", target_thousands);
    printf("  keys/block:   %d\n", KEYS_PER_BLOCK);
    printf("  est blocks:   %" PRIu64 "\n", est_blocks);
    printf("============================================\n\n");

    unlink(MPT_PATH);

    mpt_t *m = mpt_create(MPT_PATH);
    ASSERT(m != NULL, "mpt_create failed");

    uint8_t key[KEY_SIZE];
    uint8_t val[VALUE_LEN_MAX];

    uint64_t next_id = 0;
    uint64_t total_blocks = 0;
    double total_mpt_time = 0;
    double min_ms = 1e9, max_ms = 0;
    double t_start = now_sec();

    printf("%-7s | %-9s | %-10s | %-8s | %-8s | %-6s | %s\n",
           "block", "keys", "mpt(ms)", "put(ms)", "root(ms)", "RSS", "root");
    printf("--------+-----------+------------+----------+"
           "----------+--------+-----\n");

    while (next_id < target_keys) {
        uint64_t blk_end = next_id + KEYS_PER_BLOCK;
        if (blk_end > target_keys) blk_end = target_keys;

        /* --- mpt_put --- */
        double t0 = now_sec();
        for (uint64_t i = next_id; i < blk_end; i++) {
            gen_key(key, i);
            uint8_t vlen = gen_value(val, i);
            mpt_put(m, key, val, vlen);
        }
        double t1 = now_sec();

        /* --- mpt_root --- */
        hash_t root = mpt_root(m);
        double t2 = now_sec();

        double put_ms = (t1 - t0) * 1000.0;
        double root_ms = (t2 - t1) * 1000.0;
        double total_ms = (t2 - t0) * 1000.0;

        next_id = blk_end;
        total_blocks++;

        if (total_blocks > 1) {
            total_mpt_time += total_ms;
            if (total_ms < min_ms) min_ms = total_ms;
            if (total_ms > max_ms) max_ms = total_ms;
        }

        /* Print stats */
        bool is_last = (next_id >= target_keys);
        if (total_blocks <= 5 || total_blocks % 10 == 0 || is_last) {
            printf("%5" PRIu64 "   | %7.2fK  | %8.2f   | %6.2f   | %6.2f    | %4zuMB | ",
                   total_blocks,
                   (double)mpt_size(m) / 1e3,
                   total_ms, put_ms, root_ms,
                   get_rss_mb());
            print_root(&root);
            printf("\n");
            fflush(stdout);
        }
    }

    double elapsed = now_sec() - t_start;
    double avg_ms = (total_blocks > 1) ? total_mpt_time / (total_blocks - 1) : 0;

    printf("\n============================================\n");
    printf("  Results (%" PRIu64 " keys, %" PRIu64 " blocks)\n",
           mpt_size(m), total_blocks);
    printf("============================================\n");
    printf("  avg:     %.2fms/block\n", avg_ms);
    printf("  min:     %.2fms\n", min_ms);
    printf("  max:     %.2fms\n", max_ms);
    printf("  nodes:   %u  (%.1fMB)\n",
           mpt_nodes(m), (double)mpt_nodes(m) * 128 / 1e6);
    printf("  leaves:  %u  (%.1fMB)\n",
           mpt_leaves(m), (double)mpt_leaves(m) * 160 / 1e6);
    printf("  RSS:     %zuMB\n", get_rss_mb());
    printf("  time:    %.1fs\n", elapsed);
    printf("============================================\n");

    mpt_close(m);
    unlink(MPT_PATH);

    return 0;
}
