/**
 * Benchmark: incremental vs full root hash at realistic scale.
 *
 * Build a large tree (100K-1M entries), then:
 * - Modify 100-150 keys per "block"
 * - Compute root hash (incremental — only dirty paths rehashed)
 * - Compare with full recomputation (all nodes re-dirtied first)
 *
 * Also measures cost of accumulating N blocks before computing root.
 */
#include "hashed_art.h"
#include "hash.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

static uint32_t encode_raw(const uint8_t key[32], const void *val,
                            uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    rlp_out[0] = 0xa0;
    memcpy(rlp_out + 1, val, 32);
    return 33;
}

static void make_key(uint32_t seed, uint8_t out[32]) {
    memset(out, 0, 32);
    memcpy(out, &seed, 4);
    hash_t h = hash_keccak256(out, 32);
    memcpy(out, h.bytes, 32);
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* Simple xoshiro PRNG */
static uint64_t rng_s[4];
static void rng_seed(uint64_t s) {
    rng_s[0] = s; rng_s[1] = s ^ 0x9E3779B97F4A7C15ULL;
    rng_s[2] = s ^ 0x6A09E667F3BCC908ULL; rng_s[3] = s ^ 0xBB67AE8584CAA73BULL;
    for (int i = 0; i < 8; i++) {
        uint64_t t = rng_s[1] << 17;
        rng_s[2] ^= rng_s[0]; rng_s[3] ^= rng_s[1];
        rng_s[1] ^= rng_s[2]; rng_s[0] ^= rng_s[3];
        rng_s[2] ^= t; rng_s[3] = (rng_s[3] << 45) | (rng_s[3] >> 19);
    }
}
static uint64_t rng_next(void) {
    uint64_t result = ((rng_s[1] * 5) << 7 | (rng_s[1] * 5) >> 57) * 9;
    uint64_t t = rng_s[1] << 17;
    rng_s[2] ^= rng_s[0]; rng_s[3] ^= rng_s[1];
    rng_s[1] ^= rng_s[2]; rng_s[0] ^= rng_s[3];
    rng_s[2] ^= t; rng_s[3] = (rng_s[3] << 45) | (rng_s[3] >> 19);
    return result;
}

static void modify_n_keys(hart_t *t, int n_entries, int n_modify) {
    for (int i = 0; i < n_modify; i++) {
        uint32_t idx = (uint32_t)(rng_next() % n_entries);
        uint8_t k[32], v[32];
        make_key(idx, k);
        uint64_t r = rng_next();
        memcpy(v, &r, 8);
        memset(v + 8, (uint8_t)(r >> 8), 24);
        hart_insert(t, k, v);
    }
}

static void dirty_all(hart_t *t, int n_entries) {
    /* Re-insert every key to mark all paths dirty */
    for (int i = 0; i < n_entries; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)i, k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(t, k, v);
    }
}

static void bench_incremental_vs_full(int n_entries, int dirty_per_block) {
    printf("\n=== N=%d entries, %d dirty per block ===\n", n_entries, dirty_per_block);

    hart_t tree;
    hart_init(&tree, 32);

    /* Build tree */
    double t0 = now_ms();
    for (int i = 0; i < n_entries; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)i, k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree, k, v);
    }
    double build_ms = now_ms() - t0;
    printf("  build: %.1fms (%.0f entries/ms)\n", build_ms, n_entries / build_ms);

    /* Full root (baseline — all dirty) */
    uint8_t root[32];
    t0 = now_ms();
    hart_root_hash(&tree, encode_raw, NULL, root);
    double full_ms = now_ms() - t0;
    printf("  full root: %.2fms\n", full_ms);

    /* Incremental: modify dirty_per_block keys, compute root */
    rng_seed(42);
    int rounds = 20;
    double total_incr = 0;
    for (int r = 0; r < rounds; r++) {
        modify_n_keys(&tree, n_entries, dirty_per_block);
        t0 = now_ms();
        hart_root_hash(&tree, encode_raw, NULL, root);
        total_incr += now_ms() - t0;
    }
    double avg_incr = total_incr / rounds;
    printf("  incremental (%d dirty): %.2fms avg  (%.0fx faster than full)\n",
           dirty_per_block, avg_incr, full_ms / avg_incr);

    /* Simulate accumulated blocks: modify N*dirty_per_block keys, then one root */
    int block_counts[] = { 1, 16, 64, 256, 1024 };
    for (int bi = 0; bi < 5; bi++) {
        int blocks = block_counts[bi];
        int total_dirty = blocks * dirty_per_block;
        if (total_dirty > n_entries) total_dirty = n_entries;

        /* Start clean */
        hart_root_hash(&tree, encode_raw, NULL, root);

        /* Accumulate dirty over N blocks */
        rng_seed(1000 + bi);
        for (int b = 0; b < blocks; b++)
            modify_n_keys(&tree, n_entries, dirty_per_block);

        t0 = now_ms();
        hart_root_hash(&tree, encode_raw, NULL, root);
        double acc_ms = now_ms() - t0;
        printf("  %4d blocks accumulated (%d dirty): %.2fms\n",
               blocks, total_dirty, acc_ms);
    }

    printf("  arena: %.1fMB\n", tree.arena_cap / (1024.0 * 1024.0));
    hart_destroy(&tree);
}

int main(int argc, char **argv) {
    printf("Hart incremental vs full root hash benchmark\n");

    /* Quick mode (default) or full mode (--full) */
    bool full_mode = (argc > 1 && strcmp(argv[1], "--full") == 0);

    bench_incremental_vs_full(100000, 150);
    bench_incremental_vs_full(1000000, 150);
    bench_incremental_vs_full(5000000, 150);
    bench_incremental_vs_full(10000000, 150);
    bench_incremental_vs_full(30000000, 150);

    if (full_mode) {
        bench_incremental_vs_full(100000000, 150);
        bench_incremental_vs_full(200000000, 150);
    }

    return 0;
}
