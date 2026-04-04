/**
 * Benchmark: hart_root_hash (recursive/scalar) vs hart_root_hash_avx512 (iterative/batched)
 * under realistic conditions — large trees, incremental updates, cache pressure.
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

static void bench_full_root(int n) {
    printf("\n=== Full root computation (N=%d) ===\n", n);

    hart_t tree;
    hart_init(&tree, 32);
    for (int i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)i, k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree, k, v);
    }

    uint8_t root[32];
    int iters = n > 50000 ? 3 : (n > 10000 ? 10 : 50);

    /* Scalar recursive */
    double best_scalar = 1e9;
    for (int r = 0; r < iters; r++) {
        /* Mark all dirty by re-inserting one key per path */
        for (int i = 0; i < n; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)i, k);
            memset(v, (uint8_t)((i + r) & 0xFF), 32);
            hart_insert(&tree, k, v);
        }
        double t0 = now_ms();
        hart_root_hash(&tree, encode_raw, NULL, root);
        double dt = now_ms() - t0;
        if (dt < best_scalar) best_scalar = dt;
    }

    /* AVX-512 iterative */
    double best_avx = 1e9;
    for (int r = 0; r < iters; r++) {
        for (int i = 0; i < n; i++) {
            uint8_t k[32], v[32];
            make_key((uint32_t)i, k);
            memset(v, (uint8_t)((i + r + 100) & 0xFF), 32);
            hart_insert(&tree, k, v);
        }
        double t0 = now_ms();
        hart_root_hash_avx512(&tree, encode_raw, NULL, root);
        double dt = now_ms() - t0;
        if (dt < best_avx) best_avx = dt;
    }

    printf("  scalar:  %7.2f ms\n", best_scalar);
    printf("  avx512:  %7.2f ms  (%.2fx)\n", best_avx, best_scalar / best_avx);

    hart_destroy(&tree);
}

static void bench_incremental(int n, int dirty_pct) {
    printf("\n=== Incremental update (N=%d, %d%% dirty) ===\n", n, dirty_pct);

    hart_t tree_s, tree_a;
    hart_init(&tree_s, 32);
    hart_init(&tree_a, 32);

    /* Build identical trees */
    for (int i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key((uint32_t)i, k);
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree_s, k, v);
        hart_insert(&tree_a, k, v);
    }

    /* Initial root (clears all dirty flags) */
    uint8_t root_s[32], root_a[32];
    hart_root_hash(&tree_s, encode_raw, NULL, root_s);
    hart_root_hash_avx512(&tree_a, encode_raw, NULL, root_a);

    int n_dirty = n * dirty_pct / 100;
    if (n_dirty < 1) n_dirty = 1;
    int iters = n > 50000 ? 5 : 20;

    double total_scalar = 0, total_avx = 0;

    for (int r = 0; r < iters; r++) {
        /* Modify same subset in both trees */
        for (int i = 0; i < n_dirty; i++) {
            uint32_t idx = (uint32_t)((i * 7 + r * 13) % n);
            uint8_t k[32], v[32];
            make_key(idx, k);
            memset(v, (uint8_t)((idx + r) & 0xFF), 32);
            hart_insert(&tree_s, k, v);
            hart_insert(&tree_a, k, v);
        }

        double t0 = now_ms();
        hart_root_hash(&tree_s, encode_raw, NULL, root_s);
        total_scalar += now_ms() - t0;

        t0 = now_ms();
        hart_root_hash_avx512(&tree_a, encode_raw, NULL, root_a);
        total_avx += now_ms() - t0;

        if (memcmp(root_s, root_a, 32) != 0) {
            printf("  MISMATCH at iter %d!\n", r);
            return;
        }
    }

    double avg_s = total_scalar / iters;
    double avg_a = total_avx / iters;
    printf("  scalar:  %7.2f ms avg\n", avg_s);
    printf("  avx512:  %7.2f ms avg  (%.2fx)\n", avg_a, avg_s / avg_a);
}

int main(void) {
    printf("Hart root hash benchmark: scalar vs AVX-512 iterative\n");

    /* Full root (all nodes dirty) */
    bench_full_root(100);
    bench_full_root(1000);
    bench_full_root(10000);
    bench_full_root(100000);

    /* Incremental (only some nodes dirty — realistic scenario) */
    bench_incremental(10000, 1);    /* 1% dirty */
    bench_incremental(10000, 10);   /* 10% dirty */
    bench_incremental(100000, 1);   /* 1% dirty, large tree */
    bench_incremental(100000, 10);  /* 10% dirty, large tree */

    return 0;
}
