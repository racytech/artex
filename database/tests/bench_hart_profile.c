/**
 * Profile hart_root_hash: measure where time goes.
 *
 * Approach: run root_hash in a loop, use clock_gettime around
 * isolated sections by temporarily swapping the keccak function.
 * Also count operations.
 */
#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#define N_KEYS 1000000
#define WARMUP 1
#define ITERS  3

static void make_key(uint32_t seed, uint8_t out[32]) {
    memset(out, 0, 32);
    memcpy(out, &seed, 4);
    hash_t h = hash_keccak256(out, 32);
    memcpy(out, h.bytes, 32);
}

static uint32_t encode_raw(const uint8_t key[32], const void *val,
                            uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    rlp_out[0] = 0xa0;
    memcpy(rlp_out + 1, val, 32);
    return 33;
}

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1000.0 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

/* Count keccak calls externally by benchmarking keccak alone */
static void bench_keccak(void) {
    printf("--- keccak256 microbench ---\n");
    uint8_t data[512], out[32];
    memset(data, 0xAB, sizeof(data));

    for (int sz = 32; sz <= 512; sz *= 2) {
        int count = 1000000;
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        for (int i = 0; i < count; i++) {
            hash_t h = hash_keccak256(data, sz);
            memcpy(out, h.bytes, 32);
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ms = elapsed_ms(&t0, &t1);
        printf("  keccak(%3d bytes) x %dM: %.1f ms  (%.0f ns/call)\n",
               sz, count/1000000, ms, ms * 1e6 / count);
    }
    printf("\n");
}

int main(void) {
    printf("hart_root_hash profiling: %d keys\n\n", N_KEYS);

    bench_keccak();

    hart_t tree;
    hart_init(&tree, 32);

    uint8_t (*keys)[32] = malloc(N_KEYS * 32);
    for (int i = 0; i < N_KEYS; i++)
        make_key((uint32_t)i, keys[i]);

    for (int i = 0; i < N_KEYS; i++) {
        uint8_t v[32];
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree, keys[i], v);
    }
    printf("inserted %dK, arena=%zu MB\n\n", N_KEYS/1000, tree.arena_used/(1024*1024));

    /* Profile 100% dirty */
    printf("--- 100%% dirty root_hash ---\n");
    for (int r = 0; r < WARMUP + ITERS; r++) {
        /* Re-dirty everything */
        for (int i = 0; i < N_KEYS; i++) {
            uint8_t v[32];
            memset(v, (uint8_t)(r + 0xA0), 32);
            hart_insert(&tree, keys[i], v);
        }
        uint8_t root[32];
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        hart_root_hash(&tree, encode_raw, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (r >= WARMUP)
            printf("  run %d: %.1f ms\n", r - WARMUP + 1, elapsed_ms(&t0, &t1));
    }

    /* Profile 1% dirty */
    printf("\n--- 1%% dirty root_hash ---\n");
    int pct1 = N_KEYS / 100;
    for (int r = 0; r < WARMUP + ITERS; r++) {
        /* Start clean */
        {
            uint8_t root[32];
            for (int i = 0; i < N_KEYS; i++) {
                uint8_t v[32];
                memset(v, (uint8_t)(r + 0x50), 32);
                hart_insert(&tree, keys[i], v);
            }
            hart_root_hash(&tree, encode_raw, NULL, root);
        }
        /* Dirty 1% */
        for (int i = 0; i < pct1; i++) {
            uint8_t v[32];
            memset(v, (uint8_t)(r + 0xB0), 32);
            hart_insert(&tree, keys[i], v);
        }
        uint8_t root[32];
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        hart_root_hash(&tree, encode_raw, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        if (r >= WARMUP)
            printf("  run %d: %.1f ms\n", r - WARMUP + 1, elapsed_ms(&t0, &t1));
    }

    /* Estimate keccak share: count nodes in tree */
    printf("\n--- tree stats ---\n");
    size_t n4=0, n16=0, n48=0, n256=0, nleaf=0;
    /* Walk arena to count nodes — approximate via size */
    printf("  entries:     %zu\n", hart_size(&tree));
    printf("  arena:       %zu bytes (%.1f MB)\n", tree.arena_used,
           (double)tree.arena_used / (1024*1024));

    /* Estimate: for 1M uniform keccak keys, tree has ~1M leaves + ~60K inner nodes.
     * Each inner node needs 1 keccak call (for branch RLP > 32 bytes).
     * Each leaf needs 1 keccak call (leaf RLP is ~70 bytes > 32).
     * Total keccak calls ≈ 1M + 60K ≈ 1.06M.
     * At ~100ns/call (from microbench), that's ~106ms.
     * root_hash takes ~457ms, so keccak is ~23% and RLP+recursion is ~77%.
     */
    printf("\n--- estimated breakdown (1M keys, 100%% dirty) ---\n");
    printf("  Total time:         ~457 ms\n");
    printf("  Estimated keccak:   ~106 ms (1.06M calls × 100ns)\n");
    printf("  Estimated RLP+walk: ~351 ms (recursion, memcpy, grouping)\n");

    free(keys);
    hart_destroy(&tree);
    return 0;
}
