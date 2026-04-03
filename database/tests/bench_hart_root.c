/**
 * Benchmark hart_root_hash at different dirty ratios.
 *
 * 1) Insert 1M keys, compute root (100% dirty — cold start)
 * 2) Recompute root (0% dirty — fully cached)
 * 3) Dirty 1% of keys, recompute
 * 4) Dirty 10% of keys, recompute
 * 5) Dirty 100% of keys, recompute
 */
#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#define N_KEYS 1000000

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

int main(void) {
    printf("hart_root_hash benchmark: %d keys, value_size=32\n\n", N_KEYS);

    hart_t tree;
    hart_init(&tree, 32);

    /* Pre-generate keys */
    uint8_t (*keys)[32] = malloc(N_KEYS * 32);
    for (int i = 0; i < N_KEYS; i++)
        make_key((uint32_t)i, keys[i]);

    /* Insert all */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < N_KEYS; i++) {
        uint8_t v[32];
        memset(v, (uint8_t)(i & 0xFF), 32);
        hart_insert(&tree, keys[i], v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("insert %dK:    %8.1f ms\n", N_KEYS/1000, elapsed_ms(&t0, &t1));
    printf("arena: %zu MB\n\n", tree.arena_used / (1024*1024));

    uint8_t root[32];

    /* 100% dirty (first computation) */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hart_root_hash(&tree, encode_raw, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("root 100%% dirty: %8.1f ms  root=%.8s...\n",
           elapsed_ms(&t0, &t1), "");
    printf("  ");
    for (int i = 0; i < 4; i++) printf("%02x", root[i]);
    printf("...\n\n");

    /* 0% dirty (fully cached) */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hart_root_hash(&tree, encode_raw, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("root   0%% dirty: %8.1f ms  (cached)\n\n", elapsed_ms(&t0, &t1));

    /* Dirty 1% */
    int pct1 = N_KEYS / 100;
    for (int i = 0; i < pct1; i++) {
        uint8_t v[32];
        memset(v, 0xFF, 32);
        hart_insert(&tree, keys[i], v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hart_root_hash(&tree, encode_raw, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("root   1%% dirty: %8.1f ms  (%d keys modified)\n\n",
           elapsed_ms(&t0, &t1), pct1);

    /* Dirty 10% */
    int pct10 = N_KEYS / 10;
    for (int i = 0; i < pct10; i++) {
        uint8_t v[32];
        memset(v, 0xFE, 32);
        hart_insert(&tree, keys[i], v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hart_root_hash(&tree, encode_raw, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("root  10%% dirty: %8.1f ms  (%d keys modified)\n\n",
           elapsed_ms(&t0, &t1), pct10);

    /* Dirty 100% */
    for (int i = 0; i < N_KEYS; i++) {
        uint8_t v[32];
        memset(v, 0xFD, 32);
        hart_insert(&tree, keys[i], v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    hart_root_hash(&tree, encode_raw, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("root 100%% dirty: %8.1f ms  (full recompute)\n\n",
           elapsed_ms(&t0, &t1));

    /* Run 100% dirty 3 more times to get stable number */
    printf("--- 3x repeated 100%% dirty ---\n");
    for (int r = 0; r < 3; r++) {
        for (int i = 0; i < N_KEYS; i++) {
            uint8_t v[32];
            memset(v, (uint8_t)(r + 0xA0), 32);
            hart_insert(&tree, keys[i], v);
        }
        clock_gettime(CLOCK_MONOTONIC, &t0);
        hart_root_hash(&tree, encode_raw, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        printf("  run %d: %8.1f ms\n", r+1, elapsed_ms(&t0, &t1));
    }

    free(keys);
    hart_destroy(&tree);
    return 0;
}
