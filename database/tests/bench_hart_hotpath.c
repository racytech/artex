/**
 * Benchmark: hart hot-path latency for prefetch optimization.
 *
 * Measures per-operation latency of:
 *   1. hart_get (SLOAD) — random key lookup
 *   2. hart_insert (SSTORE) — random key write (existing key, value update)
 *   3. hart_root_hash — incremental MPT root (after modifying K entries)
 *   4. hart_iter — full iteration (eviction serialization)
 *
 * Each test runs with:
 *   - Warm cache: repeat same operations (data in L1/L2)
 *   - Cold cache: flush cache between operations (simulate real workload)
 *
 * Run before and after adding prefetch to compare.
 */

#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define STOR_VAL_SIZE 32
#define CACHE_LINE 64

static double now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}

static void make_key(uint64_t slot, uint8_t key[32]) {
    uint8_t buf[8];
    for (int i = 7; i >= 0; i--) { buf[i] = slot & 0xFF; slot >>= 8; }
    hash_t h = hash_keccak256(buf, 8);
    memcpy(key, h.bytes, 32);
}

static void make_val(uint64_t v, uint8_t val[32]) {
    memset(val, 0, 32);
    val[31] = (uint8_t)(v & 0xFF);
    val[30] = (uint8_t)((v >> 8) & 0xFF);
}

static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *v = (const uint8_t *)leaf_val;
    int skip = 0;
    while (skip < 31 && v[skip] == 0) skip++;
    int len = 32 - skip;
    if (len == 1 && v[skip] <= 0x7f) { rlp_out[0] = v[skip]; return 1; }
    rlp_out[0] = 0x80 + (uint8_t)len;
    memcpy(rlp_out + 1, v + skip, len);
    return 1 + (uint32_t)len;
}

/* Attempt to flush hart arena from CPU cache */
static void flush_cache(hart_t *ht) {
    if (!ht->arena) return;
    volatile uint8_t sink = 0;
    /* Read every cache line to evict from L1/L2 with a large dummy array */
    static volatile uint8_t trash[8 * 1024 * 1024]; /* 8MB > L2 */
    for (size_t i = 0; i < sizeof(trash); i += CACHE_LINE)
        sink += trash[i];
    (void)sink;
}

/* Build a hart with N entries */
static void populate_hart(hart_t *ht, uint32_t n_slots) {
    hart_init_cap(ht, STOR_VAL_SIZE, n_slots * 96 < 1024 ? 1024 : n_slots * 96);
    for (uint32_t i = 0; i < n_slots; i++) {
        uint8_t key[32], val[32];
        make_key(i, key);
        make_val(i + 1, val);
        hart_insert(ht, key, val);
    }
    /* Compute root once so dirty flags are cleared */
    uint8_t root[32];
    hart_root_hash(ht, stor_encode, NULL, root);
}

/* Generate random indices for lookups */
static uint32_t *random_indices(uint32_t n, uint32_t max, uint64_t seed) {
    uint32_t *idx = malloc(n * sizeof(uint32_t));
    for (uint32_t i = 0; i < n; i++) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        idx[i] = (uint32_t)((seed >> 32) % max);
    }
    return idx;
}

static void bench_get(hart_t *ht, uint32_t n_slots, uint32_t n_ops, bool cold) {
    uint32_t *indices = random_indices(n_ops, n_slots, 42);
    uint8_t key[32];
    volatile const void *sink;

    if (cold) flush_cache(ht);

    double t0 = now_ns();
    for (uint32_t i = 0; i < n_ops; i++) {
        make_key(indices[i], key);
        sink = hart_get(ht, key);
    }
    double t1 = now_ns();
    (void)sink;

    printf("    get:  %5u ops, %.0f ns/op (%s)\n",
           n_ops, (t1 - t0) / n_ops, cold ? "cold" : "warm");
    free(indices);
}

static void bench_insert_existing(hart_t *ht, uint32_t n_slots, uint32_t n_ops, bool cold) {
    uint32_t *indices = random_indices(n_ops, n_slots, 123);
    uint8_t key[32], val[32];

    if (cold) flush_cache(ht);

    double t0 = now_ns();
    for (uint32_t i = 0; i < n_ops; i++) {
        make_key(indices[i], key);
        make_val(i + 999, val);
        hart_insert(ht, key, val);
    }
    double t1 = now_ns();

    printf("    ins:  %5u ops, %.0f ns/op (%s)\n",
           n_ops, (t1 - t0) / n_ops, cold ? "cold" : "warm");
    free(indices);
}

static void bench_root_hash(hart_t *ht, uint32_t n_slots, uint32_t n_dirty, bool cold) {
    /* Dirty some entries first */
    for (uint32_t i = 0; i < n_dirty; i++) {
        uint8_t key[32], val[32];
        make_key(i, key);
        make_val(i + 777, val);
        hart_insert(ht, key, val);
    }

    if (cold) flush_cache(ht);

    uint8_t root[32];
    double t0 = now_ns();
    hart_root_hash(ht, stor_encode, NULL, root);
    double t1 = now_ns();

    printf("    root: %5u dirty/%u total, %.0f μs (%s)\n",
           n_dirty, n_slots, (t1 - t0) / 1000, cold ? "cold" : "warm");
}

static void bench_iter(hart_t *ht, uint32_t n_slots, bool cold) {
    if (cold) flush_cache(ht);

    uint32_t count = 0;
    volatile uint8_t sink = 0;

    double t0 = now_ns();
    hart_iter_t *it = hart_iter_create(ht);
    if (it) {
        while (hart_iter_next(it)) {
            sink += hart_iter_key(it)[0];
            count++;
        }
        hart_iter_destroy(it);
    }
    double t1 = now_ns();
    (void)sink;

    printf("    iter: %5u entries, %.0f μs total, %.0f ns/entry (%s)\n",
           count, (t1 - t0) / 1000, (t1 - t0) / count, cold ? "cold" : "warm");
}

static void bench_suite(uint32_t n_slots) {
    hart_t ht;
    populate_hart(&ht, n_slots);

    printf("\n  === %u slots (arena: %zuKB used, %zuKB cap) ===\n",
           n_slots, ht.arena_used / 1024, ht.arena_cap / 1024);

    uint32_t n_ops = n_slots < 1000 ? 10000 : 5000;

    /* Warm cache */
    bench_get(&ht, n_slots, n_ops, false);
    bench_insert_existing(&ht, n_slots, n_ops, false);
    bench_root_hash(&ht, n_slots, n_slots < 100 ? n_slots : 100, false);
    bench_iter(&ht, n_slots, false);

    /* Cold cache */
    bench_get(&ht, n_slots, n_ops, true);
    bench_insert_existing(&ht, n_slots, n_ops, true);

    /* Rebuild root cleanly for cold test */
    {
        uint8_t root[32];
        hart_root_hash(&ht, stor_encode, NULL, root);
    }
    /* Re-dirty for cold root test */
    bench_root_hash(&ht, n_slots, n_slots < 100 ? n_slots : 100, true);
    bench_iter(&ht, n_slots, true);

    hart_destroy(&ht);
}

int main(void) {
    printf("=== Hart Hot-Path Latency Benchmark ===\n");
    printf("(baseline — before prefetch optimization)\n");

    bench_suite(50);
    bench_suite(100);
    bench_suite(500);
    bench_suite(1000);
    bench_suite(5000);
    bench_suite(10000);

    printf("\n=== done ===\n");
    return 0;
}
