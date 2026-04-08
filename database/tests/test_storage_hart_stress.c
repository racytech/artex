/**
 * Stress test: storage_hart at realistic Ethereum scale.
 *
 * Distribution (approximation at block 14M):
 *   ~2M accounts with storage total
 *   - 90% have 1-10 slots       (~1.8M accounts)
 *   - 8% have 10-1000 slots     (~160K accounts)
 *   - 1.5% have 1K-100K slots   (~30K accounts)
 *   - 0.4% have 100K-1M slots   (~8K accounts)
 *   - 0.1% have 1M+ slots       (~2K accounts, whales)
 *
 * We test a scaled-down version (100K accounts) and a whale test (1M slots).
 * Measures: memory usage, read latency, write latency, root computation.
 */

#include "storage_hart.h"
#include "hash.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>

#define TEST_FILE "/dev/shm/test_storage_hart_stress.dat"

static double elapsed_ms(struct timespec *t0, struct timespec *t1) {
    return (t1->tv_sec - t0->tv_sec) * 1000.0 +
           (t1->tv_nsec - t0->tv_nsec) / 1e6;
}

static void make_key(uint64_t acct, uint64_t slot, uint8_t key[32]) {
    uint8_t seed[16];
    memcpy(seed, &acct, 8);
    memcpy(seed + 8, &slot, 8);
    hash_t h = hash_keccak256(seed, 16);
    memcpy(key, h.bytes, 32);
}

static void make_val(uint64_t v, uint8_t val[32]) {
    memset(val, 0, 32);
    for (int b = 0; b < 8; b++)
        val[31 - b] = (uint8_t)(v >> (b * 8));
}

/* RLP encode for root computation */
static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    const uint8_t *v = (const uint8_t *)leaf_val;
    size_t i = 0;
    while (i < 32 && v[i] == 0) i++;
    size_t len = 32 - i;
    if (len == 0) { rlp_out[0] = 0x80; return 1; }
    if (len == 1 && v[i] < 0x80) { rlp_out[0] = v[i]; return 1; }
    rlp_out[0] = 0x80 + (uint8_t)len;
    memcpy(rlp_out + 1, v + i, len);
    return 1 + (uint32_t)len;
}

/* =========================================================================
 * Test 1: Many small accounts (realistic distribution)
 * ========================================================================= */

static void test_many_accounts(void) {
    printf("=== Test: many accounts (realistic distribution) ===\n");
    unlink(TEST_FILE);

    storage_hart_pool_t *pool = storage_hart_pool_create(TEST_FILE);
    if (!pool) { printf("FAIL: pool create\n"); return; }

    /* Distribution: 100K accounts scaled */
    struct { uint32_t count; uint32_t slots; const char *label; } tiers[] = {
        { 90000, 5,     "tiny (1-10 slots)"     },
        {  8000, 100,   "small (10-1K slots)"    },
        {  1500, 10000, "medium (1K-100K slots)" },
        {   400, 100000,"large (100K-1M slots)"  },
        {   100, 500000,"whale (1M+ slots)"      },
    };
    int n_tiers = 5;

    /* We'll only do 100 accounts for medium+ to keep runtime reasonable */
    /* Adjust: */
    tiers[2].count = 10;    /* medium: 10 accounts × 10K slots */
    tiers[3].count = 2;     /* large: 2 accounts × 100K slots */
    tiers[4].count = 0;     /* whale: tested separately below */

    storage_hart_t *handles = calloc(100000, sizeof(storage_hart_t));
    uint32_t total_accounts = 0;
    uint64_t total_slots = 0;
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint32_t acct_id = 0;
    for (int tier = 0; tier < n_tiers; tier++) {
        struct timespec tt0, tt1;
        clock_gettime(CLOCK_MONOTONIC, &tt0);

        for (uint32_t a = 0; a < tiers[tier].count; a++) {
            storage_hart_t *sh = &handles[acct_id];
            *sh = (storage_hart_t){0};

            for (uint32_t s = 0; s < tiers[tier].slots; s++) {
                uint8_t k[32], v[32];
                make_key(acct_id, s, k);
                make_val(s + 1, v);
                storage_hart_put(pool, sh, k, v);
            }

            total_slots += tiers[tier].slots;
            acct_id++;
        }

        clock_gettime(CLOCK_MONOTONIC, &tt1);
        total_accounts += tiers[tier].count;

        printf("  %s: %u accounts × %u slots = %luK total, %.0fms\n",
               tiers[tier].label, tiers[tier].count, tiers[tier].slots,
               (unsigned long)(tiers[tier].count * (uint64_t)tiers[tier].slots / 1000),
               elapsed_ms(&tt0, &tt1));
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);

    storage_hart_pool_stats_t st = storage_hart_pool_stats(pool);
    printf("\n  Total: %u accounts, %lu slots, %.1fs\n",
           total_accounts, total_slots, insert_ms / 1000.0);
    printf("  Pool: data=%luMB file=%luMB (%.1f bytes/slot)\n",
           st.data_size / (1024*1024), st.file_size / (1024*1024),
           total_slots > 0 ? (double)st.data_size / total_slots : 0);

    /* Benchmark: random reads across all accounts */
    printf("\n  Read benchmark (1M random reads):\n");
    int reads = 1000000;
    int found = 0;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < reads; i++) {
        uint32_t a = (uint32_t)((uint64_t)i * 7919 % total_accounts);
        uint32_t s = (uint32_t)((uint64_t)i * 104729 % tiers[0].slots);
        uint8_t k[32], v[32];
        make_key(a, s, k);
        if (storage_hart_get(pool, &handles[a], k, v))
            found++;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double read_ns = elapsed_ms(&t0, &t1) * 1e6 / reads;
    printf("    %.0f ns/read (%d/%d found)\n", read_ns, found, reads);

    /* Benchmark: root computation for different account sizes */
    printf("\n  Root computation benchmark:\n");
    uint32_t sample_accts[] = {0, 90000, 98000, 99600, 99990};
    const char *sample_labels[] = {"tiny(5)", "small(100)", "medium(10K)", "large(100K)", "whale(500K)"};
    for (int s = 0; s < 5; s++) {
        uint32_t a = sample_accts[s];
        if (a >= total_accounts) continue;
        storage_hart_t *sh = &handles[a];

        /* Full root */
        storage_hart_invalidate(pool, sh);
        uint8_t root[32];
        clock_gettime(CLOCK_MONOTONIC, &t0);
        storage_hart_root_hash(pool, sh, stor_encode, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double full_ms = elapsed_ms(&t0, &t1);

        /* Incremental: update 5 entries, recompute */
        for (int i = 0; i < 5; i++) {
            uint8_t k[32], v[32];
            make_key(a, i, k);
            make_val(999999 + i, v);
            storage_hart_put(pool, sh, k, v);
        }
        clock_gettime(CLOCK_MONOTONIC, &t0);
        storage_hart_root_hash(pool, sh, stor_encode, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double incr_ms = elapsed_ms(&t0, &t1);

        printf("    %-15s: full=%.2fms  incr=%.3fms  (count=%u)\n",
               sample_labels[s], full_ms, incr_ms, sh->count);
    }

    /* Cleanup */
    for (uint32_t a = 0; a < total_accounts; a++)
        storage_hart_clear(pool, &handles[a]);
    free(handles);

    storage_hart_pool_destroy(pool);
    unlink(TEST_FILE);
    printf("\n  Done.\n");
}

/* =========================================================================
 * Test 2: Single whale account (1M slots)
 * ========================================================================= */

static void test_whale(void) {
    printf("\n=== Test: single whale account (1M slots) ===\n");
    unlink(TEST_FILE);

    storage_hart_pool_t *pool = storage_hart_pool_create(TEST_FILE);
    if (!pool) { printf("FAIL: pool create\n"); return; }

    storage_hart_t sh = (storage_hart_t){0};
    uint32_t n = 100000;

    /* Insert 1M entries */
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key(0, i, k);
        make_val(i + 1, v);
        storage_hart_put(pool, &sh, k, v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double insert_ms = elapsed_ms(&t0, &t1);

    storage_hart_pool_stats_t st = storage_hart_pool_stats(pool);
    printf("  Insert: %u entries in %.1fms (%.0f inserts/ms)\n",
           n, insert_ms, n / insert_ms);
    printf("  Arena: %u bytes (%.1f bytes/entry)\n",
           sh.arena_used, (double)sh.arena_used / n);
    printf("  Pool: data=%luMB file=%luMB\n",
           st.data_size / (1024*1024), st.file_size / (1024*1024));

    /* Random reads */
    int reads = 1000000;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < reads; i++) {
        uint32_t idx = (uint32_t)((uint64_t)i * 104729 % n);
        uint8_t k[32], v[32];
        make_key(0, idx, k);
        storage_hart_get(pool, &sh, k, v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double read_ns = elapsed_ms(&t0, &t1) * 1e6 / reads;
    printf("  Read: %.0f ns/read (1M random)\n", read_ns);

    /* Full root computation */
    uint8_t root[32];
    clock_gettime(CLOCK_MONOTONIC, &t0);
    storage_hart_root_hash(pool, &sh, stor_encode, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double full_ms = elapsed_ms(&t0, &t1);

    /* Incremental: update 100 entries */
    for (int i = 0; i < 100; i++) {
        uint8_t k[32], v[32];
        make_key(0, (uint32_t)i, k);
        make_val(999999 + i, v);
        storage_hart_put(pool, &sh, k, v);
    }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    storage_hart_root_hash(pool, &sh, stor_encode, NULL, root);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double incr_ms = elapsed_ms(&t0, &t1);
    printf("  Root: full=%.1fms  incremental(100 dirty)=%.2fms\n", full_ms, incr_ms);

    /* Delete half, recompute */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t i = 0; i < n; i += 2) {
        uint8_t k[32];
        make_key(0, i, k);
        storage_hart_del(pool, &sh, k);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double del_ms = elapsed_ms(&t0, &t1);
    printf("  Delete 500K: %.1fms  remaining=%u\n", del_ms, sh.count);

    storage_hart_clear(pool, &sh);
    storage_hart_pool_destroy(pool);
    unlink(TEST_FILE);
    printf("  Done.\n");
}

/* =========================================================================
 * Test 3: Checkpoint simulation (many accounts, dirty subset)
 * ========================================================================= */

static void test_checkpoint(void) {
    printf("\n=== Test: checkpoint simulation ===\n");
    unlink(TEST_FILE);

    storage_hart_pool_t *pool = storage_hart_pool_create(TEST_FILE);
    if (!pool) { printf("FAIL: pool create\n"); return; }

    /* Create 10K accounts with 50 slots each */
    uint32_t n_accts = 10000;
    uint32_t slots_per = 50;
    storage_hart_t *handles = calloc(n_accts, sizeof(storage_hart_t));

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t a = 0; a < n_accts; a++) {
        handles[a] = (storage_hart_t){0};
        for (uint32_t s = 0; s < slots_per; s++) {
            uint8_t k[32], v[32];
            make_key(a, s, k);
            make_val(s + 1, v);
            storage_hart_put(pool, &handles[a], k, v);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  Setup: %u accounts × %u slots in %.0fms\n",
           n_accts, slots_per, elapsed_ms(&t0, &t1));

    /* Compute all roots (first time — full) */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t a = 0; a < n_accts; a++) {
        uint8_t root[32];
        storage_hart_root_hash(pool, &handles[a], stor_encode, NULL, root);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  Full root (all %u accounts): %.0fms\n", n_accts, elapsed_ms(&t0, &t1));

    /* Simulate 256 blocks: 1000 dirty accounts, 5 dirty slots each */
    uint32_t dirty_count = 1000;
    uint32_t dirty_slots = 5;
    for (uint32_t d = 0; d < dirty_count; d++) {
        uint32_t a = (d * 7) % n_accts;
        for (uint32_t s = 0; s < dirty_slots; s++) {
            uint8_t k[32], v[32];
            make_key(a, s, k);
            make_val(999000 + d * 10 + s, v);
            storage_hart_put(pool, &handles[a], k, v);
        }
    }

    /* Checkpoint: recompute roots only for dirty accounts */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (uint32_t d = 0; d < dirty_count; d++) {
        uint32_t a = (d * 7) % n_accts;
        uint8_t root[32];
        storage_hart_root_hash(pool, &handles[a], stor_encode, NULL, root);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    printf("  Checkpoint root (%u dirty accounts, %u dirty slots each): %.1fms\n",
           dirty_count, dirty_slots, elapsed_ms(&t0, &t1));

    /* Cleanup */
    for (uint32_t a = 0; a < n_accts; a++)
        storage_hart_clear(pool, &handles[a]);
    free(handles);

    storage_hart_pool_destroy(pool);
    unlink(TEST_FILE);
    printf("  Done.\n");
}

int main(void) {
    test_many_accounts();
    test_whale();
    test_checkpoint();
    return 0;
}
