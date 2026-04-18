/**
 * bench_storage_hart_scale — Scale benchmark for storage_hart pool.
 *
 * Simulates real chain_replay patterns:
 *   - Many accounts with varying storage sizes (1-10K slots)
 *   - Storage growth causes arena resizes (pool_alloc + copy + pool_free)
 *   - Measures pool fragmentation, freelist efficiency, and waste
 *
 * Usage: bench_storage_hart_scale [num_accounts] [slots_per_account]
 */

#include "storage_hart.h"
#include "keccak256.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void make_key(uint64_t idx, uint8_t key[32]) {
    uint8_t buf[8];
    for (int i = 0; i < 8; i++) buf[i] = (uint8_t)(idx >> (i * 8));
    hash_t h = hash_keccak256(buf, 8);
    memcpy(key, h.bytes, 32);
}

static double elapsed_ms(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1e6;
}

static void print_pool_stats(storage_hart_pool_t *pool, const char *label) {
    storage_hart_pool_stats_t st = storage_hart_pool_stats(pool);
    printf("\n=== %s ===\n", label);
    printf("  pool data_size:  %.1f MB\n", st.data_size / (1024.0 * 1024));
    printf("  pool free_bytes: %.1f MB (%.1f%%)\n",
           st.free_bytes / (1024.0 * 1024),
           st.data_size > 0 ? 100.0 * st.free_bytes / st.data_size : 0);
    printf("  pool file_size:  %.1f MB\n", st.file_size / (1024.0 * 1024));
    printf("  live data:       %.1f MB\n",
           (st.data_size - st.free_bytes) / (1024.0 * 1024));
}

/* ======================================================================= */

static void test_many_small_accounts(storage_hart_pool_t *pool) {
    printf("\n########## Test: Many small accounts (1-5 slots each) ##########\n");

    int num_accounts = 10000;
    storage_hart_t *sh = calloc(num_accounts, sizeof(storage_hart_t));
    uint8_t val[32] = {0xAA};
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int a = 0; a < num_accounts; a++) {
        int slots = 1 + (a % 5);  /* 1-5 slots per account */
        for (int s = 0; s < slots; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 1000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    char label[128];
    snprintf(label, sizeof(label), "%d small accounts (1-5 slots, %.0fms)",
             num_accounts, elapsed_ms(&t0, &t1));
    print_pool_stats(pool, label);

    /* Compute per-account stats */
    uint64_t total_arena_cap = 0;
    uint64_t total_arena_used = 0;
    for (int a = 0; a < num_accounts; a++) {
        total_arena_cap += sh[a].arena_cap;
        total_arena_used += sh[a].arena_used;
    }
    printf("  total arena_cap:  %.1f MB\n", total_arena_cap / (1024.0 * 1024));
    printf("  total arena_used: %.1f MB\n", total_arena_used / (1024.0 * 1024));
    printf("  internal waste:   %.1f MB (%.1f%%)\n",
           (total_arena_cap - total_arena_used) / (1024.0 * 1024),
           total_arena_cap > 0 ? 100.0 * (total_arena_cap - total_arena_used) / total_arena_cap : 0);
    printf("  avg arena_cap:    %lu bytes\n", total_arena_cap / num_accounts);
    printf("  avg arena_used:   %lu bytes\n", total_arena_used / num_accounts);

    /* Cleanup */
    for (int a = 0; a < num_accounts; a++)
        storage_hart_clear(pool, &sh[a]);
    free(sh);

    print_pool_stats(pool, "After cleanup (all freed)");
}

static void test_growing_accounts(storage_hart_pool_t *pool) {
    printf("\n########## Test: Accounts that grow over time ##########\n");

    int num_accounts = 1000;
    int rounds = 10;        /* simulate 10 blocks of growth */
    int slots_per_round = 5;
    storage_hart_t *sh = calloc(num_accounts, sizeof(storage_hart_t));
    uint8_t val[32] = {0xBB};
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int r = 0; r < rounds; r++) {
        for (int a = 0; a < num_accounts; a++) {
            for (int s = 0; s < slots_per_round; s++) {
                uint8_t key[32];
                make_key((uint64_t)a * 100000 + r * 1000 + s, key);
                storage_hart_put(pool, &sh[a], key, val);
            }
        }
        if (r == 0 || r == 4 || r == 9) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            char label[128];
            snprintf(label, sizeof(label), "Round %d/%d (%d slots/acct, %.0fms)",
                     r + 1, rounds, (r + 1) * slots_per_round, elapsed_ms(&t0, &t1));
            print_pool_stats(pool, label);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    uint64_t total_cap = 0, total_used = 0;
    for (int a = 0; a < num_accounts; a++) {
        total_cap += sh[a].arena_cap;
        total_used += sh[a].arena_used;
    }
    printf("  total arena_cap:  %.1f MB\n", total_cap / (1024.0 * 1024));
    printf("  total arena_used: %.1f MB\n", total_used / (1024.0 * 1024));
    printf("  internal waste:   %.1f%% (cap vs used)\n",
           total_cap > 0 ? 100.0 * (total_cap - total_used) / total_cap : 0);

    for (int a = 0; a < num_accounts; a++)
        storage_hart_clear(pool, &sh[a]);
    free(sh);
    print_pool_stats(pool, "After cleanup");
}

static void test_mixed_sizes(storage_hart_pool_t *pool) {
    printf("\n########## Test: Mixed account sizes (realistic) ##########\n");

    /* Simulate mainnet distribution:
     * 90% accounts: 1-10 slots (EOAs, simple tokens)
     * 9% accounts:  10-100 slots (DEX pools, lending)
     * 1% accounts:  100-1000 slots (whale contracts) */
    int total_accounts = 10000;
    storage_hart_t *sh = calloc(total_accounts, sizeof(storage_hart_t));
    uint8_t val[32] = {0xCC};
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    uint64_t total_slots = 0;
    for (int a = 0; a < total_accounts; a++) {
        int slots;
        if (a < total_accounts * 90 / 100)
            slots = 1 + (a % 10);             /* 1-10 */
        else if (a < total_accounts * 99 / 100)
            slots = 10 + (a % 90);            /* 10-100 */
        else
            slots = 100 + (a % 900);          /* 100-1000 */

        for (int s = 0; s < slots; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
        total_slots += slots;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    char label[128];
    snprintf(label, sizeof(label), "%d accounts, %lu total slots (%.0fms)",
             total_accounts, total_slots, elapsed_ms(&t0, &t1));
    print_pool_stats(pool, label);

    uint64_t total_cap = 0, total_used = 0;
    uint64_t small_cap = 0, small_used = 0;
    uint64_t med_cap = 0, med_used = 0;
    uint64_t large_cap = 0, large_used = 0;
    for (int a = 0; a < total_accounts; a++) {
        total_cap += sh[a].arena_cap;
        total_used += sh[a].arena_used;
        if (a < total_accounts * 90 / 100) {
            small_cap += sh[a].arena_cap;
            small_used += sh[a].arena_used;
        } else if (a < total_accounts * 99 / 100) {
            med_cap += sh[a].arena_cap;
            med_used += sh[a].arena_used;
        } else {
            large_cap += sh[a].arena_cap;
            large_used += sh[a].arena_used;
        }
    }

    printf("  Per-tier breakdown:\n");
    printf("    Small (90%%): cap=%.1fMB used=%.1fMB waste=%.1f%%\n",
           small_cap/(1024.0*1024), small_used/(1024.0*1024),
           small_cap > 0 ? 100.0*(small_cap-small_used)/small_cap : 0);
    printf("    Medium (9%%): cap=%.1fMB used=%.1fMB waste=%.1f%%\n",
           med_cap/(1024.0*1024), med_used/(1024.0*1024),
           med_cap > 0 ? 100.0*(med_cap-med_used)/med_cap : 0);
    printf("    Large  (1%%): cap=%.1fMB used=%.1fMB waste=%.1f%%\n",
           large_cap/(1024.0*1024), large_used/(1024.0*1024),
           large_cap > 0 ? 100.0*(large_cap-large_used)/large_cap : 0);
    printf("  Total: cap=%.1fMB used=%.1fMB waste=%.1f%%\n",
           total_cap/(1024.0*1024), total_used/(1024.0*1024),
           total_cap > 0 ? 100.0*(total_cap-total_used)/total_cap : 0);

    for (int a = 0; a < total_accounts; a++)
        storage_hart_clear(pool, &sh[a]);
    free(sh);
    print_pool_stats(pool, "After cleanup");
}

static void test_churn(storage_hart_pool_t *pool) {
    printf("\n########## Test: Storage churn (add/delete/re-add) ##########\n");

    int num_accounts = 5000;
    storage_hart_t *sh = calloc(num_accounts, sizeof(storage_hart_t));
    uint8_t val[32] = {0xDD};
    struct timespec t0, t1;

    /* Phase 1: Insert 10 slots each */
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int a = 0; a < num_accounts; a++) {
        for (int s = 0; s < 10; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }
    print_pool_stats(pool, "Phase 1: 5K accounts × 10 slots");

    /* Phase 2: Delete 5 slots from each, add 5 new ones */
    for (int a = 0; a < num_accounts; a++) {
        for (int s = 0; s < 5; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_del(pool, &sh[a], key);
        }
        for (int s = 10; s < 15; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }
    print_pool_stats(pool, "Phase 2: Deleted 5 + added 5 per account");

    /* Phase 3: Grow each account to 20 slots */
    for (int a = 0; a < num_accounts; a++) {
        for (int s = 15; s < 20; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    char label[128];
    snprintf(label, sizeof(label), "Phase 3: Grown to 15 slots each (%.0fms total)",
             elapsed_ms(&t0, &t1));
    print_pool_stats(pool, label);

    uint64_t total_cap = 0, total_used = 0;
    for (int a = 0; a < num_accounts; a++) {
        total_cap += sh[a].arena_cap;
        total_used += sh[a].arena_used;
    }
    printf("  arena cap vs used: %.1fMB / %.1fMB (%.1f%% internal waste)\n",
           total_cap/(1024.0*1024), total_used/(1024.0*1024),
           total_cap > 0 ? 100.0*(total_cap-total_used)/total_cap : 0);

    for (int a = 0; a < num_accounts; a++)
        storage_hart_clear(pool, &sh[a]);
    free(sh);
    print_pool_stats(pool, "After cleanup");
}

static void test_pool_reuse_after_clear(storage_hart_pool_t *pool) {
    printf("\n########## Test: Pool reuse after account destruction ##########\n");

    int num_accounts = 5000;
    storage_hart_t *sh = calloc(num_accounts, sizeof(storage_hart_t));
    uint8_t val[32] = {0xEE};

    /* Create accounts */
    for (int a = 0; a < num_accounts; a++) {
        for (int s = 0; s < 10; s++) {
            uint8_t key[32];
            make_key((uint64_t)a * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }
    storage_hart_pool_stats_t st1 = storage_hart_pool_stats(pool);
    printf("  After creation: data_size=%.1fMB\n", st1.data_size/(1024.0*1024));

    /* Destroy half */
    for (int a = 0; a < num_accounts; a += 2)
        storage_hart_clear(pool, &sh[a]);

    storage_hart_pool_stats_t st2 = storage_hart_pool_stats(pool);
    printf("  After destroying half: data_size=%.1fMB free=%.1fMB\n",
           st2.data_size/(1024.0*1024), st2.free_bytes/(1024.0*1024));

    /* Create new accounts in freed space */
    for (int a = 0; a < num_accounts; a += 2) {
        memset(&sh[a], 0, sizeof(sh[a]));
        for (int s = 0; s < 10; s++) {
            uint8_t key[32];
            make_key((uint64_t)(num_accounts + a) * 100000 + s, key);
            storage_hart_put(pool, &sh[a], key, val);
        }
    }

    storage_hart_pool_stats_t st3 = storage_hart_pool_stats(pool);
    printf("  After re-creation: data_size=%.1fMB free=%.1fMB\n",
           st3.data_size/(1024.0*1024), st3.free_bytes/(1024.0*1024));
    printf("  data_size growth: %.1fMB (ideal=0 if fully reused)\n",
           (st3.data_size - st1.data_size)/(1024.0*1024));

    for (int a = 0; a < num_accounts; a++)
        storage_hart_clear(pool, &sh[a]);
    free(sh);
    print_pool_stats(pool, "After cleanup");
}

int main(int argc, char **argv) {
    printf("========================================\n");
    printf("Storage Hart Pool Scale Benchmark\n");
    printf("========================================\n");

    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/bench_stor_pool.dat");
    if (!pool) {
        fprintf(stderr, "Failed to create pool\n");
        return 1;
    }

    test_many_small_accounts(pool);
    test_growing_accounts(pool);
    test_mixed_sizes(pool);
    test_churn(pool);
    test_pool_reuse_after_clear(pool);

    printf("\n========================================\n");
    printf("Final pool state:\n");
    storage_hart_pool_stats_t final = storage_hart_pool_stats(pool);
    printf("  data_size:  %.1f MB\n", final.data_size / (1024.0*1024));
    printf("  free_bytes: %.1f MB\n", final.free_bytes / (1024.0*1024));
    printf("  file_size:  %.1f MB\n", final.file_size / (1024.0*1024));
    printf("========================================\n");

    storage_hart_pool_destroy(pool);
    return 0;
}
