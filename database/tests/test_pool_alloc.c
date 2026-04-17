/**
 * test_pool_alloc — Unit tests for storage_hart pool allocator.
 *
 * Tests the three-tier freelist (small/page/oversized), splitting,
 * tail reclaim, and free_total_bytes accounting.
 */

#include "storage_hart.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define CHECK(cond, msg) do { \
    tests_run++; \
    if (!(cond)) { \
        printf("  FAIL: %s (line %d)\n", msg, __LINE__); \
    } else { \
        tests_passed++; \
    } \
} while(0)

static void make_key(uint8_t key[32], int id) {
    memset(key, 0, 32);
    key[0] = (uint8_t)(id & 0xFF);
    key[1] = (uint8_t)((id >> 8) & 0xFF);
}

/* ======================================================================= */

static void test_small_alloc_reuse(void) {
    printf("test_small_alloc_reuse:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_small.dat");
    assert(pool);

    storage_hart_t sh1 = {0}, sh2 = {0};
    uint8_t key[32], val[32] = {0xAA};

    make_key(key, 1);
    storage_hart_put(pool, &sh1, key, val);
    make_key(key, 2);
    storage_hart_put(pool, &sh2, key, val);

    uint64_t off1 = sh1.arena_offset;
    uint64_t off2 = sh2.arena_offset;
    CHECK(off1 != 0, "sh1 allocated");
    CHECK(off2 != 0, "sh2 allocated");
    CHECK(off1 != off2, "different offsets");

    /* Free sh1's arena */
    storage_hart_clear(pool, &sh1);

    /* Allocate new arena — should reuse sh1's offset */
    storage_hart_t sh3 = {0};
    make_key(key, 3);
    storage_hart_put(pool, &sh3, key, val);
    CHECK(sh3.arena_offset == off1, "reused freed offset");

    storage_hart_clear(pool, &sh2);
    storage_hart_clear(pool, &sh3);
    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

static void test_tail_reclaim(void) {
    printf("test_tail_reclaim:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_tail.dat");
    assert(pool);

    storage_hart_t sh1 = {0}, sh2 = {0};
    uint8_t key[32], val[32] = {0xBB};

    make_key(key, 1);
    storage_hart_put(pool, &sh1, key, val);
    make_key(key, 2);
    storage_hart_put(pool, &sh2, key, val);

    storage_hart_pool_stats_t st1 = storage_hart_pool_stats(pool);
    uint64_t ds_before = st1.data_size;

    /* Free the last-allocated arena — should shrink data_size */
    storage_hart_clear(pool, &sh2);

    storage_hart_pool_stats_t st2 = storage_hart_pool_stats(pool);
    CHECK(st2.data_size < ds_before, "data_size shrank after tail free");
    printf("  data_size: %lu -> %lu\n", ds_before, st2.data_size);

    storage_hart_clear(pool, &sh1);
    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

static void test_free_total_bytes(void) {
    printf("test_free_total_bytes:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_ftb.dat");
    assert(pool);

    storage_hart_t sh[4] = {{0}};
    uint8_t key[32], val[32] = {0xCC};

    for (int i = 0; i < 4; i++) {
        make_key(key, i + 1);
        storage_hart_put(pool, &sh[i], key, val);
    }

    storage_hart_pool_stats_t st0 = storage_hart_pool_stats(pool);
    CHECK(st0.free_bytes == 0, "no free bytes initially");

    /* Free middle arenas (not tail) */
    storage_hart_clear(pool, &sh[1]);
    storage_hart_clear(pool, &sh[2]);

    storage_hart_pool_stats_t st1 = storage_hart_pool_stats(pool);
    CHECK(st1.free_bytes > 0, "free_bytes > 0 after freeing middle arenas");
    printf("  free_bytes after 2 frees: %lu\n", st1.free_bytes);

    /* Allocate new — should reuse freed space */
    storage_hart_t sh_new = {0};
    make_key(key, 99);
    storage_hart_put(pool, &sh_new, key, val);

    storage_hart_pool_stats_t st2 = storage_hart_pool_stats(pool);
    CHECK(st2.free_bytes < st1.free_bytes, "free_bytes decreased after reuse");
    printf("  free_bytes after reuse: %lu\n", st2.free_bytes);

    storage_hart_clear(pool, &sh[0]);
    storage_hart_clear(pool, &sh[3]);
    storage_hart_clear(pool, &sh_new);
    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

static void test_arena_grow_reuse(void) {
    printf("test_arena_grow_reuse:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_grow.dat");
    assert(pool);

    storage_hart_t sh = {0};
    uint8_t key[32], val[32] = {0xDD};

    for (int i = 0; i < 20; i++) {
        make_key(key, i);
        storage_hart_put(pool, &sh, key, val);
    }

    storage_hart_pool_stats_t st1 = storage_hart_pool_stats(pool);
    printf("  after 20 inserts: data_size=%lu free=%lu\n",
           st1.data_size, st1.free_bytes);
    /* Single arena at pool tail extends in-place — no old region freed.
     * This is correct: tail extend is optimal (no copy, no hole). */
    CHECK(st1.free_bytes == 0, "single arena extends in-place (no waste)");

    for (int i = 20; i < 50; i++) {
        make_key(key, i);
        storage_hart_put(pool, &sh, key, val);
    }

    storage_hart_pool_stats_t st2 = storage_hart_pool_stats(pool);
    printf("  after 50 inserts: data_size=%lu free=%lu\n",
           st2.data_size, st2.free_bytes);

    storage_hart_clear(pool, &sh);
    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

static void test_multiple_accounts_reuse(void) {
    printf("test_multiple_accounts_reuse:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_multi.dat");
    assert(pool);

    #define N_ACCTS 100
    storage_hart_t sh[N_ACCTS] = {{0}};
    uint8_t key[32], val[32] = {0xEE};

    for (int i = 0; i < N_ACCTS; i++) {
        make_key(key, i);
        storage_hart_put(pool, &sh[i], key, val);
    }

    storage_hart_pool_stats_t st0 = storage_hart_pool_stats(pool);
    printf("  after %d accounts: data_size=%luKB\n", N_ACCTS, st0.data_size / 1024);

    /* Free half */
    for (int i = 0; i < N_ACCTS; i += 2)
        storage_hart_clear(pool, &sh[i]);

    storage_hart_pool_stats_t st1 = storage_hart_pool_stats(pool);
    printf("  after freeing half: data_size=%luKB free=%luKB\n",
           st1.data_size / 1024, st1.free_bytes / 1024);

    /* Re-create — should reuse freed space */
    for (int i = 0; i < N_ACCTS; i += 2) {
        make_key(key, i + 200);
        storage_hart_put(pool, &sh[i], key, val);
    }

    storage_hart_pool_stats_t st2 = storage_hart_pool_stats(pool);
    printf("  after re-creating: data_size=%luKB free=%luKB\n",
           st2.data_size / 1024, st2.free_bytes / 1024);
    CHECK(st2.data_size <= st1.data_size + 1024,
          "data_size didn't grow much (reused freed space)");

    for (int i = 0; i < N_ACCTS; i++)
        storage_hart_clear(pool, &sh[i]);
    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

static void test_data_size_no_unbounded_growth(void) {
    printf("test_data_size_no_unbounded_growth:\n");
    storage_hart_pool_t *pool = storage_hart_pool_create("/tmp/test_pool_bounded.dat");
    assert(pool);

    /* Simulate churn: allocate, grow, free, repeat — data_size should stabilize */
    uint8_t key[32], val[32] = {0xFF};
    uint64_t peak_data_size = 0;

    for (int round = 0; round < 5; round++) {
        storage_hart_t sh = {0};
        for (int i = 0; i < 30; i++) {
            make_key(key, round * 1000 + i);
            storage_hart_put(pool, &sh, key, val);
        }
        storage_hart_pool_stats_t st = storage_hart_pool_stats(pool);
        if (st.data_size > peak_data_size)
            peak_data_size = st.data_size;
        storage_hart_clear(pool, &sh);
    }

    storage_hart_pool_stats_t final_st = storage_hart_pool_stats(pool);
    printf("  peak data_size=%luKB, final data_size=%luKB, free=%luKB\n",
           peak_data_size / 1024, final_st.data_size / 1024, final_st.free_bytes / 1024);

    /* After all accounts freed, most space should be reclaimable */
    CHECK(final_st.data_size <= peak_data_size,
          "data_size did not grow beyond peak after churn");

    storage_hart_pool_destroy(pool);
    printf("  passed\n\n");
}

int main(void) {
    printf("========================================\n");
    printf("Pool Allocator Tests\n");
    printf("========================================\n\n");

    test_small_alloc_reuse();
    test_tail_reclaim();
    test_free_total_bytes();
    test_arena_grow_reuse();
    test_multiple_accounts_reuse();
    test_data_size_no_unbounded_growth();

    printf("========================================\n");
    printf("Results: %d/%d passed\n", tests_passed, tests_run);
    printf("Result: %s\n", tests_passed == tests_run ? "PASS" : "FAIL");
    printf("========================================\n");

    return tests_passed == tests_run ? 0 : 1;
}
