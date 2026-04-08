#include "storage_hart.h"
#include "hash.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define TEST_FILE "/dev/shm/test_storage_hart.dat"
static int errors = 0;
#define CHECK(cond, msg) do { if (!(cond)) { printf("  FAIL: %s (line %d)\n", msg, __LINE__); errors++; } } while(0)

static void make_key(uint64_t idx, uint8_t key[32]) {
    uint8_t seed[8]; memcpy(seed, &idx, 8);
    hash_t h = hash_keccak256(seed, 8);
    memcpy(key, h.bytes, 32);
}

int main(void) {
    printf("=== storage_hart tests ===\n");
    unlink(TEST_FILE);

    storage_hart_pool_t *pool = storage_hart_pool_create(TEST_FILE);
    CHECK(pool != NULL, "pool create");

    /* Test 1: basic put/get */
    printf("test_basic_put_get:\n");
    storage_hart_t sh = (storage_hart_t){0};
    uint8_t k[32], v[32], out[32];
    make_key(42, k);
    memset(v, 0, 32); v[31] = 99;
    CHECK(storage_hart_put(pool, &sh, k, v), "put");
    CHECK(sh.count == 1, "count=1");
    CHECK(storage_hart_get(pool, &sh, k, out), "get");
    CHECK(out[31] == 99, "val=99");
    printf("  OK\n");

    /* Test 2: not found */
    printf("test_not_found:\n");
    uint8_t k2[32]; make_key(999, k2);
    CHECK(!storage_hart_get(pool, &sh, k2, out), "not found");
    printf("  OK\n");

    /* Test 3: update */
    printf("test_update:\n");
    memset(v, 0, 32); v[31] = 200;
    storage_hart_put(pool, &sh, k, v);
    storage_hart_get(pool, &sh, k, out);
    CHECK(out[31] == 200, "updated val=200");
    CHECK(sh.count == 1, "count still 1");
    printf("  OK\n");

    /* Test 4: multiple entries */
    printf("test_multiple:\n");
    for (int i = 0; i < 100; i++) {
        uint8_t ki[32], vi[32];
        make_key(i, ki);
        memset(vi, 0, 32); vi[31] = (uint8_t)(i + 1);
        storage_hart_put(pool, &sh, ki, vi);
    }
    CHECK(sh.count == 100, "count=100");
    for (int i = 0; i < 100; i++) {
        uint8_t ki[32], vi[32];
        make_key(i, ki);
        CHECK(storage_hart_get(pool, &sh, ki, vi), "get in loop");
        CHECK(vi[31] == (uint8_t)(i + 1), "val matches");
    }
    printf("  OK\n");

    /* Test 5: delete */
    printf("test_delete:\n");
    make_key(50, k);
    storage_hart_del(pool, &sh, k);
    CHECK(sh.count == 99, "count=99 after delete");
    CHECK(!storage_hart_get(pool, &sh, k, out), "deleted key not found");
    make_key(49, k);
    CHECK(storage_hart_get(pool, &sh, k, out), "neighbor still exists");
    printf("  OK\n");

    /* Test 6: clear */
    printf("test_clear:\n");
    storage_hart_clear(pool, &sh);
    CHECK(sh.count == 0, "count=0 after clear");
    CHECK(sh.arena_offset == 0, "offset=0 after clear");
    make_key(0, k);
    CHECK(!storage_hart_get(pool, &sh, k, out), "empty after clear");
    printf("  OK\n");

    /* Test 7: large account */
    printf("test_large:\n");
    storage_hart_t sh2 = (storage_hart_t){0};
    for (uint32_t i = 0; i < 10000; i++) {
        uint8_t ki[32], vi[32];
        make_key(i, ki);
        memset(vi, 0, 32);
        vi[28] = (uint8_t)(i >> 24); vi[29] = (uint8_t)(i >> 16);
        vi[30] = (uint8_t)(i >> 8); vi[31] = (uint8_t)i;
        storage_hart_put(pool, &sh2, ki, vi);
    }
    CHECK(sh2.count == 10000, "count=10000");
    /* Verify random samples */
    for (int i = 0; i < 100; i++) {
        uint32_t idx = (uint32_t)(i * 97) % 10000;
        uint8_t ki[32], vi[32];
        make_key(idx, ki);
        CHECK(storage_hart_get(pool, &sh2, ki, vi), "large get");
    }
    printf("  count=%u arena=%u bytes\n", sh2.count, sh2.arena_used);
    storage_hart_clear(pool, &sh2);
    printf("  OK\n");

    /* Test 8: foreach iteration */
    printf("test_foreach:\n");
    storage_hart_t sh3 = (storage_hart_t){0};
    for (int i = 0; i < 10; i++) {
        uint8_t ki[32], vi[32];
        make_key(i, ki);
        memset(vi, 0, 32); vi[31] = (uint8_t)(i + 1);
        storage_hart_put(pool, &sh3, ki, vi);
    }
    uint32_t iter_count = 0;
    storage_hart_foreach(pool, &sh3,
        (storage_hart_iter_cb)({
            bool _cb(const uint8_t key[32], const uint8_t val[32], void *ctx) {
                (*(uint32_t *)ctx)++;
                return true;
            }
            _cb;
        }), &iter_count);
    CHECK(iter_count == 10, "foreach counted 10");
    storage_hart_clear(pool, &sh3);
    printf("  OK\n");

    /* Test 9: pool persistence */
    printf("test_persistence:\n");
    storage_hart_t sh4 = (storage_hart_t){0};
    make_key(7, k); memset(v, 0, 32); v[31] = 77;
    storage_hart_put(pool, &sh4, k, v);
    uint64_t saved_offset = sh4.arena_offset;
    storage_hart_pool_sync(pool);
    storage_hart_pool_destroy(pool);

    pool = storage_hart_pool_open(TEST_FILE);
    CHECK(pool != NULL, "reopen");
    sh4.arena_offset = saved_offset; /* restore handle */
    CHECK(storage_hart_get(pool, &sh4, k, out), "get after reopen");
    CHECK(out[31] == 77, "val=77 after reopen");
    printf("  OK\n");

    storage_hart_pool_destroy(pool);
    unlink(TEST_FILE);

    printf("\n=== Results: %d errors ===\n", errors);
    return errors > 0 ? 1 : 0;
}
