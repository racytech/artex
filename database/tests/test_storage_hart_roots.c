/**
 * Test: storage_hart root computation matches hart root computation.
 *
 * For various entry counts, inserts the same key-value pairs into both
 * a regular hart and a storage_hart, computes root hashes, and verifies
 * they match. Also tests incremental updates (dirty-path recomputation).
 */
#include "storage_hart.h"
#include "hashed_art.h"
#include "hash.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define TEST_FILE "/dev/shm/test_storage_hart_roots.dat"
#define VALUE_SIZE 32

static int errors = 0;
#define CHECK(cond, msg) do { if (!(cond)) { printf("  FAIL: %s (line %d)\n", msg, __LINE__); errors++; } } while(0)

static void make_key(uint64_t idx, uint8_t key[32]) {
    uint8_t seed[8]; memcpy(seed, &idx, 8);
    hash_t h = hash_keccak256(seed, 8);
    memcpy(key, h.bytes, 32);
}

static void make_val(uint64_t idx, uint8_t val[32]) {
    memset(val, 0, 32);
    for (int b = 0; b < 8; b++)
        val[31 - b] = (uint8_t)((idx + 1) >> (b * 8));
}

/* RLP encode storage value — same callback used by both */
static size_t rlp_be(const uint8_t *be, size_t be_len, uint8_t *out) {
    size_t i = 0;
    while (i < be_len && be[i] == 0) i++;
    size_t len = be_len - i;
    if (len == 0)                     { out[0] = 0x80;       return 1; }
    if (len == 1 && be[i] < 0x80)     { out[0] = be[i];      return 1; }
    out[0] = 0x80 + (uint8_t)len;
    memcpy(out + 1, be + i, len);
    return 1 + len;
}

static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    return (uint32_t)rlp_be((const uint8_t *)leaf_val, VALUE_SIZE, rlp_out);
}

static uint32_t stor_encode_sh(const uint8_t key[32], const void *leaf_val,
                                uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    return (uint32_t)rlp_be((const uint8_t *)leaf_val, VALUE_SIZE, rlp_out);
}

static void print_hash(const char *label, const uint8_t h[32]) {
    printf("  %s: ", label);
    for (int i = 0; i < 8; i++) printf("%02x", h[i]);
    printf("...\n");
}

/* Test: insert N entries into both, compare roots */
static void test_root_match(storage_hart_pool_t *pool, uint32_t n) {
    printf("test_root_match (n=%u):\n", n);

    hart_t h;
    hart_init(&h, VALUE_SIZE);

    storage_hart_t sh = (storage_hart_t){0};

    for (uint32_t i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key(i, k);
        make_val(i, v);
        hart_insert(&h, k, v);
        storage_hart_put(pool, &sh, k, v);
    }

    uint8_t root_hart[32], root_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root_hart);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root_sh);

    bool match = memcmp(root_hart, root_sh, 32) == 0;
    CHECK(match, "roots match");
    if (!match) {
        print_hash("hart", root_hart);
        print_hash("sh  ", root_sh);
    }

    hart_destroy(&h);
    storage_hart_clear(pool, &sh);
    printf("  OK (count=%u)\n", n);
}

/* Test: insert N entries, compute root, then update 10, recompute */
static void test_incremental_root(storage_hart_pool_t *pool, uint32_t n) {
    printf("test_incremental_root (n=%u):\n", n);

    hart_t h;
    hart_init(&h, VALUE_SIZE);
    storage_hart_t sh = (storage_hart_t){0};

    /* Insert initial entries */
    for (uint32_t i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key(i, k);
        make_val(i, v);
        hart_insert(&h, k, v);
        storage_hart_put(pool, &sh, k, v);
    }

    /* Compute first root */
    uint8_t root1_h[32], root1_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root1_h);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root1_sh);
    CHECK(memcmp(root1_h, root1_sh, 32) == 0, "initial roots match");

    /* Update 10 entries */
    for (uint32_t i = 0; i < 10 && i < n; i++) {
        uint8_t k[32], v[32];
        make_key(i, k);
        make_val(i + 1000000, v);  /* different value */
        hart_insert(&h, k, v);
        storage_hart_put(pool, &sh, k, v);
    }

    /* Recompute (incremental — only dirty paths) */
    uint8_t root2_h[32], root2_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root2_h);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root2_sh);
    CHECK(memcmp(root2_h, root2_sh, 32) == 0, "incremental roots match");

    /* Root should differ from first computation */
    CHECK(memcmp(root1_h, root2_h, 32) != 0, "root changed after update");

    hart_destroy(&h);
    storage_hart_clear(pool, &sh);
    printf("  OK\n");
}

/* Test: insert, delete some, compute root */
static void test_root_after_delete(storage_hart_pool_t *pool, uint32_t n) {
    printf("test_root_after_delete (n=%u):\n", n);

    hart_t h;
    hart_init(&h, VALUE_SIZE);
    storage_hart_t sh = (storage_hart_t){0};

    for (uint32_t i = 0; i < n; i++) {
        uint8_t k[32], v[32];
        make_key(i, k);
        make_val(i, v);
        hart_insert(&h, k, v);
        storage_hart_put(pool, &sh, k, v);
    }

    /* Delete half the entries */
    for (uint32_t i = 0; i < n; i += 2) {
        uint8_t k[32];
        make_key(i, k);
        hart_delete(&h, k);
        storage_hart_del(pool, &sh, k);
    }

    uint8_t root_h[32], root_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root_h);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root_sh);

    bool match = memcmp(root_h, root_sh, 32) == 0;
    CHECK(match, "roots match after delete");
    if (!match) {
        print_hash("hart", root_h);
        print_hash("sh  ", root_sh);
    }

    hart_destroy(&h);
    storage_hart_clear(pool, &sh);
    printf("  OK\n");
}

/* Test: single entry (edge case) */
static void test_single_entry(storage_hart_pool_t *pool) {
    printf("test_single_entry:\n");

    hart_t h;
    hart_init(&h, VALUE_SIZE);
    storage_hart_t sh = (storage_hart_t){0};

    uint8_t k[32], v[32];
    make_key(0, k);
    make_val(0, v);
    hart_insert(&h, k, v);
    storage_hart_put(pool, &sh, k, v);

    uint8_t root_h[32], root_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root_h);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root_sh);

    CHECK(memcmp(root_h, root_sh, 32) == 0, "single entry root match");

    hart_destroy(&h);
    storage_hart_clear(pool, &sh);
    printf("  OK\n");
}

/* Test: empty trie root */
static void test_empty_root(storage_hart_pool_t *pool) {
    printf("test_empty_root:\n");

    hart_t h;
    hart_init(&h, VALUE_SIZE);
    storage_hart_t sh = (storage_hart_t){0};

    uint8_t root_h[32], root_sh[32];
    hart_root_hash(&h, stor_encode, NULL, root_h);
    storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root_sh);

    /* Both should be EMPTY_ROOT = keccak256(0x80) */
    static const uint8_t EMPTY_ROOT[32] = {
        0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
        0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
        0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
        0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
    };
    CHECK(memcmp(root_h, EMPTY_ROOT, 32) == 0, "hart empty root");
    CHECK(memcmp(root_sh, EMPTY_ROOT, 32) == 0, "sh empty root");

    hart_destroy(&h);
    printf("  OK\n");
}

/* Benchmark: root computation time */
static void bench_root(storage_hart_pool_t *pool) {
    printf("\nbench_root_computation:\n");
    uint32_t sizes[] = {10, 100, 1000, 10000};

    for (int s = 0; s < 4; s++) {
        uint32_t n = sizes[s];
        storage_hart_t sh = (storage_hart_t){0};

        for (uint32_t i = 0; i < n; i++) {
            uint8_t k[32], v[32];
            make_key(i, k);
            make_val(i, v);
            storage_hart_put(pool, &sh, k, v);
        }

        /* Full computation */
        uint8_t root[32];
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);
        storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double full_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                          (t1.tv_nsec - t0.tv_nsec) / 1e6;

        /* Update 10 entries, then incremental */
        for (uint32_t i = 0; i < 10 && i < n; i++) {
            uint8_t k[32], v[32];
            make_key(i, k);
            make_val(i + 999999, v);
            storage_hart_put(pool, &sh, k, v);
        }
        clock_gettime(CLOCK_MONOTONIC, &t0);
        storage_hart_root_hash(pool, &sh, stor_encode_sh, NULL, root);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double incr_ms = (t1.tv_sec - t0.tv_sec) * 1000.0 +
                          (t1.tv_nsec - t0.tv_nsec) / 1e6;

        printf("  %6u entries: full=%.2fms  incremental=%.2fms\n", n, full_ms, incr_ms);
        storage_hart_clear(pool, &sh);
    }
}

int main(void) {
    printf("=== storage_hart root computation tests ===\n\n");
    unlink(TEST_FILE);

    storage_hart_pool_t *pool = storage_hart_pool_create(TEST_FILE);
    if (!pool) { printf("FAIL: pool create\n"); return 1; }

    test_empty_root(pool);
    test_single_entry(pool);
    test_root_match(pool, 2);
    test_root_match(pool, 10);
    test_root_match(pool, 100);
    test_root_match(pool, 1000);
    test_root_match(pool, 10000);
    test_incremental_root(pool, 100);
    test_incremental_root(pool, 1000);
    test_root_after_delete(pool, 20);
    test_root_after_delete(pool, 200);

    bench_root(pool);

    storage_hart_pool_destroy(pool);
    unlink(TEST_FILE);

    printf("\n=== Results: %d errors ===\n", errors);
    return errors > 0 ? 1 : 0;
}
