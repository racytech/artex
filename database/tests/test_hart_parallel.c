/**
 * Test hart_root_hash_parallel produces identical results to hart_root_hash.
 *
 * Tests:
 *   1. Small tree (100 entries) — root may be Node4/Node16
 *   2. Medium tree (10000 entries) — root is Node256
 *   3. Large tree (500000 entries) — timing benchmark
 *   4. After delete — verify parallel handles reduced tree
 *   5. Incremental — modify subset, verify parallel matches serial
 */
#define _GNU_SOURCE
#include "hashed_art.h"
#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

/* Same encoder as test_hashed_art.c */
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

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 4; i++) printf("%02x", h[i]);
    printf("...");
    for (int i = 28; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static int test_n(const char *name, int n, bool benchmark) {
    printf("%s (%d entries):\n", name, n);

    hart_t tree;
    hart_init_cap(&tree, 32, n > 1024 ? (size_t)n * 64 : 65536);

    for (int i = 0; i < n; i++) {
        uint8_t key[32], val[32];
        make_key((uint32_t)i, key);
        memset(val, (uint8_t)(i + 1), 32);
        hart_insert(&tree, key, val);
    }

    hart_invalidate_all(&tree);
    uint8_t serial[32];
    double t0 = now_ms();
    hart_root_hash(&tree, encode_raw, NULL, serial);
    double t1 = now_ms();

    hart_invalidate_all(&tree);
    uint8_t parallel[32];
    double t2 = now_ms();
    hart_root_hash_parallel(&tree, encode_raw, NULL, parallel);
    double t3 = now_ms();

    print_hash("serial  ", serial);
    print_hash("parallel", parallel);

    if (benchmark) {
        printf("  serial:   %.1fms\n", t1 - t0);
        printf("  parallel: %.1fms\n", t3 - t2);
        if (t3 - t2 > 0)
            printf("  speedup:  %.1fx\n", (t1 - t0) / (t3 - t2));
    }

    int ok = memcmp(serial, parallel, 32) == 0;
    printf("  %s\n", ok ? "OK: match" : "FAIL: mismatch");
    hart_destroy(&tree);
    return ok ? 0 : 1;
}

static int test_after_delete(void) {
    printf("\ntest_after_delete (1000 insert, delete 200):\n");

    hart_t tree;
    hart_init_cap(&tree, 32, 65536);

    uint8_t keys[1000][32];
    for (int i = 0; i < 1000; i++) {
        uint8_t val[32];
        make_key((uint32_t)i, keys[i]);
        memset(val, (uint8_t)(i + 1), 32);
        hart_insert(&tree, keys[i], val);
    }

    for (int i = 0; i < 1000; i += 5)
        hart_delete(&tree, keys[i]);

    hart_invalidate_all(&tree);
    uint8_t serial[32];
    hart_root_hash(&tree, encode_raw, NULL, serial);

    hart_invalidate_all(&tree);
    uint8_t parallel[32];
    hart_root_hash_parallel(&tree, encode_raw, NULL, parallel);

    print_hash("serial  ", serial);
    print_hash("parallel", parallel);
    int ok = memcmp(serial, parallel, 32) == 0;
    printf("  %s\n", ok ? "OK: match" : "FAIL: mismatch");
    hart_destroy(&tree);
    return ok ? 0 : 1;
}

static int test_incremental(void) {
    printf("\ntest_incremental (10000 entries, modify 100):\n");

    hart_t tree;
    hart_init_cap(&tree, 32, 1 << 20);

    uint8_t keys[10000][32], vals[10000][32];
    for (int i = 0; i < 10000; i++) {
        make_key((uint32_t)i, keys[i]);
        memset(vals[i], (uint8_t)(i + 1), 32);
        hart_insert(&tree, keys[i], vals[i]);
    }

    /* Compute initial root (marks all clean) */
    uint8_t root0[32];
    hart_root_hash(&tree, encode_raw, NULL, root0);

    /* Modify 100 entries — only these paths become dirty */
    for (int i = 0; i < 100; i++) {
        memset(vals[i * 100], 0xFF, 32);
        hart_insert(&tree, keys[i * 100], vals[i * 100]);
    }

    /* Serial incremental */
    uint8_t serial[32];
    hart_root_hash(&tree, encode_raw, NULL, serial);

    /* Re-dirty for parallel */
    for (int i = 0; i < 100; i++)
        hart_insert(&tree, keys[i * 100], vals[i * 100]);

    uint8_t parallel[32];
    hart_root_hash_parallel(&tree, encode_raw, NULL, parallel);

    print_hash("serial  ", serial);
    print_hash("parallel", parallel);
    int ok = memcmp(serial, parallel, 32) == 0;
    printf("  %s\n", ok ? "OK: match" : "FAIL: mismatch");
    hart_destroy(&tree);
    return ok ? 0 : 1;
}

static int test_single_entry(void) {
    printf("\ntest_single_entry:\n");

    hart_t tree;
    hart_init(&tree, 32);
    uint8_t key[32], val[32];
    make_key(42, key);
    memset(val, 0xAB, 32);
    hart_insert(&tree, key, val);

    hart_invalidate_all(&tree);
    uint8_t serial[32], parallel[32];
    hart_root_hash(&tree, encode_raw, NULL, serial);
    hart_invalidate_all(&tree);
    hart_root_hash_parallel(&tree, encode_raw, NULL, parallel);

    print_hash("serial  ", serial);
    print_hash("parallel", parallel);
    int ok = memcmp(serial, parallel, 32) == 0;
    printf("  %s\n", ok ? "OK: match" : "FAIL: mismatch");
    hart_destroy(&tree);
    return ok ? 0 : 1;
}

int main(void) {
    int errors = 0;
    errors += test_single_entry();
    errors += test_n("\ntest_small", 100, false);
    errors += test_n("\ntest_medium", 10000, false);
    errors += test_n("\ntest_large", 500000, true);
    errors += test_after_delete();
    errors += test_incremental();

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
