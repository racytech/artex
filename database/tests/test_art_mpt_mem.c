/*
 * ART→MPT test: verify mem_art produces the same MPT root hashes
 * as compact_art for identical data.
 *
 * Strategy: insert/delete/update the same operations on both a compact_art
 * and a mem_art, compute MPT root hashes from each, verify they match.
 */

#include "art_mpt.h"
#include "art_iface.h"
#include "compact_art.h"
#include "mem_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>

/* =========================================================================
 * Test infrastructure
 * ========================================================================= */

static int tests_passed = 0;
static int tests_failed = 0;

#define CHECK(cond, ...) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: " __VA_ARGS__); \
        fprintf(stderr, " (line %d)\n", __LINE__); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define PASS(msg) do { \
    printf("  OK: %s\n", msg); \
    tests_passed++; \
} while(0)

/* =========================================================================
 * PRNG
 * ========================================================================= */

static uint64_t rng_state;
static void rng_seed(uint64_t seed) { rng_state = seed ? seed : 1; }
static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}
static uint32_t rng_range(uint32_t max) { return (uint32_t)(rng_next() % max); }
static void rng_bytes(uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++) out[i] = (uint8_t)(rng_next() & 0xff);
}

/* =========================================================================
 * Value encoder — fixed 32-byte value, return as raw bytes
 * ========================================================================= */

static uint32_t test_encode(const uint8_t *key, const void *leaf_val,
                             uint32_t val_size, uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    /* RLP encode: 0xa0 + 32 bytes for a 32-byte value */
    if (val_size < 32) return 0;
    rlp_out[0] = 0xa0;
    memcpy(rlp_out + 1, leaf_val, 32);
    return 33;
}

/* =========================================================================
 * Helpers: compute root from both trees, compare
 * ========================================================================= */

static void print_hash(const char *label, const uint8_t h[32]) {
    fprintf(stderr, "  %s: ", label);
    for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", h[i]);
    fprintf(stderr, "\n");
}

static bool roots_match(art_mpt_t *am_compact, art_mpt_t *am_mem,
                          int round, const char *op) {
    uint8_t h_compact[32], h_mem[32];
    art_mpt_root_hash(am_compact, h_compact);
    art_mpt_root_hash(am_mem, h_mem);

    if (memcmp(h_compact, h_mem, 32) != 0) {
        fprintf(stderr, "MISMATCH at round %d after %s:\n", round, op);
        print_hash("compact", h_compact);
        print_hash("mem    ", h_mem);
        return false;
    }
    return true;
}

/* =========================================================================
 * Test 1: Random inserts — both trees get same keys/values
 * ========================================================================= */

static void test_inserts(void) {
    printf("test_inserts:\n");
    rng_seed(42);

    /* compact_art */
    compact_art_t ca;
    compact_art_init(&ca, 32, 32, false, NULL, NULL);
    art_mpt_t *am_ca = art_mpt_create(&ca, test_encode, NULL);

    /* mem_art */
    mem_art_t ma;
    mem_art_init_cap(&ma, 12288);
    art_iface_mem_ctx_t ma_ctx = { .tree = &ma, .key_size = 32, .value_size = 32 };
    art_mpt_t *am_ma = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);

    CHECK(am_ca && am_ma, "create mpt contexts");

    int N = 2000;
    for (int i = 0; i < N; i++) {
        uint8_t key[32], val[32];
        rng_bytes(key, 32);
        rng_bytes(val, 32);

        compact_art_insert(&ca, key, val);
        mem_art_insert(&ma, key, 32, val, 32);

        if (i < 10 || i % 100 == 0) {
            CHECK(roots_match(am_ca, am_ma, i, "insert"),
                  "roots diverge at insert %d", i);
        }
    }

    CHECK(roots_match(am_ca, am_ma, N, "final"), "final insert verify");
    PASS("random inserts (2000 keys)");

    art_mpt_destroy(am_ca);
    art_mpt_destroy(am_ma);
    compact_art_destroy(&ca);
    mem_art_destroy(&ma);
}

/* =========================================================================
 * Test 2: Insert then update
 * ========================================================================= */

static void test_updates(void) {
    printf("test_updates:\n");
    rng_seed(99);

    compact_art_t ca;
    compact_art_init(&ca, 32, 32, false, NULL, NULL);
    art_mpt_t *am_ca = art_mpt_create(&ca, test_encode, NULL);

    mem_art_t ma;
    mem_art_init_cap(&ma, 12288);
    art_iface_mem_ctx_t ma_ctx = { .tree = &ma, .key_size = 32, .value_size = 32 };
    art_mpt_t *am_ma = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);

    /* Insert 500 keys */
    uint8_t saved_keys[500][32];
    for (int i = 0; i < 500; i++) {
        rng_bytes(saved_keys[i], 32);
        uint8_t val[32]; rng_bytes(val, 32);
        compact_art_insert(&ca, saved_keys[i], val);
        mem_art_insert(&ma, saved_keys[i], 32, val, 32);
    }
    CHECK(roots_match(am_ca, am_ma, 0, "initial"), "initial state");

    /* Update 1000 random existing keys */
    for (int i = 0; i < 1000; i++) {
        int ki = rng_range(500);
        uint8_t val[32]; rng_bytes(val, 32);
        compact_art_insert(&ca, saved_keys[ki], val);
        mem_art_insert(&ma, saved_keys[ki], 32, val, 32);

        if (i % 100 == 0) {
            CHECK(roots_match(am_ca, am_ma, i, "update"),
                  "roots diverge at update %d", i);
        }
    }

    CHECK(roots_match(am_ca, am_ma, 1000, "final"), "final update verify");
    PASS("insert then update (500 keys, 1000 updates)");

    art_mpt_destroy(am_ca);
    art_mpt_destroy(am_ma);
    compact_art_destroy(&ca);
    mem_art_destroy(&ma);
}

/* =========================================================================
 * Test 3: Insert then delete
 * ========================================================================= */

static void test_deletes(void) {
    printf("test_deletes:\n");
    rng_seed(777);

    compact_art_t ca;
    compact_art_init(&ca, 32, 32, false, NULL, NULL);
    art_mpt_t *am_ca = art_mpt_create(&ca, test_encode, NULL);

    mem_art_t ma;
    mem_art_init_cap(&ma, 12288);
    art_iface_mem_ctx_t ma_ctx = { .tree = &ma, .key_size = 32, .value_size = 32 };
    art_mpt_t *am_ma = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);

    /* Insert 1000 keys */
    uint8_t saved_keys[1000][32];
    bool active[1000];
    for (int i = 0; i < 1000; i++) {
        rng_bytes(saved_keys[i], 32);
        uint8_t val[32]; rng_bytes(val, 32);
        compact_art_insert(&ca, saved_keys[i], val);
        mem_art_insert(&ma, saved_keys[i], 32, val, 32);
        active[i] = true;
    }
    CHECK(roots_match(am_ca, am_ma, 0, "initial"), "initial state");

    /* Delete 800 random keys */
    int deleted = 0;
    for (int round = 0; deleted < 800; round++) {
        int ki = rng_range(1000);
        if (!active[ki]) continue;

        compact_art_delete(&ca, saved_keys[ki]);
        mem_art_delete(&ma, saved_keys[ki], 32);
        active[ki] = false;
        deleted++;

        if (deleted % 100 == 0) {
            CHECK(roots_match(am_ca, am_ma, deleted, "delete"),
                  "roots diverge at delete %d", deleted);
        }
    }

    CHECK(roots_match(am_ca, am_ma, deleted, "final"), "final delete verify");
    PASS("insert then delete (1000 keys, delete 800)");

    art_mpt_destroy(am_ca);
    art_mpt_destroy(am_ma);
    compact_art_destroy(&ca);
    mem_art_destroy(&ma);
}

/* =========================================================================
 * Test 4: Mixed operations
 * ========================================================================= */

static void test_mixed(void) {
    printf("test_mixed:\n");
    rng_seed(31337);

    compact_art_t ca;
    compact_art_init(&ca, 32, 32, false, NULL, NULL);
    art_mpt_t *am_ca = art_mpt_create(&ca, test_encode, NULL);

    mem_art_t ma;
    mem_art_init_cap(&ma, 12288);
    art_iface_mem_ctx_t ma_ctx = { .tree = &ma, .key_size = 32, .value_size = 32 };
    art_mpt_t *am_ma = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);

    uint8_t all_keys[5000][32];
    bool active[5000];
    int num_keys = 0, active_count = 0;
    memset(active, 0, sizeof(active));

    for (int round = 0; round < 5000; round++) {
        int op = rng_range(10);  /* 0-5: insert, 6-8: update, 9: delete */

        if (op <= 5 && num_keys < 5000) {
            /* Insert */
            rng_bytes(all_keys[num_keys], 32);
            uint8_t val[32]; rng_bytes(val, 32);
            compact_art_insert(&ca, all_keys[num_keys], val);
            mem_art_insert(&ma, all_keys[num_keys], 32, val, 32);
            active[num_keys] = true;
            num_keys++;
            active_count++;
        } else if (op <= 8 && active_count > 0) {
            /* Update */
            int ki = rng_range(num_keys);
            while (!active[ki]) ki = rng_range(num_keys);
            uint8_t val[32]; rng_bytes(val, 32);
            compact_art_insert(&ca, all_keys[ki], val);
            mem_art_insert(&ma, all_keys[ki], 32, val, 32);
        } else if (active_count > 0) {
            /* Delete */
            int ki = rng_range(num_keys);
            while (!active[ki]) ki = rng_range(num_keys);
            compact_art_delete(&ca, all_keys[ki]);
            mem_art_delete(&ma, all_keys[ki], 32);
            active[ki] = false;
            active_count--;
        }

        if (round % 250 == 0) {
            CHECK(roots_match(am_ca, am_ma, round, "mixed"),
                  "roots diverge at mixed round %d", round);
        }
    }

    CHECK(roots_match(am_ca, am_ma, 5000, "final"), "final mixed verify");
    PASS("mixed operations (5000 rounds)");

    art_mpt_destroy(am_ca);
    art_mpt_destroy(am_ma);
    compact_art_destroy(&ca);
    mem_art_destroy(&ma);
}

/* =========================================================================
 * Test 5: Incremental vs full recompute for mem_art
 * ========================================================================= */

static void test_mem_incremental(void) {
    printf("test_mem_incremental:\n");
    rng_seed(55555);

    mem_art_t ma;
    mem_art_init_cap(&ma, 12288);
    art_iface_mem_ctx_t ma_ctx = { .tree = &ma, .key_size = 32, .value_size = 32 };

    /* Incremental context */
    art_mpt_t *am_inc = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);

    int N = 1000;
    uint8_t saved_keys[1000][32];

    for (int i = 0; i < N; i++) {
        rng_bytes(saved_keys[i], 32);
        uint8_t val[32]; rng_bytes(val, 32);
        mem_art_insert(&ma, saved_keys[i], 32, val, 32);

        if (i < 10 || i % 100 == 0) {
            /* Compute incremental */
            uint8_t h_inc[32];
            art_mpt_root_hash(am_inc, h_inc);

            /* Compute full (fresh context, no cache) */
            art_mpt_t *am_full = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);
            uint8_t h_full[32];
            art_mpt_root_hash(am_full, h_full);
            art_mpt_destroy(am_full);

            if (memcmp(h_inc, h_full, 32) != 0) {
                fprintf(stderr, "  FAIL: incremental vs full diverge at insert %d\n", i);
                print_hash("incremental", h_inc);
                print_hash("full       ", h_full);
                tests_failed++;
                art_mpt_destroy(am_inc);
                mem_art_destroy(&ma);
                return;
            }
        }
    }

    /* Delete some and verify */
    for (int i = 0; i < 500; i++) {
        mem_art_delete(&ma, saved_keys[i], 32);

        if (i % 100 == 0) {
            uint8_t h_inc[32];
            art_mpt_root_hash(am_inc, h_inc);

            art_mpt_t *am_full = art_mpt_create_iface(art_iface_mem(&ma_ctx), test_encode, NULL);
            uint8_t h_full[32];
            art_mpt_root_hash(am_full, h_full);
            art_mpt_destroy(am_full);

            CHECK(memcmp(h_inc, h_full, 32) == 0,
                  "incremental vs full diverge at delete %d", i);
        }
    }

    PASS("mem_art incremental vs full (1000 insert, 500 delete)");

    art_mpt_destroy(am_inc);
    mem_art_destroy(&ma);
}

/* =========================================================================
 * main
 * ========================================================================= */

int main(void) {
    uint32_t seed = (uint32_t)time(NULL);
    printf("=== ART→MPT mem_art Cross-Validation (seed=%u) ===\n\n", seed);

    test_inserts();
    test_updates();
    test_deletes();
    test_mixed();
    test_mem_incremental();

    printf("\n=== Results: %d passed, %d failed (seed=%u) ===\n",
           tests_passed, tests_failed, seed);

    return tests_failed > 0 ? 1 : 0;
}
