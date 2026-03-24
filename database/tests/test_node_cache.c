/**
 * Test Node Cache — stress test for two-list LRU with depth pinning.
 *
 * Simulates the scenario at mainnet scale where:
 *   1. Branch nodes (depth 0-4) are loaded during trie walks
 *   2. A burst of leaf nodes (depth 7-8) floods the cache
 *   3. We check if branch nodes survive (pinned) or are evicted (unpinned)
 *
 * Compares hit rates between the two scenarios to validate pinning.
 */

#include "node_cache.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Generate a deterministic 32-byte hash from an index */
static void make_hash(uint8_t hash[32], uint64_t idx) {
    memset(hash, 0, 32);
    memcpy(hash, &idx, sizeof(idx));
    /* Mix bits for better distribution */
    uint64_t h = idx;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    memcpy(hash, &h, 8);
    memcpy(hash + 8, &idx, 8);
}

/* Generate fake RLP data */
static void make_rlp(uint8_t *rlp, uint16_t *len, uint64_t idx) {
    *len = 64 + (idx % 200);  /* 64-263 bytes */
    memset(rlp, (uint8_t)(idx & 0xFF), *len);
}

#define CHECK(cond, ...) do { \
    tests_run++; \
    if (!(cond)) { \
        printf("  FAIL: "); printf(__VA_ARGS__); printf(" (line %d)\n", __LINE__); \
    } else { \
        tests_passed++; \
    } \
} while(0)

static int tests_run = 0;
static int tests_passed = 0;

/* =========================================================================
 * Test 1: Basic pinning — depth 0-4 go to pinned list
 * ========================================================================= */

static void test_basic_pinning(void) {
    printf("--- test_basic_pinning ---\n");

    node_cache_t nc;
    node_cache_init(&nc, 1024 * 1024);  /* 1MB budget */

    uint8_t hash[32], rlp[NODE_CACHE_MAX_RLP];
    uint16_t rlp_len;

    /* Insert 100 pinned nodes (depth 0-4) */
    for (int i = 0; i < 100; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        node_cache_put(&nc, hash, rlp, rlp_len, (uint8_t)(i % 5));
    }

    node_cache_stats_t s = node_cache_stats(&nc);
    CHECK(s.pin_count == 100, "expected 100 pinned, got %lu", s.pin_count);
    CHECK(s.entry_count == 100, "expected 100 entries, got %lu", s.entry_count);
    CHECK(s.evictions == 0, "expected 0 evictions, got %lu", s.evictions);

    /* All should be retrievable */
    uint8_t buf[NODE_CACHE_MAX_RLP];
    int hits = 0;
    for (int i = 0; i < 100; i++) {
        make_hash(hash, i);
        if (node_cache_get(&nc, hash, buf, sizeof(buf)) > 0) hits++;
    }
    CHECK(hits == 100, "expected 100 hits, got %d", hits);

    node_cache_destroy(&nc);
}

/* =========================================================================
 * Test 2: Unpinned eviction — depth 5+ evicted when over budget
 * ========================================================================= */

static void test_unpinned_eviction(void) {
    printf("--- test_unpinned_eviction ---\n");

    /* Small cache: ~50 entries worth */
    uint64_t budget = 50 * sizeof(nc_entry_t);
    node_cache_t nc;
    node_cache_init(&nc, budget);

    uint8_t hash[32], rlp[NODE_CACHE_MAX_RLP];
    uint16_t rlp_len;

    /* Insert 100 unpinned nodes (depth 8) — should evict ~50 */
    for (int i = 0; i < 100; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        node_cache_put(&nc, hash, rlp, rlp_len, 8);
    }

    node_cache_stats_t s = node_cache_stats(&nc);
    CHECK(s.evictions > 0, "expected evictions, got %lu", s.evictions);
    CHECK(s.pin_count == 0, "expected 0 pinned, got %lu", s.pin_count);
    CHECK(s.entry_count <= 50, "expected <=50 entries, got %lu", s.entry_count);

    node_cache_destroy(&nc);
}

/* =========================================================================
 * Test 3: Pinned survive leaf burst — the key scenario
 * ========================================================================= */

static void test_pinned_survive_burst(void) {
    printf("--- test_pinned_survive_burst ---\n");

    /* Budget: ~500 entries. We'll pin 50, then flood with 1000 unpinned. */
    uint64_t budget = 500 * sizeof(nc_entry_t);
    node_cache_t nc;
    node_cache_init(&nc, budget);

    uint8_t hash[32], rlp[NODE_CACHE_MAX_RLP], buf[NODE_CACHE_MAX_RLP];
    uint16_t rlp_len;

    /* Insert 50 branch nodes (depth 0-4) — these should survive */
    for (int i = 0; i < 50; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        node_cache_put(&nc, hash, rlp, rlp_len, (uint8_t)(i % 5));
    }

    node_cache_stats_t s1 = node_cache_stats(&nc);
    CHECK(s1.pin_count == 50, "expected 50 pinned, got %lu", s1.pin_count);

    /* Flood with 1000 leaf nodes (depth 8) — exceeds budget massively */
    for (int i = 1000; i < 2000; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        node_cache_put(&nc, hash, rlp, rlp_len, 8);
    }

    node_cache_stats_t s2 = node_cache_stats(&nc);
    CHECK(s2.evictions > 0, "expected evictions after flood");
    CHECK(s2.pin_count == 50, "pinned should survive, got %lu", s2.pin_count);

    /* Verify ALL 50 branch nodes are still accessible */
    int branch_hits = 0;
    for (int i = 0; i < 50; i++) {
        make_hash(hash, i);
        if (node_cache_get(&nc, hash, buf, sizeof(buf)) > 0) branch_hits++;
    }
    CHECK(branch_hits == 50, "all 50 branch nodes should survive, got %d hits", branch_hits);

    /* Some leaf nodes should have been evicted */
    int leaf_hits = 0;
    for (int i = 1000; i < 2000; i++) {
        make_hash(hash, i);
        if (node_cache_get(&nc, hash, buf, sizeof(buf)) > 0) leaf_hits++;
    }
    CHECK(leaf_hits < 1000, "some leaves should be evicted, got %d hits", leaf_hits);

    printf("  Branch survival: %d/50, Leaf retention: %d/1000\n",
           branch_hits, leaf_hits);

    node_cache_destroy(&nc);
}

/* =========================================================================
 * Test 4: Promotion — unknown depth → known depth pins the node
 * ========================================================================= */

static void test_promotion(void) {
    printf("--- test_promotion ---\n");

    uint64_t budget = 100 * sizeof(nc_entry_t);
    node_cache_t nc;
    node_cache_init(&nc, budget);

    uint8_t hash[32], rlp[NODE_CACHE_MAX_RLP], buf[NODE_CACHE_MAX_RLP];
    uint16_t rlp_len;

    /* Insert with unknown depth — goes to unpinned */
    make_hash(hash, 42);
    make_rlp(rlp, &rlp_len, 42);
    node_cache_put(&nc, hash, rlp, rlp_len, NC_DEPTH_UNKNOWN);

    node_cache_stats_t s1 = node_cache_stats(&nc);
    CHECK(s1.pin_count == 0, "should be unpinned initially");

    /* Re-insert with depth 2 — should promote to pinned */
    node_cache_put(&nc, hash, rlp, rlp_len, 2);

    node_cache_stats_t s2 = node_cache_stats(&nc);
    CHECK(s2.pin_count == 1, "should be promoted to pinned, got %lu", s2.pin_count);
    CHECK(s2.entry_count == 1, "still 1 entry");

    /* Flood with unpinned to trigger evictions */
    for (int i = 100; i < 300; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        node_cache_put(&nc, hash, rlp, rlp_len, 8);
    }

    /* The promoted node should survive */
    make_hash(hash, 42);
    uint32_t len = node_cache_get(&nc, hash, buf, sizeof(buf));
    CHECK(len > 0, "promoted node should survive eviction");

    node_cache_destroy(&nc);
}

/* =========================================================================
 * Test 5: Throughput benchmark — measure ops/sec
 * ========================================================================= */

static void test_throughput(void) {
    printf("--- test_throughput ---\n");

    /* 64MB cache — ~60K entries */
    uint64_t budget = 64 * 1024 * 1024;
    node_cache_t nc;
    node_cache_init(&nc, budget);

    uint8_t hash[32], rlp[NODE_CACHE_MAX_RLP], buf[NODE_CACHE_MAX_RLP];
    uint16_t rlp_len;

    int N = 200000;

    /* Phase 1: Insert N nodes (mixed depths) */
    double t0 = now_sec();
    for (int i = 0; i < N; i++) {
        make_hash(hash, i);
        make_rlp(rlp, &rlp_len, i);
        uint8_t depth = (i < 1000) ? (uint8_t)(i % 5) : 8;
        node_cache_put(&nc, hash, rlp, rlp_len, depth);
    }
    double insert_sec = now_sec() - t0;

    /* Phase 2: Read N nodes (mix of hits and misses) */
    t0 = now_sec();
    int hits = 0;
    for (int i = 0; i < N; i++) {
        make_hash(hash, i);
        if (node_cache_get(&nc, hash, buf, sizeof(buf)) > 0) hits++;
    }
    double read_sec = now_sec() - t0;

    node_cache_stats_t s = node_cache_stats(&nc);

    printf("  Insert: %d ops in %.3fs = %.0f ops/sec\n",
           N, insert_sec, N / insert_sec);
    printf("  Read:   %d ops in %.3fs = %.0f ops/sec (hits=%d)\n",
           N, read_sec, N / read_sec, hits);
    printf("  Entries: %lu, Pinned: %lu, Evictions: %lu\n",
           s.entry_count, s.pin_count, s.evictions);

    CHECK(s.pin_count == 1000, "expected 1000 pinned (depth 0-4)");
    CHECK(insert_sec < 5.0, "insert should be fast");
    CHECK(read_sec < 5.0, "read should be fast");

    node_cache_destroy(&nc);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Node Cache Tests ===\n\n");

    test_basic_pinning();
    test_unpinned_eviction();
    test_pinned_survive_burst();
    test_promotion();
    test_throughput();

    printf("\n========================================\n");
    printf("  %d / %d tests passed\n", tests_passed, tests_run);
    printf("========================================\n");

    return (tests_passed == tests_run) ? 0 : 1;
}
