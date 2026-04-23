/**
 * hart_invalidate_all_parallel vs hart_invalidate_all cross-verification.
 *
 * Builds two ART trees with identical keys, computes root on each (populates
 * cached hashes so every node is clean), then invalidates one serially and
 * the other in parallel. Uses hart_count_internal_nodes to confirm:
 *   1. Both trees have the same total node count (identical structure).
 *   2. After either invalidate, every internal node is dirty (clean == 0).
 *
 * A parallel-invalidate bug that misses a subtree would show up as
 * clean > 0 on the parallel tree while serial produces clean == 0.
 */

#include "hashed_art.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define CHECK(cond, fmt, ...) do { \
    if (cond) { tests_passed++; printf("  PASS %s\n", #cond); } \
    else      { tests_failed++; printf("  FAIL " fmt "\n", ##__VA_ARGS__); } \
} while (0)

/* Dummy value encoder — we never call hart_root_hash directly here, just
 * the internal compute via invalidate. Provide a no-op to satisfy the
 * signature when we do compute roots. */
static size_t encode_val(const void *v, void *out, void *ctx) {
    (void)v; (void)out; (void)ctx;
    return 0;
}

/* Deterministic key generator — spreads across the full 0..255 hi-nibble
 * space to force a NODE_48 or NODE_256 at the root. */
static void make_key(uint32_t i, uint8_t key[32]) {
    memset(key, 0, 32);
    /* Scatter across the root by varying byte 0; vary deeper bytes too so
     * the tree is actually branching, not a single chain. */
    key[0]  = (uint8_t)(i & 0xFF);
    key[1]  = (uint8_t)((i >> 8) & 0xFF);
    key[31] = (uint8_t)i;
}

static void populate(hart_t *t, uint32_t n_keys) {
    for (uint32_t i = 0; i < n_keys; i++) {
        uint8_t key[32];
        make_key(i, key);
        uint32_t val = i;
        hart_insert(t, key, &val);
    }
}

static void run_cross_check(uint32_t n_keys, const char *label) {
    printf("\n[%s] n_keys=%u\n", label, n_keys);

    hart_t a, b;
    hart_init(&a, sizeof(uint32_t));
    hart_init(&b, sizeof(uint32_t));
    populate(&a, n_keys);
    populate(&b, n_keys);

    /* Structural sanity — both trees should have identical internal node
     * counts since they were built with the same keys. */
    uint32_t clean_a_before = 0, clean_b_before = 0;
    uint32_t total_a = hart_count_internal_nodes(&a, &clean_a_before);
    uint32_t total_b = hart_count_internal_nodes(&b, &clean_b_before);
    printf("  nodes: a=%u b=%u\n", total_a, total_b);
    CHECK(total_a == total_b, "structural mismatch: %u vs %u", total_a, total_b);

    /* Pre-compute roots so every branch-node has a cached hash. Note that
     * extension-node compression means some intermediate nodes may remain
     * dirty after compute (the hash helper skips single-child nodes in the
     * RLP form). That's fine — what we're verifying is the INVALIDATE, not
     * the compute. Any node that IS clean post-compute must become dirty
     * after invalidate; any node that was dirty stays dirty (idempotent). */
    uint8_t root_a[32], root_b[32];
    hart_root_hash(&a, encode_val, NULL, root_a);
    hart_root_hash(&b, encode_val, NULL, root_b);

    uint32_t clean_a_mid = 0, clean_b_mid = 0;
    (void)hart_count_internal_nodes(&a, &clean_a_mid);
    (void)hart_count_internal_nodes(&b, &clean_b_mid);
    printf("  post-hash clean: a=%u/%u b=%u/%u (shape info, not a check)\n",
           clean_a_mid, total_a, clean_b_mid, total_b);
    /* Both trees must have identical post-compute dirty-bit state since
     * inputs and code paths were identical. */
    CHECK(clean_a_mid == clean_b_mid,
          "post-hash divergence: a=%u b=%u", clean_a_mid, clean_b_mid);

    /* Serial invalidate on a, parallel invalidate on b. */
    hart_invalidate_all(&a);
    hart_invalidate_all_parallel(&b);

    uint32_t clean_a_after = 0, clean_b_after = 0;
    (void)hart_count_internal_nodes(&a, &clean_a_after);
    (void)hart_count_internal_nodes(&b, &clean_b_after);
    printf("  post-invalidate clean: serial=%u parallel=%u (expect 0 for both)\n",
           clean_a_after, clean_b_after);
    CHECK(clean_a_after == 0, "serial invalidate left %u clean nodes",
          clean_a_after);
    CHECK(clean_b_after == 0, "parallel invalidate left %u clean nodes",
          clean_b_after);
    CHECK(clean_a_after == clean_b_after,
          "serial/parallel divergence: serial=%u parallel=%u",
          clean_a_after, clean_b_after);

    hart_destroy(&a);
    hart_destroy(&b);
}

int main(void) {
    printf("hart_invalidate_all_parallel cross-verification\n");
    printf("===============================================\n");

    /* Small tree — root is likely NODE_4 / NODE_16, falls through to the
     * serial path inside hart_invalidate_all_parallel. Both paths produce
     * the same outcome via invalidate_recursive. */
    run_cross_check(10,    "small_tree");

    /* Medium tree — forces NODE_48 at the root, exercises the 16-hi-group
     * split and 4-thread fan-out. */
    run_cross_check(500,   "medium_tree");

    /* Large tree — NODE_256 at root, many internal branches. Real stress
     * test for the parallel coverage. */
    run_cross_check(20000, "large_tree");

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed ? 1 : 0;
}
