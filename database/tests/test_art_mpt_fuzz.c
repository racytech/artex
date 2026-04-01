/*
 * ART→MPT Randomized Fuzz Test
 *
 * Generates random sequences of insert/delete/update operations on compact_art,
 * computes MPT root hash incrementally (with cache) vs full recompute (no cache),
 * and verifies they always match.
 *
 * This catches hash cache invalidation bugs — if a dirty flag isn't propagated
 * correctly after an operation, the incremental hash will use a stale cached
 * value and diverge from the full recompute.
 *
 * Operations tested:
 *   - Random inserts (new keys)
 *   - Random updates (existing keys with new values)
 *   - Random deletes
 *   - Mixed insert/update/delete batches
 *   - Varying tree sizes (1 to 10K+ entries)
 *   - Varying value sizes (1 to 128 bytes)
 */

#include "art_mpt.h"
#include "compact_art.h"
#include "arena.h"

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
 * Simple PRNG (xorshift64)
 * ========================================================================= */

static uint64_t rng_state;

static void rng_seed(uint64_t seed) { rng_state = seed ? seed : 1; }

static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static uint32_t rng_range(uint32_t max) {
    return (uint32_t)(rng_next() % max);
}

static void rng_bytes(uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++)
        out[i] = (uint8_t)(rng_next() & 0xff);
}

/* =========================================================================
 * Value encoder — stores (len:2 || data) in compact_art leaf,
 * returns data as RLP (raw bytes, like the vector test).
 * ========================================================================= */

typedef struct {
    uint16_t len;
    uint8_t  data[];
} leaf_val_t;

static uint32_t fuzz_encode(const uint8_t *key, const void *leaf_val,
                             uint32_t val_size, uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    if (!leaf_val || val_size < sizeof(leaf_val_t)) return 0;
    const leaf_val_t *lv = leaf_val;
    if (lv->len == 0) return 0;
    memcpy(rlp_out, lv->data, lv->len);
    return lv->len;
}

/* =========================================================================
 * Key/value management
 * ========================================================================= */

#define MAX_KEYS 16384

typedef struct {
    uint8_t key[32];
    bool    active;
} key_entry_t;

static key_entry_t keys[MAX_KEYS];
static int num_keys = 0;
static int active_count = 0;

static void reset_keys(void) {
    num_keys = 0;
    active_count = 0;
    memset(keys, 0, sizeof(keys));
}

static int add_key(void) {
    if (num_keys >= MAX_KEYS) return -1;
    /* Generate random 32-byte key directly — random bytes already give
     * uniform distribution across the ART key space */
    rng_bytes(keys[num_keys].key, 32);
    keys[num_keys].active = true;
    active_count++;
    return num_keys++;
}

static int pick_active(void) {
    if (active_count == 0) return -1;
    int target = rng_range(active_count);
    int count = 0;
    for (int i = 0; i < num_keys; i++) {
        if (keys[i].active) {
            if (count == target) return i;
            count++;
        }
    }
    return -1;
}

static leaf_val_t *make_value(uint16_t len) {
    leaf_val_t *v = malloc(sizeof(leaf_val_t) + len);
    v->len = len;
    rng_bytes(v->data, len);
    return v;
}

/* =========================================================================
 * Hash comparison helpers
 * ========================================================================= */

static void print_hash(const char *label, const uint8_t h[32]) {
    fprintf(stderr, "  %s: ", label);
    for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", h[i]);
    fprintf(stderr, "\n");
}

static bool hashes_match(const uint8_t a[32], const uint8_t b[32]) {
    return memcmp(a, b, 32) == 0;
}

/* Compute incremental root, then full recompute, compare */
static bool verify_roots(compact_art_t *tree, art_mpt_t *am,
                          int round, const char *op) {
    uint8_t inc[32], full[32];

    /* Incremental (uses cache) */
    art_mpt_root_hash(am, inc);

    /* Full recompute (no cache, doesn't modify dirty flags) */
    art_mpt_root_hash_full(tree, fuzz_encode, NULL, full);

    if (!hashes_match(inc, full)) {
        fprintf(stderr, "MISMATCH at round %d after %s (size=%zu):\n",
                round, op, compact_art_size(tree));
        print_hash("incremental", inc);
        print_hash("full       ", full);
        return false;
    }
    return true;
}

/* =========================================================================
 * Test 1: Random inserts — build tree from empty, verify after each insert
 * ========================================================================= */

static void test_random_inserts(void) {
    printf("test_random_inserts:\n");
    rng_seed(12345);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    CHECK(tree, "create tree");
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);
    CHECK(am, "create mpt");

    int N = 2000;
    for (int i = 0; i < N; i++) {
        int ki = add_key();
        CHECK(ki >= 0, "add key");

        uint16_t vlen = 1 + rng_range(64);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);

        /* Verify every 50 inserts (and first 10) */
        if (i < 10 || i % 50 == 0) {
            if (!verify_roots(tree, am, i, "insert")) {
                tests_failed++;
                art_mpt_destroy(am);
                compact_art_destroy(tree);
                return; /* early exit on failure */
            }
        }
    }

    CHECK(verify_roots(tree, am, N, "final"), "final insert verify");
    PASS("random inserts (2000 keys)");

    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 2: Insert then update — modify existing values
 * ========================================================================= */

static void test_insert_then_update(void) {
    printf("test_insert_then_update:\n");
    rng_seed(67890);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Insert 500 keys */
    for (int i = 0; i < 500; i++) {
        int ki = add_key();
        uint16_t vlen = 1 + rng_range(32);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    CHECK(verify_roots(tree, am, 0, "initial"), "initial state");

    /* Update 1000 random existing keys */
    for (int i = 0; i < 1000; i++) {
        int ki = pick_active();
        CHECK(ki >= 0, "pick active");

        uint16_t vlen = 1 + rng_range(64);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);

        if (i < 10 || i % 100 == 0) {
            if (!verify_roots(tree, am, i, "update")) {
                tests_failed++;
                art_mpt_destroy(am);
                compact_art_destroy(tree);
                return; /* early exit on failure */
            }
        }
    }

    CHECK(verify_roots(tree, am, 1000, "final"), "final update verify");
    PASS("insert then update (500 keys, 1000 updates)");

    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 3: Insert then delete — shrink tree
 * ========================================================================= */

static void test_insert_then_delete(void) {
    printf("test_insert_then_delete:\n");
    rng_seed(11111);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Insert 1000 keys */
    for (int i = 0; i < 1000; i++) {
        int ki = add_key();
        uint16_t vlen = 1 + rng_range(32);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    CHECK(verify_roots(tree, am, 0, "initial"), "initial state");

    /* Delete 800 random keys */
    for (int i = 0; i < 800; i++) {
        int ki = pick_active();
        if (ki < 0) break;

        compact_art_delete(tree, keys[ki].key);
        keys[ki].active = false;
        active_count--;

        if (i < 10 || i % 50 == 0) {
            if (!verify_roots(tree, am, i, "delete")) {
                tests_failed++;
                art_mpt_destroy(am);
                compact_art_destroy(tree);
                return; /* early exit on failure */
            }
        }
    }

    CHECK(verify_roots(tree, am, 800, "final"), "final delete verify");
    PASS("insert then delete (1000 keys, delete 800)");

    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 4: Mixed operations — random insert/update/delete interleaved
 * ========================================================================= */

static void test_mixed_operations(void) {
    printf("test_mixed_operations:\n");
    rng_seed(99999);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    int rounds = 5000;
    for (int i = 0; i < rounds; i++) {
        uint32_t op = rng_range(100);

        if (op < 50 || active_count < 5) {
            /* INSERT new key (50% chance, or forced if tree is small) */
            int ki = add_key();
            if (ki >= 0) {
                uint16_t vlen = 1 + rng_range(64);
                leaf_val_t *v = make_value(vlen);
                compact_art_insert(tree, keys[ki].key, v);
                free(v);
            }
        } else if (op < 80) {
            /* UPDATE existing key (30% chance) */
            int ki = pick_active();
            if (ki >= 0) {
                uint16_t vlen = 1 + rng_range(64);
                leaf_val_t *v = make_value(vlen);
                compact_art_insert(tree, keys[ki].key, v);
                free(v);
            }
        } else {
            /* DELETE existing key (20% chance) */
            int ki = pick_active();
            if (ki >= 0) {
                compact_art_delete(tree, keys[ki].key);
                keys[ki].active = false;
                active_count--;
            }
        }

        /* Verify periodically */
        if (i < 20 || i % 100 == 0) {
            if (!verify_roots(tree, am, i, "mixed")) {
                fprintf(stderr, "  tree size=%zu, active=%d, round=%d\n",
                        compact_art_size(tree), active_count, i);
                tests_failed++;
                art_mpt_destroy(am);
                compact_art_destroy(tree);
                return; /* early exit on failure */
            }
        }
    }

    CHECK(verify_roots(tree, am, rounds, "final"), "final mixed verify");
    PASS("mixed operations (5000 rounds)");

    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 5: Batch + verify — simulate block execution pattern
 *
 * N rounds of: do batch of ops → compute root → next batch
 * This tests that root computation properly clears dirty flags
 * so the NEXT batch's incremental computation starts clean.
 * ========================================================================= */

static void test_batch_rounds(void) {
    printf("test_batch_rounds:\n");
    rng_seed(55555);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Initial population */
    for (int i = 0; i < 500; i++) {
        int ki = add_key();
        uint16_t vlen = 1 + rng_range(32);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    art_mpt_root_hash(am, (uint8_t[32]){0});

    /* 200 rounds: each round = 5-20 ops + root verify */
    for (int round = 0; round < 200; round++) {
        int batch_size = 5 + rng_range(16);
        for (int j = 0; j < batch_size; j++) {
            uint32_t op = rng_range(100);
            if (op < 40 || active_count < 10) {
                int ki = add_key();
                if (ki >= 0) {
                    uint16_t vlen = 1 + rng_range(32);
                    leaf_val_t *v = make_value(vlen);
                    compact_art_insert(tree, keys[ki].key, v);
                    free(v);
                }
            } else if (op < 75) {
                int ki = pick_active();
                if (ki >= 0) {
                    uint16_t vlen = 1 + rng_range(32);
                    leaf_val_t *v = make_value(vlen);
                    compact_art_insert(tree, keys[ki].key, v);
                    free(v);
                }
            } else {
                int ki = pick_active();
                if (ki >= 0) {
                    compact_art_delete(tree, keys[ki].key);
                    keys[ki].active = false;
                    active_count--;
                }
            }
        }

        if (!verify_roots(tree, am, round, "batch")) {
            fprintf(stderr, "  batch round=%d, batch_size=%d, tree size=%zu\n",
                    round, batch_size, compact_art_size(tree));
            tests_failed++;
            art_mpt_destroy(am);
            compact_art_destroy(tree);
            return;
        }
    }

    PASS("batch rounds (200 rounds, ~2000 ops)");
    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 6: Leaf value rewrite with same key — update in-place
 *
 * Tests that updating a leaf's value marks the path dirty even when
 * the key already exists (compact_art_insert overwrites).
 * ========================================================================= */

static void test_value_rewrite(void) {
    printf("test_value_rewrite:\n");
    rng_seed(77777);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Insert 100 keys */
    for (int i = 0; i < 100; i++) {
        int ki = add_key();
        leaf_val_t *v = make_value(16);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    uint8_t root1[32];
    art_mpt_root_hash(am, root1);

    /* Rewrite same key with different value — must change root */
    int ki = pick_active();
    CHECK(ki >= 0, "pick key");
    leaf_val_t *v = make_value(16);
    compact_art_insert(tree, keys[ki].key, v);
    free(v);

    uint8_t root2[32], full2[32];
    art_mpt_root_hash(am, root2);
    art_mpt_root_hash_full(tree, fuzz_encode, NULL, full2);

    CHECK(!hashes_match(root1, root2), "root changed after value rewrite");
    if (!hashes_match(root2, full2)) {
        print_hash("incremental", root2);
        print_hash("full       ", full2);
    }
    CHECK(hashes_match(root2, full2), "incremental == full after rewrite");

    PASS("value rewrite same key");
    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 7: Delete and re-insert same key
 *
 * Delete a key, compute root, then re-insert with different value.
 * Tests dirty propagation through delete→insert sequence.
 * ========================================================================= */

static void test_delete_reinsert(void) {
    printf("test_delete_reinsert:\n");
    rng_seed(33333);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Insert 200 keys */
    for (int i = 0; i < 200; i++) {
        int ki = add_key();
        leaf_val_t *v = make_value(16);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    uint8_t root_full[32];
    art_mpt_root_hash(am, root_full);

    /* Delete 50 random keys, verify, re-insert each with new value, verify */
    for (int i = 0; i < 50; i++) {
        int ki = pick_active();
        if (ki < 0) break;

        uint8_t saved_key[32];
        memcpy(saved_key, keys[ki].key, 32);

        /* Delete */
        compact_art_delete(tree, saved_key);
        keys[ki].active = false;
        active_count--;

        if (!verify_roots(tree, am, i, "delete")) {
            tests_failed++;
            art_mpt_destroy(am);
            compact_art_destroy(tree);
            return;
        }

        /* Re-insert with different value */
        leaf_val_t *v = make_value(24);
        compact_art_insert(tree, saved_key, v);
        free(v);
        keys[ki].active = true;
        active_count++;

        if (!verify_roots(tree, am, i, "reinsert")) {
            tests_failed++;
            art_mpt_destroy(am);
            compact_art_destroy(tree);
            return;
        }
    }

    PASS("delete and re-insert (200 keys, 50 delete+reinsert cycles)");
    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 8: Large-scale stress — 10K keys, many rounds
 * ========================================================================= */

static void test_large_scale(void) {
    printf("test_large_scale:\n");
    rng_seed(42);
    reset_keys();

    compact_art_t tree_storage;
    compact_art_t *tree = &tree_storage;
    compact_art_init(tree, 32, sizeof(leaf_val_t) + 128, false, NULL, NULL);
    art_mpt_t *am = art_mpt_create(tree, fuzz_encode, NULL);

    /* Build to 10K entries */
    for (int i = 0; i < 10000; i++) {
        int ki = add_key();
        if (ki < 0) break;
        uint16_t vlen = 1 + rng_range(64);
        leaf_val_t *v = make_value(vlen);
        compact_art_insert(tree, keys[ki].key, v);
        free(v);
    }
    CHECK(verify_roots(tree, am, 0, "initial 10K"), "initial 10K");

    /* 500 rounds of batched modifications */
    for (int round = 0; round < 500; round++) {
        int batch = 3 + rng_range(10);
        for (int j = 0; j < batch; j++) {
            uint32_t op = rng_range(100);
            if (op < 30 && num_keys < MAX_KEYS) {
                int ki = add_key();
                if (ki >= 0) {
                    leaf_val_t *v = make_value(1 + rng_range(64));
                    compact_art_insert(tree, keys[ki].key, v);
                    free(v);
                }
            } else if (op < 70) {
                int ki = pick_active();
                if (ki >= 0) {
                    leaf_val_t *v = make_value(1 + rng_range(64));
                    compact_art_insert(tree, keys[ki].key, v);
                    free(v);
                }
            } else {
                int ki = pick_active();
                if (ki >= 0) {
                    compact_art_delete(tree, keys[ki].key);
                    keys[ki].active = false;
                    active_count--;
                }
            }
        }

        if (!verify_roots(tree, am, round, "large batch")) {
            fprintf(stderr, "  round=%d tree_size=%zu active=%d\n",
                    round, compact_art_size(tree), active_count);
            tests_failed++;
            art_mpt_destroy(am);
            compact_art_destroy(tree);
            return;
        }
    }

    PASS("large-scale (10K+ keys, 500 batch rounds)");
    art_mpt_destroy(am);
    compact_art_destroy(tree); /* stack-allocated, just frees pools */
}

/* =========================================================================
 * Test 9: Small-pool mode — per-account sized compact_art
 *
 * Tests compact_art_init_ex with small reserves (4MB/8MB) to verify
 * it works correctly for per-account storage use case.
 * ========================================================================= */

#define SMALL_NODE_RESERVE  (4ULL * 1024 * 1024)   /* 4 MB */
#define SMALL_LEAF_RESERVE  (8ULL * 1024 * 1024)   /* 8 MB */

static void test_small_pool(void) {
    printf("test_small_pool:\n");
    rng_seed(44444);
    reset_keys();

    /* Create many small trees (simulates many accounts with storage) */
    #define NUM_SMALL_TREES 100
    compact_art_t trees[NUM_SMALL_TREES];
    art_mpt_t *mpts[NUM_SMALL_TREES];

    for (int t = 0; t < NUM_SMALL_TREES; t++) {
        bool ok = compact_art_init_ex(&trees[t], 32, sizeof(leaf_val_t) + 64,
                                       false, NULL, NULL,
                                       SMALL_NODE_RESERVE, SMALL_LEAF_RESERVE);
        CHECK(ok, "init small tree %d", t);
        mpts[t] = art_mpt_create(&trees[t], fuzz_encode, NULL);
        CHECK(mpts[t], "create mpt %d", t);
    }

    /* Insert varying amounts of data per tree (1-200 keys) */
    for (int t = 0; t < NUM_SMALL_TREES; t++) {
        int n = 1 + rng_range(200);
        for (int i = 0; i < n; i++) {
            uint8_t key[32];
            rng_bytes(key, 32);
            leaf_val_t *v = make_value(1 + rng_range(32));
            compact_art_insert(&trees[t], key, v);
            free(v);
        }

        /* Verify each tree */
        if (!verify_roots(&trees[t], mpts[t], t, "small tree insert")) {
            tests_failed++;
            for (int j = 0; j < NUM_SMALL_TREES; j++) {
                art_mpt_destroy(mpts[j]);
                compact_art_destroy(&trees[j]);
            }
            return;
        }
    }

    /* Update some entries in each tree */
    for (int t = 0; t < NUM_SMALL_TREES; t++) {
        /* Can't easily pick existing keys without tracking them per-tree,
         * so just insert more (some will be new, some overwrites) */
        for (int i = 0; i < 20; i++) {
            uint8_t key[32];
            rng_bytes(key, 32);
            leaf_val_t *v = make_value(1 + rng_range(32));
            compact_art_insert(&trees[t], key, v);
            free(v);
        }
        if (!verify_roots(&trees[t], mpts[t], t, "small tree update")) {
            tests_failed++;
            for (int j = 0; j < NUM_SMALL_TREES; j++) {
                art_mpt_destroy(mpts[j]);
                compact_art_destroy(&trees[j]);
            }
            return;
        }
    }

    /* Clean up all trees */
    for (int t = 0; t < NUM_SMALL_TREES; t++) {
        art_mpt_destroy(mpts[t]);
        compact_art_destroy(&trees[t]);
    }

    PASS("small-pool mode (100 trees, 1-200 keys each)");
}

/* =========================================================================
 * Test 10: Arena-backed compact_art — per-account storage simulation
 *
 * Creates compact_arts backed by a shared arena, verifies MPT hashes,
 * then resets the arena and repeats (simulating checkpoint windows).
 * ========================================================================= */

static void test_arena_backed(void) {
    printf("test_arena_backed:\n");
    rng_seed(88888);

    arena_t arena;
    CHECK(arena_init(&arena, 64 * 1024 * 1024), "init arena 64MB");

    #define NODE_SLICE (256 * 1024)  /* 256 KB per account nodes */
    #define LEAF_SLICE (512 * 1024)  /* 512 KB per account leaves */
    #define NUM_ACCTS  50

    /* Simulate 10 checkpoint windows */
    for (int window = 0; window < 10; window++) {
        compact_art_t trees[NUM_ACCTS];
        art_mpt_t *mpts[NUM_ACCTS];

        /* Create per-account arts from arena */
        for (int a = 0; a < NUM_ACCTS; a++) {
            void *nmem = arena_alloc(&arena, NODE_SLICE);
            void *lmem = arena_alloc(&arena, LEAF_SLICE);
            CHECK(nmem && lmem, "arena alloc window=%d acct=%d", window, a);

            bool ok = compact_art_init_arena(&trees[a], 32, sizeof(leaf_val_t) + 64,
                                              false, NULL, NULL,
                                              nmem, NODE_SLICE, lmem, LEAF_SLICE);
            CHECK(ok, "init arena tree");
            mpts[a] = art_mpt_create(&trees[a], fuzz_encode, NULL);
            CHECK(mpts[a], "create mpt");
        }

        /* Insert random data per account (10-100 slots each) */
        for (int a = 0; a < NUM_ACCTS; a++) {
            int n = 10 + rng_range(91);
            for (int i = 0; i < n; i++) {
                uint8_t key[32];
                rng_bytes(key, 32);
                leaf_val_t *v = make_value(1 + rng_range(32));
                compact_art_insert(&trees[a], key, v);
                free(v);
            }

            if (!verify_roots(&trees[a], mpts[a], window * 100 + a, "arena tree")) {
                tests_failed++;
                for (int j = 0; j < NUM_ACCTS; j++) {
                    art_mpt_destroy(mpts[j]);
                    compact_art_destroy(&trees[j]);
                }
                arena_destroy(&arena);
                return;
            }
        }

        /* Destroy arts and reset arena (simulate eviction) */
        for (int a = 0; a < NUM_ACCTS; a++) {
            art_mpt_destroy(mpts[a]);
            compact_art_destroy(&trees[a]); /* doesn't munmap — arena owned */
        }
        arena_reset(&arena);
    }

    arena_destroy(&arena);
    PASS("arena-backed (50 accounts × 10 windows)");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    uint64_t seed = 0;
    if (argc > 1) seed = (uint64_t)atoll(argv[1]);
    if (seed == 0) seed = (uint64_t)time(NULL);
    printf("=== ART→MPT Fuzz Test (seed=%lu) ===\n\n", seed);

    /* Override the per-test seeds with the global seed for truly random runs */
    rng_seed(seed);

    test_random_inserts();
    test_insert_then_update();
    test_insert_then_delete();
    test_mixed_operations();
    test_batch_rounds();
    test_value_rewrite();
    test_delete_reinsert();
    test_large_scale();
    test_small_pool();
    test_arena_backed();

    printf("\n=== Results: %d passed, %d failed (seed=%lu) ===\n",
           tests_passed, tests_failed, seed);
    return tests_failed > 0 ? 1 : 0;
}
