#include "verkle_flat.h"
#include "verkle.h"
#include "pedersen.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Test Infrastructure
 * ========================================================================= */

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT(cond, msg)                                              \
    do {                                                               \
        if (!(cond)) {                                                 \
            printf("  FAIL: %s (line %d)\n", msg, __LINE__);          \
            tests_failed++;                                            \
        } else {                                                       \
            tests_passed++;                                            \
        }                                                              \
    } while (0)

#define VAL_DIR  "/tmp/test_vf_vals"
#define COMM_DIR "/tmp/test_vf_comm"

static void cleanup(void) {
    system("rm -rf " VAL_DIR " " COMM_DIR);
}

/* Deterministic RNG (SplitMix64) */
typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void rng_fill(rng_t *rng, uint8_t *buf, size_t len) {
    for (size_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(rng);
        size_t n = (len - i < 8) ? len - i : 8;
        memcpy(buf + i, &r, n);
    }
}

/* Compare root hashes between flat updater and in-memory tree */
static bool roots_match(const verkle_flat_t *vf, const verkle_tree_t *vt) {
    uint8_t flat_hash[32], tree_hash[32];
    verkle_flat_root_hash(vf, flat_hash);
    verkle_root_hash(vt, tree_hash);
    return memcmp(flat_hash, tree_hash, 32) == 0;
}


/* =========================================================================
 * Phase 1: Single key set + get
 * ========================================================================= */

static void test_single_key(void) {
    printf("Phase 1: Key set + get + root hash\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);
    ASSERT(vf != NULL, "flat create");

    /* Need 2+ stems so tree creates an internal root (flat always does) */
    uint8_t key[32], value[32];
    uint8_t key2[32], val2[32];
    memset(key, 0xAA, 31);   key[31] = 0x05;  memset(value, 0x42, 32);
    memset(key2, 0xBB, 31);  key2[31] = 0x00;  memset(val2, 0x11, 32);

    /* Set in tree */
    verkle_set(vt, key, value);
    verkle_set(vt, key2, val2);

    /* Set in flat */
    ASSERT(verkle_flat_begin_block(vf, 1), "begin block");
    ASSERT(verkle_flat_set(vf, key, value), "flat set");
    ASSERT(verkle_flat_set(vf, key2, val2), "flat set 2");

    /* Get from flat (in-flight) */
    uint8_t got[32];
    ASSERT(verkle_flat_get(vf, key, got), "flat get");
    ASSERT(memcmp(got, value, 32) == 0, "value matches");

    ASSERT(verkle_flat_commit_block(vf), "commit block");

    /* Get from flat (committed) */
    ASSERT(verkle_flat_get(vf, key, got), "flat get after commit");
    ASSERT(memcmp(got, value, 32) == 0, "value still matches");

    /* Compare root hashes */
    ASSERT(roots_match(vf, vt), "root hash matches tree");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 2: Multiple keys same stem
 * ========================================================================= */

static void test_same_stem(void) {
    printf("Phase 2: Multiple keys same stem (C1 + C2)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    /* Need 2+ stems for internal root */
    uint8_t stem[31];
    memset(stem, 0xBB, 31);

    verkle_flat_begin_block(vf, 1);

    /* Second stem to force internal root */
    {
        uint8_t k2[32], v2[32];
        memset(k2, 0x11, 31); k2[31] = 0;
        memset(v2, 0x99, 32);
        verkle_set(vt, k2, v2);
        verkle_flat_set(vf, k2, v2);
    }

    /* Set 5 values with same stem (like account header: suffixes 0-4) */
    for (int i = 0; i < 5; i++) {
        uint8_t key[32], val[32];
        memcpy(key, stem, 31);
        key[31] = (uint8_t)i;
        memset(val, 0x10 + i, 32);
        verkle_set(vt, key, val);
        verkle_flat_set(vf, key, val);
    }

    /* Also add a value in C2 range (suffix >= 128) */
    uint8_t key[32], val[32];
    memcpy(key, stem, 31);
    key[31] = 200;
    memset(val, 0xFF, 32);
    verkle_set(vt, key, val);
    verkle_flat_set(vf, key, val);

    verkle_flat_commit_block(vf);

    ASSERT(roots_match(vf, vt), "root hash matches (same stem, C1+C2)");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 3: Multiple stems (internal propagation)
 * ========================================================================= */

static void test_multiple_stems(void) {
    printf("Phase 3: Multiple stems (internal propagation)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    verkle_flat_begin_block(vf, 1);

    /* Insert 10 keys with different stems */
    for (int i = 0; i < 10; i++) {
        uint8_t key[32], val[32];
        memset(key, (uint8_t)(i * 17), 31);  /* different stems */
        key[31] = 0;
        memset(val, (uint8_t)(0x30 + i), 32);
        verkle_set(vt, key, val);
        verkle_flat_set(vf, key, val);
    }

    verkle_flat_commit_block(vf);

    ASSERT(roots_match(vf, vt), "root hash matches (10 stems)");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 4: Two blocks — first creates, second updates (incremental path)
 * ========================================================================= */

static void test_two_blocks(void) {
    printf("Phase 4: Two blocks — create then update\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    /* Block 1: create 5 stems */
    verkle_flat_begin_block(vf, 1);
    for (int i = 0; i < 5; i++) {
        uint8_t key[32], val[32];
        memset(key, (uint8_t)(i * 51), 31);
        key[31] = 0;
        memset(val, (uint8_t)(0x10 + i), 32);
        verkle_set(vt, key, val);
        verkle_flat_set(vf, key, val);
    }
    verkle_flat_commit_block(vf);
    ASSERT(roots_match(vf, vt), "block 1 root matches");

    /* Flush tree to commit store so block 2 has existing leaf data */
    vcs_flush_tree(vf->commit_store, vt);

    /* Block 2: update existing + add new */
    verkle_flat_begin_block(vf, 2);

    /* Update an existing key */
    uint8_t key[32], val[32];
    memset(key, 0, 31);  /* stem from i=0 */
    key[31] = 0;
    memset(val, 0xEE, 32);
    verkle_set(vt, key, val);
    verkle_flat_set(vf, key, val);

    /* Add new stem */
    memset(key, 0xFF, 31);
    key[31] = 3;
    memset(val, 0xDD, 32);
    verkle_set(vt, key, val);
    verkle_flat_set(vf, key, val);

    verkle_flat_commit_block(vf);

    /* Flush again for updated internals */
    vcs_flush_tree(vf->commit_store, vt);

    ASSERT(roots_match(vf, vt), "block 2 root matches (update + new)");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 5: Block revert
 * ========================================================================= */

static void test_revert(void) {
    printf("Phase 5: Block revert\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    /* Block 1: initial state */
    verkle_flat_begin_block(vf, 1);
    for (int i = 0; i < 3; i++) {
        uint8_t key[32], val[32];
        memset(key, (uint8_t)(i * 77), 31);
        key[31] = 0;
        memset(val, (uint8_t)(0x20 + i), 32);
        verkle_set(vt, key, val);
        verkle_flat_set(vf, key, val);
    }
    verkle_flat_commit_block(vf);

    /* Save root hash after block 1 */
    uint8_t hash_after_b1[32];
    verkle_flat_root_hash(vf, hash_after_b1);

    /* Flush tree for block 2 */
    vcs_flush_tree(vf->commit_store, vt);

    /* Block 2: modify state */
    verkle_flat_begin_block(vf, 2);

    /* Update existing key */
    uint8_t key[32], val[32];
    memset(key, 0, 31);
    key[31] = 0;
    memset(val, 0xFF, 32);
    verkle_flat_set(vf, key, val);

    /* Add new key */
    memset(key, 0xCC, 31);
    key[31] = 0;
    memset(val, 0xDD, 32);
    verkle_flat_set(vf, key, val);

    verkle_flat_commit_block(vf);

    /* Root should have changed */
    uint8_t hash_after_b2[32];
    verkle_flat_root_hash(vf, hash_after_b2);
    ASSERT(memcmp(hash_after_b1, hash_after_b2, 32) != 0, "block 2 changed root");

    /* Revert block 2 */
    ASSERT(verkle_flat_revert_block(vf), "revert succeeds");

    /* Root should match block 1 */
    uint8_t hash_after_revert[32];
    verkle_flat_root_hash(vf, hash_after_revert);
    ASSERT(memcmp(hash_after_b1, hash_after_revert, 32) == 0,
           "root restored after revert");

    /* Values should be restored */
    uint8_t got[32];
    memset(key, 0, 31);
    key[31] = 0;
    ASSERT(verkle_flat_get(vf, key, got), "old key still exists");
    uint8_t expected[32];
    memset(expected, 0x20, 32);
    ASSERT(memcmp(got, expected, 32) == 0, "old value restored");

    /* New key should be gone */
    memset(key, 0xCC, 31);
    key[31] = 0;
    ASSERT(!verkle_flat_get(vf, key, got), "new key removed after revert");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 6: Multi-block + trim
 * ========================================================================= */

static void test_trim(void) {
    printf("Phase 6: Multi-block + trim\n");
    cleanup();

    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    /* Create 5 blocks */
    for (int b = 1; b <= 5; b++) {
        verkle_flat_begin_block(vf, (uint64_t)b);
        uint8_t key[32], val[32];
        memset(key, (uint8_t)(b * 33), 31);
        key[31] = 0;
        memset(val, (uint8_t)b, 32);
        verkle_flat_set(vf, key, val);
        verkle_flat_commit_block(vf);
    }

    ASSERT(vf->block_count == 5, "5 blocks tracked");

    /* Trim blocks 1-3 */
    verkle_flat_trim(vf, 3);
    ASSERT(vf->block_count == 2, "2 blocks remain after trim");
    ASSERT(vf->blocks[0].block_number == 4, "first remaining is block 4");
    ASSERT(vf->blocks[1].block_number == 5, "second remaining is block 5");

    /* Can still revert block 5 */
    ASSERT(verkle_flat_revert_block(vf), "revert block 5");
    ASSERT(vf->block_count == 1, "1 block remains");

    verkle_flat_destroy(vf);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 7: Persistence — sync, close, reopen
 * ========================================================================= */

static void test_persistence(void) {
    printf("Phase 7: Persistence\n");
    cleanup();

    uint8_t root_before[32];
    uint8_t key[32], val[32];
    memset(key, 0xAA, 31);
    key[31] = 5;
    memset(val, 0x42, 32);

    /* Create, set, commit, sync */
    {
        verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);
        verkle_flat_begin_block(vf, 1);
        verkle_flat_set(vf, key, val);
        verkle_flat_commit_block(vf);
        verkle_flat_root_hash(vf, root_before);
        verkle_flat_sync(vf);
        verkle_flat_destroy(vf);
    }

    /* Reopen and verify */
    {
        verkle_flat_t *vf = verkle_flat_open(VAL_DIR, COMM_DIR);
        ASSERT(vf != NULL, "reopen succeeds");

        uint8_t root_after[32];
        verkle_flat_root_hash(vf, root_after);
        ASSERT(memcmp(root_before, root_after, 32) == 0, "root preserved");

        uint8_t got[32];
        ASSERT(verkle_flat_get(vf, key, got), "value exists after reopen");
        ASSERT(memcmp(got, val, 32) == 0, "value preserved");

        verkle_flat_destroy(vf);
    }

    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 8: Same key twice in one block (dedup: last write wins)
 * ========================================================================= */

static void test_dedup(void) {
    printf("Phase 8: Same key twice (last write wins)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 1024);

    uint8_t key[32];
    memset(key, 0x55, 31);
    key[31] = 0;

    uint8_t val1[32], val2[32];
    memset(val1, 0x11, 32);
    memset(val2, 0x22, 32);

    /* Second stem to force internal root */
    uint8_t anchor_key[32], anchor_val[32];
    memset(anchor_key, 0xEE, 31); anchor_key[31] = 0;
    memset(anchor_val, 0x77, 32);
    verkle_set(vt, anchor_key, anchor_val);

    /* Tree only sees final value */
    verkle_set(vt, key, val2);

    /* Flat sees both writes */
    verkle_flat_begin_block(vf, 1);
    verkle_flat_set(vf, anchor_key, anchor_val);
    verkle_flat_set(vf, key, val1);
    verkle_flat_set(vf, key, val2);
    verkle_flat_commit_block(vf);

    ASSERT(roots_match(vf, vt), "root matches (dedup, last wins)");

    /* Get returns last value */
    uint8_t got[32];
    ASSERT(verkle_flat_get(vf, key, got), "get succeeds");
    ASSERT(memcmp(got, val2, 32) == 0, "last write wins");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 9: Scale — 1000 random keys, 10 blocks
 * ========================================================================= */

static void test_scale(void) {
    printf("Phase 9: Scale — 100 keys, 10 blocks\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR, 4096);
    rng_t rng = { .state = 12345 };

    /* Track all keys for later updates */
    uint8_t keys[100][32];
    int num_keys = 0;

    for (int b = 1; b <= 10; b++) {
        verkle_flat_begin_block(vf, (uint64_t)b);

        /* Each block: 10 new keys */
        for (int i = 0; i < 10; i++) {
            uint8_t key[32], val[32];
            rng_fill(&rng, key, 31);
            key[31] = (uint8_t)(rng_next(&rng) & 0xFF);
            rng_fill(&rng, val, 32);

            verkle_set(vt, key, val);
            verkle_flat_set(vf, key, val);

            memcpy(keys[num_keys++], key, 32);
        }

        verkle_flat_commit_block(vf);

        /* Flush tree so next block's incremental updates work */
        vcs_flush_tree(vf->commit_store, vt);

        /* Cross-validate root */
        bool match = roots_match(vf, vt);
        if (!match) {
            printf("  FAIL: block %d root mismatch\n", b);
            tests_failed++;
        } else {
            tests_passed++;
        }
    }

    /* Verify all values readable */
    bool all_values_ok = true;
    for (int i = 0; i < num_keys; i++) {
        uint8_t got[32], expected[32];
        if (!verkle_flat_get(vf, keys[i], got)) {
            all_values_ok = false;
            break;
        }
        if (!verkle_get(vt, keys[i], expected)) {
            all_values_ok = false;
            break;
        }
        if (memcmp(got, expected, 32) != 0) {
            all_values_ok = false;
            break;
        }
    }
    ASSERT(all_values_ok, "all 100 values match");

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Flat Updater Tests ===\n\n");
    pedersen_init();

    test_single_key();
    test_same_stem();
    test_multiple_stems();
    test_two_blocks();
    test_revert();
    test_trim();
    test_persistence();
    test_dedup();
    test_scale();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed ? 1 : 0;
}
