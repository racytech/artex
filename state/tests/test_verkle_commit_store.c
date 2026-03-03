#include "verkle_commit_store.h"
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

#define TEST_DIR "/tmp/test_vcs"

static void cleanup(void) {
    system("rm -rf " TEST_DIR);
}

/* =========================================================================
 * Phase 1: Create/Destroy
 * ========================================================================= */

static void test_lifecycle(void) {
    printf("Phase 1: Create/destroy\n");
    cleanup();

    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "vcs_create returns non-NULL");

    vcs_destroy(cs);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 2: Leaf Commitment Round-Trip
 * ========================================================================= */

static void test_leaf_round_trip(void) {
    printf("Phase 2: Leaf commitment round-trip\n");
    cleanup();

    /* Build a tree with a value to get real commitment points */
    verkle_tree_t *vt = verkle_create();
    uint8_t key[32];
    memset(key, 0xAA, 31);
    key[31] = 0x05;
    uint8_t val[32];
    memset(val, 0x42, 32);
    verkle_set(vt, key, val);

    /* Access the leaf's commitments (root is a leaf since single insert) */
    ASSERT(vt->root != NULL, "root exists");
    ASSERT(vt->root->type == VERKLE_LEAF, "root is a leaf");

    banderwagon_point_t c1_orig = vt->root->leaf.c1;
    banderwagon_point_t c2_orig = vt->root->leaf.c2;
    banderwagon_point_t commit_orig = vt->root->leaf.commitment;

    /* Store and retrieve */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "store created");

    uint8_t stem[31];
    memcpy(stem, vt->root->leaf.stem, 31);

    bool ok = vcs_put_leaf(cs, stem, &c1_orig, &c2_orig, &commit_orig);
    ASSERT(ok, "put_leaf succeeds");

    banderwagon_point_t c1_got, c2_got, commit_got;
    ok = vcs_get_leaf(cs, stem, &c1_got, &c2_got, &commit_got);
    ASSERT(ok, "get_leaf succeeds");

    ASSERT(banderwagon_eq(&c1_orig, &c1_got), "C1 round-trips");
    ASSERT(banderwagon_eq(&c2_orig, &c2_got), "C2 round-trips");
    ASSERT(banderwagon_eq(&commit_orig, &commit_got), "commitment round-trips");

    vcs_destroy(cs);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 3: Internal Commitment Round-Trip
 * ========================================================================= */

static void test_internal_round_trip(void) {
    printf("Phase 3: Internal commitment round-trip\n");
    cleanup();

    /* Build a tree with two leaves to get an internal node */
    verkle_tree_t *vt = verkle_create();

    uint8_t key1[32], key2[32];
    memset(key1, 0x11, 31); key1[31] = 0;
    memset(key2, 0x22, 31); key2[31] = 0;

    uint8_t val[32];
    memset(val, 0xBB, 32);
    verkle_set(vt, key1, val);
    verkle_set(vt, key2, val);

    /* Root should be an internal node */
    ASSERT(vt->root != NULL, "root exists");
    ASSERT(vt->root->type == VERKLE_INTERNAL, "root is internal");

    banderwagon_point_t root_commit = vt->root->internal.commitment;
    ASSERT(!banderwagon_is_identity(&root_commit), "root commitment non-identity");

    /* Store and retrieve at different depths */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "store created");

    /* Depth 0 (root) */
    bool ok = vcs_put_internal(cs, 0, NULL, &root_commit);
    ASSERT(ok, "put_internal depth=0 succeeds");

    banderwagon_point_t got;
    ok = vcs_get_internal(cs, 0, NULL, &got);
    ASSERT(ok, "get_internal depth=0 succeeds");
    ASSERT(banderwagon_eq(&root_commit, &got), "depth=0 round-trips");

    /* Depth 1 (arbitrary path byte) */
    uint8_t path1[1] = {0x11};
    ok = vcs_put_internal(cs, 1, path1, &root_commit);
    ASSERT(ok, "put_internal depth=1 succeeds");

    ok = vcs_get_internal(cs, 1, path1, &got);
    ASSERT(ok, "get_internal depth=1 succeeds");
    ASSERT(banderwagon_eq(&root_commit, &got), "depth=1 round-trips");

    /* Depth 15 (deeper path) */
    uint8_t path15[15];
    for (int i = 0; i < 15; i++) path15[i] = (uint8_t)(i + 1);
    ok = vcs_put_internal(cs, 15, path15, &root_commit);
    ASSERT(ok, "put_internal depth=15 succeeds");

    ok = vcs_get_internal(cs, 15, path15, &got);
    ASSERT(ok, "get_internal depth=15 succeeds");
    ASSERT(banderwagon_eq(&root_commit, &got), "depth=15 round-trips");

    vcs_destroy(cs);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 4: Get Non-Existent
 * ========================================================================= */

static void test_get_nonexistent(void) {
    printf("Phase 4: Get non-existent entries\n");
    cleanup();

    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "store created");

    uint8_t stem[31];
    memset(stem, 0xFF, 31);

    banderwagon_point_t c1, c2, commit;
    ASSERT(!vcs_get_leaf(cs, stem, &c1, &c2, &commit),
           "get_leaf returns false for unknown stem");

    banderwagon_point_t got;
    ASSERT(!vcs_get_internal(cs, 0, NULL, &got),
           "get_internal returns false for unknown path");

    uint8_t path[5] = {1, 2, 3, 4, 5};
    ASSERT(!vcs_get_internal(cs, 5, path, &got),
           "get_internal returns false for unknown deep path");

    vcs_destroy(cs);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 5: Overwrite Leaf
 * ========================================================================= */

static void test_overwrite(void) {
    printf("Phase 5: Overwrite leaf commitment\n");
    cleanup();

    /* Build two different trees to get two different commitment sets */
    verkle_tree_t *vt1 = verkle_create();
    uint8_t key[32];
    memset(key, 0xAA, 31); key[31] = 0;
    uint8_t val1[32]; memset(val1, 0x11, 32);
    verkle_set(vt1, key, val1);

    verkle_tree_t *vt2 = verkle_create();
    uint8_t val2[32]; memset(val2, 0x22, 32);
    verkle_set(vt2, key, val2);

    /* Both have same stem but different commitments */
    uint8_t stem[31];
    memcpy(stem, vt1->root->leaf.stem, 31);

    banderwagon_point_t c1_v1 = vt1->root->leaf.c1;
    banderwagon_point_t c2_v1 = vt1->root->leaf.c2;
    banderwagon_point_t cm_v1 = vt1->root->leaf.commitment;

    banderwagon_point_t c1_v2 = vt2->root->leaf.c1;
    banderwagon_point_t c2_v2 = vt2->root->leaf.c2;
    banderwagon_point_t cm_v2 = vt2->root->leaf.commitment;

    ASSERT(!banderwagon_eq(&cm_v1, &cm_v2),
           "different values produce different commitments");

    /* Store first version */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    vcs_put_leaf(cs, stem, &c1_v1, &c2_v1, &cm_v1);

    /* Overwrite with second version */
    vcs_put_leaf(cs, stem, &c1_v2, &c2_v2, &cm_v2);

    /* Retrieve — should get second version */
    banderwagon_point_t c1_got, c2_got, cm_got;
    bool ok = vcs_get_leaf(cs, stem, &c1_got, &c2_got, &cm_got);
    ASSERT(ok, "get_leaf after overwrite succeeds");
    ASSERT(banderwagon_eq(&c1_v2, &c1_got), "C1 is updated value");
    ASSERT(banderwagon_eq(&c2_v2, &c2_got), "C2 is updated value");
    ASSERT(banderwagon_eq(&cm_v2, &cm_got), "commitment is updated value");

    vcs_destroy(cs);
    verkle_destroy(vt1);
    verkle_destroy(vt2);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 6: Flush Tree
 * ========================================================================= */

static void test_flush_tree(void) {
    printf("Phase 6: Flush tree\n");
    cleanup();

    /* Build a tree with multiple leaves (forces internal nodes) */
    verkle_tree_t *vt = verkle_create();

    uint8_t key1[32], key2[32], key3[32];
    memset(key1, 0x11, 31); key1[31] = 0;
    memset(key2, 0x22, 31); key2[31] = 0;
    memset(key3, 0x33, 31); key3[31] = 5;

    uint8_t val[32]; memset(val, 0xCC, 32);
    verkle_set(vt, key1, val);
    verkle_set(vt, key2, val);
    verkle_set(vt, key3, val);

    /* Flush all commitments */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "store created");

    bool ok = vcs_flush_tree(cs, vt);
    ASSERT(ok, "flush_tree succeeds");

    /* Verify root (internal) commitment */
    ASSERT(vt->root->type == VERKLE_INTERNAL, "root is internal");
    banderwagon_point_t root_got;
    ok = vcs_get_internal(cs, 0, NULL, &root_got);
    ASSERT(ok, "root commitment found in store");
    ASSERT(banderwagon_eq(&vt->root->internal.commitment, &root_got),
           "root commitment matches");

    /* Verify each leaf's commitments by looking them up by stem */
    /* Walk to find the leaves and check each */
    int leaves_found = 0;
    for (int i = 0; i < VERKLE_WIDTH; i++) {
        verkle_node_t *child = vt->root->internal.children[i];
        if (!child) continue;

        if (child->type == VERKLE_LEAF) {
            banderwagon_point_t c1, c2, cm;
            ok = vcs_get_leaf(cs, child->leaf.stem, &c1, &c2, &cm);
            ASSERT(ok, "leaf commitment found in store");
            ASSERT(banderwagon_eq(&child->leaf.commitment, &cm),
                   "leaf commitment matches");
            ASSERT(banderwagon_eq(&child->leaf.c1, &c1),
                   "leaf C1 matches");
            ASSERT(banderwagon_eq(&child->leaf.c2, &c2),
                   "leaf C2 matches");
            leaves_found++;
        } else {
            /* Internal child — check its commitment too */
            uint8_t path[1] = {(uint8_t)i};
            banderwagon_point_t ic;
            ok = vcs_get_internal(cs, 1, path, &ic);
            ASSERT(ok, "internal child commitment found in store");
            ASSERT(banderwagon_eq(&child->internal.commitment, &ic),
                   "internal child commitment matches");

            /* Also check leaves under this internal */
            for (int j = 0; j < VERKLE_WIDTH; j++) {
                verkle_node_t *grandchild = child->internal.children[j];
                if (!grandchild) continue;
                if (grandchild->type == VERKLE_LEAF) {
                    banderwagon_point_t gc1, gc2, gcm;
                    ok = vcs_get_leaf(cs, grandchild->leaf.stem,
                                      &gc1, &gc2, &gcm);
                    ASSERT(ok, "nested leaf commitment found");
                    ASSERT(banderwagon_eq(&grandchild->leaf.commitment, &gcm),
                           "nested leaf commitment matches");
                    leaves_found++;
                }
            }
        }
    }

    ASSERT(leaves_found == 3, "all 3 leaves found and verified");

    vcs_destroy(cs);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 7: Persistence Across Close/Reopen
 * ========================================================================= */

static void test_persistence(void) {
    printf("Phase 7: Persistence across close/reopen\n");
    cleanup();

    /* Build a tree */
    verkle_tree_t *vt = verkle_create();

    uint8_t key1[32], key2[32];
    memset(key1, 0xAA, 31); key1[31] = 0;
    memset(key2, 0xBB, 31); key2[31] = 1;
    uint8_t val[32]; memset(val, 0xDD, 32);
    verkle_set(vt, key1, val);
    verkle_set(vt, key2, val);

    /* Save stems and commitments for later verification */
    ASSERT(vt->root->type == VERKLE_INTERNAL, "root is internal");
    banderwagon_point_t saved_root = vt->root->internal.commitment;

    /* Find both leaves and save their data */
    uint8_t saved_stem1[31], saved_stem2[31];
    banderwagon_point_t saved_c1_a, saved_c2_a, saved_cm_a;
    banderwagon_point_t saved_c1_b, saved_c2_b, saved_cm_b;
    int leaf_idx = 0;

    for (int i = 0; i < VERKLE_WIDTH && leaf_idx < 2; i++) {
        verkle_node_t *child = vt->root->internal.children[i];
        if (!child) continue;
        if (child->type == VERKLE_LEAF) {
            if (leaf_idx == 0) {
                memcpy(saved_stem1, child->leaf.stem, 31);
                saved_c1_a = child->leaf.c1;
                saved_c2_a = child->leaf.c2;
                saved_cm_a = child->leaf.commitment;
            } else {
                memcpy(saved_stem2, child->leaf.stem, 31);
                saved_c1_b = child->leaf.c1;
                saved_c2_b = child->leaf.c2;
                saved_cm_b = child->leaf.commitment;
            }
            leaf_idx++;
        }
    }
    ASSERT(leaf_idx == 2, "found 2 leaves");

    /* Flush, sync, and close */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);
    ASSERT(cs != NULL, "store created");

    bool ok = vcs_flush_tree(cs, vt);
    ASSERT(ok, "flush succeeds");
    vcs_sync(cs);
    vcs_destroy(cs);
    verkle_destroy(vt);

    /* Reopen and verify everything survived */
    cs = vcs_open(TEST_DIR);
    ASSERT(cs != NULL, "store reopened");

    /* Root commitment */
    banderwagon_point_t got_root;
    ok = vcs_get_internal(cs, 0, NULL, &got_root);
    ASSERT(ok, "root found after reopen");
    ASSERT(banderwagon_eq(&saved_root, &got_root),
           "root commitment survived restart");

    /* Leaf 1 */
    banderwagon_point_t gc1, gc2, gcm;
    ok = vcs_get_leaf(cs, saved_stem1, &gc1, &gc2, &gcm);
    ASSERT(ok, "leaf 1 found after reopen");
    ASSERT(banderwagon_eq(&saved_cm_a, &gcm),
           "leaf 1 commitment survived");
    ASSERT(banderwagon_eq(&saved_c1_a, &gc1),
           "leaf 1 C1 survived");
    ASSERT(banderwagon_eq(&saved_c2_a, &gc2),
           "leaf 1 C2 survived");

    /* Leaf 2 */
    ok = vcs_get_leaf(cs, saved_stem2, &gc1, &gc2, &gcm);
    ASSERT(ok, "leaf 2 found after reopen");
    ASSERT(banderwagon_eq(&saved_cm_b, &gcm),
           "leaf 2 commitment survived");
    ASSERT(banderwagon_eq(&saved_c1_b, &gc1),
           "leaf 2 C1 survived");
    ASSERT(banderwagon_eq(&saved_c2_b, &gc2),
           "leaf 2 C2 survived");

    vcs_destroy(cs);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 8: Root Node (Depth-0 Internal)
 * ========================================================================= */

static void test_root_node(void) {
    printf("Phase 8: Root node store/load\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    /* Single leaf — root is a leaf */
    uint8_t key[32]; memset(key, 0x55, 31); key[31] = 0;
    uint8_t val[32]; memset(val, 0x77, 32);
    verkle_set(vt, key, val);

    ASSERT(vt->root->type == VERKLE_LEAF, "root is a leaf");

    /* Store the leaf commitment via put_leaf */
    verkle_commit_store_t *cs = vcs_create(TEST_DIR, 1024);

    bool ok = vcs_flush_tree(cs, vt);
    ASSERT(ok, "flush single-leaf tree succeeds");

    /* Load it back */
    banderwagon_point_t c1, c2, cm;
    ok = vcs_get_leaf(cs, vt->root->leaf.stem, &c1, &c2, &cm);
    ASSERT(ok, "leaf-as-root found");
    ASSERT(banderwagon_eq(&vt->root->leaf.commitment, &cm),
           "leaf-as-root commitment matches");

    /* Now add a second key to make root internal */
    uint8_t key2[32]; memset(key2, 0x66, 31); key2[31] = 0;
    verkle_set(vt, key2, val);
    ASSERT(vt->root->type == VERKLE_INTERNAL, "root became internal");

    /* Flush again */
    ok = vcs_flush_tree(cs, vt);
    ASSERT(ok, "flush internal-root tree succeeds");

    /* Load root internal commitment */
    banderwagon_point_t root_got;
    ok = vcs_get_internal(cs, 0, NULL, &root_got);
    ASSERT(ok, "internal root found");
    ASSERT(banderwagon_eq(&vt->root->internal.commitment, &root_got),
           "internal root commitment matches");

    vcs_destroy(cs);
    verkle_destroy(vt);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Commitment Store Tests ===\n\n");

    test_lifecycle();
    test_leaf_round_trip();
    test_internal_round_trip();
    test_get_nonexistent();
    test_overwrite();
    test_flush_tree();
    test_persistence();
    test_root_node();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
