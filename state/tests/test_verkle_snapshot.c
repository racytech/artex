#include "verkle_snapshot.h"
#include "verkle_state.h"
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

#define SNAP_PATH "/tmp/test_verkle_snapshot.bin"

static void cleanup(void) {
    remove(SNAP_PATH);
}

/* =========================================================================
 * Phase 1: Empty Tree
 * ========================================================================= */

static void test_empty_tree(void) {
    printf("Phase 1: Empty tree save/load\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    bool ok = verkle_snapshot_save(vt, SNAP_PATH);
    ASSERT(ok, "save empty tree succeeds");

    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load returns non-NULL");
    ASSERT(loaded->root == NULL, "loaded tree root is NULL");

    uint8_t hash[32], zero[32] = {0};
    verkle_root_hash(loaded, hash);
    ASSERT(memcmp(hash, zero, 32) == 0, "loaded root hash is all zeros");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 2: Single Leaf
 * ========================================================================= */

static void test_single_leaf(void) {
    printf("Phase 2: Single leaf save/load\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();
    uint8_t key[32];
    memset(key, 0xAA, 31); key[31] = 5;
    uint8_t val[32]; memset(val, 0x42, 32);
    verkle_set(vt, key, val);

    /* Capture original root hash */
    uint8_t orig_hash[32];
    verkle_root_hash(vt, orig_hash);

    bool ok = verkle_snapshot_save(vt, SNAP_PATH);
    ASSERT(ok, "save single leaf succeeds");

    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load returns non-NULL");
    ASSERT(loaded->root != NULL, "loaded root is non-NULL");

    /* Verify value */
    uint8_t got[32];
    bool found = verkle_get(loaded, key, got);
    ASSERT(found, "value found in loaded tree");
    ASSERT(memcmp(got, val, 32) == 0, "value matches");

    /* Verify root hash matches */
    uint8_t loaded_hash[32];
    verkle_root_hash(loaded, loaded_hash);
    ASSERT(memcmp(orig_hash, loaded_hash, 32) == 0,
           "root hash matches original");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 3: Multiple Leaves
 * ========================================================================= */

static void test_multiple_leaves(void) {
    printf("Phase 3: Multiple leaves save/load\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    uint8_t key1[32], key2[32], key3[32];
    memset(key1, 0x11, 31); key1[31] = 0;
    memset(key2, 0x22, 31); key2[31] = 0;
    memset(key3, 0x33, 31); key3[31] = 10;

    uint8_t val1[32], val2[32], val3[32];
    memset(val1, 0xAA, 32);
    memset(val2, 0xBB, 32);
    memset(val3, 0xCC, 32);

    verkle_set(vt, key1, val1);
    verkle_set(vt, key2, val2);
    verkle_set(vt, key3, val3);

    uint8_t orig_hash[32];
    verkle_root_hash(vt, orig_hash);

    verkle_snapshot_save(vt, SNAP_PATH);
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load succeeds");

    /* Verify all values */
    uint8_t got[32];
    ASSERT(verkle_get(loaded, key1, got) && memcmp(got, val1, 32) == 0,
           "key1 value matches");
    ASSERT(verkle_get(loaded, key2, got) && memcmp(got, val2, 32) == 0,
           "key2 value matches");
    ASSERT(verkle_get(loaded, key3, got) && memcmp(got, val3, 32) == 0,
           "key3 value matches");

    /* Root hash matches */
    uint8_t loaded_hash[32];
    verkle_root_hash(loaded, loaded_hash);
    ASSERT(memcmp(orig_hash, loaded_hash, 32) == 0,
           "root hash matches");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 4: Sparse Leaf
 * ========================================================================= */

static void test_sparse_leaf(void) {
    printf("Phase 4: Sparse leaf (2 of 256 suffixes set)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    /* Same stem, two different suffixes */
    uint8_t key_a[32], key_b[32];
    memset(key_a, 0x55, 31); key_a[31] = 3;
    memset(key_b, 0x55, 31); key_b[31] = 200;

    uint8_t val_a[32], val_b[32];
    memset(val_a, 0x11, 32);
    memset(val_b, 0x22, 32);

    verkle_set(vt, key_a, val_a);
    verkle_set(vt, key_b, val_b);

    verkle_snapshot_save(vt, SNAP_PATH);
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load succeeds");

    /* Both values present */
    uint8_t got[32];
    ASSERT(verkle_get(loaded, key_a, got) && memcmp(got, val_a, 32) == 0,
           "suffix 3 value matches");
    ASSERT(verkle_get(loaded, key_b, got) && memcmp(got, val_b, 32) == 0,
           "suffix 200 value matches");

    /* Unset suffix returns false */
    uint8_t key_c[32];
    memset(key_c, 0x55, 31); key_c[31] = 100;
    ASSERT(!verkle_get(loaded, key_c, got),
           "unset suffix not found");

    /* Root hashes match */
    uint8_t h1[32], h2[32];
    verkle_root_hash(vt, h1);
    verkle_root_hash(loaded, h2);
    ASSERT(memcmp(h1, h2, 32) == 0, "root hash matches");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 5: Large Values
 * ========================================================================= */

static void test_large_values(void) {
    printf("Phase 5: Large values (non-zero upper bytes)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32];
    memset(key, 0x77, 31); key[31] = 0;

    /* Value with all bytes set */
    uint8_t val[32];
    for (int i = 0; i < 32; i++) val[i] = (uint8_t)(i * 7 + 0xAB);

    verkle_set(vt, key, val);

    verkle_snapshot_save(vt, SNAP_PATH);
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load succeeds");

    uint8_t got[32];
    ASSERT(verkle_get(loaded, key, got), "value found");
    ASSERT(memcmp(got, val, 32) == 0, "all 32 bytes round-trip correctly");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 6: Commitment Preservation
 * ========================================================================= */

static void test_commitment_preservation(void) {
    printf("Phase 6: Commitment preservation (no recomputation)\n");
    cleanup();

    verkle_tree_t *vt = verkle_create();

    /* Build a tree with multiple keys */
    for (int i = 0; i < 10; i++) {
        uint8_t key[32], val[32];
        memset(key, 0, 31);
        key[0] = (uint8_t)(i * 17);  /* different stems */
        key[31] = 0;
        memset(val, (uint8_t)(i + 1), 32);
        verkle_set(vt, key, val);
    }

    banderwagon_point_t orig_root;
    verkle_root_commitment(vt, &orig_root);

    verkle_snapshot_save(vt, SNAP_PATH);
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load succeeds");

    banderwagon_point_t loaded_root;
    verkle_root_commitment(loaded, &loaded_root);
    ASSERT(banderwagon_eq(&orig_root, &loaded_root),
           "root commitment point matches exactly");

    /* Verify serialized hashes match too */
    uint8_t h1[32], h2[32];
    verkle_root_hash(vt, h1);
    verkle_root_hash(loaded, h2);
    ASSERT(memcmp(h1, h2, 32) == 0, "root hash matches");

    verkle_destroy(vt);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 7: Code Chunks via verkle_state
 * ========================================================================= */

static void test_code_chunks(void) {
    printf("Phase 7: Code chunks + header fields survive snapshot\n");
    cleanup();

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xDE, 0xAD};

    /* Set header fields */
    verkle_state_set_nonce(vs, addr, 42);
    uint8_t bal[32] = {0}; bal[0] = 100;
    verkle_state_set_balance(vs, addr, bal);

    /* Set code */
    uint8_t code[100];
    for (int i = 0; i < 100; i++) code[i] = (uint8_t)(i + 1);
    verkle_state_set_code(vs, addr, code, 100);

    /* Save the underlying tree */
    uint8_t orig_hash[32];
    verkle_state_root_hash(vs, orig_hash);

    verkle_snapshot_save(vs->tree, SNAP_PATH);

    /* Load into a new state */
    verkle_tree_t *loaded_tree = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded_tree != NULL, "load succeeds");

    /* Wrap in a new state (swap tree) */
    verkle_state_t *vs2 = verkle_state_create();
    verkle_destroy(vs2->tree);
    vs2->tree = loaded_tree;

    /* Verify header */
    ASSERT(verkle_state_get_nonce(vs2, addr) == 42, "nonce survived");
    uint8_t got_bal[32];
    verkle_state_get_balance(vs2, addr, got_bal);
    ASSERT(memcmp(got_bal, bal, 32) == 0, "balance survived");

    /* Verify code */
    ASSERT(verkle_state_get_code_size(vs2, addr) == 100, "code_size survived");
    uint8_t got_code[100];
    uint64_t len = verkle_state_get_code(vs2, addr, got_code, 100);
    ASSERT(len == 100, "code length correct");
    ASSERT(memcmp(got_code, code, 100) == 0, "code bytes match");

    /* Root hash matches */
    uint8_t loaded_hash[32];
    verkle_state_root_hash(vs2, loaded_hash);
    ASSERT(memcmp(orig_hash, loaded_hash, 32) == 0, "root hash matches");

    verkle_state_destroy(vs);
    verkle_state_destroy(vs2);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 8: File Overwrite
 * ========================================================================= */

static void test_overwrite(void) {
    printf("Phase 8: File overwrite\n");
    cleanup();

    /* Save tree A */
    verkle_tree_t *vt_a = verkle_create();
    uint8_t key_a[32]; memset(key_a, 0xAA, 31); key_a[31] = 0;
    uint8_t val_a[32]; memset(val_a, 0x11, 32);
    verkle_set(vt_a, key_a, val_a);
    verkle_snapshot_save(vt_a, SNAP_PATH);

    /* Save tree B to same path (overwrite) */
    verkle_tree_t *vt_b = verkle_create();
    uint8_t key_b[32]; memset(key_b, 0xBB, 31); key_b[31] = 0;
    uint8_t val_b[32]; memset(val_b, 0x22, 32);
    verkle_set(vt_b, key_b, val_b);
    verkle_snapshot_save(vt_b, SNAP_PATH);

    /* Load — should get tree B, not A */
    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded != NULL, "load succeeds");

    uint8_t got[32];
    ASSERT(!verkle_get(loaded, key_a, got), "key_a NOT found (overwritten)");
    ASSERT(verkle_get(loaded, key_b, got) && memcmp(got, val_b, 32) == 0,
           "key_b found with correct value");

    uint8_t h1[32], h2[32];
    verkle_root_hash(vt_b, h1);
    verkle_root_hash(loaded, h2);
    ASSERT(memcmp(h1, h2, 32) == 0, "root hash matches tree B");

    verkle_destroy(vt_a);
    verkle_destroy(vt_b);
    verkle_destroy(loaded);
    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Phase 9: Bad Magic
 * ========================================================================= */

static void test_bad_magic(void) {
    printf("Phase 9: Bad magic rejected\n");
    cleanup();

    /* Write garbage to file */
    FILE *f = fopen(SNAP_PATH, "wb");
    const char *garbage = "NOT_A_SNAPSHOT_FILE!!";
    fwrite(garbage, 1, strlen(garbage), f);
    fclose(f);

    verkle_tree_t *loaded = verkle_snapshot_load(SNAP_PATH);
    ASSERT(loaded == NULL, "bad magic returns NULL");

    printf("  OK\n\n");
    cleanup();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Snapshot Tests ===\n\n");

    test_empty_tree();
    test_single_leaf();
    test_multiple_leaves();
    test_sparse_leaf();
    test_large_values();
    test_commitment_preservation();
    test_code_chunks();
    test_overwrite();
    test_bad_magic();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
