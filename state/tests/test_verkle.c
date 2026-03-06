#include "verkle.h"
#include <stdio.h>
#include <string.h>

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

static void print_bytes(const char *label, const uint8_t *data, int len) {
    printf("  %s: ", label);
    for (int i = 0; i < len; i++) printf("%02x", data[i]);
    printf("\n");
}

/** Build a key from a stem pattern byte + suffix. */
static void make_key(uint8_t key[32], uint8_t stem_fill, uint8_t suffix) {
    memset(key, stem_fill, 31);
    key[31] = suffix;
}

/** Build a key with a specific stem prefix that diverges at a given depth. */
static void make_key_diverge(uint8_t key[32], uint8_t common,
                             int diverge_depth, uint8_t diverge_byte,
                             uint8_t suffix)
{
    memset(key, common, 31);
    key[diverge_depth] = diverge_byte;
    key[31] = suffix;
}

/** Build a value from a single fill byte. */
static void make_value(uint8_t value[32], uint8_t fill) {
    memset(value, 0, 32);
    value[0] = fill;
}

/* =========================================================================
 * Phase 1: Create/Destroy Empty Tree
 * ========================================================================= */

static void test_empty_tree(void) {
    printf("Phase 1: Empty tree\n");

    verkle_tree_t *vt = verkle_create();
    ASSERT(vt != NULL, "verkle_create returns non-NULL");

    /* Root commitment of empty tree should be identity */
    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);
    ASSERT(banderwagon_is_identity(&root),
           "empty tree root commitment is identity");

    /* Root hash of empty tree should be all zeros */
    uint8_t hash[32];
    verkle_root_hash(vt, hash);
    uint8_t zero[32] = {0};
    ASSERT(memcmp(hash, zero, 32) == 0,
           "empty tree root hash is all zeros");

    /* Get on empty tree returns false */
    uint8_t key[32] = {0};
    uint8_t value[32];
    ASSERT(!verkle_get(vt, key, value),
           "get on empty tree returns false");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 2: Single Insert and Get
 * ========================================================================= */

static void test_single_insert(void) {
    printf("Phase 2: Single insert and get\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], value[32], got[32];
    make_key(key, 0x01, 0x00);
    make_value(value, 42);

    ASSERT(verkle_set(vt, key, value), "set returns true");
    ASSERT(verkle_get(vt, key, got), "get returns true for inserted key");
    ASSERT(memcmp(got, value, 32) == 0, "get returns correct value");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 3: Single Insert — Commitment Not Identity
 * ========================================================================= */

static void test_single_commit(void) {
    printf("Phase 3: Single insert commitment\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], value[32];
    make_key(key, 0x01, 0x00);
    make_value(value, 1);

    verkle_set(vt, key, value);

    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);
    ASSERT(!banderwagon_is_identity(&root),
           "non-empty tree root commitment is NOT identity");

    /* Root hash should be non-zero */
    uint8_t hash[32];
    verkle_root_hash(vt, hash);
    uint8_t zero[32] = {0};
    ASSERT(memcmp(hash, zero, 32) != 0,
           "non-empty tree root hash is non-zero");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 4: Multiple Inserts — Same Stem (Different Suffixes)
 * ========================================================================= */

static void test_same_stem(void) {
    printf("Phase 4: Multiple inserts, same stem\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], value[32], got[32];

    /* Insert 4 values under same stem (all stem bytes = 0xAA) */
    for (int i = 0; i < 4; i++) {
        make_key(key, 0xAA, (uint8_t)i);
        make_value(value, (uint8_t)(10 + i));
        ASSERT(verkle_set(vt, key, value), "set same-stem key ok");
    }

    /* Verify all can be retrieved */
    for (int i = 0; i < 4; i++) {
        make_key(key, 0xAA, (uint8_t)i);
        ASSERT(verkle_get(vt, key, got), "get same-stem key ok");
        ASSERT(got[0] == (uint8_t)(10 + i), "value matches");
    }

    /* Suffix not inserted should return false */
    make_key(key, 0xAA, 0xFF);
    ASSERT(!verkle_get(vt, key, got),
           "get unset suffix returns false");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 5: Different Stems — Forces Internal Node Creation
 * ========================================================================= */

static void test_different_stems(void) {
    printf("Phase 5: Different stems (internal node creation)\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key1[32], key2[32], val1[32], val2[32], got[32];

    /* Two keys with completely different stems */
    make_key(key1, 0x01, 0x00);
    make_key(key2, 0x02, 0x00);
    make_value(val1, 11);
    make_value(val2, 22);

    ASSERT(verkle_set(vt, key1, val1), "set key1");
    ASSERT(verkle_set(vt, key2, val2), "set key2");

    ASSERT(verkle_get(vt, key1, got), "get key1");
    ASSERT(got[0] == 11, "key1 value correct");

    ASSERT(verkle_get(vt, key2, got), "get key2");
    ASSERT(got[0] == 22, "key2 value correct");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Stem Split — Shared Prefix
 * ========================================================================= */

static void test_stem_split(void) {
    printf("Phase 6: Stem split (shared prefix)\n");

    verkle_tree_t *vt = verkle_create();

    /* Two keys: stems share bytes 0-3, diverge at byte 4 */
    uint8_t key1[32], key2[32], val1[32], val2[32], got[32];
    make_key_diverge(key1, 0x55, 4, 0xAA, 0x00);
    make_key_diverge(key2, 0x55, 4, 0xBB, 0x00);
    make_value(val1, 100);
    make_value(val2, 200);

    ASSERT(verkle_set(vt, key1, val1), "set key1 (split)");
    ASSERT(verkle_set(vt, key2, val2), "set key2 (split)");

    ASSERT(verkle_get(vt, key1, got), "get key1 after split");
    ASSERT(got[0] == 100, "key1 value correct after split");

    ASSERT(verkle_get(vt, key2, got), "get key2 after split");
    ASSERT(got[0] == 200, "key2 value correct after split");

    /* Key with same prefix but different diverge byte should not exist */
    uint8_t key3[32];
    make_key_diverge(key3, 0x55, 4, 0xCC, 0x00);
    ASSERT(!verkle_get(vt, key3, got),
           "nonexistent diverging key returns false");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 7: Update Value — Commitment Changes
 * ========================================================================= */

static void test_update_value(void) {
    printf("Phase 7: Update value\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], val1[32], val2[32], got[32];
    make_key(key, 0x03, 0x42);
    make_value(val1, 1);
    make_value(val2, 5);

    verkle_set(vt, key, val1);

    /* Capture root commitment before update */
    banderwagon_point_t root_before;
    verkle_root_commitment(vt, &root_before);

    /* Update the value */
    ASSERT(verkle_set(vt, key, val2), "update returns true");
    ASSERT(verkle_get(vt, key, got), "get after update");
    ASSERT(got[0] == 5, "updated value is correct");

    /* Root commitment should change */
    banderwagon_point_t root_after;
    verkle_root_commitment(vt, &root_after);
    ASSERT(!banderwagon_eq(&root_before, &root_after),
           "root commitment changes after value update");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 8: Get Nonexistent Keys
 * ========================================================================= */

static void test_get_nonexistent(void) {
    printf("Phase 8: Get nonexistent keys\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], value[32], got[32];
    make_key(key, 0x10, 0x00);
    make_value(value, 77);
    verkle_set(vt, key, value);

    /* Different stem entirely */
    uint8_t wrong_stem[32];
    make_key(wrong_stem, 0x20, 0x00);
    ASSERT(!verkle_get(vt, wrong_stem, got),
           "different stem returns false");

    /* Same stem but unset suffix */
    uint8_t wrong_suffix[32];
    make_key(wrong_suffix, 0x10, 0xFF);
    ASSERT(!verkle_get(vt, wrong_suffix, got),
           "unset suffix returns false");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 9: Commitment Determinism
 * ========================================================================= */

static void test_commitment_determinism(void) {
    printf("Phase 9: Commitment determinism\n");

    /* Insert keys in order A, B, C */
    verkle_tree_t *vt1 = verkle_create();
    uint8_t kA[32], kB[32], kC[32];
    uint8_t vA[32], vB[32], vC[32];

    make_key(kA, 0x01, 0x00);
    make_key(kB, 0x02, 0x00);
    make_key(kC, 0x03, 0x00);
    make_value(vA, 10);
    make_value(vB, 20);
    make_value(vC, 30);

    verkle_set(vt1, kA, vA);
    verkle_set(vt1, kB, vB);
    verkle_set(vt1, kC, vC);

    banderwagon_point_t root1;
    verkle_root_commitment(vt1, &root1);

    /* Insert keys in order C, A, B */
    verkle_tree_t *vt2 = verkle_create();
    verkle_set(vt2, kC, vC);
    verkle_set(vt2, kA, vA);
    verkle_set(vt2, kB, vB);

    banderwagon_point_t root2;
    verkle_root_commitment(vt2, &root2);

    ASSERT(banderwagon_eq(&root1, &root2),
           "same keys/values in different order yield same root");

    verkle_destroy(vt1);
    verkle_destroy(vt2);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 10: Root Hash Consistency
 * ========================================================================= */

static void test_root_hash(void) {
    printf("Phase 10: Root hash consistency\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32], value[32];
    make_key(key, 0x42, 0x07);
    make_value(value, 99);
    verkle_set(vt, key, value);

    /* root_hash should equal serialize(root_commitment) */
    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);

    uint8_t hash_via_api[32];
    verkle_root_hash(vt, hash_via_api);

    uint8_t hash_manual[32];
    banderwagon_serialize(hash_manual, &root);

    ASSERT(memcmp(hash_via_api, hash_manual, 32) == 0,
           "root_hash == serialize(root_commitment)");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 11: Multiple Stems with Shared Prefixes of Varying Length
 * ========================================================================= */

static void test_multi_split(void) {
    printf("Phase 11: Multiple splits at varying depths\n");

    verkle_tree_t *vt = verkle_create();

    /* 4 keys with increasingly shared prefixes:
     * key1: [AA AA AA AA 01 ...]  suffix=0
     * key2: [AA AA AA AA 02 ...]  suffix=0  (diverge at depth 4)
     * key3: [AA AA BB ...]        suffix=0  (diverge at depth 2)
     * key4: [BB ...]              suffix=0  (diverge at depth 0)
     */
    uint8_t key1[32], key2[32], key3[32], key4[32];
    uint8_t val1[32], val2[32], val3[32], val4[32];
    uint8_t got[32];

    make_key_diverge(key1, 0xAA, 4, 0x01, 0x00);
    make_key_diverge(key2, 0xAA, 4, 0x02, 0x00);
    make_key_diverge(key3, 0xAA, 2, 0xBB, 0x00);
    memset(key4, 0xBB, 31); key4[31] = 0x00;

    make_value(val1, 1);
    make_value(val2, 2);
    make_value(val3, 3);
    make_value(val4, 4);

    ASSERT(verkle_set(vt, key1, val1), "set key1");
    ASSERT(verkle_set(vt, key2, val2), "set key2");
    ASSERT(verkle_set(vt, key3, val3), "set key3");
    ASSERT(verkle_set(vt, key4, val4), "set key4");

    ASSERT(verkle_get(vt, key1, got) && got[0] == 1, "key1 correct");
    ASSERT(verkle_get(vt, key2, got) && got[0] == 2, "key2 correct");
    ASSERT(verkle_get(vt, key3, got) && got[0] == 3, "key3 correct");
    ASSERT(verkle_get(vt, key4, got) && got[0] == 4, "key4 correct");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 12: Values in C1 and C2 Halves
 * ========================================================================= */

static void test_c1_c2_halves(void) {
    printf("Phase 12: Values in C1 and C2 halves\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key_c1[32], key_c2[32];
    uint8_t val_c1[32], val_c2[32], got[32];

    /* suffix 10 → C1 (suffix < 128) */
    make_key(key_c1, 0x50, 10);
    make_value(val_c1, 0xAA);

    /* suffix 200 → C2 (suffix >= 128) */
    make_key(key_c2, 0x50, 200);
    make_value(val_c2, 0xBB);

    ASSERT(verkle_set(vt, key_c1, val_c1), "set C1 value");
    ASSERT(verkle_set(vt, key_c2, val_c2), "set C2 value");

    ASSERT(verkle_get(vt, key_c1, got) && got[0] == 0xAA, "C1 value correct");
    ASSERT(verkle_get(vt, key_c2, got) && got[0] == 0xBB, "C2 value correct");

    /* Commitment should be non-identity */
    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);
    ASSERT(!banderwagon_is_identity(&root),
           "root with C1 and C2 values is not identity");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 13: Large Value (Full 32-Byte Value Splitting)
 * ========================================================================= */

static void test_large_value(void) {
    printf("Phase 13: Large 32-byte value\n");

    verkle_tree_t *vt = verkle_create();

    uint8_t key[32];
    make_key(key, 0x60, 0x00);

    /* A value that uses all 32 bytes (upper 16 bytes non-zero) */
    uint8_t value[32];
    for (int i = 0; i < 32; i++) value[i] = (uint8_t)(i + 1);

    ASSERT(verkle_set(vt, key, value), "set large value");

    uint8_t got[32];
    ASSERT(verkle_get(vt, key, got), "get large value");
    ASSERT(memcmp(got, value, 32) == 0, "large value round-trips");

    /* Verify commitment is deterministic */
    uint8_t hash1[32], hash2[32];
    verkle_root_hash(vt, hash1);

    verkle_tree_t *vt2 = verkle_create();
    verkle_set(vt2, key, value);
    verkle_root_hash(vt2, hash2);

    ASSERT(memcmp(hash1, hash2, 32) == 0,
           "same key/value produces same root hash");

    verkle_destroy(vt);
    verkle_destroy(vt2);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 14: Stress — Many Keys
 * ========================================================================= */

static void test_many_keys(void) {
    printf("Phase 14: Many keys (64 different stems)\n");

    verkle_tree_t *vt = verkle_create();
    uint8_t key[32], value[32], got[32];

    /* Insert 64 keys with different stems */
    for (int i = 0; i < 64; i++) {
        make_key(key, (uint8_t)i, 0x00);
        make_value(value, (uint8_t)(i + 100));
        ASSERT(verkle_set(vt, key, value), "set many keys");
    }

    /* Verify all */
    bool all_correct = true;
    for (int i = 0; i < 64; i++) {
        make_key(key, (uint8_t)i, 0x00);
        if (!verkle_get(vt, key, got) || got[0] != (uint8_t)(i + 100)) {
            printf("  FAIL: key %d incorrect\n", i);
            all_correct = false;
        }
    }
    ASSERT(all_correct, "all 64 keys retrieved correctly");

    /* Root should be non-identity */
    banderwagon_point_t root;
    verkle_root_commitment(vt, &root);
    ASSERT(!banderwagon_is_identity(&root),
           "64-key tree has non-identity root");

    verkle_destroy(vt);
    printf("  OK\n\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Tree Tests ===\n\n");

    test_empty_tree();
    test_single_insert();
    test_single_commit();
    test_same_stem();
    test_different_stems();
    test_stem_split();
    test_update_value();
    test_get_nonexistent();
    test_commitment_determinism();
    test_root_hash();
    test_multi_split();
    test_c1_c2_halves();
    test_large_value();
    test_many_keys();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
