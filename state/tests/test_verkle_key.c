#include "verkle_key.h"
#include "pedersen.h"
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

/* =========================================================================
 * Phase 1: Determinism
 * ========================================================================= */

static void test_determinism(void) {
    printf("Phase 1: Determinism\n");

    uint8_t addr[20] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE};
    uint8_t tree_index[32] = {0};
    tree_index[0] = 42;

    uint8_t key1[32], key2[32];
    verkle_derive_key(key1, addr, tree_index, 7);
    verkle_derive_key(key2, addr, tree_index, 7);

    ASSERT(memcmp(key1, key2, 32) == 0,
           "same inputs produce same key");

    /* Call again to make sure it's stable */
    uint8_t key3[32];
    verkle_derive_key(key3, addr, tree_index, 7);
    ASSERT(memcmp(key1, key3, 32) == 0,
           "deterministic across multiple calls");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 2: Different Addresses → Different Stems
 * ========================================================================= */

static void test_different_addresses(void) {
    printf("Phase 2: Different addresses produce different stems\n");

    uint8_t addr1[20] = {0};
    uint8_t addr2[20] = {0};
    addr1[0] = 1;
    addr2[0] = 2;

    uint8_t tree_index[32] = {0};

    uint8_t stem1[31], stem2[31];
    verkle_derive_stem(stem1, addr1, tree_index);
    verkle_derive_stem(stem2, addr2, tree_index);

    ASSERT(memcmp(stem1, stem2, 31) != 0,
           "different addresses yield different stems");

    /* Also test addresses differing in higher bytes */
    uint8_t addr3[20] = {0};
    uint8_t addr4[20] = {0};
    addr3[19] = 0xFF;
    addr4[19] = 0xFE;

    uint8_t stem3[31], stem4[31];
    verkle_derive_stem(stem3, addr3, tree_index);
    verkle_derive_stem(stem4, addr4, tree_index);

    ASSERT(memcmp(stem3, stem4, 31) != 0,
           "addresses differing in high byte yield different stems");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 3: Different Tree Indices → Different Stems
 * ========================================================================= */

static void test_different_tree_indices(void) {
    printf("Phase 3: Different tree indices produce different stems\n");

    uint8_t addr[20] = {0x42};

    uint8_t ti0[32] = {0};
    uint8_t ti1[32] = {0};
    ti1[0] = 1;

    uint8_t stem0[31], stem1[31];
    verkle_derive_stem(stem0, addr, ti0);
    verkle_derive_stem(stem1, addr, ti1);

    ASSERT(memcmp(stem0, stem1, 31) != 0,
           "tree_index 0 vs 1 yield different stems");

    /* Test difference in upper half of tree_index */
    uint8_t ti_hi0[32] = {0};
    uint8_t ti_hi1[32] = {0};
    ti_hi1[16] = 1;  /* differs in upper 128 bits */

    uint8_t stem_hi0[31], stem_hi1[31];
    verkle_derive_stem(stem_hi0, addr, ti_hi0);
    verkle_derive_stem(stem_hi1, addr, ti_hi1);

    ASSERT(memcmp(stem_hi0, stem_hi1, 31) != 0,
           "tree_index differing in upper half yield different stems");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 4: Same Stem, Different Suffix
 * ========================================================================= */

static void test_same_stem_different_suffix(void) {
    printf("Phase 4: Same stem, different suffix\n");

    uint8_t addr[20] = {0x10, 0x20, 0x30};
    uint8_t tree_index[32] = {0};
    tree_index[0] = 5;

    uint8_t key_a[32], key_b[32];
    verkle_derive_key(key_a, addr, tree_index, 0);
    verkle_derive_key(key_b, addr, tree_index, 100);

    /* First 31 bytes (stem) should be identical */
    ASSERT(memcmp(key_a, key_b, 31) == 0,
           "stem is shared across suffixes");

    /* Byte 31 should differ */
    ASSERT(key_a[31] == 0, "suffix byte is 0 for sub_index=0");
    ASSERT(key_b[31] == 100, "suffix byte is 100 for sub_index=100");
    ASSERT(key_a[31] != key_b[31],
           "suffix byte differs");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 5: Account Convenience Keys
 * ========================================================================= */

static void test_account_convenience(void) {
    printf("Phase 5: Account convenience keys\n");

    uint8_t addr[20];
    for (int i = 0; i < 20; i++) addr[i] = (uint8_t)(i + 1);

    uint8_t key_ver[32], key_bal[32], key_non[32], key_ch[32], key_cs[32];
    verkle_account_version_key(key_ver, addr);
    verkle_account_balance_key(key_bal, addr);
    verkle_account_nonce_key(key_non, addr);
    verkle_account_code_hash_key(key_ch, addr);
    verkle_account_code_size_key(key_cs, addr);

    /* All should share the same stem (tree_index=0 for all) */
    ASSERT(memcmp(key_ver, key_bal, 31) == 0, "version/balance share stem");
    ASSERT(memcmp(key_ver, key_non, 31) == 0, "version/nonce share stem");
    ASSERT(memcmp(key_ver, key_ch, 31) == 0, "version/code_hash share stem");
    ASSERT(memcmp(key_ver, key_cs, 31) == 0, "version/code_size share stem");

    /* Check suffix bytes */
    ASSERT(key_ver[31] == VERKLE_VERSION_SUFFIX, "version suffix = 0");
    ASSERT(key_bal[31] == VERKLE_BALANCE_SUFFIX, "balance suffix = 1");
    ASSERT(key_non[31] == VERKLE_NONCE_SUFFIX, "nonce suffix = 2");
    ASSERT(key_ch[31] == VERKLE_CODE_HASH_SUFFIX, "code_hash suffix = 3");
    ASSERT(key_cs[31] == VERKLE_CODE_SIZE_SUFFIX, "code_size suffix = 4");

    /* Verify these match manual derivation */
    uint8_t key_manual[32];
    uint8_t zero_ti[32] = {0};
    verkle_derive_key(key_manual, addr, zero_ti, VERKLE_BALANCE_SUFFIX);
    ASSERT(memcmp(key_bal, key_manual, 32) == 0,
           "balance_key matches manual derive with tree_index=0, suffix=1");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Storage Key Math
 * ========================================================================= */

static void test_storage_key_math(void) {
    printf("Phase 6: Storage key math\n");

    uint8_t addr[20] = {0xDE, 0xAD};

    /* slot=0: tree_index = (0 >> 8) + 1 = 1, sub_index = 0 */
    uint8_t slot0[32] = {0};
    uint8_t key0[32];
    verkle_storage_key(key0, addr, slot0);
    ASSERT(key0[31] == 0, "slot 0: sub_index = 0");

    /* Verify stem matches derive_key with tree_index=1 */
    uint8_t ti1[32] = {0};
    ti1[0] = 1;
    uint8_t key_manual[32];
    verkle_derive_key(key_manual, addr, ti1, 0);
    ASSERT(memcmp(key0, key_manual, 32) == 0,
           "slot 0 matches tree_index=1, sub_index=0");

    /* slot=255: tree_index = (255 >> 8) + 1 = 1, sub_index = 255 */
    uint8_t slot255[32] = {0};
    slot255[0] = 255;
    uint8_t key255[32];
    verkle_storage_key(key255, addr, slot255);
    ASSERT(key255[31] == 255, "slot 255: sub_index = 255");
    ASSERT(memcmp(key0, key255, 31) == 0,
           "slots 0 and 255 share stem (same tree_index)");

    /* slot=256: tree_index = (256 >> 8) + 1 = 2, sub_index = 0 */
    uint8_t slot256[32] = {0};
    slot256[0] = 0;    /* 256 & 0xFF = 0 */
    slot256[1] = 1;    /* 256 >> 8 = 1 (LE: byte[1] = 1) */
    uint8_t key256[32];
    verkle_storage_key(key256, addr, slot256);
    ASSERT(key256[31] == 0, "slot 256: sub_index = 0");
    ASSERT(memcmp(key0, key256, 31) != 0,
           "slot 256 has different stem from slot 0");

    /* Verify slot 256 matches tree_index=2 */
    uint8_t ti2[32] = {0};
    ti2[0] = 2;
    verkle_derive_key(key_manual, addr, ti2, 0);
    ASSERT(memcmp(key256, key_manual, 32) == 0,
           "slot 256 matches tree_index=2, sub_index=0");

    /* slot=257: tree_index = 2, sub_index = 1 */
    uint8_t slot257[32] = {0};
    slot257[0] = 1;    /* 257 & 0xFF = 1 */
    slot257[1] = 1;    /* 257 >> 8 = 1 */
    uint8_t key257[32];
    verkle_storage_key(key257, addr, slot257);
    ASSERT(key257[31] == 1, "slot 257: sub_index = 1");
    ASSERT(memcmp(key256, key257, 31) == 0,
           "slots 256 and 257 share stem");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 7: Non-Zero Stems
 * ========================================================================= */

static void test_non_zero_stems(void) {
    printf("Phase 7: Non-zero stems\n");

    uint8_t zero_addr[20] = {0};
    uint8_t zero_ti[32] = {0};
    uint8_t stem[31];

    verkle_derive_stem(stem, zero_addr, zero_ti);

    /* Even with all-zero inputs, stem should be non-zero
     * because domain separator (2) ensures non-trivial commitment */
    uint8_t zero_stem[31] = {0};
    ASSERT(memcmp(stem, zero_stem, 31) != 0,
           "zero-address stem is non-zero (domain separator)");

    /* A more typical address */
    uint8_t addr[20] = {0x01, 0x02, 0x03};
    verkle_derive_stem(stem, addr, zero_ti);
    ASSERT(memcmp(stem, zero_stem, 31) != 0,
           "typical address stem is non-zero");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 8: Domain Separation
 * ========================================================================= */

static void test_domain_separation(void) {
    printf("Phase 8: Domain separation\n");

    /* Key derivation uses domain separator = 2 in scalar[0].
     * Leaf commitment uses marker = 1 in scalar[0].
     * Even with the same remaining scalars, outputs should differ. */

    uint8_t addr[20] = {0};
    uint8_t tree_index[32] = {0};

    /* Get the key derivation stem */
    uint8_t stem[31];
    verkle_derive_stem(stem, addr, tree_index);

    /* Now manually compute what a "leaf commitment" would produce
     * with the same address/tree_index data but marker=1 */
    uint8_t scalars_leaf[4][32];
    memset(scalars_leaf, 0, sizeof(scalars_leaf));
    scalars_leaf[0][0] = 1;  /* leaf marker, not key domain */
    memcpy(scalars_leaf[1], addr, 20);
    memcpy(scalars_leaf[2], tree_index, 16);
    memcpy(scalars_leaf[3], tree_index + 16, 16);

    banderwagon_point_t leaf_commit;
    pedersen_commit(&leaf_commit, scalars_leaf, 4);

    uint8_t leaf_field[32];
    banderwagon_map_to_field(leaf_field, &leaf_commit);

    /* The stem from key derivation should differ from leaf commitment's field output */
    ASSERT(memcmp(stem, leaf_field, 31) != 0,
           "key derivation output differs from leaf commitment with same data");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 9: Storage Key — Header vs Storage Separation
 * ========================================================================= */

static void test_header_storage_separation(void) {
    printf("Phase 9: Header vs storage key separation\n");

    uint8_t addr[20];
    for (int i = 0; i < 20; i++) addr[i] = (uint8_t)(0x50 + i);

    /* Account header stem (tree_index=0) */
    uint8_t header_key[32];
    verkle_account_balance_key(header_key, addr);

    /* Storage slot 0 stem (tree_index=1) */
    uint8_t slot0[32] = {0};
    uint8_t storage_key[32];
    verkle_storage_key(storage_key, addr, slot0);

    /* Stems must differ — header uses tree_index=0, storage uses tree_index=1 */
    ASSERT(memcmp(header_key, storage_key, 31) != 0,
           "account header and storage slot 0 have different stems");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 10: Large Storage Slot
 * ========================================================================= */

static void test_large_storage_slot(void) {
    printf("Phase 10: Large storage slot\n");

    uint8_t addr[20] = {0x77};

    /* slot = 0x10000 (65536): tree_index = (65536 >> 8) + 1 = 257, sub_index = 0 */
    uint8_t slot[32] = {0};
    slot[0] = 0x00;  /* 65536 & 0xFF = 0 */
    slot[1] = 0x00;  /* (65536 >> 8) & 0xFF = 0 */
    slot[2] = 0x01;  /* (65536 >> 16) & 0xFF = 1 */

    uint8_t key[32];
    verkle_storage_key(key, addr, slot);

    ASSERT(key[31] == 0, "slot 0x10000: sub_index = 0");

    /* Verify: tree_index should be 257 = 0x0101 LE */
    uint8_t expected_ti[32] = {0};
    expected_ti[0] = 0x01;  /* 257 & 0xFF */
    expected_ti[1] = 0x01;  /* (257 >> 8) & 0xFF */

    uint8_t key_manual[32];
    verkle_derive_key(key_manual, addr, expected_ti, 0);
    ASSERT(memcmp(key, key_manual, 32) == 0,
           "slot 0x10000 matches tree_index=257, sub_index=0");

    printf("  OK\n\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle Key Derivation Tests ===\n\n");

    pedersen_init();

    test_determinism();
    test_different_addresses();
    test_different_tree_indices();
    test_same_stem_different_suffix();
    test_account_convenience();
    test_storage_key_math();
    test_non_zero_stems();
    test_domain_separation();
    test_header_storage_separation();
    test_large_storage_slot();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
