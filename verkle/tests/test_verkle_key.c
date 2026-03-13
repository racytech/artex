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
    printf("Phase 5: Account convenience keys (EIP-6800)\n");

    uint8_t addr[20];
    for (int i = 0; i < 20; i++) addr[i] = (uint8_t)(i + 1);

    uint8_t key_bd[32], key_ch[32];
    verkle_account_basic_data_key(key_bd, addr);
    verkle_account_code_hash_key(key_ch, addr);

    /* Both should share the same stem (tree_index=0 for all) */
    ASSERT(memcmp(key_bd, key_ch, 31) == 0, "basic_data/code_hash share stem");

    /* Check suffix bytes */
    ASSERT(key_bd[31] == VERKLE_BASIC_DATA_SUFFIX, "basic_data suffix = 0");
    ASSERT(key_ch[31] == VERKLE_CODE_HASH_SUFFIX, "code_hash suffix = 1");

    /* Verify these match manual derivation */
    uint8_t key_manual[32];
    uint8_t zero_ti[32] = {0};
    verkle_derive_key(key_manual, addr, zero_ti, VERKLE_BASIC_DATA_SUFFIX);
    ASSERT(memcmp(key_bd, key_manual, 32) == 0,
           "basic_data_key matches manual derive with tree_index=0, suffix=0");

    verkle_derive_key(key_manual, addr, zero_ti, VERKLE_CODE_HASH_SUFFIX);
    ASSERT(memcmp(key_ch, key_manual, 32) == 0,
           "code_hash_key matches manual derive with tree_index=0, suffix=1");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Storage Key Math
 * ========================================================================= */

static void test_storage_key_math(void) {
    printf("Phase 6: Storage key math (EIP-6800)\n");

    uint8_t addr[20] = {0xDE, 0xAD};
    uint8_t zero_ti[32] = {0};

    /* slot=0 (header storage): tree_index=0, suffix=64+0=64 */
    uint8_t slot0[32] = {0};
    uint8_t key0[32];
    verkle_storage_key(key0, addr, slot0);
    ASSERT(key0[31] == VERKLE_HEADER_STORAGE_OFFSET,
           "slot 0: suffix = 64");

    /* Verify stem matches header stem (tree_index=0) */
    uint8_t key_manual[32];
    verkle_derive_key(key_manual, addr, zero_ti, VERKLE_HEADER_STORAGE_OFFSET);
    ASSERT(memcmp(key0, key_manual, 32) == 0,
           "slot 0 matches tree_index=0, suffix=64");

    /* slot=63 (last header storage): suffix=64+63=127 */
    uint8_t slot63[32] = {0};
    slot63[0] = 63;
    uint8_t key63[32];
    verkle_storage_key(key63, addr, slot63);
    ASSERT(key63[31] == 127, "slot 63: suffix = 127");
    ASSERT(memcmp(key0, key63, 31) == 0,
           "slots 0 and 63 share header stem");

    /* slot=64 (first main storage): MAIN_STORAGE_OFFSET + 64
     * tree_index = (2^248 + 64) >> 8 = 2^240
     * sub_index = 64 & 0xFF = 64 */
    uint8_t slot64[32] = {0};
    slot64[0] = 64;
    uint8_t key64[32];
    verkle_storage_key(key64, addr, slot64);
    ASSERT(key64[31] == 64, "slot 64: sub_index = 64");
    ASSERT(memcmp(key0, key64, 31) != 0,
           "slot 64 has different stem from header slots");

    /* Verify slot 64: tree_index = 2^240 → byte[30] = 1 in LE */
    uint8_t ti_main[32] = {0};
    ti_main[30] = 1;
    verkle_derive_key(key_manual, addr, ti_main, 64);
    ASSERT(memcmp(key64, key_manual, 32) == 0,
           "slot 64 matches tree_index=2^240, sub_index=64");

    /* slot=255 (main storage): sub_index=255, same tree_index as slot 64 */
    uint8_t slot255[32] = {0};
    slot255[0] = 255;
    uint8_t key255[32];
    verkle_storage_key(key255, addr, slot255);
    ASSERT(key255[31] == 255, "slot 255: sub_index = 255");
    ASSERT(memcmp(key64, key255, 31) == 0,
           "slots 64 and 255 share stem");

    /* slot=256: sub_index=0, tree_index = 2^240 + 1 */
    uint8_t slot256[32] = {0};
    slot256[0] = 0;
    slot256[1] = 1;
    uint8_t key256[32];
    verkle_storage_key(key256, addr, slot256);
    ASSERT(key256[31] == 0, "slot 256: sub_index = 0");
    ASSERT(memcmp(key64, key256, 31) != 0,
           "slot 256 has different stem from slot 64");

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
    printf("Phase 9: Header storage shares stem with account header\n");

    uint8_t addr[20];
    for (int i = 0; i < 20; i++) addr[i] = (uint8_t)(0x50 + i);

    /* Account header stem (tree_index=0, suffix=0) */
    uint8_t header_key[32];
    verkle_account_basic_data_key(header_key, addr);

    /* Header storage slot 0 (tree_index=0, suffix=64) — shares stem! */
    uint8_t slot0[32] = {0};
    uint8_t storage_key0[32];
    verkle_storage_key(storage_key0, addr, slot0);

    /* In EIP-6800, header storage slots 0-63 share the same stem */
    ASSERT(memcmp(header_key, storage_key0, 31) == 0,
           "header storage slot 0 shares stem with account header");
    ASSERT(header_key[31] == VERKLE_BASIC_DATA_SUFFIX, "header suffix=0");
    ASSERT(storage_key0[31] == VERKLE_HEADER_STORAGE_OFFSET, "storage slot 0 suffix=64");

    /* Main storage slot 64 has different stem */
    uint8_t slot64[32] = {0};
    slot64[0] = 64;
    uint8_t storage_key64[32];
    verkle_storage_key(storage_key64, addr, slot64);
    ASSERT(memcmp(header_key, storage_key64, 31) != 0,
           "main storage slot 64 has different stem from header");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 10: Large Storage Slot
 * ========================================================================= */

static void test_large_storage_slot(void) {
    printf("Phase 10: Large storage slot (EIP-6800)\n");

    uint8_t addr[20] = {0x77};

    /* slot = 0x10000 (65536, main storage since >= 64)
     * pos = MAIN_STORAGE_OFFSET + 65536 = 2^248 + 65536
     * sub_index = 65536 & 0xFF = 0
     * tree_index = (2^248 + 65536) >> 8 = 2^240 + 256
     * In LE: tree_index[0]=0, tree_index[1]=1, ..., tree_index[30]=1 */
    uint8_t slot[32] = {0};
    slot[0] = 0x00;  /* 65536 & 0xFF = 0 */
    slot[1] = 0x00;  /* (65536 >> 8) & 0xFF = 0 */
    slot[2] = 0x01;  /* (65536 >> 16) & 0xFF = 1 */

    uint8_t key[32];
    verkle_storage_key(key, addr, slot);

    ASSERT(key[31] == 0, "slot 0x10000: sub_index = 0");

    /* tree_index = slot>>8 + 2^240
     * slot>>8 = 256 → byte[0]=0, byte[1]=1
     * 2^240 → byte[30]=1
     * Result: byte[1]=1, byte[30]=1, rest zero */
    uint8_t expected_ti[32] = {0};
    expected_ti[1] = 0x01;   /* 256 from slot>>8 */
    expected_ti[30] = 0x01;  /* 2^240 from MAIN_STORAGE_OFFSET */

    uint8_t key_manual[32];
    verkle_derive_key(key_manual, addr, expected_ti, 0);
    ASSERT(memcmp(key, key_manual, 32) == 0,
           "slot 0x10000 matches expected tree_index");

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
