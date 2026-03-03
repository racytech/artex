#include "verkle_state.h"
#include "verkle_key.h"
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

/* =========================================================================
 * Phase 1: Create/Destroy
 * ========================================================================= */

static void test_lifecycle(void) {
    printf("Phase 1: Create/destroy\n");

    verkle_state_t *vs = verkle_state_create();
    ASSERT(vs != NULL, "create returns non-NULL");

    uint8_t hash[32];
    verkle_state_root_hash(vs, hash);
    uint8_t zero[32] = {0};
    ASSERT(memcmp(hash, zero, 32) == 0,
           "empty state root hash is all zeros");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 2: Nonce Round-Trip
 * ========================================================================= */

static void test_nonce(void) {
    printf("Phase 2: Nonce round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0x01, 0x02, 0x03};

    ASSERT(verkle_state_get_nonce(vs, addr) == 0,
           "default nonce is 0");

    verkle_state_set_nonce(vs, addr, 42);
    ASSERT(verkle_state_get_nonce(vs, addr) == 42,
           "nonce round-trips 42");

    verkle_state_set_nonce(vs, addr, 0xFFFFFFFFFFFFFFFFULL);
    ASSERT(verkle_state_get_nonce(vs, addr) == 0xFFFFFFFFFFFFFFFFULL,
           "nonce round-trips max uint64");

    verkle_state_set_nonce(vs, addr, 0);
    ASSERT(verkle_state_get_nonce(vs, addr) == 0,
           "nonce round-trips back to 0");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 3: Balance Round-Trip
 * ========================================================================= */

static void test_balance(void) {
    printf("Phase 3: Balance round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0x10, 0x20};

    uint8_t zero_bal[32] = {0};
    uint8_t got[32];
    verkle_state_get_balance(vs, addr, got);
    ASSERT(memcmp(got, zero_bal, 32) == 0,
           "default balance is zero");

    /* Set a large balance (uses upper bytes too) */
    uint8_t balance[32];
    for (int i = 0; i < 32; i++) balance[i] = (uint8_t)(i + 1);
    verkle_state_set_balance(vs, addr, balance);
    verkle_state_get_balance(vs, addr, got);
    ASSERT(memcmp(got, balance, 32) == 0,
           "balance round-trips full 32 bytes");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 4: Code Hash Round-Trip
 * ========================================================================= */

static void test_code_hash(void) {
    printf("Phase 4: Code hash round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xAA};

    uint8_t got[32];
    verkle_state_get_code_hash(vs, addr, got);
    uint8_t zero[32] = {0};
    ASSERT(memcmp(got, zero, 32) == 0,
           "default code hash is zero");

    uint8_t hash[32];
    for (int i = 0; i < 32; i++) hash[i] = (uint8_t)(0xFF - i);
    verkle_state_set_code_hash(vs, addr, hash);
    verkle_state_get_code_hash(vs, addr, got);
    ASSERT(memcmp(got, hash, 32) == 0,
           "code hash round-trips");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 5: Code Size Round-Trip
 * ========================================================================= */

static void test_code_size(void) {
    printf("Phase 5: Code size round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xBB};

    ASSERT(verkle_state_get_code_size(vs, addr) == 0,
           "default code size is 0");

    verkle_state_set_code_size(vs, addr, 24576);
    ASSERT(verkle_state_get_code_size(vs, addr) == 24576,
           "code size round-trips 24576");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Version Round-Trip
 * ========================================================================= */

static void test_version(void) {
    printf("Phase 6: Version round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xCC};

    ASSERT(verkle_state_get_version(vs, addr) == 0,
           "default version is 0");

    verkle_state_set_version(vs, addr, 1);
    ASSERT(verkle_state_get_version(vs, addr) == 1,
           "version round-trips 1");

    verkle_state_set_version(vs, addr, 255);
    ASSERT(verkle_state_get_version(vs, addr) == 255,
           "version round-trips 255");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 7: Storage Round-Trip
 * ========================================================================= */

static void test_storage(void) {
    printf("Phase 7: Storage round-trip\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xDD};

    uint8_t slot0[32] = {0};
    uint8_t slot1[32] = {0}; slot1[0] = 1;
    uint8_t slot256[32] = {0}; slot256[1] = 1;  /* 256 in LE */

    uint8_t got[32], zero[32] = {0};

    /* Default storage is zero */
    verkle_state_get_storage(vs, addr, slot0, got);
    ASSERT(memcmp(got, zero, 32) == 0, "default storage is zero");

    /* Set and get slot 0 */
    uint8_t val1[32] = {0}; val1[0] = 42;
    verkle_state_set_storage(vs, addr, slot0, val1);
    verkle_state_get_storage(vs, addr, slot0, got);
    ASSERT(memcmp(got, val1, 32) == 0, "storage slot 0 round-trips");

    /* Set and get slot 1 */
    uint8_t val2[32] = {0}; val2[0] = 99;
    verkle_state_set_storage(vs, addr, slot1, val2);
    verkle_state_get_storage(vs, addr, slot1, got);
    ASSERT(memcmp(got, val2, 32) == 0, "storage slot 1 round-trips");

    /* Set and get slot 256 (different tree_index) */
    uint8_t val3[32] = {0}; val3[0] = 77;
    verkle_state_set_storage(vs, addr, slot256, val3);
    verkle_state_get_storage(vs, addr, slot256, got);
    ASSERT(memcmp(got, val3, 32) == 0, "storage slot 256 round-trips");

    /* Slot 0 still intact */
    verkle_state_get_storage(vs, addr, slot0, got);
    ASSERT(memcmp(got, val1, 32) == 0, "slot 0 unaffected by other writes");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 8: Default Values
 * ========================================================================= */

static void test_defaults(void) {
    printf("Phase 8: Default values\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xEE};
    uint8_t got[32], zero[32] = {0};

    ASSERT(verkle_state_get_version(vs, addr) == 0, "default version");
    ASSERT(verkle_state_get_nonce(vs, addr) == 0, "default nonce");

    verkle_state_get_balance(vs, addr, got);
    ASSERT(memcmp(got, zero, 32) == 0, "default balance");

    verkle_state_get_code_hash(vs, addr, got);
    ASSERT(memcmp(got, zero, 32) == 0, "default code hash");

    ASSERT(verkle_state_get_code_size(vs, addr) == 0, "default code size");

    uint8_t slot[32] = {0};
    verkle_state_get_storage(vs, addr, slot, got);
    ASSERT(memcmp(got, zero, 32) == 0, "default storage");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 9: Multiple Accounts
 * ========================================================================= */

static void test_multiple_accounts(void) {
    printf("Phase 9: Multiple accounts\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr1[20] = {0x01};
    uint8_t addr2[20] = {0x02};

    verkle_state_set_nonce(vs, addr1, 100);
    verkle_state_set_nonce(vs, addr2, 200);

    ASSERT(verkle_state_get_nonce(vs, addr1) == 100,
           "account 1 nonce correct");
    ASSERT(verkle_state_get_nonce(vs, addr2) == 200,
           "account 2 nonce correct");

    /* Setting one doesn't affect the other */
    verkle_state_set_nonce(vs, addr1, 150);
    ASSERT(verkle_state_get_nonce(vs, addr1) == 150,
           "account 1 nonce updated");
    ASSERT(verkle_state_get_nonce(vs, addr2) == 200,
           "account 2 nonce unchanged");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 10: Account Existence
 * ========================================================================= */

static void test_existence(void) {
    printf("Phase 10: Account existence\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0x50};

    ASSERT(!verkle_state_exists(vs, addr),
           "account does not exist before any set");

    verkle_state_set_nonce(vs, addr, 1);
    ASSERT(verkle_state_exists(vs, addr),
           "account exists after setting nonce");

    /* Another address still doesn't exist */
    uint8_t addr2[20] = {0x60};
    ASSERT(!verkle_state_exists(vs, addr2),
           "other account does not exist");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 11: Root Changes
 * ========================================================================= */

static void test_root_changes(void) {
    printf("Phase 11: Root changes after modifications\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0x70};

    uint8_t hash0[32], hash1[32], hash2[32], hash3[32];

    verkle_state_root_hash(vs, hash0);

    verkle_state_set_nonce(vs, addr, 1);
    verkle_state_root_hash(vs, hash1);
    ASSERT(memcmp(hash0, hash1, 32) != 0,
           "root changes after nonce set");

    uint8_t bal[32] = {0}; bal[0] = 100;
    verkle_state_set_balance(vs, addr, bal);
    verkle_state_root_hash(vs, hash2);
    ASSERT(memcmp(hash1, hash2, 32) != 0,
           "root changes after balance set");

    uint8_t slot[32] = {0};
    uint8_t sval[32] = {0}; sval[0] = 42;
    verkle_state_set_storage(vs, addr, slot, sval);
    verkle_state_root_hash(vs, hash3);
    ASSERT(memcmp(hash2, hash3, 32) != 0,
           "root changes after storage set");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 12: End-to-End
 * ========================================================================= */

static void test_end_to_end(void) {
    printf("Phase 12: End-to-end\n");

    verkle_state_t *vs = verkle_state_create();

    uint8_t alice[20] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
                         0x11, 0x22, 0x33, 0x44, 0x55,
                         0x66, 0x77, 0x88, 0x99, 0x00,
                         0xAB, 0xCD, 0xEF, 0x01, 0x23};

    /* Set all account fields */
    verkle_state_set_version(vs, alice, 1);
    verkle_state_set_nonce(vs, alice, 7);

    uint8_t balance[32] = {0};
    balance[0] = 0x00; balance[1] = 0xE1; balance[2] = 0xF5; balance[3] = 0x05;
    /* 100000000 in LE = 0x05F5E100 */
    verkle_state_set_balance(vs, alice, balance);

    uint8_t code_hash[32];
    for (int i = 0; i < 32; i++) code_hash[i] = (uint8_t)(0xC0 + i);
    verkle_state_set_code_hash(vs, alice, code_hash);

    verkle_state_set_code_size(vs, alice, 1024);

    /* Set a few storage slots */
    uint8_t slot0[32] = {0};
    uint8_t val0[32] = {0}; val0[0] = 0xFF;
    verkle_state_set_storage(vs, alice, slot0, val0);

    uint8_t slot1[32] = {0}; slot1[0] = 1;
    uint8_t val1[32] = {0}; val1[0] = 0xAB;
    verkle_state_set_storage(vs, alice, slot1, val1);

    /* Capture root */
    uint8_t root[32];
    verkle_state_root_hash(vs, root);
    uint8_t zero[32] = {0};
    ASSERT(memcmp(root, zero, 32) != 0, "root is non-zero");

    /* Verify all fields */
    ASSERT(verkle_state_get_version(vs, alice) == 1, "version=1");
    ASSERT(verkle_state_get_nonce(vs, alice) == 7, "nonce=7");

    uint8_t got_bal[32];
    verkle_state_get_balance(vs, alice, got_bal);
    ASSERT(memcmp(got_bal, balance, 32) == 0, "balance matches");

    uint8_t got_ch[32];
    verkle_state_get_code_hash(vs, alice, got_ch);
    ASSERT(memcmp(got_ch, code_hash, 32) == 0, "code hash matches");

    ASSERT(verkle_state_get_code_size(vs, alice) == 1024, "code_size=1024");

    uint8_t got_s[32];
    verkle_state_get_storage(vs, alice, slot0, got_s);
    ASSERT(memcmp(got_s, val0, 32) == 0, "storage slot 0 matches");

    verkle_state_get_storage(vs, alice, slot1, got_s);
    ASSERT(memcmp(got_s, val1, 32) == 0, "storage slot 1 matches");

    ASSERT(verkle_state_exists(vs, alice), "alice exists");

    /* Build a second state with same ops, verify same root */
    verkle_state_t *vs2 = verkle_state_create();
    verkle_state_set_version(vs2, alice, 1);
    verkle_state_set_nonce(vs2, alice, 7);
    verkle_state_set_balance(vs2, alice, balance);
    verkle_state_set_code_hash(vs2, alice, code_hash);
    verkle_state_set_code_size(vs2, alice, 1024);
    verkle_state_set_storage(vs2, alice, slot0, val0);
    verkle_state_set_storage(vs2, alice, slot1, val1);

    uint8_t root2[32];
    verkle_state_root_hash(vs2, root2);
    ASSERT(memcmp(root, root2, 32) == 0,
           "same operations produce same root");

    verkle_state_destroy(vs);
    verkle_state_destroy(vs2);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 13: Code Round-Trip (Small)
 * ========================================================================= */

static void test_code_small(void) {
    printf("Phase 13: Code round-trip (small, 50 bytes)\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xA1};

    /* 50-byte bytecode (not aligned to 32) */
    uint8_t code[50];
    for (int i = 0; i < 50; i++) code[i] = (uint8_t)(i + 1);

    bool ok = verkle_state_set_code(vs, addr, code, 50);
    ASSERT(ok, "set_code succeeds");

    ASSERT(verkle_state_get_code_size(vs, addr) == 50,
           "code_size set to 50");

    uint8_t got[50];
    memset(got, 0, 50);
    uint64_t len = verkle_state_get_code(vs, addr, got, 50);
    ASSERT(len == 50, "get_code returns 50");
    ASSERT(memcmp(got, code, 50) == 0, "code bytes match exactly");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 14: Code Round-Trip (Aligned)
 * ========================================================================= */

static void test_code_aligned(void) {
    printf("Phase 14: Code round-trip (aligned, 64 bytes = 2 chunks)\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xA2};

    uint8_t code[64];
    for (int i = 0; i < 64; i++) code[i] = (uint8_t)(0xFF - i);

    verkle_state_set_code(vs, addr, code, 64);

    uint8_t got[64];
    uint64_t len = verkle_state_get_code(vs, addr, got, 64);
    ASSERT(len == 64, "get_code returns 64");
    ASSERT(memcmp(got, code, 64) == 0, "aligned code round-trips");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 15: Code Round-Trip (Large)
 * ========================================================================= */

static void test_code_large(void) {
    printf("Phase 15: Code round-trip (large, 1000 bytes)\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xA3};

    uint8_t code[1000];
    for (int i = 0; i < 1000; i++) code[i] = (uint8_t)(i * 7 + 3);

    verkle_state_set_code(vs, addr, code, 1000);

    ASSERT(verkle_state_get_code_size(vs, addr) == 1000,
           "code_size set to 1000");

    /* Read full */
    uint8_t got[1000];
    memset(got, 0, 1000);
    uint64_t len = verkle_state_get_code(vs, addr, got, 1000);
    ASSERT(len == 1000, "get_code returns 1000");
    ASSERT(memcmp(got, code, 1000) == 0, "1000 bytes match");

    /* Read partial (max_len < code_size) */
    uint8_t partial[100];
    len = verkle_state_get_code(vs, addr, partial, 100);
    ASSERT(len == 100, "partial read returns 100");
    ASSERT(memcmp(partial, code, 100) == 0, "partial bytes match");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 16: Code + Header Integration
 * ========================================================================= */

static void test_code_header_integration(void) {
    printf("Phase 16: Code + header integration\n");

    verkle_state_t *vs = verkle_state_create();
    uint8_t addr[20] = {0xA4};

    /* Set some header fields first */
    verkle_state_set_nonce(vs, addr, 42);
    uint8_t bal[32] = {0}; bal[0] = 100;
    verkle_state_set_balance(vs, addr, bal);

    /* Set code */
    uint8_t code[80];
    for (int i = 0; i < 80; i++) code[i] = (uint8_t)i;
    verkle_state_set_code(vs, addr, code, 80);

    /* code_size should be set */
    ASSERT(verkle_state_get_code_size(vs, addr) == 80,
           "code_size reflects set_code");

    /* code_hash should NOT be set (caller's responsibility) */
    uint8_t hash[32], zero[32] = {0};
    verkle_state_get_code_hash(vs, addr, hash);
    ASSERT(memcmp(hash, zero, 32) == 0,
           "code_hash not set by set_code");

    /* Other header fields unaffected */
    ASSERT(verkle_state_get_nonce(vs, addr) == 42,
           "nonce unaffected by set_code");
    uint8_t got_bal[32];
    verkle_state_get_balance(vs, addr, got_bal);
    ASSERT(memcmp(got_bal, bal, 32) == 0,
           "balance unaffected by set_code");

    /* Code still readable */
    uint8_t got[80];
    uint64_t len = verkle_state_get_code(vs, addr, got, 80);
    ASSERT(len == 80, "code readable after header ops");
    ASSERT(memcmp(got, code, 80) == 0, "code bytes intact");

    verkle_state_destroy(vs);
    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 17: Code Key Domain Separation
 * ========================================================================= */

static void test_code_domain_separation(void) {
    printf("Phase 17: Code key domain separation\n");

    uint8_t addr[20] = {0xA5};

    /* Code chunk 0 key (domain 3) */
    uint8_t code_key[32];
    verkle_code_chunk_key(code_key, addr, 0);

    /* Storage slot 0 key (domain 2) */
    uint8_t storage_key[32];
    uint8_t slot[32] = {0};
    verkle_storage_key(storage_key, addr, slot);

    /* Account header key (domain 2, tree_index=0) */
    uint8_t header_key[32];
    verkle_account_version_key(header_key, addr);

    /* All stems must differ (different domains / tree_indices) */
    ASSERT(memcmp(code_key, storage_key, 31) != 0,
           "code stem differs from storage stem");
    ASSERT(memcmp(code_key, header_key, 31) != 0,
           "code stem differs from header stem");

    /* Two code chunks in the same group share a stem */
    uint8_t code_key2[32];
    verkle_code_chunk_key(code_key2, addr, 1);
    ASSERT(memcmp(code_key, code_key2, 31) == 0,
           "chunks 0 and 1 share stem (same group)");
    ASSERT(code_key[31] != code_key2[31],
           "chunks 0 and 1 differ in suffix");

    /* Chunk 256 is in a different group → different stem */
    uint8_t code_key256[32];
    verkle_code_chunk_key(code_key256, addr, 256);
    ASSERT(memcmp(code_key, code_key256, 31) != 0,
           "chunk 0 and chunk 256 have different stems");

    printf("  OK\n\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Verkle State Tests ===\n\n");

    test_lifecycle();
    test_nonce();
    test_balance();
    test_code_hash();
    test_code_size();
    test_version();
    test_storage();
    test_defaults();
    test_multiple_accounts();
    test_existence();
    test_root_changes();
    test_end_to_end();
    test_code_small();
    test_code_aligned();
    test_code_large();
    test_code_header_integration();
    test_code_domain_separation();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
