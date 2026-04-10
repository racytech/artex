/**
 * Test: EIP-161 prune + re-touch across blocks — multiple variants.
 *
 * Reference: finalize_block + compute_root every block (hart_delete atomic with hash)
 * Test:      finalize_block every block, compute_root only at checkpoint
 *
 * Variants:
 *   1. Basic: prune A, re-touch next block
 *   2. Storage: prune A that has storage slots, re-touch
 *   3. Phantom: touch unknown account (never EXISTED), re-touch next block
 *   4. Self-destruct: A self-destructs, re-touch next block
 *   5. Multi-cycle: prune → re-touch → prune → re-touch
 *   6. Same-block: prune and re-touch in same block (should always work)
 *   7. Gap: prune in block 1, skip block 2, re-touch in block 3
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>

static int total_errors = 0;

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

static address_t make_addr(uint8_t seed) {
    address_t a = {0};
    for (int i = 0; i < 20; i++) a.bytes[i] = seed + (uint8_t)i;
    return a;
}

/* Helper: run one block through finalize + optional root + reset */
static hash_t end_block(state_t *st, bool prune, bool compute) {
    state_finalize_block(st, prune);
    hash_t root = {0};
    if (compute)
        root = state_compute_root(st, prune);
    state_reset_block(st);
    return root;
}

/* =========================================================================
 * Test 1: Basic prune + re-touch
 * ========================================================================= */
static void test_basic_prune_retouch(void) {
    printf("\n=== Test 1: Basic prune + re-touch ===\n");
    address_t A = make_addr(0x10);
    address_t B = make_addr(0x20);
    uint256_t bal100 = uint256_from_uint64(100);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal50  = uint256_from_uint64(50);
    uint256_t zero   = UINT256_ZERO;

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &A, &bal100);
    evm_state_mark_existed(ref, &A);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: drain A */
    evm_state_set_balance(ref, &A, &zero);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: re-touch A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &A, &bal100);
    evm_state_mark_existed(tst, &A);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    /* Block 1: drain A — finalize only */
    evm_state_set_balance(tst, &A, &zero);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    /* Block 2: re-touch A — checkpoint */
    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 2: Prune account WITH storage slots
 * ========================================================================= */
static void test_prune_with_storage(void) {
    printf("\n=== Test 2: Prune account with storage ===\n");
    address_t A = make_addr(0x30);
    address_t B = make_addr(0x40);
    uint256_t bal100 = uint256_from_uint64(100);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal50  = uint256_from_uint64(50);
    uint256_t zero   = UINT256_ZERO;
    uint256_t skey   = uint256_from_uint64(1);
    uint256_t sval   = uint256_from_uint64(42);

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &A, &bal100);
    evm_state_set_storage(ref, &A, &skey, &sval);  /* A has storage! */
    evm_state_mark_existed(ref, &A);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: drain A (still has storage, but is "empty" per EIP-161) */
    evm_state_set_balance(ref, &A, &zero);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: re-touch A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &A, &bal100);
    evm_state_set_storage(tst, &A, &skey, &sval);
    evm_state_mark_existed(tst, &A);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    evm_state_set_balance(tst, &A, &zero);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 3: Phantom account (touched but never EXISTED)
 * ========================================================================= */
static void test_phantom_retouch(void) {
    printf("\n=== Test 3: Phantom account re-touch ===\n");
    address_t A = make_addr(0x50);  /* phantom — will be accessed but not funded */
    address_t B = make_addr(0x60);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal50  = uint256_from_uint64(50);

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: touch A (just read balance — creates phantom) */
    evm_state_get_balance(ref, &A);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: fund A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    evm_state_get_balance(tst, &A);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 4: Self-destruct + re-touch
 * ========================================================================= */
static void test_selfdestruct_retouch(void) {
    printf("\n=== Test 4: Self-destruct + re-touch ===\n");
    address_t A = make_addr(0x70);
    address_t B = make_addr(0x80);
    uint256_t bal100 = uint256_from_uint64(100);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal50  = uint256_from_uint64(50);
    uint256_t skey   = uint256_from_uint64(1);
    uint256_t sval   = uint256_from_uint64(99);
    uint8_t code[] = {0x60, 0x00, 0xff};  /* PUSH0 SELFDESTRUCT */

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &A, &bal100);
    evm_state_set_code(ref, &A, code, sizeof(code));
    evm_state_set_storage(ref, &A, &skey, &sval);
    evm_state_mark_existed(ref, &A);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: A self-destructs */
    evm_state_self_destruct(ref, &A);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: send ETH to A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &A, &bal100);
    evm_state_set_code(tst, &A, code, sizeof(code));
    evm_state_set_storage(tst, &A, &skey, &sval);
    evm_state_mark_existed(tst, &A);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    evm_state_self_destruct(tst, &A);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 5: Multi-cycle: prune → re-touch → prune → re-touch
 * ========================================================================= */
static void test_multi_cycle(void) {
    printf("\n=== Test 5: Multi-cycle prune/re-touch ===\n");
    address_t A = make_addr(0x90);
    address_t B = make_addr(0xA0);
    uint256_t bal100 = uint256_from_uint64(100);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal50  = uint256_from_uint64(50);
    uint256_t bal25  = uint256_from_uint64(25);
    uint256_t zero   = UINT256_ZERO;

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &A, &bal100);
    evm_state_mark_existed(ref, &A);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: drain A */
    evm_state_set_balance(ref, &A, &zero);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: re-touch A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 3: drain A again */
    evm_state_set_balance(ref, &A, &zero);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 4: re-touch A again */
    evm_state_set_balance(ref, &A, &bal25);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &A, &bal100);
    evm_state_mark_existed(tst, &A);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    /* All blocks: finalize only */
    evm_state_set_balance(tst, &A, &zero);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &zero);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    /* Block 4: checkpoint */
    evm_state_set_balance(tst, &A, &bal25);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 6: Prune + re-touch with gap (block in between with no A access)
 * ========================================================================= */
static void test_gap_retouch(void) {
    printf("\n=== Test 6: Prune + gap + re-touch ===\n");
    address_t A = make_addr(0xB0);
    address_t B = make_addr(0xC0);
    uint256_t bal100 = uint256_from_uint64(100);
    uint256_t bal200 = uint256_from_uint64(200);
    uint256_t bal300 = uint256_from_uint64(300);
    uint256_t bal50  = uint256_from_uint64(50);
    uint256_t zero   = UINT256_ZERO;

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &A, &bal100);
    evm_state_mark_existed(ref, &A);
    evm_state_set_balance(ref, &B, &bal200);
    evm_state_mark_existed(ref, &B);
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: drain A */
    evm_state_set_balance(ref, &A, &zero);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: only touch B (A not accessed) */
    evm_state_set_balance(ref, &B, &bal300);
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 3: re-touch A */
    evm_state_set_balance(ref, &A, &bal50);
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &A, &bal100);
    evm_state_mark_existed(tst, &A);
    evm_state_set_balance(tst, &B, &bal200);
    evm_state_mark_existed(tst, &B);
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    evm_state_set_balance(tst, &A, &zero);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &B, &bal300);
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    evm_state_set_balance(tst, &A, &bal50);
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);
}

/* =========================================================================
 * Test 7: Many accounts pruned at once (DoS cleanup scenario)
 * ========================================================================= */
static void test_mass_prune_retouch(void) {
    printf("\n=== Test 7: Mass prune + selective re-touch ===\n");
    address_t keeper = make_addr(0xFF);
    uint256_t bal_big = uint256_from_uint64(999999);
    uint256_t bal1    = uint256_from_uint64(1);
    uint256_t zero    = UINT256_ZERO;

    #define N_ACCTS 50

    /* Reference */
    evm_state_t *ref = evm_state_create(NULL);
    state_t *rst = evm_state_get_state(ref);
    evm_state_set_balance(ref, &keeper, &bal_big);
    evm_state_mark_existed(ref, &keeper);
    for (int i = 0; i < N_ACCTS; i++) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(ref, &addr, &bal1);
        evm_state_mark_existed(ref, &addr);
    }
    evm_state_commit(ref);
    state_set_prune_empty(rst, true);
    evm_state_compute_mpt_root(ref, true);
    evm_state_clear_prestate_dirty(ref);

    /* Block 1: drain all N accounts */
    for (int i = 0; i < N_ACCTS; i++) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(ref, &addr, &zero);
    }
    evm_state_commit_tx(ref);
    end_block(rst, true, true);
    evm_state_commit(ref);

    /* Block 2: re-touch only even-numbered accounts */
    for (int i = 0; i < N_ACCTS; i += 2) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(ref, &addr, &bal1);
    }
    evm_state_commit_tx(ref);
    hash_t ref_root = end_block(rst, true, true);

    /* Test */
    evm_state_t *tst = evm_state_create(NULL);
    state_t *tst_st = evm_state_get_state(tst);
    evm_state_set_balance(tst, &keeper, &bal_big);
    evm_state_mark_existed(tst, &keeper);
    for (int i = 0; i < N_ACCTS; i++) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(tst, &addr, &bal1);
        evm_state_mark_existed(tst, &addr);
    }
    evm_state_commit(tst);
    state_set_prune_empty(tst_st, true);
    evm_state_compute_mpt_root(tst, true);
    evm_state_clear_prestate_dirty(tst);

    for (int i = 0; i < N_ACCTS; i++) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(tst, &addr, &zero);
    }
    evm_state_commit_tx(tst);
    end_block(tst_st, true, false);
    evm_state_commit(tst);

    for (int i = 0; i < N_ACCTS; i += 2) {
        address_t addr = make_addr((uint8_t)i);
        evm_state_set_balance(tst, &addr, &bal1);
    }
    evm_state_commit_tx(tst);
    hash_t tst_root = end_block(tst_st, true, true);

    int ok = memcmp(ref_root.bytes, tst_root.bytes, 32) == 0;
    printf("  %s\n", ok ? "PASS" : "FAIL");
    if (!ok) {
        print_hash("expected", ref_root.bytes);
        print_hash("got     ", tst_root.bytes);
        total_errors++;
    }
    evm_state_destroy(ref);
    evm_state_destroy(tst);

    #undef N_ACCTS
}

int main(void) {
    test_basic_prune_retouch();
    test_prune_with_storage();
    test_phantom_retouch();
    test_selfdestruct_retouch();
    test_multi_cycle();
    test_gap_retouch();
    test_mass_prune_retouch();

    printf("\n=== %s (%d errors) ===\n",
           total_errors ? "FAIL" : "ALL PASS", total_errors);
    return total_errors ? 1 : 0;
}
