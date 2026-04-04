/**
 * Test: verify whether state_finalize_block causes stale storage roots.
 *
 * Scenario:
 *   Block 1: modify account A's storage
 *   → state_finalize_block (non-checkpoint, clears flags + blk_dirty)
 *   Block 2: modify account B's balance (different account)
 *   → state_compute_root_ex(compute_hash=true) (checkpoint)
 *
 * Compare with reference that calls state_compute_root every block.
 * If they differ, the bug is confirmed.
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>

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

int main(void) {
    int errors = 0;

    /* ====== Reference: compute root every block ====== */
    printf("=== Reference (per-block root) ===\n");
    evm_state_t *ref = evm_state_create(NULL);
    state_t *ref_st = evm_state_get_state(ref);

    address_t a1 = make_addr(0x10);
    address_t a2 = make_addr(0x20);

    /* Setup: a1 with balance + storage, a2 with balance */
    evm_state_set_nonce(ref, &a1, 1);
    uint256_t bal1 = uint256_from_uint64(1000);
    evm_state_set_balance(ref, &a1, &bal1);
    evm_state_mark_existed(ref, &a1);
    uint256_t skey1 = uint256_from_uint64(1);
    uint256_t sval1 = uint256_from_uint64(100);
    evm_state_set_storage(ref, &a1, &skey1, &sval1);

    evm_state_set_nonce(ref, &a2, 1);
    uint256_t bal2 = uint256_from_uint64(2000);
    evm_state_set_balance(ref, &a2, &bal2);
    evm_state_mark_existed(ref, &a2);

    evm_state_commit(ref);
    hash_t ref_root0 = evm_state_compute_mpt_root(ref, false);
    evm_state_clear_prestate_dirty(ref);
    print_hash("genesis root", ref_root0.bytes);

    /* Block 1: modify a1 storage */
    printf("\nBlock 1: modify a1 storage\n");
    uint256_t sval2 = uint256_from_uint64(999);
    evm_state_set_storage(ref, &a1, &skey1, &sval2);
    evm_state_commit_tx(ref);
    hash_t ref_root1 = evm_state_compute_mpt_root(ref, false);
    evm_state_commit(ref);
    print_hash("ref root1", ref_root1.bytes);

    /* Block 2: modify a2 balance */
    printf("\nBlock 2: modify a2 balance\n");
    uint256_t bal3 = uint256_from_uint64(3000);
    evm_state_set_balance(ref, &a2, &bal3);
    evm_state_commit_tx(ref);
    hash_t ref_root2 = evm_state_compute_mpt_root(ref, false);
    print_hash("ref root2", ref_root2.bytes);

    evm_state_destroy(ref);

    /* ====== Test: use finalize_block for block 1, compute_root for block 2 ====== */
    printf("\n=== Test (finalize_block + checkpoint) ===\n");
    evm_state_t *test = evm_state_create(NULL);
    state_t *test_st = evm_state_get_state(test);

    /* Same setup */
    evm_state_set_nonce(test, &a1, 1);
    evm_state_set_balance(test, &a1, &bal1);
    evm_state_mark_existed(test, &a1);
    evm_state_set_storage(test, &a1, &skey1, &sval1);

    evm_state_set_nonce(test, &a2, 1);
    evm_state_set_balance(test, &a2, &bal2);
    evm_state_mark_existed(test, &a2);

    evm_state_commit(test);
    hash_t test_root0 = evm_state_compute_mpt_root(test, false);
    evm_state_clear_prestate_dirty(test);

    int genesis_match = memcmp(ref_root0.bytes, test_root0.bytes, 32) == 0;
    printf("genesis match: %s\n", genesis_match ? "YES" : "NO");
    if (!genesis_match) errors++;

    /* Block 1: modify a1 storage — use finalize_block (skip hash) */
    printf("\nBlock 1: modify a1 storage (finalize only)\n");
    evm_state_set_storage(test, &a1, &skey1, &sval2);
    evm_state_commit_tx(test);
    state_finalize_block(test_st, false);
    evm_state_commit(test);

    /* Block 2: modify a2 balance — checkpoint (compute hash) */
    printf("Block 2: modify a2 balance (checkpoint)\n");
    evm_state_set_balance(test, &a2, &bal3);
    evm_state_commit_tx(test);
    hash_t test_root2 = evm_state_compute_mpt_root(test, false);
    print_hash("test root2", test_root2.bytes);

    int root2_match = memcmp(ref_root2.bytes, test_root2.bytes, 32) == 0;
    printf("\nroot2 match: %s\n", root2_match ? "YES" : "NO");
    if (!root2_match) {
        printf("  BUG CONFIRMED: finalize_block causes stale storage root\n");
        print_hash("  expected", ref_root2.bytes);
        print_hash("  got     ", test_root2.bytes);
        errors++;
    }

    evm_state_destroy(test);

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
