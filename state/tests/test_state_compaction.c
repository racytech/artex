/**
 * Test that compacting the accounts vector + rebuilding acct_index
 * produces the same state root as the original layout.
 *
 * Flow:
 *   1. Create state with several accounts (some will be pruned)
 *   2. Execute modifications + commit_tx
 *   3. Compute root (root_before)
 *   4. Compact: rebuild vector + acct_index with only EXISTED accounts
 *   5. Compute root again (root_after)
 *   6. Verify root_before == root_after
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"

/* Not in public header — internal access for testing */
extern state_t *evm_state_get_state(evm_state_t *es);
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void make_addr(uint8_t seed, address_t *out) {
    memset(out->bytes, 0, 20);
    out->bytes[19] = seed;
}

static void print_hash(const char *label, const hash_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

int main(void) {
    evm_state_t *es = evm_state_create(NULL);
    if (!es) { fprintf(stderr, "FAIL: create\n"); return 1; }
    evm_state_set_prune_empty(es, true);

    /* Create 10 accounts */
    address_t addrs[10];
    for (int i = 0; i < 10; i++) {
        make_addr((uint8_t)(i + 1), &addrs[i]);
        uint256_t bal = uint256_from_uint64((uint64_t)(i + 1) * 1000);
        evm_state_set_balance(es, &addrs[i], &bal);
        if (i < 5) {
            /* First 5 get code */
            uint8_t code[] = {0x60, 0x00, 0x55};  /* PUSH 0, SSTORE */
            evm_state_set_code(es, &addrs[i], code, 3);
        }
        evm_state_mark_existed(es, &addrs[i]);
    }

    /* Commit and compute genesis root */
    evm_state_commit(es);
    hash_t genesis = evm_state_compute_mpt_root(es, true);
    print_hash("genesis", &genesis);

    evm_state_clear_prestate_dirty(es);

    /* Simulate a transaction:
     * - Touch addrs[7,8,9] with zero balance (phantoms after prune)
     * - Self-destruct addr[2]
     * - Modify addr[0] balance */
    uint256_t zero = UINT256_ZERO;
    evm_state_add_balance(es, &addrs[7], &zero);  /* touch - stays */
    evm_state_add_balance(es, &addrs[8], &zero);  /* touch - stays */
    evm_state_add_balance(es, &addrs[9], &zero);  /* touch - stays */

    /* Self-destruct addr[2] */
    evm_state_self_destruct(es, &addrs[2]);

    /* Modify addr[0] */
    uint256_t new_bal = uint256_from_uint64(99999);
    evm_state_set_balance(es, &addrs[0], &new_bal);

    /* Also create some phantom addresses (never existed) */
    address_t phantom1, phantom2;
    make_addr(0xAA, &phantom1);
    make_addr(0xBB, &phantom2);
    evm_state_add_balance(es, &phantom1, &zero);
    evm_state_add_balance(es, &phantom2, &zero);

    /* Commit tx */
    evm_state_commit_tx(es);

    /* Compute root */
    hash_t root_before = evm_state_compute_mpt_root(es, true);
    print_hash("before compaction", &root_before);

    /* Now compact: get the internal state and rebuild */
    state_t *st = evm_state_get_state(es);
    if (!st) { fprintf(stderr, "FAIL: get_state\n"); return 1; }

    /* Call the compaction function */
    printf("  calling state_compact (st=%p)...\n", (void*)st);
    fflush(stdout);
    state_compact(st);
    printf("  compaction done\n");

    /* Compute root again — should match */
    hash_t root_after = evm_state_compute_mpt_root(es, true);
    print_hash("after compaction", &root_after);

    int match = memcmp(root_before.bytes, root_after.bytes, 32) == 0;
    printf("\n  roots match: %s\n", match ? "YES" : "NO");

    /* Verify accounts are still accessible */
    uint64_t n0 = evm_state_get_nonce(es, &addrs[0]);
    uint256_t b0 = evm_state_get_balance(es, &addrs[0]);
    uint256_t b5 = evm_state_get_balance(es, &addrs[5]);
    printf("  addr[0] nonce=%lu bal=%lu\n", n0, uint256_to_uint64(&b0));
    printf("  addr[5] bal=%lu\n", uint256_to_uint64(&b5));

    evm_state_destroy(es);
    return match ? 0 : 1;
}
