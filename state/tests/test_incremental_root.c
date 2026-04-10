/**
 * Test: incremental root computation vs full recomputation.
 *
 * Simulates --validate-every-256 pattern:
 *   1. Build initial state, compute root (caches hashes)
 *   2. Modify accounts over N "blocks" with finalize_block
 *   3. Checkpoint: compute_root — incremental
 *   4. Compare with full recomputation (rebuild harts from scratch)
 *
 * If they differ, dirty flag propagation is broken.
 */
#include "state.h"
#include "evm_state.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

static void print_hash(const char *label, const uint8_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

static address_t make_addr(uint32_t seed) {
    address_t a = {0};
    uint8_t buf[32] = {0};
    memcpy(buf, &seed, 4);
    hash_t h = hash_keccak256(buf, 32);
    memcpy(a.bytes, h.bytes, 20);
    return a;
}

/* Simulate one block: modify some accounts, commit tx, then finalize */
static void sim_block(evm_state_t *es, state_t *st, int block_num,
                       int n_mods, bool compute_hash) {
    state_begin_block(st, (uint64_t)block_num);
    state_set_prune_empty(st, block_num >= 10); /* enable pruning after block 10 */

    for (int i = 0; i < n_mods; i++) {
        /* Pick an account deterministically */
        uint32_t idx = (uint32_t)((block_num * 137 + i * 31) % 500);
        address_t addr = make_addr(idx);

        /* Modify balance */
        uint256_t bal = uint256_from_uint64((uint64_t)(block_num * 1000 + i));
        evm_state_set_balance(es, &addr, &bal);
        evm_state_mark_existed(es, &addr);

        /* Some accounts get storage modifications */
        if (i % 3 == 0) {
            uint256_t skey = uint256_from_uint64((uint64_t)(i + 1));
            uint256_t sval = uint256_from_uint64((uint64_t)(block_num * 100 + i));
            evm_state_set_storage(es, &addr, &skey, &sval);
        }

        /* Some accounts get storage deleted */
        if (i % 7 == 0 && block_num > 5) {
            uint256_t skey = uint256_from_uint64((uint64_t)(i + 1));
            uint256_t sval = UINT256_ZERO;
            evm_state_set_storage(es, &addr, &skey, &sval);
        }
    }

    evm_state_commit_tx(es);

    /* Simulate block_execute: finalize every block, compute root at checkpoint */
    bool prune = (block_num >= 10);
    state_finalize_block(st, prune);
    if (compute_hash)
        state_compute_root(st, prune);
    state_reset_block(st);
}

/* Full recomputation: save state, reload into fresh instance, compute root */
static hash_t full_recompute(evm_state_t *es, state_t *st, bool prune) {
    /* Save to temp file */
    const char *tmp = "/tmp/test_incremental_root.bin";
    hash_t dummy = {0};
    state_save(st, tmp, &dummy);

    /* Load into fresh state */
    evm_state_t *fresh = evm_state_create(NULL);
    state_t *fresh_st = evm_state_get_state(fresh);
    state_load(fresh_st, tmp, NULL);

    /* Mark all accounts dirty for full recomputation */
    /* Loading rebuilds harts from scratch — all nodes born dirty */
    hash_t root = state_compute_root(fresh_st, prune);

    evm_state_destroy(fresh);
    unlink(tmp);
    return root;
}

static int test_incremental_vs_full(int n_accounts, int n_blocks,
                                     int checkpoint_interval, int mods_per_block) {
    printf("\n=== n_accts=%d, n_blocks=%d, checkpoint=%d, mods=%d ===\n",
           n_accounts, n_blocks, checkpoint_interval, mods_per_block);

    evm_state_t *es = evm_state_create(NULL);
    state_t *st = evm_state_get_state(es);

    /* Setup initial accounts */
    for (int i = 0; i < n_accounts; i++) {
        address_t addr = make_addr((uint32_t)i);
        uint256_t bal = uint256_from_uint64((uint64_t)(i * 100 + 1));
        evm_state_set_balance(es, &addr, &bal);
        evm_state_set_nonce(es, &addr, (uint64_t)(i % 10));
        evm_state_mark_existed(es, &addr);

        /* Some accounts get storage */
        if (i % 5 == 0) {
            uint256_t skey = uint256_from_uint64(1);
            uint256_t sval = uint256_from_uint64((uint64_t)(i * 7));
            evm_state_set_storage(es, &addr, &skey, &sval);
        }
    }
    evm_state_commit(es);

    /* Initial root (caches all hashes) */
    hash_t initial = evm_state_compute_mpt_root(es, false);
    evm_state_clear_prestate_dirty(es);
    print_hash("initial root", initial.bytes);

    /* Simulate blocks */
    int errors = 0;
    for (int b = 1; b <= n_blocks; b++) {
        bool is_checkpoint = (b % checkpoint_interval == 0) || (b == n_blocks);
        sim_block(es, st, b, mods_per_block, is_checkpoint);

        if (is_checkpoint) {
            /* Get the incremental root (just computed by sim_block) */
            bool prune = (b >= 10);
            hash_t incr = evm_state_compute_mpt_root(es, prune);

            /* Full recompute from saved/loaded state */
            hash_t full = full_recompute(es, st, prune);

            bool match = memcmp(incr.bytes, full.bytes, 32) == 0;
            printf("  block %3d: %s", b, match ? "OK" : "FAIL");
            if (!match) {
                printf("\n");
                print_hash("  incremental", incr.bytes);
                print_hash("  full       ", full.bytes);
                errors++;
            } else {
                printf("\n");
            }
        }
    }

    evm_state_destroy(es);
    return errors;
}

int main(void) {
    int errors = 0;

    /* Small scale — fast, catches basic bugs */
    errors += test_incremental_vs_full(100, 20, 4, 10);
    errors += test_incremental_vs_full(100, 20, 4, 30);

    /* Medium scale — more accounts, longer intervals */
    errors += test_incremental_vs_full(500, 50, 10, 20);
    errors += test_incremental_vs_full(500, 100, 25, 50);

    /* Simulate validate-every-256 pattern */
    errors += test_incremental_vs_full(1000, 512, 256, 100);

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
