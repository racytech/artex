/**
 * State overlay stress tests — validate data flow consistency.
 *
 * Tests the invariant: incremental MPT root must always match a full
 * recompute from flat_state. Exercises all mutation paths:
 *   - balance/nonce/code changes
 *   - storage writes
 *   - snapshot/revert
 *   - commit_tx / commit (per-block)
 *   - compute_mpt_root (incremental vs full)
 *   - evict + reload cycle
 *   - multi-block sequences
 *   - self-destruct
 *   - EIP-161 empty account pruning
 */

#include "evm_state.h"
#include "evm.h"
#include "transaction.h"
#include "fork.h"
#include "flat_state.h"
#include "flat_store.h"
#include "compact_art.h"
#include "account_trie.h"
#include "state_overlay.h"
#include "uint256.h"
#include "hash.h"
#include "keccak256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

static int tests_passed = 0;
static int tests_failed = 0;

#define CHECK(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL: %s (line %d)\n", msg, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define PASS(msg) do { \
    printf("  OK: %s\n", msg); \
    tests_passed++; \
} while(0)

/* =========================================================================
 * Helpers
 * ========================================================================= */

static const char *FLAT_PATH = "/dev/shm/test_overlay_stress";

static address_t make_addr(uint8_t hi, uint8_t lo) {
    address_t a;
    memset(a.bytes, 0, 20);
    a.bytes[18] = hi;
    a.bytes[19] = lo;
    return a;
}

typedef struct {
    evm_state_t  *es;
    flat_state_t *fs;
} test_ctx_t;

static test_ctx_t make_ctx(void) {
    test_ctx_t ctx = {0};
    ctx.es = evm_state_create(NULL);
    ctx.fs = flat_state_create(FLAT_PATH);
    if (ctx.es && ctx.fs)
        evm_state_set_flat_state(ctx.es, ctx.fs);
    return ctx;
}

static void free_ctx(test_ctx_t *ctx) {
    if (ctx->es) {
        evm_state_set_flat_state(ctx->es, NULL);
        evm_state_destroy(ctx->es);
    }
    if (ctx->fs) flat_state_destroy(ctx->fs);
    /* Clean up files */
    unlink("/dev/shm/test_overlay_stress_acct.art");
    unlink("/dev/shm/test_overlay_stress_stor.art");
    memset(ctx, 0, sizeof(*ctx));
}

/* Compute root via normal incremental path */
static hash_t compute_root(evm_state_t *es, bool prune) {
    return evm_state_compute_mpt_root(es, prune);
}

/* Compute root via full recompute (invalidate all caches first) */
static hash_t compute_root_full(test_ctx_t *ctx, bool prune) {
    /* First do the normal incremental compute to sync all dirty data */
    hash_t inc = evm_state_compute_mpt_root(ctx->es, prune);
    (void)inc;
    /* Now invalidate and recompute from disk */
    compact_art_t *a_art = flat_state_account_art(ctx->fs);
    flat_store_t  *a_store = flat_state_account_store(ctx->fs);
    account_trie_t *at = account_trie_create(a_art, a_store, NULL);
    hash_t full = {0};
    if (at) {
        account_trie_root(at, full.bytes);
        account_trie_destroy(at);
    }
    return full;
}

static bool roots_match(hash_t a, hash_t b) {
    return memcmp(a.bytes, b.bytes, 32) == 0;
}

static void print_hash(const char *label, hash_t h) {
    fprintf(stderr, "  %s: 0x", label);
    for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", h.bytes[i]);
    fprintf(stderr, "\n");
}

/* =========================================================================
 * Test 1: Single account balance — incremental == full
 * ========================================================================= */

static void test_single_balance(void) {
    printf("test_single_balance:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    address_t a = make_addr(0, 1);
    uint256_t bal = uint256_from_uint64(1000);
    evm_state_set_balance(ctx.es, &a, &bal);
    evm_state_mark_existed(ctx.es, &a);
    evm_state_commit(ctx.es);

    hash_t inc = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    if (!roots_match(inc, full)) {
        print_hash("incremental", inc);
        print_hash("full", full);
    }
    CHECK(roots_match(inc, full), "incremental == full for single account");
    CHECK(memcmp(inc.bytes, ((hash_t){0}).bytes, 32) != 0, "root is not zero");

    PASS("single balance");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 2: Multiple accounts — genesis-like setup then modify one
 * ========================================================================= */

static void test_genesis_then_modify(void) {
    printf("test_genesis_then_modify:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup "genesis" — 100 accounts with varying balances */
    for (int i = 0; i < 100; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000 + i * 1000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);

    /* Compute genesis root and clear dirty */
    hash_t genesis_root = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Modify one account (simulating a block with 1 tx) */
    address_t sender = make_addr(0, 5);
    uint256_t new_bal = uint256_from_uint64(999000);
    evm_state_set_balance(ctx.es, &sender, &new_bal);
    evm_state_set_nonce(ctx.es, &sender, 1);
    evm_state_commit_tx(ctx.es);

    /* Add coinbase reward */
    address_t coinbase = make_addr(0, 50);
    uint256_t reward = uint256_from_uint64(5000000000000000000ULL);
    evm_state_add_balance(ctx.es, &coinbase, &reward);

    hash_t inc = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    CHECK(!roots_match(inc, genesis_root), "root changed after modification");
    if (!roots_match(inc, full)) {
        print_hash("incremental", inc);
        print_hash("full", full);
    }
    CHECK(roots_match(inc, full), "incremental == full after modify");

    PASS("genesis then modify");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 3: Evict cycle — root stable across evict + reload
 * ========================================================================= */

static void test_evict_cycle(void) {
    printf("test_evict_cycle:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup state */
    for (int i = 0; i < 50; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000 + i);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    hash_t root1 = compute_root(ctx.es, false);

    /* Evict — flush to disk, clear cache */
    evm_state_evict_cache(ctx.es);

    /* Modify one account (forces reload from flat_state) */
    address_t a = make_addr(0, 10);
    uint256_t new_bal = uint256_from_uint64(999999);
    evm_state_set_balance(ctx.es, &a, &new_bal);
    evm_state_commit_tx(ctx.es);

    hash_t root2 = compute_root(ctx.es, false);
    hash_t root2_full = compute_root_full(&ctx, false);

    CHECK(!roots_match(root1, root2), "root changed after evict + modify");
    if (!roots_match(root2, root2_full)) {
        print_hash("incremental", root2);
        print_hash("full", root2_full);
    }
    CHECK(roots_match(root2, root2_full), "incremental == full after evict");

    PASS("evict cycle");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 4: Multi-block simulation — N blocks with coinbase rewards
 * ========================================================================= */

static void test_multi_block_coinbase(void) {
    printf("test_multi_block_coinbase:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Genesis: 20 accounts */
    for (int i = 0; i < 20; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    hash_t genesis = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Simulate 100 blocks: each block pays reward to a different coinbase */
    hash_t prev_root = genesis;
    for (int blk = 1; blk <= 100; blk++) {
        evm_state_begin_block(ctx.es, blk);
        evm_state_commit(ctx.es);

        /* Coinbase reward — cycles through addresses 0-19 */
        address_t coinbase = make_addr(0, (uint8_t)(blk % 20));
        uint256_t reward = uint256_from_uint64(5000000);
        evm_state_add_balance(ctx.es, &coinbase, &reward);

        hash_t root = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);

        if (!roots_match(root, full)) {
            fprintf(stderr, "  Block %d: MISMATCH\n", blk);
            print_hash("incremental", root);
            print_hash("full", full);
            CHECK(false, "incremental == full in multi-block");
        }
        CHECK(!roots_match(root, prev_root), "root changes each block");
        prev_root = root;
    }

    PASS("multi-block coinbase (100 blocks)");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 5: Multi-block with eviction every N blocks
 * ========================================================================= */

static void test_multi_block_with_evict(void) {
    printf("test_multi_block_with_evict:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Genesis: 30 accounts */
    for (int i = 0; i < 30; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* 50 blocks, evict every 10 */
    for (int blk = 1; blk <= 50; blk++) {
        evm_state_begin_block(ctx.es, blk);
        evm_state_commit(ctx.es);

        address_t coinbase = make_addr(0, (uint8_t)(blk % 30));
        uint256_t reward = uint256_from_uint64(5000000);
        evm_state_add_balance(ctx.es, &coinbase, &reward);

        hash_t root = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);

        if (!roots_match(root, full)) {
            fprintf(stderr, "  Block %d: MISMATCH\n", blk);
            print_hash("incremental", root);
            print_hash("full", full);
            CHECK(false, "incremental == full with evict");
        }

        if (blk % 10 == 0) {
            evm_state_evict_cache(ctx.es);
        }
    }

    PASS("multi-block with evict every 10");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 6: Transaction with revert — SSTORE then OOG
 * ========================================================================= */

static void test_storage_revert(void) {
    printf("test_storage_revert:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    address_t contract = make_addr(0, 1);
    address_t sender = make_addr(0, 2);
    uint256_t bal = uint256_from_uint64(1000000);
    evm_state_set_balance(ctx.es, &contract, &bal);
    evm_state_set_balance(ctx.es, &sender, &bal);
    evm_state_mark_existed(ctx.es, &contract);
    evm_state_mark_existed(ctx.es, &sender);
    evm_state_commit(ctx.es);
    hash_t pre_root = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Start "block" */
    evm_state_begin_block(ctx.es, 1);
    evm_state_commit(ctx.es);

    /* Simulate tx: snapshot → SSTORE → revert (OOG) */
    uint32_t snap = evm_state_snapshot(ctx.es);

    uint256_t key = uint256_from_uint64(1);
    uint256_t val = uint256_from_uint64(42);
    evm_state_set_storage(ctx.es, &contract, &key, &val);

    /* Verify storage was set */
    uint256_t got = evm_state_get_storage(ctx.es, &contract, &key);
    CHECK(uint256_to_uint64(&got) == 42, "storage set before revert");

    /* Revert (simulating OOG) */
    evm_state_revert(ctx.es, snap);

    /* Storage should be reverted */
    got = evm_state_get_storage(ctx.es, &contract, &key);
    CHECK(uint256_is_zero(&got), "storage reverted to zero");

    /* Commit tx (nonce increment + gas fee happen outside snapshot) */
    evm_state_set_nonce(ctx.es, &sender, 1);
    uint256_t reduced = uint256_from_uint64(999000);
    evm_state_set_balance(ctx.es, &sender, &reduced);
    evm_state_commit_tx(ctx.es);

    /* Coinbase reward */
    address_t coinbase = make_addr(0, 3);
    uint256_t reward = uint256_from_uint64(5000000);
    evm_state_add_balance(ctx.es, &coinbase, &reward);

    hash_t root = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    CHECK(!roots_match(root, pre_root), "root changed after tx");
    if (!roots_match(root, full)) {
        print_hash("incremental", root);
        print_hash("full", full);
    }
    CHECK(roots_match(root, full), "incremental == full after revert");

    PASS("storage revert");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 7: New account receiving ETH (not in genesis)
 * ========================================================================= */

static void test_new_account_transfer(void) {
    printf("test_new_account_transfer:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Genesis: 2 accounts */
    address_t sender = make_addr(0, 1);
    address_t existing = make_addr(0, 2);
    uint256_t bal = uint256_from_uint64(10000000);
    evm_state_set_balance(ctx.es, &sender, &bal);
    evm_state_set_balance(ctx.es, &existing, &bal);
    evm_state_mark_existed(ctx.es, &sender);
    evm_state_mark_existed(ctx.es, &existing);
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block 1: transfer to brand new address */
    evm_state_begin_block(ctx.es, 1);
    evm_state_commit(ctx.es);

    address_t recipient = make_addr(0, 99);
    uint256_t amount = uint256_from_uint64(1000000);
    evm_state_sub_balance(ctx.es, &sender, &amount);
    evm_state_add_balance(ctx.es, &recipient, &amount);
    evm_state_set_nonce(ctx.es, &sender, 1);
    evm_state_commit_tx(ctx.es);

    /* Coinbase */
    address_t coinbase = make_addr(0, 50);
    uint256_t reward = uint256_from_uint64(5000000);
    evm_state_add_balance(ctx.es, &coinbase, &reward);

    hash_t root = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    if (!roots_match(root, full)) {
        print_hash("incremental", root);
        print_hash("full", full);
    }
    CHECK(roots_match(root, full), "incremental == full with new recipient");

    PASS("new account transfer");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 8: Large genesis (8000+ accounts) then single tx
 * Mimics the chain_replay scenario that fails at block 46242
 * ========================================================================= */

static void test_large_genesis_then_tx(void) {
    printf("test_large_genesis_then_tx:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Large genesis: 8000 accounts */
    for (int i = 0; i < 8000; i++) {
        address_t a;
        memset(a.bytes, 0, 20);
        a.bytes[17] = (uint8_t)(i >> 16);
        a.bytes[18] = (uint8_t)(i >> 8);
        a.bytes[19] = (uint8_t)(i & 0xFF);
        uint256_t bal = uint256_from_uint64(1000000000ULL + i);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    hash_t genesis = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);
    CHECK(memcmp(genesis.bytes, ((hash_t){0}).bytes, 32) != 0, "genesis root non-zero");

    /* Simulate empty blocks with coinbase rewards + evict every 256 */
    for (int blk = 1; blk <= 100; blk++) {
        evm_state_begin_block(ctx.es, blk);
        evm_state_commit(ctx.es);

        /* Coinbase: unique addresses (not in genesis) */
        address_t coinbase;
        memset(coinbase.bytes, 0, 20);
        coinbase.bytes[0] = 0xCC;
        coinbase.bytes[18] = (uint8_t)(blk >> 8);
        coinbase.bytes[19] = (uint8_t)(blk & 0xFF);
        uint256_t reward = uint256_from_uint64(5000000000000000000ULL);
        evm_state_add_balance(ctx.es, &coinbase, &reward);

        hash_t root = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);
        if (!roots_match(root, full)) {
            fprintf(stderr, "  Block %d (empty): MISMATCH\n", blk);
            print_hash("incremental", root);
            print_hash("full", full);
            CHECK(false, "incremental == full for empty blocks");
        }

        if (blk % 256 == 0)
            evm_state_evict_cache(ctx.es);
    }

    /* Now simulate a block WITH a transaction */
    evm_state_begin_block(ctx.es, 101);
    evm_state_commit(ctx.es);

    /* Simple transfer: genesis account → new address */
    address_t sender = make_addr(0, 5);
    address_t recipient;
    memset(recipient.bytes, 0, 20);
    recipient.bytes[0] = 0xDD;
    recipient.bytes[19] = 0x01;

    uint256_t send_amount = uint256_from_uint64(1000000);
    evm_state_sub_balance(ctx.es, &sender, &send_amount);
    evm_state_add_balance(ctx.es, &recipient, &send_amount);
    evm_state_set_nonce(ctx.es, &sender, 1);
    evm_state_commit_tx(ctx.es);

    /* Coinbase reward */
    address_t coinbase;
    memset(coinbase.bytes, 0, 20);
    coinbase.bytes[0] = 0xCC;
    coinbase.bytes[18] = 0;
    coinbase.bytes[19] = 101;
    uint256_t reward = uint256_from_uint64(5000000000000000000ULL);
    evm_state_add_balance(ctx.es, &coinbase, &reward);

    hash_t final_root = compute_root(ctx.es, false);
    hash_t final_full = compute_root_full(&ctx, false);

    if (!roots_match(final_root, final_full)) {
        fprintf(stderr, "  Block 101 (with tx): MISMATCH\n");
        print_hash("incremental", final_root);
        print_hash("full", final_full);
    }
    CHECK(roots_match(final_root, final_full),
          "incremental == full after large genesis + tx");

    PASS("large genesis (8K accounts) then tx");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 9: Self-destruct
 * ========================================================================= */

static void test_self_destruct(void) {
    printf("test_self_destruct:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    address_t contract = make_addr(0, 1);
    address_t beneficiary = make_addr(0, 2);
    uint256_t bal = uint256_from_uint64(1000000);
    evm_state_set_balance(ctx.es, &contract, &bal);
    evm_state_set_balance(ctx.es, &beneficiary, &bal);
    uint256_t key = uint256_from_uint64(1);
    uint256_t val = uint256_from_uint64(42);
    evm_state_set_storage(ctx.es, &contract, &key, &val);
    evm_state_mark_existed(ctx.es, &contract);
    evm_state_mark_existed(ctx.es, &beneficiary);
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block: self-destruct contract, send balance to beneficiary */
    evm_state_begin_block(ctx.es, 1);
    evm_state_commit(ctx.es);

    evm_state_add_balance(ctx.es, &beneficiary, &bal);
    evm_state_set_balance(ctx.es, &contract, &(uint256_t){0});
    evm_state_self_destruct(ctx.es, &contract);
    evm_state_commit_tx(ctx.es);

    hash_t root = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    if (!roots_match(root, full)) {
        print_hash("incremental", root);
        print_hash("full", full);
    }
    CHECK(roots_match(root, full), "incremental == full after self-destruct");

    PASS("self-destruct");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 10: EIP-161 empty pruning
 * ========================================================================= */

static void test_eip161_pruning(void) {
    printf("test_eip161_pruning:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    address_t a = make_addr(0, 1);
    address_t b = make_addr(0, 2);
    uint256_t bal = uint256_from_uint64(1000000);
    evm_state_set_balance(ctx.es, &a, &bal);
    evm_state_set_balance(ctx.es, &b, &bal);
    evm_state_mark_existed(ctx.es, &a);
    evm_state_mark_existed(ctx.es, &b);
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Enable EIP-161 pruning */
    evm_state_set_prune_empty(ctx.es, true);

    /* Block: drain account A to zero (should be pruned) */
    evm_state_begin_block(ctx.es, 1);
    evm_state_commit(ctx.es);

    evm_state_set_balance(ctx.es, &a, &(uint256_t){0});
    evm_state_commit_tx(ctx.es);

    hash_t root = compute_root(ctx.es, true);
    hash_t full = compute_root_full(&ctx, true);

    if (!roots_match(root, full)) {
        print_hash("incremental", root);
        print_hash("full", full);
    }
    CHECK(roots_match(root, full), "incremental == full with EIP-161 pruning");

    /* Verify pruned account is gone from trie */
    CHECK(!evm_state_exists(ctx.es, &a), "account A pruned");
    CHECK(evm_state_exists(ctx.es, &b), "account B still exists");

    PASS("EIP-161 pruning");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 11: Full transaction_execute path — simple ETH transfer
 * Exercises the exact code path used by chain_replay / block_executor
 * ========================================================================= */

static void test_transaction_execute_transfer(void) {
    printf("test_transaction_execute_transfer:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Create EVM instance with Frontier config (pre-EIP-155, pre-EIP-161) */
    evm_t *evm = evm_create(ctx.es, chain_config_mainnet());
    CHECK(evm != NULL, "create evm");

    /* Genesis: sender with 100 ETH, some other accounts */
    address_t sender = make_addr(0xAA, 0x01);
    address_t recipient = make_addr(0xBB, 0x01);
    address_t coinbase = make_addr(0xCC, 0x01);

    uint256_t hundred_eth = uint256_from_uint64(100);
    uint256_t one_eth_factor = uint256_from_uint64(1000000000000000000ULL);
    hundred_eth = uint256_mul(&hundred_eth, &one_eth_factor);

    evm_state_set_balance(ctx.es, &sender, &hundred_eth);
    evm_state_mark_existed(ctx.es, &sender);

    /* Add some other genesis accounts to make the trie non-trivial */
    for (int i = 0; i < 50; i++) {
        address_t a = make_addr(0x10, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000000ULL + i);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }

    evm_state_commit(ctx.es);
    hash_t genesis_root = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Set block environment (Frontier era — block 46242 equivalent) */
    evm_block_env_t block_env = {
        .number = 100,
        .timestamp = 1438870000,
        .gas_limit = 22000,
        .difficulty = uint256_from_uint64(0x1545be7224dULL),
        .coinbase = coinbase,
        .base_fee = uint256_from_uint64(0),
    };
    evm_set_block_env(evm, &block_env);

    /* Commit at block start (as block_executor does) */
    evm_state_set_prune_empty(ctx.es, false); /* Frontier: no EIP-161 */
    evm_state_commit(ctx.es);
    evm_state_begin_block(ctx.es, 100);

    /* Build a simple ETH transfer transaction */
    transaction_t tx = {
        .type = TX_TYPE_LEGACY,
        .nonce = 0,
        .sender = sender,
        .to = recipient,
        .value = one_eth_factor,  /* 1 ETH */
        .gas_limit = 21000,
        .gas_price = uint256_from_uint64(50000000000ULL), /* 50 Gwei */
        .data = NULL,
        .data_size = 0,
        .is_create = false,
        .access_list = NULL,
        .access_list_count = 0,
        .max_fee_per_blob_gas = {0},
        .blob_versioned_hashes = NULL,
        .blob_versioned_hashes_count = 0,
        .authorization_list = NULL,
        .authorization_list_count = 0,
    };

    block_env_t tx_block_env = {
        .coinbase = coinbase,
        .block_number = 100,
        .timestamp = 1438870000,
        .gas_limit = 22000,
        .difficulty = uint256_from_uint64(0x1545be7224dULL),
        .base_fee = uint256_from_uint64(0),
        .skip_coinbase_payment = false,
    };

    /* Execute transaction */
    transaction_result_t result;
    bool ok = transaction_execute(evm, &tx, &tx_block_env, &result);
    CHECK(ok, "transaction_execute succeeded");
    CHECK(result.status == EVM_SUCCESS, "tx status is SUCCESS");
    CHECK(result.gas_used == 21000, "gas used is 21000");
    transaction_result_free(&result);

    /* commit_tx (as block_executor does after each tx) */
    evm_state_commit_tx(ctx.es);

    /* Add block reward (5 ETH, Frontier era) */
    uint256_t five_eth = uint256_from_uint64(5);
    five_eth = uint256_mul(&five_eth, &one_eth_factor);
    evm_state_add_balance(ctx.es, &coinbase, &five_eth);

    /* Compute root — this is what chain_replay does */
    hash_t root = compute_root(ctx.es, false);
    hash_t full = compute_root_full(&ctx, false);

    CHECK(!roots_match(root, genesis_root), "root changed after tx");
    if (!roots_match(root, full)) {
        fprintf(stderr, "  TRANSACTION EXECUTE: MISMATCH\n");
        print_hash("incremental", root);
        print_hash("full", full);
    }
    CHECK(roots_match(root, full), "incremental == full after transaction_execute");

    /* Verify account states */
    uint256_t sender_bal = evm_state_get_balance(ctx.es, &sender);
    uint256_t recip_bal = evm_state_get_balance(ctx.es, &recipient);
    uint64_t sender_nonce = evm_state_get_nonce(ctx.es, &sender);

    CHECK(sender_nonce == 1, "sender nonce incremented");
    CHECK(!uint256_is_zero(&recip_bal), "recipient received ETH");

    /* Gas cost = 21000 * 50 Gwei = 1,050,000 Gwei = 0.00105 ETH */
    /* Sender should have: 100 - 1 - 0.00105 = 98.99895 ETH */
    /* Recipient should have: 1 ETH */
    CHECK(uint256_eq(&recip_bal, &one_eth_factor), "recipient has 1 ETH");

    evm_destroy(evm);
    PASS("transaction_execute transfer");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 12: Multiple tx blocks with transaction_execute + evict
 * Full chain_replay simulation
 * ========================================================================= */

static void test_multi_tx_blocks_with_evict(void) {
    printf("test_multi_tx_blocks_with_evict:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    evm_t *evm = evm_create(ctx.es, chain_config_mainnet());
    CHECK(evm != NULL, "create evm");

    /* Genesis: 100 funded accounts */
    uint256_t one_eth = uint256_from_uint64(1000000000000000000ULL);
    uint256_t init_bal = uint256_from_uint64(1000);
    init_bal = uint256_mul(&init_bal, &one_eth); /* 1000 ETH each */

    for (int i = 0; i < 100; i++) {
        address_t a = make_addr(0x10, (uint8_t)i);
        evm_state_set_balance(ctx.es, &a, &init_bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Simulate 30 blocks: some empty, some with 1 tx */
    for (int blk = 1; blk <= 30; blk++) {
        address_t coinbase = make_addr(0xCC, (uint8_t)blk);

        evm_block_env_t block_env = {
            .number = blk,
            .timestamp = 1438870000 + blk * 15,
            .gas_limit = 5000000,
            .difficulty = uint256_from_uint64(0x1545be7224dULL),
            .coinbase = coinbase,
            .base_fee = uint256_from_uint64(0),
        };
        evm_set_block_env(evm, &block_env);

        evm_state_set_prune_empty(ctx.es, false);
        evm_state_commit(ctx.es);
        evm_state_begin_block(ctx.es, blk);

        /* Every 3rd block has a transaction */
        if (blk % 3 == 0) {
            address_t sender = make_addr(0x10, (uint8_t)((blk / 3) % 100));
            address_t recipient = make_addr(0xDD, (uint8_t)blk);
            uint64_t sender_nonce = evm_state_get_nonce(ctx.es, &sender);

            transaction_t tx = {
                .type = TX_TYPE_LEGACY,
                .nonce = sender_nonce,
                .sender = sender,
                .to = recipient,
                .value = one_eth, /* 1 ETH */
                .gas_limit = 21000,
                .gas_price = uint256_from_uint64(20000000000ULL), /* 20 Gwei */
                .data = NULL,
                .data_size = 0,
                .is_create = false,
            };

            block_env_t tx_env = {
                .coinbase = coinbase,
                .block_number = blk,
                .timestamp = 1438870000 + blk * 15,
                .gas_limit = 5000000,
                .difficulty = uint256_from_uint64(0x1545be7224dULL),
                .base_fee = uint256_from_uint64(0),
                .skip_coinbase_payment = false,
            };

            transaction_result_t result;
            bool ok = transaction_execute(evm, &tx, &tx_env, &result);
            if (ok) transaction_result_free(&result);
            evm_state_commit_tx(ctx.es);
        }

        /* Block reward */
        uint256_t five_eth = uint256_from_uint64(5);
        five_eth = uint256_mul(&five_eth, &one_eth);
        evm_state_add_balance(ctx.es, &coinbase, &five_eth);

        hash_t root = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);

        if (!roots_match(root, full)) {
            fprintf(stderr, "  Block %d: MISMATCH\n", blk);
            print_hash("incremental", root);
            print_hash("full", full);
            evm_destroy(evm);
            CHECK(false, "incremental == full in multi-tx simulation");
        }

        /* Evict every 10 blocks */
        if (blk % 10 == 0)
            evm_state_evict_cache(ctx.es);
    }

    evm_destroy(evm);
    PASS("multi-tx blocks with evict (30 blocks)");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 13: Evict per-block — root stability with evict every block
 *
 * Simulates --validate-every 1: compute root, evict, next block.
 * Catches hash cache corruption from flush+evict cycle.
 * ========================================================================= */

static void test_evict_per_block(void) {
    printf("test_evict_per_block:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Genesis: 200 accounts with balances + some with storage */
    for (int i = 0; i < 200; i++) {
        address_t a = make_addr((uint8_t)(i >> 8), (uint8_t)(i & 0xff));
        uint256_t bal = uint256_from_uint64(1000000 + i * 100);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
        if (i % 10 == 0) {
            uint256_t key = uint256_from_uint64(i);
            uint256_t val = uint256_from_uint64(i * 42);
            evm_state_set_storage(ctx.es, &a, &key, &val);
        }
    }
    evm_state_commit(ctx.es);
    hash_t genesis = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);
    evm_state_evict_cache(ctx.es);

    /* Simulate 50 blocks with evict after every block */
    hash_t prev_root = genesis;
    for (int blk = 1; blk <= 50; blk++) {
        evm_state_begin_block(ctx.es, blk);

        /* Each block: modify 2-3 accounts (balance, storage) */
        address_t coinbase = make_addr(0, (uint8_t)(blk % 20));
        uint256_t reward = uint256_from_uint64(5000000);
        evm_state_add_balance(ctx.es, &coinbase, &reward);

        address_t sender = make_addr(0, (uint8_t)((blk + 50) % 200));
        address_t recip = make_addr(0, (uint8_t)((blk + 100) % 200));
        uint256_t amount = uint256_from_uint64(100);
        evm_state_sub_balance(ctx.es, &sender, &amount);
        evm_state_add_balance(ctx.es, &recip, &amount);

        /* Occasionally write storage */
        if (blk % 5 == 0) {
            address_t stor_acct = make_addr(0, (uint8_t)((blk * 10) % 200));
            uint256_t skey = uint256_from_uint64(blk);
            uint256_t sval = uint256_from_uint64(blk * 1000);
            evm_state_set_storage(ctx.es, &stor_acct, &skey, &sval);
        }

        evm_state_commit_tx(ctx.es);
        evm_state_commit(ctx.es);

        /* Compute root + verify incremental == full */
        hash_t inc = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);

        CHECK(!roots_match(prev_root, inc), "root changed");
        if (!roots_match(inc, full)) {
            fprintf(stderr, "  block %d: incremental != full\n", blk);
            print_hash("incremental", inc);
            print_hash("full", full);
        }
        CHECK(roots_match(inc, full), "incremental == full");

        prev_root = inc;

        /* Evict after every block (like --validate-every 1) */
        evm_state_evict_cache(ctx.es);
    }

    PASS("evict per-block (50 blocks)");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 14: Reverted CREATE doesn't leak dirty flags
 *
 * CREATE + revert should leave state unchanged. Catches the mpt_dirty
 * leak we fixed in JOURNAL_CODE and JOURNAL_ACCOUNT_CREATE.
 * ========================================================================= */

static void test_reverted_create_no_leak(void) {
    printf("test_reverted_create_no_leak:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup: 10 accounts */
    for (int i = 0; i < 10; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    hash_t root_before = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block 1: snapshot, create account, set code, revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);

    address_t new_acct = make_addr(0, 99);
    evm_state_create_account(ctx.es, &new_acct);
    uint8_t code[] = {0x60, 0x00, 0x60, 0x00, 0xf3}; /* PUSH 0, PUSH 0, RETURN */
    evm_state_set_code(ctx.es, &new_acct, code, sizeof(code));
    uint256_t stor_key = uint256_from_uint64(1);
    uint256_t stor_val = uint256_from_uint64(42);
    evm_state_set_storage(ctx.es, &new_acct, &stor_key, &stor_val);

    /* Revert everything */
    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    /* Root should be unchanged */
    hash_t root_after = compute_root(ctx.es, false);
    hash_t root_full = compute_root_full(&ctx, false);

    if (!roots_match(root_before, root_after)) {
        print_hash("before", root_before);
        print_hash("after", root_after);
        print_hash("full", root_full);
    }
    CHECK(roots_match(root_before, root_after), "root unchanged after reverted CREATE");
    CHECK(roots_match(root_after, root_full), "incremental == full after revert");

    PASS("reverted CREATE no dirty leak");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 15: Reverted CALL touch doesn't leak dirty flags
 *
 * Simulates add_balance(0) + revert (CALL to empty account that fails).
 * ========================================================================= */

static void test_reverted_touch_no_leak(void) {
    printf("test_reverted_touch_no_leak:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup: 10 accounts, one empty */
    for (int i = 0; i < 10; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(i == 5 ? 0 : 1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    evm_state_commit(ctx.es);
    hash_t root_before = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block 1: snapshot, touch empty account (add_balance(0)), revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);

    address_t empty = make_addr(0, 5);
    uint256_t zero = UINT256_ZERO;
    evm_state_add_balance(ctx.es, &empty, &zero);

    /* Revert */
    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    hash_t root_after = compute_root(ctx.es, false);
    hash_t root_full = compute_root_full(&ctx, false);

    if (!roots_match(root_before, root_after)) {
        print_hash("before", root_before);
        print_hash("after", root_after);
        print_hash("full", root_full);
    }
    CHECK(roots_match(root_before, root_after), "root unchanged after reverted touch");
    CHECK(roots_match(root_after, root_full), "incremental == full after revert");

    PASS("reverted touch no dirty leak");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 16: Reverted storage write doesn't leak dirty flags
 * ========================================================================= */

static void test_reverted_sstore_no_leak(void) {
    printf("test_reverted_sstore_no_leak:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup: 5 accounts, account 3 has storage */
    for (int i = 0; i < 5; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    address_t stor_acct = make_addr(0, 3);
    uint256_t skey = uint256_from_uint64(1);
    uint256_t sval = uint256_from_uint64(100);
    evm_state_set_storage(ctx.es, &stor_acct, &skey, &sval);
    evm_state_commit(ctx.es);
    hash_t root_before = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block 1: snapshot, write storage, revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);

    uint256_t new_sval = uint256_from_uint64(999);
    evm_state_set_storage(ctx.es, &stor_acct, &skey, &new_sval);
    /* Also write a new slot */
    uint256_t skey2 = uint256_from_uint64(2);
    uint256_t sval2 = uint256_from_uint64(777);
    evm_state_set_storage(ctx.es, &stor_acct, &skey2, &sval2);

    /* Revert */
    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    hash_t root_after = compute_root(ctx.es, false);
    hash_t root_full = compute_root_full(&ctx, false);

    if (!roots_match(root_before, root_after)) {
        print_hash("before", root_before);
        print_hash("after", root_after);
        print_hash("full", root_full);
    }
    CHECK(roots_match(root_before, root_after), "root unchanged after reverted SSTORE");
    CHECK(roots_match(root_after, root_full), "incremental == full after revert");

    PASS("reverted SSTORE no dirty leak");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 17: EIP-161 RIPEMD special case
 *
 * Touch RIPEMD (0x3) with prune_empty=true, revert.
 * RIPEMD should still get pruned (dirty persists through revert).
 * ========================================================================= */

static void test_ripemd_pruning(void) {
    printf("test_ripemd_pruning:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");
    evm_state_set_prune_empty(ctx.es, true);

    /* Setup: 5 accounts + RIPEMD (0x3) as empty */
    for (int i = 0; i < 5; i++) {
        address_t a = make_addr(0, (uint8_t)(i + 1));
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    /* RIPEMD — empty account that exists in trie */
    address_t ripemd = {{0}};
    ripemd.bytes[19] = 3;
    uint256_t zero_bal = UINT256_ZERO;
    evm_state_set_balance(ctx.es, &ripemd, &zero_bal);
    evm_state_mark_existed(ctx.es, &ripemd);

    evm_state_commit(ctx.es);
    hash_t root_with_ripemd = compute_root(ctx.es, true);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block: snapshot, touch RIPEMD (add_balance(0)), revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);
    evm_state_add_balance(ctx.es, &ripemd, &zero_bal);
    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    hash_t root_after = compute_root(ctx.es, true);

    /* RIPEMD should be pruned — root must differ from root_with_ripemd */
    CHECK(!roots_match(root_with_ripemd, root_after),
          "RIPEMD pruned after reverted touch (root changed)");

    /* Verify the empty RIPEMD no longer exists */
    CHECK(!evm_state_exists(ctx.es, &ripemd), "RIPEMD not in state after prune");

    hash_t root_full = compute_root_full(&ctx, true);
    CHECK(roots_match(root_after, root_full), "incremental == full after RIPEMD prune");

    PASS("RIPEMD EIP-161 pruning");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 18: RIPEMD NOT pruned on pre-SD (prune_empty=false)
 * ========================================================================= */

static void test_ripemd_no_prune_frontier(void) {
    printf("test_ripemd_no_prune_frontier:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");
    evm_state_set_prune_empty(ctx.es, false);

    /* Setup: RIPEMD as empty existed account */
    address_t ripemd = {{0}};
    ripemd.bytes[19] = 3;
    uint256_t zero_bal = UINT256_ZERO;
    evm_state_set_balance(ctx.es, &ripemd, &zero_bal);
    evm_state_mark_existed(ctx.es, &ripemd);

    address_t other = make_addr(0, 1);
    uint256_t bal = uint256_from_uint64(1000000);
    evm_state_set_balance(ctx.es, &other, &bal);
    evm_state_mark_existed(ctx.es, &other);

    evm_state_commit(ctx.es);
    hash_t root_before = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block: snapshot, touch RIPEMD, revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);
    evm_state_add_balance(ctx.es, &ripemd, &zero_bal);
    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    hash_t root_after = compute_root(ctx.es, false);

    /* Pre-SD: RIPEMD should NOT be pruned, root unchanged */
    if (!roots_match(root_before, root_after)) {
        print_hash("before", root_before);
        print_hash("after", root_after);
    }
    CHECK(roots_match(root_before, root_after),
          "RIPEMD NOT pruned on Frontier (root unchanged)");

    PASS("RIPEMD no prune on Frontier");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 19: Evict stability with storage — per-block evict with storage mods
 *
 * Exercises the storage trie hash cache across eviction boundaries.
 * ========================================================================= */

static void test_evict_storage_stability(void) {
    printf("test_evict_storage_stability:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Genesis: 20 accounts, 5 with storage */
    for (int i = 0; i < 20; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000 + i);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
        if (i < 5) {
            for (int s = 0; s < 10; s++) {
                uint256_t sk = uint256_from_uint64(s);
                uint256_t sv = uint256_from_uint64(i * 1000 + s);
                evm_state_set_storage(ctx.es, &a, &sk, &sv);
            }
        }
    }
    evm_state_commit(ctx.es);
    compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);
    evm_state_evict_cache(ctx.es);

    /* 30 blocks: each modifies 1 storage slot, evict after each */
    for (int blk = 1; blk <= 30; blk++) {
        evm_state_begin_block(ctx.es, blk);

        address_t acct = make_addr(0, (uint8_t)(blk % 5));
        uint256_t sk = uint256_from_uint64(blk % 10);
        uint256_t sv = uint256_from_uint64(blk * 9999);
        evm_state_set_storage(ctx.es, &acct, &sk, &sv);

        /* Also do a balance change (coinbase reward) */
        address_t cb = make_addr(0, (uint8_t)((blk + 10) % 20));
        uint256_t rw = uint256_from_uint64(5000);
        evm_state_add_balance(ctx.es, &cb, &rw);

        evm_state_commit_tx(ctx.es);
        evm_state_commit(ctx.es);

        hash_t inc = compute_root(ctx.es, false);
        hash_t full = compute_root_full(&ctx, false);

        if (!roots_match(inc, full)) {
            fprintf(stderr, "  block %d: incremental != full\n", blk);
            print_hash("incremental", inc);
            print_hash("full", full);
        }
        CHECK(roots_match(inc, full), "incremental == full with storage");

        evm_state_evict_cache(ctx.es);
    }

    PASS("evict stability with storage (30 blocks)");
    free_ctx(&ctx);
}

/* =========================================================================
 * Test 20: Reverted self-destruct doesn't leak dirty
 * ========================================================================= */

static void test_reverted_selfdestruct_no_leak(void) {
    printf("test_reverted_selfdestruct_no_leak:\n");
    test_ctx_t ctx = make_ctx();
    CHECK(ctx.es && ctx.fs, "create state");

    /* Setup: 5 accounts, one with code and storage */
    for (int i = 0; i < 5; i++) {
        address_t a = make_addr(0, (uint8_t)i);
        uint256_t bal = uint256_from_uint64(1000000);
        evm_state_set_balance(ctx.es, &a, &bal);
        evm_state_mark_existed(ctx.es, &a);
    }
    address_t contract = make_addr(0, 3);
    uint8_t code[] = {0x60, 0x00, 0xff}; /* PUSH 0, SELFDESTRUCT */
    evm_state_set_code(ctx.es, &contract, code, sizeof(code));
    uint256_t sk = uint256_from_uint64(1);
    uint256_t sv = uint256_from_uint64(42);
    evm_state_set_storage(ctx.es, &contract, &sk, &sv);

    evm_state_commit(ctx.es);
    hash_t root_before = compute_root(ctx.es, false);
    evm_state_clear_prestate_dirty(ctx.es);

    /* Block 1: snapshot, self-destruct, revert */
    evm_state_begin_block(ctx.es, 1);
    uint32_t snap = evm_state_snapshot(ctx.es);

    evm_state_self_destruct(ctx.es, &contract);

    evm_state_revert(ctx.es, snap);
    evm_state_commit_tx(ctx.es);
    evm_state_commit(ctx.es);

    hash_t root_after = compute_root(ctx.es, false);
    hash_t root_full = compute_root_full(&ctx, false);

    if (!roots_match(root_before, root_after)) {
        print_hash("before", root_before);
        print_hash("after", root_after);
        print_hash("full", root_full);
    }
    CHECK(roots_match(root_before, root_after),
          "root unchanged after reverted SELFDESTRUCT");
    CHECK(roots_match(root_after, root_full), "incremental == full");

    PASS("reverted SELFDESTRUCT no dirty leak");
    free_ctx(&ctx);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== State Overlay Stress Tests ===\n\n");

    test_single_balance();
    test_genesis_then_modify();
    test_evict_cycle();
    test_multi_block_coinbase();
    test_multi_block_with_evict();
    test_storage_revert();
    test_new_account_transfer();
    test_large_genesis_then_tx();
    test_self_destruct();
    test_eip161_pruning();
    test_transaction_execute_transfer();
    test_multi_tx_blocks_with_evict();
    test_evict_per_block();
    test_reverted_create_no_leak();
    test_reverted_touch_no_leak();
    test_reverted_sstore_no_leak();
    test_ripemd_pruning();
    test_ripemd_no_prune_frontier();
    test_evict_storage_stability();
    test_reverted_selfdestruct_no_leak();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
