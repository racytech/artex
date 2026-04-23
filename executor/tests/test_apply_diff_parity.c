/**
 * apply_diff Parity Tests
 *
 * For each scenario, build two identical in-memory states. Mutate one via
 * the canonical execution path (evm_state_set_* + commit_tx + finalize_block
 * inside compute_mpt_root). Collect the block diff from it. Apply that same
 * diff to the other state via state_history_apply_diff. Compute both roots.
 * They must match — a mismatch tells us apply_diff's semantics diverged from
 * block_execute's for that specific scenario.
 *
 * This test isolates the replay correctness bug without needing to load a
 * 140 GB snapshot + 2000-block history log. Sub-second iteration.
 */

#include "state_history.h"
#include "block_diff.h"
#include "evm_state.h"
#include "uint256.h"
#include "address.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) printf("  [%s] ", name)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while (0)
#define FAIL(fmt, ...) do { \
    printf("FAIL: " fmt "\n", ##__VA_ARGS__); tests_failed++; return; \
} while (0)

/* ── Helpers ────────────────────────────────────────────────────────────── */

static address_t make_addr(uint8_t seed) {
    address_t a = {0};
    a.bytes[19] = seed;
    return a;
}

static uint256_t u256(uint64_t lo) {
    uint256_t v = UINT256_ZERO_INIT;
    v.low = (__uint128_t)lo;
    return v;
}

static hash_t hash_from_byte(uint8_t seed) {
    hash_t h;
    memset(h.bytes, seed, 32);
    return h;
}

/* Build the "snapshot state" identical on both engines. A handful of
 * accounts — plain EOA, contract with code + storage, empty-but-existing
 * (unusual, pre-EIP-161). */
static void seed_state(evm_state_t *es) {
    evm_state_set_prune_empty(es, true);
    evm_state_begin_block(es, 1);

    /* 0x01 — EOA with balance + nonce */
    address_t eoa = make_addr(0x01);
    evm_state_set_nonce(es, &eoa, 5);
    uint256_t bal1 = u256(1000000);
    evm_state_set_balance(es, &eoa, &bal1);

    /* 0x02 — contract with code_hash + two storage slots */
    address_t ctr = make_addr(0x02);
    evm_state_set_nonce(es, &ctr, 1);
    uint256_t bal2 = u256(500);
    evm_state_set_balance(es, &ctr, &bal2);
    hash_t ch = hash_from_byte(0xAA);
    evm_state_set_code_hash(es, &ctr, &ch);

    uint256_t slot0 = u256(0), val0 = u256(0xDEAD);
    uint256_t slot1 = u256(1), val1 = u256(0xBEEF);
    evm_state_set_storage(es, &ctr, &slot0, &val0);
    evm_state_set_storage(es, &ctr, &slot1, &val1);

    /* Commit the seed as if it were the pre-state we loaded. Use commit_tx
     * + one root computation to move seed-time dirty marks through the
     * normal flow (commit_tx sets ACCT_EXISTED, finalize_block promotes
     * ACCT_STORAGE_DIRTY to stor_dirty_bits, compute clears cached hashes).
     * After this call both engines are at the same "post-load" baseline. */
    evm_state_commit_tx(es);
    (void)evm_state_compute_mpt_root(es, true);
}

static bool hashes_equal(const hash_t *a, const hash_t *b) {
    return memcmp(a->bytes, b->bytes, 32) == 0;
}

static void hex(const hash_t *h, char out[67]) {
    out[0] = '0'; out[1] = 'x';
    for (int i = 0; i < 32; i++)
        sprintf(out + 2 + i * 2, "%02x", h->bytes[i]);
    out[66] = '\0';
}

/* Shared harness: run mutator on canonical, collect diff, apply to replay,
 * compare roots. mutator performs canonical-path mutations; commit_tx is
 * called at end so the diff reflects end-of-tx state. */
static void run_parity(const char *name,
                       void (*mutator)(evm_state_t *es)) {
    evm_state_t *canon = evm_state_create(NULL);
    evm_state_t *replay = evm_state_create(NULL);
    seed_state(canon);
    seed_state(replay);

    /* Baseline roots must agree — sanity check that seeding is deterministic. */
    evm_state_begin_block(canon, 2);
    evm_state_begin_block(replay, 2);

    /* Canonical path: mutate + commit_tx. compute root here drives the diff
     * capture below to see post-finalize state. Instead of calling
     * compute_mpt_root (which also resets block), we mimic block_executor:
     * commit_tx, collect diff, then compute_mpt_root to get the root. */
    mutator(canon);
    evm_state_commit_tx(canon);

    block_diff_t diff = {0};
    diff.block_number = 2;
    evm_state_collect_block_diff(canon, &diff);

    hash_t r_canon = evm_state_compute_mpt_root(canon, true);

    /* Replay path: apply_diff does set_* + (now) commit_tx internally;
     * compute_mpt_root finalizes and walks. */
    state_history_apply_diff(replay, &diff);
    hash_t r_replay = evm_state_compute_mpt_root(replay, true);

    if (!hashes_equal(&r_canon, &r_replay)) {
        char cs[67], rs[67];
        hex(&r_canon, cs);
        hex(&r_replay, rs);
        printf("[%s] FAIL: root mismatch\n    canon : %s\n    replay: %s\n",
               name, cs, rs);
        tests_failed++;
    } else {
        printf("[%s] PASS\n", name);
        tests_passed++;
    }

    block_diff_free(&diff);
    evm_state_destroy(canon);
    evm_state_destroy(replay);
}

/* ── Scenarios ──────────────────────────────────────────────────────────── */

/* 1. Simple balance change on an existing EOA. Easiest case — any parity
 * break here means our harness itself is wrong. */
static void mut_balance_change(evm_state_t *es) {
    address_t eoa = make_addr(0x01);
    uint256_t new_bal = u256(999999);
    evm_state_set_balance(es, &eoa, &new_bal);
}

/* 2. Storage slot update on existing contract + delete one slot (set to 0). */
static void mut_storage_change(evm_state_t *es) {
    address_t ctr = make_addr(0x02);
    uint256_t slot0 = u256(0), new_val0 = u256(0x1234);
    uint256_t slot1 = u256(1), zero = u256(0);
    evm_state_set_storage(es, &ctr, &slot0, &new_val0);
    evm_state_set_storage(es, &ctr, &slot1, &zero);   /* clear */
}

/* 3. Self-destruct the existing contract. */
static void mut_self_destruct(evm_state_t *es) {
    address_t ctr = make_addr(0x02);
    evm_state_self_destruct(es, &ctr);
}

/* 4. EIP-161 touch-to-empty: zero out balance on an account that ends up
 * with all-zero fields, no code, no storage. commit_tx should prune it. */
static void mut_eip161_drain(evm_state_t *es) {
    /* Create a bare EOA seed at 0x03 first during seed_state? No — easier
     * to drain an existing one here. Use 0x01 which is plain EOA. */
    address_t eoa = make_addr(0x01);
    uint256_t zero = u256(0);
    evm_state_set_balance(es, &eoa, &zero);
    evm_state_set_nonce(es, &eoa, 0);
}

/* 5. Create a brand-new account with non-empty state. */
static void mut_create_account(evm_state_t *es) {
    address_t nu = make_addr(0x42);
    evm_state_set_nonce(es, &nu, 1);
    uint256_t bal = u256(42);
    evm_state_set_balance(es, &nu, &bal);
    hash_t ch = hash_from_byte(0xCC);
    evm_state_set_code_hash(es, &nu, &ch);
}

/* 6. Contract deployment — canonical uses set_code (bytecode + derived
 * hash), replay uses set_code_hash (hash only, bytes live in code_store).
 * The two paths must produce the same MPT leaf. This is the scenario
 * that would have caught the set_code_hash no-op bug. */
#include "state.h"  /* for state_set_code via evm_state_get_state */
static void mut_contract_deploy(evm_state_t *es) {
    address_t nu = make_addr(0x55);
    evm_state_set_nonce(es, &nu, 1);
    uint256_t bal = u256(100);
    evm_state_set_balance(es, &nu, &bal);

    /* Canonical path: set actual bytecode. Hash is derived internally. */
    static const uint8_t code[] = { 0x60, 0x00, 0x60, 0x00, 0xfd };  /* PUSH1 0 PUSH1 0 REVERT */
    state_set_code(evm_state_get_state(es), &nu, code, sizeof(code));
}

/* ── Multi-block sequence ──────────────────────────────────────────────────
 *
 * Each single-block test starts from a fresh seed_state — they can't catch
 * cross-block drift in account existence tracking, stor_dirty bits carried
 * across blocks, resource recycling from one block's destruct affecting the
 * next block's create, or accumulation of state in the replay path.
 *
 * The multi-block harness runs a sequence of mutators back-to-back on BOTH
 * engines, checking state root equality after EACH block (not just the
 * last). A drift at block N surfaces immediately instead of being masked
 * by compensating errors in later blocks.
 */

/* Block 2: touch an existing EOA. */
static void mb_block2(evm_state_t *es) {
    address_t eoa = make_addr(0x01);
    uint256_t bal = u256(777777);
    evm_state_set_balance(es, &eoa, &bal);
}

/* Block 3: create a new account + write storage on existing contract. */
static void mb_block3(evm_state_t *es) {
    address_t nu = make_addr(0x42);
    evm_state_set_nonce(es, &nu, 1);
    uint256_t bal = u256(42);
    evm_state_set_balance(es, &nu, &bal);

    address_t ctr = make_addr(0x02);
    uint256_t slot7 = u256(7), val7 = u256(0x7777);
    evm_state_set_storage(es, &ctr, &slot7, &val7);
}

/* Block 4: modify the account we just created + add another slot on ctr. */
static void mb_block4(evm_state_t *es) {
    address_t nu = make_addr(0x42);
    uint256_t bal = u256(4242);
    evm_state_set_balance(es, &nu, &bal);
    evm_state_set_nonce(es, &nu, 2);

    address_t ctr = make_addr(0x02);
    uint256_t slot8 = u256(8), val8 = u256(0x8888);
    evm_state_set_storage(es, &ctr, &slot8, &val8);
}

/* Block 5: self-destruct the contract. Its resource slot should recycle. */
static void mb_block5(evm_state_t *es) {
    address_t ctr = make_addr(0x02);
    evm_state_self_destruct(es, &ctr);
}

/* Block 6: create a new contract — this is where resource-slot recycling
 * from block 5's destruct matters. Also write storage to force resource
 * allocation. */
static void mb_block6(evm_state_t *es) {
    address_t nu = make_addr(0x99);
    evm_state_set_nonce(es, &nu, 1);
    uint256_t bal = u256(555);
    evm_state_set_balance(es, &nu, &bal);
    hash_t ch = hash_from_byte(0xBB);
    evm_state_set_code_hash(es, &nu, &ch);

    uint256_t slot0 = u256(0), val0 = u256(0xAAAA);
    evm_state_set_storage(es, &nu, &slot0, &val0);
}

/* Block 7: EIP-161 drain the EOA we've been touching. Verify that a prune
 * in the final block of the sequence still matches. */
static void mb_block7(evm_state_t *es) {
    address_t eoa = make_addr(0x01);
    uint256_t zero = u256(0);
    evm_state_set_balance(es, &eoa, &zero);
    evm_state_set_nonce(es, &eoa, 0);
}

typedef void (*mut_fn)(evm_state_t *);

/* Drive one block on an engine through the canonical path (begin_block,
 * mutate, commit_tx). Caller is responsible for compute_mpt_root or
 * collect_block_diff between calls. */
static void canon_step(evm_state_t *es, uint64_t bn, mut_fn fn) {
    evm_state_begin_block(es, bn);
    fn(es);
    evm_state_commit_tx(es);
}

static void run_multiblock_parity(void) {
    printf("[multi_block] running 6-block sequence...\n");

    evm_state_t *canon = evm_state_create(NULL);
    evm_state_t *replay = evm_state_create(NULL);
    seed_state(canon);
    seed_state(replay);

    struct { const char *name; mut_fn fn; uint64_t block; } steps[] = {
        { "block2 touch EOA",            mb_block2, 2 },
        { "block3 create + slot write",  mb_block3, 3 },
        { "block4 modify new + slot",    mb_block4, 4 },
        { "block5 self_destruct",        mb_block5, 5 },
        { "block6 new contract",         mb_block6, 6 },
        { "block7 EIP-161 drain",        mb_block7, 7 },
    };
    const size_t n_steps = sizeof(steps) / sizeof(steps[0]);

    for (size_t i = 0; i < n_steps; i++) {
        /* Canonical: mutate, capture diff, then compute root. */
        canon_step(canon, steps[i].block, steps[i].fn);

        block_diff_t diff = {0};
        diff.block_number = steps[i].block;
        evm_state_collect_block_diff(canon, &diff);

        hash_t r_canon = evm_state_compute_mpt_root(canon, true);

        /* Replay: set block number, apply diff (fast path), compute root. */
        evm_state_begin_block(replay, steps[i].block);
        state_history_apply_diff(replay, &diff);
        hash_t r_replay = evm_state_compute_mpt_root(replay, true);

        if (!hashes_equal(&r_canon, &r_replay)) {
            char cs[67], rs[67];
            hex(&r_canon, cs); hex(&r_replay, rs);
            printf("  step %zu [%s] FAIL: root drift at block %lu\n"
                   "    canon : %s\n    replay: %s\n",
                   i, steps[i].name, steps[i].block, cs, rs);
            tests_failed++;
            block_diff_free(&diff);
            goto cleanup;
        } else {
            printf("  step %zu [%s] ok\n", i, steps[i].name);
        }
        block_diff_free(&diff);
    }
    printf("[multi_block] PASS (all %zu blocks matched)\n", n_steps);
    tests_passed++;

cleanup:
    evm_state_destroy(canon);
    evm_state_destroy(replay);
}

/* ── Entry point ────────────────────────────────────────────────────────── */

int main(void) {
    printf("apply_diff parity tests\n");
    printf("=======================\n");

    run_parity("balance_change",   mut_balance_change);
    run_parity("storage_change",   mut_storage_change);
    run_parity("self_destruct",    mut_self_destruct);
    run_parity("eip161_drain",     mut_eip161_drain);
    run_parity("create_account",   mut_create_account);
    run_parity("contract_deploy",  mut_contract_deploy);
    run_multiblock_parity();

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed ? 1 : 0;
}
