/*
 * Test: shared storage trie refcount correctness
 *
 * Simulates multiple accounts sharing a storage mpt_store.
 * Creates accounts with overlapping storage keys to produce
 * shared subtree nodes, then verifies nodes survive after
 * individual account updates.
 */

#include "mpt_store.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TEST_PATH "/dev/shm/test_mpt_shared"
#define NUM_ACCOUNTS 1000
#define SLOTS_PER_ACCOUNT 50
#define NUM_ROUNDS 10000

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

/* Simple RLP encode: [key, value] leaf */
static uint32_t make_rlp(const uint8_t key[32], const uint8_t value[32],
                          uint8_t *out) {
    out[0] = 0xc0 + 66;  /* list of 66 bytes */
    out[1] = 0xa0;        /* 32-byte string */
    memcpy(out + 2, key, 32);
    out[34] = 0xa0;
    memcpy(out + 35, value, 32);
    return 67;
}

int main(void) {
    srand(42);

    char dat[256], free_p[256];
    snprintf(dat, sizeof(dat), "%s.dat", TEST_PATH);
    snprintf(free_p, sizeof(free_p), "%s.free", TEST_PATH);
    unlink(dat);
    unlink(free_p);

    mpt_store_t *ms = mpt_store_create(TEST_PATH, 100000);
    if (!ms) { fprintf(stderr, "FAIL: create\n"); return 1; }
    mpt_store_set_shared(ms, true);

    /* Storage roots per account */
    uint8_t roots[NUM_ACCOUNTS][32];
    memset(roots, 0, sizeof(roots));

    /* Generate fixed storage keys (same keys for all accounts — creates sharing) */
    uint8_t slot_keys[SLOTS_PER_ACCOUNT][32];
    for (int i = 0; i < SLOTS_PER_ACCOUNT; i++) {
        for (int j = 0; j < 32; j++)
            slot_keys[i][j] = (uint8_t)(i * 37 + j * 13);
        /* Hash the key like Ethereum does */
        uint8_t tmp[32];
        keccak(slot_keys[i], 32, tmp);
        memcpy(slot_keys[i], tmp, 32);
    }

    int errors = 0;

    /* Round 0: initialize all accounts with same storage values */
    fprintf(stderr, "Phase 1: Initialize %d accounts with %d slots each...\n",
            NUM_ACCOUNTS, SLOTS_PER_ACCOUNT);
    for (int a = 0; a < NUM_ACCOUNTS; a++) {
        mpt_store_set_root(ms, roots[a]);

        if (!mpt_store_begin_batch(ms)) {
            fprintf(stderr, "FAIL: begin_batch acct=%d\n", a);
            errors++;
            break;
        }

        for (int s = 0; s < SLOTS_PER_ACCOUNT; s++) {
            uint8_t value[32] = {0};
            value[0] = 0x01;  /* same value for all accounts initially */
            value[31] = (uint8_t)s;

            uint8_t rlp[80];
            uint32_t rlp_len = make_rlp(slot_keys[s], value, rlp);
            uint8_t hash[32];
            keccak(rlp, rlp_len, hash);
            mpt_store_update(ms, hash, rlp, rlp_len);
        }

        if (!mpt_store_commit_batch(ms)) {
            fprintf(stderr, "FAIL: commit acct=%d\n", a);
            errors++;
            break;
        }
        mpt_store_root(ms, roots[a]);
    }
    mpt_store_flush(ms);

    /* Verify all roots are the same (all accounts have same storage) */
    bool all_same = true;
    for (int a = 1; a < NUM_ACCOUNTS; a++) {
        if (memcmp(roots[0], roots[a], 32) != 0) {
            all_same = false;
            break;
        }
    }
    fprintf(stderr, "  All roots identical: %s\n", all_same ? "yes (shared nodes)" : "no");

    mpt_store_stats_t st = mpt_store_stats(ms);
    fprintf(stderr, "  Nodes: %lu\n", (unsigned long)st.node_count);

    /* Phase 2: Modify accounts one at a time — this creates divergence
     * and tests whether shared nodes survive */
    fprintf(stderr, "\nPhase 2: %d rounds of individual account updates...\n", NUM_ROUNDS);

    for (int round = 0; round < NUM_ROUNDS; round++) {
        /* Pick a random account and modify one random slot */
        int a = round % NUM_ACCOUNTS;
        int s = rand() % SLOTS_PER_ACCOUNT;

        mpt_store_set_root(ms, roots[a]);

        if (!mpt_store_begin_batch(ms)) {
            fprintf(stderr, "FAIL: begin_batch round=%d acct=%d\n", round, a);
            errors++;
            break;
        }

        /* New value unique to this round */
        uint8_t value[32] = {0};
        value[0] = (uint8_t)((round + 2) & 0xFF);
        value[1] = (uint8_t)((round + 2) >> 8);
        value[31] = (uint8_t)s;

        uint8_t rlp[80];
        uint32_t rlp_len = make_rlp(slot_keys[s], value, rlp);
        uint8_t hash[32];
        keccak(rlp, rlp_len, hash);
        mpt_store_update(ms, hash, rlp, rlp_len);

        if (!mpt_store_commit_batch(ms)) {
            fprintf(stderr, "FAIL: commit round=%d acct=%d — LOST NODE?\n", round, a);
            errors++;
            /* Don't break — continue to see how many fail */
            continue;
        }
        mpt_store_root(ms, roots[a]);

        /* Flush every 10 rounds (simulates checkpoint) */
        if ((round + 1) % 10 == 0) {
            mpt_store_flush(ms);
        }

        if ((round + 1) % 50 == 0) {
            st = mpt_store_stats(ms);
            fprintf(stderr, "  round %d: nodes=%lu\n",
                    round + 1, (unsigned long)st.node_count);
        }
    }
    mpt_store_flush(ms);

    /* Phase 3: Verify all accounts' tries are still accessible */
    fprintf(stderr, "\nPhase 3: Verify all account roots are loadable...\n");
    for (int a = 0; a < NUM_ACCOUNTS; a++) {
        mpt_store_set_root(ms, roots[a]);

        /* Try to update all slots — commit_batch will load existing nodes */
        if (!mpt_store_begin_batch(ms)) {
            fprintf(stderr, "FAIL: verify begin_batch acct=%d\n", a);
            errors++;
            continue;
        }

        for (int s = 0; s < SLOTS_PER_ACCOUNT; s++) {
            uint8_t value[32] = {0};
            value[0] = 0xFF;
            value[31] = (uint8_t)s;

            uint8_t rlp[80];
            uint32_t rlp_len = make_rlp(slot_keys[s], value, rlp);
            uint8_t hash[32];
            keccak(rlp, rlp_len, hash);
            mpt_store_update(ms, hash, rlp, rlp_len);
        }

        if (!mpt_store_commit_batch(ms)) {
            fprintf(stderr, "FAIL: verify commit acct=%d — LOST NODE\n", a);
            errors++;
        }
        mpt_store_root(ms, roots[a]);
    }
    mpt_store_flush(ms);

    /* Phase 4: Reopen and verify */
    fprintf(stderr, "\nPhase 4: Reopen and verify...\n");
    uint8_t saved_roots[NUM_ACCOUNTS][32];
    memcpy(saved_roots, roots, sizeof(roots));
    mpt_store_destroy(ms);

    ms = mpt_store_open(TEST_PATH);
    if (!ms) {
        fprintf(stderr, "FAIL: reopen\n");
        errors++;
    } else {
        mpt_store_set_shared(ms, true);
        /* Verify each root by doing one more update round */
        for (int a = 0; a < NUM_ACCOUNTS; a++) {
            mpt_store_set_root(ms, saved_roots[a]);
            if (!mpt_store_begin_batch(ms)) {
                fprintf(stderr, "FAIL: reopen verify begin acct=%d\n", a);
                errors++;
                continue;
            }
            uint8_t value[32] = {0};
            value[0] = 0xAA;
            uint8_t rlp[80];
            uint32_t rlp_len = make_rlp(slot_keys[0], value, rlp);
            uint8_t hash[32];
            keccak(rlp, rlp_len, hash);
            mpt_store_update(ms, hash, rlp, rlp_len);

            if (!mpt_store_commit_batch(ms)) {
                fprintf(stderr, "FAIL: reopen verify commit acct=%d — LOST NODE\n", a);
                errors++;
            }
        }
        mpt_store_destroy(ms);
    }

    /* Cleanup */
    unlink(dat);
    unlink(free_p);

    if (errors > 0) {
        fprintf(stderr, "\nFAIL: %d errors\n", errors);
        return 1;
    }
    fprintf(stderr, "\nPASS\n");
    return 0;
}
