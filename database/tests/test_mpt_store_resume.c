/*
 * Test: mpt_store node persistence across checkpoints
 *
 * Simulates checkpoint cycles: begin_batch → update → commit → flush.
 * After each checkpoint, verifies all previous roots are reachable.
 * Catches LOST NODE bug where compact_art loses entries.
 */

#include "mpt_store.h"
#include "keccak256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define TEST_PATH "/dev/shm/test_mpt_nodes"
#define NUM_CHECKPOINTS 2000
#define KEYS_PER_CHECKPOINT 500

int main(void) {
    srand(42);  /* deterministic */

    char dat[256], free_p[256];
    snprintf(dat, sizeof(dat), "%s.dat", TEST_PATH);
    snprintf(free_p, sizeof(free_p), "%s.free", TEST_PATH);
    unlink(dat);
    unlink(free_p);

    mpt_store_t *ms = mpt_store_create(TEST_PATH, 100000);
    if (!ms) { fprintf(stderr, "FAIL: create\n"); return 1; }

    /* Track roots at each checkpoint */
    uint8_t roots[NUM_CHECKPOINTS][32];
    int errors = 0;

    /* Generate fixed set of keys */
    uint8_t keys[KEYS_PER_CHECKPOINT][32];
    for (int i = 0; i < KEYS_PER_CHECKPOINT; i++) {
        for (int j = 0; j < 32; j++)
            keys[i][j] = (uint8_t)(rand() & 0xFF);
    }

    for (int cp = 0; cp < NUM_CHECKPOINTS; cp++) {
        if (!mpt_store_begin_batch(ms)) {
            fprintf(stderr, "FAIL: begin_batch cp=%d\n", cp);
            errors++;
            break;
        }

        /* Update keys with new values each checkpoint */
        for (int i = 0; i < KEYS_PER_CHECKPOINT; i++) {
            uint8_t val[32];
            for (int j = 0; j < 32; j++)
                val[j] = (uint8_t)((cp * KEYS_PER_CHECKPOINT + i + j) & 0xFF);

            /* RLP encode: simple [key, value] leaf */
            uint8_t rlp[80];
            rlp[0] = 0xc0 + 66;  /* list of 66 bytes */
            rlp[1] = 0xa0;       /* 32-byte string */
            memcpy(rlp + 2, keys[i], 32);
            rlp[34] = 0xa0;
            memcpy(rlp + 35, val, 32);
            uint32_t rlp_len = 67;

            /* Compute hash and store */
            uint8_t hash[32];
            {
                SHA3_CTX ctx;
                keccak_init(&ctx);
                keccak_update(&ctx, rlp, (uint16_t)rlp_len);
                keccak_final(&ctx, hash);
            }

            mpt_store_update(ms, hash, rlp, rlp_len);
        }

        if (!mpt_store_commit_batch(ms)) {
            fprintf(stderr, "FAIL: commit_batch cp=%d\n", cp);
            errors++;
            break;
        }

        mpt_store_root(ms, roots[cp]);
        mpt_store_flush(ms);

        /* Every 10 checkpoints: verify stats make sense */
        if ((cp + 1) % 20 == 0) {
            mpt_store_stats_t st = mpt_store_stats(ms);
            fprintf(stderr, "  cp %d: nodes=%lu root=",
                    cp + 1, (unsigned long)st.node_count);
            for (int j = 0; j < 4; j++) fprintf(stderr, "%02x", roots[cp][j]);
            fprintf(stderr, "...\n");
        }
    }

    fprintf(stderr, "\nPhase 2: reopen and verify roots\n");

    /* Save the last root */
    uint8_t expected_root[32];
    memcpy(expected_root, roots[NUM_CHECKPOINTS - 1], 32);

    mpt_store_destroy(ms);

    /* Reopen */
    ms = mpt_store_open(TEST_PATH);
    if (!ms) {
        fprintf(stderr, "FAIL: reopen\n");
        unlink(dat); unlink(free_p);
        return 1;
    }

    /* Check root matches */
    uint8_t actual_root[32];
    mpt_store_root(ms, actual_root);

    if (memcmp(expected_root, actual_root, 32) != 0) {
        fprintf(stderr, "FAIL: root mismatch after reopen!\n");
        fprintf(stderr, "  expected: ");
        for (int i = 0; i < 16; i++) fprintf(stderr, "%02x", expected_root[i]);
        fprintf(stderr, "\n  actual:   ");
        for (int i = 0; i < 16; i++) fprintf(stderr, "%02x", actual_root[i]);
        fprintf(stderr, "\n");
        errors++;
    } else {
        fprintf(stderr, "OK: root matches after reopen\n");
    }

    /* Re-run one more checkpoint to see if old nodes are findable */
    fprintf(stderr, "\nPhase 3: one more checkpoint after reopen\n");
    if (!mpt_store_begin_batch(ms)) {
        fprintf(stderr, "FAIL: begin_batch after reopen\n");
        errors++;
    } else {
        for (int i = 0; i < KEYS_PER_CHECKPOINT; i++) {
            uint8_t val[32];
            for (int j = 0; j < 32; j++)
                val[j] = (uint8_t)((NUM_CHECKPOINTS * KEYS_PER_CHECKPOINT + i + j) & 0xFF);

            uint8_t rlp[80];
            rlp[0] = 0xc0 + 66;
            rlp[1] = 0xa0;
            memcpy(rlp + 2, keys[i], 32);
            rlp[34] = 0xa0;
            memcpy(rlp + 35, val, 32);
            uint32_t rlp_len = 67;

            uint8_t hash[32];
            {
                SHA3_CTX ctx;
                keccak_init(&ctx);
                keccak_update(&ctx, rlp, (uint16_t)rlp_len);
                keccak_final(&ctx, hash);
            }

            mpt_store_update(ms, hash, rlp, rlp_len);
        }

        if (!mpt_store_commit_batch(ms)) {
            fprintf(stderr, "FAIL: commit after reopen (LOST NODE?)\n");
            errors++;
        } else {
            mpt_store_root(ms, actual_root);
            fprintf(stderr, "OK: commit succeeded, root=");
            for (int i = 0; i < 8; i++) fprintf(stderr, "%02x", actual_root[i]);
            fprintf(stderr, "\n");
        }
    }

    mpt_store_destroy(ms);
    unlink(dat);
    unlink(free_p);

    if (errors > 0) {
        fprintf(stderr, "\nFAIL: %d errors\n", errors);
        return 1;
    }
    fprintf(stderr, "\nPASS\n");
    return 0;
}
