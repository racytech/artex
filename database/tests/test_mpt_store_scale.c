/*
 * MPT Store Scale Stress Test — Matches Real chain_replay Execution Flow
 *
 * Simulates the checkpoint loop:
 *   1. For each dirty account: set_root → begin_batch → update slots → commit_batch
 *   2. flush()
 *   3. Repeat
 *
 * Detects LOST NODEs via the commit stats counter.
 *
 * Usage:
 *   ./test_mpt_store_scale [num_accounts] [slots_per_account] [num_checkpoints]
 */

#include "mpt_store.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <inttypes.h>

#define TEST_PATH "/dev/shm/test_mpt_scale"

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

static uint64_t rng_state[4];

static void rng_seed(uint64_t seed) {
    rng_state[0] = seed;
    rng_state[1] = seed ^ 0x6a09e667f3bcc908ULL;
    rng_state[2] = seed ^ 0xbb67ae8584caa73bULL;
    rng_state[3] = seed ^ 0x3c6ef372fe94f82bULL;
    for (int i = 0; i < 20; i++) {
        uint64_t t = rng_state[1] << 17;
        rng_state[2] ^= rng_state[0];
        rng_state[3] ^= rng_state[1];
        rng_state[1] ^= rng_state[2];
        rng_state[0] ^= rng_state[3];
        rng_state[2] ^= t;
        rng_state[3] = (rng_state[3] << 45) | (rng_state[3] >> 19);
    }
}

static uint64_t rng_next(void) {
    uint64_t result = rng_state[1] * 5;
    result = ((result << 7) | (result >> 57)) * 9;
    uint64_t t = rng_state[1] << 17;
    rng_state[2] ^= rng_state[0];
    rng_state[3] ^= rng_state[1];
    rng_state[1] ^= rng_state[2];
    rng_state[0] ^= rng_state[3];
    rng_state[2] ^= t;
    rng_state[3] = (rng_state[3] << 45) | (rng_state[3] >> 19);
    return result;
}

static uint64_t rng_range(uint64_t n) { return rng_next() % n; }

static void make_slot_key(uint32_t account, uint32_t slot, uint8_t out[32]) {
    uint8_t input[8];
    input[0] = (uint8_t)(account >> 24);
    input[1] = (uint8_t)(account >> 16);
    input[2] = (uint8_t)(account >> 8);
    input[3] = (uint8_t)(account);
    input[4] = (uint8_t)(slot >> 24);
    input[5] = (uint8_t)(slot >> 16);
    input[6] = (uint8_t)(slot >> 8);
    input[7] = (uint8_t)(slot);
    keccak(input, 8, out);
}

static uint32_t make_slot_value(uint32_t slot, uint32_t version, uint8_t *out) {
    uint32_t val_bytes;
    switch (slot % 5) {
        case 0: val_bytes = 1;  break;
        case 1: val_bytes = 8;  break;
        case 2: val_bytes = 20; break;
        case 3: val_bytes = 32; break;
        default: val_bytes = 4;
    }
    out[0] = 0x80 + (uint8_t)val_bytes;
    for (uint32_t i = 0; i < val_bytes; i++)
        out[1 + i] = (uint8_t)(version ^ slot ^ i);
    return 1 + val_bytes;
}

static const uint8_t EMPTY_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

static double elapsed_sec(struct timespec *t0) {
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    return (t1.tv_sec - t0->tv_sec) + (t1.tv_nsec - t0->tv_nsec) / 1e9;
}

int main(int argc, char **argv) {
    uint32_t num_accounts       = 100000;
    uint32_t slots_per_account  = 20;
    uint32_t num_checkpoints    = 100;

    if (argc >= 4) {
        num_accounts      = (uint32_t)atoi(argv[1]);
        slots_per_account = (uint32_t)atoi(argv[2]);
        num_checkpoints   = (uint32_t)atoi(argv[3]);
    }

    rng_seed(42);

    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    fprintf(stderr, "=== MPT Store Checkpoint Stress Test ===\n");
    fprintf(stderr, "  accounts=%u  slots/acct=%u  checkpoints=%u\n",
            num_accounts, slots_per_account, num_checkpoints);

    char dat[256], free_p[256];
    snprintf(dat, sizeof(dat), "%s.dat", TEST_PATH);
    snprintf(free_p, sizeof(free_p), "%s.free", TEST_PATH);
    unlink(dat); unlink(free_p);

    mpt_store_t *storage_mpt = mpt_store_create(TEST_PATH, 0);
    if (!storage_mpt) { fprintf(stderr, "FAIL: create\n"); return 1; }
    mpt_store_set_shared(storage_mpt, true);

    uint8_t *roots = malloc((size_t)num_accounts * 32);
    for (uint32_t a = 0; a < num_accounts; a++)
        memcpy(roots + a * 32, EMPTY_ROOT, 32);

    uint64_t total_commits = 0, total_updates = 0;
    uint32_t total_lost = 0;
    uint32_t live_accounts = 0;

    for (uint32_t cp = 0; cp < num_checkpoints; cp++) {
        mpt_store_reset_commit_stats(storage_mpt);

        /* Create new accounts */
        uint32_t new_accts = num_accounts / num_checkpoints;
        if (new_accts < 100) new_accts = 100;
        if (live_accounts + new_accts > num_accounts)
            new_accts = num_accounts - live_accounts;
        uint32_t first_new = live_accounts;
        live_accounts += new_accts;

        /* New accounts: full initialization.
         * All accounts use the same slot indices (0..slots_per_account)
         * but different values → creates shared internal nodes when
         * different accounts produce identical subtrees. */
        for (uint32_t i = 0; i < new_accts; i++) {
            uint32_t a = first_new + i;
            mpt_store_set_root(storage_mpt, roots + a * 32);
            if (!mpt_store_begin_batch(storage_mpt)) goto fail;

            for (uint32_t s = 0; s < slots_per_account; s++) {
                uint32_t slot = s;
                uint8_t key[32]; make_slot_key(a, slot, key);
                uint8_t rlp[64];
                uint32_t rlp_len = make_slot_value(slot, cp + 1, rlp);
                mpt_store_update(storage_mpt, key, rlp, rlp_len);
                total_updates++;
            }
            if (!mpt_store_commit_batch(storage_mpt)) goto fail;
            mpt_store_root(storage_mpt, roots + a * 32);
            total_commits++;
        }

        /* Existing accounts: update a large fraction of live accounts.
         * In chain_replay, 50-100K accounts can be dirty per checkpoint.
         * Use 50% to create heavy deletion pressure in one flush window. */
        uint32_t re_dirty = live_accounts / 2;
        if (re_dirty < 10) re_dirty = 10;

        for (uint32_t i = 0; i < re_dirty; i++) {
            uint32_t a = (uint32_t)rng_range(live_accounts);
            mpt_store_set_root(storage_mpt, roots + a * 32);
            if (!mpt_store_begin_batch(storage_mpt)) goto fail;

            /* 20% chance of full re-write (simulates contract SSTORE storm) */
            bool full_rewrite = (rng_range(5) == 0);
            uint32_t n_slots = full_rewrite
                             ? slots_per_account
                             : (uint32_t)(rng_range(5) + 1);

            for (uint32_t s = 0; s < n_slots; s++) {
                uint32_t slot = full_rewrite ? s : (uint32_t)rng_range(slots_per_account);
                uint8_t key[32]; make_slot_key(a, slot, key);
                uint8_t rlp[64];
                uint32_t rlp_len = make_slot_value(slot, cp + 1, rlp);
                mpt_store_update(storage_mpt, key, rlp, rlp_len);
                total_updates++;
            }
            if (!mpt_store_commit_batch(storage_mpt)) goto fail;
            mpt_store_root(storage_mpt, roots + a * 32);
            total_commits++;
        }

        /* Flush (checkpoint) */
        mpt_store_flush(storage_mpt);

        /* Check for LOST NODEs this checkpoint */
        uint32_t cp_lost = mpt_store_get_commit_stats(storage_mpt).lost_nodes;
        total_lost += cp_lost;

        if (cp_lost > 0) {
            fprintf(stderr, "  cp %u: ** %u LOST NODEs ** (total=%u) — FAIL FAST\n",
                    cp + 1, cp_lost, total_lost);
            goto fail;
        }

        if ((cp + 1) % 10 == 0 || cp == 0) {
            mpt_store_stats_t st = mpt_store_stats(storage_mpt);
            fprintf(stderr, "  cp %u: live=%u nodes=%lu dat=%.1fMB commits=%lu (%.1fs)\n",
                    cp + 1, live_accounts,
                    (unsigned long)st.node_count,
                    (double)st.data_file_size / (1024 * 1024),
                    (unsigned long)total_commits, elapsed_sec(&t0));
        }
    }

    /* --- Final stress: re-commit ALL accounts in one window, then verify --- */
    fprintf(stderr, "\nFinal: re-commit all %u accounts + flush + verify...\n", live_accounts);
    mpt_store_reset_commit_stats(storage_mpt);
    for (uint32_t a = 0; a < live_accounts; a++) {
        mpt_store_set_root(storage_mpt, roots + a * 32);
        if (!mpt_store_begin_batch(storage_mpt)) goto fail;
        for (uint32_t s = 0; s < slots_per_account; s++) {
            uint8_t key[32]; make_slot_key(a, s, key);
            uint8_t rlp[64];
            uint32_t rlp_len = make_slot_value(s, num_checkpoints + 1, rlp);
            mpt_store_update(storage_mpt, key, rlp, rlp_len);
        }
        if (!mpt_store_commit_batch(storage_mpt)) goto fail;
        mpt_store_root(storage_mpt, roots + a * 32);
    }
    mpt_store_flush(storage_mpt);
    uint32_t final_lost1 = mpt_store_get_commit_stats(storage_mpt).lost_nodes;
    fprintf(stderr, "  After re-commit+flush: %u LOST NODEs\n", final_lost1);
    total_lost += final_lost1;

    /* Now re-commit all again — do the old roots still work? */
    mpt_store_reset_commit_stats(storage_mpt);
    for (uint32_t a = 0; a < live_accounts; a++) {
        mpt_store_set_root(storage_mpt, roots + a * 32);
        if (!mpt_store_begin_batch(storage_mpt)) goto fail;
        uint8_t key[32]; make_slot_key(a, 0, key);
        uint8_t rlp[64];
        uint32_t rlp_len = make_slot_value(0, num_checkpoints + 2, rlp);
        mpt_store_update(storage_mpt, key, rlp, rlp_len);
        if (!mpt_store_commit_batch(storage_mpt)) goto fail;
        mpt_store_root(storage_mpt, roots + a * 32);
    }
    mpt_store_flush(storage_mpt);
    uint32_t final_lost2 = mpt_store_get_commit_stats(storage_mpt).lost_nodes;
    fprintf(stderr, "  After 2nd re-commit+flush: %u LOST NODEs\n", final_lost2);
    total_lost += final_lost2;

    if (total_lost > 0) goto fail;

    mpt_store_destroy(storage_mpt);
    free(roots);
    unlink(dat); unlink(free_p);

    fprintf(stderr, "\n=== PASS (%.1fs, commits=%lu, updates=%lu, lost=0) ===\n",
            elapsed_sec(&t0), (unsigned long)total_commits, (unsigned long)total_updates);
    return 0;

fail:
    mpt_store_destroy(storage_mpt);
    free(roots);
    unlink(dat); unlink(free_p);

    fprintf(stderr, "\n=== FAIL (%.1fs, commits=%lu, updates=%lu, lost=%u) ===\n",
            elapsed_sec(&t0), (unsigned long)total_commits,
            (unsigned long)total_updates, total_lost);
    return 1;
}
