/*
 * profile_mpt_root — Profile where mpt_store_commit_batch spends time.
 *
 * Builds a realistic trie (N accounts, each with a storage trie), then
 * runs batched updates measuring:
 *   - Storage trie commits (per-account root switching + commit)
 *   - Account trie staging (RLP encode + update)
 *   - Account trie commit (sort + dedup + trie walk + hash)
 *   - Cache hit rates during each phase
 *
 * Usage:
 *   profile_mpt_root [--accounts 10000] [--dirty 200] [--slots-per 10] [--rounds 20]
 */

#include "mpt_store.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

/* =========================================================================
 * PRNG — splitmix64
 * ========================================================================= */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void rng_bytes(rng_t *rng, uint8_t *buf, size_t n) {
    for (size_t i = 0; i < n; i += 8) {
        uint64_t v = rng_next(rng);
        size_t c = n - i < 8 ? n - i : 8;
        memcpy(buf + i, &v, c);
    }
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

static double ms_diff(struct timespec *a, struct timespec *b) {
    return (b->tv_sec - a->tv_sec) * 1000.0 +
           (b->tv_nsec - a->tv_nsec) / 1e6;
}

/* Make a deterministic 32-byte key from an index */
static void make_key(uint64_t idx, uint8_t key[32]) {
    uint8_t buf[8];
    memcpy(buf, &idx, 8);
    hash_t h = hash_keccak256(buf, 8);
    memcpy(key, h.bytes, 32);
}

/* Make a fake account RLP (~70-104 bytes) */
static size_t make_account_rlp(rng_t *rng, const uint8_t storage_root[32],
                                uint8_t *out) {
    /* Simplified: [nonce, balance, storage_root, code_hash] as RLP list */
    uint64_t nonce = rng_next(rng) % 1000;
    uint8_t balance[8];
    rng_bytes(rng, balance, 8);

    /* Build a rough RLP — not perfectly valid but same size profile */
    size_t p = 0;
    out[p++] = 0xf8; /* long list prefix */
    size_t len_pos = p++;

    /* nonce */
    if (nonce == 0) { out[p++] = 0x80; }
    else if (nonce < 128) { out[p++] = (uint8_t)nonce; }
    else {
        int nb = 0; uint64_t tmp = nonce;
        while (tmp) { nb++; tmp >>= 8; }
        out[p++] = 0x80 + nb;
        for (int i = nb - 1; i >= 0; i--)
            out[p++] = (uint8_t)(nonce >> (i * 8));
    }

    /* balance (8 bytes) */
    out[p++] = 0x88;
    memcpy(out + p, balance, 8); p += 8;

    /* storage_root (32 bytes) */
    out[p++] = 0xa0;
    memcpy(out + p, storage_root, 32); p += 32;

    /* code_hash (32 bytes — empty) */
    out[p++] = 0xa0;
    /* keccak256("") */
    static const uint8_t empty_code[32] = {
        0xc5,0xd2,0x46,0x01,0x86,0xf7,0x23,0x3c,
        0x92,0x7e,0x7d,0xb2,0xdc,0xc7,0x03,0xc0,
        0xe5,0x00,0xb6,0x53,0xca,0x82,0x27,0x3b,
        0x7b,0xfa,0xd8,0x04,0x5d,0x85,0xa4,0x70
    };
    memcpy(out + p, empty_code, 32); p += 32;

    out[len_pos] = (uint8_t)(p - len_pos - 1);
    return p;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    int total_accounts = 50000;
    int dirty_per_round = 200;
    int slots_per_account = 5;
    int rounds = 20;

    static struct option long_opts[] = {
        {"accounts", required_argument, 0, 'a'},
        {"dirty",    required_argument, 0, 'd'},
        {"slots-per",required_argument, 0, 's'},
        {"rounds",   required_argument, 0, 'r'},
        {0, 0, 0, 0}
    };

    int c;
    while ((c = getopt_long(argc, argv, "a:d:s:r:", long_opts, NULL)) != -1) {
        switch (c) {
        case 'a': total_accounts = atoi(optarg); break;
        case 'd': dirty_per_round = atoi(optarg); break;
        case 's': slots_per_account = atoi(optarg); break;
        case 'r': rounds = atoi(optarg); break;
        }
    }

    fprintf(stderr, "Profile MPT root computation\n");
    fprintf(stderr, "  accounts=%d  dirty/round=%d  slots/account=%d  rounds=%d\n\n",
            total_accounts, dirty_per_round, slots_per_account, rounds);

    /* Create stores in /tmp */
    const char *acct_path = "/tmp/profile_mpt_acct";
    const char *stor_path = "/tmp/profile_mpt_stor";

    /* Clean up old files */
    char buf[256];
    snprintf(buf, sizeof(buf), "%s.idx", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.dat", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.free", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.idx", stor_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.dat", stor_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.free", stor_path); unlink(buf);

    mpt_store_t *acct_mpt = mpt_store_create(acct_path, total_accounts * 4);
    mpt_store_t *stor_mpt = mpt_store_create(stor_path, total_accounts * slots_per_account * 4);

    if (!acct_mpt || !stor_mpt) {
        fprintf(stderr, "Failed to create MPT stores\n");
        return 1;
    }

    mpt_store_set_shared(stor_mpt, true);

    rng_t rng = { .state = 42 };

    /* Pre-allocate per-account storage roots */
    uint8_t (*storage_roots)[32] = calloc(total_accounts, 32);
    for (int i = 0; i < total_accounts; i++)
        memcpy(storage_roots[i], EMPTY_ROOT, 32);

    /* =====================================================================
     * Phase 0: Populate trie with initial accounts
     * ===================================================================== */

    fprintf(stderr, "Populating %d accounts...\n", total_accounts);
    struct timespec pop_start, pop_end;
    clock_gettime(CLOCK_MONOTONIC, &pop_start);

    /* Insert accounts in batches of 1000 */
    for (int batch_start = 0; batch_start < total_accounts; batch_start += 1000) {
        int batch_end = batch_start + 1000;
        if (batch_end > total_accounts) batch_end = total_accounts;

        /* First create some storage for these accounts */
        for (int i = batch_start; i < batch_end; i++) {
            mpt_store_set_root(stor_mpt, storage_roots[i]);
            mpt_store_begin_batch(stor_mpt);
            for (int s = 0; s < slots_per_account; s++) {
                uint8_t slot_key[32];
                make_key((uint64_t)i * 1000 + s, slot_key);
                uint8_t val[33];
                val[0] = 0xa0;
                rng_bytes(&rng, val + 1, 32);
                mpt_store_update(stor_mpt, slot_key, val, 33);
            }
            mpt_store_commit_batch(stor_mpt);
            mpt_store_root(stor_mpt, storage_roots[i]);
        }

        /* Now insert accounts */
        mpt_store_begin_batch(acct_mpt);
        for (int i = batch_start; i < batch_end; i++) {
            uint8_t addr_key[32];
            make_key(i, addr_key);
            uint8_t rlp[120];
            size_t rlp_len = make_account_rlp(&rng, storage_roots[i], rlp);
            mpt_store_update(acct_mpt, addr_key, rlp, rlp_len);
        }
        mpt_store_commit_batch(acct_mpt);

        if ((batch_end % 10000) == 0 || batch_end == total_accounts)
            fprintf(stderr, "  %d/%d accounts\n", batch_end, total_accounts);
    }

    clock_gettime(CLOCK_MONOTONIC, &pop_end);
    fprintf(stderr, "  populated in %.1f ms\n\n", ms_diff(&pop_start, &pop_end));

    /* Flush to disk so we measure realistic I/O patterns */
    mpt_store_flush(acct_mpt);
    mpt_store_flush(stor_mpt);

    /* =====================================================================
     * Phase 1: Profile dirty update rounds
     * ===================================================================== */

    fprintf(stderr, "%6s  %8s  %8s  %8s  %8s  %7s  %7s\n",
            "round", "total_ms", "stor_ms", "stage_ms", "commt_ms",
            "d_accts", "d_slots");
    fprintf(stderr, "------  --------  --------  --------  --------  "
                    "-------  -------\n");

    for (int r = 0; r < rounds; r++) {
        /* Pick dirty accounts (Zipf-like: bias toward recent) */
        int *dirty_idx = malloc(dirty_per_round * sizeof(int));
        for (int i = 0; i < dirty_per_round; i++) {
            /* Zipf: square root distribution biased toward higher indices */
            uint64_t x = rng_next(&rng) % (uint64_t)total_accounts;
            uint64_t y = rng_next(&rng) % (uint64_t)total_accounts;
            dirty_idx[i] = (int)(x > y ? x : y);  /* max of 2 = skewed high */
        }

        struct timespec t0, t1, t2, t3;
        /* --- Storage trie updates --- */
        clock_gettime(CLOCK_MONOTONIC, &t0);

        int total_dirty_slots = 0;
        for (int i = 0; i < dirty_per_round; i++) {
            int idx = dirty_idx[i];

            mpt_store_set_root(stor_mpt, storage_roots[idx]);
            mpt_store_begin_batch(stor_mpt);

            /* Update 1-3 random slots per dirty account */
            int n_slots = 1 + (int)(rng_next(&rng) % 3);
            total_dirty_slots += n_slots;
            for (int s = 0; s < n_slots; s++) {
                uint8_t slot_key[32];
                make_key((uint64_t)idx * 1000 + (rng_next(&rng) % (slots_per_account * 2)), slot_key);
                uint8_t val[33];
                val[0] = 0xa0;
                rng_bytes(&rng, val + 1, 32);
                mpt_store_update(stor_mpt, slot_key, val, 33);
            }

            mpt_store_commit_batch(stor_mpt);
            mpt_store_root(stor_mpt, storage_roots[idx]);
        }

        clock_gettime(CLOCK_MONOTONIC, &t1);

        /* --- Account trie staging --- */
        mpt_store_begin_batch(acct_mpt);
        for (int i = 0; i < dirty_per_round; i++) {
            int idx = dirty_idx[i];
            uint8_t addr_key[32];
            make_key(idx, addr_key);
            uint8_t rlp[120];
            size_t rlp_len = make_account_rlp(&rng, storage_roots[idx], rlp);
            mpt_store_update(acct_mpt, addr_key, rlp, rlp_len);
        }

        clock_gettime(CLOCK_MONOTONIC, &t2);

        /* --- Account trie commit --- */
        mpt_store_commit_batch(acct_mpt);

        clock_gettime(CLOCK_MONOTONIC, &t3);

        double stor_ms  = ms_diff(&t0, &t1);
        double stage_ms = ms_diff(&t1, &t2);
        double commt_ms = ms_diff(&t2, &t3);
        double total_ms = ms_diff(&t0, &t3);

        fprintf(stderr, "%6d  %7.1f   %7.1f   %7.1f   %7.1f   %6d  %7d\n",
                r, total_ms, stor_ms, stage_ms, commt_ms,
                dirty_per_round, total_dirty_slots);

        free(dirty_idx);
    }

    /* Summary */
    fprintf(stderr, "\nFinal stats:\n");
    mpt_store_stats_t acct_st = mpt_store_stats(acct_mpt);
    mpt_store_stats_t stor_st = mpt_store_stats(stor_mpt);
    fprintf(stderr, "  account trie: %llu nodes\n",
            (unsigned long long)acct_st.node_count);
    fprintf(stderr, "  storage trie: %llu nodes\n",
            (unsigned long long)stor_st.node_count);

    /* Cleanup */
    mpt_store_destroy(acct_mpt);
    mpt_store_destroy(stor_mpt);
    free(storage_roots);

    snprintf(buf, sizeof(buf), "%s.idx", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.dat", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.free", acct_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.idx", stor_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.dat", stor_path); unlink(buf);
    snprintf(buf, sizeof(buf), "%s.free", stor_path); unlink(buf);

    return 0;
}
