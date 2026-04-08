/**
 * Benchmark: hart vs mpt_store for storage at realistic Ethereum scale.
 *
 * Simulates a workload matching real chain replay:
 *   - 1000 accounts with varying slot counts (10-100K)
 *   - Per-block: 200 accounts touched, 5 SSTOREs each
 *   - 100 blocks simulated
 *
 * Measures: total time, memory usage, disk usage.
 */

#include "hashed_art.h"
#include "mpt_store.h"
#include "hash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <unistd.h>

#define STOR_VAL_SIZE 32

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void make_key(uint64_t acct, uint64_t slot, uint8_t out[32]) {
    uint8_t buf[16];
    memcpy(buf, &acct, 8);
    memcpy(buf + 8, &slot, 8);
    hash_t h = hash_keccak256(buf, 16);
    memcpy(out, h.bytes, 32);
}

static void make_val(uint64_t v, uint8_t out[32]) {
    memset(out, 0, 32);
    for (int i = 31; v > 0 && i >= 0; i--) {
        out[i] = v & 0xFF;
        v >>= 8;
    }
}

static size_t rlp_be(const uint8_t *val, size_t len, uint8_t *out) {
    size_t skip = 0;
    while (skip < len - 1 && val[skip] == 0) skip++;
    size_t trimmed = len - skip;
    const uint8_t *data = val + skip;
    if (trimmed == 1 && data[0] <= 0x7f) { out[0] = data[0]; return 1; }
    if (trimmed <= 55) {
        out[0] = 0x80 + (uint8_t)trimmed;
        memcpy(out + 1, data, trimmed);
        return 1 + trimmed;
    }
    return 0;
}

static uint32_t stor_encode(const uint8_t key[32], const void *leaf_val,
                             uint8_t *rlp_out, void *ctx) {
    (void)key; (void)ctx;
    return (uint32_t)rlp_be((const uint8_t *)leaf_val, STOR_VAL_SIZE, rlp_out);
}

static uint64_t file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (uint64_t)st.st_size;
}

/* =========================================================================
 * Hart benchmark
 * ========================================================================= */

static void bench_hart(int n_accounts, const uint32_t *slot_counts,
                       int n_blocks, int accts_per_block, int slots_per_acct) {
    printf("\n--- HART (in-memory ART trie) ---\n");

    hart_t *harts = calloc(n_accounts, sizeof(hart_t));
    for (int i = 0; i < n_accounts; i++)
        hart_init_cap(&harts[i], STOR_VAL_SIZE, slot_counts[i] * 96 < 1024 ? 1024 : slot_counts[i] * 96);

    /* Populate */
    double t0 = now_ms();
    uint64_t total_slots = 0;
    for (int a = 0; a < n_accounts; a++) {
        for (uint32_t s = 0; s < slot_counts[a]; s++) {
            uint8_t key[32], val[32];
            make_key(a, s, key);
            make_val(a * 100000 + s + 1, val);
            hart_insert(&harts[a], key, val);
            total_slots++;
        }
    }
    double t1 = now_ms();
    printf("  populate: %lu slots in %.0fms (%.0f slots/s)\n",
           total_slots, t1 - t0, total_slots / ((t1 - t0) / 1000.0));

    /* Memory */
    size_t total_arena = 0;
    for (int i = 0; i < n_accounts; i++)
        total_arena += harts[i].arena_cap;
    printf("  memory: %.1fMB (arena_cap)\n", total_arena / 1e6);

    /* Block simulation */
    double block_total = 0;
    uint64_t seed = 42;
    for (int b = 0; b < n_blocks; b++) {
        double bt0 = now_ms();
        for (int ai = 0; ai < accts_per_block; ai++) {
            seed = seed * 6364136223846793005ULL + 1;
            int acct = (int)((seed >> 32) % n_accounts);
            /* SLOAD 5 slots */
            for (int si = 0; si < slots_per_acct; si++) {
                uint8_t key[32];
                uint64_t slot = (seed + si) % slot_counts[acct];
                make_key(acct, slot, key);
                hart_get(&harts[acct], key);
            }
            /* SSTORE 5 slots */
            for (int si = 0; si < slots_per_acct; si++) {
                uint8_t key[32], val[32];
                uint64_t slot = (seed + si + 100) % slot_counts[acct];
                make_key(acct, slot, key);
                make_val(b * 1000000 + ai * 1000 + si, val);
                hart_insert(&harts[acct], key, val);
            }
            /* Root hash (incremental) */
            uint8_t root[32];
            hart_root_hash(&harts[acct], stor_encode, NULL, root);
        }
        block_total += now_ms() - bt0;
    }
    printf("  %d blocks: %.0fms total, %.1fms/block\n",
           n_blocks, block_total, block_total / n_blocks);

    for (int i = 0; i < n_accounts; i++) hart_destroy(&harts[i]);
    free(harts);
}

/* =========================================================================
 * mpt_store benchmark
 * ========================================================================= */

static void bench_mpt(int n_accounts, const uint32_t *slot_counts,
                      int n_blocks, int accts_per_block, int slots_per_acct) {
    printf("\n--- MPT_STORE (disk-backed persistent trie) ---\n");

    const char *path = "/tmp/bench_mpt_compare";
    char idx_path[256], dat_path[256];
    snprintf(idx_path, sizeof(idx_path), "%s.idx", path);
    snprintf(dat_path, sizeof(dat_path), "%s.dat", path);
    unlink(idx_path); unlink(dat_path);

    uint64_t total_slots = 0;
    for (int i = 0; i < n_accounts; i++) total_slots += slot_counts[i];

    mpt_store_t *ms = mpt_store_create(path, total_slots * 4);
    if (!ms) { printf("  FAILED to create mpt_store\n"); return; }
    mpt_store_set_shared(ms, true);

    static const uint8_t EMPTY_ROOT[32] = {
        0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
        0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
        0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
        0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
    };

    uint8_t (*roots)[32] = calloc(n_accounts, 32);

    /* Populate */
    double t0 = now_ms();
    for (int a = 0; a < n_accounts; a++) {
        mpt_store_set_root(ms, EMPTY_ROOT);
        mpt_store_begin_batch(ms);
        for (uint32_t s = 0; s < slot_counts[a]; s++) {
            uint8_t key[32], val[32], rlp[34];
            make_key(a, s, key);
            make_val(a * 100000 + s + 1, val);
            size_t rl = rlp_be(val, 32, rlp);
            mpt_store_update(ms, key, rlp, rl);
        }
        mpt_store_commit_batch(ms);
        mpt_store_root(ms, roots[a]);
    }
    double t1 = now_ms();
    printf("  populate: %lu slots in %.0fms (%.0f slots/s)\n",
           total_slots, t1 - t0, total_slots / ((t1 - t0) / 1000.0));

    mpt_store_flush(ms);
    printf("  disk: idx=%.1fMB dat=%.1fMB\n",
           file_size(idx_path) / 1e6, file_size(dat_path) / 1e6);

    /* Block simulation */
    double block_total = 0;
    uint64_t seed = 42;
    for (int b = 0; b < n_blocks; b++) {
        double bt0 = now_ms();
        for (int ai = 0; ai < accts_per_block; ai++) {
            seed = seed * 6364136223846793005ULL + 1;
            int acct = (int)((seed >> 32) % n_accounts);

            mpt_store_set_root(ms, roots[acct]);

            /* SLOAD 5 slots */
            uint8_t rbuf[64];
            for (int si = 0; si < slots_per_acct; si++) {
                uint8_t key[32];
                uint64_t slot = (seed + si) % slot_counts[acct];
                make_key(acct, slot, key);
                mpt_store_get(ms, key, rbuf, sizeof(rbuf));
            }
            /* SSTORE 5 slots */
            mpt_store_begin_batch(ms);
            for (int si = 0; si < slots_per_acct; si++) {
                uint8_t key[32], val[32], rlp[34];
                uint64_t slot = (seed + si + 100) % slot_counts[acct];
                make_key(acct, slot, key);
                make_val(b * 1000000 + ai * 1000 + si, val);
                size_t rl = rlp_be(val, 32, rlp);
                mpt_store_update(ms, key, rlp, rl);
            }
            mpt_store_commit_batch(ms);
            mpt_store_root(ms, roots[acct]);
        }
        block_total += now_ms() - bt0;
    }
    printf("  %d blocks: %.0fms total, %.1fms/block\n",
           n_blocks, block_total, block_total / n_blocks);

    mpt_store_destroy(ms);
    free(roots);
    unlink(idx_path); unlink(dat_path);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Storage Backend Comparison: hart vs mpt_store ===\n");

    /* Small scale: 100 accounts, 10-1000 slots */
    {
        int n = 100;
        uint32_t *slots = malloc(n * sizeof(uint32_t));
        for (int i = 0; i < n; i++) slots[i] = 10 + (i % 991);
        printf("\n========== SMALL: %d accounts, avg ~500 slots ==========\n", n);
        bench_hart(n, slots, 100, 50, 5);
        bench_mpt(n, slots, 100, 50, 5);
        free(slots);
    }

    /* Medium scale: 1000 accounts, 10-10000 slots */
    {
        int n = 1000;
        uint32_t *slots = malloc(n * sizeof(uint32_t));
        for (int i = 0; i < n; i++) slots[i] = 10 + (i % 9991);
        printf("\n========== MEDIUM: %d accounts, avg ~5000 slots ==========\n", n);
        bench_hart(n, slots, 100, 200, 5);
        bench_mpt(n, slots, 100, 200, 5);
        free(slots);
    }

    /* Large scale: 5000 accounts, 10-50000 slots (whale contracts) */
    {
        int n = 5000;
        uint32_t *slots = malloc(n * sizeof(uint32_t));
        for (int i = 0; i < n; i++) slots[i] = 10 + (i % 49991);
        printf("\n========== LARGE: %d accounts, avg ~25000 slots ==========\n", n);
        bench_hart(n, slots, 50, 200, 5);
        bench_mpt(n, slots, 50, 200, 5);
        free(slots);
    }

    printf("\n=== done ===\n");
    return 0;
}
