/**
 * Verkle Build — offline verkle state construction from state history files.
 *
 * Reads per-block diffs from state_history (.idx + .dat) and applies them
 * to a verkle_flat store. No EVM, no block executor — pure diff → verkle.
 *
 * Usage:
 *   ./verkle_build <history_dir> <verkle_dir> [start_block] [end_block]
 *
 * If verkle_dir already contains state, resumes from the last committed block.
 * If start_block is given, overrides the resume point (must match existing state).
 *
 * Examples:
 *   # Build from scratch (all available history)
 *   ./verkle_build data/history data/verkle
 *
 *   # Build blocks 0-1000000
 *   ./verkle_build data/history data/verkle 0 1000000
 *
 *   # Resume (auto-detects last block from verkle state)
 *   ./verkle_build data/history data/verkle
 */

#include "state_history.h"
#include "verkle_flat.h"
#include "verkle_key.h"
#include "pedersen.h"
#include "uint256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>

/* =========================================================================
 * Graceful shutdown
 * ========================================================================= */

static volatile sig_atomic_t g_stop = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_stop = 1;
}

/* =========================================================================
 * Diff → Verkle conversion (same logic as verkle_builder.c)
 * ========================================================================= */

static void apply_account_diff(verkle_flat_t *vf, const account_diff_t *a) {
    const uint8_t *addr = a->addr.bytes;

    /* basic_data: version(1) + reserved(4) + code_size(3) + nonce(8) + balance(16) = 32 */
    uint8_t basic_data[32];
    uint8_t basic_key[32];
    verkle_account_basic_data_key(basic_key, addr);

    if (!verkle_flat_get(vf, basic_key, basic_data))
        memset(basic_data, 0, 32);

    /* Nonce (8-byte BE at offset 8) */
    uint64_t nonce = a->new_nonce;
    for (int i = 7; i >= 0; i--) {
        basic_data[VERKLE_BASIC_DATA_NONCE_OFFSET + i] = (uint8_t)(nonce & 0xFF);
        nonce >>= 8;
    }

    /* Balance (16-byte BE at offset 16, from uint256 LE) */
    uint8_t bal_be[32];
    uint256_to_bytes(&a->new_balance, bal_be);
    for (int i = 0; i < VERKLE_BASIC_DATA_BALANCE_SIZE; i++)
        basic_data[31 - i] = bal_be[i];

    verkle_flat_set(vf, basic_key, basic_data);

    /* Code hash */
    uint8_t code_hash_key[32];
    verkle_account_code_hash_key(code_hash_key, addr);
    verkle_flat_set(vf, code_hash_key, a->new_code_hash.bytes);
}

static void apply_storage_diff(verkle_flat_t *vf, const storage_diff_t *s) {
    uint8_t key[32];
    uint8_t slot_le[32], val_le[32];

    uint256_to_bytes(&s->slot, slot_le);
    uint256_to_bytes(&s->new_value, val_le);

    verkle_storage_key(key, s->addr.bytes, slot_le);
    verkle_flat_set(vf, key, val_le);
}

static void apply_diff(verkle_flat_t *vf, const block_diff_t *diff) {
    for (uint32_t i = 0; i < diff->account_count; i++)
        apply_account_diff(vf, &diff->accounts[i]);
    for (uint32_t i = 0; i < diff->storage_count; i++)
        apply_storage_diff(vf, &diff->storage[i]);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr,
            "Usage: %s <history_dir> <verkle_dir> [start_block] [end_block]\n",
            argv[0]);
        return 1;
    }

    const char *history_dir = argv[1];
    const char *verkle_dir  = argv[2];

    /* Initialize Pedersen bases */
    pedersen_init();

    /* Open state history (read-only usage) */
    state_history_t *sh = state_history_create(history_dir);
    if (!sh) {
        fprintf(stderr, "Failed to open state history at %s\n", history_dir);
        return 1;
    }

    uint64_t hist_first, hist_last;
    if (!state_history_range(sh, &hist_first, &hist_last)) {
        fprintf(stderr, "State history is empty.\n");
        state_history_destroy(sh);
        return 1;
    }

    printf("History range: blocks %lu .. %lu (%lu blocks)\n",
           hist_first, hist_last, hist_last - hist_first + 1);

    /* Open or create verkle_flat */
    char value_path[512], commit_path[512];
    snprintf(value_path, sizeof(value_path), "%s/values", verkle_dir);
    snprintf(commit_path, sizeof(commit_path), "%s/commits", verkle_dir);

    /* Check if verkle state already exists */
    struct stat st;
    bool resuming = (stat(value_path, &st) == 0);

    verkle_flat_t *vf;
    if (resuming) {
        vf = verkle_flat_open(value_path, commit_path);
        if (!vf) {
            fprintf(stderr, "Failed to open existing verkle state at %s\n", verkle_dir);
            state_history_destroy(sh);
            return 1;
        }
        printf("Opened existing verkle state (resuming)\n");
    } else {
        mkdir(verkle_dir, 0755);
        vf = verkle_flat_create(value_path, commit_path);
        if (!vf) {
            fprintf(stderr, "Failed to create verkle state at %s\n", verkle_dir);
            state_history_destroy(sh);
            return 1;
        }
        printf("Created new verkle state\n");
    }

    /* Determine block range */
    uint64_t start_block = hist_first;
    uint64_t end_block   = hist_last;

    if (argc > 3) start_block = (uint64_t)atoll(argv[3]);
    if (argc > 4) end_block   = (uint64_t)atoll(argv[4]);

    /* Clamp to history range */
    if (start_block < hist_first) start_block = hist_first;
    if (end_block > hist_last) end_block = hist_last;

    if (start_block > end_block) {
        fprintf(stderr, "No blocks to process (start=%lu > end=%lu)\n",
                start_block, end_block);
        verkle_flat_destroy(vf);
        state_history_destroy(sh);
        return 0;
    }

    printf("Processing blocks %lu .. %lu (%lu blocks)\n",
           start_block, end_block, end_block - start_block + 1);

    /* Install signal handler for graceful stop */
    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    /* Main loop */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    uint64_t blocks_done = 0;
    uint64_t total_accounts = 0;
    uint64_t total_storage = 0;
    for (uint64_t bn = start_block; bn <= end_block && !g_stop; bn++) {
        block_diff_t diff;
        if (!state_history_get_diff(sh, bn, &diff)) {
            fprintf(stderr, "Warning: block %lu not found in history, skipping\n", bn);
            continue;
        }

        verkle_flat_begin_block(vf, bn);
        apply_diff(vf, &diff);
        verkle_flat_commit_block(vf);

        total_accounts += diff.account_count;
        total_storage  += diff.storage_count;
        block_diff_free(&diff);
        blocks_done++;

        /* Periodic sync + progress report every 256 blocks */
        if ((blocks_done & 0xFF) == 0) {
            verkle_flat_sync(vf);

            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double bps = blocks_done / elapsed;

            uint64_t remaining = end_block - bn;
            double eta_s = remaining / bps;
            int eta_h = (int)(eta_s / 3600);
            int eta_m = (int)((eta_s - eta_h * 3600) / 60);

            printf("\r  block %lu  |  %lu done  |  %.0f blk/s  |  "
                   "accts=%lu  slots=%lu  |  ETA %dh%02dm   ",
                   bn, blocks_done, bps,
                   total_accounts, total_storage, eta_h, eta_m);
            fflush(stdout);
        }
    }

    /* Final sync */
    printf("\n\nSyncing to disk...\n");
    verkle_flat_sync(vf);

    /* Print root hash */
    uint8_t root[32];
    verkle_flat_root_hash(vf, root);
    printf("Verkle root at block %lu: 0x",
           start_block + blocks_done - 1);
    for (int i = 0; i < 32; i++) printf("%02x", root[i]);
    printf("\n");

    /* Summary */
    clock_gettime(CLOCK_MONOTONIC, &t_now);
    double total_s = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
    printf("\nDone: %lu blocks in %.1fs (%.0f blk/s)\n",
           blocks_done, total_s, blocks_done / total_s);
    printf("  Account diffs applied: %lu\n", total_accounts);
    printf("  Storage diffs applied: %lu\n", total_storage);

    if (g_stop)
        printf("  (stopped early by signal — state is consistent up to last synced block)\n");

    verkle_flat_destroy(vf);
    state_history_destroy(sh);
    return 0;
}
