/**
 * Chain Replay Tool
 *
 * Re-executes Ethereum blocks from Era1 archive files.
 * Uses the sync engine for execution, validation, and checkpointing.
 *
 * Usage: ./chain_replay [--clean] <era1_dir> <genesis.json> [start_block] [end_block]
 */

#include "sync.h"
#include "era1.h"
#include "block.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

bool g_trace_calls = false;  // Debug: trace CALL gas

#define ERA1_BLOCKS_PER_FILE 8192

#ifndef CHECKPOINT_INTERVAL
#define CHECKPOINT_INTERVAL  256
#endif

/* Persistent store paths */
static const char *VALUE_DIR  = "/tmp/chain_replay_values";
static const char *COMMIT_DIR = "/tmp/chain_replay_commits";
static const char *CKPT_PATH  = "/tmp/chain_replay.ckpt";
static const char *MPT_PATH   = "/tmp/chain_replay_mpt";
static const char *CODE_PATH  = "/tmp/chain_replay_code";

/* =========================================================================
 * Graceful shutdown via SIGINT
 * ========================================================================= */

static volatile sig_atomic_t g_shutdown = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* =========================================================================
 * Era1 file management
 * ========================================================================= */

typedef struct {
    char     **paths;     /* sorted file paths */
    size_t     count;
    era1_t     current;   /* currently open file */
    int        current_idx;
} era1_archive_t;

static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static bool archive_open(era1_archive_t *ar, const char *dir) {
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;

    DIR *d = opendir(dir);
    if (!d) {
        perror("opendir");
        return false;
    }

    /* Collect .era1 files */
    size_t cap = 64;
    ar->paths = malloc(cap * sizeof(char *));
    if (!ar->paths) { closedir(d); return false; }

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 5 || strcmp(ent->d_name + nlen - 5, ".era1") != 0)
            continue;

        if (ar->count >= cap) {
            cap *= 2;
            ar->paths = realloc(ar->paths, cap * sizeof(char *));
        }

        size_t pathlen = strlen(dir) + 1 + nlen + 1;
        ar->paths[ar->count] = malloc(pathlen);
        snprintf(ar->paths[ar->count], pathlen, "%s/%s", dir, ent->d_name);
        ar->count++;
    }
    closedir(d);

    if (ar->count == 0) {
        fprintf(stderr, "No .era1 files found in %s\n", dir);
        free(ar->paths);
        ar->paths = NULL;
        return false;
    }

    /* Sort by filename (alphabetical = chronological for era1) */
    qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

static void archive_close(era1_archive_t *ar) {
    if (ar->current_idx >= 0)
        era1_close(&ar->current);
    for (size_t i = 0; i < ar->count; i++)
        free(ar->paths[i]);
    free(ar->paths);
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;
}

/* Ensure the correct era1 file is open for the given block */
static bool archive_ensure(era1_archive_t *ar, uint64_t block_number) {
    /* Check if current file contains this block */
    if (ar->current_idx >= 0 && era1_contains(&ar->current, block_number))
        return true;

    /* Close current */
    if (ar->current_idx >= 0) {
        era1_close(&ar->current);
        ar->current_idx = -1;
    }

    /* Find the right file */
    int file_idx = (int)(block_number / ERA1_BLOCKS_PER_FILE);
    if ((size_t)file_idx >= ar->count) {
        fprintf(stderr, "No era1 file for block %lu (have %zu files)\n",
                block_number, ar->count);
        return false;
    }

    if (!era1_open(&ar->current, ar->paths[file_idx])) {
        fprintf(stderr, "Failed to open %s\n", ar->paths[file_idx]);
        return false;
    }

    ar->current_idx = file_idx;

    if (!era1_contains(&ar->current, block_number)) {
        fprintf(stderr, "File %s doesn't contain block %lu (has %lu-%lu)\n",
                ar->paths[file_idx], block_number,
                ar->current.start_block,
                ar->current.start_block + ar->current.count - 1);
        era1_close(&ar->current);
        ar->current_idx = -1;
        return false;
    }

    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    /* Check for --clean flag */
    bool force_clean = false;
    int arg_offset = 0;
    if (argc > 1 && strcmp(argv[1], "--clean") == 0) {
        force_clean = true;
        arg_offset = 1;
    }

    if (argc - arg_offset < 3) {
        fprintf(stderr,
            "Usage: %s [--clean] <era1_dir> <genesis.json> [start_block] [end_block]\n"
            "\n"
            "Options:\n"
            "  --clean   Delete existing checkpoint and state, start from genesis\n"
            "\n"
            "Checkpoints every %d blocks to %s\n",
            argv[0], CHECKPOINT_INTERVAL, CKPT_PATH);
        return 1;
    }

    const char *era1_dir     = argv[1 + arg_offset];
    const char *genesis_path = argv[2 + arg_offset];
    uint64_t user_start = (argc - arg_offset > 3)
                          ? (uint64_t)atoll(argv[3 + arg_offset]) : 0;
    uint64_t end_block  = (argc - arg_offset > 4)
                          ? (uint64_t)atoll(argv[4 + arg_offset]) : UINT64_MAX;

    /* Install signal handler */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, NULL);

    /* Open era1 archive */
    era1_archive_t archive;
    if (!archive_open(&archive, era1_dir)) return 1;
    printf("Era1 archive: %zu files in %s\n", archive.count, era1_dir);

    /* Clean up old state if requested */
    if (force_clean) {
        printf("--clean: removing existing state and checkpoint\n");
        unlink(CKPT_PATH);
        char cmd[512];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s %s %s.idx %s.dat %s_storage.idx %s_storage.dat %s.idx %s.dat 2>/dev/null",
                 VALUE_DIR, COMMIT_DIR, MPT_PATH, MPT_PATH, MPT_PATH, MPT_PATH,
                 CODE_PATH, CODE_PATH);
        (void)system(cmd);
    }

    /* Create sync engine */
    sync_config_t cfg = {
        .chain_config        = chain_config_mainnet(),
        .verkle_value_dir    = NULL,
        .verkle_commit_dir   = NULL,
        .mpt_path            = NULL,
        .checkpoint_path     = CKPT_PATH,
        .checkpoint_interval = CHECKPOINT_INTERVAL,
        .validate_state_root = true,
    };
#ifdef ENABLE_VERKLE
    cfg.verkle_value_dir  = VALUE_DIR;
    cfg.verkle_commit_dir = COMMIT_DIR;
#endif
#ifdef ENABLE_MPT
    cfg.mpt_path = MPT_PATH;
    cfg.code_store_path = CODE_PATH;
#endif

    sync_t *sync = sync_create(&cfg);
    if (!sync) {
        fprintf(stderr, "Failed to create sync engine\n");
        archive_close(&archive);
        return 1;
    }

    uint64_t resumed = sync_resumed_from(sync);
    uint64_t start_block;

    if (resumed > 0) {
        start_block = resumed + 1;
        if (user_start > start_block) {
            fprintf(stderr,
                "Warning: requested start_block %lu > checkpoint %lu + 1\n"
                "Cannot skip ahead — use --clean to restart from genesis\n",
                user_start, resumed);
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
    } else {
        /* Read genesis block hash from era1 */
        hash_t gen_hash = {0};
        if (archive_ensure(&archive, 0)) {
            uint8_t *hdr_rlp, *body_rlp;
            size_t hdr_len, body_len;
            if (era1_read_block(&archive.current, 0,
                                &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
                gen_hash = hash_keccak256(hdr_rlp, hdr_len);
                free(hdr_rlp);
                free(body_rlp);
            }
        }

        if (!sync_load_genesis(sync, genesis_path, &gen_hash)) {
            fprintf(stderr, "Failed to load genesis state\n");
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }

        start_block = (user_start > 0) ? user_start : 1;
    }

    /* Progress tracking */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    printf("Replaying blocks %lu to %lu...\n\n", start_block,
           end_block == UINT64_MAX
               ? (uint64_t)(archive.count * ERA1_BLOCKS_PER_FILE - 1)
               : end_block);

    uint64_t window_txs = 0;
    struct timespec t_window;
    clock_gettime(CLOCK_MONOTONIC, &t_window);

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        /* Graceful shutdown */
        if (g_shutdown) {
            printf("\nSIGINT received — saving checkpoint\n");
            sync_checkpoint(sync);
            break;
        }

        if (!archive_ensure(&archive, bn))
            break;

        /* Read block from era1 */
        uint8_t *hdr_rlp, *body_rlp;
        size_t hdr_len, body_len;
        if (!era1_read_block(&archive.current, bn,
                             &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
            fprintf(stderr, "Block %lu: failed to read from era1\n", bn);
            break;
        }

        /* Compute block hash */
        hash_t blk_hash = hash_keccak256(hdr_rlp, hdr_len);

        /* Decode header and body */
        block_header_t header;
        if (!block_header_decode_rlp(&header, hdr_rlp, hdr_len)) {
            fprintf(stderr, "Block %lu: failed to decode header\n", bn);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        block_body_t body;
        if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
            fprintf(stderr, "Block %lu: failed to decode body\n", bn);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        /* Execute + validate via sync engine */
        sync_block_result_t result;
        if (!sync_execute_block(sync, &header, &body, &blk_hash, &result)) {
            fprintf(stderr, "Block %lu: fatal execution error\n", bn);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        window_txs += result.tx_count;

        /* Progress every CHECKPOINT_INTERVAL blocks, on failure, or last block */
        if (bn % CHECKPOINT_INTERVAL == 0 || !result.ok || bn == end_block) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double bps = (bn - start_block + 1) / (elapsed > 0 ? elapsed : 1);
            double win_secs = (t_now.tv_sec - t_window.tv_sec) +
                              (t_now.tv_nsec - t_window.tv_nsec) / 1e9;
            double tps = window_txs / (win_secs > 0 ? win_secs : 1);
            sync_status_t st = sync_get_status(sync);
            printf("Block %lu | %lu txs | %.0f tps | gas %lu | %.0f blk/s | ok %lu fail %lu\n",
                   bn, window_txs, tps, result.gas_used, bps,
                   st.blocks_ok, st.blocks_fail);
            window_txs = 0;
            clock_gettime(CLOCK_MONOTONIC, &t_window);
        }

        bool block_ok = result.ok;

        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (!block_ok) {
            if (result.error == SYNC_GAS_MISMATCH) {
                fprintf(stderr, "Block %lu: GAS MISMATCH  got %lu  expected %lu  diff %+ld\n",
                        bn, result.actual_gas, result.expected_gas,
                        (long)result.actual_gas - (long)result.expected_gas);
            } else if (result.error == SYNC_ROOT_MISMATCH) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&result.actual_root, got_hex);
                hash_to_hex(&result.expected_root, exp_hex);
                fprintf(stderr, "Block %lu: STATE ROOT MISMATCH (at batch checkpoint)\n"
                        "  got:      %s\n  expected: %s\n",
                        bn, got_hex, exp_hex);
            }
            fprintf(stderr, "\nFirst failure at block %lu — stopping.\n", bn);
            break;
        }
    }

    /* Summary */
    clock_gettime(CLOCK_MONOTONIC, &t_now);
    double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;

    sync_status_t st = sync_get_status(sync);

    printf("\n===== Summary =====\n");
    printf("Blocks OK:     %lu\n", st.blocks_ok);
    printf("Blocks failed: %lu\n", st.blocks_fail);
    printf("Total gas:     %lu\n", st.total_gas);
    printf("Elapsed:       %.1f s\n", elapsed);
    printf("Speed:         %.0f blk/s\n",
           (st.blocks_ok + st.blocks_fail) / (elapsed > 0 ? elapsed : 1));

    sync_destroy(sync);  /* saves final checkpoint */
    archive_close(&archive);

    return st.blocks_fail > 0 ? 1 : 0;
}
