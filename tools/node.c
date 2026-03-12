/**
 * Art Node — Full Ethereum execution client.
 *
 * Two-phase sync:
 *   1. Era1 replay (genesis → Paris): batch-mode execution from era1 files.
 *      Fast (~2000+ blk/s) with deferred root validation at checkpoint
 *      boundaries and background MPT flush.
 *
 *   2. CL sync (Paris → head): live-mode execution driven by the Consensus
 *      Layer via Engine API (newPayload + forkchoiceUpdated). Per-block root
 *      validation with synchronous flush.
 *
 * Usage:
 *   ./art_node --genesis data/mainnet_genesis.json \
 *              --era1 data/era1 \
 *              --jwt-secret jwt.hex \
 *              [--data-dir /tmp/art] \
 *              [--port 8551] [--host 127.0.0.1]
 */

#include "chain_tip.h"
#include "engine.h"
#include "block.h"
#include "block_store.h"
#include "fork.h"
#include "hash.h"
#include "keccak256.h"

/* Era1 reader */
#include "era1.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define PARIS_BLOCK        15537394
#define ERA1_BLOCKS_PER_FILE 8192
#define CHECKPOINT_INTERVAL  256

/* Default paths (relative to data_dir) */
#define DEFAULT_DATA_DIR   "/tmp/art_node"

/* =========================================================================
 * Globals
 * ========================================================================= */

static volatile int g_shutdown = 0;
static engine_t    *g_engine   = NULL;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    if (g_engine)
        engine_stop(g_engine);
}

/* =========================================================================
 * Era1 archive (same pattern as chain_replay)
 * ========================================================================= */

typedef struct {
    era1_t  current;
    char  **paths;
    size_t  count;
    int     current_file;
} archive_t;

static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static bool archive_open(archive_t *ar, const char *dir) {
    memset(ar, 0, sizeof(*ar));
    ar->current_file = -1;

    DIR *d = opendir(dir);
    if (!d) return false;

    size_t cap = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t len = strlen(ent->d_name);
        if (len < 5 || strcmp(ent->d_name + len - 5, ".era1") != 0)
            continue;
        if (ar->count >= cap) {
            cap = cap ? cap * 2 : 64;
            ar->paths = realloc(ar->paths, cap * sizeof(char *));
        }
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        ar->paths[ar->count++] = strdup(path);
    }
    closedir(d);

    if (ar->count == 0) return false;
    qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

static void archive_close(archive_t *ar) {
    if (ar->current_file >= 0)
        era1_close(&ar->current);
    for (size_t i = 0; i < ar->count; i++)
        free(ar->paths[i]);
    free(ar->paths);
    memset(ar, 0, sizeof(*ar));
    ar->current_file = -1;
}

static bool archive_ensure(archive_t *ar, uint64_t block_number,
                            const char *era1_dir) {
    (void)era1_dir;
    int file_idx = (int)(block_number / ERA1_BLOCKS_PER_FILE);
    if (file_idx == ar->current_file) return true;
    if ((size_t)file_idx >= ar->count) return false;

    if (ar->current_file >= 0)
        era1_close(&ar->current);

    if (!era1_open(&ar->current, ar->paths[file_idx])) {
        ar->current_file = -1;
        return false;
    }
    ar->current_file = file_idx;
    return true;
}

/* =========================================================================
 * Usage
 * ========================================================================= */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options]\n\n"
        "  --genesis <path>       Genesis JSON file (required for fresh start)\n"
        "  --era1 <dir>           Directory containing era1 files\n"
        "  --jwt-secret <path>    JWT secret file for Engine API (required for CL sync)\n"
        "  --data-dir <path>      Data directory (default: %s)\n"
        "  --port <port>          Engine API port (default: 8551)\n"
        "  --host <addr>          Engine API host (default: 127.0.0.1)\n"
        "  --clean                Delete existing state and start fresh\n"
        "  --era1-only            Stop after era1 replay (don't start Engine API)\n"
        "  --verbose              Verbose output\n",
        prog, DEFAULT_DATA_DIR);
    exit(1);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    /* Parse CLI args */
    const char *genesis_path   = NULL;
    const char *era1_dir       = NULL;
    const char *jwt_path       = NULL;
    const char *data_dir       = DEFAULT_DATA_DIR;
    const char *host           = "127.0.0.1";
    uint16_t    port           = 8551;
    bool        force_clean    = false;
    bool        era1_only      = false;
    bool        verbose        = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--genesis") == 0 && i + 1 < argc)
            genesis_path = argv[++i];
        else if (strcmp(argv[i], "--era1") == 0 && i + 1 < argc)
            era1_dir = argv[++i];
        else if (strcmp(argv[i], "--jwt-secret") == 0 && i + 1 < argc)
            jwt_path = argv[++i];
        else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc)
            data_dir = argv[++i];
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc)
            port = (uint16_t)atoi(argv[++i]);
        else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc)
            host = argv[++i];
        else if (strcmp(argv[i], "--clean") == 0)
            force_clean = true;
        else if (strcmp(argv[i], "--era1-only") == 0)
            era1_only = true;
        else if (strcmp(argv[i], "--verbose") == 0)
            verbose = true;
        else
            usage(argv[0]);
    }

    /* Create data directory */
    mkdir(data_dir, 0755);

    /* Build paths */
    char mpt_path[512], code_path[512], ckpt_path[512], tip_path[512];
    char blk_path[512];
    snprintf(mpt_path,  sizeof(mpt_path),  "%s/mpt",        data_dir);
    snprintf(code_path, sizeof(code_path),  "%s/code",       data_dir);
    snprintf(ckpt_path, sizeof(ckpt_path),  "%s/checkpoint", data_dir);
    snprintf(tip_path,  sizeof(tip_path),   "%s/tip_state",  data_dir);
    snprintf(blk_path,  sizeof(blk_path),   "%s/blocks.dat", data_dir);

    /* Clean up if requested */
    if (force_clean) {
        printf("--clean: removing existing state\n");
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s/*.idx %s/*.dat %s/mpt* %s/code* "
                 "%s/checkpoint* %s/tip_state* %s/blocks* 2>/dev/null",
                 data_dir, data_dir, data_dir, data_dir,
                 data_dir, data_dir, data_dir);
        (void)system(cmd);
    }

    /* Create chain tip (owns sync engine) */
    chain_tip_config_t tip_cfg = {
        .sync_config = {
            .chain_config        = chain_config_mainnet(),
            .mpt_path            = mpt_path,
            .code_store_path     = code_path,
            .checkpoint_path     = ckpt_path,
            .checkpoint_interval = CHECKPOINT_INTERVAL,
            .validate_state_root = true,
        },
        .tip_state_path = tip_path,
        .merge_block    = PARIS_BLOCK,
        .verbose        = verbose,
    };

    chain_tip_t *tip = chain_tip_create(&tip_cfg);
    if (!tip) {
        fprintf(stderr, "Failed to create chain tip\n");
        return 1;
    }

    /* Check if we need genesis */
    sync_status_t st = chain_tip_get_status(tip);
    uint64_t start_block;

    if (st.last_block > 0) {
        start_block = st.last_block + 1;
        printf("Resumed from block %lu\n", st.last_block);
    } else {
        if (!genesis_path) {
            fprintf(stderr, "Error: --genesis required for fresh start\n");
            chain_tip_destroy(tip);
            return 1;
        }

        /* Read genesis block hash from era1 if available */
        hash_t gen_hash = {0};
        if (era1_dir) {
            archive_t ar;
            if (archive_open(&ar, era1_dir)) {
                if (archive_ensure(&ar, 0, era1_dir)) {
                    uint8_t *hdr_rlp, *body_rlp;
                    size_t hdr_len, body_len;
                    if (era1_read_block(&ar.current, 0,
                                        &hdr_rlp, &hdr_len,
                                        &body_rlp, &body_len)) {
                        gen_hash = hash_keccak256(hdr_rlp, hdr_len);
                        free(hdr_rlp);
                        free(body_rlp);
                    }
                }
                archive_close(&ar);
            }
        }

        if (!chain_tip_load_genesis(tip, genesis_path, &gen_hash)) {
            fprintf(stderr, "Failed to load genesis\n");
            chain_tip_destroy(tip);
            return 1;
        }
        start_block = 1;
    }

    /* =====================================================================
     * Phase 1: Era1 replay (batch mode)
     * ===================================================================== */
    if (era1_dir && start_block <= PARIS_BLOCK) {
        archive_t archive;
        if (!archive_open(&archive, era1_dir)) {
            fprintf(stderr, "Failed to open era1 directory: %s\n", era1_dir);
            chain_tip_destroy(tip);
            return 1;
        }

        uint64_t end_block = PARIS_BLOCK;
        /* Cap to available era1 files */
        uint64_t era1_max = archive.count * ERA1_BLOCKS_PER_FILE - 1;
        if (end_block > era1_max) end_block = era1_max;

        printf("Era1 replay: blocks %lu → %lu\n", start_block, end_block);

        signal(SIGINT, sigint_handler);
        signal(SIGTERM, sigint_handler);

        struct timespec t_start, t_now, t_window;
        clock_gettime(CLOCK_MONOTONIC, &t_start);
        t_window = t_start;
        uint64_t window_txs = 0;

        for (uint64_t bn = start_block; bn <= end_block && !g_shutdown; bn++) {
            if (!archive_ensure(&archive, bn, era1_dir)) {
                fprintf(stderr, "Failed to load era1 file for block %lu\n", bn);
                break;
            }

            /* Decode block */
            uint8_t *hdr_rlp, *body_rlp;
            size_t hdr_len, body_len;
            if (!era1_read_block(&archive.current, bn,
                                 &hdr_rlp, &hdr_len,
                                 &body_rlp, &body_len)) {
                fprintf(stderr, "Failed to read block %lu from era1\n", bn);
                break;
            }

            block_header_t header;
            block_body_t body;
            hash_t blk_hash = hash_keccak256(hdr_rlp, hdr_len);

            if (!block_header_decode_rlp(&header, hdr_rlp, hdr_len)) {
                fprintf(stderr, "Block %lu: header decode failed\n", bn);
                free(hdr_rlp); free(body_rlp);
                break;
            }

            if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
                fprintf(stderr, "Block %lu: body decode failed\n", bn);
                free(hdr_rlp); free(body_rlp);
                break;
            }

            /* Execute via chain_tip batch mode */
            sync_block_result_t result;
            if (!chain_tip_execute_batch(tip, &header, &body, &blk_hash, &result)) {
                fprintf(stderr, "Block %lu: execution failed\n", bn);
                block_body_free(&body);
                free(hdr_rlp); free(body_rlp);
                break;
            }

            window_txs += result.tx_count;

            if (!result.ok) {
                if (result.error == SYNC_GAS_MISMATCH) {
                    fprintf(stderr, "Block %lu: GAS MISMATCH got %lu expected %lu\n",
                            bn, result.actual_gas, result.expected_gas);
                } else {
                    fprintf(stderr, "Block %lu: STATE ROOT MISMATCH\n", bn);
                }
                block_body_free(&body);
                free(hdr_rlp); free(body_rlp);
                break;
            }

            /* Progress report */
            if (bn % CHECKPOINT_INTERVAL == 0) {
                clock_gettime(CLOCK_MONOTONIC, &t_now);
                double win_secs = (t_now.tv_sec - t_window.tv_sec) +
                                  (t_now.tv_nsec - t_window.tv_nsec) / 1e9;
                double tps = window_txs / (win_secs > 0 ? win_secs : 1);
                double bps = CHECKPOINT_INTERVAL / (win_secs > 0 ? win_secs : 1);

                st = chain_tip_get_status(tip);
                printf("Block %lu | %lu txs | %.0f tps | %.0f blk/s | "
                       "ok %lu fail %lu\n",
                       bn, window_txs, tps, bps,
                       st.blocks_ok, st.blocks_fail);

                window_txs = 0;
                t_window = t_now;
            }

            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
        }

        archive_close(&archive);

        if (g_shutdown) {
            printf("\nSIGINT — saving checkpoint\n");
            chain_tip_checkpoint(tip);
        }

        st = chain_tip_get_status(tip);
        double total_secs;
        clock_gettime(CLOCK_MONOTONIC, &t_now);
        total_secs = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
        printf("\nEra1 replay: %lu blocks in %.1fs (%.0f blk/s)\n",
               st.blocks_ok, total_secs,
               st.blocks_ok / (total_secs > 0 ? total_secs : 1));
    }

    if (g_shutdown || era1_only) {
        chain_tip_destroy(tip);
        return 0;
    }

    /* =====================================================================
     * Phase 2: Transition to live CL sync
     * ===================================================================== */
    st = chain_tip_get_status(tip);
    if (st.last_block >= PARIS_BLOCK || !era1_dir) {
        chain_tip_go_live(tip);
    }

    if (!jwt_path && !era1_only) {
        fprintf(stderr, "Error: --jwt-secret required for CL sync\n");
        chain_tip_destroy(tip);
        return 1;
    }

    /* =====================================================================
     * Phase 3: Start Engine API server
     * ===================================================================== */
    engine_config_t eng_cfg = {
        .host           = host,
        .port           = port,
        .jwt_secret_path = jwt_path,
        .tip             = tip,
    };

    g_engine = engine_create(&eng_cfg);
    if (!g_engine) {
        fprintf(stderr, "Failed to create Engine API server\n");
        chain_tip_destroy(tip);
        return 1;
    }

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    printf("Engine API listening on %s:%d\n", host, port);
    printf("Waiting for CL connection...\n");

    int rc = engine_run(g_engine);

    engine_destroy(g_engine);
    chain_tip_destroy(tip);
    return rc;
}
