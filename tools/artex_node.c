/**
 * Artex Node — Full Ethereum Execution Client
 *
 * Two-phase sync:
 *   Phase 1: Era1 replay (genesis → Paris) in batch mode
 *   Phase 2: CL sync (Paris → head) via Engine API in live mode
 *
 * Usage:
 *   artex_node --genesis <path> --era1 <dir> --jwt-secret <path>
 *              [--data-dir <path>] [--port <n>] [--host <addr>]
 *              [--clean] [--era1-only] [--checkpoint-interval <n>]
 */

#include "sync.h"
#include "engine.h"
#include "evm_state.h"
#include "era1.h"
#include "block.h"
#include "hash.h"
#include "fork.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#define ERA1_BLOCKS_PER_FILE 8192
#define PARIS_BLOCK          15537394

#ifndef CHECKPOINT_INTERVAL
#define CHECKPOINT_INTERVAL  256
#endif

/* =========================================================================
 * Signal handling
 * ========================================================================= */

static volatile sig_atomic_t g_shutdown = 0;
static engine_t *g_engine = NULL;

static void signal_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    if (g_engine)
        engine_stop(g_engine);
}

/* =========================================================================
 * Era1 archive management (same pattern as chain_replay)
 * ========================================================================= */

typedef struct {
    char     **paths;
    size_t     count;
    era1_t     current;
    int        current_idx;
} era1_archive_t;

static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static bool archive_open(era1_archive_t *ar, const char *dir) {
    memset(ar, 0, sizeof(*ar));
    ar->current_idx = -1;

    DIR *d = opendir(dir);
    if (!d) { perror("opendir"); return false; }

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

static bool archive_ensure(era1_archive_t *ar, uint64_t block_number) {
    if (ar->current_idx >= 0 && era1_contains(&ar->current, block_number))
        return true;

    if (ar->current_idx >= 0) {
        era1_close(&ar->current);
        ar->current_idx = -1;
    }

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
        fprintf(stderr, "File %s doesn't contain block %lu\n",
                ar->paths[file_idx], block_number);
        era1_close(&ar->current);
        ar->current_idx = -1;
        return false;
    }
    return true;
}

/* =========================================================================
 * Stats helpers
 * ========================================================================= */

static size_t get_rss_kb(void) {
    FILE *f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[256];
    size_t rss = 0;
    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            rss = (size_t)atol(line + 6);
            break;
        }
    }
    fclose(f);
    return rss;
}

static void print_stats(sync_t *sync) {
    evm_state_stats_t ss = sync_get_state_stats(sync);
    size_t rss_mb = get_rss_kb() / 1024;
    printf("  | cache: %zuK accts, %zuK slots (%zuMB arena)\n",
           ss.cache_accounts / 1000, ss.cache_slots / 1000,
           ss.cache_arena_bytes / (1024*1024));
#ifdef ENABLE_MPT
    printf("  | mpt: acct %luK nodes, stor %luK nodes\n",
           ss.acct_mpt_nodes / 1000,
           ss.stor_mpt_nodes / 1000);
    printf("  | code: %luK (hit %.1f%%, LRU %u/%uK) | disk: %.1fGB/%.1fGB | RSS %zuMB\n",
           ss.code_count / 1000,
           (ss.code_cache_hits + ss.code_cache_misses)
               ? 100.0 * ss.code_cache_hits / (ss.code_cache_hits + ss.code_cache_misses) : 0,
           ss.code_cache_count / 1000, ss.code_cache_capacity / 1000,
           ss.acct_mpt_data_bytes / 1e9, ss.stor_mpt_data_bytes / 1e9,
           rss_mb);
#else
    printf("  | RSS %zuMB\n", rss_mb);
#endif
}

/* =========================================================================
 * CLI argument parsing
 * ========================================================================= */

typedef struct {
    const char *genesis_path;
    const char *era1_dir;
    const char *jwt_secret_path;
    const char *data_dir;
    const char *host;
    uint16_t    port;
    uint32_t    checkpoint_interval;
    bool        clean;
    bool        era1_only;
} artex_args_t;

static void print_usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --genesis <path> --era1 <dir> --jwt-secret <path>\n"
        "          [--data-dir <path>] [--port <n>] [--host <addr>]\n"
        "          [--clean] [--era1-only] [--checkpoint-interval <n>]\n"
        "\n"
        "Options:\n"
        "  --genesis <path>        Genesis JSON file (required)\n"
        "  --era1 <dir>            Era1 archive directory (required)\n"
        "  --jwt-secret <path>     JWT secret hex file (required for CL sync)\n"
        "  --data-dir <path>       Data directory (default: data/artex)\n"
        "  --port <n>              Engine API port (default: 8551)\n"
        "  --host <addr>           Engine API listen address (default: 127.0.0.1)\n"
        "  --clean                 Delete existing state and start fresh\n"
        "  --era1-only             Exit after era1 replay (no CL sync)\n"
        "  --checkpoint-interval   Blocks between checkpoints (default: %d)\n",
        prog, CHECKPOINT_INTERVAL);
}

static bool parse_args(int argc, char **argv, artex_args_t *args) {
    memset(args, 0, sizeof(*args));
    args->data_dir = "data/artex";
    args->host = "127.0.0.1";
    args->port = 8551;
    args->checkpoint_interval = CHECKPOINT_INTERVAL;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--genesis") == 0 && i + 1 < argc) {
            args->genesis_path = argv[++i];
        } else if (strcmp(argv[i], "--era1") == 0 && i + 1 < argc) {
            args->era1_dir = argv[++i];
        } else if (strcmp(argv[i], "--jwt-secret") == 0 && i + 1 < argc) {
            args->jwt_secret_path = argv[++i];
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            args->data_dir = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            args->port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            args->host = argv[++i];
        } else if (strcmp(argv[i], "--checkpoint-interval") == 0 && i + 1 < argc) {
            args->checkpoint_interval = (uint32_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--clean") == 0) {
            args->clean = true;
        } else if (strcmp(argv[i], "--era1-only") == 0) {
            args->era1_only = true;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            exit(0);
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return false;
        }
    }

    if (!args->genesis_path) {
        fprintf(stderr, "Error: --genesis is required\n");
        return false;
    }
    if (!args->era1_dir) {
        fprintf(stderr, "Error: --era1 is required\n");
        return false;
    }
    if (!args->era1_only && !args->jwt_secret_path) {
        fprintf(stderr, "Error: --jwt-secret is required for CL sync (use --era1-only to skip)\n");
        return false;
    }

    return true;
}

/* =========================================================================
 * Data directory setup
 * ========================================================================= */

static bool ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return S_ISDIR(st.st_mode);
    return mkdir(path, 0755) == 0;
}

static char *path_join(const char *dir, const char *name) {
    size_t len = strlen(dir) + 1 + strlen(name) + 1;
    char *path = malloc(len);
    snprintf(path, len, "%s/%s", dir, name);
    return path;
}

/* =========================================================================
 * Era1 Replay (Phase 1)
 * ========================================================================= */

static bool era1_replay(sync_t *sync, era1_archive_t *archive,
                        uint64_t start_block, uint64_t end_block,
                        uint32_t ckpt_interval) {
    struct timespec t_start, t_now, t_window;
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    clock_gettime(CLOCK_MONOTONIC, &t_window);

    uint64_t window_txs = 0;
    uint64_t window_gas = 0;

    printf("Phase 1: Era1 replay — blocks %lu to %lu\n\n", start_block, end_block);

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        if (g_shutdown) {
            printf("\nShutdown requested — saving checkpoint\n");
            sync_checkpoint(sync);
            return false;
        }

        if (!archive_ensure(archive, bn))
            return false;

        uint8_t *hdr_rlp, *body_rlp;
        size_t hdr_len, body_len;
        if (!era1_read_block(&archive->current, bn,
                             &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
            fprintf(stderr, "Block %lu: failed to read from era1\n", bn);
            return false;
        }

        hash_t blk_hash = hash_keccak256(hdr_rlp, hdr_len);

        block_header_t header;
        if (!block_header_decode_rlp(&header, hdr_rlp, hdr_len)) {
            fprintf(stderr, "Block %lu: failed to decode header\n", bn);
            free(hdr_rlp); free(body_rlp);
            return false;
        }

        block_body_t body;
        if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
            fprintf(stderr, "Block %lu: failed to decode body\n", bn);
            free(hdr_rlp); free(body_rlp);
            return false;
        }

        sync_block_result_t result;
        if (!sync_execute_block(sync, &header, &body, &blk_hash, &result)) {
            fprintf(stderr, "Block %lu: fatal execution error\n", bn);
            block_body_free(&body);
            free(hdr_rlp); free(body_rlp);
            return false;
        }

        window_txs += result.tx_count;
        window_gas += result.gas_used;

        if (bn % ckpt_interval == 0 || !result.ok || bn == end_block) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double win_secs = (t_now.tv_sec - t_window.tv_sec) +
                              (t_now.tv_nsec - t_window.tv_nsec) / 1e9;
            double bps = ckpt_interval / (win_secs > 0 ? win_secs : 1);
            double tps = window_txs / (win_secs > 0 ? win_secs : 1);
            double mgps = (window_gas / 1e6) / (win_secs > 0 ? win_secs : 1);
            uint64_t remaining = (bn < PARIS_BLOCK) ? PARIS_BLOCK - bn : 0;
            double eta_hrs = remaining / (bps > 0 ? bps : 1) / 3600.0;
            printf("Block %lu | %lu txs | %.0f tps | %.1f Mgas/s | %.0f blk/s | %luK to Paris (%.1fh)\n",
                   bn, window_txs, tps, mgps, bps,
                   remaining / 1000, eta_hrs);

            if (bn % (ckpt_interval * 8) == 0)
                print_stats(sync);

            window_txs = 0;
            window_gas = 0;
            clock_gettime(CLOCK_MONOTONIC, &t_window);
        }

        bool block_ok = result.ok;
        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (!block_ok) {
            if (result.error == SYNC_GAS_MISMATCH) {
                fprintf(stderr, "Block %lu: GAS MISMATCH  got %lu  expected %lu\n",
                        bn, result.actual_gas, result.expected_gas);
            } else if (result.error == SYNC_ROOT_MISMATCH) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&result.actual_root, got_hex);
                hash_to_hex(&result.expected_root, exp_hex);
                fprintf(stderr, "Block %lu: STATE ROOT MISMATCH\n"
                        "  got:      %s\n  expected: %s\n",
                        bn, got_hex, exp_hex);
            }
            fprintf(stderr, "First failure at block %lu — stopping.\n", bn);
            return false;
        }
    }

    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    artex_args_t args;
    if (!parse_args(argc, argv, &args))
        return 1;

    /* Install signal handlers */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Create data directory */
    if (!ensure_dir(args.data_dir)) {
        fprintf(stderr, "Failed to create data directory: %s\n", args.data_dir);
        return 1;
    }

    char *mpt_path  = path_join(args.data_dir, "mpt");
    char *code_path = path_join(args.data_dir, "code");
    char *ckpt_path = path_join(args.data_dir, "checkpoint");

    /* Clean up old state if requested */
    if (args.clean) {
        printf("--clean: removing existing state and checkpoint\n");
        unlink(ckpt_path);
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s.idx %s.dat %s_storage.idx %s_storage.dat %s.idx %s.dat 2>/dev/null",
                 mpt_path, mpt_path, mpt_path, mpt_path,
                 code_path, code_path);
        (void)system(cmd);
    }

    /* Open era1 archive */
    era1_archive_t archive;
    if (!archive_open(&archive, args.era1_dir)) {
        free(mpt_path); free(code_path); free(ckpt_path);
        return 1;
    }
    printf("Era1 archive: %zu files in %s\n", archive.count, args.era1_dir);

    /* Create sync engine */
    sync_config_t cfg = {
        .chain_config        = chain_config_mainnet(),
        .verkle_value_dir    = NULL,
        .verkle_commit_dir   = NULL,
        .mpt_path            = NULL,
        .checkpoint_path     = ckpt_path,
        .checkpoint_interval = args.checkpoint_interval,
        .validate_state_root = true,
    };
#ifdef ENABLE_MPT
    cfg.mpt_path = mpt_path;
    cfg.code_store_path = code_path;
#endif

    sync_t *sync = sync_create(&cfg);
    if (!sync) {
        fprintf(stderr, "Failed to create sync engine\n");
        archive_close(&archive);
        free(mpt_path); free(code_path); free(ckpt_path);
        return 1;
    }

    uint64_t resumed = sync_resumed_from(sync);
    uint64_t start_block;

    if (resumed > 0) {
        start_block = resumed + 1;
        printf("Resumed from checkpoint at block %lu\n", resumed);
    } else {
        /* Load genesis */
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

        if (!sync_load_genesis(sync, args.genesis_path, &gen_hash)) {
            fprintf(stderr, "Failed to load genesis state\n");
            sync_destroy(sync);
            archive_close(&archive);
            free(mpt_path); free(code_path); free(ckpt_path);
            return 1;
        }
        start_block = 1;
    }

    /* =====================================================================
     * Phase 1: Era1 Replay (batch mode)
     * ===================================================================== */

    uint64_t era1_end = PARIS_BLOCK;
    uint64_t max_era1 = archive.count * ERA1_BLOCKS_PER_FILE - 1;
    if (era1_end > max_era1)
        era1_end = max_era1;

    bool replay_ok = true;
    if (start_block <= era1_end) {
        replay_ok = era1_replay(sync, &archive, start_block, era1_end,
                                args.checkpoint_interval);
    }

    archive_close(&archive);

    if (!replay_ok || g_shutdown || args.era1_only) {
        sync_status_t st = sync_get_status(sync);
        printf("\n===== Summary =====\n");
        printf("Blocks OK:     %lu\n", st.blocks_ok);
        printf("Blocks failed: %lu\n", st.blocks_fail);
        printf("Total gas:     %lu\n", st.total_gas);

        sync_destroy(sync);
        free(mpt_path); free(code_path); free(ckpt_path);
        return (replay_ok && st.blocks_fail == 0) ? 0 : 1;
    }

    /* =====================================================================
     * Transition: batch → live mode
     * ===================================================================== */

    printf("\n===== Transition to CL sync =====\n");
    printf("Saving checkpoint and switching to live mode...\n");
    sync_checkpoint(sync);
    sync_set_live_mode(sync, true);

    /* =====================================================================
     * Phase 2: CL Sync via Engine API (live mode)
     * ===================================================================== */

    printf("Phase 2: CL sync — starting Engine API\n");

    engine_config_t eng_cfg = {
        .host = args.host,
        .port = args.port,
        .jwt_secret_path = args.jwt_secret_path,
        .evm = NULL,
        .evm_state = NULL,
        .sync = sync,
    };

    engine_t *eng = engine_create(&eng_cfg);
    if (!eng) {
        fprintf(stderr, "Failed to create engine\n");
        sync_destroy(sync);
        free(mpt_path); free(code_path); free(ckpt_path);
        return 1;
    }

    g_engine = eng;
    printf("Engine API listening on %s:%d\n", args.host, args.port);
    printf("Waiting for CL connection...\n\n");

    engine_run(eng);  /* blocking — returns on SIGINT/SIGTERM */

    /* =====================================================================
     * Cleanup
     * ===================================================================== */

    printf("\nShutting down...\n");
    g_engine = NULL;
    engine_destroy(eng);

    sync_status_t st = sync_get_status(sync);
    printf("\n===== Summary =====\n");
    printf("Blocks OK:     %lu\n", st.blocks_ok);
    printf("Blocks failed: %lu\n", st.blocks_fail);
    printf("Total gas:     %lu\n", st.total_gas);

    sync_destroy(sync);  /* saves final checkpoint */
    free(mpt_path);
    free(code_path);
    free(ckpt_path);

    return 0;
}
