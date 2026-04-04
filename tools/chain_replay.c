/**
 * Chain Replay Tool
 *
 * Re-executes Ethereum blocks from Era1 archive files.
 * Uses the sync engine for execution, validation, and checkpointing.
 *
 * Usage: ./chain_replay [--clean] [--follow] <era1_dir> <genesis.json> [start_block] [end_block]
 */

#include "sync.h"
#include "evm_state.h"
#include "state.h"
#include "era1.h"
#include "block.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include "evm_tracer.h"
#ifdef ENABLE_TUI
#include "tui.h"
#endif
#include "logger.h"

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

#define LOG_ERR LOG_ERROR

#define ERA1_BLOCKS_PER_FILE 8192
#define PARIS_BLOCK          15537394  /* last PoW block (The Merge) */

/* Reconstruct metadata — same format as state_reconstruct.c */
#define META_MAGIC   0x54504D52  /* "RMPT" */
#define META_VERSION 1

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t last_block;
    uint8_t  state_root[32];
    uint8_t  prune_empty;
    uint8_t  reserved[7];
} reconstruct_meta_t;  /* 56 bytes */

static bool meta_write(const char *path, uint64_t block,
                       const uint8_t root[32], bool prune_empty) {
    reconstruct_meta_t meta = {0};
    meta.magic = META_MAGIC;
    meta.version = META_VERSION;
    meta.last_block = block;
    memcpy(meta.state_root, root, 32);
    meta.prune_empty = prune_empty ? 1 : 0;

    FILE *f = fopen(path, "wb");
    if (!f) return false;
    bool ok = fwrite(&meta, sizeof(meta), 1, f) == 1;
    fclose(f);
    return ok;
}

#ifndef CHECKPOINT_INTERVAL
#define CHECKPOINT_INTERVAL  256
#endif

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

/* Default persistent store paths */
#define DEFAULT_DATA_DIR_NAME ".artex"

static char value_dir[512];
static char commit_dir[512];
static char mpt_path[512];
static char flat_path[512];
static char code_path[512];
static char history_path[512];

static void set_data_paths(const char *data_dir) {
    snprintf(value_dir, sizeof(value_dir), "%s/chain_replay_values", data_dir);
    snprintf(commit_dir, sizeof(commit_dir), "%s/chain_replay_commits", data_dir);
    snprintf(mpt_path, sizeof(mpt_path), "%s/chain_replay_mpt", data_dir);
    snprintf(flat_path, sizeof(flat_path), "%s/chain_replay_flat", data_dir);
    snprintf(code_path, sizeof(code_path), "%s/chain_replay_code", data_dir);
    snprintf(history_path, sizeof(history_path), "%s/chain_replay_history", data_dir);
}

/* =========================================================================
 * Graceful shutdown via SIGINT
 * ========================================================================= */

static volatile sig_atomic_t g_shutdown = 0;
static volatile sig_atomic_t g_shutdown_pending = 0;

static void sigint_handler(int sig) {
    (void)sig;
    if (g_shutdown) {
        /* Second Ctrl+C — force immediate exit from loop */
        g_shutdown = 2;
    } else {
        g_shutdown = 1;
    }
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

/* Rescan the era1 directory for newly downloaded files */
static bool archive_rescan(era1_archive_t *ar, const char *dir) {
    /* Close current file */
    if (ar->current_idx >= 0) {
        era1_close(&ar->current);
        ar->current_idx = -1;
    }

    /* Free old paths */
    for (size_t i = 0; i < ar->count; i++)
        free(ar->paths[i]);
    free(ar->paths);
    ar->paths = NULL;
    ar->count = 0;

    /* Re-open directory */
    DIR *d = opendir(dir);
    if (!d) return false;

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

    if (ar->count > 0)
        qsort(ar->paths, ar->count, sizeof(char *), cmp_strings);
    return true;
}

/* Ensure the correct era1 file is open for the given block.
 * If follow=true and the file isn't available yet, poll until it appears. */
static bool archive_ensure(era1_archive_t *ar, uint64_t block_number,
                           bool follow, const char *dir) {
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

    /* If file not available, either fail or wait */
    while ((size_t)file_idx >= ar->count) {
        if (!follow) {
            fprintf(stderr, "No era1 file for block %lu (have %zu files)\n",
                    block_number, ar->count);
            return false;
        }
        if (g_shutdown) return false;
        printf("Waiting for era1 file %05d (block %lu)...\n",
               file_idx, block_number);
        sleep(5);
        archive_rescan(ar, dir);
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
 * Block prefetch — decode next block while executor runs current one
 * ========================================================================= */

typedef struct {
    /* Decoded block data (owned by prefetch, swapped to consumer) */
    uint8_t        *hdr_rlp;
    size_t          hdr_len;
    uint8_t        *body_rlp;
    size_t          body_len;
    block_header_t  header;
    block_body_t    body;
    hash_t          blk_hash;
    uint64_t        block_number;
    bool            valid;       /* decode succeeded */
} prefetched_block_t;

typedef struct {
    pthread_t       thread;
    pthread_mutex_t mutex;
    pthread_cond_t  cond_ready;    /* prefetch → main: block is ready */
    pthread_cond_t  cond_consumed; /* main → prefetch: buffer consumed */

    era1_archive_t  archive;       /* own archive instance */
    const char     *era1_dir;
    bool            follow_mode;

    prefetched_block_t buf;        /* single-slot buffer */
    bool            has_data;      /* buffer contains a ready block */
    uint64_t        next_block;    /* next block to prefetch */
    uint64_t        end_block;
    bool            stop;          /* signal thread to exit */
    bool            error;         /* prefetch hit a fatal error */
} block_prefetch_t;

static void *prefetch_thread_fn(void *arg) {
    block_prefetch_t *pf = (block_prefetch_t *)arg;

    while (1) {
        pthread_mutex_lock(&pf->mutex);

        /* Wait until buffer is consumed or stop requested */
        while (pf->has_data && !pf->stop)
            pthread_cond_wait(&pf->cond_consumed, &pf->mutex);

        if (pf->stop || pf->next_block > pf->end_block) {
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        uint64_t bn = pf->next_block++;
        pthread_mutex_unlock(&pf->mutex);

        /* Read + decode outside the lock */
        prefetched_block_t blk;
        memset(&blk, 0, sizeof(blk));
        blk.block_number = bn;
        blk.valid = false;

        if (!archive_ensure(&pf->archive, bn, pf->follow_mode, pf->era1_dir)) {
            pthread_mutex_lock(&pf->mutex);
            pf->error = true;
            pf->has_data = false;
            pthread_cond_signal(&pf->cond_ready);
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        if (!era1_read_block(&pf->archive.current, bn,
                             &blk.hdr_rlp, &blk.hdr_len,
                             &blk.body_rlp, &blk.body_len)) {
            pthread_mutex_lock(&pf->mutex);
            pf->error = true;
            pf->has_data = false;
            pthread_cond_signal(&pf->cond_ready);
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        blk.blk_hash = hash_keccak256(blk.hdr_rlp, blk.hdr_len);

        if (!block_header_decode_rlp(&blk.header, blk.hdr_rlp, blk.hdr_len) ||
            !block_body_decode_rlp(&blk.body, blk.body_rlp, blk.body_len)) {
            free(blk.hdr_rlp);
            free(blk.body_rlp);
            pthread_mutex_lock(&pf->mutex);
            pf->error = true;
            pf->has_data = false;
            pthread_cond_signal(&pf->cond_ready);
            pthread_mutex_unlock(&pf->mutex);
            break;
        }

        blk.valid = true;

        /* Hand off to consumer */
        pthread_mutex_lock(&pf->mutex);
        pf->buf = blk;
        pf->has_data = true;
        pthread_cond_signal(&pf->cond_ready);
        pthread_mutex_unlock(&pf->mutex);
    }

    return NULL;
}

static bool prefetch_start(block_prefetch_t *pf, const char *era1_dir,
                           uint64_t start_block, uint64_t end_block,
                           bool follow) {
    memset(pf, 0, sizeof(*pf));
    pthread_mutex_init(&pf->mutex, NULL);
    pthread_cond_init(&pf->cond_ready, NULL);
    pthread_cond_init(&pf->cond_consumed, NULL);

    pf->era1_dir = era1_dir;
    pf->follow_mode = follow;
    pf->next_block = start_block;
    pf->end_block = end_block;

    if (!archive_open(&pf->archive, era1_dir))
        return false;

    pthread_create(&pf->thread, NULL, prefetch_thread_fn, pf);
    return true;
}

/** Get next prefetched block. Caller takes ownership of rlp buffers + body. */
static bool prefetch_get(block_prefetch_t *pf, prefetched_block_t *out) {
    pthread_mutex_lock(&pf->mutex);

    while (!pf->has_data && !pf->error && !pf->stop)
        pthread_cond_wait(&pf->cond_ready, &pf->mutex);

    if (pf->error || !pf->has_data) {
        pthread_mutex_unlock(&pf->mutex);
        return false;
    }

    *out = pf->buf;
    pf->has_data = false;
    pthread_cond_signal(&pf->cond_consumed);
    pthread_mutex_unlock(&pf->mutex);
    return true;
}

static void prefetch_stop(block_prefetch_t *pf) {
    pthread_mutex_lock(&pf->mutex);
    pf->stop = true;
    pthread_cond_signal(&pf->cond_consumed);
    pthread_mutex_unlock(&pf->mutex);

    pthread_join(pf->thread, NULL);
    archive_close(&pf->archive);
    pthread_mutex_destroy(&pf->mutex);
    pthread_cond_destroy(&pf->cond_ready);
    pthread_cond_destroy(&pf->cond_consumed);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    /* Parse flags */
    bool force_clean = false;
    bool follow_mode = false;
    bool resume_mode = false;
#ifdef ENABLE_DEBUG
    bool no_evict = false;
#endif
#ifdef ENABLE_EVM_TRACE
    uint64_t trace_block = UINT64_MAX;  /* UINT64_MAX = no tracing */
#endif
    uint64_t dump_prestate_block = UINT64_MAX;
    const char *dump_prestate_path = NULL;
    uint64_t save_state_block = UINT64_MAX;
    const char *save_state_path = NULL;
    const char *load_state_path = NULL;
    bool no_validate = false;
    uint64_t snapshot_every = 0;  /* 0 = disabled */
#ifdef ENABLE_HISTORY
    bool no_history = false;
#endif
    bool use_tui = true;
    uint32_t validate_every = CHECKPOINT_INTERVAL;
    /* Default data dir: ~/.artex */
    char default_data_dir[512];
    const char *home = getenv("HOME");
    snprintf(default_data_dir, sizeof(default_data_dir), "%s/%s",
             home ? home : ".", DEFAULT_DATA_DIR_NAME);
    const char *data_dir = default_data_dir;
    int arg_offset = 0;
    while (arg_offset + 1 < argc && argv[1 + arg_offset][0] == '-') {
        if (strcmp(argv[1 + arg_offset], "--no-tui") == 0) {
            use_tui = false;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--clean") == 0) {
            force_clean = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--follow") == 0) {
            follow_mode = true;
            arg_offset++;
#ifdef ENABLE_DEBUG
        } else if (strcmp(argv[1 + arg_offset], "--no-evict") == 0) {
            no_evict = true;
            arg_offset++;
#endif
        } else if (strcmp(argv[1 + arg_offset], "--data-dir") == 0 && arg_offset + 2 < argc) {
            data_dir = argv[2 + arg_offset];
            arg_offset += 2;
#ifdef ENABLE_HISTORY
        } else if (strcmp(argv[1 + arg_offset], "--no-history") == 0) {
            no_history = true;
            arg_offset++;
#endif
        } else if (strcmp(argv[1 + arg_offset], "--resume") == 0) {
            resume_mode = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--validate-every") == 0 && arg_offset + 2 < argc) {
            validate_every = (uint32_t)atoi(argv[2 + arg_offset]);
            if (validate_every == 0) validate_every = 1;
            arg_offset += 2;
#ifdef ENABLE_EVM_TRACE
        } else if (strcmp(argv[1 + arg_offset], "--trace-block") == 0 && arg_offset + 2 < argc) {
            trace_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
#endif
        } else if (strcmp(argv[1 + arg_offset], "--no-validate") == 0) {
            no_validate = true;
            arg_offset++;
        } else if (strcmp(argv[1 + arg_offset], "--snapshot-every") == 0 && arg_offset + 2 < argc) {
            snapshot_every = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--save-state") == 0 && arg_offset + 2 < argc) {
            save_state_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--load-state") == 0 && arg_offset + 2 < argc) {
            load_state_path = argv[2 + arg_offset];
            arg_offset += 2;
        } else if (strcmp(argv[1 + arg_offset], "--dump-prestate") == 0 && arg_offset + 2 < argc) {
            dump_prestate_block = (uint64_t)atoll(argv[2 + arg_offset]);
            arg_offset += 2;
            /* Optional output path */
            if (arg_offset + 1 < argc && argv[1 + arg_offset][0] != '-') {
                dump_prestate_path = argv[1 + arg_offset];
                arg_offset++;
            }
        } else {
            break;
        }
    }

    if (argc - arg_offset < 3) {
        fprintf(stderr,
            "Usage: %s [options] <era1_dir> <genesis.json> [start_block] [end_block]\n"
            "\n"
            "Options:\n"
            "  --no-tui              Disable ncurses terminal UI\n"
            "  --clean               Delete existing state files, start from genesis\n"
            "  --follow              Wait for new era1 files instead of stopping\n"
            "  --data-dir DIR        Set data directory for all state files (default: ~/.artex)\n"
            "  --resume              Resume from existing MPT state (reads .meta)\n"
            "  --no-history          Disable per-block state diff history\n"
            "  --trace-block N       Enable EIP-3155 EVM trace for block N (to stderr)\n"
            "  --no-validate         Skip all root validation, compute once at end\n"
            "  --snapshot-every N    Auto-save state every N blocks (e.g. 1000000)\n"
            "  --save-state N [P]    Save full state after block N to binary file P\n"
            "                        (default: state_<N>.bin)\n"
            "  --load-state P        Load state from binary file P, resume from that block\n"
            "  --dump-prestate N [P] Dump pre-state alloc.json for block N to path P\n"
            "                        (default: alloc_<N>.json). Two-pass: executes block\n"
            "                        to discover accessed accounts, then reloads checkpoint\n"
            "                        and dumps their pre-execution values.\n"
            "  --validate-every N    Validate state root every N blocks (default: %d)\n"
            "\n"
            "Validates state root every %d blocks\n",
            argv[0], CHECKPOINT_INTERVAL, CHECKPOINT_INTERVAL);
        return 1;
    }

    set_data_paths(data_dir);

    /* Ensure data directory exists */
    mkdir(data_dir, 0755);

    const char *era1_dir     = argv[1 + arg_offset];
    const char *genesis_path = argv[2 + arg_offset];
    uint64_t user_start = (argc - arg_offset > 3)
                          ? (uint64_t)atoll(argv[3 + arg_offset]) : 0;
    uint64_t end_block  = (argc - arg_offset > 4)
                          ? (uint64_t)atoll(argv[4 + arg_offset]) : UINT64_MAX;

    /* Install signal handler — no SA_RESTART so blocking reads can be interrupted */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT, &sa, NULL);  /* intentionally no SA_RESTART for SIGINT */

    /* Initialize TUI before heavy work so user sees progress */
#ifdef ENABLE_TUI
    if (use_tui) {
        if (!tui_init()) {
            use_tui = false;
        } else {
            freopen("/dev/null", "w", stdout);
            freopen("/tmp/chain_replay_tui.log", "w", stderr);

            /* Build info bar */
            char build[256];
            snprintf(build, sizeof(build), "ARTEX  "
                "MPT:%s  VERKLE:%s  HISTORY:%s  DEBUG:%s  EVM_TRACE:%s  CKPT:%d",
                "ON",
                "off",
#ifdef ENABLE_HISTORY
                "ON",
#else
                "off",
#endif
#ifdef ENABLE_DEBUG
                "ON",
#else
                "off",
#endif
#ifdef ENABLE_EVM_TRACE
                "ON",
#else
                "off",
#endif
                CHECKPOINT_INTERVAL
            );
            tui_set_build_info(build);

            LOG_INFO("Starting chain_replay...");
            LOG_INFO("Era1 dir: %s", era1_dir);
            LOG_INFO("Genesis:  %s", genesis_path);
            LOG_INFO("Data dir: %s", data_dir);
            LOG_WARN("Press Ctrl+C for graceful shutdown");
        }
    }
#else
    (void)use_tui;
    use_tui = false;
#endif

    /* Open era1 archive */
    era1_archive_t archive;
    if (!archive_open(&archive, era1_dir)) {
        LOG_ERR("Failed to open era1 directory: %s", era1_dir);
#ifdef ENABLE_TUI
        if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
        return 1;
    }
    LOG_INFO("Era1 archive: %zu files in %s", archive.count, era1_dir);

    /* Clean up old state if requested */
    if (force_clean) {
        printf("--clean: removing existing state files\n");
        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s %s %s.idx %s.dat %s_storage.idx %s_storage.dat"
                 " %s.idx %s.dat %s_acct.art %s_stor.art %s.meta"
                 " 2>/dev/null",
                 value_dir, commit_dir, mpt_path, mpt_path, mpt_path, mpt_path,
                 code_path, code_path, flat_path, flat_path, mpt_path);
        (void)system(cmd);
#ifdef ENABLE_HISTORY
        snprintf(cmd, sizeof(cmd), "rm -rf %s 2>/dev/null", history_path);
        (void)system(cmd);
#endif
    }

    /* Create sync engine */
    sync_config_t cfg = {
        .chain_config        = chain_config_mainnet(),
        .verkle_value_dir    = NULL,
        .verkle_commit_dir   = NULL,
        .mpt_path            = NULL,
        .checkpoint_interval = validate_every,
        .validate_state_root = !no_validate,
#ifdef ENABLE_DEBUG
        .no_evict            = no_evict,
#endif
    };
#ifdef ENABLE_HISTORY
    if (!no_history)
        cfg.history_dir = history_path;
#endif
    cfg.mpt_path = mpt_path;
    cfg.code_store_path = code_path;

    LOG_INFO("Loading state...");
#ifdef ENABLE_TUI
    if (use_tui) { tui_tick(); }
#endif
    sync_t *sync = sync_create(&cfg);
    if (!sync) {
        LOG_ERR("Failed to create sync engine");
#ifdef ENABLE_TUI
        if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
        archive_close(&archive);
        return 1;
    }
    LOG_INFO("State loaded successfully");
    {
        evm_state_stats_t dbg_ss = sync_get_state_stats(sync);
        LOG_INFO("state: %lu accts, %lu storage_accts",
                 (unsigned long)dbg_ss.cache_accounts,
                 (unsigned long)dbg_ss.cache_slots);
    }

    uint64_t start_block = (user_start > 0) ? user_start : 1;

    if (load_state_path) {
        /* Load state from binary snapshot */
        evm_state_t *es = sync_get_state(sync);
        state_t *st = evm_state_get_state(es);
        hash_t loaded_root;
        if (!state_load(st, load_state_path, &loaded_root)) {
            LOG_ERR("Failed to load state from %s", load_state_path);
#ifdef ENABLE_TUI
            if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
        state_stats_t ss = state_get_stats(st);
        /* current_block was set by state_load */
        uint64_t loaded_block = state_get_block(st);
        start_block = loaded_block + 1;
        char root_hex[67];
        hash_to_hex(&loaded_root, root_hex);
        LOG_INFO("Loaded state: %u accounts at block %lu from %s (root: %s)",
                 ss.account_count, loaded_block, load_state_path, root_hex);

        /* Populate block hash ring from era1 */
        uint64_t hash_start = loaded_block >= 256 ? loaded_block - 255 : 0;
        size_t hash_count = (size_t)(loaded_block - hash_start + 1);
        hash_t *hashes = calloc(hash_count, sizeof(hash_t));
        if (hashes) {
            for (uint64_t i = 0; i < hash_count; i++) {
                uint64_t hbn = hash_start + i;
                if (archive_ensure(&archive, hbn, false, era1_dir)) {
                    uint8_t *h_rlp = NULL; size_t h_len = 0;
                    uint8_t *b_rlp = NULL; size_t b_len = 0;
                    if (era1_read_block(&archive.current, hbn,
                                         &h_rlp, &h_len, &b_rlp, &b_len)) {
                        hashes[i] = hash_keccak256(h_rlp, h_len);
                        free(h_rlp); free(b_rlp);
                    }
                }
            }
            sync_resume(sync, loaded_block, hashes, hash_count);
            free(hashes);
        }
    } else if (resume_mode) {
        /* Read .meta to find last good block */
        char meta_path[512];
        snprintf(meta_path, sizeof(meta_path), "%s.meta", mpt_path);
        reconstruct_meta_t meta;
        FILE *mf = fopen(meta_path, "rb");
        if (!mf || fread(&meta, sizeof(meta), 1, mf) != 1 ||
            meta.magic != META_MAGIC) {
            if (mf) fclose(mf);
            LOG_ERR("No valid .meta file at %s — cannot resume", meta_path);
#ifdef ENABLE_TUI
            if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
        fclose(mf);

        LOG_INFO("Resuming from block %lu", meta.last_block);
        start_block = meta.last_block + 1;

        /* Truncate history to last good block (removes stale/buggy diffs) */
        sync_truncate_history(sync, meta.last_block);

        /* Populate block hash ring from era1 (last 256 blocks) */
        uint64_t hash_start = meta.last_block >= 256 ? meta.last_block - 255 : 0;
        size_t hash_count = (size_t)(meta.last_block - hash_start + 1);
        hash_t *hashes = calloc(hash_count, sizeof(hash_t));
        if (hashes) {
            for (uint64_t i = 0; i < hash_count; i++) {
                uint64_t hbn = hash_start + i;
                if (archive_ensure(&archive, hbn, false, era1_dir)) {
                    uint8_t *h_rlp = NULL; size_t h_len = 0;
                    uint8_t *b_rlp = NULL; size_t b_len = 0;
                    if (era1_read_block(&archive.current, hbn,
                                         &h_rlp, &h_len, &b_rlp, &b_len)) {
                        hashes[i] = hash_keccak256(h_rlp, h_len);
                        free(h_rlp); free(b_rlp);
                    }
                }
            }
            sync_resume(sync, meta.last_block, hashes, hash_count);
            free(hashes);
        }
    } else {
        /* Read genesis block hash from era1 */
        hash_t gen_hash = {0};
        if (archive_ensure(&archive, 0, false, era1_dir)) {
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
            LOG_ERR("Failed to load genesis state");
#ifdef ENABLE_TUI
            if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
            sync_destroy(sync);
            archive_close(&archive);
            return 1;
        }
    }

    /* Progress tracking */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    uint64_t display_end = end_block == UINT64_MAX
        ? (uint64_t)(archive.count * ERA1_BLOCKS_PER_FILE - 1)
        : end_block;

    if (!use_tui)
        printf("Replaying blocks %lu to %lu...\n\n", start_block, display_end);
    else
        LOG_INFO("Replaying blocks %lu to %lu...", start_block, display_end);

    uint64_t window_txs = 0;
    uint64_t window_gas = 0;
    uint64_t window_transfers = 0;
    uint64_t window_calls = 0;
    struct timespec t_window;
    clock_gettime(CLOCK_MONOTONIC, &t_window);
    struct timespec t_last_tui_update = {0, 0}; /* force immediate first update */

    /* TUI rolling window — independent of checkpoint window for smooth stats */
    uint64_t tui_window_blocks = 0;
    uint64_t tui_window_txs = 0;
    uint64_t tui_window_gas = 0;
    uint64_t tui_window_transfers = 0;
    uint64_t tui_window_calls = 0;
    struct timespec t_tui_window;
    clock_gettime(CLOCK_MONOTONIC, &t_tui_window);
    #define TUI_WINDOW_SECS 2.0  /* reset rolling window every 2s */


    /* Start block prefetch thread */
    block_prefetch_t prefetch;
    if (!prefetch_start(&prefetch, era1_dir, start_block, end_block, follow_mode)) {
        LOG_ERR("Failed to start block prefetch");
        sync_destroy(sync);
        archive_close(&archive);
#ifdef ENABLE_TUI
        if (use_tui) { tui_set_finished(); while (tui_tick()) usleep(50000); tui_shutdown(); freopen("/dev/tty", "w", stdout); freopen("/dev/tty", "w", stderr); }
#endif
        return 1;
    }

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        /* Graceful shutdown: finish current batch, checkpoint at next boundary */
        if (g_shutdown && !g_shutdown_pending) {
            g_shutdown_pending = true;
            LOG_WARN("SIGINT received - finishing batch to next checkpoint...");
            LOG_WARN("(press Ctrl+C again to stop immediately)");
        }
        /* Second Ctrl+C — stop immediately, sync_destroy handles cleanup */
        if (g_shutdown >= 2) {
            LOG_WARN("Forced stop - cleaning up...");
            break;
        }

        /* Get next block from prefetch thread */
        prefetched_block_t blk;
        if (!prefetch_get(&prefetch, &blk)) {
            LOG_ERROR("Block %lu: prefetch failed", bn);
            break;
        }

        uint8_t *hdr_rlp = blk.hdr_rlp;
        uint8_t *body_rlp = blk.body_rlp;
        block_header_t header = blk.header;
        block_body_t body = blk.body;
        hash_t blk_hash = blk.blk_hash;

        /* Flush pre-execution state for dump-prestate (before target block runs) */
        if (bn == dump_prestate_block) {
            evm_state_t *pre_es = sync_get_state(sync);
            bool pe = (header.number >= 2675000);
            evm_state_compute_mpt_root(pre_es, pe);
            evm_state_flush(pre_es);
        }

        /* Enable EVM tracing for the target block */
#ifdef ENABLE_EVM_TRACE
        if (bn == trace_block) {
            evm_tracer_init(stderr);
            fprintf(stderr, "=== EVM TRACE: block %lu ===\n", bn);
        }
#endif

        /* Execute + validate via sync engine */
        sync_block_result_t result;
        if (!sync_execute_block(sync, &header, &body, &blk_hash, &result)) {
            LOG_ERROR("Block %lu: fatal execution error", bn);
            LOG_WARN("hint: sync_execute_block returned false (internal error, not validation)");
            LOG_WARN("hint: check for OOM, disk I/O errors, or corrupt era1 data");
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        /* Disable tracing after the target block */
#ifdef ENABLE_EVM_TRACE
        if (bn == trace_block) {
            g_evm_tracer.enabled = false;
            fprintf(stderr, "=== END EVM TRACE: block %lu ===\n", bn);
        }
#endif

        /* Dump prestate for this block (two-pass) */
        if (bn == dump_prestate_block) {
            evm_state_t *es = sync_get_state(sync);

            /* Pass 1: Collect all accessed addresses and storage keys */
            #define MAX_ADDRS  8192
            #define MAX_SLOTS  65536
            address_t *addrs = malloc(MAX_ADDRS * sizeof(address_t));
            size_t n_addrs = evm_state_collect_addresses(es, addrs, MAX_ADDRS);
            printf("Prestate: collected %zu addresses from cache\n", n_addrs);

            /* Collect storage keys per address */
            typedef struct { address_t addr; uint256_t *keys; size_t count; } addr_slots_t;
            addr_slots_t *addr_slots = malloc(n_addrs * sizeof(addr_slots_t));
            uint256_t *slot_buf = malloc(MAX_SLOTS * sizeof(uint256_t));
            size_t total_slots = 0;
            for (size_t i = 0; i < n_addrs; i++) {
                addr_slots[i].addr = addrs[i];
                size_t n = evm_state_collect_storage_keys(es, &addrs[i],
                    slot_buf + total_slots,
                    MAX_SLOTS - total_slots);
                addr_slots[i].keys = slot_buf + total_slots;
                addr_slots[i].count = n;
                total_slots += n;
            }
            printf("Prestate: collected %zu storage keys\n", total_slots);

            /* Pass 2: Destroy sync, recreate from flushed pre-execution state */
            sync_destroy(sync);
            sync = sync_create(&cfg);
            if (!sync) {
                fprintf(stderr, "Failed to recreate sync for prestate dump\n");
                free(addrs); free(addr_slots); free(slot_buf);
                archive_close(&archive);
                return 1;
            }

            /* Re-populate block hash ring (lost on sync_destroy) */
            {
                uint64_t bh_start = bn > 256 ? bn - 256 : 0;
                size_t bh_count = (size_t)(bn - bh_start);
                hash_t *bh_hashes = calloc(bh_count, sizeof(hash_t));
                if (bh_hashes) {
                    for (uint64_t i = 0; i < bh_count; i++) {
                        uint64_t bh_bn = bh_start + i;
                        if (archive_ensure(&archive, bh_bn, false, era1_dir)) {
                            uint8_t *h_rlp = NULL; size_t h_len = 0;
                            uint8_t *b_rlp = NULL; size_t b_len = 0;
                            if (era1_read_block(&archive.current, bh_bn,
                                                 &h_rlp, &h_len, &b_rlp, &b_len)) {
                                bh_hashes[i] = hash_keccak256(h_rlp, h_len);
                                free(h_rlp); free(b_rlp);
                            }
                        }
                    }
                    sync_resume(sync, bn - 1, bh_hashes, bh_count);
                    free(bh_hashes);
                }
            }

            es = sync_get_state(sync);

            /* Build output path */
            char default_path[256];
            if (!dump_prestate_path) {
                snprintf(default_path, sizeof(default_path), "alloc_%lu.json", bn);
                dump_prestate_path = default_path;
            }

            /* Query each address/slot from clean state and dump as alloc.json */
            FILE *out = fopen(dump_prestate_path, "w");
            if (!out) {
                fprintf(stderr, "Failed to open %s for writing\n", dump_prestate_path);
                free(addrs); free(addr_slots); free(slot_buf);
                break;
            }

            fprintf(out, "{\n");
            for (size_t i = 0; i < n_addrs; i++) {
                address_t *a = &addrs[i];
                /* Touch the account to load it into cache */
                bool exists = evm_state_exists(es, a);
                uint64_t nonce = evm_state_get_nonce(es, a);
                uint256_t balance = evm_state_get_balance(es, a);

                if (i > 0) fprintf(out, ",\n");
                fprintf(out, "  \"0x");
                for (int j = 0; j < 20; j++) fprintf(out, "%02x", a->bytes[j]);
                fprintf(out, "\": {\n");

                /* Balance */
                uint8_t bal[32]; uint256_to_bytes(&balance, bal);
                fprintf(out, "    \"balance\": \"0x");
                int s = 0; while (s < 31 && bal[s] == 0) s++;
                for (int j = s; j < 32; j++) fprintf(out, "%02x", bal[j]);
                fprintf(out, "\",\n");

                /* Nonce */
                fprintf(out, "    \"nonce\": \"0x%lx\"", nonce);

                /* Code */
                uint32_t code_size = evm_state_get_code_size(es, a);
                if (code_size > 0) {
                    const uint8_t *code = evm_state_get_code_ptr(es, a, &code_size);
                    if (code && code_size > 0) {
                        fprintf(out, ",\n    \"code\": \"0x");
                        for (uint32_t c = 0; c < code_size; c++)
                            fprintf(out, "%02x", code[c]);
                        fprintf(out, "\"");
                    }
                }

                /* Storage */
                if (addr_slots[i].count > 0) {
                    fprintf(stderr, "  dumping %zu slots for 0x", addr_slots[i].count);
                    for (int j = 0; j < 20; j++) fprintf(stderr, "%02x", a->bytes[j]);
                    fprintf(stderr, "\n");
                    fprintf(out, ",\n    \"storage\": {");
                    bool first_slot = true;
                    size_t zero_count = 0;
                    for (size_t si = 0; si < addr_slots[i].count; si++) {
                        uint256_t val = evm_state_get_storage(es, a, &addr_slots[i].keys[si]);
                        if (uint256_is_zero(&val)) { zero_count++; continue; }
                        if (!first_slot) fprintf(out, ",");
                        first_slot = false;
                        uint8_t kbe[32], vbe[32];
                        uint256_to_bytes(&addr_slots[i].keys[si], kbe);
                        uint256_to_bytes(&val, vbe);
                        fprintf(out, "\n      \"0x");
                        for (int j = 0; j < 32; j++) fprintf(out, "%02x", kbe[j]);
                        fprintf(out, "\": \"0x");
                        for (int j = 0; j < 32; j++) fprintf(out, "%02x", vbe[j]);
                        fprintf(out, "\"");
                    }
                    fprintf(out, "\n    }");
                    if (zero_count > 0)
                        fprintf(stderr, "    %zu slots returned zero\n", zero_count);
                }

                fprintf(out, "\n  }");
                (void)exists;
            }
            fprintf(out, "\n}\n");
            fclose(out);

            printf("Prestate dumped to %s (%zu accounts, %zu storage keys)\n",
                   dump_prestate_path, n_addrs, total_slots);

            /* Write env.json with blockHashes for BLOCKHASH opcode support */
            {
                /* Derive env path from alloc path (same directory) */
                char env_path[512];
                const char *last_slash = strrchr(dump_prestate_path, '/');
                if (last_slash) {
                    size_t dir_len = (size_t)(last_slash - dump_prestate_path);
                    snprintf(env_path, sizeof(env_path), "%.*s/env.json",
                             (int)dir_len, dump_prestate_path);
                } else {
                    snprintf(env_path, sizeof(env_path), "env.json");
                }

                FILE *ef = fopen(env_path, "w");
                if (ef) {
                    fprintf(ef, "{\n");
                    fprintf(ef, "  \"currentCoinbase\": \"0x");
                    for (int j = 0; j < 20; j++) fprintf(ef, "%02x", header.coinbase.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"currentDifficulty\": \"0x%lx\",\n",
                            uint256_to_uint64(&header.difficulty));
                    fprintf(ef, "  \"currentGasLimit\": \"0x%lx\",\n", header.gas_limit);
                    fprintf(ef, "  \"currentNumber\": \"0x%lx\",\n", header.number);
                    fprintf(ef, "  \"currentTimestamp\": \"0x%lx\",\n", header.timestamp);
                    fprintf(ef, "  \"parentHash\": \"0x");
                    for (int j = 0; j < 32; j++) fprintf(ef, "%02x", header.parent_hash.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"blockHashes\": {\n");
                    bool first_bh = true;
                    uint64_t start_bh = bn > 256 ? bn - 256 : 1;
                    for (uint64_t bh_num = start_bh; bh_num < bn; bh_num++) {
                        hash_t bh;
                        if (sync_get_block_hash(sync, bh_num, &bh)) {
                            if (!first_bh) fprintf(ef, ",\n");
                            first_bh = false;
                            fprintf(ef, "    \"%lu\": \"0x", bh_num);
                            for (int j = 0; j < 32; j++) fprintf(ef, "%02x", bh.bytes[j]);
                            fprintf(ef, "\"");
                        }
                    }
                    fprintf(ef, "\n  }\n}\n");
                    fclose(ef);
                    printf("Environment dumped to %s (with %lu block hashes)\n",
                           env_path, bn - start_bh);
                }
            }

            free(addrs);
            free(addr_slots);
            free(slot_buf);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;  /* done — exit main loop */
        }

        window_txs += result.tx_count;
        window_transfers += result.transfer_count;
        window_calls += result.call_count;
        window_gas += result.gas_used;

#ifdef ENABLE_TUI
        if (use_tui) {
            tui_window_blocks++;
            tui_window_txs += result.tx_count;
            tui_window_gas += result.gas_used;
            tui_window_transfers += result.transfer_count;
            tui_window_calls += result.call_count;

            if (!tui_tick()) {
                g_shutdown = 1;
                g_shutdown_pending = true;
            }
            /* Update stats panel every ~0.5s for live feel */
            struct timespec t_tui_now;
            clock_gettime(CLOCK_MONOTONIC, &t_tui_now);
            double tui_dt = (t_tui_now.tv_sec - t_last_tui_update.tv_sec) +
                            (t_tui_now.tv_nsec - t_last_tui_update.tv_nsec) / 1e9;
            if (tui_dt >= 0.5) {
                double elapsed = (t_tui_now.tv_sec - t_start.tv_sec) +
                                 (t_tui_now.tv_nsec - t_start.tv_nsec) / 1e9;
                double tui_win = (t_tui_now.tv_sec - t_tui_window.tv_sec) +
                                 (t_tui_now.tv_nsec - t_tui_window.tv_nsec) / 1e9;
                double bps = tui_window_blocks / (tui_win > 0 ? tui_win : 1);
                double ltps = tui_window_txs / (tui_win > 0 ? tui_win : 1);
                double lmgps = (tui_window_gas / 1e6) / (tui_win > 0 ? tui_win : 1);
                evm_state_stats_t lss = sync_get_state_stats(sync);
                sync_history_stats_t lhs = sync_get_history_stats(sync);
                sync_status_t lst = sync_get_status(sync);
                tui_stats_t ts = {
                    .block_number     = bn,
                    .target_block     = display_end < PARIS_BLOCK ? display_end : PARIS_BLOCK,
                    .blocks_per_sec   = bps,
                    .tps              = ltps,
                    .mgas_per_sec     = lmgps,
                    .window_secs      = tui_win,
                    .window_txs       = tui_window_txs,
                    .window_transfers = tui_window_transfers,
                    .window_calls     = tui_window_calls,
                    .checkpoint_interval = validate_every,
                    .cache_accounts   = lss.cache_accounts,
                    .cache_slots      = lss.cache_slots,
                    .cache_arena_mb   = lss.cache_arena_bytes / (1024*1024),
                    .rss_mb           = get_rss_kb() / 1024,
                    .total_blocks_ok  = lst.blocks_ok,
                    .total_blocks_fail = lst.blocks_fail,
                    .elapsed_secs     = elapsed,
                    .root_stor_ms     = lss.root_stor_ms,
                    .root_acct_ms     = lss.root_acct_ms,
                    .root_dirty_count = lss.root_dirty_count,
                    .flat_acct_count = lss.flat_acct_count,
                    .flat_stor_count = lss.flat_stor_count,
                    .code_count       = lss.code_count,
                    .code_cache_hit_pct = (lss.code_cache_hits + lss.code_cache_misses)
                        ? 100.0 * lss.code_cache_hits / (lss.code_cache_hits + lss.code_cache_misses) : 0,
                    .flush_ms         = 0,
                    .checkpoint_total_ms = 0,
                    .history_blocks   = lhs.blocks,
                    .history_mb       = lhs.disk_mb,
                };
                tui_update_stats(&ts);
                t_last_tui_update = t_tui_now;

                /* Reset rolling window if enough time has passed */
                if (tui_win >= TUI_WINDOW_SECS) {
                    tui_window_blocks = 0;
                    tui_window_txs = 0;
                    tui_window_gas = 0;
                    tui_window_transfers = 0;
                    tui_window_calls = 0;
                    t_tui_window = t_tui_now;
                }
            }
        }
#endif

        /* Progress every CHECKPOINT_INTERVAL blocks, on failure, or last block */
        if (bn % CHECKPOINT_INTERVAL == 0 || !result.ok || bn == end_block) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double win_secs = (t_now.tv_sec - t_window.tv_sec) +
                              (t_now.tv_nsec - t_window.tv_nsec) / 1e9;
            double bps = CHECKPOINT_INTERVAL / (win_secs > 0 ? win_secs : 1);
            double tps = window_txs / (win_secs > 0 ? win_secs : 1);
            double mgps = (window_gas / 1e6) / (win_secs > 0 ? win_secs : 1);
            evm_state_stats_t ss = sync_get_state_stats(sync);
            sync_history_stats_t hs = sync_get_history_stats(sync);
            size_t rss_mb = get_rss_kb() / 1024;
            sync_status_t st = sync_get_status(sync);

#ifdef ENABLE_TUI
            if (use_tui) {
                tui_stats_t ts = {
                    .block_number     = bn,
                    .target_block     = display_end < PARIS_BLOCK ? display_end : PARIS_BLOCK,
                    .blocks_per_sec   = bps,
                    .tps              = tps,
                    .mgas_per_sec     = mgps,
                    .window_secs      = win_secs,
                    .window_txs       = window_txs,
                    .window_transfers = window_transfers,
                    .window_calls     = window_calls,
                    .checkpoint_interval = validate_every,
                    .cache_accounts   = ss.cache_accounts,
                    .cache_slots      = ss.cache_slots,
                    .cache_arena_mb   = ss.cache_arena_bytes / (1024*1024),
                    .rss_mb           = rss_mb,
                    .total_blocks_ok  = st.blocks_ok,
                    .total_blocks_fail = st.blocks_fail,
                    .elapsed_secs     = elapsed,
                    .root_stor_ms     = ss.root_stor_ms,
                    .root_acct_ms     = ss.root_acct_ms,
                    .root_dirty_count = ss.root_dirty_count,
                    .flat_acct_count = ss.flat_acct_count,
                    .flat_stor_count = ss.flat_stor_count,
                    .code_count       = ss.code_count,
                    .code_cache_hit_pct = (ss.code_cache_hits + ss.code_cache_misses)
                        ? 100.0 * ss.code_cache_hits / (ss.code_cache_hits + ss.code_cache_misses) : 0,
                    .flush_ms         = 0,
                    .checkpoint_total_ms = 0,
                    .history_blocks   = hs.blocks,
                    .history_mb       = hs.disk_mb,
                };
                tui_update_stats(&ts);
                if (bn % CHECKPOINT_INTERVAL == 0)
                    LOG_INFO("Checkpoint validated at block %lu", bn);
            } else
#endif
            {
                uint64_t remaining = (bn < PARIS_BLOCK) ? PARIS_BLOCK - bn : 0;
                printf("Block %lu | %lu txs (%luT %luC) | %.0f tps | %.1f Mgas/s | %.0f blk/s | %.1fs/256blk | %luK to Paris\n",
                       bn, window_txs, window_transfers, window_calls,
                       tps, mgps, bps, win_secs,
                       remaining / 1000);

                /* Get detailed memory breakdown directly from state */
                state_t *_st = evm_state_get_state(sync_get_state(sync));
                state_stats_t ms = _st ? state_get_stats(_st) : (state_stats_t){0};

                printf("  └ %zuK accts %zuK stor | vec=%zuMB res=%zuMB idx=%zuMB stor=%zuMB | tracked=%zuMB RSS=%zuMB\n",
                       ms.account_count / 1000, ms.storage_account_count / 1000,
                       ms.acct_vec_bytes / (1024*1024),
                       ms.res_vec_bytes / (1024*1024),
                       ms.acct_arena_bytes / (1024*1024),
                       ms.stor_arena_bytes / (1024*1024),
                       ms.total_tracked / (1024*1024),
                       rss_mb);
                {
                    double root_ms = ss.wait_flush_ms;
                    double evm_ms = ss.exec_ms - root_ms;
                    double other_ms = win_secs * 1000.0 - ss.exec_ms;
                    printf("  └ evm=%.0fms  root=%.0fms  other=%.0fms\n",
                           evm_ms > 0 ? evm_ms : 0,
                           root_ms,
                           other_ms > 0 ? other_ms : 0);
                }
            }
            window_txs = 0;
            window_gas = 0;
            window_transfers = 0;
            window_calls = 0;
            clock_gettime(CLOCK_MONOTONIC, &t_window);
        }

        /* Write .meta after ensuring all data is on disk.
         * Wait for background flush to complete first, so the .dat file
         * is consistent with the recorded block number. */
        if (result.ok && validate_every > 0 &&
            bn % validate_every == 0) {
            sync_ensure_flushed(sync);
            bool pe = (bn >= 2675000);
            char mp[512];
            snprintf(mp, sizeof(mp), "%s.meta", mpt_path);
            meta_write(mp, bn, header.state_root.bytes, pe);
        }

        /* SIGINT: exit after validated checkpoint */
        if (g_shutdown_pending && result.ok &&
            bn % CHECKPOINT_INTERVAL == 0) {
            LOG_INFO("Checkpoint validated at block %lu - exiting.", bn);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
        }

        /* --save-state: save full state after target block */
        if (bn == save_state_block) {
            state_t *st = evm_state_get_state(sync_get_state(sync));
            char default_save[256];
            const char *sp = save_state_path;
            if (!sp) {
                snprintf(default_save, sizeof(default_save),
                         "state_%lu.bin", bn);
                sp = default_save;
            }
            fprintf(stderr, "Saving state at block %lu to %s...\n", bn, sp);
            if (state_save(st, sp, &header.state_root)) {
                state_stats_t ss = state_get_stats(st);
                fprintf(stderr, "  saved: %u accounts, %.0fMB tracked\n",
                        ss.account_count, ss.total_tracked / 1e6);
            } else {
                fprintf(stderr, "  FAILED to save state!\n");
            }
        }

        /* --snapshot-every: auto-save state at regular intervals */
        if (snapshot_every > 0 && bn % snapshot_every == 0) {
            state_t *st = evm_state_get_state(sync_get_state(sync));
            char snap_path[256];
            snprintf(snap_path, sizeof(snap_path), "%s/state_%lu.bin",
                     data_dir, bn);
            if (state_save(st, snap_path, &header.state_root)) {
                state_stats_t ss = state_get_stats(st);
                fprintf(stderr, "  snapshot @%lu: %u accts, %.0fMB → %s\n",
                        bn, ss.account_count, ss.total_tracked / 1e6, snap_path);
            }
        }

        bool block_ok = result.ok;

        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (!block_ok) {
            if (result.error == SYNC_GAS_MISMATCH) {
                LOG_ERROR("Block %lu: GAS MISMATCH  got %lu  expected %lu  diff %+ld",
                        bn, result.actual_gas, result.expected_gas,
                        (long)result.actual_gas - (long)result.expected_gas);
                LOG_WARN("hint: gas diff usually means opcode cost bug or missing gas rule");

                /* Log our per-tx gas for comparison against era1 expected */
                if (result.receipts && result.receipt_count > 0) {
                    LOG_INFO("per-tx gas (%zu txs):", result.receipt_count);
                    for (size_t ti = 0; ti < result.receipt_count; ti++) {
                        LOG_INFO("  tx %3zu: gas=%7lu  cum=%lu  status=%u",
                                ti, result.receipts[ti].gas_used,
                                result.receipts[ti].cumulative_gas,
                                result.receipts[ti].status_code);
                    }
                    /* Free receipts */
                    for (size_t ti = 0; ti < result.receipt_count; ti++) {
                        if (result.receipts[ti].logs)
                            free(result.receipts[ti].logs);
                    }
                    free(result.receipts);
                    result.receipts = NULL;
                }
            } else if (result.error == SYNC_ROOT_MISMATCH) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&result.actual_root, got_hex);
                hash_to_hex(&result.expected_root, exp_hex);
                LOG_ERROR("Block %lu: STATE ROOT MISMATCH (at batch checkpoint)", bn);
                LOG_ERROR("got:      %s", got_hex);
                LOG_ERROR("expected: %s", exp_hex);
                LOG_WARN("hint: root is validated every %u blocks - bug is in range [%lu..%lu]",
                        validate_every,
                        bn > validate_every ? bn - validate_every + 1 : 1, bn);
                LOG_WARN("hint: if resumed from checkpoint, try --clean to replay fresh");
                LOG_WARN("hint: use evm_statetest for per-block differential fuzzing against geth");
#ifdef ENABLE_DEBUG
                evm_state_debug_dump(sync_get_state(sync));
#endif
            }

            /* Ensure flush before writing .meta */
            sync_ensure_flushed(sync);
            /* Write .meta so state_reconstruct can rebuild to last good checkpoint */
            {
                uint64_t safe_block = ((bn - 1) / CHECKPOINT_INTERVAL) * CHECKPOINT_INTERVAL;
                bool pe = (safe_block >= 2675000);
                /* Read the expected root for the safe block from era1 */
                uint8_t safe_root[32] = {0};
                if (archive_ensure(&archive, safe_block, false, era1_dir)) {
                    uint8_t *sr_hdr = NULL; size_t sr_hlen = 0;
                    uint8_t *sr_body = NULL; size_t sr_blen = 0;
                    if (era1_read_block(&archive.current, safe_block,
                                         &sr_hdr, &sr_hlen, &sr_body, &sr_blen)) {
                        block_header_t sr_header;
                        if (block_header_decode_rlp(&sr_header, sr_hdr, sr_hlen))
                            memcpy(safe_root, sr_header.state_root.bytes, 32);
                        free(sr_hdr); free(sr_body);
                    }
                }
                char meta_path[512];
                snprintf(meta_path, sizeof(meta_path), "%s.meta", mpt_path);
                if (meta_write(meta_path, safe_block, safe_root, pe)) {
                    LOG_WARN("wrote %s — last good checkpoint: block %lu", meta_path, safe_block);
                    LOG_WARN("to debug: state_reconstruct --resume to block %lu, "
                             "then replay blocks %lu..%lu with per-block validation",
                             safe_block, safe_block + 1, bn);
                }
            }

            /* Auto-dump pre/post state for the failing block */
            {
                char auto_dir[512];
                snprintf(auto_dir, sizeof(auto_dir), "known_issues/block_%lu", bn);
                mkdir("known_issues", 0755);
                mkdir(auto_dir, 0755);
                LOG_INFO("auto-dumping pre/post state to %s/ ...", auto_dir);

                evm_state_t *es = sync_get_state(sync);

#ifdef ENABLE_HISTORY
                /* pre_alloc.json (original values) + post_alloc.json (current values)
                 * with debug flags on each account */
                evm_state_dump_debug(es, auto_dir);
#endif

                /* Write env.json */
                char env_p[512];
                snprintf(env_p, sizeof(env_p), "%s/env.json", auto_dir);
                FILE *ef = fopen(env_p, "w");
                if (ef) {
                    fprintf(ef, "{\n");
                    fprintf(ef, "  \"currentCoinbase\": \"0x");
                    for (int j = 0; j < 20; j++) fprintf(ef, "%02x", header.coinbase.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"currentDifficulty\": \"0x%lx\",\n",
                            uint256_to_uint64(&header.difficulty));
                    fprintf(ef, "  \"currentGasLimit\": \"0x%lx\",\n", header.gas_limit);
                    fprintf(ef, "  \"currentNumber\": \"0x%lx\",\n", header.number);
                    fprintf(ef, "  \"currentTimestamp\": \"0x%lx\",\n", header.timestamp);
                    fprintf(ef, "  \"parentHash\": \"0x");
                    for (int j = 0; j < 32; j++) fprintf(ef, "%02x", header.parent_hash.bytes[j]);
                    fprintf(ef, "\",\n");
                    fprintf(ef, "  \"blockHashes\": {\n");
                    bool fbh = true;
                    uint64_t sbh = bn > 256 ? bn - 256 : 1;
                    for (uint64_t bhn = sbh; bhn < bn; bhn++) {
                        hash_t bh;
                        if (sync_get_block_hash(sync, bhn, &bh)) {
                            if (!fbh) fprintf(ef, ",\n");
                            fbh = false;
                            fprintf(ef, "    \"%lu\": \"0x", bhn);
                            for (int j = 0; j < 32; j++) fprintf(ef, "%02x", bh.bytes[j]);
                            fprintf(ef, "\"");
                        }
                    }
                    fprintf(ef, "\n  }\n}\n");
                    fclose(ef);
                }

            }

            /* Discard dirty state so destroy doesn't flush failing block to MPT.
             * MPT on disk stays at last good checkpoint C. */
            evm_state_discard_pending(sync_get_state(sync));

            LOG_ERROR("First failure at block %lu - stopping.", bn);
            break;
        }
    }

    /* --no-validate: compute and verify root at the last processed block */
    if (no_validate) {
        sync_status_t fst = sync_get_status(sync);
        uint64_t last = fst.last_block;
        if (last > 0) {
            fprintf(stderr, "\nComputing final state root at block %lu...\n", last);
            evm_state_t *es = sync_get_state(sync);
            bool prune = (last >= 2675000);
            struct timespec _fr0, _fr1;
            clock_gettime(CLOCK_MONOTONIC, &_fr0);
            hash_t actual = evm_state_compute_mpt_root(es, prune);
            clock_gettime(CLOCK_MONOTONIC, &_fr1);
            double fr_ms = (_fr1.tv_sec - _fr0.tv_sec) * 1000.0 +
                           (_fr1.tv_nsec - _fr0.tv_nsec) / 1e6;

            /* Read expected root from era1 header */
            hash_t expected = {0};
            if (archive_ensure(&archive, last, false, era1_dir)) {
                uint8_t *h_rlp = NULL; size_t h_len = 0;
                uint8_t *b_rlp = NULL; size_t b_len = 0;
                if (era1_read_block(&archive.current, last,
                                     &h_rlp, &h_len, &b_rlp, &b_len)) {
                    block_header_t hdr;
                    if (block_header_decode_rlp(&hdr, h_rlp, h_len))
                        expected = hdr.state_root;
                    free(h_rlp); free(b_rlp);
                }
            }

            char got_hex[67], exp_hex[67];
            hash_to_hex(&actual, got_hex);
            hash_to_hex(&expected, exp_hex);

            if (memcmp(actual.bytes, expected.bytes, 32) == 0) {
                fprintf(stderr, "  PASS: root matches at block %lu (%.0fms)\n",
                        last, fr_ms);
            } else {
                fprintf(stderr, "  FAIL: root mismatch at block %lu (%.0fms)\n",
                        last, fr_ms);
                fprintf(stderr, "  got:      %s\n", got_hex);
                fprintf(stderr, "  expected: %s\n", exp_hex);
            }
        }
    }

    /* Block SIGINT during cleanup — flush must complete cleanly */
    sigset_t block_set, old_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGINT);
    sigprocmask(SIG_BLOCK, &block_set, &old_set);

    clock_gettime(CLOCK_MONOTONIC, &t_now);
    double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;

    sync_status_t st = sync_get_status(sync);

#ifdef ENABLE_TUI
    if (use_tui) {
        /* Show summary in log panel, then wait for 'q' */
        LOG_INFO("===== Summary =====");
        LOG_INFO("Blocks OK:     %lu", st.blocks_ok);
        LOG_INFO("Blocks failed: %lu", st.blocks_fail);
        LOG_INFO("Total gas:     %lu", st.total_gas);
        LOG_INFO("Elapsed:       %.1f s", elapsed);
        LOG_INFO("Speed:         %.0f blk/s",
                 (st.blocks_ok + st.blocks_fail) / (elapsed > 0 ? elapsed : 1));

        tui_set_finished();

        /* Block until user presses 'q' */
        while (tui_tick()) {
            usleep(50000);  /* 50ms — responsive without burning CPU */
        }

        tui_shutdown();
        freopen("/dev/tty", "w", stdout);
        freopen("/dev/tty", "w", stderr);
    } else
#endif
    {
        printf("\n===== Summary =====\n");
        printf("Blocks OK:     %lu\n", st.blocks_ok);
        printf("Blocks failed: %lu\n", st.blocks_fail);
        printf("Total gas:     %lu\n", st.total_gas);
        printf("Elapsed:       %.1f s\n", elapsed);
        printf("Speed:         %.0f blk/s\n",
               (st.blocks_ok + st.blocks_fail) / (elapsed > 0 ? elapsed : 1));
    }

    /* Stop prefetch and destroy sync (flushes + msyncs all data to disk) */
    prefetch_stop(&prefetch);
    sync_ensure_flushed(sync);
    sync_destroy(sync);

    /* Write .meta AFTER all data is on disk (sync_destroy msyncs mmap pages) */
    if (st.blocks_fail == 0 && st.last_block > 0) {
        bool pe = (st.last_block >= 2675000);
        uint8_t root[32] = {0};
        if (archive_ensure(&archive, st.last_block, false, era1_dir)) {
            uint8_t *h_rlp = NULL; size_t h_len = 0;
            uint8_t *b_rlp = NULL; size_t b_len = 0;
            if (era1_read_block(&archive.current, st.last_block,
                                 &h_rlp, &h_len, &b_rlp, &b_len)) {
                block_header_t hdr;
                if (block_header_decode_rlp(&hdr, h_rlp, h_len))
                    memcpy(root, hdr.state_root.bytes, 32);
                free(h_rlp); free(b_rlp);
            }
        }
        char mp[512];
        snprintf(mp, sizeof(mp), "%s.meta", mpt_path);
        meta_write(mp, st.last_block, root, pe);
    }
    archive_close(&archive);

    return st.blocks_fail > 0 ? 1 : 0;
}
