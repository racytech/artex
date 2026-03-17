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
#include "evm_tracer.h"

#ifdef ENABLE_DEBUG
bool g_trace_calls = false;
#endif

#define ERA1_BLOCKS_PER_FILE 8192
#define PARIS_BLOCK          15537394  /* last PoW block (The Merge) */

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
#define DEFAULT_DATA_DIR "/home/racytech/workspace/art/data"

static char value_dir[512];
static char commit_dir[512];
static char ckpt_path[512];
static char mpt_path[512];
static char code_path[512];
static char history_path[512];
static char flat_state_path[512];

static void set_data_paths(const char *data_dir) {
    snprintf(value_dir, sizeof(value_dir), "%s/chain_replay_values", data_dir);
    snprintf(commit_dir, sizeof(commit_dir), "%s/chain_replay_commits", data_dir);
    snprintf(ckpt_path, sizeof(ckpt_path), "%s/chain_replay.ckpt", data_dir);
    snprintf(mpt_path, sizeof(mpt_path), "%s/chain_replay_mpt", data_dir);
    snprintf(code_path, sizeof(code_path), "%s/chain_replay_code", data_dir);
    snprintf(history_path, sizeof(history_path), "%s/chain_replay_history", data_dir);
    snprintf(flat_state_path, sizeof(flat_state_path), "%s/chain_replay_flat", data_dir);
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
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    /* Parse flags */
    bool force_clean = false;
    bool follow_mode = false;
#ifdef ENABLE_DEBUG
    bool no_evict = false;
#endif
    uint64_t trace_block = UINT64_MAX;  /* UINT64_MAX = no tracing */
    uint64_t dump_prestate_block = UINT64_MAX;
    const char *dump_prestate_path = NULL;
#ifdef ENABLE_HISTORY
    bool no_history = false;
#endif
    const char *data_dir = DEFAULT_DATA_DIR;
    int arg_offset = 0;
    while (arg_offset + 1 < argc && argv[1 + arg_offset][0] == '-') {
        if (strcmp(argv[1 + arg_offset], "--clean") == 0) {
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
        } else if (strcmp(argv[1 + arg_offset], "--trace-block") == 0 && arg_offset + 2 < argc) {
            trace_block = (uint64_t)atoll(argv[2 + arg_offset]);
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
            "  --clean               Delete existing checkpoint and state, start from genesis\n"
            "  --follow              Wait for new era1 files instead of stopping\n"
            "  --data-dir DIR        Set data directory for all state files (default: data/)\n"
            "  --no-history          Disable per-block state diff history\n"
            "  --trace-block N       Enable EIP-3155 EVM trace for block N (to stderr)\n"
            "  --dump-prestate N [P] Dump pre-state alloc.json for block N to path P\n"
            "                        (default: alloc_<N>.json). Two-pass: executes block\n"
            "                        to discover accessed accounts, then reloads checkpoint\n"
            "                        and dumps their pre-execution values.\n"
            "\n"
            "Checkpoints every %d blocks to %s\n",
            argv[0], CHECKPOINT_INTERVAL, ckpt_path);
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
        unlink(ckpt_path);
        char cmd[512];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s %s %s.idx %s.dat %s_storage.idx %s_storage.dat %s.idx %s.dat"
                 " %s_flat_acct.idx %s_flat_stor.idx 2>/dev/null",
                 value_dir, commit_dir, mpt_path, mpt_path, mpt_path, mpt_path,
                 code_path, code_path, flat_state_path, flat_state_path);
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
        .checkpoint_path     = ckpt_path,
        .checkpoint_interval = CHECKPOINT_INTERVAL,
        .validate_state_root = true,
#ifdef ENABLE_DEBUG
        .no_evict            = no_evict,
#endif
    };
#ifdef ENABLE_VERKLE
    cfg.verkle_value_dir  = value_dir;
    cfg.verkle_commit_dir = commit_dir;
#endif
#ifdef ENABLE_HISTORY
    if (!no_history)
        cfg.history_dir = history_path;
#endif
#ifdef ENABLE_MPT
    cfg.mpt_path = mpt_path;
    cfg.code_store_path = code_path;
    cfg.flat_state_path = NULL;  /* flat_state disabled — stale files cause corruption */
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
    uint64_t window_gas = 0;
    uint64_t window_transfers = 0;
    uint64_t window_calls = 0;
    struct timespec t_window;
    clock_gettime(CLOCK_MONOTONIC, &t_window);

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        /* Graceful shutdown: finish current batch, checkpoint at next boundary */
        if (g_shutdown && !g_shutdown_pending) {
            g_shutdown_pending = true;
            printf("\nSIGINT received — finishing batch to next checkpoint...\n");
            printf("  (press Ctrl+C again to stop immediately)\n");
        }
        /* Second Ctrl+C — stop immediately, sync_destroy handles cleanup */
        if (g_shutdown >= 2) {
            printf("\nForced stop — cleaning up...\n");
            break;
        }

        if (!archive_ensure(&archive, bn, follow_mode, era1_dir))
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
            fprintf(stderr, "Block %lu: fatal execution error\n"
                    "  hint: this means sync_execute_block returned false (internal error, not validation)\n"
                    "  hint: check for OOM, disk I/O errors, or corrupt era1 data\n", bn);
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

            /* Pass 2: Destroy sync, recreate from checkpoint (clean pre-state) */
            sync_destroy(sync);
            sync = sync_create(&cfg);
            if (!sync) {
                fprintf(stderr, "Failed to recreate sync for prestate dump\n");
                free(addrs); free(addr_slots); free(slot_buf);
                archive_close(&archive);
                return 1;
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
            uint64_t remaining = (bn < PARIS_BLOCK) ? PARIS_BLOCK - bn : 0;
            printf("Block %lu | %lu txs (%luT %luC) | %.0f tps | %.1f Mgas/s | %.0f blk/s | %.1fs/256blk | %luK to Paris\n",
                   bn, window_txs, window_transfers, window_calls,
                   tps, mgps, bps, win_secs,
                   remaining / 1000);

            /* Cache/store stats every checkpoint */
            {
                evm_state_stats_t ss = sync_get_state_stats(sync);
                size_t rss_mb = get_rss_kb() / 1024;
                printf("  └ cache: %zuK accts, %zuK slots (%zuMB arena)\n",
                       ss.cache_accounts / 1000, ss.cache_slots / 1000,
                       ss.cache_arena_bytes / (1024*1024));
#ifdef ENABLE_MPT
                printf("  └ mpt: acct %luK nodes, stor %luK nodes\n",
                       ss.acct_mpt_nodes / 1000,
                       ss.stor_mpt_nodes / 1000);
                printf("  └ code: %luK (hit %.1f%%, LRU %u/%uK) disk: %.1fGB/%.1fGB RSS %zuMB\n",
                       ss.code_count / 1000,
                       (ss.code_cache_hits + ss.code_cache_misses)
                           ? 100.0 * ss.code_cache_hits / (ss.code_cache_hits + ss.code_cache_misses) : 0,
                       ss.code_cache_count / 1000, ss.code_cache_capacity / 1000,
                       ss.acct_mpt_data_bytes / 1e9, ss.stor_mpt_data_bytes / 1e9,
                       rss_mb);

                /* Root computation timing */
                double root_total = ss.root_stor_ms + ss.root_acct_ms;
                if (root_total > 0.1) {
                    printf("  └ root: stor=%.1f ms  acct=%.1f ms  total=%.1f ms (%zu dirty)\n",
                           ss.root_stor_ms, ss.root_acct_ms, root_total,
                           ss.root_dirty_count);
                    /* Storage trie commit breakdown */
                    mpt_commit_stats_t sc = ss.stor_commit;
                    if (sc.commits > 0)
                        printf("    └ stor: keccak=%.1f  load=%.1f  check=%.1f  del=%.1f  enc=%.1f  sort=%.1f ms"
                               "  nodes=%u  loaded=%u (cache=%u disk=%u)  chk_hit=%u  del=%u  commits=%u\n",
                               sc.keccak_ns / 1e6, sc.load_ns / 1e6,
                               sc.check_ns / 1e6, sc.delete_ns / 1e6,
                               sc.encode_ns / 1e6, sc.sort_ns / 1e6,
                               sc.nodes_hashed, sc.nodes_loaded,
                               sc.load_cache_hits, sc.load_disk_reads,
                               sc.check_hits, sc.deletes, sc.commits);
                    /* Account trie commit breakdown */
                    mpt_commit_stats_t ac = ss.acct_commit;
                    if (ac.commits > 0)
                        printf("    └ acct: keccak=%.1f  load=%.1f  check=%.1f  del=%.1f  enc=%.1f  sort=%.1f ms"
                               "  nodes=%u  loaded=%u (cache=%u disk=%u)  chk_hit=%u  del=%u\n",
                               ac.keccak_ns / 1e6, ac.load_ns / 1e6,
                               ac.check_ns / 1e6, ac.delete_ns / 1e6,
                               ac.encode_ns / 1e6, ac.sort_ns / 1e6,
                               ac.nodes_hashed, ac.nodes_loaded,
                               ac.load_cache_hits, ac.load_disk_reads,
                               ac.check_hits, ac.deletes);
                }

                /* Background flush timing (from previous checkpoint, joined at this one) */
                sync_checkpoint_stats_t cs = sync_get_checkpoint_stats(sync);
                if (cs.valid) {
                    printf("  └ flush: acct=%.1f ms  stor=%.1f ms  total=%.1f ms",
                           cs.flush.acct_ms, cs.flush.stor_ms, cs.flush_total_ms);
                    if (cs.flush_join_ms > 1.0)
                        printf("  join=%.1f ms", cs.flush_join_ms);
                    printf("\n");
                }

                /* Checkpoint total */
                if (cs.root_total_ms > 0.1)
                    printf("  └ checkpoint: total=%.1f ms\n", cs.root_total_ms);
#else
                printf("  └ RSS %zuMB\n", rss_mb);
#endif
            }
            window_txs = 0;
            window_gas = 0;
            window_transfers = 0;
            window_calls = 0;
            clock_gettime(CLOCK_MONOTONIC, &t_window);
        }

        /* SIGINT: exit after validated checkpoint */
        if (g_shutdown_pending && result.ok &&
            bn % CHECKPOINT_INTERVAL == 0) {
            printf("Checkpoint validated at block %lu — exiting.\n", bn);
            block_body_free(&body);
            free(hdr_rlp);
            free(body_rlp);
            break;
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
                fprintf(stderr, "  hint: gas diff usually means opcode cost bug or missing gas rule\n");

                /* Log our per-tx gas for comparison against era1 expected */
                if (result.receipts && result.receipt_count > 0) {
                    fprintf(stderr, "  per-tx gas (%zu txs, divergent only):\n", result.receipt_count);
                    for (size_t ti = 0; ti < result.receipt_count; ti++) {
                        fprintf(stderr, "    tx %3zu: gas=%7lu  cum=%lu  status=%u\n",
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

                /* Auto-dump prestate for the failing block */
                {
                    char auto_dir[512];
                    snprintf(auto_dir, sizeof(auto_dir), "known_issues/block_%lu", bn);
                    mkdir("known_issues", 0755);
                    mkdir(auto_dir, 0755);
                    fprintf(stderr, "  auto-dumping prestate to %s/ ...\n", auto_dir);

                    evm_state_t *es = sync_get_state(sync);

                    /* Collect addresses and storage keys from dirty cache */
                    address_t *d_addrs = malloc(MAX_ADDRS * sizeof(address_t));
                    size_t d_n_addrs = evm_state_collect_addresses(es, d_addrs, MAX_ADDRS);

                    typedef struct { address_t addr; uint256_t *keys; size_t count; } dslot_t;
                    dslot_t *d_aslots = malloc(d_n_addrs * sizeof(dslot_t));
                    uint256_t *d_sbuf = malloc(MAX_SLOTS * sizeof(uint256_t));
                    size_t d_total_slots = 0;
                    for (size_t di = 0; di < d_n_addrs; di++) {
                        d_aslots[di].addr = d_addrs[di];
                        size_t n = evm_state_collect_storage_keys(es, &d_addrs[di],
                            d_sbuf + d_total_slots, MAX_SLOTS - d_total_slots);
                        d_aslots[di].keys = d_sbuf + d_total_slots;
                        d_aslots[di].count = n;
                        d_total_slots += n;
                    }

                    /* Recreate sync from checkpoint for clean pre-state */
                    sync_destroy(sync);
                    sync = sync_create(&cfg);
                    if (!sync) {
                        fprintf(stderr, "  failed to recreate sync for prestate dump\n");
                        free(d_addrs); free(d_aslots); free(d_sbuf);
                        archive_close(&archive);
                        return 1;
                    }
                    es = sync_get_state(sync);

                    /* Write alloc.json */
                    char alloc_p[512];
                    snprintf(alloc_p, sizeof(alloc_p), "%s/alloc.json", auto_dir);
                    FILE *af = fopen(alloc_p, "w");
                    if (af) {
                        fprintf(af, "{\n");
                        for (size_t di = 0; di < d_n_addrs; di++) {
                            address_t *a = &d_addrs[di];
                            evm_state_exists(es, a);
                            uint64_t nn = evm_state_get_nonce(es, a);
                            uint256_t bal = evm_state_get_balance(es, a);
                            if (di > 0) fprintf(af, ",\n");
                            fprintf(af, "  \"0x");
                            for (int j = 0; j < 20; j++) fprintf(af, "%02x", a->bytes[j]);
                            fprintf(af, "\": {\n");
                            uint8_t bb[32]; uint256_to_bytes(&bal, bb);
                            fprintf(af, "    \"balance\": \"0x");
                            int s = 0; while (s < 31 && bb[s] == 0) s++;
                            for (int j = s; j < 32; j++) fprintf(af, "%02x", bb[j]);
                            fprintf(af, "\",\n");
                            fprintf(af, "    \"nonce\": \"0x%lx\"", nn);
                            uint32_t csz = evm_state_get_code_size(es, a);
                            if (csz > 0) {
                                const uint8_t *code = evm_state_get_code_ptr(es, a, &csz);
                                if (code && csz > 0) {
                                    fprintf(af, ",\n    \"code\": \"0x");
                                    for (uint32_t c = 0; c < csz; c++) fprintf(af, "%02x", code[c]);
                                    fprintf(af, "\"");
                                }
                            }
                            if (d_aslots[di].count > 0) {
                                fprintf(af, ",\n    \"storage\": {");
                                bool fs = true;
                                for (size_t si = 0; si < d_aslots[di].count; si++) {
                                    uint256_t val = evm_state_get_storage(es, a, &d_aslots[di].keys[si]);
                                    if (uint256_is_zero(&val)) continue;
                                    if (!fs) fprintf(af, ",");
                                    fs = false;
                                    uint8_t kb[32], vb[32];
                                    uint256_to_bytes(&d_aslots[di].keys[si], kb);
                                    uint256_to_bytes(&val, vb);
                                    fprintf(af, "\n      \"0x");
                                    for (int j = 0; j < 32; j++) fprintf(af, "%02x", kb[j]);
                                    fprintf(af, "\": \"0x");
                                    for (int j = 0; j < 32; j++) fprintf(af, "%02x", vb[j]);
                                    fprintf(af, "\"");
                                }
                                fprintf(af, "\n    }");
                            }
                            fprintf(af, "\n  }");
                        }
                        fprintf(af, "\n}\n");
                        fclose(af);
                        fprintf(stderr, "  dumped %zu accounts, %zu slots to %s\n",
                                d_n_addrs, d_total_slots, alloc_p);
                    }

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

                    free(d_addrs); free(d_aslots); free(d_sbuf);
                    /* sync was recreated — don't use old state after this */
                }
            } else if (result.error == SYNC_ROOT_MISMATCH) {
                char got_hex[67], exp_hex[67];
                hash_to_hex(&result.actual_root, got_hex);
                hash_to_hex(&result.expected_root, exp_hex);
                fprintf(stderr, "Block %lu: STATE ROOT MISMATCH (at batch checkpoint)\n"
                        "  got:      %s\n  expected: %s\n",
                        bn, got_hex, exp_hex);
                fprintf(stderr, "  hint: root is validated every %u blocks — bug is in range [%lu..%lu]\n",
                        CHECKPOINT_INTERVAL,
                        bn > CHECKPOINT_INTERVAL ? bn - CHECKPOINT_INTERVAL + 1 : 1, bn);
                fprintf(stderr, "  hint: if resumed from checkpoint, try deleting state files and replaying fresh\n"
                        "  hint: run with --trace-block N to trace a specific block\n"
                        "  hint: use evm_statetest for per-block differential fuzzing against geth\n");
#ifdef ENABLE_DEBUG
                evm_state_debug_dump(sync_get_state(sync));
#endif
            }
            fprintf(stderr, "\nFirst failure at block %lu — stopping.\n", bn);
            break;
        }
    }

    /* Block SIGINT during cleanup — flush must complete cleanly */
    sigset_t block_set, old_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGINT);
    sigprocmask(SIG_BLOCK, &block_set, &old_set);

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
