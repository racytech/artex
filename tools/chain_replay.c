/**
 * Chain Replay Tool
 *
 * Re-executes Ethereum blocks from Era1 archive files, building
 * verkle state from genesis. Verifies gas_used matches headers.
 *
 * Supports checkpointing: state is saved every CHECKPOINT_INTERVAL blocks
 * and can resume from the last checkpoint instead of genesis.
 *
 * Usage: ./chain_replay <era1_dir> <genesis.json> [start_block] [end_block]
 *        ./chain_replay --clean <era1_dir> <genesis.json> [start_block] [end_block]
 */

#include "era1.h"
#include "block.h"
#include "block_executor.h"
#include "dao_fork.h"
#include "evm.h"
#include "evm_state.h"
#include "verkle_state.h"
#include "fork.h"
#include "hash.h"
#include "uint256.h"
#include "keccak256.h"
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

bool g_trace_calls = false;  // Debug: trace CALL gas

#define ERA1_BLOCKS_PER_FILE 8192
#define BLOCK_HASH_WINDOW    256
#define CHECKPOINT_INTERVAL  256

/* Persistent store paths */
static const char *VALUE_DIR  = "/tmp/chain_replay_values";
static const char *COMMIT_DIR = "/tmp/chain_replay_commits";
static const char *CKPT_PATH  = "/tmp/chain_replay.ckpt";
static const char *CKPT_TMP   = "/tmp/chain_replay.ckpt.tmp";
static const char *MPT_PATH   = "/tmp/chain_replay_mpt";

/* =========================================================================
 * Graceful shutdown via SIGINT
 * ========================================================================= */

static volatile sig_atomic_t g_shutdown = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* =========================================================================
 * Checkpoint format
 * ========================================================================= */

#define CKPT_MAGIC   0x54504B43  /* "CKPT" little-endian */
#define CKPT_VERSION 1

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t block_number;        /* last fully committed block */
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
    hash_t   block_hashes[BLOCK_HASH_WINDOW];  /* ring buffer */
    hash_t   checksum;            /* keccak256 of everything above */
} checkpoint_t;

static void checkpoint_compute_checksum(checkpoint_t *ckpt) {
    /* Checksum covers everything except the checksum field itself */
    size_t payload_size = offsetof(checkpoint_t, checksum);
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, (const uint8_t *)ckpt, (uint16_t)payload_size);
    keccak_final(&ctx, ckpt->checksum.bytes);
}

static bool checkpoint_verify(const checkpoint_t *ckpt) {
    if (ckpt->magic != CKPT_MAGIC || ckpt->version != CKPT_VERSION)
        return false;

    /* Recompute checksum */
    hash_t expected;
    size_t payload_size = offsetof(checkpoint_t, checksum);
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, (const uint8_t *)ckpt, (uint16_t)payload_size);
    keccak_final(&ctx, expected.bytes);

    return memcmp(expected.bytes, ckpt->checksum.bytes, 32) == 0;
}

/** Save checkpoint atomically (write tmp + rename). */
static bool checkpoint_save(const char *path, uint64_t block_number,
                            const hash_t block_hashes[BLOCK_HASH_WINDOW],
                            uint64_t total_gas, uint64_t blocks_ok,
                            uint64_t blocks_fail) {
    checkpoint_t ckpt;
    memset(&ckpt, 0, sizeof(ckpt));
    ckpt.magic        = CKPT_MAGIC;
    ckpt.version      = CKPT_VERSION;
    ckpt.block_number = block_number;
    ckpt.total_gas    = total_gas;
    ckpt.blocks_ok    = blocks_ok;
    ckpt.blocks_fail  = blocks_fail;
    memcpy(ckpt.block_hashes, block_hashes, sizeof(ckpt.block_hashes));
    checkpoint_compute_checksum(&ckpt);

    /* Write to temp file */
    FILE *f = fopen(CKPT_TMP, "wb");
    if (!f) {
        perror("checkpoint_save: fopen tmp");
        return false;
    }
    if (fwrite(&ckpt, sizeof(ckpt), 1, f) != 1) {
        perror("checkpoint_save: fwrite");
        fclose(f);
        unlink(CKPT_TMP);
        return false;
    }
    fflush(f);
    fsync(fileno(f));
    fclose(f);

    /* Atomic rename */
    if (rename(CKPT_TMP, path) != 0) {
        perror("checkpoint_save: rename");
        unlink(CKPT_TMP);
        return false;
    }
    return true;
}

/** Load and verify checkpoint. Returns false if missing or corrupt. */
static bool checkpoint_load(const char *path, checkpoint_t *out) {
    FILE *f = fopen(path, "rb");
    if (!f) return false;

    if (fread(out, sizeof(*out), 1, f) != 1) {
        fclose(f);
        return false;
    }
    fclose(f);

    if (!checkpoint_verify(out)) {
        fprintf(stderr, "Checkpoint corrupt — ignoring\n");
        return false;
    }
    return true;
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
 * Genesis state loading
 * ========================================================================= */

static bool load_genesis(evm_state_t *state, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        perror("load_genesis: fopen");
        return false;
    }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *json_str = malloc(fsize + 1);
    if (!json_str) { fclose(f); return false; }
    fread(json_str, 1, fsize, f);
    json_str[fsize] = '\0';
    fclose(f);

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        fprintf(stderr, "load_genesis: JSON parse error\n");
        return false;
    }

    size_t count = 0;
    cJSON *entry;
    cJSON_ArrayForEach(entry, root) {
        /* Key is address (hex), value has "balance" field */
        const char *addr_hex = entry->string;
        if (!addr_hex) continue;

        address_t addr;
        if (!address_from_hex(addr_hex, &addr)) continue;

        cJSON *bal_item = cJSON_GetObjectItem(entry, "balance");
        if (!bal_item || !cJSON_IsString(bal_item)) continue;

        uint256_t balance = uint256_from_hex(bal_item->valuestring);
        if (!uint256_is_zero(&balance)) {
            evm_state_add_balance(state, &addr, &balance);
        } else {
            /* Zero-balance accounts still exist in genesis state.
             * Touch the account so it appears in the cache. */
            evm_state_create_account(state, &addr);
        }
        count++;
    }

    cJSON_Delete(root);

    printf("Genesis: loaded %zu accounts\n", count);
    return true;
}

/* =========================================================================
 * Utility: check if directory exists
 * ========================================================================= */

static bool dir_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static bool file_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

/* =========================================================================
 * Reconstruct block hashes from era1 (for resume without full ring buffer)
 * ========================================================================= */

static void __attribute__((unused))
load_block_hashes(era1_archive_t *archive,
                  hash_t block_hashes[BLOCK_HASH_WINDOW],
                  uint64_t up_to_block) {
    /* Load up to BLOCK_HASH_WINDOW block hashes ending at up_to_block */
    uint64_t start = (up_to_block >= BLOCK_HASH_WINDOW)
                     ? up_to_block - BLOCK_HASH_WINDOW + 1 : 0;

    for (uint64_t bn = start; bn <= up_to_block; bn++) {
        if (!archive_ensure(archive, bn)) continue;

        uint8_t *hdr_rlp, *body_rlp;
        size_t hdr_len, body_len;
        if (era1_read_block(&archive->current, bn,
                            &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
            block_hashes[bn % BLOCK_HASH_WINDOW] =
                hash_keccak256(hdr_rlp, hdr_len);
            free(hdr_rlp);
            free(body_rlp);
        }
    }
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
            "Checkpoints every %d blocks to %s\n"
            "State stored in %s and %s\n",
            argv[0], CHECKPOINT_INTERVAL, CKPT_PATH, VALUE_DIR, COMMIT_DIR);
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

    /* =====================================================================
     * Decide: resume from checkpoint or fresh start
     * ===================================================================== */

    verkle_state_t *vs = NULL;
    evm_state_t *state = NULL;
    hash_t block_hashes[BLOCK_HASH_WINDOW];
    memset(block_hashes, 0, sizeof(block_hashes));

    uint64_t start_block = user_start;
    uint64_t total_gas   = 0;
    uint64_t blocks_ok   = 0;
    uint64_t blocks_fail = 0;
    bool resumed = false;

    /* Clean up old state if requested */
    if (force_clean) {
        printf("--clean: removing existing state and checkpoint\n");
        unlink(CKPT_PATH);
        /* Remove store directories (best-effort) */
        char cmd[512];
        snprintf(cmd, sizeof(cmd),
                 "rm -rf %s %s %s.idx %s.dat %s_storage.idx %s_storage.dat 2>/dev/null",
                 VALUE_DIR, COMMIT_DIR, MPT_PATH, MPT_PATH, MPT_PATH, MPT_PATH);
        (void)system(cmd);
    }

    /* Try to resume from checkpoint */
    checkpoint_t ckpt;
    if (!force_clean &&
        file_exists(CKPT_PATH) &&
        dir_exists(VALUE_DIR) &&
        dir_exists(COMMIT_DIR) &&
        checkpoint_load(CKPT_PATH, &ckpt))
    {
        printf("Checkpoint found: block %lu (ok=%lu fail=%lu gas=%lu)\n",
               ckpt.block_number, ckpt.blocks_ok, ckpt.blocks_fail, ckpt.total_gas);

        /* If user requested a start_block beyond our checkpoint, we can't skip */
        if (user_start > ckpt.block_number + 1) {
            fprintf(stderr,
                "Warning: requested start_block %lu > checkpoint %lu + 1\n"
                "Cannot skip ahead — use --clean to restart from genesis\n",
                user_start, ckpt.block_number);
            archive_close(&archive);
            return 1;
        }

        /* Open existing flat state */
        vs = verkle_state_open_flat(VALUE_DIR, COMMIT_DIR);
        if (!vs) {
            fprintf(stderr, "Failed to open existing verkle state — starting fresh\n");
        } else {
            /* Restore checkpoint data */
            memcpy(block_hashes, ckpt.block_hashes, sizeof(block_hashes));
            start_block = ckpt.block_number + 1;
            total_gas   = ckpt.total_gas;
            blocks_ok   = ckpt.blocks_ok;
            blocks_fail = ckpt.blocks_fail;
            resumed = true;
            printf("Resuming from block %lu\n", start_block);
        }
    }

    /* Fresh start if no checkpoint or resume failed */
    if (!vs) {
        vs = verkle_state_create_flat(VALUE_DIR, COMMIT_DIR);
        if (!vs) {
            fprintf(stderr, "Failed to create verkle state\n");
            archive_close(&archive);
            return 1;
        }
    }

    state = evm_state_create(vs, MPT_PATH);
    if (!state) {
        fprintf(stderr, "Failed to create EVM state\n");
        verkle_state_destroy(vs);
        archive_close(&archive);
        return 1;
    }

    evm_t *evm = evm_create(state, chain_config_mainnet());
    if (!evm) {
        fprintf(stderr, "Failed to create EVM\n");
        evm_state_destroy(state);
        verkle_state_destroy(vs);
        archive_close(&archive);
        return 1;
    }

    /* If fresh start, load genesis */
    if (!resumed) {
        /* Open block 0 for genesis state writes (required for flat backend) */
        evm_state_begin_block(state, 0);

        if (!load_genesis(state, genesis_path)) {
            fprintf(stderr, "Failed to load genesis state\n");
            evm_destroy(evm);
            evm_state_destroy(state);
            verkle_state_destroy(vs);
            archive_close(&archive);
            return 1;
        }

        /* Commit genesis as original state (EIP-2200) + flush to flat backend */
        evm_state_commit(state);
        evm_state_finalize(state);

        /* Flush block_dirty accounts to verkle and commit block 0 in flat backend.
         * Without this, begin_block(0) leaves block_active=true and subsequent
         * begin_block(1) silently fails, causing incorrect block tracking. */
        evm_state_compute_state_root_ex(state, false);

        /* Genesis block (block 0) has no transactions — read its hash */
        if (archive_ensure(&archive, 0)) {
            uint8_t *hdr_rlp, *body_rlp;
            size_t hdr_len, body_len;
            if (era1_read_block(&archive.current, 0,
                                &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
                block_hashes[0] = hash_keccak256(hdr_rlp, hdr_len);
                free(hdr_rlp);
                free(body_rlp);
            }
        }

        /* Save genesis checkpoint */
        verkle_state_sync(vs);
        checkpoint_save(CKPT_PATH, 0, block_hashes, 0, 0, 0);
        printf("Genesis checkpoint saved\n");

        if (start_block == 0)
            start_block = 1;
    }

    /* Progress tracking */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    printf("Replaying blocks %lu to %lu...\n\n", start_block,
           end_block == UINT64_MAX
               ? (uint64_t)(archive.count * ERA1_BLOCKS_PER_FILE - 1)
               : end_block);

    /* Track last checkpoint and last executed block */
    uint64_t last_checkpoint_block = resumed ? ckpt.block_number : 0;
    uint64_t last_executed_block   = start_block > 1 ? start_block - 1 : 0;

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
        /* Graceful shutdown */
        if (g_shutdown) {
            printf("\nSIGINT received — saving checkpoint at block %lu\n", bn - 1);
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
            blocks_fail++;
            continue;
        }

        /* Compute block hash and store in ring buffer */
        hash_t blk_hash = hash_keccak256(hdr_rlp, hdr_len);
        block_hashes[bn % BLOCK_HASH_WINDOW] = blk_hash;

        /* Decode header and body */
        block_header_t header;
        if (!block_header_decode_rlp(&header, hdr_rlp, hdr_len)) {
            fprintf(stderr, "Block %lu: failed to decode header\n", bn);
            free(hdr_rlp);
            free(body_rlp);
            blocks_fail++;
            continue;
        }

        block_body_t body;
        if (!block_body_decode_rlp(&body, body_rlp, body_len)) {
            fprintf(stderr, "Block %lu: failed to decode body\n", bn);
            free(hdr_rlp);
            free(body_rlp);
            blocks_fail++;
            continue;
        }

        /* Execute */
        block_result_t result = block_execute(evm, &header, &body, block_hashes);
        last_executed_block = bn;

        /* Verify gas_used matches header */
        bool gas_match = (result.gas_used == header.gas_used);

        /* Compute MPT root for pre-Verkle blocks.
         * Full trie rebuild is O(n) over all cached accounts, so only validate:
         *   - blocks with transactions (state changes beyond coinbase)
         *   - every CHECKPOINT_INTERVAL blocks (periodic sanity check)
         *   - the very last block in the range
         */
        bool root_match = true;
        bool check_root = (bn % CHECKPOINT_INTERVAL == 0 ||
                           result.tx_count > 0 ||
                           block_body_uncle_count(&body) > 0 ||
                           bn == end_block);
        hash_t mpt_root = {0};
        if (check_root) {
            bool prune = (evm->fork >= FORK_SPURIOUS_DRAGON);
            mpt_root = evm_state_compute_mpt_root(state, prune);
            root_match = (memcmp(mpt_root.bytes, header.state_root.bytes, 32) == 0);
        }

        if (!gas_match) {
            fprintf(stderr, "Block %lu: GAS MISMATCH  got %lu  expected %lu  diff %+ld  (txs: %zu)\n",
                    bn, result.gas_used, header.gas_used,
                    (long)result.gas_used - (long)header.gas_used,
                    result.tx_count);
            for (size_t ti = 0; ti < result.tx_count && ti < result.receipt_count; ti++) {
                fprintf(stderr, "  tx[%zu]: gas_used=%lu cumulative=%lu\n",
                        ti, result.receipts[ti].gas_used,
                        result.receipts[ti].cumulative_gas);
            }
            blocks_fail++;
        } else if (!root_match) {
            char got_hex[67], exp_hex[67];
            hash_to_hex(&mpt_root, got_hex);
            hash_to_hex(&header.state_root, exp_hex);
            fprintf(stderr, "Block %lu: STATE ROOT MISMATCH  (txs: %zu)\n"
                    "  got:      %s\n  expected: %s\n",
                    bn, result.tx_count, got_hex, exp_hex);
            blocks_fail++;
        } else {
            blocks_ok++;
        }
        bool block_ok = gas_match && root_match;

        total_gas += result.gas_used;

        /* Progress every 256 blocks */
        if (bn % CHECKPOINT_INTERVAL == 0 || !block_ok || bn == end_block) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double bps = (bn - start_block + 1) / (elapsed > 0 ? elapsed : 1);
            size_t uc = block_body_uncle_count(&body);
            printf("Block %lu | %lu txs | %zu uncles | gas %lu | %.0f blk/s | ok %lu fail %lu\n",
                   bn, result.tx_count, uc, result.gas_used, bps, blocks_ok, blocks_fail);
        }

        /* Checkpoint every CHECKPOINT_INTERVAL blocks (only if clean) */
        if (blocks_fail == 0 &&
            bn - last_checkpoint_block >= CHECKPOINT_INTERVAL) {
            verkle_state_sync(vs);
            checkpoint_save(CKPT_PATH, bn, block_hashes,
                            total_gas, blocks_ok, blocks_fail);
            last_checkpoint_block = bn;
        }

        block_result_free(&result);
        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (!block_ok) {
            fprintf(stderr, "\nFirst failure at block %lu — stopping.\n", bn);

            /* Revert the failing block's state from the verkle store so
             * the persistent state matches the last good block. Without
             * this, the store has block N's state but the checkpoint says
             * an earlier block, causing double-application on resume. */
            verkle_state_revert_block(vs);

            /* Save checkpoint at the last good block (bn - 1) */
            uint64_t last_good = bn - 1;
            if (last_good > last_checkpoint_block) {
                /* Exclude the failing block's gas from the checkpoint */
                uint64_t good_gas = total_gas - result.gas_used;
                verkle_state_sync(vs);
                checkpoint_save(CKPT_PATH, last_good, block_hashes,
                                good_gas, blocks_ok, 0);
                last_checkpoint_block = last_good;
                printf("Checkpoint saved at block %lu (reverted failing block)\n",
                       last_good);
            }
            break;
        }
    }

    /* Final checkpoint — save at last executed block */
    if (blocks_fail == 0 && last_executed_block > last_checkpoint_block) {
        verkle_state_sync(vs);
        checkpoint_save(CKPT_PATH, last_executed_block, block_hashes,
                        total_gas, blocks_ok, blocks_fail);
        printf("\nCheckpoint saved at block %lu\n", last_executed_block);
    }

    /* Summary */
    clock_gettime(CLOCK_MONOTONIC, &t_now);
    double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                     (t_now.tv_nsec - t_start.tv_nsec) / 1e9;

    printf("\n===== Summary =====\n");
    printf("Blocks OK:     %lu\n", blocks_ok);
    printf("Blocks failed: %lu\n", blocks_fail);
    printf("Total gas:     %lu\n", total_gas);
    printf("Elapsed:       %.1f s\n", elapsed);
    printf("Speed:         %.0f blk/s\n",
           (blocks_ok + blocks_fail) / (elapsed > 0 ? elapsed : 1));

    evm_destroy(evm);
    evm_state_destroy(state);
    verkle_state_destroy(vs);
    archive_close(&archive);

    return blocks_fail > 0 ? 1 : 0;
}
