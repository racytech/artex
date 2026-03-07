/**
 * Chain Replay Tool
 *
 * Re-executes Ethereum blocks from Era1 archive files, building
 * verkle state from genesis. Verifies state roots match headers.
 *
 * Usage: ./chain_replay <era1_dir> <genesis_json> [start_block] [end_block]
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
#include <cjson/cJSON.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>

#define ERA1_BLOCKS_PER_FILE 8192
#define BLOCK_HASH_WINDOW    256

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
        }
        count++;
    }

    cJSON_Delete(root);
    printf("Genesis: loaded %zu accounts\n", count);
    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <era1_dir> <genesis.json> [start_block] [end_block]\n", argv[0]);
        return 1;
    }

    const char *era1_dir = argv[1];
    const char *genesis_path = argv[2];
    uint64_t start_block = argc > 3 ? (uint64_t)atoll(argv[3]) : 0;
    uint64_t end_block   = argc > 4 ? (uint64_t)atoll(argv[4]) : UINT64_MAX;

    /* Open era1 archive */
    era1_archive_t archive;
    if (!archive_open(&archive, era1_dir)) return 1;

    printf("Era1 archive: %zu files in %s\n", archive.count, era1_dir);

    /* Create verkle state (in-memory for now) */
    verkle_state_t *vs = verkle_state_create();
    if (!vs) {
        fprintf(stderr, "Failed to create verkle state\n");
        archive_close(&archive);
        return 1;
    }

    evm_state_t *state = evm_state_create(vs);
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

    /* Load genesis state */
    if (!load_genesis(state, genesis_path)) {
        fprintf(stderr, "Failed to load genesis state\n");
        evm_destroy(evm);
        evm_state_destroy(state);
        verkle_state_destroy(vs);
        archive_close(&archive);
        return 1;
    }

    /* Commit genesis state */
    evm_state_commit(state);

    /* Block hash ring buffer for BLOCKHASH opcode */
    hash_t block_hashes[BLOCK_HASH_WINDOW];
    memset(block_hashes, 0, sizeof(block_hashes));

    /* Progress tracking */
    struct timespec t_start, t_now;
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    uint64_t total_gas = 0;
    uint64_t blocks_ok = 0;
    uint64_t blocks_fail = 0;

    printf("Replaying blocks %lu to %lu...\n\n", start_block,
           end_block == UINT64_MAX ? (uint64_t)(archive.count * ERA1_BLOCKS_PER_FILE - 1) : end_block);

    /* Genesis block (block 0) has no transactions — just skip to block 1
     * and use the genesis state root from the block 1 parent hash */
    if (start_block == 0) {
        /* Read block 0 header to get its hash for the ring buffer */
        if (archive_ensure(&archive, 0)) {
            uint8_t *hdr_rlp, *body_rlp;
            size_t hdr_len, body_len;
            if (era1_read_block(&archive.current, 0,
                                &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
                block_hashes[0] = block_hash_from_rlp(hdr_rlp, hdr_len);
                free(hdr_rlp);
                free(body_rlp);
            }
        }
        start_block = 1;
    }

    for (uint64_t bn = start_block; bn <= end_block; bn++) {
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
        hash_t blk_hash = block_hash_from_rlp(hdr_rlp, hdr_len);
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

        /* Verify state root */
        bool root_match = hash_equal(&result.state_root, &header.state_root);

        if (!root_match) {
            char expected[67], got[67];
            hash_to_hex(&header.state_root, expected);
            hash_to_hex(&result.state_root, got);
            fprintf(stderr, "Block %lu: STATE ROOT MISMATCH\n"
                    "  expected: %s\n"
                    "  got:      %s\n"
                    "  gas_used: %lu (header: %lu)\n"
                    "  txs: %zu, success: %d\n",
                    bn, expected, got,
                    result.gas_used, header.gas_used,
                    result.tx_count, result.success);
            blocks_fail++;
        } else {
            blocks_ok++;
        }

        total_gas += result.gas_used;

        /* Progress every 1000 blocks */
        if (bn % 1000 == 0 || !root_match) {
            clock_gettime(CLOCK_MONOTONIC, &t_now);
            double elapsed = (t_now.tv_sec - t_start.tv_sec) +
                             (t_now.tv_nsec - t_start.tv_nsec) / 1e9;
            double bps = (bn - (argc > 2 ? (uint64_t)atoll(argv[2]) : 0)) / (elapsed > 0 ? elapsed : 1);
            printf("Block %lu | %lu txs | gas %lu | %.0f blk/s | ok %lu fail %lu\n",
                   bn, result.tx_count, result.gas_used, bps, blocks_ok, blocks_fail);
        }

        block_result_free(&result);
        block_body_free(&body);
        free(hdr_rlp);
        free(body_rlp);

        if (blocks_fail > 10) {
            fprintf(stderr, "\nToo many failures, stopping.\n");
            break;
        }
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
