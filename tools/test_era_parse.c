/**
 * test_era_parse — Verify era file block header parsing across all post-merge forks.
 *
 * Jumps to each fork boundary, parses a few blocks, and verifies fork-specific
 * header fields. Optionally compares against a reference RPC endpoint.
 *
 * Usage: test_era_parse <era_directory> [rpc_url]
 *
 * Checks per fork:
 *   Bellatrix (Paris):  base_fee, difficulty=0, no withdrawals, no blob gas
 *   Capella (Shanghai): has_withdrawals_root
 *   Deneb (Cancun):     has_blob_gas, has_parent_beacon_root, parent_beacon_root != 0
 *
 * If rpc_url is provided, fetches the same block from RPC and compares
 * number, timestamp, gas_limit, gas_used, base_fee, parent_hash, coinbase,
 * and fork-specific fields.
 */

#include "era.h"
#include "block.h"
#include "hash.h"
#include "uint256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void to_hex(const uint8_t *data, size_t len, char *out) {
    for (size_t i = 0; i < len; i++)
        sprintf(out + i * 2, "%02x", data[i]);
    out[len * 2] = '\0';
}

static bool is_zero(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++)
        if (data[i] != 0) return false;
    return true;
}

/* =========================================================================
 * Minimal RPC client (optional, via curl pipe)
 * ========================================================================= */

typedef struct {
    uint64_t number, timestamp, gas_limit, gas_used, nonce;
    uint8_t  parent_hash[32], coinbase[20], state_root[32], mix_hash[32];
    uint8_t  receipts_root[32];
    uint256_t base_fee, difficulty;
    bool     has_withdrawals_root;
    uint8_t  withdrawals_root[32];
    bool     has_blob_gas;
    uint64_t blob_gas_used, excess_blob_gas;
    bool     has_parent_beacon_root;
    uint8_t  parent_beacon_root[32];
} rpc_header_t;

static bool parse_hex_field(const char *json, const char *key, uint8_t *out, size_t len) {
    char search[128];
    snprintf(search, sizeof(search), "\"%s\":\"0x", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    for (size_t i = 0; i < len; i++) {
        unsigned int b;
        if (sscanf(p + i * 2, "%02x", &b) != 1) return false;
        out[i] = (uint8_t)b;
    }
    return true;
}

static bool parse_hex_uint64(const char *json, const char *key, uint64_t *out) {
    char search[128];
    snprintf(search, sizeof(search), "\"%s\":\"0x", key);
    const char *p = strstr(json, search);
    if (!p) return false;
    p += strlen(search);
    *out = strtoull(p, NULL, 16);
    return true;
}

static bool fetch_rpc_header(const char *rpc_url, uint64_t block_num, rpc_header_t *out) {
    memset(out, 0, sizeof(*out));

    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
        "curl -s '%s' -X POST -H 'Content-Type: application/json' "
        "-d '{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\","
        "\"params\":[\"0x%lx\",false],\"id\":1}'",
        rpc_url, block_num);

    FILE *fp = popen(cmd, "r");
    if (!fp) return false;

    char *buf = malloc(64 * 1024);
    size_t n = fread(buf, 1, 64 * 1024 - 1, fp);
    pclose(fp);
    buf[n] = '\0';

    if (strstr(buf, "\"error\"") || !strstr(buf, "\"result\"")) {
        free(buf);
        return false;
    }

    parse_hex_uint64(buf, "number", &out->number);
    parse_hex_uint64(buf, "timestamp", &out->timestamp);
    parse_hex_uint64(buf, "gasLimit", &out->gas_limit);
    parse_hex_uint64(buf, "gasUsed", &out->gas_used);
    parse_hex_field(buf, "parentHash", out->parent_hash, 32);
    parse_hex_field(buf, "miner", out->coinbase, 20);
    parse_hex_field(buf, "stateRoot", out->state_root, 32);
    parse_hex_field(buf, "mixHash", out->mix_hash, 32);

    /* baseFeePerGas */
    char search[] = "\"baseFeePerGas\":\"0x";
    const char *p = strstr(buf, search);
    if (p) {
        p += strlen(search);
        out->base_fee = uint256_from_uint64(strtoull(p, NULL, 16));
    }

    /* withdrawalsRoot */
    if (parse_hex_field(buf, "withdrawalsRoot", out->withdrawals_root, 32))
        out->has_withdrawals_root = true;

    /* blobGasUsed / excessBlobGas */
    if (parse_hex_uint64(buf, "blobGasUsed", &out->blob_gas_used)) {
        out->has_blob_gas = true;
        parse_hex_uint64(buf, "excessBlobGas", &out->excess_blob_gas);
    }

    /* parentBeaconBlockRoot */
    if (parse_hex_field(buf, "parentBeaconBlockRoot", out->parent_beacon_root, 32))
        out->has_parent_beacon_root = true;

    free(buf);
    return out->number == block_num;
}

/* =========================================================================
 * Fork boundary blocks (mainnet)
 * ========================================================================= */

typedef struct {
    const char *name;
    uint64_t    first_block;  /* first block of this fork */
    uint64_t    check_count;  /* how many blocks to verify */
} fork_check_t;

static const fork_check_t FORK_CHECKS[] = {
    { "Bellatrix", 15537394, 5 },  /* Paris/Merge */
    { "Capella",   17034870, 5 },  /* Shanghai */
    { "Deneb",     19426587, 5 },  /* Cancun */
    { "Pectra",    22431000, 5 },  /* Prague/Electra — EIP-7702, EIP-2935, EIP-7685 */
    { "Fusaka",    23946606, 5 },  /* Fulu/Osaka — PeerDAS, EOF, EIP-7823 */
};
#define NUM_FORKS (sizeof(FORK_CHECKS) / sizeof(FORK_CHECKS[0]))

/* =========================================================================
 * Era file collection + skip-to-block
 * ========================================================================= */

typedef struct {
    char   **paths;
    size_t   count;
} era_files_t;

static int cmp_str(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

static bool collect_era_files(const char *dir, era_files_t *out) {
    DIR *d = opendir(dir);
    if (!d) return false;

    out->paths = NULL;
    out->count = 0;

    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        size_t nlen = strlen(ent->d_name);
        if (nlen < 4 || strcmp(ent->d_name + nlen - 4, ".era") != 0) continue;
        if (nlen >= 5 && strcmp(ent->d_name + nlen - 5, ".era1") == 0) continue;
        out->paths = realloc(out->paths, (out->count + 1) * sizeof(char *));
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        out->paths[out->count++] = strdup(path);
    }
    closedir(d);

    if (out->count > 1)
        qsort(out->paths, out->count, sizeof(char *), cmp_str);
    return out->count > 0;
}

static void free_era_files(era_files_t *ef) {
    for (size_t i = 0; i < ef->count; i++) free(ef->paths[i]);
    free(ef->paths);
}

/**
 * Find era file containing target_block and iterate to it.
 * Returns true if found, with era opened and iter positioned at the block.
 */
static bool find_block(era_files_t *ef, uint64_t target_block,
                       era_t *era, era_iter_t *it,
                       block_header_t *hdr, block_body_t *body,
                       uint8_t block_hash[32]) {
    /* Binary search: probe first block of each era file */
    size_t lo = 0, hi = ef->count;

    /* Find the right era file — try opening files and checking first block */
    size_t best = 0;
    for (size_t i = 0; i < ef->count; i++) {
        era_t probe;
        if (!era_open(&probe, ef->paths[i])) continue;
        era_iter_t pit = era_iter(&probe);
        block_header_t ph;
        block_body_t pb;
        uint8_t bh[32];
        uint64_t slot;
        if (era_iter_next(&pit, &ph, &pb, bh, &slot)) {
            block_body_free(&pb);
            if (ph.number <= target_block) {
                best = i;
            } else {
                era_close(&probe);
                break;
            }
        }
        era_close(&probe);
    }

    /* Open the best file and scan to target */
    if (!era_open(era, ef->paths[best])) return false;
    *it = era_iter(era);

    uint64_t slot;
    while (era_iter_next(it, hdr, body, block_hash, &slot)) {
        if (hdr->number == target_block) return true;
        if (hdr->number > target_block) break;
        block_body_free(body);
    }

    /* Try next file if needed */
    era_close(era);
    for (size_t i = best + 1; i < ef->count; i++) {
        if (!era_open(era, ef->paths[i])) continue;
        *it = era_iter(era);
        while (era_iter_next(it, hdr, body, block_hash, &slot)) {
            if (hdr->number == target_block) return true;
            if (hdr->number > target_block) { era_close(era); return false; }
            block_body_free(body);
        }
        era_close(era);
    }
    return false;
}

/* =========================================================================
 * Comparison helpers
 * ========================================================================= */

static int check_field_u64(const char *name, uint64_t ours, uint64_t expected) {
    if (ours != expected) {
        printf("    FAIL %s: ours=%lu expected=%lu\n", name, ours, expected);
        return 1;
    }
    printf("    ok   %s: %lu\n", name, ours);
    return 0;
}

static int check_field_bytes(const char *name, const uint8_t *ours,
                             const uint8_t *expected, size_t len) {
    if (memcmp(ours, expected, len) != 0) {
        char h1[65], h2[65];
        to_hex(ours, len < 32 ? len : 32, h1);
        to_hex(expected, len < 32 ? len : 32, h2);
        printf("    FAIL %s: ours=%s expected=%s\n", name, h1, h2);
        return 1;
    }
    char h[65];
    to_hex(ours, len < 32 ? len : 32, h);
    printf("    ok   %s: %s\n", name, h);
    return 0;
}

static int check_field_bool(const char *name, bool ours, bool expected) {
    if (ours != expected) {
        printf("    FAIL %s: ours=%s expected=%s\n", name,
               ours ? "true" : "false", expected ? "true" : "false");
        return 1;
    }
    printf("    ok   %s: %s\n", name, ours ? "true" : "false");
    return 0;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <era_directory> [rpc_url]\n", argv[0]);
        fprintf(stderr, "  rpc_url: e.g. https://ethereum-rpc.publicnode.com\n");
        return 1;
    }

    const char *era_dir = argv[1];
    const char *rpc_url = argc > 2 ? argv[2] : NULL;

    era_files_t ef;
    if (!collect_era_files(era_dir, &ef)) {
        fprintf(stderr, "No .era files found in %s\n", era_dir);
        return 1;
    }
    printf("Found %zu era files\n\n", ef.count);

    int total_errors = 0;

    for (size_t fi = 0; fi < NUM_FORKS; fi++) {
        const fork_check_t *fc = &FORK_CHECKS[fi];
        printf("=== %s (block %lu+) ===\n", fc->name, fc->first_block);

        era_t era;
        era_iter_t it;
        block_header_t hdr;
        block_body_t body;
        uint8_t block_hash[32];
        uint64_t checked = 0;

        if (!find_block(&ef, fc->first_block, &era, &it, &hdr, &body, block_hash)) {
            printf("  SKIP: block %lu not found in era files\n\n", fc->first_block);
            continue;
        }

        /* We found the first block — check it and subsequent blocks */
        do {
            char hash_str[65];
            to_hex(block_hash, 32, hash_str);
            printf("\n  Block %lu (hash=%s)\n", hdr.number, hash_str);

            int errs = 0;

            /* Common checks */
            if (!hdr.has_base_fee) {
                printf("    FAIL: missing base_fee\n");
                errs++;
            }
            if (!uint256_is_zero(&hdr.difficulty)) {
                printf("    FAIL: difficulty != 0\n");
                errs++;
            }

            /* Fork-specific checks */
            if (strcmp(fc->name, "Bellatrix") == 0) {
                errs += check_field_bool("has_withdrawals_root", hdr.has_withdrawals_root, false);
                errs += check_field_bool("has_blob_gas", hdr.has_blob_gas, false);
                errs += check_field_bool("has_parent_beacon_root", hdr.has_parent_beacon_root, false);
            } else if (strcmp(fc->name, "Capella") == 0) {
                errs += check_field_bool("has_withdrawals_root", hdr.has_withdrawals_root, true);
                errs += check_field_bool("has_blob_gas", hdr.has_blob_gas, false);
                errs += check_field_bool("has_parent_beacon_root", hdr.has_parent_beacon_root, false);
            } else if (strcmp(fc->name, "Deneb") == 0 ||
                       strcmp(fc->name, "Pectra") == 0 ||
                       strcmp(fc->name, "Fusaka") == 0) {
                errs += check_field_bool("has_withdrawals_root", hdr.has_withdrawals_root, true);
                errs += check_field_bool("has_blob_gas", hdr.has_blob_gas, true);
                errs += check_field_bool("has_parent_beacon_root", hdr.has_parent_beacon_root, true);
                if (hdr.has_parent_beacon_root && is_zero(hdr.parent_beacon_root.bytes, 32)) {
                    printf("    FAIL: parent_beacon_root is zero\n");
                    errs++;
                }
                if (hdr.has_parent_beacon_root) {
                    char br[65];
                    to_hex(hdr.parent_beacon_root.bytes, 32, br);
                    printf("    info parent_beacon_root: %s\n", br);
                }
                if (hdr.has_blob_gas) {
                    printf("    info blob_gas_used=%lu excess_blob_gas=%lu\n",
                           hdr.blob_gas_used, hdr.excess_blob_gas);
                }
                /* Pectra-specific: EIP-7685 requestsHash */
                if (strcmp(fc->name, "Pectra") == 0 ||
                    strcmp(fc->name, "Fusaka") == 0) {
                    printf("    info has_requests_hash=%s\n",
                           hdr.has_requests_hash ? "true" : "false");
                }
            }

            /* RPC comparison */
            if (rpc_url && errs == 0) {
                rpc_header_t rpc;
                if (fetch_rpc_header(rpc_url, hdr.number, &rpc)) {
                    printf("    --- RPC comparison ---\n");
                    errs += check_field_u64("number", hdr.number, rpc.number);
                    errs += check_field_u64("timestamp", hdr.timestamp, rpc.timestamp);
                    errs += check_field_u64("gas_limit", hdr.gas_limit, rpc.gas_limit);
                    errs += check_field_u64("gas_used", hdr.gas_used, rpc.gas_used);
                    errs += check_field_bytes("parent_hash", hdr.parent_hash.bytes,
                                              rpc.parent_hash, 32);
                    errs += check_field_bytes("coinbase", hdr.coinbase.bytes,
                                              rpc.coinbase, 20);

                    if (hdr.has_blob_gas && rpc.has_blob_gas) {
                        errs += check_field_u64("blob_gas_used", hdr.blob_gas_used,
                                                rpc.blob_gas_used);
                        errs += check_field_u64("excess_blob_gas", hdr.excess_blob_gas,
                                                rpc.excess_blob_gas);
                    }

                    if (hdr.has_parent_beacon_root && rpc.has_parent_beacon_root) {
                        errs += check_field_bytes("parent_beacon_root",
                                                  hdr.parent_beacon_root.bytes,
                                                  rpc.parent_beacon_root, 32);
                    }
                } else {
                    printf("    WARN: RPC fetch failed for block %lu\n", hdr.number);
                }
            }

            total_errors += errs;
            block_body_free(&body);
            checked++;

            /* Read next block */
            if (checked >= fc->check_count) break;
            uint64_t slot;
            if (!era_iter_next(&it, &hdr, &body, block_hash, &slot)) break;
        } while (1);

        era_close(&era);
        printf("\n  %s: %lu blocks checked, %d errors\n\n",
               fc->name, checked, total_errors);
    }

    printf("========================================\n");
    printf("Total errors: %d\n", total_errors);
    printf("Result: %s\n", total_errors == 0 ? "PASS" : "FAIL");
    printf("========================================\n");

    free_era_files(&ef);
    return total_errors == 0 ? 0 : 1;
}
