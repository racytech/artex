/**
 * State Reconstruct — rebuild EVM state from genesis + history diffs.
 *
 * Reads per-block diffs from state_history files and applies them to a
 * fresh evm_state, then computes the MPT root for validation.
 *
 * Usage:
 *   ./state_reconstruct <history_dir> <genesis.json> <era1_dir> [target_block]
 *
 * If target_block is omitted, replays all available blocks in history.
 * The expected state root is read from the era1 block header.
 *
 * Output state files are written to <history_dir>/../reconstruct_*
 */

#include "state_history.h"
#include "evm_state.h"
#include "era1.h"
#include "block.h"
#include "hash.h"
#include "fork.h"
#include "code_store.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <sys/stat.h>
#include <cjson/cJSON.h>

#define ERA1_BLOCKS_PER_FILE 8192

/* =========================================================================
 * Era1 archive (minimal — just for reading one block header)
 * ========================================================================= */

typedef struct {
    char   **paths;
    size_t   count;
    era1_t   current;
    int      current_idx;
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
    if (ar->current_idx >= 0) era1_close(&ar->current);
    for (size_t i = 0; i < ar->count; i++) free(ar->paths[i]);
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
    if ((size_t)file_idx >= ar->count) return false;
    if (!era1_open(&ar->current, ar->paths[file_idx])) return false;
    ar->current_idx = file_idx;
    return era1_contains(&ar->current, block_number);
}

/* =========================================================================
 * Genesis loading (same as sync.c)
 * ========================================================================= */

static bool load_genesis(evm_state_t *state, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) { perror("load_genesis"); return false; }

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    char *json_str = malloc(fsize + 1);
    if (!json_str) { fclose(f); return false; }
    size_t nread = fread(json_str, 1, fsize, f);
    (void)nread;
    json_str[fsize] = '\0';
    fclose(f);

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root) {
        fprintf(stderr, "Genesis: JSON parse error\n");
        return false;
    }

    size_t count = 0;
    cJSON *entry;
    cJSON_ArrayForEach(entry, root) {
        const char *addr_hex = entry->string;
        if (!addr_hex) continue;

        address_t addr;
        if (!address_from_hex(addr_hex, &addr)) continue;

        cJSON *bal_item = cJSON_GetObjectItem(entry, "balance");
        if (!bal_item || !cJSON_IsString(bal_item)) continue;

        uint256_t balance = uint256_from_hex(bal_item->valuestring);
        if (!uint256_is_zero(&balance))
            evm_state_add_balance(state, &addr, &balance);
        else
            evm_state_create_account(state, &addr);
        count++;
    }

    cJSON_Delete(root);
    printf("Genesis: loaded %zu accounts\n", count);
    return true;
}

/* =========================================================================
 * Metadata file — records what block the MPT snapshot corresponds to
 * ========================================================================= */

#define META_MAGIC 0x54504D52  /* "RMPT" */
#define META_VERSION 1

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t last_block;
    uint8_t  state_root[32];
    uint8_t  prune_empty;
    uint8_t  reserved[7];
} reconstruct_meta_t;  /* 56 bytes */

static bool meta_write(const char *mpt_path, uint64_t block,
                       const hash_t *root, bool prune_empty) {
    char path[512];
    snprintf(path, sizeof(path), "%s.meta", mpt_path);

    reconstruct_meta_t meta = {0};
    meta.magic = META_MAGIC;
    meta.version = META_VERSION;
    meta.last_block = block;
    memcpy(meta.state_root, root->bytes, 32);
    meta.prune_empty = prune_empty ? 1 : 0;

    FILE *f = fopen(path, "wb");
    if (!f) { perror("meta_write"); return false; }
    if (fwrite(&meta, sizeof(meta), 1, f) != 1) {
        fclose(f); return false;
    }
    fclose(f);
    return true;
}

static bool meta_read(const char *mpt_path, reconstruct_meta_t *out) {
    char path[512];
    snprintf(path, sizeof(path), "%s.meta", mpt_path);

    FILE *f = fopen(path, "rb");
    if (!f) return false;
    if (fread(out, sizeof(*out), 1, f) != 1) {
        fclose(f); return false;
    }
    fclose(f);

    if (out->magic != META_MAGIC) {
        fprintf(stderr, "Invalid meta magic: 0x%08x\n", out->magic);
        return false;
    }
    if (out->version != META_VERSION) {
        fprintf(stderr, "Unsupported meta version: %u\n", out->version);
        return false;
    }
    return true;
}

/* =========================================================================
 * Print hash
 * ========================================================================= */

static void print_hash(const hash_t *h) {
    for (int i = 0; i < 32; i++) printf("%02x", h->bytes[i]);
}

/* =========================================================================
 * Main
 * ========================================================================= */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <history_dir> <genesis.json> <era1_dir> [target_block] [options]\n"
        "\n"
        "Options:\n"
        "  --resume                 Resume from existing MPT snapshot (reads .meta)\n"
        "  --evict-interval N       Evict cache every N blocks (default: 256)\n",
        prog);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        usage(argv[0]);
        return 1;
    }

    const char *history_dir  = argv[1];
    const char *genesis_path = argv[2];
    const char *era1_dir     = argv[3];
    uint64_t    target_block = 0;
    bool        has_target   = false;
    bool        resume_mode  = false;
    uint64_t    evict_interval    = 256;

    /* Parse positional + optional flags */
    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--resume") == 0) {
            resume_mode = true;
        } else if (strcmp(argv[i], "--evict-interval") == 0 && i + 1 < argc) {
            evict_interval = strtoull(argv[++i], NULL, 10);
        } else if (argv[i][0] != '-') {
            target_block = strtoull(argv[i], NULL, 10);
            has_target = true;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    /* ── Open history (read-only, no consumer thread needed) ──────────── */
    /* We use state_history_create which spawns a consumer thread, but we
     * won't push any diffs — we only read. The thread will idle. */
    state_history_t *sh = state_history_create(history_dir);
    if (!sh) {
        fprintf(stderr, "Failed to open history at %s\n", history_dir);
        return 1;
    }

    uint64_t hist_first, hist_last;
    if (!state_history_range(sh, &hist_first, &hist_last)) {
        fprintf(stderr, "History is empty\n");
        state_history_destroy(sh);
        return 1;
    }

    printf("History range: %lu .. %lu (%lu blocks)\n",
           hist_first, hist_last, hist_last - hist_first + 1);

    if (!has_target)
        target_block = hist_last;

    if (target_block > hist_last) {
        fprintf(stderr, "Target block %lu exceeds history last block %lu\n",
                target_block, hist_last);
        state_history_destroy(sh);
        return 1;
    }

    /* ── Read expected state root from era1 ──────────────────────────── */
    era1_archive_t archive;
    if (!archive_open(&archive, era1_dir)) {
        fprintf(stderr, "Failed to open era1 directory %s\n", era1_dir);
        state_history_destroy(sh);
        return 1;
    }

    hash_t expected_root = {0};
    if (!archive_ensure(&archive, target_block)) {
        fprintf(stderr, "Era1 file not found for block %lu\n", target_block);
        archive_close(&archive);
        state_history_destroy(sh);
        return 1;
    }

    uint8_t *hdr_rlp = NULL;
    size_t hdr_len = 0;
    uint8_t *body_rlp = NULL;
    size_t body_len = 0;
    if (!era1_read_block(&archive.current, target_block,
                         &hdr_rlp, &hdr_len, &body_rlp, &body_len)) {
        fprintf(stderr, "Failed to read block %lu from era1\n", target_block);
        archive_close(&archive);
        state_history_destroy(sh);
        return 1;
    }

    block_header_t header;
    if (!block_header_decode_rlp(&header, hdr_rlp, hdr_len)) {
        fprintf(stderr, "Failed to decode block %lu header\n", target_block);
        free(hdr_rlp);
        free(body_rlp);
        archive_close(&archive);
        state_history_destroy(sh);
        return 1;
    }
    expected_root = header.state_root;
    free(hdr_rlp);
    free(body_rlp);
    /* Keep archive open for per-block debug validation */

    printf("Target block: %lu\n", target_block);
    printf("Expected root: 0x"); print_hash(&expected_root); printf("\n");
    printf("Evict interval: %lu blocks (validation at final block only)\n",
           evict_interval);

    /* ── Build output paths ──────────────────────────────────────────── */
    char mpt_path[512];
    char code_path[512];

    /* Derive data dir from history_dir (parent directory) */
    char data_dir[512];
    strncpy(data_dir, history_dir, sizeof(data_dir) - 1);
    data_dir[sizeof(data_dir) - 1] = '\0';
    /* Strip trailing slash */
    size_t dlen = strlen(data_dir);
    if (dlen > 0 && data_dir[dlen - 1] == '/') data_dir[--dlen] = '\0';
    /* Go up one directory */
    char *last_slash = strrchr(data_dir, '/');
    if (last_slash) *last_slash = '\0';

    snprintf(mpt_path, sizeof(mpt_path), "%s/chain_replay_mpt", data_dir);
    snprintf(code_path, sizeof(code_path), "%s/chain_replay_code", data_dir);

    printf("MPT output: %s\n", mpt_path);
    printf("Code store: %s (existing)\n", code_path);

    /* ── Create evm_state (fresh or resume) ──────────────────────────── */
    code_store_t *cs = code_store_open(code_path);
    if (!cs) {
        fprintf(stderr, "Failed to open code store at %s\n", code_path);
        state_history_destroy(sh);
        return 1;
    }

    uint64_t start_block = hist_first;  /* first block to replay */

    if (resume_mode) {
        /* ── Resume from existing snapshot ──────────────────────────── */
        reconstruct_meta_t meta;
        if (!meta_read(mpt_path, &meta)) {
            fprintf(stderr, "No valid .meta file at %s — cannot resume\n", mpt_path);
            code_store_destroy(cs);
            state_history_destroy(sh);
            return 1;
        }

        printf("\nResuming from snapshot at block %lu\n", meta.last_block);
        printf("Snapshot root: 0x");
        hash_t snap_root;
        memcpy(snap_root.bytes, meta.state_root, 32);
        print_hash(&snap_root);
        printf("\n");

        start_block = meta.last_block + 1;

        if (start_block > target_block) {
            printf("Snapshot already at or past target block — nothing to do\n");
            code_store_destroy(cs);
            state_history_destroy(sh);
            return 0;
        }
    }

    evm_state_t *es = evm_state_create(cs);
    if (!es) {
        fprintf(stderr, "Failed to create evm_state\n");
        code_store_destroy(cs);
        state_history_destroy(sh);
        return 1;
    }

    if (!resume_mode) {
        /* ── Load genesis ──────────────────────────────────────────── */
        printf("\nLoading genesis...\n");
        evm_state_begin_block(es, 0);

        if (!load_genesis(es, genesis_path)) {
            fprintf(stderr, "Failed to load genesis\n");
            evm_state_destroy(es);
            code_store_destroy(cs);
            state_history_destroy(sh);
            return 1;
        }

        evm_state_commit(es);
        evm_state_finalize(es);

        {
            hash_t genesis_root = evm_state_compute_mpt_root(es, false);
            printf("Genesis root: 0x"); print_hash(&genesis_root); printf("\n");
        }
    }

    /* ── Replay diffs ────────────────────────────────────────────────── */
    /* Follow chain_replay's exact lifecycle:
     *   batch_mode ON → per block: begin_block → apply_diff (set_*) → commit → finalize
     *   every 256 blocks: compute_mpt_root → flush → evict_cache
     */
    printf("\nReplaying blocks %lu .. %lu (%lu blocks)...\n",
           start_block, target_block, target_block - start_block + 1);

    evm_state_set_batch_mode(es, true);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    uint64_t applied = 0;
    for (uint64_t bn = start_block; bn <= target_block; bn++) {
        block_diff_t diff;
        if (!state_history_get_diff(sh, bn, &diff)) {
            fprintf(stderr, "\nFailed to read diff for block %lu\n", bn);
            break;
        }

        /* Mirror chain_replay: commit at block start to snapshot originals
         * and clear tx-level flags (created, dirty, code_dirty, etc.) */
        evm_state_commit(es);
        evm_state_begin_block(es, bn);

        evm_state_apply_diff_bulk(es, &diff);
        block_diff_free(&diff);
        applied++;

        /* Build trie incrementally + flush + evict at interval boundaries */
        if (bn % evict_interval == 0) {
            bool pe = (bn >= 2675000);
            evm_state_compute_mpt_root(es, pe);
            evm_state_flush(es);
            evm_state_evict_cache(es);

            if (bn % (evict_interval * 40) == 0)
                printf("\n  checkpoint %lu done\n", bn);
        }

        if (applied % 10000 == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            double elapsed = (t1.tv_sec - t0.tv_sec) +
                             (t1.tv_nsec - t0.tv_nsec) / 1e9;
            double bps = applied / elapsed;
            double eta = (target_block - bn) / bps;
            printf("\r  block %lu / %lu  (%lu applied, %.0f blk/s, ETA %.0fs)",
                   bn, target_block, applied, bps, eta);
            fflush(stdout);
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double elapsed = (t1.tv_sec - t0.tv_sec) +
                     (t1.tv_nsec - t0.tv_nsec) / 1e9;

    printf("\n\nReplay complete: %lu blocks in %.1f seconds (%.0f blk/s)\n",
           applied, elapsed, applied / elapsed);

    /* ── Compute MPT root ────────────────────────────────────────────── */
    printf("\nComputing MPT root...\n");
    struct timespec tr0, tr1;
    clock_gettime(CLOCK_MONOTONIC, &tr0);

    /* Spurious Dragon prune_empty starts at block 2,675,000.
     * For blocks before that, use prune_empty=false. */
    bool prune_empty = (target_block >= 2675000);
    hash_t actual_root = evm_state_compute_mpt_root(es, prune_empty);

    clock_gettime(CLOCK_MONOTONIC, &tr1);
    double root_ms = (tr1.tv_sec - tr0.tv_sec) * 1000.0 +
                     (tr1.tv_nsec - tr0.tv_nsec) / 1e6;

    printf("Root computation: %.1f ms\n", root_ms);
    printf("\n");
    printf("Actual root:   0x"); print_hash(&actual_root); printf("\n");
    printf("Expected root: 0x"); print_hash(&expected_root); printf("\n");

    if (memcmp(actual_root.bytes, expected_root.bytes, 32) == 0) {
        printf("\n** STATE ROOT MATCH — reconstruction successful! **\n");
    } else {
        printf("\n** STATE ROOT MISMATCH — reconstruction failed **\n");
    }

    /* ── Flush to disk + write metadata ─────────────────────────────── */
    if (memcmp(actual_root.bytes, expected_root.bytes, 32) == 0) {
        printf("\nFlushing reconstructed state to disk...\n");
        evm_state_flush(es);
        if (meta_write(mpt_path, target_block, &actual_root, prune_empty)) {
            printf("State saved to %s (block %lu)\n", mpt_path, target_block);
        } else {
            fprintf(stderr, "Warning: state flushed but meta write failed\n");
        }
    }

    /* ── Cleanup ─────────────────────────────────────────────────────── */
    archive_close(&archive);
    evm_state_destroy(es);
    code_store_destroy(cs);
    state_history_destroy(sh);

    return 0;
}
