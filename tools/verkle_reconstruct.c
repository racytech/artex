/**
 * Verkle Reconstruct — rebuild Verkle state from genesis + history diffs.
 *
 * Reads per-block diffs from state_history files and applies them to a
 * fresh verkle_state, computing the Verkle root for validation.
 *
 * This is the Verkle counterpart of state_reconstruct (which targets MPT).
 * Same diff format, same history files — different state backend.
 *
 * Usage:
 *   ./verkle_reconstruct <history_dir> <genesis.json> [target_block] [options]
 *
 * Options:
 *   --resume              Resume from existing Verkle snapshot (reads .meta)
 *   --commit-interval N   Commit + sync every N blocks (default: 256)
 *
 * Output state files are written to <history_dir>/../verkle_state/
 */

#include "state_history.h"
#include "verkle_state.h"
#include "code_store.h"
#include "mem_art.h"
#include "hash.h"
#include "address.h"
#include "uint256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <cjson/cJSON.h>

/* =========================================================================
 * Slot Tracker — per-account storage slot index for SELFDESTRUCT
 *
 * During sequential replay, every storage write is recorded here so we
 * know which slots to zero when an account is destructed.
 *
 * Implementation: mem_art keyed by address (20 bytes), value is a
 * slot_set_t pointer. Each slot_set_t holds a dynamic array of 32-byte
 * slot keys.
 * ========================================================================= */

typedef struct {
    uint8_t (*keys)[32];   /* array of 32-byte slot keys */
    uint32_t count;
    uint32_t cap;
} slot_set_t;

typedef struct {
    mem_art_t tree;        /* address (20B) → slot_set_t* */
} slot_tracker_t;

static bool slot_tracker_init(slot_tracker_t *st) {
    return mem_art_init(&st->tree);
}

static bool slot_tracker_free_cb(const uint8_t *key, size_t key_len,
                                 const void *value, size_t value_len,
                                 void *user_data) {
    (void)key; (void)key_len; (void)value_len; (void)user_data;
    const slot_set_t *ss = (const slot_set_t *)value;
    free(ss->keys);
    return true;  /* continue iteration */
}

static void slot_tracker_destroy(slot_tracker_t *st) {
    mem_art_foreach(&st->tree, slot_tracker_free_cb, NULL);
    mem_art_destroy(&st->tree);
}

/** Record that `addr` has written `slot`. Deduplicates. */
static void slot_tracker_add(slot_tracker_t *st,
                              const uint8_t addr[20],
                              const uint8_t slot[32]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);

    if (!ss) {
        /* First slot for this address — create a new set */
        slot_set_t new_ss = {0};
        new_ss.cap = 8;
        new_ss.keys = malloc(new_ss.cap * 32);
        if (!new_ss.keys) return;
        memcpy(new_ss.keys[0], slot, 32);
        new_ss.count = 1;
        /* Store the struct by value in the ART */
        mem_art_insert(&st->tree, addr, 20, &new_ss, sizeof(slot_set_t));
        return;
    }

    /* Check for duplicate */
    for (uint32_t i = 0; i < ss->count; i++) {
        if (memcmp(ss->keys[i], slot, 32) == 0)
            return;
    }

    /* Grow if needed */
    if (ss->count >= ss->cap) {
        uint32_t new_cap = ss->cap * 2;
        uint8_t (*new_keys)[32] = realloc(ss->keys, new_cap * 32);
        if (!new_keys) return;
        ss->keys = new_keys;
        ss->cap = new_cap;
    }

    memcpy(ss->keys[ss->count], slot, 32);
    ss->count++;
}

/** Get all slots for an address. Returns NULL if none. */
static const uint8_t *slot_tracker_get(const slot_tracker_t *st,
                                        const uint8_t addr[20],
                                        uint32_t *out_count) {
    size_t val_len = 0;
    const slot_set_t *ss = (const slot_set_t *)mem_art_get(
        &st->tree, addr, 20, &val_len);
    if (!ss || ss->count == 0) {
        *out_count = 0;
        return NULL;
    }
    *out_count = ss->count;
    return (const uint8_t *)ss->keys;
}

/** Remove all slots for an address (after destruction). */
static void slot_tracker_clear(slot_tracker_t *st,
                                const uint8_t addr[20]) {
    size_t val_len = 0;
    slot_set_t *ss = (slot_set_t *)mem_art_get_mut(
        &st->tree, addr, 20, &val_len);
    if (ss) {
        free(ss->keys);
        ss->keys = NULL;
        ss->count = 0;
        ss->cap = 0;
    }
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void print_hash(const uint8_t *h) {
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* =========================================================================
 * Metadata file — records what block the Verkle snapshot corresponds to
 * ========================================================================= */

#define VMETA_MAGIC   0x4C4B5256  /* "VRKL" */
#define VMETA_VERSION 1

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint64_t last_block;
    uint8_t  state_root[32];
    uint8_t  reserved[8];
} verkle_meta_t;  /* 56 bytes */

static bool meta_write(const char *dir, uint64_t block,
                       const uint8_t root[32]) {
    char path[512];
    snprintf(path, sizeof(path), "%s/.meta", dir);

    verkle_meta_t meta = {0};
    meta.magic = VMETA_MAGIC;
    meta.version = VMETA_VERSION;
    meta.last_block = block;
    memcpy(meta.state_root, root, 32);

    FILE *f = fopen(path, "wb");
    if (!f) { perror("meta_write"); return false; }
    if (fwrite(&meta, sizeof(meta), 1, f) != 1) {
        fclose(f); return false;
    }
    fclose(f);
    return true;
}

static bool meta_read(const char *dir, verkle_meta_t *out) {
    char path[512];
    snprintf(path, sizeof(path), "%s/.meta", dir);

    FILE *f = fopen(path, "rb");
    if (!f) return false;
    if (fread(out, sizeof(*out), 1, f) != 1) {
        fclose(f); return false;
    }
    fclose(f);

    if (out->magic != VMETA_MAGIC || out->version != VMETA_VERSION) {
        fprintf(stderr, "Invalid verkle meta: magic=0x%08x version=%u\n",
                out->magic, out->version);
        return false;
    }
    return true;
}

/* =========================================================================
 * Genesis loading — apply initial allocations to verkle state
 * ========================================================================= */

static bool load_genesis(verkle_state_t *vs, const char *path) {
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

        /* Set version = 0 for all genesis accounts */
        verkle_state_set_version(vs, addr.bytes, 0);

        /* Balance */
        cJSON *bal_item = cJSON_GetObjectItem(entry, "balance");
        if (bal_item && cJSON_IsString(bal_item)) {
            uint256_t balance = uint256_from_hex(bal_item->valuestring);
            uint8_t bal_bytes[32];
            uint256_to_bytes_le(&balance, bal_bytes);
            verkle_state_set_balance(vs, addr.bytes, bal_bytes);
        }

        /* Nonce */
        cJSON *nonce_item = cJSON_GetObjectItem(entry, "nonce");
        if (nonce_item && cJSON_IsString(nonce_item)) {
            uint64_t nonce = strtoull(nonce_item->valuestring, NULL, 0);
            verkle_state_set_nonce(vs, addr.bytes, nonce);
        }

        /* Code */
        cJSON *code_item = cJSON_GetObjectItem(entry, "code");
        if (code_item && cJSON_IsString(code_item)) {
            /* TODO: parse hex code, call verkle_state_set_code()
             * and verkle_state_set_code_hash() */
        }

        /* Storage */
        cJSON *storage_item = cJSON_GetObjectItem(entry, "storage");
        if (storage_item) {
            cJSON *slot_entry;
            cJSON_ArrayForEach(slot_entry, storage_item) {
                if (!slot_entry->string || !cJSON_IsString(slot_entry))
                    continue;
                uint256_t slot = uint256_from_hex(slot_entry->string);
                uint256_t value = uint256_from_hex(slot_entry->valuestring);
                uint8_t slot_bytes[32], val_bytes[32];
                uint256_to_bytes_le(&slot, slot_bytes);
                uint256_to_bytes_le(&value, val_bytes);
                verkle_state_set_storage(vs, addr.bytes, slot_bytes, val_bytes);
            }
        }

        count++;
    }

    cJSON_Delete(root);
    printf("Genesis: loaded %zu accounts\n", count);
    return true;
}

/* =========================================================================
 * Apply a single block diff to verkle state
 * ========================================================================= */

static bool apply_diff_to_verkle(verkle_state_t *vs,
                                 code_store_t *cs,
                                 slot_tracker_t *tracker,
                                 const block_diff_t *diff) {
    for (uint16_t g = 0; g < diff->group_count; g++) {
        const addr_diff_t *grp = &diff->groups[g];
        const uint8_t *addr = grp->addr.bytes;

        /* Handle destruction: clear all state for this account */
        if (grp->flags & ACCT_DIFF_DESTRUCTED) {
            uint32_t slot_count = 0;
            const uint8_t *slots = slot_tracker_get(tracker, addr,
                                                     &slot_count);
            verkle_state_clear_account(vs, addr, slots, slot_count);
            slot_tracker_clear(tracker, addr);
        }

        /* Account creation — set version */
        if (grp->flags & ACCT_DIFF_CREATED) {
            verkle_state_set_version(vs, addr, 0);
        }

        /* Nonce */
        if (grp->field_mask & FIELD_NONCE) {
            verkle_state_set_nonce(vs, addr, grp->nonce);
        }

        /* Balance */
        if (grp->field_mask & FIELD_BALANCE) {
            uint8_t bal_bytes[32];
            uint256_to_bytes_le(&grp->balance, bal_bytes);
            verkle_state_set_balance(vs, addr, bal_bytes);
        }

        /* Code hash + code chunks */
        if (grp->field_mask & FIELD_CODE_HASH) {
            verkle_state_set_code_hash(vs, addr, grp->code_hash.bytes);

            /* Verkle stores code in 31-byte chunks. Load actual bytecode
             * from code_store, chunk it, and write to tree. */
            if (cs) {
                uint32_t code_len = code_store_get_size(cs,
                                                         grp->code_hash.bytes);
                if (code_len > 0) {
                    uint8_t *code = malloc(code_len);
                    if (code) {
                        uint32_t got = code_store_get(cs,
                                                       grp->code_hash.bytes,
                                                       code, code_len);
                        if (got == code_len) {
                            verkle_state_set_code(vs, addr, code, code_len);
                        }
                        free(code);
                    }
                } else {
                    /* Code hash set but code not in store — could be
                     * the empty code hash (keccak of empty). Set size 0. */
                    verkle_state_set_code_size(vs, addr, 0);
                }
            } else {
                /* No code store available — can only set code_hash.
                 * Code chunks and code_size will be missing.
                 * TODO: decide if this is a fatal error. */
                fprintf(stderr, "Warning: code_hash changed for ");
                for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", addr[i]);
                fprintf(stderr, " but no code_store available\n");
            }
        }

        /* Storage slots — write to verkle + track for future SELFDESTRUCT */
        for (uint16_t s = 0; s < grp->slot_count; s++) {
            const slot_diff_t *sd = &grp->slots[s];
            uint8_t slot_bytes[32], val_bytes[32];
            uint256_to_bytes_le(&sd->slot, slot_bytes);
            uint256_to_bytes_le(&sd->value, val_bytes);
            verkle_state_set_storage(vs, addr, slot_bytes, val_bytes);
            slot_tracker_add(tracker, addr, slot_bytes);
        }
    }

    return true;
}

/* =========================================================================
 * Main
 * ========================================================================= */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s <history_dir> <genesis.json> [target_block] [options]\n"
        "\n"
        "Rebuilds Verkle state from genesis + history diffs.\n"
        "\n"
        "Options:\n"
        "  --resume              Resume from existing snapshot (reads .meta)\n"
        "  --commit-interval N   Commit + sync every N blocks (default: 256)\n",
        prog);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }

    const char *history_dir  = argv[1];
    const char *genesis_path = argv[2];
    uint64_t    target_block = 0;
    bool        has_target   = false;
    bool        resume_mode  = false;
    uint64_t    commit_interval = 256;

    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--resume") == 0) {
            resume_mode = true;
        } else if (strcmp(argv[i], "--commit-interval") == 0 && i + 1 < argc) {
            commit_interval = strtoull(argv[++i], NULL, 10);
        } else if (argv[i][0] != '-') {
            target_block = strtoull(argv[i], NULL, 10);
            has_target = true;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            usage(argv[0]);
            return 1;
        }
    }

    /* ── Open history ──────────────────────────────────────────────────── */
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

    /* ── Build output paths ────────────────────────────────────────────── */
    char data_dir[512];
    strncpy(data_dir, history_dir, sizeof(data_dir) - 1);
    data_dir[sizeof(data_dir) - 1] = '\0';
    size_t dlen = strlen(data_dir);
    if (dlen > 0 && data_dir[dlen - 1] == '/') data_dir[--dlen] = '\0';
    char *last_slash = strrchr(data_dir, '/');
    if (last_slash) *last_slash = '\0';

    char val_dir[512], comm_dir[512], code_path[512];
    snprintf(val_dir, sizeof(val_dir), "%s/verkle_values", data_dir);
    snprintf(comm_dir, sizeof(comm_dir), "%s/verkle_commits", data_dir);
    snprintf(code_path, sizeof(code_path), "%s/chain_replay_code", data_dir);

    printf("Target block:    %lu\n", target_block);
    printf("Commit interval: %lu blocks\n", commit_interval);
    printf("Value store:     %s\n", val_dir);
    printf("Commit store:    %s\n", comm_dir);
    printf("Code store:      %s (existing)\n", code_path);

    /* ── Open code store (read-only, for bytecode during code_hash diffs) */
    code_store_t *cs = code_store_open(code_path);
    if (!cs) {
        fprintf(stderr, "Warning: code store not found at %s\n", code_path);
        fprintf(stderr, "  Code chunks will not be written to Verkle tree.\n");
        /* Non-fatal — we can still reconstruct without code chunks,
         * but the Verkle root will differ from a full reconstruction. */
    }

    /* ── Create or resume Verkle state ─────────────────────────────────── */
    uint64_t start_block = hist_first;
    verkle_state_t *vs = NULL;

    if (resume_mode) {
        verkle_meta_t meta;
        if (!meta_read(val_dir, &meta)) {
            fprintf(stderr, "No valid .meta file — cannot resume\n");
            if (cs) code_store_destroy(cs);
            state_history_destroy(sh);
            return 1;
        }

        printf("\nResuming from snapshot at block %lu\n", meta.last_block);
        printf("Snapshot root: 0x"); print_hash(meta.state_root); printf("\n");

        start_block = meta.last_block + 1;

        if (start_block > target_block) {
            printf("Snapshot already at or past target — nothing to do\n");
            if (cs) code_store_destroy(cs);
            state_history_destroy(sh);
            return 0;
        }

        vs = verkle_state_open_flat(val_dir, comm_dir);
    } else {
        mkdir(val_dir, 0755);
        mkdir(comm_dir, 0755);
        vs = verkle_state_create_flat(val_dir, comm_dir);
    }

    if (!vs) {
        fprintf(stderr, "Failed to create/open verkle state\n");
        if (cs) code_store_destroy(cs);
        state_history_destroy(sh);
        return 1;
    }

    /* ── Load genesis (only on fresh start) ────────────────────────────── */
    if (!resume_mode) {
        printf("\nLoading genesis...\n");
        verkle_state_begin_block(vs, 0);

        if (!load_genesis(vs, genesis_path)) {
            fprintf(stderr, "Failed to load genesis\n");
            verkle_state_destroy(vs);
            if (cs) code_store_destroy(cs);
            state_history_destroy(sh);
            return 1;
        }

        verkle_state_commit_block(vs);

        uint8_t genesis_root[32];
        verkle_state_root_hash(vs, genesis_root);
        printf("Genesis Verkle root: 0x"); print_hash(genesis_root);
        printf("\n");
    }

    /* ── Init slot tracker (for SELFDESTRUCT support) ─────────────────── */
    slot_tracker_t tracker;
    if (!slot_tracker_init(&tracker)) {
        fprintf(stderr, "Failed to init slot tracker\n");
        verkle_state_destroy(vs);
        if (cs) code_store_destroy(cs);
        state_history_destroy(sh);
        return 1;
    }

    /* ── Replay diffs ──────────────────────────────────────────────────── */
    printf("\nReplaying blocks %lu .. %lu (%lu blocks)...\n",
           start_block, target_block, target_block - start_block + 1);

    double t0 = now_sec();
    uint64_t applied = 0;

    for (uint64_t bn = start_block; bn <= target_block; bn++) {
        block_diff_t diff;
        if (!state_history_get_diff(sh, bn, &diff)) {
            fprintf(stderr, "\nFailed to read diff for block %lu\n", bn);
            break;
        }

        verkle_state_begin_block(vs, bn);
        apply_diff_to_verkle(vs, cs, &tracker, &diff);
        verkle_state_commit_block(vs);
        block_diff_free(&diff);
        applied++;

        /* Periodic sync + progress */
        if (bn % commit_interval == 0) {
            verkle_state_sync(vs);

            if (bn % (commit_interval * 40) == 0) {
                double elapsed = now_sec() - t0;
                double bps = applied / elapsed;
                double eta = (target_block - bn) / bps;
                printf("\n  checkpoint %lu  (%.0f blk/s, ETA %.0fs)",
                       bn, bps, eta);
            }
        }

        if (applied % 10000 == 0) {
            double elapsed = now_sec() - t0;
            double bps = applied / elapsed;
            double eta = (target_block - bn) / bps;
            printf("\r  block %lu / %lu  (%lu applied, %.0f blk/s, ETA %.0fs)",
                   bn, target_block, applied, bps, eta);
            fflush(stdout);
        }
    }

    double elapsed = now_sec() - t0;
    printf("\n\nReplay complete: %lu blocks in %.1f seconds (%.0f blk/s)\n",
           applied, elapsed, applied > 0 ? applied / elapsed : 0);

    /* ── Compute Verkle root ───────────────────────────────────────────── */
    printf("\nComputing Verkle root...\n");
    double t_root = now_sec();

    uint8_t actual_root[32];
    verkle_state_root_hash(vs, actual_root);

    double root_ms = (now_sec() - t_root) * 1000.0;
    printf("Root computation: %.1f ms\n", root_ms);
    printf("Verkle root: 0x"); print_hash(actual_root); printf("\n");

    /* NOTE: There is no era1 expected Verkle root — Ethereum mainnet
     * uses MPT roots. Verkle root validation requires either:
     *   (a) A reference Verkle implementation to compare against, or
     *   (b) Pre-computed Verkle roots from a trusted source.
     *
     * For now, we just print the root and save the snapshot.
     * Cross-validation can be done by running two independent
     * Verkle reconstructions and comparing roots. */

    /* ── Flush + save metadata ─────────────────────────────────────────── */
    printf("\nFlushing Verkle state to disk...\n");
    verkle_state_sync(vs);

    if (meta_write(val_dir, target_block, actual_root)) {
        printf("State saved (block %lu)\n", target_block);
    } else {
        fprintf(stderr, "Warning: state flushed but meta write failed\n");
    }

    /* ── Cleanup ───────────────────────────────────────────────────────── */
    slot_tracker_destroy(&tracker);
    verkle_state_destroy(vs);
    if (cs) code_store_destroy(cs);
    state_history_destroy(sh);

    printf("\nDone.\n");
    return 0;
}
