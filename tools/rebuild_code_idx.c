/*
 * rebuild_code_idx — Rebuild code_store .idx with a properly sized hash table.
 *
 * Usage: rebuild_code_idx <path> [capacity]
 *
 *   <path>      Base path (e.g. ~/.artex/chain_replay_code)
 *   [capacity]  Capacity hint for new table (default: 5000000)
 *
 * Steps:
 *   1. Rename <path>.idx → <path>.idx.old
 *   2. Create new <path>.idx with given capacity
 *   3. Iterate all entries from .idx.old, insert into new .idx
 *   4. Verify: iterate old, confirm every entry exists in new with matching record
 */

#include "disk_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CODE_HASH_SIZE 32
#define RECORD_SIZE    12  /* code_record_t: offset(8) + length(4) */

/* --- Copy callback context --- */

typedef struct {
    disk_table_t *src;
    disk_table_t *dst;
    uint64_t      count;
    uint64_t      errors;
} copy_ctx_t;

static void copy_cb(const uint8_t *key, void *user_data) {
    copy_ctx_t *ctx = user_data;
    uint8_t record[RECORD_SIZE];

    if (!disk_table_get(ctx->src, key, record)) {
        ctx->errors++;
        return;
    }
    if (!disk_table_put(ctx->dst, key, record)) {
        ctx->errors++;
        return;
    }
    ctx->count++;
    if (ctx->count % 100000 == 0)
        fprintf(stderr, "  %lu entries copied...\n", ctx->count);
}

/* --- Verify callback context --- */

typedef struct {
    const disk_table_t *src;
    const disk_table_t *check;
    uint64_t verified;
    uint64_t mismatches;
} verify_ctx_t;

static void verify_cb(const uint8_t *key, void *user_data) {
    verify_ctx_t *ctx = user_data;
    uint8_t rec_src[RECORD_SIZE];
    uint8_t rec_chk[RECORD_SIZE];

    if (!disk_table_get(ctx->src, key, rec_src)) {
        ctx->mismatches++;
        return;
    }
    if (!disk_table_get(ctx->check, key, rec_chk)) {
        ctx->mismatches++;
        if (ctx->mismatches <= 5)
            fprintf(stderr, "  MISMATCH: key exists in old but not in new\n");
        return;
    }
    if (memcmp(rec_src, rec_chk, RECORD_SIZE) != 0) {
        ctx->mismatches++;
        if (ctx->mismatches <= 5)
            fprintf(stderr, "  MISMATCH: record differs for key\n");
        return;
    }
    ctx->verified++;
    if (ctx->verified % 100000 == 0)
        fprintf(stderr, "  %lu entries verified...\n", ctx->verified);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <base_path> [capacity]\n", argv[0]);
        fprintf(stderr, "  base_path: e.g. ~/.artex/chain_replay_code\n");
        fprintf(stderr, "  capacity:  hint for new table (default: 5000000)\n");
        return 1;
    }

    const char *base_path = argv[1];
    uint64_t capacity = argc > 2 ? (uint64_t)atoll(argv[2]) : 5000000;

    /* Build paths */
    size_t len = strlen(base_path);
    char *idx_path = malloc(len + 16);
    char *old_path = malloc(len + 16);
    if (!idx_path || !old_path) {
        fprintf(stderr, "malloc failed\n");
        return 1;
    }
    sprintf(idx_path, "%s.idx", base_path);
    sprintf(old_path, "%s.idx.old", base_path);

    /* Step 1: rename .idx → .idx.old */
    fprintf(stderr, "Renaming %s → %s\n", idx_path, old_path);
    if (rename(idx_path, old_path) != 0) {
        perror("rename failed");
        free(idx_path); free(old_path);
        return 1;
    }

    /* Step 2: open old table */
    fprintf(stderr, "Opening old index...\n");
    disk_table_t *old_dt = disk_table_open(old_path);
    if (!old_dt) {
        fprintf(stderr, "Failed to open %s\n", old_path);
        rename(old_path, idx_path);
        free(idx_path); free(old_path);
        return 1;
    }

    uint64_t entry_count = disk_table_count(old_dt);
    fprintf(stderr, "Old index: %lu entries\n", entry_count);

    /* Step 3: create new table */
    if (capacity < entry_count * 2)
        capacity = entry_count * 2;

    fprintf(stderr, "Creating new index with capacity %lu...\n", capacity);
    disk_table_t *new_dt = disk_table_create(idx_path, CODE_HASH_SIZE,
                                              RECORD_SIZE, capacity);
    if (!new_dt) {
        fprintf(stderr, "Failed to create new index\n");
        disk_table_destroy(old_dt);
        rename(old_path, idx_path);
        free(idx_path); free(old_path);
        return 1;
    }

    /* Step 4: copy all entries */
    fprintf(stderr, "Copying entries...\n");
    copy_ctx_t cc = { .src = old_dt, .dst = new_dt, .count = 0, .errors = 0 };
    disk_table_foreach_key(old_dt, copy_cb, &cc);
    disk_table_sync(new_dt);

    fprintf(stderr, "Copied %lu entries (%lu errors)\n", cc.count, cc.errors);

    /* Step 5: verify — iterate old, confirm every entry matches in new */
    fprintf(stderr, "Verifying...\n");
    verify_ctx_t vc = { .src = old_dt, .check = new_dt,
                        .verified = 0, .mismatches = 0 };
    disk_table_foreach_key(old_dt, verify_cb, &vc);

    uint64_t new_count = disk_table_count(new_dt);

    disk_table_destroy(new_dt);
    disk_table_destroy(old_dt);

    /* Report */
    fprintf(stderr, "\n========================================\n");
    fprintf(stderr, "  Old entries:   %lu\n", entry_count);
    fprintf(stderr, "  Copied:        %lu\n", cc.count);
    fprintf(stderr, "  New entries:    %lu\n", new_count);
    fprintf(stderr, "  Verified:      %lu\n", vc.verified);
    fprintf(stderr, "  Mismatches:    %lu\n", vc.mismatches);
    fprintf(stderr, "  Copy errors:   %lu\n", cc.errors);

    bool ok = (cc.errors == 0) && (vc.mismatches == 0) &&
              (cc.count == entry_count) && (new_count == entry_count);

    fprintf(stderr, "  Status:        %s\n", ok ? "PASSED" : "FAILED");
    fprintf(stderr, "========================================\n");
    fprintf(stderr, "  Old index: %s %s\n", old_path, ok ? "(safe to delete)" : "(KEEP)");
    fprintf(stderr, "  New index: %s\n", idx_path);

    free(idx_path);
    free(old_path);
    return ok ? 0 : 1;
}
