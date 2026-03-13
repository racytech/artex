/*
 * Verkle Flat Updater — Stress Test
 *
 * Cross-validates verkle_flat against the in-memory verkle tree.
 * Same operations on both, root hash compared after every block.
 * Fails immediately on first mismatch with full diagnostics.
 *
 * Phases:
 *   1. Genesis: N accounts (2 keys each: nonce + balance), build tree + flush
 *   2. Block processing: K blocks, mixed ops (update, new account, revert)
 *   3. Revert stress: commit then revert alternating blocks, verify root
 *   4. Persistence: sync, close, reopen, verify root + random spot checks
 *   5. Final scan: verify every tracked key in both stores matches
 *
 * Usage: ./test_stress_verkle_flat [accounts_K] [blocks]
 *   Default: 10 (10K accounts), 200 blocks
 *
 * Fixed seed — fully reproducible.
 */

#include "verkle_flat.h"
#include "verkle.h"
#include "pedersen.h"
#include "verkle_commit_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define OPS_PER_BLOCK       200
#define REVERT_BLOCKS       50
#define SPOT_CHECK_COUNT    500

#define VAL_DIR   "/tmp/stress_vf_vals"
#define COMM_DIR  "/tmp/stress_vf_comm"

#define MASTER_SEED 0xDEADBEEF42ULL

/* =========================================================================
 * Fail-Fast
 * ========================================================================= */

#define ASSERT_MSG(cond, fmt, ...) do {                                   \
    if (!(cond)) {                                                        \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n",                         \
                __FILE__, __LINE__, ##__VA_ARGS__);                       \
        abort();                                                          \
    }                                                                     \
} while(0)

static int checks_passed = 0;

#define CHECK(cond, fmt, ...) do {                                        \
    if (!(cond)) {                                                        \
        fprintf(stderr, "FAIL %s:%d: " fmt "\n",                         \
                __FILE__, __LINE__, ##__VA_ARGS__);                       \
        abort();                                                          \
    }                                                                     \
    checks_passed++;                                                      \
} while(0)

/* =========================================================================
 * RNG (SplitMix64)
 * ========================================================================= */

typedef struct { uint64_t state; } rng_t;

static inline uint64_t rng_next(rng_t *rng) {
    uint64_t z = (rng->state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void rng_fill(rng_t *rng, uint8_t *buf, size_t len) {
    for (size_t i = 0; i < len; i += 8) {
        uint64_t r = rng_next(rng);
        size_t n = (len - i < 8) ? len - i : 8;
        memcpy(buf + i, &r, n);
    }
}

/* =========================================================================
 * Key Tracker — tracks all keys for final verification
 * ========================================================================= */

typedef struct {
    uint8_t  key[32];
    uint8_t  value[32];
} tracked_kv_t;

static tracked_kv_t *tracked = NULL;
static int tracked_count = 0;
static int tracked_cap = 0;

static void track_set(const uint8_t key[32], const uint8_t value[32]) {
    /* Update existing or append */
    for (int i = 0; i < tracked_count; i++) {
        if (memcmp(tracked[i].key, key, 32) == 0) {
            memcpy(tracked[i].value, value, 32);
            return;
        }
    }
    if (tracked_count >= tracked_cap) {
        tracked_cap = tracked_cap ? tracked_cap * 2 : 8192;
        tracked = realloc(tracked, tracked_cap * sizeof(tracked_kv_t));
        ASSERT_MSG(tracked, "realloc tracked");
    }
    memcpy(tracked[tracked_count].key, key, 32);
    memcpy(tracked[tracked_count].value, value, 32);
    tracked_count++;
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void cleanup(void) {
    system("rm -rf " VAL_DIR " " COMM_DIR);
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void hex8(char *out, const uint8_t *data) {
    for (int i = 0; i < 8; i++)
        sprintf(out + i * 2, "%02x", data[i]);
    out[16] = '\0';
}

static void compare_roots(const verkle_flat_t *vf, const verkle_tree_t *vt,
                           const char *context)
{
    uint8_t flat_hash[32], tree_hash[32];
    verkle_flat_root_hash(vf, flat_hash);
    verkle_root_hash(vt, tree_hash);

    if (memcmp(flat_hash, tree_hash, 32) != 0) {
        char fh[17], th[17];
        hex8(fh, flat_hash);
        hex8(th, tree_hash);
        fprintf(stderr, "ROOT MISMATCH at %s\n  flat: %s...\n  tree: %s...\n",
                context, fh, th);
        abort();
    }
    checks_passed++;
}

/* Apply one set to both tree and flat */
static void dual_set(verkle_flat_t *vf, verkle_tree_t *vt,
                      const uint8_t key[32], const uint8_t val[32])
{
    verkle_set(vt, key, val);
    ASSERT_MSG(verkle_flat_set(vf, key, val), "flat_set failed");
    track_set(key, val);
}

/* =========================================================================
 * Phase 1: Genesis — bulk account creation
 * ========================================================================= */

static void phase1_genesis(verkle_flat_t *vf, verkle_tree_t *vt,
                            int num_accounts, rng_t *rng)
{
    printf("Phase 1: Genesis — %d accounts\n", num_accounts);
    double t0 = now_sec();

    ASSERT_MSG(verkle_flat_begin_block(vf, 0), "begin genesis block");

    for (int i = 0; i < num_accounts; i++) {
        uint8_t stem[31];
        rng_fill(rng, stem, 31);

        /* Nonce key: stem + suffix 0 */
        uint8_t key[32], val[32];
        memcpy(key, stem, 31);
        key[31] = 0;
        memset(val, 0, 32);
        val[0] = 1;  /* nonce = 1 */
        dual_set(vf, vt, key, val);

        /* Balance key: stem + suffix 1 */
        key[31] = 1;
        rng_fill(rng, val, 32);
        val[31] &= 0x0F;  /* cap balance to reasonable range */
        dual_set(vf, vt, key, val);

        if ((i + 1) % 5000 == 0)
            printf("  %d/%d accounts...\n", i + 1, num_accounts);
    }

    ASSERT_MSG(verkle_flat_commit_block(vf), "commit genesis");

    /* Flush tree to commit store so subsequent blocks have existing leaf data */
    vcs_flush_tree(vf->commit_store, vt);

    double elapsed = now_sec() - t0;
    printf("  Done: %d accounts, %d tracked keys, %.2fs\n",
           num_accounts, tracked_count, elapsed);

    compare_roots(vf, vt, "post-genesis");
    printf("  Root hash: MATCH\n\n");
}

/* =========================================================================
 * Phase 2: Block processing — mixed operations
 * ========================================================================= */

static void phase2_blocks(verkle_flat_t *vf, verkle_tree_t *vt,
                           int num_blocks, rng_t *rng)
{
    printf("Phase 2: Block processing — %d blocks, %d ops/block\n",
           num_blocks, OPS_PER_BLOCK);
    double t0 = now_sec();

    for (int b = 1; b <= num_blocks; b++) {
        ASSERT_MSG(verkle_flat_begin_block(vf, (uint64_t)b), "begin block %d", b);

        for (int op = 0; op < OPS_PER_BLOCK; op++) {
            uint64_t r = rng_next(rng);
            int pct = (int)(r % 100);

            if (pct < 60 && tracked_count > 0) {
                /* 60%: update existing key */
                int idx = (int)(rng_next(rng) % (uint64_t)tracked_count);
                uint8_t val[32];
                rng_fill(rng, val, 32);
                dual_set(vf, vt, tracked[idx].key, val);

            } else if (pct < 85) {
                /* 25%: new account (2 keys) */
                uint8_t stem[31];
                rng_fill(rng, stem, 31);

                uint8_t key[32], val[32];
                memcpy(key, stem, 31);
                key[31] = 0;
                memset(val, 0, 32);
                val[0] = 1;
                dual_set(vf, vt, key, val);

                key[31] = 1;
                rng_fill(rng, val, 32);
                dual_set(vf, vt, key, val);

            } else {
                /* 15%: storage write on existing stem */
                if (tracked_count > 0) {
                    int idx = (int)(rng_next(rng) % (uint64_t)tracked_count);
                    uint8_t key[32], val[32];
                    memcpy(key, tracked[idx].key, 31);  /* reuse stem */
                    key[31] = (uint8_t)(rng_next(rng) & 0xFF);
                    rng_fill(rng, val, 32);
                    dual_set(vf, vt, key, val);
                }
            }
        }

        ASSERT_MSG(verkle_flat_commit_block(vf), "commit block %d", b);

        /* Flush tree commitments so next block's incremental updates work */
        vcs_flush_tree(vf->commit_store, vt);

        /* Compare roots every block */
        char ctx[64];
        snprintf(ctx, sizeof(ctx), "block %d", b);
        compare_roots(vf, vt, ctx);

        if (b % 20 == 0) {
            double elapsed = now_sec() - t0;
            printf("  block %3d | keys %6d | %.1fs\n",
                   b, tracked_count, elapsed);
        }
    }

    double elapsed = now_sec() - t0;
    printf("  Done: %d blocks, %d keys, %.2fs (%.1f blocks/s)\n\n",
           num_blocks, tracked_count, elapsed, num_blocks / elapsed);
}

/* =========================================================================
 * Phase 3: Revert stress — commit then revert alternating blocks
 * ========================================================================= */

static void phase3_revert(verkle_flat_t *vf, verkle_tree_t *vt __attribute__((unused)),
                           rng_t *rng)
{
    printf("Phase 3: Revert stress — %d cycles\n", REVERT_BLOCKS);
    double t0 = now_sec();

    /* Save current root */
    uint8_t stable_root[32];
    verkle_flat_root_hash(vf, stable_root);

    for (int i = 0; i < REVERT_BLOCKS; i++) {
        uint64_t block_num = 10000 + (uint64_t)i;

        /* Begin + make changes */
        ASSERT_MSG(verkle_flat_begin_block(vf, block_num),
                   "begin revert block %d", i);

        for (int op = 0; op < 50; op++) {
            if (tracked_count > 0) {
                int idx = (int)(rng_next(rng) % (uint64_t)tracked_count);
                uint8_t val[32];
                rng_fill(rng, val, 32);
                /* Only set in flat, NOT in tree (we'll revert) */
                ASSERT_MSG(verkle_flat_set(vf, tracked[idx].key, val),
                           "flat_set in revert block");
            }
        }

        ASSERT_MSG(verkle_flat_commit_block(vf), "commit revert block %d", i);

        /* Root should have changed */
        uint8_t new_root[32];
        verkle_flat_root_hash(vf, new_root);

        /* Revert */
        ASSERT_MSG(verkle_flat_revert_block(vf), "revert block %d", i);

        /* Root should be restored */
        uint8_t reverted_root[32];
        verkle_flat_root_hash(vf, reverted_root);
        CHECK(memcmp(reverted_root, stable_root, 32) == 0,
              "root mismatch after revert cycle %d", i);
    }

    double elapsed = now_sec() - t0;
    printf("  Done: %d revert cycles, all roots restored, %.2fs\n\n",
           REVERT_BLOCKS, elapsed);
}

/* =========================================================================
 * Phase 4: Persistence — sync, close, reopen, verify
 * ========================================================================= */

static void phase4_persistence(verkle_flat_t **vf_ptr, verkle_tree_t *vt __attribute__((unused))) {
    printf("Phase 4: Persistence — sync, close, reopen\n");
    double t0 = now_sec();

    verkle_flat_t *vf = *vf_ptr;

    /* Save root before close */
    uint8_t root_before[32];
    verkle_flat_root_hash(vf, root_before);

    /* Sync + close */
    verkle_flat_sync(vf);
    verkle_flat_destroy(vf);

    /* Reopen */
    vf = verkle_flat_open(VAL_DIR, COMM_DIR);
    ASSERT_MSG(vf, "reopen failed");
    *vf_ptr = vf;

    /* Verify root */
    uint8_t root_after[32];
    verkle_flat_root_hash(vf, root_after);
    CHECK(memcmp(root_before, root_after, 32) == 0,
          "root mismatch after reopen");

    /* Spot-check random values */
    int spot_count = SPOT_CHECK_COUNT;
    if (spot_count > tracked_count) spot_count = tracked_count;

    rng_t spot_rng = { .state = 0x5907C43C4ULL };
    int mismatches = 0;
    for (int i = 0; i < spot_count; i++) {
        int idx = (int)(rng_next(&spot_rng) % (uint64_t)tracked_count);
        uint8_t got[32];
        if (!verkle_flat_get(vf, tracked[idx].key, got)) {
            fprintf(stderr, "  spot check %d: key not found\n", i);
            mismatches++;
        } else if (memcmp(got, tracked[idx].value, 32) != 0) {
            fprintf(stderr, "  spot check %d: value mismatch\n", i);
            mismatches++;
        }
    }
    CHECK(mismatches == 0, "%d spot check mismatches", mismatches);

    double elapsed = now_sec() - t0;
    printf("  Done: %d spot checks OK, %.2fs\n\n", spot_count, elapsed);
}

/* =========================================================================
 * Phase 5: Final scan — verify all tracked keys
 * ========================================================================= */

static void phase5_final_scan(const verkle_flat_t *vf, const verkle_tree_t *vt) {
    printf("Phase 5: Final scan — %d tracked keys\n", tracked_count);
    double t0 = now_sec();

    int mismatches = 0;
    int missing_flat = 0;
    int missing_tree = 0;

    for (int i = 0; i < tracked_count; i++) {
        uint8_t flat_val[32], tree_val[32];
        bool flat_ok = verkle_flat_get(vf, tracked[i].key, flat_val);
        bool tree_ok = verkle_get(vt, tracked[i].key, tree_val);

        if (!flat_ok) { missing_flat++; continue; }
        if (!tree_ok) { missing_tree++; continue; }

        if (memcmp(flat_val, tracked[i].value, 32) != 0) mismatches++;
        if (memcmp(tree_val, tracked[i].value, 32) != 0) mismatches++;
    }

    CHECK(missing_flat == 0, "%d keys missing from flat store", missing_flat);
    CHECK(missing_tree == 0, "%d keys missing from tree", missing_tree);
    CHECK(mismatches == 0, "%d value mismatches", mismatches);

    compare_roots(vf, vt, "final");

    double elapsed = now_sec() - t0;
    printf("  Done: all %d keys verified, %.2fs\n\n", tracked_count, elapsed);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    int accounts_k = 10;
    int num_blocks = 200;

    if (argc > 1) accounts_k = atoi(argv[1]);
    if (argc > 2) num_blocks  = atoi(argv[2]);

    int num_accounts = accounts_k * 1000;

    printf("=== Verkle Flat Stress Test ===\n");
    printf("Accounts: %dK, Blocks: %d, Ops/block: %d\n\n",
           accounts_k, num_blocks, OPS_PER_BLOCK);

    cleanup();
    pedersen_init();

    rng_t rng = { .state = MASTER_SEED };

    verkle_tree_t *vt = verkle_create();
    verkle_flat_t *vf = verkle_flat_create(VAL_DIR, COMM_DIR);
    ASSERT_MSG(vf, "flat create failed");

    double t_start = now_sec();

    phase1_genesis(vf, vt, num_accounts, &rng);
    phase2_blocks(vf, vt, num_blocks, &rng);
    phase3_revert(vf, vt, &rng);
    phase4_persistence(&vf, vt);
    phase5_final_scan(vf, vt);

    double t_total = now_sec() - t_start;

    printf("=== PASSED: %d checks, %.1fs total ===\n", checks_passed, t_total);

    verkle_flat_destroy(vf);
    verkle_destroy(vt);
    free(tracked);
    cleanup();

    return 0;
}
