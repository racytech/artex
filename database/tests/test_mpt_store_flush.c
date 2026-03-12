/*
 * MPT Store Flush Roundtrip Test
 *
 * Tests that data survives flush → close → reopen cycles.
 * Exercises both sync flush (mpt_store_flush) and background flush
 * (mpt_store_flush_bg + mpt_store_flush_wait) paths.
 *
 * Verifies:
 *   1. Root hash is preserved across close/reopen
 *   2. All values are retrievable via mpt_store_get after reopen
 *   3. Background flush produces identical results to sync flush
 *   4. Multiple batch+flush rounds maintain consistency
 */

#include "mpt_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

#define STORE_PATH_SYNC "/tmp/test_mpt_flush_sync"
#define STORE_PATH_BG   "/tmp/test_mpt_flush_bg"
#define STORE_PATH_MULTI "/tmp/test_mpt_flush_multi"

static void cleanup_path(const char *path) {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s.idx", path);
    unlink(buf);
    snprintf(buf, sizeof(buf), "%s.dat", path);
    unlink(buf);
}

static void cleanup_all(void) {
    cleanup_path(STORE_PATH_SYNC);
    cleanup_path(STORE_PATH_BG);
    cleanup_path(STORE_PATH_MULTI);
}

static void print_hash(const uint8_t h[32]) {
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
}

/* Generate a deterministic 32-byte key from an index */
static void make_key(uint32_t idx, uint8_t key[32]) {
    memset(key, 0, 32);
    key[0] = (uint8_t)(idx >> 24);
    key[1] = (uint8_t)(idx >> 16);
    key[2] = (uint8_t)(idx >> 8);
    key[3] = (uint8_t)(idx);
    /* Mix bytes to spread across trie */
    for (int i = 4; i < 32; i++)
        key[i] = (uint8_t)(idx * 31 + i * 7);
}

/* Generate a deterministic value from an index */
static void make_value(uint32_t idx, uint8_t *val, size_t *len) {
    /* RLP-encode a small integer: 0x80 + length prefix + bytes */
    size_t n = 4 + (idx % 60);  /* variable length 4..63 */
    *len = n;
    for (size_t i = 0; i < n; i++)
        val[i] = (uint8_t)(idx * 13 + i * 3);
}

#define NUM_KEYS 2000
#define VAL_BUF  128

/* =========================================================================
 * Test 1: Sync flush → close → reopen → verify
 * ========================================================================= */

static int test_sync_flush(void) {
    printf("Test 1: sync flush roundtrip (%d keys)...\n", NUM_KEYS);
    cleanup_path(STORE_PATH_SYNC);

    /* Phase 1: create, insert, flush, close */
    mpt_store_t *ms = mpt_store_create(STORE_PATH_SYNC, NUM_KEYS + 1024);
    if (!ms) { printf("  FAIL: cannot create store\n"); return 1; }

    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32], val[VAL_BUF];
        size_t vlen;
        make_key(i, key);
        make_value(i, val, &vlen);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    uint8_t root_before[32];
    mpt_store_root(ms, root_before);

    mpt_store_flush(ms);    /* <-- exercises sync pwrite path */
    mpt_store_destroy(ms);

    /* Phase 2: reopen, verify root and all values */
    ms = mpt_store_open(STORE_PATH_SYNC);
    if (!ms) { printf("  FAIL: cannot reopen store\n"); return 1; }

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);

    if (memcmp(root_before, root_after, 32) != 0) {
        printf("  FAIL: root mismatch after reopen\n");
        printf("    before: "); print_hash(root_before); printf("\n");
        printf("    after:  "); print_hash(root_after); printf("\n");
        mpt_store_destroy(ms);
        return 1;
    }

    /* Verify every key is retrievable */
    int mismatches = 0;
    for (uint32_t i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32], expected_val[VAL_BUF], got_val[VAL_BUF];
        size_t expected_len;
        make_key(i, key);
        make_value(i, expected_val, &expected_len);

        uint32_t got_len = mpt_store_get(ms, key, got_val, VAL_BUF);
        if (got_len != expected_len ||
            memcmp(got_val, expected_val, expected_len) != 0) {
            if (mismatches < 3) {
                printf("  FAIL: key %u value mismatch (got_len=%u expected=%zu)\n",
                       i, got_len, expected_len);
            }
            mismatches++;
        }
    }

    mpt_store_destroy(ms);

    if (mismatches > 0) {
        printf("  FAIL: %d/%d values mismatched\n", mismatches, NUM_KEYS);
        return 1;
    }

    printf("  PASS (root + %d values verified)\n", NUM_KEYS);
    return 0;
}

/* =========================================================================
 * Test 2: Background flush → wait → close → reopen → verify
 * ========================================================================= */

static int test_bg_flush(void) {
    printf("Test 2: background flush roundtrip (%d keys)...\n", NUM_KEYS);
    cleanup_path(STORE_PATH_BG);

    /* Phase 1: create, insert, bg flush, wait, close */
    mpt_store_t *ms = mpt_store_create(STORE_PATH_BG, NUM_KEYS + 1024);
    if (!ms) { printf("  FAIL: cannot create store\n"); return 1; }

    mpt_store_begin_batch(ms);
    for (uint32_t i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32], val[VAL_BUF];
        size_t vlen;
        make_key(i, key);
        make_value(i, val, &vlen);
        mpt_store_update(ms, key, val, vlen);
    }
    mpt_store_commit_batch(ms);

    uint8_t root_before[32];
    mpt_store_root(ms, root_before);

    mpt_store_flush_bg(ms);   /* <-- exercises io_uring / bg thread path */
    mpt_store_flush_wait(ms);
    mpt_store_destroy(ms);

    /* Phase 2: reopen, verify root and all values */
    ms = mpt_store_open(STORE_PATH_BG);
    if (!ms) { printf("  FAIL: cannot reopen store\n"); return 1; }

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);

    if (memcmp(root_before, root_after, 32) != 0) {
        printf("  FAIL: root mismatch after reopen\n");
        printf("    before: "); print_hash(root_before); printf("\n");
        printf("    after:  "); print_hash(root_after); printf("\n");
        mpt_store_destroy(ms);
        return 1;
    }

    int mismatches = 0;
    for (uint32_t i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32], expected_val[VAL_BUF], got_val[VAL_BUF];
        size_t expected_len;
        make_key(i, key);
        make_value(i, expected_val, &expected_len);

        uint32_t got_len = mpt_store_get(ms, key, got_val, VAL_BUF);
        if (got_len != expected_len ||
            memcmp(got_val, expected_val, expected_len) != 0) {
            if (mismatches < 3) {
                printf("  FAIL: key %u value mismatch (got_len=%u expected=%zu)\n",
                       i, got_len, expected_len);
            }
            mismatches++;
        }
    }

    mpt_store_destroy(ms);

    if (mismatches > 0) {
        printf("  FAIL: %d/%d values mismatched\n", mismatches, NUM_KEYS);
        return 1;
    }

    printf("  PASS (root + %d values verified)\n", NUM_KEYS);
    return 0;
}

/* =========================================================================
 * Test 3: Multiple batch+flush rounds (simulates chain_replay checkpoint)
 * ========================================================================= */

#define NUM_ROUNDS  5
#define KEYS_PER_ROUND 500

static int test_multi_round_flush(void) {
    printf("Test 3: multi-round flush (%d rounds x %d keys)...\n",
           NUM_ROUNDS, KEYS_PER_ROUND);
    cleanup_path(STORE_PATH_MULTI);

    mpt_store_t *ms = mpt_store_create(STORE_PATH_MULTI,
                                        NUM_ROUNDS * KEYS_PER_ROUND + 4096);
    if (!ms) { printf("  FAIL: cannot create store\n"); return 1; }

    uint8_t roots[NUM_ROUNDS][32];
    uint32_t total_keys = 0;

    for (int r = 0; r < NUM_ROUNDS; r++) {
        mpt_store_begin_batch(ms);
        for (uint32_t i = 0; i < KEYS_PER_ROUND; i++) {
            uint8_t key[32], val[VAL_BUF];
            size_t vlen;
            make_key(total_keys + i, key);
            make_value(total_keys + i, val, &vlen);
            mpt_store_update(ms, key, val, vlen);
        }
        mpt_store_commit_batch(ms);
        total_keys += KEYS_PER_ROUND;

        mpt_store_root(ms, roots[r]);

        /* Alternate between sync and bg flush */
        if (r % 2 == 0) {
            mpt_store_flush(ms);
        } else {
            mpt_store_flush_bg(ms);
            mpt_store_flush_wait(ms);
        }
    }

    mpt_store_destroy(ms);

    /* Reopen and verify final root + all values */
    ms = mpt_store_open(STORE_PATH_MULTI);
    if (!ms) { printf("  FAIL: cannot reopen store\n"); return 1; }

    uint8_t root_after[32];
    mpt_store_root(ms, root_after);

    if (memcmp(root_after, roots[NUM_ROUNDS - 1], 32) != 0) {
        printf("  FAIL: final root mismatch\n");
        printf("    expected: "); print_hash(roots[NUM_ROUNDS - 1]); printf("\n");
        printf("    got:      "); print_hash(root_after); printf("\n");
        mpt_store_destroy(ms);
        return 1;
    }

    int mismatches = 0;
    for (uint32_t i = 0; i < total_keys; i++) {
        uint8_t key[32], expected_val[VAL_BUF], got_val[VAL_BUF];
        size_t expected_len;
        make_key(i, key);
        make_value(i, expected_val, &expected_len);

        uint32_t got_len = mpt_store_get(ms, key, got_val, VAL_BUF);
        if (got_len != expected_len ||
            memcmp(got_val, expected_val, expected_len) != 0) {
            if (mismatches < 3) {
                printf("  FAIL: key %u value mismatch (got_len=%u expected=%zu)\n",
                       i, got_len, expected_len);
            }
            mismatches++;
        }
    }

    mpt_store_destroy(ms);

    if (mismatches > 0) {
        printf("  FAIL: %d/%d values mismatched\n", mismatches, total_keys);
        return 1;
    }

    printf("  PASS (root + %d values verified across %d rounds)\n",
           total_keys, NUM_ROUNDS);
    return 0;
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("MPT Store Flush Roundtrip Tests\n");
    printf("================================\n");

    int failures = 0;
    failures += test_sync_flush();
    failures += test_bg_flush();
    failures += test_multi_round_flush();

    cleanup_all();

    printf("\n%s (%d test%s failed)\n",
           failures == 0 ? "ALL PASSED" : "FAILED",
           failures, failures == 1 ? "" : "s");
    return failures > 0 ? 1 : 0;
}
