/*
 * Block Store — basic correctness test.
 *
 * Tests: create, sequential put, get_hash, get_parent, fill_ring, truncate,
 * close/reopen persistence.
 */

#include "block_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PATH "/tmp/test_block_store.dat"
#define NUM_BLOCKS 500

static void make_hash(uint64_t bn, uint8_t seed, uint8_t out[32]) {
    memset(out, 0, 32);
    out[0] = (uint8_t)(bn >> 24);
    out[1] = (uint8_t)(bn >> 16);
    out[2] = (uint8_t)(bn >> 8);
    out[3] = (uint8_t)(bn);
    out[4] = seed;
    for (int i = 5; i < 32; i++)
        out[i] = (uint8_t)(bn * 7 + i * seed);
}

static int test_basic(void) {
    printf("Test 1: create, put %d blocks, read back...\n", NUM_BLOCKS);
    unlink(PATH);

    block_store_t *bs = block_store_create(PATH);
    if (!bs) { printf("  FAIL: create\n"); return 1; }

    /* Insert blocks 0..NUM_BLOCKS-1 */
    for (uint64_t i = 0; i < NUM_BLOCKS; i++) {
        uint8_t bh[32], ph[32];
        make_hash(i, 0xAA, bh);
        make_hash(i, 0xBB, ph);
        if (!block_store_put(bs, i, bh, ph, 1600000000 + i * 12)) {
            printf("  FAIL: put block %lu\n", i);
            block_store_destroy(bs);
            return 1;
        }
    }

    if (block_store_count(bs) != NUM_BLOCKS) {
        printf("  FAIL: count %lu != %d\n", block_store_count(bs), NUM_BLOCKS);
        block_store_destroy(bs);
        return 1;
    }

    /* Verify every block */
    int mismatches = 0;
    for (uint64_t i = 0; i < NUM_BLOCKS; i++) {
        uint8_t got[32], expected[32];
        make_hash(i, 0xAA, expected);
        if (!block_store_get_hash(bs, i, got) ||
            memcmp(got, expected, 32) != 0)
            mismatches++;

        make_hash(i, 0xBB, expected);
        if (!block_store_get_parent(bs, i, got) ||
            memcmp(got, expected, 32) != 0)
            mismatches++;

        uint64_t ts = block_store_get_timestamp(bs, i);
        if (ts != 1600000000 + i * 12)
            mismatches++;
    }

    block_store_destroy(bs);

    if (mismatches > 0) {
        printf("  FAIL: %d mismatches\n", mismatches);
        return 1;
    }
    printf("  PASS\n");
    return 0;
}

static int test_reopen(void) {
    printf("Test 2: close and reopen, verify persistence...\n");

    block_store_t *bs = block_store_open(PATH);
    if (!bs) { printf("  FAIL: reopen\n"); return 1; }

    if (block_store_count(bs) != NUM_BLOCKS) {
        printf("  FAIL: count %lu != %d\n", block_store_count(bs), NUM_BLOCKS);
        block_store_destroy(bs);
        return 1;
    }

    /* Spot check a few blocks */
    int mismatches = 0;
    for (uint64_t i = 0; i < NUM_BLOCKS; i += 50) {
        uint8_t got[32], expected[32];
        make_hash(i, 0xAA, expected);
        if (!block_store_get_hash(bs, i, got) ||
            memcmp(got, expected, 32) != 0)
            mismatches++;
    }

    block_store_destroy(bs);

    if (mismatches > 0) {
        printf("  FAIL: %d mismatches\n", mismatches);
        return 1;
    }
    printf("  PASS\n");
    return 0;
}

static int test_fill_ring(void) {
    printf("Test 3: fill_ring from head=%d...\n", NUM_BLOCKS - 1);

    block_store_t *bs = block_store_open(PATH);
    if (!bs) { printf("  FAIL: reopen\n"); return 1; }

    uint8_t ring[256][32];
    uint32_t filled = block_store_fill_ring(bs, NUM_BLOCKS - 1, ring);

    if (filled != 256) {
        printf("  FAIL: filled=%u (expected 256)\n", filled);
        block_store_destroy(bs);
        return 1;
    }

    /* Verify a few entries */
    int mismatches = 0;
    for (uint64_t bn = NUM_BLOCKS - 256; bn < (uint64_t)NUM_BLOCKS; bn++) {
        uint8_t expected[32];
        make_hash(bn, 0xAA, expected);
        if (memcmp(ring[bn % 256], expected, 32) != 0)
            mismatches++;
    }

    block_store_destroy(bs);

    if (mismatches > 0) {
        printf("  FAIL: %d ring mismatches\n", mismatches);
        return 1;
    }
    printf("  PASS\n");
    return 0;
}

static int test_truncate(void) {
    printf("Test 4: truncate to block 300, verify...\n");

    block_store_t *bs = block_store_open(PATH);
    if (!bs) { printf("  FAIL: reopen\n"); return 1; }

    if (!block_store_truncate(bs, 300)) {
        printf("  FAIL: truncate\n");
        block_store_destroy(bs);
        return 1;
    }

    if (block_store_count(bs) != 301) {
        printf("  FAIL: count=%lu (expected 301)\n", block_store_count(bs));
        block_store_destroy(bs);
        return 1;
    }

    if (block_store_highest(bs) != 300) {
        printf("  FAIL: highest=%lu (expected 300)\n", block_store_highest(bs));
        block_store_destroy(bs);
        return 1;
    }

    /* Block 300 should still exist */
    uint8_t got[32], expected[32];
    make_hash(300, 0xAA, expected);
    if (!block_store_get_hash(bs, 300, got) ||
        memcmp(got, expected, 32) != 0) {
        printf("  FAIL: block 300 mismatch\n");
        block_store_destroy(bs);
        return 1;
    }

    /* Block 301 should not exist */
    if (block_store_get_hash(bs, 301, got)) {
        printf("  FAIL: block 301 should not exist\n");
        block_store_destroy(bs);
        return 1;
    }

    block_store_destroy(bs);
    printf("  PASS\n");
    return 0;
}

int main(void) {
    printf("Block Store Tests\n");
    printf("==================\n");

    int failures = 0;
    failures += test_basic();
    failures += test_reopen();
    failures += test_fill_ring();
    failures += test_truncate();

    unlink(PATH);

    printf("\n%s (%d test%s failed)\n",
           failures == 0 ? "ALL PASSED" : "FAILED",
           failures, failures == 1 ? "" : "s");
    return failures > 0 ? 1 : 0;
}
