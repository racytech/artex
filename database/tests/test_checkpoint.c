#include "../include/data_layer.h"
#include "../include/checkpoint.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

// ============================================================================
// Test config
// ============================================================================

#define KEY_SIZE   32
#define VALUE_SIZE 4
#define STATE_PATH "/tmp/test_ckpt_state.dat"
#define CODE_PATH  "/tmp/test_ckpt_code.dat"
#define INDEX_PATH "/tmp/test_ckpt_index.dat"

// ============================================================================
// Helpers
// ============================================================================

static void make_key(uint8_t key[KEY_SIZE], uint32_t i) {
    memset(key, 0, KEY_SIZE);
    // Store i in last 4 bytes (big-endian for sorted order)
    key[KEY_SIZE - 4] = (uint8_t)(i >> 24);
    key[KEY_SIZE - 3] = (uint8_t)(i >> 16);
    key[KEY_SIZE - 2] = (uint8_t)(i >> 8);
    key[KEY_SIZE - 1] = (uint8_t)(i);
}

static void make_value(uint8_t *buf, uint16_t *len, uint32_t i) {
    // Simple 4-byte value: store i as bytes
    *len = 4;
    buf[0] = (uint8_t)(i & 0xFF);
    buf[1] = (uint8_t)((i >> 8) & 0xFF);
    buf[2] = (uint8_t)((i >> 16) & 0xFF);
    buf[3] = (uint8_t)((i >> 24) & 0xFF);
}

static void cleanup_files(void) {
    unlink(STATE_PATH);
    unlink(CODE_PATH);
    unlink(INDEX_PATH);
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "%s.tmp", INDEX_PATH);
    unlink(tmp);
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// ============================================================================
// Phase 1: Checkpoint + Reload (100K keys)
// ============================================================================

static int test_checkpoint_reload(void) {
    printf("\n=== Phase 1: Checkpoint + Reload (100K keys) ===\n");
    cleanup_files();

    const uint32_t N = 100000;

    // Create and populate
    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    double t0 = now_sec();
    const uint32_t BATCH = 5000;
    for (uint32_t b = 0; b < N; b += BATCH) {
        uint32_t end = b + BATCH;
        if (end > N) end = N;
        for (uint32_t i = b; i < end; i++) {
            uint8_t key[KEY_SIZE];
            uint8_t val[4];
            uint16_t vlen;
            make_key(key, i);
            make_value(val, &vlen, i);
            dl_put(dl, key, val, vlen);
        }
        dl_merge(dl);
    }
    double t1 = now_sec();
    printf("  Insert + merge: %.3f s (%u batches)\n",
           t1 - t0, (N + BATCH - 1) / BATCH);

    // Checkpoint
    t0 = now_sec();
    if (!dl_checkpoint(dl, INDEX_PATH, 42)) {
        printf("FAIL: dl_checkpoint\n");
        dl_destroy(dl);
        return 1;
    }
    t1 = now_sec();
    printf("  Checkpoint: %.3f s\n", t1 - t0);

    dl_stats_t s1 = dl_stats(dl);
    dl_destroy(dl);

    // Reload
    uint64_t block_num = 0;
    t0 = now_sec();
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    t1 = now_sec();
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }
    printf("  Reload: %.3f s\n", t1 - t0);

    if (block_num != 42) {
        printf("FAIL: block_number = %lu, expected 42\n", block_num);
        dl_destroy(dl);
        return 1;
    }

    dl_stats_t s2 = dl_stats(dl);
    if (s2.index_keys != s1.index_keys) {
        printf("FAIL: index_keys = %lu, expected %lu\n",
               s2.index_keys, s1.index_keys);
        dl_destroy(dl);
        return 1;
    }

    // Verify all keys readable
    uint32_t verified = 0;
    for (uint32_t i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t expected[4];
        uint16_t expected_len;
        make_key(key, i);
        make_value(expected, &expected_len, i);

        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            printf("FAIL: key %u not found after reload\n", i);
            dl_destroy(dl);
            return 1;
        }
        if (got_len != expected_len || memcmp(got, expected, got_len) != 0) {
            printf("FAIL: key %u value mismatch\n", i);
            dl_destroy(dl);
            return 1;
        }
        verified++;
    }

    printf("  Verified: %u/%u keys correct\n", verified, N);
    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 2: Code Entries Survive
// ============================================================================

static int test_code_entries_survive(void) {
    printf("\n=== Phase 2: Code Entries Survive ===\n");
    cleanup_files();

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Store code entries
    const uint32_t NUM_CODES = 100;
    for (uint32_t i = 0; i < NUM_CODES; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, 1000000 + i);  // different key space from state

        // Create variable-length bytecode
        uint32_t code_len = 32 + (i % 1000);
        uint8_t *code = malloc(code_len);
        memset(code, (uint8_t)(i & 0xFF), code_len);
        code[0] = (uint8_t)(i & 0xFF);
        code[1] = (uint8_t)((i >> 8) & 0xFF);

        if (!dl_put_code(dl, key, code, code_len)) {
            printf("FAIL: dl_put_code %u\n", i);
            free(code);
            dl_destroy(dl);
            return 1;
        }
        free(code);
    }

    // Also store some state
    for (uint32_t i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    dl_stats_t s1 = dl_stats(dl);
    printf("  Before: index=%lu, code_count=%u\n",
           s1.index_keys, s1.code_count);

    // Checkpoint + destroy + reload
    if (!dl_checkpoint(dl, INDEX_PATH, 99)) {
        printf("FAIL: dl_checkpoint\n");
        dl_destroy(dl);
        return 1;
    }
    dl_destroy(dl);

    uint64_t block_num = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }

    dl_stats_t s2 = dl_stats(dl);
    printf("  After:  index=%lu, code_count=%u\n",
           s2.index_keys, s2.code_count);

    if (s2.code_count != s1.code_count) {
        printf("FAIL: code_count = %u, expected %u\n",
               s2.code_count, s1.code_count);
        dl_destroy(dl);
        return 1;
    }

    // Verify code entries
    for (uint32_t i = 0; i < NUM_CODES; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, 1000000 + i);

        uint32_t code_len = 32 + (i % 1000);
        uint8_t *expected = malloc(code_len);
        memset(expected, (uint8_t)(i & 0xFF), code_len);
        expected[0] = (uint8_t)(i & 0xFF);
        expected[1] = (uint8_t)((i >> 8) & 0xFF);

        uint8_t *got = malloc(code_len);
        uint32_t got_len = 0;
        if (!dl_get_code(dl, key, got, &got_len)) {
            printf("FAIL: code %u not found after reload\n", i);
            free(expected);
            free(got);
            dl_destroy(dl);
            return 1;
        }
        if (got_len != code_len || memcmp(got, expected, code_len) != 0) {
            printf("FAIL: code %u data mismatch\n", i);
            free(expected);
            free(got);
            dl_destroy(dl);
            return 1;
        }
        free(expected);
        free(got);
    }

    // Verify state entries
    for (uint32_t i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t expected[4];
        uint16_t expected_len;
        make_key(key, i);
        make_value(expected, &expected_len, i);

        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            printf("FAIL: state key %u not found\n", i);
            dl_destroy(dl);
            return 1;
        }
    }

    printf("  Verified: %u code entries + 1000 state entries\n", NUM_CODES);
    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 3: Free List Survives
// ============================================================================

static int test_free_list_survives(void) {
    printf("\n=== Phase 3: Free List Survives ===\n");
    cleanup_files();

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Insert 10K keys
    const uint32_t N = 10000;
    for (uint32_t i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    // Delete 5000 keys → creates free slots
    for (uint32_t i = 0; i < 5000; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        dl_delete(dl, key);
    }
    dl_merge(dl);

    dl_stats_t s1 = dl_stats(dl);
    printf("  Before: index=%lu, free_slots=%u\n",
           s1.index_keys, s1.free_slots);

    if (s1.free_slots != 5000) {
        printf("FAIL: expected 5000 free slots, got %u\n", s1.free_slots);
        dl_destroy(dl);
        return 1;
    }

    // Checkpoint + destroy + reload
    if (!dl_checkpoint(dl, INDEX_PATH, 50)) {
        printf("FAIL: dl_checkpoint\n");
        dl_destroy(dl);
        return 1;
    }
    dl_destroy(dl);

    uint64_t block_num = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }

    dl_stats_t s2 = dl_stats(dl);
    printf("  After:  index=%lu, free_slots=%u\n",
           s2.index_keys, s2.free_slots);

    if (s2.free_slots != s1.free_slots) {
        printf("FAIL: free_slots = %u, expected %u\n",
               s2.free_slots, s1.free_slots);
        dl_destroy(dl);
        return 1;
    }

    if (s2.index_keys != s1.index_keys) {
        printf("FAIL: index_keys = %lu, expected %lu\n",
               s2.index_keys, s1.index_keys);
        dl_destroy(dl);
        return 1;
    }

    // Insert new keys → should reuse free slots
    for (uint32_t i = 0; i < 3000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, N + i);  // new key range
        make_value(val, &vlen, N + i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    dl_stats_t s3 = dl_stats(dl);
    printf("  After reuse: index=%lu, free_slots=%u\n",
           s3.index_keys, s3.free_slots);

    // Should have 5000 - 3000 = 2000 free slots remaining
    if (s3.free_slots != 2000) {
        printf("FAIL: expected 2000 free slots after reuse, got %u\n",
               s3.free_slots);
        dl_destroy(dl);
        return 1;
    }

    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 4: Multiple Checkpoints
// ============================================================================

static int test_multiple_checkpoints(void) {
    printf("\n=== Phase 4: Multiple Checkpoints ===\n");
    cleanup_files();

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Block 100: insert 50K keys
    for (uint32_t i = 0; i < 50000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    if (!dl_checkpoint(dl, INDEX_PATH, 100)) {
        printf("FAIL: checkpoint at block 100\n");
        dl_destroy(dl);
        return 1;
    }
    printf("  Checkpoint at block 100: 50K keys\n");

    // Block 200: insert 50K more
    for (uint32_t i = 50000; i < 100000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    if (!dl_checkpoint(dl, INDEX_PATH, 200)) {
        printf("FAIL: checkpoint at block 200\n");
        dl_destroy(dl);
        return 1;
    }
    printf("  Checkpoint at block 200: 100K keys\n");

    dl_destroy(dl);

    // Reload from latest (block 200)
    uint64_t block_num = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }

    if (block_num != 200) {
        printf("FAIL: block_number = %lu, expected 200\n", block_num);
        dl_destroy(dl);
        return 1;
    }

    dl_stats_t s = dl_stats(dl);
    if (s.index_keys != 100000) {
        printf("FAIL: index_keys = %lu, expected 100000\n", s.index_keys);
        dl_destroy(dl);
        return 1;
    }

    // Verify all 100K keys
    for (uint32_t i = 0; i < 100000; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            printf("FAIL: key %u not found\n", i);
            dl_destroy(dl);
            return 1;
        }
    }

    printf("  Verified: 100K keys from block 200 checkpoint\n");
    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 5: Crash Simulation
// ============================================================================

static int test_crash_simulation(void) {
    printf("\n=== Phase 5: Crash Simulation ===\n");
    cleanup_files();

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Block 100: insert 10K keys + checkpoint
    const uint32_t N1 = 10000;
    for (uint32_t i = 0; i < N1; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    if (!dl_checkpoint(dl, INDEX_PATH, 100)) {
        printf("FAIL: checkpoint at block 100\n");
        dl_destroy(dl);
        return 1;
    }
    printf("  Checkpoint at block 100: %u keys\n", N1);

    // Block 101-110: insert 5K more + merge (NO checkpoint)
    const uint32_t N2 = 5000;
    for (uint32_t i = N1; i < N1 + N2; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);
    printf("  Post-checkpoint merges: %u more keys (no checkpoint)\n", N2);

    // "Crash": destroy without checkpoint
    dl_destroy(dl);
    printf("  Simulated crash\n");

    // Recovery: open from block 100 checkpoint
    uint64_t block_num = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }

    if (block_num != 100) {
        printf("FAIL: block_number = %lu, expected 100\n", block_num);
        dl_destroy(dl);
        return 1;
    }

    dl_stats_t s = dl_stats(dl);
    printf("  Recovered: index_keys=%lu (expected %u)\n",
           s.index_keys, N1);

    if (s.index_keys != N1) {
        printf("FAIL: index_keys = %lu, expected %u\n", s.index_keys, N1);
        dl_destroy(dl);
        return 1;
    }

    // Block 100 keys should exist
    for (uint32_t i = 0; i < N1; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            printf("FAIL: block-100 key %u not found\n", i);
            dl_destroy(dl);
            return 1;
        }
    }

    // Post-checkpoint keys should NOT exist in index
    // (they were in state.dat but index doesn't know about them)
    uint32_t missing = 0;
    for (uint32_t i = N1; i < N1 + N2; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            missing++;
        }
    }

    printf("  Post-checkpoint keys missing: %u/%u (expected all)\n",
           missing, N2);
    if (missing != N2) {
        printf("FAIL: expected %u missing, got %u\n", N2, missing);
        dl_destroy(dl);
        return 1;
    }

    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 6: CRC Integrity
// ============================================================================

static int test_crc_integrity(void) {
    printf("\n=== Phase 6: CRC Integrity ===\n");
    cleanup_files();

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Insert some data
    for (uint32_t i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        uint8_t val[4];
        uint16_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        dl_put(dl, key, val, vlen);
    }
    dl_merge(dl);

    if (!dl_checkpoint(dl, INDEX_PATH, 10)) {
        printf("FAIL: dl_checkpoint\n");
        dl_destroy(dl);
        return 1;
    }
    dl_destroy(dl);

    // Verify good checkpoint loads
    uint64_t block_num = 0;
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (!dl) { printf("FAIL: good checkpoint failed to load\n"); return 1; }
    dl_destroy(dl);
    printf("  Good checkpoint loads: OK\n");

    // Corrupt one byte in the data section (after header)
    int fd = open(INDEX_PATH, O_RDWR);
    if (fd < 0) { printf("FAIL: open index.dat\n"); return 1; }

    uint8_t byte;
    // Corrupt at offset 4096 + 10 (inside first entry)
    if (pread(fd, &byte, 1, 4096 + 10) != 1) {
        printf("FAIL: pread corruption byte\n");
        close(fd);
        return 1;
    }
    byte ^= 0xFF;  // flip all bits
    if (pwrite(fd, &byte, 1, 4096 + 10) != 1) {
        printf("FAIL: pwrite corruption byte\n");
        close(fd);
        return 1;
    }
    close(fd);

    // Attempt to load corrupted checkpoint — should fail
    // Need fresh state_store and code_store for the attempt
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    if (dl) {
        printf("FAIL: corrupted checkpoint should not load\n");
        dl_destroy(dl);
        return 1;
    }
    printf("  Corrupted checkpoint rejected: OK\n");

    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Phase 7: Scale (1M keys)
// ============================================================================

static int test_scale_1m(void) {
    printf("\n=== Phase 7: Scale (1M keys) ===\n");
    cleanup_files();

    const uint32_t N = 1000000;

    data_layer_t *dl = dl_create(STATE_PATH, CODE_PATH, KEY_SIZE, VALUE_SIZE);
    if (!dl) { printf("FAIL: dl_create\n"); return 1; }

    // Insert 1M keys in batches of 5K (like real blocks)
    double t0 = now_sec();
    const uint32_t BATCH = 5000;
    for (uint32_t b = 0; b < N; b += BATCH) {
        uint32_t end = b + BATCH;
        if (end > N) end = N;
        for (uint32_t i = b; i < end; i++) {
            uint8_t key[KEY_SIZE];
            uint8_t val[4];
            uint16_t vlen;
            make_key(key, i);
            make_value(val, &vlen, i);
            dl_put(dl, key, val, vlen);
        }
        dl_merge(dl);
    }
    double t1 = now_sec();
    printf("  Insert + merge: %.3f s (%.0f keys/s)\n",
           t1 - t0, N / (t1 - t0));

    // Checkpoint
    t0 = now_sec();
    if (!dl_checkpoint(dl, INDEX_PATH, 1000)) {
        printf("FAIL: dl_checkpoint\n");
        dl_destroy(dl);
        return 1;
    }
    t1 = now_sec();
    printf("  Checkpoint: %.3f s\n", t1 - t0);

    dl_stats_t s1 = dl_stats(dl);
    dl_destroy(dl);

    // Reload
    uint64_t block_num = 0;
    t0 = now_sec();
    dl = dl_open(STATE_PATH, CODE_PATH, INDEX_PATH,
                 KEY_SIZE, VALUE_SIZE, &block_num);
    t1 = now_sec();
    if (!dl) { printf("FAIL: dl_open\n"); return 1; }
    printf("  Reload: %.3f s\n", t1 - t0);

    dl_stats_t s2 = dl_stats(dl);
    if (s2.index_keys != s1.index_keys) {
        printf("FAIL: index_keys = %lu, expected %lu\n",
               s2.index_keys, s1.index_keys);
        dl_destroy(dl);
        return 1;
    }

    // Spot-check 10K random keys
    uint32_t verified = 0;
    for (uint32_t i = 0; i < N; i += N / 10000) {
        uint8_t key[KEY_SIZE];
        uint8_t expected[4];
        uint16_t expected_len;
        make_key(key, i);
        make_value(expected, &expected_len, i);

        uint8_t got[62];
        uint16_t got_len = 0;
        if (!dl_get(dl, key, got, &got_len)) {
            printf("FAIL: key %u not found\n", i);
            dl_destroy(dl);
            return 1;
        }
        if (got_len != expected_len || memcmp(got, expected, got_len) != 0) {
            printf("FAIL: key %u value mismatch\n", i);
            dl_destroy(dl);
            return 1;
        }
        verified++;
    }

    printf("  Spot-checked: %u keys correct\n", verified);
    printf("  index.dat covers %lu keys\n", s2.index_keys);
    dl_destroy(dl);
    printf("  PASS\n");
    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== Checkpoint & Recovery Test Suite ===\n");

    int failures = 0;
    failures += test_checkpoint_reload();
    failures += test_code_entries_survive();
    failures += test_free_list_survives();
    failures += test_multiple_checkpoints();
    failures += test_crash_simulation();
    failures += test_crc_integrity();
    failures += test_scale_1m();

    cleanup_files();

    printf("\n========================================\n");
    if (failures == 0) {
        printf("ALL PHASES PASSED\n");
    } else {
        printf("FAILURES: %d\n", failures);
    }
    printf("========================================\n");

    return failures;
}
