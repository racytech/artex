/*
 * Code Store Tests
 *
 * Validates content-addressed code storage:
 *   1. Basic put/get/contains/get_size
 *   2. Deduplication (same code stored once)
 *   3. Multiple distinct codes
 *   4. Persistence (close + reopen)
 *   5. Large code (~24KB)
 *   6. Empty code (zero-length)
 *   7. Buffer too small (returns required size)
 *   8. Stress: many unique + duplicate codes
 */

#include "../include/code_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

#define BASE_PATH "/tmp/test_code_store"

static int test_num = 0;
static int pass_count = 0;

#define RUN(fn) do { \
    test_num++; \
    printf("  [%2d] %-50s", test_num, #fn); \
    fn(); \
    pass_count++; \
    printf("OK\n"); \
} while (0)

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("FAIL\n      %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        exit(1); \
    } \
} while (0)

static void cleanup(void) {
    unlink(BASE_PATH ".idx");
    unlink(BASE_PATH ".dat");
}

/* Generate a deterministic code hash from index */
static void make_hash(uint64_t i, uint8_t hash[32]) {
    memset(hash, 0, 32);
    memcpy(hash, &i, 8);
    uint64_t x = i * 0x517cc1b727220a95ULL;
    memcpy(hash + 8, &x, 8);
    x = x * 0x6c62272e07bb0142ULL;
    memcpy(hash + 16, &x, 8);
    x = x * 0x9e3779b97f4a7c15ULL;
    memcpy(hash + 24, &x, 8);
}

/* Generate deterministic code bytes from index */
static void make_code(uint64_t i, uint8_t *buf, uint32_t len) {
    for (uint32_t j = 0; j < len; j++) {
        buf[j] = (uint8_t)((i * 31 + j * 7) & 0xFF);
    }
}

/* =========================================================================
 * Tests
 * ========================================================================= */

static void test_create_destroy(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 1000);
    ASSERT(cs != NULL, "create failed");
    ASSERT(code_store_count(cs) == 0, "count should be 0");
    code_store_destroy(cs);
}

static void test_put_get_single(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 1000);
    ASSERT(cs != NULL, "create failed");

    uint8_t hash[32];
    make_hash(1, hash);
    uint8_t code[100];
    make_code(1, code, 100);

    ASSERT(code_store_put(cs, hash, code, 100), "put failed");
    ASSERT(code_store_count(cs) == 1, "count should be 1");
    ASSERT(code_store_contains(cs, hash), "should contain hash");
    ASSERT(code_store_get_size(cs, hash) == 100, "size should be 100");

    uint8_t buf[100];
    uint32_t len = code_store_get(cs, hash, buf, 100);
    ASSERT(len == 100, "get returned %u", len);
    ASSERT(memcmp(buf, code, 100) == 0, "data mismatch");

    code_store_destroy(cs);
}

static void test_dedup(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 1000);

    uint8_t hash[32];
    make_hash(42, hash);
    uint8_t code[200];
    make_code(42, code, 200);

    /* Put same code 100 times */
    for (int i = 0; i < 100; i++) {
        ASSERT(code_store_put(cs, hash, code, 200), "put %d failed", i);
    }

    /* Should be stored once */
    ASSERT(code_store_count(cs) == 1, "count should be 1, got %" PRIu64,
           code_store_count(cs));

    /* Verify data */
    uint8_t buf[200];
    uint32_t len = code_store_get(cs, hash, buf, 200);
    ASSERT(len == 200, "get returned %u", len);
    ASSERT(memcmp(buf, code, 200) == 0, "data mismatch");

    code_store_destroy(cs);
}

static void test_multiple_codes(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 1000);

    #define N_CODES 500
    uint8_t codes[N_CODES][256];
    uint8_t hashes[N_CODES][32];
    uint32_t lengths[N_CODES];

    for (int i = 0; i < N_CODES; i++) {
        make_hash((uint64_t)i, hashes[i]);
        lengths[i] = 50 + (uint32_t)(i % 200);  /* 50-249 bytes */
        make_code((uint64_t)i, codes[i], lengths[i]);
        ASSERT(code_store_put(cs, hashes[i], codes[i], lengths[i]),
               "put %d failed", i);
    }

    ASSERT(code_store_count(cs) == N_CODES, "count should be %d", N_CODES);

    /* Verify all */
    uint8_t buf[256];
    for (int i = 0; i < N_CODES; i++) {
        uint32_t len = code_store_get(cs, hashes[i], buf, 256);
        ASSERT(len == lengths[i], "code %d: expected %u, got %u",
               i, lengths[i], len);
        ASSERT(memcmp(buf, codes[i], lengths[i]) == 0,
               "code %d: data mismatch", i);
    }

    code_store_destroy(cs);
}

static void test_persistence(void) {
    cleanup();

    uint8_t hash[32];
    make_hash(99, hash);
    uint8_t code[150];
    make_code(99, code, 150);

    /* Create, put, sync, destroy */
    {
        code_store_t *cs = code_store_create(BASE_PATH, 1000);
        ASSERT(cs != NULL, "create failed");
        ASSERT(code_store_put(cs, hash, code, 150), "put failed");
        code_store_sync(cs);
        code_store_destroy(cs);
    }

    /* Reopen, verify */
    {
        code_store_t *cs = code_store_open(BASE_PATH);
        ASSERT(cs != NULL, "open failed");
        ASSERT(code_store_count(cs) == 1, "count should be 1");
        ASSERT(code_store_contains(cs, hash), "should contain hash");

        uint8_t buf[150];
        uint32_t len = code_store_get(cs, hash, buf, 150);
        ASSERT(len == 150, "get returned %u", len);
        ASSERT(memcmp(buf, code, 150) == 0, "data mismatch after reopen");
        code_store_destroy(cs);
    }
}

static void test_large_code(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 100);

    #define LARGE_LEN 24576  /* 24KB */
    uint8_t *code = malloc(LARGE_LEN);
    ASSERT(code != NULL, "malloc failed");
    make_code(7, code, LARGE_LEN);

    uint8_t hash[32];
    make_hash(7, hash);

    ASSERT(code_store_put(cs, hash, code, LARGE_LEN), "put large failed");
    ASSERT(code_store_get_size(cs, hash) == LARGE_LEN, "size mismatch");

    uint8_t *buf = malloc(LARGE_LEN);
    ASSERT(buf != NULL, "malloc failed");
    uint32_t len = code_store_get(cs, hash, buf, LARGE_LEN);
    ASSERT(len == LARGE_LEN, "get returned %u", len);
    ASSERT(memcmp(buf, code, LARGE_LEN) == 0, "large code data mismatch");

    free(code);
    free(buf);
    code_store_destroy(cs);
}

static void test_empty_code(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 100);

    uint8_t hash[32];
    make_hash(0, hash);

    /* Store empty code (code_len = 0) */
    ASSERT(code_store_put(cs, hash, NULL, 0), "put empty failed");
    ASSERT(code_store_contains(cs, hash), "should contain empty");
    ASSERT(code_store_get_size(cs, hash) == 0, "size should be 0");

    uint8_t buf[1];
    uint32_t len = code_store_get(cs, hash, buf, 1);
    ASSERT(len == 0, "get should return 0 for empty code");

    code_store_destroy(cs);
}

static void test_buffer_too_small(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 100);

    uint8_t hash[32];
    make_hash(5, hash);
    uint8_t code[500];
    make_code(5, code, 500);

    ASSERT(code_store_put(cs, hash, code, 500), "put failed");

    /* Buffer too small → returns required size */
    uint8_t small_buf[10];
    memset(small_buf, 0xAA, 10);
    uint32_t len = code_store_get(cs, hash, small_buf, 10);
    ASSERT(len == 500, "should return required size 500, got %u", len);

    /* Verify buffer untouched */
    uint8_t expected[10];
    memset(expected, 0xAA, 10);
    ASSERT(memcmp(small_buf, expected, 10) == 0, "buffer should be untouched");

    code_store_destroy(cs);
}

static void test_not_found(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 100);

    uint8_t hash[32];
    make_hash(999, hash);

    ASSERT(!code_store_contains(cs, hash), "should not contain");
    ASSERT(code_store_get_size(cs, hash) == 0, "size should be 0");

    uint8_t buf[100];
    uint32_t len = code_store_get(cs, hash, buf, 100);
    ASSERT(len == 0, "get should return 0");

    code_store_destroy(cs);
}

static void test_stress(void) {
    cleanup();
    code_store_t *cs = code_store_create(BASE_PATH, 20000);

    #define STRESS_UNIQUE  10000
    #define STRESS_DUPES   50000

    /* Insert unique codes */
    for (uint64_t i = 0; i < STRESS_UNIQUE; i++) {
        uint8_t hash[32], code[128];
        make_hash(i, hash);
        uint32_t code_len = 32 + (uint32_t)(i % 96);  /* 32-127 bytes */
        make_code(i, code, code_len);
        ASSERT(code_store_put(cs, hash, code, code_len), "put %" PRIu64 " failed", i);
    }

    ASSERT(code_store_count(cs) == STRESS_UNIQUE,
           "count should be %d, got %" PRIu64, STRESS_UNIQUE, code_store_count(cs));

    /* Insert duplicates (should be no-ops) */
    for (uint64_t i = 0; i < STRESS_DUPES; i++) {
        uint64_t idx = i % STRESS_UNIQUE;
        uint8_t hash[32], code[128];
        make_hash(idx, hash);
        uint32_t code_len = 32 + (uint32_t)(idx % 96);
        make_code(idx, code, code_len);
        ASSERT(code_store_put(cs, hash, code, code_len), "dup put failed");
    }

    /* Count should not change */
    ASSERT(code_store_count(cs) == STRESS_UNIQUE,
           "count after dupes: %" PRIu64, code_store_count(cs));

    /* Verify all codes */
    uint8_t buf[128];
    for (uint64_t i = 0; i < STRESS_UNIQUE; i++) {
        uint8_t hash[32], code[128];
        make_hash(i, hash);
        uint32_t code_len = 32 + (uint32_t)(i % 96);
        make_code(i, code, code_len);

        uint32_t len = code_store_get(cs, hash, buf, 128);
        ASSERT(len == code_len, "code %" PRIu64 ": expected %u, got %u",
               i, code_len, len);
        ASSERT(memcmp(buf, code, code_len) == 0,
               "code %" PRIu64 ": data mismatch", i);
    }

    /* Persistence check */
    code_store_sync(cs);
    code_store_destroy(cs);

    cs = code_store_open(BASE_PATH);
    ASSERT(cs != NULL, "reopen failed");
    ASSERT(code_store_count(cs) == STRESS_UNIQUE,
           "count after reopen: %" PRIu64, code_store_count(cs));

    /* Spot-check 100 random codes */
    for (uint64_t i = 0; i < 100; i++) {
        uint64_t idx = (i * 97) % STRESS_UNIQUE;
        uint8_t hash[32], code[128];
        make_hash(idx, hash);
        uint32_t code_len = 32 + (uint32_t)(idx % 96);
        make_code(idx, code, code_len);

        uint32_t len = code_store_get(cs, hash, buf, 128);
        ASSERT(len == code_len, "reopen code %" PRIu64 ": len mismatch", idx);
        ASSERT(memcmp(buf, code, code_len) == 0,
               "reopen code %" PRIu64 ": data mismatch", idx);
    }

    code_store_destroy(cs);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== code_store Tests ===\n\n");

    printf("Phase 1: Basic Operations\n");
    RUN(test_create_destroy);
    RUN(test_put_get_single);
    RUN(test_not_found);
    RUN(test_empty_code);
    RUN(test_buffer_too_small);

    printf("\nPhase 2: Deduplication\n");
    RUN(test_dedup);

    printf("\nPhase 3: Multiple Codes\n");
    RUN(test_multiple_codes);

    printf("\nPhase 4: Persistence\n");
    RUN(test_persistence);

    printf("\nPhase 5: Large Code\n");
    RUN(test_large_code);

    printf("\nPhase 6: Stress (10K unique + 50K dupes)\n");
    RUN(test_stress);

    cleanup();
    printf("\n=== Results: %d / %d passed ===\n", pass_count, test_num);
    return 0;
}
