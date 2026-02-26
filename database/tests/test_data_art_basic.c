/*
 * Basic tests for persistent ART (data_art)
 *
 * Tests the core functionality of the disk-backed adaptive radix tree.
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

// Test counter
static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) \
    do { \
        tests_run++; \
        printf("TEST: %s ... ", name); \
        fflush(stdout);

#define PASS() \
        tests_passed++; \
        printf("✓ PASS\n"); \
    } while(0)

#define ASSERT(cond) \
    do { \
        if (!(cond)) { \
            printf("✗ FAIL\n"); \
            printf("  Assertion failed: %s\n", #cond); \
            printf("  at %s:%d\n", __FILE__, __LINE__); \
            return; \
        } \
    } while(0)

// ============================================================================
// Test Helpers
// ============================================================================

// Helper: create a 32-byte zero-padded key from a string
static void make_key32(uint8_t key_out[32], const char *str) {
    memset(key_out, 0, 32);
    size_t len = strlen(str);
    if (len > 32) len = 32;
    memcpy(key_out, str, len);
}

// ============================================================================
// Basic Tests
// ============================================================================

static void test_create_destroy(void) {
    TEST("create and destroy tree");

    system("rm -rf /tmp/test_basic_create && mkdir -p /tmp/test_basic_create");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_create/art.dat", 32);
    ASSERT(tree != NULL);
    ASSERT(data_art_size(tree) == 0);
    ASSERT(data_art_is_empty(tree));

    data_art_destroy(tree);

    PASS();
}

static void test_create_with_defaults(void) {
    TEST("create tree with mmap defaults");

    system("rm -rf /tmp/test_basic_defaults && mkdir -p /tmp/test_basic_defaults");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_defaults/art.dat", 32);
    ASSERT(tree != NULL);

    data_art_destroy(tree);

    PASS();
}

static void test_insert_single_small_value(void) {
    TEST("insert single small key-value");

    system("rm -rf /tmp/test_basic_insert && mkdir -p /tmp/test_basic_insert");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_insert/art.dat", 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "hello");
    const char *value = "world";

    bool success = data_art_insert(tree, key, 32,
                                     value, strlen(value) + 1);
    ASSERT(success);
    ASSERT(data_art_size(tree) == 1);
    ASSERT(!data_art_is_empty(tree));

    // Verify the value can be retrieved
    size_t value_len = 0;
    const void *retrieved = data_art_get(tree, key, 32, &value_len);
    ASSERT(retrieved != NULL);
    ASSERT(value_len == strlen(value) + 1);
    ASSERT(strcmp((const char *)retrieved, value) == 0);
    free((void *)retrieved);

    data_art_destroy(tree);

    PASS();
}

static void test_insert_large_value_overflow(void) {
    TEST("insert large value requiring overflow pages");

    system("rm -rf /tmp/test_basic_overflow && mkdir -p /tmp/test_basic_overflow");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_overflow/art.dat", 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "bigkey");

    // Create a large value that exceeds MAX_INLINE_DATA at any page size
    size_t large_size = MAX_INLINE_DATA + 1000;
    char *large_value = malloc(large_size);
    ASSERT(large_value != NULL);
    memset(large_value, 'X', large_size - 1);
    large_value[large_size - 1] = '\0';

    bool success = data_art_insert(tree, key, 32,
                                     large_value, large_size);
    ASSERT(success);
    ASSERT(data_art_size(tree) == 1);
    ASSERT(tree->overflow_pages_allocated > 0);

    free(large_value);
    data_art_destroy(tree);

    PASS();
}

static void test_get_nonexistent(void) {
    TEST("get nonexistent key returns NULL");

    system("rm -rf /tmp/test_basic_nokey && mkdir -p /tmp/test_basic_nokey");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_nokey/art.dat", 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "missing");

    size_t value_len = 0;
    const void *result = data_art_get(tree, key, 32, &value_len);
    ASSERT(result == NULL);
    ASSERT(!data_art_contains(tree, key, 32));

    data_art_destroy(tree);

    PASS();
}

static void test_statistics(void) {
    TEST("statistics tracking");

    system("rm -rf /tmp/test_basic_stats && mkdir -p /tmp/test_basic_stats");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_stats/art.dat", 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "test");
    const char *value = "value";

    data_art_insert(tree, key, 32,
                    value, strlen(value) + 1);

    data_art_stats_t stats;
    data_art_get_stats(tree, &stats);

    ASSERT(stats.num_entries == 1);
    ASSERT(stats.version == 2);  // version incremented once by auto-commit insert
    ASSERT(stats.nodes_allocated > 0);

    // Print stats for visual inspection
    printf("\n");
    data_art_print_stats(tree);

    data_art_destroy(tree);

    PASS();
}

static void test_flush_and_persistence(void) {
    TEST("flush tree to disk");

    system("rm -rf /tmp/test_basic_flush && mkdir -p /tmp/test_basic_flush");

    data_art_tree_t *tree = data_art_create("/tmp/test_basic_flush/art.dat", 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "persist");
    const char *value = "data";

    data_art_insert(tree, key, 32,
                    value, strlen(value) + 1);

    bool flushed = data_art_flush(tree);
    ASSERT(flushed);

    // Get root for later recovery
    node_ref_t root = data_art_get_root(tree);
    ASSERT(!node_ref_is_null(root));

    data_art_destroy(tree);

    PASS();
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(void) {
    printf("\n");
    printf("========================================\n");
    printf("Persistent ART (data_art) Basic Tests\n");
    printf("========================================\n\n");

    // Basic functionality
    test_create_destroy();
    test_create_with_defaults();
    test_insert_single_small_value();
    test_insert_large_value_overflow();
    test_get_nonexistent();
    test_statistics();
    test_flush_and_persistence();

    // Summary
    printf("\n");
    printf("========================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("========================================\n\n");

    return (tests_passed == tests_run) ? 0 : 1;
}
