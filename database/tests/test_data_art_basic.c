/*
 * Basic tests for persistent ART (data_art)
 * 
 * Tests the core functionality of the disk-backed adaptive radix tree.
 */

#include "data_art.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#define TEST_DB_FILE "/tmp/test_data_art.db"

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

static void cleanup_test_file(void) {
    unlink(TEST_DB_FILE);
}

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

    cleanup_test_file();

    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    ASSERT(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    ASSERT(tree != NULL);
    ASSERT(data_art_size(tree) == 0);
    ASSERT(data_art_is_empty(tree));

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    cleanup_test_file();
    PASS();
}

static void test_create_with_buffer_pool(void) {
    TEST("create tree with buffer pool");
    
    cleanup_test_file();
    
    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);
    
    buffer_pool_config_t config = {
        .capacity = 16,
        .enable_statistics = true
    };
    
    buffer_pool_t *bp = buffer_pool_create(&config, pm);
    ASSERT(bp != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    ASSERT(tree != NULL);
    ASSERT(tree->buffer_pool == bp);
    
    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    
    cleanup_test_file();
    PASS();
}

static void test_insert_single_small_value(void) {
    TEST("insert single small key-value");

    cleanup_test_file();

    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    ASSERT(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
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

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    cleanup_test_file();
    PASS();
}

static void test_insert_large_value_overflow(void) {
    TEST("insert large value requiring overflow pages");

    cleanup_test_file();

    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    ASSERT(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
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
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    cleanup_test_file();
    PASS();
}

static void test_get_nonexistent(void) {
    TEST("get nonexistent key returns NULL");

    cleanup_test_file();

    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    ASSERT(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "missing");

    size_t value_len = 0;
    const void *result = data_art_get(tree, key, 32, &value_len);
    ASSERT(result == NULL);
    ASSERT(!data_art_contains(tree, key, 32));

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    cleanup_test_file();
    PASS();
}

static void test_statistics(void) {
    TEST("statistics tracking");

    cleanup_test_file();

    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    ASSERT(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    ASSERT(tree != NULL);

    uint8_t key[32];
    make_key32(key, "test");
    const char *value = "value";

    data_art_insert(tree, key, 32,
                    value, strlen(value) + 1);

    data_art_stats_t stats;
    data_art_get_stats(tree, &stats);

    ASSERT(stats.num_entries == 1);
    ASSERT(stats.version == 1);
    ASSERT(stats.nodes_allocated > 0);

    // Print stats for visual inspection
    printf("\n");
    data_art_print_stats(tree);

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    cleanup_test_file();
    PASS();
}

static void test_flush_and_persistence(void) {
    TEST("flush tree to disk");
    
    cleanup_test_file();
    
    page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
    ASSERT(pm != NULL);
    
    buffer_pool_config_t config = {.capacity = 16, .enable_statistics = true};
    buffer_pool_t *bp = buffer_pool_create(&config, pm);
    ASSERT(bp != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
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
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    
    cleanup_test_file();
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
    test_create_with_buffer_pool();
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
