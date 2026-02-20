/**
 * Test: data_art - Delete Operations
 * 
 * Basic tests for delete functionality
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_DB_PATH "/tmp/test_data_art_delete.db"

// Test tracking
static int tests_run = 0;
static int tests_passed = 0;

// Remove test database
static void cleanup_test_db(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
}

#define TEST(name) \
    do { \
        printf("TEST: %s\n", name); \
        fflush(stdout); \
        tests_run++; \
        cleanup_test_db(); \
    } while (0)

#define PASS() \
    do { \
        printf("  ✓ PASS\n\n"); \
        tests_passed++; \
    } while (0)

#define FAIL(msg) \
    do { \
        printf("  ✗ FAIL: %s\n\n", msg); \
        return false; \
    } while (0)

/**
 * Test: Delete single key from tree
 */
static bool test_delete_single_key(void) {
    TEST("delete single key");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    assert(tree != NULL);
    
    // Insert a key
    const char *key = "test_key";
    const char *value = "test_value";
    
    if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), value, strlen(value))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to insert key");
    }
    
    // Verify it exists
    size_t value_len;
    const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
    if (!retrieved) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Key not found after insertion");
    }
    free((void *)retrieved);
    
    // Delete it
    if (!data_art_delete(tree, (const uint8_t *)key, strlen(key))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to delete key");
    }
    
    // Verify it's gone
    retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
    if (retrieved) {
        free((void *)retrieved);
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Key still exists after deletion");
    }
    
    // Verify tree size
    if (data_art_size(tree) != 0) {
        fprintf(stderr, "Expected size 0, got %zu\n", data_art_size(tree));
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Tree size incorrect after deletion");
    }
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Delete from tree with multiple keys
 */
static bool test_delete_multiple_keys(void) {
    TEST("delete from tree with multiple keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    assert(tree != NULL);
    
    // Insert multiple keys
    const char *keys[] = {"apple", "banana", "cherry", "date", "elderberry"};
    int num_keys = 5;
    
    for (int i = 0; i < num_keys; i++) {
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)keys[i], strlen(keys[i]), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert key");
        }
    }
    
    printf("  Inserted %d keys\n", num_keys);
    
    // Delete middle key
    if (!data_art_delete(tree, (const uint8_t *)keys[2], strlen(keys[2]))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to delete middle key");
    }
    
    printf("  Deleted key: %s\n", keys[2]);
    
    // Verify deleted key is gone
    size_t value_len;
    const void *retrieved = data_art_get(tree, (const uint8_t *)keys[2], strlen(keys[2]), &value_len);
    if (retrieved) {
        free((void *)retrieved);
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Deleted key still exists");
    }
    
    // Verify other keys still exist
    for (int i = 0; i < num_keys; i++) {
        if (i == 2) continue;  // Skip deleted key
        
        retrieved = data_art_get(tree, (const uint8_t *)keys[i], strlen(keys[i]), &value_len);
        if (!retrieved) {
            fprintf(stderr, "Key %d disappeared after deleting another key\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Other key disappeared after deletion");
        }
        free((void *)retrieved);
    }
    
    printf("  Other keys still exist\n");
    
    // Verify size
    if (data_art_size(tree) != 4) {
        fprintf(stderr, "Expected size 4, got %zu\n", data_art_size(tree));
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Tree size incorrect");
    }
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Delete non-existent key
 */
static bool test_delete_nonexistent(void) {
    TEST("delete non-existent key");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    assert(tree != NULL);
    
    // Insert a key
    const char *key = "exists";
    const char *value = "value";
    data_art_insert(tree, (const uint8_t *)key, strlen(key), value, strlen(value));
    
    // Try to delete non-existent key
    const char *nonexistent = "does_not_exist";
    if (data_art_delete(tree, (const uint8_t *)nonexistent, strlen(nonexistent))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Deleted non-existent key (should return false)");
    }
    
    // Verify existing key still there
    size_t value_len;
    const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
    if (!retrieved) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Existing key disappeared");
    }
    free((void *)retrieved);
    
    // Verify size unchanged
    if (data_art_size(tree) != 1) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Tree size changed after failed delete");
    }
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Delete all keys
 */
static bool test_delete_all(void) {
    TEST("delete all keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    assert(tree != NULL);
    
    const int num_keys = 20;
    
    // Insert keys
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        data_art_insert(tree, (const uint8_t *)key, strlen(key), value, strlen(value));
    }
    
    printf("  Inserted %d keys\n", num_keys);
    
    // Delete all keys
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        if (!data_art_delete(tree, (const uint8_t *)key, strlen(key))) {
            fprintf(stderr, "Failed to delete key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to delete key");
        }
    }
    
    printf("  Deleted all %d keys\n", num_keys);
    
    // Verify tree is empty
    if (data_art_size(tree) != 0) {
        fprintf(stderr, "Expected size 0, got %zu\n", data_art_size(tree));
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Tree not empty after deleting all keys");
    }
    
    // Verify all keys are gone
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
        if (retrieved) {
            fprintf(stderr, "Key %d still exists\n", i);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Key still exists after deletion");
        }
    }
    
    printf("  Tree is empty\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Insert, delete, re-insert same key
 */
static bool test_delete_and_reinsert(void) {
    TEST("delete and re-insert same key");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    assert(tree != NULL);
    
    const char *key = "test_key";
    const char *value1 = "first_value";
    const char *value2 = "second_value";
    
    // Insert
    data_art_insert(tree, (const uint8_t *)key, strlen(key), value1, strlen(value1));
    
    // Delete
    if (!data_art_delete(tree, (const uint8_t *)key, strlen(key))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to delete key");
    }
    
    // Re-insert with different value
    if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), value2, strlen(value2))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to re-insert key");
    }
    
    // Verify new value
    size_t value_len;
    const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
    if (!retrieved) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Re-inserted key not found");
    }
    
    if (value_len != strlen(value2) || memcmp(retrieved, value2, value_len) != 0) {
        fprintf(stderr, "Expected: %s\n", value2);
        fprintf(stderr, "Got: %.*s\n", (int)value_len, (char *)retrieved);
        free((void *)retrieved);
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Re-inserted value incorrect");
    }
    
    free((void *)retrieved);
    
    printf("  Successfully re-inserted with new value\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

int main(void) {
    printf("\n");
    printf("================================================================\n");
    printf("          Persistent ART - Delete Tests                        \n");
    printf("================================================================\n");
    printf("\n");
    
    // Run all delete tests
    test_delete_single_key();
    test_delete_multiple_keys();
    test_delete_nonexistent();
    test_delete_all();
    test_delete_and_reinsert();
    
    // Summary
    printf("================================================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("================================================================\n");
    printf("\n");
    
    cleanup_test_db();
    
    return (tests_passed == tests_run) ? 0 : 1;
}
