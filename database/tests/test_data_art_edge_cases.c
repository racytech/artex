/**
 * Test: data_art - Edge Cases for Insert and Search
 * 
 * Comprehensive edge case testing to validate correctness before delete implementation
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_DB_PATH "/tmp/test_data_art_edge_cases.db"

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
 * Test: Empty key
 */
static bool test_empty_key(void) {
    TEST("empty key");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Insert empty key
    const uint8_t *empty_key = (const uint8_t *)"";
    const char *value = "empty_value";
    
    if (!data_art_insert(tree, empty_key, 0, value, strlen(value))) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to insert empty key");
    }
    
    // Verify retrieval
    size_t value_len;
    const void *retrieved = data_art_get(tree, empty_key, 0, &value_len);
    
    if (!retrieved) {
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Failed to retrieve empty key");
    }
    
    if (value_len != strlen(value) || memcmp(retrieved, value, value_len) != 0) {
        free((void *)retrieved);
        data_art_destroy(tree);
        page_manager_destroy(pm);
        FAIL("Value mismatch for empty key");
    }
    
    free((void *)retrieved);
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Single byte keys
 */
static bool test_single_byte_keys(void) {
    TEST("single byte keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Insert all 256 possible single-byte keys
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, &key, 1, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert single byte key");
        }
    }
    
    printf("  Inserted 256 single-byte keys\n");
    
    // Verify all keys
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        char expected[32];
        snprintf(expected, sizeof(expected), "value_%d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, &key, 1, &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve single byte key");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for key %d\n", i);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch for single byte key");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All 256 keys verified\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Keys with common prefixes of varying lengths
 */
static bool test_common_prefixes(void) {
    TEST("keys with common prefixes");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Test various prefix lengths (up to 20 bytes)
    const char *keys[] = {
        "a",
        "ab",
        "abc",
        "abcd",
        "abcde",
        "abcdef",
        "abcdefg",
        "abcdefgh",
        "abcdefghi",
        "abcdefghij",
        "abcdefghijk",           // 11 bytes
        "abcdefghijkl",          // 12 bytes
        "abcdefghijklm",         // 13 bytes - triggers lazy expansion
        "abcdefghijklmn",        // 14 bytes
        "abcdefghijklmno",       // 15 bytes
        "abcdefghijklmnop",      // 16 bytes
        "abcdefghijklmnopq",     // 17 bytes
        "abcdefghijklmnopqr",    // 18 bytes
        "abcdefghijklmnopqrs",   // 19 bytes
        "abcdefghijklmnopqrst",  // 20 bytes
    };
    
    int num_keys = sizeof(keys) / sizeof(keys[0]);
    
    // Insert all keys
    for (int i = 0; i < num_keys; i++) {
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)keys[i], strlen(keys[i]), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key '%s'\n", keys[i]);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert key with common prefix");
        }
    }
    
    printf("  Inserted %d keys with nested prefixes\n", num_keys);
    
    // Verify all keys
    for (int i = 0; i < num_keys; i++) {
        char expected[32];
        snprintf(expected, sizeof(expected), "value_%d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)keys[i], 
                                             strlen(keys[i]), &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key '%s'\n", keys[i]);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve key with common prefix");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for key '%s'\n", keys[i]);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch for key with common prefix");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All %d keys verified\n", num_keys);
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Keys that differ only at the last byte
 */
static bool test_last_byte_difference(void) {
    TEST("keys differing only at last byte");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Create keys with same 15-byte prefix, different last byte
    const char *prefix = "common_prefix__";  // 15 bytes
    
    for (int i = 0; i < 256; i++) {
        char key[17];
        snprintf(key, sizeof(key), "%s%c", prefix, (char)i);
        
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, 16, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key ending with byte %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert key with last byte difference");
        }
    }
    
    printf("  Inserted 256 keys with 15-byte common prefix\n");
    
    // Verify all keys
    for (int i = 0; i < 256; i++) {
        char key[17];
        snprintf(key, sizeof(key), "%s%c", prefix, (char)i);
        
        char expected[32];
        snprintf(expected, sizeof(expected), "value_%d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)key, 16, &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key ending with byte %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve key with last byte difference");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for key ending with byte %d\n", i);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch for key with last byte difference");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All 256 keys verified\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Update existing keys (overwrite values)
 */
static bool test_key_updates(void) {
    TEST("updating existing keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    const int num_keys = 100;
    
    // Insert initial values
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        char value[32];
        snprintf(value, sizeof(value), "original_value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert key for update test");
        }
    }
    
    printf("  Inserted %d keys with original values\n", num_keys);
    
    // Update all values
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        char value[32];
        snprintf(value, sizeof(value), "updated_value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to update key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to update key");
        }
    }
    
    printf("  Updated all %d keys\n", num_keys);
    
    // Verify updates
    for (int i = 0; i < num_keys; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        char expected[32];
        snprintf(expected, sizeof(expected), "updated_value_%d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve updated key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve updated key");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for updated key %d\n", i);
            fprintf(stderr, "Expected: %s\n", expected);
            fprintf(stderr, "Got: %.*s\n", (int)value_len, (char *)retrieved);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch for updated key");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All %d updated values verified\n", num_keys);
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Very long keys (test lazy expansion extensively)
 */
static bool test_very_long_keys(void) {
    TEST("very long keys (up to 256 bytes)");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Test keys of increasing length
    const int lengths[] = {50, 100, 150, 200, 256};
    const int num_lengths = sizeof(lengths) / sizeof(lengths[0]);
    
    for (int l = 0; l < num_lengths; l++) {
        int key_len = lengths[l];
        
        // Create 10 keys of this length with different patterns
        for (int i = 0; i < 10; i++) {
            char *key = malloc(key_len + 1);
            
            // Fill with pattern
            for (int j = 0; j < key_len; j++) {
                key[j] = 'a' + ((i + j) % 26);
            }
            key[key_len] = '\0';
            
            char value[64];
            snprintf(value, sizeof(value), "value_len%d_num%d", key_len, i);
            
            if (!data_art_insert(tree, (const uint8_t *)key, key_len, 
                                 value, strlen(value))) {
                fprintf(stderr, "Failed to insert key of length %d, number %d\n", key_len, i);
                free(key);
                data_art_destroy(tree);
                page_manager_destroy(pm);
                FAIL("Failed to insert very long key");
            }
            
            free(key);
        }
    }
    
    printf("  Inserted keys of lengths: ");
    for (int l = 0; l < num_lengths; l++) {
        printf("%d%s", lengths[l], l < num_lengths - 1 ? ", " : "\n");
    }
    
    // Verify all keys
    for (int l = 0; l < num_lengths; l++) {
        int key_len = lengths[l];
        
        for (int i = 0; i < 10; i++) {
            char *key = malloc(key_len + 1);
            
            // Recreate same pattern
            for (int j = 0; j < key_len; j++) {
                key[j] = 'a' + ((i + j) % 26);
            }
            key[key_len] = '\0';
            
            char expected[64];
            snprintf(expected, sizeof(expected), "value_len%d_num%d", key_len, i);
            
            size_t value_len;
            const void *retrieved = data_art_get(tree, (const uint8_t *)key, key_len, &value_len);
            
            if (!retrieved) {
                fprintf(stderr, "Failed to retrieve key of length %d, number %d\n", key_len, i);
                free(key);
                data_art_destroy(tree);
                page_manager_destroy(pm);
                FAIL("Failed to retrieve very long key");
            }
            
            if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
                fprintf(stderr, "Value mismatch for key of length %d, number %d\n", key_len, i);
                free((void *)retrieved);
                free(key);
                data_art_destroy(tree);
                page_manager_destroy(pm);
                FAIL("Value mismatch for very long key");
            }
            
            free((void *)retrieved);
            free(key);
        }
    }
    
    printf("  All long keys verified\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Keys with all possible byte values (including null bytes)
 */
static bool test_binary_keys(void) {
    TEST("binary keys with null bytes");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Create keys with null bytes at different positions
    const int key_len = 32;
    
    for (int null_pos = 0; null_pos < key_len; null_pos++) {
        uint8_t key[32];
        
        // Fill with pattern
        for (int i = 0; i < key_len; i++) {
            if (i == null_pos) {
                key[i] = 0;  // Null byte
            } else {
                key[i] = (uint8_t)(i + 1);
            }
        }
        
        char value[64];
        snprintf(value, sizeof(value), "null_at_pos_%d", null_pos);
        
        if (!data_art_insert(tree, key, key_len, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key with null at position %d\n", null_pos);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert binary key with null byte");
        }
    }
    
    printf("  Inserted %d keys with null bytes at different positions\n", key_len);
    
    // Verify all keys
    for (int null_pos = 0; null_pos < key_len; null_pos++) {
        uint8_t key[32];
        
        // Recreate same pattern
        for (int i = 0; i < key_len; i++) {
            if (i == null_pos) {
                key[i] = 0;
            } else {
                key[i] = (uint8_t)(i + 1);
            }
        }
        
        char expected[64];
        snprintf(expected, sizeof(expected), "null_at_pos_%d", null_pos);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, key, key_len, &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key with null at position %d\n", null_pos);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve binary key with null byte");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for key with null at position %d\n", null_pos);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch for binary key with null byte");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All binary keys verified\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Node type transitions (NODE_4 -> NODE_16 -> NODE_48 -> NODE_256)
 */
static bool test_node_growth(void) {
    TEST("node type transitions");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    const char *prefix = "prefix_";
    
    // Insert enough keys to trigger all node transitions
    // NODE_4: 4 children, NODE_16: 16 children, NODE_48: 48 children, NODE_256: 256 children
    
    printf("  Testing NODE_4 -> NODE_16 transition (4 to 5 children)\n");
    for (int i = 0; i < 6; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%s%02x", prefix, i);
        
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key for NODE_4->16 transition\n");
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed during NODE_4->16 transition");
        }
    }
    
    printf("  Testing NODE_16 -> NODE_48 transition (16 to 17 children)\n");
    for (int i = 6; i < 20; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%s%02x", prefix, i);
        
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key for NODE_16->48 transition\n");
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed during NODE_16->48 transition");
        }
    }
    
    printf("  Testing NODE_48 -> NODE_256 transition (48 to 49 children)\n");
    for (int i = 20; i < 60; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%s%02x", prefix, i);
        
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, strlen(key), 
                             value, strlen(value))) {
            fprintf(stderr, "Failed to insert key for NODE_48->256 transition\n");
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed during NODE_48->256 transition");
        }
    }
    
    printf("  Inserted 60 keys to test all node transitions\n");
    
    // Verify all keys still accessible
    for (int i = 0; i < 60; i++) {
        char key[32];
        snprintf(key, sizeof(key), "%s%02x", prefix, i);
        
        char expected[32];
        snprintf(expected, sizeof(expected), "value_%d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)key, strlen(key), &value_len);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key %d after node transitions\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve key after node transitions");
        }
        
        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "Value mismatch for key %d after node transitions\n", i);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch after node transitions");
        }
        
        free((void *)retrieved);
    }
    
    printf("  All keys verified after node transitions\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Search for non-existent keys
 */
static bool test_nonexistent_keys(void) {
    TEST("searching for non-existent keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Insert some keys
    const char *existing_keys[] = {
        "apple", "banana", "cherry", "date", "elderberry"
    };
    
    for (int i = 0; i < 5; i++) {
        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);
        
        data_art_insert(tree, (const uint8_t *)existing_keys[i], 
                       strlen(existing_keys[i]), value, strlen(value));
    }
    
    // Try to find keys that don't exist
    const char *nonexistent_keys[] = {
        "appl",           // Prefix of existing key
        "apples",         // Existing key + suffix
        "banan",          // Prefix of existing key
        "cherries",       // Existing key + suffix
        "fig",            // Completely different
        "grape",          // Completely different
        "",               // Empty (if not inserted)
        "zebra",          // Alphabetically after all keys
    };
    
    int num_nonexistent = sizeof(nonexistent_keys) / sizeof(nonexistent_keys[0]);
    
    for (int i = 0; i < num_nonexistent; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)nonexistent_keys[i], 
                                             strlen(nonexistent_keys[i]), &value_len);
        
        if (retrieved) {
            fprintf(stderr, "Found non-existent key: '%s'\n", nonexistent_keys[i]);
            free((void *)retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Found a key that should not exist");
        }
    }
    
    printf("  Correctly returned NULL for %d non-existent keys\n", num_nonexistent);
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

/**
 * Test: Prefix split at different depths
 */
static bool test_prefix_splits(void) {
    TEST("prefix splits at various depths");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    // Insert keys that will cause prefix splits at various matching points
    const char *test_cases[][3] = {
        // {key1, key2, description}
        {"abc", "abd", "split at depth 2"},
        {"longprefix_a", "longprefix_b", "split after 11 bytes (lazy)"},
        {"verylongprefix123_x", "verylongprefix123_y", "split after 18 bytes (lazy)"},
        {"same", "same", "exact duplicate"},
    };
    
    int num_cases = 4;
    
    for (int i = 0; i < num_cases; i++) {
        const char *key1 = test_cases[i][0];
        const char *key2 = test_cases[i][1];
        
        printf("  Testing: %s\n", test_cases[i][2]);
        
        char value1[32];
        snprintf(value1, sizeof(value1), "value1_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key1, strlen(key1), 
                             value1, strlen(value1))) {
            fprintf(stderr, "Failed to insert first key in case %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert first key in prefix split test");
        }
        
        char value2[32];
        snprintf(value2, sizeof(value2), "value2_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key2, strlen(key2), 
                             value2, strlen(value2))) {
            fprintf(stderr, "Failed to insert second key in case %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to insert second key in prefix split test");
        }
        
        // Verify both keys
        size_t len1, len2;
        const void *ret1 = data_art_get(tree, (const uint8_t *)key1, strlen(key1), &len1);
        const void *ret2 = data_art_get(tree, (const uint8_t *)key2, strlen(key2), &len2);
        
        if (!ret1 || !ret2) {
            fprintf(stderr, "Failed to retrieve keys after prefix split case %d\n", i);
            if (ret1) free((void *)ret1);
            if (ret2) free((void *)ret2);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Failed to retrieve keys after prefix split");
        }
        
        // For duplicate key case, should have updated value
        const char *expected1 = (strcmp(key1, key2) == 0) ? value2 : value1;
        
        if (len1 != strlen(expected1) || memcmp(ret1, expected1, len1) != 0) {
            fprintf(stderr, "Value mismatch for key1 in case %d\n", i);
            free((void *)ret1);
            free((void *)ret2);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("Value mismatch in prefix split test");
        }
        
        free((void *)ret1);
        free((void *)ret2);
    }
    
    printf("  All prefix split cases handled correctly\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
    PASS();
    return true;
}

int main(void) {
    printf("\n");
    printf("================================================================\n");
    printf("     Persistent ART - Edge Case Tests (Insert & Search)        \n");
    printf("================================================================\n");
    printf("\n");
    
    // Run all edge case tests
    test_empty_key();
    test_single_byte_keys();
    test_common_prefixes();
    test_last_byte_difference();
    test_key_updates();
    test_very_long_keys();
    test_binary_keys();
    test_node_growth();
    test_nonexistent_keys();
    test_prefix_splits();
    
    // Summary
    printf("================================================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("================================================================\n");
    printf("\n");
    
    cleanup_test_db();
    
    return (tests_passed == tests_run) ? 0 : 1;
}
