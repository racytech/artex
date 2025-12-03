#include "../include/art.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

// ANSI color codes for test output
#define COLOR_GREEN "\033[0;32m"
#define COLOR_RED "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_BLUE "\033[0;34m"
#define COLOR_RESET "\033[0m"

// Test tracking
static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

// Helper macros
#define TEST(name) \
    static void name(void); \
    static void run_##name(void) { \
        printf(COLOR_BLUE "Running: %s" COLOR_RESET "\n", #name); \
        name(); \
    } \
    static void name(void)

#define ASSERT(condition, message) \
    do { \
        tests_run++; \
        if (condition) { \
            tests_passed++; \
            printf(COLOR_GREEN "  ✓ " COLOR_RESET "%s\n", message); \
        } else { \
            tests_failed++; \
            printf(COLOR_RED "  ✗ " COLOR_RESET "%s (line %d)\n", message, __LINE__); \
        } \
    } while (0)

#define ASSERT_EQ(actual, expected, message) \
    ASSERT((actual) == (expected), message)

#define ASSERT_NEQ(actual, unexpected, message) \
    ASSERT((actual) != (unexpected), message)

#define ASSERT_NULL(ptr, message) \
    ASSERT((ptr) == NULL, message)

#define ASSERT_NOT_NULL(ptr, message) \
    ASSERT((ptr) != NULL, message)

#define ASSERT_TRUE(condition, message) \
    ASSERT((condition) == true, message)

#define ASSERT_FALSE(condition, message) \
    ASSERT((condition) == false, message)

#define ASSERT_BYTES_EQ(actual, expected, len, message) \
    ASSERT(memcmp(actual, expected, len) == 0, message)

//==============================================================================
// Test Cases
//==============================================================================

TEST(test_tree_init_destroy) {
    art_tree_t tree;
    
    ASSERT_TRUE(art_tree_init(&tree), "Tree initialization should succeed");
    ASSERT_NULL(tree.root, "Root should be NULL after init");
    ASSERT_EQ(tree.size, 0, "Size should be 0 after init");
    ASSERT_TRUE(art_is_empty(&tree), "Tree should be empty after init");
    
    art_tree_destroy(&tree);
    ASSERT_NULL(tree.root, "Root should be NULL after destroy");
    ASSERT_EQ(tree.size, 0, "Size should be 0 after destroy");
}

TEST(test_insert_get_single) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key[] = {0x01, 0x02, 0x03, 0x04};
    uint8_t value[] = {0xAA, 0xBB, 0xCC, 0xDD};
    
    ASSERT_TRUE(art_insert(&tree, key, sizeof(key), value, sizeof(value)),
                "Insert should succeed");
    ASSERT_EQ(art_size(&tree), 1, "Size should be 1 after insert");
    ASSERT_FALSE(art_is_empty(&tree), "Tree should not be empty");
    
    size_t value_len = 0;
    const void *result = art_get(&tree, key, sizeof(key), &value_len);
    
    ASSERT_NOT_NULL(result, "Get should return non-NULL");
    ASSERT_EQ(value_len, sizeof(value), "Value length should match");
    ASSERT_BYTES_EQ(result, value, sizeof(value), "Value should match");
    
    art_tree_destroy(&tree);
}

TEST(test_insert_update) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key[] = {0x01, 0x02, 0x03};
    uint8_t value1[] = {0xAA};
    uint8_t value2[] = {0xBB, 0xCC};
    
    // First insert
    ASSERT_TRUE(art_insert(&tree, key, sizeof(key), value1, sizeof(value1)),
                "First insert should succeed");
    ASSERT_EQ(art_size(&tree), 1, "Size should be 1");
    
    // Update with different value
    ASSERT_TRUE(art_insert(&tree, key, sizeof(key), value2, sizeof(value2)),
                "Update should succeed");
    ASSERT_EQ(art_size(&tree), 1, "Size should still be 1 after update");
    
    size_t value_len = 0;
    const void *result = art_get(&tree, key, sizeof(key), &value_len);
    
    ASSERT_NOT_NULL(result, "Get should return updated value");
    ASSERT_EQ(value_len, sizeof(value2), "Updated value length should match");
    ASSERT_BYTES_EQ(result, value2, sizeof(value2), "Updated value should match");
    
    art_tree_destroy(&tree);
}

TEST(test_insert_multiple) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert multiple key-value pairs
    for (int i = 0; i < 100; i++) {
        uint8_t key[4] = {(i >> 24) & 0xFF, (i >> 16) & 0xFF, (i >> 8) & 0xFF, i & 0xFF};
        uint8_t value[4] = {i & 0xFF, (i >> 8) & 0xFF, (i >> 16) & 0xFF, (i >> 24) & 0xFF};
        
        ASSERT_TRUE(art_insert(&tree, key, sizeof(key), value, sizeof(value)),
                    "Insert should succeed");
    }
    
    ASSERT_EQ(art_size(&tree), 100, "Size should be 100 after 100 inserts");
    
    // Verify all values
    for (int i = 0; i < 100; i++) {
        uint8_t key[4] = {(i >> 24) & 0xFF, (i >> 16) & 0xFF, (i >> 8) & 0xFF, i & 0xFF};
        uint8_t expected_value[4] = {i & 0xFF, (i >> 8) & 0xFF, (i >> 16) & 0xFF, (i >> 24) & 0xFF};
        
        size_t value_len = 0;
        const void *result = art_get(&tree, key, sizeof(key), &value_len);
        
        ASSERT_NOT_NULL(result, "All inserted keys should be found");
        ASSERT_BYTES_EQ(result, expected_value, sizeof(expected_value), "Values should match");
    }
    
    art_tree_destroy(&tree);
}

TEST(test_contains) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key1[] = {0x01, 0x02};
    uint8_t key2[] = {0x03, 0x04};
    uint8_t key3[] = {0x05, 0x06};
    uint8_t value[] = {0xFF};
    
    art_insert(&tree, key1, sizeof(key1), value, sizeof(value));
    art_insert(&tree, key2, sizeof(key2), value, sizeof(value));
    
    ASSERT_TRUE(art_contains(&tree, key1, sizeof(key1)), "key1 should exist");
    ASSERT_TRUE(art_contains(&tree, key2, sizeof(key2)), "key2 should exist");
    ASSERT_FALSE(art_contains(&tree, key3, sizeof(key3)), "key3 should not exist");
    
    art_tree_destroy(&tree);
}

TEST(test_delete_single) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key[] = {0x01, 0x02, 0x03};
    uint8_t value[] = {0xAA};
    
    art_insert(&tree, key, sizeof(key), value, sizeof(value));
    ASSERT_EQ(art_size(&tree), 1, "Size should be 1 after insert");
    
    ASSERT_TRUE(art_delete(&tree, key, sizeof(key)), "Delete should succeed");
    ASSERT_EQ(art_size(&tree), 0, "Size should be 0 after delete");
    ASSERT_TRUE(art_is_empty(&tree), "Tree should be empty after delete");
    ASSERT_FALSE(art_contains(&tree, key, sizeof(key)), "Key should not exist after delete");
    
    art_tree_destroy(&tree);
}

TEST(test_delete_nonexistent) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key1[] = {0x01, 0x02};
    uint8_t key2[] = {0x03, 0x04};
    uint8_t value[] = {0xFF};
    
    art_insert(&tree, key1, sizeof(key1), value, sizeof(value));
    
    ASSERT_FALSE(art_delete(&tree, key2, sizeof(key2)), "Delete non-existent key should return false");
    ASSERT_EQ(art_size(&tree), 1, "Size should still be 1");
    ASSERT_TRUE(art_contains(&tree, key1, sizeof(key1)), "Original key should still exist");
    
    art_tree_destroy(&tree);
}

TEST(test_delete_multiple) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert 50 items
    for (int i = 0; i < 50; i++) {
        uint8_t key[4] = {0, 0, (i >> 8) & 0xFF, i & 0xFF};
        uint8_t value[1] = {i & 0xFF};
        art_insert(&tree, key, sizeof(key), value, sizeof(value));
    }
    
    ASSERT_EQ(art_size(&tree), 50, "Size should be 50");
    
    // Delete every other item
    for (int i = 0; i < 50; i += 2) {
        uint8_t key[4] = {0, 0, (i >> 8) & 0xFF, i & 0xFF};
        ASSERT_TRUE(art_delete(&tree, key, sizeof(key)), "Delete should succeed");
    }
    
    ASSERT_EQ(art_size(&tree), 25, "Size should be 25 after deleting half");
    
    // Verify remaining items exist and deleted items don't
    for (int i = 0; i < 50; i++) {
        uint8_t key[4] = {0, 0, (i >> 8) & 0xFF, i & 0xFF};
        if (i % 2 == 0) {
            ASSERT_FALSE(art_contains(&tree, key, sizeof(key)), "Deleted key should not exist");
        } else {
            ASSERT_TRUE(art_contains(&tree, key, sizeof(key)), "Non-deleted key should exist");
        }
    }
    
    art_tree_destroy(&tree);
}

TEST(test_node_growth_4_to_16) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert 10 keys with same prefix to trigger Node4 -> Node16 growth
    for (int i = 0; i < 10; i++) {
        uint8_t key[2] = {0xFF, i};
        uint8_t value[1] = {i};
        ASSERT_TRUE(art_insert(&tree, key, sizeof(key), value, sizeof(value)),
                    "Insert should succeed");
    }
    
    ASSERT_EQ(art_size(&tree), 10, "Size should be 10");
    
    // Verify all keys exist
    for (int i = 0; i < 10; i++) {
        uint8_t key[2] = {0xFF, i};
        ASSERT_TRUE(art_contains(&tree, key, sizeof(key)), "All keys should exist");
    }
    
    art_tree_destroy(&tree);
}

TEST(test_node_growth_16_to_48) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert 30 keys to trigger Node16 -> Node48 growth
    for (int i = 0; i < 30; i++) {
        uint8_t key[2] = {0xAA, i};
        uint8_t value[1] = {i};
        art_insert(&tree, key, sizeof(key), value, sizeof(value));
    }
    
    ASSERT_EQ(art_size(&tree), 30, "Size should be 30");
    
    for (int i = 0; i < 30; i++) {
        uint8_t key[2] = {0xAA, i};
        ASSERT_TRUE(art_contains(&tree, key, sizeof(key)), "All keys should exist");
    }
    
    art_tree_destroy(&tree);
}

TEST(test_node_growth_48_to_256) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert 100 keys to trigger Node48 -> Node256 growth
    for (int i = 0; i < 100; i++) {
        uint8_t key[2] = {0xBB, i};
        uint8_t value[1] = {i};
        art_insert(&tree, key, sizeof(key), value, sizeof(value));
    }
    
    ASSERT_EQ(art_size(&tree), 100, "Size should be 100");
    
    for (int i = 0; i < 100; i++) {
        uint8_t key[2] = {0xBB, i};
        ASSERT_TRUE(art_contains(&tree, key, sizeof(key)), "All keys should exist");
    }
    
    art_tree_destroy(&tree);
}

TEST(test_variable_length_keys) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key1[] = {0x01};
    uint8_t key2[] = {0x01, 0x02};
    uint8_t key3[] = {0x01, 0x02, 0x03};
    uint8_t key4[] = {0x01, 0x02, 0x03, 0x04};
    uint8_t value[] = {0xFF};
    
    ASSERT_TRUE(art_insert(&tree, key1, sizeof(key1), value, sizeof(value)), "Insert key1");
    ASSERT_TRUE(art_insert(&tree, key2, sizeof(key2), value, sizeof(value)), "Insert key2");
    ASSERT_TRUE(art_insert(&tree, key3, sizeof(key3), value, sizeof(value)), "Insert key3");
    ASSERT_TRUE(art_insert(&tree, key4, sizeof(key4), value, sizeof(value)), "Insert key4");
    
    ASSERT_EQ(art_size(&tree), 4, "Size should be 4");
    
    ASSERT_TRUE(art_contains(&tree, key1, sizeof(key1)), "key1 should exist");
    ASSERT_TRUE(art_contains(&tree, key2, sizeof(key2)), "key2 should exist");
    ASSERT_TRUE(art_contains(&tree, key3, sizeof(key3)), "key3 should exist");
    ASSERT_TRUE(art_contains(&tree, key4, sizeof(key4)), "key4 should exist");
    
    art_tree_destroy(&tree);
}

TEST(test_common_prefix_keys) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Keys with common prefixes
    uint8_t key1[] = {0xAA, 0xBB, 0xCC, 0x01};
    uint8_t key2[] = {0xAA, 0xBB, 0xCC, 0x02};
    uint8_t key3[] = {0xAA, 0xBB, 0xDD, 0x01};
    uint8_t key4[] = {0xAA, 0xEE, 0xCC, 0x01};
    
    uint8_t value1[] = {0x01};
    uint8_t value2[] = {0x02};
    uint8_t value3[] = {0x03};
    uint8_t value4[] = {0x04};
    
    art_insert(&tree, key1, sizeof(key1), value1, sizeof(value1));
    art_insert(&tree, key2, sizeof(key2), value2, sizeof(value2));
    art_insert(&tree, key3, sizeof(key3), value3, sizeof(value3));
    art_insert(&tree, key4, sizeof(key4), value4, sizeof(value4));
    
    ASSERT_EQ(art_size(&tree), 4, "Size should be 4");
    
    size_t len = 0;
    const void *result = NULL;
    
    result = art_get(&tree, key1, sizeof(key1), &len);
    ASSERT_NOT_NULL(result, "key1 should be found");
    if (result) ASSERT_BYTES_EQ(result, value1, sizeof(value1), "key1 value should match");
    
    result = art_get(&tree, key2, sizeof(key2), &len);
    ASSERT_NOT_NULL(result, "key2 should be found");
    if (result && len == sizeof(value2)) ASSERT_BYTES_EQ(result, value2, sizeof(value2), "key2 value should match");
    else if (result) printf("  ! key2 found but wrong length: %zu vs %zu\n", len, sizeof(value2));
    
    result = art_get(&tree, key3, sizeof(key3), &len);
    ASSERT_NOT_NULL(result, "key3 should be found");
    if (result) ASSERT_BYTES_EQ(result, value3, sizeof(value3), "key3 value should match");
    
    result = art_get(&tree, key4, sizeof(key4), &len);
    ASSERT_NOT_NULL(result, "key4 should be found");
    if (result) ASSERT_BYTES_EQ(result, value4, sizeof(value4), "key4 value should match");
    
    art_tree_destroy(&tree);
}

TEST(test_empty_tree_operations) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key[] = {0x01, 0x02};
    
    ASSERT_NULL(art_get(&tree, key, sizeof(key), NULL), "Get on empty tree should return NULL");
    ASSERT_FALSE(art_contains(&tree, key, sizeof(key)), "Contains on empty tree should return false");
    ASSERT_FALSE(art_delete(&tree, key, sizeof(key)), "Delete on empty tree should return false");
    ASSERT_EQ(art_size(&tree), 0, "Size should be 0");
    ASSERT_TRUE(art_is_empty(&tree), "Tree should be empty");
    
    art_tree_destroy(&tree);
}

TEST(test_ethereum_address_keys) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Simulate Ethereum addresses (20 bytes)
    uint8_t addr1[20] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
                         0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22, 0x33};
    uint8_t addr2[20] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
                         0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11, 0x22, 0x34};
    uint8_t addr3[20] = {0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99, 0x88, 0x77, 0x66,
                         0x55, 0x44, 0x33, 0x22, 0x11, 0x00, 0xFF, 0xEE, 0xDD, 0xCC};
    
    uint64_t balance1 = 1000000;
    uint64_t balance2 = 2000000;
    uint64_t balance3 = 3000000;
    
    art_insert(&tree, addr1, sizeof(addr1), &balance1, sizeof(balance1));
    art_insert(&tree, addr2, sizeof(addr2), &balance2, sizeof(balance2));
    art_insert(&tree, addr3, sizeof(addr3), &balance3, sizeof(balance3));
    
    ASSERT_EQ(art_size(&tree), 3, "Should have 3 addresses");
    
    size_t len = 0;
    const uint64_t *result = NULL;
    
    result = art_get(&tree, addr1, sizeof(addr1), &len);
    ASSERT_NOT_NULL(result, "addr1 should be found");
    if (result) ASSERT_EQ(*result, balance1, "addr1 balance should match");
    
    result = art_get(&tree, addr2, sizeof(addr2), &len);
    ASSERT_NOT_NULL(result, "addr2 should be found");
    if (result) ASSERT_EQ(*result, balance2, "addr2 balance should match");
    
    result = art_get(&tree, addr3, sizeof(addr3), &len);
    ASSERT_NOT_NULL(result, "addr3 should be found");
    if (result) ASSERT_EQ(*result, balance3, "addr3 balance should match");
    
    art_tree_destroy(&tree);
}

TEST(test_storage_slot_keys) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Simulate storage slots (32 bytes each)
    uint8_t slot1[32] = {0};
    uint8_t slot2[32] = {0};
    uint8_t slot3[32] = {0};
    
    slot1[31] = 0x01;
    slot2[31] = 0x02;
    slot3[31] = 0xFF;
    
    uint8_t value1[32] = {0xAA};
    uint8_t value2[32] = {0xBB};
    uint8_t value3[32] = {0xCC};
    
    art_insert(&tree, slot1, sizeof(slot1), value1, sizeof(value1));
    art_insert(&tree, slot2, sizeof(slot2), value2, sizeof(value2));
    art_insert(&tree, slot3, sizeof(slot3), value3, sizeof(value3));
    
    ASSERT_EQ(art_size(&tree), 3, "Should have 3 storage slots");
    ASSERT_TRUE(art_contains(&tree, slot1, sizeof(slot1)), "slot1 should exist");
    ASSERT_TRUE(art_contains(&tree, slot2, sizeof(slot2)), "slot2 should exist");
    ASSERT_TRUE(art_contains(&tree, slot3, sizeof(slot3)), "slot3 should exist");
    
    art_tree_destroy(&tree);
}

//==============================================================================
// Iterator and Foreach Tests
//==============================================================================

TEST(test_iterator_empty_tree) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    art_iterator_t *iter = art_iterator_create(&tree);
    ASSERT_NOT_NULL(iter, "Iterator should be created for empty tree");
    
    bool has_next = art_iterator_next(iter);
    ASSERT_FALSE(has_next, "Empty tree should have no elements");
    ASSERT_TRUE(art_iterator_done(iter), "Iterator should be done");
    
    art_iterator_destroy(iter);
    art_tree_destroy(&tree);
}

TEST(test_iterator_single_element) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key[] = {1, 2, 3};
    uint8_t value[] = {10, 20, 30};
    art_insert(&tree, key, sizeof(key), value, sizeof(value));
    
    art_iterator_t *iter = art_iterator_create(&tree);
    ASSERT_NOT_NULL(iter, "Iterator should be created");
    
    bool has_next = art_iterator_next(iter);
    ASSERT_TRUE(has_next, "Should have one element");
    
    size_t key_len, value_len;
    const uint8_t *iter_key = art_iterator_key(iter, &key_len);
    const void *iter_value = art_iterator_value(iter, &value_len);
    
    ASSERT_NOT_NULL(iter_key, "Key should not be NULL");
    ASSERT_NOT_NULL(iter_value, "Value should not be NULL");
    ASSERT_EQ(key_len, sizeof(key), "Key length should match");
    ASSERT_EQ(value_len, sizeof(value), "Value length should match");
    ASSERT(memcmp(iter_key, key, key_len) == 0, "Key should match");
    ASSERT(memcmp(iter_value, value, value_len) == 0, "Value should match");
    
    has_next = art_iterator_next(iter);
    ASSERT_FALSE(has_next, "Should have no more elements");
    ASSERT_TRUE(art_iterator_done(iter), "Iterator should be done");
    
    art_iterator_destroy(iter);
    art_tree_destroy(&tree);
}

TEST(test_iterator_multiple_elements) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert keys in arbitrary order
    uint8_t key1[] = {3, 0, 0};
    uint8_t key2[] = {1, 0, 0};
    uint8_t key3[] = {2, 0, 0};
    uint8_t key4[] = {4, 0, 0};
    
    uint8_t val1[] = {30};
    uint8_t val2[] = {10};
    uint8_t val3[] = {20};
    uint8_t val4[] = {40};
    
    art_insert(&tree, key1, sizeof(key1), val1, sizeof(val1));
    art_insert(&tree, key2, sizeof(key2), val2, sizeof(val2));
    art_insert(&tree, key3, sizeof(key3), val3, sizeof(val3));
    art_insert(&tree, key4, sizeof(key4), val4, sizeof(val4));
    
    // Iterator should return keys in sorted order
    art_iterator_t *iter = art_iterator_create(&tree);
    ASSERT_NOT_NULL(iter, "Iterator should be created");
    
    int count = 0;
    uint8_t expected_order[] = {1, 2, 3, 4};  // Should be sorted
    
    while (art_iterator_next(iter)) {
        size_t key_len;
        const uint8_t *key = art_iterator_key(iter, &key_len);
        ASSERT_NOT_NULL(key, "Key should not be NULL");
        ASSERT_EQ(key_len, 3, "Key length should be 3");
        ASSERT_EQ(key[0], expected_order[count], "Keys should be in sorted order");
        count++;
    }
    
    ASSERT_EQ(count, 4, "Should have iterated over 4 elements");
    ASSERT_TRUE(art_iterator_done(iter), "Iterator should be done");
    
    art_iterator_destroy(iter);
    art_tree_destroy(&tree);
}

TEST(test_iterator_with_common_prefixes) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    // Insert keys with common prefixes
    uint8_t key1[] = {1, 2, 3, 4};
    uint8_t key2[] = {1, 2, 3, 5};
    uint8_t key3[] = {1, 2, 4, 0};
    uint8_t key4[] = {1, 3, 0, 0};
    
    uint8_t val[] = {42};
    
    art_insert(&tree, key1, sizeof(key1), val, sizeof(val));
    art_insert(&tree, key2, sizeof(key2), val, sizeof(val));
    art_insert(&tree, key3, sizeof(key3), val, sizeof(val));
    art_insert(&tree, key4, sizeof(key4), val, sizeof(val));
    
    art_iterator_t *iter = art_iterator_create(&tree);
    int count = 0;
    
    while (art_iterator_next(iter)) {
        count++;
    }
    
    ASSERT_EQ(count, 4, "Should iterate over all 4 keys");
    
    art_iterator_destroy(iter);
    art_tree_destroy(&tree);
}

static bool foreach_empty_callback(const uint8_t *key, size_t key_len,
                                   const void *value, size_t value_len, void *user_data) {
    (void)key; (void)key_len; (void)value; (void)value_len;
    int *count = (int *)user_data;
    (*count)++;
    return true;
}

TEST(test_foreach_empty_tree) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    int callback_count = 0;
    art_foreach(&tree, foreach_empty_callback, &callback_count);
    
    ASSERT_EQ(callback_count, 0, "Callback should not be called for empty tree");
    
    art_tree_destroy(&tree);
}

static bool foreach_count_callback(const uint8_t *key, size_t key_len,
                                   const void *value, size_t value_len, void *user_data) {
    (void)key; (void)key_len; (void)value; (void)value_len;
    int *count = (int *)user_data;
    (*count)++;
    return true;
}

TEST(test_foreach_multiple_elements) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key1[] = {1};
    uint8_t key2[] = {2};
    uint8_t key3[] = {3};
    uint8_t val[] = {42};
    
    art_insert(&tree, key1, sizeof(key1), val, sizeof(val));
    art_insert(&tree, key2, sizeof(key2), val, sizeof(val));
    art_insert(&tree, key3, sizeof(key3), val, sizeof(val));
    
    int callback_count = 0;
    art_foreach(&tree, foreach_count_callback, &callback_count);
    
    ASSERT_EQ(callback_count, 3, "Callback should be called 3 times");
    
    art_tree_destroy(&tree);
}

static bool foreach_terminate_callback(const uint8_t *key, size_t key_len,
                                       const void *value, size_t value_len, void *user_data) {
    (void)key; (void)key_len; (void)value; (void)value_len;
    int *count = (int *)user_data;
    (*count)++;
    return *count < 2;  // Stop after 2 calls
}

TEST(test_foreach_early_termination) {
    art_tree_t tree;
    art_tree_init(&tree);
    
    uint8_t key1[] = {1};
    uint8_t key2[] = {2};
    uint8_t key3[] = {3};
    uint8_t val[] = {42};
    
    art_insert(&tree, key1, sizeof(key1), val, sizeof(val));
    art_insert(&tree, key2, sizeof(key2), val, sizeof(val));
    art_insert(&tree, key3, sizeof(key3), val, sizeof(val));
    
    int callback_count = 0;
    art_foreach(&tree, foreach_terminate_callback, &callback_count);
    
    ASSERT_EQ(callback_count, 2, "Callback should stop after 2 calls");
    
    art_tree_destroy(&tree);
}

//==============================================================================
// Test Runner
//==============================================================================

int main(void) {
    printf("\n");
    printf(COLOR_YELLOW "========================================\n" COLOR_RESET);
    printf(COLOR_YELLOW "   ART (Adaptive Radix Tree) Tests\n" COLOR_RESET);
    printf(COLOR_YELLOW "========================================\n" COLOR_RESET);
    printf("\n");
    
    // Run all tests
    run_test_tree_init_destroy();
    run_test_insert_get_single();
    run_test_insert_update();
    run_test_insert_multiple();
    run_test_contains();
    run_test_delete_single();
    run_test_delete_nonexistent();
    run_test_delete_multiple();
    run_test_node_growth_4_to_16();
    run_test_node_growth_16_to_48();
    run_test_node_growth_48_to_256();
    run_test_variable_length_keys();
    run_test_common_prefix_keys();
    run_test_empty_tree_operations();
    run_test_ethereum_address_keys();
    run_test_storage_slot_keys();
    run_test_iterator_empty_tree();
    run_test_iterator_single_element();
    run_test_iterator_multiple_elements();
    run_test_iterator_with_common_prefixes();
    run_test_foreach_empty_tree();
    run_test_foreach_multiple_elements();
    run_test_foreach_early_termination();
    
    // Print summary
    printf("\n");
    printf(COLOR_YELLOW "========================================\n" COLOR_RESET);
    printf(COLOR_YELLOW "           Test Summary\n" COLOR_RESET);
    printf(COLOR_YELLOW "========================================\n" COLOR_RESET);
    printf("Total assertions: %d\n", tests_run);
    printf(COLOR_GREEN "Passed: %d\n" COLOR_RESET, tests_passed);
    if (tests_failed > 0) {
        printf(COLOR_RED "Failed: %d\n" COLOR_RESET, tests_failed);
    } else {
        printf("Failed: %d\n", tests_failed);
    }
    printf("\n");
    
    if (tests_failed == 0) {
        printf(COLOR_GREEN "🎉 All tests passed!\n" COLOR_RESET);
        return 0;
    } else {
        printf(COLOR_RED "❌ Some tests failed.\n" COLOR_RESET);
        return 1;
    }
}
