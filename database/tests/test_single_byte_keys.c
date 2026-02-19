#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "data_art.h"

int main() {
    printf("Testing single-byte key handling...\n\n");
    
    // Create tree
    data_art_tree_t *tree = data_art_create("/tmp/test_single_byte.db", false);
    if (!tree) {
        fprintf(stderr, "Failed to create tree\n");
        return 1;
    }
    
    // Test 1: Insert and retrieve a single-byte key
    printf("Test 1: Single-byte key [0x2c]\n");
    uint8_t key1[] = {0x2c};
    const char *value1 = "value_for_0x2c";
    
    if (!data_art_insert(tree, key1, 1, value1, strlen(value1))) {
        fprintf(stderr, "FAIL: Could not insert key\n");
        return 1;
    }
    printf("  Inserted key [0x2c]\n");
    
    size_t len;
    const void *retrieved = data_art_get(tree, key1, 1, &len);
    if (!retrieved) {
        fprintf(stderr, "FAIL: Could not retrieve key [0x2c]\n");
        return 1;
    }
    printf("  Retrieved key [0x2c]: %.*s\n", (int)len, (char*)retrieved);
    free((void*)retrieved);
    
    // Test 2: Insert another single-byte key
    printf("\nTest 2: Insert second single-byte key [0xab]\n");
    uint8_t key2[] = {0xab};
    const char *value2 = "value_for_0xab";
    
    if (!data_art_insert(tree, key2, 1, value2, strlen(value2))) {
        fprintf(stderr, "FAIL: Could not insert second key\n");
        return 1;
    }
    printf("  Inserted key [0xab]\n");
    
    // Verify both keys exist
    printf("\nVerifying both keys...\n");
    retrieved = data_art_get(tree, key1, 1, &len);
    if (!retrieved) {
        fprintf(stderr, "FAIL: Could not retrieve key [0x2c] after inserting [0xab]\n");
        return 1;
    }
    printf("  Key [0x2c] still exists: %.*s\n", (int)len, (char*)retrieved);
    free((void*)retrieved);
    
    retrieved = data_art_get(tree, key2, 1, &len);
    if (!retrieved) {
        fprintf(stderr, "FAIL: Could not retrieve key [0xab]\n");
        return 1;
    }
    printf("  Key [0xab] exists: %.*s\n", (int)len, (char*)retrieved);
    free((void*)retrieved);
    
    // Test 3: Insert a multi-byte key starting with 0x2c
    printf("\nTest 3: Insert multi-byte key [0x2c, 0x00]\n");
    uint8_t key3[] = {0x2c, 0x00};
    const char *value3 = "value_for_0x2c_0x00";
    
    if (!data_art_insert(tree, key3, 2, value3, strlen(value3))) {
        fprintf(stderr, "FAIL: Could not insert key [0x2c, 0x00]\n");
        return 1;
    }
    printf("  Inserted key [0x2c, 0x00]\n");
    
    // Verify all three keys
    printf("\nVerifying all three keys...\n");
    retrieved = data_art_get(tree, key1, 1, &len);
    if (!retrieved) {
        fprintf(stderr, "FAIL: Could not retrieve key [0x2c] after inserting [0x2c, 0x00]\n");
        return 1;
    }
    printf("  Key [0x2c] (1 byte) exists: %.*s\n", (int)len, (char*)retrieved);
    free((void*)retrieved);
    
    retrieved = data_art_get(tree, key3, 2, &len);
    if (!retrieved) {
        fprintf(stderr, "FAIL: Could not retrieve key [0x2c, 0x00]\n");
        return 1;
    }
    printf("  Key [0x2c, 0x00] (2 bytes) exists: %.*s\n", (int)len, (char*)retrieved);
    free((void*)retrieved);
    
    data_art_destroy(tree);
    printf("\n✓ All tests passed!\n");
    return 0;
}
