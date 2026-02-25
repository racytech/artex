/**
 * Simple test to reproduce the delete bug
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_DIR "/tmp/test_delete_simple"
#define TEST_ART_PATH TEST_DIR "/art.dat"

int main(void) {
    // Clean up any previous test data
    system("rm -rf " TEST_DIR " && mkdir -p " TEST_DIR);

    // Create tree
    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    if (!tree) {
        fprintf(stderr, "Failed to create tree\n");
        return 1;
    }

    printf("=== Testing Simple Insert/Delete ===\n\n");

    // Insert 3 keys
    const char *keys[] = {"key1", "key2", "key3"};
    const char *values[] = {"value1", "value2", "value3"};

    printf("Phase 1: INSERT 3 keys\n");
    for (int i = 0; i < 3; i++) {
        bool success = data_art_insert(tree, (const uint8_t *)keys[i],
                                      strlen(keys[i]), values[i], strlen(values[i]));
        printf("  Insert '%s': %s\n", keys[i], success ? "OK" : "FAIL");
        assert(success);
    }

    printf("\nPhase 2: VERIFY all 3 keys exist\n");
    for (int i = 0; i < 3; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, (const uint8_t *)keys[i],
                                            strlen(keys[i]), &value_len);
        printf("  Get '%s': %s\n", keys[i], retrieved ? "FOUND" : "NOT FOUND");
        assert(retrieved != NULL);
        assert(value_len == strlen(values[i]));
        assert(memcmp(retrieved, values[i], value_len) == 0);
        free((void *)retrieved);
    }

    printf("\nPhase 3: DELETE 'key2'\n");
    bool deleted = data_art_delete(tree, (const uint8_t *)"key2", strlen("key2"));
    printf("  Delete 'key2': %s\n", deleted ? "OK" : "FAIL");
    assert(deleted);

    printf("\nPhase 4: VERIFY 'key2' is gone, others remain\n");
    // key1 should exist
    size_t value_len;
    const void *retrieved = data_art_get(tree, (const uint8_t *)"key1",
                                        strlen("key1"), &value_len);
    printf("  Get 'key1': %s\n", retrieved ? "FOUND" : "NOT FOUND");
    assert(retrieved != NULL);
    free((void *)retrieved);

    // key2 should NOT exist
    retrieved = data_art_get(tree, (const uint8_t *)"key2", strlen("key2"), &value_len);
    printf("  Get 'key2': %s\n", retrieved ? "FOUND (BUG!)" : "NOT FOUND (correct)");
    if (retrieved != NULL) {
        fprintf(stderr, "\n✗✗✗ BUG DETECTED ✗✗✗\n");
        fprintf(stderr, "Deleted key 'key2' still exists!\n");
        free((void *)retrieved);
        data_art_destroy(tree);
        return 1;
    }

    // key3 should exist
    retrieved = data_art_get(tree, (const uint8_t *)"key3", strlen("key3"), &value_len);
    printf("  Get 'key3': %s\n", retrieved ? "FOUND" : "NOT FOUND");
    assert(retrieved != NULL);
    free((void *)retrieved);

    printf("\n✓✓✓ ALL TESTS PASSED ✓✓✓\n");

    data_art_destroy(tree);
    return 0;
}
