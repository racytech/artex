#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "mem_art.h"

int main() {
    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "Failed to initialize tree\n");
        return 1;
    }
    
    // Test case: two 1-byte keys that differ at position 0
    uint8_t key1[] = {0x2c};
    uint8_t key2[] = {0xab};
    uint64_t value1 = 1;
    uint64_t value2 = 2;
    
    printf("Inserting key1 [0x2c]...\n");
    if (!art_insert(&tree, key1, 1, &value1, sizeof(value1))) {
        fprintf(stderr, "Failed to insert key1\n");
        return 1;
    }
    
    printf("Inserting key2 [0xab]...\n");
    if (!art_insert(&tree, key2, 1, &value2, sizeof(value2))) {
        fprintf(stderr, "Failed to insert key2\n");
        return 1;
    }
    
    // Verify key1 can be retrieved before key3
    printf("\nVerifying key1 before inserting key3...\n");
    size_t vlen;
    const void *v1_before = art_get(&tree, key1, 1, &vlen);
    if (!v1_before || *(uint64_t*)v1_before != value1) {
        fprintf(stderr, "Failed to retrieve key1 BEFORE key3 insertion\n");
        return 1;
    }
    printf("✓ key1 retrieved successfully before key3\n");
    
    // Now try a third key that would require navigating via 0x00
    // Actually, let's try two keys where ONE is a prefix of the other
    uint8_t key3[] = {0x2c, 0x00, 0x01};
    uint64_t value3 = 3;
    
    printf("Inserting key3 [0x2c 0x00 0x01] (shares first byte with key1)...\n");
    if (!art_insert(&tree, key3, 3, &value3, sizeof(value3))) {
        fprintf(stderr, "Failed to insert key3\n");
        return 1;
    }
    
    printf("All inserts succeeded\n");
    
    // Verify all can be retrieved
    const void *v1 = art_get(&tree, key1, 1, &vlen);
    const void *v2 = art_get(&tree, key2, 1, &vlen);
    const void *v3 = art_get(&tree, key3, 3, &vlen);
    
    if (!v1 || *(uint64_t*)v1 != value1) {
        fprintf(stderr, "Failed to retrieve key1\n");
        return 1;
    }
    if (!v2 || *(uint64_t*)v2 != value2) {
        fprintf(stderr, "Failed to retrieve key2\n");
        return 1;
    }
    if (!v3 || *(uint64_t*)v3 != value3) {
        fprintf(stderr, "Failed to retrieve key3\n");
        return 1;
    }
    
    printf("All keys retrieved successfully\n");
    
    art_tree_destroy(&tree);
    return 0;
}
