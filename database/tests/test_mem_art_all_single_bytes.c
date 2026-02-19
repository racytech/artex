#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mem_art.h"

int main() {
    printf("Testing 256 single-byte keys (all possible bytes)...\n");
    
    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "Failed to initialize tree\n");
        return 1;
    }
    
    uint64_t values[256];
    
    // Insert all 256 possible single-byte keys
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        values[i] = i * 100;
        
        if (!art_insert(&tree, &key, 1, &values[i], sizeof(uint64_t))) {
            fprintf(stderr, "Failed to insert key [0x%02x]\n", key);
            return 1;
        }
    }
    
    printf("All 256 keys inserted\n");
    
    // Verify all can be retrieved
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        size_t vlen;
        const void *retrieved = art_get(&tree, &key, 1, &vlen);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key [0x%02x]\n", key);
            return 1;
        }
        
        if (vlen != sizeof(uint64_t) || *(uint64_t*)retrieved != values[i]) {
            fprintf(stderr, "Value mismatch for key [0x%02x]: expected %lu, got %lu\n",
                    key, values[i], *(uint64_t*)retrieved);
            return 1;
        }
    }
    
    printf("All 256 keys retrieved successfully\n");
    
    // Update all values
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        values[i] = i * 200;  // New value
        
        if (!art_insert(&tree, &key, 1, &values[i], sizeof(uint64_t))) {
            fprintf(stderr, "Failed to update key [0x%02x]\n", key);
            return 1;
        }
    }
    
    printf("All 256 keys updated\n");
    
    // Verify updates
    for (int i = 0; i < 256; i++) {
        uint8_t key = (uint8_t)i;
        size_t vlen;
        const void *retrieved = art_get(&tree, &key, 1, &vlen);
        
        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve updated key [0x%02x]\n", key);
            return 1;
        }
        
        if (vlen != sizeof(uint64_t) || *(uint64_t*)retrieved != values[i]) {
            fprintf(stderr, "Update value mismatch for key [0x%02x]: expected %lu, got %lu\n",
                    key, values[i], *(uint64_t*)retrieved);
            return 1;
        }
    }
    
    printf("All 256 updated values verified\n");
    printf("\n✓ All tests passed!\n");
    
    art_tree_destroy(&tree);
    return 0;
}
