#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "mem_art.h"

int main() {
    srand(12345);
    
    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "Failed to initialize tree\n");
        return 1;
    }
    
    printf("Simulating first 3 iterations with seed 12345...\n\n");
    
    // Simulate what happens in iterations 1-3
    int total_keys = 0;
    
    for (int iter = 1; iter <= 3; iter++) {
        printf("=== Iteration %d ===\n", iter);
        int batch_size = 100 + (rand() % 100);
        printf("Batch size: %d\n", batch_size);
        
        for (int i = 0; i < batch_size; i++) {
            uint8_t key[256];
            size_t key_len;
            
            int r = rand() % 100;
            if (r < 20) {
                key_len = 1 + (rand() % 3);
            } else if (r < 50) {
                key_len = 4 + (rand() % 7);
            } else if (r < 80) {
                key_len = 11 + (rand() % 40);
            } else {
                key_len = 51 + (rand() % 150);
            }
            
            for (size_t j = 0; j < key_len; j++) {
                key[j] = rand() % 256;
            }
            
            uint64_t value = total_keys + i;
            
            // Check if exists
            size_t vlen;
            const void *existing = art_get(&tree, key, key_len, &vlen);
            
            // Insert
            if (!art_insert(&tree, key, key_len, &value, sizeof(value))) {
                fprintf(stderr, "Failed to insert at iter=%d, i=%d\n", iter, i);
                return 1;
            }
            
            if (!existing) {
                // Track key 186 specifically
                if (value == 186) {
                    printf("  [i=%d] Inserted NEW key with value 186: len=%zu, bytes=", i, key_len);
                    for (size_t j = 0; j < key_len; j++) {
                        printf("%02x ", key[j]);
                    }
                    printf("\n");
                }
            } else {
                if (value == 186) {
                    printf("  [i=%d] Key 186 was an UPDATE (already existed)\n", i);
                }
            }
        }
        
        total_keys += batch_size;
        printf("Total keys processed: %d\n\n", total_keys);
    }
    
    // Now try to retrieve key [0x60]
    printf("=== Attempting to retrieve [0x60] ===\n");
    uint8_t test_key[] = {0x60};
    size_t vlen;
    const void *result = art_get(&tree, test_key, 1, &vlen);
    
    if (result) {
        printf("✓ Found [0x60], value = %lu\n", *(uint64_t*)result);
    } else {
        printf("✗ Key [0x60] NOT FOUND\n");
    }
    
    art_tree_destroy(&tree);
    return result ? 0 : 1;
}
