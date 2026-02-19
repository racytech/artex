#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "mem_art.h"

// Test that specifically stresses prefix cases that previously failed
int main(int argc, char **argv) {
    unsigned int seed = (argc > 1) ? atoi(argv[1]) : (unsigned int)time(NULL);
    int duration_sec = (argc > 2) ? atoi(argv[2]) : 30;
    
    printf("=== mem_art Prefix Stress Test ===\n");
    printf("Seed: %u\n", seed);
    printf("Duration: %d seconds\n\n", duration_sec);
    
    srand(seed);
    
    art_tree_t tree;
    if (!art_tree_init(&tree)) {
        fprintf(stderr, "Failed to initialize tree\n");
        return 1;
    }
    
    typedef struct {
        uint8_t key[256];
        size_t key_len;
        uint64_t value;
    } entry_t;
    
    entry_t *entries = NULL;
    size_t num_entries = 0;
    size_t capacity = 0;
    
    time_t start = time(NULL);
    int iteration = 0;
    size_t total_inserts = 0;
    
    while (time(NULL) - start < duration_sec) {
        iteration++;
        
        // Generate keys with various prefix patterns
        int batch_size = 100 + (rand() % 100);
        
        for (int i = 0; i < batch_size; i++) {
            // Generate a temporary key
            uint8_t temp_key[256];
            size_t temp_key_len;
            
            // Generate keys with various patterns:
            // 20% - very short (1-3 bytes)
            // 30% - short (4-10 bytes)  
            // 30% - medium (11-50 bytes)
            // 20% - long (51-200 bytes)
            int r = rand() % 100;
            if (r < 20) {
                temp_key_len = 1 + (rand() % 3);
            } else if (r < 50) {
                temp_key_len = 4 + (rand() % 7);
            } else if (r < 80) {
                temp_key_len = 11 + (rand() % 40);
            } else {
                temp_key_len = 51 + (rand() % 150);
            }
            
            // Fill with random bytes (including 0x00!)
            for (size_t j = 0; j < temp_key_len; j++) {
                temp_key[j] = rand() % 256;
            }
            
            uint64_t value = total_inserts + i;
            
            // Check if this key already exists
            size_t existing_vlen;
            const void *existing = art_get(&tree, temp_key, temp_key_len, &existing_vlen);
            bool is_update = (existing != NULL);
            
            // Insert or update
            if (!art_insert(&tree, temp_key, temp_key_len, &value, sizeof(value))) {
                fprintf(stderr, "Failed to insert key at iteration %d, index %d\n", iteration, i);
                fprintf(stderr, "Key length: %zu, Value: %lu\n", temp_key_len, value);
                return 1;
            }
            
            if (is_update) {
                // It's an update - find and update the old entry
                for (size_t j = 0; j < num_entries; j++) {
                    if (entries[j].key_len == temp_key_len &&
                        memcmp(entries[j].key, temp_key, temp_key_len) == 0) {
                        // Update the value in our tracking array
                        entries[j].value = value;
                        break;
                    }
                }
                // Don't add to tracking array for updates
                continue;
            }
            
            // It's a new key - add to tracking array
            // Expand array if needed
            if (num_entries >= capacity) {
                capacity = capacity == 0 ? 1000 : capacity * 2;
                entries = realloc(entries, capacity * sizeof(entry_t));
                if (!entries) {
                    fprintf(stderr, "Out of memory\n");
                    return 1;
                }
            }
            
            entry_t *entry = &entries[num_entries];
            memcpy(entry->key, temp_key, temp_key_len);
            entry->key_len = temp_key_len;
            entry->value = value;
            
            num_entries++;
        }
        
        total_inserts += batch_size;
        
        // Verify all entries can be retrieved
        for (size_t i = 0; i < num_entries; i++) {
            size_t vlen;
            const void *retrieved = art_get(&tree, entries[i].key, entries[i].key_len, &vlen);
            if (!retrieved) {
                fprintf(stderr, "\n=== FAILURE ===\n");
                fprintf(stderr, "Iteration: %d\n", iteration);
                fprintf(stderr, "Failed to retrieve key index %zu\n", i);
                fprintf(stderr, "Key length: %zu\n", entries[i].key_len);
                fprintf(stderr, "Key bytes: ");
                for (size_t j = 0; j < entries[i].key_len; j++) {
                    fprintf(stderr, "%02x ", entries[i].key[j]);
                }
                fprintf(stderr, "\n");
                fprintf(stderr, "Expected value: %lu\n", entries[i].value);
                fprintf(stderr, "Total entries in tracking: %zu\n", num_entries);
                
                // Try to find if there's a duplicate in our tracking
                int dup_count = 0;
                for (size_t k = 0; k < num_entries; k++) {
                    if (entries[k].key_len == entries[i].key_len &&
                        memcmp(entries[k].key, entries[i].key, entries[i].key_len) == 0) {
                        dup_count++;
                        if (dup_count > 1) {
                            fprintf(stderr, "WARNING: Found duplicate at index %zu with value %lu\n", 
                                    k, entries[k].value);
                        }
                    }
                }
                return 1;
            }
            
            if (vlen != sizeof(uint64_t) || *(uint64_t*)retrieved != entries[i].value) {
                fprintf(stderr, "\n=== VALUE MISMATCH ===\n");
                fprintf(stderr, "Iteration: %d, Index: %zu\n", iteration, i);
                fprintf(stderr, "Key length: %zu\n", entries[i].key_len);
                fprintf(stderr, "Key bytes: ");
                for (size_t j = 0; j < entries[i].key_len; j++) {
                    fprintf(stderr, "%02x ", entries[i].key[j]);
                }
                fprintf(stderr, "\n");
                fprintf(stderr, "Expected value: %lu, Got: %lu\n", entries[i].value, *(uint64_t*)retrieved);
                
                // Check if there's another key with this value
                for (size_t k = 0; k < num_entries; k++) {
                    if (k != i && entries[k].value == *(uint64_t*)retrieved) {
                        fprintf(stderr, "This value belongs to key index %zu:\n", k);
                        fprintf(stderr, "  Key length: %zu\n", entries[k].key_len);
                        fprintf(stderr, "  Key bytes: ");
                        for (size_t j = 0; j < entries[k].key_len; j++) {
                            fprintf(stderr, "%02x ", entries[k].key[j]);
                        }
                        fprintf(stderr, "\n");
                        break;
                    }
                }
                return 1;
            }
        }
        
        if (iteration % 10 == 0) {
            printf("\rIteration %d: %zu keys", iteration, num_entries);
            fflush(stdout);
        }
    }
    
    printf("\n\n=== SUCCESS ===\n");
    printf("Total iterations: %d\n", iteration);
    printf("Total keys: %zu\n", num_entries);
    printf("All keys inserted and retrieved successfully!\n");
    
    art_tree_destroy(&tree);
    free(entries);
    
    return 0;
}
