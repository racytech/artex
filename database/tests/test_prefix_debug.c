#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "data_art.h"
#include "page_manager.h"

static void generate_prefix_key(uint8_t *key, size_t max_len, const char *prefix, uint64_t num) {
    snprintf((char *)key, max_len, "%s_%020lu", prefix, num);
}

int main() {
    const char *db_path = "/tmp/test_prefix.db";
    remove(db_path);
    
    page_manager_t *pm = page_manager_create(db_path, false);
    data_art_tree_t *tree = data_art_create(pm, NULL, NULL, 32);
    
    // Insert just a few keys
    for (int p = 0; p < 2; p++) {
        char prefix[32];
        snprintf(prefix, sizeof(prefix), "prefix_%02d", p);
        
        for (int i = 0; i < 3; i++) {
            uint8_t key[128];
            generate_prefix_key(key, sizeof(key), prefix, i);
            size_t key_len = strlen((char *)key);
            
            char value[64];
            snprintf(value, sizeof(value), "value_p%d_i%d", p, i);
            
            printf("Inserting: '%s' -> '%s'\n", (char*)key, value);
            if (!data_art_insert(tree, key, key_len, value, strlen(value))) {
                fprintf(stderr, "Failed to insert!\n");
                return 1;
            }
        }
    }
    
    printf("\nTree size after inserts: %zu\n\n", data_art_size(tree));
    
    // Try to retrieve
    for (int p = 0; p < 2; p++) {
        char prefix[32];
        snprintf(prefix, sizeof(prefix), "prefix_%02d", p);
        
        for (int i = 0; i < 3; i++) {
            uint8_t key[128];
            generate_prefix_key(key, sizeof(key), prefix, i);
            size_t key_len = strlen((char *)key);
            
            size_t value_len;
            const void *retrieved = data_art_get(tree, key, key_len, &value_len);
            
            if (!retrieved) {
                fprintf(stderr, "FAILED TO FIND: '%s'\n", (char*)key);
            } else {
                printf("Found: '%s' -> '%.*s'\n", (char*)key, (int)value_len, (char*)retrieved);
                free((void *)retrieved);
            }
        }
    }
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    remove(db_path);
    
    return 0;
}
