/**
 * Focused delete test with explicit debug output to identify the bug
 */

#include "data_art.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define TEST_DB_PATH "/tmp/test_delete_debug.db"

int main(void) {
    // Enable debug logging
    log_set_level(LOG_LEVEL_DEBUG);
    
    // Clean up
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
    
    // Create page manager
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm);
    
    // Create buffer pool with very small capacity to force evictions
    buffer_pool_config_t bp_config = {
        .capacity = 5,  // Very small to trigger caching issues
        .enable_statistics = true
    };
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp);
    
    // Create tree
    data_art_tree_t *tree = data_art_create(pm, bp);
    assert(tree);
    
    printf("\n=== Testing Delete with Buffer Pool ===\n");
    printf("Buffer pool capacity: %zu\n\n", bp_config.capacity);
    
    // Insert 10 keys
    printf("Phase 1: INSERT 10 keys\n");
    for (int i = 0; i < 10; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(value, sizeof(value), "value_%03d", i);
        
        printf("  Insert key_%03d\n", i);
        assert(data_art_insert(tree, (uint8_t*)key, strlen(key), value, strlen(value)));
    }
    printf("Tree size after inserts: %zu\n\n", data_art_size(tree));
    
    // Verify all exist
    printf("Phase 2: VERIFY all 10 keys exist\n");
    for (int i = 0; i < 10; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", i);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (uint8_t*)key, strlen(key), &value_len);
        printf("  Get key_%03d: %s\n", i, retrieved ? "FOUND" : "NOT FOUND");
        assert(retrieved != NULL);
        free((void*)retrieved);
    }
    printf("\n");
    
    // Delete keys 2, 5, 7
    printf("Phase 3: DELETE keys 2, 5, 7\n");
    int to_delete[] = {2, 5, 7};
    for (int i = 0; i < 3; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", to_delete[i]);
        
        printf("  >>> Deleting key_%03d\n", to_delete[i]);
        printf("      Tree root before delete: page=%lu\n", tree->root.page_id);
        
        bool success = data_art_delete(tree, (uint8_t*)key, strlen(key));
        printf("      Delete result: %s\n", success ? "SUCCESS" : "FAILED");
        printf("      Tree root after delete: page=%lu\n", tree->root.page_id);
        printf("      Tree size after delete: %zu\n", data_art_size(tree));
        assert(success);
    }
    printf("\n");
    
    // Force buffer pool evictions by doing more operations
    printf("Phase 4: FORCE buffer pool evictions\n");
    for (int i = 10; i < 20; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%03d", i);
        snprintf(value, sizeof(value), "value_%03d", i);
        printf("  Insert key_%03d (to trigger buffer pool evictions)\n", i);
        assert(data_art_insert(tree, (uint8_t*)key, strlen(key), value, strlen(value)));
    }
    printf("\n");
    
    // Now check if deleted keys are really gone
    printf("Phase 5: VERIFY deleted keys are gone\n");
    for (int i = 0; i < 3; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", to_delete[i]);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (uint8_t*)key, strlen(key), &value_len);
        
        printf("  >>> Checking key_%03d: %s", to_delete[i], 
               retrieved ? "FOUND (BUG!)" : "NOT FOUND (correct)");
        
        if (retrieved) {
            printf(" - VALUE: %.*s\n", (int)value_len, (char*)retrieved);
            printf("\n*** BUG DETECTED ***\n");
            printf("Deleted key key_%03d still exists after buffer pool evictions!\n", to_delete[i]);
            printf("This suggests the page wasn't properly invalidated from the buffer pool.\n");
            free((void*)retrieved);
            
            // Print buffer pool stats
            buffer_pool_stats_t stats;
            buffer_pool_get_stats(bp, &stats);
            printf("\nBuffer Pool Stats:\n");
            printf("  Frames: %zu/%zu\n", stats.num_frames, stats.capacity);
            printf("  Cache hits: %lu\n", stats.cache_hits);
            printf("  Cache misses: %lu\n", stats.cache_misses);
            printf("  Evictions: %lu\n", stats.evictions);
            
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            return 1;
        }
        printf("\n");
    }
    
    // Verify remaining keys still exist
    printf("\nPhase 6: VERIFY non-deleted keys still exist\n");
    int expected_exist[] = {0, 1, 3, 4, 6, 8, 9};
    for (int i = 0; i < 7; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%03d", expected_exist[i]);
        
        size_t value_len;
        const void *retrieved = data_art_get(tree, (uint8_t*)key, strlen(key), &value_len);
        printf("  Get key_%03d: %s\n", expected_exist[i], retrieved ? "FOUND" : "NOT FOUND");
        assert(retrieved != NULL);
        free((void*)retrieved);
    }
    
    printf("\n✓✓✓ ALL TESTS PASSED ✓✓✓\n");
    
    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    return 0;
}
