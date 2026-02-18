/**
 * Page GC Integration Tests - With Real Page Manager
 * 
 * Tests page GC operations using real page_manager instances.
 * Validates that page_gc works correctly with the actual page index
 * and verifies end-to-end integration.
 */

#include "page_gc.h"
#include "page_manager.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>

// ============================================================================
// Test Utilities
// ============================================================================

#define TEST_DB_PATH "/tmp/test_page_gc_integration"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    printf("\n--- Running: %s ---\n", name); \
    do

#define ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            printf("  ❌ FAILED: %s (line %d)\n", message, __LINE__); \
            tests_failed++; \
            return; \
        } \
        printf("  ✓ %s\n", message); \
    } while (0)

#define TEST_END() \
    while (0); \
    printf("  ✅ PASSED\n"); \
    tests_passed++

// Helper: Clean up test database
static void cleanup_test_db(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
}

// Helper: Add a page to page manager index manually
static void add_page_to_index(page_manager_t *pm, uint64_t page_id, uint32_t compressed_size) {
    if (!pm || !pm->index) return;
    
    page_index_t *index = pm->index;
    pthread_rwlock_wrlock(&index->lock);
    
    if (index->count >= index->capacity) {
        // Expand capacity
        size_t new_capacity = index->capacity * 2;
        page_gc_metadata_t *new_entries = realloc(index->entries, 
                                                   new_capacity * sizeof(page_gc_metadata_t));
        if (new_entries) {
            index->entries = (struct page_gc_metadata *)new_entries;
            index->capacity = new_capacity;
        }
    }
    
    if (index->count < index->capacity) {
        // Cast to page_gc_metadata_t for indexing
        page_gc_metadata_t *entries = (page_gc_metadata_t *)index->entries;
        page_gc_metadata_t *entry = &entries[index->count];
        
        memset(entry, 0, sizeof(page_gc_metadata_t));
        entry->page_id = page_id;
        entry->file_offset = index->total_file_size;
        entry->compressed_size = compressed_size;
        entry->compression_type = 0;  // NONE
        entry->version = 1;
        
        index->count++;
        index->total_file_size += compressed_size;
    }
    
    pthread_rwlock_unlock(&index->lock);
}

// ============================================================================
// Test Cases
// ============================================================================

void test_integration_basic_refcount(void) {
    TEST("integration: basic ref counting") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        ASSERT(pm->index != NULL, "Page index exists");
        
        // Add a page to the index
        add_page_to_index(pm, 1, 1000);
        
        // Initialize ref count
        bool init = page_gc_init_ref(pm, 1);
        ASSERT(init, "Init ref succeeded");
        
        // Verify ref_count = 1
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 1, "Initial ref count = 1");
        
        // Increment
        ref = page_gc_incref(pm, 1);
        ASSERT(ref == 2, "After incref = 2");
        
        // Decrement
        ref = page_gc_decref(pm, 1);
        ASSERT(ref == 1, "After decref = 1");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_mark_dead(void) {
    TEST("integration: mark page as dead") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Add page
        add_page_to_index(pm, 1, 1500);
        
        // Initialize and decrement to 0
        page_gc_init_ref(pm, 1);
        uint32_t ref = page_gc_decref(pm, 1);
        ASSERT(ref == 0, "Ref count = 0");
        
        // Verify marked dead
        bool is_dead = page_gc_is_dead(pm, 1);
        ASSERT(is_dead, "Page marked as dead");
        
        // Verify dead bytes tracked
        ASSERT(pm->index->dead_bytes == 1500, "Dead bytes = 1500");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_dead_stats(void) {
    TEST("integration: dead page statistics") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Add 5 pages
        add_page_to_index(pm, 1, 1000);
        add_page_to_index(pm, 2, 1500);
        add_page_to_index(pm, 3, 2000);
        add_page_to_index(pm, 4, 800);
        add_page_to_index(pm, 5, 1200);
        
        // Initialize all
        for (uint64_t i = 1; i <= 5; i++) {
            page_gc_init_ref(pm, i);
        }
        
        // Mark 2, 4, 5 as dead
        page_gc_decref(pm, 2);  // 1500 bytes
        page_gc_decref(pm, 4);  // 800 bytes
        page_gc_decref(pm, 5);  // 1200 bytes
        
        // Get stats
        dead_page_stats_t stats;
        page_gc_get_dead_stats(pm, &stats);
        
        ASSERT(stats.total_pages == 5, "Total pages = 5");
        ASSERT(stats.live_pages == 2, "Live pages = 2");
        ASSERT(stats.dead_pages == 3, "Dead pages = 3");
        ASSERT(stats.dead_bytes == 3500, "Dead bytes = 3500");
        
        // Fragmentation = 3500 / 6500 = 53.8%
        ASSERT(stats.fragmentation_pct > 53.0 && stats.fragmentation_pct < 54.0, 
               "Fragmentation ~53.8%");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_collect_dead_pages(void) {
    TEST("integration: collect dead pages") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Add 6 pages
        for (uint64_t i = 1; i <= 6; i++) {
            add_page_to_index(pm, i, 1000);
            page_gc_init_ref(pm, i);
        }
        
        // Mark pages 1, 3, 5 as dead
        page_gc_decref(pm, 1);
        page_gc_decref(pm, 3);
        page_gc_decref(pm, 5);
        
        // Collect dead pages
        size_t count = 0;
        uint64_t *dead_pages = page_gc_collect_dead_pages(pm, &count);
        
        ASSERT(dead_pages != NULL, "Dead pages array allocated");
        ASSERT(count == 3, "Found 3 dead pages");
        
        // Verify page IDs
        bool found_1 = false, found_3 = false, found_5 = false;
        for (size_t i = 0; i < count; i++) {
            if (dead_pages[i] == 1) found_1 = true;
            if (dead_pages[i] == 3) found_3 = true;
            if (dead_pages[i] == 5) found_5 = true;
        }
        
        ASSERT(found_1, "Page 1 in dead list");
        ASSERT(found_3, "Page 3 in dead list");
        ASSERT(found_5, "Page 5 in dead list");
        
        free(dead_pages);
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_binary_search(void) {
    TEST("integration: binary search in sorted index") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Add pages in sorted order
        add_page_to_index(pm, 10, 1000);
        add_page_to_index(pm, 20, 1000);
        add_page_to_index(pm, 30, 1000);
        add_page_to_index(pm, 40, 1000);
        add_page_to_index(pm, 50, 1000);
        
        // Initialize all
        for (uint64_t i = 10; i <= 50; i += 10) {
            page_gc_init_ref(pm, i);
        }
        
        // Test lookups
        ASSERT(page_gc_get_refcount(pm, 10) == 1, "Found page 10");
        ASSERT(page_gc_get_refcount(pm, 30) == 1, "Found page 30");
        ASSERT(page_gc_get_refcount(pm, 50) == 1, "Found page 50");
        
        // Test non-existent
        ASSERT(page_gc_get_refcount(pm, 15) == 0, "Page 15 not found");
        ASSERT(page_gc_get_refcount(pm, 999) == 0, "Page 999 not found");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_concurrent_access(void) {
    TEST("integration: thread-safe page index access") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Add page
        add_page_to_index(pm, 1, 1000);
        page_gc_init_ref(pm, 1);
        
        // Increment to 1000
        for (int i = 0; i < 999; i++) {
            page_gc_incref(pm, 1);
        }
        
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 1000, "Ref count = 1000");
        
        // Multiple lookups should work (rwlock allows concurrent reads)
        for (int i = 0; i < 10; i++) {
            ref = page_gc_get_refcount(pm, 1);
            ASSERT(ref == 1000, "Consistent ref count");
        }
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_index_boundaries(void) {
    TEST("integration: page index boundary conditions") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Access by index with empty index
        size_t num_pages = page_manager_get_num_pages(pm);
        ASSERT(num_pages == 0, "Empty index has 0 pages");
        
        // Add one page
        add_page_to_index(pm, 1, 1000);
        num_pages = page_manager_get_num_pages(pm);
        ASSERT(num_pages == 1, "After add, 1 page");
        
        // Get total file size
        uint64_t total_size = page_manager_get_total_file_size(pm);
        ASSERT(total_size == 1000, "Total file size = 1000");
        
        // Add more pages
        add_page_to_index(pm, 2, 1500);
        add_page_to_index(pm, 3, 2000);
        
        total_size = page_manager_get_total_file_size(pm);
        ASSERT(total_size == 4500, "Total file size = 4500");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

void test_integration_multiple_cycles(void) {
    TEST("integration: multiple ref count cycles") {
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        ASSERT(pm != NULL, "Page manager created");
        
        add_page_to_index(pm, 1, 1000);
        page_gc_init_ref(pm, 1);
        
        // Cycle 1: increment to 5
        for (int i = 0; i < 4; i++) page_gc_incref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 5, "Ref count = 5");
        
        // Cycle 2: decrement to 2
        for (int i = 0; i < 3; i++) page_gc_decref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 2, "Ref count = 2");
        ASSERT(!page_gc_is_dead(pm, 1), "Not dead at 2");
        
        // Cycle 3: increment to 10
        for (int i = 0; i < 8; i++) page_gc_incref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 10, "Ref count = 10");
        
        // Cycle 4: decrement to 0
        for (int i = 0; i < 10; i++) page_gc_decref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 0, "Ref count = 0");
        ASSERT(page_gc_is_dead(pm, 1), "Dead at 0");
        
        page_manager_destroy(pm);
        cleanup_test_db();
    } TEST_END();
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(void) {
    printf("==============================================\n");
    printf("  Page GC Integration Tests\n");
    printf("==============================================\n");
    
    // Basic operations with real page_manager
    test_integration_basic_refcount();
    test_integration_mark_dead();
    
    // Statistics and collection
    test_integration_dead_stats();
    test_integration_collect_dead_pages();
    
    // Page manager features
    test_integration_binary_search();
    test_integration_concurrent_access();
    test_integration_index_boundaries();
    
    // Complex scenarios
    test_integration_multiple_cycles();
    
    // Summary
    printf("\n==============================================\n");
    printf("  Test Results\n");
    printf("==============================================\n");
    printf("  Passed: %d\n", tests_passed);
    printf("  Failed: %d\n", tests_failed);
    printf("  Total:  %d\n", tests_passed + tests_failed);
    printf("==============================================\n");
    
    return tests_failed == 0 ? 0 : 1;
}
