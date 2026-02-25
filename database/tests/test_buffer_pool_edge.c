/*
 * Buffer Pool Tests - Edge Cases
 * 
 * Tests edge cases and error handling:
 * - NULL parameter handling
 * - Invalid page IDs
 * - Concurrent operations (basic)
 * - Full cache behavior
 * - Empty cache operations
 * - Large capacity stress test
 */

#include "buffer_pool.h"
#include "page_manager.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

// ============================================================================
// Test Utilities
// ============================================================================

#define TEST_DB_FILE "/tmp/test_buffer_pool_edge.db"

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

/**
 * Helper: Allocate and initialize a page on disk
 */
static uint64_t alloc_and_write_page(page_manager_t *pm, uint8_t fill_byte) {
    uint64_t page_id = page_manager_alloc(pm, 0);
    if (page_id == 0) {
        return 0;
    }
    
    // Initialize page
    page_t page;
    memset(&page, 0, sizeof(page_t));
    page.header.page_id = page_id;
    page.header.write_counter = 0;
    memset(page.data, fill_byte, sizeof(page.data));
    
    // Compute checksum
    page_compute_checksum(&page);
    
    // Write to disk
    if (page_manager_write(pm, &page) != PAGE_SUCCESS) {
        return 0;
    }
    
    page_manager_sync(pm);
    return page_id;
}

// ============================================================================
// Edge Case Tests
// ============================================================================

void test_null_buffer_pool_operations(void) {
    TEST("null_buffer_pool_operations") {
        // All operations should handle NULL gracefully
        page_t *page = buffer_pool_get(NULL, 1);
        ASSERT(page == NULL, "get returns NULL");
        
        bool result = buffer_pool_pin(NULL, 1);
        ASSERT(!result, "pin returns false");
        
        result = buffer_pool_unpin(NULL, 1);
        ASSERT(!result, "unpin returns false");
        
        result = buffer_pool_mark_dirty(NULL, 1);
        ASSERT(!result, "mark_dirty returns false");
        
        result = buffer_pool_flush_page(NULL, 1);
        ASSERT(!result, "flush_page returns false");
        
        int flushed = buffer_pool_flush_all(NULL);
        ASSERT(flushed == -1, "flush_all returns -1");
        
        result = buffer_pool_evict(NULL, 1);
        ASSERT(!result, "evict returns false");
        
        result = buffer_pool_contains(NULL, 1);
        ASSERT(!result, "contains returns false");
        
        size_t mem = buffer_pool_memory_usage(NULL);
        ASSERT(mem == 0, "memory_usage returns 0");
        
        // Destroy should not crash
        buffer_pool_destroy(NULL);
        ASSERT(1, "destroy handles NULL");
    } TEST_END();
}

void test_null_page_manager(void) {
    TEST("null_page_manager") {
        buffer_pool_t *bp = buffer_pool_create(NULL, NULL);
        ASSERT(bp == NULL, "Create fails with NULL page manager");
    } TEST_END();
}

void test_invalid_page_id(void) {
    TEST("invalid_page_id") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Try to get page that doesn't exist
        page_t *page = buffer_pool_get(bp, 99999);
        ASSERT(page == NULL, "Cannot get non-existent page");
        
        // Pin/unpin non-existent page
        bool result = buffer_pool_pin(bp, 99999);
        ASSERT(!result, "Cannot pin non-existent page");
        
        result = buffer_pool_unpin(bp, 99999);
        ASSERT(!result, "Cannot unpin non-existent page");
        
        result = buffer_pool_mark_dirty(bp, 99999);
        ASSERT(!result, "Cannot mark non-existent page dirty");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_operations_on_not_cached_page(void) {
    TEST("operations_on_not_cached_page") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Allocate page but don't load it into cache
        uint64_t page_id = page_manager_alloc(pm, 0);
        
        // Try operations on page not in cache
        bool result = buffer_pool_pin(bp, page_id);
        ASSERT(!result, "Cannot pin page not in cache");
        
        result = buffer_pool_unpin(bp, page_id);
        ASSERT(!result, "Cannot unpin page not in cache");
        
        result = buffer_pool_mark_dirty(bp, page_id);
        ASSERT(!result, "Cannot mark page not in cache as dirty");
        
        result = buffer_pool_flush_page(bp, page_id);
        ASSERT(!result, "Cannot flush page not in cache");
        
        result = buffer_pool_evict(bp, page_id);
        ASSERT(!result, "Cannot evict page not in cache");
        
        ASSERT(!buffer_pool_contains(bp, page_id), "Page not in cache");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_zero_capacity(void) {
    TEST("zero_capacity") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 0,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        ASSERT(bp != NULL, "Buffer pool created with zero capacity");
        
        // Try to get a page (should fail - no room)
        uint64_t page_id = page_manager_alloc(pm, 0);
        page_t *page = buffer_pool_get(bp, page_id);
        ASSERT(page == NULL, "Cannot load page with zero capacity");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_single_capacity(void) {
    TEST("single_capacity") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 1,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        uint64_t page1 = alloc_and_write_page(pm, 0x00);
        uint64_t page2 = alloc_and_write_page(pm, 0x00);
        
        // Load first page
        page_t *p1 = buffer_pool_get(bp, page1);
        ASSERT(p1 != NULL, "First page loaded");
        
        // Load second page (should evict first)
        page_t *p2 = buffer_pool_get(bp, page2);
        ASSERT(p2 != NULL, "Second page loaded");
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 1, "Only one frame");
        ASSERT(stats.evictions == 1, "First page evicted");
        
        ASSERT(!buffer_pool_contains(bp, page1), "First page evicted");
        ASSERT(buffer_pool_contains(bp, page2), "Second page in cache");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_empty_cache_operations(void) {
    TEST("empty_cache_operations") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Operations on empty cache
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 0, "Cache empty");
        
        int flushed = buffer_pool_flush_all(bp);
        ASSERT(flushed == 0, "No pages to flush");
        
        buffer_pool_print_stats(bp);
        ASSERT(1, "Print stats on empty cache");
        
        size_t mem = buffer_pool_memory_usage(bp);
        ASSERT(mem > 0, "Memory usage accounts for struct");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_full_cache_all_pinned(void) {
    TEST("full_cache_all_pinned") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 3,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Fill cache and pin all
        uint64_t pages[3];
        for (int i = 0; i < 3; i++) {
            pages[i] = alloc_and_write_page(pm, 0x00);
            buffer_pool_get(bp, pages[i]);
            buffer_pool_pin(bp, pages[i]);
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 3, "All pages pinned");
        
        // Try to load new page (should fail)
        uint64_t new_page = alloc_and_write_page(pm, 0x00);
        page_t *p = buffer_pool_get(bp, new_page);
        ASSERT(p == NULL, "Cannot load when all pinned");
        
        // Unpin one page
        buffer_pool_unpin(bp, pages[0]);
        
        // Now should succeed
        p = buffer_pool_get(bp, new_page);
        ASSERT(p != NULL, "Can load after unpinning");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_multiple_pin_unpin_balance(void) {
    TEST("multiple_pin_unpin_balance") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page_id);
        
        // Pin 100 times
        for (int i = 0; i < 100; i++) {
            buffer_pool_pin(bp, page_id);
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.pins == 100, "100 pins recorded");
        ASSERT(stats.num_pinned == 1, "Still one pinned page");
        
        // Unpin 99 times
        for (int i = 0; i < 99; i++) {
            buffer_pool_unpin(bp, page_id);
        }
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.unpins == 99, "99 unpins recorded");
        ASSERT(stats.num_pinned == 1, "Still pinned (count=1)");
        
        // Unpin last time
        buffer_pool_unpin(bp, page_id);
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 0, "No longer pinned");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_dirty_page_persistence(void) {
    TEST("dirty_page_persistence") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        
        // Write pattern to usable data area (last 4 bytes reserved for torn page tail marker)
        page_t *page = buffer_pool_get(bp, page_id);
        for (int i = 0; i < (int)PAGE_DATA_SIZE; i++) {
            page->data[i] = (uint8_t)(i % 256);
        }
        buffer_pool_mark_dirty(bp, page_id);

        // Flush
        buffer_pool_flush_page(bp, page_id);

        // Evict page
        buffer_pool_evict(bp, page_id);

        // Load again and verify pattern
        page = buffer_pool_get(bp, page_id);
        ASSERT(page != NULL, "Page reloaded");

        bool pattern_correct = true;
        for (int i = 0; i < (int)PAGE_DATA_SIZE; i++) {
            if (page->data[i] != (uint8_t)(i % 256)) {
                pattern_correct = false;
                break;
            }
        }
        ASSERT(pattern_correct, "Data persisted correctly");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_large_capacity_stress(void) {
    TEST("large_capacity_stress") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 1000,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Load 500 pages
        uint64_t pages[500];
        for (int i = 0; i < 500; i++) {
            pages[i] = alloc_and_write_page(pm, 0x00);
            page_t *page = buffer_pool_get(bp, pages[i]);
            ASSERT(page != NULL, "Page loaded");
            
            // Mark half as dirty
            if (i % 2 == 0) {
                buffer_pool_mark_dirty(bp, pages[i]);
            }
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 500, "500 frames loaded");
        ASSERT(stats.num_dirty == 250, "250 dirty pages");
        ASSERT(stats.cache_misses == 500, "500 cache misses");
        
        // Access pages in random order (cache hits)
        for (int i = 0; i < 100; i++) {
            uint64_t idx = rand() % 500;
            buffer_pool_get(bp, pages[idx]);
        }
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_hits >= 100, "Cache hits recorded");
        
        // Flush all
        int flushed = buffer_pool_flush_all(bp);
        ASSERT(flushed == 250, "All dirty pages flushed");
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 0, "No dirty pages after flush");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_cache_hit_rate_calculation(void) {
    TEST("cache_hit_rate_calculation") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        
        // 1 miss, 9 hits = 90% hit rate
        buffer_pool_get(bp, page_id); // miss
        for (int i = 0; i < 9; i++) {
            buffer_pool_get(bp, page_id); // hit
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_hits == 9, "9 cache hits");
        ASSERT(stats.cache_misses == 1, "1 cache miss");
        ASSERT(stats.cache_hit_rate == 0.9, "90% hit rate");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_utilization_calculation(void) {
    TEST("utilization_calculation") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 100,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Load 50 pages = 50% utilization
        for (int i = 0; i < 50; i++) {
            uint64_t page_id = alloc_and_write_page(pm, 0x00);
            buffer_pool_get(bp, page_id);
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 50, "50 frames loaded");
        ASSERT(stats.utilization == 0.5, "50% utilization");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Basic Concurrency Test (Simple)
// ============================================================================

typedef struct {
    buffer_pool_t *bp;
    uint64_t *page_ids;
    int num_pages;
    int thread_id;
} thread_args_t;

void *thread_access_pages(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    
    // Each thread accesses pages 100 times
    for (int i = 0; i < 100; i++) {
        int idx = rand() % args->num_pages;
        buffer_pool_get(args->bp, args->page_ids[idx]);
    }
    
    return NULL;
}

void test_basic_concurrent_access(void) {
    TEST("basic_concurrent_access") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        
        buffer_pool_config_t config = {
            .capacity = 50,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Pre-allocate pages
        const int NUM_PAGES = 20;
        uint64_t page_ids[NUM_PAGES];
        for (int i = 0; i < NUM_PAGES; i++) {
            page_ids[i] = alloc_and_write_page(pm, 0x00);
            buffer_pool_get(bp, page_ids[i]);
        }
        
        // Create threads
        const int NUM_THREADS = 4;
        pthread_t threads[NUM_THREADS];
        thread_args_t args[NUM_THREADS];
        
        for (int i = 0; i < NUM_THREADS; i++) {
            args[i].bp = bp;
            args[i].page_ids = page_ids;
            args[i].num_pages = NUM_PAGES;
            args[i].thread_id = i;
            pthread_create(&threads[i], NULL, thread_access_pages, &args[i]);
        }
        
        // Wait for threads
        for (int i = 0; i < NUM_THREADS; i++) {
            pthread_join(threads[i], NULL);
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_hits > 0, "Concurrent accesses succeeded");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(void) {
    printf("==============================================\n");
    printf("  Buffer Pool Tests - Edge Cases\n");
    printf("==============================================\n");
    
    // Remove old test file
    remove(TEST_DB_FILE);
    
    // Run tests
    test_null_buffer_pool_operations();
    test_null_page_manager();
    test_invalid_page_id();
    test_operations_on_not_cached_page();
    test_zero_capacity();
    test_single_capacity();
    test_empty_cache_operations();
    test_full_cache_all_pinned();
    test_multiple_pin_unpin_balance();
    test_dirty_page_persistence();
    test_large_capacity_stress();
    test_cache_hit_rate_calculation();
    test_utilization_calculation();
    test_basic_concurrent_access();
    
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
