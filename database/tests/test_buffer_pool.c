/*
 * Buffer Pool Tests - Basic Functionality
 * 
 * Tests core buffer pool operations:
 * - Creation and destruction
 * - Page get (cache hit/miss)
 * - Pin/unpin mechanism
 * - Dirty page tracking
 * - LRU eviction
 * - Statistics tracking
 */

#include "buffer_pool.h"
#include "page_manager.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// ============================================================================
// Test Utilities
// ============================================================================

#define TEST_DB_FILE "/tmp/test_buffer_pool.db"
#define TEST_CAPACITY 10

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
// Test Cases
// ============================================================================

void test_buffer_pool_create_destroy(void) {
    TEST("buffer_pool_create_destroy") {
        // Create page manager
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Create buffer pool with default config
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        ASSERT(bp != NULL, "Buffer pool created");
        
        // Verify initial state
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.capacity == 10000, "Default capacity is 10000");
        ASSERT(stats.num_frames == 0, "No frames initially");
        ASSERT(stats.cache_hits == 0, "No cache hits initially");
        ASSERT(stats.cache_misses == 0, "No cache misses initially");
        
        // Destroy
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        ASSERT(1, "Buffer pool destroyed");
    } TEST_END();
}

void test_buffer_pool_create_custom_config(void) {
    TEST("buffer_pool_create_custom_config") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Page manager created");
        
        // Create with custom config
        buffer_pool_config_t config = {
            .capacity = TEST_CAPACITY,
            .enable_statistics = true,
        };
        
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        ASSERT(bp != NULL, "Buffer pool created with custom config");
        
        // Verify config
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.capacity == TEST_CAPACITY, "Custom capacity applied");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_get_cache_miss(void) {
    TEST("buffer_pool_get_cache_miss") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Allocate and initialize a page
        uint64_t page_id = alloc_and_write_page(pm, 0xAB);
        ASSERT(page_id != 0, "Page allocated and written");
        
        // Get page (cache miss)
        page_t *page = buffer_pool_get(bp, page_id);
        ASSERT(page != NULL, "Page retrieved from buffer pool");
        ASSERT(page->data[0] == 0xAB, "Page content correct");
        
        // Check stats
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_misses == 1, "Cache miss recorded");
        ASSERT(stats.cache_hits == 0, "No cache hits yet");
        ASSERT(stats.num_frames == 1, "One frame in cache");
        ASSERT(stats.total_loads == 1, "One page loaded");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_get_cache_hit(void) {
    TEST("buffer_pool_get_cache_hit") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        
        // First access (miss)
        page_t *page1 = buffer_pool_get(bp, page_id);
        ASSERT(page1 != NULL, "First access successful");
        
        // Second access (hit)
        page_t *page2 = buffer_pool_get(bp, page_id);
        ASSERT(page2 != NULL, "Second access successful");
        ASSERT(page1 == page2, "Same page pointer returned");
        
        // Check stats
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_misses == 1, "One cache miss");
        ASSERT(stats.cache_hits == 1, "One cache hit");
        ASSERT(stats.cache_hit_rate == 0.5, "50% hit rate");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_pin_unpin(void) {
    TEST("buffer_pool_pin_unpin") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_config_t config = {.capacity = TEST_CAPACITY, .enable_statistics = true};
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        page_t *page = buffer_pool_get(bp, page_id);
        ASSERT(page != NULL, "Page loaded");
        
        // Pin page
        bool pinned = buffer_pool_pin(bp, page_id);
        ASSERT(pinned, "Page pinned");
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 1, "One pinned page");
        ASSERT(stats.pins == 1, "Pin counter incremented");
        
        // Pin again
        pinned = buffer_pool_pin(bp, page_id);
        ASSERT(pinned, "Page pinned again");
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 1, "Still one pinned page");
        ASSERT(stats.pins == 2, "Pin counter incremented");
        
        // Unpin once
        bool unpinned = buffer_pool_unpin(bp, page_id);
        ASSERT(unpinned, "Page unpinned");
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 1, "Still pinned (count > 0)");
        ASSERT(stats.unpins == 1, "Unpin counter incremented");
        
        // Unpin again
        unpinned = buffer_pool_unpin(bp, page_id);
        ASSERT(unpinned, "Page unpinned again");
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_pinned == 0, "No pinned pages");
        ASSERT(stats.unpins == 2, "Unpin counter incremented");
        
        // Cannot unpin when already at 0
        unpinned = buffer_pool_unpin(bp, page_id);
        ASSERT(!unpinned, "Cannot unpin at count 0");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_mark_dirty(void) {
    TEST("buffer_pool_mark_dirty") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        page_t *page = buffer_pool_get(bp, page_id);
        ASSERT(page != NULL, "Page loaded");
        
        // Initially not dirty
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 0, "No dirty pages");
        
        // Mark dirty
        bool marked = buffer_pool_mark_dirty(bp, page_id);
        ASSERT(marked, "Page marked dirty");
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 1, "One dirty page");
        
        // Mark dirty again (should stay at 1)
        marked = buffer_pool_mark_dirty(bp, page_id);
        ASSERT(marked, "Page marked dirty again");
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 1, "Still one dirty page");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_flush_page(void) {
    TEST("buffer_pool_flush_page") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        page_t *page = buffer_pool_get(bp, page_id);
        ASSERT(page != NULL, "Page loaded");
        
        // Modify page
        memset(page->data, 0xCC, sizeof(page->data));
        buffer_pool_mark_dirty(bp, page_id);
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 1, "Page is dirty");
        
        // Flush page
        bool flushed = buffer_pool_flush_page(bp, page_id);
        ASSERT(flushed, "Page flushed");
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 0, "No dirty pages after flush");
        ASSERT(stats.total_flushes == 1, "Flush counter incremented");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_flush_all(void) {
    TEST("buffer_pool_flush_all") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Create and dirty 5 pages
        uint64_t page_ids[5];
        for (int i = 0; i < 5; i++) {
            page_ids[i] = alloc_and_write_page(pm, 0x00);
            page_t *page = buffer_pool_get(bp, page_ids[i]);
            ASSERT(page != NULL, "Page loaded");
            memset(page->data, 0xDD + i, sizeof(page->data));
            buffer_pool_mark_dirty(bp, page_ids[i]);
        }
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 5, "Five dirty pages");
        
        // Flush all
        int flushed = buffer_pool_flush_all(bp);
        ASSERT(flushed == 5, "All pages flushed");
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_dirty == 0, "No dirty pages after flush_all");
        ASSERT(stats.total_flushes == 5, "Flush counter correct");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_lru_eviction(void) {
    TEST("buffer_pool_lru_eviction") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_config_t config = {.capacity = 3, .enable_statistics = true};
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Fill cache (capacity = 3)
        uint64_t page1 = alloc_and_write_page(pm, 0x00);
        uint64_t page2 = alloc_and_write_page(pm, 0x00);
        uint64_t page3 = alloc_and_write_page(pm, 0x00);
        
        buffer_pool_get(bp, page1);
        buffer_pool_get(bp, page2);
        buffer_pool_get(bp, page3);
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 3, "Cache full");
        
        // Access page1 again (moves to head)
        buffer_pool_get(bp, page1);
        
        // Allocate new page (should evict page2, the LRU)
        uint64_t page4 = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page4);
        
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.num_frames == 3, "Cache still at capacity");
        ASSERT(stats.evictions == 1, "One eviction occurred");
        ASSERT(stats.evictions_clean == 1, "Clean page evicted");
        
        // Verify page2 not in cache, others are
        ASSERT(buffer_pool_contains(bp, page1), "page1 in cache");
        ASSERT(!buffer_pool_contains(bp, page2), "page2 evicted");
        ASSERT(buffer_pool_contains(bp, page3), "page3 in cache");
        ASSERT(buffer_pool_contains(bp, page4), "page4 in cache");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_evict_dirty_page(void) {
    TEST("buffer_pool_evict_dirty_page") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_config_t config = {.capacity = 2, .enable_statistics = true};
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Fill cache and make page1 dirty
        uint64_t page1 = alloc_and_write_page(pm, 0x00);
        uint64_t page2 = alloc_and_write_page(pm, 0x00);
        
        page_t *p1 = buffer_pool_get(bp, page1);
        memset(p1->data, 0xEE, sizeof(p1->data));
        buffer_pool_mark_dirty(bp, page1);
        
        buffer_pool_get(bp, page2);
        
        // Allocate new page (should evict and flush page1)
        uint64_t page3 = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page3);
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.evictions == 1, "One eviction");
        ASSERT(stats.evictions_dirty == 1, "Dirty page evicted");
        ASSERT(stats.total_flushes == 1, "Page flushed during eviction");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_cannot_evict_pinned(void) {
    TEST("buffer_pool_cannot_evict_pinned") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_config_t config = {.capacity = 2, .enable_statistics = true};
        buffer_pool_t *bp = buffer_pool_create(&config, pm);
        
        // Fill cache and pin both pages
        uint64_t page1 = alloc_and_write_page(pm, 0x00);
        uint64_t page2 = alloc_and_write_page(pm, 0x00);
        
        buffer_pool_get(bp, page1);
        buffer_pool_get(bp, page2);
        
        buffer_pool_pin(bp, page1);
        buffer_pool_pin(bp, page2);
        
        // Try to allocate new page (should fail - all pinned)
        uint64_t page3 = alloc_and_write_page(pm, 0x00);
        page_t *p3 = buffer_pool_get(bp, page3);
        ASSERT(p3 == NULL, "Cannot load page when all frames pinned");
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.evictions == 0, "No evictions (all pinned)");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_manual_evict(void) {
    TEST("buffer_pool_manual_evict") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page_id);
        
        ASSERT(buffer_pool_contains(bp, page_id), "Page in cache");
        
        // Manual evict
        bool evicted = buffer_pool_evict(bp, page_id);
        ASSERT(evicted, "Page evicted");
        ASSERT(!buffer_pool_contains(bp, page_id), "Page not in cache");
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.evictions == 1, "Eviction recorded");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_manual_evict_pinned_fails(void) {
    TEST("buffer_pool_manual_evict_pinned_fails") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page_id);
        buffer_pool_pin(bp, page_id);
        
        // Cannot evict pinned page
        bool evicted = buffer_pool_evict(bp, page_id);
        ASSERT(!evicted, "Cannot evict pinned page");
        ASSERT(buffer_pool_contains(bp, page_id), "Page still in cache");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_reset_stats(void) {
    TEST("buffer_pool_reset_stats") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Generate some activity
        uint64_t page_id = alloc_and_write_page(pm, 0x00);
        buffer_pool_get(bp, page_id);
        buffer_pool_get(bp, page_id);
        buffer_pool_mark_dirty(bp, page_id);
        
        buffer_pool_stats_t stats;
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_hits > 0, "Stats recorded");
        
        // Reset
        buffer_pool_reset_stats(bp);
        buffer_pool_get_stats(bp, &stats);
        ASSERT(stats.cache_hits == 0, "Cache hits reset");
        ASSERT(stats.cache_misses == 0, "Cache misses reset");
        ASSERT(stats.capacity > 0, "Capacity preserved");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

void test_buffer_pool_memory_usage(void) {
    TEST("buffer_pool_memory_usage") {
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        size_t initial = buffer_pool_memory_usage(bp);
        ASSERT(initial > 0, "Initial memory usage non-zero");
        
        // Add 10 pages
        for (int i = 0; i < 10; i++) {
            uint64_t page_id = alloc_and_write_page(pm, 0x00);
            buffer_pool_get(bp, page_id);
        }
        
        size_t after = buffer_pool_memory_usage(bp);
        ASSERT(after > initial, "Memory usage increased");
        
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Test: Buffer Pool Reload
// ============================================================================

void test_buffer_pool_reload(void) {
    TEST("buffer_pool_reload") {
        // Setup
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Created page manager");
        
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        ASSERT(bp != NULL, "Created buffer pool");
        
        // Allocate and write a page
        uint64_t page_id = page_manager_alloc(pm, 100);
        ASSERT(page_id > 0, "Allocated page");
        
        page_t test_page;
        memset(&test_page, 0, sizeof(test_page));
        test_page.header.page_id = page_id;
        strcpy((char *)test_page.data, "Version 1");
        page_compute_checksum(&test_page);
        page_manager_write(pm, &test_page);
        
        // Load into buffer pool
        page_t *cached = buffer_pool_get(bp, page_id);
        ASSERT(cached != NULL, "Page loaded into buffer pool");
        ASSERT(strcmp((char *)cached->data, "Version 1") == 0, "Cache has Version 1");
        
        // Write new version directly to disk (bypassing buffer pool)
        strcpy((char *)test_page.data, "Version 2");
        page_compute_checksum(&test_page);
        page_manager_write(pm, &test_page);
        
        // Cache still has old version
        cached = buffer_pool_get(bp, page_id);
        ASSERT(strcmp((char *)cached->data, "Version 1") == 0, "Cache still has Version 1");
        
        // Reload from disk
        bool reloaded = buffer_pool_reload(bp, page_id);
        ASSERT(reloaded, "Page reloaded successfully");
        
        // Cache should now have new version
        cached = buffer_pool_get(bp, page_id);
        ASSERT(strcmp((char *)cached->data, "Version 2") == 0, "Cache updated to Version 2");
        
        // Cleanup
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Test: Buffer Pool Invalidate
// ============================================================================

void test_buffer_pool_invalidate(void) {
    TEST("buffer_pool_invalidate") {
        // Setup
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Created page manager");
        
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        ASSERT(bp != NULL, "Created buffer pool");
        
        // Allocate and load a page
        uint64_t page_id = page_manager_alloc(pm, 100);
        page_t test_page;
        memset(&test_page, 0, sizeof(test_page));
        test_page.header.page_id = page_id;
        strcpy((char *)test_page.data, "Test Data");
        page_compute_checksum(&test_page);
        page_manager_write(pm, &test_page);
        
        page_t *cached = buffer_pool_get(bp, page_id);
        ASSERT(cached != NULL, "Page loaded");
        ASSERT(buffer_pool_contains(bp, page_id), "Page in cache");
        
        // Invalidate
        bool invalidated = buffer_pool_invalidate(bp, page_id);
        ASSERT(invalidated, "Page invalidated");
        
        // Should no longer be in cache
        ASSERT(!buffer_pool_contains(bp, page_id), "Page removed from cache");
        
        // Getting it again should reload from disk
        cached = buffer_pool_get(bp, page_id);
        ASSERT(cached != NULL, "Page reloaded from disk");
        ASSERT(strcmp((char *)cached->data, "Test Data") == 0, "Data correct after reload");
        
        // Cleanup
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Test: Buffer Pool Clear
// ============================================================================

void test_buffer_pool_clear(void) {
    TEST("buffer_pool_clear") {
        // Setup
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Created page manager");
        
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        ASSERT(bp != NULL, "Created buffer pool");
        
        // Load 5 pages into cache
        uint64_t page_ids[5];
        for (int i = 0; i < 5; i++) {
            page_ids[i] = page_manager_alloc(pm, 100);
            page_t test_page;
            memset(&test_page, 0, sizeof(test_page));
            test_page.header.page_id = page_ids[i];
            sprintf((char *)test_page.data, "Page %d", i);
            page_compute_checksum(&test_page);
            page_manager_write(pm, &test_page);
            
            page_t *cached = buffer_pool_get(bp, page_ids[i]);
            ASSERT(cached != NULL, "Page loaded");
        }
        
        // Verify all are in cache
        for (int i = 0; i < 5; i++) {
            ASSERT(buffer_pool_contains(bp, page_ids[i]), "Page in cache");
        }
        
        // Clear all pages
        int cleared = buffer_pool_clear(bp);
        ASSERT(cleared == 5, "All 5 pages cleared");
        
        // Verify all are removed from cache
        for (int i = 0; i < 5; i++) {
            ASSERT(!buffer_pool_contains(bp, page_ids[i]), "Page removed from cache");
        }
        
        // Pages can still be loaded from disk
        page_t *cached = buffer_pool_get(bp, page_ids[0]);
        ASSERT(cached != NULL, "Page 0 reloaded from disk");
        ASSERT(strcmp((char *)cached->data, "Page 0") == 0, "Data correct after clear");
        
        // Cleanup
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Test: Clear Flushes Dirty Pages
// ============================================================================

void test_buffer_pool_clear_flushes_dirty(void) {
    TEST("buffer_pool_clear_flushes_dirty") {
        // Setup
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        ASSERT(pm != NULL, "Created page manager");
        
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        ASSERT(bp != NULL, "Created buffer pool");
        
        // Allocate a page
        uint64_t page_id = page_manager_alloc(pm, 100);
        page_t test_page;
        memset(&test_page, 0, sizeof(test_page));
        test_page.header.page_id = page_id;
        strcpy((char *)test_page.data, "Original");
        page_compute_checksum(&test_page);
        page_manager_write(pm, &test_page);
        
        // Load and modify
        page_t *cached = buffer_pool_get(bp, page_id);
        strcpy((char *)cached->data, "Modified");
        page_compute_checksum(cached);
        buffer_pool_mark_dirty(bp, page_id);
        
        // Clear (should flush dirty page)
        int cleared = buffer_pool_clear(bp);
        ASSERT(cleared == 1, "One page cleared");
        
        // Destroy and recreate to verify persistence
        buffer_pool_destroy(bp);
        bp = buffer_pool_create(NULL, pm);
        
        // Reload and verify modification persisted
        cached = buffer_pool_get(bp, page_id);
        ASSERT(strcmp((char *)cached->data, "Modified") == 0, "Dirty page flushed on clear");
        
        // Cleanup
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Test: Reload Handles Non-Existent Page
// ============================================================================

void test_buffer_pool_reload_nonexistent(void) {
    TEST("buffer_pool_reload on non-existent page") {
        // Setup
        page_manager_t *pm = page_manager_create(TEST_DB_FILE, false);
        buffer_pool_t *bp = buffer_pool_create(NULL, pm);
        
        // Try to reload a page that doesn't exist in cache
        // Should succeed since there's nothing to reload
        bool reloaded = buffer_pool_reload(bp, 99999);
        ASSERT(reloaded, "Reload succeeds for page not in cache (no-op)");
        
        // Cleanup
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
    } TEST_END();
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(void) {
    printf("==============================================\n");
    printf("  Buffer Pool Tests - Basic Functionality\n");
    printf("==============================================\n");
    
    // Remove old test file
    remove(TEST_DB_FILE);
    
    // Run tests
    test_buffer_pool_create_destroy();
    test_buffer_pool_create_custom_config();
    test_buffer_pool_get_cache_miss();
    test_buffer_pool_get_cache_hit();
    test_buffer_pool_pin_unpin();
    test_buffer_pool_mark_dirty();
    test_buffer_pool_flush_page();
    test_buffer_pool_flush_all();
    test_buffer_pool_lru_eviction();
    test_buffer_pool_evict_dirty_page();
    test_buffer_pool_cannot_evict_pinned();
    test_buffer_pool_manual_evict();
    test_buffer_pool_manual_evict_pinned_fails();
    test_buffer_pool_reset_stats();
    test_buffer_pool_memory_usage();
    
    // Cache coherency tests
    test_buffer_pool_reload();
    test_buffer_pool_invalidate();
    test_buffer_pool_clear();
    test_buffer_pool_clear_flushes_dirty();
    test_buffer_pool_reload_nonexistent();
    
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
