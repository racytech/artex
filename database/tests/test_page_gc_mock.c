/**
 * Page GC Tests - Reference Counting and Dead Page Tracking
 * 
 * Tests core page GC operations:
 * - Reference counting (incref/decref/get)
 * - Dead page marking and detection
 * - Dead page statistics
 * - Dead page collection
 * - Thread safety (atomic operations)
 * - Edge cases (ref_count overflow, double decref, etc.)
 */

#include "page_gc.h"
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

#define TEST_DB_FILE "/tmp/test_page_gc.db"

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

// ============================================================================
// Mock Page Manager (Stub Implementation)
// ============================================================================

// Simple in-memory page index for testing
typedef struct {
    page_gc_metadata_t *entries;
    size_t count;
    size_t capacity;
    uint64_t total_file_size;
    uint64_t dead_bytes;
} mock_page_index_t;

static mock_page_index_t *mock_index = NULL;

// Mock implementation of page_manager functions
struct page_gc_metadata *page_manager_get_metadata(page_manager_t *pm, uint64_t page_id) {
    (void)pm;  // Unused
    
    if (!mock_index) {
        return NULL;
    }
    
    // Linear search (fine for testing)
    for (size_t i = 0; i < mock_index->count; i++) {
        if (mock_index->entries[i].page_id == page_id) {
            return &mock_index->entries[i];
        }
    }
    
    return NULL;
}

struct page_gc_metadata *page_manager_get_metadata_by_index(page_manager_t *pm, size_t index) {
    (void)pm;  // Unused
    
    if (!mock_index || index >= mock_index->count) {
        return NULL;
    }
    
    return &mock_index->entries[index];
}

size_t page_manager_get_num_pages(page_manager_t *pm) {
    (void)pm;  // Unused
    return mock_index ? mock_index->count : 0;
}

uint64_t page_manager_get_total_file_size(page_manager_t *pm) {
    (void)pm;  // Unused
    return mock_index ? mock_index->total_file_size : 0;
}

void page_manager_update_dead_stats(page_manager_t *pm, uint32_t dead_bytes) {
    (void)pm;  // Unused
    if (mock_index) {
        mock_index->dead_bytes += dead_bytes;
    }
}

// Helper: Create mock page index
static void mock_create_index(size_t initial_capacity) {
    mock_index = malloc(sizeof(mock_page_index_t));
    mock_index->entries = calloc(initial_capacity, sizeof(page_gc_metadata_t));
    mock_index->count = 0;
    mock_index->capacity = initial_capacity;
    mock_index->total_file_size = 0;
    mock_index->dead_bytes = 0;
}

// Helper: Destroy mock page index
static void mock_destroy_index(void) {
    if (mock_index) {
        free(mock_index->entries);
        free(mock_index);
        mock_index = NULL;
    }
}

// Helper: Add page to mock index
static void mock_add_page(uint64_t page_id, uint32_t compressed_size) {
    if (!mock_index || mock_index->count >= mock_index->capacity) {
        return;
    }
    
    page_gc_metadata_t *entry = &mock_index->entries[mock_index->count++];
    entry->page_id = page_id;
    entry->file_offset = mock_index->total_file_size;
    entry->compressed_size = compressed_size;
    entry->compression_type = 0;  // NONE
    entry->checksum = 0;
    entry->version = 1;
    entry->ref_count = 0;
    entry->flags = 0;
    entry->last_access_time = 0;
    
    mock_index->total_file_size += compressed_size;
}

// ============================================================================
// Test Cases
// ============================================================================

void test_page_gc_init_ref(void) {
    TEST("page_gc_init_ref") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;  // Mock doesn't need real pm
        
        // Initialize ref count
        bool init = page_gc_init_ref(pm, 1);
        ASSERT(init, "Init ref succeeded");
        
        // Verify ref_count = 1
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 1, "Ref count initialized to 1");
        
        // Verify flags cleared
        page_gc_metadata_t *meta = page_manager_get_metadata(pm, 1);
        ASSERT(meta->flags == 0, "Flags cleared");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_incref(void) {
    TEST("page_gc_incref") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        // Initialize
        page_gc_init_ref(pm, 1);
        
        // Increment multiple times
        uint32_t ref = page_gc_incref(pm, 1);
        ASSERT(ref == 2, "First incref: 1->2");
        
        ref = page_gc_incref(pm, 1);
        ASSERT(ref == 3, "Second incref: 2->3");
        
        ref = page_gc_incref(pm, 1);
        ASSERT(ref == 4, "Third incref: 3->4");
        
        // Verify final count
        ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 4, "Final ref_count is 4");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_decref(void) {
    TEST("page_gc_decref") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        // Initialize to 3
        page_gc_init_ref(pm, 1);
        page_gc_incref(pm, 1);
        page_gc_incref(pm, 1);
        
        // Decrement
        uint32_t ref = page_gc_decref(pm, 1);
        ASSERT(ref == 2, "First decref: 3->2");
        
        ref = page_gc_decref(pm, 1);
        ASSERT(ref == 1, "Second decref: 2->1");
        
        // Page not dead yet
        bool is_dead = page_gc_is_dead(pm, 1);
        ASSERT(!is_dead, "Page not dead at ref_count=1");
        
        // Final decrement should mark as dead
        ref = page_gc_decref(pm, 1);
        ASSERT(ref == 0, "Final decref: 1->0");
        
        // Should be marked dead
        is_dead = page_gc_is_dead(pm, 1);
        ASSERT(is_dead, "Page marked dead at ref_count=0");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_decref_at_zero(void) {
    TEST("page_gc_decref at zero") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        // Initialize ref_count but manually set to 0
        page_gc_init_ref(pm, 1);
        page_gc_metadata_t *meta = page_manager_get_metadata(pm, 1);
        meta->ref_count = 0;
        
        // Attempt to decref at 0 should fail gracefully
        uint32_t ref = page_gc_decref(pm, 1);
        ASSERT(ref == 0, "Decref at 0 returns 0");
        
        // Ref count should still be 0
        ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 0, "Ref count remains 0");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_mark_dead(void) {
    TEST("page_gc_mark_dead") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        page_gc_init_ref(pm, 1);
        
        // Manually set ref_count to 0
        page_gc_metadata_t *meta = page_manager_get_metadata(pm, 1);
        meta->ref_count = 0;
        
        // Mark dead
        bool marked = page_gc_mark_dead(pm, 1);
        ASSERT(marked, "Mark dead succeeded");
        
        // Verify DEAD flag set
        bool is_dead = page_gc_is_dead(pm, 1);
        ASSERT(is_dead, "DEAD flag is set");
        
        // Verify dead bytes updated
        ASSERT(mock_index->dead_bytes == 1000, "Dead bytes updated");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_mark_dead_with_refcount(void) {
    TEST("page_gc_mark_dead with ref_count > 0") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        page_gc_init_ref(pm, 1);  // ref_count = 1
        
        // Attempt to mark dead with ref_count > 0 should fail
        bool marked = page_gc_mark_dead(pm, 1);
        ASSERT(!marked, "Mark dead fails with ref_count > 0");
        
        // Should not be marked dead
        bool is_dead = page_gc_is_dead(pm, 1);
        ASSERT(!is_dead, "DEAD flag not set");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_dead_stats(void) {
    TEST("page_gc_get_dead_stats") {
        mock_create_index(10);
        
        // Add 5 pages
        mock_add_page(1, 1000);
        mock_add_page(2, 1500);
        mock_add_page(3, 2000);
        mock_add_page(4, 800);
        mock_add_page(5, 1200);
        
        page_manager_t *pm = NULL;
        
        // Initialize all pages
        for (uint64_t i = 1; i <= 5; i++) {
            page_gc_init_ref(pm, i);
        }
        
        // Mark pages 2, 4, 5 as dead (decref to 0)
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
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_collect_dead_pages(void) {
    TEST("page_gc_collect_dead_pages") {
        mock_create_index(10);
        
        // Add 6 pages
        for (uint64_t i = 1; i <= 6; i++) {
            mock_add_page(i, 1000);
            page_manager_t *pm = NULL;
            page_gc_init_ref(pm, i);
        }
        
        page_manager_t *pm = NULL;
        
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
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_collect_no_dead_pages(void) {
    TEST("page_gc_collect_dead_pages with no dead pages") {
        mock_create_index(10);
        
        // Add 3 pages, all alive
        for (uint64_t i = 1; i <= 3; i++) {
            mock_add_page(i, 1000);
            page_manager_t *pm = NULL;
            page_gc_init_ref(pm, i);
        }
        
        page_manager_t *pm = NULL;
        
        // Collect dead pages
        size_t count = 0;
        uint64_t *dead_pages = page_gc_collect_dead_pages(pm, &count);
        
        ASSERT(dead_pages == NULL, "No dead pages array");
        ASSERT(count == 0, "Count is 0");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_nonexistent_page(void) {
    TEST("page_gc operations on non-existent page") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        
        // Operations on non-existent page should fail gracefully
        bool init = page_gc_init_ref(pm, 999);
        ASSERT(!init, "Init ref fails for non-existent page");
        
        uint32_t ref = page_gc_incref(pm, 999);
        ASSERT(ref == 0, "Incref returns 0 for non-existent page");
        
        ref = page_gc_decref(pm, 999);
        ASSERT(ref == 0, "Decref returns 0 for non-existent page");
        
        ref = page_gc_get_refcount(pm, 999);
        ASSERT(ref == 0, "Get refcount returns 0 for non-existent page");
        
        bool is_dead = page_gc_is_dead(pm, 999);
        ASSERT(!is_dead, "Non-existent page not marked dead");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_multiple_inc_dec_cycles(void) {
    TEST("page_gc multiple inc/dec cycles") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        page_gc_init_ref(pm, 1);
        
        // Cycle 1: inc to 5, dec to 2
        for (int i = 0; i < 4; i++) page_gc_incref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 5, "Ref count = 5");
        
        for (int i = 0; i < 3; i++) page_gc_decref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 2, "Ref count = 2");
        ASSERT(!page_gc_is_dead(pm, 1), "Not dead at 2");
        
        // Cycle 2: inc to 10, dec to 0
        for (int i = 0; i < 8; i++) page_gc_incref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 10, "Ref count = 10");
        
        for (int i = 0; i < 10; i++) page_gc_decref(pm, 1);
        ASSERT(page_gc_get_refcount(pm, 1) == 0, "Ref count = 0");
        ASSERT(page_gc_is_dead(pm, 1), "Dead at 0");
        
        mock_destroy_index();
    } TEST_END();
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

typedef struct {
    page_manager_t *pm;
    uint64_t page_id;
    int iterations;
} thread_test_args_t;

static void *thread_incref(void *arg) {
    thread_test_args_t *args = (thread_test_args_t *)arg;
    
    for (int i = 0; i < args->iterations; i++) {
        page_gc_incref(args->pm, args->page_id);
    }
    
    return NULL;
}

static void *thread_decref(void *arg) {
    thread_test_args_t *args = (thread_test_args_t *)arg;
    
    for (int i = 0; i < args->iterations; i++) {
        page_gc_decref(args->pm, args->page_id);
    }
    
    return NULL;
}

void test_page_gc_thread_safety_incref(void) {
    TEST("page_gc thread safety - concurrent incref") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        page_gc_init_ref(pm, 1);
        
        // Start 4 threads, each increments 1000 times
        pthread_t threads[4];
        thread_test_args_t args = {
            .pm = pm,
            .page_id = 1,
            .iterations = 1000
        };
        
        for (int i = 0; i < 4; i++) {
            pthread_create(&threads[i], NULL, thread_incref, &args);
        }
        
        for (int i = 0; i < 4; i++) {
            pthread_join(threads[i], NULL);
        }
        
        // Should be 1 (init) + 4000 (4 threads × 1000) = 4001
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 4001, "Ref count = 4001 after concurrent increments");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_thread_safety_mixed(void) {
    TEST("page_gc thread safety - mixed inc/dec") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        page_gc_init_ref(pm, 1);
        
        // Increment to 2000 first
        for (int i = 0; i < 1999; i++) {
            page_gc_incref(pm, 1);
        }
        
        // Start 2 inc threads and 2 dec threads
        pthread_t threads[4];
        thread_test_args_t args = {
            .pm = pm,
            .page_id = 1,
            .iterations = 500
        };
        
        pthread_create(&threads[0], NULL, thread_incref, &args);
        pthread_create(&threads[1], NULL, thread_incref, &args);
        pthread_create(&threads[2], NULL, thread_decref, &args);
        pthread_create(&threads[3], NULL, thread_decref, &args);
        
        for (int i = 0; i < 4; i++) {
            pthread_join(threads[i], NULL);
        }
        
        // Should be 2000 + 1000 (inc) - 1000 (dec) = 2000
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 2000, "Ref count = 2000 after mixed operations");
        
        mock_destroy_index();
    } TEST_END();
}

// ============================================================================
// Edge Cases
// ============================================================================

void test_page_gc_large_refcount(void) {
    TEST("page_gc large ref_count") {
        mock_create_index(10);
        mock_add_page(1, 1000);
        
        page_manager_t *pm = NULL;
        page_gc_init_ref(pm, 1);
        
        // Increment to large number (10,000)
        for (int i = 0; i < 9999; i++) {
            page_gc_incref(pm, 1);
        }
        
        uint32_t ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 10000, "Ref count = 10,000");
        
        // Decrement back to 0
        for (int i = 0; i < 10000; i++) {
            page_gc_decref(pm, 1);
        }
        
        ref = page_gc_get_refcount(pm, 1);
        ASSERT(ref == 0, "Ref count back to 0");
        ASSERT(page_gc_is_dead(pm, 1), "Page marked dead");
        
        mock_destroy_index();
    } TEST_END();
}

void test_page_gc_empty_index(void) {
    TEST("page_gc with empty index") {
        mock_create_index(10);
        // Don't add any pages
        
        page_manager_t *pm = NULL;
        
        dead_page_stats_t stats;
        page_gc_get_dead_stats(pm, &stats);
        
        ASSERT(stats.total_pages == 0, "Total pages = 0");
        ASSERT(stats.live_pages == 0, "Live pages = 0");
        ASSERT(stats.dead_pages == 0, "Dead pages = 0");
        ASSERT(stats.dead_bytes == 0, "Dead bytes = 0");
        ASSERT(stats.fragmentation_pct == 0.0, "Fragmentation = 0%");
        
        size_t count = 0;
        uint64_t *dead_pages = page_gc_collect_dead_pages(pm, &count);
        ASSERT(dead_pages == NULL, "No dead pages");
        ASSERT(count == 0, "Count = 0");
        
        mock_destroy_index();
    } TEST_END();
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main(void) {
    printf("==============================================\n");
    printf("  Page GC Tests - Reference Counting\n");
    printf("==============================================\n");
    
    // Basic operations
    test_page_gc_init_ref();
    test_page_gc_incref();
    test_page_gc_decref();
    test_page_gc_decref_at_zero();
    
    // Dead page marking
    test_page_gc_mark_dead();
    test_page_gc_mark_dead_with_refcount();
    
    // Statistics and collection
    test_page_gc_dead_stats();
    test_page_gc_collect_dead_pages();
    test_page_gc_collect_no_dead_pages();
    
    // Error handling
    test_page_gc_nonexistent_page();
    test_page_gc_multiple_inc_dec_cycles();
    
    // Thread safety
    test_page_gc_thread_safety_incref();
    test_page_gc_thread_safety_mixed();
    
    // Edge cases
    test_page_gc_large_refcount();
    test_page_gc_empty_index();
    
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
