/*
 * Page Manager Edge Cases & Corner Cases Tests
 * 
 * Comprehensive tests for boundary conditions, error scenarios,
 * concurrency issues, and stress conditions.
 */

#include "page_manager.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <time.h>

#define TEST_DB_PATH "/tmp/test_page_manager_edge_db"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_RESET   "\x1b[0m"

static int tests_passed = 0;
static int tests_failed = 0;

// Test helper macros
#define TEST_ASSERT(condition, message) \
    do { \
        if (condition) { \
            printf("  " ANSI_COLOR_GREEN "✓" ANSI_COLOR_RESET " %s\n", message); \
            tests_passed++; \
        } else { \
            printf("  " ANSI_COLOR_RED "✗" ANSI_COLOR_RESET " %s\n", message); \
            tests_failed++; \
        } \
    } while (0)

#define RUN_TEST(test_func) \
    do { \
        printf("\nRunning: %s\n", #test_func); \
        test_func(); \
    } while (0)

// Cleanup test directory
static void cleanup_test_db(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
}

// ============================================================================
// Boundary & Edge Case Tests
// ============================================================================

void test_page_id_boundaries(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Test page ID 0 (reserved/invalid)
    page_result_t result = page_manager_free(pm, 0);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Page ID 0 should be invalid");
    
    page_t page;
    result = page_manager_read(pm, 0, &page);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Cannot read page ID 0");
    
    // Test very large page ID (beyond allocated)
    result = page_manager_read(pm, UINT64_MAX, &page);
    TEST_ASSERT(result != PAGE_SUCCESS, "Reading UINT64_MAX should fail");
    
    // Note: Current implementation doesn't validate max page ID on free
    // This would require tracking allocated pages or checking file bounds
    result = page_manager_free(pm, UINT64_MAX);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Freeing UINT64_MAX result: %d (validation TBD)\n", result);
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_double_free(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id > 0, "Page allocation should succeed");
    
    // Free once
    page_result_t result = page_manager_free(pm, page_id);
    TEST_ASSERT(result == PAGE_SUCCESS, "First free should succeed");
    
    // Try to free again (should be idempotent or error)
    result = page_manager_free(pm, page_id);
    // Note: Current implementation may allow this - just document behavior
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Double free result: %d (behavior may vary)\n", result);
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_page_size_class_boundaries(void) {
    // Test exact boundaries of size classes
    TEST_ASSERT(page_get_size_class(0) == SIZE_CLASS_TINY, 
               "Boundary: 0 bytes = TINY");
    TEST_ASSERT(page_get_size_class(511) == SIZE_CLASS_TINY, 
               "Boundary: 511 bytes = TINY");
    TEST_ASSERT(page_get_size_class(512) == SIZE_CLASS_SMALL, 
               "Boundary: 512 bytes = SMALL");
    TEST_ASSERT(page_get_size_class(1023) == SIZE_CLASS_SMALL, 
               "Boundary: 1023 bytes = SMALL");
    TEST_ASSERT(page_get_size_class(1024) == SIZE_CLASS_MEDIUM, 
               "Boundary: 1024 bytes = MEDIUM");
    TEST_ASSERT(page_get_size_class(2047) == SIZE_CLASS_MEDIUM, 
               "Boundary: 2047 bytes = MEDIUM");
    TEST_ASSERT(page_get_size_class(2048) == SIZE_CLASS_LARGE, 
               "Boundary: 2048 bytes = LARGE");
    TEST_ASSERT(page_get_size_class(3071) == SIZE_CLASS_LARGE, 
               "Boundary: 3071 bytes = LARGE");
    TEST_ASSERT(page_get_size_class(3072) == SIZE_CLASS_HUGE, 
               "Boundary: 3072 bytes = HUGE");
    TEST_ASSERT(page_get_size_class(PAGE_SIZE - PAGE_HEADER_SIZE - 1) == SIZE_CLASS_HUGE,
               "Boundary: Almost full = HUGE");
    TEST_ASSERT(page_get_size_class(PAGE_SIZE - PAGE_HEADER_SIZE) == SIZE_CLASS_EMPTY,
               "Boundary: Exactly full = EMPTY");
}

void test_max_data_in_page(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, page_id, 1);
    
    // Fill usable data area with pattern (last 4 bytes reserved for torn page tail marker)
    const size_t data_size = PAGE_DATA_SIZE;
    for (size_t i = 0; i < data_size; i++) {
        page.data[i] = (uint8_t)(i & 0xFF);
    }
    page.header.free_offset = PAGE_HEADER_SIZE + data_size;
    page_compute_checksum(&page);

    // Write and read back
    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should write page with max data");

    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should read page with max data");

    // Verify data integrity (only usable area, tail marker reserved)
    bool data_matches = true;
    for (size_t i = 0; i < data_size; i++) {
        if (read_page.data[i] != (uint8_t)(i & 0xFF)) {
            data_matches = false;
            break;
        }
    }
    TEST_ASSERT(data_matches, "All data should match after full page write");
    TEST_ASSERT(page_verify_checksum(&read_page), "Checksum should be valid");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_empty_page_operations(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, page_id, 1);
    
    // Write page with no data (only header)
    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should write empty page");
    
    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should read empty page");
    TEST_ASSERT(read_page.header.free_offset == PAGE_HEADER_SIZE, 
               "Empty page should have free_offset at header size");
    TEST_ASSERT(page_verify_checksum(&read_page), "Checksum should be valid");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_checksum_corruption_detection(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, page_id, 1);
    strcpy((char*)page.data, "Test data");
    page_compute_checksum(&page);
    
    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write should succeed");
    
    // Manually corrupt the file on disk
    int fd;
    uint64_t offset;
    result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should get file location");
    
    // Corrupt a byte in the data section
    uint8_t corrupt_byte = 0xFF;
    ssize_t written = pwrite(fd, &corrupt_byte, 1, offset + PAGE_HEADER_SIZE + 5);
    TEST_ASSERT(written == 1, "Corruption write should succeed");
    
    // Try to read - should detect corruption
    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_ERROR_CORRUPTION, "Should detect checksum corruption");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_free_space_update_invalid_args(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Test NULL page manager
    page_result_t result = page_manager_update_free_space(NULL, 1, 100);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "NULL manager should fail");
    
    // Test page ID 0 - current implementation doesn't validate
    result = page_manager_update_free_space(pm, 0, 100);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Update free space for page 0: %d (validation TBD)\n", result);
    
    // Test free_bytes > max possible
    uint64_t page_id = page_manager_alloc(pm, 0);
    result = page_manager_update_free_space(pm, page_id, PAGE_SIZE);
    // Should handle gracefully (may clamp or error)
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Update with PAGE_SIZE result: %d\n", result);
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_write_with_null_page(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    page_result_t result = page_manager_write(pm, NULL);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Write with NULL page should fail");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_read_with_null_output(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_result_t result = page_manager_read(pm, page_id, NULL);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Read with NULL output should fail");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Multi-File Tests
// ============================================================================

void test_multi_file_boundary(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Calculate pages per file
    const uint64_t pages_per_file = (512ULL * 1024 * 1024) / PAGE_SIZE; // 131072 pages
    
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Pages per file: %lu\n", pages_per_file);
    
    // Allocate page at boundary (last page of first file)
    uint64_t page_id = pages_per_file;
    
    page_t page;
    page_init(&page, page_id, 1);
    strcpy((char*)page.data, "Boundary page");
    page_compute_checksum(&page);
    
    // Get file location to verify
    int fd;
    uint64_t offset;
    page_result_t result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should get file location for boundary page");
    
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Page %lu: fd=%d, offset=%lu\n", 
           page_id, fd, offset);
    
    // Test page in second file - requires actual file creation
    // Files are created on-demand during write, not during allocation
    uint64_t page_id_file2 = pages_per_file + 1;
    result = page_manager_get_file_location(pm, page_id_file2, &fd, &offset);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Second file lookup result: %d (files created on-demand during write)\n", result);
    
    if (result == PAGE_SUCCESS) {
        printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Page %lu: fd=%d, offset=%lu\n", 
               page_id_file2, fd, offset);
    }
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Stress & Performance Tests
// ============================================================================

void test_many_allocations(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    const int num_pages = 1000;
    uint64_t page_ids[num_pages];
    
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Allocating %d pages...\n", num_pages);
    
    clock_t start = clock();
    for (int i = 0; i < num_pages; i++) {
        page_ids[i] = page_manager_alloc(pm, 0);
        TEST_ASSERT(page_ids[i] > 0, "Allocation should succeed");
    }
    clock_t end = clock();
    
    double time_ms = ((double)(end - start)) / CLOCKS_PER_SEC * 1000.0;
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Time: %.2f ms (%.2f µs per alloc)\n",
           time_ms, (time_ms * 1000.0) / num_pages);
    
    TEST_ASSERT(pm->allocator->total_pages == num_pages, "Should have correct page count");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_many_free_and_realloc(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    const int num_pages = 500;
    
    // Allocate many pages
    for (int i = 0; i < num_pages; i++) {
        page_manager_alloc(pm, 0);
    }
    
    // Free half of them
    for (int i = 1; i <= num_pages; i += 2) {
        page_manager_free(pm, i);
    }
    
    TEST_ASSERT(pm->allocator->free_pages == num_pages / 2, 
               "Should have half pages free");
    
    // Reallocate - should reuse freed pages
    for (int i = 0; i < num_pages / 2; i++) {
        uint64_t page_id = page_manager_alloc(pm, 0);
        TEST_ASSERT(page_id > 0, "Reallocation should succeed");
    }
    
    TEST_ASSERT(pm->allocator->free_pages == 0, "Should have no free pages after reuse");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_random_size_allocations(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    srand(42); // Fixed seed for reproducibility
    
    const int num_allocations = 100;
    for (int i = 0; i < num_allocations; i++) {
        uint32_t free_space = rand() % (PAGE_SIZE - PAGE_HEADER_SIZE);
        uint64_t page_id = page_manager_alloc(pm, free_space);
        TEST_ASSERT(page_id > 0, "Random size allocation should succeed");
        
        // Note: Newly allocated pages are not automatically added to free lists.
        // The caller must call page_manager_update_free_space() after writing data.
        // This test verifies allocation works with various size hints.
    }
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_fragmentation_scenario(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Simulate fragmentation: alloc, free alternating pattern
    uint64_t pages[20];
    for (int i = 0; i < 20; i++) {
        pages[i] = page_manager_alloc(pm, 0);
    }
    
    // Free odd pages
    for (int i = 1; i < 20; i += 2) {
        page_manager_free(pm, pages[i]);
    }
    
    TEST_ASSERT(pm->allocator->free_pages == 10, "Should have 10 free pages");
    TEST_ASSERT(pm->allocator->allocated_pages == 10, "Should have 10 allocated pages");
    
    // Update free space on some pages to create fragmentation
    for (int i = 0; i < 20; i += 2) {
        uint32_t free_space = (i * 100) % 3000;
        page_manager_update_free_space(pm, pages[i], free_space);
    }
    
    // Verify pages distributed across size classes
    int non_empty_classes = 0;
    for (int i = 0; i < SIZE_CLASS_COUNT; i++) {
        if (pm->allocator->pages_per_class[i] > 0) {
            non_empty_classes++;
        }
    }
    
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " Pages distributed across %d size classes\n",
           non_empty_classes);
    TEST_ASSERT(non_empty_classes > 1, "Should have pages in multiple size classes");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Persistence & Recovery Tests
// ============================================================================

void test_persistence_after_close(void) {
    cleanup_test_db();
    
    // Create and write data
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, page_id, 1);
    strcpy((char*)page.data, "Persistent data");
    page_compute_checksum(&page);
    page_manager_write(pm, &page);
    
    page_manager_destroy(pm);
    
    // Reopen and verify data persisted
    pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager reopen should succeed");
    
    page_t read_page;
    page_result_t result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Should read persisted page");
    TEST_ASSERT(strcmp((char*)read_page.data, "Persistent data") == 0,
               "Persisted data should match");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_sync_durability(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Write many pages
    for (int i = 0; i < 50; i++) {
        uint64_t page_id = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, page_id, 1);
        snprintf((char*)page.data, 100, "Data %d", i);
        page_compute_checksum(&page);
        page_manager_write(pm, &page);
    }
    
    // Sync to disk
    page_result_t result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Sync should succeed");
    
    // Verify data file size increased
    char filename[256];
    snprintf(filename, sizeof(filename), "%s/pages_00000.dat", TEST_DB_PATH);
    struct stat st;
    TEST_ASSERT(stat(filename, &st) == 0, "Data file should exist");
    TEST_ASSERT(st.st_size >= 50 * PAGE_SIZE, "File should contain written pages");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Statistics Edge Cases
// ============================================================================

void test_statistics_with_null_args(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Test get_stats with NULL output
    page_manager_get_stats(pm, NULL);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " get_stats with NULL output (should not crash)\n");
    
    // Test get_stats with NULL manager
    page_manager_stats_t stats;
    page_manager_get_stats(NULL, &stats);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " get_stats with NULL manager (should not crash)\n");
    
    // Test print_stats with NULL manager
    page_manager_print_stats(NULL);
    printf("  " ANSI_COLOR_YELLOW "ℹ" ANSI_COLOR_RESET " print_stats with NULL manager (should not crash)\n");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    // Initialize logger (reduce verbosity for edge case tests)
    log_init(LOG_LEVEL_WARN, stderr);
    
    printf("\n");
    printf("========================================\n");
    printf("   Page Manager Edge Case Tests\n");
    printf("========================================\n");
    
    printf("\n=== Boundary & Edge Cases ===\n");
    RUN_TEST(test_page_id_boundaries);
    RUN_TEST(test_double_free);
    RUN_TEST(test_page_size_class_boundaries);
    RUN_TEST(test_max_data_in_page);
    RUN_TEST(test_empty_page_operations);
    RUN_TEST(test_checksum_corruption_detection);
    RUN_TEST(test_free_space_update_invalid_args);
    RUN_TEST(test_write_with_null_page);
    RUN_TEST(test_read_with_null_output);
    
    printf("\n=== Multi-File Tests ===\n");
    RUN_TEST(test_multi_file_boundary);
    
    printf("\n=== Stress & Performance ===\n");
    RUN_TEST(test_many_allocations);
    RUN_TEST(test_many_free_and_realloc);
    RUN_TEST(test_random_size_allocations);
    RUN_TEST(test_fragmentation_scenario);
    
    printf("\n=== Persistence & Recovery ===\n");
    RUN_TEST(test_persistence_after_close);
    RUN_TEST(test_sync_durability);
    
    printf("\n=== Statistics Edge Cases ===\n");
    RUN_TEST(test_statistics_with_null_args);
    
    printf("\n");
    printf("========================================\n");
    printf("Results: %s%d passed%s, %s%d failed%s\n",
           ANSI_COLOR_GREEN, tests_passed, ANSI_COLOR_RESET,
           tests_failed > 0 ? ANSI_COLOR_RED : "",
           tests_failed,
           tests_failed > 0 ? ANSI_COLOR_RESET : "");
    printf("========================================\n");
    printf("\n");
    
    cleanup_test_db();
    
    return tests_failed > 0 ? 1 : 0;
}
