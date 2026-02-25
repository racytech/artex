/*
 * Page Manager Tests
 * 
 * Tests for page allocation, I/O, and free space management.
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

#define TEST_DB_PATH "/tmp/test_page_manager_db"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RED     "\x1b[31m"
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
// Tests
// ============================================================================

void test_page_manager_create_destroy(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    TEST_ASSERT(pm->allocator != NULL, "Allocator should be initialized");
    TEST_ASSERT(pm->allocator->next_page_id == 1, "Next page ID should start at 1");
    TEST_ASSERT(pm->allocator->num_data_files == 1, "Should have one data file");
    
    page_manager_destroy(pm);
    
    // Verify data file was created
    char filename[256];
    snprintf(filename, sizeof(filename), "%s/pages_00000.dat", TEST_DB_PATH);
    struct stat st;
    TEST_ASSERT(stat(filename, &st) == 0, "Data file should exist");
    
    cleanup_test_db();
}

void test_page_allocation(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Allocate first page (should be page 1)
    uint64_t page_id1 = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id1 == 1, "First allocated page should be ID 1");
    TEST_ASSERT(pm->allocator->total_pages == 1, "Total pages should be 1");
    TEST_ASSERT(pm->allocator->allocated_pages == 1, "Allocated pages should be 1");
    
    // Allocate second page
    uint64_t page_id2 = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id2 == 2, "Second allocated page should be ID 2");
    TEST_ASSERT(pm->allocator->total_pages == 2, "Total pages should be 2");
    
    // Allocate third page
    uint64_t page_id3 = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id3 == 3, "Third allocated page should be ID 3");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_page_free_marks_dead(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    // Allocate and write pages
    uint64_t page_id1 = page_manager_alloc(pm, 0);
    uint64_t page_id2 = page_manager_alloc(pm, 0);
    uint64_t page_id3 = page_manager_alloc(pm, 0);
    (void)page_id1; (void)page_id3;

    page_t page;
    page_init(&page, page_id2, 1);
    page_manager_write(pm, &page);

    TEST_ASSERT(pm->allocator->allocated_pages == 3, "Should have 3 allocated pages");

    // Free page 2 — marks dead in append-only, does NOT reuse
    page_result_t result = page_manager_free(pm, page_id2);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page free should succeed");
    TEST_ASSERT(pm->allocator->allocated_pages == 2, "Should have 2 allocated pages");

    // New alloc returns fresh page_id (no reuse)
    uint64_t page_id4 = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id4 == 4, "New alloc should return fresh page_id 4, not reuse 2");
    TEST_ASSERT(page_id4 != page_id2, "Should NOT reuse freed page in append-only mode");

    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_page_init_and_checksum(void) {
    cleanup_test_db();
    
    page_t page;
    page_init(&page, 42, 100);
    
    TEST_ASSERT(page.header.page_id == 42, "Page ID should be 42");
    TEST_ASSERT(page.header.version == 100, "Version should be 100");
    TEST_ASSERT(page.header.free_offset == PAGE_HEADER_SIZE, 
               "Free offset should start at header size");
    TEST_ASSERT(page.header.num_nodes == 0, "Num nodes should be 0");
    TEST_ASSERT(page.header.compression_type == 0, "Compression type should be 0");
    
    // Verify checksum
    TEST_ASSERT(page_verify_checksum(&page), "Checksum should be valid after init");
    
    // Modify data and verify checksum fails
    page.data[0] = 0xFF;
    TEST_ASSERT(!page_verify_checksum(&page), "Checksum should be invalid after modification");
    
    // Recompute and verify
    page_compute_checksum(&page);
    TEST_ASSERT(page_verify_checksum(&page), "Checksum should be valid after recompute");
    
    cleanup_test_db();
}

void test_page_write_and_read(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Allocate a page
    uint64_t page_id = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id > 0, "Page allocation should succeed");
    
    // Initialize page with test data
    page_t write_page;
    page_init(&write_page, page_id, 1);
    
    const char *test_data = "Hello, Page Manager!";
    memcpy(write_page.data, test_data, strlen(test_data) + 1);
    write_page.header.free_offset = PAGE_HEADER_SIZE + strlen(test_data) + 1;
    write_page.header.num_nodes = 1;
    // Must recompute checksum after modifying data
    page_compute_checksum(&write_page);
    
    // Write page to disk
    page_result_t result = page_manager_write(pm, &write_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    TEST_ASSERT(pm->pages_written == 1, "Pages written counter should be 1");
    
    // Read page back
    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page read should succeed");
    TEST_ASSERT(pm->pages_read == 1, "Pages read counter should be 1");
    
    // Verify page contents
    TEST_ASSERT(read_page.header.page_id == page_id, "Page ID should match");
    TEST_ASSERT(read_page.header.version == 1, "Version should match");
    TEST_ASSERT(read_page.header.num_nodes == 1, "Num nodes should match");
    TEST_ASSERT(strcmp((char*)read_page.data, test_data) == 0, "Data should match");
    TEST_ASSERT(page_verify_checksum(&read_page), "Checksum should be valid");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_multiple_pages_io(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    const int num_pages = 10;
    uint64_t page_ids[num_pages];
    
    // Allocate and write multiple pages
    for (int i = 0; i < num_pages; i++) {
        page_ids[i] = page_manager_alloc(pm, 0);
        
        page_t page;
        page_init(&page, page_ids[i], i + 1);
        
        // Write unique data to each page
        snprintf((char*)page.data, 100, "Page %d data", i);
        page.header.num_nodes = i;
        // Must recompute checksum after modifying data
        page_compute_checksum(&page);
        
        page_result_t result = page_manager_write(pm, &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    }
    
    TEST_ASSERT(pm->pages_written == num_pages, "Should have written all pages");
    
    // Read and verify all pages
    for (int i = 0; i < num_pages; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, page_ids[i], &page);
        
        TEST_ASSERT(result == PAGE_SUCCESS, "Page read should succeed");
        TEST_ASSERT(page.header.page_id == page_ids[i], "Page ID should match");
        TEST_ASSERT(page.header.version == (uint64_t)(i + 1), "Version should match");
        TEST_ASSERT(page.header.num_nodes == (uint32_t)i, "Num nodes should match");
        
        char expected_data[100];
        snprintf(expected_data, 100, "Page %d data", i);
        TEST_ASSERT(strcmp((char*)page.data, expected_data) == 0, "Data should match");
        TEST_ASSERT(page_verify_checksum(&page), "Checksum should be valid");
    }
    
    TEST_ASSERT(pm->pages_read == num_pages, "Should have read all pages");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_page_index_persistence(void) {
    cleanup_test_db();

    // Write 10 pages, destroy, recreate, verify all readable
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

        for (int i = 0; i < 10; i++) {
            uint64_t pid = page_manager_alloc(pm, 0);
            page_t page;
            page_init(&page, pid, i + 1);
            snprintf((char*)page.data, 100, "Persist test page %d", i);
            page_compute_checksum(&page);
            page_manager_write(pm, &page);
        }

        // Save index explicitly (also happens in destroy)
        page_manager_save_index(pm);
        page_manager_save_metadata(pm, 0);
        page_manager_destroy(pm);
    }

    // Reopen and verify
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "Page manager reopen should succeed");
        TEST_ASSERT(pm->allocator->next_page_id == 11,
                    "next_page_id should be restored to 11");

        size_t idx_count = page_manager_get_num_pages(pm);
        TEST_ASSERT(idx_count == 10, "Index should have 10 entries after reload");

        // Read all pages back
        int all_ok = 1;
        for (int i = 0; i < 10; i++) {
            page_t page;
            page_result_t r = page_manager_read(pm, (uint64_t)(i + 1), &page);
            if (r != PAGE_SUCCESS) { all_ok = 0; break; }
            char expected[100];
            snprintf(expected, 100, "Persist test page %d", i);
            if (strcmp((char*)page.data, expected) != 0) { all_ok = 0; break; }
        }
        TEST_ASSERT(all_ok, "All 10 pages readable after restart");

        page_manager_destroy(pm);
    }

    cleanup_test_db();
}

void test_dead_page_tracking(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    // Allocate and write pages
    for (int i = 0; i < 5; i++) {
        uint64_t pid = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, pid, 1);
        page_manager_write(pm, &page);
    }

    // Free pages 2 and 4 (mark as dead)
    page_manager_free(pm, 2);
    page_manager_free(pm, 4);

    // Check stats
    page_manager_stats_t stats;
    page_manager_get_stats(pm, &stats);
    TEST_ASSERT(stats.dead_pages == 2, "Should have 2 dead pages");
    TEST_ASSERT(stats.dead_bytes == 2 * PAGE_SIZE, "Dead bytes = 2 * PAGE_SIZE");
    TEST_ASSERT(stats.allocated_pages == 3, "Should have 3 allocated pages");

    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_page_manager_sync(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Write some pages
    for (int i = 0; i < 5; i++) {
        uint64_t page_id = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, page_id, 1);
        page_manager_write(pm, &page);
    }
    
    // Sync all files
    page_result_t result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page manager sync should succeed");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_statistics(void) {
    cleanup_test_db();
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    // Allocate and write some pages
    for (int i = 0; i < 5; i++) {
        uint64_t page_id = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, page_id, 1);
        page_manager_write(pm, &page);
    }
    
    // Get statistics
    page_manager_stats_t stats;
    page_manager_get_stats(pm, &stats);
    
    TEST_ASSERT(stats.total_pages == 5, "Total pages should be 5");
    TEST_ASSERT(stats.allocated_pages == 5, "Allocated pages should be 5");
    TEST_ASSERT(stats.pages_written == 5, "Pages written should be 5");
    TEST_ASSERT(stats.bytes_written == 5 * PAGE_SIZE, "Bytes written should match");
    TEST_ASSERT(stats.num_data_files == 1, "Should have 1 data file");
    
    // Print statistics (visual verification)
    printf("\n");
    page_manager_print_stats(pm);
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_error_handling(void) {
    cleanup_test_db();
    
    // Test NULL page manager
    uint64_t page_id = page_manager_alloc(NULL, 0);
    TEST_ASSERT(page_id == 0, "Alloc with NULL manager should return 0");
    
    page_result_t result = page_manager_free(NULL, 1);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Free with NULL manager should fail");
    
    // Test invalid page ID
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");
    
    result = page_manager_free(pm, 0);
    TEST_ASSERT(result == PAGE_ERROR_INVALID_ARG, "Cannot free page 0");
    
    // Test read of non-existent page (not in index)
    page_t page;
    result = page_manager_read(pm, 9999, &page);
    TEST_ASSERT(result == PAGE_ERROR_NOT_FOUND, "Read of non-existent page should return NOT_FOUND");
    
    page_manager_destroy(pm);
    cleanup_test_db();
}

// ============================================================================
// Main
// ============================================================================

void test_page_corruption_detection(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    // Allocate and write a valid page
    uint64_t page_id = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id != 0, "Page allocation should succeed");

    page_t page;
    page_init(&page, page_id, 1);
    snprintf((char*)page.data, 100, "Valid page data for corruption test");
    page_compute_checksum(&page);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Sync should succeed");

    // Verify the page reads back correctly before corruption
    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read before corruption should succeed");
    TEST_ASSERT(page_verify_checksum(&read_page), "Checksum should be valid before corruption");

    // Get the file location and corrupt the page data directly on disk
    int fd;
    uint64_t offset;
    result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    TEST_ASSERT(result == PAGE_SUCCESS, "Get file location should succeed");

    // Corrupt bytes in the data area (after the header)
    uint8_t garbage[] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE};
    ssize_t written = pwrite(fd, garbage, sizeof(garbage), offset + PAGE_HEADER_SIZE + 10);
    TEST_ASSERT(written == sizeof(garbage), "Corruption write should succeed");
    fsync(fd);

    // Reading the corrupted page should detect checksum mismatch
    page_t corrupted_page;
    result = page_manager_read(pm, page_id, &corrupted_page);
    TEST_ASSERT(result == PAGE_ERROR_CORRUPTION, "Read of corrupted page should return CORRUPTION");

    // Verify the in-memory checksum function also detects it
    // (manually read the raw bytes to test page_verify_checksum)
    pread(fd, &corrupted_page, PAGE_SIZE, offset);
    TEST_ASSERT(!page_verify_checksum(&corrupted_page), "Checksum verification should fail on corrupted page");

    // Write a new valid page to the same ID — should succeed
    page_init(&page, page_id, 2);
    snprintf((char*)page.data, 100, "Repaired page data");
    page_compute_checksum(&page);
    result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Writing repaired page should succeed");

    // Read back the repaired page — should succeed
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read of repaired page should succeed");
    TEST_ASSERT(page_verify_checksum(&read_page), "Repaired page checksum should be valid");

    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_fsync_retry_and_health(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    // Verify default config
    TEST_ASSERT(pm->fsync_retry_max == 3, "Default retry max should be 3");
    TEST_ASSERT(pm->fsync_retry_delay_us == 100, "Default retry delay should be 100us");

    // Health should start OK
    db_health_t health;
    page_manager_get_health(pm, &health);
    TEST_ASSERT(health.state == DB_HEALTH_OK, "Initial health should be OK");
    TEST_ASSERT(health.fsync_failures == 0, "Initial fsync failures should be 0");
    TEST_ASSERT(health.fsync_retries == 0, "Initial fsync retries should be 0");

    // Write some pages and sync — should succeed normally
    for (int i = 0; i < 10; i++) {
        uint64_t page_id = page_manager_alloc(pm, 0);
        TEST_ASSERT(page_id != 0, "Page allocation should succeed");

        page_t page;
        page_init(&page, page_id, 1);
        snprintf((char*)page.data, 100, "Test page %d", i);
        page_compute_checksum(&page);
        page_result_t result = page_manager_write(pm, &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    }

    page_result_t result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Sync should succeed");

    // Verify health after successful sync
    page_manager_get_health(pm, &health);
    TEST_ASSERT(health.state == DB_HEALTH_OK, "Health should still be OK after successful sync");
    TEST_ASSERT(health.total_fsync_calls > 0, "Should have recorded fsync calls");
    TEST_ASSERT(health.fsync_failures == 0, "No failures on good sync");

    // Test sync with invalid fd — forces fsync failure
    // Save real fd, replace with -1
    int saved_fd = pm->allocator->data_file_fds[0];
    pm->allocator->data_file_fds[0] = -1;

    result = page_manager_sync(pm);
    TEST_ASSERT(result != PAGE_SUCCESS, "Sync with bad fd should fail");

    page_manager_get_health(pm, &health);
    TEST_ASSERT(health.state == DB_HEALTH_FAILING, "Health should be FAILING after fsync failure");
    TEST_ASSERT(health.fsync_failures > 0, "Should record fsync failure");
    TEST_ASSERT(health.last_error_errno != 0, "Should record errno from failure");

    // Restore fd
    pm->allocator->data_file_fds[0] = saved_fd;

    // Sync should work again
    result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Sync should succeed after restoring fd");

    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_torn_write_detection(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    // Allocate and write a valid page
    uint64_t page_id = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id != 0, "Page allocation should succeed");

    page_t page;
    page_init(&page, page_id, 1);
    snprintf((char*)page.data, 100, "Valid page data for torn write test");
    page_compute_checksum(&page);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    result = page_manager_sync(pm);
    TEST_ASSERT(result == PAGE_SUCCESS, "Sync should succeed");

    // Verify it reads back fine
    page_t read_page;
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read should succeed after normal write");

    // Simulate torn write: overwrite only the first half of the page on disk
    // This changes the header (including write_counter) but leaves the old tail
    int fd;
    uint64_t offset;
    result = page_manager_get_file_location(pm, page_id, &fd, &offset);
    TEST_ASSERT(result == PAGE_SUCCESS, "Get file location should succeed");

    // Create a "new version" of the page with different data + incremented counter
    page_t torn_page;
    memcpy(&torn_page, &page, PAGE_SIZE);
    snprintf((char*)torn_page.data, 100, "Torn page data - ONLY FIRST HALF WRITTEN");
    torn_page.header.write_counter++;
    // Don't update tail marker — simulates torn write
    page_compute_checksum(&torn_page);

    // Write only the first half (header + start of data, but NOT the tail)
    ssize_t written = pwrite(fd, &torn_page, PAGE_SIZE / 2, offset);
    TEST_ASSERT(written == PAGE_SIZE / 2, "Partial write should succeed");
    fsync(fd);

    // Read should detect torn write or corruption
    page_t corrupted_page;
    result = page_manager_read(pm, page_id, &corrupted_page);
    TEST_ASSERT(result == PAGE_ERROR_TORN_WRITE || result == PAGE_ERROR_CORRUPTION,
                "Read of torn page should detect error");

    // Write a valid page to repair
    page_init(&page, page_id, 2);
    snprintf((char*)page.data, 100, "Repaired after torn write");
    page_compute_checksum(&page);
    result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Writing repaired page should succeed");

    // Read back should succeed
    result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read of repaired page should succeed");

    page_manager_destroy(pm);
    cleanup_test_db();
}

void test_write_counter_increments(void) {
    cleanup_test_db();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager creation should succeed");

    uint64_t page_id = page_manager_alloc(pm, 0);
    TEST_ASSERT(page_id != 0, "Page allocation should succeed");

    page_t page;
    page_init(&page, page_id, 1);
    TEST_ASSERT(page.header.write_counter == 0, "Initial write counter should be 0");

    // Write the page 5 times
    for (int i = 0; i < 5; i++) {
        snprintf((char*)page.data, 100, "Write iteration %d", i);
        page_compute_checksum(&page);
        page_result_t result = page_manager_write(pm, &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Page write should succeed");
    }

    // write_counter should be 5 (incremented by each write)
    TEST_ASSERT(page.header.write_counter == 5,
                "Write counter should be 5 after 5 writes");

    // Sync and read back
    page_manager_sync(pm);
    page_t read_page;
    page_result_t result = page_manager_read(pm, page_id, &read_page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read should succeed");
    TEST_ASSERT(read_page.header.write_counter == 5,
                "Read-back write counter should be 5");

    // Verify tail marker matches
    uint32_t tail_counter;
    memcpy(&tail_counter, (uint8_t *)&read_page + PAGE_TAIL_MARKER_OFFSET,
           sizeof(tail_counter));
    TEST_ASSERT(tail_counter == 5, "Tail marker should match header counter");

    page_manager_destroy(pm);
    cleanup_test_db();
}

int main(void) {
    // Initialize logger
    log_init(LOG_LEVEL_INFO, stderr);
    
    printf("\n");
    printf("========================================\n");
    printf("   Page Manager Tests\n");
    printf("========================================\n");
    
    RUN_TEST(test_page_manager_create_destroy);
    RUN_TEST(test_page_allocation);
    RUN_TEST(test_page_free_marks_dead);
    RUN_TEST(test_page_init_and_checksum);
    RUN_TEST(test_page_write_and_read);
    RUN_TEST(test_multiple_pages_io);
    RUN_TEST(test_page_index_persistence);
    RUN_TEST(test_dead_page_tracking);
    RUN_TEST(test_page_manager_sync);
    RUN_TEST(test_statistics);
    RUN_TEST(test_error_handling);
    RUN_TEST(test_page_corruption_detection);
    RUN_TEST(test_fsync_retry_and_health);
    RUN_TEST(test_torn_write_detection);
    RUN_TEST(test_write_counter_increments);

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
