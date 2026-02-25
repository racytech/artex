/*
 * Page Manager Compression Tests
 *
 * Tests for LZ4/ZSTD page compression: round-trip, skip logic,
 * mixed reads, persistence, corruption detection, index tracking.
 */

#include "page_manager.h"
#include "page_gc.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#define TEST_DB_PATH "/tmp/test_pm_compression"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_RESET   "\x1b[0m"

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_ASSERT(condition, message) \
    do { \
        if (condition) { \
            printf("  " ANSI_COLOR_GREEN "✓" ANSI_COLOR_RESET " %s\n", message); \
            tests_passed++; \
        } else { \
            printf("  " ANSI_COLOR_RED "✗" ANSI_COLOR_RESET " %s (line %d)\n", message, __LINE__); \
            tests_failed++; \
        } \
    } while (0)

#define RUN_TEST(test_func) \
    do { \
        printf("\n--- %s ---\n", #test_func); \
        test_func(); \
    } while (0)

static void cleanup_test_db(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
}

// Fill page data with a repeating pattern (highly compressible)
static void fill_page_pattern(page_t *page, uint8_t pattern) {
    for (size_t i = 0; i < PAGE_SIZE - sizeof(page_header_t); i++) {
        page->data[i] = pattern;
    }
}

// Fill page data with pseudo-random bytes (incompressible)
static void fill_page_random(page_t *page, uint32_t seed) {
    uint32_t state = seed;
    for (size_t i = 0; i < PAGE_SIZE - sizeof(page_header_t); i++) {
        state = state * 1103515245 + 12345;
        page->data[i] = (uint8_t)(state >> 16);
    }
}

// ============================================================================
// Tests
// ============================================================================

void test_lz4_roundtrip(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);
    TEST_ASSERT(pm->compression_enabled == true, "Compression enabled");
    TEST_ASSERT(pm->default_compression_type == COMPRESSION_LZ4, "Type is LZ4");

    uint64_t pid = page_manager_alloc(pm, 0);
    TEST_ASSERT(pid > 0, "Page allocated");

    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0xAB);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write compressed page");

    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read compressed page");
    TEST_ASSERT(readback.header.page_id == pid, "Page ID matches");

    // Verify data matches
    int data_match = (memcmp(page.data, readback.data, sizeof(page.data)) == 0);
    TEST_ASSERT(data_match, "Data matches after LZ4 round-trip");

    // Verify compression actually happened (patterned data is highly compressible)
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    TEST_ASSERT(meta != NULL, "Metadata found");
    TEST_ASSERT(meta->compression_type == COMPRESSION_LZ4, "Index shows LZ4");
    TEST_ASSERT(meta->compressed_size < PAGE_SIZE, "Compressed smaller than PAGE_SIZE");
    printf("    (compressed: %u -> %u bytes, %.1f%% savings)\n",
           PAGE_SIZE, meta->compressed_size,
           100.0 * (1.0 - (double)meta->compressed_size / PAGE_SIZE));

    page_manager_destroy(pm);
}

void test_zstd5_roundtrip(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_ZSTD_5);

    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0xCD);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write ZSTD-5 page");

    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read ZSTD-5 page");

    int data_match = (memcmp(page.data, readback.data, sizeof(page.data)) == 0);
    TEST_ASSERT(data_match, "Data matches after ZSTD-5 round-trip");

    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    TEST_ASSERT(meta->compression_type == COMPRESSION_ZSTD_5, "Index shows ZSTD-5");
    TEST_ASSERT(meta->compressed_size < PAGE_SIZE, "Compressed smaller than PAGE_SIZE");
    printf("    (compressed: %u -> %u bytes)\n", PAGE_SIZE, meta->compressed_size);

    page_manager_destroy(pm);
}

void test_zstd19_roundtrip(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_ZSTD_19);

    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0xEF);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write ZSTD-19 page");

    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read ZSTD-19 page");

    int data_match = (memcmp(page.data, readback.data, sizeof(page.data)) == 0);
    TEST_ASSERT(data_match, "Data matches after ZSTD-19 round-trip");

    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    TEST_ASSERT(meta->compression_type == COMPRESSION_ZSTD_19, "Index shows ZSTD-19");
    TEST_ASSERT(meta->compressed_size < PAGE_SIZE, "Compressed smaller than PAGE_SIZE");
    printf("    (compressed: %u -> %u bytes)\n", PAGE_SIZE, meta->compressed_size);

    page_manager_destroy(pm);
}

void test_compression_skip_incompressible(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_random(&page, 12345);  // Random data = incompressible

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write incompressible page");

    // Compression should be skipped (savings < 10%)
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    TEST_ASSERT(meta != NULL, "Metadata found");
    TEST_ASSERT(meta->compression_type == COMPRESSION_NONE, "Compression skipped for random data");
    TEST_ASSERT(meta->compressed_size == PAGE_SIZE, "Written at full PAGE_SIZE");

    // Verify data still reads correctly
    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read back incompressible page");

    int data_match = (memcmp(page.data, readback.data, sizeof(page.data)) == 0);
    TEST_ASSERT(data_match, "Data matches for incompressible page");

    page_manager_destroy(pm);
}

void test_mixed_compressed_uncompressed(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 5 pages uncompressed (explicitly disable since default is LZ4)
    page_manager_set_compression(pm, COMPRESSION_NONE);
    uint64_t pids[10];
    page_t pages[10];
    for (int i = 0; i < 5; i++) {
        pids[i] = page_manager_alloc(pm, 0);
        page_init(&pages[i], pids[i], 1);
        fill_page_pattern(&pages[i], (uint8_t)(0x10 + i));
        page_result_t r = page_manager_write(pm, &pages[i]);
        TEST_ASSERT(r == PAGE_SUCCESS, "Write uncompressed page");
    }

    // Enable LZ4 and write 5 more
    page_manager_set_compression(pm, COMPRESSION_LZ4);
    for (int i = 5; i < 10; i++) {
        pids[i] = page_manager_alloc(pm, 0);
        page_init(&pages[i], pids[i], 1);
        fill_page_pattern(&pages[i], (uint8_t)(0x10 + i));
        page_result_t r = page_manager_write(pm, &pages[i]);
        TEST_ASSERT(r == PAGE_SUCCESS, "Write compressed page");
    }

    // Read all 10 back in reverse order
    int all_match = 1;
    for (int i = 9; i >= 0; i--) {
        page_t readback;
        page_result_t r = page_manager_read(pm, pids[i], &readback);
        if (r != PAGE_SUCCESS || memcmp(pages[i].data, readback.data, sizeof(pages[i].data)) != 0) {
            all_match = 0;
            printf("    MISMATCH on page %d (pid=%lu)\n", i, pids[i]);
        }
    }
    TEST_ASSERT(all_match, "All 10 pages (mixed compressed/uncompressed) read back correctly");

    // Verify compression metadata
    page_gc_metadata_t *meta0 = (page_gc_metadata_t *)page_manager_get_metadata(pm, pids[0]);
    page_gc_metadata_t *meta5 = (page_gc_metadata_t *)page_manager_get_metadata(pm, pids[5]);
    TEST_ASSERT(meta0->compression_type == COMPRESSION_NONE, "First 5 pages uncompressed");
    TEST_ASSERT(meta5->compression_type == COMPRESSION_LZ4, "Last 5 pages LZ4 compressed");

    page_manager_destroy(pm);
}

void test_compression_persistence(void) {
    cleanup_test_db();

    // Phase 1: write compressed pages
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "Page manager created (phase 1)");

        page_manager_set_compression(pm, COMPRESSION_LZ4);

        for (int i = 0; i < 5; i++) {
            uint64_t pid = page_manager_alloc(pm, 0);
            page_t page;
            page_init(&page, pid, 1);
            fill_page_pattern(&page, (uint8_t)(0x30 + i));
            page_manager_write(pm, &page);
        }

        page_manager_save_metadata(pm, 0);
        page_manager_save_index(pm);
        page_manager_destroy(pm);
    }

    // Phase 2: reopen and read back (compression_enabled doesn't matter for reads)
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "Page manager reopened (phase 2)");

        int all_ok = 1;
        for (uint64_t pid = 1; pid <= 5; pid++) {
            page_t readback;
            page_result_t r = page_manager_read(pm, pid, &readback);
            if (r != PAGE_SUCCESS) {
                printf("    Failed to read page %lu: %d\n", pid, r);
                all_ok = 0;
                continue;
            }
            // Verify pattern
            uint8_t expected = (uint8_t)(0x30 + (pid - 1));
            if (readback.data[0] != expected || readback.data[100] != expected) {
                printf("    Data mismatch on page %lu\n", pid);
                all_ok = 0;
            }
        }
        TEST_ASSERT(all_ok, "All 5 compressed pages readable after restart");

        // Verify index metadata survived
        page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, 1);
        TEST_ASSERT(meta != NULL, "Index entry exists after restart");
        TEST_ASSERT(meta->compression_type == COMPRESSION_LZ4, "Compression type persisted");
        TEST_ASSERT(meta->compressed_size < PAGE_SIZE, "Compressed size persisted");

        page_manager_destroy(pm);
    }
}

void test_write_compressed_explicit(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Disable compression globally for this test
    page_manager_set_compression(pm, COMPRESSION_NONE);
    TEST_ASSERT(pm->compression_enabled == false, "Compression disabled");

    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0x55);

    // Explicitly write with LZ4 despite global compression being off
    page_result_t result = page_manager_write_compressed(pm, &page, COMPRESSION_LZ4);
    TEST_ASSERT(result == PAGE_SUCCESS, "write_compressed() succeeded");

    // Verify it was actually compressed
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    TEST_ASSERT(meta->compression_type == COMPRESSION_LZ4, "Explicitly compressed as LZ4");
    TEST_ASSERT(meta->compressed_size < PAGE_SIZE, "Actually compressed");

    // Verify global setting unchanged
    TEST_ASSERT(pm->compression_enabled == false, "Global compression still disabled");

    // Read back
    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read back explicitly compressed page");

    int data_match = (memcmp(page.data, readback.data, sizeof(page.data)) == 0);
    TEST_ASSERT(data_match, "Data matches for explicit compression");

    page_manager_destroy(pm);
}

void test_corruption_detection_compressed(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0x77);

    page_result_t result = page_manager_write(pm, &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Write compressed page");

    // Get file location and corrupt bytes
    int fd;
    uint64_t offset;
    result = page_manager_get_file_location(pm, pid, &fd, &offset);
    TEST_ASSERT(result == PAGE_SUCCESS, "Got file location");

    // Corrupt middle of compressed data
    uint8_t garbage[16] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                           0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
    ssize_t w = pwrite(fd, garbage, sizeof(garbage), offset + 10);
    TEST_ASSERT(w == sizeof(garbage), "Corrupted compressed data on disk");

    // Read should fail with corruption
    page_t readback;
    result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_ERROR_CORRUPTION, "Corruption detected on compressed page");

    page_manager_destroy(pm);
}

void test_compressed_size_in_index(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    // Write 5 pages with patterned data
    uint64_t pids[5];
    for (int i = 0; i < 5; i++) {
        pids[i] = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, pids[i], 1);
        fill_page_pattern(&page, (uint8_t)(0xA0 + i));
        page_manager_write(pm, &page);
    }

    // All should show compressed_size < PAGE_SIZE
    int all_compressed = 1;
    uint32_t total_compressed = 0;
    for (int i = 0; i < 5; i++) {
        page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pids[i]);
        if (!meta || meta->compressed_size >= PAGE_SIZE || meta->compression_type != COMPRESSION_LZ4) {
            all_compressed = 0;
            if (meta) {
                printf("    page %d: comp_type=%u, comp_size=%u\n",
                       i, meta->compression_type, meta->compressed_size);
            }
        }
        if (meta) total_compressed += meta->compressed_size;
    }
    TEST_ASSERT(all_compressed, "All 5 pages show compressed in index");

    uint64_t total_file_size = page_manager_get_total_file_size(pm);
    TEST_ASSERT(total_file_size == total_compressed, "total_file_size matches sum of compressed sizes");
    TEST_ASSERT(total_file_size < 5 * PAGE_SIZE, "Total file size less than 5 * PAGE_SIZE");
    printf("    (5 pages: %u compressed bytes vs %u uncompressed)\n",
           total_compressed, 5 * PAGE_SIZE);

    page_manager_destroy(pm);
}

void test_append_cursor_variable_sizes(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    // Write several pages, track cursor position
    uint64_t expected_offset = 0;
    for (int i = 0; i < 5; i++) {
        uint64_t pid = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, pid, 1);
        fill_page_pattern(&page, (uint8_t)(0xB0 + i));
        page_manager_write(pm, &page);

        page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
        if (meta) {
            expected_offset += meta->compressed_size;
        }
    }

    // Cursor should equal sum of compressed sizes
    TEST_ASSERT(pm->allocator->current_file_offset == expected_offset,
                "Append cursor advanced by compressed sizes");
    TEST_ASSERT(expected_offset < 5 * PAGE_SIZE,
                "Total offset less than 5 * PAGE_SIZE (compression saved space)");
    printf("    (cursor at %lu, would be %u without compression)\n",
           expected_offset, 5 * PAGE_SIZE);

    // Write one more page and verify it doesn't overlap
    uint64_t pid6 = page_manager_alloc(pm, 0);
    page_t page6;
    page_init(&page6, pid6, 1);
    fill_page_pattern(&page6, 0xFF);
    page_manager_write(pm, &page6);

    page_gc_metadata_t *meta6 = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid6);
    TEST_ASSERT(meta6 != NULL && meta6->file_offset == expected_offset,
                "6th page starts where cursor was");

    // Verify all 6 pages read correctly (no overlap)
    int all_ok = 1;
    for (uint64_t pid = 1; pid <= 6; pid++) {
        page_t readback;
        if (page_manager_read(pm, pid, &readback) != PAGE_SUCCESS) {
            all_ok = 0;
        }
    }
    TEST_ASSERT(all_ok, "All 6 pages readable (no overlap from variable cursor)");

    page_manager_destroy(pm);
}

void test_dead_bytes_with_compression(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    // Write page P
    uint64_t pid = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, pid, 1);
    fill_page_pattern(&page, 0xAA);
    page_manager_write(pm, &page);

    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, pid);
    uint32_t original_compressed_size = meta->compressed_size;
    TEST_ASSERT(original_compressed_size < PAGE_SIZE, "First write compressed");

    uint64_t dead_before = pm->index->dead_bytes;

    // Rewrite same page with different data
    fill_page_pattern(&page, 0xBB);
    page.header.page_id = pid;
    page_manager_write(pm, &page);

    uint64_t dead_after = pm->index->dead_bytes;
    TEST_ASSERT(dead_after == dead_before + original_compressed_size,
                "Dead bytes increased by original compressed size");
    printf("    (dead_bytes: %lu -> %lu, delta=%lu, original_compressed=%u)\n",
           dead_before, dead_after, dead_after - dead_before, original_compressed_size);

    // Verify new data reads correctly
    page_t readback;
    page_result_t result = page_manager_read(pm, pid, &readback);
    TEST_ASSERT(result == PAGE_SUCCESS, "Read rewritten page");
    TEST_ASSERT(readback.data[0] == 0xBB, "Data reflects rewrite");

    page_manager_destroy(pm);
}

void test_compression_stats(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    page_manager_set_compression(pm, COMPRESSION_LZ4);

    // Write 10 pages
    for (int i = 0; i < 10; i++) {
        uint64_t pid = page_manager_alloc(pm, 0);
        page_t page;
        page_init(&page, pid, 1);
        fill_page_pattern(&page, (uint8_t)(0xC0 + i));
        page_manager_write(pm, &page);
    }

    page_manager_stats_t stats;
    page_manager_get_stats(pm, &stats);

    TEST_ASSERT(stats.pages_written == 10, "10 pages written");
    TEST_ASSERT(stats.bytes_written < 10 * PAGE_SIZE, "bytes_written reflects compression");
    TEST_ASSERT(stats.bytes_written > 0, "bytes_written is positive");
    printf("    (10 pages: %lu bytes written vs %u uncompressed)\n",
           stats.bytes_written, 10 * PAGE_SIZE);

    // Read them all back and check read stats
    for (uint64_t pid = 1; pid <= 10; pid++) {
        page_t readback;
        page_manager_read(pm, pid, &readback);
    }

    page_manager_get_stats(pm, &stats);
    TEST_ASSERT(stats.pages_read == 10, "10 pages read");
    TEST_ASSERT(stats.bytes_read < 10 * PAGE_SIZE, "bytes_read reflects compressed I/O");
    TEST_ASSERT(stats.bytes_read > 0, "bytes_read is positive");

    page_manager_destroy(pm);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("========================================\n");
    printf("  Page Manager Compression Tests\n");
    printf("========================================\n");

    RUN_TEST(test_lz4_roundtrip);
    RUN_TEST(test_zstd5_roundtrip);
    RUN_TEST(test_zstd19_roundtrip);
    RUN_TEST(test_compression_skip_incompressible);
    RUN_TEST(test_mixed_compressed_uncompressed);
    RUN_TEST(test_compression_persistence);
    RUN_TEST(test_write_compressed_explicit);
    RUN_TEST(test_corruption_detection_compressed);
    RUN_TEST(test_compressed_size_in_index);
    RUN_TEST(test_append_cursor_variable_sizes);
    RUN_TEST(test_dead_bytes_with_compression);
    RUN_TEST(test_compression_stats);

    printf("\n========================================\n");
    printf("Results: %s%d passed%s, %s%d failed%s\n",
           tests_passed > 0 ? ANSI_COLOR_GREEN : "", tests_passed, ANSI_COLOR_RESET,
           tests_failed > 0 ? ANSI_COLOR_RED : "", tests_failed, ANSI_COLOR_RESET);
    printf("========================================\n");

    cleanup_test_db();
    return tests_failed > 0 ? 1 : 0;
}
