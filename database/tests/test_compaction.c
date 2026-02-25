/*
 * Database Compaction Tests — In-Place Defragmentation
 *
 * Tests for analyze, is_recommended, in-place compaction, and journal recovery.
 * Uses page_manager directly (no data_art tree needed).
 */

#include "db_compaction.h"
#include "page_manager.h"
#include "page_gc.h"
#include "crc32.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#define TEST_DB_PATH "/tmp/test_compaction"
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

// Fill page with repeating pattern (compressible)
static void fill_page_pattern(page_t *page, uint8_t pattern) {
    for (size_t i = 0; i < PAGE_SIZE - sizeof(page_header_t); i++) {
        page->data[i] = pattern;
    }
}

// Write a page with data and return its page_id
static uint64_t write_test_page(page_manager_t *pm, uint8_t pattern) {
    uint64_t page_id = page_manager_alloc(pm, 0);
    page_t page;
    page_init(&page, page_id, 1);
    fill_page_pattern(&page, pattern);
    page_compute_checksum(&page);
    page_manager_write(pm, &page);
    return page_id;
}

// ============================================================================
// Analysis Tests
// ============================================================================

void test_analyze_empty(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    compaction_stats_t stats;
    bool ok = db_compaction_analyze(pm, &stats);
    TEST_ASSERT(ok, "Analyze succeeded");
    TEST_ASSERT(stats.total_pages_before == 0, "No pages");
    TEST_ASSERT(stats.live_pages == 0, "No live pages");
    TEST_ASSERT(stats.dead_pages == 0, "No dead pages");
    TEST_ASSERT(stats.success, "Stats success");

    page_manager_destroy(pm);
}

void test_analyze_no_dead(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10 pages
    for (int i = 0; i < 10; i++) {
        write_test_page(pm, (uint8_t)(i + 1));
    }

    compaction_stats_t stats;
    bool ok = db_compaction_analyze(pm, &stats);
    TEST_ASSERT(ok, "Analyze succeeded");
    TEST_ASSERT(stats.total_pages_before == 10, "10 pages total");
    TEST_ASSERT(stats.live_pages == 10, "10 live pages");
    TEST_ASSERT(stats.dead_pages == 0, "0 dead pages");
    TEST_ASSERT(stats.file_size_before > 0, "File has data");

    page_manager_destroy(pm);
}

void test_analyze_with_dead(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10 pages, free 5
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    for (int i = 0; i < 5; i++) {
        page_manager_free(pm, ids[i]);
    }

    compaction_stats_t stats;
    bool ok = db_compaction_analyze(pm, &stats);
    TEST_ASSERT(ok, "Analyze succeeded");
    TEST_ASSERT(stats.total_pages_before == 10, "10 pages total");
    TEST_ASSERT(stats.live_pages == 5, "5 live pages");
    TEST_ASSERT(stats.dead_pages == 5, "5 dead pages");
    TEST_ASSERT(stats.space_reclaimed > 0, "Space to reclaim");

    page_manager_destroy(pm);
}

void test_is_recommended(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10 pages, free none — not recommended
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    TEST_ASSERT(!db_compaction_is_recommended(pm), "Not recommended (0% dead)");

    // Free 5 → 50% fragmentation → recommended
    for (int i = 0; i < 5; i++) {
        page_manager_free(pm, ids[i]);
    }
    TEST_ASSERT(db_compaction_is_recommended(pm), "Recommended (50% dead)");

    page_manager_destroy(pm);
}

// ============================================================================
// Compaction Tests
// ============================================================================

void test_compact_no_dead(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 5 pages
    uint64_t ids[5];
    for (int i = 0; i < 5; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 10));
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Compact succeeded (no dead)");
    TEST_ASSERT(stats.success, "Stats success");
    TEST_ASSERT(stats.dead_pages == 0, "No dead pages");

    // Verify all pages still readable
    for (int i = 0; i < 5; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Page readable after no-op compact");
    }

    page_manager_destroy(pm);
}

void test_compact_basic(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 20 pages, free first 10
    uint64_t ids[20];
    for (int i = 0; i < 20; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    for (int i = 0; i < 10; i++) {
        page_manager_free(pm, ids[i]);
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    config.verbose = true;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);

    TEST_ASSERT(ok, "Compact succeeded");
    TEST_ASSERT(stats.success, "Stats success");
    TEST_ASSERT(stats.total_pages_before == 20, "20 pages before");
    TEST_ASSERT(stats.live_pages == 10, "10 live pages");
    TEST_ASSERT(stats.dead_pages == 10, "10 dead pages");
    TEST_ASSERT(stats.pages_moved == 10, "10 pages moved");
    TEST_ASSERT(stats.space_reclaimed > 0, "Space reclaimed");
    TEST_ASSERT(stats.total_pages_after == 10, "10 pages after");

    // Verify surviving pages still readable with correct data
    for (int i = 10; i < 20; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Surviving page readable");
        TEST_ASSERT(page.data[0] == (uint8_t)(i + 1), "Page data correct");
    }

    // Verify no tmp files were created (in-place compaction)
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s/pages_00000.dat.tmp", TEST_DB_PATH);
    TEST_ASSERT(access(tmp_path, F_OK) != 0, "No tmp files created (in-place)");

    // Verify no journal left behind
    char journal_path[512];
    snprintf(journal_path, sizeof(journal_path), "%s/compaction.journal", TEST_DB_PATH);
    TEST_ASSERT(access(journal_path, F_OK) != 0, "Journal cleaned up");

    page_manager_destroy(pm);
}

void test_compact_preserves_compression(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Ensure LZ4 compression is active
    page_manager_set_compression(pm, COMPRESSION_LZ4);

    // Write 10 pages (will be compressed), free 5
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }

    // Check that pages are indeed compressed
    page_gc_metadata_t *meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, ids[5]);
    TEST_ASSERT(meta != NULL, "Metadata found");
    TEST_ASSERT(meta->compression_type == COMPRESSION_LZ4, "Page is LZ4 compressed");
    uint32_t compressed_size_before = meta->compressed_size;
    TEST_ASSERT(compressed_size_before < PAGE_SIZE, "Compressed size < PAGE_SIZE");

    // Free first 5
    for (int i = 0; i < 5; i++) {
        page_manager_free(pm, ids[i]);
    }

    // Compact
    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Compact succeeded");

    // Verify compression preserved
    meta = (page_gc_metadata_t *)page_manager_get_metadata(pm, ids[5]);
    TEST_ASSERT(meta != NULL, "Metadata still found after compaction");
    TEST_ASSERT(meta->compression_type == COMPRESSION_LZ4, "Still LZ4 after compaction");
    TEST_ASSERT(meta->compressed_size == compressed_size_before, "Same compressed size");

    // Verify data readable
    page_t page;
    page_result_t result = page_manager_read(pm, ids[5], &page);
    TEST_ASSERT(result == PAGE_SUCCESS, "Page readable after compaction");
    TEST_ASSERT(page.data[0] == 6, "Data correct after compaction");

    page_manager_destroy(pm);
}

void test_compact_reclaims_space(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 20 pages, free 10
    uint64_t ids[20];
    for (int i = 0; i < 20; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }

    // Get file size before
    struct stat st_before;
    int fd = page_manager_get_data_fd(pm, 0);
    fstat(fd, &st_before);
    uint64_t size_before = (uint64_t)st_before.st_size;

    for (int i = 0; i < 10; i++) {
        page_manager_free(pm, ids[i]);
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Compact succeeded");

    // Get file size after — same fd, file was truncated in-place
    fd = page_manager_get_data_fd(pm, 0);
    struct stat st_after;
    fstat(fd, &st_after);
    uint64_t size_after = (uint64_t)st_after.st_size;

    TEST_ASSERT(size_after < size_before, "File size decreased (in-place truncation)");

    printf("  Size before: %lu, after: %lu, saved: %lu bytes (%.1f%%)\n",
           size_before, size_after, size_before - size_after,
           (double)(size_before - size_after) / size_before * 100.0);

    page_manager_destroy(pm);
}

void test_compact_dry_run(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    for (int i = 0; i < 5; i++) {
        page_manager_free(pm, ids[i]);
    }

    // Get file size before dry run
    struct stat st;
    int fd = page_manager_get_data_fd(pm, 0);
    fstat(fd, &st);
    uint64_t size_before = (uint64_t)st.st_size;

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    config.dry_run = true;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Dry run succeeded");
    TEST_ASSERT(stats.success, "Stats success");
    TEST_ASSERT(stats.dead_pages == 5, "5 dead pages reported");
    TEST_ASSERT(stats.space_reclaimed > 0, "Space to reclaim reported");

    // Verify file unchanged
    fd = page_manager_get_data_fd(pm, 0);
    fstat(fd, &st);
    TEST_ASSERT((uint64_t)st.st_size == size_before, "File size unchanged after dry run");

    // Verify index unchanged (dead pages still there)
    TEST_ASSERT(db_compaction_is_recommended(pm), "Still recommended after dry run");

    page_manager_destroy(pm);
}

void test_compact_continues_after(void) {
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10, free 5, compact
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    for (int i = 0; i < 5; i++) {
        page_manager_free(pm, ids[i]);
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(stats.success, "Compaction succeeded");

    // Now write 5 more pages after compaction
    uint64_t new_ids[5];
    for (int i = 0; i < 5; i++) {
        new_ids[i] = write_test_page(pm, (uint8_t)(i + 100));
    }

    // Verify new pages are readable
    for (int i = 0; i < 5; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, new_ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "New page readable after compaction");
        TEST_ASSERT(page.data[0] == (uint8_t)(i + 100), "New page data correct");
    }

    // Verify old surviving pages still readable
    for (int i = 5; i < 10; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Old page still readable");
        TEST_ASSERT(page.data[0] == (uint8_t)(i + 1), "Old page data correct");
    }

    page_manager_destroy(pm);
}

void test_compact_persistence(void) {
    cleanup_test_db();

    // Phase 1: write, free, compact
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "PM created (phase 1)");

        uint64_t ids[20];
        for (int i = 0; i < 20; i++) {
            ids[i] = write_test_page(pm, (uint8_t)(i + 1));
        }
        for (int i = 0; i < 10; i++) {
            page_manager_free(pm, ids[i]);
        }

        compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
        compaction_stats_t stats;
        bool ok = db_compaction_run(pm, &config, &stats);
        TEST_ASSERT(ok, "Compaction succeeded");
        TEST_ASSERT(stats.total_pages_after == 10, "10 pages after compaction");

        page_manager_destroy(pm);
    }

    // Phase 2: reopen and verify
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "PM created (phase 2)");

        // Check index was loaded correctly
        size_t num_pages = page_manager_get_num_pages(pm);
        TEST_ASSERT(num_pages == 10, "10 pages in reopened index");

        // Verify surviving pages (IDs 11-20) are readable
        for (uint64_t id = 11; id <= 20; id++) {
            page_t page;
            page_result_t result = page_manager_read(pm, id, &page);
            TEST_ASSERT(result == PAGE_SUCCESS, "Page readable after reopen");
        }

        page_manager_destroy(pm);
    }
}

// ============================================================================
// In-Place Specific Tests
// ============================================================================

void test_compact_dead_at_end(void) {
    // Dead pages only at the end — no moves needed, just truncation
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10 pages, free last 5 (at end of file)
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }

    // Get size before
    int fd = page_manager_get_data_fd(pm, 0);
    struct stat st;
    fstat(fd, &st);
    uint64_t size_before = (uint64_t)st.st_size;

    for (int i = 5; i < 10; i++) {
        page_manager_free(pm, ids[i]);
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Compact succeeded");
    TEST_ASSERT(stats.success, "Stats success");
    TEST_ASSERT(stats.pages_moved == 0, "No pages moved (dead at end)");
    TEST_ASSERT(stats.dead_pages == 5, "5 dead pages");

    // File should be smaller (dead pages truncated)
    fd = page_manager_get_data_fd(pm, 0);
    fstat(fd, &st);
    uint64_t size_after = (uint64_t)st.st_size;
    TEST_ASSERT(size_after < size_before, "File truncated");

    // Verify live pages still readable
    for (int i = 0; i < 5; i++) {
        page_t page;
        page_result_t result = page_manager_read(pm, ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Live page readable");
        TEST_ASSERT(page.data[0] == (uint8_t)(i + 1), "Data correct");
    }

    page_manager_destroy(pm);
}

void test_compact_interleaved_dead(void) {
    // Dead pages interleaved with live — requires moves
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Page manager created");

    // Write 10 pages, free every other one (1, 3, 5, 7, 9)
    uint64_t ids[10];
    for (int i = 0; i < 10; i++) {
        ids[i] = write_test_page(pm, (uint8_t)(i + 1));
    }
    for (int i = 0; i < 10; i += 2) {
        page_manager_free(pm, ids[i]);
    }

    compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
    compaction_stats_t stats;
    bool ok = db_compaction_run(pm, &config, &stats);
    TEST_ASSERT(ok, "Compact succeeded");
    TEST_ASSERT(stats.success, "Stats success");
    TEST_ASSERT(stats.live_pages == 5, "5 live pages");
    TEST_ASSERT(stats.pages_moved > 0, "Pages were moved");

    // Verify live pages (even indices: 2,4,6,8,10 → patterns 2,4,6,8,10)
    for (int i = 1; i < 10; i += 2) {
        page_t page;
        page_result_t result = page_manager_read(pm, ids[i], &page);
        TEST_ASSERT(result == PAGE_SUCCESS, "Live page readable after interleaved compact");
        TEST_ASSERT(page.data[0] == (uint8_t)(i + 1), "Data correct");
    }

    page_manager_destroy(pm);
}

void test_compact_journal_recovery(void) {
    // Simulate crash during compaction by manually writing a journal
    // then calling db_compaction_recover()
    cleanup_test_db();

    uint64_t live_page_ids[5];
    uint32_t compressed_sizes[5];

    // Phase 1: write pages and record their locations
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "PM created (phase 1)");

        // Write 10 pages, free first 5
        uint64_t ids[10];
        for (int i = 0; i < 10; i++) {
            ids[i] = write_test_page(pm, (uint8_t)(i + 1));
        }
        for (int i = 0; i < 5; i++) {
            page_manager_free(pm, ids[i]);
        }

        // Record live page info before simulating crash
        for (int i = 0; i < 5; i++) {
            live_page_ids[i] = ids[i + 5];
            page_gc_metadata_t *meta = (page_gc_metadata_t *)
                page_manager_get_metadata(pm, ids[i + 5]);
            compressed_sizes[i] = meta->compressed_size;
        }

        // Get compressed size of dead pages for computing offsets
        page_gc_metadata_t *first_dead = (page_gc_metadata_t *)
            page_manager_get_metadata(pm, ids[0]);
        uint32_t dead_compressed_size = first_dead->compressed_size;

        // Manually write a compaction journal as if we crashed mid-compaction
        // The dead pages are at the start (ids[0..4]), live pages follow (ids[5..9])
        // Move plan: move live pages forward to close the gap
        compaction_journal_entry_t entries[5];
        uint64_t write_cursor = 0;

        // First 5 dead pages: their total size is the gap to close
        uint64_t gap_size = 5 * (uint64_t)dead_compressed_size;

        for (int i = 0; i < 5; i++) {
            page_gc_metadata_t *meta = (page_gc_metadata_t *)
                page_manager_get_metadata(pm, live_page_ids[i]);
            entries[i] = (compaction_journal_entry_t){
                .page_id = live_page_ids[i],
                .old_offset = meta->file_offset,
                .new_offset = write_cursor,
                .compressed_size = meta->compressed_size,
                .padding = 0,
            };
            write_cursor += meta->compressed_size;
        }

        // Write the journal
        char journal_path[512];
        snprintf(journal_path, sizeof(journal_path), "%s/compaction.journal", TEST_DB_PATH);

        int jfd = open(journal_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        TEST_ASSERT(jfd >= 0, "Journal file created");

        compaction_journal_header_t header = {
            .magic = COMPACTION_JOURNAL_MAGIC,
            .version = COMPACTION_JOURNAL_VERSION,
            .state = COMPACTION_STATE_COMPACTING,
            .file_idx = 0,
            .num_entries = 5,
            .final_file_size = write_cursor,
            .crc = 0,
            .padding = 0,
        };
        header.crc = compute_crc32((const uint8_t *)&header,
                                    offsetof(compaction_journal_header_t, crc));

        write(jfd, &header, sizeof(header));
        write(jfd, entries, sizeof(entries));
        fsync(jfd);
        close(jfd);

        // Save index/metadata before "crash"
        page_manager_save_index(pm);
        page_manager_save_metadata(pm, 0);

        page_manager_destroy(pm);
    }

    // Phase 2: reopen (simulating restart after crash) and recover
    {
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        TEST_ASSERT(pm != NULL, "PM created (phase 2)");

        // Journal should exist
        char journal_path[512];
        snprintf(journal_path, sizeof(journal_path), "%s/compaction.journal", TEST_DB_PATH);
        TEST_ASSERT(access(journal_path, F_OK) == 0, "Journal exists before recovery");

        // Run recovery
        bool ok = db_compaction_recover(pm);
        TEST_ASSERT(ok, "Recovery succeeded");

        // Journal should be deleted
        TEST_ASSERT(access(journal_path, F_OK) != 0, "Journal deleted after recovery");

        // Verify live pages are readable at new locations
        for (int i = 0; i < 5; i++) {
            page_t page;
            page_result_t result = page_manager_read(pm, live_page_ids[i], &page);
            TEST_ASSERT(result == PAGE_SUCCESS, "Page readable after recovery");
            TEST_ASSERT(page.data[0] == (uint8_t)(i + 6), "Page data correct after recovery");
        }

        page_manager_destroy(pm);
    }
}

void test_compact_recovery_done_state(void) {
    // Journal with DONE state should just be deleted
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "PM created");

    // Write a journal with DONE state
    char journal_path[512];
    snprintf(journal_path, sizeof(journal_path), "%s/compaction.journal", TEST_DB_PATH);

    int jfd = open(journal_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    TEST_ASSERT(jfd >= 0, "Journal created");

    compaction_journal_header_t header = {
        .magic = COMPACTION_JOURNAL_MAGIC,
        .version = COMPACTION_JOURNAL_VERSION,
        .state = COMPACTION_STATE_DONE,
        .file_idx = 0,
        .num_entries = 0,
        .final_file_size = 0,
        .crc = 0,
        .padding = 0,
    };
    header.crc = compute_crc32((const uint8_t *)&header,
                                offsetof(compaction_journal_header_t, crc));
    write(jfd, &header, sizeof(header));
    fsync(jfd);
    close(jfd);

    bool ok = db_compaction_recover(pm);
    TEST_ASSERT(ok, "Recovery for DONE state succeeded");
    TEST_ASSERT(access(journal_path, F_OK) != 0, "DONE journal deleted");

    page_manager_destroy(pm);
}

void test_compact_no_journal_recovery(void) {
    // No journal file — recovery should be a no-op
    cleanup_test_db();
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "PM created");

    bool ok = db_compaction_recover(pm);
    TEST_ASSERT(ok, "Recovery with no journal succeeded");

    page_manager_destroy(pm);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== Database Compaction Tests (In-Place) ===\n");

    RUN_TEST(test_analyze_empty);
    RUN_TEST(test_analyze_no_dead);
    RUN_TEST(test_analyze_with_dead);
    RUN_TEST(test_is_recommended);
    RUN_TEST(test_compact_no_dead);
    RUN_TEST(test_compact_basic);
    RUN_TEST(test_compact_preserves_compression);
    RUN_TEST(test_compact_reclaims_space);
    RUN_TEST(test_compact_dry_run);
    RUN_TEST(test_compact_continues_after);
    RUN_TEST(test_compact_persistence);
    RUN_TEST(test_compact_dead_at_end);
    RUN_TEST(test_compact_interleaved_dead);
    RUN_TEST(test_compact_journal_recovery);
    RUN_TEST(test_compact_recovery_done_state);
    RUN_TEST(test_compact_no_journal_recovery);

    printf("\n=== Results: %d passed, %d failed ===\n", tests_passed, tests_failed);

    cleanup_test_db();
    return tests_failed > 0 ? 1 : 0;
}
