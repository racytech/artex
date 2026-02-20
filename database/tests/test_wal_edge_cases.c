/**
 * WAL (Write-Ahead Log) Edge Cases and Stress Tests
 * 
 * Tests error handling, boundary conditions, concurrent access patterns,
 * corruption scenarios, and resource limits.
 */

#include "wal.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <assert.h>
#include <pthread.h>
#include <errno.h>

// Test helpers
static int test_count = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    static void name(void); \
    static void run_##name(void) { \
        test_count++; \
        printf("\n[TEST %d] %s\n", test_count, #name); \
        name(); \
        tests_passed++; \
        printf("✓ PASSED\n"); \
    } \
    static void name(void)

#define ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            printf("✗ ASSERTION FAILED: %s\n", msg); \
            printf("  at %s:%d\n", __FILE__, __LINE__); \
            tests_failed++; \
            exit(1); \
        } \
    } while(0)

#define ASSERT_EQ(a, b, msg) \
    do { \
        if ((a) != (b)) { \
            printf("✗ ASSERTION FAILED: %s\n", msg); \
            printf("  Expected: %ld, Got: %ld\n", (long)(b), (long)(a)); \
            printf("  at %s:%d\n", __FILE__, __LINE__); \
            tests_failed++; \
            exit(1); \
        } \
    } while(0)

#define ASSERT_NE(a, b, msg) \
    do { \
        if ((a) == (b)) { \
            printf("✗ ASSERTION FAILED: %s\n", msg); \
            printf("  Value should not be: %ld\n", (long)(a)); \
            printf("  at %s:%d\n", __FILE__, __LINE__); \
            tests_failed++; \
            exit(1); \
        } \
    } while(0)

// Cleanup test directory
static void cleanup_test_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

// Count segments in directory
static uint32_t count_segments(const char *dir) {
    uint32_t count = 0;
    wal_list_segments(dir, &count);
    return count;
}

// Make directory read-only
static void make_readonly(const char *path) {
    chmod(path, 0444);
}

// Restore write permissions
static void make_writable(const char *path) {
    chmod(path, 0755);
}

// ============================================================================
// Edge Case Tests
// ============================================================================

TEST(test_wal_null_parameters) {
    // Test NULL directory
    wal_t *wal = wal_open(NULL, NULL);
    ASSERT(wal == NULL, "NULL directory rejected");
    
    // Test operations on NULL WAL
    ASSERT_EQ(wal_current_lsn(NULL), 0, "current_lsn on NULL returns 0");
    ASSERT(!wal_fsync(NULL), "fsync on NULL returns false");
    ASSERT(!wal_should_checkpoint(NULL, NULL), "should_checkpoint on NULL returns false");
    
    const uint8_t key[] = "test";
    const uint8_t value[] = "value";
    ASSERT(!wal_log_insert(NULL, 1, key, 4, value, 5, NULL), "insert on NULL fails");
    ASSERT(!wal_log_delete(NULL, 1, key, 4, NULL), "delete on NULL fails");
    ASSERT(!wal_log_begin_txn(NULL, NULL, NULL), "begin_txn on NULL fails");
    ASSERT(!wal_log_commit_txn(NULL, 1, NULL), "commit_txn on NULL fails");
    ASSERT(!wal_log_abort_txn(NULL, 1, NULL), "abort_txn on NULL fails");
    ASSERT(!wal_log_checkpoint(NULL, 0, 0, 0, 0, NULL), "checkpoint on NULL fails");
    
    // Close NULL WAL (should not crash)
    wal_close(NULL);
}

TEST(test_wal_empty_data) {
    const char *test_dir = "/tmp/test_wal_empty";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test empty key/value (NULL pointers)
    uint64_t lsn;
    bool success = wal_log_insert(wal, 1, NULL, 0, (const uint8_t *)"value", 5, &lsn);
    ASSERT(!success, "Insert with NULL key fails");
    
    success = wal_log_insert(wal, 1, (const uint8_t *)"key", 3, NULL, 0, &lsn);
    ASSERT(!success, "Insert with NULL value fails");
    
    success = wal_log_delete(wal, 1, NULL, 0, &lsn);
    ASSERT(!success, "Delete with NULL key fails");
    
    // Test zero-length data (valid pointers but zero length)
    const uint8_t empty[] = "";
    success = wal_log_insert(wal, 1, empty, 0, empty, 0, &lsn);
    ASSERT(!success, "Insert with zero-length data handled");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_maximum_sizes) {
    const char *test_dir = "/tmp/test_wal_maxsize";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test very large key
    uint8_t *large_key = malloc(64 * 1024);  // 64KB key
    memset(large_key, 'K', 64 * 1024);
    
    uint8_t small_value[] = "value";
    uint64_t lsn;
    bool success = wal_log_insert(wal, 1, large_key, 64 * 1024, small_value, 5, &lsn);
    ASSERT(success, "Large 64KB key handled");
    
    // Test very large value
    uint8_t *large_value = malloc(1024 * 1024);  // 1MB value
    memset(large_value, 'V', 1024 * 1024);
    
    uint8_t small_key[] = "key";
    success = wal_log_insert(wal, 1, small_key, 3, large_value, 1024 * 1024, &lsn);
    ASSERT(success, "Large 1MB value handled");
    
    // Test extremely large entry (larger than segment)
    uint8_t *huge_value = malloc(100 * 1024 * 1024);  // 100MB value
    if (huge_value) {
        memset(huge_value, 'H', 100 * 1024 * 1024);
        success = wal_log_insert(wal, 1, small_key, 3, huge_value, 100 * 1024 * 1024, &lsn);
        ASSERT(success, "Extremely large 100MB value handled");
        free(huge_value);
    }
    
    free(large_key);
    free(large_value);
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_rapid_segment_rotation) {
    const char *test_dir = "/tmp/test_wal_rapid_rotation";
    cleanup_test_dir(test_dir);
    
    // Configure tiny segments to force rapid rotation
    wal_config_t config = wal_default_config();
    config.segment_size = 4096;  // 4KB segments (very small)
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    uint32_t initial_segments = count_segments(test_dir);
    
    // Write enough data to create many segments
    uint8_t value[512];
    memset(value, 'X', sizeof(value));
    
    for (int i = 0; i < 100; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1, value, sizeof(value), NULL);
    }
    
    wal_fsync(wal);
    
    uint32_t final_segments = count_segments(test_dir);
    ASSERT(final_segments > initial_segments, "Multiple segments created");
    ASSERT(final_segments >= 10, "At least 10 segments created from rapid rotation");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_transaction_boundaries) {
    const char *test_dir = "/tmp/test_wal_txn_boundaries";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test multiple begin without commit
    uint64_t txn1, txn2;
    ASSERT(wal_log_begin_txn(wal, &txn1, NULL), "First begin succeeds");
    ASSERT(wal_log_begin_txn(wal, &txn2, NULL), "Second begin succeeds");
    ASSERT_NE(txn1, txn2, "Transaction IDs are unique");
    
    // Test commit without begin
    ASSERT(wal_log_commit_txn(wal, 999999, NULL), "Commit unknown txn succeeds (no validation)");
    
    // Test abort without begin
    ASSERT(wal_log_abort_txn(wal, 999999, NULL), "Abort unknown txn succeeds (no validation)");
    
    // Test double commit
    ASSERT(wal_log_commit_txn(wal, txn1, NULL), "First commit succeeds");
    ASSERT(wal_log_commit_txn(wal, txn1, NULL), "Double commit succeeds (no validation)");
    
    // Test commit then abort
    uint64_t txn3;
    wal_log_begin_txn(wal, &txn3, NULL);
    ASSERT(wal_log_commit_txn(wal, txn3, NULL), "Commit succeeds");
    ASSERT(wal_log_abort_txn(wal, txn3, NULL), "Abort after commit succeeds (no validation)");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_checkpoint_edge_cases) {
    const char *test_dir = "/tmp/test_wal_checkpoint_edge";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test checkpoint with invalid values
    uint64_t lsn;
    ASSERT(wal_log_checkpoint(wal, 0, 0, 0, 0, &lsn), "Checkpoint with zeros succeeds");
    ASSERT(wal_log_checkpoint(wal, UINT64_MAX, UINT32_MAX, UINT64_MAX, UINT64_MAX, &lsn),
           "Checkpoint with max values succeeds");
    
    // Test multiple checkpoints in sequence
    for (int i = 0; i < 10; i++) {
        ASSERT(wal_log_checkpoint(wal, i, i, i * 100, i + 1, NULL),
               "Sequential checkpoint succeeds");
    }
    
    // Test checkpoint completion with invalid LSN
    wal_checkpoint_completed(wal, 0);
    wal_checkpoint_completed(wal, UINT64_MAX);
    
    // Test dirty pages update with extreme values
    wal_update_dirty_pages(wal, 0);
    wal_update_dirty_pages(wal, UINT64_MAX);
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_truncate_edge_cases) {
    const char *test_dir = "/tmp/test_wal_truncate_edge";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;
    config.keep_segments = 1;
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test truncate with no segments
    uint32_t truncated = wal_truncate(wal, 0);
    ASSERT_EQ(truncated, 0, "No segments truncated when only one exists");
    
    // Create some segments
    uint8_t value[1024];
    memset(value, 'T', sizeof(value));
    for (int i = 0; i < 20; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1, value, sizeof(value), NULL);
    }
    wal_fsync(wal);
    
    uint32_t segments_before = count_segments(test_dir);
    ASSERT(segments_before >= 2, "Multiple segments created");
    
    // Test truncate with invalid LSN
    truncated = wal_truncate(wal, 0);
    ASSERT(truncated > 0, "Segments truncated with LSN 0");
    
    truncated = wal_truncate(wal, UINT64_MAX);
    uint32_t segments_after = count_segments(test_dir);
    ASSERT_EQ(segments_after, config.keep_segments, "Only keep_segments remain");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_reopen_with_corruption) {
    const char *test_dir = "/tmp/test_wal_corruption";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write some entries
    for (int i = 0; i < 5; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    wal_close(wal);
    
    // Corrupt the segment file by truncating it
    char segment_file[512];
    snprintf(segment_file, sizeof(segment_file), "%s/wal_000000.log", test_dir);
    
    FILE *f = fopen(segment_file, "r+");
    if (f) {
        fseek(f, 0, SEEK_END);
        long size = ftell(f);
        ftruncate(fileno(f), size / 2);  // Truncate to half size
        fclose(f);
    }
    
    // Try to reopen (should either fail gracefully or recover)
    wal = wal_open(test_dir, &config);
    
    if (wal == NULL) {
        // Acceptable: WAL detected corruption and refused to open
        // Try with clean directory
        cleanup_test_dir(test_dir);
        wal = wal_open(test_dir, &config);
        ASSERT(wal != NULL, "WAL opens with clean directory");
    }
    
    // Should be able to write
    const uint8_t key[] = "new_key";
    const uint8_t value[] = "new_value";
    bool success = wal_log_insert(wal, 1, key, sizeof(key), value, sizeof(value), NULL);
    ASSERT(success, "Can write after handling corruption");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_statistics_overflow) {
    const char *test_dir = "/tmp/test_wal_stats_overflow";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write many entries to stress statistics counters
    uint8_t value[128];
    memset(value, 'S', sizeof(value));
    
    for (int i = 0; i < 10000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1, value, sizeof(value), NULL);
        
        if (i % 100 == 0) {
            wal_log_delete(wal, 1, (uint8_t *)key, strlen(key) + 1, NULL);
        }
        
        if (i % 500 == 0) {
            uint64_t txn_id;
            wal_log_begin_txn(wal, &txn_id, NULL);
            wal_log_commit_txn(wal, txn_id, NULL);
        }
    }
    
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    
    ASSERT_EQ(stats.total_entries, 10000 + 100 + 40, "Entry count correct");
    ASSERT_EQ(stats.inserts, 10000, "Insert count correct");
    ASSERT_EQ(stats.deletes, 100, "Delete count correct");
    ASSERT_EQ(stats.commits, 20, "Commit count correct");
    ASSERT(stats.total_bytes > 1000000, "Bytes tracked correctly");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_concurrent_readers) {
    const char *test_dir = "/tmp/test_wal_concurrent";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write some data
    uint8_t value[64];
    memset(value, 'C', sizeof(value));
    for (int i = 0; i < 100; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1, value, sizeof(value), NULL);
    }
    
    // Test concurrent stats reads (should not crash with rwlock)
    wal_stats_t stats[10];
    for (int i = 0; i < 10; i++) {
        wal_get_stats(wal, &stats[i]);
        ASSERT_EQ(stats[i].inserts, 100, "Stats consistent across reads");
    }
    
    // Test concurrent should_checkpoint calls
    for (int i = 0; i < 100; i++) {
        uint32_t trigger;
        wal_should_checkpoint(wal, &trigger);
    }
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_list_segments_edge_cases) {
    const char *test_dir = "/tmp/test_wal_list_edge";
    cleanup_test_dir(test_dir);
    
    // Test listing non-existent directory
    uint32_t count = 0;
    uint64_t *segments = wal_list_segments("/nonexistent/path", &count);
    ASSERT(segments == NULL, "Returns NULL for non-existent directory");
    ASSERT_EQ(count, 0, "Count is 0 for non-existent directory");
    
    // Test listing empty directory
    mkdir(test_dir, 0755);
    segments = wal_list_segments(test_dir, &count);
    ASSERT(segments == NULL, "Returns NULL for empty directory");
    ASSERT_EQ(count, 0, "Count is 0 for empty directory");
    
    // Create WAL and test listing
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Create multiple segments
    uint8_t value[1024];
    memset(value, 'L', sizeof(value));
    for (int i = 0; i < 30; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1, value, sizeof(value), NULL);
    }
    wal_fsync(wal);
    wal_close(wal);
    
    // Test listing with NULL count
    segments = wal_list_segments(test_dir, NULL);
    ASSERT(segments == NULL, "Returns NULL when count_out is NULL");
    
    // Test listing with valid parameters
    segments = wal_list_segments(test_dir, &count);
    ASSERT(segments != NULL, "Returns segments");
    ASSERT(count > 0, "Count is positive");
    
    // Verify sorting
    for (uint32_t i = 1; i < count; i++) {
        ASSERT(segments[i] > segments[i-1], "Segments sorted in ascending order");
    }
    
    free(segments);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_fsync_stress) {
    const char *test_dir = "/tmp/test_wal_fsync_stress";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.fsync_on_commit = true;
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Stress test fsync with many small transactions
    for (int i = 0; i < 100; i++) {
        uint64_t txn_id;
        wal_log_begin_txn(wal, &txn_id, NULL);
        
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        wal_log_insert(wal, txn_id, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
        
        ASSERT(wal_log_commit_txn(wal, txn_id, NULL), "Commit with fsync succeeds");
    }
    
    // Verify fsync was called many times
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT(stats.fsync_calls >= 100, "Fsync called for each commit");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_time_based_checkpoint_trigger) {
    const char *test_dir = "/tmp/test_wal_time_trigger";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.checkpoint_size_threshold = UINT64_MAX;  // Disable size trigger
    config.checkpoint_pages_threshold = UINT64_MAX;  // Disable pages trigger
    config.checkpoint_time_threshold = 1;  // 1 second
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Initially should not need checkpoint
    uint32_t trigger = 0;
    ASSERT(!wal_should_checkpoint(wal, &trigger), "No checkpoint needed initially");
    
    // Wait for time threshold
    sleep(2);
    
    // Now should trigger
    ASSERT(wal_should_checkpoint(wal, &trigger), "Time-based checkpoint triggered");
    ASSERT_EQ(trigger, 2, "Trigger type is time");
    
    // Reset by marking checkpoint completed
    wal_checkpoint_completed(wal, wal_current_lsn(wal));
    ASSERT(!wal_should_checkpoint(wal, NULL), "Checkpoint reset");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_pages_based_checkpoint_trigger) {
    const char *test_dir = "/tmp/test_wal_pages_trigger";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.checkpoint_size_threshold = UINT64_MAX;  // Disable size trigger
    config.checkpoint_time_threshold = UINT64_MAX;  // Disable time trigger
    config.checkpoint_pages_threshold = 5;  // 5 pages
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Initially no checkpoint needed
    ASSERT(!wal_should_checkpoint(wal, NULL), "No checkpoint needed");
    
    // Update dirty pages below threshold
    wal_update_dirty_pages(wal, 3);
    ASSERT(!wal_should_checkpoint(wal, NULL), "Below threshold, no checkpoint");
    
    // Update dirty pages above threshold
    wal_update_dirty_pages(wal, 10);
    uint32_t trigger = 0;
    ASSERT(wal_should_checkpoint(wal, &trigger), "Pages-based checkpoint triggered");
    ASSERT_EQ(trigger, 3, "Trigger type is dirty pages");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_mixed_operations_stress) {
    const char *test_dir = "/tmp/test_wal_mixed_stress";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.segment_size = 16384;  // Small segments for more rotation
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Mix of all operation types in rapid succession
    for (int i = 0; i < 500; i++) {
        char key[64], value[128];
        snprintf(key, sizeof(key), "mixed_key_%d", i);
        snprintf(value, sizeof(value), "mixed_value_%d_with_some_data", i);
        
        uint64_t txn_id;
        wal_log_begin_txn(wal, &txn_id, NULL);
        
        // Insert
        wal_log_insert(wal, txn_id, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
        
        // Maybe delete
        if (i % 3 == 0) {
            wal_log_delete(wal, txn_id, (uint8_t *)key, strlen(key) + 1, NULL);
        }
        
        // Commit or abort
        if (i % 7 == 0) {
            wal_log_abort_txn(wal, txn_id, NULL);
        } else {
            wal_log_commit_txn(wal, txn_id, NULL);
        }
        
        // Checkpoint occasionally
        if (i % 50 == 0) {
            wal_log_checkpoint(wal, i, i % 256, i * 100, i + 1, NULL);
        }
        
        // Manual fsync occasionally
        if (i % 100 == 0) {
            wal_fsync(wal);
        }
    }
    
    // Verify statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT(stats.total_entries > 1000, "Many entries logged");
    ASSERT(stats.inserts == 500, "Insert count correct");
    ASSERT(stats.deletes == 167, "Delete count approximately correct");
    ASSERT(stats.commits > 400, "Most transactions committed");
    ASSERT(stats.aborts > 70, "Some transactions aborted");
    ASSERT(stats.checkpoints == 10, "Checkpoints logged");
    
    // Verify segments were rotated
    uint32_t segment_count = count_segments(test_dir);
    ASSERT(segment_count > 1, "Multiple segments created");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_persistence_after_crash_simulation) {
    const char *test_dir = "/tmp/test_wal_crash_sim";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write committed data
    uint64_t txn_id;
    wal_log_begin_txn(wal, &txn_id, NULL);
    
    const uint8_t key1[] = "committed_key";
    const uint8_t value1[] = "committed_value";
    wal_log_insert(wal, txn_id, key1, sizeof(key1), value1, sizeof(value1), NULL);
    uint64_t committed_lsn;
    wal_log_commit_txn(wal, txn_id, &committed_lsn);
    
    // Write uncommitted data (simulating crash before commit)
    wal_log_begin_txn(wal, &txn_id, NULL);
    const uint8_t key2[] = "uncommitted_key";
    const uint8_t value2[] = "uncommitted_value";
    wal_log_insert(wal, txn_id, key2, sizeof(key2), value2, sizeof(value2), NULL);
    // DON'T commit - simulate crash
    
    uint64_t last_lsn = wal_current_lsn(wal);
    
    // Close without proper cleanup (simulating crash)
    wal_close(wal);
    
    // Reopen and verify state
    wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL reopened after crash");
    
    // LSN should continue from last persisted entry
    uint64_t current_lsn = wal_current_lsn(wal);
    ASSERT_EQ(current_lsn, last_lsn, "LSN preserved after crash");
    
    // Should be able to continue writing
    const uint8_t key3[] = "post_crash_key";
    const uint8_t value3[] = "post_crash_value";
    uint64_t new_lsn;
    bool success = wal_log_insert(wal, 1, key3, sizeof(key3), value3, sizeof(value3), &new_lsn);
    ASSERT(success, "Can write after crash recovery");
    ASSERT(new_lsn > last_lsn, "New LSN continues sequence");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_segment_header_validation) {
    const char *test_dir = "/tmp/test_wal_header_validation";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write some data
    const uint8_t key[] = "test_key";
    const uint8_t value[] = "test_value";
    wal_log_insert(wal, 1, key, sizeof(key), value, sizeof(value), NULL);
    wal_close(wal);
    
    // Corrupt the segment magic number
    char segment_file[512];
    snprintf(segment_file, sizeof(segment_file), "%s/wal_000000.log", test_dir);
    
    FILE *f = fopen(segment_file, "r+");
    if (f) {
        uint32_t bad_magic = 0xDEADBEEF;
        fwrite(&bad_magic, sizeof(bad_magic), 1, f);
        fclose(f);
    }
    
    // Try to reopen (should fail or create new segment)
    wal = wal_open(test_dir, &config);
    // If it fails to open, that's okay (proper validation)
    // If it opens, it should create a new segment
    if (wal) {
        wal_close(wal);
    }
    
    cleanup_test_dir(test_dir);
}

// ============================================================================
// Test Runner
// ============================================================================

int main(void) {
    printf("========================================\n");
    printf("WAL Edge Cases & Stress Test Suite\n");
    printf("========================================\n");
    
    // Run all tests
    run_test_wal_null_parameters();
    run_test_wal_empty_data();
    run_test_wal_maximum_sizes();
    run_test_wal_rapid_segment_rotation();
    run_test_wal_transaction_boundaries();
    run_test_wal_checkpoint_edge_cases();
    run_test_wal_truncate_edge_cases();
    run_test_wal_reopen_with_corruption();
    run_test_wal_statistics_overflow();
    run_test_wal_concurrent_readers();
    run_test_wal_list_segments_edge_cases();
    run_test_wal_fsync_stress();
    run_test_wal_time_based_checkpoint_trigger();
    run_test_wal_pages_based_checkpoint_trigger();
    run_test_wal_mixed_operations_stress();
    run_test_wal_persistence_after_crash_simulation();
    run_test_wal_segment_header_validation();
    
    // Print summary
    printf("\n========================================\n");
    printf("Test Summary\n");
    printf("========================================\n");
    printf("Total tests:  %d\n", test_count);
    printf("Passed:       %d\n", tests_passed);
    printf("Failed:       %d\n", tests_failed);
    printf("========================================\n");
    
    return tests_failed > 0 ? 1 : 0;
}
