/**
 * WAL (Write-Ahead Log) Tests
 * 
 * Tests WAL lifecycle, write operations, transactions, durability,
 * checkpoint coordination, and segment management.
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

// Cleanup test directory
static void cleanup_test_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

// Count files in directory
static uint32_t count_files_in_dir(const char *dir, const char *prefix) {
    DIR *d = opendir(dir);
    if (!d) return 0;
    
    uint32_t count = 0;
    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (strncmp(entry->d_name, prefix, strlen(prefix)) == 0) {
            count++;
        }
    }
    closedir(d);
    return count;
}

// ============================================================================
// Test Cases
// ============================================================================

TEST(test_wal_lifecycle) {
    const char *test_dir = "/tmp/test_wal_lifecycle";
    cleanup_test_dir(test_dir);
    
    // Test default config
    wal_config_t config = wal_default_config();
    ASSERT_EQ(config.checkpoint_size_threshold, WAL_CHECKPOINT_SIZE_THRESHOLD,
              "Default checkpoint size threshold");
    ASSERT_EQ(config.segment_size, WAL_SEGMENT_SIZE, "Default segment size");
    ASSERT(config.fsync_on_commit, "Default fsync on commit");
    
    // Create new WAL
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created successfully");
    ASSERT_EQ(wal_current_lsn(wal), 0, "Initial LSN is 0");
    
    // Verify directory created
    struct stat st;
    ASSERT(stat(test_dir, &st) == 0 && S_ISDIR(st.st_mode),
           "WAL directory created");
    
    // Verify first segment created
    uint32_t segment_count = count_files_in_dir(test_dir, "wal_");
    ASSERT_EQ(segment_count, 1, "First segment created");
    
    // Close and reopen
    wal_close(wal);
    
    wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL reopened successfully");
    ASSERT_EQ(wal_current_lsn(wal), 0, "LSN preserved after reopen");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_insert_operations) {
    const char *test_dir = "/tmp/test_wal_insert";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Log insert
    const uint8_t key[] = "test_key";
    const uint8_t value[] = "test_value_12345";
    uint64_t lsn = 0;
    
    bool success = wal_log_insert(wal, 1, key, sizeof(key), value, sizeof(value), &lsn);
    ASSERT(success, "Insert logged successfully");
    ASSERT_EQ(lsn, 1, "First LSN is 1");
    ASSERT_EQ(wal_current_lsn(wal), 1, "Current LSN updated");
    
    // Log multiple inserts
    for (int i = 0; i < 10; i++) {
        char key_buf[32];
        char value_buf[64];
        snprintf(key_buf, sizeof(key_buf), "key_%d", i);
        snprintf(value_buf, sizeof(value_buf), "value_%d_data", i);
        
        success = wal_log_insert(wal, 1,
                                (uint8_t *)key_buf, strlen(key_buf) + 1,
                                (uint8_t *)value_buf, strlen(value_buf) + 1,
                                NULL);
        ASSERT(success, "Multiple inserts logged");
    }
    
    ASSERT_EQ(wal_current_lsn(wal), 11, "LSN incremented correctly");
    
    // Check statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.total_entries, 11, "Entry count correct");
    ASSERT_EQ(stats.inserts, 11, "Insert count correct");
    ASSERT(stats.total_bytes > 0, "Bytes written");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_delete_operations) {
    const char *test_dir = "/tmp/test_wal_delete";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Log delete
    const uint8_t key[] = "key_to_delete";
    uint64_t lsn = 0;
    
    bool success = wal_log_delete(wal, 1, key, sizeof(key), &lsn);
    ASSERT(success, "Delete logged successfully");
    ASSERT_EQ(lsn, 1, "Delete LSN is 1");
    
    // Check statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.deletes, 1, "Delete count correct");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_transactions) {
    const char *test_dir = "/tmp/test_wal_txn";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Begin transaction
    uint64_t txn_id = 0;
    uint64_t begin_lsn = 0;
    bool success = wal_log_begin_txn(wal, &txn_id, &begin_lsn);
    ASSERT(success, "Begin transaction logged");
    ASSERT(txn_id > 0, "Transaction ID assigned");
    ASSERT_EQ(begin_lsn, 1, "Begin LSN is 1");
    
    // Log operations within transaction
    const uint8_t key[] = "txn_key";
    const uint8_t value[] = "txn_value";
    success = wal_log_insert(wal, txn_id, key, sizeof(key), value, sizeof(value), NULL);
    ASSERT(success, "Insert in transaction logged");
    
    // Commit transaction
    uint64_t commit_lsn = 0;
    success = wal_log_commit_txn(wal, txn_id, &commit_lsn);
    ASSERT(success, "Commit transaction logged");
    ASSERT(commit_lsn > begin_lsn, "Commit LSN after begin");
    
    // Check statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.commits, 1, "Commit count correct");
    
    // Test abort transaction
    uint64_t abort_txn_id = 0;
    success = wal_log_begin_txn(wal, &abort_txn_id, NULL);
    ASSERT(success, "Second transaction started");
    
    uint64_t abort_lsn = 0;
    success = wal_log_abort_txn(wal, abort_txn_id, &abort_lsn);
    ASSERT(success, "Abort transaction logged");
    
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.aborts, 1, "Abort count correct");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_checkpoint) {
    const char *test_dir = "/tmp/test_wal_checkpoint";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Initially should not need checkpoint
    ASSERT(!wal_should_checkpoint(wal, NULL), "No checkpoint needed initially");
    
    // Log checkpoint
    uint64_t checkpoint_lsn = 0;
    bool success = wal_log_checkpoint(wal, 1, 0, 100, 2, &checkpoint_lsn);
    ASSERT(success, "Checkpoint logged successfully");
    ASSERT_EQ(checkpoint_lsn, 1, "Checkpoint LSN is 1");
    
    // Check statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.checkpoints, 1, "Checkpoint count correct");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_checkpoint_triggers) {
    const char *test_dir = "/tmp/test_wal_triggers";
    cleanup_test_dir(test_dir);
    
    // Configure low thresholds for testing
    wal_config_t config = wal_default_config();
    config.checkpoint_size_threshold = 1024;  // 1KB
    config.checkpoint_time_threshold = 2;      // 2 seconds
    config.checkpoint_pages_threshold = 10;    // 10 pages
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Test WAL size trigger
    uint8_t large_value[512];
    memset(large_value, 'A', sizeof(large_value));
    
    for (int i = 0; i < 5; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      large_value, sizeof(large_value), NULL);
    }
    
    uint32_t trigger = 0;
    bool needs_checkpoint = wal_should_checkpoint(wal, &trigger);
    ASSERT(needs_checkpoint, "WAL size trigger activated");
    ASSERT_EQ(trigger, 1, "Trigger type is WAL size");
    
    // Mark checkpoint completed
    wal_checkpoint_completed(wal, wal_current_lsn(wal));
    ASSERT(!wal_should_checkpoint(wal, NULL), "Checkpoint reset state");
    
    // Test dirty pages trigger
    wal_update_dirty_pages(wal, 15);
    needs_checkpoint = wal_should_checkpoint(wal, &trigger);
    ASSERT(needs_checkpoint, "Dirty pages trigger activated");
    ASSERT_EQ(trigger, 3, "Trigger type is dirty pages");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_fsync) {
    const char *test_dir = "/tmp/test_wal_fsync";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.fsync_on_commit = true;
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Log insert and commit (should fsync)
    uint64_t txn_id = 0;
    wal_log_begin_txn(wal, &txn_id, NULL);
    
    const uint8_t key[] = "sync_key";
    const uint8_t value[] = "sync_value";
    wal_log_insert(wal, txn_id, key, sizeof(key), value, sizeof(value), NULL);
    
    bool success = wal_log_commit_txn(wal, txn_id, NULL);
    ASSERT(success, "Commit with fsync succeeded");
    
    // Manual fsync
    success = wal_fsync(wal);
    ASSERT(success, "Manual fsync succeeded");
    
    // Check fsync stats
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT(stats.fsync_calls > 0, "Fsync was called");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_segment_rotation) {
    const char *test_dir = "/tmp/test_wal_rotation";
    cleanup_test_dir(test_dir);
    
    // Configure small segment size for testing
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;  // 8KB segments
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Verify initial segment count
    uint32_t initial_segments = count_files_in_dir(test_dir, "wal_");
    ASSERT_EQ(initial_segments, 1, "Started with one segment");
    
    // Write enough data to trigger rotation
    uint8_t large_value[1024];
    memset(large_value, 'B', sizeof(large_value));
    
    for (int i = 0; i < 20; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      large_value, sizeof(large_value), NULL);
    }
    
    // Force buffer flush
    wal_fsync(wal);
    
    // Check if new segment was created
    uint32_t final_segments = count_files_in_dir(test_dir, "wal_");
    ASSERT(final_segments > initial_segments, "Segment rotation occurred");
    
    // Check statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    ASSERT(stats.total_segments > 1, "Multiple segments created");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_truncate) {
    const char *test_dir = "/tmp/test_wal_truncate";
    cleanup_test_dir(test_dir);
    
    // Configure small segments
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;
    config.keep_segments = 2;
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Create multiple segments
    uint8_t value[1024];
    memset(value, 'C', sizeof(value));
    
    for (int i = 0; i < 30; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      value, sizeof(value), NULL);
    }
    wal_fsync(wal);
    
    uint32_t segments_before = count_files_in_dir(test_dir, "wal_");
    ASSERT(segments_before >= 3, "Multiple segments created");
    
    // Truncate old segments
    uint32_t truncated = wal_truncate(wal, wal_current_lsn(wal));
    ASSERT(truncated > 0, "Segments were truncated");
    
    uint32_t segments_after = count_files_in_dir(test_dir, "wal_");
    ASSERT(segments_after <= config.keep_segments, "Old segments removed");
    ASSERT(segments_after < segments_before, "Segment count decreased");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_list_segments) {
    const char *test_dir = "/tmp/test_wal_list";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Create some segments
    uint8_t value[1024];
    memset(value, 'D', sizeof(value));
    
    for (int i = 0; i < 20; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      value, sizeof(value), NULL);
    }
    wal_fsync(wal);
    
    // List segments
    uint32_t segment_count = 0;
    uint64_t *segments = wal_list_segments(test_dir, &segment_count);
    
    ASSERT(segments != NULL, "Segments listed");
    ASSERT(segment_count > 0, "Found segments");
    
    // Verify segments are sorted
    for (uint32_t i = 1; i < segment_count; i++) {
        ASSERT(segments[i] > segments[i-1], "Segments sorted in order");
    }
    
    free(segments);
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_persistence) {
    const char *test_dir = "/tmp/test_wal_persistence";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write some entries
    for (int i = 0; i < 5; i++) {
        char key[32];
        char value[64];
        snprintf(key, sizeof(key), "persist_key_%d", i);
        snprintf(value, sizeof(value), "persist_value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    uint64_t last_lsn = wal_current_lsn(wal);
    ASSERT(last_lsn > 0, "Entries logged");
    
    wal_close(wal);
    
    // Reopen and verify LSN persisted
    wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL reopened");
    ASSERT_EQ(wal_current_lsn(wal), last_lsn, "LSN persisted across restart");
    
    // Can continue writing
    const uint8_t key[] = "new_key";
    const uint8_t value[] = "new_value";
    uint64_t new_lsn = 0;
    bool success = wal_log_insert(wal, 1, key, sizeof(key), value, sizeof(value), &new_lsn);
    ASSERT(success, "Can write after reopen");
    ASSERT(new_lsn > last_lsn, "LSN continues incrementing");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_statistics) {
    const char *test_dir = "/tmp/test_wal_stats";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Perform various operations
    uint64_t txn_id;
    wal_log_begin_txn(wal, &txn_id, NULL);
    
    const uint8_t key1[] = "stat_key_1";
    const uint8_t value1[] = "stat_value_1";
    wal_log_insert(wal, txn_id, key1, sizeof(key1), value1, sizeof(value1), NULL);
    
    const uint8_t key2[] = "stat_key_2";
    wal_log_delete(wal, txn_id, key2, sizeof(key2), NULL);
    
    wal_log_commit_txn(wal, txn_id, NULL);
    
    // Get statistics
    wal_stats_t stats;
    wal_get_stats(wal, &stats);
    
    ASSERT_EQ(stats.total_entries, 4, "Total entries correct");
    ASSERT_EQ(stats.inserts, 1, "Insert count correct");
    ASSERT_EQ(stats.deletes, 1, "Delete count correct");
    ASSERT_EQ(stats.commits, 1, "Commit count correct");
    ASSERT(stats.total_bytes > 0, "Bytes tracked");
    ASSERT(stats.fsync_calls > 0, "Fsync tracked");
    
    // Print statistics (for manual inspection)
    printf("\n");
    wal_print_stats(wal);
    
    // Reset statistics
    wal_reset_stats(wal);
    wal_get_stats(wal, &stats);
    ASSERT_EQ(stats.total_entries, 0, "Stats reset");
    ASSERT_EQ(stats.inserts, 0, "Insert stats reset");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

// ============================================================================
// Test Runner
// ============================================================================

int main(void) {
    printf("========================================\n");
    printf("WAL Test Suite\n");
    printf("========================================\n");
    
    // Run all tests
    run_test_wal_lifecycle();
    run_test_wal_insert_operations();
    run_test_wal_delete_operations();
    run_test_wal_transactions();
    run_test_wal_checkpoint();
    run_test_wal_checkpoint_triggers();
    run_test_wal_fsync();
    run_test_wal_segment_rotation();
    run_test_wal_truncate();
    run_test_wal_list_segments();
    run_test_wal_persistence();
    run_test_wal_statistics();
    
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
