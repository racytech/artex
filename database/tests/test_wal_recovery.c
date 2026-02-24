/**
 * WAL Recovery Functions Test
 * 
 * Tests wal_replay, wal_find_last_lsn, and wal_validate_segment implementations.
 */

#include "wal.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
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

// Simple apply function that counts entries
typedef struct {
    uint64_t insert_count;
    uint64_t delete_count;
    uint64_t commit_count;
    uint64_t abort_count;
    uint64_t checkpoint_count;
    uint64_t last_lsn_seen;
} replay_context_t;

static bool test_apply_fn(void *ctx, const wal_entry_header_t *header,
                         const void *payload) {
    replay_context_t *context = (replay_context_t *)ctx;
    
    context->last_lsn_seen = header->lsn;
    
    switch (header->entry_type) {
        case WAL_ENTRY_INSERT:
            context->insert_count++;
            break;
        case WAL_ENTRY_DELETE:
            context->delete_count++;
            break;
        case WAL_ENTRY_COMMIT_TXN:
            context->commit_count++;
            break;
        case WAL_ENTRY_ABORT_TXN:
            context->abort_count++;
            break;
        case WAL_ENTRY_CHECKPOINT:
            context->checkpoint_count++;
            break;
        default:
            break;
    }
    
    return true;
}

// ============================================================================
// Test Cases
// ============================================================================

TEST(test_wal_find_last_lsn_empty) {
    const char *test_dir = "/tmp/test_wal_find_lsn_empty";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Empty WAL should return 0
    uint64_t last_lsn = wal_find_last_lsn(wal);
    ASSERT_EQ(last_lsn, 0, "Empty WAL returns LSN 0");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_find_last_lsn_with_entries) {
    const char *test_dir = "/tmp/test_wal_find_lsn";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write some entries
    for (int i = 0; i < 10; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    uint64_t expected_lsn = wal_current_lsn(wal);
    wal_close(wal);
    
    // Reopen and find last LSN
    wal = wal_open(test_dir, &config);
    uint64_t found_lsn = wal_find_last_lsn(wal);
    
    ASSERT_EQ(found_lsn, expected_lsn, "Found correct last LSN");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_validate_segment_valid) {
    const char *test_dir = "/tmp/test_wal_validate_valid";
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
    wal_fsync(wal);
    
    // Validate segment 0
    uint32_t errors = 0;
    bool valid = wal_validate_segment(wal, 0, &errors);
    
    ASSERT(valid, "Segment is valid");
    ASSERT_EQ(errors, 0, "No errors found");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_validate_segment_corrupted) {
    const char *test_dir = "/tmp/test_wal_validate_corrupt";
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
    
    // Corrupt the segment by writing garbage in the middle
    char segment_file[512];
    snprintf(segment_file, sizeof(segment_file), "%s/wal_000000.log", test_dir);
    
    FILE *f = fopen(segment_file, "r+");
    if (f) {
        fseek(f, 600, SEEK_SET);  // Skip header, corrupt an entry
        uint8_t garbage[32];
        memset(garbage, 0xFF, sizeof(garbage));
        fwrite(garbage, 1, sizeof(garbage), f);
        fclose(f);
    }
    
    // Validate (should detect corruption)
    wal = wal_open(test_dir, &config);
    uint32_t errors = 0;
    bool valid = wal_validate_segment(wal, 0, &errors);
    
    ASSERT(!valid, "Corruption detected");
    ASSERT(errors > 0, "Errors reported");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_empty) {
    const char *test_dir = "/tmp/test_wal_replay_empty";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    replay_context_t context = {0};
    int64_t replayed = wal_replay(wal, 1, 100, &context, test_apply_fn);
    
    ASSERT_EQ(replayed, 0, "No entries to replay");
    ASSERT_EQ(context.insert_count, 0, "No inserts");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_all_entries) {
    const char *test_dir = "/tmp/test_wal_replay_all";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write mixed operations
    uint64_t txn_id = 1;
    wal_log_begin_txn(wal, txn_id, NULL);
    
    for (int i = 0; i < 10; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        wal_log_insert(wal, txn_id, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    wal_log_delete(wal, txn_id, (const uint8_t *)"key_5", 6, NULL);
    wal_log_commit_txn(wal, txn_id, NULL);
    wal_log_checkpoint(wal, 1, 0, 100, 2, NULL);
    
    uint64_t end_lsn = wal_current_lsn(wal);
    
    // Replay all
    replay_context_t context = {0};
    int64_t replayed = wal_replay(wal, 1, end_lsn, &context, test_apply_fn);
    
    ASSERT(replayed > 0, "Entries replayed");
    ASSERT_EQ(context.insert_count, 10, "10 inserts replayed");
    ASSERT_EQ(context.delete_count, 1, "1 delete replayed");
    ASSERT_EQ(context.commit_count, 1, "1 commit replayed");
    ASSERT_EQ(context.checkpoint_count, 1, "1 checkpoint replayed");
    ASSERT_EQ(context.last_lsn_seen, end_lsn, "Last LSN matches");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_partial_range) {
    const char *test_dir = "/tmp/test_wal_replay_partial";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write 20 entries
    for (int i = 0; i < 20; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key_%d", i);
        snprintf(value, sizeof(value), "value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    // Replay only entries 5-15
    replay_context_t context = {0};
    int64_t replayed = wal_replay(wal, 5, 15, &context, test_apply_fn);
    
    ASSERT(replayed > 0, "Entries replayed");
    ASSERT_EQ(context.insert_count, 11, "11 inserts in range [5,15]");
    ASSERT(context.last_lsn_seen >= 5 && context.last_lsn_seen <= 15,
           "Last LSN in range");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_multiple_segments) {
    const char *test_dir = "/tmp/test_wal_replay_multi_seg";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    config.segment_size = 8192;  // Small segments
    
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write enough to create multiple segments
    uint8_t value[1024];
    memset(value, 'X', sizeof(value));
    
    for (int i = 0; i < 30; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      value, sizeof(value), NULL);
    }
    
    uint64_t end_lsn = wal_current_lsn(wal);
    
    // Replay all
    replay_context_t context = {0};
    int64_t replayed = wal_replay(wal, 1, end_lsn, &context, test_apply_fn);
    
    ASSERT(replayed > 0, "Entries replayed across segments");
    ASSERT_EQ(context.insert_count, 30, "All 30 inserts replayed");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_with_checkpoint) {
    const char *test_dir = "/tmp/test_wal_replay_checkpoint";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    // Write entries before checkpoint
    for (int i = 0; i < 5; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "old_key_%d", i);
        snprintf(value, sizeof(value), "old_value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    // Checkpoint
    uint64_t checkpoint_lsn;
    wal_log_checkpoint(wal, 1, 0, 5, 2, &checkpoint_lsn);
    
    // Write entries after checkpoint
    for (int i = 0; i < 5; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "new_key_%d", i);
        snprintf(value, sizeof(value), "new_value_%d", i);
        wal_log_insert(wal, 1, (uint8_t *)key, strlen(key) + 1,
                      (uint8_t *)value, strlen(value) + 1, NULL);
    }
    
    uint64_t end_lsn = wal_current_lsn(wal);
    
    // Replay from checkpoint (typical recovery scenario)
    replay_context_t context = {0};
    int64_t replayed = wal_replay(wal, checkpoint_lsn, end_lsn, &context, test_apply_fn);
    
    ASSERT(replayed > 0, "Entries after checkpoint replayed");
    // Should replay checkpoint + 5 new inserts
    ASSERT(context.insert_count == 5, "Only new inserts replayed");
    ASSERT_EQ(context.checkpoint_count, 1, "Checkpoint entry replayed");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

TEST(test_wal_replay_invalid_parameters) {
    const char *test_dir = "/tmp/test_wal_replay_invalid";
    cleanup_test_dir(test_dir);
    
    wal_config_t config = wal_default_config();
    wal_t *wal = wal_open(test_dir, &config);
    ASSERT(wal != NULL, "WAL created");
    
    replay_context_t context = {0};
    
    // NULL WAL
    int64_t result = wal_replay(NULL, 1, 10, &context, test_apply_fn);
    ASSERT_EQ(result, -1, "NULL WAL rejected");
    
    // NULL apply function
    result = wal_replay(wal, 1, 10, &context, NULL);
    ASSERT_EQ(result, -1, "NULL apply_fn rejected");
    
    // Invalid LSN range
    result = wal_replay(wal, 10, 1, &context, test_apply_fn);
    ASSERT_EQ(result, -1, "Invalid LSN range rejected");
    
    wal_close(wal);
    cleanup_test_dir(test_dir);
}

// ============================================================================
// Test Runner
// ============================================================================

int main(void) {
    printf("========================================\n");
    printf("WAL Recovery Functions Test Suite\n");
    printf("========================================\n");
    
    // Run all tests
    run_test_wal_find_last_lsn_empty();
    run_test_wal_find_last_lsn_with_entries();
    run_test_wal_validate_segment_valid();
    run_test_wal_validate_segment_corrupted();
    run_test_wal_replay_empty();
    run_test_wal_replay_all_entries();
    run_test_wal_replay_partial_range();
    run_test_wal_replay_multiple_segments();
    run_test_wal_replay_with_checkpoint();
    run_test_wal_replay_invalid_parameters();
    
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
