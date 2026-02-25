/**
 * Crash Recovery Test Suite for Persistent ART
 *
 * Proves durability and crash resilience by testing:
 *   1. End-to-end recovery (insert -> close -> reopen -> recover -> verify)
 *   2. Truncated WAL entry (simulated crash mid-write)
 *   3. Corrupted WAL entry (bit flip in payload)
 *   4. Checkpoint-based recovery
 *   5. Delete replay (insert → delete → crash → recover → verify deletions persisted)
 *   6. Uncommitted transaction skipped during recovery
 *   7. Mixed committed + uncommitted transactions
 */

#include "data_art.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "wal.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

// ============================================================================
// Test Framework
// ============================================================================

#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_RESET  "\033[0m"

static int test_count = 0;
static int tests_passed = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "  FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "    at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

#define ASSERT_EQ(a, b, msg) do { \
    long _a = (long)(a), _b = (long)(b); \
    if (_a != _b) { \
        fprintf(stderr, COLOR_RED "  FAILED: %s (got %ld, expected %ld)\n" COLOR_RESET, \
                msg, _a, _b); \
        fprintf(stderr, "    at %s:%d\n", __FILE__, __LINE__); \
        exit(1); \
    } \
} while(0)

// ============================================================================
// Key/Value Generation
// ============================================================================

#define KEY_SIZE      32
#define VALUE_MAX_LEN 64

static void generate_key(uint8_t *key, int index) {
    memset(key, 0, KEY_SIZE);
    key[0] = (uint8_t)((index >> 24) & 0xFF);
    key[1] = (uint8_t)((index >> 16) & 0xFF);
    key[2] = (uint8_t)((index >> 8) & 0xFF);
    key[3] = (uint8_t)(index & 0xFF);
    uint32_t state = (uint32_t)index * 2654435761u;
    for (int i = 4; i < KEY_SIZE; i++) {
        state = state * 1103515245 + 12345;
        key[i] = (uint8_t)((state >> 16) & 0xFF);
    }
}

static size_t generate_value(char *buf, size_t max_len, int index) {
    int n = snprintf(buf, max_len, "val_%06d_payload_data", index);
    return (size_t)n;
}

// ============================================================================
// Test Environment
// ============================================================================

#define BASE_DB_PATH  "/tmp/test_crash_recovery_db"
#define BASE_WAL_PATH "/tmp/test_crash_recovery_wal"

typedef struct {
    page_manager_t  *pm;
    buffer_pool_t   *bp;
    wal_t           *wal;
    data_art_tree_t *tree;
    char db_path[256];
    char wal_path[256];
} test_env_t;

static void cleanup_paths(const char *db_path, const char *wal_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", db_path, wal_path);
    system(cmd);
    sync();
    usleep(10000);
}

static void make_paths(test_env_t *env, const char *suffix) {
    snprintf(env->db_path, sizeof(env->db_path), "%s_%s", BASE_DB_PATH, suffix);
    snprintf(env->wal_path, sizeof(env->wal_path), "%s_%s", BASE_WAL_PATH, suffix);
}

static void open_env(test_env_t *env) {
    env->pm = page_manager_create(env->db_path, false);
    ASSERT(env->pm != NULL, "page_manager_create");

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    env->bp = buffer_pool_create(&bp_config, env->pm);
    ASSERT(env->bp != NULL, "buffer_pool_create");

    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = 8 * 1024 * 1024;  // 8MB
    env->wal = wal_open(env->wal_path, &wal_config);
    ASSERT(env->wal != NULL, "wal_open");

    env->tree = data_art_create(env->pm, env->bp, env->wal, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void close_env(test_env_t *env) {
    if (env->tree) { data_art_destroy(env->tree); env->tree = NULL; }
    if (env->wal)  { wal_close(env->wal);         env->wal = NULL; }
    if (env->bp)   { buffer_pool_destroy(env->bp); env->bp = NULL; }
    if (env->pm)   { page_manager_destroy(env->pm); env->pm = NULL; }
}

static void insert_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, start + i);
        size_t vlen = generate_value(value, sizeof(value), start + i);
        bool ok = data_art_insert(env->tree, key, KEY_SIZE, value, vlen);
        ASSERT(ok, "data_art_insert");
    }
}

static void verify_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char expected[VALUE_MAX_LEN];
        generate_key(key, start + i);
        size_t expected_len = generate_value(expected, sizeof(expected), start + i);

        size_t got_len = 0;
        const void *got = data_art_get(env->tree, key, KEY_SIZE, &got_len);
        if (!got) {
            char msg[128];
            snprintf(msg, sizeof(msg), "Key %d not found after recovery", start + i);
            ASSERT(false, msg);
        }
        ASSERT_EQ((long)got_len, (long)expected_len, "value length mismatch");
        ASSERT(memcmp(got, expected, got_len) == 0, "value content mismatch");
        free((void *)got);
    }
}

static void verify_key_absent(test_env_t *env, int index) {
    uint8_t key[KEY_SIZE];
    generate_key(key, index);
    size_t len = 0;
    const void *got = data_art_get(env->tree, key, KEY_SIZE, &len);
    if (got) {
        free((void *)got);
        char msg[128];
        snprintf(msg, sizeof(msg), "Key %d should NOT exist but was found", index);
        ASSERT(false, msg);
    }
}

static off_t get_file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    return st.st_size;
}

static void find_wal_segment(const char *wal_dir, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/wal_000000.log", wal_dir);
}

// ============================================================================
// Test 1: End-to-End Recovery
// ============================================================================

static void test_end_to_end_recovery(void) {
    printf("  Inserting 100 keys...\n");
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    insert_keys(&env, 0, 100);
    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    printf("  Closing and reopening...\n");
    close_env(&env);
    open_env(&env);

    printf("  Recovering from WAL...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered > 0, "data_art_recover should succeed");
    printf("  Recovered %ld entries\n", recovered);

    printf("  Verifying all 100 keys...\n");
    verify_keys(&env, 0, 100);
    verify_key_absent(&env, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size");

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 2: Truncated WAL Entry
// ============================================================================

static void test_truncated_wal_entry(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    printf("  Inserting 60 keys...\n");
    insert_keys(&env, 0, 60);
    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    close_env(&env);

    // Truncate the last entry in half
    char seg_path[512];
    find_wal_segment(env.wal_path, seg_path, sizeof(seg_path));
    off_t file_size = get_file_size(seg_path);
    ASSERT(file_size > 0, "WAL segment exists");

    off_t truncated_size = file_size - 50;
    printf("  Truncating WAL from %ld to %ld bytes (cutting last entry)...\n",
           (long)file_size, (long)truncated_size);
    int ret = truncate(seg_path, truncated_size);
    ASSERT(ret == 0, "truncate WAL segment");

    // Reopen and recover
    open_env(&env);
    printf("  Recovering from truncated WAL...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered >= 0, "recovery should not report error for truncation");
    printf("  Recovered %ld entries\n", recovered);

    // The last entry (index 59) was truncated, so 59 keys should be present
    printf("  Verifying 59 keys present, key 59 absent...\n");
    verify_keys(&env, 0, 59);
    verify_key_absent(&env, 59);

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 3: Corrupted WAL Entry
// ============================================================================

static void test_corrupted_wal_entry(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    printf("  Inserting 20 keys...\n");
    insert_keys(&env, 0, 20);
    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    close_env(&env);

    // Corrupt the last entry's payload
    char seg_path[512];
    find_wal_segment(env.wal_path, seg_path, sizeof(seg_path));
    off_t file_size = get_file_size(seg_path);
    ASSERT(file_size > 0, "WAL segment exists");

    // Corrupt 4 bytes near the end (within the last entry's payload)
    off_t corruption_offset = file_size - 20;
    printf("  Corrupting 4 bytes at offset %ld (file size %ld)...\n",
           (long)corruption_offset, (long)file_size);
    int fd = open(seg_path, O_WRONLY);
    ASSERT(fd >= 0, "open WAL segment for corruption");
    uint8_t garbage[4] = {0xDE, 0xAD, 0xBE, 0xEF};
    ssize_t written = pwrite(fd, garbage, sizeof(garbage), corruption_offset);
    ASSERT(written == sizeof(garbage), "pwrite corruption bytes");
    close(fd);

    // Reopen and recover — should return -1 due to CRC mismatch
    open_env(&env);
    printf("  Recovering from corrupted WAL...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    printf("  Recovery returned %ld (expected -1 due to corruption)\n", recovered);
    ASSERT_EQ(recovered, -1L, "recovery should report error for corruption");

    // But the 19 entries before the corrupted one were still applied
    printf("  Verifying 19 keys present, key 19 absent...\n");
    verify_keys(&env, 0, 19);
    verify_key_absent(&env, 19);

    // Validate segment should also report errors
    uint32_t errors = 0;
    bool valid = wal_validate_segment(env.wal, 0, &errors);
    ASSERT(!valid, "segment should be invalid");
    ASSERT(errors > 0, "errors should be reported");
    printf("  wal_validate_segment: valid=%d, errors=%u\n", valid, errors);

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 4: Recovery with Checkpoint in WAL
// ============================================================================

static void test_checkpoint_recovery(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);
    ASSERT(data_art_flush(env.tree), "flush before checkpoint");
    ASSERT(wal_fsync(env.wal), "fsync before checkpoint");

    printf("  Creating checkpoint...\n");
    uint64_t checkpoint_lsn = 0;
    bool ok = data_art_checkpoint(env.tree, &checkpoint_lsn);
    ASSERT(ok, "data_art_checkpoint");
    ASSERT(checkpoint_lsn > 0, "checkpoint LSN assigned");
    printf("  Checkpoint at LSN %lu\n", checkpoint_lsn);

    printf("  Inserting 30 more keys...\n");
    insert_keys(&env, 50, 30);
    ASSERT(data_art_flush(env.tree), "flush after more inserts");
    ASSERT(wal_fsync(env.wal), "fsync after more inserts");

    close_env(&env);

    // Reopen and recover from scratch (LSN 0) — replays all 50 inserts,
    // the checkpoint entry (sets root/size), then 30 more inserts.
    // This validates that WAL replay handles checkpoint entries correctly
    // when rebuilding the full tree from scratch.
    open_env(&env);
    printf("  Recovering from WAL (full replay with checkpoint in middle)...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered > 0, "data_art_recover should succeed");
    printf("  Recovered %ld entries (50 inserts + 1 checkpoint + 30 inserts)\n", recovered);
    ASSERT_EQ(recovered, 81L, "should replay 50 + 1 checkpoint + 30 entries");

    // All 80 keys should be present
    printf("  Verifying all 80 keys...\n");
    verify_keys(&env, 0, 80);
    ASSERT_EQ((long)data_art_size(env.tree), 80L, "tree size after recovery");

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 5: Delete Replay Recovery
// ============================================================================

static void test_delete_replay_recovery(void) {
    test_env_t env = {0};
    make_paths(&env, "t5");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert keys 0-49
    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    // Delete keys 10-29 (20 deletions)
    printf("  Deleting keys 10-29...\n");
    for (int i = 10; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_delete(env.tree, key, KEY_SIZE);
        ASSERT(ok, "delete existing key");
    }

    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    // Simulate crash
    printf("  Closing and reopening...\n");
    close_env(&env);
    open_env(&env);

    // Recover
    printf("  Recovering from WAL...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered > 0, "recovery should succeed");
    printf("  Recovered %ld entries (50 inserts + 20 deletes)\n", recovered);

    // Keys 0-9 should exist
    printf("  Verifying keys 0-9 present...\n");
    verify_keys(&env, 0, 10);

    // Keys 10-29 should NOT exist
    printf("  Verifying keys 10-29 absent...\n");
    for (int i = 10; i < 30; i++) {
        verify_key_absent(&env, i);
    }

    // Keys 30-49 should exist
    printf("  Verifying keys 30-49 present...\n");
    verify_keys(&env, 30, 20);

    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after delete recovery");

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 6: Uncommitted Transaction Skipped During Recovery
// ============================================================================

static void test_uncommitted_txn_skipped(void) {
    test_env_t env = {0};
    make_paths(&env, "t6");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert 20 keys normally (auto-commit, no BEGIN/COMMIT in WAL)
    printf("  Inserting 20 auto-committed keys...\n");
    insert_keys(&env, 0, 20);

    // Simulate incomplete transaction: write WAL entries directly
    // BEGIN_TXN for fake txn, then 10 INSERTs, but NO COMMIT
    uint64_t fake_txn_id = 9999;
    printf("  Writing uncommitted txn (id=%lu) with 10 inserts to WAL...\n", fake_txn_id);
    ASSERT(wal_log_begin_txn(env.wal, fake_txn_id, NULL), "wal_log_begin_txn");

    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 1000 + i);
        size_t vlen = generate_value(value, sizeof(value), 1000 + i);
        ASSERT(wal_log_insert(env.wal, fake_txn_id, key, KEY_SIZE,
                              (const uint8_t *)value, (uint32_t)vlen, NULL),
               "wal_log_insert for uncommitted txn");
    }
    // Intentionally NO wal_log_commit_txn — simulating crash mid-commit

    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    printf("  Closing and reopening...\n");
    close_env(&env);
    open_env(&env);

    printf("  Recovering from WAL (should skip uncommitted txn)...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered > 0, "recovery should succeed");
    printf("  Recovered %ld entries\n", recovered);

    // 20 auto-committed keys should exist
    printf("  Verifying 20 auto-committed keys present...\n");
    verify_keys(&env, 0, 20);

    // 10 uncommitted keys should NOT exist
    printf("  Verifying 10 uncommitted keys absent...\n");
    for (int i = 0; i < 10; i++) {
        verify_key_absent(&env, 1000 + i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size (uncommitted excluded)");

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 7: Mixed Committed + Uncommitted Transactions
// ============================================================================

static void test_mixed_committed_uncommitted(void) {
    test_env_t env = {0};
    make_paths(&env, "t7");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert 10 keys via auto-commit
    printf("  Inserting 10 auto-committed keys...\n");
    insert_keys(&env, 0, 10);

    // Begin explicit txn1, insert 5 keys, commit
    printf("  Committing explicit txn with 5 keys...\n");
    uint64_t txn_id1;
    ASSERT(data_art_begin_txn(env.tree, &txn_id1), "begin_txn");
    for (int i = 0; i < 5; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 100 + i);
        size_t vlen = generate_value(value, sizeof(value), 100 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");

    // Write uncommitted txn2 directly to WAL (simulating crash mid-commit)
    uint64_t fake_txn_id = 8888;
    printf("  Writing uncommitted txn (id=%lu) with 5 inserts to WAL...\n", fake_txn_id);
    ASSERT(wal_log_begin_txn(env.wal, fake_txn_id, NULL), "wal_log_begin_txn");
    for (int i = 0; i < 5; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 2000 + i);
        size_t vlen = generate_value(value, sizeof(value), 2000 + i);
        ASSERT(wal_log_insert(env.wal, fake_txn_id, key, KEY_SIZE,
                              (const uint8_t *)value, (uint32_t)vlen, NULL),
               "wal_log_insert for uncommitted txn");
    }
    // NO commit for fake_txn_id

    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    printf("  Closing and reopening...\n");
    close_env(&env);
    open_env(&env);

    printf("  Recovering from WAL...\n");
    int64_t recovered = data_art_recover(env.tree, 0);
    ASSERT(recovered > 0, "recovery should succeed");
    printf("  Recovered %ld entries\n", recovered);

    // 10 auto-committed keys should exist
    printf("  Verifying 10 auto-committed keys present...\n");
    verify_keys(&env, 0, 10);

    // 5 committed txn keys should exist
    printf("  Verifying 5 committed txn keys present...\n");
    verify_keys(&env, 100, 5);

    // 5 uncommitted keys should NOT exist
    printf("  Verifying 5 uncommitted keys absent...\n");
    for (int i = 0; i < 5; i++) {
        verify_key_absent(&env, 2000 + i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 15L, "tree size (15 = 10 auto + 5 committed)");

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    log_set_level(LOG_LEVEL_ERROR);

    printf("\n");
    printf("========================================\n");
    printf(" Crash Recovery Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_end_to_end_recovery);
    RUN_TEST(test_truncated_wal_entry);
    RUN_TEST(test_corrupted_wal_entry);
    RUN_TEST(test_checkpoint_recovery);
    RUN_TEST(test_delete_replay_recovery);
    RUN_TEST(test_uncommitted_txn_skipped);
    RUN_TEST(test_mixed_committed_uncommitted);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
