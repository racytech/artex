/**
 * Persistence Test Suite for mmap-backed ART
 *
 * Proves durability by testing:
 *   1. End-to-end persistence (insert -> destroy -> open -> verify)
 *   2. Large dataset persistence (1000 keys)
 *   3. Checkpoint persistence (checkpoint -> destroy -> open -> verify)
 *   4. Delete persistence (insert -> delete -> destroy -> open -> verify deletions)
 *   5. Mixed insert+delete persistence
 *   6. Transaction commit persistence
 *   7. Transaction abort not persisted
 *   8. Multiple reopen cycles
 *   9. Persistence with overflow values
 *  10. Page growth after deletes and reinserts
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/stat.h>

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

#define BASE_DIR "/tmp/test_persistence"

typedef struct {
    data_art_tree_t *tree;
    char dir_path[256];
    char art_path[512];
} test_env_t;

static void cleanup_dir(const char *dir_path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir_path);
    system(cmd);
    sync();
    usleep(10000);
}

static void make_paths(test_env_t *env, const char *suffix) {
    snprintf(env->dir_path, sizeof(env->dir_path), "%s_%s", BASE_DIR, suffix);
    snprintf(env->art_path, sizeof(env->art_path), "%s/art.dat", env->dir_path);
}

static void create_tree(test_env_t *env) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", env->dir_path);
    system(cmd);

    env->tree = data_art_create(env->art_path, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void open_tree(test_env_t *env) {
    env->tree = data_art_open(env->art_path, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_open");
}

static void close_tree(test_env_t *env) {
    if (env->tree) { data_art_destroy(env->tree); env->tree = NULL; }
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
            snprintf(msg, sizeof(msg), "Key %d not found after reopen", start + i);
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

// ============================================================================
// Test 1: End-to-End Persistence
// ============================================================================

static void test_end_to_end_persistence(void) {
    printf("  Inserting 100 keys...\n");
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    insert_keys(&env, 0, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size before close");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 100 keys...\n");
    verify_keys(&env, 0, 100);
    verify_key_absent(&env, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 2: Large Dataset Persistence (1000 keys)
// ============================================================================

static void test_large_dataset_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    printf("  Inserting 1000 keys...\n");
    insert_keys(&env, 0, 1000);
    ASSERT_EQ((long)data_art_size(env.tree), 1000L, "tree size");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 1000 keys...\n");
    verify_keys(&env, 0, 1000);
    ASSERT_EQ((long)data_art_size(env.tree), 1000L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 3: Checkpoint Persistence
// ============================================================================

static void test_checkpoint_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    printf("  Creating checkpoint...\n");
    uint64_t checkpoint_lsn = 0;
    bool ok = data_art_checkpoint(env.tree, &checkpoint_lsn);
    ASSERT(ok, "data_art_checkpoint");
    printf("  Checkpoint at LSN %lu\n", checkpoint_lsn);

    printf("  Inserting 30 more keys...\n");
    insert_keys(&env, 50, 30);

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // All 80 keys should be present (mmap persists everything)
    printf("  Verifying all 80 keys...\n");
    verify_keys(&env, 0, 80);
    ASSERT_EQ((long)data_art_size(env.tree), 80L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 4: Delete Persistence
// ============================================================================

static void test_delete_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_dir(env.dir_path);
    create_tree(&env);

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

    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after deletes");

    // Close and reopen
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

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

    ASSERT_EQ((long)data_art_size(env.tree), 30L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 5: Mixed Insert + Delete Persistence
// ============================================================================

static void test_mixed_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t5");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 10 keys via auto-commit
    printf("  Inserting 10 keys...\n");
    insert_keys(&env, 0, 10);

    // Begin explicit txn, insert 5 more keys, commit
    printf("  Committing explicit txn with 5 keys...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 5; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 100 + i);
        size_t vlen = generate_value(value, sizeof(value), 100 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");

    ASSERT_EQ((long)data_art_size(env.tree), 15L, "tree size (10 auto + 5 committed)");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // 10 auto-committed keys should exist
    printf("  Verifying 10 auto-committed keys present...\n");
    verify_keys(&env, 0, 10);

    // 5 committed txn keys should exist
    printf("  Verifying 5 committed txn keys present...\n");
    verify_keys(&env, 100, 5);

    ASSERT_EQ((long)data_art_size(env.tree), 15L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 6: Transaction Commit Persistence
// ============================================================================

static void test_txn_commit_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t6");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert via explicit transaction
    printf("  Inserting 50 keys via explicit txn...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, i);
        size_t vlen = generate_value(value, sizeof(value), i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after commit");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 50 keys...\n");
    verify_keys(&env, 0, 50);
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 7: Transaction Abort Not Persisted
// ============================================================================

static void test_txn_abort_not_persisted(void) {
    test_env_t env = {0};
    make_paths(&env, "t7");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 20 keys normally
    printf("  Inserting 20 auto-committed keys...\n");
    insert_keys(&env, 0, 20);

    // Begin txn with 10 more keys, then ABORT
    printf("  Inserting 10 keys in txn, then aborting...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        char value[VALUE_MAX_LEN];
        generate_key(key, 1000 + i);
        size_t vlen = generate_value(value, sizeof(value), 1000 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "txn insert");
    }
    ASSERT(data_art_abort_txn(env.tree), "abort_txn");

    // Only 20 keys should exist (aborted txn not applied)
    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size after abort");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // 20 auto-committed keys should exist
    printf("  Verifying 20 auto-committed keys present...\n");
    verify_keys(&env, 0, 20);

    // 10 aborted keys should NOT exist
    printf("  Verifying 10 aborted keys absent...\n");
    for (int i = 0; i < 10; i++) {
        verify_key_absent(&env, 1000 + i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 20L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 8: Multiple Reopen Cycles
// ============================================================================

static void test_multiple_reopen_cycles(void) {
    test_env_t env = {0};
    make_paths(&env, "t8");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 50 keys
    printf("  Inserting 50 keys...\n");
    insert_keys(&env, 0, 50);

    // Checkpoint
    printf("  Checkpointing...\n");
    uint64_t ckpt_lsn;
    ASSERT(data_art_checkpoint(env.tree, &ckpt_lsn), "checkpoint");

    // Close and reopen
    printf("  Cycle 1: close and reopen...\n");
    close_tree(&env);
    open_tree(&env);

    // Insert 50 more keys
    printf("  Inserting 50 more keys...\n");
    insert_keys(&env, 50, 50);

    // Close and reopen again
    printf("  Cycle 2: close and reopen...\n");
    close_tree(&env);
    open_tree(&env);

    // Verify all 100 keys present
    printf("  Verifying all 100 keys...\n");
    verify_keys(&env, 0, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 100L, "tree size");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 9: Persistence with Overflow Values
// ============================================================================

static void test_overflow_value_persistence(void) {
    test_env_t env = {0};
    make_paths(&env, "t9");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 10 keys with 8KB values (triggers overflow pages)
    const size_t big_val_len = 8 * 1024;
    char *big_val = malloc(big_val_len);
    ASSERT(big_val != NULL, "malloc big value");

    printf("  Inserting 10 keys with 8KB values...\n");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);
        bool ok = data_art_insert(env.tree, key, KEY_SIZE, big_val, big_val_len);
        ASSERT(ok, "insert overflow key");
    }

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying 10 overflow keys...\n");
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);

        size_t found_len = 0;
        const void *found = data_art_get(env.tree, key, KEY_SIZE, &found_len);
        ASSERT(found != NULL, "overflow key should exist");
        ASSERT_EQ((long)found_len, (long)big_val_len, "overflow value length");
        ASSERT(memcmp(found, big_val, big_val_len) == 0, "overflow value content");
        free((void *)found);
    }
    ASSERT_EQ((long)data_art_size(env.tree), 10L, "tree size");

    free(big_val);
    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 10: Page Growth After Deletes and Reinserts
// ============================================================================

static void test_page_growth_after_deletes(void) {
    test_env_t env = {0};
    make_paths(&env, "t10");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 200 keys
    printf("  Inserting 200 keys...\n");
    insert_keys(&env, 0, 200);

    // Delete 150 keys (0-149)
    printf("  Deleting 150 keys...\n");
    for (int i = 0; i < 150; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_delete(env.tree, key, KEY_SIZE);
        ASSERT(ok, "delete key");
    }

    // Verify tree size is 50
    ASSERT_EQ((long)data_art_size(env.tree), 50L, "tree size after deletes");

    // Insert 150 NEW keys (starting at 1000)
    printf("  Inserting 150 new keys...\n");
    insert_keys(&env, 1000, 150);

    // Close and reopen
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    // Verify all 200 keys present (50 surviving + 150 new)
    printf("  Verifying 50 surviving keys (150-199)...\n");
    verify_keys(&env, 150, 50);

    printf("  Verifying 150 new keys (1000-1149)...\n");
    verify_keys(&env, 1000, 150);

    // Deleted keys should be absent
    printf("  Verifying deleted keys (0-149) absent...\n");
    for (int i = 0; i < 150; i++) {
        verify_key_absent(&env, i);
    }

    ASSERT_EQ((long)data_art_size(env.tree), 200L, "final tree size");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    log_set_level(LOG_LEVEL_ERROR);

    printf("\n");
    printf("========================================\n");
    printf(" Persistence Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_end_to_end_persistence);
    RUN_TEST(test_large_dataset_persistence);
    RUN_TEST(test_checkpoint_persistence);
    RUN_TEST(test_delete_persistence);
    RUN_TEST(test_mixed_persistence);
    RUN_TEST(test_txn_commit_persistence);
    RUN_TEST(test_txn_abort_not_persisted);
    RUN_TEST(test_multiple_reopen_cycles);
    RUN_TEST(test_overflow_value_persistence);
    RUN_TEST(test_page_growth_after_deletes);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
