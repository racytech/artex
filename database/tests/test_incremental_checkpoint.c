/**
 * Incremental Checkpoint Test Suite
 *
 * Verifies that the dirty page bitmap correctly tracks modified pages
 * and that checkpoint only syncs dirty pages (not the entire file).
 *
 * Tests:
 *   1. Fresh tree has zero dirty pages
 *   2. Inserts mark pages dirty, checkpoint clears bitmap
 *   3. Dirty count << total pages (incremental, not full-file)
 *   4. Multiple checkpoint cycles: each clears, new writes re-dirty
 *   5. Data survives incremental checkpoint (close → reopen → verify)
 *   6. Delete operations mark pages dirty
 *   7. Explicit transaction commit marks pages dirty
 */

#include "data_art.h"
#include "mmap_storage.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>

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
// Test Environment
// ============================================================================

#define KEY_SIZE 32
#define BASE_DIR "/tmp/test_incr_ckpt"

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

static void insert_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char value[64];
        generate_key(key, start + i);
        int n = snprintf(value, sizeof(value), "val_%06d_payload_data", start + i);
        bool ok = data_art_insert(env->tree, key, KEY_SIZE, value, (size_t)n);
        ASSERT(ok, "data_art_insert");
    }
}

static void verify_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char expected[64];
        generate_key(key, start + i);
        int n = snprintf(expected, sizeof(expected), "val_%06d_payload_data", start + i);
        size_t got_len = 0;
        const void *got = data_art_get(env->tree, key, KEY_SIZE, &got_len);
        if (!got) {
            char msg[128];
            snprintf(msg, sizeof(msg), "Key %d not found", start + i);
            ASSERT(false, msg);
        }
        ASSERT_EQ((long)got_len, (long)n, "value length mismatch");
        ASSERT(memcmp(got, expected, got_len) == 0, "value content mismatch");
        free((void *)got);
    }
}

static size_t dirty_count(test_env_t *env) {
    return mmap_storage_dirty_count(env->tree->mmap_storage);
}

static size_t total_pages(test_env_t *env) {
    return env->tree->mmap_storage->mapped_size / PAGE_SIZE;
}

// ============================================================================
// Test 1: Fresh tree has zero dirty pages
// ============================================================================

static void test_fresh_tree_clean(void) {
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // data_art_create calls mmap_storage_checkpoint which clears the bitmap.
    // The initial write_slot calls write to page 0 (header) which is skipped.
    // So dirty count should be 0 after creation.
    size_t dc = dirty_count(&env);
    printf("  Dirty pages after create: %zu (total pages: %zu)\n", dc, total_pages(&env));
    ASSERT_EQ(dc, 0, "fresh tree should have 0 dirty pages");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 2: Inserts mark pages dirty, checkpoint clears bitmap
// ============================================================================

static void test_inserts_mark_dirty(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    ASSERT_EQ(dirty_count(&env), 0, "start clean");

    // Insert 100 keys — should dirty some pages
    printf("  Inserting 100 keys...\n");
    insert_keys(&env, 0, 100);

    size_t dc_after_insert = dirty_count(&env);
    printf("  Dirty pages after 100 inserts: %zu\n", dc_after_insert);
    ASSERT(dc_after_insert > 0, "inserts should mark pages dirty");

    // Checkpoint — should clear all dirty bits
    printf("  Checkpointing...\n");
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint");

    size_t dc_after_ckpt = dirty_count(&env);
    printf("  Dirty pages after checkpoint: %zu\n", dc_after_ckpt);
    ASSERT_EQ(dc_after_ckpt, 0, "checkpoint should clear all dirty bits");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 3: Dirty count << total pages (incremental behavior)
// ============================================================================

static void test_dirty_count_is_incremental(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 1000 keys to get a reasonable tree size, then checkpoint
    printf("  Inserting 1000 keys...\n");
    insert_keys(&env, 0, 1000);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 1");
    ASSERT_EQ(dirty_count(&env), 0, "clean after checkpoint");

    size_t tp = total_pages(&env);
    printf("  Total pages: %zu\n", tp);

    // Insert 10 more keys — should dirty only a small fraction of pages
    printf("  Inserting 10 more keys...\n");
    insert_keys(&env, 1000, 10);

    size_t dc = dirty_count(&env);
    printf("  Dirty pages after 10 inserts: %zu / %zu total (%.1f%%)\n",
           dc, tp, 100.0 * dc / tp);
    ASSERT(dc > 0, "should have dirty pages");
    ASSERT(dc < tp / 2, "dirty pages should be << total pages");

    // Verify: the incremental insert only touched a small portion
    // With 1000 keys already in, adding 10 more touches ~20-40 pages
    // (new leaf slots + some inner node CoW). Definitely not all pages.
    printf("  Ratio: %.1f%% of pages dirty (expected <5%%)\n",
           100.0 * dc / tp);

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 4: Multiple checkpoint cycles
// ============================================================================

static void test_multiple_checkpoint_cycles(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    for (int cycle = 0; cycle < 5; cycle++) {
        // Insert a batch
        printf("  Cycle %d: inserting keys %d-%d...\n",
               cycle, cycle * 100, cycle * 100 + 99);
        insert_keys(&env, cycle * 100, 100);

        size_t dc_before = dirty_count(&env);
        printf("  Cycle %d: dirty pages before checkpoint: %zu\n", cycle, dc_before);
        ASSERT(dc_before > 0, "should have dirty pages before checkpoint");

        // Checkpoint
        ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint");

        size_t dc_after = dirty_count(&env);
        printf("  Cycle %d: dirty pages after checkpoint: %zu\n", cycle, dc_after);
        ASSERT_EQ(dc_after, 0, "checkpoint should clear dirty bits");
    }

    // Verify all data
    printf("  Verifying all 500 keys...\n");
    verify_keys(&env, 0, 500);

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 5: Data survives incremental checkpoint (close → reopen → verify)
// ============================================================================

static void test_data_survives_incremental_checkpoint(void) {
    test_env_t env = {0};
    make_paths(&env, "t5");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert in batches with checkpoints
    printf("  Batch 1: 200 keys + checkpoint...\n");
    insert_keys(&env, 0, 200);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 1");

    printf("  Batch 2: 200 more keys + checkpoint...\n");
    insert_keys(&env, 200, 200);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint 2");

    printf("  Batch 3: 100 more keys (no checkpoint before close)...\n");
    insert_keys(&env, 400, 100);

    // Close triggers checkpoint in data_art_destroy
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    printf("  Verifying all 500 keys after reopen...\n");
    verify_keys(&env, 0, 500);
    ASSERT_EQ((long)data_art_size(env.tree), 500L, "tree size after reopen");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 6: Delete operations mark pages dirty
// ============================================================================

static void test_deletes_mark_dirty(void) {
    test_env_t env = {0};
    make_paths(&env, "t6");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert 100 keys and checkpoint to get a clean baseline
    insert_keys(&env, 0, 100);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint");
    ASSERT_EQ(dirty_count(&env), 0, "clean after checkpoint");

    // Delete 20 keys — should dirty pages (CoW on delete path)
    printf("  Deleting 20 keys...\n");
    for (int i = 0; i < 20; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_delete(env.tree, key, KEY_SIZE);
        ASSERT(ok, "delete");
    }

    size_t dc = dirty_count(&env);
    printf("  Dirty pages after 20 deletes: %zu\n", dc);
    ASSERT(dc > 0, "deletes should mark pages dirty");

    // Checkpoint and verify clean
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint after deletes");
    ASSERT_EQ(dirty_count(&env), 0, "clean after checkpoint");

    // Verify surviving keys
    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    verify_keys(&env, 20, 80);
    ASSERT_EQ((long)data_art_size(env.tree), 80L, "tree size");

    close_tree(&env);
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 7: Explicit transaction commit marks pages dirty
// ============================================================================

static void test_txn_commit_marks_dirty(void) {
    test_env_t env = {0};
    make_paths(&env, "t7");
    cleanup_dir(env.dir_path);
    create_tree(&env);

    // Insert some keys and checkpoint to get clean baseline
    insert_keys(&env, 0, 50);
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint");
    ASSERT_EQ(dirty_count(&env), 0, "clean after checkpoint");

    // Begin explicit transaction, insert keys, commit
    printf("  Committing txn with 100 keys...\n");
    uint64_t txn_id;
    ASSERT(data_art_begin_txn(env.tree, &txn_id), "begin_txn");
    for (int i = 0; i < 100; i++) {
        uint8_t key[KEY_SIZE];
        char value[64];
        generate_key(key, 1000 + i);
        int n = snprintf(value, sizeof(value), "val_%06d_payload_data", 1000 + i);
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, (size_t)n), "txn insert");
    }
    ASSERT(data_art_commit_txn(env.tree), "commit_txn");

    size_t dc = dirty_count(&env);
    printf("  Dirty pages after txn commit (100 keys): %zu\n", dc);
    ASSERT(dc > 0, "txn commit should mark pages dirty");

    // Checkpoint and verify
    ASSERT(data_art_checkpoint(env.tree, NULL), "checkpoint after txn");
    ASSERT_EQ(dirty_count(&env), 0, "clean after checkpoint");

    printf("  Closing and reopening...\n");
    close_tree(&env);
    open_tree(&env);

    verify_keys(&env, 0, 50);
    verify_keys(&env, 1000, 100);
    ASSERT_EQ((long)data_art_size(env.tree), 150L, "tree size");

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
    printf(" Incremental Checkpoint Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_fresh_tree_clean);
    RUN_TEST(test_inserts_mark_dirty);
    RUN_TEST(test_dirty_count_is_incremental);
    RUN_TEST(test_multiple_checkpoint_cycles);
    RUN_TEST(test_data_survives_incremental_checkpoint);
    RUN_TEST(test_deletes_mark_dirty);
    RUN_TEST(test_txn_commit_marks_dirty);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
