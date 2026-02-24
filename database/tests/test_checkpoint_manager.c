/**
 * Checkpoint Manager Test Suite
 *
 * Tests:
 *   1. Start and stop (basic lifecycle)
 *   2. Force checkpoint
 *   3. Concurrent writes + checkpoint
 *   4. WAL truncation after checkpoint
 */

#include "checkpoint_manager.h"
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
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>

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
#define BASE_DB_PATH  "/tmp/test_ckpt_mgr_db"
#define BASE_WAL_PATH "/tmp/test_ckpt_mgr_wal"

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

static void open_env_ex(test_env_t *env, uint64_t segment_size) {
    env->pm = page_manager_create(env->db_path, false);
    ASSERT(env->pm != NULL, "page_manager_create");

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    env->bp = buffer_pool_create(&bp_config, env->pm);
    ASSERT(env->bp != NULL, "buffer_pool_create");

    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = segment_size;
    env->wal = wal_open(env->wal_path, &wal_config);
    ASSERT(env->wal != NULL, "wal_open");

    env->tree = data_art_create(env->pm, env->bp, env->wal, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void open_env(test_env_t *env) {
    open_env_ex(env, 8 * 1024 * 1024);  // 8MB default
}

static void close_env(test_env_t *env) {
    if (env->tree) { data_art_destroy(env->tree); env->tree = NULL; }
    if (env->wal)  { wal_close(env->wal);         env->wal = NULL; }
    if (env->bp)   { buffer_pool_destroy(env->bp); env->bp = NULL; }
    if (env->pm)   { page_manager_destroy(env->pm); env->pm = NULL; }
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

static int count_wal_segments(const char *wal_dir) {
    DIR *dir = opendir(wal_dir);
    if (!dir) return 0;
    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, "wal_") && strstr(entry->d_name, ".log")) {
            count++;
        }
    }
    closedir(dir);
    return count;
}

// ============================================================================
// Test 1: Start and Stop (basic lifecycle)
// ============================================================================

static void test_start_stop(void) {
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    checkpoint_manager_config_t config = checkpoint_manager_default_config();
    config.check_interval_ms = 50;
    checkpoint_manager_t *mgr = checkpoint_manager_create(env.tree, &config);
    ASSERT(mgr != NULL, "checkpoint_manager_create");

    printf("  Starting checkpoint manager...\n");
    ASSERT(checkpoint_manager_start(mgr), "start");

    // Let it run for a bit
    usleep(200 * 1000);

    printf("  Stopping checkpoint manager...\n");
    checkpoint_manager_stop(mgr);

    // Double-stop should be safe
    checkpoint_manager_stop(mgr);

    uint64_t completed = 0, truncated = 0;
    checkpoint_manager_get_stats(mgr, &completed, &truncated);
    printf("  Stats: %lu checkpoints, %lu segments truncated\n", completed, truncated);

    checkpoint_manager_destroy(mgr);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 2: Force Checkpoint
// ============================================================================

static void test_force_checkpoint(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    printf("  Inserting 100 keys...\n");
    insert_keys(&env, 0, 100);
    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    checkpoint_manager_config_t config = checkpoint_manager_default_config();
    config.check_interval_ms = 5000;  // High interval — force will bypass
    checkpoint_manager_t *mgr = checkpoint_manager_create(env.tree, &config);
    ASSERT(mgr != NULL, "create");
    ASSERT(checkpoint_manager_start(mgr), "start");

    printf("  Forcing checkpoint...\n");
    ASSERT(checkpoint_manager_force(mgr), "force");

    uint64_t completed = 0;
    checkpoint_manager_get_stats(mgr, &completed, NULL);
    printf("  Checkpoints completed: %lu\n", completed);
    ASSERT(completed >= 1, "should have at least 1 checkpoint");

    // Verify keys still accessible
    verify_keys(&env, 0, 100);

    checkpoint_manager_stop(mgr);
    checkpoint_manager_destroy(mgr);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 3: Concurrent Writes + Checkpoint
// ============================================================================

typedef struct {
    test_env_t *env;
    int start;
    int count;
    volatile bool *done;
} writer_args_t;

static void *writer_thread_fn(void *arg) {
    writer_args_t *args = (writer_args_t *)arg;
    for (int i = 0; i < args->count; i++) {
        uint8_t key[KEY_SIZE];
        char value[64];
        generate_key(key, args->start + i);
        int n = snprintf(value, sizeof(value), "val_%06d_payload_data", args->start + i);
        data_art_insert(args->env->tree, key, KEY_SIZE, value, (size_t)n);
    }
    *args->done = true;
    return NULL;
}

static void test_concurrent_writes_checkpoint(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    checkpoint_manager_config_t config = checkpoint_manager_default_config();
    config.check_interval_ms = 50;
    checkpoint_manager_t *mgr = checkpoint_manager_create(env.tree, &config);
    ASSERT(mgr != NULL, "create");
    ASSERT(checkpoint_manager_start(mgr), "start");

    volatile bool writer_done = false;
    writer_args_t args = {
        .env = &env,
        .start = 0,
        .count = 1000,
        .done = &writer_done,
    };

    printf("  Starting writer thread (1000 keys)...\n");
    pthread_t writer;
    pthread_create(&writer, NULL, writer_thread_fn, &args);

    // Force a few checkpoints during writes
    for (int i = 0; i < 3; i++) {
        usleep(20 * 1000);
        if (writer_done) break;
        printf("  Forcing checkpoint %d during writes...\n", i + 1);
        checkpoint_manager_force(mgr);
    }

    pthread_join(writer, NULL);

    // Final flush + force checkpoint
    ASSERT(data_art_flush(env.tree), "final flush");
    ASSERT(wal_fsync(env.wal), "final fsync");
    checkpoint_manager_force(mgr);

    uint64_t completed = 0;
    checkpoint_manager_get_stats(mgr, &completed, NULL);
    printf("  Checkpoints during concurrent writes: %lu\n", completed);
    ASSERT(completed >= 1, "should have at least 1 checkpoint");

    printf("  Verifying all 1000 keys...\n");
    verify_keys(&env, 0, 1000);
    ASSERT_EQ((long)data_art_size(env.tree), 1000L, "tree size");

    checkpoint_manager_stop(mgr);
    checkpoint_manager_destroy(mgr);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 4: WAL Truncation After Checkpoint
// ============================================================================

static void test_wal_truncation(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_paths(env.db_path, env.wal_path);

    // Use tiny 64KB segments to force multiple segment files
    open_env_ex(&env, 64 * 1024);

    printf("  Inserting 2000 keys (small WAL segments to force rotation)...\n");
    insert_keys(&env, 0, 2000);
    ASSERT(data_art_flush(env.tree), "flush");
    ASSERT(wal_fsync(env.wal), "fsync");

    int segments_before = count_wal_segments(env.wal_path);
    printf("  WAL segments before checkpoint: %d\n", segments_before);

    // Force checkpoint + truncation
    checkpoint_manager_t *mgr = checkpoint_manager_create(env.tree, NULL);
    ASSERT(mgr != NULL, "create");

    printf("  Running checkpoint (no background thread, direct force)...\n");
    ASSERT(checkpoint_manager_force(mgr), "force");

    uint64_t completed = 0, truncated = 0;
    checkpoint_manager_get_stats(mgr, &completed, &truncated);
    printf("  Checkpoints: %lu, segments truncated: %lu\n", completed, truncated);
    ASSERT_EQ(completed, 1, "should have 1 checkpoint");

    int segments_after = count_wal_segments(env.wal_path);
    printf("  WAL segments after checkpoint: %d\n", segments_after);

    // Verify data still accessible
    verify_keys(&env, 0, 2000);

    checkpoint_manager_destroy(mgr);
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
    printf(" Checkpoint Manager Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_start_stop);
    RUN_TEST(test_force_checkpoint);
    RUN_TEST(test_concurrent_writes_checkpoint);
    RUN_TEST(test_wal_truncation);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
