/**
 * Checkpoint Manager Test Suite
 *
 * Tests:
 *   1. Start and stop (basic lifecycle)
 *   2. Force checkpoint
 *   3. Concurrent writes + checkpoint
 *   4. Multiple checkpoints with data verification
 */

#include "checkpoint_manager.h"
#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>

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
#define BASE_DIR "/tmp/test_ckpt_mgr"

typedef struct {
    data_art_tree_t *tree;
    char dir_path[256];
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
}

static void open_env(test_env_t *env) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "mkdir -p %s", env->dir_path);
    system(cmd);

    char art_path[512];
    snprintf(art_path, sizeof(art_path), "%s/art.dat", env->dir_path);

    env->tree = data_art_create(art_path, KEY_SIZE);
    ASSERT(env->tree != NULL, "data_art_create");
}

static void close_env(test_env_t *env) {
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

// ============================================================================
// Test 1: Start and Stop (basic lifecycle)
// ============================================================================

static void test_start_stop(void) {
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_dir(env.dir_path);
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
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 2: Force Checkpoint
// ============================================================================

static void test_force_checkpoint(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_dir(env.dir_path);
    open_env(&env);

    printf("  Inserting 100 keys...\n");
    insert_keys(&env, 0, 100);

    checkpoint_manager_config_t config = checkpoint_manager_default_config();
    config.check_interval_ms = 5000;  // High interval -- force will bypass
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
    cleanup_dir(env.dir_path);
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
    cleanup_dir(env.dir_path);
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

    // Final force checkpoint
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
    cleanup_dir(env.dir_path);
}

// ============================================================================
// Test 4: Multiple Checkpoints with Data Verification
// ============================================================================

static void test_multiple_checkpoints(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_dir(env.dir_path);
    open_env(&env);

    printf("  Inserting 2000 keys with periodic checkpoints...\n");

    checkpoint_manager_t *mgr = checkpoint_manager_create(env.tree, NULL);
    ASSERT(mgr != NULL, "create");

    // Insert in batches with checkpoints between
    for (int batch = 0; batch < 4; batch++) {
        insert_keys(&env, batch * 500, 500);
        printf("  Forcing checkpoint after batch %d (keys %d-%d)...\n",
               batch, batch * 500, batch * 500 + 499);
        ASSERT(checkpoint_manager_force(mgr), "force");
    }

    uint64_t completed = 0, truncated = 0;
    checkpoint_manager_get_stats(mgr, &completed, &truncated);
    printf("  Checkpoints: %lu, segments truncated: %lu\n", completed, truncated);
    ASSERT_EQ(completed, 4, "should have 4 checkpoints");

    // Verify data still accessible
    verify_keys(&env, 0, 2000);

    checkpoint_manager_destroy(mgr);
    close_env(&env);
    cleanup_dir(env.dir_path);
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
    RUN_TEST(test_multiple_checkpoints);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
