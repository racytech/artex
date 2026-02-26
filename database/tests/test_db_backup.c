/**
 * Database Backup Test Suite
 *
 * Tests:
 *  1. Export empty tree
 *  2. Round-trip single key
 *  3. Round-trip 1000 keys
 *  4. Overflow values round-trip
 *  5. Backup info metadata
 *  6. Corrupted header detection
 *  7. Corrupted data detection
 *  8. Truncated file detection
 *  9. Invalid path handling
 */

#include "db_backup.h"
#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

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
#define BASE_DIR         "/tmp/test_backup"
#define BACKUP_FILE_PATH "/tmp/test_backup_export.artb"

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

static void cleanup_backup(void) {
    remove(BACKUP_FILE_PATH);
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

static size_t generate_value(char *buf, size_t max_len, int index) {
    int n = snprintf(buf, max_len, "val_%06d_payload_data", index);
    return (size_t)n;
}

static void insert_keys(test_env_t *env, int start, int count) {
    for (int i = 0; i < count; i++) {
        uint8_t key[KEY_SIZE];
        char value[64];
        generate_key(key, start + i);
        size_t vlen = generate_value(value, sizeof(value), start + i);
        bool ok = data_art_insert(env->tree, key, KEY_SIZE, value, vlen);
        ASSERT(ok, "insert_keys: data_art_insert");
    }
}

// ============================================================================
// Tests
// ============================================================================

static void test_export_empty_tree(void) {
    test_count++;
    printf("\n--- Test %d: Export empty tree ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "empty_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export empty tree");

    db_backup_info_t info;
    ok = db_backup_info(BACKUP_FILE_PATH, &info);
    ASSERT(ok, "backup info");
    ASSERT_EQ(info.entry_count, 0, "entry_count should be 0");
    ASSERT_EQ(info.key_size, KEY_SIZE, "key_size should match");
    ASSERT_EQ(info.version, DB_BACKUP_VERSION, "version should match");

    test_env_t dst = {0};
    make_paths(&dst, "empty_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "import empty backup");
    ASSERT_EQ(data_art_size(dst.tree), 0, "imported tree should be empty");

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    close_env(&src);
    cleanup_dir(src.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_round_trip_single(void) {
    test_count++;
    printf("\n--- Test %d: Round-trip single key ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "single_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    uint8_t key[KEY_SIZE];
    generate_key(key, 42);
    const char *value = "hello_world";
    bool ok = data_art_insert(src.tree, key, KEY_SIZE, value, strlen(value));
    ASSERT(ok, "insert single key");

    ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export single key");

    test_env_t dst = {0};
    make_paths(&dst, "single_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "import single key");
    ASSERT_EQ(data_art_size(dst.tree), 1, "imported tree should have 1 key");

    size_t found_len = 0;
    const void *found = data_art_get(dst.tree, key, KEY_SIZE, &found_len);
    ASSERT(found != NULL, "key should exist in imported tree");
    ASSERT_EQ(found_len, strlen(value), "value length should match");
    ASSERT(memcmp(found, value, found_len) == 0, "value content should match");
    free((void *)found);

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    close_env(&src);
    cleanup_dir(src.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_round_trip_1000(void) {
    test_count++;
    printf("\n--- Test %d: Round-trip 1000 keys ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "1k_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    insert_keys(&src, 0, 1000);
    ASSERT_EQ(data_art_size(src.tree), 1000, "source should have 1000 keys");

    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export 1000 keys");

    test_env_t dst = {0};
    make_paths(&dst, "1k_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "import 1000 keys");
    ASSERT_EQ(data_art_size(dst.tree), 1000, "imported tree should have 1000 keys");

    for (int i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        char expected_val[64];
        generate_key(key, i);
        size_t expected_len = generate_value(expected_val, sizeof(expected_val), i);

        size_t found_len = 0;
        const void *found = data_art_get(dst.tree, key, KEY_SIZE, &found_len);
        ASSERT(found != NULL, "key should exist");
        ASSERT_EQ(found_len, expected_len, "value length should match");
        ASSERT(memcmp(found, expected_val, found_len) == 0, "value should match");
        free((void *)found);
    }

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    close_env(&src);
    cleanup_dir(src.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_round_trip_overflow(void) {
    test_count++;
    printf("\n--- Test %d: Overflow values round-trip ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "overflow_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    const size_t big_val_len = 8 * 1024;
    char *big_val = malloc(big_val_len);
    ASSERT(big_val != NULL, "malloc big value");

    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);
        bool ok = data_art_insert(src.tree, key, KEY_SIZE, big_val, big_val_len);
        ASSERT(ok, "insert overflow key");
    }
    ASSERT_EQ(data_art_size(src.tree), 10, "source should have 10 keys");

    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export overflow keys");

    test_env_t dst = {0};
    make_paths(&dst, "overflow_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "import overflow keys");
    ASSERT_EQ(data_art_size(dst.tree), 10, "imported tree should have 10 keys");

    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        memset(big_val, (uint8_t)(i + 'A'), big_val_len);

        size_t found_len = 0;
        const void *found = data_art_get(dst.tree, key, KEY_SIZE, &found_len);
        ASSERT(found != NULL, "overflow key should exist");
        ASSERT_EQ(found_len, big_val_len, "overflow value length should match");
        ASSERT(memcmp(found, big_val, big_val_len) == 0, "overflow value should match");
        free((void *)found);
    }

    free(big_val);
    close_env(&dst);
    cleanup_dir(dst.dir_path);
    close_env(&src);
    cleanup_dir(src.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_backup_info(void) {
    test_count++;
    printf("\n--- Test %d: Backup info metadata ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "info_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    insert_keys(&src, 0, 500);

    time_t before = time(NULL);
    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    time_t after = time(NULL);
    ASSERT(ok, "export for info test");

    db_backup_info_t info;
    ok = db_backup_info(BACKUP_FILE_PATH, &info);
    ASSERT(ok, "backup info");
    ASSERT_EQ(info.entry_count, 500, "entry_count should be 500");
    ASSERT_EQ(info.version, DB_BACKUP_VERSION, "version should match");
    ASSERT_EQ(info.key_size, KEY_SIZE, "key_size should match");
    ASSERT(info.timestamp >= (uint64_t)before && info.timestamp <= (uint64_t)after,
           "timestamp should be recent");

    close_env(&src);
    cleanup_dir(src.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_corrupted_header(void) {
    test_count++;
    printf("\n--- Test %d: Corrupted header detection ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "corrhdr_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    insert_keys(&src, 0, 10);
    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export for corruption test");
    close_env(&src);
    cleanup_dir(src.dir_path);

    FILE *fp = fopen(BACKUP_FILE_PATH, "r+b");
    ASSERT(fp != NULL, "open backup for corruption");
    uint32_t bad_magic = 0xDEADBEEF;
    fwrite(&bad_magic, 4, 1, fp);
    fclose(fp);

    test_env_t dst = {0};
    make_paths(&dst, "corrhdr_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(!ok, "import corrupted header should fail");

    db_backup_info_t info;
    ok = db_backup_info(BACKUP_FILE_PATH, &info);
    ASSERT(!ok, "info on corrupted header should fail");

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_corrupted_data(void) {
    test_count++;
    printf("\n--- Test %d: Corrupted data detection ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "corrdata_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    insert_keys(&src, 0, 100);
    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export for data corruption test");
    close_env(&src);
    cleanup_dir(src.dir_path);

    FILE *fp = fopen(BACKUP_FILE_PATH, "r+b");
    ASSERT(fp != NULL, "open backup for data corruption");
    fseek(fp, sizeof(db_backup_header_t) + 50, SEEK_SET);
    uint8_t bad_byte = 0xFF;
    fwrite(&bad_byte, 1, 1, fp);
    fclose(fp);

    test_env_t dst = {0};
    make_paths(&dst, "corrdata_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(!ok, "import with corrupted data should fail");

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_truncated_file(void) {
    test_count++;
    printf("\n--- Test %d: Truncated file detection ---\n", test_count);

    test_env_t src = {0};
    make_paths(&src, "trunc_src");
    cleanup_dir(src.dir_path);
    cleanup_backup();
    open_env(&src);

    insert_keys(&src, 0, 100);
    bool ok = db_backup_export(src.tree, BACKUP_FILE_PATH);
    ASSERT(ok, "export for truncation test");
    close_env(&src);
    cleanup_dir(src.dir_path);

    struct stat st;
    stat(BACKUP_FILE_PATH, &st);
    truncate(BACKUP_FILE_PATH, st.st_size / 2);

    test_env_t dst = {0};
    make_paths(&dst, "trunc_dst");
    cleanup_dir(dst.dir_path);
    open_env(&dst);

    ok = db_backup_import(dst.tree, BACKUP_FILE_PATH);
    ASSERT(!ok, "import truncated file should fail");

    close_env(&dst);
    cleanup_dir(dst.dir_path);
    cleanup_backup();

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

static void test_invalid_paths(void) {
    test_count++;
    printf("\n--- Test %d: Invalid path handling ---\n", test_count);

    test_env_t env = {0};
    make_paths(&env, "invalid_src");
    cleanup_dir(env.dir_path);
    open_env(&env);

    bool ok = db_backup_export(env.tree, "/nonexistent/dir/backup.artb");
    ASSERT(!ok, "export to invalid path should fail");

    ok = db_backup_import(env.tree, "/nonexistent/backup.artb");
    ASSERT(!ok, "import from nonexistent file should fail");

    db_backup_info_t info;
    ok = db_backup_info("/nonexistent/backup.artb", &info);
    ASSERT(!ok, "info on nonexistent file should fail");

    ok = db_backup_export(NULL, BACKUP_FILE_PATH);
    ASSERT(!ok, "export with NULL tree should fail");
    ok = db_backup_export(env.tree, NULL);
    ASSERT(!ok, "export with NULL path should fail");
    ok = db_backup_import(NULL, BACKUP_FILE_PATH);
    ASSERT(!ok, "import with NULL tree should fail");
    ok = db_backup_info(NULL, &info);
    ASSERT(!ok, "info with NULL path should fail");

    close_env(&env);
    cleanup_dir(env.dir_path);

    printf(COLOR_GREEN "  PASSED\n" COLOR_RESET);
    tests_passed++;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    log_init(LOG_LEVEL_WARN, stderr);

    printf("\n");
    printf("========================================\n");
    printf("   Database Backup Tests\n");
    printf("========================================\n");

    test_export_empty_tree();
    test_round_trip_single();
    test_round_trip_1000();
    test_round_trip_overflow();
    test_backup_info();
    test_corrupted_header();
    test_corrupted_data();
    test_truncated_file();
    test_invalid_paths();

    printf("\n");
    printf("========================================\n");
    printf("  Results: %d/%d passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return tests_passed == test_count ? 0 : 1;
}
