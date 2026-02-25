/**
 * Persistent ART Iterator Test Suite
 *
 * Tests:
 *   1. Empty tree iteration
 *   2. Single key iteration
 *   3. Sorted order (100 keys)
 *   4. Large scale (10,000 keys)
 *   5. Concurrent writes during iteration
 *   6. Iterator with overflow values
 *   7. Seek exact match
 *   8. Seek between keys
 *   9. Seek before/past all keys
 *  10. Range scan with seek
 *  11. Seek on empty tree
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
#define BASE_DB_PATH  "/tmp/test_iter_db"
#define BASE_WAL_PATH "/tmp/test_iter_wal"

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
    bp_config.capacity = 1024;
    env->bp = buffer_pool_create(&bp_config, env->pm);
    ASSERT(env->bp != NULL, "buffer_pool_create");

    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = 8 * 1024 * 1024;
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
        ASSERT(ok, "data_art_insert");
    }
}

// ============================================================================
// Test 1: Empty Tree Iteration
// ============================================================================

static void test_empty_tree(void) {
    test_env_t env = {0};
    make_paths(&env, "t1");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "iterator_create on empty tree");

    ASSERT(!data_art_iterator_next(iter), "next() should return false on empty tree");
    ASSERT(data_art_iterator_done(iter), "should be done");
    ASSERT(data_art_iterator_key(iter, NULL) == NULL, "key should be NULL");
    ASSERT(data_art_iterator_value(iter, NULL) == NULL, "value should be NULL");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 2: Single Key Iteration
// ============================================================================

static void test_single_key(void) {
    test_env_t env = {0};
    make_paths(&env, "t2");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    uint8_t key[KEY_SIZE];
    generate_key(key, 42);
    char value[64];
    size_t vlen = generate_value(value, sizeof(value), 42);
    ASSERT(data_art_insert(env.tree, key, KEY_SIZE, value, vlen), "insert");

    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "create");

    printf("  Iterating single key...\n");
    ASSERT(data_art_iterator_next(iter), "first next()");

    size_t got_klen = 0, got_vlen = 0;
    const uint8_t *got_key = data_art_iterator_key(iter, &got_klen);
    const void *got_val = data_art_iterator_value(iter, &got_vlen);

    ASSERT(got_key != NULL, "key not NULL");
    ASSERT(got_val != NULL, "value not NULL");
    ASSERT_EQ(got_klen, KEY_SIZE, "key length");
    ASSERT_EQ(got_vlen, vlen, "value length");
    ASSERT(memcmp(got_key, key, KEY_SIZE) == 0, "key content");
    ASSERT(memcmp(got_val, value, vlen) == 0, "value content");

    ASSERT(!data_art_iterator_next(iter), "second next() should return false");
    ASSERT(data_art_iterator_done(iter), "should be done");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 3: Sorted Order (100 keys)
// ============================================================================

static void test_sorted_order(void) {
    test_env_t env = {0};
    make_paths(&env, "t3");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    int n = 100;
    printf("  Inserting %d keys...\n", n);
    insert_keys(&env, 0, n);

    printf("  Iterating and checking sorted order...\n");
    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "create");

    uint8_t prev_key[KEY_SIZE];
    memset(prev_key, 0, KEY_SIZE);
    int count = 0;
    bool first = true;

    while (data_art_iterator_next(iter)) {
        size_t klen = 0;
        const uint8_t *key = data_art_iterator_key(iter, &klen);
        ASSERT(key != NULL, "key not NULL during iteration");
        ASSERT_EQ(klen, KEY_SIZE, "key length");

        if (!first) {
            // Verify lexicographic ordering: prev_key < key
            int cmp = memcmp(prev_key, key, KEY_SIZE);
            if (cmp >= 0) {
                fprintf(stderr, COLOR_RED "  FAILED: keys not in sorted order at position %d\n" COLOR_RESET, count);
                exit(1);
            }
        }
        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;
    }

    printf("  Iterated %d keys\n", count);
    ASSERT_EQ(count, n, "should iterate all keys");
    ASSERT(data_art_iterator_done(iter), "should be done");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 4: Large Scale (10,000 keys)
// ============================================================================

static void test_large_scale(void) {
    test_env_t env = {0};
    make_paths(&env, "t4");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    int n = 10000;
    printf("  Inserting %d keys...\n", n);
    insert_keys(&env, 0, n);

    printf("  Iterating all %d keys...\n", n);
    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "create");

    uint8_t prev_key[KEY_SIZE];
    memset(prev_key, 0, KEY_SIZE);
    int count = 0;
    bool first = true;

    while (data_art_iterator_next(iter)) {
        size_t klen = 0;
        const uint8_t *key = data_art_iterator_key(iter, &klen);
        ASSERT(key != NULL, "key not NULL");

        if (!first) {
            int cmp = memcmp(prev_key, key, KEY_SIZE);
            if (cmp >= 0) {
                fprintf(stderr, COLOR_RED "  FAILED: keys not sorted at position %d\n" COLOR_RESET, count);
                exit(1);
            }
        }
        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;
    }

    printf("  Iterated %d keys\n", count);
    ASSERT_EQ(count, n, "should iterate all keys");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 5: Concurrent Writes During Iteration
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
        size_t vlen = generate_value(value, sizeof(value), args->start + i);
        data_art_insert(args->env->tree, key, KEY_SIZE, value, vlen);
    }
    *args->done = true;
    return NULL;
}

static void test_concurrent_writes(void) {
    test_env_t env = {0};
    make_paths(&env, "t5");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    int initial_count = 200;
    printf("  Inserting %d initial keys...\n", initial_count);
    insert_keys(&env, 0, initial_count);

    // Create iterator (captures committed root snapshot)
    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "create");

    // Start writer thread adding more keys
    volatile bool writer_done = false;
    writer_args_t args = {
        .env = &env,
        .start = 10000,  // Non-overlapping range
        .count = 200,
        .done = &writer_done,
    };

    printf("  Starting writer thread (200 more keys)...\n");
    pthread_t writer;
    pthread_create(&writer, NULL, writer_thread_fn, &args);

    // Iterate — should see exactly the initial keys (snapshot isolation)
    int count = 0;
    while (data_art_iterator_next(iter)) {
        count++;
    }

    pthread_join(writer, NULL);

    printf("  Iterator saw %d keys (expected %d from snapshot)\n", count, initial_count);
    ASSERT_EQ(count, initial_count, "iterator should see snapshot, not concurrent writes");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 6: Iterator with Overflow Values
// ============================================================================

static void test_overflow_values(void) {
    test_env_t env = {0};
    make_paths(&env, "t6");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Create a large value that will trigger overflow pages
    // MAX_INLINE_DATA = PAGE_SIZE - PAGE_HEADER_SIZE - LEAF_HEADER_SIZE
    // For 4KB pages, this is roughly ~4000 bytes. Use 8KB to be safe.
    size_t large_value_len = 8192;
    char *large_value = malloc(large_value_len);
    ASSERT(large_value != NULL, "malloc large value");

    int n = 5;
    printf("  Inserting %d keys with %zu-byte values (overflow)...\n", n, large_value_len);
    for (int i = 0; i < n; i++) {
        // Fill value with deterministic pattern
        for (size_t j = 0; j < large_value_len; j++) {
            large_value[j] = (char)((i * 31 + j * 7) & 0xFF);
        }

        uint8_t key[KEY_SIZE];
        generate_key(key, i);
        bool ok = data_art_insert(env.tree, key, KEY_SIZE, large_value, large_value_len);
        ASSERT(ok, "insert overflow key");
    }

    printf("  Iterating and verifying overflow values...\n");
    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "create");

    int count = 0;
    while (data_art_iterator_next(iter)) {
        size_t vlen = 0;
        const void *val = data_art_iterator_value(iter, &vlen);
        ASSERT(val != NULL, "value not NULL");
        ASSERT_EQ(vlen, large_value_len, "overflow value length");

        // Verify content (we can't easily map back to index from sorted order,
        // but we can verify the length is correct and data is non-zero)
        const uint8_t *bytes = (const uint8_t *)val;
        bool has_nonzero = false;
        for (size_t j = 0; j < vlen; j++) {
            if (bytes[j] != 0) { has_nonzero = true; break; }
        }
        ASSERT(has_nonzero, "overflow value should have non-zero content");

        count++;
    }

    printf("  Iterated %d overflow keys\n", count);
    ASSERT_EQ(count, n, "should iterate all overflow keys");

    data_art_iterator_destroy(iter);
    free(large_value);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 7: Seek Exact Match
// ============================================================================

static void test_seek_exact_match(void) {
    test_env_t env = {0};
    make_paths(&env, "t7");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert keys 0-99
    printf("  Inserting 100 keys...\n");
    insert_keys(&env, 0, 100);
    ASSERT(data_art_flush(env.tree), "flush");

    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "iterator_create");

    // Seek to key 50 — should find exact match
    uint8_t seek_key[KEY_SIZE];
    generate_key(seek_key, 50);
    printf("  Seeking to key 50...\n");
    bool found = data_art_iterator_seek(iter, seek_key, KEY_SIZE);
    ASSERT(found, "seek should find key 50");

    size_t klen;
    const uint8_t *k = data_art_iterator_key(iter, &klen);
    ASSERT(k != NULL, "key should not be NULL after seek");
    ASSERT_EQ((long)klen, (long)KEY_SIZE, "key length");
    ASSERT(memcmp(k, seek_key, KEY_SIZE) == 0, "should be exactly key 50");

    // Continue iterating — next key should be key 51 (or the next in sorted order)
    bool has_next = data_art_iterator_next(iter);
    ASSERT(has_next, "should have more keys after 50");

    // Count remaining keys (including the one we just got)
    int count = 1;
    while (data_art_iterator_next(iter)) count++;
    printf("  Keys after seek to 50: 1 (seek result) + 1 (next) + %d (remaining)\n", count);

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 8: Seek Between Keys
// ============================================================================

static void test_seek_between_keys(void) {
    test_env_t env = {0};
    make_paths(&env, "t8");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert only even-indexed keys: 0, 2, 4, 6, ..., 198
    printf("  Inserting 100 even-indexed keys...\n");
    for (int i = 0; i < 100; i++) {
        uint8_t key[KEY_SIZE];
        char value[64];
        generate_key(key, i * 2);
        size_t vlen = generate_value(value, sizeof(value), i * 2);
        bool ok = data_art_insert(env.tree, key, KEY_SIZE, value, vlen);
        ASSERT(ok, "insert even key");
    }
    ASSERT(data_art_flush(env.tree), "flush");

    data_art_iterator_t *iter = data_art_iterator_create(env.tree);

    // Seek to key 3 (doesn't exist) — should land on the first key >= 3
    uint8_t seek_key[KEY_SIZE];
    generate_key(seek_key, 3);
    printf("  Seeking to key 3 (not present, should find next)...\n");
    bool found = data_art_iterator_seek(iter, seek_key, KEY_SIZE);
    ASSERT(found, "seek should find a key >= 3");

    // The result should be key 4 (the next even key after 3 in sorted order)
    // But since generate_key uses hash-based suffix, the sorted order is
    // determined by the full 32-byte key, not just the index.
    // What we CAN verify: the returned key >= seek_key (key 3)
    size_t klen;
    const uint8_t *k = data_art_iterator_key(iter, &klen);
    ASSERT(k != NULL, "key should not be NULL");
    int cmp = memcmp(k, seek_key, KEY_SIZE);
    ASSERT(cmp >= 0, "returned key should be >= seek key");
    printf("  Found key >= seek target (cmp=%d)\n", cmp);

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 9: Seek Before / Past All Keys
// ============================================================================

static void test_seek_boundaries(void) {
    test_env_t env = {0};
    make_paths(&env, "t9");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert keys 100-199
    printf("  Inserting keys 100-199...\n");
    insert_keys(&env, 100, 100);
    ASSERT(data_art_flush(env.tree), "flush");

    // Find the actual first and last keys in sorted order
    data_art_iterator_t *full = data_art_iterator_create(env.tree);
    ASSERT(data_art_iterator_next(full), "tree not empty");
    size_t first_klen;
    const uint8_t *first_key = data_art_iterator_key(full, &first_klen);
    uint8_t saved_first[KEY_SIZE];
    memcpy(saved_first, first_key, KEY_SIZE);
    data_art_iterator_destroy(full);

    // Seek before all keys: use all-zeros key
    printf("  Seeking to all-zeros key (before all)...\n");
    data_art_iterator_t *iter1 = data_art_iterator_create(env.tree);
    uint8_t zero_key[KEY_SIZE];
    memset(zero_key, 0, KEY_SIZE);
    bool found = data_art_iterator_seek(iter1, zero_key, KEY_SIZE);
    ASSERT(found, "seek to before-all should find first key");
    size_t klen;
    const uint8_t *k = data_art_iterator_key(iter1, &klen);
    ASSERT(k != NULL, "key should not be NULL");
    ASSERT(memcmp(k, saved_first, KEY_SIZE) == 0, "should be the first key in tree");
    printf("  Found first key in tree\n");
    data_art_iterator_destroy(iter1);

    // Seek past all keys: use all-0xFF key
    printf("  Seeking to all-0xFF key (past all)...\n");
    data_art_iterator_t *iter2 = data_art_iterator_create(env.tree);
    uint8_t max_key[KEY_SIZE];
    memset(max_key, 0xFF, KEY_SIZE);
    found = data_art_iterator_seek(iter2, max_key, KEY_SIZE);
    ASSERT(!found, "seek to past-all should return false");
    ASSERT(data_art_iterator_done(iter2), "should be done");
    printf("  Correctly returned no result\n");
    data_art_iterator_destroy(iter2);

    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 10: Range Scan with Seek
// ============================================================================

static void test_range_scan(void) {
    test_env_t env = {0};
    make_paths(&env, "t10");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert 500 keys (0-499)
    printf("  Inserting 500 keys...\n");
    insert_keys(&env, 0, 500);
    ASSERT(data_art_flush(env.tree), "flush");

    // First: collect all keys in sorted order to identify range boundaries
    data_art_iterator_t *full = data_art_iterator_create(env.tree);
    uint8_t sorted_keys[500][KEY_SIZE];
    int total = 0;
    while (data_art_iterator_next(full)) {
        size_t klen;
        const uint8_t *k = data_art_iterator_key(full, &klen);
        ASSERT(k != NULL && klen == KEY_SIZE, "valid key");
        memcpy(sorted_keys[total], k, KEY_SIZE);
        total++;
    }
    ASSERT_EQ(total, 500, "all keys iterated");
    data_art_iterator_destroy(full);

    // Seek to key at index 100 (in sorted order), iterate to key at index 399
    uint8_t start_key[KEY_SIZE], end_key[KEY_SIZE];
    memcpy(start_key, sorted_keys[100], KEY_SIZE);
    memcpy(end_key, sorted_keys[399], KEY_SIZE);

    printf("  Range scan: sorted[100] to sorted[399]...\n");
    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    bool found = data_art_iterator_seek(iter, start_key, KEY_SIZE);
    ASSERT(found, "seek to start of range");

    int range_count = 0;
    uint8_t prev_key[KEY_SIZE] = {0};
    bool first = true;

    do {
        size_t klen;
        const uint8_t *k = data_art_iterator_key(iter, &klen);
        ASSERT(k != NULL, "key in range");

        // Stop when past end key
        if (memcmp(k, end_key, KEY_SIZE) > 0) break;

        // Verify sorted order
        if (!first) {
            ASSERT(memcmp(k, prev_key, KEY_SIZE) > 0, "keys should be in sorted order");
        }
        memcpy(prev_key, k, KEY_SIZE);
        first = false;
        range_count++;
    } while (data_art_iterator_next(iter));

    printf("  Range scan returned %d keys (expected 300)\n", range_count);
    ASSERT_EQ(range_count, 300, "range scan count");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 11: Seek on Empty Tree
// ============================================================================

static void test_seek_empty_tree(void) {
    test_env_t env = {0};
    make_paths(&env, "t11");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    data_art_iterator_t *iter = data_art_iterator_create(env.tree);
    ASSERT(iter != NULL, "iterator_create on empty tree");

    uint8_t seek_key[KEY_SIZE];
    generate_key(seek_key, 42);
    printf("  Seeking in empty tree...\n");
    bool found = data_art_iterator_seek(iter, seek_key, KEY_SIZE);
    ASSERT(!found, "seek on empty tree should return false");
    ASSERT(data_art_iterator_done(iter), "should be done");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 12: Prefix iteration — basic
// ============================================================================

static void test_prefix_basic(void) {
    test_env_t env;
    make_paths(&env, "prefix_basic");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert keys with three different prefixes: "aaa_", "bbb_", "ccc_"
    const char *prefixes[] = {"aaa_", "bbb_", "ccc_"};
    int per_prefix = 10;

    for (int p = 0; p < 3; p++) {
        for (int i = 0; i < per_prefix; i++) {
            uint8_t key[KEY_SIZE];
            memset(key, 0, KEY_SIZE);
            snprintf((char *)key, KEY_SIZE, "%s%04d", prefixes[p], i);
            uint8_t val[8];
            snprintf((char *)val, 8, "v%d", p * per_prefix + i);
            ASSERT(data_art_insert(env.tree, key, KEY_SIZE, val, strlen((char *)val) + 1),
                   "insert failed");
        }
    }
    ASSERT_EQ(data_art_size(env.tree), 30, "should have 30 keys");

    // Iterate with prefix "bbb_" — should get exactly 10 keys
    uint8_t prefix[KEY_SIZE];
    memset(prefix, 0, KEY_SIZE);
    memcpy(prefix, "bbb_", 4);

    data_art_iterator_t *iter = data_art_iterator_create_prefix(env.tree, prefix, 4);
    ASSERT(iter != NULL, "create_prefix returned NULL");

    int count = 0;
    do {
        if (data_art_iterator_done(iter)) break;
        size_t klen;
        const uint8_t *k = data_art_iterator_key(iter, &klen);
        ASSERT(k != NULL, "key is NULL");
        ASSERT(memcmp(k, "bbb_", 4) == 0, "key doesn't start with bbb_");
        count++;
    } while (data_art_iterator_next(iter));

    ASSERT_EQ(count, per_prefix, "prefix should yield exactly 10 keys");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 13: Prefix iteration — no match
// ============================================================================

static void test_prefix_no_match(void) {
    test_env_t env;
    make_paths(&env, "prefix_nomatch");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert keys with prefix "aaa_"
    for (int i = 0; i < 20; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "aaa_%04d", i);
        uint8_t val[4] = {1, 2, 3, 4};
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, val, 4), "insert failed");
    }

    // Iterate with prefix "zzz_" — no match
    uint8_t prefix[KEY_SIZE];
    memset(prefix, 0, KEY_SIZE);
    memcpy(prefix, "zzz_", 4);

    data_art_iterator_t *iter = data_art_iterator_create_prefix(env.tree, prefix, 4);
    ASSERT(iter != NULL, "create_prefix returned NULL");
    ASSERT(data_art_iterator_done(iter), "should be done immediately (no match)");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 14: Prefix iteration — all keys match
// ============================================================================

static void test_prefix_all_match(void) {
    test_env_t env;
    make_paths(&env, "prefix_all");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    // Insert 50 keys all starting with "common_"
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "common_%04d", i);
        uint8_t val[4] = {0};
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, val, 4), "insert failed");
    }

    // Iterate with prefix "common_" — all 50 should match
    uint8_t prefix[KEY_SIZE];
    memset(prefix, 0, KEY_SIZE);
    memcpy(prefix, "common_", 7);

    data_art_iterator_t *iter = data_art_iterator_create_prefix(env.tree, prefix, 7);
    ASSERT(iter != NULL, "create_prefix returned NULL");

    int count = 0;
    do {
        if (data_art_iterator_done(iter)) break;
        count++;
    } while (data_art_iterator_next(iter));

    ASSERT_EQ(count, 50, "all 50 keys should match prefix");

    data_art_iterator_destroy(iter);
    close_env(&env);
    cleanup_paths(env.db_path, env.wal_path);
}

// ============================================================================
// Test 15: Prefix iteration — empty prefix (full iteration)
// ============================================================================

static void test_prefix_empty(void) {
    test_env_t env;
    make_paths(&env, "prefix_empty");
    cleanup_paths(env.db_path, env.wal_path);
    open_env(&env);

    for (int i = 0; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "key_%04d", i);
        uint8_t val[4] = {0};
        ASSERT(data_art_insert(env.tree, key, KEY_SIZE, val, 4), "insert failed");
    }

    // prefix_len = 0 should behave like full iteration
    data_art_iterator_t *iter = data_art_iterator_create_prefix(env.tree, NULL, 0);
    ASSERT(iter != NULL, "create_prefix with NULL prefix returned NULL");

    int count = 0;
    while (data_art_iterator_next(iter)) {
        count++;
    }

    ASSERT_EQ(count, 30, "empty prefix should return all 30 keys");

    data_art_iterator_destroy(iter);
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
    printf(" Persistent ART Iterator Test Suite\n");
    printf("========================================\n\n");

    #define RUN_TEST(fn) do { \
        test_count++; \
        printf("[TEST %d] %s\n", test_count, #fn); \
        fn(); \
        tests_passed++; \
        printf(COLOR_GREEN "  PASSED\n\n" COLOR_RESET); \
    } while(0)

    RUN_TEST(test_empty_tree);
    RUN_TEST(test_single_key);
    RUN_TEST(test_sorted_order);
    RUN_TEST(test_large_scale);
    RUN_TEST(test_concurrent_writes);
    RUN_TEST(test_overflow_values);
    RUN_TEST(test_seek_exact_match);
    RUN_TEST(test_seek_between_keys);
    RUN_TEST(test_seek_boundaries);
    RUN_TEST(test_range_scan);
    RUN_TEST(test_seek_empty_tree);
    RUN_TEST(test_prefix_basic);
    RUN_TEST(test_prefix_no_match);
    RUN_TEST(test_prefix_all_match);
    RUN_TEST(test_prefix_empty);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", tests_passed, test_count);
    printf("========================================\n\n");

    return (tests_passed == test_count) ? 0 : 1;
}
