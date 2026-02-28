#include "../include/bitmap_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define TEST_PATH     "/tmp/test_bitmap_art.dat"
#define KEY_SIZE      32
#define VALUE_SIZE    4

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { \
        printf("  FAIL: %s (line %d)\n", msg, __LINE__); \
        return 0; \
    } \
} while(0)

#define RUN_TEST(fn) do { \
    unlink(TEST_PATH); \
    tests_run++; \
    printf("%-50s ", #fn); \
    fflush(stdout); \
    if (fn()) { \
        printf("PASS\n"); \
        tests_passed++; \
    } else { \
        printf("FAIL\n"); \
    } \
    unlink(TEST_PATH); \
} while(0)

// Generate a deterministic 32-byte key from an integer.
static void make_key(uint8_t *key, uint32_t i) {
    memset(key, 0, KEY_SIZE);
    // Spread bits for good distribution
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = i & 0xFF;
    // Add some variation in middle bytes
    key[16] = (i * 7) & 0xFF;
    key[17] = (i * 13) & 0xFF;
    key[31] = (i * 31) & 0xFF;
}

static void make_value(uint32_t *val, uint32_t i) {
    *val = i * 1000 + 42;
}

// ============================================================================
// Tests
// ============================================================================

static int test_create_close(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE),
           "open should succeed");
    ASSERT(bart_size(&tree) == 0, "new tree should be empty");
    bart_close(&tree);
    return 1;
}

static int test_insert_get_single(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    uint8_t key[KEY_SIZE];
    uint32_t val;
    make_key(key, 1);
    make_value(&val, 1);

    ASSERT(bart_insert(&tree, key, &val), "insert should succeed");
    ASSERT(bart_size(&tree) == 1, "size should be 1");

    const void *got = bart_get(&tree, key);
    ASSERT(got != NULL, "get should find key");

    uint32_t got_val;
    memcpy(&got_val, got, VALUE_SIZE);
    ASSERT(got_val == val, "value should match");

    bart_close(&tree);
    return 1;
}

static int test_insert_multiple(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        ASSERT(bart_insert(&tree, key, &val), "insert");
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size should match");

    // Verify all keys
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t expected;
        make_key(key, i);
        make_value(&expected, i);

        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "get should find key");
        uint32_t got_val;
        memcpy(&got_val, got, VALUE_SIZE);
        ASSERT(got_val == expected, "value should match");
    }

    // Verify missing key
    uint8_t missing[KEY_SIZE];
    make_key(missing, N + 999);
    ASSERT(bart_get(&tree, missing) == NULL, "missing key should return NULL");

    bart_close(&tree);
    return 1;
}

static int test_update(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    uint8_t key[KEY_SIZE];
    make_key(key, 42);

    uint32_t val1 = 111;
    ASSERT(bart_insert(&tree, key, &val1), "insert");
    ASSERT(bart_size(&tree) == 1, "size 1 after insert");

    uint32_t val2 = 222;
    ASSERT(bart_insert(&tree, key, &val2), "update");
    ASSERT(bart_size(&tree) == 1, "size still 1 after update");

    const void *got = bart_get(&tree, key);
    ASSERT(got != NULL, "get");
    uint32_t got_val;
    memcpy(&got_val, got, VALUE_SIZE);
    ASSERT(got_val == 222, "should have updated value");

    bart_close(&tree);
    return 1;
}

static int test_delete(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    uint8_t key[KEY_SIZE];
    uint32_t val;
    make_key(key, 1);
    make_value(&val, 1);

    ASSERT(bart_insert(&tree, key, &val), "insert");
    ASSERT(bart_size(&tree) == 1, "size 1");

    ASSERT(bart_delete(&tree, key), "delete should succeed");
    ASSERT(bart_size(&tree) == 0, "size 0 after delete");
    ASSERT(bart_get(&tree, key) == NULL, "get should return NULL after delete");

    // Double delete should return false
    ASSERT(!bart_delete(&tree, key), "double delete should fail");

    bart_close(&tree);
    return 1;
}

static int test_delete_multiple(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 200;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size after inserts");

    // Delete even keys
    for (int i = 0; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        ASSERT(bart_delete(&tree, key), "delete even key");
    }
    ASSERT(bart_size(&tree) == (size_t)N / 2, "size after deletes");

    // Verify odd keys still present, even keys gone
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        if (i % 2 == 0) {
            ASSERT(bart_get(&tree, key) == NULL, "even key should be gone");
        } else {
            ASSERT(bart_get(&tree, key) != NULL, "odd key should exist");
        }
    }

    bart_close(&tree);
    return 1;
}

static int test_commit_reopen(void) {
    // Insert, commit, close, reopen — data should persist.
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 500;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }

    ASSERT(bart_commit(&tree), "commit should succeed");
    bart_close(&tree);

    // Reopen
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == (size_t)N, "size should persist");

    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t expected;
        make_key(key, i);
        make_value(&expected, i);

        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "key should persist");
        uint32_t got_val;
        memcpy(&got_val, got, VALUE_SIZE);
        ASSERT(got_val == expected, "value should persist");
    }

    bart_close(&tree);
    return 1;
}

static int test_uncommitted_lost(void) {
    // Insert without commit, close, reopen — data should be lost.
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Commit some data
    uint8_t key1[KEY_SIZE];
    uint32_t val1;
    make_key(key1, 1);
    make_value(&val1, 1);
    bart_insert(&tree, key1, &val1);
    ASSERT(bart_commit(&tree), "commit key1");

    // Insert more without commit
    uint8_t key2[KEY_SIZE];
    uint32_t val2;
    make_key(key2, 2);
    make_value(&val2, 2);
    bart_insert(&tree, key2, &val2);
    ASSERT(bart_size(&tree) == 2, "size 2 before close");

    // Close without committing key2
    bart_close(&tree);

    // Reopen — only key1 should exist
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == 1, "only committed data should persist");
    ASSERT(bart_get(&tree, key1) != NULL, "key1 should persist");
    ASSERT(bart_get(&tree, key2) == NULL, "key2 should be lost");

    bart_close(&tree);
    return 1;
}

static int test_rollback(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    uint8_t key1[KEY_SIZE], key2[KEY_SIZE];
    uint32_t val1, val2;
    make_key(key1, 1);
    make_value(&val1, 1);
    make_key(key2, 2);
    make_value(&val2, 2);

    bart_insert(&tree, key1, &val1);
    ASSERT(bart_commit(&tree), "commit key1");

    bart_insert(&tree, key2, &val2);
    ASSERT(bart_size(&tree) == 2, "size 2 before rollback");

    bart_rollback(&tree);
    ASSERT(bart_size(&tree) == 1, "size 1 after rollback");
    ASSERT(bart_get(&tree, key1) != NULL, "key1 should survive rollback");
    ASSERT(bart_get(&tree, key2) == NULL, "key2 should be rolled back");

    bart_close(&tree);
    return 1;
}

static int test_iterator(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 100;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }

    // Iterate and check sorted order
    bart_iterator_t *it = bart_iterator_create(&tree);
    ASSERT(it != NULL, "iterator create");

    int count = 0;
    uint8_t prev_key[KEY_SIZE];
    memset(prev_key, 0, KEY_SIZE);

    while (bart_iterator_next(it)) {
        const uint8_t *k = bart_iterator_key(it);
        ASSERT(k != NULL, "iterator key");

        if (count > 0) {
            ASSERT(memcmp(prev_key, k, KEY_SIZE) < 0,
                   "keys should be in sorted order");
        }
        memcpy(prev_key, k, KEY_SIZE);
        count++;
    }

    ASSERT(count == N, "iterator should visit all keys");
    ASSERT(bart_iterator_done(it), "iterator should be done");

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

static int test_seek(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Insert keys 0, 10, 20, ..., 90
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i * 10);
        make_value(&val, i * 10);
        bart_insert(&tree, key, &val);
    }

    bart_iterator_t *it = bart_iterator_create(&tree);
    ASSERT(it != NULL, "iterator create");

    // Seek to key 25 — should land on key 30 (first >= 25)
    uint8_t seek_key[KEY_SIZE];
    make_key(seek_key, 25);
    bool found = bart_iterator_seek(it, seek_key);
    ASSERT(found, "seek should find something");

    uint8_t expected[KEY_SIZE];
    make_key(expected, 30);
    const uint8_t *got = bart_iterator_key(it);
    ASSERT(got != NULL, "seek key");
    ASSERT(memcmp(got, expected, KEY_SIZE) == 0, "seek should land on key 30");

    // Seek to exact key
    make_key(seek_key, 50);
    found = bart_iterator_seek(it, seek_key);
    ASSERT(found, "exact seek");
    got = bart_iterator_key(it);
    make_key(expected, 50);
    ASSERT(memcmp(got, expected, KEY_SIZE) == 0, "exact seek should match");

    // Seek beyond all keys
    make_key(seek_key, 999);
    found = bart_iterator_seek(it, seek_key);
    ASSERT(!found, "seek beyond should fail");

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

static int test_contains(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    uint8_t key[KEY_SIZE];
    uint32_t val;
    make_key(key, 42);
    make_value(&val, 42);

    ASSERT(!bart_contains(&tree, key), "should not contain before insert");
    bart_insert(&tree, key, &val);
    ASSERT(bart_contains(&tree, key), "should contain after insert");
    bart_delete(&tree, key);
    ASSERT(!bart_contains(&tree, key), "should not contain after delete");

    bart_close(&tree);
    return 1;
}

static int test_multiple_commits(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Commit 1: insert keys 0-99
    for (int i = 0; i < 100; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_commit(&tree), "commit 1");

    // Commit 2: insert keys 100-199, delete keys 0-49
    for (int i = 100; i < 200; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        bart_delete(&tree, key);
    }
    ASSERT(bart_commit(&tree), "commit 2");
    ASSERT(bart_size(&tree) == 150, "150 keys after commit 2");

    // Close and reopen
    bart_close(&tree);
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == 150, "150 keys persisted");

    // Verify
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        ASSERT(bart_get(&tree, key) == NULL, "deleted key should be gone");
    }
    for (int i = 50; i < 200; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        ASSERT(bart_get(&tree, key) != NULL, "surviving key should exist");
    }

    bart_close(&tree);
    return 1;
}

static int test_stress(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    const int N = 100000;
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        if (!bart_insert(&tree, key, &val)) {
            printf("  insert failed at i=%d\n", i);
            bart_close(&tree);
            return 0;
        }
    }
    ASSERT(bart_size(&tree) == (size_t)N, "size after stress insert");

    // Verify all
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t expected;
        make_key(key, i);
        make_value(&expected, i);

        const void *got = bart_get(&tree, key);
        if (!got) {
            printf("  missing key at i=%d\n", i);
            bart_close(&tree);
            return 0;
        }
        uint32_t got_val;
        memcpy(&got_val, got, VALUE_SIZE);
        if (got_val != expected) {
            printf("  value mismatch at i=%d: got %u expected %u\n",
                   i, got_val, expected);
            bart_close(&tree);
            return 0;
        }
    }

    // Commit and reopen
    ASSERT(bart_commit(&tree), "commit");
    bart_close(&tree);

    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");
    ASSERT(bart_size(&tree) == (size_t)N, "size after reopen");

    // Verify after reopen
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t expected;
        make_key(key, i);
        make_value(&expected, i);

        const void *got = bart_get(&tree, key);
        if (!got) {
            printf("  missing key after reopen at i=%d\n", i);
            bart_close(&tree);
            return 0;
        }
        uint32_t got_val;
        memcpy(&got_val, got, VALUE_SIZE);
        if (got_val != expected) {
            printf("  value mismatch after reopen at i=%d\n", i);
            bart_close(&tree);
            return 0;
        }
    }

    bart_close(&tree);
    return 1;
}

static int test_mixed_operations(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    // Phase 1: insert 500, commit
    for (int i = 0; i < 500; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_commit(&tree), "commit 1");

    // Phase 2: delete 100, update 100, insert 200, commit
    for (int i = 0; i < 100; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        bart_delete(&tree, key);
    }
    for (int i = 100; i < 200; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val = 99999;
        make_key(key, i);
        bart_insert(&tree, key, &val);
    }
    for (int i = 500; i < 700; i++) {
        uint8_t key[KEY_SIZE];
        uint32_t val;
        make_key(key, i);
        make_value(&val, i);
        bart_insert(&tree, key, &val);
    }
    ASSERT(bart_commit(&tree), "commit 2");

    // Close and reopen
    bart_close(&tree);
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "reopen");

    // Expected: keys 100-699, first 100 deleted, 100-199 updated
    ASSERT(bart_size(&tree) == 600, "600 keys");

    for (int i = 0; i < 100; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        ASSERT(bart_get(&tree, key) == NULL, "deleted");
    }
    for (int i = 100; i < 200; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        const void *got = bart_get(&tree, key);
        ASSERT(got != NULL, "updated key exists");
        uint32_t got_val;
        memcpy(&got_val, got, VALUE_SIZE);
        ASSERT(got_val == 99999, "updated value");
    }
    for (int i = 200; i < 700; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        ASSERT(bart_get(&tree, key) != NULL, "key exists");
    }

    bart_close(&tree);
    return 1;
}

static int test_empty_tree_iterator(void) {
    bitmap_art_t tree;
    ASSERT(bart_open(&tree, TEST_PATH, KEY_SIZE, VALUE_SIZE), "open");

    bart_iterator_t *it = bart_iterator_create(&tree);
    ASSERT(it != NULL, "iterator create");
    ASSERT(!bart_iterator_next(it), "next on empty should be false");
    ASSERT(bart_iterator_done(it), "should be done");

    bart_iterator_destroy(it);
    bart_close(&tree);
    return 1;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== Bitmap ART Tests ===\n\n");

    RUN_TEST(test_create_close);
    RUN_TEST(test_insert_get_single);
    RUN_TEST(test_insert_multiple);
    RUN_TEST(test_update);
    RUN_TEST(test_delete);
    RUN_TEST(test_delete_multiple);
    RUN_TEST(test_commit_reopen);
    RUN_TEST(test_uncommitted_lost);
    RUN_TEST(test_rollback);
    RUN_TEST(test_iterator);
    RUN_TEST(test_seek);
    RUN_TEST(test_contains);
    RUN_TEST(test_multiple_commits);
    RUN_TEST(test_stress);
    RUN_TEST(test_mixed_operations);
    RUN_TEST(test_empty_tree_iterator);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
