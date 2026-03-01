#include "../include/nibble_trie.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TEST_PATH  "/tmp/test_nibble_trie.dat"

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

/* Generate deterministic 32-byte key from integer */
static void make_key(uint8_t *key, uint32_t i) {
    memset(key, 0, 32);
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = i & 0xFF;
    key[16] = (i * 7) & 0xFF;
    key[17] = (i * 13) & 0xFF;
    key[31] = (i * 31) & 0xFF;
}

/* Generate deterministic 32-byte value from integer */
static void make_value(uint8_t *val, uint32_t i) {
    memset(val, 0, 32);
    uint32_t v = i * 1000 + 42;
    memcpy(val, &v, 4);
    val[16] = (i * 3) & 0xFF;
    val[31] = (i * 17) & 0xFF;
}

/* ====================================================================== */

static int test_create_close(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");
    ASSERT(nt_size(&t) == 0, "empty");
    nt_close(&t);
    return 1;
}

static int test_insert_get_single(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val[32];
    make_key(key, 1);
    make_value(val, 1);

    ASSERT(nt_insert(&t, key, val), "insert");
    ASSERT(nt_size(&t) == 1, "size");

    const uint8_t *got = nt_get(&t, key);
    ASSERT(got != NULL, "get not null");
    ASSERT(memcmp(got, val, 32) == 0, "value match");

    uint8_t miss[32];
    make_key(miss, 999);
    ASSERT(nt_get(&t, miss) == NULL, "miss is null");

    nt_close(&t);
    return 1;
}

static int test_insert_multiple(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    const int N = 1000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        ASSERT(nt_insert(&t, key, val), "insert");
    }
    ASSERT(nt_size(&t) == (size_t)N, "size");

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "get");
        ASSERT(memcmp(got, val, 32) == 0, "value match");
    }

    make_key(key, N + 1);
    ASSERT(nt_get(&t, key) == NULL, "miss");

    nt_close(&t);
    return 1;
}

static int test_update(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val1[32], val2[32];
    make_key(key, 42);
    make_value(val1, 42);
    make_value(val2, 9999);

    ASSERT(nt_insert(&t, key, val1), "insert");
    ASSERT(nt_size(&t) == 1, "size 1");

    ASSERT(nt_insert(&t, key, val2), "update");
    ASSERT(nt_size(&t) == 1, "size still 1");

    const uint8_t *got = nt_get(&t, key);
    ASSERT(got != NULL, "get");
    ASSERT(memcmp(got, val2, 32) == 0, "updated value");

    nt_close(&t);
    return 1;
}

static int test_delete(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val[32];
    make_key(key, 1);
    make_value(val, 1);

    ASSERT(nt_insert(&t, key, val), "insert");
    ASSERT(nt_size(&t) == 1, "size 1");

    ASSERT(nt_delete(&t, key), "delete");
    ASSERT(nt_size(&t) == 0, "size 0");
    ASSERT(nt_get(&t, key) == NULL, "gone");

    ASSERT(!nt_delete(&t, key), "double delete fails");

    nt_close(&t);
    return 1;
}

static int test_delete_multiple(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    const int N = 200;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "inserted all");

    /* Delete evens */
    for (int i = 0; i < N; i += 2) {
        make_key(key, i);
        ASSERT(nt_delete(&t, key), "delete even");
    }
    ASSERT(nt_size(&t) == (size_t)(N / 2), "half left");

    /* Verify odds remain, evens gone */
    for (int i = 0; i < N; i++) {
        make_key(key, i);
        if (i % 2 == 0) {
            ASSERT(nt_get(&t, key) == NULL, "even gone");
        } else {
            make_value(val, i);
            const uint8_t *got = nt_get(&t, key);
            ASSERT(got != NULL, "odd present");
            ASSERT(memcmp(got, val, 32) == 0, "odd value");
        }
    }

    nt_close(&t);
    return 1;
}

static int test_commit_reopen(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    const int N = 500;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit");
    nt_close(&t);

    /* Reopen */
    ASSERT(nt_open(&t, TEST_PATH, 32), "reopen");
    ASSERT(nt_size(&t) == (size_t)N, "size preserved");

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "get after reopen");
        ASSERT(memcmp(got, val, 32) == 0, "value after reopen");
    }

    nt_close(&t);
    return 1;
}

static int test_uncommitted_lost(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key1[32], val1[32], key2[32], val2[32];
    make_key(key1, 1);
    make_value(val1, 1);
    make_key(key2, 2);
    make_value(val2, 2);

    nt_insert(&t, key1, val1);
    ASSERT(nt_commit(&t), "commit key1");

    nt_insert(&t, key2, val2);
    /* no commit */
    nt_close(&t);

    /* Reopen: key2 should be lost */
    ASSERT(nt_open(&t, TEST_PATH, 32), "reopen");
    ASSERT(nt_size(&t) == 1, "only committed");
    ASSERT(nt_get(&t, key1) != NULL, "key1 present");
    ASSERT(nt_get(&t, key2) == NULL, "key2 lost");

    nt_close(&t);
    return 1;
}

static int test_rollback(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key1[32], val1[32], key2[32], val2[32];
    make_key(key1, 1);
    make_value(val1, 1);
    make_key(key2, 2);
    make_value(val2, 2);

    nt_insert(&t, key1, val1);
    ASSERT(nt_commit(&t), "commit");

    nt_insert(&t, key2, val2);
    ASSERT(nt_size(&t) == 2, "size 2");

    nt_rollback(&t);
    ASSERT(nt_size(&t) == 1, "rolled back");
    ASSERT(nt_get(&t, key1) != NULL, "key1 present");
    ASSERT(nt_get(&t, key2) == NULL, "key2 rolled back");

    nt_close(&t);
    return 1;
}

static int test_iterator(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    const int N = 100;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create iter");

    int count = 0;
    uint8_t prev[32];
    memset(prev, 0, 32);

    while (nt_iterator_next(it)) {
        const uint8_t *k = nt_iterator_key(it);
        ASSERT(k != NULL, "iter key not null");
        if (count > 0) {
            ASSERT(memcmp(prev, k, 32) < 0, "sorted order");
        }
        memcpy(prev, k, 32);
        count++;
    }

    ASSERT(count == N, "iter count");
    ASSERT(nt_iterator_done(it), "done");
    nt_iterator_destroy(it);

    nt_close(&t);
    return 1;
}

static int test_seek(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    /* Insert keys 0, 10, 20, ..., 90 */
    uint8_t key[32], val[32];
    for (int i = 0; i < 100; i += 10) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");

    /* Seek to key 25 — should land on 30 */
    make_key(key, 25);
    bool found = nt_iterator_seek(it, key);
    ASSERT(found, "seek found");

    uint8_t expected[32];
    make_key(expected, 30);
    ASSERT(memcmp(nt_iterator_key(it), expected, 32) == 0, "seek landed on 30");

    /* Next should be 40 */
    ASSERT(nt_iterator_next(it), "next after seek");
    make_key(expected, 40);
    ASSERT(memcmp(nt_iterator_key(it), expected, 32) == 0, "next is 40");

    nt_iterator_destroy(it);
    nt_close(&t);
    return 1;
}

static int test_contains(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val[32];
    make_key(key, 42);
    make_value(val, 42);

    ASSERT(!nt_contains(&t, key), "not yet");
    nt_insert(&t, key, val);
    ASSERT(nt_contains(&t, key), "contains");
    nt_delete(&t, key);
    ASSERT(!nt_contains(&t, key), "deleted");

    nt_close(&t);
    return 1;
}

static int test_multiple_commits(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val[32];

    /* Commit cycle 1: insert 0-99 */
    for (int i = 0; i < 100; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit 1");

    /* Commit cycle 2: delete evens, insert 100-149 */
    for (int i = 0; i < 100; i += 2) {
        make_key(key, i);
        nt_delete(&t, key);
    }
    for (int i = 100; i < 150; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit 2");

    nt_close(&t);

    /* Reopen and verify */
    ASSERT(nt_open(&t, TEST_PATH, 32), "reopen");
    ASSERT(nt_size(&t) == 100, "50 odds + 50 new = 100");

    for (int i = 0; i < 100; i++) {
        make_key(key, i);
        if (i % 2 == 0) {
            ASSERT(nt_get(&t, key) == NULL, "even deleted");
        } else {
            ASSERT(nt_get(&t, key) != NULL, "odd present");
        }
    }
    for (int i = 100; i < 150; i++) {
        make_key(key, i);
        ASSERT(nt_get(&t, key) != NULL, "new present");
    }

    nt_close(&t);
    return 1;
}

static int test_stress(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    const int N = 100000;
    uint8_t key[32], val[32];

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_size(&t) == (size_t)N, "inserted");

    /* Verify all */
    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "get");
        ASSERT(memcmp(got, val, 32) == 0, "val");
    }

    ASSERT(nt_commit(&t), "commit");
    nt_close(&t);

    /* Reopen and verify */
    ASSERT(nt_open(&t, TEST_PATH, 32), "reopen");
    ASSERT(nt_size(&t) == (size_t)N, "size after reopen");

    for (int i = 0; i < N; i++) {
        make_key(key, i);
        make_value(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "get after reopen");
        ASSERT(memcmp(got, val, 32) == 0, "val after reopen");
    }

    nt_close(&t);
    return 1;
}

static int test_mixed_operations(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    uint8_t key[32], val[32];

    /* Phase 1: insert 0-499, commit */
    for (int i = 0; i < 500; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit 1");

    /* Phase 2: delete 0-249, update 250-499, insert 500-749 */
    for (int i = 0; i < 250; i++) {
        make_key(key, i);
        nt_delete(&t, key);
    }
    for (int i = 250; i < 500; i++) {
        make_key(key, i);
        make_value(val, i + 10000);
        nt_insert(&t, key, val);
    }
    for (int i = 500; i < 750; i++) {
        make_key(key, i);
        make_value(val, i);
        nt_insert(&t, key, val);
    }
    ASSERT(nt_commit(&t), "commit 2");
    nt_close(&t);

    /* Verify */
    ASSERT(nt_open(&t, TEST_PATH, 32), "reopen");
    ASSERT(nt_size(&t) == 500, "500 left");

    for (int i = 0; i < 250; i++) {
        make_key(key, i);
        ASSERT(nt_get(&t, key) == NULL, "deleted range");
    }
    for (int i = 250; i < 500; i++) {
        make_key(key, i);
        make_value(val, i + 10000);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "updated present");
        ASSERT(memcmp(got, val, 32) == 0, "updated value");
    }
    for (int i = 500; i < 750; i++) {
        make_key(key, i);
        make_value(val, i);
        const uint8_t *got = nt_get(&t, key);
        ASSERT(got != NULL, "new present");
        ASSERT(memcmp(got, val, 32) == 0, "new value");
    }

    nt_close(&t);
    return 1;
}

static int test_empty_iterator(void) {
    nibble_trie_t t;
    ASSERT(nt_open(&t, TEST_PATH, 32), "open");

    nt_iterator_t *it = nt_iterator_create(&t);
    ASSERT(it != NULL, "create");
    ASSERT(!nt_iterator_next(it), "empty next");
    ASSERT(nt_iterator_done(it), "done");
    nt_iterator_destroy(it);

    nt_close(&t);
    return 1;
}

int main(void) {
    printf("=== Nibble Trie Tests ===\n\n");

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
    RUN_TEST(test_empty_iterator);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
