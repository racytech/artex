/*
 * Online File Compaction — Unit Tests
 *
 * Validates that data_art_compact() correctly relocates live nodes,
 * shrinks the file, and preserves all key-value data.
 */

#include "../include/data_art.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <inttypes.h>

#define KEY_SIZE 32

static int tests_run = 0;
static int tests_passed = 0;

#define ASSERT(cond, fmt, ...) do {                                         \
    if (!(cond)) {                                                          \
        fprintf(stderr, "  FAIL: " fmt " (%s:%d)\n",                        \
                ##__VA_ARGS__, __FILE__, __LINE__);                          \
        return false;                                                       \
    }                                                                       \
} while(0)

#define RUN_TEST(fn) do {                                                   \
    tests_run++;                                                            \
    printf("  [%d] %-55s", tests_run, #fn);                                 \
    fflush(stdout);                                                         \
    if (fn()) { tests_passed++; printf("PASS\n"); }                         \
    else { printf("FAIL\n"); }                                              \
} while(0)

/* ── Helpers ──────────────────────────────────────────────────────── */

static void make_key(uint8_t *key, uint64_t index) {
    memset(key, 0, KEY_SIZE);
    /* Spread bits for good tree fanout */
    uint32_t h = (uint32_t)index * 2654435761u;
    key[0] = (h >> 24) & 0xFF;
    key[1] = (h >> 16) & 0xFF;
    key[2] = (h >> 8)  & 0xFF;
    key[3] = h & 0xFF;
    for (int i = 4; i < 8; i++) {
        h = h * 1103515245u + 12345;
        key[i] = (h >> 16) & 0xFF;
    }
}

static void make_value(char *buf, size_t *len, uint64_t index) {
    *len = (size_t)snprintf(buf, 64, "value-%" PRIu64, index) + 1;
}

static char *test_path(const char *name) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "/tmp/test_compact_%s_%d.art", name, getpid());
    return buf;
}

static off_t file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    return st.st_size;
}

/* ── Test 1: Insert N, delete 50%, compact, verify remaining ──── */

static bool test_compact_basic(void) {
    const int N = 5000;
    const char *path = test_path("basic");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create failed");

    /* Insert N keys */
    uint64_t txn;
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn");
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        ASSERT(data_art_insert(tree, key, KEY_SIZE, val, vlen), "insert %d", i);
    }
    ASSERT(data_art_commit_txn(tree), "commit");
    data_art_checkpoint(tree, NULL);

    ASSERT(data_art_size(tree) == (size_t)N, "size after insert: %zu", data_art_size(tree));

    /* Delete odd-indexed keys (50%) */
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn 2");
    for (int i = 1; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        data_art_delete(tree, key, KEY_SIZE);
    }
    ASSERT(data_art_commit_txn(tree), "commit 2");
    data_art_checkpoint(tree, NULL);

    size_t remaining = N / 2;
    ASSERT(data_art_size(tree) == remaining, "size after delete: %zu", data_art_size(tree));

    off_t size_before = file_size(path);

    /* Compact */
    data_art_compact_result_t result;
    ASSERT(data_art_compact(tree, &result), "compact");
    ASSERT(result.pages_freed > 0, "pages_freed=%lu", result.pages_freed);
    ASSERT(result.nodes_relocated > 0, "nodes_relocated=%lu", result.nodes_relocated);

    off_t size_after = file_size(path);
    ASSERT(size_after < size_before,
           "file should shrink: %ld → %ld", (long)size_before, (long)size_after);

    /* Verify all remaining keys */
    ASSERT(data_art_size(tree) == remaining, "size after compact: %zu", data_art_size(tree));
    for (int i = 0; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        char expected[64]; size_t elen;
        make_key(key, i);
        make_value(expected, &elen, i);

        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        ASSERT(val, "key %d missing after compact", i);
        ASSERT(vlen == elen, "key %d vlen %zu != %zu", i, vlen, elen);
        ASSERT(memcmp(val, expected, vlen) == 0, "key %d value mismatch", i);
        free((void *)val);
    }

    /* Verify deleted keys are still gone */
    for (int i = 1; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        ASSERT(!val, "key %d should be deleted", i);
    }

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 2: Compact already-compact tree (no-op) ────────────────── */

static bool test_compact_noop(void) {
    const int N = 200;
    const char *path = test_path("noop");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    uint64_t txn;
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn");
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        data_art_insert(tree, key, KEY_SIZE, val, vlen);
    }
    ASSERT(data_art_commit_txn(tree), "commit");
    data_art_checkpoint(tree, NULL);

    /* Compact — may reclaim CoW dead pages from insert, but tree stays valid */
    data_art_compact_result_t result;
    ASSERT(data_art_compact(tree, &result), "compact");

    ASSERT(data_art_size(tree) == (size_t)N, "size after compact: %zu", data_art_size(tree));

    /* Verify all keys */
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        ASSERT(val, "key %d missing", i);
        free((void *)val);
    }

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 3: Compact empty tree ──────────────────────────────────── */

static bool test_compact_empty(void) {
    const char *path = test_path("empty");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    data_art_compact_result_t result;
    ASSERT(data_art_compact(tree, &result), "compact empty");
    ASSERT(result.pages_freed == 0, "freed=%lu", result.pages_freed);

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 4: Compact → insert more → verify growth works ─────────── */

static bool test_compact_then_grow(void) {
    const int N = 2000;
    const char *path = test_path("grow");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    /* Insert N keys */
    uint64_t txn;
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn");
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        data_art_insert(tree, key, KEY_SIZE, val, vlen);
    }
    ASSERT(data_art_commit_txn(tree), "commit");

    /* Delete 70% */
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn 2");
    for (int i = 0; i < N; i++) {
        if (i % 10 < 7) {
            uint8_t key[KEY_SIZE];
            make_key(key, i);
            data_art_delete(tree, key, KEY_SIZE);
        }
    }
    ASSERT(data_art_commit_txn(tree), "commit 2");
    data_art_checkpoint(tree, NULL);

    /* Compact */
    data_art_compact_result_t result;
    ASSERT(data_art_compact(tree, &result), "compact");
    ASSERT(result.pages_freed > 0, "should free pages");

    /* Insert more keys (tests that file growth works after truncation) */
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn 3");
    for (int i = N; i < N + 1000; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        ASSERT(data_art_insert(tree, key, KEY_SIZE, val, vlen), "insert %d", i);
    }
    ASSERT(data_art_commit_txn(tree), "commit 3");

    /* Verify surviving original keys + new keys */
    for (int i = 0; i < N; i++) {
        if (i % 10 >= 7) {
            uint8_t key[KEY_SIZE];
            make_key(key, i);
            size_t vlen;
            const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
            ASSERT(val, "original key %d missing", i);
            free((void *)val);
        }
    }
    for (int i = N; i < N + 1000; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        ASSERT(val, "new key %d missing", i);
        free((void *)val);
    }

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 5: Persistence — compact → checkpoint → close → reopen ── */

static bool test_compact_persistence(void) {
    const int N = 3000;
    const char *path = test_path("persist");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    uint64_t txn;
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn");
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        data_art_insert(tree, key, KEY_SIZE, val, vlen);
    }
    ASSERT(data_art_commit_txn(tree), "commit");

    /* Delete every 3rd key */
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn 2");
    for (int i = 0; i < N; i += 3) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        data_art_delete(tree, key, KEY_SIZE);
    }
    ASSERT(data_art_commit_txn(tree), "commit 2");
    data_art_checkpoint(tree, NULL);

    size_t expected_size = data_art_size(tree);

    /* Compact + checkpoint */
    ASSERT(data_art_compact(tree, NULL), "compact");
    data_art_checkpoint(tree, NULL);

    /* Close */
    data_art_destroy(tree);
    tree = NULL;

    /* Reopen */
    tree = data_art_open(path, KEY_SIZE);
    ASSERT(tree, "reopen after compact");
    ASSERT(data_art_size(tree) == expected_size,
           "size after reopen: %zu (expected %zu)", data_art_size(tree), expected_size);

    /* Verify all surviving keys */
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        if (i % 3 == 0) {
            ASSERT(!val, "key %d should be deleted", i);
        } else {
            ASSERT(val, "key %d missing after reopen", i);
            char expected[64]; size_t elen;
            make_value(expected, &elen, i);
            ASSERT(vlen == elen && memcmp(val, expected, vlen) == 0,
                   "key %d value mismatch", i);
            free((void *)val);
        }
    }

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 6: Iterator works after compaction ──────────────────────── */

static bool test_compact_iterator(void) {
    const int N = 1000;
    const char *path = test_path("iter");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    uint64_t txn;
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn");
    for (int i = 0; i < N; i++) {
        uint8_t key[KEY_SIZE];
        char val[64]; size_t vlen;
        make_key(key, i);
        make_value(val, &vlen, i);
        data_art_insert(tree, key, KEY_SIZE, val, vlen);
    }
    ASSERT(data_art_commit_txn(tree), "commit");

    /* Delete half */
    ASSERT(data_art_begin_txn(tree, &txn), "begin_txn 2");
    for (int i = 0; i < N; i += 2) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        data_art_delete(tree, key, KEY_SIZE);
    }
    ASSERT(data_art_commit_txn(tree), "commit 2");
    data_art_checkpoint(tree, NULL);

    ASSERT(data_art_compact(tree, NULL), "compact");

    /* Iterate and verify sorted order + count */
    data_art_iterator_t *it = data_art_iterator_create(tree);
    ASSERT(it, "iterator_create");

    uint8_t prev_key[KEY_SIZE];
    int count = 0;
    bool first = true;

    while (data_art_iterator_next(it)) {
        size_t klen;
        const uint8_t *key = data_art_iterator_key(it, &klen);
        ASSERT(key && klen == KEY_SIZE, "iter key at %d", count);

        if (!first) {
            ASSERT(memcmp(prev_key, key, KEY_SIZE) < 0, "sort order at %d", count);
        }
        memcpy(prev_key, key, KEY_SIZE);
        first = false;
        count++;
    }
    data_art_iterator_destroy(it);

    ASSERT(count == N / 2, "iterator count %d (expected %d)", count, N / 2);

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Test 7: Large test — 50K keys, heavy deletes, file shrinks ──── */

static bool test_compact_large(void) {
    const int N = 50000;
    const char *path = test_path("large");

    data_art_tree_t *tree = data_art_create(path, KEY_SIZE);
    ASSERT(tree, "create");

    /* Insert in batches of 5000 */
    for (int batch = 0; batch < N; batch += 5000) {
        uint64_t txn;
        ASSERT(data_art_begin_txn(tree, &txn), "begin_txn batch %d", batch);
        for (int i = batch; i < batch + 5000 && i < N; i++) {
            uint8_t key[KEY_SIZE];
            char val[64]; size_t vlen;
            make_key(key, i);
            make_value(val, &vlen, i);
            data_art_insert(tree, key, KEY_SIZE, val, vlen);
        }
        ASSERT(data_art_commit_txn(tree), "commit batch %d", batch);
    }
    data_art_checkpoint(tree, NULL);

    /* Delete 80% */
    for (int batch = 0; batch < N; batch += 5000) {
        uint64_t txn;
        ASSERT(data_art_begin_txn(tree, &txn), "begin_txn del %d", batch);
        for (int i = batch; i < batch + 5000 && i < N; i++) {
            if (i % 5 != 0) {  /* keep every 5th key */
                uint8_t key[KEY_SIZE];
                make_key(key, i);
                data_art_delete(tree, key, KEY_SIZE);
            }
        }
        ASSERT(data_art_commit_txn(tree), "commit del %d", batch);
    }
    data_art_checkpoint(tree, NULL);

    size_t remaining = N / 5;
    ASSERT(data_art_size(tree) == remaining, "size before compact: %zu", data_art_size(tree));

    off_t size_before = file_size(path);

    data_art_compact_result_t result;
    ASSERT(data_art_compact(tree, &result), "compact");

    off_t size_after = file_size(path);
    ASSERT(size_after < size_before,
           "file should shrink: %ld → %ld", (long)size_before, (long)size_after);

    /* Verify file shrunk significantly (>50% for 80% delete) */
    double ratio = (double)size_after / (double)size_before;
    ASSERT(ratio < 0.5,
           "file should shrink >50%%: ratio=%.2f (%ld → %ld)",
           ratio, (long)size_before, (long)size_after);

    printf("(%.1fMB→%.1fMB, %lu nodes relocated) ",
           size_before / (1024.0 * 1024.0),
           size_after / (1024.0 * 1024.0),
           result.nodes_relocated);

    /* Verify surviving keys */
    ASSERT(data_art_size(tree) == remaining, "size after compact: %zu", data_art_size(tree));
    for (int i = 0; i < N; i += 5) {
        uint8_t key[KEY_SIZE];
        make_key(key, i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        ASSERT(val, "key %d missing", i);
        char expected[64]; size_t elen;
        make_value(expected, &elen, i);
        ASSERT(vlen == elen && memcmp(val, expected, vlen) == 0,
               "key %d value mismatch", i);
        free((void *)val);
    }

    data_art_destroy(tree);
    unlink(path);
    return true;
}

/* ── Main ─────────────────────────────────────────────────────────── */

int main(void) {
    printf("\n=== Online File Compaction Tests ===\n\n");

    RUN_TEST(test_compact_basic);
    RUN_TEST(test_compact_noop);
    RUN_TEST(test_compact_empty);
    RUN_TEST(test_compact_then_grow);
    RUN_TEST(test_compact_persistence);
    RUN_TEST(test_compact_iterator);
    RUN_TEST(test_compact_large);

    printf("\n  %d/%d tests passed\n\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
