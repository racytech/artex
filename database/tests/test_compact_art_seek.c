#include "compact_art.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int assertions = 0;
#define ASSERT(cond) do { \
    assertions++; \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        abort(); \
    } \
} while(0)

#define KEY_SIZE 4
#define VAL_SIZE 4

static void make_key(uint32_t v, uint8_t out[KEY_SIZE]) {
    out[0] = (v >> 24) & 0xFF;
    out[1] = (v >> 16) & 0xFF;
    out[2] = (v >> 8) & 0xFF;
    out[3] = v & 0xFF;
}

static uint32_t read_key_u32(const uint8_t *k) {
    return ((uint32_t)k[0] << 24) | ((uint32_t)k[1] << 16) |
           ((uint32_t)k[2] << 8) | k[3];
}

// ============================================================================
// Phase 1: Seek on empty tree
// ============================================================================

static void test_empty_tree(void) {
    int start = assertions;
    printf("Phase 1: Seek on empty tree\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);
    ASSERT(compact_art_size(&tree) == 0);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);
    ASSERT(iter != NULL);

    uint8_t key[KEY_SIZE];
    make_key(42, key);
    ASSERT(!compact_art_iterator_seek(iter, key));
    ASSERT(compact_art_iterator_done(iter));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 2: Seek exact match
// ============================================================================

static void test_seek_exact(void) {
    int start = assertions;
    printf("Phase 2: Seek to existing key (exact match)\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    // Insert keys 10, 20, 30, 40, 50
    uint32_t vals[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        uint8_t k[KEY_SIZE];
        make_key(vals[i], k);
        compact_art_insert(&tree, k, &vals[i]);
    }
    ASSERT(compact_art_size(&tree) == 5);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to key 30 (exists)
    uint8_t seek_key[KEY_SIZE];
    make_key(30, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));

    // First next() should return key 30
    ASSERT(compact_art_iterator_next(iter));
    const uint8_t *k = compact_art_iterator_key(iter);
    ASSERT(k != NULL);
    ASSERT(read_key_u32(k) == 30);

    // Next should return 40
    ASSERT(compact_art_iterator_next(iter));
    k = compact_art_iterator_key(iter);
    ASSERT(read_key_u32(k) == 40);

    // Next should return 50
    ASSERT(compact_art_iterator_next(iter));
    k = compact_art_iterator_key(iter);
    ASSERT(read_key_u32(k) == 50);

    // No more
    ASSERT(!compact_art_iterator_next(iter));
    ASSERT(compact_art_iterator_done(iter));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 3: Seek to non-existing key (lands on next)
// ============================================================================

static void test_seek_between(void) {
    int start = assertions;
    printf("Phase 3: Seek to non-existing key (lands on next)\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    uint32_t vals[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        uint8_t k[KEY_SIZE];
        make_key(vals[i], k);
        compact_art_insert(&tree, k, &vals[i]);
    }

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to 25 (between 20 and 30)
    uint8_t seek_key[KEY_SIZE];
    make_key(25, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));

    // Should land on 30
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 30);

    // Continue: 40, 50
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 40);
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 50);
    ASSERT(!compact_art_iterator_next(iter));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 4: Seek past all keys
// ============================================================================

static void test_seek_past_end(void) {
    int start = assertions;
    printf("Phase 4: Seek past all keys\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    uint32_t vals[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        uint8_t k[KEY_SIZE];
        make_key(vals[i], k);
        compact_art_insert(&tree, k, &vals[i]);
    }

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to 100 (past all keys)
    uint8_t seek_key[KEY_SIZE];
    make_key(100, seek_key);
    ASSERT(!compact_art_iterator_seek(iter, seek_key));
    ASSERT(compact_art_iterator_done(iter));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 5: Seek before all keys
// ============================================================================

static void test_seek_before_start(void) {
    int start = assertions;
    printf("Phase 5: Seek before all keys (returns first)\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    uint32_t vals[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        uint8_t k[KEY_SIZE];
        make_key(vals[i], k);
        compact_art_insert(&tree, k, &vals[i]);
    }

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to 1 (before all keys)
    uint8_t seek_key[KEY_SIZE];
    make_key(1, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));

    // Should return 10 (first key)
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 10);

    // Continue: 20, 30
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 20);
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 30);
    ASSERT(!compact_art_iterator_next(iter));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 6: Multiple seeks on same iterator
// ============================================================================

static void test_multiple_seeks(void) {
    int start = assertions;
    printf("Phase 6: Multiple seeks on same iterator\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    for (uint32_t i = 0; i < 10; i++) {
        uint32_t v = (i + 1) * 100;
        uint8_t k[KEY_SIZE];
        make_key(v, k);
        compact_art_insert(&tree, k, &v);
    }

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to 500
    uint8_t seek_key[KEY_SIZE];
    make_key(500, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 500);

    // Re-seek to 200
    make_key(200, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 200);

    // Re-seek to 950 (between 900 and 1000)
    make_key(950, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 1000);

    // Re-seek past end
    make_key(2000, seek_key);
    ASSERT(!compact_art_iterator_seek(iter, seek_key));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 7: Seek with 32-byte keys (realistic)
// ============================================================================

static void test_seek_32byte_keys(void) {
    int start = assertions;
    printf("Phase 7: Seek with 32-byte keys\n");

    compact_art_t tree;
    compact_art_init(&tree, 32, 4);

    // Insert 100 keys with first byte varying
    uint8_t keys[100][32];
    for (int i = 0; i < 100; i++) {
        memset(keys[i], 0, 32);
        keys[i][0] = (uint8_t)(i * 2 + 10);  // 10, 12, 14, ..., 208
        uint32_t val = i;
        compact_art_insert(&tree, keys[i], &val);
    }
    ASSERT(compact_art_size(&tree) == 100);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to key with first byte = 50 (exists at index 20)
    uint8_t seek[32];
    memset(seek, 0, 32);
    seek[0] = 50;
    ASSERT(compact_art_iterator_seek(iter, seek));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(compact_art_iterator_key(iter)[0] == 50);

    // Seek to key with first byte = 51 (doesn't exist, next is 52)
    seek[0] = 51;
    ASSERT(compact_art_iterator_seek(iter, seek));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(compact_art_iterator_key(iter)[0] == 52);

    // Seek to key with first byte = 0 (before all)
    seek[0] = 0;
    ASSERT(compact_art_iterator_seek(iter, seek));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(compact_art_iterator_key(iter)[0] == 10);

    // Seek to key with first byte = 255 (past all, max is 208)
    seek[0] = 255;
    ASSERT(!compact_art_iterator_seek(iter, seek));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 8: Seek + iterate verifies sorted order
// ============================================================================

static void test_seek_then_iterate_all(void) {
    int start = assertions;
    printf("Phase 8: Seek to first key then iterate all\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    // Insert 1000 keys: 1, 2, 3, ..., 1000
    for (uint32_t i = 1; i <= 1000; i++) {
        uint8_t k[KEY_SIZE];
        make_key(i, k);
        compact_art_insert(&tree, k, &i);
    }
    ASSERT(compact_art_size(&tree) == 1000);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to key 500
    uint8_t seek_key[KEY_SIZE];
    make_key(500, seek_key);
    ASSERT(compact_art_iterator_seek(iter, seek_key));

    // Iterate remaining: should get 500, 501, ..., 1000
    uint32_t count = 0;
    uint32_t prev = 499;
    while (compact_art_iterator_next(iter)) {
        const uint8_t *k = compact_art_iterator_key(iter);
        uint32_t v = read_key_u32(k);
        ASSERT(v > prev);  // sorted order
        prev = v;
        count++;
    }
    ASSERT(count == 501);  // 500 through 1000 inclusive

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 9: Stress test — random keys, random seeks
// ============================================================================

// Simple deterministic PRNG
static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t va = *(const uint32_t *)a;
    uint32_t vb = *(const uint32_t *)b;
    if (va < vb) return -1;
    if (va > vb) return 1;
    return 0;
}

static void test_stress(void) {
    int start = assertions;
    printf("Phase 9: Stress test (100K keys, 10K seeks)\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    // Insert 100K unique random keys
    uint32_t rng = 12345;
    size_t N = 100000;
    uint32_t *all_keys = malloc(N * sizeof(uint32_t));
    ASSERT(all_keys != NULL);

    // Generate unique keys using a hash-like spread
    for (size_t i = 0; i < N; i++) {
        all_keys[i] = xorshift32(&rng);
    }

    // Remove duplicates by sorting and deduplicating
    qsort(all_keys, N, sizeof(uint32_t), cmp_u32);
    size_t unique = 1;
    for (size_t i = 1; i < N; i++) {
        if (all_keys[i] != all_keys[i - 1]) {
            all_keys[unique++] = all_keys[i];
        }
    }
    N = unique;

    // Insert all
    for (size_t i = 0; i < N; i++) {
        uint8_t k[KEY_SIZE];
        make_key(all_keys[i], k);
        uint32_t val = (uint32_t)i;
        compact_art_insert(&tree, k, &val);
    }
    ASSERT(compact_art_size(&tree) == N);
    printf("  inserted %zu unique keys\n", N);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Perform 10K random seeks and verify
    size_t num_seeks = 10000;
    rng = 99999;
    for (size_t s = 0; s < num_seeks; s++) {
        uint32_t target = xorshift32(&rng);
        uint8_t seek_key[KEY_SIZE];
        make_key(target, seek_key);

        // Find expected: first all_keys[i] >= target (binary search)
        size_t lo = 0, hi = N;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (all_keys[mid] < target)
                lo = mid + 1;
            else
                hi = mid;
        }

        bool found = compact_art_iterator_seek(iter, seek_key);

        if (lo >= N) {
            // No key >= target
            ASSERT(!found);
        } else {
            ASSERT(found);
            ASSERT(compact_art_iterator_next(iter));
            const uint8_t *k = compact_art_iterator_key(iter);
            uint32_t got = read_key_u32(k);
            ASSERT(got == all_keys[lo]);
        }
    }

    printf("  verified %zu seeks\n", num_seeks);

    compact_art_iterator_destroy(iter);
    free(all_keys);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Phase 10: Single-element tree edge cases
// ============================================================================

static void test_single_element(void) {
    int start = assertions;
    printf("Phase 10: Single-element tree\n");

    compact_art_t tree;
    compact_art_init(&tree, KEY_SIZE, VAL_SIZE);

    uint32_t v = 42;
    uint8_t k[KEY_SIZE];
    make_key(42, k);
    compact_art_insert(&tree, k, &v);

    compact_art_iterator_t *iter = compact_art_iterator_create(&tree);

    // Seek to exact key
    ASSERT(compact_art_iterator_seek(iter, k));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 42);
    ASSERT(!compact_art_iterator_next(iter));

    // Seek before
    uint8_t before[KEY_SIZE];
    make_key(10, before);
    ASSERT(compact_art_iterator_seek(iter, before));
    ASSERT(compact_art_iterator_next(iter));
    ASSERT(read_key_u32(compact_art_iterator_key(iter)) == 42);

    // Seek after
    uint8_t after[KEY_SIZE];
    make_key(100, after);
    ASSERT(!compact_art_iterator_seek(iter, after));

    compact_art_iterator_destroy(iter);
    compact_art_destroy(&tree);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== Compact ART Iterator Seek Tests ===\n\n");

    test_empty_tree();
    test_seek_exact();
    test_seek_between();
    test_seek_past_end();
    test_seek_before_start();
    test_multiple_seeks();
    test_seek_32byte_keys();
    test_seek_then_iterate_all();
    test_stress();
    test_single_element();

    printf("=== ALL PHASES PASSED (%d total assertions) ===\n", assertions);
    return 0;
}
