#include "ih_hash_store.h"
#include "intermediate_hashes.h"  /* ih_cursor_t */
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

static int assertions = 0;
#define ASSERT(cond) do { \
    assertions++; \
    if (!(cond)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        abort(); \
    } \
} while(0)

static void print_hash(const char *label, const hash_t *h) {
    printf("  %s: 0x", label);
    for (int i = 0; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

/* =========================================================================
 * Temp directory helpers
 * ========================================================================= */

static char *make_temp_dir(void) {
    char tmpl[] = "/tmp/test_ihs_XXXXXX";
    char *dir = mkdtemp(tmpl);
    if (!dir) { perror("mkdtemp"); abort(); }
    return strdup(dir);
}

static void remove_temp_dir(const char *dir) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    (void)system(cmd);
}

/* =========================================================================
 * Phase 1: Empty Trie
 * ========================================================================= */

static void test_empty_trie(void) {
    printf("Phase 1: Empty trie root = HASH_EMPTY_STORAGE\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    hash_t root = ihs_root(ih);
    ASSERT(hash_equal(&root, &HASH_EMPTY_STORAGE));

    hash_t built = ihs_build(ih, NULL, NULL, NULL, 0);
    ASSERT(hash_equal(&built, &HASH_EMPTY_STORAGE));

    root = ihs_root(ih);
    ASSERT(hash_equal(&root, &HASH_EMPTY_STORAGE));

    ASSERT(ihs_entry_count(ih) == 0);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions);
}

/* =========================================================================
 * Phase 2: Single Key-Value
 * ========================================================================= */

static void test_single_key(void) {
    int start = assertions;
    printf("Phase 2: Single key-value\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    uint8_t key[32] = {0};
    key[0] = 0xAB; key[1] = 0xCD;

    const uint8_t *keys[] = { key };
    const uint8_t value[] = "hello";
    const uint8_t *values[] = { value };
    uint16_t value_lens[] = { 5 };

    hash_t root = ihs_build(ih, keys, values, value_lens, 1);
    ASSERT(!hash_equal(&root, &HASH_EMPTY_STORAGE));

    hash_t root2 = ihs_build(ih, keys, values, value_lens, 1);
    ASSERT(hash_equal(&root, &root2));

    ASSERT(ihs_entry_count(ih) == 0);

    print_hash("root", &root);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 3: Ethereum Test Vector (variable-length keys)
 * ========================================================================= */

static void test_ethereum_vector(void) {
    int start = assertions;
    printf("Phase 3: Ethereum test vector (doe/dog/dogglesworth)\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    const uint8_t *keys[] = {
        (const uint8_t *)"doe",
        (const uint8_t *)"dog",
        (const uint8_t *)"dogglesworth",
    };
    size_t key_lens[] = { 3, 3, 12 };

    const uint8_t *values[] = {
        (const uint8_t *)"reindeer",
        (const uint8_t *)"puppy",
        (const uint8_t *)"cat",
    };
    size_t value_lens[] = { 8, 5, 3 };

    hash_t root = ihs_build_varlen(ih, keys, key_lens, values, value_lens, 3);

    hash_t expected;
    bool ok = hash_from_hex(
        "0x8aad789dff2f538bca5d8ea56e8abe10f4c7ba3a5dea95fea4cd6e7c3a1168d3",
        &expected);
    ASSERT(ok);

    print_hash("expected", &expected);
    print_hash("got     ", &root);

    ASSERT(hash_equal(&root, &expected));

    size_t count = ihs_entry_count(ih);
    printf("  branch entries: %zu\n", count);
    ASSERT(count > 0);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 4: Multiple 32-byte Keys
 * ========================================================================= */

static void test_multiple_keys(void) {
    int start = assertions;
    printf("Phase 4: Multiple 32-byte keys\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    uint8_t key_data[5][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x11;
    key_data[1][0] = 0x22;
    key_data[2][0] = 0x33;
    key_data[3][0] = 0x44;
    key_data[4][0] = 0x55;

    const uint8_t *keys[5];
    const uint8_t *values[5];
    uint16_t value_lens[5];

    const char *vals[] = { "alpha", "beta", "gamma", "delta", "epsilon" };
    for (int i = 0; i < 5; i++) {
        keys[i] = key_data[i];
        values[i] = (const uint8_t *)vals[i];
        value_lens[i] = (uint16_t)strlen(vals[i]);
    }

    hash_t root = ihs_build(ih, keys, values, value_lens, 5);
    ASSERT(!hash_equal(&root, &HASH_EMPTY_STORAGE));

    size_t count = ihs_entry_count(ih);
    printf("  branch entries: %zu\n", count);
    ASSERT(count > 0);

    print_hash("root", &root);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 5: Rebuild Determinism
 * ========================================================================= */

static void test_rebuild_determinism(void) {
    int start = assertions;
    printf("Phase 5: Rebuild produces same root\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    uint8_t key_data[3][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0xAA; key_data[0][1] = 0x11;
    key_data[1][0] = 0xAA; key_data[1][1] = 0x22;
    key_data[2][0] = 0xBB; key_data[2][1] = 0x33;

    const uint8_t *keys[3];
    const uint8_t *values[3];
    uint16_t value_lens[3];

    const char *vals[] = { "one", "two", "three" };
    for (int i = 0; i < 3; i++) {
        keys[i] = key_data[i];
        values[i] = (const uint8_t *)vals[i];
        value_lens[i] = (uint16_t)strlen(vals[i]);
    }

    hash_t root1 = ihs_build(ih, keys, values, value_lens, 3);
    size_t count1 = ihs_entry_count(ih);

    hash_t root2 = ihs_build(ih, keys, values, value_lens, 3);
    size_t count2 = ihs_entry_count(ih);

    ASSERT(hash_equal(&root1, &root2));
    ASSERT(count1 == count2);

    /* Build with a separate state */
    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    ASSERT(ih2 != NULL);
    hash_t root3 = ihs_build(ih2, keys, values, value_lens, 3);
    ASSERT(hash_equal(&root1, &root3));

    print_hash("root", &root1);
    printf("  branch entries: %zu\n", count1);

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 6: Shared Prefix
 * ========================================================================= */

static void test_shared_prefix(void) {
    int start = assertions;
    printf("Phase 6: Shared prefix keys (nibble-level branching)\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    uint8_t key_data[2][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x3A;
    key_data[1][0] = 0x3B;

    const uint8_t *keys[2] = { key_data[0], key_data[1] };
    const uint8_t value_a[] = "val_a";
    const uint8_t value_b[] = "val_b";
    const uint8_t *values[2] = { value_a, value_b };
    uint16_t value_lens[2] = { 5, 5 };

    hash_t root = ihs_build(ih, keys, values, value_lens, 2);
    ASSERT(!hash_equal(&root, &HASH_EMPTY_STORAGE));

    size_t count = ihs_entry_count(ih);
    printf("  branch entries: %zu\n", count);
    ASSERT(count > 0);

    print_hash("root", &root);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 7: Single variable-length key
 * ========================================================================= */

static void test_single_varlen(void) {
    int start = assertions;
    printf("Phase 7: Single variable-length key\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    const uint8_t *keys[] = { (const uint8_t *)"a" };
    size_t key_lens[] = { 1 };
    const uint8_t *values[] = { (const uint8_t *)"b" };
    size_t value_lens[] = { 1 };

    hash_t root = ihs_build_varlen(ih, keys, key_lens, values, value_lens, 1);
    ASSERT(!hash_equal(&root, &HASH_EMPTY_STORAGE));
    ASSERT(ihs_entry_count(ih) == 0);

    hash_t root2 = ihs_build_varlen(ih, keys, key_lens, values, value_lens, 1);
    ASSERT(hash_equal(&root, &root2));

    print_hash("root", &root);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 8: Two variable-length keys
 * ========================================================================= */

static void test_two_varlen(void) {
    int start = assertions;
    printf("Phase 8: Two variable-length keys (prefix relationship)\n");

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    const uint8_t *keys[] = {
        (const uint8_t *)"do",
        (const uint8_t *)"dog",
    };
    size_t key_lens[] = { 2, 3 };
    const uint8_t *values[] = {
        (const uint8_t *)"verb",
        (const uint8_t *)"puppy",
    };
    size_t value_lens[] = { 4, 5 };

    hash_t root = ihs_build_varlen(ih, keys, key_lens, values, value_lens, 2);
    ASSERT(!hash_equal(&root, &HASH_EMPTY_STORAGE));

    size_t count = ihs_entry_count(ih);
    printf("  branch entries: %zu\n", count);
    ASSERT(count > 0);

    hash_t root2 = ihs_build_varlen(ih, keys, key_lens, values, value_lens, 2);
    ASSERT(hash_equal(&root, &root2));

    print_hash("root", &root);

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Mock Cursor (array-backed, for ihs_update tests)
 * ========================================================================= */

typedef struct {
    const uint8_t *const *keys;
    const uint8_t *const *vals;
    const size_t *vlens;
    size_t count;
    size_t pos;
} mock_cursor_ctx_t;

static bool mock_seek(void *ctx, const uint8_t *seek_key, size_t key_len) {
    mock_cursor_ctx_t *mc = ctx;
    (void)key_len;
    size_t lo = 0, hi = mc->count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (memcmp(mc->keys[mid], seek_key, 32) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    mc->pos = lo;
    return lo < mc->count;
}

static bool mock_next(void *ctx) {
    mock_cursor_ctx_t *mc = ctx;
    if (mc->pos < mc->count) mc->pos++;
    return mc->pos < mc->count;
}

static bool mock_valid(void *ctx) {
    mock_cursor_ctx_t *mc = ctx;
    return mc->pos < mc->count;
}

static const uint8_t *mock_key(void *ctx, size_t *out_len) {
    mock_cursor_ctx_t *mc = ctx;
    if (mc->pos >= mc->count) return NULL;
    *out_len = 32;
    return mc->keys[mc->pos];
}

static const uint8_t *mock_value(void *ctx, size_t *out_len) {
    mock_cursor_ctx_t *mc = ctx;
    if (mc->pos >= mc->count) return NULL;
    *out_len = mc->vlens[mc->pos];
    return mc->vals[mc->pos];
}

static ih_cursor_t make_mock_cursor(mock_cursor_ctx_t *mc) {
    ih_cursor_t c;
    c.ctx = mc;
    c.seek = mock_seek;
    c.next = mock_next;
    c.valid = mock_valid;
    c.key = mock_key;
    c.value = mock_value;
    return c;
}

/* =========================================================================
 * Phase 9: ihs_update — single value update
 * ========================================================================= */

static void test_update_single_value(void) {
    int start = assertions;
    printf("Phase 9: ihs_update — single value update\n");

    uint8_t key_data[5][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x11;
    key_data[1][0] = 0x22;
    key_data[2][0] = 0x33;
    key_data[3][0] = 0x44;
    key_data[4][0] = 0x55;

    const uint8_t *keys[5];
    const char *vals_orig[] = { "alpha", "beta", "gamma", "delta", "epsilon" };
    const uint8_t *values[5];
    uint16_t value_lens[5];
    size_t value_lens_sz[5];

    for (int i = 0; i < 5; i++) {
        keys[i] = key_data[i];
        values[i] = (const uint8_t *)vals_orig[i];
        value_lens[i] = (uint16_t)strlen(vals_orig[i]);
        value_lens_sz[i] = strlen(vals_orig[i]);
    }

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ASSERT(ih != NULL);

    hash_t root_before = ihs_build(ih, keys, values, value_lens, 5);
    ASSERT(!hash_equal(&root_before, &HASH_EMPTY_STORAGE));

    const uint8_t new_val[] = "GAMMA";
    const uint8_t *dirty_keys[] = { key_data[2] };
    const uint8_t *dirty_vals[] = { new_val };
    size_t dirty_vlens[] = { 5 };

    const uint8_t *cursor_vals[5];
    size_t cursor_vlens[5];
    for (int i = 0; i < 5; i++) {
        cursor_vals[i] = values[i];
        cursor_vlens[i] = value_lens_sz[i];
    }
    cursor_vals[2] = new_val;
    cursor_vlens[2] = 5;

    mock_cursor_ctx_t mc = { keys, cursor_vals, cursor_vlens, 5, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 1, &cursor);

    /* Compare against full rebuild */
    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    const uint8_t *all_vals[5];
    uint16_t all_vlens[5];
    for (int i = 0; i < 5; i++) {
        all_vals[i] = cursor_vals[i];
        all_vlens[i] = (uint16_t)cursor_vlens[i];
    }
    hash_t root_rebuild = ihs_build(ih2, keys, all_vals, all_vlens, 5);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);

    ASSERT(hash_equal(&root_update, &root_rebuild));
    ASSERT(!hash_equal(&root_update, &root_before));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 10: ihs_update — multiple value updates
 * ========================================================================= */

static void test_update_multiple_values(void) {
    int start = assertions;
    printf("Phase 10: ihs_update — multiple value updates\n");

    uint8_t key_data[10][32];
    memset(key_data, 0, sizeof(key_data));
    for (int i = 0; i < 10; i++)
        key_data[i][0] = (uint8_t)((i + 1) * 0x11);

    const uint8_t *keys[10];
    const uint8_t *values[10];
    uint16_t value_lens[10];
    char val_bufs[10][16];

    for (int i = 0; i < 10; i++) {
        keys[i] = key_data[i];
        snprintf(val_bufs[i], 16, "val_%d", i);
        values[i] = (const uint8_t *)val_bufs[i];
        value_lens[i] = (uint16_t)strlen(val_bufs[i]);
    }

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys, values, value_lens, 10);

    char new_bufs[3][16];
    snprintf(new_bufs[0], 16, "NEW_1");
    snprintf(new_bufs[1], 16, "NEW_4");
    snprintf(new_bufs[2], 16, "NEW_7");

    const uint8_t *dirty_keys[3] = { key_data[1], key_data[4], key_data[7] };
    const uint8_t *dirty_vals[3] = {
        (const uint8_t *)new_bufs[0],
        (const uint8_t *)new_bufs[1],
        (const uint8_t *)new_bufs[2],
    };
    size_t dirty_vlens[3] = { 5, 5, 5 };

    const uint8_t *cursor_vals[10];
    size_t cursor_vlens[10];
    for (int i = 0; i < 10; i++) {
        cursor_vals[i] = values[i];
        cursor_vlens[i] = value_lens[i];
    }
    cursor_vals[1] = dirty_vals[0]; cursor_vlens[1] = 5;
    cursor_vals[4] = dirty_vals[1]; cursor_vlens[4] = 5;
    cursor_vals[7] = dirty_vals[2]; cursor_vlens[7] = 5;

    mock_cursor_ctx_t mc = { keys, cursor_vals, cursor_vlens, 10, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 3, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t all_vlens[10];
    for (int i = 0; i < 10; i++) all_vlens[i] = (uint16_t)cursor_vlens[i];
    hash_t root_rebuild = ihs_build(ih2, keys, cursor_vals, all_vlens, 10);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 11: ihs_update — insert new key
 * ========================================================================= */

static void test_update_insert(void) {
    int start = assertions;
    printf("Phase 11: ihs_update — insert new key\n");

    uint8_t key_data[4][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x11;
    key_data[1][0] = 0x33;
    key_data[2][0] = 0x55;
    key_data[3][0] = 0x22;

    const uint8_t *keys3[3] = { key_data[0], key_data[1], key_data[2] };
    const uint8_t *vals3[] = { (const uint8_t *)"aaa", (const uint8_t *)"bbb", (const uint8_t *)"ccc" };
    uint16_t vlens3[] = { 3, 3, 3 };

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys3, vals3, vlens3, 3);

    const uint8_t *dirty_keys[] = { key_data[3] };
    const uint8_t *dirty_vals[] = { (const uint8_t *)"ddd" };
    size_t dirty_vlens[] = { 3 };

    const uint8_t *keys4[4] = { key_data[0], key_data[3], key_data[1], key_data[2] };
    const uint8_t *vals4[] = {
        (const uint8_t *)"aaa", (const uint8_t *)"ddd",
        (const uint8_t *)"bbb", (const uint8_t *)"ccc"
    };
    size_t vlens4[] = { 3, 3, 3, 3 };

    mock_cursor_ctx_t mc = { keys4, vals4, vlens4, 4, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 1, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t vlens4_u16[] = { 3, 3, 3, 3 };
    hash_t root_rebuild = ihs_build(ih2, keys4, vals4, vlens4_u16, 4);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 12: ihs_update — insert into empty branch slot
 * ========================================================================= */

static void test_update_insert_new_slot(void) {
    int start = assertions;
    printf("Phase 12: ihs_update — insert into empty branch slot\n");

    uint8_t key_data[3][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x3A;
    key_data[1][0] = 0x3B;
    key_data[2][0] = 0x3C;

    const uint8_t *keys2[2] = { key_data[0], key_data[1] };
    const uint8_t *vals2[] = { (const uint8_t *)"aa", (const uint8_t *)"bb" };
    uint16_t vlens2[] = { 2, 2 };

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys2, vals2, vlens2, 2);

    const uint8_t *dirty_keys[] = { key_data[2] };
    const uint8_t *dirty_vals[] = { (const uint8_t *)"cc" };
    size_t dirty_vlens[] = { 2 };

    const uint8_t *keys3[3] = { key_data[0], key_data[1], key_data[2] };
    const uint8_t *vals3[] = {
        (const uint8_t *)"aa", (const uint8_t *)"bb", (const uint8_t *)"cc"
    };
    size_t vlens3[] = { 2, 2, 2 };

    mock_cursor_ctx_t mc = { keys3, vals3, vlens3, 3, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 1, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t vlens3_u16[] = { 2, 2, 2 };
    hash_t root_rebuild = ihs_build(ih2, keys3, vals3, vlens3_u16, 3);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 13: ihs_update — insert splitting extension
 * ========================================================================= */

static void test_update_extension_split(void) {
    int start = assertions;
    printf("Phase 13: ihs_update — insert splitting extension\n");

    uint8_t key_data[3][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x3A;
    key_data[1][0] = 0x3B;
    key_data[2][0] = 0x40;

    const uint8_t *keys2[2] = { key_data[0], key_data[1] };
    const uint8_t *vals2[] = { (const uint8_t *)"aa", (const uint8_t *)"bb" };
    uint16_t vlens2[] = { 2, 2 };

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys2, vals2, vlens2, 2);

    const uint8_t *dirty_keys[] = { key_data[2] };
    const uint8_t *dirty_vals[] = { (const uint8_t *)"cc" };
    size_t dirty_vlens[] = { 2 };

    const uint8_t *keys3[3] = { key_data[0], key_data[1], key_data[2] };
    const uint8_t *vals3[] = {
        (const uint8_t *)"aa", (const uint8_t *)"bb", (const uint8_t *)"cc"
    };
    size_t vlens3[] = { 2, 2, 2 };

    mock_cursor_ctx_t mc = { keys3, vals3, vlens3, 3, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 1, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t vlens3_u16[] = { 2, 2, 2 };
    hash_t root_rebuild = ihs_build(ih2, keys3, vals3, vlens3_u16, 3);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 14: ihs_update — sequential blocks
 * ========================================================================= */

static void test_update_sequential_blocks(void) {
    int start = assertions;
    printf("Phase 14: ihs_update — sequential blocks\n");

    uint8_t key_data[6][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x11;
    key_data[1][0] = 0x22;
    key_data[2][0] = 0x33;
    key_data[3][0] = 0x44;
    key_data[4][0] = 0x55;
    key_data[5][0] = 0x66;

    const char *v[] = { "a1", "b2", "c3", "d4", "e5" };
    const uint8_t *keys[5];
    const uint8_t *values[5];
    uint16_t vlens[5];
    for (int i = 0; i < 5; i++) {
        keys[i] = key_data[i];
        values[i] = (const uint8_t *)v[i];
        vlens[i] = 2;
    }

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys, values, vlens, 5);

    /* Block 1: update keys[0] and keys[3] */
    {
        const uint8_t *dk[] = { key_data[0], key_data[3] };
        const uint8_t *dv[] = { (const uint8_t *)"A1", (const uint8_t *)"D4" };
        size_t dvl[] = { 2, 2 };

        const uint8_t *cv[5];
        size_t cvl[5];
        for (int i = 0; i < 5; i++) { cv[i] = values[i]; cvl[i] = 2; }
        cv[0] = dv[0]; cv[3] = dv[1];

        mock_cursor_ctx_t mc = { keys, cv, cvl, 5, 0 };
        ih_cursor_t cursor = make_mock_cursor(&mc);
        hash_t r1 = ihs_update(ih, dk, dv, dvl, 2, &cursor);

        char *rdir = make_temp_dir();
        ihs_state_t *ref = ihs_create(rdir);
        uint16_t rvl[5] = {2,2,2,2,2};
        hash_t r1_ref = ihs_build(ref, keys, cv, rvl, 5);
        ASSERT(hash_equal(&r1, &r1_ref));
        ihs_destroy(ref);
        remove_temp_dir(rdir);
        free(rdir);

        values[0] = dv[0];
        values[3] = dv[1];
    }

    /* Block 2: insert key[5] = 0x66 */
    {
        const uint8_t *dk[] = { key_data[5] };
        const uint8_t *dv[] = { (const uint8_t *)"f6" };
        size_t dvl[] = { 2 };

        const uint8_t *ck[6];
        const uint8_t *cv[6];
        size_t cvl[6];
        for (int i = 0; i < 5; i++) {
            ck[i] = key_data[i]; cv[i] = values[i]; cvl[i] = 2;
        }
        ck[5] = key_data[5]; cv[5] = dv[0]; cvl[5] = 2;

        mock_cursor_ctx_t mc = { ck, cv, cvl, 6, 0 };
        ih_cursor_t cursor = make_mock_cursor(&mc);
        hash_t r2 = ihs_update(ih, dk, dv, dvl, 1, &cursor);

        char *rdir = make_temp_dir();
        ihs_state_t *ref = ihs_create(rdir);
        uint16_t rvl[6] = {2,2,2,2,2,2};
        hash_t r2_ref = ihs_build(ref, ck, cv, rvl, 6);

        print_hash("block2 update ", &r2);
        print_hash("block2 rebuild", &r2_ref);
        ASSERT(hash_equal(&r2, &r2_ref));
        ihs_destroy(ref);
        remove_temp_dir(rdir);
        free(rdir);
    }

    ihs_destroy(ih);
    remove_temp_dir(dir);
    free(dir);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 15: ihs_update — all keys dirty
 * ========================================================================= */

static void test_update_all_dirty(void) {
    int start = assertions;
    printf("Phase 15: ihs_update — all keys dirty\n");

    uint8_t key_data[5][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0x11;
    key_data[1][0] = 0x22;
    key_data[2][0] = 0x33;
    key_data[3][0] = 0x44;
    key_data[4][0] = 0x55;

    const uint8_t *keys[5];
    const uint8_t *orig_vals[5];
    uint16_t orig_vlens[5];
    for (int i = 0; i < 5; i++) {
        keys[i] = key_data[i];
        orig_vals[i] = (const uint8_t *)"old";
        orig_vlens[i] = 3;
    }

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys, orig_vals, orig_vlens, 5);

    const uint8_t *dirty_keys[5];
    const uint8_t *dirty_vals[5];
    size_t dirty_vlens[5];
    char new_bufs[5][8];
    for (int i = 0; i < 5; i++) {
        dirty_keys[i] = key_data[i];
        snprintf(new_bufs[i], 8, "new_%d", i);
        dirty_vals[i] = (const uint8_t *)new_bufs[i];
        dirty_vlens[i] = strlen(new_bufs[i]);
    }

    size_t cursor_vlens[5];
    for (int i = 0; i < 5; i++) cursor_vlens[i] = dirty_vlens[i];
    mock_cursor_ctx_t mc = { keys, dirty_vals, cursor_vlens, 5, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dirty_keys, dirty_vals, dirty_vlens, 5, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t new_vlens[5];
    for (int i = 0; i < 5; i++) new_vlens[i] = (uint16_t)dirty_vlens[i];
    hash_t root_rebuild = ihs_build(ih2, keys, dirty_vals, new_vlens, 5);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Phase 16: ihs_update — shared prefix, deep branch
 * ========================================================================= */

static void test_update_shared_prefix(void) {
    int start = assertions;
    printf("Phase 16: ihs_update — shared prefix, deep branch\n");

    uint8_t key_data[4][32];
    memset(key_data, 0, sizeof(key_data));
    key_data[0][0] = 0xAA; key_data[0][1] = 0x11;
    key_data[1][0] = 0xAA; key_data[1][1] = 0x22;
    key_data[2][0] = 0xAA; key_data[2][1] = 0x33;
    key_data[3][0] = 0xBB; key_data[3][1] = 0x11;

    const uint8_t *keys[4];
    const uint8_t *values[4];
    uint16_t vlens[4];
    for (int i = 0; i < 4; i++) {
        keys[i] = key_data[i];
        values[i] = (const uint8_t *)"val";
        vlens[i] = 3;
    }

    char *dir = make_temp_dir();
    ihs_state_t *ih = ihs_create(dir);
    ihs_build(ih, keys, values, vlens, 4);

    const uint8_t *dk[] = { key_data[1] };
    const uint8_t *dv[] = { (const uint8_t *)"NEW" };
    size_t dvl[] = { 3 };

    const uint8_t *cv[4];
    size_t cvl[4];
    for (int i = 0; i < 4; i++) { cv[i] = values[i]; cvl[i] = 3; }
    cv[1] = dv[0];

    mock_cursor_ctx_t mc = { keys, cv, cvl, 4, 0 };
    ih_cursor_t cursor = make_mock_cursor(&mc);

    hash_t root_update = ihs_update(ih, dk, dv, dvl, 1, &cursor);

    char *dir2 = make_temp_dir();
    ihs_state_t *ih2 = ihs_create(dir2);
    uint16_t rvl[4] = {3,3,3,3};
    hash_t root_rebuild = ihs_build(ih2, keys, cv, rvl, 4);

    print_hash("update ", &root_update);
    print_hash("rebuild", &root_rebuild);
    ASSERT(hash_equal(&root_update, &root_rebuild));

    ihs_destroy(ih);
    ihs_destroy(ih2);
    remove_temp_dir(dir);
    remove_temp_dir(dir2);
    free(dir);
    free(dir2);
    printf("  PASS (%d assertions)\n\n", assertions - start);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== IH Hash Store Tests ===\n\n");

    test_empty_trie();
    test_single_key();
    test_ethereum_vector();
    test_multiple_keys();
    test_rebuild_determinism();
    test_shared_prefix();
    test_single_varlen();
    test_two_varlen();

    /* ihs_update tests */
    test_update_single_value();
    test_update_multiple_values();
    test_update_insert();
    test_update_insert_new_slot();
    test_update_extension_split();
    test_update_sequential_blocks();
    test_update_all_dirty();
    test_update_shared_prefix();

    printf("=== ALL PHASES PASSED (%d total assertions) ===\n", assertions);
    return 0;
}
