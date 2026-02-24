/**
 * Test: data_art - Node Growth Operations
 *
 * Tests node capacity transitions: 4->16->48->256
 */

#include "data_art.h"
#include "buffer_pool.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <time.h>

#define TEST_DB_PATH "/tmp/test_data_art_growth.db"

// Test counter
static int tests_run = 0;
static int tests_passed = 0;

// Remove test database directory
static void cleanup_test_db(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
}

// Test result macro
#define TEST(name) \
    do { \
        printf("TEST: %s ... ", name); \
        fflush(stdout); \
        tests_run++; \
        cleanup_test_db(); \
    } while (0)

#define PASS() \
    do { \
        printf("PASS\n"); \
        tests_passed++; \
    } while (0)

#define FAIL(msg) \
    do { \
        printf("FAIL: %s\n", msg); \
        return false; \
    } while (0)

// Helper: create a 32-byte key from a string (zero-padded)
static void make_key32(uint8_t key_out[32], const char *str) {
    memset(key_out, 0, 32);
    size_t len = strlen(str);
    if (len > 32) len = 32;
    memcpy(key_out, str, len);
}

/**
 * Test: Insert 5 keys to trigger NODE_4 -> NODE_16 growth
 */
static bool test_node4_to_node16_growth(void) {
    TEST("NODE_4 grows to NODE_16 after 4 children");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    // Insert 5 keys (should trigger NODE_4 -> NODE_16)
    const char *key_strings[] = {
        "prefix_a", "prefix_b", "prefix_c", "prefix_d", "prefix_e"
    };

    for (int i = 0; i < 5; i++) {
        uint8_t key[32];
        make_key32(key, key_strings[i]);

        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to insert key");
        }
    }

    // Verify all keys can be retrieved
    for (int i = 0; i < 5; i++) {
        uint8_t key[32];
        make_key32(key, key_strings[i]);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);
        if (!retrieved) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to retrieve key");
        }

        char expected[32];
        snprintf(expected, sizeof(expected), "value_%d", i);

        if (value_len != strlen(expected) || memcmp(retrieved, expected, value_len) != 0) {
            free(retrieved);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieved value doesn't match");
        }

        free(retrieved);
    }

    if (data_art_size(tree) != 5) {
        data_art_destroy(tree);
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL("incorrect tree size");
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Insert 17 keys to trigger NODE_16 -> NODE_48 growth
 */
static bool test_node16_to_node48_growth(void) {
    TEST("NODE_16 grows to NODE_48 after 16 children");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    // Insert 17 keys with common prefix (all 32 bytes, zero-padded)
    for (int i = 0; i < 17; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "key_%02d", i);

        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to insert key");
        }
    }

    // Verify all keys
    for (int i = 0; i < 17; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "key_%02d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);
        if (!retrieved) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to retrieve key");
        }
        free(retrieved);
    }

    if (data_art_size(tree) != 17) {
        data_art_destroy(tree);
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL("incorrect tree size");
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Insert 49 keys to trigger NODE_48 -> NODE_256 growth
 */
static bool test_node48_to_node256_growth(void) {
    TEST("NODE_48 grows to NODE_256 after 48 children");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    // Insert 49 keys (all 32 bytes, zero-padded)
    for (int i = 0; i < 49; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "k_%03d", i);

        char value[32];
        snprintf(value, sizeof(value), "v_%d", i);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to insert key");
        }
    }

    // Verify all keys
    for (int i = 0; i < 49; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "k_%03d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);
        if (!retrieved) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to retrieve key");
        }
        free(retrieved);
    }

    if (data_art_size(tree) != 49) {
        data_art_destroy(tree);
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL("incorrect tree size");
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Bulk insert with many keys
 */
static bool test_bulk_insert(void) {
    TEST("bulk insert 100 keys");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 100;

    for (int i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "bulk_key_%04d", i);

        char value[64];
        snprintf(value, sizeof(value), "value_%d", i);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to insert key");
        }
    }

    // Verify all keys
    for (int i = 0; i < NUM_KEYS; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        snprintf((char *)key, 32, "bulk_key_%04d", i);

        char expected_value[64];
        snprintf(expected_value, sizeof(expected_value), "value_%d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);
        if (!retrieved) {
            fprintf(stderr, "FAILED TO RETRIEVE KEY: bulk_key_%04d (index %d)\n", i, i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("failed to retrieve key");
        }

        if (value_len != strlen(expected_value) ||
            memcmp(retrieved, expected_value, value_len) != 0) {
            free(retrieved);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }

        free(retrieved);
    }

    if (data_art_size(tree) != NUM_KEYS) {
        data_art_destroy(tree);
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL("incorrect tree size");
    }

    printf("\n");
    data_art_print_stats(tree);

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Random 32-byte key-value pairs
 */
static bool test_random_keys(void) {
    TEST("random 32-byte keys");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 1024;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 500;
    uint8_t (*keys)[32] = malloc(NUM_KEYS * 32);
    char **values = malloc(NUM_KEYS * sizeof(char *));
    size_t *value_lens = malloc(NUM_KEYS * sizeof(size_t));

    srand(12345); // Fixed seed for reproducibility

    // Generate random 32-byte keys and values
    for (int i = 0; i < NUM_KEYS; i++) {
        for (int j = 0; j < 32; j++) {
            keys[i][j] = rand() % 256;
        }

        value_lens[i] = 1 + (rand() % 500);
        values[i] = malloc(value_lens[i]);
        for (size_t j = 0; j < value_lens[i]; j++) {
            values[i][j] = rand() % 256;
        }

        if (!data_art_insert(tree, keys[i], 32, values[i], value_lens[i])) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            goto cleanup_fail;
        }
    }

    // Verify all keys can be retrieved
    for (int i = 0; i < NUM_KEYS; i++) {
        size_t retrieved_len;
        void *retrieved = data_art_get(tree, keys[i], 32, &retrieved_len);

        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key %d\n", i);
            goto cleanup_fail;
        }

        if (retrieved_len != value_lens[i] ||
            memcmp(retrieved, values[i], value_lens[i]) != 0) {
            fprintf(stderr, "Value mismatch for key %d\n", i);
            free(retrieved);
            goto cleanup_fail;
        }

        free(retrieved);
    }

    if (data_art_size(tree) != NUM_KEYS) {
        fprintf(stderr, "Tree size mismatch: expected %d, got %zu\n",
                NUM_KEYS, data_art_size(tree));
        goto cleanup_fail;
    }

    // Cleanup success
    for (int i = 0; i < NUM_KEYS; i++) {
        free(values[i]);
    }
    free(keys);
    free(values);
    free(value_lens);

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;

cleanup_fail:
    for (int i = 0; i < NUM_KEYS; i++) {
        free(values[i]);
    }
    free(keys);
    free(values);
    free(value_lens);
    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    FAIL("random key test failed");
}

/**
 * Test: Keys with long shared prefixes (triggers lazy expansion)
 * 27 bytes shared prefix + 5 varying bytes = 32 bytes
 */
static bool test_long_prefix_keys(void) {
    TEST("long shared prefix keys (lazy expansion)");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 100;
    uint8_t keys[NUM_KEYS][32];
    char values[NUM_KEYS][64];

    // 27 bytes of shared prefix (> 10, triggers lazy expansion) + unique suffix
    for (int i = 0; i < NUM_KEYS; i++) {
        memset(keys[i], 'P', 27);
        memset(keys[i] + 27, 0, 5);
        keys[i][27] = (uint8_t)i;

        snprintf(values[i], sizeof(values[i]), "value_%d", i);

        if (!data_art_insert(tree, keys[i], 32, values[i], strlen(values[i]))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("insertion failed");
        }
    }

    // Verify all keys
    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        void *retrieved = data_art_get(tree, keys[i], 32, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key %d\n", i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }

        if (value_len != strlen(values[i]) ||
            memcmp(retrieved, values[i], value_len) != 0) {
            fprintf(stderr, "Value mismatch for key %d\n", i);
            free(retrieved);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }

        free(retrieved);
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Keys that differ only in the first byte (padded to 32)
 */
static bool test_single_byte_keys(void) {
    TEST("single byte varying keys (padded to 32)");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 512;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    // Insert 256 keys: first byte varies 0..255, rest zero-padded to 32
    for (int i = 0; i < 256; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        key[0] = (uint8_t)i;

        char value[32];
        snprintf(value, sizeof(value), "value_%d", i);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key 0x%02x\n", i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("insertion failed");
        }
    }

    // Verify all keys
    for (int i = 0; i < 256; i++) {
        uint8_t key[32];
        memset(key, 0, 32);
        key[0] = (uint8_t)i;

        char expected_value[32];
        snprintf(expected_value, sizeof(expected_value), "value_%d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key 0x%02x\n", i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }

        if (value_len != strlen(expected_value) ||
            memcmp(retrieved, expected_value, value_len) != 0) {
            free(retrieved);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }

        free(retrieved);
    }

    if (data_art_size(tree) != 256) {
        data_art_destroy(tree);
        buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL("tree size mismatch");
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: 32-byte keys that differ only at byte 31
 */
static bool test_late_divergence_keys(void) {
    TEST("keys with late divergence (byte 31)");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 256;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 50;
    uint8_t keys[NUM_KEYS][32];

    // Create 32-byte keys identical except for the last byte
    for (int i = 0; i < NUM_KEYS; i++) {
        memset(keys[i], 'A', 32);
        keys[i][31] = (uint8_t)i;

        char value[32];
        snprintf(value, sizeof(value), "val_%d", i);

        if (!data_art_insert(tree, keys[i], 32, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("insertion failed");
        }
    }

    // Verify all keys
    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        void *retrieved = data_art_get(tree, keys[i], 32, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key %d (last byte=0x%02x)\n",
                    i, keys[i][31]);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }

        free(retrieved);
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Short content keys zero-padded to 32 bytes
 */
static bool test_empty_and_short_keys(void) {
    TEST("short content keys padded to 32 bytes");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 64;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    // Test with varying-content 32-byte keys:
    // len=1: "X\0\0...\0", len=2: "XX\0...\0", etc.
    // Each is unique because they differ at byte position (len-1)
    for (int len = 1; len <= 20; len++) {
        uint8_t key[32];
        memset(key, 0, 32);
        memset(key, 'X', len);

        char value[32];
        snprintf(value, sizeof(value), "len_%d", len);

        if (!data_art_insert(tree, key, 32, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key with %d X bytes\n", len);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("insertion failed");
        }
    }

    // Verify all keys
    for (int len = 1; len <= 20; len++) {
        uint8_t key[32];
        memset(key, 0, 32);
        memset(key, 'X', len);

        char expected[32];
        snprintf(expected, sizeof(expected), "len_%d", len);

        size_t value_len;
        void *retrieved = data_art_get(tree, key, 32, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Failed to retrieve key with %d X bytes\n", len);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }

        if (value_len != strlen(expected) ||
            memcmp(retrieved, expected, value_len) != 0) {
            free(retrieved);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }

        free(retrieved);
    }

    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

/**
 * Test: Large scale stress test with 2K keys
 */
static bool test_stress_2k_keys(void) {
    TEST("stress test - 2K random 32-byte keys");

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);

    buffer_pool_config_t bp_config = buffer_pool_default_config();
    bp_config.capacity = 4096;
    buffer_pool_t *bp = buffer_pool_create(&bp_config, pm);
    assert(bp != NULL);

    data_art_tree_t *tree = data_art_create(pm, bp, NULL, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 2000;
    srand(54321); // Fixed seed

    printf("\n  Inserting %d keys... ", NUM_KEYS);
    fflush(stdout);

    // Store keys for verification
    uint8_t (*keys)[32] = malloc(NUM_KEYS * 32);

    for (int i = 0; i < NUM_KEYS; i++) {
        memset(keys[i], 0, 32);
        snprintf((char *)keys[i], 32, "stress_%08d_%04d", rand(), i);

        char value[32];
        int value_len = snprintf(value, sizeof(value), "v%d", i);

        if (!data_art_insert(tree, keys[i], 32, value, value_len)) {
            fprintf(stderr, "\n  Failed to insert key %d\n", i);
            free(keys);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("insertion failed");
        }

        if ((i + 1) % 500 == 0) {
            printf("%d... ", i + 1);
            fflush(stdout);
        }
    }
    printf("done\n");

    // Verify a sample of keys (every 10th)
    printf("  Verifying sample... ");
    fflush(stdout);

    for (int i = 0; i < NUM_KEYS; i += 10) {
        char expected_value[32];
        snprintf(expected_value, sizeof(expected_value), "v%d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, keys[i], 32, &value_len);

        if (!retrieved) {
            fprintf(stderr, "\n  Failed to retrieve key %d\n", i);
            free(keys);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }

        if (value_len != strlen(expected_value) ||
            memcmp(retrieved, expected_value, value_len) != 0) {
            fprintf(stderr, "\n  Value mismatch for key %d\n", i);
            free(retrieved);
            free(keys);
            data_art_destroy(tree);
            buffer_pool_destroy(bp);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }

        free(retrieved);
    }
    printf("done\n");

    printf("  Tree size: %zu\n", data_art_size(tree));

    free(keys);
    data_art_destroy(tree);
    buffer_pool_destroy(bp);
    page_manager_destroy(pm);

    PASS();
    return true;
}

int main(void) {
    printf("\n");
    printf("========================================\n");
    printf("Persistent ART - Node Growth Tests\n");
    printf("========================================\n");
    printf("\n");

    // Run tests
    test_node4_to_node16_growth();
    test_node16_to_node48_growth();
    test_node48_to_node256_growth();
    test_bulk_insert();

    // Edge case tests
    test_single_byte_keys();
    test_empty_and_short_keys();
    test_late_divergence_keys();
    test_long_prefix_keys();
    test_random_keys();
    // test_stress_2k_keys();

    // Summary
    printf("\n");
    printf("========================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("========================================\n");

    cleanup_test_db();

    return (tests_passed == tests_run) ? 0 : 1;
}
