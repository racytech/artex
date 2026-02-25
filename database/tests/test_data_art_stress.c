/**
 * Test: data_art - Stress Testing
 *
 * Comprehensive stress tests for large datasets and edge cases
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#define TEST_DIR "/tmp/test_data_art_stress"
#define TEST_ART_PATH TEST_DIR "/art.dat"

// Test tracking
static int tests_run = 0;
static int tests_passed = 0;
static int verbose = 0;

// Remove test database
static void cleanup_test_db(void) {
    system("rm -rf " TEST_DIR " && mkdir -p " TEST_DIR);
}

#define TEST(name) \
    do { \
        printf("TEST: %s\n", name); \
        fflush(stdout); \
        tests_run++; \
        cleanup_test_db(); \
    } while (0)

#define PASS() \
    do { \
        printf("  ✓ PASS\n\n"); \
        tests_passed++; \
    } while (0)

#define FAIL(msg) \
    do { \
        printf("  ✗ FAIL: %s\n\n", msg); \
        return false; \
    } while (0)

/**
 * Helper: Generate a deterministic random key using rand()
 */
static void generate_key(int index, int seed, char *buf, size_t buf_size, size_t *key_len) {
    // Use rand() to match the original failing pattern
    (void)index; // Not used in this version
    *key_len = snprintf(buf, buf_size, "stress_key_%08d_%04d", rand(), index);
}

/**
 * Test: Incremental stress test - start small and increase
 */
static bool test_incremental_stress(void) {
    TEST("incremental stress test - up to 2000 keys");

    const int sizes[] = {100, 250, 500, 750, 1000, 1500, 2000};
    const int num_sizes = sizeof(sizes) / sizeof(sizes[0]);

    for (int s = 0; s < num_sizes; s++) {
        int num_keys = sizes[s];
        printf("  Testing with %d keys...\n", num_keys);

        cleanup_test_db();

        data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
        if (!tree) {
            fprintf(stderr, "  Failed to create tree\n");
            FAIL("tree creation failed");
        }

        // Seed RNG for deterministic key generation
        srand(12345);

        // Insert keys
        printf("    Inserting... ");
        fflush(stdout);

        for (int i = 0; i < num_keys; i++) {
            char key[128];
            size_t key_len;
            generate_key(i, 12345, key, sizeof(key), &key_len);

            char value[64];
            int value_len = snprintf(value, sizeof(value), "value_%d", i);

            if (!data_art_insert(tree, (const uint8_t *)key, key_len,
                                 value, value_len)) {
                fprintf(stderr, "\n    Failed to insert key %d (len=%zu): %s\n",
                        i, key_len, key);
                data_art_destroy(tree);
                FAIL("insertion failed");
            }

            // Debug: verify immediately after insertion
            if (i < 10 || i == 2) {
                size_t test_len;
                void *test = data_art_get(tree, (const uint8_t *)key, key_len, &test_len);
                if (!test) {
                    fprintf(stderr, "\n    WARNING: Key %d was just inserted but cannot be retrieved!\n", i);
                    fprintf(stderr, "    Key: %s\n", key);
                    data_art_destroy(tree);
                    FAIL("immediate verification failed");
                }
                free(test);

                // Also verify all previous keys still exist
                srand(12345); // Reset
                for (int check = 0; check <= i; check++) {
                    char check_key[128];
                    size_t check_len;
                    generate_key(check, 12345, check_key, sizeof(check_key), &check_len);
                    void *check_val = data_art_get(tree, (const uint8_t *)check_key, check_len, &test_len);
                    if (!check_val) {
                        fprintf(stderr, "\n    CORRUPTION DETECTED after inserting key %d!\n", i);
                        fprintf(stderr, "    Key %d is now missing: %s\n", check, check_key);
                        data_art_destroy(tree);
                        FAIL("tree corruption detected");
                    }
                    free(check_val);
                }
                // Restore RNG state
                srand(12345);
                for (int skip = 0; skip <= i; skip++) {
                    char dummy[128];
                    size_t dummy_len;
                    generate_key(skip, 12345, dummy, sizeof(dummy), &dummy_len);
                }
            }

            // Check ALL previously inserted keys after key 149 to see which ones disappear
            if (i == 149) {
                printf("\n    After inserting key 149, checking all previous keys...\n");
                srand(12345); // Reset
                int missing_count = 0;
                for (int check = 0; check < 149; check++) {
                    char check_key[128];
                    size_t check_len;
                    generate_key(check, 12345, check_key, sizeof(check_key), &check_len);
                    size_t test_len;
                    void *check_val = data_art_get(tree, (const uint8_t *)check_key, check_len, &test_len);
                    if (!check_val) {
                        if (missing_count < 10) {  // Only print first 10
                            printf("      Key %d MISSING: %s\n", check, check_key);
                        }
                        missing_count++;
                    } else {
                        free(check_val);
                    }
                }
                if (missing_count > 0) {
                    printf("      Total missing keys: %d out of 149\n", missing_count);
                    printf("      STOPPING TEST - major corruption detected\n");
                    data_art_destroy(tree);
                    FAIL("massive corruption after key 149");
                } else {
                    printf("      All 149 keys verified OK after key 149\n");
                }
                // Restore RNG state
                srand(12345);
                for (int skip = 0; skip <= i; skip++) {
                    char dummy[128];
                    size_t dummy_len;
                    generate_key(skip, 12345, dummy, sizeof(dummy), &dummy_len);
                }
            }

            if ((i + 1) % 500 == 0) {
                printf("%d... ", i + 1);
                fflush(stdout);
            }
        }
        printf("done\n");

        // Debug: check if key 2 exists right after all insertions
        printf("    Debug: checking key 2 before verification... ");
        srand(12345);
        for (int skip = 0; skip < 2; skip++) {
            char dummy[128];
            size_t dummy_len;
            generate_key(skip, 12345, dummy, sizeof(dummy), &dummy_len);
        }
        char key2[128];
        size_t key2_len;
        generate_key(2, 12345, key2, sizeof(key2), &key2_len);
        size_t test_len;
        void *test2 = data_art_get(tree, (const uint8_t *)key2, key2_len, &test_len);
        printf("%s\n", test2 ? "EXISTS" : "MISSING!");
        if (test2) free(test2);

        // Verify all keys - reseed RNG to get same sequence
        printf("    Verifying... ");
        fflush(stdout);
        srand(12345); // Reset to same seed

        int verified = 0;
        for (int i = 0; i < num_keys; i++) {
            char key[128];
            size_t key_len;
            generate_key(i, 12345, key, sizeof(key), &key_len);

            char expected_value[64];
            snprintf(expected_value, sizeof(expected_value), "value_%d", i);

            size_t value_len;
            void *retrieved = data_art_get(tree, (const uint8_t *)key, key_len,
                                            &value_len);

            if (!retrieved) {
                fprintf(stderr, "\n    Failed to retrieve key %d (len=%zu): %s\n",
                        i, key_len, key);
                fprintf(stderr, "    Verified %d/%d keys before failure\n",
                        verified, num_keys);

                // Print some debugging info about the key
                fprintf(stderr, "\n    Key details:\n");
                fprintf(stderr, "      Index: %d\n", i);
                fprintf(stderr, "      Length: %zu\n", key_len);
                fprintf(stderr, "      Bytes (hex): ");
                for (size_t j = 0; j < key_len && j < 40; j++) {
                    fprintf(stderr, "%02x ", (unsigned char)key[j]);
                }
                fprintf(stderr, "\n");

                // Try to find if a similar key exists
                fprintf(stderr, "\n    Checking if key was inserted...\n");
                fprintf(stderr, "    Trying to re-insert the key:\n");
                bool reinsert = data_art_insert(tree, (const uint8_t *)key, key_len, "test", 4);
                fprintf(stderr, "      Re-insert %s (update=%s)\n",
                        reinsert ? "succeeded" : "failed",
                        reinsert ? "no" : "yes");

                data_art_destroy(tree);
                FAIL("retrieval failed");
            }

            if (value_len != strlen(expected_value) ||
                memcmp(retrieved, expected_value, value_len) != 0) {
                fprintf(stderr, "\n    Value mismatch for key %d\n", i);
                fprintf(stderr, "    Expected: %s (len=%zu)\n",
                        expected_value, strlen(expected_value));
                fprintf(stderr, "    Got: %.*s (len=%zu)\n",
                        (int)value_len, (char *)retrieved, value_len);
                free(retrieved);
                data_art_destroy(tree);
                FAIL("value mismatch");
            }

            free(retrieved);
            verified++;

            if ((i + 1) % 500 == 0) {
                printf("%d... ", i + 1);
                fflush(stdout);
            }
        }
        printf("done\n");

        printf("    Tree size: %zu (expected %d)\n", data_art_size(tree), num_keys);

        if (data_art_size(tree) != (size_t)num_keys) {
            data_art_destroy(tree);
            FAIL("tree size mismatch");
        }

        data_art_destroy(tree);
    }

    PASS();
    return true;
}

/**
 * Test: Focused test with large dataset
 */
static bool test_focused_2000(void) {
    TEST("focused test - exactly 10000 keys");

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 10000;

    // Seed RNG
    srand(54321);

    printf("  Inserting %d keys...\n", NUM_KEYS);

    // Store keys for later verification
    char **keys = malloc(NUM_KEYS * sizeof(char *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));

    for (int i = 0; i < NUM_KEYS; i++) {
        keys[i] = malloc(128);
        generate_key(i, 54321, keys[i], 128, &key_lens[i]);

        char value[64];
        int value_len = snprintf(value, sizeof(value), "v%d", i);

        if (!data_art_insert(tree, (const uint8_t *)keys[i], key_lens[i],
                             value, value_len)) {
            fprintf(stderr, "  Failed to insert key %d\n", i);
            goto cleanup_fail;
        }

        if ((i + 1) % 500 == 0) {
            printf("    Inserted %d keys\n", i + 1);
        }
    }

    printf("  All keys inserted successfully\n");
    printf("  Tree size: %zu\n\n", data_art_size(tree));

    // Verify in order - reseed RNG
    printf("  Verifying in insertion order...\n");
    srand(54321); // Reset to same seed

    for (int i = 0; i < NUM_KEYS; i++) {
        char expected_value[64];
        snprintf(expected_value, sizeof(expected_value), "v%d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, (const uint8_t *)keys[i], key_lens[i],
                                        &value_len);

        if (!retrieved) {
            fprintf(stderr, "  FAILED at key %d\n", i);
            fprintf(stderr, "  Key (len=%zu): %s\n", key_lens[i], keys[i]);

            // Try to print the tree structure around this key
            fprintf(stderr, "\n  Trying adjacent keys:\n");
            for (int j = (i > 5 ? i - 5 : 0); j < i && j < NUM_KEYS; j++) {
                size_t test_len;
                void *test = data_art_get(tree, (const uint8_t *)keys[j], key_lens[j], &test_len);
                fprintf(stderr, "    Key %d: %s\n", j, test ? "OK" : "MISSING");
                if (test) free(test);
            }

            goto cleanup_fail;
        }

        if (value_len != strlen(expected_value) ||
            memcmp(retrieved, expected_value, value_len) != 0) {
            fprintf(stderr, "  Value mismatch at key %d\n", i);
            free(retrieved);
            goto cleanup_fail;
        }

        free(retrieved);

        if ((i + 1) % 500 == 0) {
            printf("    Verified %d keys\n", i + 1);
        }
    }

    printf("  All keys verified successfully\n\n");

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);

    data_art_destroy(tree);

    PASS();
    return true;

cleanup_fail:
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);
    data_art_destroy(tree);
    FAIL("test failed");
}

/**
 * Test: Minimal reproduction with just 3 keys
 * Keys 32, 34, and 149 where 149 corrupts 32 and 34
 */
static bool test_minimal_reproduction(void) {
    printf("\n  === MINIMAL REPRODUCTION TEST ===\n");
    printf("  This test inserts 3 keys that trigger corruption:\n");
    printf("    1. Key 32:  stress_key_602419417_0032\n");
    printf("    2. Key 34:  stress_key_60241667_0034 (shares 16-byte prefix with key 32)\n");
    printf("    3. Key 149: stress_key_601177067_0149 (shares 13-byte prefix with keys 32/34)\n");
    printf("  Expected: Key 149 insertion corrupts keys 32 and 34\n\n");

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    if (!tree) {
        FAIL("Failed to create tree");
    }

    printf("  Inserting 3 specific keys to reproduce corruption...\n");

    // Generate the exact keys
    srand(12345);

    // Key 32
    for (int i = 0; i < 32; i++) rand();
    char key32[128];
    size_t len32 = snprintf(key32, sizeof(key32), "stress_key_%08d_%04d", rand(), 32);
    char val32[32];
    size_t vallen32 = snprintf(val32, sizeof(val32), "value_%04d", 32);

    // Key 34
    srand(12345);
    for (int i = 0; i < 34; i++) rand();
    char key34[128];
    size_t len34 = snprintf(key34, sizeof(key34), "stress_key_%08d_%04d", rand(), 34);
    char val34[32];
    size_t vallen34 = snprintf(val34, sizeof(val34), "value_%04d", 34);

    // Key 149
    srand(12345);
    for (int i = 0; i < 149; i++) rand();
    char key149[128];
    size_t len149 = snprintf(key149, sizeof(key149), "stress_key_%08d_%04d", rand(), 149);
    char val149[32];
    size_t vallen149 = snprintf(val149, sizeof(val149), "value_%04d", 149);

    printf("  Key 32:  %s\n", key32);
    printf("  Key 34:  %s\n", key34);
    printf("  Key 149: %s\n", key149);

    // Insert key 32
    printf("\n  Inserting key 32...\n");
    if (!data_art_insert(tree, (const uint8_t *)key32, len32,
                         (const uint8_t *)val32, vallen32)) {
        data_art_destroy(tree);
        FAIL("Failed to insert key 32");
    }

    // Verify key 32
    size_t test_len;
    const void *test32 = data_art_get(tree, (const uint8_t *)key32, len32, &test_len);
    if (!test32) {
        data_art_destroy(tree);
        FAIL("Key 32 not found after insertion");
    }
    printf("  Key 32 verified OK\n");

    // Insert key 34
    printf("\n  Inserting key 34...\n");
    if (!data_art_insert(tree, (const uint8_t *)key34, len34,
                         (const uint8_t *)val34, vallen34)) {
        data_art_destroy(tree);
        FAIL("Failed to insert key 34");
    }

    // Verify both keys
    test32 = data_art_get(tree, (const uint8_t *)key32, len32, &test_len);
    if (!test32) {
        data_art_destroy(tree);
        FAIL("Key 32 disappeared after key 34");
    }
    printf("  Key 32 still OK after key 34\n");

    const void *test34 = data_art_get(tree, (const uint8_t *)key34, len34, &test_len);
    if (!test34) {
        data_art_destroy(tree);
        FAIL("Key 34 not found after insertion");
    }
    printf("  Key 34 verified OK\n");

    // Now insert key 149 - this should corrupt keys 32 and 34
    printf("\n  Inserting key 149 (the corruption trigger)...\n");
    if (!data_art_insert(tree, (const uint8_t *)key149, len149,
                         (const uint8_t *)val149, vallen149)) {
        data_art_destroy(tree);
        FAIL("Failed to insert key 149");
    }

    printf("  Key 149 inserted successfully\n");

    // Now check all three keys
    printf("\n  Verifying all keys after key 149 insertion...\n");

    test32 = data_art_get(tree, (const uint8_t *)key32, len32, &test_len);
    if (!test32) {
        printf("  ✗ Key 32 DISAPPEARED!\n");
        data_art_destroy(tree);
        FAIL("Key 32 corrupted by key 149 insertion");
    }
    printf("  ✓ Key 32 OK\n");

    test34 = data_art_get(tree, (const uint8_t *)key34, len34, &test_len);
    if (!test34) {
        printf("  ✗ Key 34 DISAPPEARED!\n");
        data_art_destroy(tree);
        FAIL("Key 34 corrupted by key 149 insertion");
    }
    printf("  ✓ Key 34 OK\n");

    const void *test149 = data_art_get(tree, (const uint8_t *)key149, len149, &test_len);
    if (!test149) {
        printf("  ✗ Key 149 not found!\n");
        data_art_destroy(tree);
        FAIL("Key 149 not retrievable");
    }
    printf("  ✓ Key 149 OK\n");

    printf("\n  All 3 keys verified successfully!\n");

    data_art_destroy(tree);

    PASS();
}

/**
 * Test: Sequential insertion order
 */
static bool test_sequential_keys(void) {
    TEST("sequential keys 0000-1999");

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    const int NUM_KEYS = 2000;

    printf("  Inserting sequential keys...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        int key_len = snprintf(key, sizeof(key), "seq_%04d", i);

        char value[32];
        int value_len = snprintf(value, sizeof(value), "val_%d", i);

        if (!data_art_insert(tree, (const uint8_t *)key, key_len,
                             value, value_len)) {
            fprintf(stderr, "  Failed to insert key %d: %s\n", i, key);
            data_art_destroy(tree);
            FAIL("insertion failed");
        }

        if ((i + 1) % 500 == 0) {
            printf("    Inserted %d keys\n", i + 1);
        }
    }

    printf("  Verifying...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        char key[32];
        int key_len = snprintf(key, sizeof(key), "seq_%04d", i);

        char expected[32];
        snprintf(expected, sizeof(expected), "val_%d", i);

        size_t value_len;
        void *retrieved = data_art_get(tree, (const uint8_t *)key, key_len,
                                        &value_len);

        if (!retrieved) {
            fprintf(stderr, "  Failed to retrieve key %d: %s\n", i, key);
            data_art_destroy(tree);
            FAIL("retrieval failed");
        }

        if (value_len != strlen(expected) ||
            memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "  Value mismatch at key %d\n", i);
            free(retrieved);
            data_art_destroy(tree);
            FAIL("value mismatch");
        }

        free(retrieved);

        if ((i + 1) % 500 == 0) {
            printf("    Verified %d keys\n", i + 1);
        }
    }

    printf("  All verified\n");

    data_art_destroy(tree);

    PASS();
    return true;
}

int main(int argc, char **argv) {
    // Check for verbose flag
    if (argc > 1 && strcmp(argv[1], "-v") == 0) {
        verbose = 1;
    }

    printf("\n");
    printf("========================================\n");
    printf("Persistent ART - Stress Tests\n");
    printf("========================================\n");
    printf("\n");

    // Run tests based on argument
    if (argc > 1 && strcmp(argv[1], "minimal") == 0) {
        printf("Running MINIMAL reproduction test only...\n\n");
        test_minimal_reproduction();
    } else if (argc > 1 && strcmp(argv[1], "incremental") == 0) {
        test_incremental_stress();
    } else if (argc > 1 && strcmp(argv[1], "focused") == 0) {
        test_focused_2000();
    } else {
        // Run all tests
        test_minimal_reproduction();
        test_sequential_keys();
        test_incremental_stress();
        test_focused_2000();
    }

    // Summary
    printf("========================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("========================================\n");

    system("rm -rf " TEST_DIR);

    return (tests_passed == tests_run) ? 0 : 1;
}
