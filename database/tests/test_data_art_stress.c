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

#define TEST_DB_PATH "/tmp/test_data_art_stress.db"

// Test tracking
static int tests_run = 0;
static int tests_passed = 0;
static int verbose = 0;

// Remove test database
static void cleanup_test_db(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
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
    TEST("incremental stress test - debugging");
    
    const int sizes[] = {100, 150, 200, 250, 300};  // Binary search
    const int num_sizes = sizeof(sizes) / sizeof(sizes[0]);
    
    for (int s = 0; s < num_sizes; s++) {
        int num_keys = sizes[s];
        printf("  Testing with %d keys...\n", num_keys);
        
        cleanup_test_db();
        
        page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
        if (!pm) {
            fprintf(stderr, "  Failed to create page manager\n");
            FAIL("page manager creation failed");
        }
        
        data_art_tree_t *tree = data_art_create(pm, NULL);
        if (!tree) {
            page_manager_destroy(pm);
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
                page_manager_destroy(pm);
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
                    page_manager_destroy(pm);
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
                        page_manager_destroy(pm);
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
            
            // Check key 2 every insertion after key 10 to find when it disappears
            if (i > 2 && i <= 50) {
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
                if (!test2) {
                    fprintf(stderr, "\n    KEY 2 DISAPPEARED after inserting key %d!\n", i);
                    fprintf(stderr, "    Key 2: %s\n", key2);
                    fprintf(stderr, "    Last inserted key %d: %s\n", i, key);
                    data_art_destroy(tree);
                    page_manager_destroy(pm);
                    FAIL("key corruption detected");
                }
                free(test2);
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
                page_manager_destroy(pm);
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
                page_manager_destroy(pm);
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
            page_manager_destroy(pm);
            FAIL("tree size mismatch");
        }
        
        data_art_destroy(tree);
        page_manager_destroy(pm);
    }
    
    PASS();
    return true;
}

/**
 * Test: Focused test around the failure point
 */
static bool test_focused_2000(void) {
    TEST("focused test - exactly 2000 keys");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
    assert(tree != NULL);
    
    const int NUM_KEYS = 2000;
    
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
    page_manager_destroy(pm);
    
    PASS();
    return true;

cleanup_fail:
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);
    data_art_destroy(tree);
    page_manager_destroy(pm);
    FAIL("test failed");
}

/**
 * Test: Sequential insertion order
 */
static bool test_sequential_keys(void) {
    TEST("sequential keys 0000-1999");
    
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    assert(pm != NULL);
    
    data_art_tree_t *tree = data_art_create(pm, NULL);
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
            page_manager_destroy(pm);
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
            page_manager_destroy(pm);
            FAIL("retrieval failed");
        }
        
        if (value_len != strlen(expected) ||
            memcmp(retrieved, expected, value_len) != 0) {
            fprintf(stderr, "  Value mismatch at key %d\n", i);
            free(retrieved);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            FAIL("value mismatch");
        }
        
        free(retrieved);
        
        if ((i + 1) % 500 == 0) {
            printf("    Verified %d keys\n", i + 1);
        }
    }
    
    printf("  All verified\n");
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
    
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
    
    // Run tests
    test_sequential_keys();
    test_incremental_stress();
    test_focused_2000();
    
    // Summary
    printf("========================================\n");
    printf("Test Results: %d/%d passed\n", tests_passed, tests_run);
    printf("========================================\n");
    
    cleanup_test_db();
    
    return (tests_passed == tests_run) ? 0 : 1;
}
