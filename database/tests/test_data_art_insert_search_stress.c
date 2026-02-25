/**
 * Stress Test for Persistent ART - Insert and Search Operations
 *
 * Tests insert and search with various key patterns:
 * - Random keys with different lengths
 * - Sequential keys
 * - Keys with common prefixes
 * - Binary keys
 *
 * FAIL-FAST MODE: Runs continuously for specified duration, fails immediately on first error.
 * All tests are reproducible using fixed random seeds.
 *
 * Usage:
 *   ./test_data_art_insert_search_stress           # Run all individual tests once
 *   ./test_data_art_insert_search_stress <seconds> # Run continuous stress test for N seconds
 */

#include "data_art.h"
#include "logger.h"
#include "../third_party/uthash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#define TEST_DIR "/tmp/test_data_art_stress_insert_search"
#define TEST_ART_PATH TEST_DIR "/art.dat"

// ANSI color codes
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_BLUE   "\033[0;34m"
#define COLOR_RESET  "\033[0m"

#define TEST(name) printf("\n" COLOR_YELLOW "TEST: %s" COLOR_RESET "\n", name)
#define PASS() do { printf("  " COLOR_GREEN "✓ PASS" COLOR_RESET "\n"); return true; } while(0)
#define FAIL(msg) do { printf("  " COLOR_RED "✗ FAIL: %s" COLOR_RESET "\n", msg); return false; } while(0)
#define FAIL_FAST(msg, ...) do { \
    fprintf(stderr, "\n" COLOR_RED "✗✗✗ FAIL-FAST ERROR ✗✗✗" COLOR_RESET "\n"); \
    fprintf(stderr, COLOR_RED msg COLOR_RESET "\n", ##__VA_ARGS__); \
    exit(1); \
} while(0)

// Global flag for signal handling
static volatile sig_atomic_t keep_running = 1;

static void signal_handler(int signum) {
    (void)signum;
    keep_running = 0;
}

// Hash table entry to track unique keys and their latest values
typedef struct key_entry {
    char hash_key[300];  // Hex string representation of the key
    uint8_t *key;
    size_t key_len;
    char *value;
    size_t value_len;
    int last_index;  // Index where this key was last inserted
    UT_hash_handle hh;
} key_entry_t;

// Helper: Create hash key from binary key
static void make_hash_key(const uint8_t *key, size_t key_len, char *hash_key, size_t hash_key_size) {
    size_t offset = 0;
    for (size_t i = 0; i < key_len && offset + 2 < hash_key_size; i++) {
        offset += snprintf(hash_key + offset, hash_key_size - offset, "%02x", key[i]);
    }
    hash_key[offset] = '\0';
}

// Helper: Clean up test database
static void cleanup_test_db(void) {
    system("rm -rf " TEST_DIR " && mkdir -p " TEST_DIR);
    // Ensure the filesystem has time to fully remove the directory
    sync();
    usleep(10000);  // 10ms delay to let filesystem catch up

    // Verify it's actually gone and recreated
    struct stat st;
    if (stat(TEST_DIR, &st) != 0) {
        fprintf(stderr, "WARNING: Test directory does not exist after cleanup!\n");
    }
}

// Helper: Generate random key
static void generate_random_key(uint8_t *key, size_t len, unsigned int *seed) {
    for (size_t i = 0; i < len; i++) {
        key[i] = (uint8_t)(rand_r(seed) % 256);
    }
}

// Helper: Generate random printable key
static void generate_printable_key(uint8_t *key, size_t len, unsigned int *seed) {
    for (size_t i = 0; i < len; i++) {
        key[i] = (uint8_t)(32 + (rand_r(seed) % 95)); // ASCII 32-126
    }
}

// Helper: Generate sequential key
static void generate_sequential_key(uint8_t *key, size_t max_len, uint64_t num) {
    snprintf((char *)key, max_len, "key_%020lu", num);
}

// Helper: Generate key with common prefix
static void generate_prefix_key(uint8_t *key, size_t max_len, const char *prefix, uint64_t num) {
    snprintf((char *)key, max_len, "%s_%020lu", prefix, num);
}

// Helper: Generate random value with varying length
static void generate_random_value(char *value, size_t max_len, int index, unsigned int *seed) {
    // Value length between 8 and max_len
    size_t value_len = 8 + (rand_r(seed) % (max_len - 7));

    // Include some metadata
    int prefix_len = snprintf(value, max_len, "val_%d_", index);

    // Fill rest with random printable chars
    for (size_t i = prefix_len; i < value_len - 1 && i < max_len - 1; i++) {
        value[i] = (char)(32 + (rand_r(seed) % 95));
    }
    value[value_len - 1] = '\0';
}

/**
 * Test: Random keys with varying lengths
 * Seed: 42 (reproducible)
 */
static bool test_random_keys_varying_length(void) {
    TEST("random keys with varying lengths (1-128 bytes)");

    const int NUM_KEYS = 1000;
    const unsigned int SEED = 42;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    // Generate and insert random keys
    uint8_t **keys = malloc(NUM_KEYS * sizeof(uint8_t *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));
    char **values = malloc(NUM_KEYS * sizeof(char *));

    printf("  Generating and inserting %d keys with varying lengths...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        // Random length between 1 and 128
        key_lens[i] = 1 + (rand_r(&seed) % 128);
        keys[i] = malloc(key_lens[i]);

        generate_random_key(keys[i], key_lens[i], &seed);

        // Create value with random length (16-256 bytes)
        values[i] = malloc(256);
        generate_random_value(values[i], 256, i, &seed);

        if (!data_art_insert(tree, keys[i], key_lens[i], values[i], strlen(values[i]))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            FAIL("Insert failed");
        }

        if ((i + 1) % 100 == 0) {
            printf("    Inserted %d keys\n", i + 1);
        }
    }

    printf("  Verifying all keys can be found...\n");

    int found = 0;
    int not_found = 0;
    int value_mismatch = 0;

    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            not_found++;
            if (not_found <= 5) {
                fprintf(stderr, "Key %d not found (len=%zu)\n", i, key_lens[i]);
            }
        } else {
            if (value_len != strlen(values[i]) ||
                memcmp(retrieved, values[i], value_len) != 0) {
                value_mismatch++;
                if (value_mismatch <= 5) {
                    fprintf(stderr, "Value mismatch for key %d\n", i);
                }
            } else {
                found++;
            }
            free((void *)retrieved);
        }

        if ((i + 1) % 100 == 0) {
            printf("    Verified %d keys\n", i + 1);
        }
    }

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);

    data_art_destroy(tree);

    printf("  Results: Found=%d, NotFound=%d, Mismatch=%d\n", found, not_found, value_mismatch);

    if (not_found > 0) {
        FAIL("Keys not found after insertion");
    }
    if (value_mismatch > 0) {
        FAIL("Value mismatches detected");
    }

    PASS();
    return true;
}

/**
 * Test: Random printable keys (easier to debug)
 * Seed: 100 (reproducible)
 */
static bool test_random_printable_keys(void) {
    TEST("random printable keys (16-64 bytes)");

    const int NUM_KEYS = 500;
    const unsigned int SEED = 100;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    uint8_t **keys = malloc(NUM_KEYS * sizeof(uint8_t *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));

    printf("  Inserting %d printable keys...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        key_lens[i] = 16 + (rand_r(&seed) % 49); // 16-64 bytes
        keys[i] = malloc(key_lens[i] + 1);

        generate_printable_key(keys[i], key_lens[i], &seed);
        keys[i][key_lens[i]] = '\0'; // Null terminate for debugging

        char value[128];
        generate_random_value(value, sizeof(value), i, &seed);

        if (!data_art_insert(tree, keys[i], key_lens[i], value, strlen(value))) {
            // Print the key for debugging
            fprintf(stderr, "Failed to insert key %d: '%.*s'\n", i, (int)key_lens[i], keys[i]);
            FAIL("Insert failed");
        }
    }

    printf("  Verifying all keys...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            fprintf(stderr, "Key %d not found: '%.*s'\n", i, (int)key_lens[i], keys[i]);
            FAIL("Key not found");
        }

        // Just verify we got something back (values are random)
        if (value_len == 0) {
            fprintf(stderr, "Empty value for key %d\n", i);
            free((void *)retrieved);
            FAIL("Empty value");
        }

        free((void *)retrieved);
    }

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);

    data_art_destroy(tree);

    printf("  All %d keys verified successfully\n", NUM_KEYS);

    PASS();
    return true;
}

/**
 * Test: Sequential keys with different lengths
 */
static bool test_sequential_keys(void) {
    TEST("sequential keys with varying lengths");

    const int NUM_KEYS = 1000;
    const unsigned int SEED = 150;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    printf("  Inserting %d sequential keys...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        uint8_t key[64];
        generate_sequential_key(key, sizeof(key), i);
        size_t key_len = strlen((char *)key);

        char value[512];
        generate_random_value(value, sizeof(value), i, &seed);

        if (!data_art_insert(tree, key, key_len, value, strlen(value))) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            FAIL("Insert failed");
        }
    }

    printf("  Verifying all keys in forward order...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        uint8_t key[64];
        generate_sequential_key(key, sizeof(key), i);
        size_t key_len = strlen((char *)key);

        size_t value_len;
        const void *retrieved = data_art_get(tree, key, key_len, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Key %d not found\n", i);
            FAIL("Key not found");
        }

        free((void *)retrieved);
    }

    printf("  Verifying all keys in reverse order...\n");

    for (int i = NUM_KEYS - 1; i >= 0; i--) {
        uint8_t key[64];
        generate_sequential_key(key, sizeof(key), i);
        size_t key_len = strlen((char *)key);

        size_t value_len;
        const void *retrieved = data_art_get(tree, key, key_len, &value_len);

        if (!retrieved) {
            fprintf(stderr, "Key %d not found in reverse lookup\n", i);
            FAIL("Key not found");
        }

        free((void *)retrieved);
    }

    data_art_destroy(tree);

    printf("  All %d keys verified in both directions\n", NUM_KEYS);

    PASS();
    return true;
}

/**
 * Test: Keys with common prefixes (tests path compression)
 */
static bool test_common_prefix_keys(void) {
    TEST("keys with common prefixes");

    const int NUM_PREFIXES = 10;
    const int KEYS_PER_PREFIX = 100;  // Restored to original value
    const int TOTAL_KEYS = NUM_PREFIXES * KEYS_PER_PREFIX;
    const unsigned int SEED = 250;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    printf("  Inserting %d keys with %d prefixes...\n", TOTAL_KEYS, NUM_PREFIXES);

    for (int p = 0; p < NUM_PREFIXES; p++) {
        char prefix[32];
        snprintf(prefix, sizeof(prefix), "prefix_%02d", p);

        for (int i = 0; i < KEYS_PER_PREFIX; i++) {
            uint8_t key[128];
            generate_prefix_key(key, sizeof(key), prefix, i);
            size_t key_len = strlen((char *)key);

            if (p == 0 && i < 2) {
                fprintf(stderr, "DEBUG INSERT: key='%s', len=%zu\n", (char*)key, key_len);
            }

            char value[256];
            generate_random_value(value, sizeof(value), p * KEYS_PER_PREFIX + i, &seed);

            if (!data_art_insert(tree, key, key_len, value, strlen(value))) {
                fprintf(stderr, "Failed to insert key prefix=%d, num=%d\n", p, i);
                FAIL("Insert failed");
            }
        }

        if ((p + 1) % 2 == 0) {
            printf("    Inserted %d prefixes (%d keys)\n", p + 1, (p + 1) * KEYS_PER_PREFIX);
        }
    }

    printf("  Verifying all keys...\n");
    fprintf(stderr, "DEBUG: Tree size before verify: %zu\n", data_art_size(tree));

    int verified = 0;
    for (int p = 0; p < NUM_PREFIXES; p++) {
        char prefix[32];
        snprintf(prefix, sizeof(prefix), "prefix_%02d", p);

        for (int i = 0; i < KEYS_PER_PREFIX; i++) {
            uint8_t key[128];
            generate_prefix_key(key, sizeof(key), prefix, i);
            size_t key_len = strlen((char *)key);

            if (p == 0 && i < 2) {
                fprintf(stderr, "DEBUG SEARCH: key='%s', len=%zu\n", (char*)key, key_len);
            }

            size_t value_len;
            const void *retrieved = data_art_get(tree, key, key_len, &value_len);

            if (!retrieved) {
                fprintf(stderr, "Key not found: prefix=%d, num=%d, key='%s'\n", p, i, (char*)key);
                // Try to print some tree stats
                fprintf(stderr, "Tree size: %zu\n", data_art_size(tree));
                FAIL("Key not found");
            }

            verified++;
            free((void *)retrieved);
        }
    }

    printf("  Verified %d keys successfully\n", verified);

    data_art_destroy(tree);

    printf("  All %d keys verified\n", TOTAL_KEYS);

    PASS();
    return true;
}

/**
 * Test: Binary keys with null bytes
 * Seed: 200 (reproducible)
 */
static bool test_binary_keys_with_nulls(void) {
    TEST("binary keys with null bytes");

    const int NUM_KEYS = 500;
    const unsigned int SEED = 200;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    uint8_t **keys = malloc(NUM_KEYS * sizeof(uint8_t *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));

    printf("  Inserting %d binary keys with embedded nulls...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        key_lens[i] = 16 + (rand_r(&seed) % 49); // 16-64 bytes
        keys[i] = malloc(key_lens[i]);

        generate_random_key(keys[i], key_lens[i], &seed);

        // Ensure at least one null byte in the middle
        if (key_lens[i] > 2) {
            keys[i][rand_r(&seed) % (key_lens[i] - 2) + 1] = 0;
        }

        char value[512];
        generate_random_value(value, sizeof(value), i, &seed);

        if (!data_art_insert(tree, keys[i], key_lens[i], value, strlen(value))) {
            fprintf(stderr, "Failed to insert binary key %d\n", i);
            FAIL("Insert failed");
        }
    }

    printf("  Verifying all binary keys...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            fprintf(stderr, "Binary key %d not found (len=%zu)\n", i, key_lens[i]);
            FAIL("Key not found");
        }

        // Just verify we got data back (values are random)
        if (value_len == 0) {
            fprintf(stderr, "Empty value for binary key %d\n", i);
            free((void *)retrieved);
            FAIL("Empty value");
        }

        free((void *)retrieved);
    }

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);

    data_art_destroy(tree);

    printf("  All %d binary keys verified\n", NUM_KEYS);

    PASS();
    return true;
}

/**
 * Test: Very long keys (up to 256 bytes)
 * Seed: 300 (reproducible)
 */
static bool test_very_long_keys(void) {
    TEST("very long keys (128-256 bytes)");

    const int NUM_KEYS = 200;
    const unsigned int SEED = 300;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    uint8_t **keys = malloc(NUM_KEYS * sizeof(uint8_t *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));

    printf("  Inserting %d very long keys...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        key_lens[i] = 128 + (rand_r(&seed) % 129); // 128-256 bytes
        keys[i] = malloc(key_lens[i]);

        generate_printable_key(keys[i], key_lens[i], &seed);

        char value[1024];
        generate_random_value(value, sizeof(value), i, &seed);

        if (!data_art_insert(tree, keys[i], key_lens[i], value, strlen(value))) {
            fprintf(stderr, "Failed to insert very long key %d (len=%zu)\n", i, key_lens[i]);
            FAIL("Insert failed");
        }
    }

    printf("  Verifying all very long keys...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            fprintf(stderr, "Very long key %d not found (len=%zu)\n", i, key_lens[i]);
            FAIL("Key not found");
        }

        free((void *)retrieved);
    }

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);

    data_art_destroy(tree);

    printf("  All %d very long keys verified\n", NUM_KEYS);

    PASS();
    return true;
}

/**
 * Test: Update existing keys
 * Seed: 400 (reproducible)
 */
static bool test_update_keys(void) {
    TEST("update existing keys");

    const int NUM_KEYS = 300;
    const unsigned int SEED = 400;
    unsigned int seed = SEED;

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    uint8_t **keys = malloc(NUM_KEYS * sizeof(uint8_t *));
    size_t *key_lens = malloc(NUM_KEYS * sizeof(size_t));

    printf("  Inserting %d keys...\n", NUM_KEYS);

    for (int i = 0; i < NUM_KEYS; i++) {
        key_lens[i] = 8 + (rand_r(&seed) % 25); // 8-32 bytes
        keys[i] = malloc(key_lens[i]);

        generate_printable_key(keys[i], key_lens[i], &seed);

        char value[256];
        generate_random_value(value, sizeof(value), i, &seed);

        if (!data_art_insert(tree, keys[i], key_lens[i], value, strlen(value))) {
            FAIL("Insert failed");
        }
    }

    printf("  Updating all keys with new values...\n");

    // Store updated values for verification
    char **updated_values = malloc(NUM_KEYS * sizeof(char *));

    for (int i = 0; i < NUM_KEYS; i++) {
        updated_values[i] = malloc(512);
        generate_random_value(updated_values[i], 512, i + NUM_KEYS, &seed); // Different seed offset

        char new_value[512];
        strcpy(new_value, updated_values[i]);

        if (!data_art_insert(tree, keys[i], key_lens[i], new_value, strlen(new_value))) {
            FAIL("Update failed");
        }
    }

    printf("  Verifying updated values...\n");

    for (int i = 0; i < NUM_KEYS; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            fprintf(stderr, "Key %d not found after update\n", i);
            FAIL("Key not found");
        }

        if (value_len != strlen(updated_values[i]) ||
            memcmp(retrieved, updated_values[i], value_len) != 0) {
            fprintf(stderr, "Key %d has wrong value after update\n", i);
            free((void *)retrieved);
            FAIL("Value not updated");
        }

        free((void *)retrieved);
    }

    // Verify tree size is still NUM_KEYS (updates don't add entries)
    if (data_art_size(tree) != NUM_KEYS) {
        fprintf(stderr, "Tree size should be %d but is %zu\n", NUM_KEYS, data_art_size(tree));
        FAIL("Tree size incorrect after updates");
    }

    // Cleanup
    for (int i = 0; i < NUM_KEYS; i++) {
        free(keys[i]);
    }
    free(keys);
    free(key_lens);

    data_art_destroy(tree);

    printf("  All %d keys updated and verified\n", NUM_KEYS);

    PASS();
    return true;
}

/**
 * Continuous Stress Test - Runs for specified duration with fail-fast
 */
static void run_continuous_stress_test(int duration_seconds, unsigned int base_seed) {
    printf("\n");
    printf("================================================================\n");
    printf("  CONTINUOUS STRESS TEST (FAIL-FAST MODE)                      \n");
    printf("  Duration: %d seconds                                         \n", duration_seconds);
    printf("  Base Seed: %u                                                \n", base_seed);
    printf("  Press Ctrl+C to stop early                                   \n");
    printf("================================================================\n");

    signal(SIGINT, signal_handler);

    time_t start_time = time(NULL);
    time_t end_time = start_time + duration_seconds;
    uint64_t iteration = 0;
    uint64_t total_keys_inserted = 0;
    uint64_t total_keys_verified = 0;

    while (keep_running && time(NULL) < end_time) {
        iteration++;
        unsigned int iteration_seed = base_seed + iteration;
        unsigned int seed = iteration_seed;  // Working copy that will be modified

        // Vary batch size between 100-500 keys
        int batch_size = 100 + (rand_r(&seed) % 401);

        cleanup_test_db();

        data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
        if (!tree) {
            FAIL_FAST("Failed to create tree at iteration %lu", iteration);
        }

        // Allocate arrays for this batch
        uint8_t **keys = malloc(batch_size * sizeof(uint8_t *));
        size_t *key_lens = malloc(batch_size * sizeof(size_t));
        char **values = malloc(batch_size * sizeof(char *));

        // Hash table to track unique keys and their latest values
        key_entry_t *unique_keys = NULL;
        int unique_count = 0;

        // Insert phase
        for (int i = 0; i < batch_size; i++) {
            // Random key length 1-128
            key_lens[i] = 1 + (rand_r(&seed) % 128);
            keys[i] = malloc(key_lens[i]);
            generate_random_key(keys[i], key_lens[i], &seed);

            // Random value length 16-512
            values[i] = malloc(512);
            generate_random_value(values[i], 512, i, &seed);

            if (!data_art_insert(tree, keys[i], key_lens[i], values[i], strlen(values[i]))) {
                fprintf(stderr, "Iteration: %lu, Key: %d/%d, KeyLen: %zu\n",
                        iteration, i, batch_size, key_lens[i]);
                fprintf(stderr, "Seed: %u (use this to reproduce)\n", iteration_seed);
                FAIL_FAST("Insert failed at iteration %lu, key %d", iteration, i);
            }

            // Track this key in hash table (will update if duplicate)
            char hash_key[300];
            make_hash_key(keys[i], key_lens[i], hash_key, sizeof(hash_key));

            key_entry_t *entry = NULL;
            HASH_FIND_STR(unique_keys, hash_key, entry);

            if (entry) {
                // Duplicate key - update with latest value
                free(entry->value);
                entry->value = strdup(values[i]);
                entry->value_len = strlen(values[i]);
                entry->last_index = i;
            } else {
                // New unique key
                entry = malloc(sizeof(key_entry_t));
                strcpy(entry->hash_key, hash_key);
                entry->key = malloc(key_lens[i]);
                memcpy(entry->key, keys[i], key_lens[i]);
                entry->key_len = key_lens[i];
                entry->value = strdup(values[i]);
                entry->value_len = strlen(values[i]);
                entry->last_index = i;
                HASH_ADD_STR(unique_keys, hash_key, entry);
                unique_count++;
            }

            total_keys_inserted++;
        }

        // Verify phase - check all UNIQUE keys can be retrieved with LATEST values
        key_entry_t *entry, *tmp;
        HASH_ITER(hh, unique_keys, entry, tmp) {
            size_t value_len;
            const void *retrieved = data_art_get(tree, entry->key, entry->key_len, &value_len);

            if (!retrieved) {
                fprintf(stderr, "Iteration: %lu, Unique Key (last at index %d): KeyLen: %zu\n",
                        iteration, entry->last_index, entry->key_len);
                fprintf(stderr, "Seed: %u (use this to reproduce)\n", iteration_seed);

                // Dump all key-value pairs to file for debugging
                char dump_file[256];
                snprintf(dump_file, sizeof(dump_file), "/tmp/key_not_found_iter%lu_key%d_seed%u.txt",
                         iteration, entry->last_index, iteration_seed);
                FILE *dump = fopen(dump_file, "w");
                if (dump) {
                    fprintf(dump, "KEY NOT FOUND\n");
                    fprintf(dump, "Iteration: %lu, Missing Key (last at index %d)\n", iteration, entry->last_index);
                    fprintf(dump, "Seed: %u\n\n", iteration_seed);

                    fprintf(dump, "MISSING KEY:\n");
                    fprintf(dump, "  Key len=%zu, hex=", entry->key_len);
                    for (size_t k = 0; k < entry->key_len; k++) fprintf(dump, "%02x", entry->key[k]);
                    fprintf(dump, "\n  Expected value: %s\n\n", entry->value);

                    fprintf(dump, "ALL KEYS IN THIS BATCH:\n");
                    for (int j = 0; j < batch_size; j++) {
                        fprintf(dump, "Key[%d] len=%zu, hex=", j, key_lens[j]);
                        for (size_t k = 0; k < key_lens[j]; k++) fprintf(dump, "%02x", keys[j][k]);
                        fprintf(dump, ", value_len=%zu, value=%s\n", strlen(values[j]), values[j]);
                    }
                    fclose(dump);
                    fprintf(stderr, "Dumped all keys to: %s\n", dump_file);
                }

                FAIL_FAST("Key not found at iteration %lu, last at index %d", iteration, entry->last_index);
            }

            if (value_len != entry->value_len) {
                fprintf(stderr, "Iteration: %lu, Unique Key (last at index %d): KeyLen: %zu\n",
                        iteration, entry->last_index, entry->key_len);
                fprintf(stderr, "Expected value_len: %zu, Got: %zu\n", entry->value_len, value_len);
                fprintf(stderr, "Seed: %u (use this to reproduce)\n", iteration_seed);

                // Dump all key-value pairs to file for debugging
                char dump_file[256];
                snprintf(dump_file, sizeof(dump_file), "/tmp/corruption_iter%lu_key%d_seed%u.txt",
                         iteration, entry->last_index, iteration_seed);
                FILE *dump = fopen(dump_file, "w");
                if (dump) {
                    fprintf(dump, "CORRUPTION DETECTED\n");
                    fprintf(dump, "Iteration: %lu, Failing Key (last at index %d)\n", iteration, entry->last_index);
                    fprintf(dump, "Seed: %u\n\n", iteration_seed);

                    fprintf(dump, "FAILING KEY:\n");
                    fprintf(dump, "  Key len=%zu, hex=", entry->key_len);
                    for (size_t k = 0; k < entry->key_len; k++) fprintf(dump, "%02x", entry->key[k]);
                    fprintf(dump, "\n  Expected value_len=%zu, Got value_len=%zu\n", entry->value_len, value_len);
                    fprintf(dump, "  Expected value: %s\n", entry->value);
                    fprintf(dump, "  Retrieved value: %.*s\n\n", (int)value_len, (const char *)retrieved);

                    fprintf(dump, "ALL KEYS IN THIS BATCH:\n");
                    for (int j = 0; j < batch_size; j++) {
                        fprintf(dump, "Key[%d] len=%zu, hex=", j, key_lens[j]);
                        for (size_t k = 0; k < key_lens[j]; k++) fprintf(dump, "%02x", keys[j][k]);
                        fprintf(dump, ", value_len=%zu, value=%s\n", strlen(values[j]), values[j]);
                    }
                    fclose(dump);
                    fprintf(stderr, "Dumped all keys to: %s\n", dump_file);
                }

                free((void *)retrieved);
                FAIL_FAST("Value length mismatch at iteration %lu, last at index %d", iteration, entry->last_index);
            }

            if (memcmp(retrieved, entry->value, value_len) != 0) {
                fprintf(stderr, "Iteration: %lu, Unique Key (last at index %d): KeyLen: %zu\n",
                        iteration, entry->last_index, entry->key_len);
                fprintf(stderr, "Value content mismatch (lengths match: %zu bytes)\n", value_len);
                fprintf(stderr, "Seed: %u (use this to reproduce)\n", iteration_seed);
                free((void *)retrieved);
                FAIL_FAST("Value content mismatch at iteration %lu, last at index %d", iteration, entry->last_index);
            }

            free((void *)retrieved);
            total_keys_verified++;
        }

        // Verify tree size matches UNIQUE key count
        if (data_art_size(tree) != (size_t)unique_count) {
            FAIL_FAST("Tree size mismatch at iteration %lu: expected %d unique keys, got %zu",
                     iteration, unique_count, data_art_size(tree));
        }

        // Cleanup hash table
        HASH_ITER(hh, unique_keys, entry, tmp) {
            HASH_DEL(unique_keys, entry);
            free(entry->key);
            free(entry->value);
            free(entry);
        }

        // Cleanup
        for (int i = 0; i < batch_size; i++) {
            free(keys[i]);
            free(values[i]);
        }
        free(keys);
        free(key_lens);
        free(values);

        data_art_destroy(tree);

        // Progress update every 10 iterations
        if (iteration % 10 == 0) {
            time_t now = time(NULL);
            int elapsed = (int)(now - start_time);
            int remaining = duration_seconds - elapsed;
            printf("\r" COLOR_BLUE "[%ds/%ds]" COLOR_RESET " Iterations: %lu, Keys inserted: %lu, Keys verified: %lu  ",
                   elapsed, duration_seconds, iteration, total_keys_inserted, total_keys_verified);
            fflush(stdout);

            if (remaining <= 0) break;
        }
    }

    time_t actual_duration = time(NULL) - start_time;

    printf("\n\n");
    printf("================================================================\n");
    printf(COLOR_GREEN "✓ STRESS TEST PASSED" COLOR_RESET "\n");
    printf("  Duration: %ld seconds\n", actual_duration);
    printf("  Iterations: %lu\n", iteration);
    printf("  Total keys inserted: %lu\n", total_keys_inserted);
    printf("  Total keys verified: %lu\n", total_keys_verified);
    printf("  Average: %.1f keys/sec\n", (double)total_keys_inserted / actual_duration);
    printf("================================================================\n");

    system("rm -rf " TEST_DIR);
}

int main(int argc, char *argv[]) {
    // Check if continuous stress test mode
    if (argc > 1) {
        int duration = atoi(argv[1]);
        if (duration <= 0) {
            fprintf(stderr, "Usage: %s [duration_in_seconds] [seed]\n", argv[0]);
            fprintf(stderr, "  No argument: Run all individual tests\n");
            fprintf(stderr, "  With duration: Run continuous stress test for N seconds\n");
            fprintf(stderr, "  With seed: Base seed for reproducibility (default: current time)\n");
            return 1;
        }

        unsigned int base_seed = (unsigned int)time(NULL);
        if (argc > 2) {
            base_seed = (unsigned int)atoi(argv[2]);
        }

        run_continuous_stress_test(duration, base_seed);
        return 0;
    }

    // Run individual tests
    printf("\n");
    printf("================================================================\n");
    printf("  Persistent ART - Insert & Search Stress Tests                \n");
    printf("  All tests use fixed seeds for reproducibility                \n");
    printf("================================================================\n");

    int total = 0;
    int passed = 0;

    // Run all tests
    if (test_random_keys_varying_length()) passed++;
    total++;

    if (test_random_printable_keys()) passed++;
    total++;

    if (test_sequential_keys()) passed++;
    total++;

    if (test_common_prefix_keys()) passed++;
    total++;

    if (test_binary_keys_with_nulls()) passed++;
    total++;

    if (test_very_long_keys()) passed++;
    total++;

    if (test_update_keys()) passed++;
    total++;

    printf("\n");
    printf("================================================================\n");
    printf("Test Results: %d/%d passed\n", passed, total);
    printf("================================================================\n");
    printf("\n");

    system("rm -rf " TEST_DIR);

    return (passed == total) ? 0 : 1;
}
