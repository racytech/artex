/**
 * Reproduce a specific seed from continuous stress test
 * Usage: ./test_reproduce_seed <seed>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <stdbool.h>
#include "data_art.h"

#define TEST_DIR "/tmp/test_reproduce_seed"
#define TEST_ART_PATH TEST_DIR "/art.dat"

// Helper: Generate random key with all byte values
static void generate_random_key(uint8_t *key, size_t key_len, unsigned int *seed) {
    for (size_t i = 0; i < key_len; i++) {
        key[i] = rand_r(seed) % 256;
    }
}

// Helper: Generate random value with varying length
static void generate_random_value(char *value, size_t max_len, int index, unsigned int *seed) {
    size_t value_len = 8 + (rand_r(seed) % (max_len - 7));
    int prefix_len = snprintf(value, max_len, "val_%d_", index);

    if (prefix_len < 0 || prefix_len >= (int)max_len) {
        value[0] = '\0';
        return;
    }

    for (size_t i = prefix_len; i < value_len && i < max_len - 1; i++) {
        value[i] = 'a' + (rand_r(seed) % 26);
    }
    value[value_len < max_len ? value_len : max_len - 1] = '\0';
}

static void cleanup_test_db(void) {
    system("rm -rf " TEST_DIR " && mkdir -p " TEST_DIR);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <seed>\n", argv[0]);
        fprintf(stderr, "Example: %s 2072679742\n", argv[0]);
        return 1;
    }

    unsigned int iteration_seed = (unsigned int)atoi(argv[1]);
    unsigned int seed = iteration_seed;

    printf("Reproducing with seed: %u\n", iteration_seed);

    // Vary batch size between 100-500 keys
    int batch_size = 100 + (rand_r(&seed) % 401);
    printf("Batch size: %d\n", batch_size);

    cleanup_test_db();

    data_art_tree_t *tree = data_art_create(TEST_ART_PATH, 32);
    assert(tree != NULL);

    // Allocate arrays for this batch
    uint8_t **keys = malloc(batch_size * sizeof(uint8_t *));
    size_t *key_lens = malloc(batch_size * sizeof(size_t));
    char **values = malloc(batch_size * sizeof(char *));

    printf("Inserting %d keys...\n", batch_size);

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
            fprintf(stderr, "Insert failed at key %d/%d (len=%zu)\n", i, batch_size, key_lens[i]);
            return 1;
        }

        if ((i + 1) % 50 == 0) {
            printf("  Inserted %d keys\n", i + 1);
        }
    }

    printf("Tree size: %zu\n", data_art_size(tree));
    printf("Verifying all %d keys...\n", batch_size);

    // Verify phase
    for (int i = 0; i < batch_size; i++) {
        size_t value_len;
        const void *retrieved = data_art_get(tree, keys[i], key_lens[i], &value_len);

        if (!retrieved) {
            fprintf(stderr, "\n✗ Key %d NOT FOUND (len=%zu)\n", i, key_lens[i]);
            fprintf(stderr, "Key bytes: ");
            for (size_t j = 0; j < key_lens[i] && j < 32; j++) {
                fprintf(stderr, "%02x ", keys[i][j]);
            }
            if (key_lens[i] > 32) fprintf(stderr, "... (%zu bytes total)", key_lens[i]);
            fprintf(stderr, "\n");

            // Try to search for some nearby keys to see tree state
            fprintf(stderr, "\nChecking nearby keys:\n");
            for (int j = (i > 5 ? i - 5 : 0); j < i && j < batch_size; j++) {
                size_t test_len;
                const void *test = data_art_get(tree, keys[j], key_lens[j], &test_len);
                fprintf(stderr, "  Key %d: %s\n", j, test ? "FOUND" : "NOT FOUND");
                if (test) free((void *)test);
            }

            return 1;
        }

        if (value_len != strlen(values[i])) {
            fprintf(stderr, "\n✗ Value length mismatch at key %d: expected %zu, got %zu\n",
                    i, strlen(values[i]), value_len);
            free((void *)retrieved);
            return 1;
        }

        if (memcmp(retrieved, values[i], value_len) != 0) {
            fprintf(stderr, "\n✗ Value content mismatch at key %d\n", i);
            free((void *)retrieved);
            return 1;
        }

        free((void *)retrieved);

        if ((i + 1) % 50 == 0) {
            printf("  Verified %d keys\n", i + 1);
        }
    }

    printf("\n✓ All %d keys verified successfully!\n", batch_size);

    // Cleanup
    for (int i = 0; i < batch_size; i++) {
        free(keys[i]);
        free(values[i]);
    }
    free(keys);
    free(key_lens);
    free(values);

    data_art_destroy(tree);
    system("rm -rf " TEST_DIR);

    return 0;
}
