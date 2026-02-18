/**
 * Comprehensive Stress Test for Persistent ART - Insert, Search, Delete
 * 
 * FAIL-FAST MODE: Tests all operations in realistic patterns:
 * - Insert random keys
 * - Search to verify all keys exist
 * - Delete random subset of keys
 * - Verify deleted keys are gone and remaining keys still exist
 * - Repeat with new keys
 * 
 * Usage:
 *   ./test_data_art_full_stress <seconds> [use_buffer_pool]
 */

#include "data_art.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "logger.h"
#include "../third_party/uthash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#define TEST_DB_PATH "/tmp/test_data_art_full_stress.db"

// ANSI color codes
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_BLUE   "\033[0;34m"
#define COLOR_CYAN   "\033[0;36m"
#define COLOR_RESET  "\033[0m"

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

// Key tracking structure
typedef struct key_entry {
    char hash_key[300];
    uint8_t *key;
    size_t key_len;
    char *value;
    size_t value_len;
    bool deleted;  // Track if this key has been deleted
    int insert_index;
    int delete_index;  // -1 if not deleted
    UT_hash_handle hh;
} key_entry_t;

// Statistics
typedef struct {
    uint64_t total_inserts;
    uint64_t total_searches;
    uint64_t total_deletes;
    uint64_t iterations;
} stats_t;

// Helper: Clean up test database
static void cleanup_test_db(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DB_PATH);
    system(cmd);
    sync();
    usleep(10000);
}

// Helper: Generate random key
static void generate_random_key(uint8_t *key, size_t len, unsigned int *seed) {
    for (size_t i = 0; i < len; i++) {
        key[i] = (uint8_t)(rand_r(seed) % 256);
    }
}

// Helper: Generate random value
static void generate_random_value(char *value, size_t max_len, int index, unsigned int *seed) {
    size_t value_len = 16 + (rand_r(seed) % (max_len - 16));
    snprintf(value, value_len + 1, "val_%d_", index);
    size_t prefix_len = strlen(value);
    
    for (size_t i = prefix_len; i < value_len; i++) {
        value[i] = 33 + (rand_r(seed) % 94);  // Printable ASCII
    }
    value[value_len] = '\0';
}

// Helper: Create hash key
static void make_hash_key(const uint8_t *key, size_t key_len, char *hash_key, size_t hash_key_size) {
    size_t offset = 0;
    for (size_t i = 0; i < key_len && offset + 2 < hash_key_size; i++) {
        offset += snprintf(hash_key + offset, hash_key_size - offset, "%02x", key[i]);
    }
    hash_key[offset] = '\0';
}

/**
 * Main stress test with insert, search, and delete
 */
static void run_full_stress_test(int duration_seconds, bool use_buffer_pool, unsigned int base_seed) {
    printf("\n");
    printf("================================================================\n");
    printf("  FULL STRESS TEST - INSERT + SEARCH + DELETE                  \n");
    printf("  Duration: %d seconds                                         \n", duration_seconds);
    printf("  Buffer Pool: %s                                              \n", use_buffer_pool ? "ENABLED" : "DISABLED");
    printf("  Press Ctrl+C to stop early                                   \n");
    printf("================================================================\n");
    
    signal(SIGINT, signal_handler);
    
    time_t start_time = time(NULL);
    time_t end_time = start_time + duration_seconds;
    time_t last_status = start_time;
    
    printf("Base seed: %u\n\n", base_seed);
    
    stats_t stats = {0};
    
    cleanup_test_db();
    
    // Create persistent tree
    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    if (!pm) {
        FAIL_FAST("Failed to create page manager");
    }
    
    buffer_pool_t *bp = NULL;
    if (use_buffer_pool) {
        buffer_pool_config_t config = buffer_pool_default_config();
        config.capacity = 1000;
        bp = buffer_pool_create(&config, pm);
        if (!bp) {
            page_manager_destroy(pm);
            FAIL_FAST("Failed to create buffer pool");
        }
    }
    
    data_art_tree_t *tree = data_art_create(pm, bp);
    if (!tree) {
        if (bp) buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL_FAST("Failed to create tree");
    }
    
    // Track all keys ever inserted
    key_entry_t *all_keys = NULL;
    int next_key_index = 0;
    
    while (keep_running && time(NULL) < end_time) {
        stats.iterations++;
        unsigned int seed = base_seed + stats.iterations;
        
        // Phase 1: INSERT new keys
        int insert_batch = 50 + (rand_r(&seed) % 151);  // 50-200 keys
        
        for (int i = 0; i < insert_batch; i++) {
            size_t key_len = 1 + (rand_r(&seed) % 128);
            uint8_t *key = malloc(key_len);
            generate_random_key(key, key_len, &seed);
            
            char *value = malloc(512);
            generate_random_value(value, 512, next_key_index, &seed);
            
            if (!data_art_insert(tree, key, key_len, value, strlen(value))) {
                FAIL_FAST("Insert failed at iteration %lu, key %d", stats.iterations, i);
            }
            
            // Track this key
            char hash_key[300];
            make_hash_key(key, key_len, hash_key, sizeof(hash_key));
            
            key_entry_t *entry = NULL;
            HASH_FIND_STR(all_keys, hash_key, entry);
            
            if (entry) {
                // Duplicate - update value
                free(entry->value);
                entry->value = strdup(value);
                entry->value_len = strlen(value);
            } else {
                // New key
                entry = malloc(sizeof(key_entry_t));
                strcpy(entry->hash_key, hash_key);
                entry->key = malloc(key_len);
                memcpy(entry->key, key, key_len);
                entry->key_len = key_len;
                entry->value = strdup(value);
                entry->value_len = strlen(value);
                entry->deleted = false;
                entry->insert_index = next_key_index;
                entry->delete_index = -1;
                HASH_ADD_STR(all_keys, hash_key, entry);
            }
            
            free(key);
            free(value);
            next_key_index++;
            stats.total_inserts++;
        }
        
        // Phase 2: SEARCH - verify all non-deleted keys exist
        int search_count = 0;
        key_entry_t *entry, *tmp;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (entry->deleted) {
                // Should NOT exist
                size_t value_len;
                const void *retrieved = data_art_get(tree, entry->key, entry->key_len, &value_len);
                if (retrieved != NULL) {
                    free((void *)retrieved);
                    FAIL_FAST("Deleted key still exists! Iteration %lu, deleted at index %d",
                             stats.iterations, entry->delete_index);
                }
            } else {
                // Should exist
                size_t value_len;
                const void *retrieved = data_art_get(tree, entry->key, entry->key_len, &value_len);
                if (!retrieved) {
                    FAIL_FAST("Key not found! Iteration %lu, inserted at index %d",
                             stats.iterations, entry->insert_index);
                }
                
                if (value_len != entry->value_len || memcmp(retrieved, entry->value, value_len) != 0) {
                    free((void *)retrieved);
                    FAIL_FAST("Value mismatch! Iteration %lu, key index %d",
                             stats.iterations, entry->insert_index);
                }
                
                free((void *)retrieved);
                search_count++;
            }
            stats.total_searches++;
        }
        
        // Phase 3: DELETE random subset of keys (20-40%)
        int active_count = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (!entry->deleted) active_count++;
        }
        
        if (active_count > 10) {
            int delete_count = (active_count * (20 + (rand_r(&seed) % 21))) / 100;  // 20-40%
            int deleted = 0;
            
            HASH_ITER(hh, all_keys, entry, tmp) {
                if (deleted >= delete_count) break;
                if (entry->deleted) continue;
                
                // Delete this key
                if (!data_art_delete(tree, entry->key, entry->key_len)) {
                    FAIL_FAST("Delete failed! Iteration %lu, key index %d",
                             stats.iterations, entry->insert_index);
                }
                
                entry->deleted = true;
                entry->delete_index = deleted;
                deleted++;
                stats.total_deletes++;
            }
        }
        
        // Phase 4: Verify tree size matches active keys
        int expected_size = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (!entry->deleted) expected_size++;
        }
        
        if (data_art_size(tree) != (size_t)expected_size) {
            FAIL_FAST("Tree size mismatch! Expected %d, got %zu at iteration %lu",
                     expected_size, data_art_size(tree), stats.iterations);
        }
        
        // Progress update
        time_t now = time(NULL);
        if (now - last_status >= 15) {
            int elapsed = now - start_time;
            printf("[%02d:%02d/%02d:%02d] Iter: %4lu | Inserts: %7lu | Searches: %7lu | Deletes: %6lu | Active Keys: %5d\n",
                   elapsed / 60, elapsed % 60,
                   duration_seconds / 60, duration_seconds % 60,
                   stats.iterations, stats.total_inserts, stats.total_searches, 
                   stats.total_deletes, expected_size);
            last_status = now;
        }
    }
    
    // Final summary
    int active_keys = 0;
    int deleted_keys = 0;
    key_entry_t *entry, *tmp;
    HASH_ITER(hh, all_keys, entry, tmp) {
        if (entry->deleted) deleted_keys++;
        else active_keys++;
    }
    
    time_t total_duration = time(NULL) - start_time;
    
    // Cleanup
    HASH_ITER(hh, all_keys, entry, tmp) {
        HASH_DEL(all_keys, entry);
        free(entry->key);
        free(entry->value);
        free(entry);
    }
    
    data_art_destroy(tree);
    if (bp) buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    cleanup_test_db();
    
    printf("\n");
    printf("================================================================\n");
    printf(COLOR_GREEN "✓ STRESS TEST PASSED" COLOR_RESET "\n");
    printf("  Duration: %ld seconds\n", total_duration);
    printf("  Iterations: %lu\n", stats.iterations);
    printf("  Total inserts: %lu\n", stats.total_inserts);
    printf("  Total searches: %lu\n", stats.total_searches);
    printf("  Total deletes: %lu\n", stats.total_deletes);
    printf("  Final active keys: %d\n", active_keys);
    printf("  Final deleted keys: %d\n", deleted_keys);
    printf("  Average throughput: %.1f ops/sec\n", 
           (double)(stats.total_inserts + stats.total_searches + stats.total_deletes) / total_duration);
    printf("================================================================\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <duration_seconds> [use_buffer_pool] [seed]\n", argv[0]);
        fprintf(stderr, "  duration_seconds: How long to run the test\n");
        fprintf(stderr, "  use_buffer_pool: 1 to enable, 0 to disable (default: 0)\n");
        fprintf(stderr, "  seed: Random seed for reproducibility (default: current time)\n");
        return 1;
    }
    
    int duration = atoi(argv[1]);
    if (duration <= 0) {
        fprintf(stderr, "Duration must be positive\n");
        return 1;
    }
    
    bool use_buffer_pool = false;
    if (argc > 2) {
        use_buffer_pool = (atoi(argv[2]) != 0);
    }
    
    unsigned int base_seed = (unsigned int)time(NULL);
    if (argc > 3) {
        base_seed = (unsigned int)atoi(argv[3]);
    }
    
    run_full_stress_test(duration, use_buffer_pool, base_seed);
    return 0;
}
