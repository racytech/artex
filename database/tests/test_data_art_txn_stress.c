/**
 * Transaction-Based Stress Test for Persistent ART
 * 
 * Uses atomic multi-key updates via transaction buffer to test:
 * - Batched inserts within transactions
 * - Batched deletes within transactions
 * - Mixed insert/delete operations in single transaction
 * - Random commit/abort to test rollback semantics
 * - Verification that all operations are atomic
 * 
 * FIXED-SIZE KEYS: Uses either 20-byte (address) or 32-byte (hash) keys
 * to match Ethereum use case. Variable-length values.
 * 
 * Usage:
 *   ./test_data_art_txn_stress <seconds> [use_buffer_pool] [key_size]
 *   key_size: 20 (address) or 32 (hash), default=32
 */

#include "data_art.h"
#include "txn_buffer.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "wal.h"
#include "logger.h"
#include "../third_party/uthash.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#define TEST_DB_PATH "/tmp/test_data_art_txn_stress.db"
#define TEST_WAL_PATH "/tmp/test_data_art_txn_stress_wal"

// ANSI color codes
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_BLUE   "\033[0;34m"
#define COLOR_CYAN   "\033[0;36m"
#define COLOR_MAGENTA "\033[0;35m"
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
    uint64_t total_commits;
    uint64_t total_aborts;
    uint64_t iterations;
} stats_t;

// Helper: Clean up test database
static void cleanup_test_db(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s %s", TEST_DB_PATH, TEST_WAL_PATH);
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
 * Transaction-based stress test with batched operations
 * key_size: 20 for addresses, 32 for hashes
 */
static void run_txn_stress_test(int duration_seconds, bool use_buffer_pool, unsigned int base_seed, size_t key_size) {
    // Validate key size
    if (key_size != 20 && key_size != 32) {
        FAIL_FAST("Invalid key size %zu - must be 20 (address) or 32 (hash)", key_size);
    }
    
    // Reduce logging noise
    log_set_level(LOG_LEVEL_ERROR);
    
    printf("\n");
    printf("================================================================\n");
    printf("  TRANSACTION STRESS TEST - ATOMIC MULTI-KEY UPDATES           \n");
    printf("  Duration: %d seconds                                         \n", duration_seconds);
    printf("  Key Size: %zu bytes (%s)                                     \n", key_size, key_size == 20 ? "Address" : "Hash");
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
    
    // Create WAL for durability
    wal_config_t wal_config = wal_default_config();
    wal_config.segment_size = 8 * 1024 * 1024;  // 8MB segments
    wal_t *wal = wal_open(TEST_WAL_PATH, &wal_config);
    if (!wal) {
        if (bp) buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL_FAST("Failed to create WAL");
    }
    
    data_art_tree_t *tree = data_art_create(pm, bp, wal, key_size);
    if (!tree) {
        wal_close(wal);
        if (bp) buffer_pool_destroy(bp);
        page_manager_destroy(pm);
        FAIL_FAST("Failed to create tree");
    }
    
    // Track all keys ever inserted
    key_entry_t *all_keys = NULL;
    int next_key_index = 0;
    
    // Track keys pending in current transaction
    key_entry_t *pending_keys = NULL;
    
    while (keep_running && time(NULL) < end_time) {
        stats.iterations++;
        unsigned int seed = base_seed + stats.iterations;
        
        // Checkpoint every 10 iterations to test recovery
        if (stats.iterations % 10 == 0) {
            uint64_t checkpoint_lsn;
            if (data_art_checkpoint(tree, &checkpoint_lsn)) {
                printf(COLOR_CYAN "Checkpoint created at LSN %lu (iteration %lu)" COLOR_RESET "\n",
                       checkpoint_lsn, stats.iterations);
            }
        }
        
        // =================================================================
        // TRANSACTION BEGIN
        // =================================================================
        uint64_t txn_id;
        if (!data_art_begin_txn(tree, &txn_id)) {
            FAIL_FAST("Failed to begin transaction at iteration %lu", stats.iterations);
        }
        
        // Clear pending keys tracker
        key_entry_t *entry, *tmp;
        HASH_ITER(hh, pending_keys, entry, tmp) {
            HASH_DEL(pending_keys, entry);
            free(entry->key);
            free(entry->value);
            free(entry);
        }
        pending_keys = NULL;
        
        // Phase 1: BATCH INSERT within transaction (20-100 keys)
        int insert_batch = 20 + (rand_r(&seed) % 81);
        
        for (int i = 0; i < insert_batch; i++) {
            uint8_t *key = malloc(key_size);
            generate_random_key(key, key_size, &seed);
            
            size_t max_value_len = 512;
            char *value = malloc(max_value_len);
            generate_random_value(value, max_value_len, next_key_index, &seed);
            
            if (!data_art_insert(tree, key, key_size, value, strlen(value))) {
                FAIL_FAST("Insert (txn) failed at iteration %lu, key %d", stats.iterations, i);
            }
            
            // Track this key as pending
            char hash_key[300];
            make_hash_key(key, key_size, hash_key, sizeof(hash_key));
            
            key_entry_t *pending = malloc(sizeof(key_entry_t));
            strcpy(pending->hash_key, hash_key);
            pending->key = malloc(key_size);
            memcpy(pending->key, key, key_size);
            pending->key_len = key_size;
            pending->value = strdup(value);
            pending->value_len = strlen(value);
            pending->deleted = false;
            pending->insert_index = next_key_index;
            pending->delete_index = -1;
            HASH_ADD_STR(pending_keys, hash_key, pending);
            
            free(key);
            free(value);
            next_key_index++;
            stats.total_inserts++;
        }
        
        // Phase 2: BATCH DELETE within same transaction (10-30% of active keys)
        int active_count = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (!entry->deleted) active_count++;
        }
        
        if (active_count > 10) {
            int delete_count = (active_count * (10 + (rand_r(&seed) % 21))) / 100;  // 10-30%
            int deleted = 0;
            
            HASH_ITER(hh, all_keys, entry, tmp) {
                if (deleted >= delete_count) break;
                if (entry->deleted) continue;
                
                // Delete this key within transaction
                if (!data_art_delete(tree, entry->key, entry->key_len)) {
                    FAIL_FAST("Delete (txn) failed! Iteration %lu, key index %d",
                             stats.iterations, entry->insert_index);
                }
                
                // Track deletion in pending
                char hash_key[300];
                make_hash_key(entry->key, entry->key_len, hash_key, sizeof(hash_key));
                
                key_entry_t *pending = malloc(sizeof(key_entry_t));
                strcpy(pending->hash_key, hash_key);
                pending->key = malloc(entry->key_len);
                memcpy(pending->key, entry->key, entry->key_len);
                pending->key_len = entry->key_len;
                pending->value = NULL;
                pending->value_len = 0;
                pending->deleted = true;
                pending->insert_index = entry->insert_index;
                pending->delete_index = deleted;
                HASH_ADD_STR(pending_keys, hash_key, pending);
                
                deleted++;
                stats.total_deletes++;
            }
        }
        
        // Verify tree size is unchanged during transaction (operations buffered)
        size_t tree_size_before_commit = data_art_size(tree);
        int expected_size_before = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (!entry->deleted) expected_size_before++;
        }
        
        if (tree_size_before_commit != (size_t)expected_size_before) {
            FAIL_FAST("Tree modified before commit! Expected %d, got %zu at iteration %lu",
                     expected_size_before, tree_size_before_commit, stats.iterations);
        }
        
        // =================================================================
        // RANDOM COMMIT OR ABORT (90% commit, 10% abort)
        // =================================================================
        bool should_commit = (rand_r(&seed) % 100) < 90;
        
        if (should_commit) {
            // COMMIT - apply all operations atomically
            if (!data_art_commit_txn(tree)) {
                FAIL_FAST("Failed to commit transaction at iteration %lu", stats.iterations);
            }
            stats.total_commits++;
            
            // Apply pending operations to tracking
            HASH_ITER(hh, pending_keys, entry, tmp) {
                key_entry_t *tracked = NULL;
                HASH_FIND_STR(all_keys, entry->hash_key, tracked);
                
                if (entry->deleted) {
                    // Mark as deleted
                    if (tracked) {
                        tracked->deleted = true;
                        tracked->delete_index = entry->delete_index;
                    }
                } else {
                    // Insert/update
                    if (tracked) {
                        // Update existing
                        free(tracked->value);
                        tracked->value = strdup(entry->value);
                        tracked->value_len = entry->value_len;
                        tracked->deleted = false;
                        tracked->insert_index = entry->insert_index;
                    } else {
                        // New key
                        key_entry_t *new_entry = malloc(sizeof(key_entry_t));
                        strcpy(new_entry->hash_key, entry->hash_key);
                        new_entry->key = malloc(entry->key_len);
                        memcpy(new_entry->key, entry->key, entry->key_len);
                        new_entry->key_len = entry->key_len;
                        new_entry->value = strdup(entry->value);
                        new_entry->value_len = entry->value_len;
                        new_entry->deleted = false;
                        new_entry->insert_index = entry->insert_index;
                        new_entry->delete_index = -1;
                        HASH_ADD_STR(all_keys, hash_key, new_entry);
                    }
                }
            }
        } else {
            // ABORT - discard all operations
            if (!data_art_abort_txn(tree)) {
                FAIL_FAST("Failed to abort transaction at iteration %lu", stats.iterations);
            }
            stats.total_aborts++;
            
            // Don't update tracking - operations were rolled back
            printf(COLOR_YELLOW "Aborted transaction (iteration %lu, %d inserts, %d deletes rolled back)" COLOR_RESET "\n",
                   stats.iterations, insert_batch, 
                   HASH_COUNT(pending_keys) - insert_batch);
        }
        
        // =================================================================
        // VERIFICATION: Ensure atomicity guarantees hold
        // =================================================================
        
        // Verify all non-deleted tracked keys exist in tree
        int search_count = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (entry->deleted) {
                // Should NOT exist
                size_t value_len;
                const void *retrieved = data_art_get(tree, entry->key, entry->key_len, &value_len);
                if (retrieved != NULL) {
                    fprintf(stderr, "\n=== ATOMICITY VIOLATION ===");
                    fprintf(stderr, "\nDeleted key found: insert_index=%d, delete_index=%d",
                            entry->insert_index, entry->delete_index);
                    fprintf(stderr, "\nKey (first 32 bytes): ");
                    for (size_t i = 0; i < entry->key_len && i < 32; i++) {
                        fprintf(stderr, "%02x ", entry->key[i]);
                    }
                    fprintf(stderr, "\n===========================\n");
                    free((void *)retrieved);
                    FAIL_FAST("Deleted key still exists after transaction! Iteration %lu", stats.iterations);
                }
            } else {
                // Should exist
                size_t value_len;
                const void *retrieved = data_art_get(tree, entry->key, entry->key_len, &value_len);
                if (!retrieved) {
                    FAIL_FAST("Key not found after transaction! Iteration %lu, insert_index=%d",
                             stats.iterations, entry->insert_index);
                }
                
                if (value_len != entry->value_len || memcmp(retrieved, entry->value, value_len) != 0) {
                    free((void *)retrieved);
                    FAIL_FAST("Value mismatch after transaction! Iteration %lu, insert_index=%d",
                             stats.iterations, entry->insert_index);
                }
                
                free((void *)retrieved);
                search_count++;
            }
            stats.total_searches++;
        }
        
        // Verify tree size matches expected
        int expected_size = 0;
        HASH_ITER(hh, all_keys, entry, tmp) {
            if (!entry->deleted) expected_size++;
        }
        
        if (data_art_size(tree) != (size_t)expected_size) {
            FAIL_FAST("Tree size mismatch after transaction! Expected %d, got %zu at iteration %lu",
                     expected_size, data_art_size(tree), stats.iterations);
        }
        
        // Progress update
        time_t now = time(NULL);
        if (now - last_status >= 15) {
            int elapsed = now - start_time;
            printf("[%02d:%02d/%02d:%02d] Iter: %4lu | Commits: %4lu | Aborts: %3lu | Inserts: %7lu | Deletes: %6lu | Active: %5d\n",
                   elapsed / 60, elapsed % 60,
                   duration_seconds / 60, duration_seconds % 60,
                   stats.iterations, stats.total_commits, stats.total_aborts,
                   stats.total_inserts, stats.total_deletes, expected_size);
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
    
    HASH_ITER(hh, pending_keys, entry, tmp) {
        HASH_DEL(pending_keys, entry);
        free(entry->key);
        if (entry->value) free(entry->value);
        free(entry);
    }
    
    data_art_destroy(tree);
    wal_close(wal);
    if (bp) buffer_pool_destroy(bp);
    page_manager_destroy(pm);
    cleanup_test_db();
    
    printf("\n");
    printf("================================================================\n");
    printf(COLOR_GREEN "✓ TRANSACTION STRESS TEST PASSED" COLOR_RESET "\n");
    printf("  Duration: %ld seconds\n", total_duration);
    printf("  Iterations: %lu\n", stats.iterations);
    printf("  Total commits: %lu (%.1f%%)\n", stats.total_commits, 
           100.0 * stats.total_commits / (stats.total_commits + stats.total_aborts));
    printf("  Total aborts: %lu (%.1f%%)\n", stats.total_aborts,
           100.0 * stats.total_aborts / (stats.total_commits + stats.total_aborts));
    printf("  Total inserts: %lu\n", stats.total_inserts);
    printf("  Total deletes: %lu\n", stats.total_deletes);
    printf("  Total searches: %lu\n", stats.total_searches);
    printf("  Final active keys: %d\n", active_keys);
    printf("  Final deleted keys: %d\n", deleted_keys);
    printf("  Average throughput: %.1f ops/sec\n", 
           (double)(stats.total_inserts + stats.total_searches + stats.total_deletes) / total_duration);
    printf("================================================================\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <duration_seconds> [use_buffer_pool] [seed] [key_size]\n", argv[0]);
        fprintf(stderr, "  duration_seconds: How long to run the test\n");
        fprintf(stderr, "  use_buffer_pool: 1 to enable, 0 to disable (default: 0)\n");
        fprintf(stderr, "  seed: Random seed for reproducibility (default: current time)\n");
        fprintf(stderr, "  key_size: 20 (address) or 32 (hash) bytes (default: 32)\n");
        fprintf(stderr, "\nNote: Uses transaction buffer for atomic multi-key updates\n");
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
    
    size_t key_size = 32;  // Default: 32-byte hash
    if (argc > 4) {
        key_size = (size_t)atoi(argv[4]);
        if (key_size != 20 && key_size != 32) {
            fprintf(stderr, "Invalid key_size: %zu - must be 20 or 32\n", key_size);
            return 1;
        }
    }
    
    run_txn_stress_test(duration, use_buffer_pool, base_seed, key_size);
    return 0;
}
