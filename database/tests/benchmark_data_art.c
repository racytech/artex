/**
 * Benchmark: data_art - Performance Testing
 * 
 * Benchmarks for measuring insertion and lookup performance
 */

#include "data_art.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#define BENCHMARK_DB_PATH "/tmp/benchmark_data_art.db"

// Cleanup benchmark database
static void cleanup_benchmark_db(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", BENCHMARK_DB_PATH);
    system(cmd);
}

// Get current time in microseconds
static uint64_t get_time_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

// Format number with thousand separators
static void format_number(uint64_t num, char *buf, size_t buf_size) {
    if (num < 1000) {
        snprintf(buf, buf_size, "%lu", num);
        return;
    }
    
    char temp[64];
    snprintf(temp, sizeof(temp), "%lu", num);
    int len = strlen(temp);
    int pos = 0;
    
    for (int i = 0; i < len; i++) {
        if (i > 0 && (len - i) % 3 == 0) {
            buf[pos++] = ',';
        }
        buf[pos++] = temp[i];
    }
    buf[pos] = '\0';
}

// Print benchmark header
static void print_header(const char *title) {
    printf("\n");
    printf("================================================================\n");
    printf("%s\n", title);
    printf("================================================================\n");
}

// Print benchmark result
static void print_result(const char *operation, int num_keys, uint64_t elapsed_us) {
    char num_str[32];
    format_number(num_keys, num_str, sizeof(num_str));
    
    double elapsed_s = elapsed_us / 1000000.0;
    double ops_per_sec = num_keys / elapsed_s;
    double us_per_op = (double)elapsed_us / num_keys;
    
    char ops_str[32];
    format_number((uint64_t)ops_per_sec, ops_str, sizeof(ops_str));
    
    printf("  %-20s: %s keys\n", operation, num_str);
    printf("  %-20s: %.3f seconds\n", "Total time", elapsed_s);
    printf("  %-20s: %s ops/sec\n", "Throughput", ops_str);
    printf("  %-20s: %.2f µs/op\n", "Latency", us_per_op);
}

/**
 * Benchmark: Sequential key insertion
 */
static void benchmark_sequential_insert(int num_keys) {
    print_header("BENCHMARK: Sequential Key Insertion");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Inserting %d sequential keys...\n", num_keys);
    
    uint64_t start = get_time_us();
    
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "seq_key_%010d", i);
        
        char value[64];
        int value_len = snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, key_len, value, value_len)) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Sequential insert", num_keys, elapsed);
    
    printf("  %-20s: %zu\n", "Final tree size", data_art_size(tree));
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

/**
 * Benchmark: Random key insertion
 */
static void benchmark_random_insert(int num_keys) {
    print_header("BENCHMARK: Random Key Insertion");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Inserting %d random keys...\n", num_keys);
    
    // Seed for reproducible results
    srand(42);
    
    uint64_t start = get_time_us();
    
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "random_%010d_%08d", i, rand());
        
        char value[64];
        int value_len = snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, key_len, value, value_len)) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Random insert", num_keys, elapsed);
    
    printf("  %-20s: %zu\n", "Final tree size", data_art_size(tree));
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

/**
 * Benchmark: Sequential key lookup
 */
static void benchmark_sequential_lookup(int num_keys) {
    print_header("BENCHMARK: Sequential Key Lookup");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Setting up: inserting %d keys...\n", num_keys);
    
    // First insert all keys
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "seq_key_%010d", i);
        
        char value[64];
        int value_len = snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, key_len, value, value_len)) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
    }
    
    printf("  Looking up %d sequential keys...\n", num_keys);
    
    uint64_t start = get_time_us();
    
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "seq_key_%010d", i);
        
        size_t value_len;
        const void *value = data_art_get(tree, (const uint8_t *)key, key_len, &value_len);
        
        if (!value) {
            fprintf(stderr, "Failed to lookup key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
        
        free((void *)value);
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Sequential lookup", num_keys, elapsed);
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

/**
 * Benchmark: Random key lookup
 */
static void benchmark_random_lookup(int num_keys) {
    print_header("BENCHMARK: Random Key Lookup");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Setting up: inserting %d keys...\n", num_keys);
    
    // First insert all keys
    srand(42);
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "random_%010d_%08d", i, rand());
        
        char value[64];
        int value_len = snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, key_len, value, value_len)) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
    }
    
    printf("  Looking up %d random keys...\n", num_keys);
    
    uint64_t start = get_time_us();
    
    // Reset seed to get same sequence
    srand(42);
    for (int i = 0; i < num_keys; i++) {
        char key[64];
        int key_len = snprintf(key, sizeof(key), "random_%010d_%08d", i, rand());
        
        size_t value_len;
        const void *value = data_art_get(tree, (const uint8_t *)key, key_len, &value_len);
        
        if (!value) {
            fprintf(stderr, "Failed to lookup key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
        
        free((void *)value);
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Random lookup", num_keys, elapsed);
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

/**
 * Benchmark: Mixed operations (50% insert, 50% lookup)
 */
static void benchmark_mixed_operations(int num_keys) {
    print_header("BENCHMARK: Mixed Operations (50% insert, 50% lookup)");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Running %d mixed operations...\n", num_keys);
    
    srand(42);
    
    uint64_t start = get_time_us();
    
    int lookups = 0;
    int inserts = 0;
    
    for (int i = 0; i < num_keys; i++) {
        // 50% chance of lookup (if keys exist), 50% insert
        if (i > 0 && (rand() % 2 == 0)) {
            // Lookup random existing key
            int lookup_idx = rand() % i;
            char key[64];
            int key_len = snprintf(key, sizeof(key), "mixed_key_%010d", lookup_idx);
            
            size_t value_len;
            const void *value = data_art_get(tree, (const uint8_t *)key, key_len, &value_len);
            if (value) {
                free((void *)value);
                lookups++;
            }
        } else {
            // Insert new key
            char key[64];
            int key_len = snprintf(key, sizeof(key), "mixed_key_%010d", i);
            
            char value[64];
            int value_len = snprintf(value, sizeof(value), "value_%d", i);
            
            if (data_art_insert(tree, (const uint8_t *)key, key_len, value, value_len)) {
                inserts++;
            }
        }
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Mixed operations", num_keys, elapsed);
    
    printf("  %-20s: %d inserts, %d lookups\n", "Operation breakdown", inserts, lookups);
    printf("  %-20s: %zu\n", "Final tree size", data_art_size(tree));
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

/**
 * Benchmark: Variable key sizes
 */
static void benchmark_variable_key_sizes(int num_keys) {
    print_header("BENCHMARK: Variable Key Sizes");
    
    cleanup_benchmark_db();
    
    page_manager_t *pm = page_manager_create(BENCHMARK_DB_PATH, false);
    if (!pm) {
        fprintf(stderr, "Failed to create page manager\n");
        return;
    }
    
    data_art_tree_t *tree = data_art_create(pm, NULL, 32);
    if (!tree) {
        page_manager_destroy(pm);
        fprintf(stderr, "Failed to create tree\n");
        return;
    }
    
    printf("  Inserting %d keys with variable sizes (8-128 bytes)...\n", num_keys);
    
    srand(42);
    
    uint64_t start = get_time_us();
    
    for (int i = 0; i < num_keys; i++) {
        // Random key size between 8 and 128 bytes
        int key_size = 8 + (rand() % 121);
        
        char key[256];
        int pos = snprintf(key, sizeof(key), "var_%010d_", i);
        
        // Fill rest with random characters
        for (int j = pos; j < key_size - 1; j++) {
            key[j] = 'a' + (rand() % 26);
        }
        key[key_size - 1] = '\0';
        
        char value[64];
        int value_len = snprintf(value, sizeof(value), "value_%d", i);
        
        if (!data_art_insert(tree, (const uint8_t *)key, key_size - 1, value, value_len)) {
            fprintf(stderr, "Failed to insert key %d\n", i);
            data_art_destroy(tree);
            page_manager_destroy(pm);
            return;
        }
    }
    
    uint64_t end = get_time_us();
    uint64_t elapsed = end - start;
    
    print_result("Variable key insert", num_keys, elapsed);
    
    printf("  %-20s: %zu\n", "Final tree size", data_art_size(tree));
    
    data_art_destroy(tree);
    page_manager_destroy(pm);
}

int main(int argc, char **argv) {
    // Show help
    if (argc > 1 && (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0)) {
        printf("\n");
        printf("Usage: %s [BENCHMARK] [NUM_KEYS]\n", argv[0]);
        printf("\n");
        printf("Benchmarks:\n");
        printf("  insert     - Sequential and random insertion benchmarks\n");
        printf("  lookup     - Sequential and random lookup benchmarks\n");
        printf("  mixed      - Mixed insert/lookup operations (50/50)\n");
        printf("  variable   - Variable key size benchmark\n");
        printf("  quick      - Quick benchmark with 10K keys\n");
        printf("  [NUM_KEYS] - Run all benchmarks with specified number of keys\n");
        printf("  (default)  - Run all benchmarks with 100K keys\n");
        printf("\n");
        printf("Examples:\n");
        printf("  %s quick              # Quick test with 10K keys\n", argv[0]);
        printf("  %s insert 50000       # Test insertion with 50K keys\n", argv[0]);
        printf("  %s 200000             # Run all with 200K keys\n", argv[0]);
        printf("\n");
        return 0;
    }
    
    printf("\n");
    printf("================================================================\n");
    printf("           Persistent ART - Performance Benchmarks              \n");
    printf("================================================================\n");
    
    // Default: run all benchmarks with 100K keys
    int num_keys = 100000;
    
    // Parse arguments
    if (argc > 1) {
        // Check for specific benchmark
        if (strcmp(argv[1], "insert") == 0) {
            num_keys = (argc > 2) ? atoi(argv[2]) : 100000;
            benchmark_sequential_insert(num_keys);
            benchmark_random_insert(num_keys);
        } else if (strcmp(argv[1], "lookup") == 0) {
            num_keys = (argc > 2) ? atoi(argv[2]) : 100000;
            benchmark_sequential_lookup(num_keys);
            benchmark_random_lookup(num_keys);
        } else if (strcmp(argv[1], "mixed") == 0) {
            num_keys = (argc > 2) ? atoi(argv[2]) : 100000;
            benchmark_mixed_operations(num_keys);
        } else if (strcmp(argv[1], "variable") == 0) {
            num_keys = (argc > 2) ? atoi(argv[2]) : 100000;
            benchmark_variable_key_sizes(num_keys);
        } else if (strcmp(argv[1], "quick") == 0) {
            // Quick benchmark with fewer keys
            num_keys = 10000;
            benchmark_sequential_insert(num_keys);
            benchmark_random_insert(num_keys);
            benchmark_sequential_lookup(num_keys);
            benchmark_random_lookup(num_keys);
        } else {
            // Try to parse as number
            num_keys = atoi(argv[1]);
            if (num_keys <= 0) {
                num_keys = 100000;
            }
            goto run_all;
        }
    } else {
run_all:
        // Run all benchmarks
        benchmark_sequential_insert(num_keys);
        benchmark_random_insert(num_keys);
        benchmark_sequential_lookup(num_keys);
        benchmark_random_lookup(num_keys);
        benchmark_mixed_operations(num_keys);
        benchmark_variable_key_sizes(num_keys);
    }
    
    printf("\n");
    printf("================================================================\n");
    printf("                    Benchmarks Complete                         \n");
    printf("================================================================\n");
    printf("\n");
    
    cleanup_benchmark_db();
    
    return 0;
}
