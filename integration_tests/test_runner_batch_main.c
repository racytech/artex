/**
 * Batch Integration Test Runner
 * 
 * Run multiple test files or entire directories of Ethereum test fixtures
 */

#include "test_runner.h"
#include "test_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/resource.h>

static void print_usage(const char *program) {
    printf("Usage: %s [options] <file_or_directory>...\n\n", program);
    printf("Options:\n");
    printf("  -v, --verbose          Verbose output\n");
    printf("  -s, --stop-on-fail     Stop on first failure\n");
    printf("  -f, --fork <name>      Only test specific fork (can specify multiple)\n");
    printf("  -t, --timeout <ms>     Test timeout in milliseconds (default: 30000)\n");
    printf("  -h, --help             Show this help message\n\n");
    printf("Examples:\n");
    printf("  %s integration_tests/fixtures/state_tests/frontier/\n", program);
    printf("  %s -v -f Berlin integration_tests/fixtures/\n", program);
    printf("  %s --stop-on-fail test1.json test2.json\n\n", program);
}

int main(int argc, char **argv) {
    /* EVM allows 1024-deep call stacks. Increase stack to 32MB for deep recursion. */
    struct rlimit rl;
    if (getrlimit(RLIMIT_STACK, &rl) == 0 && rl.rlim_cur < 32UL * 1024 * 1024) {
        rl.rlim_cur = 32UL * 1024 * 1024;
        setrlimit(RLIMIT_STACK, &rl);
    }

    test_runner_config_t config = {
        .verbose = false,
        .stop_on_fail = false,
        .fork_filter = NULL,
        .fork_filter_count = 0,
        .timeout_ms = 30000
    };
    
    char **fork_filters = NULL;
    size_t fork_filter_cap = 0;
    
    char **test_paths = NULL;
    size_t test_path_count = 0;
    size_t test_path_cap = 0;
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--verbose") == 0) {
            config.verbose = true;
        } else if (strcmp(argv[i], "-s") == 0 || strcmp(argv[i], "--stop-on-fail") == 0) {
            config.stop_on_fail = true;
        } else if (strcmp(argv[i], "-f") == 0 || strcmp(argv[i], "--fork") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "ERROR: --fork requires an argument\n");
                return 1;
            }
            i++;
            
            // Add fork filter
            if (config.fork_filter_count >= fork_filter_cap) {
                fork_filter_cap = fork_filter_cap == 0 ? 4 : fork_filter_cap * 2;
                fork_filters = realloc(fork_filters, fork_filter_cap * sizeof(char*));
            }
            fork_filters[config.fork_filter_count++] = argv[i];
            
        } else if (strcmp(argv[i], "-t") == 0 || strcmp(argv[i], "--timeout") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "ERROR: --timeout requires an argument\n");
                return 1;
            }
            i++;
            config.timeout_ms = strtoull(argv[i], NULL, 10);
            
        } else if (argv[i][0] == '-') {
            fprintf(stderr, "ERROR: Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        } else {
            // Test path
            if (test_path_count >= test_path_cap) {
                test_path_cap = test_path_cap == 0 ? 4 : test_path_cap * 2;
                test_paths = realloc(test_paths, test_path_cap * sizeof(char*));
            }
            test_paths[test_path_count++] = argv[i];
        }
    }
    
    if (test_path_count == 0) {
        fprintf(stderr, "ERROR: No test files or directories specified\n\n");
        print_usage(argv[0]);
        return 1;
    }
    
    config.fork_filter = fork_filters;
    
    printf("================================================================================\n");
    printf("Batch Integration Test Runner\n");
    printf("================================================================================\n");
    if (config.verbose) printf("Verbose: enabled\n");
    if (config.stop_on_fail) printf("Stop on fail: enabled\n");
    if (config.fork_filter_count > 0) {
        printf("Fork filter: ");
        for (size_t i = 0; i < config.fork_filter_count; i++) {
            printf("%s%s", fork_filters[i], i < config.fork_filter_count - 1 ? ", " : "");
        }
        printf("\n");
    }
    printf("Timeout: %lu ms\n", config.timeout_ms);
    printf("Test paths: %zu\n", test_path_count);
    printf("================================================================================\n\n");
    
    // Initialize test runner
    test_runner_t runner;
    if (!test_runner_init(&runner, &config)) {
        fprintf(stderr, "ERROR: Failed to initialize test runner\n");
        free(fork_filters);
        free(test_paths);
        return 1;
    }
    
    // Initialize results
    test_results_t results;
    test_results_init(&results);
    
    // Process each test path
    for (size_t i = 0; i < test_path_count; i++) {
        const char *path = test_paths[i];
        
        // Check if it's a directory
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            // Run directory
            test_runner_run_directory(&runner, path, &results);
        } else {
            // Run single file
            test_runner_run_file(&runner, path, &results);
        }
        
        // Stop if configured
        if (config.stop_on_fail && results.failed > 0) {
            printf("\nStopping due to failures (--stop-on-fail)\n");
            break;
        }
    }
    
    // Print final results
    printf("\n");
    test_results_print(&results, config.verbose);
    
    // Cleanup
    int exit_code = (results.failed == 0 && results.errors == 0) ? 0 : 1;
    
    test_results_free(&results);
    test_runner_destroy(&runner);
    free(fork_filters);
    free(test_paths);
    
    return exit_code;
}
