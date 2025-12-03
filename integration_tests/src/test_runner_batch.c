/**
 * Test Runner - File and Directory Execution
 */

#include "test_runner.h"
#include "test_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

//==============================================================================
// File Test Execution
//==============================================================================

bool test_runner_run_file(test_runner_t *runner,
                          const char *filepath,
                          test_results_t *results) {
    if (!runner || !filepath || !results) return false;
    
    // Determine test type from path
    bool is_state_test = strstr(filepath, "state_tests") != NULL;
    bool is_blockchain_test = strstr(filepath, "blockchain_tests") != NULL;
    bool is_transaction_test = strstr(filepath, "transaction_tests") != NULL;
    
    if (!is_state_test && !is_blockchain_test && !is_transaction_test) {
        // Try to detect from filename
        if (strstr(filepath, "state") || strstr(filepath, "State")) {
            is_state_test = true;
        } else if (strstr(filepath, "blockchain") || strstr(filepath, "Blockchain")) {
            is_blockchain_test = true;
        } else if (strstr(filepath, "transaction") || strstr(filepath, "Transaction")) {
            is_transaction_test = true;
        }
    }
    
    if (runner->config.verbose) {
        printf("Processing: %s\n", filepath);
        printf("  Type: %s\n", 
               is_state_test ? "state_test" :
               is_blockchain_test ? "blockchain_test" :
               is_transaction_test ? "transaction_test" : "unknown");
    }
    
    test_result_t result;
    bool success = false;
    
    if (is_state_test) {
        state_test_t *test = NULL;
        if (!parse_state_test(filepath, &test)) {
            if (runner->config.verbose) {
                fprintf(stderr, "  ERROR: Failed to parse state test\n");
            }
            return false;
        }
        
        success = test_runner_run_state_test(runner, test, NULL, &result);
        state_test_free(test);
        
    } else if (is_blockchain_test) {
        blockchain_test_t *test = NULL;
        if (!parse_blockchain_test(filepath, &test)) {
            if (runner->config.verbose) {
                fprintf(stderr, "  ERROR: Failed to parse blockchain test\n");
            }
            return false;
        }
        
        success = test_runner_run_blockchain_test(runner, test, &result);
        blockchain_test_free(test);
        
    } else if (is_transaction_test) {
        transaction_test_t *test = NULL;
        if (!parse_transaction_test(filepath, &test)) {
            if (runner->config.verbose) {
                fprintf(stderr, "  ERROR: Failed to parse transaction test\n");
            }
            return false;
        }
        
        success = test_runner_run_transaction_test(runner, test, NULL, &result);
        transaction_test_free(test);
        
    } else {
        if (runner->config.verbose) {
            fprintf(stderr, "  ERROR: Unknown test type\n");
        }
        return false;
    }
    
    if (success) {
        test_results_add(results, &result);
        test_result_free(&result);
    }
    
    return success;
}

//==============================================================================
// Directory Test Execution
//==============================================================================

static bool is_json_file(const char *filename) {
    size_t len = strlen(filename);
    return len > 5 && strcmp(filename + len - 5, ".json") == 0;
}

static bool is_directory(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        return false;
    }
    return S_ISDIR(st.st_mode);
}

static void process_directory_recursive(test_runner_t *runner,
                                        const char *dirpath,
                                        test_results_t *results) {
    DIR *dir = opendir(dirpath);
    if (!dir) {
        if (runner->config.verbose) {
            fprintf(stderr, "ERROR: Cannot open directory: %s\n", dirpath);
        }
        return;
    }
    
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // Skip . and ..
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        // Build full path
        char fullpath[4096];
        snprintf(fullpath, sizeof(fullpath), "%s/%s", dirpath, entry->d_name);
        
        if (is_directory(fullpath)) {
            // Recursively process subdirectory
            process_directory_recursive(runner, fullpath, results);
        } else if (is_json_file(entry->d_name)) {
            // Run test file
            test_runner_run_file(runner, fullpath, results);
            
            // Stop on fail if configured
            if (runner->config.stop_on_fail && results->failed > 0) {
                break;
            }
        }
    }
    
    closedir(dir);
}

bool test_runner_run_directory(test_runner_t *runner,
                               const char *directory,
                               test_results_t *results) {
    if (!runner || !directory || !results) return false;
    
    if (runner->config.verbose) {
        printf("================================================================================\n");
        printf("Running tests in directory: %s\n", directory);
        printf("================================================================================\n\n");
    }
    
    if (!is_directory(directory)) {
        if (runner->config.verbose) {
            fprintf(stderr, "ERROR: Not a directory: %s\n", directory);
        }
        return false;
    }
    
    process_directory_recursive(runner, directory, results);
    
    if (runner->config.verbose) {
        printf("\n");
        test_results_print(results, false);
    }
    
    return true;
}
