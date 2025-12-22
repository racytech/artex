/**
 * Fail-Fast Integration Test Runner
 * 
 * Stops on first test failure and writes failure details to a file.
 * Optimized for large test suites - stops immediately on first failure.
 */

#include "test_runner.h"
#include "test_parser.h"
#include "state_cache.h"
#include "art.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>
#include <limits.h>
#include <cjson/cJSON.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

static void print_usage(const char *program) {
    printf("Usage: %s [options] <file_or_directory>...\n\n", program);
    printf("Fail-fast test runner - stops on first failure and writes details to file.\n\n");
    printf("Options:\n");
    printf("  -v, --verbose          Verbose output\n");
    printf("  -o, --output <file>    Output file for failure details (default: failure.txt)\n");
    printf("  -f, --fork <name>      Only test specific fork (can specify multiple)\n");
    printf("  -t, --timeout <ms>     Test timeout in milliseconds (default: 30000)\n");
    printf("  -h, --help             Show this help message\n\n");
    printf("Examples:\n");
    printf("  %s large_test_file.json\n", program);
    printf("  %s -o failed.txt -v integration_tests/fixture_01/state_tests/\n", program);
    printf("  %s -f Berlin integration_tests/\n\n", program);
}

/**
 * Extract test case name from full test name
 * Example: "tests/frontier/create/test_create_one_byte.py::test_create_one_byte[...]" 
 *          -> "test_create_one_byte"
 */
static char* extract_test_case_name(const char *full_name) {
    if (!full_name) return NULL;
    
    // Find the last "::" separator
    const char *sep = strstr(full_name, "::");
    if (!sep) return strdup(full_name);
    
    sep += 2; // Skip "::"
    
    // Find the '[' bracket
    const char *bracket = strchr(sep, '[');
    if (!bracket) return strdup(sep);
    
    // Copy the part between :: and [
    size_t len = bracket - sep;
    char *result = malloc(len + 1);
    if (result) {
        memcpy(result, sep, len);
        result[len] = '\0';
    }
    return result;
}

/**
 * Extract the failing test case JSON and write to a file
 */
static bool write_test_case_json(const char *test_file,
                                  const char *test_name,
                                  const char *output_file) {
    // Read the JSON file
    cJSON *root = load_json_file(test_file);
    if (!root) {
        fprintf(stderr, "ERROR: Failed to load JSON file: %s\n", test_file);
        return false;
    }
    
    // Extract test case name
    char *case_name = extract_test_case_name(test_name);
    if (!case_name) {
        cJSON_Delete(root);
        return false;
    }
    
    // Try to find the test case in the JSON
    // State tests are usually structured as { "test_name": { ... } }
    cJSON *test_case = NULL;
    
    // First, try exact match
    test_case = cJSON_GetObjectItemCaseSensitive(root, case_name);
    
    // If not found, try searching through all children
    if (!test_case) {
        cJSON *child = root->child;
        while (child) {
            if (child->string && strstr(child->string, case_name)) {
                test_case = child;
                break;
            }
            child = child->next;
        }
    }
    
    // If still not found, just take the first test case
    if (!test_case && root->child) {
        test_case = root->child;
        fprintf(stderr, "WARNING: Could not find test case '%s', using first test case '%s'\n",
                case_name, test_case->string ? test_case->string : "(unnamed)");
    }
    
    if (!test_case) {
        fprintf(stderr, "ERROR: No test cases found in file\n");
        free(case_name);
        cJSON_Delete(root);
        return false;
    }
    
    // Create a new JSON object with just this test case
    cJSON *output = cJSON_CreateObject();
    cJSON *test_copy = cJSON_Duplicate(test_case, true);
    
    if (test_case->string) {
        cJSON_AddItemToObject(output, test_case->string, test_copy);
    } else {
        cJSON_AddItemToObject(output, case_name, test_copy);
    }
    
    // Write to file
    char *json_str = cJSON_Print(output);
    if (json_str) {
        FILE *fp = fopen(output_file, "w");
        if (fp) {
            fprintf(fp, "%s\n", json_str);
            fclose(fp);
            free(json_str);
            
            printf("Extracted test case JSON written to: %s\n", output_file);
            free(case_name);
            cJSON_Delete(output);
            cJSON_Delete(root);
            return true;
        }
        free(json_str);
    }
    
    free(case_name);
    cJSON_Delete(output);
    cJSON_Delete(root);
    return false;
}

/**
 * Export post-state from state_db to JSON file
 */
static bool write_post_state_json(state_db_t *state_db, const char *output_file) {
    if (!state_db || !output_file) {
        return false;
    }
    
    // Create JSON object for post-state
    cJSON *post_state = cJSON_CreateObject();
    if (!post_state) {
        return false;
    }
    
    // Iterate through all accounts in the state cache
    state_cache_iterator_t *iter = state_cache_iterator_create(&state_db->cache);
    if (!iter) {
        cJSON_Delete(post_state);
        return false;
    }
    
    while (state_cache_iterator_next(iter)) {
        const address_t *addr = state_cache_iterator_address(iter);
        const account_object_t *acc = state_cache_iterator_account(iter);
        
        if (!addr || !acc) continue;
        
        // Skip deleted accounts
        if (acc->deleted) continue;
        
        // Skip non-existent accounts (e.g., accounts that were queried but never materialized)
        if (!acc->exists) continue;
        
        // Convert address to hex string
        char addr_str[43];  // "0x" + 40 hex chars + null
        snprintf(addr_str, sizeof(addr_str), "0x");
        for (int i = 0; i < 20; i++) {
            snprintf(addr_str + 2 + (i * 2), 3, "%02x", addr->bytes[i]);
        }
        
        // Create account object
        cJSON *account = cJSON_CreateObject();
        
        // Add nonce
        char nonce_str[32];
        snprintf(nonce_str, sizeof(nonce_str), "0x%lx", acc->nonce);
        cJSON_AddStringToObject(account, "nonce", nonce_str);
        
        // Add balance
        char *balance_str = uint256_to_hex(&acc->balance);
        if (balance_str) {
            cJSON_AddStringToObject(account, "balance", balance_str);
            free(balance_str);
        }
        
        // Add code (if present)
        if (acc->code && acc->code_size > 0) {
            char *code_hex = malloc(2 + acc->code_size * 2 + 1);
            if (code_hex) {
                code_hex[0] = '0';
                code_hex[1] = 'x';
                for (size_t i = 0; i < acc->code_size; i++) {
                    snprintf(code_hex + 2 + (i * 2), 3, "%02x", acc->code[i]);
                }
                cJSON_AddStringToObject(account, "code", code_hex);
                free(code_hex);
            }
        }
        
        // Add storage
        cJSON *storage = cJSON_CreateObject();
        if (acc->storage_cache) {
            art_tree_t *storage_tree = (art_tree_t *)acc->storage_cache;
            art_iterator_t *storage_iter = art_iterator_create(storage_tree);
            if (storage_iter) {
                while (art_iterator_next(storage_iter)) {
                    size_t key_len, value_len;
                    const uint8_t *key_bytes = art_iterator_key(storage_iter, &key_len);
                    const void *value_ptr = art_iterator_value(storage_iter, &value_len);
                    
                    if (key_bytes && value_ptr && key_len == 32 && value_len == sizeof(uint256_t)) {
                        const uint256_t *storage_value = (const uint256_t *)value_ptr;
                        
                        // Convert key to hex string
                        char key_str[67];  // "0x" + 64 hex chars + null
                        key_str[0] = '0';
                        key_str[1] = 'x';
                        for (size_t i = 0; i < 32; i++) {
                            snprintf(key_str + 2 + (i * 2), 3, "%02x", key_bytes[i]);
                        }
                        
                        // Convert value to uint256_t hex
                        char *value_str = uint256_to_hex(storage_value);
                        
                        // Check if value is non-zero
                        uint256_t zero = uint256_from_uint64(0);
                        bool is_zero = uint256_eq(storage_value, &zero);
                        
                        // Only add non-zero storage slots
                        if (!is_zero && value_str) {
                            cJSON_AddStringToObject(storage, key_str, value_str);
                        }
                        
                        if (value_str) free(value_str);
                    }
                }
                art_iterator_destroy(storage_iter);
            }
        }
        cJSON_AddItemToObject(account, "storage", storage);
        
        // Add account to post-state
        cJSON_AddItemToObject(post_state, addr_str, account);
    }
    
    state_cache_iterator_destroy(iter);
    
    // Write to file
    char *json_str = cJSON_Print(post_state);
    if (json_str) {
        FILE *fp = fopen(output_file, "w");
        if (fp) {
            fprintf(fp, "%s\n", json_str);
            fclose(fp);
            free(json_str);
            cJSON_Delete(post_state);
            
            printf("Post-state JSON written to: %s\n", output_file);
            return true;
        }
        free(json_str);
    }
    
    cJSON_Delete(post_state);
    return false;
}

/**
 * Write failure details to output file
 */
static void write_failure_details(const char *output_file, 
                                  const char *test_file,
                                  const test_result_t *result) {
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        fprintf(stderr, "ERROR: Cannot open output file '%s': %s\n", 
                output_file, strerror(errno));
        return;
    }
    
    time_t now = time(NULL);
    char *time_str = ctime(&now);
    
    fprintf(fp, "================================================================================\n");
    fprintf(fp, "FIRST TEST FAILURE\n");
    fprintf(fp, "================================================================================\n");
    fprintf(fp, "Time: %s", time_str);
    fprintf(fp, "Test File: %s\n", test_file);
    fprintf(fp, "Test Name: %s\n", result->test_name ? result->test_name : "unknown");
    if (result->fork) {
        fprintf(fp, "Fork: %s\n", result->fork);
    }
    fprintf(fp, "Status: ");
    switch (result->status) {
        case TEST_PASS: fprintf(fp, "PASS\n"); break;
        case TEST_FAIL: fprintf(fp, "FAIL\n"); break;
        case TEST_ERROR: fprintf(fp, "ERROR\n"); break;
        case TEST_SKIP: fprintf(fp, "SKIP\n"); break;
        case TEST_TIMEOUT: fprintf(fp, "TIMEOUT\n"); break;
        default: fprintf(fp, "UNKNOWN\n"); break;
    }
    fprintf(fp, "Duration: %lu microseconds\n", result->duration_us);
    fprintf(fp, "\n");
    
    if (result->failure_count > 0) {
        fprintf(fp, "Failures (%zu):\n", result->failure_count);
        fprintf(fp, "--------------------------------------------------------------------------------\n");
        for (size_t i = 0; i < result->failure_count; i++) {
            const test_failure_t *f = &result->failures[i];
            fprintf(fp, "\n%zu. Field: %s\n", i + 1, f->field ? f->field : "unknown");
            fprintf(fp, "   Expected: %s\n", f->expected ? f->expected : "(null)");
            fprintf(fp, "   Actual:   %s\n", f->actual ? f->actual : "(null)");
            if (f->message) {
                fprintf(fp, "   Message:  %s\n", f->message);
            }
        }
    }
    
    if (result->skip_reason) {
        fprintf(fp, "\nSkip Reason: %s\n", result->skip_reason);
    }
    
    fprintf(fp, "\n================================================================================\n");
    fprintf(fp, "END OF FAILURE REPORT\n");
    fprintf(fp, "================================================================================\n");
    
    fclose(fp);
}

/**
 * Custom results structure that fails fast
 */
typedef struct {
    bool found_failure;
    const char *failing_file;
    test_result_t first_failure;
    size_t total_tests;  // Total test cases executed
    size_t total_passed; // Total test cases passed
} failfast_results_t;

/**
 * Run single file in fail-fast mode
 */
static bool run_file_failfast(test_runner_t *runner,
                               const char *filepath,
                               failfast_results_t *ff_results) {
    if (ff_results->found_failure) {
        return false; // Already failed, skip this file
    }
    
    test_results_t results;
    test_results_init(&results);
    
    bool success = test_runner_run_file(runner, filepath, &results);
    
    // Check if we got a failure
    if (results.failed > 0 || results.errors > 0) {
        // Find the first failure/error
        for (size_t i = 0; i < results.result_count; i++) {
            test_result_t *r = &results.results[i];
            if (r->status == TEST_FAIL || r->status == TEST_ERROR) {
                // Copy the failure details
                memcpy(&ff_results->first_failure, r, sizeof(test_result_t));
                
                // Duplicate dynamic strings to prevent double-free
                if (r->test_name) {
                    ff_results->first_failure.test_name = strdup(r->test_name);
                }
                if (r->fork) {
                    ff_results->first_failure.fork = strdup(r->fork);
                }
                if (r->skip_reason) {
                    ff_results->first_failure.skip_reason = strdup(r->skip_reason);
                }
                
                // Duplicate failures array
                if (r->failure_count > 0) {
                    ff_results->first_failure.failures = malloc(r->failure_count * sizeof(test_failure_t));
                    memcpy(ff_results->first_failure.failures, r->failures, 
                           r->failure_count * sizeof(test_failure_t));
                    
                    // Duplicate strings in failures
                    for (size_t j = 0; j < r->failure_count; j++) {
                        test_failure_t *src = &r->failures[j];
                        test_failure_t *dst = &ff_results->first_failure.failures[j];
                        
                        dst->field = src->field ? strdup(src->field) : NULL;
                        dst->expected = src->expected ? strdup(src->expected) : NULL;
                        dst->actual = src->actual ? strdup(src->actual) : NULL;
                        dst->message = src->message ? strdup(src->message) : NULL;
                    }
                }
                
                ff_results->found_failure = true;
                ff_results->failing_file = filepath;
                
                // Export post-state to JSON file
                char post_state_file[PATH_MAX];
                snprintf(post_state_file, sizeof(post_state_file), "post_state.json");
                write_post_state_json(runner->state_db, post_state_file);
                
                break;
            }
        }
    }
    
    // Track test counts
    ff_results->total_tests += results.result_count;
    ff_results->total_passed += results.passed;
    
    test_results_free(&results);
    return success;
}

/**
 * Run directory recursively in fail-fast mode
 */
static bool run_directory_failfast(test_runner_t *runner,
                                    const char *dirpath,
                                    failfast_results_t *ff_results) {
    if (ff_results->found_failure) {
        return false; // Already failed
    }
    
    DIR *dir = opendir(dirpath);
    if (!dir) {
        if (runner->config.verbose) {
            fprintf(stderr, "ERROR: Cannot open directory '%s': %s\n", 
                    dirpath, strerror(errno));
        }
        return false;
    }
    
    struct dirent *entry;
    char fullpath[PATH_MAX];
    
    while ((entry = readdir(dir)) != NULL && !ff_results->found_failure) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        
        snprintf(fullpath, sizeof(fullpath), "%s/%s", dirpath, entry->d_name);
        
        struct stat st;
        if (stat(fullpath, &st) != 0) {
            continue;
        }
        
        if (S_ISDIR(st.st_mode)) {
            run_directory_failfast(runner, fullpath, ff_results);
        } else if (S_ISREG(st.st_mode) && strstr(entry->d_name, ".json")) {
            run_file_failfast(runner, fullpath, ff_results);
        }
    }
    
    closedir(dir);
    return !ff_results->found_failure;
}

int main(int argc, char **argv) {
    test_runner_config_t config = {
        .verbose = false,
        .stop_on_fail = true,  // Always true for fail-fast
        .fork_filter = NULL,
        .fork_filter_count = 0,
        .timeout_ms = 30000
    };
    
    const char *output_file = "failure.txt";
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
        } else if (strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "--output") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "ERROR: --output requires an argument\n");
                return 1;
            }
            i++;
            output_file = argv[i];
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
    printf("Fail-Fast Integration Test Runner\n");
    printf("================================================================================\n");
    if (config.verbose) printf("Verbose: enabled\n");
    printf("Output file: %s\n", output_file);
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
    
    // Initialize fail-fast results
    failfast_results_t ff_results = {
        .found_failure = false,
        .failing_file = NULL,
        .first_failure = {0},
        .total_tests = 0,
        .total_passed = 0
    };
    
    // Process each test path
    for (size_t i = 0; i < test_path_count && !ff_results.found_failure; i++) {
        const char *path = test_paths[i];
        
        // Check if it's a directory
        struct stat st;
        if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
            // Run directory in fail-fast mode
            run_directory_failfast(&runner, path, &ff_results);
        } else {
            // Run single file
            run_file_failfast(&runner, path, &ff_results);
        }
    }
    
    int exit_code = 0;
    
    if (ff_results.found_failure) {
        printf("\n");
        printf("================================================================================\n");
        printf("FIRST FAILURE ENCOUNTERED\n");
        printf("================================================================================\n");
        printf("Test File: %s\n", ff_results.failing_file);
        printf("Test Name: %s\n", ff_results.first_failure.test_name ? 
                                  ff_results.first_failure.test_name : "unknown");
        if (ff_results.first_failure.fork) {
            printf("Fork: %s\n", ff_results.first_failure.fork);
        }
        printf("\n");
        
        if (ff_results.first_failure.failure_count > 0) {
            printf("Failure Details:\n");
            for (size_t i = 0; i < ff_results.first_failure.failure_count; i++) {
                const test_failure_t *f = &ff_results.first_failure.failures[i];
                printf("  - %s\n", f->field ? f->field : "unknown");
                printf("    Expected: %s\n", f->expected ? f->expected : "(null)");
                printf("    Actual:   %s\n", f->actual ? f->actual : "(null)");
                if (f->message) {
                    printf("    Message:  %s\n", f->message);
                }
            }
        }
        
        printf("\n");
        printf("Writing failure details to: %s\n", output_file);
        write_failure_details(output_file, ff_results.failing_file, &ff_results.first_failure);
        
        // Also extract the failing test case JSON
        char json_output_file[PATH_MAX];
        snprintf(json_output_file, sizeof(json_output_file), "failing_test.json");
        
        printf("Extracting failing test case JSON...\n");
        if (write_test_case_json(ff_results.failing_file, 
                                 ff_results.first_failure.test_name,
                                 json_output_file)) {
            printf("SUCCESS: You can now debug with: ./test_runner_example %s\n", json_output_file);
        } else {
            fprintf(stderr, "WARNING: Failed to extract test case JSON\n");
        }
        
        printf("Done.\n");
        
        // Cleanup first_failure
        test_result_free(&ff_results.first_failure);
        
        exit_code = 1;
    } else if (ff_results.total_tests == 0) {
        printf("\nNo tests were executed (all tests skipped or filtered).\n");
        exit_code = 0;
    } else {
        printf("\nAll tests passed! (%zu/%zu)\n", ff_results.total_passed, ff_results.total_tests);
        exit_code = 0;
    }
    
    // Cleanup
    test_runner_destroy(&runner);
    free(fork_filters);
    free(test_paths);
    
    return exit_code;
}
