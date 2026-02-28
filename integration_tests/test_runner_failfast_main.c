/**
 * Fail-Fast Integration Test Runner
 * 
 * Stops on first test failure and writes failure details to a file.
 * Optimized for large test suites - stops immediately on first failure.
 */

#include "test_runner.h"
#include "test_parser.h"
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
 * Export post-state to JSON file
 * TODO: Implement using evm_state iteration API when available
 */
static bool write_post_state_json(evm_state_t *state, const char *output_file) {
    (void)state;
    (void)output_file;
    printf("WARNING: Post-state JSON export not yet implemented for evm_state\n");
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
                write_post_state_json(runner->state, post_state_file);
                
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
