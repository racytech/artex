/**
 * Simple Test Runner Example
 * 
 * Demonstrates running state tests with the test runner infrastructure
 */

#include "test_runner.h"
#include "test_parser.h"
#include "evm_tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    if (argc < 2) {
        printf("Usage: %s <state_test.json> [fork] [--trace]\n", argv[0]);
        printf("\nExample:\n");
        printf("  %s integration_tests/fixtures/state_tests/frontier/opcodes/test_dup.json\n", argv[0]);
        printf("  %s integration_tests/fixtures/state_tests/frontier/opcodes/test_dup.json Berlin\n", argv[0]);
        printf("  %s test.json Paris --trace 2>/tmp/trace.jsonl\n", argv[0]);
        return 1;
    }

    const char *filepath = argv[1];
    const char *fork = NULL;
    bool trace = false;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--trace") == 0)
            trace = true;
        else if (!fork)
            fork = argv[i];
    }

#ifdef ENABLE_EVM_TRACE
    if (trace)
        evm_tracer_init(stderr);
#else
    if (trace)
        fprintf(stderr, "WARNING: --trace requires build with -DENABLE_EVM_TRACE=ON\n");
#endif
    
    printf("================================================================================\n");
    printf("Integration Test Runner\n");
    printf("================================================================================\n");
    printf("Test file: %s\n", filepath);
    if (fork) {
        printf("Fork filter: %s\n", fork);
    }
    printf("\n");
    
    // Parse the test
    state_test_t *test = NULL;
    if (!parse_state_test(filepath, &test)) {
        fprintf(stderr, "ERROR: Failed to parse test file\n");
        return 1;
    }
    
    printf("Loaded test: %s\n", test->name ? test->name : "(unnamed)");
    printf("Pre-state accounts: %zu\n", test->pre_state_count);
    printf("Post-conditions: %zu forks\n", test->post_count);
    printf("\n");
    
    // Initialize test runner
    test_runner_config_t config = {
        .verbose = true,
        .stop_on_fail = false,
        .fork_filter = NULL,
        .fork_filter_count = 0,
        .timeout_ms = 30000
    };
    
    test_runner_t runner;
    if (!test_runner_init(&runner, &config)) {
        fprintf(stderr, "ERROR: Failed to initialize test runner\n");
        state_test_free(test);
        return 1;
    }
    
    printf("Test runner initialized\n");
    printf("Running test...\n\n");
    
    // Run the test
    test_result_t result;
    if (!test_runner_run_state_test(&runner, test, fork, &result)) {
        fprintf(stderr, "ERROR: Failed to run test\n");
        test_runner_destroy(&runner);
        state_test_free(test);
        return 1;
    }
    
    // Print result
    printf("================================================================================\n");
    printf("Test Result\n");
    printf("================================================================================\n");
    
    const char *status_str = "UNKNOWN";
    switch (result.status) {
        case TEST_PASS: status_str = "✓ PASS"; break;
        case TEST_FAIL: status_str = "✗ FAIL"; break;
        case TEST_ERROR: status_str = "✗ ERROR"; break;
        case TEST_SKIP: status_str = "⊘ SKIP"; break;
        default: break;
    }
    
    printf("Status: %s\n", status_str);
    printf("Duration: %.2f ms\n", result.duration_us / 1000.0);
    
    if (result.fork) {
        printf("Fork: %s\n", result.fork);
    }
    
    if (result.status == TEST_SKIP && result.skip_reason) {
        printf("Skip reason: %s\n", result.skip_reason);
    }
    
    if (result.status == TEST_FAIL && result.failure_count > 0) {
        printf("\nFailures:\n");
        for (size_t i = 0; i < result.failure_count; i++) {
            const test_failure_t *f = &result.failures[i];
            printf("  [%zu] %s:\n", i + 1, f->field ? f->field : "unknown");
            printf("      Expected: %s\n", f->expected ? f->expected : "(none)");
            printf("      Actual:   %s\n", f->actual ? f->actual : "(none)");
            if (f->message) {
                printf("      Message:  %s\n", f->message);
            }
        }
    }
    
    printf("================================================================================\n");
    
    // Cleanup
    int exit_code = (result.status == TEST_PASS) ? 0 : 1;
    
    test_result_free(&result);
    test_runner_destroy(&runner);
    state_test_free(test);
    
    return exit_code;
}
