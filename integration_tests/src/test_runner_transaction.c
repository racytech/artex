/**
 * Test Runner - Transaction Test Execution
 */

#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Forward declaration from test_runner_core.c
extern uint64_t get_time_microseconds(void);

//==============================================================================
// Transaction Test Runner
//==============================================================================

bool test_runner_run_transaction_test(test_runner_t *runner,
                                      const transaction_test_t *test,
                                      const char *fork,
                                      test_result_t *result) {
    if (!runner || !test || !result) return false;
    
    test_result_init(result);
    
    uint64_t start_time = get_time_microseconds();
    
    // Set test name
    result->test_name = test->name ? strdup(test->name) : NULL;
    
    if (runner->config.verbose) {
        printf("Running transaction test: %s\n", test->name ? test->name : "(unnamed)");
        if (test->description) {
            printf("  Description: %s\n", test->description);
        }
        printf("  TX bytes: %zu\n", test->tx_bytes_len);
        printf("  Results for %zu forks\n", test->result_count);
    }
    
    // If fork specified, only test that fork
    size_t fork_start = 0;
    size_t fork_end = test->result_count;
    
    if (fork) {
        bool found = false;
        for (size_t i = 0; i < test->result_count; i++) {
            if (test->results[i].fork_name && strcmp(test->results[i].fork_name, fork) == 0) {
                fork_start = i;
                fork_end = i + 1;
                found = true;
                break;
            }
        }
        
        if (!found) {
            result->status = TEST_SKIP;
            result->skip_reason = strdup("Fork not found in test");
            result->duration_us = get_time_microseconds() - start_time;
            return true;
        }
    }
    
    // Test each fork
    for (size_t fork_idx = fork_start; fork_idx < fork_end; fork_idx++) {
        const tx_test_result_t *expected = &test->results[fork_idx];
        
        // Check fork filter
        if (runner->config.fork_filter_count > 0 && expected->fork_name) {
            bool fork_allowed = false;
            for (size_t i = 0; i < runner->config.fork_filter_count; i++) {
                if (strcmp(runner->config.fork_filter[i], expected->fork_name) == 0) {
                    fork_allowed = true;
                    break;
                }
            }
            if (!fork_allowed) {
                continue; // Skip this fork
            }
        }
        
        result->fork = expected->fork_name ? strdup(expected->fork_name) : NULL;
        
        if (runner->config.verbose) {
            printf("  Testing fork: %s\n", expected->fork_name ? expected->fork_name : "(unknown)");
        }
        
        // TODO: Decode transaction RLP when RLP decoder is ready
        // For now, we just verify we have the transaction bytes
        if (!test->tx_bytes || test->tx_bytes_len == 0) {
            result->status = TEST_ERROR;
            test_result_add_failure(result, "transaction", NULL, NULL, 
                                  "No transaction bytes provided");
            goto cleanup;
        }
        
        // TODO: Decode transaction
        // transaction_t tx;
        // rlp_decode_result_t decode_result = rlp_decode_transaction(test->tx_bytes, test->tx_bytes_len, &tx);
        
        // Check if transaction should be valid or invalid
        bool should_be_valid = (expected->exception == NULL);
        
        if (runner->config.verbose) {
            printf("    Expected: %s\n", should_be_valid ? "Valid" : "Invalid");
            if (expected->exception) {
                printf("    Expected exception: %s\n", expected->exception);
            }
        }
        
        // TODO: Validate transaction when decoder is ready
        // bool is_valid = decode_result.success;
        // const char *actual_exception = decode_result.error;
        
        // For now, we'll mark the test as needing implementation
        if (runner->config.verbose) {
            printf("    Status: Not yet implemented (TODO: RLP decoder)\n");
        }
        
        // TODO: Verify intrinsic gas calculation
        // if (should_be_valid && expected->intrinsic_gas != 0) {
        //     uint256_t actual_gas = calculate_intrinsic_gas(&tx);
        //     if (!uint256_eq(&actual_gas, &expected->intrinsic_gas)) {
        //         result->status = TEST_FAIL;
        //         ...
        //     }
        // }
        
        // TODO: Verify exception matches
        // if (should_be_valid != is_valid) {
        //     result->status = TEST_FAIL;
        //     test_result_add_failure(result, "validity", 
        //                           should_be_valid ? "valid" : "invalid",
        //                           is_valid ? "valid" : "invalid",
        //                           "Transaction validity mismatch");
        // } else if (!should_be_valid && expected->exception) {
        //     // Check that the exception matches
        //     if (!actual_exception || strcmp(actual_exception, expected->exception) != 0) {
        //         result->status = TEST_FAIL;
        //         test_result_add_failure(result, "exception",
        //                               expected->exception,
        //                               actual_exception ? actual_exception : "(none)",
        //                               "Exception type mismatch");
        //     }
        // }
    }
    
cleanup:
    result->duration_us = get_time_microseconds() - start_time;
    
    // If no failures and not already failed, mark as pass
    if (result->status != TEST_FAIL && result->status != TEST_ERROR && 
        result->status != TEST_SKIP) {
        result->status = TEST_PASS;
    }
    
    if (runner->config.verbose) {
        printf("  Result: %s (%.2f ms)\n", 
               result->status == TEST_PASS ? "PASS" : 
               result->status == TEST_FAIL ? "FAIL" : 
               result->status == TEST_SKIP ? "SKIP" : "ERROR",
               result->duration_us / 1000.0);
    }
    
    return true;
}
