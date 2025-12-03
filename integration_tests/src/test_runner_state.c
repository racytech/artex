/**
 * Test Runner - State Test Execution
 */

#include "test_runner.h"
#include "transaction.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Forward declaration from test_runner_core.c
extern uint64_t get_time_microseconds(void);

//==============================================================================
// State Test Runner
//==============================================================================

bool test_runner_run_state_test(test_runner_t *runner,
                                const state_test_t *test,
                                const char *fork,
                                test_result_t *result) {
    if (!runner || !test || !result) return false;
    
    test_result_init(result);
    
    uint64_t start_time = get_time_microseconds();
    
    // Set test name
    result->test_name = test->name ? strdup(test->name) : NULL;
    
    // If fork specified, only test that fork
    // Otherwise, test all forks in the test
    size_t fork_start = 0;
    size_t fork_end = test->post_count;
    
    if (fork) {
        // Find matching fork
        bool found = false;
        for (size_t i = 0; i < test->post_count; i++) {
            if (test->post[i].fork_name && strcmp(test->post[i].fork_name, fork) == 0) {
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
        const char *test_fork = test->post[fork_idx].fork_name;
        
        // Check fork filter
        if (runner->config.fork_filter_count > 0) {
            bool fork_allowed = false;
            for (size_t i = 0; i < runner->config.fork_filter_count; i++) {
                if (strcmp(runner->config.fork_filter[i], test_fork) == 0) {
                    fork_allowed = true;
                    break;
                }
            }
            if (!fork_allowed) {
                continue; // Skip this fork
            }
        }
        
        result->fork = test_fork ? strdup(test_fork) : NULL;
        
        // Test each post-condition for this fork
        for (size_t cond_idx = 0; cond_idx < test->post[fork_idx].condition_count; cond_idx++) {
            const test_post_condition_t *post_cond = &test->post[fork_idx].conditions[cond_idx];
            
            // Reset state for this test case
            test_runner_reset(runner);
            
            // Setup pre-state
            if (!test_runner_setup_state(runner->state_db, test->pre_state, test->pre_state_count)) {
                result->status = TEST_ERROR;
                test_result_add_failure(result, "pre_state", NULL, NULL, 
                                      "Failed to setup pre-state");
                goto cleanup;
            }
            
            // Get transaction parameters for this test case
            uint256_t gas_limit = test->transaction.gas_limit_count > post_cond->gas_index ?
                test->transaction.gas_limit[post_cond->gas_index] : uint256_from_uint64(0);
            
            uint256_t value = test->transaction.value_count > post_cond->value_index ?
                test->transaction.value[post_cond->value_index] : uint256_from_uint64(0);
            
            uint8_t *data = NULL;
            size_t data_len = 0;
            if (test->transaction.data_count > post_cond->data_index) {
                data = test->transaction.data[post_cond->data_index];
                data_len = test->transaction.data_len[post_cond->data_index];
            }
            
            // Execute transaction
            if (runner->config.verbose) {
                printf("  [%s] Test case %zu: gas_idx=%u, value_idx=%u, data_idx=%u\n",
                       test_fork, cond_idx, 
                       post_cond->gas_index, post_cond->value_index, post_cond->data_index);
            }
            
            // Convert gas_limit from uint256_t to uint64_t
            uint64_t gas = uint256_to_uint64(&gas_limit);
            
            // Debug: Show transaction details
            printf("DEBUG: Transaction to address: 0x");
            for (int i = 0; i < 20; i++) printf("%02x", test->transaction.to.bytes[i]);
            printf(", data_len=%zu, is_create=%d\n", data_len, test->transaction.is_create);
            
            // Build transaction structure
            transaction_t tx = {
                .type = TX_TYPE_LEGACY,
                .nonce = 0,  // Will be validated against current state
                .sender = test->transaction.sender,
                .to = test->transaction.to,
                .value = value,
                .gas_limit = gas,
                .gas_price = test->transaction.gas_price,  // Already uint256_t
                .data = data,
                .data_size = data_len,
                .is_create = test->transaction.is_create
            };
            
            // Get current nonce
            uint64_t current_nonce;
            if (state_db_get_nonce(runner->state_db, &test->transaction.sender, &current_nonce)) {
                tx.nonce = current_nonce;
            }
            
            // Build block environment from test env
            block_env_t block_env = {
                .coinbase = test->env.coinbase,
                .block_number = uint256_to_uint64(&test->env.number),
                .timestamp = uint256_to_uint64(&test->env.timestamp),
                .gas_limit = uint256_to_uint64(&test->env.gas_limit),
                .difficulty = test->env.difficulty,
                .base_fee = test->env.base_fee
            };
            
            // Execute transaction using transaction execution layer
            transaction_result_t tx_result;
            if (!transaction_execute(runner->evm, runner->state_db, &tx, &block_env, &tx_result)) {
                result->status = TEST_ERROR;
                test_result_add_failure(result, "execution", NULL, NULL, "Transaction execution failed");
                goto cleanup;
            }
            
            printf("DEBUG: Transaction execution status=%d, gas_used=%lu\n", 
                   tx_result.status, tx_result.gas_used);
            
            // Check execution status
            if (tx_result.status != EVM_SUCCESS && tx_result.status != EVM_REVERT) {
                result->status = TEST_FAIL;
                
                const char *status_str = "UNKNOWN";
                switch (tx_result.status) {
                    case EVM_OUT_OF_GAS: status_str = "OUT_OF_GAS"; break;
                    case EVM_INVALID_OPCODE: status_str = "INVALID_OPCODE"; break;
                    case EVM_STACK_OVERFLOW: status_str = "STACK_OVERFLOW"; break;
                    case EVM_STACK_UNDERFLOW: status_str = "STACK_UNDERFLOW"; break;
                    case EVM_INVALID_JUMP: status_str = "INVALID_JUMP"; break;
                    case EVM_INVALID_MEMORY_ACCESS: status_str = "INVALID_MEMORY_ACCESS"; break;
                    default: break;
                }
                
                char failure_msg[256];
                snprintf(failure_msg, sizeof(failure_msg), 
                        "Transaction execution failed: %s", status_str);
                test_result_add_failure(result, "execution_status", "SUCCESS", status_str, 
                                      failure_msg);
                
                transaction_result_free(&tx_result);
                
                if (runner->config.stop_on_fail) {
                    goto cleanup;
                }
            }
            
            transaction_result_free(&tx_result);
            
            // Compute final state root
            printf("DEBUG: Computing state root after transaction\n");
            
            if (!state_db_compute_state_root(runner->state_db)) {
                printf("DEBUG: state_db_compute_state_root FAILED\n");
            } else {
                printf("DEBUG: state_db_compute_state_root succeeded\n");
                hash_t computed_root;
                state_db_get_state_root(runner->state_db, &computed_root);
                printf("DEBUG: Computed root: 0x");
                for (int i = 0; i < 32; i++) printf("%02x", computed_root.bytes[i]);
                printf("\n");
            }
            
            // Verify state root
            hash_t actual_root;
            if (!test_runner_verify_state_root(runner->state_db, &post_cond->state_root, &actual_root)) {
                result->status = TEST_FAIL;
                
                char *expected_str = hash_to_hex_string(&post_cond->state_root);
                char *actual_str = hash_to_hex_string(&actual_root);
                
                test_result_add_failure(result, "state_root", expected_str, actual_str,
                                      "State root mismatch");
                
                free(expected_str);
                free(actual_str);
                
                // Continue testing other cases unless stop_on_fail is set
                if (runner->config.stop_on_fail) {
                    goto cleanup;
                }
            }
            
            // TODO: Verify logs hash when available
            // if (!hash_equals(&logs_hash, &post_cond->logs_hash)) {
            //     result->status = TEST_FAIL;
            //     ...
            // }
        }
    }
    
cleanup:
    result->duration_us = get_time_microseconds() - start_time;
    
    // If no failures were added, mark as pass
    if (result->status != TEST_FAIL && result->status != TEST_ERROR && 
        result->status != TEST_SKIP) {
        result->status = TEST_PASS;
    }
    
    return true;
}
