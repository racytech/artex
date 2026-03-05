/**
 * Test Runner - Blockchain Test Execution
 */

#include "test_runner.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// Forward declaration from test_runner_core.c
extern uint64_t get_time_microseconds(void);

//==============================================================================
// Blockchain Test Runner
//==============================================================================

bool test_runner_run_blockchain_test(test_runner_t *runner,
                                     const blockchain_test_t *test,
                                     test_result_t *result) {
    if (!runner || !test || !result) return false;
    
    test_result_init(result);
    
    uint64_t start_time = get_time_microseconds();
    
    // Set test name and fork
    result->test_name = test->name ? strdup(test->name) : NULL;
    result->fork = test->network ? strdup(test->network) : NULL;
    
    if (runner->config.verbose) {
        printf("Running blockchain test: %s\n", test->name ? test->name : "(unnamed)");
        printf("  Network: %s\n", test->network ? test->network : "(unknown)");
        printf("  Pre-state accounts: %zu\n", test->pre_state_count);
        printf("  Blocks to process: %zu\n", test->block_count);
    }
    
    // Check fork filter
    if (runner->config.fork_filter_count > 0 && test->network) {
        bool fork_allowed = false;
        for (size_t i = 0; i < runner->config.fork_filter_count; i++) {
            if (strcasecmp(runner->config.fork_filter[i], test->network) == 0) {
                fork_allowed = true;
                break;
            }
        }
        if (!fork_allowed) {
            result->status = TEST_SKIP;
            result->skip_reason = strdup("Fork not in filter");
            result->duration_us = get_time_microseconds() - start_time;
            return true;
        }
    }
    
    // Reset state
    test_runner_reset(runner);
    
    // Setup genesis/pre-state
    if (!test_runner_setup_state(runner->state, test->pre_state, test->pre_state_count)) {
        result->status = TEST_ERROR;
        test_result_add_failure(result, "genesis_state", NULL, NULL, 
                              "Failed to setup genesis state");
        goto cleanup;
    }
    
    // Verify genesis state root if available
    hash_t actual_genesis_root;
    // TODO: State root computation requires MPT integration
    memset(&actual_genesis_root, 0, sizeof(actual_genesis_root));
    
    if (runner->config.verbose) {
        char *genesis_root_str = hash_to_hex_string(&actual_genesis_root);
        printf("  Genesis state root: %s\n", genesis_root_str);
        free(genesis_root_str);
        
        char *expected_root_str = hash_to_hex_string(&test->genesis_header.state_root);
        printf("  Expected root: %s\n", expected_root_str);
        free(expected_root_str);
    }
    
    if (!hash_equals(&actual_genesis_root, &test->genesis_header.state_root)) {
        result->status = TEST_FAIL;
        
        char *expected_str = hash_to_hex_string(&test->genesis_header.state_root);
        char *actual_str = hash_to_hex_string(&actual_genesis_root);
        
        test_result_add_failure(result, "genesis_state_root", expected_str, actual_str,
                              "Genesis state root mismatch");
        
        free(expected_str);
        free(actual_str);
        
        if (runner->config.stop_on_fail) {
            goto cleanup;
        }
    }
    
    // Process each block
    for (size_t block_idx = 0; block_idx < test->block_count; block_idx++) {
        const test_block_t *block = &test->blocks[block_idx];
        
        if (runner->config.verbose) {
            printf("  Processing block %zu...\n", block_idx);
            printf("    Transactions: %zu\n", block->tx_count);
        }
        
        // TODO: Process block when EVM is ready
        // For now, we just verify we can access the block data
        
        // Process each transaction in the block
        for (size_t tx_idx = 0; tx_idx < block->tx_count; tx_idx++) {
            // TODO: Decode and execute transaction RLP
            // uint8_t *tx_rlp = block->transactions[tx_idx];
            // size_t tx_len = block->tx_len[tx_idx];
            
            // TODO: Execute transaction
            // evm_execute_transaction(runner->evm, runner->state_db, tx_rlp, tx_len);
            
            if (runner->config.verbose) {
                printf("      TX %zu: %zu bytes\n", tx_idx, block->tx_len[tx_idx]);
            }
            
            runner->total_transactions++;
        }
        
        // TODO: Verify block state root after execution
        // hash_t block_state_root;
        // state_db_get_state_root(runner->state_db, &block_state_root);
        // if (!hash_equals(&block_state_root, &block->header.state_root)) {
        //     // Block state root mismatch
        // }
    }
    
    // Verify final state
    hash_t final_state_root;
    // TODO: State root computation requires MPT integration
    memset(&final_state_root, 0, sizeof(final_state_root));
    
    if (runner->config.verbose) {
        printf("  Final state verification...\n");
        char *final_root_str = hash_to_hex_string(&final_state_root);
        printf("    Actual state root: %s\n", final_root_str);
        free(final_root_str);
    }
    
    // Verify post-state accounts (if provided)
    if (test->post_state_count > 0) {
        for (size_t i = 0; i < test->post_state_count; i++) {
            const test_account_t *expected_acc = &test->post_state[i];
            
            // Get actual account state
            uint256_t actual_balance;
            actual_balance = evm_state_get_balance(runner->state, &expected_acc->address);
            if (uint256_is_zero(&actual_balance) && !evm_state_exists(runner->state, &expected_acc->address)) {
                // Account doesn't exist
                if (!uint256_is_zero(&expected_acc->balance)) {
                    result->status = TEST_FAIL;
                    
                    char addr_str[43];
                    snprintf(addr_str, sizeof(addr_str), "0x");
                    for (int j = 0; j < 20; j++) {
                        snprintf(addr_str + 2 + j*2, 3, "%02x", expected_acc->address.bytes[j]);
                    }
                    
                    char *expected_str = uint256_to_hex_string(&expected_acc->balance);
                    
                    char msg[256];
                    snprintf(msg, sizeof(msg), "Account %s missing", addr_str);
                    
                    test_result_add_failure(result, "account_balance", expected_str, "0x0", msg);
                    free(expected_str);
                    
                    if (runner->config.stop_on_fail) {
                        goto cleanup;
                    }
                    continue;
                }
            }
            
            // Verify balance
            if (!uint256_eq(&actual_balance, &expected_acc->balance)) {
                result->status = TEST_FAIL;
                
                char addr_str[43];
                snprintf(addr_str, sizeof(addr_str), "0x");
                for (int j = 0; j < 20; j++) {
                    snprintf(addr_str + 2 + j*2, 3, "%02x", expected_acc->address.bytes[j]);
                }
                
                char *expected_str = uint256_to_hex_string(&expected_acc->balance);
                char *actual_str = uint256_to_hex_string(&actual_balance);
                
                char msg[256];
                snprintf(msg, sizeof(msg), "Balance mismatch for %s", addr_str);
                
                test_result_add_failure(result, "account_balance", expected_str, actual_str, msg);
                
                free(expected_str);
                free(actual_str);
                
                if (runner->config.stop_on_fail) {
                    goto cleanup;
                }
            }
            
            // TODO: Verify nonce, code, storage when available
        }
    }
    
    // Verify last block hash
    if (test->block_count > 0) {
        // TODO: Compute actual last block hash
        // For now, just check if it's provided
        if (runner->config.verbose) {
            char *expected_hash_str = hash_to_hex_string(&test->last_block_hash);
            printf("  Expected last block hash: %s\n", expected_hash_str);
            free(expected_hash_str);
        }
        
        // TODO: Verify when block hash calculation is implemented
        // if (!hash_equals(&actual_last_hash, &test->last_block_hash)) {
        //     result->status = TEST_FAIL;
        //     ...
        // }
    }
    
cleanup:
    result->duration_us = get_time_microseconds() - start_time;
    
    // If no failures were added and not already failed, mark as pass
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
