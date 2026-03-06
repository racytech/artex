/**
 * Test Runner - State Test Execution
 */

#include "test_runner.h"
#include "transaction.h"
#include "evm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// Forward declaration from test_runner_core.c
extern uint64_t get_time_microseconds(void);
extern chain_config_t *create_test_chain_config(const char *fork_name);
extern bool hash_equals(const hash_t *a, const hash_t *b);


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
                if (strcasecmp(runner->config.fork_filter[i], test_fork) == 0) {
                    fork_allowed = true;
                    break;
                }
            }
            if (!fork_allowed) {
                continue; // Skip this fork
            }
        }
        
        result->fork = test_fork ? strdup(test_fork) : NULL;
        
        // Update EVM chain config for this fork
        chain_config_t *fork_config = create_test_chain_config(test_fork);
        if (!fork_config) {
            // Unknown/transition fork — skip all conditions
            for (size_t ci = 0; ci < test->post[fork_idx].condition_count; ci++) {
                test_result_init(result);
                result->test_name = test->name ? strdup(test->name) : NULL;
                result->fork = test_fork ? strdup(test_fork) : NULL;
                result->status = TEST_SKIP;
                result->skip_reason = strdup("Unknown/transition fork");
                result++;
            }
            continue;
        }
        if (runner->evm) {
            runner->evm->chain_config = fork_config;
            // Note: fork will be recomputed from block number in evm_set_block_env()
        }
        
        // Test each post-condition for this fork
        for (size_t cond_idx = 0; cond_idx < test->post[fork_idx].condition_count; cond_idx++) {
            const test_post_condition_t *post_cond = &test->post[fork_idx].conditions[cond_idx];
            
            // (debug output removed)
            
            // Reset state for this test case
            test_runner_reset(runner);

            // Re-set chain config (test_runner_reset recreates EVM with default mainnet config)
            if (runner->evm) {
                runner->evm->chain_config = fork_config;
            }

            // Setup pre-state
            if (!test_runner_setup_state(runner->state, test->pre_state, test->pre_state_count)) {
                result->status = TEST_ERROR;
                test_result_add_failure(result, "pre_state", NULL, NULL,
                                      "Failed to setup pre-state");
                goto cleanup;
            }

            // Commit pre-state as "original" for EIP-2200 storage tracking
            evm_state_commit(runner->state);

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
            
            // Get access list for this test case (EIP-2930)
            test_access_list_t *access_list = NULL;
            if (test->transaction.access_lists_count > post_cond->data_index) {
                access_list = &test->transaction.access_lists[post_cond->data_index];
            }
            
            // Execute transaction
            if (runner->config.verbose) {
                printf("  [%s] Test case %zu: gas_idx=%u, value_idx=%u, data_idx=%u\n",
                       test_fork, cond_idx, 
                       post_cond->gas_index, post_cond->value_index, post_cond->data_index);
            }
            
            // Convert gas_limit from uint256_t to uint64_t
            uint64_t gas = uint256_to_uint64(&gas_limit);
            
            // (Debug output removed)
            
            // Determine transaction type based on available fields
            // The presence of accessLists in the JSON indicates EIP-2930 (even if empty)
            transaction_type_t tx_type = TX_TYPE_LEGACY;
            if (test->transaction.has_authorization_list) {
                tx_type = TX_TYPE_EIP7702;
            } else if (!uint256_is_zero(&test->transaction.max_fee_per_blob_gas) ||
                test->transaction.blob_versioned_hashes_count > 0) {
                tx_type = TX_TYPE_EIP4844;
            } else if (!uint256_is_zero(&test->transaction.max_fee_per_gas)) {
                tx_type = TX_TYPE_EIP1559;
            } else if (access_list) {
                tx_type = TX_TYPE_EIP2930;
            }
            
            // Build transaction structure
            transaction_t tx = {
                .type = tx_type,
                .nonce = 0,  // Will be set from current state
                .sender = test->transaction.sender,
                .to = test->transaction.to,
                .value = value,
                .gas_limit = gas,
                .gas_price = test->transaction.gas_price,  // Legacy
                .max_fee_per_gas = test->transaction.max_fee_per_gas,  // EIP-1559
                .max_priority_fee_per_gas = test->transaction.max_priority_fee_per_gas,  // EIP-1559
                .data = data,
                .data_size = data_len,
                .is_create = test->transaction.is_create,
                .access_list = NULL,
                .access_list_count = 0,
                .max_fee_per_blob_gas = test->transaction.max_fee_per_blob_gas,
                .blob_versioned_hashes = test->transaction.blob_versioned_hashes,
                .blob_versioned_hashes_count = test->transaction.blob_versioned_hashes_count,
                .authorization_list = NULL,
                .authorization_list_count = 0
            };

            // Convert EIP-7702 authorization list
            authorization_t *tx_auth_list = NULL;
            if (test->transaction.authorization_list_count > 0) {
                tx_auth_list = calloc(test->transaction.authorization_list_count, sizeof(authorization_t));
                if (tx_auth_list) {
                    for (size_t i = 0; i < test->transaction.authorization_list_count; i++) {
                        const test_authorization_t *src = &test->transaction.authorization_list[i];
                        tx_auth_list[i].chain_id = src->chain_id;
                        tx_auth_list[i].address = src->address;
                        tx_auth_list[i].nonce = src->nonce;
                        tx_auth_list[i].y_parity = src->y_parity;
                        tx_auth_list[i].r = src->r;
                        tx_auth_list[i].s = src->s;
                        tx_auth_list[i].signer = src->signer;
                    }
                    tx.authorization_list = tx_auth_list;
                    tx.authorization_list_count = test->transaction.authorization_list_count;
                }
            }
            
            // Convert access list to transaction format
            access_list_entry_t *tx_access_list = NULL;
            if (access_list && access_list->entries_count > 0) {
                tx_access_list = calloc(access_list->entries_count, sizeof(access_list_entry_t));
                if (tx_access_list) {
                    for (size_t i = 0; i < access_list->entries_count; i++) {
                        tx_access_list[i].address = access_list->entries[i].address;
                        tx_access_list[i].storage_keys = access_list->entries[i].storage_keys;
                        tx_access_list[i].storage_keys_count = access_list->entries[i].storage_keys_count;
                    }
                    tx.access_list = tx_access_list;
                    tx.access_list_count = access_list->entries_count;
                }
            }
            
            // Get current nonce
            tx.nonce = evm_state_get_nonce(runner->state, &test->transaction.sender);
            
            // Build block environment from test env
            block_env_t block_env = {
                .coinbase = test->env.coinbase,
                .block_number = uint256_to_uint64(&test->env.number),
                .timestamp = uint256_to_uint64(&test->env.timestamp),
                .gas_limit = uint256_to_uint64(&test->env.gas_limit),
                .difficulty = test->env.difficulty,
                .base_fee = test->env.base_fee,
                .prev_randao = test->env.prev_randao,
                .excess_blob_gas = test->env.excess_blob_gas,
                .skip_coinbase_payment = false  // State tests include coinbase payment in expected state root
            };
            
            // Set block environment BEFORE applying access list (this sets the fork)
            evm_block_env_t evm_block = {
                .number = block_env.block_number,
                .timestamp = block_env.timestamp,
                .gas_limit = block_env.gas_limit,
                .difficulty = block_env.difficulty,
                .coinbase = block_env.coinbase,
                .base_fee = block_env.base_fee,
                .chain_id = uint256_from_uint64(runner->evm->chain_config->chain_id),
                .excess_blob_gas = block_env.excess_blob_gas,
            };
            evm_set_block_env(runner->evm, &evm_block);
            
            // Apply access list (EIP-2930) - mark addresses and storage keys as warm
            if (access_list && access_list->entries_count > 0 && runner->evm->fork >= FORK_BERLIN) {
                for (size_t i = 0; i < access_list->entries_count; i++) {
                    const test_access_list_entry_t *entry = &access_list->entries[i];
                    
                    // Mark address as warm
                    evm_mark_address_warm(runner->evm, &entry->address);
                    
                    // Mark storage keys as warm
                    for (size_t k = 0; k < entry->storage_keys_count; k++) {
                        evm_mark_storage_warm(runner->evm, &entry->address, &entry->storage_keys[k]);
                    }
                }
            }
            
            // Execute transaction using transaction execution layer
            transaction_result_t tx_result;
            bool tx_executed = transaction_execute(runner->evm, &tx, &block_env, &tx_result);
            
            // Check if test expects an exception
            if (post_cond->expect_exception != NULL) {
                // Test expects transaction to fail
                if (tx_executed && (tx_result.status == EVM_SUCCESS || tx_result.status == EVM_REVERT)) {
                    // Transaction succeeded or reverted cleanly, but should have been rejected
                    result->status = TEST_FAIL;
                    
                    const char *status_name = (tx_result.status == EVM_SUCCESS) ? "SUCCESS" : "REVERT";
                    char failure_msg[512];
                    snprintf(failure_msg, sizeof(failure_msg), 
                            "Transaction should have failed with '%s' but got status %s",
                            post_cond->expect_exception, status_name);
                    test_result_add_failure(result, "expect_exception", 
                                          post_cond->expect_exception, status_name, failure_msg);
                    
                    transaction_result_free(&tx_result);
                    if (tx_access_list) free(tx_access_list);
                    if (tx_auth_list) free(tx_auth_list);
                    
                    if (runner->config.stop_on_fail) {
                        goto cleanup;
                    }
                    continue; // Skip to next test case
                }
                
                // Transaction failed as expected
                
                if (tx_executed) {
                    transaction_result_free(&tx_result);
                }
                if (tx_access_list) free(tx_access_list);
                if (tx_auth_list) free(tx_auth_list);

                // Test passes for this case
                continue;
            }
            
            // No exception expected - transaction should succeed
            if (!tx_executed) {
                result->status = TEST_ERROR;
                test_result_add_failure(result, "execution", NULL, NULL, "Transaction execution failed but no exception was expected");
                if (tx_access_list) free(tx_access_list);
                if (tx_auth_list) free(tx_auth_list);
                goto cleanup;
            }
            
            // Free access list and auth list
            if (tx_access_list) free(tx_access_list);
            if (tx_auth_list) free(tx_auth_list);
            
            // Note: EVM errors (STACK_UNDERFLOW, OUT_OF_GAS, etc.) are valid
            // transaction outcomes in Ethereum — the transaction is still processed
            // (nonce incremented, gas consumed), just the EVM execution effects are
            // reverted. The state root check below is the sole arbiter of correctness.

            transaction_result_free(&tx_result);

            // Verify state root
            // EIP-161 (Spurious Dragon+): prune empty accounts from state trie
            bool prune_empty = (runner->evm->fork >= FORK_SPURIOUS_DRAGON);
            hash_t actual_root = evm_state_compute_state_root_ex(runner->state, prune_empty);
            if (!hash_equals(&actual_root, &post_cond->state_root)) {
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
