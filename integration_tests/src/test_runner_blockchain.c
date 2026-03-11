/**
 * Test Runner - Blockchain Test Execution
 *
 * Wires parsed blockchain test fixtures to the block executor:
 *   1. Setup genesis pre-state
 *   2. Verify genesis state root
 *   3. For each block: decode RLP → block_execute() → verify state root
 *   4. Verify post-state accounts (balance, nonce, code, storage)
 */

#include "test_runner.h"
#include "block.h"
#include "block_executor.h"
#include "fork.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

// Forward declarations from test_runner_core.c
extern uint64_t get_time_microseconds(void);
extern chain_config_t *create_test_chain_config(const char *fork_name);
extern bool hash_equals(const hash_t *a, const hash_t *b);

//==============================================================================
// Helpers
//==============================================================================

static void format_address(const address_t *addr, char *buf, size_t buf_size) {
    snprintf(buf, buf_size, "0x");
    for (int i = 0; i < 20; i++)
        snprintf(buf + 2 + i * 2, 3, "%02x", addr->bytes[i]);
}

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

    // Skip Verkle tests when built without Verkle backend
#ifndef ENABLE_VERKLE
    if (test->network && strcasecmp(test->network, "Verkle") == 0) {
        result->status = TEST_SKIP;
        result->skip_reason = strdup("Verkle backend not enabled");
        result->duration_us = get_time_microseconds() - start_time;
        return true;
    }
#endif

    // Setup fork/chain config
    chain_config_t *fork_config = create_test_chain_config(test->network);
    if (!fork_config) {
        result->status = TEST_SKIP;
        result->skip_reason = strdup("Unknown/transition fork");
        result->duration_us = get_time_microseconds() - start_time;
        return true;
    }
    if (runner->evm) {
        runner->evm->chain_config = fork_config;
    }

    // Open block 0 for genesis state writes (required for flat verkle backend)
    evm_state_begin_block(runner->state, 0);

    // Setup genesis/pre-state
    if (!test_runner_setup_state(runner->state, test->pre_state, test->pre_state_count)) {
        result->status = TEST_ERROR;
        test_result_add_failure(result, "genesis_state", NULL, NULL,
                              "Failed to setup genesis state");
        goto cleanup;
    }

    // Commit pre-state as "original" for EIP-2200 storage tracking
    evm_state_commit(runner->state);

    // Flush genesis state to verkle backend + commit block 0
    evm_state_finalize(runner->state);

    // Verify genesis state root
    {
        bool prune_empty = false;
        // Determine if we need EIP-161 pruning based on fork
        uint64_t genesis_ts = uint256_to_uint64(&test->genesis_header.timestamp);
        evm_fork_t fork = fork_get_active(0, genesis_ts, fork_config);
        if (fork >= FORK_SPURIOUS_DRAGON)
            prune_empty = true;

#ifdef ENABLE_MPT
        hash_t actual_genesis_root = evm_state_compute_mpt_root(runner->state, prune_empty);
#else
        hash_t actual_genesis_root = evm_state_compute_state_root_ex(runner->state, prune_empty);
#endif

        if (runner->config.verbose) {
            char *expected_str = hash_to_hex_string(&test->genesis_header.state_root);
            char *actual_str = hash_to_hex_string(&actual_genesis_root);
            printf("  Genesis state root: %s\n", actual_str);
            printf("  Expected root:      %s\n", expected_str);
            free(expected_str);
            free(actual_str);
        }

        if (!hash_equals(&actual_genesis_root, &test->genesis_header.state_root)) {
            char *expected_str = hash_to_hex_string(&test->genesis_header.state_root);
            char *actual_str = hash_to_hex_string(&actual_genesis_root);
            test_result_add_failure(result, "genesis_state_root",
                                  expected_str, actual_str,
                                  "Genesis state root mismatch");
            free(expected_str);
            free(actual_str);
            goto cleanup;
        }
    }

    // Block hash cache for BLOCKHASH opcode (ring buffer indexed by block_number % 256)
    hash_t block_hashes[256];
    memset(block_hashes, 0, sizeof(block_hashes));

    // Compute genesis block hash from genesis RLP
    if (test->genesis_rlp && test->genesis_rlp_len > 0) {
        hash_t genesis_hash = block_hash_from_rlp(test->genesis_rlp, test->genesis_rlp_len);
        block_hashes[0 % 256] = genesis_hash;
    }

    // Process each block
    for (size_t block_idx = 0; block_idx < test->block_count; block_idx++) {
        const test_block_t *block = &test->blocks[block_idx];

        if (runner->config.verbose) {
            printf("  Processing block %zu (%zu txs)...\n",
                   block_idx, block->tx_count);
        }

        // Invalid block: test expects this block to be rejected
        if (block->expect_exception) {
            if (runner->config.verbose) {
                printf("    Expected invalid block: %s — skipping execution\n",
                       block->expect_exception);
            }
            continue;
        }

        // Decode full block RLP → header + body
        if (!block->rlp || block->rlp_len == 0) {
            result->status = TEST_ERROR;
            char msg[128];
            snprintf(msg, sizeof(msg), "Block %zu has no RLP data", block_idx);
            test_result_add_failure(result, "block_rlp", NULL, NULL, msg);
            goto cleanup;
        }

        block_header_t hdr;
        block_body_t body;
        if (!block_decode_full_rlp(block->rlp, block->rlp_len, &hdr, &body)) {
            result->status = TEST_ERROR;
            char msg[128];
            snprintf(msg, sizeof(msg), "Failed to decode block %zu RLP", block_idx);
            test_result_add_failure(result, "block_decode", NULL, NULL, msg);
            goto cleanup;
        }

        // Debug: print coinbase from decoded header
        if (runner->config.verbose) {
            char cb[43];
            format_address(&hdr.coinbase, cb, sizeof(cb));
            printf("    Header coinbase: %s, number=%lu, gas_limit=%lu, withdrawals=%zu\n",
                   cb, hdr.number, hdr.gas_limit, body.withdrawal_count);
        }

        // Execute block (pass block hashes for BLOCKHASH opcode)
        block_result_t block_result = block_execute(runner->evm, &hdr, &body, block_hashes);

        {
            printf("    gas_used=%lu (expected=%lu), tx_count=%zu, success=%d\n",
                   block_result.gas_used, hdr.gas_used, block_result.tx_count,
                   block_result.success);
            /* DEBUG: show root from block_execute */
            char *be_root_str = hash_to_hex_string(&block_result.state_root);
            printf("    block_execute root: %s\n", be_root_str);
            free(be_root_str);
        }

        // Track statistics
        runner->total_transactions += block_result.tx_count;
        runner->total_gas_used += block_result.gas_used;

        // Verify state root against expected (from JSON block header)
        // Compute state root ourselves (block_execute's finalize may affect root)
        bool prune = (runner->evm->fork >= FORK_SPURIOUS_DRAGON);
#ifdef ENABLE_MPT
        hash_t computed_root = evm_state_compute_mpt_root(runner->state, prune);
#else
        hash_t computed_root = evm_state_compute_state_root_ex(runner->state, prune);
#endif
        /* DEBUG: show computed root and expected */
        {
            char *cr_str = hash_to_hex_string(&computed_root);
            hash_t *er = (hash_t *)&block->header.state_root;
            char *er_str = hash_to_hex_string(er);
            printf("    computed_root:      %s\n", cr_str);
            printf("    expected_root:      %s\n", er_str);
            free(cr_str);
            free(er_str);
        }
        hash_t *expected_root = (hash_t *)&block->header.state_root;
        if (!hash_equals(&computed_root, expected_root)) {
            char *expected_str = hash_to_hex_string(expected_root);
            char *actual_str = hash_to_hex_string(&computed_root);
            char msg[128];
            snprintf(msg, sizeof(msg), "State root mismatch after block %zu", block_idx);
            test_result_add_failure(result, "block_state_root",
                                  expected_str, actual_str, msg);

            free(expected_str);
            free(actual_str);

            block_result_free(&block_result);
            block_body_free(&body);

            if (runner->config.stop_on_fail) {
                goto cleanup;
            }
            // Still store the block hash even on state root mismatch
            hash_t bh = block_hash_from_rlp(block->rlp, block->rlp_len);
            block_hashes[hdr.number % 256] = bh;
            continue;
        }

        // Store this block's hash for subsequent BLOCKHASH lookups
        {
            hash_t bh = block_hash_from_rlp(block->rlp, block->rlp_len);
            block_hashes[hdr.number % 256] = bh;
        }

        block_result_free(&block_result);
        block_body_free(&body);
    }

    // Verify post-state accounts (if provided)
    if (test->post_state_count > 0) {
        for (size_t i = 0; i < test->post_state_count; i++) {
            const test_account_t *expected_acc = &test->post_state[i];

            char addr_str[43];
            format_address(&expected_acc->address, addr_str, sizeof(addr_str));

            // Verify balance
            uint256_t actual_balance = evm_state_get_balance(runner->state, &expected_acc->address);
            if (!uint256_eq(&actual_balance, &expected_acc->balance)) {
                char *expected_str = uint256_to_hex_string(&expected_acc->balance);
                char *actual_str = uint256_to_hex_string(&actual_balance);
                char msg[256];
                snprintf(msg, sizeof(msg), "Balance mismatch for %s", addr_str);
                test_result_add_failure(result, "account_balance",
                                      expected_str, actual_str, msg);
                free(expected_str);
                free(actual_str);
                if (runner->config.stop_on_fail) goto cleanup;
            }

            // Verify nonce
            uint64_t actual_nonce = evm_state_get_nonce(runner->state, &expected_acc->address);
            uint64_t expected_nonce = uint256_to_uint64(&expected_acc->nonce);
            if (actual_nonce != expected_nonce) {
                char expected_str[32], actual_str[32];
                snprintf(expected_str, sizeof(expected_str), "0x%lx", expected_nonce);
                snprintf(actual_str, sizeof(actual_str), "0x%lx", actual_nonce);
                char msg[256];
                snprintf(msg, sizeof(msg), "Nonce mismatch for %s", addr_str);
                test_result_add_failure(result, "account_nonce",
                                      expected_str, actual_str, msg);
                if (runner->config.stop_on_fail) goto cleanup;
            }

            // Verify code
            if (expected_acc->code && expected_acc->code_len > 0) {
                uint32_t actual_code_len = 0;
                const uint8_t *actual_code = evm_state_get_code_ptr(runner->state,
                                                                    &expected_acc->address,
                                                                    &actual_code_len);
                if ((size_t)actual_code_len != expected_acc->code_len ||
                    (actual_code_len > 0 && memcmp(actual_code, expected_acc->code,
                                                    actual_code_len) != 0)) {
                    char expected_str[32], actual_str[32];
                    snprintf(expected_str, sizeof(expected_str), "%zu bytes", expected_acc->code_len);
                    snprintf(actual_str, sizeof(actual_str), "%u bytes", actual_code_len);
                    char msg[256];
                    snprintf(msg, sizeof(msg), "Code mismatch for %s", addr_str);
                    test_result_add_failure(result, "account_code",
                                          expected_str, actual_str, msg);
                    if (runner->config.stop_on_fail) goto cleanup;
                }
            }

            // Verify storage
            for (size_t j = 0; j < expected_acc->storage_count; j++) {
                const test_storage_entry_t *entry = &expected_acc->storage[j];
                uint256_t actual_val = evm_state_get_storage(runner->state,
                                                             &expected_acc->address,
                                                             &entry->key);
                if (!uint256_eq(&actual_val, &entry->value)) {
                    char *expected_str = uint256_to_hex_string(&entry->value);
                    char *actual_str = uint256_to_hex_string(&actual_val);
                    char msg[256];
                    snprintf(msg, sizeof(msg), "Storage mismatch for %s", addr_str);
                    test_result_add_failure(result, "account_storage",
                                          expected_str, actual_str, msg);
                    free(expected_str);
                    free(actual_str);
                    if (runner->config.stop_on_fail) goto cleanup;
                }
            }
        }
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
