/**
 * Transaction Execution Layer Implementation
 */

#include "transaction.h"
#include "evm.h"
#include "opcodes/create.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate intrinsic gas for a transaction
 * 
 * Intrinsic gas includes:
 * - Base transaction cost (21000 gas)
 * - Cost for calldata (4 gas per zero byte, 16 gas per non-zero byte)
 * - Cost for contract creation (32000 gas if creating contract)
 * - Cost for access list entries (EIP-2930)
 */
static uint64_t calculate_intrinsic_gas(const transaction_t *tx) {
    const uint64_t G_TRANSACTION = 21000;
    const uint64_t G_TX_DATA_ZERO = 4;
    const uint64_t G_TX_DATA_NONZERO = 16;  // 68 for post-Istanbul
    const uint64_t G_TX_CREATE = 32000;
    
    uint64_t gas = G_TRANSACTION;
    
    // Add cost for contract creation
    if (tx->is_create) {
        gas += G_TX_CREATE;
    }
    
    // Add cost for calldata
    if (tx->data && tx->data_size > 0) {
        for (size_t i = 0; i < tx->data_size; i++) {
            if (tx->data[i] == 0) {
                gas += G_TX_DATA_ZERO;
            } else {
                gas += G_TX_DATA_NONZERO;
            }
        }
    }
    
    // TODO: Add cost for access list (EIP-2930)
    // Each address costs 2400 gas
    // Each storage key costs 1900 gas
    
    return gas;
}

/**
 * Calculate effective gas price based on transaction type
 */
uint256_t transaction_effective_gas_price(
    const transaction_t *tx,
    const block_env_t *env)
{
    if (!tx || !env)
        return uint256_from_uint64(0);

    switch (tx->type) {
        case TX_TYPE_LEGACY:
        case TX_TYPE_EIP2930:
            return tx->gas_price;
        
        case TX_TYPE_EIP1559:
        case TX_TYPE_EIP4844: {
            // min(max_fee_per_gas, base_fee + max_priority_fee_per_gas)
            uint256_t base_plus_priority = uint256_add(&env->base_fee, &tx->max_priority_fee_per_gas);
            return uint256_lt(&tx->max_fee_per_gas, &base_plus_priority) 
                ? tx->max_fee_per_gas 
                : base_plus_priority;
        }
        
        default:
            return tx->gas_price;
    }
}

//==============================================================================
// Transaction Validation
//==============================================================================

bool transaction_validate(
    const state_db_t *state,
    const transaction_t *tx,
    const block_env_t *env)
{
    if (!state || !tx || !env)
        return false;

    // Check nonce
    uint64_t current_nonce;
    if (state_db_get_nonce(state, &tx->sender, &current_nonce)) {
        if (tx->nonce != current_nonce) {
            LOG_EVM_ERROR("Transaction nonce mismatch: expected %lu, got %lu", 
                     current_nonce, tx->nonce);
            return false;
        }
    } else {
        // Account doesn't exist - nonce must be 0
        if (tx->nonce != 0) {
            LOG_EVM_ERROR("Transaction nonce must be 0 for new account");
            return false;
        }
    }

    // Check balance for value + max gas cost
    uint256_t sender_balance;
    if (!state_db_get_balance(state, &tx->sender, &sender_balance)) {
        sender_balance = uint256_from_uint64(0);
    }

    uint256_t effective_gas_price = transaction_effective_gas_price(tx, env);
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);
    uint256_t total_cost = uint256_add(&tx->value, &gas_cost);

    if (uint256_lt(&sender_balance, &total_cost)) {
        LOG_EVM_ERROR("Insufficient balance: need %s, have %s",
                 uint256_to_hex(&total_cost),
                 uint256_to_hex(&sender_balance));
        return false;
    }

    // Check gas limit
    if (tx->gas_limit > env->gas_limit) {
        LOG_EVM_ERROR("Transaction gas limit exceeds block gas limit");
        return false;
    }

    return true;
}

//==============================================================================
// Transaction Execution
//==============================================================================

bool transaction_execute(
    evm_t *evm,
    state_db_t *state,
    const transaction_t *tx,
    const block_env_t *env,
    transaction_result_t *result)
{
    if (!evm || !state || !tx || !env || !result)
        return false;

    // Initialize result
    memset(result, 0, sizeof(transaction_result_t));
    result->status = EVM_SUCCESS;

    // Set block environment in EVM (this will determine the fork)
    evm_block_env_t block_env = {
        .number = env->block_number,
        .timestamp = env->timestamp,
        .gas_limit = env->gas_limit,
        .difficulty = env->difficulty,
        .coinbase = env->coinbase,
        .base_fee = env->base_fee,
        .chain_id = evm->chain_config ? uint256_from_uint64(evm->chain_config->chain_id) : uint256_from_uint64(1)
    };
    evm_set_block_env(evm, &block_env);

    // Calculate effective gas price
    uint256_t effective_gas_price = transaction_effective_gas_price(tx, env);
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);

    // Create snapshot for potential rollback
    uint32_t snapshot = state_db_begin_transaction(state);

    //==========================================================================
    // Pre-execution: State changes before EVM
    //==========================================================================

    // Increment sender nonce
    uint64_t sender_nonce;
    if (!state_db_get_nonce(state, &tx->sender, &sender_nonce)) {
        sender_nonce = 0;
    }
    
    // Calculate contract address BEFORE incrementing nonce
    address_t contract_address = {0};
    if (tx->is_create) {
        contract_address = calculate_create_address(&tx->sender, sender_nonce);
    }
    
    if (!state_db_set_nonce(state, &tx->sender, sender_nonce + 1)) {
        LOG_EVM_ERROR("Failed to increment sender nonce");
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }

    // Deduct upfront gas cost from sender
    if (!state_db_sub_balance(state, &tx->sender, &gas_cost)) {
        LOG_EVM_ERROR("Failed to deduct gas cost from sender");
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }

    // For contract creation, create empty account at contract address
    if (tx->is_create) {
        // Try to create account - will fail if collision
        if (!state_db_create_account(state, &contract_address)) {
            // Account already exists - check if it's a collision
            uint64_t existing_nonce;
            if (state_db_get_nonce(state, &contract_address, &existing_nonce)) {
                if (existing_nonce > 0) {
                    LOG_EVM_ERROR("Contract address collision: nonce=%lu", existing_nonce);
                    state_db_revert_to_snapshot(state, snapshot);
                    return false;
                }
            }
            // If nonce is 0, account was just accessed in cache
        }
    }
    
    // Transfer value (if any)
    if (!uint256_is_zero(&tx->value)) {
        if (!state_db_sub_balance(state, &tx->sender, &tx->value)) {
            LOG_EVM_ERROR("Failed to deduct value from sender");
            state_db_revert_to_snapshot(state, snapshot);
            return false;
        }
        
        address_t recipient = tx->is_create ? contract_address : tx->to;
        if (!state_db_add_balance(state, &recipient, &tx->value)) {
            LOG_EVM_ERROR("Failed to add value to recipient");
            state_db_revert_to_snapshot(state, snapshot);
            return false;
        }
    }

    //==========================================================================
    // EVM Execution
    //==========================================================================

    // Calculate intrinsic gas
    uint64_t intrinsic_gas = calculate_intrinsic_gas(tx);
    
    // Check if transaction has enough gas for intrinsic cost
    if (tx->gas_limit < intrinsic_gas) {
        LOG_EVM_ERROR("Insufficient gas: limit=%lu, intrinsic=%lu", 
                 tx->gas_limit, intrinsic_gas);
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }
    
    // Gas available for EVM execution
    uint64_t gas_for_execution = tx->gas_limit - intrinsic_gas;

    // Create EVM message
    address_t recipient = tx->is_create ? contract_address : tx->to;
    address_t code_addr = tx->is_create ? contract_address : tx->to;
    
    evm_message_t msg = {
        .kind = tx->is_create ? EVM_CREATE : EVM_CALL,
        .caller = tx->sender,
        .recipient = recipient,
        .code_addr = code_addr,
        .value = tx->value,
        .input_data = tx->data,
        .input_size = tx->data_size,
        .gas = gas_for_execution,  // Gas after intrinsic deduction
        .depth = 0,
        .is_static = false
    };

    // Execute EVM
    evm_result_t evm_result;
    if (!evm_execute(evm, &msg, &evm_result)) {
        LOG_EVM_ERROR("EVM execution failed");
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }

    // Store execution result
    result->status = evm_result.status;
    result->gas_used = intrinsic_gas + (gas_for_execution - evm_result.gas_left);
    result->gas_refund = evm_result.gas_refund;

    // Handle contract creation
    if (tx->is_create) {
        if (evm_result.status == EVM_SUCCESS) {
            // Store deployed code
            if (evm_result.output_data && evm_result.output_size > 0) {
                // Charge deployment gas (200 gas per byte)
                const uint64_t G_CODE_DEPOSIT = 200;
                uint64_t deployment_gas = evm_result.output_size * G_CODE_DEPOSIT;
                
                // Check if enough gas left for deployment
                if (evm_result.gas_left < deployment_gas) {
                    LOG_EVM_DEBUG("Insufficient gas for code deployment");
                    // Out of gas during deployment - delete contract account
                    state_db_suicide(state, &contract_address);
                    result->status = EVM_OUT_OF_GAS;
                    result->gas_used = tx->gas_limit - intrinsic_gas;  // All execution gas consumed
                    result->contract_created = false;
                } else {
                    // Deduct deployment gas
                    result->gas_used += deployment_gas;
                    
                    // Store contract code
                    if (!state_db_set_code(state, &contract_address, 
                                          evm_result.output_data, 
                                          evm_result.output_size)) {
                        LOG_EVM_ERROR("Failed to store contract code");
                        state_db_revert_to_snapshot(state, snapshot);
                        evm_result_free(&evm_result);
                        return false;
                    }
                    
                    result->contract_address = contract_address;
                    result->contract_created = true;
                }
            } else {
                // Empty code - contract created with no code
                result->contract_address = contract_address;
                result->contract_created = true;
            }
        } else if (evm_result.status == EVM_REVERT) {
            // REVERT during contract creation
            // Delete contract account and revert state changes
            state_db_suicide(state, &contract_address);
            result->status = EVM_SUCCESS;  // Transaction succeeds even if init code reverts
            result->contract_created = false;
        } else {
            // Other errors (out of gas, stack underflow, etc.)
            // Delete contract account
            state_db_suicide(state, &contract_address);
            result->contract_created = false;
        }
    }

    // Copy output data
    if (evm_result.output_data && evm_result.output_size > 0) {
        result->output_data = malloc(evm_result.output_size);
        if (result->output_data) {
            memcpy(result->output_data, evm_result.output_data, evm_result.output_size);
            result->output_size = evm_result.output_size;
        }
    }

    evm_result_free(&evm_result);

    //==========================================================================
    // Post-execution: Gas refunds and coinbase payment
    //==========================================================================

    // Calculate actual gas cost
    uint64_t gas_used = result->gas_used;
    uint64_t gas_refund = result->gas_refund;
    
    // Apply refund (capped at gas_used / 2 per EIP-3529)
    uint64_t max_refund = gas_used / 2;
    if (gas_refund > max_refund) {
        gas_refund = max_refund;
    }
    
    uint64_t gas_to_refund = tx->gas_limit - gas_used + gas_refund;
    uint256_t gas_to_refund_u256 = uint256_from_uint64(gas_to_refund);
    uint256_t refund_amount = uint256_mul(&effective_gas_price, &gas_to_refund_u256);

    // Refund unused gas to sender
    if (!state_db_add_balance(state, &tx->sender, &refund_amount)) {
        LOG_EVM_ERROR("Failed to refund gas to sender");
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }

    // Pay coinbase (miner) for gas used
    uint64_t gas_paid = gas_used - gas_refund;
    uint256_t gas_paid_u256 = uint256_from_uint64(gas_paid);
    uint256_t coinbase_payment = uint256_mul(&effective_gas_price, &gas_paid_u256);
    
    if (!state_db_add_balance(state, &env->coinbase, &coinbase_payment)) {
        LOG_EVM_ERROR("Failed to pay coinbase");
        state_db_revert_to_snapshot(state, snapshot);
        return false;
    }

    //==========================================================================
    // Finalize
    //==========================================================================

    // Commit all state changes
    if (!state_db_commit_transaction(state)) {
        LOG_EVM_ERROR("Failed to commit transaction");
        return false;
    }

    return true;
}

//==============================================================================
// Cleanup
//==============================================================================

void transaction_result_free(transaction_result_t *result)
{
    if (!result)
        return;

    if (result->output_data) {
        free(result->output_data);
        result->output_data = NULL;
    }
    
    result->output_size = 0;
}
