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
static uint64_t calculate_intrinsic_gas(const transaction_t *tx, evm_fork_t fork) {
    const uint64_t G_TRANSACTION = 21000;
    const uint64_t G_TX_DATA_ZERO = 4;
    const uint64_t G_TX_DATA_NONZERO_ISTANBUL = 16;   // EIP-2028 (Istanbul+)
    const uint64_t G_TX_DATA_NONZERO_LEGACY = 68;     // Pre-Istanbul
    const uint64_t G_TX_CREATE = 32000;
    const uint64_t G_ACCESS_LIST_ADDRESS = 2400;  // EIP-2930
    const uint64_t G_ACCESS_LIST_STORAGE_KEY = 1900;  // EIP-2930

    uint64_t g_tx_data_nonzero = (fork >= FORK_ISTANBUL) ?
        G_TX_DATA_NONZERO_ISTANBUL : G_TX_DATA_NONZERO_LEGACY;

    uint64_t gas = G_TRANSACTION;

    // Add cost for contract creation (Homestead+, EIP-2)
    if (tx->is_create && fork >= FORK_HOMESTEAD) {
        gas += G_TX_CREATE;

        // EIP-3860 (Shanghai+): initcode word gas
        if (fork >= FORK_SHANGHAI && tx->data_size > 0) {
            uint64_t words = (tx->data_size + 31) / 32;
            gas += 2 * words;  // INITCODE_WORD_GAS = 2
        }
    }

    // Add cost for calldata
    if (tx->data && tx->data_size > 0) {
        for (size_t i = 0; i < tx->data_size; i++) {
            if (tx->data[i] == 0) {
                gas += G_TX_DATA_ZERO;
            } else {
                gas += g_tx_data_nonzero;
            }
        }
    }

    // Add cost for access list (EIP-2930)
    if (tx->access_list && tx->access_list_count > 0) {
        for (size_t i = 0; i < tx->access_list_count; i++) {
            gas += G_ACCESS_LIST_ADDRESS;
            gas += tx->access_list[i].storage_keys_count * G_ACCESS_LIST_STORAGE_KEY;
        }
    }

    return gas;
}

/**
 * EIP-7623 (Prague+): Calculate floor data gas cost
 * floor_gas = tokens_in_calldata * FLOOR_CALLDATA_COST + TX_BASE_COST
 * tokens = zero_bytes * 1 + non_zero_bytes * 4
 */
static uint64_t calculate_floor_data_gas(const transaction_t *tx, evm_fork_t fork) {
    if (fork < FORK_PRAGUE) return 0;

    const uint64_t FLOOR_CALLDATA_COST = 10;
    const uint64_t TX_BASE_COST = 21000;

    uint64_t tokens = 0;
    if (tx->data && tx->data_size > 0) {
        for (size_t i = 0; i < tx->data_size; i++) {
            tokens += (tx->data[i] == 0) ? 1 : 4;
        }
    }

    return tokens * FLOOR_CALLDATA_COST + TX_BASE_COST;
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
    evm_state_t *state,
    const transaction_t *tx,
    const block_env_t *env)
{
    if (!state || !tx || !env)
        return false;

    // Check nonce
    uint64_t current_nonce = evm_state_get_nonce(state, &tx->sender);
    if (tx->nonce != current_nonce) {
        LOG_EVM_ERROR("Transaction nonce mismatch: expected %lu, got %lu",
                 current_nonce, tx->nonce);
        return false;
    }

    // Check balance for value + max gas cost
    uint256_t sender_balance = evm_state_get_balance(state, &tx->sender);

    uint256_t effective_gas_price = transaction_effective_gas_price(tx, env);
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);
    uint256_t total_cost = uint256_add(&tx->value, &gas_cost);

    if (uint256_lt(&sender_balance, &total_cost)) {
        LOG_EVM_ERROR("Insufficient balance for transaction");
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
    const transaction_t *tx,
    const block_env_t *env,
    transaction_result_t *result)
{
    if (!evm || !tx || !env || !result)
        return false;

    evm_state_t *state = evm->state;

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

    // Reject transaction types not supported by the current fork
    if (tx->type == TX_TYPE_EIP2930 && evm->fork < FORK_BERLIN) {
        LOG_EVM_ERROR("EIP-2930 type-1 tx not valid before Berlin");
        return false;
    }
    if (tx->type == TX_TYPE_EIP1559 && evm->fork < FORK_LONDON) {
        LOG_EVM_ERROR("EIP-1559 type-2 tx not valid before London");
        return false;
    }
    if (tx->type == TX_TYPE_EIP4844 && evm->fork < FORK_CANCUN) {
        LOG_EVM_ERROR("EIP-4844 type-3 tx not valid before Cancun");
        return false;
    }

    // EIP-3860 (Shanghai+): Reject contract creation with initcode > MAX_INITCODE_SIZE
    if (tx->is_create && evm->fork >= FORK_SHANGHAI && tx->data_size > 49152) {
        LOG_EVM_ERROR("Initcode size %zu exceeds limit 49152", tx->data_size);
        return false;
    }

    // Transaction gas limit must not exceed block gas limit
    if (tx->gas_limit > env->gas_limit) {
        LOG_EVM_ERROR("Transaction gas limit %lu exceeds block gas limit %lu",
                      tx->gas_limit, env->gas_limit);
        return false;
    }

    // EIP-1559 validations
    if (tx->type == TX_TYPE_EIP1559 || tx->type == TX_TYPE_EIP4844) {
        // max_priority_fee_per_gas must not exceed max_fee_per_gas
        if (uint256_gt(&tx->max_priority_fee_per_gas, &tx->max_fee_per_gas)) {
            LOG_EVM_ERROR("max_priority_fee_per_gas exceeds max_fee_per_gas");
            return false;
        }
        // max_fee_per_gas must be >= block base_fee
        if (uint256_lt(&tx->max_fee_per_gas, &env->base_fee)) {
            LOG_EVM_ERROR("max_fee_per_gas below block base_fee");
            return false;
        }
    }

    // Post-London: legacy/EIP-2930 gas_price must be >= base_fee
    if (evm->fork >= FORK_LONDON &&
        (tx->type == TX_TYPE_LEGACY || tx->type == TX_TYPE_EIP2930)) {
        if (uint256_lt(&tx->gas_price, &env->base_fee)) {
            LOG_EVM_ERROR("gas_price below block base_fee");
            return false;
        }
    }

    // Balance check: sender must have enough for maximum gas cost + value
    // EIP-1559: use max_fee_per_gas (not effective_gas_price) for balance check
    {
        uint256_t max_gas_price;
        if (tx->type == TX_TYPE_EIP1559 || tx->type == TX_TYPE_EIP4844) {
            max_gas_price = tx->max_fee_per_gas;
        } else {
            max_gas_price = tx->gas_price;
        }
        uint256_t gl = uint256_from_uint64(tx->gas_limit);
        uint256_t max_gas_cost = uint256_mul(&max_gas_price, &gl);
        uint256_t total_cost = uint256_add(&max_gas_cost, &tx->value);
        uint256_t sender_balance = evm_state_get_balance(state, &tx->sender);
        if (uint256_lt(&sender_balance, &total_cost)) {
            LOG_EVM_ERROR("Insufficient sender balance for max gas cost + value");
            return false;
        }
    }

    // Reject transaction if sender nonce is at max (overflow protection)
    {
        uint64_t sender_nonce = evm_state_get_nonce(state, &tx->sender);
        if (sender_nonce == UINT64_MAX) {
            LOG_EVM_ERROR("Sender nonce at maximum, cannot increment");
            return false;
        }
    }

    // Calculate effective gas price
    uint256_t effective_gas_price = transaction_effective_gas_price(tx, env);
    uint256_t gas_limit_u256 = uint256_from_uint64(tx->gas_limit);
    uint256_t gas_cost = uint256_mul(&effective_gas_price, &gas_limit_u256);

    // Create snapshot for potential rollback
    uint32_t snapshot = evm_state_snapshot(state);

    //==========================================================================
    // Pre-execution: State changes before EVM
    //==========================================================================

    // Get sender nonce
    uint64_t sender_nonce = evm_state_get_nonce(state, &tx->sender);

    // Calculate contract address BEFORE incrementing nonce
    address_t contract_address = {0};
    if (tx->is_create) {
        contract_address = calculate_create_address(&tx->sender, sender_nonce);
    }

    // Increment sender nonce
    evm_state_set_nonce(state, &tx->sender, sender_nonce + 1);

    // Deduct upfront gas cost from sender
    if (!evm_state_sub_balance(state, &tx->sender, &gas_cost)) {
        LOG_EVM_ERROR("Failed to deduct gas cost from sender");
        evm_state_revert(state, snapshot);
        return false;
    }

    // Snapshot AFTER nonce increment and gas deduction, but BEFORE value transfer
    // and contract creation. Per Ethereum spec, on EVM error/revert we revert
    // value transfer and execution state, but keep nonce increment and gas deduction.
    uint32_t exec_snapshot = evm_state_snapshot(state);

    // For contract creation, check for collision and create account
    bool collision = false;
    if (tx->is_create) {
        // EIP-7610: collision if account has nonce, code, or storage
        uint64_t existing_nonce = evm_state_get_nonce(state, &contract_address);
        uint32_t existing_code = evm_state_get_code_size(state, &contract_address);
        bool has_storage = evm_state_has_storage(state, &contract_address);
        if (existing_nonce > 0 || existing_code > 0 || has_storage) {
            LOG_EVM_DEBUG("Contract address collision: nonce=%lu, code_size=%u, storage=%d",
                         existing_nonce, existing_code, has_storage);
            collision = true;
        } else {
            evm_state_create_account(state, &contract_address);

            // EIP-161 (Spurious Dragon+): new contracts get nonce=1
            if (evm->fork >= FORK_SPURIOUS_DRAGON) {
                evm_state_set_nonce(state, &contract_address, 1);
            }
        }
    }

    //==========================================================================
    // EVM Execution
    //==========================================================================

    // Calculate intrinsic gas
    uint64_t intrinsic_gas = calculate_intrinsic_gas(tx, evm->fork);

    // EIP-7623 (Prague+): floor data gas
    uint64_t floor_data_gas = calculate_floor_data_gas(tx, evm->fork);

    // Check if transaction has enough gas for intrinsic cost
    // EIP-7623: must also have enough for floor data gas
    uint64_t min_gas_required = intrinsic_gas > floor_data_gas ? intrinsic_gas : floor_data_gas;
    if (tx->gas_limit < min_gas_required) {
        LOG_EVM_ERROR("Insufficient gas: limit=%lu, intrinsic=%lu, floor=%lu",
                 tx->gas_limit, intrinsic_gas, floor_data_gas);
        evm_state_revert(state, snapshot);
        return false;
    }

    // Handle collision: revert execution state, consume all gas
    if (collision) {
        evm_state_revert(state, exec_snapshot);
        result->status = EVM_INTERNAL_ERROR;
        result->gas_used = tx->gas_limit;
        result->gas_refund = 0;
        goto post_execution;
    }

    // Transfer value (if any)
    if (!uint256_is_zero(&tx->value)) {
        if (!evm_state_sub_balance(state, &tx->sender, &tx->value)) {
            LOG_EVM_ERROR("Failed to deduct value from sender");
            evm_state_revert(state, snapshot);
            return false;
        }

        address_t recipient = tx->is_create ? contract_address : tx->to;
        evm_state_add_balance(state, &recipient, &tx->value);
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
        evm_state_revert(state, snapshot);
        return false;
    }

    // Handle execution result based on status
    result->status = evm_result.status;

    if (evm_result.status != EVM_SUCCESS && evm_result.status != EVM_REVERT) {
        // EVM error (STACK_OVERFLOW, OUT_OF_GAS, INVALID_JUMP, etc.)
        // Per Ethereum spec: revert state changes from execution and consume ALL gas
        evm_state_revert(state, exec_snapshot);
        result->gas_used = tx->gas_limit;
        result->gas_refund = 0;
    } else if (evm_result.status == EVM_REVERT) {
        // REVERT opcode: revert state changes but refund unused gas
        evm_state_revert(state, exec_snapshot);
        result->gas_used = intrinsic_gas + (gas_for_execution - evm_result.gas_left);
        result->gas_refund = 0;
    } else {
        // SUCCESS: keep state changes, normal gas accounting
        result->gas_used = intrinsic_gas + (gas_for_execution - evm_result.gas_left);
        result->gas_refund = evm_result.gas_refund;

        // Handle contract creation on success
        if (tx->is_create) {
            if (evm_result.output_data && evm_result.output_size > 0) {
                // Charge deployment gas (200 gas per byte)
                const uint64_t G_CODE_DEPOSIT = 200;
                uint64_t deployment_gas = evm_result.output_size * G_CODE_DEPOSIT;

                // Check if enough gas left for deployment
                if (evm_result.gas_left < deployment_gas) {
                    LOG_EVM_DEBUG("Insufficient gas for code deployment");
                    evm_state_revert(state, exec_snapshot);
                    result->status = EVM_OUT_OF_GAS;
                    result->gas_used = tx->gas_limit;
                    result->gas_refund = 0;
                    result->contract_created = false;
                } else {
                    // Deduct deployment gas
                    result->gas_used += deployment_gas;

                    // Store contract code
                    evm_state_set_code(state, &contract_address,
                                      evm_result.output_data,
                                      (uint32_t)evm_result.output_size);

                    result->contract_address = contract_address;
                    result->contract_created = true;
                }
            } else {
                // Empty code - contract created with no code
                result->contract_address = contract_address;
                result->contract_created = true;
            }
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
post_execution:

    // Calculate actual gas cost
    uint64_t gas_used = result->gas_used;
    uint64_t gas_refund = result->gas_refund;

    // Apply refund cap: London+ (EIP-3529) = gas_used/5, pre-London = gas_used/2
    uint64_t max_refund = (evm->fork >= FORK_LONDON) ? gas_used / 5 : gas_used / 2;
    if (gas_refund > max_refund) {
        gas_refund = max_refund;
    }

    // EIP-7623 (Prague+): enforce floor data gas after refunds
    // gas_used after refund = gas_used - gas_refund
    // If that's less than floor_data_gas, clamp to floor_data_gas (and reduce refund accordingly)
    if (floor_data_gas > 0) {
        uint64_t gas_used_after_refund = gas_used - gas_refund;
        if (gas_used_after_refund < floor_data_gas) {
            // Clamp: effective gas_used_after_refund = floor_data_gas
            // So gas_refund = gas_used - floor_data_gas (could be 0)
            gas_refund = (gas_used > floor_data_gas) ? gas_used - floor_data_gas : 0;
            gas_used = (gas_used > floor_data_gas) ? gas_used : floor_data_gas;
        }
    }

    // Calculate gas to refund: unused gas + capped refund
    uint64_t gas_to_refund = tx->gas_limit - gas_used + gas_refund;

    uint256_t gas_to_refund_u256 = uint256_from_uint64(gas_to_refund);
    uint256_t refund_amount = uint256_mul(&effective_gas_price, &gas_to_refund_u256);

    // Refund unused gas to sender
    evm_state_add_balance(state, &tx->sender, &refund_amount);

    // Pay coinbase (miner) for gas used
    if (!env->skip_coinbase_payment) {
        uint64_t gas_paid = gas_used - gas_refund;
        uint256_t gas_paid_u256 = uint256_from_uint64(gas_paid);

        // For London+ (EIP-1559): coinbase only gets priority fee (effective_gas_price - base_fee)
        // Pre-London: coinbase gets full effective gas price
        uint256_t coinbase_gas_price;
        if (evm->fork >= FORK_LONDON) {
            coinbase_gas_price = uint256_sub(&effective_gas_price, &env->base_fee);
        } else {
            coinbase_gas_price = effective_gas_price;
        }
        uint256_t coinbase_payment = uint256_mul(&coinbase_gas_price, &gas_paid_u256);

        evm_state_add_balance(state, &env->coinbase, &coinbase_payment);
    }

    // NOTE: EIP-161 empty account cleanup and finalize are handled by caller

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
