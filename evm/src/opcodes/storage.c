/**
 * EVM Storage Opcodes Implementation
 */

#include "evm.h"
#include "opcodes/storage.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

// SLOAD (0x54): Load word from storage
evm_status_t op_sload(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->state)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_ERROR("SLOAD: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t key;
    evm_stack_pop(evm->stack, &key);

    // TODO: Check if key is cold/warm and charge appropriate gas
    // For now, charge warm access cost (100 gas)
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // Load value from storage using current contract address
    uint256_t value;
    if (!state_db_get_state(evm->state, &evm->msg.recipient, &key, &value))
    {
        // If key doesn't exist, return zero
        value = UINT256_ZERO;
    }

    if (!evm_stack_push(evm->stack, &value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// SSTORE (0x55): Store word to storage
evm_status_t op_sstore(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->state)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check if we're in a static call (no state modifications allowed)
    if (evm->msg.is_static)
    {
        LOG_ERROR("SSTORE: State modification in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_ERROR("SSTORE: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t key, value;
    evm_stack_pop(evm->stack, &key);
    evm_stack_pop(evm->stack, &value);

    // Get current value from storage
    uint256_t current_value;
    if (!state_db_get_state(evm->state, &evm->msg.recipient, &key, &current_value)) {
        // If key doesn't exist, treat as zero
        current_value = uint256_from_uint64(0);
    }

    // Calculate SSTORE gas cost (Berlin fork with EIP-2929)
    // Full implementation should consider:
    // - Original value vs current value (for gas refunds - EIP-3529)
    // - Cold vs warm access (EIP-2929)
    // - Different fork rules (EIP-2200, EIP-1283, etc.)
    
    uint64_t gas_cost;
    bool is_zero_to_nonzero = uint256_is_zero(&current_value) && !uint256_is_zero(&value);
    bool is_nonzero_to_zero = !uint256_is_zero(&current_value) && uint256_is_zero(&value);
    
    if (is_zero_to_nonzero) {
        // Setting storage from zero to non-zero (most expensive)
        // Berlin fork (EIP-2929): Add cold access cost
        gas_cost = GAS_SSTORE_SET + GAS_SLOAD_COLD;
    } else if (is_nonzero_to_zero) {
        // Clearing storage (cheaper, and may get refund)
        gas_cost = GAS_SSTORE_RESET + GAS_SLOAD_COLD;
        // TODO: Add gas refund (EIP-3529 limits refunds)
    } else if (!uint256_is_zero(&current_value)) {
        // Modifying existing non-zero value
        gas_cost = GAS_SSTORE_RESET + GAS_SLOAD_COLD;
    } else {
        // Setting zero to zero (no-op, minimal cost + cold access)
        gas_cost = GAS_SLOAD_WARM + GAS_SLOAD_COLD;
    }
    
    // Deduct gas
    if (!evm_use_gas(evm, gas_cost)) {
        return EVM_OUT_OF_GAS;
    }

    // Store value to storage using current contract address
    if (!state_db_set_state(evm->state, &evm->msg.recipient, &key, &value))
    {
        LOG_ERROR("SSTORE: Failed to set storage");
        return EVM_INTERNAL_ERROR;
    }

    return EVM_SUCCESS;
}
