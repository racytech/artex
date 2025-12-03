/**
 * EVM Storage Opcodes Implementation
 */

#include "evm.h"
#include "opcodes/storage.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate SSTORE gas cost based on fork rules
 * 
 * Implements fork-specific gas calculation for SSTORE:
 * - Frontier/Homestead: Simple set (20000) or reset (5000)
 * - Constantinople (EIP-1283): Net gas metering with refunds
 * - Petersburg: Reverted EIP-1283, back to Homestead rules
 * - Istanbul (EIP-2200): Improved net gas metering
 * - Berlin+ (EIP-2929): Added cold/warm access costs
 * 
 * @param evm EVM context (for fork detection)
 * @param current_value Current value in storage
 * @param new_value New value to store
 * @return Gas cost for this SSTORE operation
 */
static uint64_t calculate_sstore_gas(
    evm_t *evm,
    const uint256_t *current_value,
    const uint256_t *new_value)
{
    bool current_is_zero = uint256_is_zero(current_value);
    bool new_is_zero = uint256_is_zero(new_value);
    
    // Berlin+ (EIP-2929 + EIP-2200)
    // Adds cold/warm storage access costs
    if (evm->fork >= FORK_BERLIN) {
        // TODO: Implement proper cold/warm tracking
        // For now, assume all accesses are cold (worst case)
        bool is_cold = true;  // Should check evm->accessed_storage
        uint64_t access_cost = is_cold ? GAS_SLOAD_COLD : GAS_SLOAD_WARM;
        
        if (current_is_zero && !new_is_zero) {
            // Setting from zero to non-zero (most expensive)
            return GAS_SSTORE_SET + access_cost;  // 20000 + 2100 = 22100
        } else if (!current_is_zero && new_is_zero) {
            // Clearing storage (reset + access cost)
            return GAS_SSTORE_RESET + access_cost;  // 5000 + 2100 = 7100
        } else if (!current_is_zero) {
            // Modifying existing non-zero value
            return GAS_SSTORE_RESET + access_cost;  // 5000 + 2100 = 7100
        } else {
            // Setting zero to zero (no-op, just access cost + minimal)
            return GAS_SLOAD_WARM + access_cost;  // 100 + 2100 = 2200
        }
    }
    
    // Istanbul (EIP-2200) - net gas metering without cold/warm
    // More complex rules with refunds based on original value
    else if (evm->fork >= FORK_ISTANBUL) {
        // Simplified Istanbul rules (full implementation needs original value tracking)
        if (current_is_zero && !new_is_zero) {
            return GAS_SSTORE_SET;  // 20000
        } else {
            return GAS_SLOAD_ISTANBUL;  // 800
        }
    }
    
    // Constantinople (EIP-1283) - net gas metering
    // Note: Petersburg (next fork) reverted this
    else if (evm->fork == FORK_CONSTANTINOPLE) {
        // Simplified Constantinople rules
        if (current_is_zero && !new_is_zero) {
            return GAS_SSTORE_SET;  // 20000
        } else {
            return GAS_SLOAD_ISTANBUL;  // 800
        }
    }
    
    // Frontier/Homestead/Petersburg - simple rules
    else {
        if (current_is_zero && !new_is_zero) {
            return GAS_SSTORE_SET;    // 20000
        } else {
            return GAS_SSTORE_RESET;  // 5000
        }
    }
}

//==============================================================================
// Storage Opcodes
//==============================================================================

// SLOAD (0x54): Load word from storage
evm_status_t op_sload(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->state)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_EVM_ERROR("SLOAD: Stack underflow");
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
        LOG_EVM_ERROR("SSTORE: State modification in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_EVM_ERROR("SSTORE: Stack underflow");
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

    // Calculate fork-specific SSTORE gas cost
    uint64_t gas_cost = calculate_sstore_gas(evm, &current_value, &value);
    
    // Deduct gas
    if (!evm_use_gas(evm, gas_cost)) {
        return EVM_OUT_OF_GAS;
    }

    // Store value to storage using current contract address
    if (!state_db_set_state(evm->state, &evm->msg.recipient, &key, &value))
    {
        LOG_EVM_ERROR("SSTORE: Failed to set storage");
        return EVM_INTERNAL_ERROR;
    }

    return EVM_SUCCESS;
}
