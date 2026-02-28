/**
 * EVM Storage Opcodes Implementation
 */

#include "evm.h"
#include "evm_state.h"
#include "opcodes/storage.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate SSTORE gas cost and refund based on fork rules
 * 
 * Implements fork-specific gas calculation for SSTORE:
 * - Frontier/Homestead: Simple set (20000) or reset (5000) with refunds
 * - Constantinople (EIP-1283): Net gas metering with refunds
 * - Petersburg: Reverted EIP-1283, back to Homestead rules
 * - Istanbul (EIP-2200): Improved net gas metering with refunds
 * - Berlin+ (EIP-2929): Added cold/warm access costs
 * - London+ (EIP-3529): Reduced refunds (4800 vs 15000)
 * 
 * @param evm EVM context (for fork detection and warm/cold tracking)
 * @param key Storage key (for access list tracking)
 * @param current_value Current value in storage
 * @param new_value New value to store
 * @param gas_refund Output parameter for gas refund amount (can be NULL)
 * @return Gas cost for this SSTORE operation
 */
static uint64_t calculate_sstore_gas(
    evm_t *evm,
    const uint256_t *key,
    const uint256_t *current_value,
    const uint256_t *new_value,
    int64_t *gas_refund)
{
    // Initialize refund
    if (gas_refund)
    {
        *gas_refund = 0;
    }
    
    bool current_is_zero = uint256_is_zero(current_value);
    bool new_is_zero = uint256_is_zero(new_value);
    
    // Berlin+ (EIP-2929 + EIP-2200)
    // Adds cold/warm storage access costs
    if (evm->fork >= FORK_BERLIN) {
        // Check if storage slot is warm
        bool is_warm = evm_is_storage_warm(evm, &evm->msg.recipient, key);
        uint64_t access_cost = is_warm ? 0 : GAS_SLOAD_COLD;  // Cold penalty: 0 (warm) or 2100 (cold)
        
        // Mark as warm for future accesses
        if (!is_warm)
        {
            evm_mark_storage_warm(evm, &evm->msg.recipient, key);
        }
        
        // Check if value is changing
        bool value_unchanged = uint256_eq(current_value, new_value);
        
        if (value_unchanged) {
            // No change - ASSIGNED status
            // Base cost: warm_access (100) + cold access penalty if needed
            return GAS_SLOAD_WARM + access_cost;  // 100 + 2100 = 2200 (cold) or 100 + 0 = 100 (warm)
        } else if (current_is_zero && !new_is_zero) {
            // Setting from zero to non-zero - ADDED status
            return GAS_SSTORE_SET + access_cost;  // 20000 + (2100 or 0)
        } else if (!current_is_zero && new_is_zero) {
            // Clearing storage - DELETED status  
            // Berlin reset = 5000 - 2100 = 2900, total = 2900 + 2100 = 5000 (cold)
            
            // Grant refund for clearing storage
            if (gas_refund)
            {
                // London (EIP-3529): reduced refund of 4800
                // Pre-London: full refund of 15000
                *gas_refund = (evm->fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;
            }
            
            return (GAS_SSTORE_RESET - GAS_SLOAD_COLD) + access_cost;  // 2900 + (2100 or 0)
        } else {
            // Modifying existing non-zero value - MODIFIED status
            // Same as DELETED
            return (GAS_SSTORE_RESET - GAS_SLOAD_COLD) + access_cost;  // 2900 + (2100 or 0)
        }
    }
    
    // Istanbul (EIP-2200) - net gas metering without cold/warm
    // More complex rules with refunds based on original value
    else if (evm->fork >= FORK_ISTANBUL) {
        // Simplified Istanbul rules (full implementation needs original value tracking)
        if (current_is_zero && !new_is_zero) {
            return GAS_SSTORE_SET;  // 20000
        } else if (!current_is_zero && new_is_zero) {
            // Clearing storage - grant refund
            if (gas_refund)
            {
                *gas_refund = GAS_SSTORE_REFUND; // 15000
            }
            return GAS_SLOAD_ISTANBUL;  // 800
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
        } else if (!current_is_zero && new_is_zero) {
            // Clearing storage - grant refund
            if (gas_refund)
            {
                *gas_refund = GAS_SSTORE_REFUND; // 15000
            }
            return GAS_SLOAD_ISTANBUL;  // 800
        } else {
            return GAS_SLOAD_ISTANBUL;  // 800
        }
    }
    
    // Frontier/Homestead/Petersburg - simple rules
    else {
        if (current_is_zero && !new_is_zero) {
            return GAS_SSTORE_SET;    // 20000
        } else if (!current_is_zero && new_is_zero) {
            // Clearing storage - grant refund
            if (gas_refund)
            {
                *gas_refund = GAS_SSTORE_REFUND; // 15000
            }
            return GAS_SSTORE_RESET;  // 5000
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

    // Check if storage slot is cold/warm and charge appropriate gas (EIP-2929)
    uint64_t gas_cost;
    if (evm->fork >= FORK_BERLIN)
    {
        bool is_warm = evm_is_storage_warm(evm, &evm->msg.recipient, &key);
        gas_cost = is_warm ? GAS_SLOAD_WARM : GAS_SLOAD_COLD;
        
        // Mark as warm for future accesses
        if (!is_warm)
        {
            evm_mark_storage_warm(evm, &evm->msg.recipient, &key);
        }
    }
    else
    {
        // Pre-Berlin: use legacy gas cost
        gas_cost = GAS_SLOAD_WARM;
    }
    
    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // Load value from storage using current contract address
    uint256_t value = evm_state_get_storage(evm->state, &evm->msg.recipient, &key);

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
    uint256_t current_value = evm_state_get_storage(evm->state, &evm->msg.recipient, &key);
    
    // Calculate fork-specific SSTORE gas cost and refund
    int64_t gas_refund = 0;
    uint64_t gas_cost = calculate_sstore_gas(evm, &key, &current_value, &value, &gas_refund);
    
    LOG_EVM_ERROR("SSTORE: cost=%lu, refund=%ld", gas_cost, gas_refund);
    
    // Deduct gas
    if (!evm_use_gas(evm, gas_cost)) {
        return EVM_OUT_OF_GAS;
    }
    
    // Apply gas refund if any
    if (gas_refund > 0) {
        LOG_EVM_ERROR("SSTORE: Granting refund of %ld gas", gas_refund);
        evm_refund_gas(evm, (uint64_t)gas_refund);
    }

    // Store value to storage using current contract address
    evm_state_set_storage(evm->state, &evm->msg.recipient, &key, &value);
    
    // Debug: Track SSTORE for specific account
    if (evm->msg.recipient.bytes[19] == 0x0f && evm->msg.recipient.bytes[18] == 0xa4) {
        char *key_hex = uint256_to_hex(&key);
        char *val_hex = uint256_to_hex(&value);
        fprintf(stderr, "SSTORE: addr=...a40f, key=%s, value=%s, depth=%d\n", 
                key_hex ? key_hex : "NULL",
                val_hex ? val_hex : "NULL",
                evm->msg.depth);
        free(key_hex);
        free(val_hex);
    }

    return EVM_SUCCESS;
}
