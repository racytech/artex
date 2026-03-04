/**
 * EVM Storage Opcodes Implementation
 */

#include "evm.h"
#include "evm_state.h"
#include "opcodes/storage.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"
#include <stdlib.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate SSTORE gas cost and refund based on fork rules.
 *
 * Follows the geth reference implementation exactly:
 * - Pre-Istanbul: simple 20000/5000 + refund 15000 for clearing
 * - Istanbul (EIP-2200): net gas metering using original/current/new
 * - Berlin (EIP-2929): cold/warm access costs layered on EIP-2200
 * - London (EIP-3529): reduced refunds (4800 instead of 15000)
 */
static uint64_t calculate_sstore_gas(
    evm_t *evm,
    const uint256_t *key,
    const uint256_t *current_value,
    const uint256_t *original_value,
    const uint256_t *new_value,
    int64_t *gas_refund)
{
    if (gas_refund) *gas_refund = 0;

    bool current_is_zero = uint256_is_zero(current_value);
    bool new_is_zero     = uint256_is_zero(new_value);

    // ── Pre-Istanbul: simple model ──────────────────────────────────────
    if (evm->fork < FORK_ISTANBUL)
    {
        if (current_is_zero && !new_is_zero)
            return GAS_SSTORE_SET;       // 20000
        if (!current_is_zero && new_is_zero) {
            if (gas_refund) *gas_refund = GAS_SSTORE_REFUND; // 15000
            return GAS_SSTORE_RESET;     // 5000
        }
        return GAS_SSTORE_RESET;         // 5000
    }

    // ── Istanbul+ (EIP-2200 / EIP-2929 / EIP-3529) ─────────────────────
    //
    // Structure (matching geth gasSStoreEIP2929 / gasSStoreEIP2200):
    //   cost  = 0
    //   if Berlin+ && cold: cost += COLD_SLOAD_COST (2100)
    //   then apply EIP-2200 logic to determine the SSTORE-specific cost

    uint64_t cost = 0;

    // Berlin+ (EIP-2929): cold/warm access
    if (evm->fork >= FORK_BERLIN) {
        bool is_warm = evm_is_storage_warm(evm, &evm->msg.recipient, key);
        if (!is_warm) {
            cost += GAS_SLOAD_COLD;  // 2100
            evm_mark_storage_warm(evm, &evm->msg.recipient, key);
        }
    }

    // EIP-2200 gas constants depend on fork
    uint64_t sload_gas   = (evm->fork >= FORK_BERLIN) ? GAS_SLOAD_WARM : GAS_SLOAD_ISTANBUL;
    uint64_t sstore_reset = (evm->fork >= FORK_BERLIN) ? (GAS_SSTORE_RESET - GAS_SLOAD_COLD) : GAS_SSTORE_RESET;
    int64_t  clear_refund = (evm->fork >= FORK_LONDON) ? 4800 : GAS_SSTORE_REFUND;

    // (1) No change → noop
    if (uint256_eq(current_value, new_value)) {
        return cost + sload_gas;
    }

    // (2) First write in transaction (original == current)
    if (uint256_eq(original_value, current_value)) {
        bool original_is_zero = uint256_is_zero(original_value);

        if (original_is_zero) {
            // 0 → nonzero: SSTORE_SET
            return cost + GAS_SSTORE_SET;  // 20000
        }
        if (new_is_zero) {
            // nonzero → 0: grant clearing refund
            if (gas_refund) *gas_refund = clear_refund;
        }
        return cost + sstore_reset;  // 5000 (Istanbul) or 2900 (Berlin+)
    }

    // (3) Dirty write (original != current) — cheap, just SLOAD cost
    if (gas_refund) {
        bool original_is_zero = uint256_is_zero(original_value);

        // 3a. If original != 0:
        if (!original_is_zero) {
            if (current_is_zero && !new_is_zero) {
                // Slot was cleared earlier this tx, now being re-set → remove clearing refund
                *gas_refund = -clear_refund;
            } else if (!current_is_zero && new_is_zero) {
                // Slot wasn't cleared before, clearing now → grant clearing refund
                *gas_refund = clear_refund;
            }
        }

        // 3b. If resetting to original value, refund the excess cost
        if (uint256_eq(original_value, new_value)) {
            bool original_is_zero2 = uint256_is_zero(original_value);
            if (original_is_zero2) {
                // Was 0, was set to X, now resetting to 0 → refund (SSTORE_SET - sload_gas)
                *gas_refund += (int64_t)(GAS_SSTORE_SET - sload_gas);
            } else {
                // Was V, modified, now resetting to V → refund (sstore_reset - sload_gas)
                *gas_refund += (int64_t)(sstore_reset - sload_gas);
            }
        }
    }

    return cost + sload_gas;
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
    else if (evm->fork >= FORK_ISTANBUL)
    {
        // Istanbul (EIP-1884): SLOAD costs 800
        gas_cost = GAS_SLOAD_ISTANBUL;
    }
    else if (evm->fork >= FORK_TANGERINE_WHISTLE)
    {
        // EIP-150 (Tangerine Whistle): SLOAD costs 200
        gas_cost = GAS_SLOAD_LEGACY;
    }
    else
    {
        // Pre-EIP-150 (Frontier/Homestead): SLOAD costs 50
        gas_cost = GAS_SLOAD_FRONTIER;
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

    // EIP-2200 (Istanbul+): SSTORE sentry — prevent reentrancy via SSTORE
    // when gas is too low. "If gasleft is less than or equal to 2300, fail."
    if (evm->fork >= FORK_ISTANBUL && evm->gas_left <= GAS_CALL_STIPEND)
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t key, value;
    evm_stack_pop(evm->stack, &key);
    evm_stack_pop(evm->stack, &value);

    // Get current and original (committed) values from storage
    uint256_t current_value = evm_state_get_storage(evm->state, &evm->msg.recipient, &key);
    uint256_t original_value = evm_state_get_committed_storage(evm->state, &evm->msg.recipient, &key);

    // Calculate fork-specific SSTORE gas cost and refund
    int64_t gas_refund = 0;
    uint64_t gas_cost = calculate_sstore_gas(evm, &key, &current_value, &original_value, &value, &gas_refund);

    // Deduct gas
    if (!evm_use_gas(evm, gas_cost)) {
        return EVM_OUT_OF_GAS;
    }

    // Apply gas refund (can be negative for dirty writes)
    if (gas_refund > 0) {
        evm_refund_gas(evm, (uint64_t)gas_refund);
    } else if (gas_refund < 0) {
        // Negative refund: reduce accumulated refund counter
        uint64_t reduction = (uint64_t)(-gas_refund);
        if (evm->gas_refund >= reduction) {
            evm->gas_refund -= reduction;
        } else {
            evm->gas_refund = 0;
        }
    }

    // Store value to storage using current contract address
    evm_state_set_storage(evm->state, &evm->msg.recipient, &key, &value);

    return EVM_SUCCESS;
}

// TLOAD (0x5c): Load from transient storage (EIP-1153, Cancun+)
evm_status_t op_tload(evm_t *evm)
{
    if (!evm || !evm->stack)
        return EVM_INTERNAL_ERROR;

    if (evm->fork < FORK_CANCUN)
        return EVM_INVALID_OPCODE;

    uint256_t key;
    if (!evm_stack_pop(evm->stack, &key))
        return EVM_STACK_UNDERFLOW;

    if (!evm_use_gas(evm, GAS_SLOAD_WARM))  // 100 gas
        return EVM_OUT_OF_GAS;

    uint256_t value = evm_state_tload(evm->state, &evm->msg.recipient, &key);
    if (!evm_stack_push(evm->stack, &value))
        return EVM_STACK_OVERFLOW;

    return EVM_SUCCESS;
}

// TSTORE (0x5d): Store to transient storage (EIP-1153, Cancun+)
evm_status_t op_tstore(evm_t *evm)
{
    if (!evm || !evm->stack)
        return EVM_INTERNAL_ERROR;

    if (evm->fork < FORK_CANCUN)
        return EVM_INVALID_OPCODE;

    if (evm->msg.is_static)
        return EVM_STATIC_CALL_VIOLATION;

    uint256_t key, value;
    if (!evm_stack_pop(evm->stack, &key))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &value))
        return EVM_STACK_UNDERFLOW;

    if (!evm_use_gas(evm, GAS_SLOAD_WARM))  // 100 gas
        return EVM_OUT_OF_GAS;

    evm_state_tstore(evm->state, &evm->msg.recipient, &key, &value);

    return EVM_SUCCESS;
}
