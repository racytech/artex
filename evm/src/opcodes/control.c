/**
 * EVM Control Flow Opcodes Implementation
 */

#include "opcodes/control.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "gas.h"
#include "logger.h"
#include <string.h>
#include <stdlib.h>

//==============================================================================
// Simple Control Opcodes
//==============================================================================

/**
 * PC - Get program counter
 * Stack: => pc
 * Gas: 2
 */
evm_status_t op_pc(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Push current PC value to stack
    uint256_t pc_value = uint256_from_uint64(evm->pc);

    if (!evm_stack_push(evm->stack, &pc_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * GAS - Get remaining gas
 * Stack: => gas
 * Gas: 2
 */
evm_status_t op_gas(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Push current remaining gas to stack
    uint256_t gas_value = uint256_from_uint64(evm->gas_left);

    if (!evm_stack_push(evm->stack, &gas_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * STOP - Halt execution successfully
 * Stack: (no effect)
 * Gas: 0
 */
evm_status_t op_stop(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // STOP costs 0 gas
    if (!evm_use_gas(evm, GAS_ZERO))
    {
        return EVM_OUT_OF_GAS;
    }

    // Set stopped flag
    evm->stopped = true;

    return EVM_SUCCESS;
}

/**
 * INVALID - Invalid instruction (always fails)
 * Stack: (no effect)
 * Gas: All remaining gas consumed
 */
evm_status_t op_invalid(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Consume all gas
    evm->gas_left = 0;

    return EVM_INVALID_OPCODE;
}

/**
 * JUMPDEST - Mark valid jump destination
 * Stack: (no effect)
 * Gas: 1
 */
evm_status_t op_jumpdest(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, 1))
    {
        return EVM_OUT_OF_GAS;
    }

    // JUMPDEST is just a marker, does nothing during execution
    // It's validated during the JUMP/JUMPI operations

    return EVM_SUCCESS;
}

//==============================================================================
// Jump Operations
//==============================================================================

/**
 * Helper function to validate jump destination
 */
static bool is_valid_jump_dest(const evm_t *evm, uint64_t dest)
{
    if (!evm || !evm->code)
        return false;

    // Destination must be within code bounds
    if (dest >= evm->code_size)
        return false;

    // Destination must point to a JUMPDEST instruction
    if (evm->code[dest] != OP_JUMPDEST)
        return false;

    // Additional check: ensure we're not jumping into the middle of a PUSH instruction
    // We need to scan from the beginning to track PUSH data bytes
    uint64_t pc = 0;
    while (pc < evm->code_size)
    {
        uint8_t opcode = evm->code[pc];

        // If we reached our destination and it's valid, return true
        if (pc == dest)
            return true;

        // If this is a PUSH instruction, skip the immediate bytes
        if (opcode >= 0x60 && opcode <= 0x7f)  // PUSH1 to PUSH32
        {
            uint8_t push_size = opcode - 0x60 + 1;
            pc += 1 + push_size;
        }
        else
        {
            pc += 1;
        }
    }

    return false;
}

/**
 * JUMP - Alter program counter
 * Stack: dest =>
 * Gas: 8
 */
evm_status_t op_jump(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_MID))
    {
        return EVM_OUT_OF_GAS;
    }

    // Pop destination from stack
    uint256_t dest_256;
    if (!evm_stack_pop(evm->stack, &dest_256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert to uint64
    uint64_t dest = uint256_to_uint64(&dest_256);

    // Validate jump destination
    if (!is_valid_jump_dest(evm, dest))
    {
        return EVM_INVALID_JUMP;
    }

    // Set PC to destination
    evm->pc = dest;

    return EVM_SUCCESS;
}

/**
 * JUMPI - Conditionally alter program counter
 * Stack: dest cond =>
 * Gas: 10
 */
evm_status_t op_jumpi(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_HIGH))
    {
        return EVM_OUT_OF_GAS;
    }

    // Pop destination and condition from stack
    uint256_t dest_256, cond_256;
    
    if (!evm_stack_pop(evm->stack, &dest_256))
        return EVM_STACK_UNDERFLOW;
    
    if (!evm_stack_pop(evm->stack, &cond_256))
        return EVM_STACK_UNDERFLOW;

    // Check if condition is non-zero (true)
    if (uint256_is_zero(&cond_256))
    {
        // Condition is false, don't jump - increment PC
        evm->pc++;
        return EVM_SUCCESS;
    }

    // Condition is true, perform jump
    uint64_t dest = uint256_to_uint64(&dest_256);

    // Validate jump destination
    if (!is_valid_jump_dest(evm, dest))
    {
        return EVM_INVALID_JUMP;
    }

    // Set PC to destination
    evm->pc = dest;

    return EVM_SUCCESS;
}

//==============================================================================
// Return/Revert Opcodes
//==============================================================================

/**
 * RETURN - Halt execution and return output data
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
evm_status_t op_return(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop offset and size from stack
    uint256_t offset_256, size_256;
    
    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;
    
    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64
    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, GAS_ZERO + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Free any existing return data
    if (evm->return_data)
    {
        free(evm->return_data);
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }

    // Handle zero size - no data to return
    if (size == 0)
    {
        evm->stopped = true;
        return EVM_SUCCESS;
    }

    // Expand memory if needed
    if (!evm_memory_expand(evm->memory, offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Allocate return data buffer
    evm->return_data = malloc(size);
    if (!evm->return_data)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Copy data from memory to return buffer
    for (uint64_t i = 0; i < size; i++)
    {
        if (!evm_memory_read_byte(evm->memory, offset + i, &evm->return_data[i]))
        {
            free(evm->return_data);
            evm->return_data = NULL;
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    evm->return_data_size = size;
    evm->stopped = true;

    return EVM_SUCCESS;
}

/**
 * REVERT - Halt execution and revert state changes
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
evm_status_t op_revert(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // EIP-140: REVERT introduced in Byzantium
    if (evm->fork < FORK_BYZANTIUM)
        return EVM_INVALID_OPCODE;

    // Pop offset and size from stack
    uint256_t offset_256, size_256;
    
    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;
    
    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64
    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, GAS_ZERO + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Free any existing return data
    if (evm->return_data)
    {
        free(evm->return_data);
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }

    // Handle zero size - no revert data
    if (size == 0)
    {
        evm->stopped = true;
        return EVM_REVERT;
    }

    // Expand memory if needed
    if (!evm_memory_expand(evm->memory, offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Allocate return data buffer for revert data
    evm->return_data = malloc(size);
    if (!evm->return_data)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Copy data from memory to return buffer
    for (uint64_t i = 0; i < size; i++)
    {
        if (!evm_memory_read_byte(evm->memory, offset + i, &evm->return_data[i]))
        {
            free(evm->return_data);
            evm->return_data = NULL;
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    evm->return_data_size = size;
    evm->stopped = true;

    return EVM_REVERT;
}

//==============================================================================
// SELFDESTRUCT Opcode
//==============================================================================

/**
 * SELFDESTRUCT - Destroy current contract and send funds
 * Stack: beneficiary =>
 * 
 * Transfers contract balance to beneficiary and marks contract for deletion.
 * Note: EIP-6780 (Cancun) changes behavior - only deletes if created in same transaction.
 */
evm_status_t op_selfdestruct(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("SELFDESTRUCT: Cannot selfdestruct in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop beneficiary address from stack
    uint256_t beneficiary;
    if (!evm_stack_pop(evm->stack, &beneficiary))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert beneficiary from uint256 to address
    address_t beneficiary_addr;
    address_from_uint256(&beneficiary, &beneficiary_addr);

    // Get current contract balance
    uint256_t balance = evm_state_get_balance(evm->state, &evm->msg.recipient);

    // Calculate gas cost - fork-dependent
    uint64_t gas_cost = 0;

    if (evm->fork >= FORK_TANGERINE_WHISTLE)
    {
        gas_cost = 5000;  // EIP-150 base cost

        // Berlin+ (EIP-2929): Add cold account access cost
        if (evm->fork >= FORK_BERLIN)
        {
            bool is_warm = evm_is_address_warm(evm, &beneficiary_addr);
            if (!is_warm)
            {
                gas_cost += GAS_COLD_ACCOUNT_ACCESS;  // 2600
                evm_mark_address_warm(evm, &beneficiary_addr);
            }
        }

        // EIP-3529 (London+): skip new account cost if contract already self-destructed
        bool skip_new_account = false;
        if (evm->fork >= FORK_LONDON)
        {
            skip_new_account = evm_state_is_self_destructed(evm->state, &evm->msg.recipient);
        }

        if (!skip_new_account && !uint256_is_zero(&balance))
        {
            // Spurious Dragon+ (EIP-161): 25000 if beneficiary is empty
            // Pre-Spurious Dragon (EIP-150): 25000 if beneficiary doesn't exist
            bool charge_new_account;
            if (evm->fork >= FORK_SPURIOUS_DRAGON)
            {
                charge_new_account = evm_state_is_empty(evm->state, &beneficiary_addr);
            }
            else
            {
                charge_new_account = !evm_state_exists(evm->state, &beneficiary_addr);
            }

            if (charge_new_account)
            {
                gas_cost += 25000;
            }
        }
    }
    // Pre-TW: SELFDESTRUCT costs 0 gas

    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // EIP-6780 (Cancun+): SELFDESTRUCT only deletes if created in same tx
    bool created_in_tx = evm_state_is_created(evm->state, &evm->msg.recipient);
    bool do_delete = (evm->fork < FORK_CANCUN) || created_in_tx;

    // Gas refund: pre-EIP-3529 (pre-London) gives 24000 refund
    // Only if this account hasn't already been scheduled for deletion in this tx
    // Must check BEFORE marking self-destruct below
    if (evm->fork < FORK_LONDON)
    {
        if (!evm_state_is_self_destructed(evm->state, &evm->msg.recipient))
        {
            evm->gas_refund += 24000;
        }
    }

    // Transfer balance: add to beneficiary, subtract from self.
    // For self-send (beneficiary == self), add + sub cancel out → balance unchanged.
    // This is correct for Cancun EIP-6780 where pre-existing contracts survive
    // SELFDESTRUCT and self-send should NOT burn ether.
    evm_state_add_balance(evm->state, &beneficiary_addr, &balance);
    evm_state_sub_balance(evm->state, &evm->msg.recipient, &balance);

    if (do_delete)
    {
        // Account will be destroyed: zero balance (burns ether on self-send)
        uint256_t zero = UINT256_ZERO;
        evm_state_set_balance(evm->state, &evm->msg.recipient, &zero);
        // Mark contract for deletion (pre-Cancun, or created in same tx)
        evm_state_self_destruct(evm->state, &evm->msg.recipient);
    }

    LOG_EVM_DEBUG("SELFDESTRUCT: contract deleted, balance transferred");

    // Halt execution
    evm->stopped = true;

    return EVM_SUCCESS;
}
