/**
 * EVM Environmental Information Opcodes Implementation
 */

#include "opcodes/environmental.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "address.h"
#include "gas.h"
#include "logger.h"
#include <string.h>

//==============================================================================
// Address & Value Opcodes
//==============================================================================

/**
 * ADDRESS - Get address of currently executing account
 * Stack: => address
 * Gas: 2
 */
evm_status_t op_address(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Convert address to uint256 and push to stack
    uint256_t addr_value;
    memset(&addr_value, 0, sizeof(uint256_t));
    memcpy(&addr_value, &evm->msg.recipient, sizeof(address_t));

    if (!evm_stack_push(evm->stack, &addr_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * BALANCE - Get balance of an account
 * Stack: address => balance
 * Gas: 100 (warm) / 2600 (cold)
 */
evm_status_t op_balance(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop address from stack
    uint256_t addr_value;
    if (!evm_stack_pop(evm->stack, &addr_value))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert uint256 to address (take lower 20 bytes)
    address_t addr;
    memcpy(&addr, &addr_value, sizeof(address_t));

    // TODO: Check if address is cold/warm and charge appropriate gas (GAS_SLOAD_COLD vs GAS_SLOAD_WARM)
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get balance from state
    uint256_t balance;
    if (!state_db_get_balance(evm->state, &addr, &balance))
    {
        // Account doesn't exist, balance is 0
        balance = UINT256_ZERO;
    }

    if (!evm_stack_push(evm->stack, &balance))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * ORIGIN - Get transaction origin
 * Stack: => origin
 * Gas: 2
 */
evm_status_t op_origin(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Convert address to uint256 and push to stack
    uint256_t origin_value;
    memset(&origin_value, 0, sizeof(uint256_t));
    memcpy(&origin_value, &evm->tx.origin, sizeof(address_t));

    if (!evm_stack_push(evm->stack, &origin_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CALLER - Get caller address
 * Stack: => caller
 * Gas: 2
 */
evm_status_t op_caller(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Convert address to uint256 and push to stack
    uint256_t caller_value;
    memset(&caller_value, 0, sizeof(uint256_t));
    memcpy(&caller_value, &evm->msg.caller, sizeof(address_t));

    if (!evm_stack_push(evm->stack, &caller_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CALLVALUE - Get deposited value
 * Stack: => value
 * Gas: 2
 */
evm_status_t op_callvalue(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_push(evm->stack, &evm->msg.value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// Calldata Operations
//==============================================================================

/**
 * CALLDATALOAD - Load word from calldata
 * Stack: i => data[i:i+32]
 * Gas: 3
 */
evm_status_t op_calldataload(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Pop offset from stack
    uint256_t offset_256;
    if (!evm_stack_pop(evm->stack, &offset_256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert to uint64 (if offset is too large, we'll just get zeros)
    uint64_t offset = uint256_to_uint64(&offset_256);

    // Load 32 bytes from calldata
    uint256_t data;
    memset(&data, 0, sizeof(uint256_t));

    if (offset < evm->msg.input_size)
    {
        size_t available = evm->msg.input_size - offset;
        size_t to_copy = available < 32 ? available : 32;
        memcpy(&data, evm->msg.input_data + offset, to_copy);
    }

    if (!evm_stack_push(evm->stack, &data))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CALLDATASIZE - Get size of calldata
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_calldatasize(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t size = uint256_from_uint64(evm->msg.input_size);

    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CALLDATACOPY - Copy calldata to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_calldatacopy(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop arguments from stack
    uint256_t dest_offset_256, offset_256, size_256;
    
    if (!evm_stack_pop(evm->stack, &dest_offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64
    uint64_t dest_offset = uint256_to_uint64(&dest_offset_256);
    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_VERY_LOW + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    if (size == 0)
        return EVM_SUCCESS;

    // Allocate memory if needed
    if (!evm_memory_expand(evm->memory, dest_offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Copy data from calldata to memory
    for (uint64_t i = 0; i < size; i++)
    {
        uint8_t byte = 0;
        if (offset + i < evm->msg.input_size)
        {
            byte = evm->msg.input_data[offset + i];
        }
        evm_memory_write_byte(evm->memory, dest_offset + i, byte);
    }

    return EVM_SUCCESS;
}

//==============================================================================
// Code Operations
//==============================================================================

/**
 * CODESIZE - Get size of code running in current environment
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_codesize(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t size = uint256_from_uint64(evm->code_size);

    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CODECOPY - Copy code to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_codecopy(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop arguments from stack
    uint256_t dest_offset_256, offset_256, size_256;
    
    if (!evm_stack_pop(evm->stack, &dest_offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64
    uint64_t dest_offset = uint256_to_uint64(&dest_offset_256);
    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_VERY_LOW + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    if (size == 0)
        return EVM_SUCCESS;

    // Allocate memory if needed
    if (!evm_memory_expand(evm->memory, dest_offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Copy data from code to memory
    for (uint64_t i = 0; i < size; i++)
    {
        uint8_t byte = 0;
        if (offset + i < evm->code_size)
        {
            byte = evm->code[offset + i];
        }
        evm_memory_write_byte(evm->memory, dest_offset + i, byte);
    }

    return EVM_SUCCESS;
}

/**
 * GASPRICE - Get price of gas in current environment
 * Stack: => gas_price
 * Gas: 2
 */
evm_status_t op_gasprice(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_push(evm->stack, &evm->tx.gas_price))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// Return Data Operations
//==============================================================================

/**
 * RETURNDATASIZE - Get size of output data from previous call
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_returndatasize(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t size = uint256_from_uint64(evm->return_data_size);

    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * RETURNDATACOPY - Copy output data from previous call to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_returndatacopy(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop arguments from stack
    uint256_t dest_offset_256, offset_256, size_256;
    
    if (!evm_stack_pop(evm->stack, &dest_offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;
    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64
    uint64_t dest_offset = uint256_to_uint64(&dest_offset_256);
    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_VERY_LOW + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check if trying to read beyond return data
    if (offset + size > evm->return_data_size)
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    if (size == 0)
        return EVM_SUCCESS;

    // Allocate memory if needed
    if (!evm_memory_expand(evm->memory, dest_offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Copy data from return data to memory
    for (uint64_t i = 0; i < size; i++)
    {
        evm_memory_write_byte(evm->memory, dest_offset + i, evm->return_data[offset + i]);
    }

    return EVM_SUCCESS;
}

//==============================================================================
// External Code Opcodes (Stubs)
//==============================================================================

/**
 * EXTCODESIZE - Get size of an account's code (stub)
 * Stack: address => size
 * 
 * TODO: Requires StateDB code storage API
 */
evm_status_t op_extcodesize(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop address from stack
    uint256_t address_u256;
    if (!evm_stack_pop(evm->stack, &address_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Check if address is cold/warm and charge appropriate gas
    // For now, charge warm access cost
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Get code size from StateDB
    // For now, push 0 (no code)
    LOG_EVM_DEBUG("EXTCODESIZE: stub - returning size=0");

    uint256_t size = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * EXTCODECOPY - Copy an account's code to memory (stub)
 * Stack: address destOffset offset size =>
 * 
 * TODO: Requires StateDB code storage API
 */
evm_status_t op_extcodecopy(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop arguments from stack
    uint256_t size_u256, offset_u256, dest_offset_u256, address_u256;
    
    if (!evm_stack_pop(evm->stack, &size_u256) ||
        !evm_stack_pop(evm->stack, &offset_u256) ||
        !evm_stack_pop(evm->stack, &dest_offset_u256) ||
        !evm_stack_pop(evm->stack, &address_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    uint64_t dest_offset = uint256_to_uint64(&dest_offset_u256);
    uint64_t size = uint256_to_uint64(&size_u256);

    // TODO: Check if address is cold/warm and charge appropriate gas
    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_SLOAD_WARM + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Expand memory if needed
    if (size > 0)
    {
        if (!evm_memory_expand(evm->memory, dest_offset, size))
        {
            return EVM_INTERNAL_ERROR;
        }

        // TODO: Copy code from StateDB to memory
        // For now, just fill with zeros
        LOG_EVM_DEBUG("EXTCODECOPY: stub - filling memory with zeros");
        
        for (uint64_t i = 0; i < size; i++)
        {
            evm_memory_write_byte(evm->memory, dest_offset + i, 0);
        }
    }

    return EVM_SUCCESS;
}

/**
 * EXTCODEHASH - Get hash of an account's code (stub)
 * Stack: address => hash
 * 
 * TODO: Requires StateDB code storage API
 * Returns keccak256 hash of code, or 0 if account doesn't exist
 */
evm_status_t op_extcodehash(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop address from stack
    uint256_t address_u256;
    if (!evm_stack_pop(evm->stack, &address_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Check if address is cold/warm and charge appropriate gas
    // For now, charge warm access cost
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Get code hash from StateDB
    // For now, push 0 (account doesn't exist or has no code)
    LOG_EVM_DEBUG("EXTCODEHASH: stub - returning hash=0");

    uint256_t hash = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &hash))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
