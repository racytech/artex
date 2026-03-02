/**
 * EVM Environmental Information Opcodes Implementation
 */

#include "opcodes/environmental.h"
#include "evm_state.h"
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
    address_to_uint256(&evm->msg.recipient, &addr_value);

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
    address_from_uint256(&addr_value, &addr);

    // Charge gas based on fork
    uint64_t gas_cost;
    if (evm->fork >= FORK_BERLIN) {
        // EIP-2929: cold/warm access model
        bool is_warm = evm_is_address_warm(evm, &addr);
        gas_cost = is_warm ? GAS_WARM_ACCOUNT_ACCESS : GAS_COLD_ACCOUNT_ACCESS;
        if (!is_warm) evm_mark_address_warm(evm, &addr);
    } else if (evm->fork >= FORK_ISTANBUL) {
        gas_cost = 700;  // EIP-1884
    } else if (evm->fork >= FORK_TANGERINE_WHISTLE) {
        gas_cost = GAS_BALANCE;  // 400 (EIP-150)
    } else {
        gas_cost = GAS_BALANCE_FRONTIER;  // 20 (pre-EIP-150)
    }

    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get balance from state
    uint256_t balance = evm_state_get_balance(evm->state, &addr);

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
    address_to_uint256(&evm->tx.origin, &origin_value);

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
    address_to_uint256(&evm->msg.caller, &caller_value);

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

    // Load 32 bytes from calldata (big-endian)
    uint8_t data_bytes[32] = {0};
    
    if (offset < evm->msg.input_size)
    {
        size_t available = evm->msg.input_size - offset;
        size_t to_copy = available < 32 ? available : 32;
        memcpy(data_bytes, evm->msg.input_data + offset, to_copy);
    }

    // Convert bytes to uint256 (handles big-endian conversion)
    uint256_t data = uint256_from_bytes(data_bytes, 32);

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

    // Check if offset is beyond calldata using 256-bit comparison.
    // This prevents uint64 truncation from causing overflow in offset + i.
    uint256_t input_size_256 = uint256_from_uint64(evm->msg.input_size);
    if (!uint256_lt(&offset_256, &input_size_256)) {
        // Offset >= input_size: all bytes are zero
        for (uint64_t i = 0; i < size; i++) {
            evm_memory_write_byte(evm->memory, dest_offset + i, 0);
        }
    } else {
        // Offset < input_size: safe to convert to uint64
        uint64_t offset = uint256_to_uint64(&offset_256);
        for (uint64_t i = 0; i < size; i++)
        {
            uint8_t byte = 0;
            if (offset + i < evm->msg.input_size)
            {
                byte = evm->msg.input_data[offset + i];
            }
            evm_memory_write_byte(evm->memory, dest_offset + i, byte);
        }
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

    // Check for unreasonably large sizes that would cause issues
    // Any size close to UINT64_MAX is impossible to handle correctly
    // Maximum reasonable memory size is much smaller than this
    if (size > (UINT64_MAX / 2)) {
        // Size too large - would cause overflow in various calculations
        return EVM_OUT_OF_GAS;
    }

    // Check for overflow in offset + size calculation
    if (size > 0 && offset > UINT64_MAX - size) {
        return EVM_OUT_OF_GAS;
    }
    if (size > 0 && dest_offset > UINT64_MAX - size) {
        return EVM_OUT_OF_GAS;
    }

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

    // EIP-211: RETURNDATASIZE introduced in Byzantium
    if (evm->fork < FORK_BYZANTIUM)
        return EVM_INVALID_OPCODE;

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

    // EIP-211: RETURNDATACOPY introduced in Byzantium
    if (evm->fork < FORK_BYZANTIUM)
        return EVM_INVALID_OPCODE;

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
 * EXTCODESIZE - Get size of an account's code
 * Stack: address => size
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

    // Convert to address
    address_t addr;
    address_from_uint256(&address_u256, &addr);

    // Charge gas based on fork
    uint64_t gas_cost;
    if (evm->fork >= FORK_BERLIN) {
        bool is_warm = evm_is_address_warm(evm, &addr);
        gas_cost = is_warm ? GAS_WARM_ACCOUNT_ACCESS : GAS_COLD_ACCOUNT_ACCESS;
        if (!is_warm) evm_mark_address_warm(evm, &addr);
    } else if (evm->fork >= FORK_TANGERINE_WHISTLE) {
        gas_cost = GAS_EXTCODESIZE;  // 700 (EIP-150+)
    } else {
        gas_cost = GAS_EXTCODESIZE_FRONTIER;  // 20 (pre-EIP-150)
    }

    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get code size from state
    uint32_t code_size = evm_state_get_code_size(evm->state, &addr);

    // Push code size onto stack
    uint256_t size = uint256_from_uint64(code_size);
    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    LOG_EVM_DEBUG("EXTCODESIZE: address=0x...%02x%02x, size=%u",
                  addr.bytes[18], addr.bytes[19], code_size);

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

    // Pop arguments from stack: addr, destOffset, offset, size
    uint256_t address_u256, dest_offset_u256, offset_u256, size_u256;

    if (!evm_stack_pop(evm->stack, &address_u256) ||
        !evm_stack_pop(evm->stack, &dest_offset_u256) ||
        !evm_stack_pop(evm->stack, &offset_u256) ||
        !evm_stack_pop(evm->stack, &size_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    uint64_t dest_offset = uint256_to_uint64(&dest_offset_u256);
    uint64_t size = uint256_to_uint64(&size_u256);

    // Convert to address
    address_t addr;
    address_from_uint256(&address_u256, &addr);

    // Charge gas based on fork
    uint64_t access_cost;
    if (evm->fork >= FORK_BERLIN) {
        bool is_warm = evm_is_address_warm(evm, &addr);
        access_cost = is_warm ? GAS_WARM_ACCOUNT_ACCESS : GAS_COLD_ACCOUNT_ACCESS;
        if (!is_warm) evm_mark_address_warm(evm, &addr);
    } else if (evm->fork >= FORK_TANGERINE_WHISTLE) {
        access_cost = GAS_EXTCODECOPY;  // 700 (EIP-150+)
    } else {
        access_cost = GAS_EXTCODECOPY_FRONTIER;  // 20 (pre-EIP-150)
    }

    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, access_cost + copy_gas + mem_gas))
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

        // Get code from state
        uint32_t code_size = 0;
        const uint8_t *code = evm_state_get_code_ptr(evm->state, &addr, &code_size);

        // Copy code to memory (out of bounds reads return 0)
        uint64_t code_offset = uint256_to_uint64(&offset_u256);
        for (uint64_t i = 0; i < size; i++)
        {
            uint8_t byte = 0;
            uint64_t src_idx = code_offset + i;
            if (code && src_idx < code_size)
            {
                byte = code[src_idx];
            }
            evm_memory_write_byte(evm->memory, dest_offset + i, byte);
        }

        LOG_EVM_DEBUG("EXTCODECOPY: code_size=%u, offset=%lu, size=%lu",
                      code_size, code_offset, size);
    }

    return EVM_SUCCESS;
}

/**
 * EXTCODEHASH - Get hash of an account's code
 * Stack: address => hash
 * 
 * Returns keccak256 hash of code, or 0 if account doesn't exist or is empty.
 * Empty account returns empty hash (0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470)
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

    // Convert to address
    address_t addr;
    address_from_uint256(&address_u256, &addr);

    // Charge gas based on fork
    uint64_t gas_cost;
    if (evm->fork >= FORK_BERLIN) {
        bool is_warm = evm_is_address_warm(evm, &addr);
        gas_cost = is_warm ? GAS_WARM_ACCOUNT_ACCESS : GAS_COLD_ACCOUNT_ACCESS;
        if (!is_warm) evm_mark_address_warm(evm, &addr);
    } else if (evm->fork >= FORK_ISTANBUL) {
        gas_cost = GAS_EXTCODEHASH;  // 700 (EIP-1884)
    } else {
        gas_cost = 400;  // Constantinople (EIP-1052)
    }

    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get code hash from state
    uint256_t hash = UINT256_ZERO;

    // Check if account exists
    if (evm_state_exists(evm->state, &addr))
    {
        hash_t code_hash = evm_state_get_code_hash(evm->state, &addr);
        // Convert hash_t (32 bytes) to uint256_t
        hash = uint256_from_bytes(code_hash.bytes, 32);
    }
    // If account doesn't exist, hash remains 0

    if (!evm_stack_push(evm->stack, &hash))
    {
        return EVM_STACK_OVERFLOW;
    }

    LOG_EVM_DEBUG("EXTCODEHASH: address=0x...%02x%02x", 
                  addr.bytes[18], addr.bytes[19]);

    return EVM_SUCCESS;
}
