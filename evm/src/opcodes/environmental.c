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
#include "verkle_key.h"
#include "precompile.h"
#include "logger.h"
#include <string.h>

/* EIP-4762: System contracts get warm access cost (like precompiles)
 * for BASIC_DATA and CODE_HASH access. They are NOT charged witness gas
 * for address access, only for storage/code chunks. */
static const uint8_t HISTORY_STORAGE_ADDR[20] = {
    0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
    0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xfe
};

static inline bool is_system_contract(const address_t *addr) {
    return memcmp(addr->bytes, HISTORY_STORAGE_ADDR, 20) == 0;
}

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
    if (evm->fork >= FORK_VERKLE) {
        // EIP-4762: witness gas replaces cold/warm
        uint8_t vk[32];
        verkle_account_basic_data_key(vk, addr.bytes);
        gas_cost = evm_state_witness_gas_access(evm->state, vk, false, false);
        // Warm fallback: if fully warm, charge WarmStorageReadCost
        if (gas_cost == 0) gas_cost = GAS_SLOAD_WARM;
    } else if (evm->fork >= FORK_BERLIN) {
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

    // Overflow check: impossibly large size or dest_offset means OOG
    if (size_256.high != 0 || (uint64_t)(size_256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size_256) &&
        (dest_offset_256.high != 0 || (uint64_t)(dest_offset_256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

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

    // Overflow check: impossibly large size or dest_offset means OOG
    if (size_256.high != 0 || (uint64_t)(size_256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size_256) &&
        (dest_offset_256.high != 0 || (uint64_t)(dest_offset_256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t dest_offset = uint256_to_uint64(&dest_offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate dynamic gas: base + copy cost + memory expansion
    // Note: gas depends only on dest memory expansion, NOT on source offset
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_VERY_LOW + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // EIP-4762 (Verkle): charge code chunk witness gas for copied range.
    // Skip during deployment (initcode) and system calls.
    if (evm->fork >= FORK_VERKLE && size > 0 &&
        evm->msg.kind != EVM_CREATE && evm->msg.kind != EVM_CREATE2) {
        // Adjust bounds: only charge for actual code bytes, not zero-padding
        uint64_t code_offset = 0;
        bool offset_valid = (offset_256.high == 0 &&
                            (uint64_t)(offset_256.low >> 64) == 0);
        if (offset_valid)
            code_offset = uint256_to_uint64(&offset_256);

        if (offset_valid && code_offset < evm->code_size) {
            uint64_t copy_end = code_offset + size;
            if (copy_end > evm->code_size)
                copy_end = evm->code_size;
            uint64_t non_padded_len = copy_end - code_offset;

            if (non_padded_len > 0) {
                uint32_t start_chunk = (uint32_t)(code_offset / 31);
                uint32_t end_chunk = (uint32_t)((code_offset + non_padded_len - 1) / 31);
                uint64_t wgas = 0;
                for (uint32_t c = start_chunk; c <= end_chunk; c++) {
                    uint8_t ck[32];
                    verkle_code_chunk_key(ck, evm->msg.code_addr.bytes, c);
                    wgas += evm_state_witness_gas_access(evm->state, ck, false, false);
                }
                if (!evm_use_gas(evm, wgas))
                    return EVM_OUT_OF_GAS;
            }
        }
    }

    if (size == 0)
        return EVM_SUCCESS;

    // Allocate memory if needed
    if (!evm_memory_expand(evm->memory, dest_offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Source offset can be any uint256 — bytes beyond code are zero-filled
    // Check if offset_256 exceeds code_size entirely (all zeros)
    bool offset_out_of_range = (offset_256.high != 0 ||
                                (uint64_t)(offset_256.low >> 64) != 0 ||
                                uint256_to_uint64(&offset_256) >= evm->code_size);

    if (offset_out_of_range) {
        // All bytes are zero — just zero-fill destination
        for (uint64_t i = 0; i < size; i++) {
            evm_memory_write_byte(evm->memory, dest_offset + i, 0);
        }
    } else {
        uint64_t offset = uint256_to_uint64(&offset_256);
        for (uint64_t i = 0; i < size; i++) {
            uint8_t byte = 0;
            if (offset + i < evm->code_size) {
                byte = evm->code[offset + i];
            }
            evm_memory_write_byte(evm->memory, dest_offset + i, byte);
        }
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

    // Overflow check: impossibly large size or dest_offset means OOG
    if (size_256.high != 0 || (uint64_t)(size_256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size_256) &&
        (dest_offset_256.high != 0 || (uint64_t)(dest_offset_256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t dest_offset = uint256_to_uint64(&dest_offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // For RETURNDATACOPY, offset must also be checked for bounds against return_data_size.
    // If offset overflows uint64 but size > 0, it's definitely out of bounds.
    if (!uint256_is_zero(&size_256) &&
        (offset_256.high != 0 || (uint64_t)(offset_256.low >> 64) != 0))
        return EVM_INVALID_MEMORY_ACCESS;

    uint64_t offset = uint256_to_uint64(&offset_256);

    // Calculate dynamic gas: base + copy cost + memory expansion
    uint64_t copy_gas = gas_copy_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, dest_offset, size);
    if (!evm_use_gas(evm, GAS_VERY_LOW + copy_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Check for overflow and bounds (EIP-211: revert if out of bounds even for size==0)
    if (offset + size < offset || offset + size > evm->return_data_size)
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
    if (evm->fork >= FORK_VERKLE) {
        // EIP-4762: precompiles and system contracts get warm cost only
        if (is_precompile(&addr, evm->fork) || is_system_contract(&addr)) {
            gas_cost = GAS_SLOAD_WARM;
        } else {
            uint8_t vk[32];
            verkle_account_basic_data_key(vk, addr.bytes);
            gas_cost = evm_state_witness_gas_access(evm->state, vk, false, false);
            if (gas_cost == 0) gas_cost = GAS_SLOAD_WARM;
        }
    } else if (evm->fork >= FORK_BERLIN) {
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

    // Get code size from state (no delegation resolution per EIP-7702 spec)
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

    // Overflow check: impossibly large size or dest_offset means OOG
    if (size_u256.high != 0 || (uint64_t)(size_u256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size_u256) &&
        (dest_offset_u256.high != 0 || (uint64_t)(dest_offset_u256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t dest_offset = uint256_to_uint64(&dest_offset_u256);
    uint64_t size = uint256_to_uint64(&size_u256);

    // Convert to address
    address_t addr;
    address_from_uint256(&address_u256, &addr);

    // Charge gas based on fork
    uint64_t access_cost;
    if (evm->fork >= FORK_VERKLE) {
        // EIP-4762: precompiles and system contracts get warm cost only
        if (is_precompile(&addr, evm->fork) || is_system_contract(&addr)) {
            access_cost = GAS_SLOAD_WARM;
        } else {
            uint8_t vk[32];
            verkle_account_basic_data_key(vk, addr.bytes);
            access_cost = evm_state_witness_gas_access(evm->state, vk, false, false);
            if (access_cost == 0) access_cost = GAS_SLOAD_WARM;
        }
    } else if (evm->fork >= FORK_BERLIN) {
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

    // EIP-4762 (Verkle): code chunk witness gas for EXTCODECOPY.
    // Skipped for precompiles (no code). System contracts have real
    // code, so code chunks ARE charged for EXTCODECOPY.
    if (evm->fork >= FORK_VERKLE && size > 0) {
        if (!is_precompile(&addr, evm->fork)) {
            uint32_t ext_code_size = evm_state_get_code_size(evm->state, &addr);
            if (ext_code_size > 0) {
                uint64_t code_offset = 0;
                bool off_valid = (offset_u256.high == 0 &&
                                  (uint64_t)(offset_u256.low >> 64) == 0);
                if (off_valid)
                    code_offset = uint256_to_uint64(&offset_u256);

                if (off_valid && code_offset < ext_code_size) {
                    uint64_t copy_end = code_offset + size;
                    if (copy_end > ext_code_size) copy_end = ext_code_size;
                    uint64_t non_padded = copy_end - code_offset;
                    if (non_padded > 0) {
                        uint32_t start_chunk = (uint32_t)(code_offset / 31);
                        uint32_t end_chunk = (uint32_t)((code_offset + non_padded - 1) / 31);
                        uint64_t wgas = 0;
                        for (uint32_t c = start_chunk; c <= end_chunk; c++) {
                            uint8_t ck[32];
                            verkle_code_chunk_key(ck, addr.bytes, c);
                            wgas += evm_state_witness_gas_access(evm->state, ck, false, false);
                        }
                        if (!evm_use_gas(evm, wgas))
                            return EVM_OUT_OF_GAS;
                    }
                }
            }
        }
    }

    // Expand memory if needed
    if (size > 0)
    {
        if (!evm_memory_expand(evm->memory, dest_offset, size))
        {
            return EVM_INTERNAL_ERROR;
        }

        // Get code from state (no delegation resolution per EIP-7702 spec)
        uint32_t code_size = 0;
        const uint8_t *code = evm_state_get_code_ptr(evm->state, &addr, &code_size);

        // Copy code to memory (out of bounds reads return 0)
        // Check if offset overflows uint64 — if so, all bytes are zero-padded
        bool offset_out_of_range = (offset_u256.high != 0 ||
                                    (uint64_t)(offset_u256.low >> 64) != 0);
        if (offset_out_of_range)
        {
            for (uint64_t i = 0; i < size; i++)
                evm_memory_write_byte(evm->memory, dest_offset + i, 0);
        }
        else
        {
            uint64_t code_offset = uint256_to_uint64(&offset_u256);
            for (uint64_t i = 0; i < size; i++)
            {
                uint8_t byte = 0;
                uint64_t src_idx = code_offset + i;
                // Guard against uint64 overflow wrap-around
                if (code && src_idx >= code_offset && src_idx < code_size)
                {
                    byte = code[src_idx];
                }
                evm_memory_write_byte(evm->memory, dest_offset + i, byte);
            }
        }
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

    // EIP-1052: EXTCODEHASH introduced in Constantinople
    if (evm->fork < FORK_CONSTANTINOPLE)
        return EVM_INVALID_OPCODE;

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
    if (evm->fork >= FORK_VERKLE) {
        // EIP-4762: precompiles and system contracts get warm cost only
        if (is_precompile(&addr, evm->fork) || is_system_contract(&addr)) {
            gas_cost = GAS_SLOAD_WARM;
        } else {
            uint8_t vk[32];
            verkle_account_code_hash_key(vk, addr.bytes);
            gas_cost = evm_state_witness_gas_access(evm->state, vk, false, false);
            if (gas_cost == 0) gas_cost = GAS_SLOAD_WARM;
        }
    } else if (evm->fork >= FORK_BERLIN) {
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

    // Get code hash from state (no delegation resolution per EIP-7702 spec)
    // EIP-1052 + EIP-161: return 0 for empty/non-existent accounts,
    // keccak256(code) otherwise
    uint256_t hash = UINT256_ZERO;

    if (!evm_state_is_empty(evm->state, &addr))
    {
        hash_t code_hash = evm_state_get_code_hash(evm->state, &addr);
        // Convert hash_t (32 bytes) to uint256_t
        hash = uint256_from_bytes(code_hash.bytes, 32);
    }

    if (!evm_stack_push(evm->stack, &hash))
    {
        return EVM_STACK_OVERFLOW;
    }

    LOG_EVM_DEBUG("EXTCODEHASH: address=0x...%02x%02x", 
                  addr.bytes[18], addr.bytes[19]);

    return EVM_SUCCESS;
}
