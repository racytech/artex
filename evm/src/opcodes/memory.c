/**
 * EVM Memory Opcodes Implementation
 */

#include "evm.h"
#include "opcodes/memory.h"
#include "evm_memory.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

// MLOAD (0x51): Load word from memory
evm_status_t op_mload(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_ERROR("MLOAD: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t offset;
    evm_stack_pop(evm->stack, &offset);

    // Convert offset to uint64
    uint64_t mem_offset = uint256_to_uint64(&offset);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, mem_offset, 32);
    if (!evm_use_gas(evm, GAS_VERY_LOW + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Read 32 bytes from memory
    uint256_t value;
    if (!evm_memory_read_word(evm->memory, mem_offset, &value))
    {
        LOG_ERROR("MLOAD: Failed to read from memory at offset %lu", mem_offset);
        return EVM_INVALID_MEMORY_ACCESS;
    }

    if (!evm_stack_push(evm->stack, &value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

// MSTORE (0x52): Store word to memory
evm_status_t op_mstore(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_ERROR("MSTORE: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t offset, value;
    evm_stack_pop(evm->stack, &offset);
    evm_stack_pop(evm->stack, &value);

    // Convert offset to uint64
    uint64_t mem_offset = uint256_to_uint64(&offset);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, mem_offset, 32);
    if (!evm_use_gas(evm, GAS_VERY_LOW + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Write 32 bytes to memory
    if (!evm_memory_write_word(evm->memory, mem_offset, &value))
    {
        LOG_ERROR("MSTORE: Failed to write to memory at offset %lu", mem_offset);
        return EVM_INVALID_MEMORY_ACCESS;
    }

    return EVM_SUCCESS;
}

// MSTORE8 (0x53): Store byte to memory
evm_status_t op_mstore8(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_stack_require(evm->stack, 2))
    {
        LOG_ERROR("MSTORE8: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t offset, value;
    evm_stack_pop(evm->stack, &offset);
    evm_stack_pop(evm->stack, &value);

    // Convert offset to uint64
    uint64_t mem_offset = uint256_to_uint64(&offset);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, mem_offset, 1);
    if (!evm_use_gas(evm, GAS_VERY_LOW + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Extract least significant byte
    uint8_t byte_value = uint256_to_uint64(&value) & 0xFF;

    // Write 1 byte to memory
    if (!evm_memory_write_byte(evm->memory, mem_offset, byte_value))
    {
        LOG_ERROR("MSTORE8: Failed to write to memory at offset %lu", mem_offset);
        return EVM_INVALID_MEMORY_ACCESS;
    }

    return EVM_SUCCESS;
}

// MSIZE (0x59): Get memory size
evm_status_t op_msize(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get current memory size in bytes
    size_t mem_size = evm_memory_size(evm->memory);

    uint256_t size = uint256_from_uint64(mem_size);

    if (!evm_stack_push(evm->stack, &size))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
