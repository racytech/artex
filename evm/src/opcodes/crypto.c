/**
 * EVM Cryptographic Opcodes Implementation
 */

#include "opcodes/crypto.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "hash.h"
#include "gas.h"
#include <string.h>
#include <stdlib.h>

//==============================================================================
// KECCAK256 Implementation
//==============================================================================

/**
 * KECCAK256 - Compute Keccak-256 hash
 * Stack: offset size => hash
 * Memory: Read from [offset:offset+size]
 * Gas: 30 + 6 * (size in words) + memory_expansion_cost
 */
evm_status_t op_keccak256(evm_t *evm)
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

    // Calculate dynamic gas: 30 base + 6 per word + memory expansion
    uint64_t sha3_gas = gas_sha3_cost(size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, sha3_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Handle zero size - hash of empty data
    if (size == 0)
    {
        hash_t empty_hash = hash_keccak256(NULL, 0);

        // Convert hash (big-endian) to uint256
        uint256_t hash_value = uint256_from_bytes(empty_hash.bytes, HASH_SIZE);

        if (!evm_stack_push(evm->stack, &hash_value))
        {
            return EVM_STACK_OVERFLOW;
        }

        return EVM_SUCCESS;
    }

    // Expand memory if needed
    if (!evm_memory_expand(evm->memory, offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Allocate buffer to read memory data
    uint8_t *data = malloc(size);
    if (!data)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Read data from memory
    for (uint64_t i = 0; i < size; i++)
    {
        if (!evm_memory_read_byte(evm->memory, offset + i, &data[i]))
        {
            free(data);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Compute Keccak-256 hash
    hash_t hash = hash_keccak256(data, size);
    free(data);

    // Convert hash (big-endian) to uint256
    uint256_t hash_value = uint256_from_bytes(hash.bytes, HASH_SIZE);

    if (!evm_stack_push(evm->stack, &hash_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
