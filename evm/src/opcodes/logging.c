/**
 * EVM Logging Opcodes Implementation
 *
 * Stub implementations for event logging operations.
 * These will be fully implemented when log storage infrastructure is added.
 */

#include "opcodes/logging.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "gas.h"
#include "logger.h"
#include <string.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Generic LOG implementation (stub)
 * 
 * @param evm EVM instance
 * @param num_topics Number of topics (0-4)
 * @return EVM_SUCCESS (stub - always succeeds)
 */
static evm_status_t op_log_common(evm_t *evm, uint8_t num_topics)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation (logs modify state)
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("LOG%u: Cannot emit log in static call", num_topics);
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop offset and size from stack (offset is on top per EVM spec)
    uint256_t offset_u256, size_u256;
    if (!evm_stack_pop(evm->stack, &offset_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }
    if (!evm_stack_pop(evm->stack, &size_u256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Pop topics from stack
    uint256_t topics[4];
    for (uint8_t i = 0; i < num_topics; i++)
    {
        if (!evm_stack_pop(evm->stack, &topics[i]))
        {
            return EVM_STACK_UNDERFLOW;
        }
    }

    // Convert offset and size to uint64_t
    uint64_t offset = uint256_to_uint64(&offset_u256);
    uint64_t size = uint256_to_uint64(&size_u256);

    // Calculate dynamic gas: 375 + (375 * topics) + (8 * data_size) + memory expansion
    uint64_t log_gas = gas_log_cost(num_topics, size);
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, log_gas + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Expand memory if needed
    if (size > 0)
    {
        if (!evm_memory_expand(evm->memory, offset, size))
        {
            return EVM_INTERNAL_ERROR;
        }
    }

    // TODO: Store log entry in evm->logs array
    // For now, just log to debug output
    LOG_EVM_DEBUG("LOG%u: offset=%lu, size=%lu (stub - not stored)", 
                  num_topics, offset, size);

    return EVM_SUCCESS;
}

//==============================================================================
// LOG Opcodes
//==============================================================================

evm_status_t op_log0(evm_t *evm)
{
    return op_log_common(evm, 0);
}

evm_status_t op_log1(evm_t *evm)
{
    return op_log_common(evm, 1);
}

evm_status_t op_log2(evm_t *evm)
{
    return op_log_common(evm, 2);
}

evm_status_t op_log3(evm_t *evm)
{
    return op_log_common(evm, 3);
}

evm_status_t op_log4(evm_t *evm)
{
    return op_log_common(evm, 4);
}
