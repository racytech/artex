/**
 * EVM Logging Opcodes Implementation
 *
 * LOG0-LOG4 capture log entries into the EVM's log accumulator.
 * Logs are discarded on REVERT (handled by evm_execute/create).
 */

#include "opcodes/logging.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "gas.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Generic LOG implementation
 *
 * @param evm EVM instance
 * @param num_topics Number of topics (0-4)
 * @return EVM_SUCCESS on success, error status otherwise
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

    // Overflow check: impossibly large size or offset means OOG
    if (size_u256.high != 0 || (uint64_t)(size_u256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size_u256) &&
        (offset_u256.high != 0 || (uint64_t)(offset_u256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

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

    // Grow log accumulator if needed
    if (evm->log_count >= evm->log_cap)
    {
        size_t new_cap = evm->log_cap ? evm->log_cap * 2 : 8;
        evm_log_t *new_logs = realloc(evm->logs, new_cap * sizeof(evm_log_t));
        if (!new_logs) return EVM_INTERNAL_ERROR;
        evm->logs = new_logs;
        evm->log_cap = new_cap;
    }

    evm_log_t *log = &evm->logs[evm->log_count++];
    address_copy(&log->address, &evm->msg.recipient);
    log->topic_count = num_topics;
    for (uint8_t i = 0; i < num_topics; i++)
        uint256_to_bytes(&topics[i], log->topics[i].bytes);

    log->data = NULL;
    log->data_len = (size_t)size;
    if (size > 0)
    {
        log->data = malloc((size_t)size);
        if (!log->data)
        {
            evm->log_count--;
            return EVM_INTERNAL_ERROR;
        }
        const uint8_t *mem_ptr = evm_memory_get_ptr(evm->memory, offset);
        memcpy(log->data, mem_ptr, (size_t)size);
    }

    return EVM_SUCCESS;
}

//==============================================================================
// LOG Opcodes
//==============================================================================

static evm_status_t op_log0(evm_t *evm)
{
    return op_log_common(evm, 0);
}

static evm_status_t op_log1(evm_t *evm)
{
    return op_log_common(evm, 1);
}

static evm_status_t op_log2(evm_t *evm)
{
    return op_log_common(evm, 2);
}

static evm_status_t op_log3(evm_t *evm)
{
    return op_log_common(evm, 3);
}

static evm_status_t op_log4(evm_t *evm)
{
    return op_log_common(evm, 4);
}
