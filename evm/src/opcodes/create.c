/*
 * EVM CREATE Opcodes Implementation
 * Handles CREATE and CREATE2 contract creation opcodes
 */

#include "opcodes/create.h"
#include "evm.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "gas.h"
#include "logger.h"
#include "uint256.h"
#include <string.h>

//==============================================================================
// CREATE Opcodes
//==============================================================================

/**
 * CREATE - Create new contract (stub)
 * Stack: value offset size => address
 */
evm_status_t op_create(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("CREATE: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 3 arguments from stack
    uint256_t size, offset, value;
    
    if (!evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &value))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement CREATE gas logic
    // Base cost: 32000 gas
    // + memory expansion cost for init code
    // + execution cost of init code
    // + 200 gas per byte of deployed code
    if (!evm_use_gas(evm, 32000))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual contract creation logic
    LOG_EVM_DEBUG("CREATE: stub - pushing address=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CREATE2 - Create new contract with deterministic address (stub)
 * Stack: value offset size salt => address
 */
evm_status_t op_create2(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("CREATE2: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 4 arguments from stack
    uint256_t salt, size, offset, value;
    
    if (!evm_stack_pop(evm->stack, &salt) ||
        !evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &value))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement CREATE2 gas logic
    // Base cost: 32000 gas
    // + 6 gas per word for hashing init code (CREATE2-specific)
    // + memory expansion cost
    // + execution cost of init code
    // + 200 gas per byte of deployed code
    if (!evm_use_gas(evm, 32000))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual contract creation logic
    LOG_EVM_DEBUG("CREATE2: stub - pushing address=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
