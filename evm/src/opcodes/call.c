/**
 * EVM Call Opcodes Implementation
 *
 * Stub implementations for message calls and contract creation.
 * These will be fully implemented when the interpreter and call stack are added.
 */

#include "opcodes/call.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "gas.h"
#include "logger.h"
#include <string.h>

//==============================================================================
// Call Opcodes (Stubs)
//==============================================================================

/**
 * CALL - Message call into an account (stub)
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 */
evm_status_t op_call(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 7 arguments from stack
    uint256_t ret_size, ret_offset, args_size, args_offset, value, addr, gas;
    
    if (!evm_stack_pop(evm->stack, &ret_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &gas))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement complex CALL gas logic using gas_call_cost()
    // Need to account for:
    // - Base cost (depends on fork)
    // - Cold/warm access (EIP-2929)
    // - Value transfer (9000 gas if value > 0)
    // - New account creation (25000 gas)
    // - Memory expansion for args and return data
    // - Gas stipend (2300 if value > 0)
    // - 63/64 rule for gas forwarding (EIP-150)
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual call logic:
    // 1. Create new evm_message_t with depth + 1
    // 2. Set up call parameters (gas, value, input data from memory)
    // 3. Recursively call evm_execute() for the subcall
    // 4. Handle return data and copy to memory
    // 5. Push success/failure (1/0) to stack
    // Pattern:
    //   evm_message_t subcall = {
    //       .kind = EVM_CALL,
    //       .depth = evm->msg.depth + 1,
    //       .caller = evm->msg.recipient,
    //       .recipient = target_address,
    //       .gas = call_gas,
    //       .value = call_value,
    //       ...
    //   };
    //   evm_result_t subcall_result;
    //   if (evm_execute(evm, &subcall, &subcall_result)) {
    //       // Handle result, copy return data, push success
    //   }
    LOG_EVM_DEBUG("CALL: stub - pushing success=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CALLCODE - Message call with alternative account's code (stub)
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 */
evm_status_t op_callcode(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 7 arguments from stack
    uint256_t ret_size, ret_offset, args_size, args_offset, value, addr, gas;
    
    if (!evm_stack_pop(evm->stack, &ret_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &gas))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement complex CALLCODE gas logic (similar to CALL)
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual callcode logic (deprecated)
    LOG_EVM_DEBUG("CALLCODE: stub - pushing success=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * DELEGATECALL - Message call with caller's context (stub)
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 */
evm_status_t op_delegatecall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 6 arguments from stack (no value in delegatecall)
    uint256_t ret_size, ret_offset, args_size, args_offset, addr, gas;
    
    if (!evm_stack_pop(evm->stack, &ret_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &gas))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement complex DELEGATECALL gas logic
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual delegatecall logic
    LOG_EVM_DEBUG("DELEGATECALL: stub - pushing success=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * STATICCALL - Static message call (stub)
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 */
evm_status_t op_staticcall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 6 arguments from stack (no value in staticcall)
    uint256_t ret_size, ret_offset, args_size, args_offset, addr, gas;
    
    if (!evm_stack_pop(evm->stack, &ret_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &gas))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // TODO: Implement complex STATICCALL gas logic
    if (!evm_use_gas(evm, GAS_SLOAD_WARM))
    {
        return EVM_OUT_OF_GAS;
    }

    // TODO: Implement actual staticcall logic
    LOG_EVM_DEBUG("STATICCALL: stub - pushing success=0");

    // Push 0 (failure) for now
    uint256_t result = UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// Creation Opcodes (Stubs)
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
