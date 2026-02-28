/**
 * EVM Call Opcodes Implementation
 *
 * Implements CALL, CALLCODE, DELEGATECALL, STATICCALL family of opcodes.
 */

#include "opcodes/call.h"
#include "evm_state.h"
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
 * Shared preparation for CALL family opcodes
 * Handles memory expansion, gas calculation, safety checks
 * 
 * @param evm EVM context
 * @param target_addr Target address
 * @param value Value to transfer (can be zero)
 * @param gas_param Requested gas from stack
 * @param args_offset Arguments offset in memory
 * @param args_size Arguments size
 * @param ret_offset Return data offset in memory
 * @param ret_size Return data size
 * @param allow_value Whether this call type allows value transfer
 * @param gas_forwarded Output: gas to forward to subcall (including stipend)
 * @return EVM_SUCCESS if ready for subcall, error code on failure, 
 *         or EVM_SUCCESS with gas_forwarded=0 if call should fail gracefully
 */
static evm_status_t prepare_call(
    evm_t *evm,
    const address_t *target_addr,
    const uint256_t *value,
    const uint256_t *gas_param,
    uint64_t args_offset,
    uint64_t args_size,
    uint64_t ret_offset,
    uint64_t ret_size,
    bool allow_value,
    uint64_t *gas_forwarded)
{
    bool has_value = !uint256_is_zero(value);

    // Check static call violation (can't transfer value in static context)
    if (evm->msg.is_static && has_value)
    {
        LOG_EVM_ERROR("CALL: Cannot transfer value in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Check if value transfer is allowed for this call type
    if (!allow_value && has_value)
    {
        LOG_EVM_ERROR("CALL: Value transfer not allowed for this call type");
        return EVM_STATIC_CALL_VIOLATION;
    }

    //==========================================================================
    // Memory Expansion
    //==========================================================================
    
    // Expand memory for args if args_size > 0
    if (args_size > 0)
    {
        uint64_t args_end = args_offset + args_size;
        uint64_t mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, args_end);
        
        fprintf(stderr, "  Memory expansion for args: cost=%lu, gas_left_before=%lu\n", 
                mem_expansion_cost, evm->gas_left);
        
        if (!evm_use_gas(evm, mem_expansion_cost))
        {
            return EVM_OUT_OF_GAS;
        }
        
        fprintf(stderr, "  After args memory expansion: gas_left=%lu\n", evm->gas_left);
        
        if (!evm_memory_expand(evm->memory, args_offset, args_size))
        {
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Expand memory for return data if ret_size > 0
    if (ret_size > 0)
    {
        uint64_t ret_end = ret_offset + ret_size;
        uint64_t mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, ret_end);
        
        fprintf(stderr, "  Memory expansion for return: cost=%lu, gas_left_before=%lu\n", 
                mem_expansion_cost, evm->gas_left);
        
        if (!evm_use_gas(evm, mem_expansion_cost))
        {
            return EVM_OUT_OF_GAS;
        }
        
        fprintf(stderr, "  After return memory expansion: gas_left=%lu\n", evm->gas_left);
        
        if (!evm_memory_expand(evm->memory, ret_offset, ret_size))
        {
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    //==========================================================================
    // Balance Check (done before gas calculation to avoid charging for account creation)
    //==========================================================================

    // Check if caller has enough balance for value transfer
    bool balance_sufficient = true;
    if (has_value)
    {
        uint256_t caller_balance = evm_state_get_balance(evm->state, &evm->msg.recipient);

        if (uint256_lt(&caller_balance, value))
        {
            balance_sufficient = false;
            fprintf(stderr, "CALL: Insufficient balance for value transfer (need=%lu, have=%lu)\n",
                    uint256_to_uint64(value), uint256_to_uint64(&caller_balance));
        }
    }

    //==========================================================================
    // Gas Calculation
    //==========================================================================

    // Check if account exists
    bool account_exists = evm_state_exists(evm->state, target_addr);

    // Check if this is a cold access (Berlin+)
    bool is_cold = !evm_is_address_warm(evm, target_addr);
    
    // Mark address as warm for future accesses
    if (is_cold)
    {
        evm_mark_address_warm(evm, target_addr);
    }

    // Only charge for account creation if balance is sufficient
    // (don't charge if we know the call will fail)
    // UPDATE: Always charge for account creation per EIP spec
    bool treat_as_existing = account_exists; // Always use actual account state

    // Calculate CALL gas cost  
    uint64_t call_cost = gas_call_cost(evm->fork, is_cold, has_value, treat_as_existing);
    
    // Calculate stipend (will be reserved upfront and refunded if call doesn't execute)
    uint64_t stipend = has_value ? gas_call_stipend(value) : 0;

    // DEBUG: Log gas calculation details
    fprintf(stderr, "CALL GAS DEBUG: fork=%d, is_cold=%d, has_value=%d, account_exists=%d, balance_sufficient=%d, call_cost=%lu, stipend=%lu\n",
            evm->fork, is_cold, has_value, account_exists, balance_sufficient, call_cost, stipend);
    
    fprintf(stderr, "  Before deducting call_cost: gas_left=%lu\n", evm->gas_left);

    // Deduct call cost
    if (!evm_use_gas(evm, call_cost))
    {
        return EVM_OUT_OF_GAS;
    }
    
    // Add stipend to caller's gas (it's free bonus gas for value transfers)
    // The stipend will be included in gas_forwarded to the callee
    if (stipend > 0)
    {
        evm->gas_left += stipend;
        fprintf(stderr, "  Added stipend to gas_left: +%lu gas\n", stipend);
    }
    
    fprintf(stderr, "  After deducting call_cost and adding stipend: gas_left=%lu\n", evm->gas_left);

    //==========================================================================
    // Final Balance Check and Call Depth Check
    //==========================================================================

    // Check call depth limit (1024)
    if (evm->msg.depth >= 1024)
    {
        fprintf(stderr, "CALL: Call depth limit exceeded (depth=%d)\n", evm->msg.depth);
        // Signal that call should fail gracefully (push 0)
        *gas_forwarded = 0;
        return EVM_SUCCESS;
    }

    // If balance was insufficient, fail gracefully (DON'T reserve stipend)
    if (!balance_sufficient)
    {
        fprintf(stderr, "CALL: Insufficient balance, failing gracefully (caller keeps stipend bonus)\n");
        // Signal that call should fail gracefully (push 0)
        *gas_forwarded = 0;
        return EVM_SUCCESS;
    }

    //==========================================================================
    // Gas Forwarding (63/64 rule)
    //==========================================================================

    // Calculate maximum gas that can be forwarded (EIP-150: 63/64 of remaining gas)
    uint64_t gas_available = gas_max_call_gas(evm->gas_left);
    
    // Determine how much gas to forward
    // If gas_param >= gas_available OR gas_param == 0, forward all available gas
    uint64_t gas_requested = uint256_to_uint64(gas_param);
    uint64_t gas_to_forward;
    
    if (gas_requested == 0 || gas_requested >= gas_available)
    {
        gas_to_forward = gas_available;
    }
    else
    {
        gas_to_forward = gas_requested;
    }

    // Add stipend if transferring value (already added to gas_left above)
    *gas_forwarded = gas_to_forward + stipend;

    fprintf(stderr, "PREPARE_CALL DEBUG: gas_to_forward=%lu, stipend=%lu, total gas_forwarded=%lu\n",
            gas_to_forward, stipend, *gas_forwarded);
    
    // Deduct the forwarded gas from caller (will be refunded after subcall completes)
    // Note: stipend was already added to gas_left, so we're only deducting gas_to_forward
    if (!evm_use_gas(evm, gas_to_forward))
    {
        *gas_forwarded = 0;  // Initialize to prevent garbage value
        return EVM_OUT_OF_GAS;
    }
    fprintf(stderr, "PREPARE_CALL: Reserved %lu gas for subcall, gas_left now=%lu\n", 
            gas_to_forward, evm->gas_left);

    return EVM_SUCCESS;
}

//==============================================================================
// Call Opcodes
//==============================================================================

/**
 * CALL - Message call into an account
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 */
evm_status_t op_call(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    fprintf(stderr, "\n=== CALL OPCODE START: gas_left=%lu ===\n", evm->gas_left);

    // Pop 7 arguments from stack (in reverse order of how they were pushed)
    // Stack has: [... gas addr value argsOffset argsSize retOffset retSize] <- top
    // Pop order: gas (top) -> addr -> value -> argsOffset -> argsSize -> retOffset -> retSize (bottom)
    uint256_t gas, addr, value, args_offset, args_size, ret_offset, ret_size;
    
    if (!evm_stack_pop(evm->stack, &gas) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &ret_size))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Extract address (20 bytes from uint256_t)
    address_t target_addr;
    address_from_uint256(&addr, &target_addr);

    // Convert sizes to uint64_t
    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    fprintf(stderr, "Before prepare_call: gas_left=%lu\n", evm->gas_left);

    // Prepare call (memory expansion, gas calculation, checks)
    uint64_t gas_forwarded;
    evm_status_t status = prepare_call(evm, &target_addr, &value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       true, &gas_forwarded);
    
    fprintf(stderr, "PREPARE_CALL returned: status=%d, gas_forwarded=%lu, evm->gas_left=%lu\n",
            status, gas_forwarded, evm->gas_left);
    
    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If gas_forwarded is 0, call failed gracefully (depth/balance check)
    if (gas_forwarded == 0)
    {
        fprintf(stderr, "CALL failed gracefully (gas_forwarded=0), pushing 0 to stack\n");
        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        fprintf(stderr, "=== CALL OPCODE END (early): gas_left=%lu ===\n\n", evm->gas_left);
        return EVM_SUCCESS;
    }

    //==========================================================================
    // Subcall Execution (Stub)
    //==========================================================================

    // Extract call arguments from memory
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = malloc(args_size_u64);
        if (!call_args)
        {
            LOG_EVM_ERROR("CALL: Failed to allocate call arguments");
            return EVM_INTERNAL_ERROR;
        }
        
        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            LOG_EVM_ERROR("CALL: Failed to read call arguments from memory");
            free(call_args);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Create subcall message
    evm_message_t subcall_msg = {
        .kind = EVM_CALL,
        .caller = evm->msg.recipient,
        .recipient = target_addr,
        .code_addr = target_addr,
        .value = value,
        .input_data = call_args,
        .input_size = args_size_u64,
        .gas = gas_forwarded,
        .depth = evm->msg.depth + 1,
        .is_static = evm->msg.is_static
    };

    // Execute the subcall
    evm_result_t subcall_result;
    bool exec_ok = evm_execute(evm, &subcall_msg, &subcall_result);
    
    fprintf(stderr, "CALL RESULT DEBUG: exec_ok=%d, status=%d, gas_left=%lu\n",
            exec_ok, subcall_result.status, subcall_result.gas_left);
    
    if (call_args) free(call_args);

    if (!exec_ok)
    {
        LOG_EVM_ERROR("CALL: Subcall execution failed internally");
        return EVM_INTERNAL_ERROR;
    }

    // Copy return data to memory
    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64, 
                             subcall_result.output_data, copy_size))
        {
            if (subcall_result.output_data) free(subcall_result.output_data);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);
    
    // DEBUG: Log gas refund details
    uint64_t stipend_debug = gas_call_stipend(&value);
    fprintf(stderr, "CALL GAS REFUND DEBUG: subcall_result.gas_left=%lu, stipend=%lu\n",
            subcall_result.gas_left, stipend_debug);
    
    if (call_succeeded)
    {
        // Refund ALL unused gas to caller
        // (The EVM interpreter handles gas reservation for subcalls internally)
        fprintf(stderr, "CALL GAS REFUND DEBUG: refunding %lu gas to caller\n", subcall_result.gas_left);
        
        evm->gas_left += subcall_result.gas_left;
        evm->gas_refund += subcall_result.gas_refund;
    }

    if (subcall_result.output_data) free(subcall_result.output_data);

    uint256_t result;
    result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    fprintf(stderr, "=== CALL OPCODE END: gas_left=%lu ===\n\n", evm->gas_left);

    return EVM_SUCCESS;
}

/**
 * CALLCODE - Message call with alternative account's code
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 * NOTE: Deprecated, use DELEGATECALL instead
 */
evm_status_t op_callcode(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 7 arguments from stack (in reverse order of how they were pushed)
    // Stack: gas addr value argsOffset argsSize retOffset retSize
    uint256_t gas, addr, value, args_offset, args_size, ret_offset, ret_size;
    
    if (!evm_stack_pop(evm->stack, &gas) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &ret_size))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Extract address (20 bytes from uint256_t)
    address_t target_addr;
    address_from_uint256(&addr, &target_addr);

    // Convert sizes to uint64_t
    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // Prepare call (memory expansion, gas calculation, checks)
    // CALLCODE allows value transfer (unlike DELEGATECALL)
    uint64_t gas_forwarded;
    evm_status_t status = prepare_call(evm, &target_addr, &value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       true, &gas_forwarded);
    
    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If gas_forwarded is 0, call failed gracefully (depth/balance check)
    if (gas_forwarded == 0)
    {
        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = malloc(args_size_u64);
        if (!call_args)
        {
            LOG_EVM_ERROR("CALLCODE: Failed to allocate call arguments");
            return EVM_INTERNAL_ERROR;
        }
        
        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            LOG_EVM_ERROR("CALLCODE: Failed to read call arguments from memory");
            free(call_args);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Create subcall message (CALLCODE executes target code in caller's context)
    evm_message_t subcall_msg = {
        .kind = EVM_CALLCODE,
        .caller = evm->msg.recipient,
        .recipient = evm->msg.recipient,   // Execute in current contract's context
        .code_addr = target_addr,          // But use target's code
        .value = value,
        .input_data = call_args,
        .input_size = args_size_u64,
        .gas = gas_forwarded,
        .depth = evm->msg.depth + 1,
        .is_static = evm->msg.is_static
    };

    evm_result_t subcall_result;
    bool exec_ok = evm_execute(evm, &subcall_msg, &subcall_result);
    
    if (call_args) free(call_args);

    if (!exec_ok)
    {
        LOG_EVM_ERROR("CALLCODE: Subcall execution failed internally");
        return EVM_INTERNAL_ERROR;
    }

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64, 
                             subcall_result.output_data, copy_size))
        {
            if (subcall_result.output_data) free(subcall_result.output_data);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);
    
    if (call_succeeded)
    {
        // Refund ALL unused gas to caller
        // (The EVM interpreter handles gas reservation for subcalls internally)
        evm->gas_left += subcall_result.gas_left;
        evm->gas_refund += subcall_result.gas_refund;
    }

    if (subcall_result.output_data) free(subcall_result.output_data);

    uint256_t result;
    result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * DELEGATECALL - Message call with caller's context
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 * NOTE: No value parameter - delegates with caller's value
 */
evm_status_t op_delegatecall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 6 arguments from stack (no value in delegatecall)
    // Stack: gas addr argsOffset argsSize retOffset retSize
    uint256_t gas, addr, args_offset, args_size, ret_offset, ret_size;
    
    if (!evm_stack_pop(evm->stack, &gas) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &ret_size))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Extract address (20 bytes from uint256_t)
    address_t target_addr;
    address_from_uint256(&addr, &target_addr);

    // Convert sizes to uint64_t
    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // DELEGATECALL does not transfer value
    uint256_t zero_value = UINT256_ZERO;

    // Prepare call (memory expansion, gas calculation, checks)
    // DELEGATECALL does NOT allow value transfer
    uint64_t gas_forwarded;
    evm_status_t status = prepare_call(evm, &target_addr, &zero_value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       false, &gas_forwarded);
    
    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If gas_forwarded is 0, call failed gracefully (depth check)
    // Note: Balance check won't fail since value is always 0
    if (gas_forwarded == 0)
    {
        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = malloc(args_size_u64);
        if (!call_args)
        {
            LOG_EVM_ERROR("DELEGATECALL: Failed to allocate call arguments");
            return EVM_INTERNAL_ERROR;
        }
        
        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            LOG_EVM_ERROR("DELEGATECALL: Failed to read call arguments from memory");
            free(call_args);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Create subcall message (DELEGATECALL preserves caller's context)
    evm_message_t subcall_msg = {
        .kind = EVM_DELEGATECALL,
        .caller = evm->msg.caller,         // Preserve original caller
        .recipient = evm->msg.recipient,   // Execute in current contract's context
        .code_addr = target_addr,          // But use target's code
        .value = evm->msg.value,           // Preserve original value
        .input_data = call_args,
        .input_size = args_size_u64,
        .gas = gas_forwarded,
        .depth = evm->msg.depth + 1,
        .is_static = evm->msg.is_static
    };

    evm_result_t subcall_result;
    bool exec_ok = evm_execute(evm, &subcall_msg, &subcall_result);
    
    if (call_args) free(call_args);

    if (!exec_ok)
    {
        LOG_EVM_ERROR("DELEGATECALL: Subcall execution failed internally");
        return EVM_INTERNAL_ERROR;
    }

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64, 
                             subcall_result.output_data, copy_size))
        {
            if (subcall_result.output_data) free(subcall_result.output_data);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);
    
    if (call_succeeded)
    {
        evm->gas_left += subcall_result.gas_left;
        evm->gas_refund += subcall_result.gas_refund;
    }

    if (subcall_result.output_data) free(subcall_result.output_data);

    uint256_t result;
    result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * STATICCALL - Static message call (no state modifications)
 * Stack: gas addr argsOffset argsSize retOffset retSize => success
 * NOTE: No value parameter - static calls cannot transfer value
 */
evm_status_t op_staticcall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Pop 6 arguments from stack (no value in staticcall)
    // Stack: gas addr argsOffset argsSize retOffset retSize
    uint256_t gas, addr, args_offset, args_size, ret_offset, ret_size;
    
    if (!evm_stack_pop(evm->stack, &gas) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &ret_size))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Extract address (20 bytes from uint256_t)
    address_t target_addr;
    address_from_uint256(&addr, &target_addr);

    // Convert sizes to uint64_t
    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // STATICCALL does not transfer value
    uint256_t zero_value = UINT256_ZERO;

    // Prepare call (memory expansion, gas calculation, checks)
    // STATICCALL does NOT allow value transfer
    uint64_t gas_forwarded;
    evm_status_t status = prepare_call(evm, &target_addr, &zero_value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       false, &gas_forwarded);
    
    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If gas_forwarded is 0, call failed gracefully (depth check)
    if (gas_forwarded == 0)
    {
        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = malloc(args_size_u64);
        if (!call_args)
        {
            LOG_EVM_ERROR("STATICCALL: Failed to allocate call arguments");
            return EVM_INTERNAL_ERROR;
        }
        
        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            LOG_EVM_ERROR("STATICCALL: Failed to read call arguments from memory");
            free(call_args);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Create subcall message (STATICCALL is read-only)
    evm_message_t subcall_msg = {
        .kind = EVM_STATICCALL,
        .caller = evm->msg.recipient,
        .recipient = target_addr,
        .code_addr = target_addr,
        .value = UINT256_ZERO,
        .input_data = call_args,
        .input_size = args_size_u64,
        .gas = gas_forwarded,
        .depth = evm->msg.depth + 1,
        .is_static = true
    };

    evm_result_t subcall_result;
    bool exec_ok = evm_execute(evm, &subcall_msg, &subcall_result);
    
    if (call_args) free(call_args);

    if (!exec_ok)
    {
        LOG_EVM_ERROR("STATICCALL: Subcall execution failed internally");
        return EVM_INTERNAL_ERROR;
    }

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64, 
                             subcall_result.output_data, copy_size))
        {
            if (subcall_result.output_data) free(subcall_result.output_data);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);
    
    if (call_succeeded)
    {
        evm->gas_left += subcall_result.gas_left;
        evm->gas_refund += subcall_result.gas_refund;
    }

    if (subcall_result.output_data) free(subcall_result.output_data);

    uint256_t result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;
    
    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
