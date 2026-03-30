/**
 * EVM Call Opcodes Implementation
 *
 * Implements CALL, CALLCODE, DELEGATECALL, STATICCALL family of opcodes.
 */

#include "opcodes/call.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "evm_tracer.h"
#include "uint256.h"
#include "gas.h"
#include "precompile.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// g_trace_calls declared in interpreter.c (unity build)

/* Stack buffer for small CALL input data — avoids malloc in the common case.
 * 2 KB covers most contract calls. Max call depth 1024 × 2 KB = 2 MB stack. */
#define CALLDATA_STACK_SIZE 2048

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
    bool can_create_account,
    uint64_t *gas_forwarded,
    bool *graceful_failure)
{
    bool has_value = !uint256_is_zero(value);

    // Check if value transfer is allowed for this call type
    // Note: static+value check for CALL is done in op_call() before calling prepare_call.
    // CALLCODE with value in static context is permitted (EIP-214: only CALL is restricted).
    if (!allow_value && has_value)
    {
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
        if (!evm_use_gas(evm, mem_expansion_cost))
            return EVM_OUT_OF_GAS;
        if (!evm_memory_expand(evm->memory, args_offset, args_size))
            return EVM_INVALID_MEMORY_ACCESS;
    }

    // Expand memory for return data if ret_size > 0
    if (ret_size > 0)
    {
        uint64_t ret_end = ret_offset + ret_size;
        uint64_t mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, ret_end);
        if (!evm_use_gas(evm, mem_expansion_cost))
            return EVM_OUT_OF_GAS;
        if (!evm_memory_expand(evm->memory, ret_offset, ret_size))
            return EVM_INVALID_MEMORY_ACCESS;
    }

    //==========================================================================
    // Balance Check (deferred — used after gas deduction, matching geth)
    //==========================================================================

    bool balance_sufficient = true;
    if (has_value)
    {
        uint256_t caller_balance = evm_state_get_balance(evm->state, &evm->msg.recipient);
        if (g_trace_calls) {
            fprintf(stderr, "    balance_check: caller=%02x%02x..%02x%02x bal=%016lx_%016lx val=%016lx_%016lx\n",
                    evm->msg.recipient.bytes[0], evm->msg.recipient.bytes[1],
                    evm->msg.recipient.bytes[18], evm->msg.recipient.bytes[19],
                    (unsigned long)(caller_balance.low >> 64), (unsigned long)caller_balance.low,
                    (unsigned long)(value->low >> 64), (unsigned long)value->low);
        }
        if (uint256_lt(&caller_balance, value))
            balance_sufficient = false;
    }

    //==========================================================================
    // Gas Calculation
    //==========================================================================

    // For new_account gas: only CALL can create accounts; CALLCODE/DELEGATECALL/STATICCALL cannot
    // Pre-EIP-161: check if account exists
    // EIP-161+ (Spurious Dragon): check if account is empty (go-ethereum uses Empty not Exist)
    bool account_exists;
    if (!can_create_account) {
        account_exists = true;  // CALLCODE/DELEGATECALL/STATICCALL never charge new account cost
    } else if (evm->fork >= FORK_SPURIOUS_DRAGON) {
        account_exists = !evm_state_is_empty(evm->state, target_addr);
    } else {
        account_exists = evm_state_exists(evm->state, target_addr);
    }

    // Calculate stipend (bonus gas for value transfers)
    uint64_t stipend = has_value ? gas_call_stipend(value) : 0;

    {
        // Cold/warm access (Berlin+)
        bool is_cold = !evm_is_address_warm(evm, target_addr);
        if (is_cold)
            evm_mark_address_warm(evm, target_addr);

        // EIP-7702: If target has delegation designator, charge cold/warm for delegation target
        uint64_t delegation_gas_cost = 0;
        if (evm->fork >= FORK_PRAGUE) {
            address_t delegate_target;
            if (evm_resolve_delegation(evm->state, target_addr, &delegate_target)) {
                if (!evm_is_address_warm(evm, &delegate_target)) {
                    evm_mark_address_warm(evm, &delegate_target);
                    delegation_gas_cost = 2600;  // GAS_COLD_ACCOUNT_ACCESS
                } else {
                    delegation_gas_cost = 100;   // GAS_WARM_ACCESS for delegation target
                }
            }
        }

        // Calculate CALL gas overhead (cold/warm + value_transfer + account_creation)
        uint64_t call_cost = gas_call_cost(evm->fork, is_cold, has_value, account_exists);

        if (g_trace_calls) {
            fprintf(stderr, "  CALL@pc=%lu depth=%d target=%02x%02x..%02x%02x "
                    "exists=%d value=%s cold=%d cost=%lu gas_before=%lu\n",
                    evm->pc, evm->msg.depth,
                    target_addr->bytes[0], target_addr->bytes[1],
                    target_addr->bytes[18], target_addr->bytes[19],
                    account_exists, has_value?"yes":"no", is_cold,
                    call_cost, evm->gas_left);
        }

        // Deduct call overhead + delegation gas
        if (!evm_use_gas(evm, call_cost + delegation_gas_cost))
            return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Gas Forwarding
    //==========================================================================
    // Compute gas to forward BEFORE graceful failure checks. In geth, the gas
    // table deducts overhead + callGasTemp + stipend all at once. If this total
    // exceeds available gas, the instruction OOGs before balance/depth checks.
    // We check for that OOG condition first, then handle graceful failure.

    // If gas_param overflows uint64, treat as "all available" (geth behavior)
    bool gas_overflows = (gas_param->high != 0 || (uint64_t)(gas_param->low >> 64) != 0);
    uint64_t gas_requested = gas_overflows ? UINT64_MAX : uint256_to_uint64(gas_param);
    uint64_t gas_to_forward;

    if (evm->fork >= FORK_TANGERINE_WHISTLE)
    {
        // EIP-150: Cap gas forwarded at 63/64 of remaining gas
        uint64_t gas_available = gas_max_call_gas(evm->gas_left);

        if (gas_requested >= gas_available)
            gas_to_forward = gas_available;
        else
            gas_to_forward = gas_requested;
    }
    else
    {
        // Pre-EIP-150: Forward exact requested amount, OOG if insufficient
        gas_to_forward = gas_requested;
    }

    if (g_trace_calls) {
        fprintf(stderr, "    fwd_gas: requested=%lu forwarded=%lu stipend=%lu gas_left=%lu\n",
                gas_requested, gas_to_forward, stipend, evm->gas_left);
    }

    // Pre-EIP-150: OOG if requested gas exceeds available gas.
    // This check must happen BEFORE the graceful failure (balance/depth) checks
    // to match geth, where the gas table deducts gas before the opcode executes.
    // Post-EIP-150: gas_to_forward is capped at 63/64, so it always fits.
    if (evm->fork < FORK_TANGERINE_WHISTLE && gas_to_forward > evm->gas_left)
    {
        if (g_trace_calls) {
            fprintf(stderr, "    OOG: need %lu, have %lu\n",
                    gas_to_forward, evm->gas_left);
        }
        evm->gas_left = 0;
        *gas_forwarded = 0;
        return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Graceful Failure Checks
    //==========================================================================

    // Graceful failure: depth >= 1024 or insufficient balance.
    // The child would have received gas_to_forward + stipend but never executes,
    // so all of it (including stipend) is returned. Since we haven't deducted
    // gas_to_forward yet, we only credit the stipend.
    *graceful_failure = false;
    if (evm->msg.depth >= 1024 || !balance_sufficient)
    {
        evm->gas_left += stipend;
        *gas_forwarded = 0;
        *graceful_failure = true;
        return EVM_SUCCESS;
    }

    // Deduct the forwarded gas from caller (stipend is NOT deducted — it's a
    // free bonus to the child that "leaks" back to the caller via gas refund)
    if (!evm_use_gas(evm, gas_to_forward))
    {
        *gas_forwarded = 0;
        return EVM_OUT_OF_GAS;
    }

    // Callee receives forwarded gas + stipend (stipend is free bonus)
    *gas_forwarded = gas_to_forward + stipend;

    return EVM_SUCCESS;
}

//==============================================================================
// Call Opcodes
//==============================================================================

/**
 * CALL - Message call into an account
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 */
static evm_status_t op_call(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
        return EVM_INTERNAL_ERROR;

    // Pop 7 arguments: gas addr value argsOffset argsSize retOffset retSize
    uint256_t gas, addr, value, args_offset, args_size, ret_offset, ret_size;

    if (!evm_stack_pop(evm->stack, &gas) ||
        !evm_stack_pop(evm->stack, &addr) ||
        !evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &args_offset) ||
        !evm_stack_pop(evm->stack, &args_size) ||
        !evm_stack_pop(evm->stack, &ret_offset) ||
        !evm_stack_pop(evm->stack, &ret_size))
        return EVM_STACK_UNDERFLOW;

    address_t target_addr;
    address_from_uint256(&addr, &target_addr);

    // EIP-214: CALL with non-zero value in static context is forbidden
    if (evm->msg.is_static && !uint256_is_zero(&value))
    {
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Overflow check: impossibly large memory args/ret means OOG
    if (args_size.high != 0 || (uint64_t)(args_size.low >> 64) != 0 ||
        ret_size.high != 0 || (uint64_t)(ret_size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&args_size) &&
        (args_offset.high != 0 || (uint64_t)(args_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&ret_size) &&
        (ret_offset.high != 0 || (uint64_t)(ret_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    uint64_t gas_forwarded;
    bool graceful_failure;
    evm_status_t status = prepare_call(evm, &target_addr, &value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       true, true, &gas_forwarded,
                                       &graceful_failure);

    if (status != EVM_SUCCESS)
        return status;

    // If graceful failure (depth/balance check), push 0 and return
    if (graceful_failure)
    {
        // Clear return data — failed CALL produces empty return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;

        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // EIP-158: skip call to non-existing account with zero value.
    // Gas has already been charged by prepare_call. The account would be
    // created then immediately pruned by EIP-161, so skip the work.
    if (evm->fork >= FORK_SPURIOUS_DRAGON &&
        uint256_is_zero(&value) &&
        !is_precompile(&target_addr, evm->fork) &&
        !evm_state_exists(evm->state, &target_addr))
    {
        // Clear return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;

        // Return forwarded gas (minus stipend which is 0 for zero-value)
        evm->gas_left += gas_forwarded;

        uint256_t result = uint256_from_uint64(1);  // success
        if (!evm_stack_push(evm->stack, &result))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory (stack buffer for small inputs)
    uint8_t stack_buf[CALLDATA_STACK_SIZE];
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = (args_size_u64 <= CALLDATA_STACK_SIZE)
                    ? stack_buf : malloc(args_size_u64);
        if (!call_args)
            return EVM_INTERNAL_ERROR;
        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            if (call_args != stack_buf) free(call_args);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    // Create and execute subcall
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

    evm_result_t subcall_result;
    bool exec_ok = evm_execute(evm, &subcall_msg, &subcall_result);
    if (call_args && call_args != stack_buf) free(call_args);

    if (!exec_ok)
        return EVM_INTERNAL_ERROR;

    EVM_TRACE_RETURN(subcall_result.output_data, subcall_result.output_size,
                     gas_forwarded - subcall_result.gas_left, NULL);

    // Copy return data to memory
    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ?
                          ret_size_u64 : subcall_result.output_size;
        evm_memory_write(evm->memory, ret_offset_u64, subcall_result.output_data, copy_size);
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);

    if (g_trace_calls) {
        char caller_hex[41], tgt_hex[41];
        for (int i = 0; i < 20; i++) {
            sprintf(caller_hex + i*2, "%02x", evm->msg.recipient.bytes[i]);
            sprintf(tgt_hex + i*2, "%02x", target_addr.bytes[i]);
        }
        char *val_hex = uint256_to_hex(&value);
        fprintf(stderr, "  CALL depth=%d caller=%s target=%s value=%s status=%d gas_left=%lu\n",
                evm->msg.depth, caller_hex, tgt_hex, val_hex,
                subcall_result.status, subcall_result.gas_left);
    }

    // Refund unused gas on SUCCESS or REVERT (per Ethereum spec)
    if (subcall_result.status == EVM_SUCCESS || subcall_result.status == EVM_REVERT)
    {
        evm->gas_left += subcall_result.gas_left;
        if (subcall_result.status == EVM_SUCCESS)
            evm->gas_refund += subcall_result.gas_refund;
    }

    /* output_data ownership transferred to evm->return_data by evm_execute */

    uint256_t result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;
    if (!evm_stack_push(evm->stack, &result))
        return EVM_STACK_OVERFLOW;

    return EVM_SUCCESS;
}

/**
 * CALLCODE - Message call with alternative account's code
 * Stack: gas addr value argsOffset argsSize retOffset retSize => success
 * NOTE: Deprecated, use DELEGATECALL instead
 */
static evm_status_t op_callcode(evm_t *evm)
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

    // Overflow check: impossibly large memory args/ret means OOG
    if (args_size.high != 0 || (uint64_t)(args_size.low >> 64) != 0 ||
        ret_size.high != 0 || (uint64_t)(ret_size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&args_size) &&
        (args_offset.high != 0 || (uint64_t)(args_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&ret_size) &&
        (ret_offset.high != 0 || (uint64_t)(ret_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // Prepare call (memory expansion, gas calculation, checks)
    // CALLCODE allows value transfer but cannot create new accounts
    uint64_t gas_forwarded;
    bool graceful_failure;
    evm_status_t status = prepare_call(evm, &target_addr, &value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       true, false, &gas_forwarded,
                                       &graceful_failure);

    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If graceful failure (depth/balance check), push 0 and return
    if (graceful_failure)
    {
        // Clear return data — failed CALLCODE produces empty return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;

        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory (stack buffer for small inputs)
    uint8_t stack_buf[CALLDATA_STACK_SIZE];
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = (args_size_u64 <= CALLDATA_STACK_SIZE)
                    ? stack_buf : malloc(args_size_u64);
        if (!call_args)
        {
            fprintf(stderr, "FATAL: CALLCODE failed to allocate call arguments (OOM)\n");
            return EVM_INTERNAL_ERROR;
        }

        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            if (call_args != stack_buf) free(call_args);
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
    
    if (call_args && call_args != stack_buf) free(call_args);

    if (!exec_ok)
    {
        return EVM_INTERNAL_ERROR;
    }

    EVM_TRACE_RETURN(subcall_result.output_data, subcall_result.output_size,
                     gas_forwarded - subcall_result.gas_left, NULL);

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64,
                             subcall_result.output_data, copy_size))
        {
            /* output_data ownership transferred to evm->return_data by evm_execute */
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);

    if (g_trace_calls) {
        char caller_hex[41], tgt_hex[41];
        for (int i = 0; i < 20; i++) {
            sprintf(caller_hex + i*2, "%02x", evm->msg.recipient.bytes[i]);
            sprintf(tgt_hex + i*2, "%02x", target_addr.bytes[i]);
        }
        char *val_hex = uint256_to_hex(&value);
        fprintf(stderr, "  CALLCODE depth=%d caller=%s target=%s value=%s status=%d gas_left=%lu\n",
                evm->msg.depth, caller_hex, tgt_hex, val_hex,
                subcall_result.status, subcall_result.gas_left);
    }

    // Refund unused gas on SUCCESS or REVERT (per Ethereum spec)
    if (subcall_result.status == EVM_SUCCESS || subcall_result.status == EVM_REVERT)
    {
        evm->gas_left += subcall_result.gas_left;
        if (subcall_result.status == EVM_SUCCESS)
            evm->gas_refund += subcall_result.gas_refund;
    }

    /* output_data ownership transferred to evm->return_data by evm_execute */

    uint256_t result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;

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
static evm_status_t op_delegatecall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // EIP-7: DELEGATECALL introduced in Homestead
    if (evm->fork < FORK_HOMESTEAD)
        return EVM_INVALID_OPCODE;

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

    // Overflow check: impossibly large memory args/ret means OOG
    if (args_size.high != 0 || (uint64_t)(args_size.low >> 64) != 0 ||
        ret_size.high != 0 || (uint64_t)(ret_size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&args_size) &&
        (args_offset.high != 0 || (uint64_t)(args_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&ret_size) &&
        (ret_offset.high != 0 || (uint64_t)(ret_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // DELEGATECALL does not transfer value
    uint256_t zero_value = UINT256_ZERO;

    // Prepare call (memory expansion, gas calculation, checks)
    // DELEGATECALL does NOT allow value transfer and cannot create accounts
    uint64_t gas_forwarded;
    bool graceful_failure;
    evm_status_t status = prepare_call(evm, &target_addr, &zero_value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       false, false, &gas_forwarded,
                                       &graceful_failure);

    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If graceful failure (depth check), push 0 and return
    if (graceful_failure)
    {
        // Clear return data — failed call produces empty return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;

        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory (stack buffer for small inputs)
    uint8_t stack_buf[CALLDATA_STACK_SIZE];
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = (args_size_u64 <= CALLDATA_STACK_SIZE)
                    ? stack_buf : malloc(args_size_u64);
        if (!call_args)
        {
            fprintf(stderr, "FATAL: DELEGATECALL failed to allocate call arguments (OOM)\n");
            return EVM_INTERNAL_ERROR;
        }

        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            if (call_args != stack_buf) free(call_args);
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
    
    if (call_args && call_args != stack_buf) free(call_args);

    if (!exec_ok)
    {
        return EVM_INTERNAL_ERROR;
    }

    EVM_TRACE_RETURN(subcall_result.output_data, subcall_result.output_size,
                     gas_forwarded - subcall_result.gas_left, NULL);

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64,
                             subcall_result.output_data, copy_size))
        {
            /* output_data ownership transferred to evm->return_data by evm_execute */
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);

    // Refund unused gas on SUCCESS or REVERT (per Ethereum spec)
    if (subcall_result.status == EVM_SUCCESS || subcall_result.status == EVM_REVERT)
    {
        evm->gas_left += subcall_result.gas_left;
        if (subcall_result.status == EVM_SUCCESS)
            evm->gas_refund += subcall_result.gas_refund;
    }

    /* output_data ownership transferred to evm->return_data by evm_execute */

    uint256_t result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;

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
static evm_status_t op_staticcall(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory)
    {
        return EVM_INTERNAL_ERROR;
    }

    // EIP-214: STATICCALL introduced in Byzantium
    if (evm->fork < FORK_BYZANTIUM)
        return EVM_INVALID_OPCODE;

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

    // Overflow check: impossibly large memory args/ret means OOG
    if (args_size.high != 0 || (uint64_t)(args_size.low >> 64) != 0 ||
        ret_size.high != 0 || (uint64_t)(ret_size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&args_size) &&
        (args_offset.high != 0 || (uint64_t)(args_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&ret_size) &&
        (ret_offset.high != 0 || (uint64_t)(ret_offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t args_size_u64 = uint256_to_uint64(&args_size);
    uint64_t args_offset_u64 = uint256_to_uint64(&args_offset);
    uint64_t ret_size_u64 = uint256_to_uint64(&ret_size);
    uint64_t ret_offset_u64 = uint256_to_uint64(&ret_offset);

    // STATICCALL does not transfer value
    uint256_t zero_value = UINT256_ZERO;

    // STATICCALL does NOT allow value transfer and cannot create accounts
    uint64_t gas_forwarded;
    bool graceful_failure;
    evm_status_t status = prepare_call(evm, &target_addr, &zero_value, &gas,
                                       args_offset_u64, args_size_u64,
                                       ret_offset_u64, ret_size_u64,
                                       false, false, &gas_forwarded,
                                       &graceful_failure);

    if (status != EVM_SUCCESS)
    {
        return status;
    }

    // If graceful failure (depth check), push 0 and return
    if (graceful_failure)
    {
        // Clear return data — failed call produces empty return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;

        uint256_t result = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &result))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Extract call arguments from memory (stack buffer for small inputs)
    uint8_t stack_buf[CALLDATA_STACK_SIZE];
    uint8_t *call_args = NULL;
    if (args_size_u64 > 0)
    {
        call_args = (args_size_u64 <= CALLDATA_STACK_SIZE)
                    ? stack_buf : malloc(args_size_u64);
        if (!call_args)
        {
            fprintf(stderr, "FATAL: STATICCALL failed to allocate call arguments (OOM)\n");
            return EVM_INTERNAL_ERROR;
        }

        if (!evm_memory_read(evm->memory, args_offset_u64, call_args, args_size_u64))
        {
            if (call_args != stack_buf) free(call_args);
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
    
    if (call_args && call_args != stack_buf) free(call_args);

    if (!exec_ok)
    {
        return EVM_INTERNAL_ERROR;
    }

    EVM_TRACE_RETURN(subcall_result.output_data, subcall_result.output_size,
                     gas_forwarded - subcall_result.gas_left, NULL);

    if (ret_size_u64 > 0 && subcall_result.output_size > 0)
    {
        size_t copy_size = ret_size_u64 < subcall_result.output_size ? 
                          ret_size_u64 : subcall_result.output_size;
        
        if (!evm_memory_write(evm->memory, ret_offset_u64,
                             subcall_result.output_data, copy_size))
        {
            /* output_data ownership transferred to evm->return_data by evm_execute */
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    bool call_succeeded = (subcall_result.status == EVM_SUCCESS);

    // Refund unused gas on SUCCESS or REVERT (per Ethereum spec)
    if (subcall_result.status == EVM_SUCCESS || subcall_result.status == EVM_REVERT)
    {
        evm->gas_left += subcall_result.gas_left;
        if (subcall_result.status == EVM_SUCCESS)
            evm->gas_refund += subcall_result.gas_refund;
    }

    /* output_data ownership transferred to evm->return_data by evm_execute */

    uint256_t result = call_succeeded ? uint256_from_uint64(1) : UINT256_ZERO;

    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
