/**
 * VM — EOF-Native Virtual Machine Implementation
 *
 * Lifecycle, context setup, gas operations, and result helpers.
 * Interpreter loop is Phase 2.
 */

#include "vm.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Lifecycle
//==============================================================================

vm_t *vm_create(void)
{
    vm_t *vm = (vm_t *)calloc(1, sizeof(vm_t));
    if (!vm) return NULL;

    // Allocate return stack (fixed size)
    vm->return_stack = (vm_return_frame_t *)calloc(VM_MAX_RETURN_DEPTH,
                                                    sizeof(vm_return_frame_t));
    if (!vm->return_stack) {
        free(vm);
        return NULL;
    }

    // Create memory
    vm->memory = vm_memory_create();
    if (!vm->memory) {
        free(vm->return_stack);
        free(vm);
        return NULL;
    }

    return vm;
}

void vm_destroy(vm_t *vm)
{
    if (!vm) return;

    free(vm->stack);
    free(vm->return_stack);
    free(vm->return_data);
    vm_memory_destroy(vm->memory);
    free(vm);
}

void vm_reset(vm_t *vm)
{
    if (!vm) return;

    // Free operand stack (re-allocated per execute from EOF max_stack_height)
    free(vm->stack);
    vm->stack = NULL;
    vm->sp = 0;

    // Reset return stack
    vm->rsp = 0;

    // Reset memory
    vm_memory_reset(vm->memory);

    // Free return data
    free(vm->return_data);
    vm->return_data = NULL;
    vm->return_data_size = 0;

    // Reset execution state
    vm->pc = 0;
    vm->gas_left = 0;
    vm->gas_refund = 0;
    vm->stopped = false;
    vm->status = VM_SUCCESS;
    vm->container = NULL;
    vm->current_func = 0;
}

//==============================================================================
// Context Setup
//==============================================================================

void vm_set_block_env(vm_t *vm, const vm_block_env_t *block)
{
    if (!vm || !block) return;
    vm->block = *block;
}

void vm_set_tx_context(vm_t *vm, const vm_tx_context_t *tx)
{
    if (!vm || !tx) return;
    vm->tx = *tx;
}

void vm_set_host(vm_t *vm, const vm_host_iface_t *host, void *ctx)
{
    if (!vm) return;
    vm->host = host;
    vm->host_ctx = ctx;
}

//==============================================================================
// Execution
//==============================================================================

bool vm_execute(vm_t *vm, eof_container_t *container,
                const vm_message_t *msg, vm_result_t *result)
{
    if (!vm || !container || !msg || !result)
        return false;

    if (container->num_functions == 0)
        return false;

    // Reset runtime state
    vm_reset(vm);

    // Set up execution context
    vm->container = container;
    vm->msg = *msg;
    vm->gas_left = msg->gas;
    vm->current_func = 0;

    // Allocate operand stack (full depth — EOF validation guarantees no overflow)
    vm->stack = (uint256_t *)calloc(VM_MAX_STACK_DEPTH, sizeof(uint256_t));
    if (!vm->stack) {
        *result = vm_result_error(VM_INTERNAL_ERROR, 0);
        return false;
    }

    // Set PC to start of function 0's code
    vm->pc = 0;
    vm->sp = 0;
    vm->rsp = 0;

    // Run the interpreter
    vm_status_t status = vm_interpret(vm);

    // Build result from VM state
    switch (status) {
    case VM_SUCCESS:
        *result = vm_result_success(vm->gas_left, vm->return_data, vm->return_data_size);
        break;
    case VM_REVERT:
        *result = vm_result_revert(vm->gas_left, vm->return_data, vm->return_data_size);
        break;
    default:
        *result = vm_result_error(status, vm->gas_left);
        break;
    }
    return true;
}

//==============================================================================
// Gas Operations
//==============================================================================

bool vm_use_gas(vm_t *vm, uint64_t amount)
{
    if (!vm) return false;

    if (vm->gas_left < amount) {
        vm->status = VM_OUT_OF_GAS;
        vm->gas_left = 0;
        return false;
    }

    vm->gas_left -= amount;
    return true;
}

void vm_refund_gas(vm_t *vm, uint64_t amount)
{
    if (!vm) return;
    vm->gas_refund += amount;
}

uint64_t vm_get_gas_left(const vm_t *vm)
{
    return vm ? vm->gas_left : 0;
}

//==============================================================================
// Result Helpers
//==============================================================================

vm_result_t vm_result_success(uint64_t gas_left, const uint8_t *output, size_t size)
{
    vm_result_t r = {
        .status = VM_SUCCESS,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = size,
    };

    if (output && size > 0) {
        r.output_data = (uint8_t *)malloc(size);
        if (r.output_data)
            memcpy(r.output_data, output, size);
    }

    return r;
}

vm_result_t vm_result_revert(uint64_t gas_left, const uint8_t *output, size_t size)
{
    vm_result_t r = {
        .status = VM_REVERT,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = size,
    };

    if (output && size > 0) {
        r.output_data = (uint8_t *)malloc(size);
        if (r.output_data)
            memcpy(r.output_data, output, size);
    }

    return r;
}

vm_result_t vm_result_error(vm_status_t status, uint64_t gas_left)
{
    vm_result_t r = {
        .status = status,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = 0,
    };
    return r;
}

void vm_result_free(vm_result_t *result)
{
    if (!result) return;
    free(result->output_data);
    result->output_data = NULL;
    result->output_size = 0;
}
