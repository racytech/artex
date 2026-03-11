/**
 * EVM Stack Implementation — Lifecycle only.
 * Hot-path operations (push/pop/peek/dup/swap) are static inline in evm_stack.h.
 */

#include "evm_stack.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Lifecycle
//==============================================================================

evm_stack_t *evm_stack_create(void)
{
    evm_stack_t *stack = (evm_stack_t *)calloc(1, sizeof(evm_stack_t));
    if (!stack)
        return NULL;

    stack->capacity = EVM_STACK_MAX_DEPTH;
    stack->items = (uint256_t *)calloc(EVM_STACK_MAX_DEPTH, sizeof(uint256_t));
    if (!stack->items)
    {
        free(stack);
        return NULL;
    }

    stack->size = 0;
    return stack;
}

void evm_stack_destroy(evm_stack_t *stack)
{
    if (!stack)
        return;
    free(stack->items);
    free(stack);
}

void evm_stack_reset(evm_stack_t *stack)
{
    if (!stack)
        return;
    stack->size = 0;
}

void evm_stack_clear(evm_stack_t *stack)
{
    evm_stack_reset(stack);
}
