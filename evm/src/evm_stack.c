/**
 * EVM Stack Implementation
 */

#include "evm_stack.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY 32 // Start with 32 items

//==============================================================================
// Lifecycle
//==============================================================================

evm_stack_t *evm_stack_create(void)
{
    evm_stack_t *stack = (evm_stack_t *)calloc(1, sizeof(evm_stack_t));
    if (!stack)
    {
        LOG_ERROR("Failed to allocate stack");
        return NULL;
    }

    stack->capacity = INITIAL_CAPACITY;
    stack->items = (uint256_t *)calloc(stack->capacity, sizeof(uint256_t));
    if (!stack->items)
    {
        LOG_ERROR("Failed to allocate stack items");
        free(stack);
        return NULL;
    }

    stack->size = 0;

    LOG_DEBUG("Created EVM stack with initial capacity %zu", stack->capacity);
    return stack;
}

void evm_stack_destroy(evm_stack_t *stack)
{
    if (!stack)
        return;

    if (stack->items)
    {
        free(stack->items);
    }

    free(stack);
    LOG_DEBUG("Destroyed EVM stack");
}

void evm_stack_reset(evm_stack_t *stack)
{
    if (!stack)
        return;

    stack->size = 0;
    // Zero out the memory for security
    if (stack->items)
    {
        memset(stack->items, 0, stack->capacity * sizeof(uint256_t));
    }
}

void evm_stack_clear(evm_stack_t *stack)
{
    // Just an alias for reset
    evm_stack_reset(stack);
}

//==============================================================================
// Basic Operations
//==============================================================================

bool evm_stack_push(evm_stack_t *stack, const uint256_t *value)
{
    if (!stack || !value)
    {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    // Check for overflow
    if (stack->size >= EVM_STACK_MAX_DEPTH)
    {
        LOG_DEBUG("Stack overflow: size=%zu, max=%d", stack->size, EVM_STACK_MAX_DEPTH);
        return false;
    }

    // Expand capacity if needed
    if (stack->size >= stack->capacity)
    {
        size_t new_capacity = stack->capacity * 2;
        if (new_capacity > EVM_STACK_MAX_DEPTH)
        {
            new_capacity = EVM_STACK_MAX_DEPTH;
        }

        uint256_t *new_items = (uint256_t *)realloc(stack->items, new_capacity * sizeof(uint256_t));
        if (!new_items)
        {
            LOG_ERROR("Failed to expand stack capacity");
            return false;
        }

        stack->items = new_items;
        stack->capacity = new_capacity;
        LOG_DEBUG("Expanded stack capacity to %zu", new_capacity);
    }

    // Push value
    stack->items[stack->size] = *value;
    stack->size++;

    return true;
}

bool evm_stack_pop(evm_stack_t *stack, uint256_t *value)
{
    if (!stack || !value)
    {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    // Check for underflow
    if (stack->size == 0)
    {
        LOG_DEBUG("Stack underflow");
        return false;
    }

    // Pop value
    stack->size--;
    *value = stack->items[stack->size];

    // Zero out the popped item for security
    memset(&stack->items[stack->size], 0, sizeof(uint256_t));

    return true;
}

bool evm_stack_peek(const evm_stack_t *stack, uint256_t *value)
{
    if (!stack || !value)
    {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    if (stack->size == 0)
    {
        LOG_DEBUG("Cannot peek empty stack");
        return false;
    }

    *value = stack->items[stack->size - 1];
    return true;
}

bool evm_stack_get(const evm_stack_t *stack, size_t index, uint256_t *value)
{
    if (!stack || !value)
    {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    if (index >= stack->size)
    {
        LOG_DEBUG("Stack index out of bounds: index=%zu, size=%zu", index, stack->size);
        return false;
    }

    // Index 0 is top, so actual array index is size-1-index
    *value = stack->items[stack->size - 1 - index];
    return true;
}

bool evm_stack_set(evm_stack_t *stack, size_t index, const uint256_t *value)
{
    if (!stack || !value)
    {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    if (index >= stack->size)
    {
        LOG_DEBUG("Stack index out of bounds: index=%zu, size=%zu", index, stack->size);
        return false;
    }

    // Index 0 is top, so actual array index is size-1-index
    stack->items[stack->size - 1 - index] = *value;
    return true;
}

//==============================================================================
// Stack Manipulation (DUP, SWAP)
//==============================================================================

bool evm_stack_dup(evm_stack_t *stack, uint8_t n)
{
    if (!stack)
    {
        LOG_ERROR("Invalid stack");
        return false;
    }

    // DUP1-DUP16: n must be 1-16
    if (n < 1 || n > 16)
    {
        LOG_ERROR("Invalid DUP parameter: n=%u (must be 1-16)", n);
        return false;
    }

    // Check if we have enough items (need n items on stack)
    if (stack->size < n)
    {
        LOG_DEBUG("Stack underflow for DUP%u: size=%zu", n, stack->size);
        return false;
    }

    // Check if we can push one more item
    if (stack->size >= EVM_STACK_MAX_DEPTH)
    {
        LOG_DEBUG("Stack overflow for DUP%u", n);
        return false;
    }

    // Get the nth item from top (0-indexed: n-1)
    uint256_t value = stack->items[stack->size - n];

    // Push it to the top
    return evm_stack_push(stack, &value);
}

bool evm_stack_swap(evm_stack_t *stack, uint8_t n)
{
    if (!stack)
    {
        LOG_ERROR("Invalid stack");
        return false;
    }

    // SWAP1-SWAP16: n must be 1-16
    if (n < 1 || n > 16)
    {
        LOG_ERROR("Invalid SWAP parameter: n=%u (must be 1-16)", n);
        return false;
    }

    // Check if we have enough items (need n+1 items: top and nth)
    if (stack->size < n + 1)
    {
        LOG_DEBUG("Stack underflow for SWAP%u: size=%zu, need=%u", n, stack->size, n + 1);
        return false;
    }

    // Swap top with nth item
    // top is at index size-1
    // nth item is at index size-1-n
    size_t top_idx = stack->size - 1;
    size_t swap_idx = stack->size - 1 - n;

    uint256_t temp = stack->items[top_idx];
    stack->items[top_idx] = stack->items[swap_idx];
    stack->items[swap_idx] = temp;

    return true;
}

//==============================================================================
// Stack State
//==============================================================================

size_t evm_stack_size(const evm_stack_t *stack)
{
    return stack ? stack->size : 0;
}

bool evm_stack_is_empty(const evm_stack_t *stack)
{
    return stack ? (stack->size == 0) : true;
}

bool evm_stack_is_full(const evm_stack_t *stack)
{
    return stack ? (stack->size >= EVM_STACK_MAX_DEPTH) : false;
}

size_t evm_stack_available(const evm_stack_t *stack)
{
    if (!stack)
        return 0;

    return EVM_STACK_MAX_DEPTH - stack->size;
}

//==============================================================================
// Validation Helpers
//==============================================================================

bool evm_stack_require(const evm_stack_t *stack, size_t n)
{
    return stack && (stack->size >= n);
}

bool evm_stack_ensure_capacity(const evm_stack_t *stack, size_t n)
{
    if (!stack)
        return false;

    return (stack->size + n) <= EVM_STACK_MAX_DEPTH;
}
