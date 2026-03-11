/**
 * EVM Stack - 256-bit Stack Implementation
 *
 * The EVM uses a 256-bit word-based stack with a maximum depth of 1024.
 * Hot-path operations (push/pop/peek/dup/swap) are static inline for
 * zero function-call overhead in the interpreter loop.
 *
 * Stack properties:
 * - Word size: 256 bits (32 bytes)
 * - Maximum depth: 1024 items
 * - Pre-allocated to max depth (32KB, fits in L1 cache)
 * - Access: LIFO (Last In, First Out)
 */

#ifndef ART_EVM_STACK_H
#define ART_EVM_STACK_H

#include "uint256.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Constants
//==============================================================================

#define EVM_STACK_MAX_DEPTH 1024 // Maximum stack depth

//==============================================================================
// Stack Structure
//==============================================================================

typedef struct evm_stack_t
{
    uint256_t *items; // Stack items (pre-allocated to EVM_STACK_MAX_DEPTH)
    size_t size;      // Current number of items on stack
    size_t capacity;  // Allocated capacity (always EVM_STACK_MAX_DEPTH)
} evm_stack_t;

//==============================================================================
// Lifecycle (defined in evm_stack.c)
//==============================================================================

evm_stack_t *evm_stack_create(void);
void evm_stack_destroy(evm_stack_t *stack);
void evm_stack_reset(evm_stack_t *stack);
void evm_stack_clear(evm_stack_t *stack);

//==============================================================================
// Inline Hot-Path Operations
//==============================================================================

static inline bool evm_stack_push(evm_stack_t *stack, const uint256_t *value) {
    if (__builtin_expect(stack->size >= EVM_STACK_MAX_DEPTH, 0))
        return false;
    stack->items[stack->size++] = *value;
    return true;
}

static inline bool evm_stack_pop(evm_stack_t *stack, uint256_t *value) {
    if (__builtin_expect(stack->size == 0, 0))
        return false;
    *value = stack->items[--stack->size];
    return true;
}

static inline bool evm_stack_peek(const evm_stack_t *stack, uint256_t *value) {
    if (__builtin_expect(stack->size == 0, 0))
        return false;
    *value = stack->items[stack->size - 1];
    return true;
}

static inline bool evm_stack_get(const evm_stack_t *stack, size_t index,
                                  uint256_t *value) {
    if (__builtin_expect(index >= stack->size, 0))
        return false;
    *value = stack->items[stack->size - 1 - index];
    return true;
}

static inline bool evm_stack_set(evm_stack_t *stack, size_t index,
                                  const uint256_t *value) {
    if (__builtin_expect(index >= stack->size, 0))
        return false;
    stack->items[stack->size - 1 - index] = *value;
    return true;
}

static inline bool evm_stack_dup(evm_stack_t *stack, uint8_t n) {
    if (__builtin_expect(stack->size < n || stack->size >= EVM_STACK_MAX_DEPTH, 0))
        return false;
    stack->items[stack->size] = stack->items[stack->size - n];
    stack->size++;
    return true;
}

static inline bool evm_stack_swap(evm_stack_t *stack, uint8_t n) {
    if (__builtin_expect(stack->size < (size_t)(n + 1), 0))
        return false;
    size_t top_idx = stack->size - 1;
    size_t swap_idx = stack->size - 1 - n;
    uint256_t temp = stack->items[top_idx];
    stack->items[top_idx] = stack->items[swap_idx];
    stack->items[swap_idx] = temp;
    return true;
}

static inline size_t evm_stack_size(const evm_stack_t *stack) {
    return stack->size;
}

static inline bool evm_stack_is_empty(const evm_stack_t *stack) {
    return stack->size == 0;
}

static inline bool evm_stack_is_full(const evm_stack_t *stack) {
    return stack->size >= EVM_STACK_MAX_DEPTH;
}

static inline size_t evm_stack_available(const evm_stack_t *stack) {
    return EVM_STACK_MAX_DEPTH - stack->size;
}

static inline bool evm_stack_require(const evm_stack_t *stack, size_t n) {
    return stack->size >= n;
}

static inline bool evm_stack_ensure_capacity(const evm_stack_t *stack, size_t n) {
    return (stack->size + n) <= EVM_STACK_MAX_DEPTH;
}

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_STACK_H */
