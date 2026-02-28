/**
 * EVM Stack - 256-bit Stack Implementation
 *
 * The EVM uses a 256-bit word-based stack with a maximum depth of 1024.
 * This module provides push/pop operations and stack manipulation opcodes
 * (DUP1-DUP16, SWAP1-SWAP16).
 *
 * Stack properties:
 * - Word size: 256 bits (32 bytes)
 * - Maximum depth: 1024 items
 * - Access: LIFO (Last In, First Out)
 * - Operations: PUSH, POP, DUP1-16, SWAP1-16
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

/**
 * EVM stack structure
 */
typedef struct evm_stack_t
{
    uint256_t *items; // Stack items (dynamically allocated)
    size_t size;      // Current number of items on stack
    size_t capacity;  // Allocated capacity
} evm_stack_t;

//==============================================================================
// Lifecycle
//==============================================================================

/**
 * Create a new stack
 *
 * @return Stack instance, or NULL on failure
 */
evm_stack_t *evm_stack_create(void);

/**
 * Destroy stack and free resources
 *
 * @param stack Stack to destroy
 */
void evm_stack_destroy(evm_stack_t *stack);

/**
 * Reset stack to empty state
 *
 * @param stack Stack to reset
 */
void evm_stack_reset(evm_stack_t *stack);

/**
 * Clear stack (alias for reset)
 * Sets size to 0 without deallocating
 *
 * @param stack Stack to clear
 */
void evm_stack_clear(evm_stack_t *stack);

//==============================================================================
// Basic Operations
//==============================================================================

/**
 * Push a value onto the stack
 *
 * @param stack Stack instance
 * @param value Value to push
 * @return true on success, false on stack overflow
 */
bool evm_stack_push(evm_stack_t *stack, const uint256_t *value);

/**
 * Pop a value from the stack
 *
 * @param stack Stack instance
 * @param value Output parameter for popped value
 * @return true on success, false on stack underflow
 */
bool evm_stack_pop(evm_stack_t *stack, uint256_t *value);

/**
 * Peek at the top stack item without removing it
 *
 * @param stack Stack instance
 * @param value Output parameter for top value
 * @return true on success, false if stack is empty
 */
bool evm_stack_peek(const evm_stack_t *stack, uint256_t *value);

/**
 * Get stack item at index from top (0 = top, 1 = second, etc.)
 *
 * @param stack Stack instance
 * @param index Index from top
 * @param value Output parameter for value
 * @return true on success, false if index out of bounds
 */
bool evm_stack_get(const evm_stack_t *stack, size_t index, uint256_t *value);

/**
 * Set stack item at index from top (0 = top, 1 = second, etc.)
 *
 * @param stack Stack instance
 * @param index Index from top
 * @param value Value to set
 * @return true on success, false if index out of bounds
 */
bool evm_stack_set(evm_stack_t *stack, size_t index, const uint256_t *value);

//==============================================================================
// Stack Manipulation (DUP, SWAP)
//==============================================================================

/**
 * Duplicate the nth stack item to the top (DUP1-DUP16)
 * DUP1: duplicate 1st item (top)
 * DUP2: duplicate 2nd item
 * ...
 * DUP16: duplicate 16th item
 *
 * @param stack Stack instance
 * @param n Item position to duplicate (1-16)
 * @return true on success, false on error (invalid n, stack overflow, or underflow)
 */
bool evm_stack_dup(evm_stack_t *stack, uint8_t n);

/**
 * Swap the top stack item with the nth item (SWAP1-SWAP16)
 * SWAP1: swap top with 2nd item
 * SWAP2: swap top with 3rd item
 * ...
 * SWAP16: swap top with 17th item
 *
 * @param stack Stack instance
 * @param n Position to swap with (1-16)
 * @return true on success, false on error (invalid n or stack underflow)
 */
bool evm_stack_swap(evm_stack_t *stack, uint8_t n);

//==============================================================================
// Stack State
//==============================================================================

/**
 * Get current stack size
 *
 * @param stack Stack instance
 * @return Number of items on stack
 */
size_t evm_stack_size(const evm_stack_t *stack);

/**
 * Check if stack is empty
 *
 * @param stack Stack instance
 * @return true if stack is empty
 */
bool evm_stack_is_empty(const evm_stack_t *stack);

/**
 * Check if stack is full
 *
 * @param stack Stack instance
 * @return true if stack is at maximum depth
 */
bool evm_stack_is_full(const evm_stack_t *stack);

/**
 * Get available stack space
 *
 * @param stack Stack instance
 * @return Number of items that can be pushed before overflow
 */
size_t evm_stack_available(const evm_stack_t *stack);

//==============================================================================
// Validation Helpers
//==============================================================================

/**
 * Check if stack has at least n items (for underflow check)
 *
 * @param stack Stack instance
 * @param n Required number of items
 * @return true if stack has at least n items
 */
bool evm_stack_require(const evm_stack_t *stack, size_t n);

/**
 * Check if stack can fit n more items (for overflow check)
 *
 * @param stack Stack instance
 * @param n Number of items to push
 * @return true if stack has space for n more items
 */
bool evm_stack_ensure_capacity(const evm_stack_t *stack, size_t n);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_STACK_H */
