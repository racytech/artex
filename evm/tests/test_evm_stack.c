/**
 * EVM Stack Tests
 */

#include "evm_stack.h"
#include "uint256.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

// Test helper
static void print_test(const char *name)
{
    printf("  [TEST] %s\n", name);
}

// Test: Create and destroy stack
void test_stack_create_destroy(void)
{
    print_test("stack_create_destroy");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);
    assert(evm_stack_size(stack) == 0);
    assert(evm_stack_is_empty(stack));
    assert(!evm_stack_is_full(stack));
    assert(evm_stack_available(stack) == EVM_STACK_MAX_DEPTH);

    evm_stack_destroy(stack);
}

// Test: Push and pop operations
void test_stack_push_pop(void)
{
    print_test("stack_push_pop");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push some values
    uint256_t val1, val2, val3;
    val1 = uint256_from_uint64(100);
    val2 = uint256_from_uint64(200);
    val3 = uint256_from_uint64(300);

    assert(evm_stack_push(stack, &val1));
    assert(evm_stack_size(stack) == 1);
    assert(!evm_stack_is_empty(stack));

    assert(evm_stack_push(stack, &val2));
    assert(evm_stack_size(stack) == 2);

    assert(evm_stack_push(stack, &val3));
    assert(evm_stack_size(stack) == 3);

    // Pop values (LIFO order)
    uint256_t popped;
    assert(evm_stack_pop(stack, &popped));
    assert(uint256_eq(&popped, &val3));
    assert(evm_stack_size(stack) == 2);

    assert(evm_stack_pop(stack, &popped));
    assert(uint256_eq(&popped, &val2));
    assert(evm_stack_size(stack) == 1);

    assert(evm_stack_pop(stack, &popped));
    assert(uint256_eq(&popped, &val1));
    assert(evm_stack_size(stack) == 0);
    assert(evm_stack_is_empty(stack));

    // Pop from empty stack should fail
    assert(!evm_stack_pop(stack, &popped));

    evm_stack_destroy(stack);
}

// Test: Peek operation
void test_stack_peek(void)
{
    print_test("stack_peek");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    uint256_t val;
    // Peek empty stack should fail
    assert(!evm_stack_peek(stack, &val));

    uint256_t val1, val2;
    val1 = uint256_from_uint64(42);
    val2 = uint256_from_uint64(99);

    assert(evm_stack_push(stack, &val1));
    assert(evm_stack_peek(stack, &val));
    assert(uint256_eq(&val, &val1));
    assert(evm_stack_size(stack) == 1); // Peek doesn't remove

    assert(evm_stack_push(stack, &val2));
    assert(evm_stack_peek(stack, &val));
    assert(uint256_eq(&val, &val2)); // Peek shows top item
    assert(evm_stack_size(stack) == 2);

    evm_stack_destroy(stack);
}

// Test: Get and set operations
void test_stack_get_set(void)
{
    print_test("stack_get_set");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push 5 values
    uint256_t values[5];
    for (int i = 0; i < 5; i++)
    {
        values[i] = uint256_from_uint64(i * 10);
        assert(evm_stack_push(stack, &values[i]));
    }

    // Get values (index 0 = top)
    uint256_t val;
    assert(evm_stack_get(stack, 0, &val)); // top = values[4]
    assert(uint256_eq(&val, &values[4]));

    assert(evm_stack_get(stack, 1, &val)); // values[3]
    assert(uint256_eq(&val, &values[3]));

    assert(evm_stack_get(stack, 4, &val)); // bottom = values[0]
    assert(uint256_eq(&val, &values[0]));

    // Out of bounds
    assert(!evm_stack_get(stack, 5, &val));

    // Set value
    uint256_t new_val;
    new_val = uint256_from_uint64(999);
    assert(evm_stack_set(stack, 2, &new_val));

    assert(evm_stack_get(stack, 2, &val));
    assert(uint256_eq(&val, &new_val));

    evm_stack_destroy(stack);
}

// Test: DUP operations (DUP1-DUP16)
void test_stack_dup(void)
{
    print_test("stack_dup");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push some values
    uint256_t values[16];
    for (int i = 0; i < 16; i++)
    {
        values[i] = uint256_from_uint64(i + 1);
        assert(evm_stack_push(stack, &values[i]));
    }

    // DUP1 - duplicate top (16)
    size_t initial_size = evm_stack_size(stack);
    assert(evm_stack_dup(stack, 1));
    assert(evm_stack_size(stack) == initial_size + 1);

    uint256_t val;
    assert(evm_stack_peek(stack, &val));
    assert(uint256_eq(&val, &values[15])); // Duplicated top

    evm_stack_pop(stack, &val); // Remove duplicate

    // DUP16 - duplicate 16th from top (value 1)
    assert(evm_stack_dup(stack, 16));
    assert(evm_stack_peek(stack, &val));
    assert(uint256_eq(&val, &values[0])); // Duplicated 16th item

    // DUP with insufficient stack depth should fail
    evm_stack_reset(stack);
    val = uint256_from_uint64(1);
    assert(evm_stack_push(stack, &val));
    assert(!evm_stack_dup(stack, 2)); // Need 2 items, have 1

    // Invalid DUP parameter
    assert(!evm_stack_dup(stack, 0));
    assert(!evm_stack_dup(stack, 17));

    evm_stack_destroy(stack);
}

// Test: SWAP operations (SWAP1-SWAP16)
void test_stack_swap(void)
{
    print_test("stack_swap");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push values: 1, 2, 3, 4, 5 (5 is on top)
    uint256_t values[5];
    for (int i = 0; i < 5; i++)
    {
        values[i] = uint256_from_uint64(i + 1);
        assert(evm_stack_push(stack, &values[i]));
    }

    // SWAP1 - swap top with 2nd
    // Before: [1, 2, 3, 4, 5]  (5 on top)
    // After:  [1, 2, 3, 5, 4]  (4 on top)
    assert(evm_stack_swap(stack, 1));

    uint256_t val;
    assert(evm_stack_get(stack, 0, &val));
    assert(uint256_eq(&val, &values[3])); // Top is now 4

    assert(evm_stack_get(stack, 1, &val));
    assert(uint256_eq(&val, &values[4])); // 2nd is now 5

    // SWAP back
    assert(evm_stack_swap(stack, 1));

    // SWAP4 - swap top with 5th
    // Before: [1, 2, 3, 4, 5]  (5 on top)
    // After:  [5, 2, 3, 4, 1]  (1 on top)
    assert(evm_stack_swap(stack, 4));

    assert(evm_stack_get(stack, 0, &val));
    assert(uint256_eq(&val, &values[0])); // Top is now 1

    assert(evm_stack_get(stack, 4, &val));
    assert(uint256_eq(&val, &values[4])); // Bottom is now 5

    // SWAP with insufficient stack depth should fail
    evm_stack_reset(stack);
    val = uint256_from_uint64(1);
    assert(evm_stack_push(stack, &val));
    assert(!evm_stack_swap(stack, 1)); // Need 2 items, have 1

    // Invalid SWAP parameter
    assert(!evm_stack_swap(stack, 0));
    assert(!evm_stack_swap(stack, 17));

    evm_stack_destroy(stack);
}

// Test: Stack overflow
void test_stack_overflow(void)
{
    print_test("stack_overflow");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Fill stack to maximum
    uint256_t val;
    val = uint256_from_uint64(42);

    for (size_t i = 0; i < EVM_STACK_MAX_DEPTH; i++)
    {
        assert(evm_stack_push(stack, &val));
    }

    assert(evm_stack_is_full(stack));
    assert(evm_stack_available(stack) == 0);

    // Next push should fail
    assert(!evm_stack_push(stack, &val));

    // DUP should also fail (would exceed max depth)
    assert(!evm_stack_dup(stack, 1));

    evm_stack_destroy(stack);
}

// Test: Stack reset
void test_stack_reset(void)
{
    print_test("stack_reset");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push some values
    uint256_t val;
    for (int i = 0; i < 10; i++)
    {
        val = uint256_from_uint64(i);
        assert(evm_stack_push(stack, &val));
    }

    assert(evm_stack_size(stack) == 10);

    // Reset
    evm_stack_reset(stack);

    assert(evm_stack_size(stack) == 0);
    assert(evm_stack_is_empty(stack));
    assert(evm_stack_available(stack) == EVM_STACK_MAX_DEPTH);

    // Can push again
    val = uint256_from_uint64(999);
    assert(evm_stack_push(stack, &val));
    assert(evm_stack_size(stack) == 1);

    evm_stack_destroy(stack);
}

// Test: Validation helpers
void test_stack_validation(void)
{
    print_test("stack_validation");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Empty stack
    assert(!evm_stack_require(stack, 1));
    assert(evm_stack_ensure_capacity(stack, EVM_STACK_MAX_DEPTH));

    // Push 5 items
    uint256_t val;
    for (int i = 0; i < 5; i++)
    {
        val = uint256_from_uint64(i);
        assert(evm_stack_push(stack, &val));
    }

    assert(evm_stack_require(stack, 1));
    assert(evm_stack_require(stack, 5));
    assert(!evm_stack_require(stack, 6));

    assert(evm_stack_ensure_capacity(stack, 1));
    assert(evm_stack_ensure_capacity(stack, EVM_STACK_MAX_DEPTH - 5));
    assert(!evm_stack_ensure_capacity(stack, EVM_STACK_MAX_DEPTH - 4));

    evm_stack_destroy(stack);
}

int main(void)
{
    printf("Running EVM Stack Tests...\n");
    
    test_stack_create_destroy();
    test_stack_push_pop();
    test_stack_peek();
    test_stack_get_set();
    test_stack_dup();
    test_stack_swap();
    test_stack_overflow();
    test_stack_reset();
    test_stack_validation();
    
    printf("All stack tests passed! (9/9)\n");
    return 0;
}

