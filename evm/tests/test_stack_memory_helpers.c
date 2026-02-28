/**
 * Stack and Memory Helper Functions Tests
 */

#include "evm_stack.h"
#include "evm_memory.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

void test_stack_clear(void)
{
    printf("Testing evm_stack_clear...\n");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push some values
    uint256_t val1 = uint256_from_uint64(42);
    uint256_t val2 = uint256_from_uint64(100);
    uint256_t val3 = uint256_from_uint64(999);

    assert(evm_stack_push(stack, &val1));
    assert(evm_stack_push(stack, &val2));
    assert(evm_stack_push(stack, &val3));
    assert(evm_stack_size(stack) == 3);

    // Clear the stack
    evm_stack_clear(stack);

    // Verify it's empty
    assert(evm_stack_size(stack) == 0);
    assert(evm_stack_is_empty(stack));

    // Can push again
    assert(evm_stack_push(stack, &val1));
    assert(evm_stack_size(stack) == 1);

    evm_stack_destroy(stack);
    printf("  ✓ evm_stack_clear works correctly\n");
}

void test_stack_reset(void)
{
    printf("Testing evm_stack_reset...\n");

    evm_stack_t *stack = evm_stack_create();
    assert(stack != NULL);

    // Push some values
    uint256_t val = uint256_from_uint64(12345);
    for (int i = 0; i < 10; i++)
    {
        assert(evm_stack_push(stack, &val));
    }
    assert(evm_stack_size(stack) == 10);

    // Reset the stack
    evm_stack_reset(stack);

    // Verify it's empty
    assert(evm_stack_size(stack) == 0);
    assert(evm_stack_is_empty(stack));

    evm_stack_destroy(stack);
    printf("  ✓ evm_stack_reset works correctly\n");
}

void test_memory_clear(void)
{
    printf("Testing evm_memory_clear...\n");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write some data
    uint8_t data[] = {1, 2, 3, 4, 5, 6, 7, 8};
    assert(evm_memory_write(mem, 0, data, sizeof(data)));
    assert(evm_memory_write(mem, 100, data, sizeof(data)));
    assert(evm_memory_size(mem) > 0);

    size_t old_size = evm_memory_size(mem);

    // Clear the memory
    evm_memory_clear(mem);

    // Verify it's empty
    assert(evm_memory_size(mem) == 0);
    assert(evm_memory_is_empty(mem));

    // Can write again
    assert(evm_memory_write(mem, 0, data, sizeof(data)));
    assert(evm_memory_size(mem) == 32); // Rounded to word boundary

    evm_memory_destroy(mem);
    printf("  ✓ evm_memory_clear works correctly\n");
}

void test_memory_reset(void)
{
    printf("Testing evm_memory_reset...\n");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write a word
    uint256_t val = uint256_from_uint64(0xDEADBEEF);
    assert(evm_memory_write_word(mem, 0, &val));
    assert(evm_memory_size(mem) == 32);

    // Reset
    evm_memory_reset(mem);

    // Verify empty
    assert(evm_memory_size(mem) == 0);

    // Read should expand and return zeros
    uint256_t result;
    assert(evm_memory_read_word(mem, 0, &result));
    assert(uint256_is_zero(&result));

    evm_memory_destroy(mem);
    printf("  ✓ evm_memory_reset works correctly\n");
}

void test_stack_memory_integration(void)
{
    printf("Testing stack/memory integration (simulating EVM reset)...\n");

    evm_stack_t *stack = evm_stack_create();
    evm_memory_t *mem = evm_memory_create();
    assert(stack != NULL && mem != NULL);

    // Simulate some EVM operations
    uint256_t val = uint256_from_uint64(42);
    evm_stack_push(stack, &val);
    evm_stack_push(stack, &val);
    evm_stack_push(stack, &val);

    uint8_t data[64] = {0xFF};
    evm_memory_write(mem, 0, data, 64);

    assert(evm_stack_size(stack) == 3);
    assert(evm_memory_size(mem) == 64);

    // Reset both (like evm_reset does)
    evm_stack_clear(stack);
    evm_memory_clear(mem);

    // Both should be empty
    assert(evm_stack_is_empty(stack));
    assert(evm_memory_is_empty(mem));

    // Can use again
    evm_stack_push(stack, &val);
    assert(evm_stack_size(stack) == 1);

    evm_memory_write_byte(mem, 0, 0xAB);
    assert(evm_memory_size(mem) == 32); // Rounded to word

    evm_stack_destroy(stack);
    evm_memory_destroy(mem);
    printf("  ✓ Stack/memory integration works correctly\n");
}

void test_clear_vs_reset(void)
{
    printf("Testing clear vs reset (should be identical)...\n");

    evm_stack_t *s1 = evm_stack_create();
    evm_stack_t *s2 = evm_stack_create();

    uint256_t val = uint256_from_uint64(123);
    evm_stack_push(s1, &val);
    evm_stack_push(s2, &val);

    evm_stack_clear(s1);
    evm_stack_reset(s2);

    assert(evm_stack_size(s1) == evm_stack_size(s2));
    assert(evm_stack_is_empty(s1) == evm_stack_is_empty(s2));

    evm_stack_destroy(s1);
    evm_stack_destroy(s2);

    evm_memory_t *m1 = evm_memory_create();
    evm_memory_t *m2 = evm_memory_create();

    uint8_t data = 0xFF;
    evm_memory_write_byte(m1, 0, data);
    evm_memory_write_byte(m2, 0, data);

    evm_memory_clear(m1);
    evm_memory_reset(m2);

    assert(evm_memory_size(m1) == evm_memory_size(m2));
    assert(evm_memory_is_empty(m1) == evm_memory_is_empty(m2));

    evm_memory_destroy(m1);
    evm_memory_destroy(m2);

    printf("  ✓ clear and reset are functionally identical\n");
}

int main(void)
{
    printf("\n=== Stack and Memory Helper Tests ===\n\n");

    test_stack_clear();
    test_stack_reset();
    test_memory_clear();
    test_memory_reset();
    test_stack_memory_integration();
    test_clear_vs_reset();

    printf("\n=== All Helper Tests Passed ===\n\n");
    return 0;
}
