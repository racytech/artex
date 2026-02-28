/**
 * EVM Memory Tests
 */

#include "evm_memory.h"
#include "uint256.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

// Test helper
static void print_test(const char *name)
{
    printf("  [TEST] %s\n", name);
}

// Test: Create and destroy memory
void test_memory_create_destroy(void)
{
    print_test("memory_create_destroy");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);
    assert(evm_memory_size(mem) == 0);
    assert(evm_memory_is_empty(mem));
    assert(evm_memory_size_words(mem) == 0);

    evm_memory_destroy(mem);
}

// Test: Read and write byte
void test_memory_byte_operations(void)
{
    print_test("memory_byte_operations");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write byte at offset 0
    assert(evm_memory_write_byte(mem, 0, 0xAA));
    assert(evm_memory_size(mem) == 32); // Rounded up to word

    // Read it back
    uint8_t value;
    assert(evm_memory_read_byte(mem, 0, &value));
    assert(value == 0xAA);

    // Write at offset 100
    assert(evm_memory_write_byte(mem, 100, 0xBB));
    assert(evm_memory_size(mem) == 128); // Rounded up: 101 -> 128

    assert(evm_memory_read_byte(mem, 100, &value));
    assert(value == 0xBB);

    // Unwritten bytes should be zero
    assert(evm_memory_read_byte(mem, 50, &value));
    assert(value == 0x00);

    evm_memory_destroy(mem);
}

// Test: Read and write word (256-bit)
void test_memory_word_operations(void)
{
    print_test("memory_word_operations");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write a word at offset 0
    uint256_t val1 = uint256_from_uint64(0x123456789ABCDEF0ULL);
    assert(evm_memory_write_word(mem, 0, &val1));
    assert(evm_memory_size(mem) == 32);

    // Read it back
    uint256_t read_val;
    assert(evm_memory_read_word(mem, 0, &read_val));
    assert(uint256_eq(&read_val, &val1));

    // Write at offset 64
    uint256_t val2 = uint256_from_uint64(0xFEDCBA9876543210ULL);
    assert(evm_memory_write_word(mem, 64, &val2));
    assert(evm_memory_size(mem) == 96); // 64 + 32 = 96

    assert(evm_memory_read_word(mem, 64, &read_val));
    assert(uint256_eq(&read_val, &val2));

    evm_memory_destroy(mem);
}

// Test: Read and write byte array
void test_memory_array_operations(void)
{
    print_test("memory_array_operations");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write a byte array
    uint8_t data[] = {0x11, 0x22, 0x33, 0x44, 0x55};
    assert(evm_memory_write(mem, 10, data, sizeof(data)));

    // Memory should be rounded up
    assert(evm_memory_size(mem) == 32); // 10 + 5 = 15 -> 32

    // Read it back
    uint8_t read_data[5];
    assert(evm_memory_read(mem, 10, read_data, sizeof(read_data)));
    assert(memcmp(data, read_data, sizeof(data)) == 0);

    // Write zero-length array (should succeed)
    assert(evm_memory_write(mem, 0, data, 0));

    evm_memory_destroy(mem);
}

// Test: Memory expansion
void test_memory_expansion(void)
{
    print_test("memory_expansion");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Initially empty
    assert(evm_memory_size(mem) == 0);

    // Expand to 100 bytes (should round to 128)
    assert(evm_memory_expand(mem, 0, 100));
    assert(evm_memory_size(mem) == 128);

    // Expand to 200 bytes (should round to 224)
    assert(evm_memory_expand(mem, 0, 200));
    assert(evm_memory_size(mem) == 224);

    // Expanding to smaller size should not shrink
    assert(evm_memory_expand(mem, 0, 50));
    assert(evm_memory_size(mem) == 224);

    evm_memory_destroy(mem);
}

// Test: Memory size in words
void test_memory_size_words(void)
{
    print_test("memory_size_words");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    assert(evm_memory_size_words(mem) == 0);

    // Write 1 byte -> 32 bytes = 1 word
    assert(evm_memory_write_byte(mem, 0, 0xFF));
    assert(evm_memory_size_words(mem) == 1);

    // Expand to 64 bytes = 2 words
    assert(evm_memory_expand(mem, 0, 64));
    assert(evm_memory_size_words(mem) == 2);

    // Expand to 65 bytes -> rounds to 96 = 3 words
    assert(evm_memory_expand(mem, 0, 65));
    assert(evm_memory_size_words(mem) == 3);

    evm_memory_destroy(mem);
}

// Test: Memory reset
void test_memory_reset(void)
{
    print_test("memory_reset");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write some data
    uint8_t data[] = {0xAA, 0xBB, 0xCC};
    assert(evm_memory_write(mem, 0, data, sizeof(data)));
    assert(evm_memory_size(mem) == 32);

    // Reset
    evm_memory_reset(mem);
    assert(evm_memory_size(mem) == 0);
    assert(evm_memory_is_empty(mem));

    // Can write again
    assert(evm_memory_write_byte(mem, 0, 0x11));
    assert(evm_memory_size(mem) == 32);

    evm_memory_destroy(mem);
}

// Test: Memory copy
void test_memory_copy(void)
{
    print_test("memory_copy");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Write source data
    uint8_t data[] = {0x11, 0x22, 0x33, 0x44, 0x55};
    assert(evm_memory_write(mem, 0, data, sizeof(data)));

    // Copy to offset 100
    assert(evm_memory_copy(mem, 100, 0, sizeof(data)));

    // Verify copy
    uint8_t read_data[5];
    assert(evm_memory_read(mem, 100, read_data, sizeof(read_data)));
    assert(memcmp(data, read_data, sizeof(data)) == 0);

    // Test overlapping copy (move right)
    assert(evm_memory_copy(mem, 2, 0, 3));
    uint8_t expected[] = {0x11, 0x22, 0x11, 0x22, 0x33};
    assert(evm_memory_read(mem, 0, read_data, sizeof(read_data)));
    assert(memcmp(expected, read_data, sizeof(expected)) == 0);

    evm_memory_destroy(mem);
}

// Test: Gas cost calculation
void test_memory_gas_cost(void)
{
    print_test("memory_gas_cost");

    // No expansion = no cost
    assert(evm_memory_expansion_cost(0, 0) == 0);
    assert(evm_memory_expansion_cost(100, 100) == 0);
    assert(evm_memory_expansion_cost(100, 50) == 0);

    // First word: cost = 3
    uint64_t cost_1_word = evm_memory_expansion_cost(0, 32);
    assert(cost_1_word == 3);

    // Second word: incremental cost = 3
    uint64_t cost_2_words = evm_memory_expansion_cost(32, 64);
    assert(cost_2_words == 3);

    // Larger expansion has quadratic component
    uint64_t cost_100_words = evm_memory_expansion_cost(0, 3200);
    // cost = (100^2 / 512) + (3 * 100) = 19.53... + 300 = 319
    assert(cost_100_words == 319);

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Access cost for first write
    uint64_t access_cost = evm_memory_access_cost(mem, 0, 32);
    assert(access_cost == 3);

    // Expand memory
    evm_memory_expand(mem, 0, 32);

    // No additional cost for same size
    access_cost = evm_memory_access_cost(mem, 0, 32);
    assert(access_cost == 0);

    evm_memory_destroy(mem);
}

// Test: Memory pointers
void test_memory_pointers(void)
{
    print_test("memory_pointers");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Get pointer from empty memory
    const uint8_t *ptr = evm_memory_get_ptr(mem, 0);
    assert(ptr == NULL);

    // Write some data
    uint8_t data[] = {0xAA, 0xBB, 0xCC};
    assert(evm_memory_write(mem, 10, data, sizeof(data)));

    // Get const pointer
    ptr = evm_memory_get_ptr(mem, 10);
    assert(ptr != NULL);
    assert(memcmp(ptr, data, sizeof(data)) == 0);

    // Get mutable pointer (expands if needed)
    uint8_t *mut_ptr = evm_memory_get_mut_ptr(mem, 100, 5);
    assert(mut_ptr != NULL);
    memcpy(mut_ptr, data, sizeof(data));

    // Verify written data
    uint8_t read_data[3];
    assert(evm_memory_read(mem, 100, read_data, sizeof(read_data)));
    assert(memcmp(data, read_data, sizeof(data)) == 0);

    evm_memory_destroy(mem);
}

// Test: Large memory operations
void test_memory_large(void)
{
    print_test("memory_large");

    evm_memory_t *mem = evm_memory_create();
    assert(mem != NULL);

    // Expand to 10KB
    assert(evm_memory_expand(mem, 0, 10240));
    assert(evm_memory_size(mem) == 10240);
    assert(evm_memory_size_words(mem) == 320);

    // Write at various offsets
    for (size_t i = 0; i < 10; i++)
    {
        uint8_t value = (uint8_t)(i * 10);
        assert(evm_memory_write_byte(mem, i * 1000, value));
    }

    // Verify
    for (size_t i = 0; i < 10; i++)
    {
        uint8_t value;
        assert(evm_memory_read_byte(mem, i * 1000, &value));
        assert(value == (uint8_t)(i * 10));
    }

    evm_memory_destroy(mem);
}

int main(void)
{
    printf("Running EVM Memory Tests...\n");
    
    test_memory_create_destroy();
    test_memory_byte_operations();
    test_memory_word_operations();
    test_memory_array_operations();
    test_memory_expansion();
    test_memory_size_words();
    test_memory_reset();
    test_memory_copy();
    test_memory_gas_cost();
    test_memory_pointers();
    test_memory_large();
    
    printf("All memory tests passed! (11/11)\n");
    return 0;
}

