/**
 * Tests for EVM Interpreter
 */

#include "interpreter.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "state_db.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Test Helpers
//==============================================================================

static evm_t *create_test_evm(const uint8_t *code, size_t code_size)
{
    evm_t *evm = calloc(1, sizeof(evm_t));
    assert(evm != NULL);

    evm->stack = evm_stack_create();
    assert(evm->stack != NULL);

    evm->memory = evm_memory_create();
    assert(evm->memory != NULL);

    evm->code = (uint8_t *)code;
    evm->code_size = code_size;
    evm->pc = 0;
    evm->gas_left = 1000000;
    evm->stopped = false;

    return evm;
}

static void destroy_test_evm(evm_t *evm)
{
    if (evm)
    {
        if (evm->stack)
            evm_stack_destroy(evm->stack);
        if (evm->memory)
            evm_memory_destroy(evm->memory);
        if (evm->return_data)
            free(evm->return_data);
        free(evm);
    }
}

//==============================================================================
// Interpreter Tests
//==============================================================================

void test_simple_arithmetic(void)
{
    printf("  Testing simple arithmetic (2 + 3)...\n");

    // PUSH1 2, PUSH1 3, ADD, STOP
    static uint8_t code[] = {0x60, 0x02, 0x60, 0x03, 0x01, 0x00};

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));
    assert(uint256_eq(&value, &uint256_from_uint64(5)));

    destroy_test_evm(evm);
}

void test_multiple_operations(void)
{
    printf("  Testing multiple operations ((10 + 5) * 2)...\n");

    // PUSH1 10, PUSH1 5, ADD, PUSH1 2, MUL, STOP
    static uint8_t code[] = {0x60, 0x0A, 0x60, 0x05, 0x01, 0x60, 0x02, 0x02, 0x00};

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));
    assert(uint256_eq(&value, &uint256_from_uint64(30)));

    destroy_test_evm(evm);
}

void test_jump_valid(void)
{
    printf("  Testing valid JUMP...\n");

    // PUSH1 5, JUMP, INVALID, INVALID, JUMPDEST, PUSH1 42, STOP
    static uint8_t code[] = {
        0x60, 0x06,  // PUSH1 6 (jump to position 6)
        0x56,        // JUMP
        0xFE,        // INVALID (should be skipped)
        0xFE,        // INVALID (should be skipped)
        0xFE,        // INVALID (should be skipped)
        0x5B,        // JUMPDEST (position 6)
        0x60, 0x2A,  // PUSH1 42
        0x00         // STOP
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));
    assert(uint256_eq(&value, &uint256_from_uint64(42)));

    destroy_test_evm(evm);
}

void test_jumpi_taken(void)
{
    printf("  Testing JUMPI (condition true)...\n");

    // PUSH1 10, PUSH1 1, PUSH1 9, JUMPI, PUSH1 99, STOP, JUMPDEST, PUSH1 42, STOP
    static uint8_t code[] = {
        0x60, 0x0A,  // PUSH1 10 (destination)
        0x60, 0x01,  // PUSH1 1 (condition = true)
        0x57,        // JUMPI
        0x60, 0x63,  // PUSH1 99 (should be skipped)
        0x00,        // STOP
        0xFE,        // INVALID
        0x5B,        // JUMPDEST (position 10)
        0x60, 0x2A,  // PUSH1 42
        0x00         // STOP
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));
    assert(uint256_eq(&value, &uint256_from_uint64(42)));

    destroy_test_evm(evm);
}

void test_jumpi_not_taken(void)
{
    printf("  Testing JUMPI (condition false)...\n");

    // PUSH1 10, PUSH1 0, JUMPI, PUSH1 99, STOP, JUMPDEST, PUSH1 42, STOP
    static uint8_t code[] = {
        0x60, 0x0A,  // PUSH1 10 (destination)
        0x60, 0x00,  // PUSH1 0 (condition = false)
        0x57,        // JUMPI
        0x60, 0x63,  // PUSH1 99 (should be executed)
        0x00,        // STOP
        0xFE,        // INVALID
        0x5B,        // JUMPDEST (position 10)
        0x60, 0x2A,  // PUSH1 42
        0x00         // STOP
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));
    assert(uint256_eq(&value, &uint256_from_uint64(99)));

    destroy_test_evm(evm);
}

void test_return_with_data(void)
{
    printf("  Testing RETURN with data...\n");

    // PUSH1 0xAB, PUSH1 0, MSTORE8, PUSH1 1, PUSH1 0, RETURN
    static uint8_t code[] = {
        0x60, 0xAB,  // PUSH1 0xAB
        0x60, 0x00,  // PUSH1 0
        0x53,        // MSTORE8
        0x60, 0x01,  // PUSH1 1 (size)
        0x60, 0x00,  // PUSH1 0 (offset)
        0xF3         // RETURN
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(result.output_size == 1);
    assert(result.output_data != NULL);
    assert(result.output_data[0] == 0xAB);

    destroy_test_evm(evm);
}

void test_invalid_opcode(void)
{
    printf("  Testing invalid opcode...\n");

    // PUSH1 42, INVALID
    static uint8_t code[] = {0x60, 0x2A, 0xFE};

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_INVALID_OPCODE);

    destroy_test_evm(evm);
}

void test_stack_operations(void)
{
    printf("  Testing stack operations (DUP, SWAP)...\n");

    // PUSH1 10, PUSH1 20, DUP2, SWAP1, STOP
    static uint8_t code[] = {
        0x60, 0x0A,  // PUSH1 10
        0x60, 0x14,  // PUSH1 20
        0x81,        // DUP2 (duplicates 10)
        0x90,        // SWAP1
        0x00         // STOP
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 3);

    // Stack should be: [10, 10, 20] (top to bottom after SWAP)
    uint256_t val1, val2, val3;
    assert(evm_stack_pop(evm->stack, &val1));
    assert(evm_stack_pop(evm->stack, &val2));
    assert(evm_stack_pop(evm->stack, &val3));

    assert(uint256_eq(&val1, &uint256_from_uint64(10)));
    assert(uint256_eq(&val2, &uint256_from_uint64(10)));
    assert(uint256_eq(&val3, &uint256_from_uint64(20)));

    destroy_test_evm(evm);
}

void test_memory_operations(void)
{
    printf("  Testing memory operations...\n");

    // PUSH1 0xFF, PUSH1 0, MSTORE8, PUSH1 0, MLOAD, STOP
    static uint8_t code[] = {
        0x60, 0xFF,  // PUSH1 0xFF
        0x60, 0x00,  // PUSH1 0
        0x53,        // MSTORE8
        0x60, 0x00,  // PUSH1 0
        0x51,        // MLOAD
        0x00         // STOP
    };

    evm_t *evm = create_test_evm(code, sizeof(code));

    evm_result_t result = evm_interpret(evm);

    assert(result.status == EVM_SUCCESS);
    assert(evm_stack_size(evm->stack) == 1);

    uint256_t value;
    assert(evm_stack_pop(evm->stack, &value));

    // Should have 0xFF in the first byte, rest zeros
    uint8_t bytes[32];
    uint256_to_bytes(&value, bytes);
    assert(bytes[0] == 0xFF);

    destroy_test_evm(evm);
}

//==============================================================================
// Main Test Runner
//==============================================================================

int main(void)
{
    printf("Running EVM Interpreter Tests...\n");

    test_simple_arithmetic();
    test_multiple_operations();
    test_jump_valid();
    test_jumpi_taken();
    test_jumpi_not_taken();
    test_return_with_data();
    test_invalid_opcode();
    test_stack_operations();
    test_memory_operations();

    printf("All interpreter tests passed!\n");
    return 0;
}
