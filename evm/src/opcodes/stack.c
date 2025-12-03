/**
 * EVM Stack Manipulation Opcodes Implementation
 */

#include "evm.h"
#include "opcodes/stack.h"
#include "evm_stack.h"
#include "gas.h"
#include "logger.h"

// POP (0x50): Remove item from stack
evm_status_t op_pop(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_require(evm->stack, 1))
    {
        LOG_ERROR("POP: Stack underflow");
        return EVM_STACK_UNDERFLOW;
    }

    uint256_t value;
    evm_stack_pop(evm->stack, &value);

    return EVM_SUCCESS;
}

// PUSH1-PUSH32 (0x60-0x7f): Push 1-32 bytes onto stack
evm_status_t op_push(evm_t *evm, uint8_t num_bytes)
{
    if (!evm || !evm->stack || !evm->code)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (num_bytes < 1 || num_bytes > 32)
    {
        LOG_ERROR("PUSH: Invalid number of bytes: %u", num_bytes);
        return EVM_INVALID_OPCODE;
    }

    // Check if we have enough code bytes to read
    if (evm->pc + 1 + num_bytes > evm->code_size)
    {
        LOG_ERROR("PUSH: Not enough code bytes (pc=%lu, bytes=%u, code_size=%lu)",
                  evm->pc, num_bytes, evm->code_size);
        return EVM_INVALID_OPCODE;
    }

    // Read bytes from code (starting from PC+1, after the PUSH opcode)
    uint8_t bytes[32] = {0};
    for (uint8_t i = 0; i < num_bytes; i++)
    {
        bytes[31 - (num_bytes - 1 - i)] = evm->code[evm->pc + 1 + i];
    }

    uint256_t value = uint256_from_bytes(bytes, 32);

    if (!evm_stack_push(evm->stack, &value))
    {
        return EVM_STACK_OVERFLOW;
    }

    // Advance PC past the opcode AND the pushed bytes
    evm->pc += 1 + num_bytes;

    return EVM_SUCCESS;
}

// Individual PUSH opcodes
evm_status_t op_push1(evm_t *evm) { return op_push(evm, 1); }
evm_status_t op_push2(evm_t *evm) { return op_push(evm, 2); }
evm_status_t op_push3(evm_t *evm) { return op_push(evm, 3); }
evm_status_t op_push4(evm_t *evm) { return op_push(evm, 4); }
evm_status_t op_push5(evm_t *evm) { return op_push(evm, 5); }
evm_status_t op_push6(evm_t *evm) { return op_push(evm, 6); }
evm_status_t op_push7(evm_t *evm) { return op_push(evm, 7); }
evm_status_t op_push8(evm_t *evm) { return op_push(evm, 8); }
evm_status_t op_push9(evm_t *evm) { return op_push(evm, 9); }
evm_status_t op_push10(evm_t *evm) { return op_push(evm, 10); }
evm_status_t op_push11(evm_t *evm) { return op_push(evm, 11); }
evm_status_t op_push12(evm_t *evm) { return op_push(evm, 12); }
evm_status_t op_push13(evm_t *evm) { return op_push(evm, 13); }
evm_status_t op_push14(evm_t *evm) { return op_push(evm, 14); }
evm_status_t op_push15(evm_t *evm) { return op_push(evm, 15); }
evm_status_t op_push16(evm_t *evm) { return op_push(evm, 16); }
evm_status_t op_push17(evm_t *evm) { return op_push(evm, 17); }
evm_status_t op_push18(evm_t *evm) { return op_push(evm, 18); }
evm_status_t op_push19(evm_t *evm) { return op_push(evm, 19); }
evm_status_t op_push20(evm_t *evm) { return op_push(evm, 20); }
evm_status_t op_push21(evm_t *evm) { return op_push(evm, 21); }
evm_status_t op_push22(evm_t *evm) { return op_push(evm, 22); }
evm_status_t op_push23(evm_t *evm) { return op_push(evm, 23); }
evm_status_t op_push24(evm_t *evm) { return op_push(evm, 24); }
evm_status_t op_push25(evm_t *evm) { return op_push(evm, 25); }
evm_status_t op_push26(evm_t *evm) { return op_push(evm, 26); }
evm_status_t op_push27(evm_t *evm) { return op_push(evm, 27); }
evm_status_t op_push28(evm_t *evm) { return op_push(evm, 28); }
evm_status_t op_push29(evm_t *evm) { return op_push(evm, 29); }
evm_status_t op_push30(evm_t *evm) { return op_push(evm, 30); }
evm_status_t op_push31(evm_t *evm) { return op_push(evm, 31); }
evm_status_t op_push32(evm_t *evm) { return op_push(evm, 32); }

// DUP1-DUP16 (0x80-0x8f): Duplicate nth stack item
evm_status_t op_dup(evm_t *evm, uint8_t n)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (n < 1 || n > 16)
    {
        LOG_ERROR("DUP: Invalid dup position: %u", n);
        return EVM_INVALID_OPCODE;
    }

    if (!evm_stack_dup(evm->stack, n))
    {
        return EVM_STACK_UNDERFLOW;
    }

    return EVM_SUCCESS;
}

// Individual DUP opcodes
evm_status_t op_dup1(evm_t *evm) { return op_dup(evm, 1); }
evm_status_t op_dup2(evm_t *evm) { return op_dup(evm, 2); }
evm_status_t op_dup3(evm_t *evm) { return op_dup(evm, 3); }
evm_status_t op_dup4(evm_t *evm) { return op_dup(evm, 4); }
evm_status_t op_dup5(evm_t *evm) { return op_dup(evm, 5); }
evm_status_t op_dup6(evm_t *evm) { return op_dup(evm, 6); }
evm_status_t op_dup7(evm_t *evm) { return op_dup(evm, 7); }
evm_status_t op_dup8(evm_t *evm) { return op_dup(evm, 8); }
evm_status_t op_dup9(evm_t *evm) { return op_dup(evm, 9); }
evm_status_t op_dup10(evm_t *evm) { return op_dup(evm, 10); }
evm_status_t op_dup11(evm_t *evm) { return op_dup(evm, 11); }
evm_status_t op_dup12(evm_t *evm) { return op_dup(evm, 12); }
evm_status_t op_dup13(evm_t *evm) { return op_dup(evm, 13); }
evm_status_t op_dup14(evm_t *evm) { return op_dup(evm, 14); }
evm_status_t op_dup15(evm_t *evm) { return op_dup(evm, 15); }
evm_status_t op_dup16(evm_t *evm) { return op_dup(evm, 16); }

// SWAP1-SWAP16 (0x90-0x9f): Swap top with nth stack item
evm_status_t op_swap(evm_t *evm, uint8_t n)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    if (!evm_use_gas(evm, GAS_VERY_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    if (n < 1 || n > 16)
    {
        LOG_ERROR("SWAP: Invalid swap position: %u", n);
        return EVM_INVALID_OPCODE;
    }

    if (!evm_stack_swap(evm->stack, n))
    {
        return EVM_STACK_UNDERFLOW;
    }

    return EVM_SUCCESS;
}

// Individual SWAP opcodes
evm_status_t op_swap1(evm_t *evm) { return op_swap(evm, 1); }
evm_status_t op_swap2(evm_t *evm) { return op_swap(evm, 2); }
evm_status_t op_swap3(evm_t *evm) { return op_swap(evm, 3); }
evm_status_t op_swap4(evm_t *evm) { return op_swap(evm, 4); }
evm_status_t op_swap5(evm_t *evm) { return op_swap(evm, 5); }
evm_status_t op_swap6(evm_t *evm) { return op_swap(evm, 6); }
evm_status_t op_swap7(evm_t *evm) { return op_swap(evm, 7); }
evm_status_t op_swap8(evm_t *evm) { return op_swap(evm, 8); }
evm_status_t op_swap9(evm_t *evm) { return op_swap(evm, 9); }
evm_status_t op_swap10(evm_t *evm) { return op_swap(evm, 10); }
evm_status_t op_swap11(evm_t *evm) { return op_swap(evm, 11); }
evm_status_t op_swap12(evm_t *evm) { return op_swap(evm, 12); }
evm_status_t op_swap13(evm_t *evm) { return op_swap(evm, 13); }
evm_status_t op_swap14(evm_t *evm) { return op_swap(evm, 14); }
evm_status_t op_swap15(evm_t *evm) { return op_swap(evm, 15); }
evm_status_t op_swap16(evm_t *evm) { return op_swap(evm, 16); }
