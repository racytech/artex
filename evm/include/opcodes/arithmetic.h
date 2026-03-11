/**
 * EVM Arithmetic Opcodes
 *
 * Most arithmetic opcodes are inlined directly into interpreter.c dispatch labels.
 * Only op_exp remains as a function call (complex gas calculation).
 */

#ifndef ART_EVM_OPCODES_ARITHMETIC_H
#define ART_EVM_OPCODES_ARITHMETIC_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_ADD 0x01        // Addition
#define OP_MUL 0x02        // Multiplication
#define OP_SUB 0x03        // Subtraction
#define OP_DIV 0x04        // Division (unsigned)
#define OP_SDIV 0x05       // Division (signed)
#define OP_MOD 0x06        // Modulo (unsigned)
#define OP_SMOD 0x07       // Modulo (signed)
#define OP_ADDMOD 0x08     // (a + b) % N
#define OP_MULMOD 0x09     // (a * b) % N
#define OP_EXP 0x0a        // Exponentiation
#define OP_SIGNEXTEND 0x0b // Sign extension

//==============================================================================
// Opcode Implementations
//==============================================================================

/**
 * EXP - Exponentiation
 * Stack: a b => a ^ b
 * Gas: 10 + 50 * (byte_length_of_exponent)
 */
evm_status_t op_exp(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_ARITHMETIC_H */
