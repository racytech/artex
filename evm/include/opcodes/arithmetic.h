/**
 * EVM Arithmetic Opcodes
 *
 * Implements arithmetic operations on the EVM stack:
 * - ADD, MUL, SUB, DIV, SDIV, MOD, SMOD
 * - ADDMOD, MULMOD
 * - EXP
 * - SIGNEXTEND
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
 * ADD - Addition
 * Stack: a b => a + b
 * Gas: 3
 */
evm_status_t op_add(evm_t *evm);

/**
 * MUL - Multiplication
 * Stack: a b => a * b
 * Gas: 5
 */
evm_status_t op_mul(evm_t *evm);

/**
 * SUB - Subtraction
 * Stack: a b => a - b
 * Gas: 3
 */
evm_status_t op_sub(evm_t *evm);

/**
 * DIV - Unsigned division
 * Stack: a b => a / b (or 0 if b == 0)
 * Gas: 5
 */
evm_status_t op_div(evm_t *evm);

/**
 * SDIV - Signed division
 * Stack: a b => a / b (signed, or 0 if b == 0)
 * Gas: 5
 */
evm_status_t op_sdiv(evm_t *evm);

/**
 * MOD - Unsigned modulo
 * Stack: a b => a % b (or 0 if b == 0)
 * Gas: 5
 */
evm_status_t op_mod(evm_t *evm);

/**
 * SMOD - Signed modulo
 * Stack: a b => a % b (signed, or 0 if b == 0)
 * Gas: 5
 */
evm_status_t op_smod(evm_t *evm);

/**
 * ADDMOD - Modular addition
 * Stack: a b N => (a + b) % N (or 0 if N == 0)
 * Gas: 8
 */
evm_status_t op_addmod(evm_t *evm);

/**
 * MULMOD - Modular multiplication
 * Stack: a b N => (a * b) % N (or 0 if N == 0)
 * Gas: 8
 */
evm_status_t op_mulmod(evm_t *evm);

/**
 * EXP - Exponentiation
 * Stack: a b => a ^ b
 * Gas: 10 + 50 * (byte_length_of_exponent)
 */
evm_status_t op_exp(evm_t *evm);

/**
 * SIGNEXTEND - Sign extension
 * Stack: b x => sign_extended_x
 * Gas: 5
 */
evm_status_t op_signextend(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_ARITHMETIC_H */
