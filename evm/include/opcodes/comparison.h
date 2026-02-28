/**
 * EVM Comparison & Bitwise Opcodes
 *
 * Implements comparison and bitwise operations:
 * - LT, GT, SLT, SGT, EQ
 * - ISZERO
 * - AND, OR, XOR, NOT
 * - BYTE, SHL, SHR, SAR
 */

#ifndef ART_EVM_OPCODES_COMPARISON_H
#define ART_EVM_OPCODES_COMPARISON_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_LT 0x10      // Less than (unsigned)
#define OP_GT 0x11      // Greater than (unsigned)
#define OP_SLT 0x12     // Less than (signed)
#define OP_SGT 0x13     // Greater than (signed)
#define OP_EQ 0x14      // Equality
#define OP_ISZERO 0x15  // Is zero
#define OP_AND 0x16     // Bitwise AND
#define OP_OR 0x17      // Bitwise OR
#define OP_XOR 0x18     // Bitwise XOR
#define OP_NOT 0x19     // Bitwise NOT
#define OP_BYTE 0x1a    // Extract byte
#define OP_SHL 0x1b     // Shift left
#define OP_SHR 0x1c     // Shift right (logical)
#define OP_SAR 0x1d     // Shift right (arithmetic)

//==============================================================================
// Comparison Opcodes
//==============================================================================

/**
 * LT - Unsigned less than
 * Stack: a b => (a < b) ? 1 : 0
 * Gas: 3
 */
evm_status_t op_lt(evm_t *evm);

/**
 * GT - Unsigned greater than
 * Stack: a b => (a > b) ? 1 : 0
 * Gas: 3
 */
evm_status_t op_gt(evm_t *evm);

/**
 * SLT - Signed less than
 * Stack: a b => (a < b) ? 1 : 0 (signed)
 * Gas: 3
 */
evm_status_t op_slt(evm_t *evm);

/**
 * SGT - Signed greater than
 * Stack: a b => (a > b) ? 1 : 0 (signed)
 * Gas: 3
 */
evm_status_t op_sgt(evm_t *evm);

/**
 * EQ - Equality
 * Stack: a b => (a == b) ? 1 : 0
 * Gas: 3
 */
evm_status_t op_eq(evm_t *evm);

/**
 * ISZERO - Is zero
 * Stack: a => (a == 0) ? 1 : 0
 * Gas: 3
 */
evm_status_t op_iszero(evm_t *evm);

//==============================================================================
// Bitwise Opcodes
//==============================================================================

/**
 * AND - Bitwise AND
 * Stack: a b => a & b
 * Gas: 3
 */
evm_status_t op_and(evm_t *evm);

/**
 * OR - Bitwise OR
 * Stack: a b => a | b
 * Gas: 3
 */
evm_status_t op_or(evm_t *evm);

/**
 * XOR - Bitwise XOR
 * Stack: a b => a ^ b
 * Gas: 3
 */
evm_status_t op_xor(evm_t *evm);

/**
 * NOT - Bitwise NOT
 * Stack: a => ~a
 * Gas: 3
 */
evm_status_t op_not(evm_t *evm);

/**
 * BYTE - Extract byte
 * Stack: i x => x[i] (or 0 if i >= 32)
 * Gas: 3
 */
evm_status_t op_byte(evm_t *evm);

/**
 * SHL - Shift left
 * Stack: shift value => value << shift
 * Gas: 3
 */
evm_status_t op_shl(evm_t *evm);

/**
 * SHR - Logical shift right
 * Stack: shift value => value >> shift
 * Gas: 3
 */
evm_status_t op_shr(evm_t *evm);

/**
 * SAR - Arithmetic shift right
 * Stack: shift value => value >> shift (with sign extension)
 * Gas: 3
 */
evm_status_t op_sar(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_COMPARISON_H */
