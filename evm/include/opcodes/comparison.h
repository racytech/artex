/**
 * EVM Comparison & Bitwise Opcodes
 *
 * All comparison and bitwise opcodes are inlined directly into
 * interpreter.c dispatch labels. This header provides opcode definitions only.
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

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_COMPARISON_H */
