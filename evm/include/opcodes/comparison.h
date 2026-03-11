/**
 * EVM Comparison & Bitwise Opcodes — opcode definitions only.
 * Implementations are inlined directly into interpreter.c dispatch labels.
 */

#ifndef ART_EVM_OPCODES_COMPARISON_H
#define ART_EVM_OPCODES_COMPARISON_H

#define OP_LT 0x10
#define OP_GT 0x11
#define OP_SLT 0x12
#define OP_SGT 0x13
#define OP_EQ 0x14
#define OP_ISZERO 0x15
#define OP_AND 0x16
#define OP_OR 0x17
#define OP_XOR 0x18
#define OP_NOT 0x19
#define OP_BYTE 0x1a
#define OP_SHL 0x1b
#define OP_SHR 0x1c
#define OP_SAR 0x1d

#endif /* ART_EVM_OPCODES_COMPARISON_H */
