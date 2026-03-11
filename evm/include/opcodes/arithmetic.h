/**
 * EVM Arithmetic Opcodes — opcode definitions only.
 * Implementations are in opcodes/arithmetic.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_ARITHMETIC_H
#define ART_EVM_OPCODES_ARITHMETIC_H

#define OP_ADD 0x01
#define OP_MUL 0x02
#define OP_SUB 0x03
#define OP_DIV 0x04
#define OP_SDIV 0x05
#define OP_MOD 0x06
#define OP_SMOD 0x07
#define OP_ADDMOD 0x08
#define OP_MULMOD 0x09
#define OP_EXP 0x0a
#define OP_SIGNEXTEND 0x0b

#endif /* ART_EVM_OPCODES_ARITHMETIC_H */
