/**
 * EVM Environmental Information Opcodes — opcode definitions only.
 * Implementations are in opcodes/environmental.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_ENVIRONMENTAL_H
#define ART_EVM_OPCODES_ENVIRONMENTAL_H

#define OP_ADDRESS 0x30
#define OP_BALANCE 0x31
#define OP_ORIGIN 0x32
#define OP_CALLER 0x33
#define OP_CALLVALUE 0x34
#define OP_CALLDATALOAD 0x35
#define OP_CALLDATASIZE 0x36
#define OP_CALLDATACOPY 0x37
#define OP_CODESIZE 0x38
#define OP_CODECOPY 0x39
#define OP_GASPRICE 0x3a
#define OP_EXTCODESIZE 0x3b
#define OP_EXTCODECOPY 0x3c
#define OP_RETURNDATASIZE 0x3d
#define OP_RETURNDATACOPY 0x3e
#define OP_EXTCODEHASH 0x3f

#endif /* ART_EVM_OPCODES_ENVIRONMENTAL_H */
