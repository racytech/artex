/**
 * EVM Memory Opcodes — opcode definitions only.
 * Implementations are in opcodes/memory.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_MEMORY_H
#define ART_EVM_OPCODES_MEMORY_H

#define OP_MLOAD 0x51
#define OP_MSTORE 0x52
#define OP_MSTORE8 0x53
#define OP_MSIZE 0x59

#endif /* ART_EVM_OPCODES_MEMORY_H */
