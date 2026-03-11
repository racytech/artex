/**
 * EVM Block Information Opcodes — opcode definitions only.
 * Implementations are in opcodes/block.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_BLOCK_H
#define ART_EVM_OPCODES_BLOCK_H

#define OP_BLOCKHASH 0x40
#define OP_COINBASE 0x41
#define OP_TIMESTAMP 0x42
#define OP_NUMBER 0x43
#define OP_DIFFICULTY 0x44
#define OP_GASLIMIT 0x45
#define OP_CHAINID 0x46
#define OP_SELFBALANCE 0x47
#define OP_BASEFEE 0x48

#endif /* ART_EVM_OPCODES_BLOCK_H */
