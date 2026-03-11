/**
 * EVM Stack Manipulation Opcodes
 *
 * All stack manipulation opcodes (POP, PUSH0-32, DUP1-16, SWAP1-16) are
 * inlined directly into interpreter.c dispatch labels. This header provides
 * opcode definitions only.
 */

#ifndef ART_EVM_OPCODES_STACK_H
#define ART_EVM_OPCODES_STACK_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_POP 0x50 // Remove item from stack

// PUSH opcodes: 0x60-0x7f
#define OP_PUSH1 0x60
#define OP_PUSH2 0x61
#define OP_PUSH3 0x62
#define OP_PUSH4 0x63
#define OP_PUSH5 0x64
#define OP_PUSH6 0x65
#define OP_PUSH7 0x66
#define OP_PUSH8 0x67
#define OP_PUSH9 0x68
#define OP_PUSH10 0x69
#define OP_PUSH11 0x6a
#define OP_PUSH12 0x6b
#define OP_PUSH13 0x6c
#define OP_PUSH14 0x6d
#define OP_PUSH15 0x6e
#define OP_PUSH16 0x6f
#define OP_PUSH17 0x70
#define OP_PUSH18 0x71
#define OP_PUSH19 0x72
#define OP_PUSH20 0x73
#define OP_PUSH21 0x74
#define OP_PUSH22 0x75
#define OP_PUSH23 0x76
#define OP_PUSH24 0x77
#define OP_PUSH25 0x78
#define OP_PUSH26 0x79
#define OP_PUSH27 0x7a
#define OP_PUSH28 0x7b
#define OP_PUSH29 0x7c
#define OP_PUSH30 0x7d
#define OP_PUSH31 0x7e
#define OP_PUSH32 0x7f

// DUP opcodes: 0x80-0x8f
#define OP_DUP1 0x80
#define OP_DUP2 0x81
#define OP_DUP3 0x82
#define OP_DUP4 0x83
#define OP_DUP5 0x84
#define OP_DUP6 0x85
#define OP_DUP7 0x86
#define OP_DUP8 0x87
#define OP_DUP9 0x88
#define OP_DUP10 0x89
#define OP_DUP11 0x8a
#define OP_DUP12 0x8b
#define OP_DUP13 0x8c
#define OP_DUP14 0x8d
#define OP_DUP15 0x8e
#define OP_DUP16 0x8f

// SWAP opcodes: 0x90-0x9f
#define OP_SWAP1 0x90
#define OP_SWAP2 0x91
#define OP_SWAP3 0x92
#define OP_SWAP4 0x93
#define OP_SWAP5 0x94
#define OP_SWAP6 0x95
#define OP_SWAP7 0x96
#define OP_SWAP8 0x97
#define OP_SWAP9 0x98
#define OP_SWAP10 0x99
#define OP_SWAP11 0x9a
#define OP_SWAP12 0x9b
#define OP_SWAP13 0x9c
#define OP_SWAP14 0x9d
#define OP_SWAP15 0x9e
#define OP_SWAP16 0x9f

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_STACK_H */
