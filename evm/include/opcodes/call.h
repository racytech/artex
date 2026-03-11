/**
 * EVM Call Opcodes — opcode definitions only.
 * Implementations are in opcodes/call.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_CALL_H
#define ART_EVM_OPCODES_CALL_H

#define OP_CALL 0xf1
#define OP_CALLCODE 0xf2
#define OP_DELEGATECALL 0xf4
#define OP_STATICCALL 0xfa

#endif /* ART_EVM_OPCODES_CALL_H */
