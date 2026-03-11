/**
 * EVM Control Flow Opcodes — opcode definitions only.
 * Implementations are in opcodes/control.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_CONTROL_H
#define ART_EVM_OPCODES_CONTROL_H

#define OP_JUMP 0x56
#define OP_JUMPI 0x57
#define OP_PC 0x58
#define OP_JUMPDEST 0x5b
#define OP_GAS 0x5a

#define OP_STOP 0x00
#define OP_RETURN 0xf3
#define OP_REVERT 0xfd
#define OP_INVALID 0xfe
#define OP_SELFDESTRUCT 0xff

#endif /* ART_EVM_OPCODES_CONTROL_H */
