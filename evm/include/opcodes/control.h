/**
 * EVM Control Flow Opcodes
 *
 * Simple control opcodes (STOP, JUMPDEST, PC, GAS) are inlined directly
 * into interpreter.c dispatch labels. This header declares the remaining
 * control flow opcodes that are still called as functions.
 */

#ifndef ART_EVM_OPCODES_CONTROL_H
#define ART_EVM_OPCODES_CONTROL_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_JUMP 0x56     // Unconditional jump
#define OP_JUMPI 0x57    // Conditional jump
#define OP_PC 0x58       // Program counter
#define OP_JUMPDEST 0x5b // Jump destination marker
#define OP_GAS 0x5a      // Remaining gas

#define OP_STOP 0x00        // Halt execution
#define OP_RETURN 0xf3      // Halt and return output data
#define OP_REVERT 0xfd      // Halt and revert state changes
#define OP_INVALID 0xfe     // Invalid instruction
#define OP_SELFDESTRUCT 0xff // Destroy contract

//==============================================================================
// Opcode Implementations (remaining non-inlined functions)
//==============================================================================

evm_status_t op_invalid(evm_t *evm);
evm_status_t op_jump(evm_t *evm);
evm_status_t op_jumpi(evm_t *evm);
evm_status_t op_return(evm_t *evm);
evm_status_t op_revert(evm_t *evm);
evm_status_t op_selfdestruct(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_CONTROL_H */
