/**
 * EVM Control Flow Opcodes
 *
 * Implements control flow operations:
 * - JUMP, JUMPI, JUMPDEST, PC
 * - STOP, RETURN, REVERT, INVALID
 * - SELFDESTRUCT
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
// Jump Operations
//==============================================================================

/**
 * JUMP - Alter program counter
 * Stack: dest =>
 * Gas: 8
 */
evm_status_t op_jump(evm_t *evm);

/**
 * JUMPI - Conditionally alter program counter
 * Stack: dest cond =>
 * Gas: 10
 */
evm_status_t op_jumpi(evm_t *evm);

/**
 * JUMPDEST - Mark valid jump destination
 * Stack: (no effect)
 * Gas: 1
 */
evm_status_t op_jumpdest(evm_t *evm);

/**
 * PC - Get program counter
 * Stack: => pc
 * Gas: 2
 */
evm_status_t op_pc(evm_t *evm);

/**
 * GAS - Get remaining gas
 * Stack: => gas
 * Gas: 2
 */
evm_status_t op_gas(evm_t *evm);

//==============================================================================
// Execution Control
//==============================================================================

/**
 * STOP - Halt execution successfully
 * Stack: (no effect)
 * Gas: 0
 */
evm_status_t op_stop(evm_t *evm);

/**
 * RETURN - Halt execution and return output data
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
evm_status_t op_return(evm_t *evm);

/**
 * REVERT - Halt execution and revert state changes
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
evm_status_t op_revert(evm_t *evm);

/**
 * INVALID - Invalid instruction (always fails)
 * Stack: (no effect)
 * Gas: All remaining gas consumed
 */
evm_status_t op_invalid(evm_t *evm);

/**
 * SELFDESTRUCT - Destroy current contract and send funds
 * Stack: beneficiary =>
 * Gas: 5000 (or 30000 if creating new account)
 */
evm_status_t op_selfdestruct(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_CONTROL_H */
