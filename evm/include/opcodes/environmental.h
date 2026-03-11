/**
 * EVM Environmental Information Opcodes
 *
 * Simple environmental opcodes (ADDRESS, ORIGIN, CALLER, CALLVALUE,
 * CALLDATALOAD, CALLDATASIZE, CODESIZE, GASPRICE, RETURNDATASIZE)
 * are inlined directly into interpreter.c dispatch labels.
 *
 * This header declares the remaining opcodes that are still called as functions.
 */

#ifndef ART_EVM_OPCODES_ENVIRONMENTAL_H
#define ART_EVM_OPCODES_ENVIRONMENTAL_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_ADDRESS 0x30         // Address of current contract
#define OP_BALANCE 0x31         // Balance of an account
#define OP_ORIGIN 0x32          // Transaction origin
#define OP_CALLER 0x33          // Message caller
#define OP_CALLVALUE 0x34       // Message value
#define OP_CALLDATALOAD 0x35    // Load word from calldata
#define OP_CALLDATASIZE 0x36    // Size of calldata
#define OP_CALLDATACOPY 0x37    // Copy calldata to memory
#define OP_CODESIZE 0x38        // Size of code
#define OP_CODECOPY 0x39        // Copy code to memory
#define OP_GASPRICE 0x3a        // Gas price
#define OP_EXTCODESIZE 0x3b     // Size of external code
#define OP_EXTCODECOPY 0x3c     // Copy external code to memory
#define OP_RETURNDATASIZE 0x3d  // Size of return data
#define OP_RETURNDATACOPY 0x3e  // Copy return data to memory
#define OP_EXTCODEHASH 0x3f     // Hash of external code

//==============================================================================
// Opcode Implementations (remaining non-inlined functions)
//==============================================================================

evm_status_t op_balance(evm_t *evm);
evm_status_t op_calldatacopy(evm_t *evm);
evm_status_t op_codecopy(evm_t *evm);
evm_status_t op_extcodesize(evm_t *evm);
evm_status_t op_extcodecopy(evm_t *evm);
evm_status_t op_extcodehash(evm_t *evm);
evm_status_t op_returndatacopy(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_ENVIRONMENTAL_H */
