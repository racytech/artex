/**
 * EVM Environmental Information Opcodes
 *
 * Implements opcodes that provide information about the execution environment:
 * - ADDRESS, BALANCE, ORIGIN, CALLER, CALLVALUE
 * - CALLDATALOAD, CALLDATASIZE, CALLDATACOPY
 * - CODESIZE, CODECOPY
 * - GASPRICE, EXTCODESIZE, EXTCODECOPY, RETURNDATASIZE, RETURNDATACOPY
 * - EXTCODEHASH
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
// Address & Balance
//==============================================================================

/**
 * ADDRESS - Get address of currently executing account
 * Stack: => address
 * Gas: 2
 */
evm_status_t op_address(evm_t *evm);

/**
 * BALANCE - Get balance of an account
 * Stack: address => balance
 * Gas: 100 (warm) / 2600 (cold)
 */
evm_status_t op_balance(evm_t *evm);

/**
 * ORIGIN - Get transaction origin
 * Stack: => origin
 * Gas: 2
 */
evm_status_t op_origin(evm_t *evm);

/**
 * CALLER - Get caller address
 * Stack: => caller
 * Gas: 2
 */
evm_status_t op_caller(evm_t *evm);

/**
 * CALLVALUE - Get deposited value
 * Stack: => value
 * Gas: 2
 */
evm_status_t op_callvalue(evm_t *evm);

//==============================================================================
// Calldata Operations
//==============================================================================

/**
 * CALLDATALOAD - Load word from calldata
 * Stack: i => data[i:i+32]
 * Gas: 3
 */
evm_status_t op_calldataload(evm_t *evm);

/**
 * CALLDATASIZE - Get size of calldata
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_calldatasize(evm_t *evm);

/**
 * CALLDATACOPY - Copy calldata to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_calldatacopy(evm_t *evm);

//==============================================================================
// Code Operations
//==============================================================================

/**
 * CODESIZE - Get size of code running in current environment
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_codesize(evm_t *evm);

/**
 * CODECOPY - Copy code to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_codecopy(evm_t *evm);

/**
 * GASPRICE - Get price of gas in current environment
 * Stack: => gas_price
 * Gas: 2
 */
evm_status_t op_gasprice(evm_t *evm);

//==============================================================================
// External Code Operations
//==============================================================================

/**
 * EXTCODESIZE - Get size of an account's code
 * Stack: address => size
 * Gas: 100 (warm) / 2600 (cold)
 */
evm_status_t op_extcodesize(evm_t *evm);

/**
 * EXTCODECOPY - Copy an account's code to memory
 * Stack: address destOffset offset size =>
 * Gas: 100 (warm) / 2600 (cold) + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_extcodecopy(evm_t *evm);

/**
 * EXTCODEHASH - Get hash of an account's code
 * Stack: address => hash
 * Gas: 100 (warm) / 2600 (cold)
 */
evm_status_t op_extcodehash(evm_t *evm);

//==============================================================================
// Return Data Operations
//==============================================================================

/**
 * RETURNDATASIZE - Get size of output data from previous call
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_returndatasize(evm_t *evm);

/**
 * RETURNDATACOPY - Copy output data from previous call to memory
 * Stack: destOffset offset size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_returndatacopy(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_ENVIRONMENTAL_H */
