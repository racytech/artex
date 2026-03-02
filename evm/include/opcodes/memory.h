/**
 * EVM Memory Opcodes
 *
 * Implements memory operations:
 * - MLOAD, MSTORE, MSTORE8, MSIZE
 *
 * Memory is execution-local byte-addressable storage that is cleared
 * between external calls. For persistent storage, see storage.h (SLOAD/SSTORE).
 */

#ifndef ART_EVM_OPCODES_MEMORY_H
#define ART_EVM_OPCODES_MEMORY_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_MLOAD 0x51   // Load word from memory
#define OP_MSTORE 0x52  // Store word to memory
#define OP_MSTORE8 0x53 // Store byte to memory
#define OP_MSIZE 0x59   // Get memory size

//==============================================================================
// Memory Opcodes
//==============================================================================

/**
 * MLOAD - Load word from memory
 * Stack: offset => value
 * Gas: 3 + memory_expansion_cost
 */
evm_status_t op_mload(evm_t *evm);

/**
 * MSTORE - Store word to memory
 * Stack: offset value =>
 * Gas: 3 + memory_expansion_cost
 */
evm_status_t op_mstore(evm_t *evm);

/**
 * MSTORE8 - Store byte to memory
 * Stack: offset value =>
 * Gas: 3 + memory_expansion_cost
 */
evm_status_t op_mstore8(evm_t *evm);

/**
 * MSIZE - Get size of active memory in bytes
 * Stack: => size
 * Gas: 2
 */
evm_status_t op_msize(evm_t *evm);

/**
 * MCOPY - Copy memory areas (EIP-5656, Cancun+)
 * Stack: dst src size =>
 * Gas: 3 + 3 * (size in words) + memory_expansion_cost
 */
evm_status_t op_mcopy(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_MEMORY_H */
