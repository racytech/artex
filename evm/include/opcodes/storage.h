/**
 * EVM Storage Opcodes
 *
 * Implements persistent storage operations:
 * - SLOAD, SSTORE
 *
 * Storage is per-contract persistent state stored in the world state trie.
 * Unlike memory (which is execution-local), storage persists between transactions.
 */

#ifndef ART_EVM_OPCODES_STORAGE_H
#define ART_EVM_OPCODES_STORAGE_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_SLOAD 0x54   // Load word from storage
#define OP_SSTORE 0x55  // Store word to storage

//==============================================================================
// Storage Opcodes
//==============================================================================

/**
 * SLOAD - Load word from storage
 * Stack: key => value
 * Gas: 100 (warm) / 2100 (cold)
 *
 * Loads a 256-bit word from the contract's persistent storage.
 * If the storage slot has never been written, returns zero.
 */
evm_status_t op_sload(evm_t *evm);

/**
 * SSTORE - Store word to storage
 * Stack: key value =>
 * Gas: Complex (depends on current/original values, see EIP-2200)
 *      - Setting non-zero to non-zero: 5000 (warm) / 2900 (cold reset) / 20000 (set)
 *      - Clearing storage: 5000 + 15000 refund
 *
 * Stores a 256-bit word to the contract's persistent storage.
 * Cannot be used in static calls (will return EVM_STATIC_CALL_VIOLATION).
 */
evm_status_t op_sstore(evm_t *evm);

/**
 * TLOAD - Load from transient storage (EIP-1153, Cancun+)
 * Stack: key => value
 * Gas: 100
 */
evm_status_t op_tload(evm_t *evm);

/**
 * TSTORE - Store to transient storage (EIP-1153, Cancun+)
 * Stack: key value =>
 * Gas: 100
 */
evm_status_t op_tstore(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_STORAGE_H */
