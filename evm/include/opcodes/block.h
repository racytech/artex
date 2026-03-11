/**
 * EVM Block Information Opcodes
 *
 * Simple block opcodes (COINBASE, TIMESTAMP, NUMBER, DIFFICULTY, GASLIMIT,
 * CHAINID, BASEFEE) are inlined directly into interpreter.c dispatch labels.
 *
 * This header declares the remaining opcodes that are still called as functions.
 */

#ifndef ART_EVM_OPCODES_BLOCK_H
#define ART_EVM_OPCODES_BLOCK_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_BLOCKHASH 0x40   // Hash of a recent block
#define OP_COINBASE 0x41    // Beneficiary address
#define OP_TIMESTAMP 0x42   // Block timestamp
#define OP_NUMBER 0x43      // Block number
#define OP_DIFFICULTY 0x44  // Block difficulty (pre-merge) / PREVRANDAO (post-merge)
#define OP_GASLIMIT 0x45    // Block gas limit
#define OP_CHAINID 0x46     // Chain ID
#define OP_SELFBALANCE 0x47 // Balance of current contract
#define OP_BASEFEE 0x48     // Base fee (EIP-1559)

//==============================================================================
// Opcode Implementations (remaining non-inlined functions)
//==============================================================================

evm_status_t op_blockhash(evm_t *evm);
evm_status_t op_selfbalance(evm_t *evm);
evm_status_t op_blobhash(evm_t *evm);
evm_status_t op_blobbasefee(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_BLOCK_H */
