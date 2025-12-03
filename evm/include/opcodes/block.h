/**
 * EVM Block Information Opcodes
 *
 * Implements opcodes that provide information about the current block:
 * - BLOCKHASH, COINBASE, TIMESTAMP, NUMBER, DIFFICULTY/PREVRANDAO
 * - GASLIMIT, CHAINID, SELFBALANCE, BASEFEE
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
// Opcode Implementations
//==============================================================================

/**
 * BLOCKHASH - Get hash of one of the 256 most recent blocks
 * Stack: blockNumber => hash (or 0 if out of range)
 * Gas: 20
 */
evm_status_t op_blockhash(evm_t *evm);

/**
 * COINBASE - Get block beneficiary address
 * Stack: => coinbase
 * Gas: 2
 */
evm_status_t op_coinbase(evm_t *evm);

/**
 * TIMESTAMP - Get block timestamp
 * Stack: => timestamp
 * Gas: 2
 */
evm_status_t op_timestamp(evm_t *evm);

/**
 * NUMBER - Get block number
 * Stack: => number
 * Gas: 2
 */
evm_status_t op_number(evm_t *evm);

/**
 * DIFFICULTY - Get block difficulty (pre-merge) or PREVRANDAO (post-merge)
 * Stack: => difficulty
 * Gas: 2
 */
evm_status_t op_difficulty(evm_t *evm);

/**
 * GASLIMIT - Get block gas limit
 * Stack: => gas_limit
 * Gas: 2
 */
evm_status_t op_gaslimit(evm_t *evm);

/**
 * CHAINID - Get chain ID
 * Stack: => chain_id
 * Gas: 2
 */
evm_status_t op_chainid(evm_t *evm);

/**
 * SELFBALANCE - Get balance of current contract
 * Stack: => balance
 * Gas: 5
 */
evm_status_t op_selfbalance(evm_t *evm);

/**
 * BASEFEE - Get base fee (EIP-1559)
 * Stack: => base_fee
 * Gas: 2
 */
evm_status_t op_basefee(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_BLOCK_H */
