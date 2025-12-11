/**
 * EVM Contract Creation Opcodes
 *
 * Implements contract creation operations:
 * - CREATE
 * - CREATE2
 */

#ifndef ART_EVM_OPCODES_CREATE_H
#define ART_EVM_OPCODES_CREATE_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_CREATE 0xf0       // Create contract
#define OP_CREATE2 0xf5      // Create contract with deterministic address

//==============================================================================
// Creation Opcodes
//==============================================================================

/**
 * CREATE - Create new contract
 * Stack: value offset size => address
 * Gas: 32000 + deployment gas + memory_expansion_cost
 */
evm_status_t op_create(evm_t *evm);

/**
 * CREATE2 - Create new contract with deterministic address
 * Stack: value offset size salt => address
 * Gas: 32000 + deployment gas + memory_expansion_cost + keccak256 cost
 */
evm_status_t op_create2(evm_t *evm);

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate CREATE contract address: keccak256(rlp([sender, nonce]))[12:]
 * Used by both CREATE opcode and transaction-level contract creation
 */
address_t calculate_create_address(const address_t *sender, uint64_t nonce);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_CREATE_H */
