/**
 * EVM Contract Creation Opcodes — opcode definitions and shared helpers.
 * Opcode implementations are in opcodes/create.c, included into interpreter.c.
 */

#ifndef ART_EVM_OPCODES_CREATE_H
#define ART_EVM_OPCODES_CREATE_H

#include "../evm.h"

#define OP_CREATE 0xf0
#define OP_CREATE2 0xf5

/**
 * Calculate CREATE contract address: keccak256(rlp([sender, nonce]))[12:]
 * Used by both CREATE opcode and transaction-level contract creation.
 */
address_t calculate_create_address(const address_t *sender, uint64_t nonce);

#endif /* ART_EVM_OPCODES_CREATE_H */
