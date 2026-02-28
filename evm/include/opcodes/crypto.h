/**
 * EVM Cryptographic Opcodes
 *
 * Implements cryptographic hash functions:
 * - KECCAK256 (SHA3)
 */

#ifndef ART_EVM_OPCODES_CRYPTO_H
#define ART_EVM_OPCODES_CRYPTO_H

#include "../evm.h"

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Opcode Definitions
//==============================================================================

#define OP_KECCAK256 0x20 // Keccak-256 hash

//==============================================================================
// Opcode Implementations
//==============================================================================

/**
 * KECCAK256 - Compute Keccak-256 hash
 * Stack: offset size => hash
 * Memory: Read from [offset:offset+size]
 * Gas: 30 + 6 * (size in words) + memory_expansion_cost
 */
evm_status_t op_keccak256(evm_t *evm);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_OPCODES_CRYPTO_H */
