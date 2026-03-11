/**
 * EVM Block Information Opcodes Implementation
 *
 * Simple block opcodes (COINBASE, TIMESTAMP, NUMBER, DIFFICULTY, GASLIMIT,
 * CHAINID, BASEFEE) are inlined directly into interpreter.c dispatch labels.
 *
 * This file contains the remaining opcodes that are still called as functions.
 */

#include "opcodes/block.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "uint256.h"
#include "address.h"
#include "gas.h"
#include "hash.h"
#include "verkle_key.h"
#include <string.h>

//==============================================================================
// BLOCKHASH - Get hash of one of the 256 most recent blocks
//==============================================================================

static evm_status_t op_blockhash(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, 20))
    {
        return EVM_OUT_OF_GAS;
    }

    // Pop block number from stack
    uint256_t block_number;
    if (!evm_stack_pop(evm->stack, &block_number))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Check if block number is within valid range (last 256 blocks)
    uint256_t current_block = uint256_from_uint64(evm->block.number);

    // If requested block is >= current block, return 0
    if (uint256_ge(&block_number, &current_block))
    {
        if (!evm_stack_push(evm->stack, &UINT256_ZERO))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // If block is too old (>256 blocks ago), return 0
    uint256_t diff = uint256_sub(&current_block, &block_number);
    uint256_t max_depth = uint256_from_uint64(256);

    if (uint256_gt(&diff, &max_depth))
    {
        if (!evm_stack_push(evm->stack, &UINT256_ZERO))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // EIP-7709 (Verkle): read block hash from the history storage contract
    // at 0xff..fe, slot = block_number % 8192. Charges witness gas for
    // the storage slot access.
    if (evm->fork >= FORK_VERKLE) {
        #define BLOCKHASH_SERVE_WINDOW 8192
        static const uint8_t HISTORY_ADDR[20] = {
            0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
            0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xfe
        };
        uint64_t block_idx = uint256_to_uint64(&block_number);
        uint64_t slot_idx = block_idx % BLOCKHASH_SERVE_WINDOW;
        uint256_t slot = uint256_from_uint64(slot_idx);

        // Charge witness gas for the storage slot read
        address_t hist_addr;
        memcpy(hist_addr.bytes, HISTORY_ADDR, 20);
        uint8_t slot_le[32], vk[32];
        uint256_to_bytes_le(&slot, slot_le);
        verkle_storage_key(vk, HISTORY_ADDR, slot_le);
        uint64_t wgas = evm_state_witness_gas_access(evm->state, vk, false, false);
        if (wgas == 0) wgas = GAS_SLOAD_WARM;  /* warm storage read */
        if (!evm_use_gas(evm, wgas))
            return EVM_OUT_OF_GAS;

        // Read the block hash from contract storage
        uint256_t hash_value = evm_state_get_storage(evm->state, &hist_addr, &slot);
        if (!evm_stack_push(evm->stack, &hash_value))
            return EVM_STACK_OVERFLOW;
        #undef BLOCKHASH_SERVE_WINDOW
        return EVM_SUCCESS;
    }

    // Pre-Verkle: get block hash from block context cache
    uint64_t block_idx = uint256_to_uint64(&block_number);
    uint64_t hash_idx = block_idx % 256;

    uint256_t hash_value = uint256_from_bytes(evm->block.block_hash[hash_idx].bytes, 32);

    if (!evm_stack_push(evm->stack, &hash_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// SELFBALANCE - Get balance of current contract
//==============================================================================

static evm_status_t op_selfbalance(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // EIP-1884: SELFBALANCE introduced in Istanbul
    if (evm->fork < FORK_ISTANBUL)
        return EVM_INVALID_OPCODE;

    if (!evm_use_gas(evm, GAS_LOW))
    {
        return EVM_OUT_OF_GAS;
    }

    // Get balance of current contract (msg.recipient)
    uint256_t balance = evm_state_get_balance(evm->state, &evm->msg.recipient);

    if (!evm_stack_push(evm->stack, &balance))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// BLOBHASH - Get versioned hash at index (EIP-4844, Cancun+)
//==============================================================================

static evm_status_t op_blobhash(evm_t *evm)
{
    if (!evm || !evm->stack)
        return EVM_INTERNAL_ERROR;

    if (evm->fork < FORK_CANCUN)
        return EVM_INVALID_OPCODE;

    uint256_t index;
    if (!evm_stack_pop(evm->stack, &index))
        return EVM_STACK_UNDERFLOW;

    if (!evm_use_gas(evm, GAS_VERY_LOW))
        return EVM_OUT_OF_GAS;

    // EIP-4844: return versioned hash at index, or 0 if out of bounds
    uint256_t result = UINT256_ZERO;
    uint64_t idx = uint256_to_uint64(&index);

    // Check if index is within range (if index > UINT64_MAX, high bits nonzero → skip)
    uint256_t idx_check = uint256_from_uint64(idx);
    if (uint256_is_equal(&index, &idx_check) && idx < evm->tx.blob_hashes_count && evm->tx.blob_hashes) {
        // Convert hash_t (big-endian bytes) to uint256_t
        result = uint256_from_bytes(evm->tx.blob_hashes[idx].bytes, HASH_SIZE);
    }

    if (!evm_stack_push(evm->stack, &result))
        return EVM_STACK_OVERFLOW;

    return EVM_SUCCESS;
}

//==============================================================================
// BLOBBASEFEE - Get blob base fee (EIP-7516, Cancun+)
//==============================================================================

static evm_status_t op_blobbasefee(evm_t *evm)
{
    if (!evm || !evm->stack)
        return EVM_INTERNAL_ERROR;

    if (evm->fork < FORK_CANCUN)
        return EVM_INVALID_OPCODE;

    if (!evm_use_gas(evm, GAS_BASE))
        return EVM_OUT_OF_GAS;

    // EIP-4844: push blob base fee from block environment
    uint256_t blob_base_fee = evm->block.blob_base_fee;
    if (!evm_stack_push(evm->stack, &blob_base_fee))
        return EVM_STACK_OVERFLOW;

    return EVM_SUCCESS;
}
