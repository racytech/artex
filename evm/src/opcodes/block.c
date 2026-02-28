/**
 * EVM Block Information Opcodes Implementation
 */

#include "opcodes/block.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "uint256.h"
#include "address.h"
#include "gas.h"
#include "hash.h"
#include <string.h>

//==============================================================================
// Block Information Opcodes
//==============================================================================

/**
 * BLOCKHASH - Get hash of one of the 256 most recent blocks
 * Stack: blockNumber => hash (or 0 if out of range)
 * Gas: 20
 */
evm_status_t op_blockhash(evm_t *evm)
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
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // If block is too old (>256 blocks ago), return 0
    uint256_t diff = uint256_sub(&current_block, &block_number);
    uint256_t max_depth = uint256_from_uint64(256);
    
    if (uint256_gt(&diff, &max_depth))
    {
        if (!evm_stack_push(evm->stack, &UINT256_ZERO))
        {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }

    // Get block hash from block context
    // block_number is within range [current - 256, current)
    uint64_t block_idx = uint256_to_uint64(&block_number);
    uint64_t hash_idx = block_idx % 256;
    
    // Convert hash to uint256
    uint256_t hash_value;
    memset(&hash_value, 0, sizeof(uint256_t));
    memcpy(&hash_value, &evm->block.block_hash[hash_idx], sizeof(hash_t) < sizeof(uint256_t) ? sizeof(hash_t) : sizeof(uint256_t));

    if (!evm_stack_push(evm->stack, &hash_value))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * COINBASE - Get block beneficiary address
 * Stack: => coinbase
 * Gas: 2
 */
evm_status_t op_coinbase(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    // Convert address to uint256 and push to stack
    uint256_t coinbase;
    address_to_uint256(&evm->block.coinbase, &coinbase);

    if (!evm_stack_push(evm->stack, &coinbase))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * TIMESTAMP - Get block timestamp
 * Stack: => timestamp
 * Gas: 2
 */
evm_status_t op_timestamp(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t timestamp = uint256_from_uint64(evm->block.timestamp);

    if (!evm_stack_push(evm->stack, &timestamp))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * NUMBER - Get block number
 * Stack: => number
 * Gas: 2
 */
evm_status_t op_number(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t number = uint256_from_uint64(evm->block.number);

    if (!evm_stack_push(evm->stack, &number))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * DIFFICULTY - Get block difficulty (pre-merge) or PREVRANDAO (post-merge)
 * Stack: => difficulty
 * Gas: 2
 */
evm_status_t op_difficulty(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_push(evm->stack, &evm->block.difficulty))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * GASLIMIT - Get block gas limit
 * Stack: => gas_limit
 * Gas: 2
 */
evm_status_t op_gaslimit(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    uint256_t gas_limit = uint256_from_uint64(evm->block.gas_limit);

    if (!evm_stack_push(evm->stack, &gas_limit))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * CHAINID - Get chain ID
 * Stack: => chain_id
 * Gas: 2
 */
evm_status_t op_chainid(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_push(evm->stack, &evm->block.chain_id))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}

/**
 * SELFBALANCE - Get balance of current contract
 * Stack: => balance
 * Gas: 5
 */
evm_status_t op_selfbalance(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

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

/**
 * BASEFEE - Get base fee (EIP-1559)
 * Stack: => base_fee
 * Gas: 2
 */
evm_status_t op_basefee(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_BASE))
    {
        return EVM_OUT_OF_GAS;
    }

    if (!evm_stack_push(evm->stack, &evm->block.base_fee))
    {
        return EVM_STACK_OVERFLOW;
    }

    return EVM_SUCCESS;
}
