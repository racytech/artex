/**
 * EVM Control Flow Opcodes Implementation
 *
 * Simple control opcodes (STOP, JUMPDEST, PC, GAS) are inlined directly
 * into interpreter.c dispatch labels. This file contains the remaining
 * control flow opcodes that are still called as functions.
 */

#include "opcodes/control.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "uint256.h"
#include "gas.h"
#include "precompile.h"
#include "verkle_key.h"
#include "logger.h"
#include <string.h>
#include <stdlib.h>

//==============================================================================
// INVALID - Invalid instruction (always fails)
//==============================================================================

static evm_status_t op_invalid(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Consume all gas
    evm->gas_left = 0;

    return EVM_INVALID_OPCODE;
}

//==============================================================================
// Jump Operations
//==============================================================================

/**
 * Build a JUMPDEST bitmap for the given bytecode (one bit per byte offset).
 * Bits set to 1 indicate valid JUMPDEST positions (not inside PUSH data).
 * Caller must free() the returned bitmap.
 */
static uint8_t *build_jumpdest_bitmap(const uint8_t *code, size_t code_size)
{
    size_t bitmap_bytes = (code_size + 7) / 8;
    uint8_t *bitmap = (uint8_t *)calloc(bitmap_bytes, 1);
    if (!bitmap) return NULL;

    uint64_t pc = 0;
    while (pc < code_size) {
        uint8_t opcode = code[pc];
        if (opcode == OP_JUMPDEST) {
            bitmap[pc >> 3] |= (1u << (pc & 7));
        }
        if (opcode >= 0x60 && opcode <= 0x7f) {
            pc += 1 + (opcode - 0x60 + 1);  // skip PUSH data
        } else {
            pc++;
        }
    }
    return bitmap;
}

/**
 * O(1) jump destination validation using precomputed bitmap.
 */
static inline bool is_valid_jump_dest_bitmap(const uint8_t *bitmap,
                                              size_t code_size, uint64_t dest)
{
    if (dest >= code_size) return false;
    return (bitmap[dest >> 3] >> (dest & 7)) & 1;
}

/**
 * JUMP - Alter program counter
 * Stack: dest =>
 * Gas: 8
 */
static evm_status_t op_jump(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_MID))
    {
        return EVM_OUT_OF_GAS;
    }

    // Pop destination from stack
    uint256_t dest_256;
    if (!evm_stack_pop(evm->stack, &dest_256))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Destination must fit in code size — reject if high bits set
    if (dest_256.high != 0 || (uint64_t)(dest_256.low >> 64) != 0)
    {
        return EVM_INVALID_JUMP;
    }

    uint64_t dest = uint256_to_uint64(&dest_256);

    // Validate jump destination using precomputed bitmap (O(1))
    if (!is_valid_jump_dest_bitmap(evm->jumpdest_bitmap, evm->code_size, dest))
    {
        return EVM_INVALID_JUMP;
    }

    evm->pc = dest;
    return EVM_SUCCESS;
}

/**
 * JUMPI - Conditionally alter program counter
 * Stack: dest cond =>
 * Gas: 10
 */
static evm_status_t op_jumpi(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    if (!evm_use_gas(evm, GAS_HIGH))
        return EVM_OUT_OF_GAS;

    uint256_t dest_256, cond_256;

    if (!evm_stack_pop(evm->stack, &dest_256))
        return EVM_STACK_UNDERFLOW;

    if (!evm_stack_pop(evm->stack, &cond_256))
        return EVM_STACK_UNDERFLOW;

    if (uint256_is_zero(&cond_256)) {
        evm->pc++;
        return EVM_SUCCESS;
    }

    if (dest_256.high != 0 || (uint64_t)(dest_256.low >> 64) != 0)
        return EVM_INVALID_JUMP;

    uint64_t dest = uint256_to_uint64(&dest_256);

    if (!is_valid_jump_dest_bitmap(evm->jumpdest_bitmap, evm->code_size, dest))
        return EVM_INVALID_JUMP;

    evm->pc = dest;
    return EVM_SUCCESS;
}

//==============================================================================
// Return/Revert Opcodes
//==============================================================================

/**
 * RETURN - Halt execution and return output data
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
static evm_status_t op_return(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // Pop offset and size from stack
    uint256_t offset_256, size_256;

    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;

    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Check for uint256 overflow — impossibly large size means OOG
    if (size_256.high != 0 || (uint64_t)(size_256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    // Only check offset overflow when size > 0 (size=0 needs no memory access)
    if (!uint256_is_zero(&size_256) &&
        (offset_256.high != 0 || (uint64_t)(offset_256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, GAS_ZERO + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Free any existing return data
    if (evm->return_data)
    {
        free(evm->return_data);
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }

    // Handle zero size - no data to return
    if (size == 0)
    {
        evm->stopped = true;
        return EVM_SUCCESS;
    }

    // Expand memory if needed
    if (!evm_memory_expand(evm->memory, offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Allocate return data buffer and bulk-copy from memory
    evm->return_data = malloc(size);
    if (!evm->return_data)
        return EVM_INTERNAL_ERROR;

    if (!evm_memory_expand(evm->memory, offset, size)) {
        free(evm->return_data);
        evm->return_data = NULL;
        return EVM_INVALID_MEMORY_ACCESS;
    }
    memcpy(evm->return_data, evm->memory->data + offset, size);

    evm->return_data_size = size;
    evm->stopped = true;

    return EVM_SUCCESS;
}

/**
 * REVERT - Halt execution and revert state changes
 * Stack: offset size =>
 * Gas: 0 + memory_expansion_cost
 */
static evm_status_t op_revert(evm_t *evm)
{
    if (!evm)
        return EVM_INTERNAL_ERROR;

    // EIP-140: REVERT introduced in Byzantium
    if (evm->fork < FORK_BYZANTIUM)
        return EVM_INVALID_OPCODE;

    // Pop offset and size from stack
    uint256_t offset_256, size_256;

    if (!evm_stack_pop(evm->stack, &offset_256))
        return EVM_STACK_UNDERFLOW;

    if (!evm_stack_pop(evm->stack, &size_256))
        return EVM_STACK_UNDERFLOW;

    // Check for uint256 overflow — impossibly large size means OOG
    if (size_256.high != 0 || (uint64_t)(size_256.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    // Only check offset overflow when size > 0 (size=0 needs no memory access)
    if (!uint256_is_zero(&size_256) &&
        (offset_256.high != 0 || (uint64_t)(offset_256.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

    uint64_t offset = uint256_to_uint64(&offset_256);
    uint64_t size = uint256_to_uint64(&size_256);

    // Calculate memory expansion gas cost
    uint64_t mem_gas = evm_memory_access_cost(evm->memory, offset, size);
    if (!evm_use_gas(evm, GAS_ZERO + mem_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    // Free any existing return data
    if (evm->return_data)
    {
        free(evm->return_data);
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }

    // Handle zero size - no revert data
    if (size == 0)
    {
        evm->stopped = true;
        return EVM_REVERT;
    }

    // Expand memory if needed
    if (!evm_memory_expand(evm->memory, offset, size))
    {
        return EVM_INVALID_MEMORY_ACCESS;
    }

    // Allocate revert data buffer and bulk-copy from memory
    evm->return_data = malloc(size);
    if (!evm->return_data)
        return EVM_INTERNAL_ERROR;

    if (!evm_memory_expand(evm->memory, offset, size)) {
        free(evm->return_data);
        evm->return_data = NULL;
        return EVM_INVALID_MEMORY_ACCESS;
    }
    memcpy(evm->return_data, evm->memory->data + offset, size);

    evm->return_data_size = size;
    evm->stopped = true;

    return EVM_REVERT;
}

//==============================================================================
// SELFDESTRUCT Opcode
//==============================================================================

static evm_status_t op_selfdestruct(evm_t *evm)
{
    if (!evm || !evm->stack)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_DEBUG("SELFDESTRUCT: Cannot selfdestruct in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop beneficiary address from stack
    uint256_t beneficiary;
    if (!evm_stack_pop(evm->stack, &beneficiary))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert beneficiary from uint256 to address
    address_t beneficiary_addr;
    address_from_uint256(&beneficiary, &beneficiary_addr);

    // Get current contract balance
    uint256_t balance = evm_state_get_balance(evm->state, &evm->msg.recipient);

    // Calculate gas cost - fork-dependent
    uint64_t gas_cost = 0;

    if (evm->fork >= FORK_VERKLE)
    {
        // EIP-4762: base cost (5000) + witness gas for self-destruct
        gas_cost = 5000;
        bool balance_is_zero = uint256_is_zero(&balance);
        bool same_addr = (memcmp(evm->msg.recipient.bytes, beneficiary_addr.bytes, 20) == 0);

        // Read access for contract basic_data (always)
        uint8_t vk[32];
        verkle_account_basic_data_key(vk, evm->msg.recipient.bytes);
        gas_cost += evm_state_witness_gas_access(evm->state, vk, false, false);

        // If beneficiary is precompile/systemContract AND balance is zero, skip
        static const uint8_t SYSTEM_ADDR[20] = {
            0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,
            0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xfe
        };
        bool ben_is_precompile = is_precompile(&beneficiary_addr, evm->fork);
        bool ben_is_system = (memcmp(beneficiary_addr.bytes, SYSTEM_ADDR, 20) == 0);
        if ((ben_is_precompile || ben_is_system) && balance_is_zero) {
            // Only contract read access charged
        } else {
            // Read beneficiary basic_data (even when balance=0)
            if (!same_addr) {
                uint8_t vk_ben[32];
                verkle_account_basic_data_key(vk_ben, beneficiary_addr.bytes);
                gas_cost += evm_state_witness_gas_access(evm->state, vk_ben, false, false);
            }

            // Write access if transferring balance
            if (!balance_is_zero) {
                gas_cost += evm_state_witness_gas_access(evm->state, vk, true, false);
                if (!same_addr) {
                    if (evm_state_exists(evm->state, &beneficiary_addr)) {
                        uint8_t vk_ben[32];
                        verkle_account_basic_data_key(vk_ben, beneficiary_addr.bytes);
                        gas_cost += evm_state_witness_gas_access(evm->state, vk_ben, true, false);
                    } else {
                        // AddAccount: basic_data + code_hash write
                        uint8_t vk_bd[32], vk_ch[32];
                        verkle_account_basic_data_key(vk_bd, beneficiary_addr.bytes);
                        verkle_account_code_hash_key(vk_ch, beneficiary_addr.bytes);
                        gas_cost += evm_state_witness_gas_access(evm->state, vk_bd, true, false);
                        gas_cost += evm_state_witness_gas_access(evm->state, vk_ch, true, false);
                    }
                }
            }
        }
    }
    else if (evm->fork >= FORK_TANGERINE_WHISTLE)
    {
        gas_cost = 5000;  // EIP-150 base cost

        // Berlin+ (EIP-2929): Add cold account access cost
        if (evm->fork >= FORK_BERLIN)
        {
            bool is_warm = evm_is_address_warm(evm, &beneficiary_addr);
            if (!is_warm)
            {
                gas_cost += GAS_COLD_ACCOUNT_ACCESS;  // 2600
                evm_mark_address_warm(evm, &beneficiary_addr);
            }
        }

        // EIP-3529 (London+): skip new account cost if contract already self-destructed
        bool skip_new_account = false;
        if (evm->fork >= FORK_LONDON)
        {
            skip_new_account = evm_state_is_self_destructed(evm->state, &evm->msg.recipient);
        }

        if (!skip_new_account && !uint256_is_zero(&balance))
        {
            // Spurious Dragon+ (EIP-161): 25000 if beneficiary is empty
            // Pre-Spurious Dragon (EIP-150): 25000 if beneficiary doesn't exist
            bool charge_new_account;
            if (evm->fork >= FORK_SPURIOUS_DRAGON)
            {
                charge_new_account = evm_state_is_empty(evm->state, &beneficiary_addr);
            }
            else
            {
                charge_new_account = !evm_state_exists(evm->state, &beneficiary_addr);
            }

            if (charge_new_account)
            {
                gas_cost += 25000;
            }
        }
    }
    // Pre-TW: SELFDESTRUCT costs 0 gas

    if (!evm_use_gas(evm, gas_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    // EIP-6780 (Cancun+): SELFDESTRUCT only deletes if created in same tx
    bool created_in_tx = evm_state_is_created(evm->state, &evm->msg.recipient);
    bool do_delete = (evm->fork < FORK_CANCUN) || created_in_tx;

    // Gas refund: pre-EIP-3529 (pre-London) gives 24000 refund
    // Only if this account hasn't already been scheduled for deletion in this tx
    // Must check BEFORE marking self-destruct below
    if (evm->fork < FORK_LONDON)
    {
        if (!evm_state_is_self_destructed(evm->state, &evm->msg.recipient))
        {
            evm->gas_refund += 24000;
        }
    }

    // Transfer balance: add to beneficiary, subtract from self.
    // For self-send (beneficiary == self), add + sub cancel out → balance unchanged.
    // This is correct for Cancun EIP-6780 where pre-existing contracts survive
    // SELFDESTRUCT and self-send should NOT burn ether.
    evm_state_add_balance(evm->state, &beneficiary_addr, &balance);
    evm_state_sub_balance(evm->state, &evm->msg.recipient, &balance);

    if (do_delete)
    {
        // Account will be destroyed: zero balance (burns ether on self-send)
        uint256_t zero = UINT256_ZERO;
        evm_state_set_balance(evm->state, &evm->msg.recipient, &zero);
        // Mark contract for deletion (pre-Cancun, or created in same tx)
        evm_state_self_destruct(evm->state, &evm->msg.recipient);
    }

    LOG_EVM_DEBUG("SELFDESTRUCT: contract deleted, balance transferred");

    // Halt execution
    evm->stopped = true;

    return EVM_SUCCESS;
}
