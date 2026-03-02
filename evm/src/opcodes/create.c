/*
 * EVM CREATE Opcodes Implementation
 * Handles CREATE and CREATE2 contract creation opcodes
 */

#include "opcodes/create.h"
#include "evm.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "gas.h"
#include "logger.h"
#include "uint256.h"
#include "address.h"
#include "hash.h"
#include "rlp.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate CREATE contract address: keccak256(rlp([sender, nonce]))
 */
address_t calculate_create_address(const address_t *sender, uint64_t nonce)
{
    // Create RLP list [sender, nonce]
    rlp_item_t *list = rlp_list_new();
    rlp_item_t *sender_item = rlp_string(sender->bytes, ADDRESS_SIZE);
    rlp_item_t *nonce_item = rlp_uint64(nonce);
    
    rlp_list_append(list, sender_item);
    rlp_list_append(list, nonce_item);
    
    // Encode to bytes
    bytes_t encoded = rlp_encode(list);
    
    // Hash the encoded data
    hash_t hash = hash_keccak256(encoded.data, encoded.len);
    
    // Take last 20 bytes as address
    address_t result;
    memcpy(result.bytes, hash.bytes + 12, ADDRESS_SIZE);
    
    // Cleanup
    bytes_free(&encoded);
    rlp_item_free(list);
    
    return result;
}

/**
 * Calculate CREATE2 contract address: keccak256(0xff ++ sender ++ salt ++ keccak256(init_code))
 */
static address_t calculate_create2_address(const address_t *sender, const uint256_t *salt, 
                                           const uint8_t *init_code, size_t init_code_size)
{
    // Hash the init code
    hash_t init_code_hash = hash_keccak256(init_code, init_code_size);
    
    // Build: 0xff ++ sender(20) ++ salt(32) ++ init_code_hash(32) = 85 bytes
    uint8_t data[85];
    data[0] = 0xff;
    memcpy(data + 1, sender->bytes, ADDRESS_SIZE);
    
    // Copy salt as bytes (uint256_t is stored in little-endian format)
    // We need to convert it to big-endian for hashing
    const uint8_t *salt_bytes = (const uint8_t*)salt;
    for (int i = 0; i < 32; i++) {
        data[21 + i] = salt_bytes[31 - i];  // Reverse byte order
    }
    
    memcpy(data + 53, init_code_hash.bytes, 32);
    
    // Hash the combined data
    hash_t hash = hash_keccak256(data, 85);
    
    // Take last 20 bytes as address
    address_t result;
    memcpy(result.bytes, hash.bytes + 12, ADDRESS_SIZE);
    
    return result;
}

// EIP-170: Maximum contract code size (Spurious Dragon+)
#define MAX_CODE_SIZE 24576

// EIP-3860 (Shanghai+): Maximum initcode size and per-word gas cost
#define MAX_INITCODE_SIZE (2 * MAX_CODE_SIZE)  // 49152
#define INITCODE_WORD_GAS 2

/**
 * Common contract creation logic shared between CREATE and CREATE2
 * @param sender_nonce Current nonce of the sender (will be incremented on success)
 */
static evm_status_t execute_create(evm_t *evm,
                                   const uint256_t *value,
                                   const address_t *contract_addr,
                                   uint8_t *init_code,
                                   uint64_t init_code_size,
                                   evm_call_type_t msg_kind,
                                   uint64_t sender_nonce)
{
    //==========================================================================
    // Depth Check
    //==========================================================================

    if (evm->msg.depth >= 1024)
    {
        LOG_EVM_DEBUG("CREATE: Call depth limit exceeded");
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    //==========================================================================
    // Balance Check (before any state changes)
    //==========================================================================

    if (!uint256_is_zero(value))
    {
        uint256_t sender_balance = evm_state_get_balance(evm->state, &evm->msg.recipient);
        if (uint256_lt(&sender_balance, value))
        {
            LOG_EVM_DEBUG("CREATE: Insufficient balance for value transfer");
            if (init_code) free(init_code);
            uint256_t zero = UINT256_ZERO;
            if (!evm_stack_push(evm->stack, &zero))
                return EVM_STACK_OVERFLOW;
            return EVM_SUCCESS;
        }
    }

    // Increment sender nonce BEFORE snapshot — persists even if CREATE fails
    evm_state_set_nonce(evm->state, &evm->msg.recipient, sender_nonce + 1);

    //==========================================================================
    // Collision Detection (EIP-7610)
    //==========================================================================

    // EIP-7610: collision if target has code, non-zero nonce, or storage
    uint64_t target_nonce = evm_state_get_nonce(evm->state, contract_addr);
    uint32_t target_code_size = evm_state_get_code_size(evm->state, contract_addr);
    bool target_has_storage = evm_state_has_storage(evm->state, contract_addr);
    if (target_nonce > 0 || target_code_size > 0 || target_has_storage)
    {
        LOG_EVM_DEBUG("CREATE: Address collision detected");
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // Take snapshot AFTER nonce increment but BEFORE account creation/value transfer
    // Revert undoes contract creation and value transfer but preserves sender nonce
    uint32_t snapshot = evm_state_snapshot(evm->state);

    //==========================================================================
    // Contract Account Initialization
    //==========================================================================

    // Create/reset the contract account
    evm_state_create_account(evm->state, contract_addr);

    // EIP-161 (Spurious Dragon+): new contracts get nonce=1
    if (evm->fork >= FORK_SPURIOUS_DRAGON)
    {
        evm_state_set_nonce(evm->state, contract_addr, 1);
    }

    // EIP-2929 (Berlin+): mark contract address as warm
    if (evm->fork >= FORK_BERLIN)
    {
        evm_mark_address_warm(evm, contract_addr);
    }

    //==========================================================================
    // Value Transfer
    //==========================================================================

    if (!uint256_is_zero(value))
    {
        evm_state_sub_balance(evm->state, &evm->msg.recipient, value);
        evm_state_add_balance(evm->state, contract_addr, value);
    }

    //==========================================================================
    // Execute Init Code (or skip if no init code)
    //==========================================================================

    bool success;

    if (init_code_size > 0)
    {
        // Gas Forwarding: EIP-150 (Tangerine Whistle+) uses 63/64 rule
        // Pre-TW: all remaining gas is forwarded to init code
        uint64_t gas_forwarded;
        if (evm->fork >= FORK_TANGERINE_WHISTLE)
        {
            gas_forwarded = gas_max_call_gas(evm->gas_left);
        }
        else
        {
            gas_forwarded = evm->gas_left;
        }
        evm->gas_left -= gas_forwarded;

        evm_message_t init_msg = {
            .kind = msg_kind,
            .caller = evm->msg.recipient,
            .recipient = *contract_addr,
            .code_addr = *contract_addr,
            .value = *value,
            .input_data = init_code,
            .input_size = init_code_size,
            .gas = gas_forwarded,
            .depth = evm->msg.depth + 1,
            .is_static = false
        };

        evm_result_t init_result;
        evm_execute(evm, &init_msg, &init_result);

        // Free init code
        free(init_code);

        // Handle result
        success = (init_result.status == EVM_SUCCESS);

        if (success && init_result.output_size > 0)
        {
            // EIP-170 (Spurious Dragon+): max code size check
            if (evm->fork >= FORK_SPURIOUS_DRAGON &&
                init_result.output_size > MAX_CODE_SIZE)
            {
                success = false;
            }
            // EIP-3541 (London+): reject code starting with 0xEF
            else if (evm->fork >= FORK_LONDON &&
                     init_result.output_data[0] == 0xEF)
            {
                success = false;
            }
            else
            {
                // Deployment gas: 200 per byte, charged from init code's remaining gas
                uint64_t deployment_gas = init_result.output_size * 200;
                if (init_result.gas_left >= deployment_gas)
                {
                    init_result.gas_left -= deployment_gas;
                    evm_state_set_code(evm->state, contract_addr,
                                       init_result.output_data,
                                       (uint32_t)init_result.output_size);
                }
                else
                {
                    // Code deposit OOG
                    // Frontier: leave account, return remaining gas (no revert)
                    // Homestead+: consume all gas, revert state
                    if (evm->fork >= FORK_HOMESTEAD)
                    {
                        init_result.gas_left = 0;
                        success = false;
                    }
                    // else: Frontier — success stays true, account persists with no code
                }
            }
        }

        if (!success && init_result.status == EVM_SUCCESS &&
            evm->fork >= FORK_HOMESTEAD)
        {
            // Homestead+: failed deployment consumes all remaining gas
            init_result.gas_left = 0;
        }

        // Return remaining gas to parent
        evm->gas_left += init_result.gas_left;

        // Propagate gas refund from init code on success
        if (success)
        {
            evm->gas_refund += init_result.gas_refund;
        }

        if (init_result.output_data) {
            free(init_result.output_data);
        }

        // CREATE return data handling:
        // - Success: return_data = empty (deployed code is NOT return data)
        // - REVERT: return_data = revert data from initcode (keep it)
        // - Other error (OOG etc.): return_data = empty
        if (init_result.status != EVM_REVERT) {
            if (evm->return_data) {
                free(evm->return_data);
                evm->return_data = NULL;
            }
            evm->return_data_size = 0;
        }
    }
    else
    {
        // No init code — create empty contract
        success = true;
    }

    //==========================================================================
    // Commit or Revert State Changes
    //==========================================================================

    if (!success) {
        evm_state_revert(evm->state, snapshot);
    }

    //==========================================================================
    // Push Result
    //==========================================================================

    uint256_t result = UINT256_ZERO;
    if (success) {
        address_to_uint256(contract_addr, &result);
    }

    if (!evm_stack_push(evm->stack, &result))
        return EVM_STACK_OVERFLOW;

    return EVM_SUCCESS;
}

//==============================================================================
// CREATE Opcodes
//==============================================================================

/**
 * CREATE - Create new contract
 * Stack: value offset size => address
 */
evm_status_t op_create(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory || !evm->state)
        return EVM_INTERNAL_ERROR;

    if (evm->msg.is_static)
        return EVM_STATIC_CALL_VIOLATION;

    // Pop 3 arguments: value offset size (top to bottom)
    uint256_t value, offset, size;

    if (!evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &size))
        return EVM_STACK_UNDERFLOW;

    uint64_t size_u64 = uint256_to_uint64(&size);
    uint64_t offset_u64 = uint256_to_uint64(&offset);

    //==========================================================================
    // Gas Calculation
    //==========================================================================

    // EIP-3860 (Shanghai+): reject initcode exceeding max size
    if (evm->fork >= FORK_SHANGHAI && size_u64 > MAX_INITCODE_SIZE)
    {
        return EVM_OUT_OF_GAS;
    }

    uint64_t base_cost = 32000;
    uint64_t mem_expansion_cost = 0;

    if (size_u64 > 0) {
        mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, offset_u64 + size_u64);
    }

    // EIP-3860 (Shanghai+): initcode word gas (2 gas per 32-byte word)
    uint64_t initcode_cost = 0;
    if (evm->fork >= FORK_SHANGHAI && size_u64 > 0) {
        uint64_t words = (size_u64 + 31) / 32;
        initcode_cost = INITCODE_WORD_GAS * words;
    }

    if (!evm_use_gas(evm, base_cost + mem_expansion_cost + initcode_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Address Calculation
    //==========================================================================

    uint64_t nonce = evm_state_get_nonce(evm->state, &evm->msg.recipient);

    address_t contract_addr = calculate_create_address(&evm->msg.recipient, nonce);
    
    // Note: sender nonce will be incremented inside execute_create AFTER snapshot
    
    //==========================================================================
    // Extract Init Code
    //==========================================================================

    uint8_t *init_code = NULL;
    if (size_u64 > 0)
    {
        init_code = malloc(size_u64);
        if (!init_code)
        {
            LOG_EVM_ERROR("CREATE: Failed to allocate init code");
            return EVM_INTERNAL_ERROR;
        }

        if (!evm_memory_read(evm->memory, offset_u64, init_code, size_u64))
        {
            LOG_EVM_ERROR("CREATE: Failed to read init code from memory");
            free(init_code);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    //==========================================================================
    // Execute Contract Creation
    //==========================================================================

    return execute_create(evm, &value, &contract_addr, init_code, size_u64, EVM_CREATE, nonce);
}

/**
 * CREATE2 - Create new contract with deterministic address
 * Stack: value offset size salt => address
 */
evm_status_t op_create2(evm_t *evm)
{
    if (!evm || !evm->stack || !evm->memory || !evm->state)
    {
        return EVM_INTERNAL_ERROR;
    }

    // EIP-1014: CREATE2 introduced in Constantinople
    if (evm->fork < FORK_CONSTANTINOPLE)
        return EVM_INVALID_OPCODE;

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("CREATE2: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 4 arguments: value offset size salt (top to bottom)
    uint256_t value, offset, size, salt;

    if (!evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &salt))
        return EVM_STACK_UNDERFLOW;

    // Convert to uint64_t
    uint64_t size_u64 = uint256_to_uint64(&size);
    uint64_t offset_u64 = uint256_to_uint64(&offset);

    // EIP-3860 (Shanghai+): reject initcode exceeding max size
    if (evm->fork >= FORK_SHANGHAI && size_u64 > MAX_INITCODE_SIZE)
    {
        return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Gas Calculation
    //==========================================================================

    // Base cost: 32000 gas
    uint64_t base_cost = 32000;

    // Memory expansion cost
    uint64_t mem_expansion_cost = 0;
    if (size_u64 > 0) {
        mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, offset_u64 + size_u64);
    }

    // CREATE2-specific: keccak256 cost for hashing init code
    // 6 gas per word (32 bytes)
    uint64_t keccak_cost = 0;
    if (size_u64 > 0) {
        uint64_t words = (size_u64 + 31) / 32;  // Round up
        keccak_cost = 6 * words;
    }

    // EIP-3860 (Shanghai+): initcode word gas (2 gas per 32-byte word)
    uint64_t initcode_cost = 0;
    if (evm->fork >= FORK_SHANGHAI && size_u64 > 0) {
        uint64_t words = (size_u64 + 31) / 32;
        initcode_cost = INITCODE_WORD_GAS * words;
    }

    uint64_t total_gas = base_cost + mem_expansion_cost + keccak_cost + initcode_cost;

    // Deduct upfront gas
    if (!evm_use_gas(evm, total_gas))
    {
        return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Extract Init Code
    //==========================================================================

    uint8_t *init_code = NULL;
    if (size_u64 > 0)
    {
        init_code = malloc(size_u64);
        if (!init_code)
        {
            LOG_EVM_ERROR("CREATE2: Failed to allocate init code");
            return EVM_INTERNAL_ERROR;
        }

        if (!evm_memory_read(evm->memory, offset_u64, init_code, size_u64))
        {
            LOG_EVM_ERROR("CREATE2: Failed to read init code from memory");
            free(init_code);
            return EVM_INVALID_MEMORY_ACCESS;
        }
    }

    //==========================================================================
    // Address Calculation (CREATE2-specific)
    //==========================================================================

    address_t contract_addr = calculate_create2_address(&evm->msg.recipient, &salt, 
                                                        init_code, size_u64);

    // Get sender nonce for incrementing inside execute_create
    uint64_t nonce = evm_state_get_nonce(evm->state, &evm->msg.recipient);

    //==========================================================================
    // Execute Contract Creation
    //==========================================================================

    return execute_create(evm, &value, &contract_addr, init_code, size_u64, EVM_CREATE2, nonce);
}
