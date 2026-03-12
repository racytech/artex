/*
 * EVM CREATE Opcodes Implementation
 * Handles CREATE and CREATE2 contract creation opcodes
 */

#include "opcodes/create.h"
#include "evm.h"
#include "evm_state.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "evm_tracer.h"
#include "gas.h"
#include "verkle_key.h"
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

    // EIP-2681: nonce overflow check — CREATE fails if nonce >= 2^64-1
    if (sender_nonce >= UINT64_MAX)
    {
        LOG_EVM_DEBUG("CREATE: Sender nonce at maximum (EIP-2681)");
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero))
            return EVM_STACK_OVERFLOW;
        return EVM_SUCCESS;
    }

    // Increment sender nonce BEFORE snapshot — persists even if CREATE fails
    evm_state_set_nonce(evm->state, &evm->msg.recipient, sender_nonce + 1);

    //==========================================================================
    // Gas Forwarding (moved before witness gas, matching go-ethereum)
    //==========================================================================
    // In go-ethereum, gas forwarding (63/64 rule) happens in the opcode handler
    // BEFORE evm.create() charges PreCheckGas/InitGas. This affects the 1/64
    // retained by the parent, so we must do it in the same order.

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

    // Helper macro: fail CREATE, consume all forwarded gas, push 0
#define CREATE_FAIL_PUSH_ZERO() do { \
    if (init_code) free(init_code); \
    if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; } \
    evm->return_data_size = 0; \
    uint256_t zero = UINT256_ZERO; \
    if (!evm_stack_push(evm->stack, &zero)) \
        return EVM_STACK_OVERFLOW; \
    return EVM_SUCCESS; \
} while(0)

    // EIP-4762 (Verkle): ContractCreatePreCheckGas — read basic_data + code_hash
    // Charged from forwarded gas (not parent's gas_left).
    if (evm->fork >= FORK_VERKLE) {
        uint8_t vk1[32], vk2[32];
        verkle_account_basic_data_key(vk1, contract_addr->bytes);
        verkle_account_code_hash_key(vk2, contract_addr->bytes);
        uint64_t g1 = evm_state_witness_gas_access(evm->state, vk1, false, false);
        uint64_t g2 = evm_state_witness_gas_access(evm->state, vk2, false, false);
        uint64_t precheck = g1 + g2;
        if (precheck > gas_forwarded) {
            // OOG: consume all forwarded gas, parent retains its 1/64
            CREATE_FAIL_PUSH_ZERO();
        }
        gas_forwarded -= precheck;
    }

    // EIP-2929 (Berlin+): mark contract address as warm BEFORE collision check.
    // Per EIP-2929, the target address is added to accessed_addresses regardless
    // of whether CREATE/CREATE2 succeeds or fails due to collision.
    if (evm->fork >= FORK_BERLIN)
    {
        evm_mark_address_warm(evm, contract_addr);
    }

    //==========================================================================
    // Collision Detection
    //==========================================================================

    // EIP-684: collision if target has non-zero nonce or code
    // EIP-7610 (retroactive): also collision if target has non-empty storage
    uint64_t target_nonce = evm_state_get_nonce(evm->state, contract_addr);
    uint32_t target_code_size = evm_state_get_code_size(evm->state, contract_addr);
    bool collision = (target_nonce > 0 || target_code_size > 0);
    if (!collision)
    {
        collision = evm_state_has_storage(evm->state, contract_addr);
    }
    if (collision)
    {
        LOG_EVM_DEBUG("CREATE: Address collision detected");
        // Consume all forwarded gas (collision consumes all gas)
        EVM_TRACE_RETURN(NULL, 0, gas_forwarded, "contract address collision");
        CREATE_FAIL_PUSH_ZERO();
    }

    // Take snapshot AFTER nonce increment but BEFORE account creation/value transfer
    // Revert undoes contract creation and value transfer but preserves sender nonce
    uint32_t snapshot = evm_state_snapshot(evm->state);

    // EIP-4762 (Verkle): ContractCreateInitGas — write basic_data + code_hash
    // Charged from forwarded gas.
    if (evm->fork >= FORK_VERKLE) {
        uint8_t vk1[32], vk2[32];
        verkle_account_basic_data_key(vk1, contract_addr->bytes);
        verkle_account_code_hash_key(vk2, contract_addr->bytes);
        uint64_t g1 = evm_state_witness_gas_access(evm->state, vk1, true, false);
        uint64_t g2 = evm_state_witness_gas_access(evm->state, vk2, true, false);
        uint64_t init_gas = g1 + g2;
        if (init_gas > gas_forwarded) {
            // OOG: consume all forwarded gas, revert, push 0
            if (init_code) free(init_code);
            evm_state_revert(evm->state, snapshot);
            if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
            evm->return_data_size = 0;
            uint256_t zero = UINT256_ZERO;
            if (!evm_stack_push(evm->stack, &zero))
                return EVM_STACK_OVERFLOW;
            return EVM_SUCCESS;
        }
        gas_forwarded -= init_gas;
    }

#undef CREATE_FAIL_PUSH_ZERO

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

        EVM_TRACE_RETURN(init_result.output_data, init_result.output_size,
                         gas_forwarded - init_result.gas_left,
                         init_result.status != EVM_SUCCESS && init_result.status != EVM_REVERT
                             ? "execution error" : NULL);

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
            else if (evm->fork >= FORK_VERKLE)
            {
                // EIP-4762: per-chunk witness gas replaces 200/byte deployment.
                // Charge per chunk; stop on OOG (consume only partial gas).
                uint32_t num_chunks = ((uint32_t)init_result.output_size + 30) / 31;
                uint64_t total_wgas = 0;
                bool deploy_oog = false;
                for (uint32_t c = 0; c < num_chunks; c++) {
                    uint8_t ck[32];
                    verkle_code_chunk_key(ck, contract_addr->bytes, c);
                    uint64_t chunk_gas = evm_state_witness_gas_access(
                        evm->state, ck, true, false);
                    if (init_result.gas_left - total_wgas < chunk_gas) {
                        // OOG: consume available gas for this chunk, stop
                        total_wgas = init_result.gas_left;
                        deploy_oog = true;
                        break;
                    }
                    total_wgas += chunk_gas;
                }
                init_result.gas_left -= total_wgas;
                if (!deploy_oog) {
                    evm_state_set_code(evm->state, contract_addr,
                                       init_result.output_data,
                                       (uint32_t)init_result.output_size);
                } else {
                    success = false;
                }
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

        /* output_data ownership transferred to evm->return_data by evm_execute */

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
        // No init code — create empty contract, return all forwarded gas
        success = true;
        evm->gas_left += gas_forwarded;
        // CREATE produces empty return data
        if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
        evm->return_data_size = 0;
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
static evm_status_t op_create(evm_t *evm)
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

    // Overflow check: impossibly large size or offset means OOG
    if (size.high != 0 || (uint64_t)(size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size) &&
        (offset.high != 0 || (uint64_t)(offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

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

    // EIP-4762: CREATE base gas reduced from 32000 to 1000 in Verkle
    uint64_t base_cost = (evm->fork >= FORK_VERKLE) ? 1000 : 32000;
    uint64_t mem_expansion_cost = 0;

    if (size_u64 > 0) {
        if (offset_u64 > UINT64_MAX - size_u64)
            return EVM_OUT_OF_GAS;
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
            LOG_EVM_DEBUG("CREATE: Failed to read init code from memory");
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
static evm_status_t op_create2(evm_t *evm)
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
        LOG_EVM_DEBUG("CREATE2: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 4 arguments: value offset size salt (top to bottom)
    uint256_t value, offset, size, salt;

    if (!evm_stack_pop(evm->stack, &value) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &salt))
        return EVM_STACK_UNDERFLOW;

    // Overflow check: impossibly large size or offset means OOG
    if (size.high != 0 || (uint64_t)(size.low >> 64) != 0)
        return EVM_OUT_OF_GAS;
    if (!uint256_is_zero(&size) &&
        (offset.high != 0 || (uint64_t)(offset.low >> 64) != 0))
        return EVM_OUT_OF_GAS;

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

    // EIP-4762: CREATE2 base gas reduced from 32000 to 1000 in Verkle
    uint64_t base_cost = (evm->fork >= FORK_VERKLE) ? 1000 : 32000;

    // Memory expansion cost
    uint64_t mem_expansion_cost = 0;
    if (size_u64 > 0) {
        if (offset_u64 > UINT64_MAX - size_u64)
            return EVM_OUT_OF_GAS;
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
            LOG_EVM_DEBUG("CREATE2: Failed to read init code from memory");
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
