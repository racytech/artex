/*
 * EVM CREATE Opcodes Implementation
 * Handles CREATE and CREATE2 contract creation opcodes
 */

#include "opcodes/create.h"
#include "evm.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "gas.h"
#include "logger.h"
#include "uint256.h"
#include "address.h"
#include "hash.h"
#include "rlp.h"
#include "state_db.h"
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

/**
 * Deploy contract code and charge deployment gas
 */
static bool deploy_code(evm_t *evm, const address_t *contract_addr, 
                       const uint8_t *code, size_t code_size)
{
    // Calculate deployment gas: 200 gas per byte
    uint64_t deployment_gas = code_size * 200;
    
    // Check if we have enough gas
    if (evm->gas_left < deployment_gas) {
        LOG_EVM_DEBUG("CREATE: Insufficient gas for code deployment");
        return false;
    }
    
    // Deduct deployment gas
    if (!evm_use_gas(evm, deployment_gas)) {
        return false;
    }
    
    // Store code in state
    if (!state_db_set_code(evm->state, contract_addr, code, code_size)) {
        LOG_EVM_ERROR("CREATE: Failed to store contract code");
        return false;
    }
    
    return true;
}

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
    fprintf(stderr, "execute_create: addr=");
    for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", contract_addr->bytes[i]);
    fprintf(stderr, ", init_code_size=%lu\n", init_code_size);
    
    //==========================================================================
    // Depth Check
    //==========================================================================

    fprintf(stderr, "CREATE: Depth check - current depth=%d\n", evm->msg.depth);
    if (evm->msg.depth >= 1024)
    {
        LOG_EVM_DEBUG("CREATE: Call depth limit exceeded");
        fprintf(stderr, "CREATE: DEPTH EXCEEDED - returning early\n");
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero)) {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }
    fprintf(stderr, "CREATE: Depth check passed\n");

    //==========================================================================
    // Balance Check & Value Transfer
    //==========================================================================

    fprintf(stderr, "CREATE: Value to transfer: %d\n", !uint256_is_zero(value));
    
    // Take snapshot before any state changes
    uint32_t snapshot = state_db_begin_transaction(evm->state);
    if (snapshot == UINT32_MAX) {
        LOG_EVM_ERROR("CREATE: Failed to create snapshot");
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero)) {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }
    
    // Increment sender nonce AFTER taking snapshot so it can be reverted
    if (!state_db_set_nonce(evm->state, &evm->msg.recipient, sender_nonce + 1))
    {
        LOG_EVM_ERROR("CREATE: Failed to increment sender nonce");
        state_db_revert_to_snapshot(evm->state, snapshot);
        if (init_code) free(init_code);
        uint256_t zero = UINT256_ZERO;
        if (!evm_stack_push(evm->stack, &zero)) {
            return EVM_STACK_OVERFLOW;
        }
        return EVM_SUCCESS;
    }
    
    if (!uint256_is_zero(value))
    {
        uint256_t sender_balance;
        if (!state_db_get_balance(evm->state, &evm->msg.recipient, &sender_balance))
        {
            sender_balance = uint256_from_uint64(0);
        }

        // Check if sender has sufficient balance for the value transfer
        if (uint256_lt(&sender_balance, value))
        {
            // Insufficient balance - CREATE fails completely
            LOG_EVM_DEBUG("CREATE: Insufficient balance for value transfer");
            fprintf(stderr, "CREATE: INSUFFICIENT BALANCE - failing CREATE\n");
            state_db_revert_to_snapshot(evm->state, snapshot);
            if (init_code) free(init_code);
            uint256_t zero = UINT256_ZERO;
            if (!evm_stack_push(evm->stack, &zero)) {
                return EVM_STACK_OVERFLOW;
            }
            return EVM_SUCCESS;
        }

        // Transfer value from sender to new contract
        uint256_t new_sender_balance = uint256_sub(&sender_balance, value);
        state_db_set_balance(evm->state, &evm->msg.recipient, &new_sender_balance);

        uint256_t contract_balance;
        if (!state_db_get_balance(evm->state, contract_addr, &contract_balance))
        {
            contract_balance = uint256_from_uint64(0);
        }
        uint256_t new_contract_balance = uint256_add(&contract_balance, value);
        state_db_set_balance(evm->state, contract_addr, &new_contract_balance);
        
        fprintf(stderr, "CREATE: Value transferred successfully\n");
    }

    //==========================================================================
    // Gas Forwarding (63/64 rule)
    //==========================================================================

    uint64_t gas_forwarded = gas_max_call_gas(evm->gas_left);

    //==========================================================================
    // Execute Init Code
    //==========================================================================

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

    // DEBUG: Track problematic address
    bool is_problem_addr = (contract_addr->bytes[0] == 0x7e && 
                           contract_addr->bytes[1] == 0x2f && 
                           contract_addr->bytes[2] == 0xf5);
    if (is_problem_addr) {
        printf("DEBUG CREATE: Creating problematic address 0x7e2ff..., value=%d, depth=%d\n",
               !uint256_is_zero(value), evm->msg.depth);
    }

    evm_result_t init_result;
    fprintf(stderr, "CREATE: About to call evm_execute with init_code_size=%lu\n", init_code_size);
    evm_execute(evm, &init_msg, &init_result);

    fprintf(stderr, "CREATE: After evm_execute, status=%d\n", init_result.status);

    // Return unused gas
    evm->gas_left += init_result.gas_left;

    // Free init code
    if (init_code) free(init_code);

    //==========================================================================
    // Handle Result
    //==========================================================================

    bool success = (init_result.status == EVM_SUCCESS);
    
    if (is_problem_addr) {
        printf("DEBUG CREATE: Result for 0x7e2ff... status=%d, success=%d, output_size=%zu\n",
               init_result.status, success, init_result.output_size);
    }
    fprintf(stderr, "CREATE: init_result.status=%d, output_size=%zu, output_data=%p\n", 
            init_result.status, init_result.output_size, (void*)init_result.output_data);
    
    // Per EIP-161: CREATE that returns 0 bytes of code is treated as failure
    // This prevents creation of accounts with nonce but no code
    if (success && init_result.output_size == 0)
    {
        fprintf(stderr, "CREATE: Init code returned 0 bytes - treating as failure (EIP-161)\n");
        if (is_problem_addr) {
            printf("DEBUG CREATE: Init code returned 0 bytes for 0x7e2ff..., treating as failure\n");
        }
        success = false;
    }
    
    if (success)
    {
        // Deploy the returned code
        fprintf(stderr, "CREATE: Deploying %zu bytes of code to ", init_result.output_size);
        for (int i = 0; i < 20; i++) fprintf(stderr, "%02x", contract_addr->bytes[i]);
        fprintf(stderr, "\n");
        success = deploy_code(evm, contract_addr, init_result.output_data, init_result.output_size);
    }
    else
    {
        fprintf(stderr, "CREATE: Init code execution failed with status=%d\n", init_result.status);
    }

    if (init_result.output_data) {
        free(init_result.output_data);
    }

    //==========================================================================
    // Commit or Revert State Changes
    //==========================================================================

    if (success) {
        // Commit all state changes (balance transfer, nonce, code)
        state_db_commit_transaction(evm->state);
        if (is_problem_addr) {
            printf("DEBUG CREATE: Committing state for 0x7e2ff...\n");
        }
    } else {
        // Revert all state changes
        state_db_revert_to_snapshot(evm->state, snapshot);
        if (is_problem_addr) {
            printf("DEBUG CREATE: Reverting state for 0x7e2ff...\n");
        }
    }

    //==========================================================================
    // Push Result
    //==========================================================================

    uint256_t result = UINT256_ZERO;
    if (success) {
        memcpy(&result, contract_addr->bytes, ADDRESS_SIZE);
    }

    if (!evm_stack_push(evm->stack, &result))
    {
        return EVM_STACK_OVERFLOW;
    }

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
    fprintf(stderr, "=== CREATE OPCODE CALLED ===\n");
    char* msg_value_hex = uint256_to_hex(&evm->msg.value);
    fprintf(stderr, "op_create: evm->msg.value (value sent to THIS contract) = %s\n", msg_value_hex);
    free(msg_value_hex);
    
    if (!evm || !evm->stack || !evm->memory || !evm->state)
    {
        return EVM_INTERNAL_ERROR;
    }

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("CREATE: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 3 arguments from stack
    uint256_t size, offset, value;
    
    if (!evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &value))
    {
        return EVM_STACK_UNDERFLOW;
    }

    char* value_hex = uint256_to_hex(&value);
    uint64_t size_u64 = uint256_to_uint64(&size);
    uint64_t offset_u64 = uint256_to_uint64(&offset);
    fprintf(stderr, "op_create: popped value=%s, offset=%lu, size=%lu\n", value_hex, offset_u64, size_u64);
    free(value_hex);

    //==========================================================================
    // Gas Calculation
    //==========================================================================

    uint64_t base_cost = 32000;
    uint64_t mem_expansion_cost = 0;
    
    if (size_u64 > 0) {
        mem_expansion_cost = evm_memory_expansion_cost(evm->memory->size, offset_u64 + size_u64);
    }
    
    if (!evm_use_gas(evm, base_cost + mem_expansion_cost))
    {
        return EVM_OUT_OF_GAS;
    }

    //==========================================================================
    // Address Calculation
    //==========================================================================

    uint64_t nonce;
    if (!state_db_get_nonce(evm->state, &evm->msg.recipient, &nonce))
    {
        nonce = 0;
    }

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

    // Check for static call violation
    if (evm->msg.is_static)
    {
        LOG_EVM_ERROR("CREATE2: Cannot create contract in static call");
        return EVM_STATIC_CALL_VIOLATION;
    }

    // Pop 4 arguments from stack
    uint256_t salt, size, offset, value;
    
    if (!evm_stack_pop(evm->stack, &salt) ||
        !evm_stack_pop(evm->stack, &size) ||
        !evm_stack_pop(evm->stack, &offset) ||
        !evm_stack_pop(evm->stack, &value))
    {
        return EVM_STACK_UNDERFLOW;
    }

    // Convert to uint64_t
    uint64_t size_u64 = uint256_to_uint64(&size);
    uint64_t offset_u64 = uint256_to_uint64(&offset);

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
    
    uint64_t total_gas = base_cost + mem_expansion_cost + keccak_cost;
    
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
    uint64_t nonce;
    if (!state_db_get_nonce(evm->state, &evm->msg.recipient, &nonce))
    {
        nonce = 0;
    }

    //==========================================================================
    // Execute Contract Creation
    //==========================================================================

    return execute_create(evm, &value, &contract_addr, init_code, size_u64, EVM_CREATE2, nonce);
}
