/**
 * EVM Implementation
 */

#include "evm.h"
#include "interpreter.h"
#include "fork.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Internal Structures
//==============================================================================

/**
 * Saved execution context for call stack isolation
 * Used to preserve parent context during subcalls
 */
typedef struct {
    evm_stack_t *stack;
    evm_memory_t *memory;
    const uint8_t *code;
    size_t code_size;
    uint64_t pc;
    uint64_t gas_left;
    uint64_t gas_refund;
    evm_message_t msg;
    bool stopped;
    evm_status_t status;
    uint8_t *return_data;
    size_t return_data_size;
} evm_context_t;

/**
 * Save current EVM execution context
 */
static evm_context_t evm_save_context(evm_t *evm)
{
    evm_context_t ctx = {
        .stack = evm->stack,
        .memory = evm->memory,
        .code = evm->code,
        .code_size = evm->code_size,
        .pc = evm->pc,
        .gas_left = evm->gas_left,
        .gas_refund = evm->gas_refund,
        .msg = evm->msg,
        .stopped = evm->stopped,
        .status = evm->status,
        .return_data = evm->return_data,
        .return_data_size = evm->return_data_size
    };
    return ctx;
}

/**
 * Restore EVM execution context
 */
static void evm_restore_context(evm_t *evm, const evm_context_t *ctx)
{
    evm->stack = ctx->stack;
    evm->memory = ctx->memory;
    evm->code = ctx->code;
    evm->code_size = ctx->code_size;
    evm->pc = ctx->pc;
    evm->gas_left = ctx->gas_left;
    evm->gas_refund = ctx->gas_refund;
    evm->msg = ctx->msg;
    evm->stopped = ctx->stopped;
    evm->status = ctx->status;
    evm->return_data = ctx->return_data;
    evm->return_data_size = ctx->return_data_size;
}

//==============================================================================
// EVM Lifecycle
//==============================================================================

evm_t *evm_create(state_db_t *state, const chain_config_t *chain_config)
{
    if (!state)
    {
        LOG_EVM_ERROR("Cannot create EVM without StateDB");
        return NULL;
    }

    evm_t *evm = calloc(1, sizeof(evm_t));
    if (!evm)
    {
        LOG_EVM_ERROR("Failed to allocate EVM");
        return NULL;
    }

    evm->state = state;
    evm->chain_config = chain_config ? chain_config : chain_config_mainnet();
    evm->fork = FORK_FRONTIER; // Will be updated when block env is set

    // Create stack and memory
    evm->stack = evm_stack_create();
    if (!evm->stack)
    {
        LOG_EVM_ERROR("Failed to create EVM stack");
        free(evm);
        return NULL;
    }

    evm->memory = evm_memory_create();
    if (!evm->memory)
    {
        LOG_EVM_ERROR("Failed to create EVM memory");
        evm_stack_destroy(evm->stack);
        free(evm);
        return NULL;
    }

    // Initialize access tracking (EIP-2929)
    if (!art_tree_init(&evm->accessed_addresses))
    {
        LOG_EVM_ERROR("Failed to initialize accessed addresses tree");
        evm_memory_destroy(evm->memory);
        evm_stack_destroy(evm->stack);
        free(evm);
        return NULL;
    }

    if (!art_tree_init(&evm->accessed_storage))
    {
        LOG_EVM_ERROR("Failed to initialize accessed storage tree");
        art_tree_destroy(&evm->accessed_addresses);
        evm_memory_destroy(evm->memory);
        evm_stack_destroy(evm->stack);
        free(evm);
        return NULL;
    }

    LOG_EVM_DEBUG("Created EVM instance (chain: %s, id: %lu)",
              evm->chain_config->name, evm->chain_config->chain_id);

    return evm;
}

void evm_destroy(evm_t *evm)
{
    if (!evm)
    {
        return;
    }

    if (evm->stack)
    {
        evm_stack_destroy(evm->stack);
    }

    if (evm->memory)
    {
        evm_memory_destroy(evm->memory);
    }

    if (evm->return_data)
    {
        free(evm->return_data);
    }

    // Destroy access tracking trees
    art_tree_destroy(&evm->accessed_addresses);
    art_tree_destroy(&evm->accessed_storage);

    free(evm);
}

void evm_reset(evm_t *evm)
{
    if (!evm)
    {
        return;
    }

    // Reset stack and memory
    if (evm->stack)
    {
        evm_stack_clear(evm->stack);
    }

    if (evm->memory)
    {
        evm_memory_clear(evm->memory);
    }

    // Clear return data
    if (evm->return_data)
    {
        free(evm->return_data);
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }

    // Clear access tracking (for new transaction)
    art_tree_destroy(&evm->accessed_addresses);
    art_tree_destroy(&evm->accessed_storage);
    art_tree_init(&evm->accessed_addresses);
    art_tree_init(&evm->accessed_storage);

    // Reset runtime state
    evm->pc = 0;
    evm->gas_left = 0;
    evm->gas_refund = 0;
    evm->stopped = false;
    evm->status = EVM_SUCCESS;

    // Note: Don't reset chain_config or fork - those are set externally
}

//==============================================================================
// Context Setup
//==============================================================================

void evm_set_block_env(evm_t *evm, const evm_block_env_t *block)
{
    if (!evm || !block)
    {
        return;
    }

    evm->block = *block;

    // Recompute fork based on new block number
    evm->fork = fork_get_active(block->number, evm->chain_config);

    LOG_EVM_DEBUG("Set block environment: number=%lu, fork=%s",
              block->number, fork_get_name(evm->fork));
}

void evm_set_tx_context(evm_t *evm, const evm_tx_context_t *tx)
{
    if (!evm || !tx)
    {
        return;
    }

    evm->tx = *tx;
}

//==============================================================================
// Gas Operations
//==============================================================================

bool evm_use_gas(evm_t *evm, uint64_t amount)
{
    if (!evm)
    {
        return false;
    }

    if (evm->gas_left < amount)
    {
        evm->status = EVM_OUT_OF_GAS;
        evm->gas_left = 0;
        return false;
    }

    evm->gas_left -= amount;
    return true;
}

void evm_refund_gas(evm_t *evm, uint64_t amount)
{
    if (!evm)
    {
        return;
    }

    evm->gas_refund += amount;
}

uint64_t evm_get_gas_left(const evm_t *evm)
{
    return evm ? evm->gas_left : 0;
}

//==============================================================================
// Access List Operations (EIP-2929)
//==============================================================================

bool evm_is_address_warm(const evm_t *evm, const address_t *addr)
{
    if (!evm || !addr)
    {
        return false;
    }

    return art_contains(&evm->accessed_addresses, addr->bytes, 20);
}

void evm_mark_address_warm(evm_t *evm, const address_t *addr)
{
    if (!evm || !addr)
    {
        return;
    }

    // Insert with NULL value (we only care about presence, not value)
    art_insert(&evm->accessed_addresses, addr->bytes, 20, NULL, 0);
}

bool evm_is_storage_warm(const evm_t *evm, const address_t *addr, const uint256_t *key)
{
    if (!evm || !addr || !key)
    {
        return false;
    }

    // Create composite key: address (20 bytes) + storage key (32 bytes)
    uint8_t composite_key[52];
    memcpy(composite_key, addr->bytes, 20);
    uint256_to_bytes(key, composite_key + 20);

    return art_contains(&evm->accessed_storage, composite_key, 52);
}

void evm_mark_storage_warm(evm_t *evm, const address_t *addr, const uint256_t *key)
{
    if (!evm || !addr || !key)
    {
        return;
    }

    // Create composite key: address (20 bytes) + storage key (32 bytes)
    uint8_t composite_key[52];
    memcpy(composite_key, addr->bytes, 20);
    uint256_to_bytes(key, composite_key + 20);

    // Insert with NULL value (we only care about presence, not value)
    art_insert(&evm->accessed_storage, composite_key, 52, NULL, 0);
}

//==============================================================================
// Result Helpers
//==============================================================================

evm_result_t evm_result_success(uint64_t gas_left, const uint8_t *output_data, size_t output_size)
{
    evm_result_t result = {
        .status = EVM_SUCCESS,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = output_size
    };

    if (output_data && output_size > 0)
    {
        result.output_data = malloc(output_size);
        if (result.output_data)
        {
            memcpy(result.output_data, output_data, output_size);
        }
    }

    return result;
}

evm_result_t evm_result_revert(uint64_t gas_left, const uint8_t *output_data, size_t output_size)
{
    evm_result_t result = {
        .status = EVM_REVERT,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = output_size
    };

    if (output_data && output_size > 0)
    {
        result.output_data = malloc(output_size);
        if (result.output_data)
        {
            memcpy(result.output_data, output_data, output_size);
        }
    }

    return result;
}

evm_result_t evm_result_error(evm_status_t status, uint64_t gas_left)
{
    evm_result_t result = {
        .status = status,
        .gas_left = gas_left,
        .gas_refund = 0,
        .output_data = NULL,
        .output_size = 0
    };

    return result;
}

void evm_result_free(evm_result_t *result)
{
    if (!result)
    {
        return;
    }

    if (result->output_data)
    {
        free(result->output_data);
        result->output_data = NULL;
    }

    result->output_size = 0;
}

//==============================================================================
// Message Helpers
//==============================================================================

evm_message_t evm_message_call(const address_t *caller,
                                const address_t *recipient,
                                const uint256_t *value,
                                const uint8_t *input_data,
                                size_t input_size,
                                uint64_t gas,
                                int32_t depth)
{
    evm_message_t msg = {
        .kind = EVM_CALL,
        .caller = *caller,
        .recipient = *recipient,
        .code_addr = *recipient,
        .value = *value,
        .input_data = input_data,
        .input_size = input_size,
        .gas = gas,
        .depth = depth,
        .is_static = false
    };

    return msg;
}

evm_message_t evm_message_delegatecall(const address_t *caller,
                                        const address_t *recipient,
                                        const uint8_t *input_data,
                                        size_t input_size,
                                        uint64_t gas,
                                        int32_t depth)
{
    uint256_t zero_value = {0};
    
    evm_message_t msg = {
        .kind = EVM_DELEGATECALL,
        .caller = *caller,
        .recipient = *caller,        // Recipient stays the same in delegatecall
        .code_addr = *recipient,     // But code comes from the target
        .value = zero_value,
        .input_data = input_data,
        .input_size = input_size,
        .gas = gas,
        .depth = depth,
        .is_static = false
    };

    return msg;
}

evm_message_t evm_message_create(const address_t *caller,
                                    const uint256_t *value,
                                    const uint8_t *init_code,
                                    size_t init_code_size,
                                    uint64_t gas,
                                    int32_t depth)
{
    address_t zero_addr = {0};
    
    evm_message_t msg = {
        .kind = EVM_CREATE,
        .caller = *caller,
        .recipient = zero_addr,      // Will be computed
        .code_addr = zero_addr,
        .value = *value,
        .input_data = init_code,
        .input_size = init_code_size,
        .gas = gas,
        .depth = depth,
        .is_static = false
    };

    return msg;
}

//==============================================================================
// Execution
//==============================================================================

bool evm_execute(evm_t *evm, const evm_message_t *msg, evm_result_t *result)
{
    if (!evm || !msg || !result)
    {
        LOG_EVM_ERROR("Invalid arguments to evm_execute");
        return false;
    }

    // Check call depth limit (Ethereum allows max 1024 depth)
    if (msg->depth >= 1024)
    {
        LOG_EVM_ERROR("Call depth limit exceeded (depth=%d, max=1024)", msg->depth);
        *result = evm_result_error(EVM_CALL_DEPTH_EXCEEDED, msg->gas);
        return true; // Not an internal error, just depth exceeded
    }

    //==========================================================================
    // Context Isolation for Subcalls
    //==========================================================================
    
    // Determine if this is a subcall (depth > 0)
    bool is_subcall = (msg->depth > 0);
    evm_context_t saved_context;
    
    if (is_subcall)
    {
        // Save parent's execution context
        saved_context = evm_save_context(evm);
        
        // Create new stack and memory for this subcall
        evm->stack = evm_stack_create();
        evm->memory = evm_memory_create();
        
        if (!evm->stack || !evm->memory)
        {
            LOG_EVM_ERROR("Failed to create stack/memory for subcall");
            if (evm->stack) evm_stack_destroy(evm->stack);
            if (evm->memory) evm_memory_destroy(evm->memory);
            evm_restore_context(evm, &saved_context);
            *result = evm_result_error(EVM_INTERNAL_ERROR, 0);
            return false;
        }
        
        // Clear return data for this subcall
        evm->return_data = NULL;
        evm->return_data_size = 0;
    }
    else
    {
        // Top-level call - reset EVM state
        evm_reset(evm);
        
        // Initialize access list with sender and recipient (EIP-2929)
        // These addresses are warm from the start of the transaction
        if (evm->fork >= FORK_BERLIN)
        {
            evm_mark_address_warm(evm, &msg->caller);
            evm_mark_address_warm(evm, &msg->recipient);
        }
    }

    //==========================================================================
    // Setup Message Context
    //==========================================================================
    
    // Set up message context
    evm->msg = *msg;
    evm->gas_left = msg->gas;
    evm->pc = 0;
    evm->stopped = false;
    evm->status = EVM_SUCCESS;

    //==========================================================================
    // Value Transfer
    //==========================================================================
    
    // Transfer value if non-zero (for CALL family, not CREATE - CREATE handles it separately)
    // Only transfer for internal calls (depth > 0) - top-level calls already transferred in transaction layer
    if (msg->depth > 0 && (msg->kind == EVM_CALL || msg->kind == EVM_CALLCODE) && !uint256_is_zero(&msg->value))
    {
        // Deduct from caller
        uint256_t caller_balance;
        state_db_get_balance(evm->state, &msg->caller, &caller_balance);
        uint256_t new_caller_balance = uint256_sub(&caller_balance, &msg->value);
        state_db_set_balance(evm->state, &msg->caller, &new_caller_balance);
        
        // Add to recipient
        uint256_t recipient_balance;
        if (!state_db_get_balance(evm->state, &msg->recipient, &recipient_balance))
        {
            recipient_balance = uint256_from_uint64(0);
        }
        uint256_t new_recipient_balance = uint256_add(&recipient_balance, &msg->value);
        state_db_set_balance(evm->state, &msg->recipient, &new_recipient_balance);
    }

    // Handle contract creation
    if (msg->kind == EVM_CREATE || msg->kind == EVM_CREATE2)
    {
        // For CREATE, the input_data IS the init code
        evm->code = msg->input_data;
        evm->code_size = msg->input_size;
        
        LOG_EVM_DEBUG("CREATE: Running init code (%zu bytes)", evm->code_size);
    }
    else
    {
        // For CALL, load code from the recipient address in state
        const uint8_t *contract_code = NULL;
        size_t contract_code_size = 0;
        
        printf("DEBUG EVM: CALL to address 0x");
        for (int i = 0; i < 20; i++) printf("%02x", msg->code_addr.bytes[i]);
        printf(", trying to load code from state\n");
        
        if (state_db_get_code(evm->state, &msg->code_addr, &contract_code, &contract_code_size))
        {
            // Found code at this address
            evm->code = contract_code;
            evm->code_size = contract_code_size;
            printf("DEBUG EVM: Loaded %zu bytes of code from state\n", evm->code_size);
            LOG_EVM_DEBUG("CALL: Executing code from state (%zu bytes)", evm->code_size);
        }
        else
        {
            // No code at address - simple value transfer
            printf("DEBUG EVM: No code at address, treating as value transfer\n");
            LOG_EVM_DEBUG("No code at address, simple value transfer");
            
            // Cleanup and restore context if subcall
            if (is_subcall)
            {
                evm_stack_destroy(evm->stack);
                evm_memory_destroy(evm->memory);
                evm_restore_context(evm, &saved_context);
            }
            
            *result = evm_result_success(msg->gas, NULL, 0);
            return true;
        }
    }

    // Check if there's any code to execute
    if (evm->code_size == 0)
    {
        LOG_EVM_DEBUG("No code to execute");
        
        // Cleanup and restore context if subcall
        if (is_subcall)
        {
            evm_stack_destroy(evm->stack);
            evm_memory_destroy(evm->memory);
            evm_restore_context(evm, &saved_context);
        }
        
        *result = evm_result_success(evm->gas_left, NULL, 0);
        return true;
    }

    //==========================================================================
    // Execute Bytecode
    //==========================================================================
    
    // Execute the code via interpreter
    *result = evm_interpret(evm);
    
    //==========================================================================
    // Restore Context for Subcalls
    //==========================================================================
    
    if (is_subcall)
    {
        // Save subcall's return data before destroying its context
        uint8_t *subcall_return_data = evm->return_data;
        size_t subcall_return_size = evm->return_data_size;
        
        // Destroy subcall's stack and memory
        evm_stack_destroy(evm->stack);
        evm_memory_destroy(evm->memory);
        
        // Restore parent's context
        evm_restore_context(evm, &saved_context);
        
        // Update parent's return data with subcall's output
        // (This makes it available for RETURNDATASIZE/RETURNDATACOPY)
        if (evm->return_data)
        {
            free(evm->return_data);
        }
        evm->return_data = subcall_return_data;
        evm->return_data_size = subcall_return_size;
    }
    
    return true;
}

evm_status_t evm_run(evm_t *evm)
{
    if (!evm)
    {
        return EVM_INTERNAL_ERROR;
    }

    // TODO: This will be replaced by evm_interpret() or integrated with it
    LOG_EVM_DEBUG("evm_run stub - not yet implemented");
    
    return EVM_INTERNAL_ERROR;
}
