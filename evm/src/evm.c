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
// EVM Lifecycle
//==============================================================================

evm_t *evm_create(state_db_t *state, const chain_config_t *chain_config)
{
    if (!state)
    {
        LOG_ERROR("Cannot create EVM without StateDB");
        return NULL;
    }

    evm_t *evm = calloc(1, sizeof(evm_t));
    if (!evm)
    {
        LOG_ERROR("Failed to allocate EVM");
        return NULL;
    }

    evm->state = state;
    evm->chain_config = chain_config ? chain_config : chain_config_mainnet();
    evm->fork = FORK_FRONTIER; // Will be updated when block env is set

    // Create stack and memory
    evm->stack = evm_stack_create();
    if (!evm->stack)
    {
        LOG_ERROR("Failed to create EVM stack");
        free(evm);
        return NULL;
    }

    evm->memory = evm_memory_create();
    if (!evm->memory)
    {
        LOG_ERROR("Failed to create EVM memory");
        evm_stack_destroy(evm->stack);
        free(evm);
        return NULL;
    }

    LOG_DEBUG("Created EVM instance (chain: %s, id: %lu)",
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

    LOG_DEBUG("Set block environment: number=%lu, fork=%s",
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
        LOG_ERROR("Invalid arguments to evm_execute");
        return false;
    }

    // Check call depth limit (Ethereum allows max 1024 depth)
    if (msg->depth >= 1024)
    {
        LOG_ERROR("Call depth limit exceeded (depth=%d, max=1024)", msg->depth);
        *result = evm_result_error(EVM_CALL_DEPTH_EXCEEDED, msg->gas);
        return true; // Not an internal error, just depth exceeded
    }

    // Reset EVM state
    evm_reset(evm);

    // Set up message context
    evm->msg = *msg;
    evm->gas_left = msg->gas;

    // Handle contract creation
    if (msg->kind == EVM_CREATE || msg->kind == EVM_CREATE2)
    {
        // For CREATE, the input_data IS the init code
        evm->code = msg->input_data;
        evm->code_size = msg->input_size;
        
        LOG_DEBUG("CREATE: Running init code (%zu bytes)", evm->code_size);
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
            LOG_DEBUG("CALL: Executing code from state (%zu bytes)", evm->code_size);
        }
        else
        {
            // No code at address - simple value transfer
            printf("DEBUG EVM: No code at address, treating as value transfer\n");
            LOG_DEBUG("No code at address, simple value transfer");
            *result = evm_result_success(msg->gas, NULL, 0);
            return true;
        }
    }

    // Check if there's any code to execute
    if (evm->code_size == 0)
    {
        LOG_DEBUG("No code to execute");
        *result = evm_result_success(evm->gas_left, NULL, 0);
        return true;
    }

    // Execute the code via interpreter
    *result = evm_interpret(evm);
    
    return true;
}

evm_status_t evm_run(evm_t *evm)
{
    if (!evm)
    {
        return EVM_INTERNAL_ERROR;
    }

    // TODO: This will be replaced by evm_interpret() or integrated with it
    LOG_DEBUG("evm_run stub - not yet implemented");
    
    return EVM_INTERNAL_ERROR;
}
