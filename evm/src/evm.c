/**
 * EVM Implementation
 */

#include "evm.h"
#include "interpreter.h"
#include "precompile.h"
#include "transaction.h"
#include "fork.h"
#include "evm_stack.h"
#include "evm_memory.h"
#include "verkle_key.h"
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
    const uint8_t *jumpdest_bitmap;
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
        .return_data_size = evm->return_data_size,
        .jumpdest_bitmap = evm->jumpdest_bitmap
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
    evm->jumpdest_bitmap = ctx->jumpdest_bitmap;
}

//==============================================================================
// EVM Lifecycle
//==============================================================================

evm_t *evm_create(evm_state_t *state, const chain_config_t *chain_config)
{
    if (!state)
    {
        LOG_EVM_ERROR("Cannot create EVM without state");
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

    // Recompute fork based on new block number/timestamp
    evm->fork = fork_get_active(block->number, block->timestamp, evm->chain_config);

    // EIP-4844: compute blob base fee from excess blob gas
    if (evm->fork >= FORK_CANCUN) {
        evm->block.blob_base_fee = calc_blob_gas_price(&block->excess_blob_gas, evm->fork);
    }

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
    if (!evm || !addr) return false;
    return evm_state_is_address_warm(evm->state, addr);
}

void evm_mark_address_warm(evm_t *evm, const address_t *addr)
{
    if (!evm || !addr) return;
    evm_state_warm_address(evm->state, addr);
}

bool evm_is_storage_warm(const evm_t *evm, const address_t *addr, const uint256_t *key)
{
    if (!evm || !addr || !key) return false;
    return evm_state_is_slot_warm(evm->state, addr, key);
}

void evm_mark_storage_warm(evm_t *evm, const address_t *addr, const uint256_t *key)
{
    if (!evm || !addr || !key) return;
    evm_state_warm_slot(evm->state, addr, key);
}

//==============================================================================
// EIP-7702 Delegation Resolution
//==============================================================================

bool evm_resolve_delegation(evm_state_t *state, const address_t *addr, address_t *target_addr)
{
    if (!state || !addr || !target_addr) return false;

    uint32_t code_len = 0;
    const uint8_t *code = evm_state_get_code_ptr(state, addr, &code_len);

    if (code && code_len == 23 &&
        code[0] == 0xef && code[1] == 0x01 && code[2] == 0x00) {
        memcpy(target_addr->bytes, &code[3], 20);
        return true;
    }

    return false;
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

    // Check call depth limit (Ethereum allows depth 0-1024, rejects 1025+)
    // Reference: process_message() uses "depth > STACK_DEPTH_LIMIT" where limit=1024
    if (msg->depth > 1024)
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
    evm_context_t saved_context = {0};
    
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

            // EIP-2929: precompile addresses are always warm
            // Determine the highest active precompile for this fork
            uint8_t max_precompile = 4; // Frontier: 0x01-0x04
            if (evm->fork >= FORK_BYZANTIUM) max_precompile = 8;  // + MODEXP, BN256
            if (evm->fork >= FORK_ISTANBUL)  max_precompile = 9;  // + BLAKE2F
            if (evm->fork >= FORK_CANCUN)    max_precompile = 10; // + POINT_EVAL
            if (evm->fork >= FORK_PRAGUE)    max_precompile = 0x11; // + BLS12-381 (0x0B-0x11)

            for (uint8_t i = 1; i <= max_precompile; i++)
            {
                address_t precompile_addr = {0};
                precompile_addr.bytes[19] = i;
                evm_mark_address_warm(evm, &precompile_addr);
            }

            // EIP-3651 (Shanghai+): coinbase is warm from start of transaction
            if (evm->fork >= FORK_SHANGHAI)
            {
                evm_mark_address_warm(evm, &evm->block.coinbase);
            }
        }
    }

    //==========================================================================
    // Setup Message Context
    //==========================================================================
    
    // Set up message context
    evm->msg = *msg;
    evm->gas_left = msg->gas;
    evm->gas_refund = 0;  // Each frame starts with zero refund
    evm->pc = 0;
    evm->stopped = false;
    evm->status = EVM_SUCCESS;


    //==========================================================================
    // State Snapshot for Subcalls
    //==========================================================================

    // Subcalls need a state snapshot so we can revert on error/REVERT
    uint32_t subcall_snapshot = 0;
    if (is_subcall)
    {
        subcall_snapshot = evm_state_snapshot(evm->state);
    }

    //==========================================================================
    // Value Transfer & Account Touch
    //==========================================================================

    if (msg->depth > 0)
    {
        // EIP-4762 (Verkle): When calling a non-existent, non-precompile account,
        // charge witness gas for the code_hash leaf (proof of absence).
        // The basic_data write has already been charged in prepare_call().
        if (evm->fork >= FORK_VERKLE &&
            (msg->kind == EVM_CALL || msg->kind == EVM_CALLCODE) &&
            !is_precompile(&msg->recipient, evm->fork) &&
            !evm_state_exists(evm->state, &msg->recipient))
        {
            uint8_t vk[32];
            verkle_account_code_hash_key(vk, msg->recipient.bytes);
            uint64_t wgas = evm_state_witness_gas_access(evm->state, vk, true, false);
            if (wgas > 0 && !evm_use_gas(evm, wgas)) {
                evm_state_revert(evm->state, subcall_snapshot);
                evm_stack_destroy(evm->stack);
                evm_memory_destroy(evm->memory);
                evm_restore_context(evm, &saved_context);
                if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
                evm->return_data_size = 0;
                *result = evm_result_error(EVM_OUT_OF_GAS, 0);
                return true;
            }
        }

        // Touch recipient for CALL/STATICCALL — ensures the account exists in the
        // state trie. On pre-EIP-161 (Frontier/Homestead) touched empty accounts
        // appear in the state root. On EIP-161+ they are pruned at root computation.
        if (msg->kind == EVM_CALL || msg->kind == EVM_STATICCALL)
        {
            uint256_t zero = UINT256_ZERO;
            evm_state_add_balance(evm->state, &msg->recipient, &zero);
        }

        // Transfer value if non-zero (CALL or CALLCODE, not CREATE — CREATE handles it separately)
        if ((msg->kind == EVM_CALL || msg->kind == EVM_CALLCODE) && !uint256_is_zero(&msg->value))
        {
            evm_state_sub_balance(evm->state, &msg->caller, &msg->value);
            evm_state_add_balance(evm->state, &msg->recipient, &msg->value);
        }
    }

    // Handle contract creation
    if (msg->kind == EVM_CREATE || msg->kind == EVM_CREATE2)
    {
        // For CREATE, the input_data IS the init code
        evm->code = msg->input_data;
        evm->code_size = msg->input_size;

        // Calldata is empty for CREATE operations (initcode is code, not calldata)
        evm->msg.input_data = NULL;
        evm->msg.input_size = 0;

        LOG_EVM_DEBUG("CREATE: Running init code (%zu bytes)", evm->code_size);
    }
    else if (is_precompile(&msg->code_addr, evm->fork))
    {
        // Execute precompile directly — no bytecode interpretation needed
        uint64_t gas_remaining = evm->gas_left;
        uint8_t *pc_output = NULL;
        size_t pc_output_size = 0;

        evm_status_t pc_status = precompile_execute(
            &msg->code_addr,
            msg->input_data, msg->input_size,
            &gas_remaining,
            &pc_output, &pc_output_size,
            evm->fork);

        // Revert state on precompile failure (subcalls only)
        if (is_subcall && pc_status != EVM_SUCCESS)
            evm_state_revert(evm->state, subcall_snapshot);

        // Build result
        if (pc_status == EVM_SUCCESS)
            *result = evm_result_success(gas_remaining, pc_output, pc_output_size);
        else if (pc_status == EVM_REVERT)
            *result = evm_result_revert(gas_remaining, pc_output, pc_output_size);
        else
            *result = evm_result_error(pc_status, 0);

        if (pc_output) free(pc_output);

        // Cleanup subcall context and set return data on parent
        if (is_subcall)
        {
            evm_stack_destroy(evm->stack);
            evm_memory_destroy(evm->memory);
            evm_restore_context(evm, &saved_context);

            // Update parent's return data so RETURNDATASIZE/RETURNDATACOPY work
            if (evm->return_data)
                free(evm->return_data);
            if (result->output_size > 0 && result->output_data)
            {
                evm->return_data = malloc(result->output_size);
                if (evm->return_data)
                {
                    memcpy(evm->return_data, result->output_data, result->output_size);
                    evm->return_data_size = result->output_size;
                }
                else
                {
                    evm->return_data_size = 0;
                }
            }
            else
            {
                evm->return_data = NULL;
                evm->return_data_size = 0;
            }
        }

        return true;
    }
    else
    {
        // For CALL, load code from the code address in state
        uint32_t code_len = 0;
        const uint8_t *contract_code = evm_state_get_code_ptr(evm->state, &msg->code_addr, &code_len);

        // EIP-7702: If code is a delegation designator, load delegated code
        if (contract_code && code_len == 23 &&
            contract_code[0] == 0xef && contract_code[1] == 0x01 && contract_code[2] == 0x00)
        {
            address_t delegate_addr;
            memcpy(delegate_addr.bytes, &contract_code[3], 20);
            contract_code = evm_state_get_code_ptr(evm->state, &delegate_addr, &code_len);
        }

        if (contract_code && code_len > 0)
        {
            evm->code = contract_code;
            evm->code_size = code_len;
            LOG_EVM_DEBUG("CALL: Executing code from state (%zu bytes)", evm->code_size);
        }
        else
        {
            // No code at address - simple value transfer
            LOG_EVM_DEBUG("No code at address, simple value transfer");

            // Save subcall's remaining gas before restoring parent context
            uint64_t subcall_gas_remaining = evm->gas_left;

            // Cleanup and restore context if subcall
            if (is_subcall)
            {
                evm_stack_destroy(evm->stack);
                evm_memory_destroy(evm->memory);
                evm_restore_context(evm, &saved_context);
                // Codeless call produces no output — clear stale return data
                if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
                evm->return_data_size = 0;
            }

            *result = evm_result_success(subcall_gas_remaining, NULL, 0);
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
            // No code to execute — clear stale return data
            if (evm->return_data) { free(evm->return_data); evm->return_data = NULL; }
            evm->return_data_size = 0;
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
    // Revert State on Subcall Failure
    //==========================================================================

    if (is_subcall && result->status != EVM_SUCCESS)
    {
        // Revert all state changes (value transfer, SSTORE, etc.)
        evm_state_revert(evm->state, subcall_snapshot);
    }

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
