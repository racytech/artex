/**
 * EVM - Ethereum Virtual Machine Core
 *
 * This is the main EVM execution engine that interprets and executes
 * Ethereum bytecode. It manages execution context, stack, memory, and
 * integrates with StateDB for state access.
 *
 * Architecture:
 *   EVM Interpreter → Stack/Memory/Storage → StateDB → Database
 *
 * The EVM is a stack-based virtual machine with:
 * - 256-bit word size
 * - 1024 maximum stack depth
 * - Expandable memory
 * - Persistent storage (via StateDB)
 * - Gas-based execution metering
 */

#ifndef ART_EVM_H
#define ART_EVM_H

#include "evm_state.h"
#include "uint256.h"
#include "hash.h"
#include "address.h"
#include "fork.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Forward Declarations
//==============================================================================

typedef struct evm_t evm_t;
typedef struct evm_stack_t evm_stack_t;
typedef struct evm_memory_t evm_memory_t;
typedef struct evm_message_t evm_message_t;
typedef struct evm_result_t evm_result_t;

//==============================================================================
// EVM Execution Result
//==============================================================================

/**
 * EVM execution status codes
 */
typedef enum
{
    EVM_SUCCESS = 0,           // Execution completed successfully
    EVM_REVERT,                // Execution reverted (REVERT opcode)
    EVM_OUT_OF_GAS,            // Ran out of gas
    EVM_INVALID_OPCODE,        // Invalid opcode encountered
    EVM_STACK_OVERFLOW,        // Stack overflow (>1024 items)
    EVM_STACK_UNDERFLOW,       // Stack underflow (not enough items)
    EVM_INVALID_JUMP,          // Invalid jump destination
    EVM_INVALID_MEMORY_ACCESS, // Invalid memory access
    EVM_STATIC_CALL_VIOLATION, // State modification in static call
    EVM_CALL_DEPTH_EXCEEDED,   // Call depth limit exceeded (1024)
    EVM_INTERNAL_ERROR         // Internal EVM error
} evm_status_t;

/**
 * EVM execution result
 */
struct evm_result_t
{
    evm_status_t status;    // Execution status
    uint64_t gas_left;      // Remaining gas after execution
    int64_t gas_refund;     // Gas refund amount (can be negative)
    uint8_t *output_data;   // Return data (NULL if none)
    size_t output_size;     // Size of return data
    address_t created_addr; // Address of created contract (CREATE/CREATE2)
};

//==============================================================================
// EVM Message (Call Context)
//==============================================================================

/**
 * Message call type
 */
typedef enum
{
    EVM_CALL,         // Normal call with value transfer
    EVM_CALLCODE,     // Call with caller context (deprecated)
    EVM_DELEGATECALL, // Call with caller context, no value
    EVM_STATICCALL,   // Read-only call (no state modification)
    EVM_CREATE,       // Contract creation
    EVM_CREATE2       // Deterministic contract creation
} evm_call_type_t;

/**
 * EVM message - represents a message call or contract creation
 */
struct evm_message_t
{
    evm_call_type_t kind; // Type of call

    address_t caller;    // Sender of the message
    address_t recipient; // Recipient address (contract being called)
    address_t code_addr; // Address of code to execute (may differ from recipient)

    uint256_t value; // Value transferred in wei

    const uint8_t *input_data; // Input data (calldata)
    size_t input_size;         // Size of input data

    uint64_t gas; // Gas provided for execution

    int32_t depth; // Call depth (0 for top-level, max 1024)

    bool is_static; // Whether this is a static call (no state modification)
};

//==============================================================================
// Block Context
//==============================================================================

/**
 * Block environment information
 */
typedef struct
{
    uint64_t number;       // Block number
    uint64_t timestamp;    // Block timestamp
    uint64_t gas_limit;    // Block gas limit
    uint256_t difficulty;  // Block difficulty (or PREVRANDAO post-merge)
    address_t coinbase;    // Block beneficiary address
    uint256_t base_fee;    // Base fee per gas (EIP-1559)
    uint256_t chain_id;    // Chain ID
    uint256_t excess_blob_gas; // EIP-4844: excess blob gas
    uint256_t blob_base_fee;   // EIP-4844: computed blob base fee
    hash_t block_hash[256]; // Recent block hashes (for BLOCKHASH opcode)
} evm_block_env_t;

//==============================================================================
// Transaction Context
//==============================================================================

/**
 * Transaction-level information
 */
typedef struct
{
    address_t origin;    // Transaction origin (tx.origin)
    uint256_t gas_price; // Gas price for this transaction
    const hash_t *blob_hashes;  // EIP-4844: blob versioned hashes
    size_t blob_hashes_count;   // Number of blob hashes
} evm_tx_context_t;

//==============================================================================
// EVM Log Entry
//==============================================================================

/**
 * A single log entry emitted by LOG0-LOG4 opcodes.
 * Accumulated per-transaction, discarded on revert.
 */
typedef struct {
    address_t address;      // Contract that emitted the log
    hash_t    topics[4];    // Topic values (32 bytes each, big-endian)
    uint8_t   topic_count;  // Number of topics (0-4)
    uint8_t  *data;         // Log data bytes (heap-allocated, NULL if empty)
    size_t    data_len;     // Length of data
} evm_log_t;

//==============================================================================
// EVM Execution Context
//==============================================================================

/**
 * Complete EVM execution context
 */
struct evm_t
{
    // State management
    evm_state_t *state; // EVM state interface (typed layer above state_db)

    // Execution context
    evm_message_t msg;       // Current message
    evm_block_env_t block;   // Block environment
    evm_tx_context_t tx;     // Transaction context
    const chain_config_t *chain_config; // Chain configuration
    evm_fork_t fork;         // Active fork for this execution (pre-computed)

    // Runtime state
    evm_stack_t *stack;   // EVM stack
    evm_memory_t *memory; // EVM memory

    const uint8_t *code; // Bytecode being executed
    size_t code_size;    // Size of bytecode

    uint64_t pc;         // Program counter
    uint64_t gas_left;   // Remaining gas
    int64_t gas_refund;  // Gas refund accumulator (can be negative per-frame)

    // Return data from last call
    uint8_t *return_data;
    size_t return_data_size;

    // Execution flags
    bool stopped;     // Execution stopped (STOP, RETURN, REVERT)
    evm_status_t status; // Execution status

    // Log accumulator (persists across subcalls, truncated on revert)
    evm_log_t *logs;
    size_t     log_count;
    size_t     log_cap;

    // Access tracking (EIP-2929) is handled by evm_state internally
};

//==============================================================================
// EVM Lifecycle
//==============================================================================

/**
 * Create a new EVM instance
 *
 * @param state StateDB instance
 * @param chain_config Chain configuration (optional, defaults to mainnet)
 * @return EVM instance, or NULL on failure
 */
evm_t *evm_create(evm_state_t *state, const chain_config_t *chain_config);

/**
 * Destroy EVM instance and free resources
 *
 * @param evm EVM instance to destroy
 */
void evm_destroy(evm_t *evm);

/**
 * Reset EVM to initial state
 * Clears stack, memory, return data, etc.
 *
 * @param evm EVM instance
 */
void evm_reset(evm_t *evm);

//==============================================================================
// EVM Execution
//==============================================================================

/**
 * Execute a message call
 *
 * @param evm EVM instance
 * @param msg Message to execute
 * @param result Output parameter for execution result
 * @return true on successful execution (even if reverted), false on internal error
 */
bool evm_execute(evm_t *evm, const evm_message_t *msg, evm_result_t *result);

/**
 * Execute bytecode in current context
 * Main interpreter loop
 *
 * @param evm EVM instance
 * @return Execution status
 */
evm_status_t evm_run(evm_t *evm);

//==============================================================================
// Context Setup
//==============================================================================

/**
 * Set block environment
 *
 * @param evm EVM instance
 * @param block Block environment
 */
void evm_set_block_env(evm_t *evm, const evm_block_env_t *block);

/**
 * Set transaction context
 *
 * @param evm EVM instance
 * @param tx Transaction context
 */
void evm_set_tx_context(evm_t *evm, const evm_tx_context_t *tx);

//==============================================================================
// Gas Operations
//==============================================================================

/**
 * Use gas for an operation (inline hot path)
 */
static inline bool evm_use_gas(evm_t *evm, uint64_t amount) {
    if (__builtin_expect(evm->gas_left < amount, 0)) {
        evm->status = EVM_OUT_OF_GAS;
        evm->gas_left = 0;
        return false;
    }
    evm->gas_left -= amount;
    return true;
}

/**
 * Refund gas (SSTORE, SELFDESTRUCT)
 *
 * @param evm EVM instance
 * @param amount Amount of gas to refund
 */
void evm_refund_gas(evm_t *evm, uint64_t amount);

/**
 * Get remaining gas
 *
 * @param evm EVM instance
 * @return Remaining gas
 */
uint64_t evm_get_gas_left(const evm_t *evm);

//==============================================================================
// Access List Operations (EIP-2929)
//==============================================================================

/**
 * Check if an address is warm (already accessed)
 *
 * @param evm EVM instance
 * @param addr Address to check
 * @return true if address is warm, false if cold
 */
bool evm_is_address_warm(const evm_t *evm, const address_t *addr);

/**
 * Mark an address as warm (accessed)
 *
 * @param evm EVM instance
 * @param addr Address to mark as warm
 */
void evm_mark_address_warm(evm_t *evm, const address_t *addr);

/**
 * Check if a storage slot is warm (already accessed)
 *
 * @param evm EVM instance
 * @param addr Contract address
 * @param key Storage key
 * @return true if storage slot is warm, false if cold
 */
bool evm_is_storage_warm(const evm_t *evm, const address_t *addr, const uint256_t *key);

/**
 * Mark a storage slot as warm (accessed)
 *
 * @param evm EVM instance
 * @param addr Contract address
 * @param key Storage key
 */
void evm_mark_storage_warm(evm_t *evm, const address_t *addr, const uint256_t *key);

//==============================================================================
// EIP-7702 Delegation Resolution
//==============================================================================

/**
 * Check if an account has a delegation designator (EIP-7702)
 *
 * If the account's code is exactly 23 bytes starting with 0xef0100,
 * extracts the 20-byte target address.
 *
 * @param state State instance
 * @param addr Address to check
 * @param target_addr Output: delegation target address (if delegated)
 * @return true if account is delegated, false otherwise
 */
bool evm_resolve_delegation(evm_state_t *state, const address_t *addr, address_t *target_addr);

//==============================================================================
// Result Helpers
//==============================================================================

/**
 * Create a success result
 *
 * @param gas_left Remaining gas
 * @param output_data Return data (will be copied)
 * @param output_size Size of return data
 * @return Result structure
 */
evm_result_t evm_result_success(uint64_t gas_left, const uint8_t *output_data, size_t output_size);

/**
 * Create a revert result
 *
 * @param gas_left Remaining gas
 * @param output_data Revert data (will be copied)
 * @param output_size Size of revert data
 * @return Result structure
 */
evm_result_t evm_result_revert(uint64_t gas_left, const uint8_t *output_data, size_t output_size);

/**
 * Create an error result
 *
 * @param status Error status
 * @param gas_left Remaining gas
 * @return Result structure
 */
evm_result_t evm_result_error(evm_status_t status, uint64_t gas_left);

/**
 * Free result data
 *
 * @param result Result to free
 */
void evm_result_free(evm_result_t *result);

//==============================================================================
// Log Helpers
//==============================================================================

/** Free a single log entry's data (does not free the struct itself). */
void evm_log_free(evm_log_t *log);

/** Free all logs in the EVM accumulator and reset count/cap. */
void evm_logs_clear(evm_t *evm);

/** Truncate log accumulator back to a saved position (revert logs). */
static inline void evm_logs_truncate(evm_t *evm, size_t logs_before) {
    for (size_t i = logs_before; i < evm->log_count; i++)
        evm_log_free(&evm->logs[i]);
    evm->log_count = logs_before;
}

//==============================================================================
// Message Helpers
//==============================================================================

/**
 * Create a CALL message
 *
 * @param caller Caller address
 * @param recipient Recipient address
 * @param value Value to transfer
 * @param input_data Call data
 * @param input_size Size of call data
 * @param gas Gas limit
 * @param depth Call depth
 * @return Message structure
 */
evm_message_t evm_message_call(const address_t *caller,
                                const address_t *recipient,
                                const uint256_t *value,
                                const uint8_t *input_data,
                                size_t input_size,
                                uint64_t gas,
                                int32_t depth);

/**
 * Create a DELEGATECALL message
 *
 * @param caller Caller address
 * @param recipient Recipient address (code to execute)
 * @param input_data Call data
 * @param input_size Size of call data
 * @param gas Gas limit
 * @param depth Call depth
 * @return Message structure
 */
evm_message_t evm_message_delegatecall(const address_t *caller,
                                        const address_t *recipient,
                                        const uint8_t *input_data,
                                        size_t input_size,
                                        uint64_t gas,
                                        int32_t depth);

/**
 * Create a CREATE message
 *
 * @param caller Caller address
 * @param value Value to transfer
 * @param init_code Initialization code
 * @param init_code_size Size of init code
 * @param gas Gas limit
 * @param depth Call depth
 * @return Message structure
 */
evm_message_t evm_message_create(const address_t *caller,
                                    const uint256_t *value,
                                    const uint8_t *init_code,
                                    size_t init_code_size,
                                    uint64_t gas,
                                    int32_t depth);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_H */
