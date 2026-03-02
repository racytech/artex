/**
 * VM Gas Costs — Constants and Helpers
 *
 * Simplified gas model for EOF-native VM on Verkle state.
 * No fork-dependent costs — single set of constants.
 * Witness-based access charging replaces EIP-2929 warm/cold.
 */

#ifndef ART_VM_GAS_H
#define ART_VM_GAS_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

//==============================================================================
// Tier Costs (base costs for opcode categories)
//==============================================================================

#define VM_GAS_ZERO           0    // STOP, RETURN, REVERT
#define VM_GAS_BASE           2    // ADDRESS, ORIGIN, CALLER, etc.
#define VM_GAS_VERY_LOW       3    // ADD, SUB, AND, OR, XOR, etc.
#define VM_GAS_LOW            5    // MUL, DIV, MOD
#define VM_GAS_MID            8    // ADDMOD, MULMOD
#define VM_GAS_HIGH          10    // (reserved)

//==============================================================================
// Memory
//==============================================================================

#define VM_GAS_MEMORY         3    // Per-word memory expansion (linear term)
#define VM_GAS_MEMORY_QUAD  512    // Quadratic divisor

//==============================================================================
// Witness Costs (Verkle)
//==============================================================================

#define VM_WITNESS_BRANCH_COST  1900   // First access to a new stem
#define VM_WITNESS_CHUNK_COST    200   // First access to a new leaf in that stem

//==============================================================================
// Storage
//==============================================================================

#define VM_GAS_SSTORE_SET           20000  // zero → non-zero
#define VM_GAS_SSTORE_RESET          5000  // non-zero → non-zero
#define VM_GAS_SSTORE_CLEAR_REFUND   4800  // non-zero → zero refund

//==============================================================================
// Call Operations
//==============================================================================

#define VM_GAS_CALL_STIPEND    2300   // Minimum gas passed to callee with value
#define VM_GAS_CALL_VALUE      9000   // Additional cost for non-zero value transfer
#define VM_GAS_NEW_ACCOUNT    25000   // Creating new account via value transfer

//==============================================================================
// Keccak256
//==============================================================================

#define VM_GAS_SHA3            30     // Base cost
#define VM_GAS_SHA3_WORD        6     // Per 32-byte word

//==============================================================================
// Log Operations
//==============================================================================

#define VM_GAS_LOG            375     // Base cost
#define VM_GAS_LOG_TOPIC      375     // Per topic
#define VM_GAS_LOG_DATA         8     // Per data byte

//==============================================================================
// Copy Operations
//==============================================================================

#define VM_GAS_COPY             3     // Per word for CALLDATACOPY, DATACOPY, etc.

//==============================================================================
// Contract Creation
//==============================================================================

#define VM_GAS_EOFCREATE    32000     // EOFCREATE base cost
#define VM_GAS_CODEDEPOSIT    200     // Per byte of deployed code

//==============================================================================
// Transaction Costs
//==============================================================================

#define VM_GAS_TRANSACTION    21000   // Base transaction cost
#define VM_GAS_TX_CREATE      32000   // Additional for contract creation tx
#define VM_GAS_TX_DATA_ZERO       4   // Per zero byte in tx data
#define VM_GAS_TX_DATA_NONZERO   16   // Per non-zero byte in tx data

//==============================================================================
// EXP
//==============================================================================

#define VM_GAS_EXP            10     // Base cost
#define VM_GAS_EXP_BYTE       50     // Per byte of exponent

//==============================================================================
// Other
//==============================================================================

#define VM_GAS_BLOCKHASH      20     // BLOCKHASH
#define VM_GAS_SELFBALANCE     5     // SELFBALANCE

#define VM_MEMORY_WORD_SIZE   32     // Word size in bytes

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Calculate memory expansion gas cost.
 * Formula: (words^2 / 512) + (3 * words), incremental from current to new.
 */
uint64_t vm_gas_memory_expansion(uint64_t current_size, uint64_t new_size);

/**
 * Calculate copy operation gas cost (per-word × 3, rounded up).
 */
uint64_t vm_gas_copy_cost(uint64_t size);

/**
 * Calculate SHA3/KECCAK256 gas cost: 30 + 6 * ceil(size/32).
 */
uint64_t vm_gas_sha3_cost(uint64_t size);

/**
 * Calculate LOG gas cost: 375 + topics*375 + data_size*8.
 */
uint64_t vm_gas_log_cost(uint8_t topic_count, uint64_t data_size);

/**
 * Calculate EXP gas cost: 10 + 50 * exponent_bytes.
 */
uint64_t vm_gas_exp_cost(uint8_t exponent_bytes);

/**
 * Round byte size up to 32-byte word count.
 */
uint64_t vm_gas_to_word_size(uint64_t size);

/**
 * 63/64 rule for gas forwarding in calls (EIP-150).
 */
uint64_t vm_gas_max_call_gas(uint64_t gas_left);

#ifdef __cplusplus
}
#endif

#endif /* ART_VM_GAS_H */
