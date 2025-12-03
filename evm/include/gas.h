/**
 * Gas Costs - Ethereum Gas Cost Constants and Tables
 *
 * This module defines gas costs for all EVM operations based on the
 * Ethereum Yellow Paper and various EIPs. Gas costs can vary across
 * hard forks, so fork-specific costs are handled here.
 *
 * Gas cost categories:
 * - Zero tier: 0 gas (STOP, RETURN, etc.)
 * - Base tier: 2 gas (simple operations)
 * - Very low tier: 3 gas (arithmetic, bitwise)
 * - Low tier: 5 gas (multiplication)
 * - Mid tier: 8 gas (ADDMOD, MULMOD)
 * - High tier: 10 gas (JUMPI)
 * - Special: Variable costs (SSTORE, CALL, etc.)
 */

#ifndef ART_EVM_GAS_H
#define ART_EVM_GAS_H

#include "fork.h"
#include "uint256.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Gas Cost Constants
//==============================================================================

// Tier costs (base costs for opcode categories)
#define GAS_ZERO           0    // Zero tier
#define GAS_BASE           2    // Base tier
#define GAS_VERY_LOW       3    // Very low tier
#define GAS_LOW            5    // Low tier
#define GAS_MID            8    // Mid tier
#define GAS_HIGH           10   // High tier

// Memory operations
#define GAS_MEMORY         3    // Memory expansion cost per word
#define GAS_MEMORY_QUAD    512  // Quadratic divisor for memory

// Storage operations (pre-Berlin, simplified)
#define GAS_SLOAD_LEGACY        200   // Legacy SLOAD cost (pre-Istanbul)
#define GAS_SLOAD_ISTANBUL      800   // SLOAD cost (Istanbul to Berlin)
#define GAS_SLOAD_COLD          2100  // Cold SLOAD (Berlin+, EIP-2929)
#define GAS_SLOAD_WARM          100   // Warm SLOAD (Berlin+, EIP-2929)

#define GAS_SSTORE_SET          20000 // Storage slot set from zero
#define GAS_SSTORE_RESET        5000  // Storage slot reset to zero
#define GAS_SSTORE_REFUND       15000 // Refund for clearing storage (pre-London)

// Call operations
#define GAS_CALL               700   // Base cost for CALL (after Tangerine Whistle)
#define GAS_CALL_VALUE         9000  // Additional cost for non-zero value transfer
#define GAS_CALL_STIPEND       2300  // Stipend for calls with value
#define GAS_CALL_NEW_ACCOUNT   25000 // Cost for creating new account

// Account operations
#define GAS_BALANCE            400   // BALANCE cost (Berlin+, was 700 pre-Berlin for cold)
#define GAS_EXTCODESIZE        700   // EXTCODESIZE cost (cold, pre-Berlin)
#define GAS_EXTCODECOPY        700   // EXTCODECOPY base cost (cold, pre-Berlin)
#define GAS_EXTCODEHASH        700   // EXTCODEHASH cost (cold, pre-Berlin)

// Cold/warm access (EIP-2929, Berlin+)
#define GAS_COLD_ACCOUNT_ACCESS 2600 // Cold account access
#define GAS_WARM_ACCOUNT_ACCESS 100  // Warm account access
#define GAS_COLD_SLOAD         2100  // Cold storage access
#define GAS_WARM_SLOAD         100   // Warm storage access

// Contract creation
#define GAS_CREATE             32000 // CREATE opcode
#define GAS_CODEDEPOSIT        200   // Cost per byte of deployed code

// Hash operations
#define GAS_SHA3               30    // SHA3 base cost
#define GAS_SHA3_WORD          6     // SHA3 per word cost

// Log operations
#define GAS_LOG                375   // LOG base cost
#define GAS_LOG_TOPIC          375   // Cost per LOG topic
#define GAS_LOG_DATA           8     // Cost per LOG data byte

// Copy operations
#define GAS_COPY               3     // Cost per word for CALLDATACOPY, CODECOPY, etc.

// Transaction costs (not opcodes, but useful)
#define GAS_TRANSACTION        21000 // Base transaction cost
#define GAS_TX_CREATE          32000 // Additional cost for contract creation tx
#define GAS_TX_DATA_ZERO       4     // Cost per zero byte in tx data
#define GAS_TX_DATA_NONZERO    16    // Cost per non-zero byte in tx data (pre-Istanbul)
#define GAS_TX_DATA_NONZERO_ISTANBUL 16 // Istanbul kept same cost

// Precompiled contract costs (simplified)
#define GAS_PRECOMPILE_ECRECOVER    3000
#define GAS_PRECOMPILE_SHA256       60
#define GAS_PRECOMPILE_RIPEMD160    600
#define GAS_PRECOMPILE_IDENTITY     15

// Other special costs
#define GAS_SELFDESTRUCT       5000  // SELFDESTRUCT base cost (EIP-150+)
#define GAS_SELFDESTRUCT_REFUND 24000 // Refund for SELFDESTRUCT (pre-London)
#define GAS_JUMPDEST           1     // JUMPDEST cost

//==============================================================================
// Opcode Gas Cost Structure
//==============================================================================

/**
 * Gas cost information for an opcode
 */
typedef struct
{
    uint16_t base_gas;        // Base/static gas cost
    bool has_dynamic_cost;    // Whether opcode has dynamic gas calculation
} opcode_gas_info_t;

//==============================================================================
// Gas Cost Lookup
//==============================================================================

/**
 * Get base gas cost for an opcode
 * This returns the static/base cost. Dynamic costs must be calculated separately.
 *
 * @param opcode Opcode byte (0x00-0xFF)
 * @param fork Current fork
 * @return Base gas cost
 */
uint64_t gas_get_opcode_cost(uint8_t opcode, evm_fork_t fork);

/**
 * Get detailed gas info for an opcode
 *
 * @param opcode Opcode byte (0x00-0xFF)
 * @param fork Current fork
 * @return Gas information structure
 */
opcode_gas_info_t gas_get_opcode_info(uint8_t opcode, evm_fork_t fork);

/**
 * Check if an opcode has dynamic gas costs
 *
 * @param opcode Opcode byte
 * @return true if opcode has dynamic gas costs
 */
bool gas_has_dynamic_cost(uint8_t opcode);

//==============================================================================
// Dynamic Gas Cost Calculations
//==============================================================================

/**
 * Calculate memory expansion gas cost
 * Formula: (words^2 / 512) + (3 * words)
 *
 * @param current_size Current memory size in bytes
 * @param new_size New memory size in bytes after expansion
 * @return Incremental gas cost for expansion
 */
uint64_t gas_memory_expansion(uint64_t current_size, uint64_t new_size);

/**
 * Calculate copy operation gas cost (CALLDATACOPY, CODECOPY, etc.)
 *
 * @param size Number of bytes to copy
 * @return Gas cost for copy (size rounded up to words * 3)
 */
uint64_t gas_copy_cost(uint64_t size);

/**
 * Calculate SHA3 gas cost
 *
 * @param size Size of data to hash in bytes
 * @return Total gas cost (base + per-word)
 */
uint64_t gas_sha3_cost(uint64_t size);

/**
 * Calculate LOG operation gas cost
 *
 * @param topic_count Number of topics (0-4)
 * @param data_size Size of data in bytes
 * @return Total gas cost
 */
uint64_t gas_log_cost(uint8_t topic_count, uint64_t data_size);

/**
 * Calculate EXP gas cost
 *
 * @param exponent_bytes Number of bytes in exponent (not zero)
 * @param fork Current fork
 * @return Gas cost for EXP operation
 */
uint64_t gas_exp_cost(uint8_t exponent_bytes, evm_fork_t fork);

//==============================================================================
// SSTORE Gas Costs (Complex, Fork-Dependent)
//==============================================================================

/**
 * SSTORE gas cost calculation
 * Implements EIP-2200 (Istanbul), EIP-2929 (Berlin), EIP-3529 (London)
 *
 * @param fork Current fork
 * @param current_value Current storage value
 * @param original_value Original storage value (before tx)
 * @param new_value New value being stored
 * @param is_cold Whether this is a cold storage access (Berlin+)
 * @param gas_refund Output parameter for gas refund amount
 * @return Gas cost for SSTORE
 */
uint64_t gas_sstore_cost(evm_fork_t fork,
                         const uint256_t *current_value,
                         const uint256_t *original_value,
                         const uint256_t *new_value,
                         bool is_cold,
                         int64_t *gas_refund);

//==============================================================================
// CALL Family Gas Costs
//==============================================================================

/**
 * Calculate CALL opcode gas cost
 * Includes account access, value transfer, new account creation
 *
 * @param fork Current fork
 * @param is_cold Whether account is cold (Berlin+)
 * @param has_value Whether call transfers value
 * @param account_exists Whether recipient account exists
 * @return Gas cost (excluding stipend and memory expansion)
 */
uint64_t gas_call_cost(evm_fork_t fork,
                       bool is_cold,
                       bool has_value,
                       bool account_exists);

/**
 * Calculate gas stipend for CALL with value
 *
 * @param value_transferred Value being transferred
 * @return Gas stipend (2300 if value > 0, else 0)
 */
uint64_t gas_call_stipend(const uint256_t *value_transferred);

//==============================================================================
// Utility Functions
//==============================================================================

/**
 * Calculate 63/64 rule for gas forwarding in calls
 * EIP-150: Only 63/64 of remaining gas can be forwarded
 *
 * @param gas_left Gas remaining before call
 * @return Maximum gas that can be forwarded to sub-call
 */
uint64_t gas_max_call_gas(uint64_t gas_left);

/**
 * Round up byte size to word count (32-byte words)
 *
 * @param size Size in bytes
 * @return Number of 32-byte words (rounded up)
 */
uint64_t gas_to_word_size(uint64_t size);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_GAS_H */
