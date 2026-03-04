/**
 * Transaction Execution Layer
 * 
 * Handles protocol-level transaction processing including:
 * - Nonce validation and increment
 * - Gas payment and refunds
 * - Value transfers
 * - EVM execution orchestration
 * - Contract creation
 * - State finalization
 */

#ifndef ART_EVM_TRANSACTION_H
#define ART_EVM_TRANSACTION_H

#include "evm.h"
#include "address.h"
#include "uint256.h"
#include "hash.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Transaction type
 */
typedef enum {
    TX_TYPE_LEGACY = 0,      // Pre-EIP-2718 transaction
    TX_TYPE_EIP2930 = 1,     // EIP-2930: Optional access lists
    TX_TYPE_EIP1559 = 2,     // EIP-1559: Fee market change
    TX_TYPE_EIP4844 = 3      // EIP-4844: Blob transactions
} transaction_type_t;

/**
 * Access list entry (EIP-2930)
 */
typedef struct {
    address_t address;
    uint256_t *storage_keys;
    size_t storage_keys_count;
} access_list_entry_t;

/**
 * Transaction structure
 */
typedef struct {
    transaction_type_t type;
    
    // Common fields
    uint64_t nonce;
    address_t sender;
    address_t to;              // Zero address for contract creation
    uint256_t value;
    uint64_t gas_limit;
    
    // Gas pricing (depends on type)
    uint256_t gas_price;       // Legacy and EIP-2930
    uint256_t max_fee_per_gas; // EIP-1559
    uint256_t max_priority_fee_per_gas; // EIP-1559
    
    // Input data
    const uint8_t *data;
    size_t data_size;
    
    // Access list (EIP-2930)
    access_list_entry_t *access_list;
    size_t access_list_count;

    // EIP-4844 Blob fields
    uint256_t max_fee_per_blob_gas;
    const hash_t *blob_versioned_hashes;
    size_t blob_versioned_hashes_count;

    // Flags
    bool is_create;            // True if this is a contract creation
} transaction_t;

/**
 * Block environment for transaction execution
 */
typedef struct {
    address_t coinbase;        // Miner/beneficiary address
    uint64_t block_number;
    uint64_t timestamp;
    uint64_t gas_limit;
    uint256_t difficulty;
    uint256_t base_fee;        // EIP-1559 base fee
    hash_t prev_randao;        // Post-merge: PREVRANDAO (replaces difficulty)
    uint256_t excess_blob_gas; // EIP-4844 excess blob gas
    bool skip_coinbase_payment; // Skip coinbase payment (for state tests)
} block_env_t;

/**
 * Transaction execution result
 */
typedef struct {
    evm_status_t status;       // Execution status
    uint64_t gas_used;         // Actual gas consumed
    uint64_t gas_refund;       // Gas refund amount
    
    // Output
    uint8_t *output_data;      // Return data (or deployed code for CREATE)
    size_t output_size;
    
    // Contract creation
    address_t contract_address; // Created contract address (for CREATE)
    bool contract_created;      // True if contract was created
    
    // Logs (not yet implemented)
    // log_t *logs;
    // size_t log_count;
} transaction_result_t;

/**
 * Execute a transaction with full protocol-level processing
 * 
 * This function handles the complete transaction lifecycle:
 * 1. Validate transaction (nonce, balance)
 * 2. Apply pre-execution state changes (increment nonce, deduct gas)
 * 3. Execute EVM
 * 4. Apply post-execution state changes (refund gas, pay coinbase)
 * 5. Finalize state
 * 
 * @param evm EVM instance
 * @param state StateDB instance
 * @param tx Transaction to execute
 * @param env Block environment
 * @param result Output transaction result
 * @return true on success (even if EVM reverted), false on transaction-level failure
 */
bool transaction_execute(
    evm_t *evm,
    const transaction_t *tx,
    const block_env_t *env,
    transaction_result_t *result
);

/**
 * Validate transaction without executing
 * 
 * Checks:
 * - Nonce matches sender's current nonce
 * - Sender has sufficient balance for value + gas
 * - Gas limit is within block gas limit
 * 
 * @param state StateDB instance
 * @param tx Transaction to validate
 * @param env Block environment
 * @return true if valid, false otherwise
 */
bool transaction_validate(
    evm_state_t *state,
    const transaction_t *tx,
    const block_env_t *env
);

/**
 * Calculate effective gas price for a transaction
 * 
 * For legacy/EIP-2930: returns gas_price
 * For EIP-1559: returns min(max_fee_per_gas, base_fee + max_priority_fee_per_gas)
 * 
 * @param tx Transaction
 * @param env Block environment
 * @return Effective gas price
 */
uint256_t transaction_effective_gas_price(
    const transaction_t *tx,
    const block_env_t *env
);

/**
 * EIP-4844: Calculate blob base fee from excess blob gas
 *
 * @param excess_blob_gas Excess blob gas from block header
 * @return Blob base fee
 */
uint256_t calc_blob_gas_price(const uint256_t *excess_blob_gas);

/**
 * Free transaction result resources
 *
 * @param result Transaction result to free
 */
void transaction_result_free(transaction_result_t *result);

#ifdef __cplusplus
}
#endif

#endif // ART_EVM_TRANSACTION_H
