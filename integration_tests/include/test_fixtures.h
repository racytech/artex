/**
 * Integration Test Fixtures
 * 
 * Data structures for parsing and representing Ethereum test fixtures:
 * - Blockchain tests: Full block execution with state transitions
 * - State tests: Single transaction execution with state verification
 * - Transaction tests: Transaction validation tests
 */

#ifndef ART_TEST_FIXTURES_H
#define ART_TEST_FIXTURES_H

#include "uint256.h"
#include "hash.h"
#include "address.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Common Types
//==============================================================================

/**
 * Storage entry (key-value pair)
 */
typedef struct {
    uint256_t key;
    uint256_t value;
} test_storage_entry_t;

/**
 * Account state for pre/post conditions
 */
typedef struct {
    address_t address;
    uint256_t nonce;
    uint256_t balance;
    uint8_t *code;
    size_t code_len;
    
    // Storage: key-value pairs
    test_storage_entry_t *storage;
    size_t storage_count;
} test_account_t;

/**
 * Block header information
 */
typedef struct {
    hash_t parent_hash;
    hash_t uncle_hash;
    address_t coinbase;
    hash_t state_root;
    hash_t transactions_trie;
    hash_t receipt_trie;
    uint8_t bloom[256];
    uint256_t difficulty;
    uint256_t number;
    uint256_t gas_limit;
    uint256_t gas_used;
    uint256_t timestamp;
    uint8_t *extra_data;
    size_t extra_data_len;
    hash_t mix_hash;
    uint64_t nonce;
    hash_t hash;
    
    // Optional fields (post-merge)
    uint256_t base_fee;      // EIP-1559
    hash_t withdrawals_root; // EIP-4895
    uint256_t blob_gas_used; // EIP-4844
    uint256_t excess_blob_gas;
    hash_t parent_beacon_block_root;
} test_block_header_t;

/**
 * Access list entry (EIP-2930)
 * Contains an address and its associated storage keys
 */
typedef struct {
    address_t address;
    uint256_t *storage_keys;
    size_t storage_keys_count;
} test_access_list_entry_t;

/**
 * Access list for a transaction
 */
typedef struct {
    test_access_list_entry_t *entries;
    size_t entries_count;
} test_access_list_t;

/**
 * Authorization tuple for EIP-7702 set code transactions
 */
typedef struct {
    uint256_t chain_id;
    address_t address;       // Delegate target
    uint64_t nonce;
    uint8_t y_parity;
    uint256_t r;
    uint256_t s;
    address_t signer;        // Pre-computed signer from test JSON
} test_authorization_t;

/**
 * Transaction data
 */
typedef struct {
    uint256_t nonce;
    uint256_t gas_price;                    // Legacy transactions
    uint256_t max_fee_per_gas;              // EIP-1559
    uint256_t max_priority_fee_per_gas;     // EIP-1559
    uint256_t *gas_limit;   // Array for multiple test cases
    size_t gas_limit_count;
    address_t to;
    bool is_create;         // true if to is empty (contract creation)
    uint256_t *value;       // Array for multiple test cases
    size_t value_count;
    uint8_t **data;         // Array of data for multiple test cases
    size_t *data_len;
    size_t data_count;
    address_t sender;
    uint8_t secret_key[32];
    
    // Signature (for pre-signed transactions)
    uint256_t v;
    uint256_t r;
    uint256_t s;
    
    // Access lists (EIP-2930) - Array for multiple test cases
    test_access_list_t *access_lists;
    size_t access_lists_count;

    // EIP-4844 Blob transaction fields
    uint256_t max_fee_per_blob_gas;
    hash_t *blob_versioned_hashes;
    size_t blob_versioned_hashes_count;

    // EIP-7702 Authorization list
    test_authorization_t *authorization_list;
    size_t authorization_list_count;
    bool has_authorization_list;  // true if authorizationList field present in JSON
} test_transaction_t;

/**
 * Block with transactions
 */
typedef struct {
    test_block_header_t header;
    uint8_t *rlp;           // Full block RLP
    size_t rlp_len;
    uint8_t **transactions; // Array of transaction RLP
    size_t *tx_len;
    size_t tx_count;
    uint8_t **uncles;       // Array of uncle headers RLP
    size_t *uncle_len;
    size_t uncle_count;
} test_block_t;

//==============================================================================
// Blockchain Test Format
//==============================================================================

/**
 * Complete blockchain test case
 * Represents a sequence of blocks with state verification
 */
typedef struct {
    char *name;             // Test name/identifier
    char *network;          // Fork name (Frontier, Homestead, Berlin, etc.)
    
    test_block_header_t genesis_header;
    uint8_t *genesis_rlp;
    size_t genesis_rlp_len;
    
    // Pre-state: accounts before execution
    test_account_t *pre_state;
    size_t pre_state_count;
    
    // Post-state: expected accounts after execution
    test_account_t *post_state;
    size_t post_state_count;
    
    // Blocks to execute
    test_block_t *blocks;
    size_t block_count;
    
    hash_t last_block_hash; // Expected hash of last block
    
    // Chain config
    uint256_t chain_id;
} blockchain_test_t;

//==============================================================================
// State Test Format
//==============================================================================

/**
 * Environment (block context) for state test
 */
typedef struct {
    address_t coinbase;
    uint256_t gas_limit;
    uint256_t number;
    uint256_t timestamp;
    uint256_t difficulty;
    uint256_t base_fee;         // EIP-1559 (optional)
    hash_t prev_randao;         // Post-merge PREVRANDAO (currentRandom)
    uint256_t excess_blob_gas;  // EIP-4844 (currentExcessBlobGas)
} test_env_t;

/**
 * Expected post-state for a specific fork
 */
typedef struct {
    hash_t state_root;      // Expected state root hash
    hash_t logs_hash;       // Hash of logs bloom
    uint8_t *tx_bytes;      // Signed transaction RLP
    size_t tx_bytes_len;
    
    // Test case indices (for parameterized tests)
    uint32_t data_index;
    uint32_t gas_index;
    uint32_t value_index;
    
    // Exception expectation (NULL if transaction should succeed)
    char *expect_exception;
} test_post_condition_t;

/**
 * State test case
 * Single transaction execution with state verification
 */
typedef struct {
    char *name;             // Test name/identifier
    
    test_env_t env;         // Block environment
    
    // Pre-state: accounts before execution
    test_account_t *pre_state;
    size_t pre_state_count;
    
    test_transaction_t transaction;
    
    // Post-conditions by fork
    struct {
        char *fork_name;
        test_post_condition_t *conditions;
        size_t condition_count;
    } *post;
    size_t post_count;
} state_test_t;

//==============================================================================
// Transaction Test Format
//==============================================================================

/**
 * Expected transaction validation result
 */
typedef struct {
    char *fork_name;
    uint256_t intrinsic_gas;
    char *exception;        // NULL if valid, exception name if invalid
} tx_test_result_t;

/**
 * Transaction validation test
 * Tests transaction validity without execution
 */
typedef struct {
    char *name;             // Test name/identifier
    uint8_t *tx_bytes;      // Raw transaction RLP
    size_t tx_bytes_len;
    
    // Expected results by fork
    tx_test_result_t *results;
    size_t result_count;
    
    // Metadata
    char *description;
    char *url;
} transaction_test_t;

//==============================================================================
// Lifecycle Functions
//==============================================================================

/**
 * Free blockchain test and all allocated resources
 */
void blockchain_test_free(blockchain_test_t *test);

/**
 * Free state test and all allocated resources
 */
void state_test_free(state_test_t *test);

/**
 * Free transaction test and all allocated resources
 */
void transaction_test_free(transaction_test_t *test);

/**
 * Free test account and its resources
 */
void test_account_free(test_account_t *account);

#ifdef __cplusplus
}
#endif

#endif // ART_TEST_FIXTURES_H
