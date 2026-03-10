/**
 * JSON Test Fixture Parser
 * 
 * Utilities for parsing Ethereum test fixtures from JSON files using cJSON.
 * Handles common parsing patterns for hex strings, accounts, storage, etc.
 */

#ifndef ART_TEST_PARSER_H
#define ART_TEST_PARSER_H

#include "test_fixtures.h"
#include <cjson/cJSON.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// JSON Parsing Utilities
//==============================================================================

/**
 * Parse hex string to bytes
 * @param hex_str Hex string with optional "0x" prefix
 * @param out Output buffer
 * @param max_len Maximum output buffer size
 * @return Number of bytes written, or 0 on error
 */
size_t parse_hex_string(const char *hex_str, uint8_t *out, size_t max_len);

/**
 * Parse hex string and allocate buffer
 * @param hex_str Hex string with optional "0x" prefix
 * @param out_len Output length (can be NULL)
 * @return Allocated buffer (caller must free), or NULL on error
 */
uint8_t *parse_hex_alloc(const char *hex_str, size_t *out_len);

/**
 * Parse uint256 from hex string
 * @param hex_str Hex string with optional "0x" prefix
 * @param out Output uint256
 * @return true on success, false on error
 */
bool parse_uint256(const char *hex_str, uint256_t *out);

/**
 * Parse uint64 from hex string
 * @param hex_str Hex string with optional "0x" prefix
 * @param out Output uint64
 * @return true on success, false on error
 */
bool parse_uint64(const char *hex_str, uint64_t *out);

/**
 * Parse address from hex string
 * @param hex_str Hex string (20 bytes, with optional "0x" prefix)
 * @param out Output address
 * @return true on success, false on error
 */
bool parse_address(const char *hex_str, address_t *out);

/**
 * Parse hash from hex string
 * @param hex_str Hex string (32 bytes, with optional "0x" prefix)
 * @param out Output hash
 * @return true on success, false on error
 */
bool parse_hash(const char *hex_str, hash_t *out);

/**
 * Parse bloom filter from hex string
 * @param hex_str Hex string (256 bytes, with optional "0x" prefix)
 * @param out Output bloom (256 bytes)
 * @return true on success, false on error
 */
bool parse_bloom(const char *hex_str, uint8_t out[256]);

//==============================================================================
// Account and Storage Parsing
//==============================================================================

/**
 * Parse account from JSON object
 * @param json cJSON object representing an account
 * @param addr Account address
 * @param out Output account structure
 * @return true on success, false on error
 */
bool parse_account(const cJSON *json, const address_t *addr, test_account_t *out);

/**
 * Parse storage map from JSON object
 * @param json cJSON object with storage key-value pairs
 * @param out_storage Output storage array (allocated)
 * @param out_count Output storage count
 * @return true on success, false on error
 */
bool parse_storage(const cJSON *json, 
                   test_storage_entry_t **out_storage,
                   size_t *out_count);

/**
 * Parse account map (pre/post state) from JSON object
 * @param json cJSON object with address -> account mappings
 * @param out_accounts Output account array (allocated)
 * @param out_count Output account count
 * @return true on success, false on error
 */
bool parse_account_map(const cJSON *json, test_account_t **out_accounts, size_t *out_count);

//==============================================================================
// Block and Transaction Parsing
//==============================================================================

/**
 * Parse block header from JSON object
 * @param json cJSON object representing a block header
 * @param out Output block header
 * @return true on success, false on error
 */
bool parse_block_header(const cJSON *json, test_block_header_t *out);

/**
 * Parse transaction from JSON object
 * @param json cJSON object representing a transaction
 * @param out Output transaction
 * @return true on success, false on error
 */
bool parse_transaction(const cJSON *json, test_transaction_t *out);

/**
 * Parse environment (block context) from JSON object
 * @param json cJSON object representing environment
 * @param out Output environment
 * @return true on success, false on error
 */
bool parse_environment(const cJSON *json, test_env_t *out);

//==============================================================================
// Complete Test Parsing
//==============================================================================

/**
 * Parse blockchain test from JSON file
 * @param filepath Path to JSON file
 * @param out Output blockchain test (allocated)
 * @return true on success, false on error
 */
bool parse_blockchain_test(const char *filepath, blockchain_test_t **out);

/**
 * Parse engine test from JSON object (blockchain_tests_engine format)
 * @param test_obj cJSON object for one test entry
 * @param test_name Test name/identifier
 * @param out Output engine test (allocated)
 * @return true on success, false on error
 */
bool parse_engine_test_from_json(const cJSON *test_obj, const char *test_name,
                                  engine_test_t **out);

/**
 * Parse state test from JSON file
 * @param filepath Path to JSON file
 * @param out Output state test (allocated)
 * @return true on success, false on error
 */
bool parse_state_test(const char *filepath, state_test_t **out);

/**
 * Parse transaction test from JSON file
 * @param filepath Path to JSON file
 * @param out Output transaction test (allocated)
 * @return true on success, false on error
 */
bool parse_transaction_test(const char *filepath, transaction_test_t **out);

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Load JSON file into cJSON object
 * @param filepath Path to JSON file
 * @return cJSON object (caller must cJSON_Delete), or NULL on error
 */
cJSON *load_json_file(const char *filepath);

/**
 * Get string from cJSON object, with error handling
 * @param json cJSON object
 * @param key Key to lookup
 * @param out Output string (optional, can be NULL)
 * @return true if key exists and is string, false otherwise
 */
bool json_get_string(const cJSON *json, const char *key, const char **out);

/**
 * Get object from cJSON, with error handling
 * @param json cJSON object
 * @param key Key to lookup
 * @return cJSON object, or NULL if not found or wrong type
 */
const cJSON *json_get_object(const cJSON *json, const char *key);

/**
 * Get array from cJSON, with error handling
 * @param json cJSON object
 * @param key Key to lookup
 * @return cJSON array, or NULL if not found or wrong type
 */
const cJSON *json_get_array(const cJSON *json, const char *key);

#ifdef __cplusplus
}
#endif

#endif // ART_TEST_PARSER_H
