/**
 * Integration Test Runner
 * 
 * Executes Ethereum test fixtures and verifies results against expected values.
 * Supports blockchain tests, state tests, and transaction tests.
 */

#ifndef ART_TEST_RUNNER_H
#define ART_TEST_RUNNER_H

#include "test_fixtures.h"
#include "evm_state.h"
#include "evm.h"
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Test Result Types
//==============================================================================

/**
 * Test execution status
 */
typedef enum {
    TEST_PASS = 0,              // Test passed
    TEST_FAIL,                  // Test failed (assertion mismatch)
    TEST_ERROR,                 // Test error (crash, exception)
    TEST_SKIP,                  // Test skipped (unsupported fork/feature)
    TEST_TIMEOUT                // Test timed out
} test_status_t;

/**
 * Test failure details
 */
typedef struct {
    char *field;                // Which field failed (e.g., "state_root", "balance")
    char *expected;             // Expected value (hex string)
    char *actual;               // Actual value (hex string)
    char *message;              // Additional error message
} test_failure_t;

/**
 * Test execution result
 */
typedef struct {
    test_status_t status;
    char *test_name;
    char *fork;                 // Fork tested (if applicable)
    
    // Timing
    uint64_t duration_us;       // Execution time in microseconds
    
    // Failure information
    test_failure_t *failures;
    size_t failure_count;
    
    // Additional info
    char *skip_reason;          // Reason for skipping
} test_result_t;

/**
 * Aggregated test results
 */
typedef struct {
    size_t total;
    size_t passed;
    size_t failed;
    size_t errors;
    size_t skipped;
    
    test_result_t *results;
    size_t result_count;
} test_results_t;

//==============================================================================
// Test Execution Context
//==============================================================================

/**
 * Test runner configuration
 */
typedef struct {
    bool verbose;               // Print detailed output
    bool stop_on_fail;          // Stop on first failure
    char **fork_filter;         // Only run specific forks (NULL = all)
    size_t fork_filter_count;
    uint64_t timeout_ms;        // Test timeout in milliseconds
} test_runner_config_t;

/**
 * Test execution environment
 */
typedef struct {
    evm_state_t *state;         // Typed EVM state interface
    state_db_t *sdb;            // Underlying state database (lifecycle)
    evm_t *evm;                 // EVM instance
    test_runner_config_t config;
    
    // Statistics
    uint64_t total_gas_used;
    size_t total_transactions;
} test_runner_t;

//==============================================================================
// Test Runner Lifecycle
//==============================================================================

/**
 * Initialize test runner
 * 
 * @param runner Test runner instance
 * @param config Configuration (can be NULL for defaults)
 * @return true on success, false on failure
 */
bool test_runner_init(test_runner_t *runner, const test_runner_config_t *config);

/**
 * Destroy test runner and free resources
 * 
 * @param runner Test runner instance
 */
void test_runner_destroy(test_runner_t *runner);

/**
 * Reset test runner state (clear state DB, reset counters)
 * 
 * @param runner Test runner instance
 */
void test_runner_reset(test_runner_t *runner);

//==============================================================================
// State Test Execution
//==============================================================================

/**
 * Execute a state test
 * 
 * State test: single transaction execution with state verification
 * - Setup pre-state accounts
 * - Execute transaction
 * - Verify post-state root hash and logs
 * 
 * @param runner Test runner instance
 * @param test State test to execute
 * @param fork Fork to test (NULL = test all forks in test)
 * @param result Output result (allocated)
 * @return true on success, false on error
 */
bool test_runner_run_state_test(test_runner_t *runner,
                                const state_test_t *test,
                                const char *fork,
                                test_result_t *result);

//==============================================================================
// Blockchain Test Execution
//==============================================================================

/**
 * Execute a blockchain test
 * 
 * Blockchain test: multi-block execution with state transitions
 * - Setup genesis state
 * - Process blocks sequentially
 * - Verify final state and block hashes
 * 
 * @param runner Test runner instance
 * @param test Blockchain test to execute
 * @param result Output result (allocated)
 * @return true on success, false on error
 */
bool test_runner_run_blockchain_test(test_runner_t *runner,
                                     const blockchain_test_t *test,
                                     test_result_t *result);

//==============================================================================
// Transaction Test Execution
//==============================================================================

/**
 * Execute a transaction test
 * 
 * Transaction test: validate transaction without execution
 * - Decode transaction RLP
 * - Validate format and signature
 * - Check for expected exceptions
 * 
 * @param runner Test runner instance
 * @param test Transaction test to execute
 * @param fork Fork to test (NULL = test all forks)
 * @param result Output result (allocated)
 * @return true on success, false on error
 */
bool test_runner_run_transaction_test(test_runner_t *runner,
                                      const transaction_test_t *test,
                                      const char *fork,
                                      test_result_t *result);

//==============================================================================
// Test Discovery and Batch Execution
//==============================================================================

/**
 * Run all tests in a JSON file
 * 
 * Automatically detects test type and executes accordingly
 * 
 * @param runner Test runner instance
 * @param filepath Path to JSON test file
 * @param results Output results (allocated)
 * @return true on success, false on error
 */
bool test_runner_run_file(test_runner_t *runner,
                          const char *filepath,
                          test_results_t *results);

/**
 * Run all tests in a directory (recursive)
 * 
 * @param runner Test runner instance
 * @param directory Path to directory containing test files
 * @param results Output aggregated results
 * @return true on success, false on error
 */
bool test_runner_run_directory(test_runner_t *runner,
                               const char *directory,
                               test_results_t *results);

//==============================================================================
// Result Management
//==============================================================================

/**
 * Initialize test result
 * 
 * @param result Result structure to initialize
 */
void test_result_init(test_result_t *result);

/**
 * Free test result and all allocated resources
 * 
 * @param result Result to free
 */
void test_result_free(test_result_t *result);

/**
 * Add a failure to test result
 * 
 * @param result Result to update
 * @param field Field that failed
 * @param expected Expected value (hex string)
 * @param actual Actual value (hex string)
 * @param message Additional error message (can be NULL)
 */
void test_result_add_failure(test_result_t *result,
                            const char *field,
                            const char *expected,
                            const char *actual,
                            const char *message);

/**
 * Initialize test results aggregate
 * 
 * @param results Results structure to initialize
 */
void test_results_init(test_results_t *results);

/**
 * Free test results and all contained results
 * 
 * @param results Results to free
 */
void test_results_free(test_results_t *results);

/**
 * Add a test result to aggregated results
 * 
 * @param results Aggregate results
 * @param result Single test result to add (copied)
 */
void test_results_add(test_results_t *results, const test_result_t *result);

/**
 * Print test results summary
 * 
 * @param results Results to print
 * @param verbose Print detailed failure information
 */
void test_results_print(const test_results_t *results, bool verbose);

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Setup pre-state accounts in StateDB
 * 
 * @param state_db State database
 * @param accounts Array of account states
 * @param count Number of accounts
 * @return true on success, false on error
 */
bool test_runner_setup_state(evm_state_t *state,
                             const test_account_t *accounts,
                             size_t count);

/**
 * Verify post-state matches expected state root
 *
 * @param state EVM state
 * @param expected_root Expected state root hash
 * @param actual_root Output actual root hash (can be NULL)
 * @return true if matches, false otherwise
 */
bool test_runner_verify_state_root(evm_state_t *state,
                                   const hash_t *expected_root,
                                   hash_t *actual_root);

/**
 * Compare two hashes
 * 
 * @param a First hash
 * @param b Second hash
 * @return true if equal, false otherwise
 */
bool hash_equals(const hash_t *a, const hash_t *b);

/**
 * Convert hash to hex string (allocated)
 * 
 * @param hash Hash to convert
 * @return Hex string with "0x" prefix (caller must free)
 */
char *hash_to_hex_string(const hash_t *hash);

/**
 * Convert uint256 to hex string (allocated)
 * 
 * @param value Value to convert
 * @return Hex string with "0x" prefix (caller must free)
 */
char *uint256_to_hex_string(const uint256_t *value);

#ifdef __cplusplus
}
#endif

#endif // ART_TEST_RUNNER_H
