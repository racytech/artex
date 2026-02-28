/**
 * Test Parser Verification
 * 
 * Simple test to verify JSON parsing works correctly
 */

#include "test_parser.h"
#include "test_fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void print_hash(const hash_t *hash) {
    printf("0x");
    for (int i = 0; i < 32; i++) {
        printf("%02x", hash->bytes[i]);
    }
}

static void print_address(const address_t *addr) {
    printf("0x");
    for (int i = 0; i < 20; i++) {
        printf("%02x", addr->bytes[i]);
    }
}

static void test_state_test(const char *filepath) {
    printf("\n=== Testing State Test Parser ===\n");
    printf("File: %s\n", filepath);
    
    state_test_t *test = NULL;
    if (!parse_state_test(filepath, &test)) {
        printf("ERROR: Failed to parse state test\n");
        return;
    }
    
    printf("Test Name: %s\n", test->name ? test->name : "(none)");
    printf("Pre-state accounts: %zu\n", test->pre_state_count);
    
    for (size_t i = 0; i < test->pre_state_count; i++) {
        printf("  Account %zu: ", i);
        print_address(&test->pre_state[i].address);
        printf("\n    Storage entries: %zu\n", test->pre_state[i].storage_count);
    }
    
    printf("Post-conditions by fork: %zu\n", test->post_count);
    for (size_t i = 0; i < test->post_count; i++) {
        printf("  Fork: %s, Conditions: %zu\n", 
               test->post[i].fork_name, 
               test->post[i].condition_count);
    }
    
    state_test_free(test);
    printf("✓ State test parsed successfully\n");
}

static void test_blockchain_test(const char *filepath) {
    printf("\n=== Testing Blockchain Test Parser ===\n");
    printf("File: %s\n", filepath);
    
    blockchain_test_t *test = NULL;
    if (!parse_blockchain_test(filepath, &test)) {
        printf("ERROR: Failed to parse blockchain test\n");
        return;
    }
    
    printf("Test Name: %s\n", test->name ? test->name : "(none)");
    printf("Network: %s\n", test->network ? test->network : "(none)");
    printf("Pre-state accounts: %zu\n", test->pre_state_count);
    printf("Post-state accounts: %zu\n", test->post_state_count);
    printf("Blocks: %zu\n", test->block_count);
    printf("Last block hash: ");
    print_hash(&test->last_block_hash);
    printf("\n");
    
    blockchain_test_free(test);
    printf("✓ Blockchain test parsed successfully\n");
}

static void test_transaction_test(const char *filepath) {
    printf("\n=== Testing Transaction Test Parser ===\n");
    printf("File: %s\n", filepath);
    
    transaction_test_t *test = NULL;
    if (!parse_transaction_test(filepath, &test)) {
        printf("ERROR: Failed to parse transaction test\n");
        return;
    }
    
    printf("Test Name: %s\n", test->name ? test->name : "(none)");
    printf("TX bytes length: %zu\n", test->tx_bytes_len);
    printf("Results by fork: %zu\n", test->result_count);
    
    for (size_t i = 0; i < test->result_count; i++) {
        printf("  Fork: %s", test->results[i].fork_name);
        if (test->results[i].exception) {
            printf(", Exception: %s", test->results[i].exception);
        } else {
            printf(", Valid");
        }
        printf("\n");
    }
    
    if (test->description) {
        printf("Description: %s\n", test->description);
    }
    
    transaction_test_free(test);
    printf("✓ Transaction test parsed successfully\n");
}

int main(int argc, char **argv) {
    printf("Integration Test Parser Verification\n");
    printf("=====================================\n");
    
    if (argc < 2) {
        printf("Usage: %s <test_file.json>\n", argv[0]);
        printf("\nTrying default test files...\n");
        
        // Try parsing different test types
        test_state_test("integration_tests/fixtures/state_tests/frontier/opcodes/test_dup.json");
        test_blockchain_test("integration_tests/fixtures/blockchain_tests/frontier/examples/test_block_intermediate_state.json");
        test_transaction_test("integration_tests/fixtures/transaction_tests/prague/eip7702_set_code_tx/test_empty_authorization_list.json");
        
        return 0;
    }
    
    const char *filepath = argv[1];
    
    // Try to determine test type from path
    if (strstr(filepath, "state_tests")) {
        test_state_test(filepath);
    } else if (strstr(filepath, "blockchain_tests")) {
        test_blockchain_test(filepath);
    } else if (strstr(filepath, "transaction_tests")) {
        test_transaction_test(filepath);
    } else {
        printf("Unknown test type. Trying all parsers...\n");
        test_state_test(filepath);
        test_blockchain_test(filepath);
        test_transaction_test(filepath);
    }
    
    return 0;
}
