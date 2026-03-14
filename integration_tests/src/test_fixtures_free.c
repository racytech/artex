/**
 * JSON Test Parser - Cleanup Functions
 */

#include "test_fixtures.h"
#include "block.h"
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Account Cleanup
//==============================================================================

void test_account_free(test_account_t *account) {
    if (!account) return;
    
    free(account->code);
    free(account->storage);
    
    memset(account, 0, sizeof(*account));
}

//==============================================================================
// Blockchain Test Cleanup
//==============================================================================

void blockchain_test_free(blockchain_test_t *test) {
    if (!test) return;
    
    free(test->name);
    free(test->network);
    free(test->genesis_rlp);
    free(test->genesis_header.extra_data);
    
    // Free pre-state
    for (size_t i = 0; i < test->pre_state_count; i++) {
        test_account_free(&test->pre_state[i]);
    }
    free(test->pre_state);
    
    // Free post-state
    for (size_t i = 0; i < test->post_state_count; i++) {
        test_account_free(&test->post_state[i]);
    }
    free(test->post_state);
    
    // Free blocks
    for (size_t i = 0; i < test->block_count; i++) {
        test_block_t *block = &test->blocks[i];
        
        free(block->header.extra_data);
        free(block->expect_exception);
        free(block->rlp);
        
        // Free transactions
        for (size_t j = 0; j < block->tx_count; j++) {
            free(block->transactions[j]);
        }
        free(block->transactions);
        free(block->tx_len);
        
        // Free uncles
        for (size_t j = 0; j < block->uncle_count; j++) {
            free(block->uncles[j]);
        }
        free(block->uncles);
        free(block->uncle_len);
    }
    free(test->blocks);
    
    memset(test, 0, sizeof(*test));
    free(test);
}

//==============================================================================
// Engine Test Cleanup
//==============================================================================

void engine_test_free(engine_test_t *test) {
    if (!test) return;

    free(test->name);
    free(test->network);
    free(test->genesis_header.extra_data);

    /* Free pre-state */
    for (size_t i = 0; i < test->pre_state_count; i++) {
        test_account_free(&test->pre_state[i]);
    }
    free(test->pre_state);

    /* Free post-state */
    for (size_t i = 0; i < test->post_state_count; i++) {
        test_account_free(&test->post_state[i]);
    }
    free(test->post_state);

    /* Free payloads */
    for (size_t i = 0; i < test->payload_count; i++) {
        engine_test_payload_t *p = &test->payloads[i];

        for (size_t j = 0; j < p->tx_count; j++) {
            free(p->transactions[j]);
        }
        free(p->transactions);
        free(p->tx_lengths);
        free(p->withdrawals);
        for (size_t j = 0; j < p->request_count; j++) {
            free(p->requests[j]);
        }
        free(p->requests);
        free(p->request_lengths);
        free(p->validation_error);
    }
    free(test->payloads);

    memset(test, 0, sizeof(*test));
    free(test);
}

//==============================================================================
// State Test Cleanup
//==============================================================================

void state_test_free(state_test_t *test) {
    if (!test) return;
    
    free(test->name);
    
    // Free pre-state
    for (size_t i = 0; i < test->pre_state_count; i++) {
        test_account_free(&test->pre_state[i]);
    }
    free(test->pre_state);
    
    // Free transaction
    free(test->transaction.gas_limit);
    free(test->transaction.value);
    free(test->transaction.blob_versioned_hashes);
    free(test->transaction.authorization_list);

    for (size_t i = 0; i < test->transaction.data_count; i++) {
        free(test->transaction.data[i]);
    }
    free(test->transaction.data);
    free(test->transaction.data_len);
    
    // Free post-conditions
    for (size_t i = 0; i < test->post_count; i++) {
        free(test->post[i].fork_name);
        
        for (size_t j = 0; j < test->post[i].condition_count; j++) {
            free(test->post[i].conditions[j].tx_bytes);
        }
        free(test->post[i].conditions);
    }
    free(test->post);
    
    memset(test, 0, sizeof(*test));
    free(test);
}

//==============================================================================
// Transaction Test Cleanup
//==============================================================================

void transaction_test_free(transaction_test_t *test) {
    if (!test) return;
    
    free(test->name);
    free(test->tx_bytes);
    free(test->description);
    free(test->url);
    
    // Free results
    for (size_t i = 0; i < test->result_count; i++) {
        free(test->results[i].fork_name);
        free(test->results[i].exception);
    }
    free(test->results);
    
    memset(test, 0, sizeof(*test));
    free(test);
}
