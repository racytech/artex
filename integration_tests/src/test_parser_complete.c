/**
 * JSON Test Parser - Complete Test Parsing
 */

#include "test_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Blockchain Test Parsing
//==============================================================================

bool parse_blockchain_test(const char *filepath, blockchain_test_t **out) {
    if (!filepath || !out) return false;
    
    cJSON *root = load_json_file(filepath);
    if (!root) return false;
    
    // Blockchain tests have one test case per file, stored as single object key
    const cJSON *test_case = root->child;
    if (!test_case) {
        cJSON_Delete(root);
        return false;
    }
    
    blockchain_test_t *test = calloc(1, sizeof(blockchain_test_t));
    if (!test) {
        cJSON_Delete(root);
        return false;
    }
    
    // Test name
    if (test_case->string) {
        test->name = strdup(test_case->string);
    }
    
    // Network/fork
    const char *network;
    if (json_get_string(test_case, "network", &network)) {
        test->network = strdup(network);
    }
    
    // Genesis block header
    const cJSON *genesis_header = json_get_object(test_case, "genesisBlockHeader");
    if (genesis_header) {
        parse_block_header(genesis_header, &test->genesis_header);
    }
    
    // Genesis RLP
    const char *genesis_rlp_str;
    if (json_get_string(test_case, "genesisRLP", &genesis_rlp_str)) {
        test->genesis_rlp = parse_hex_alloc(genesis_rlp_str, &test->genesis_rlp_len);
    }
    
    // Pre-state
    const cJSON *pre = json_get_object(test_case, "pre");
    if (pre) {
        parse_account_map(pre, &test->pre_state, &test->pre_state_count);
    }
    
    // Post-state
    const cJSON *post = json_get_object(test_case, "postState");
    if (post) {
        parse_account_map(post, &test->post_state, &test->post_state_count);
    }
    
    // Blocks
    const cJSON *blocks = json_get_array(test_case, "blocks");
    if (blocks) {
        int block_count = cJSON_GetArraySize(blocks);
        if (block_count > 0) {
            test->blocks = calloc(block_count, sizeof(test_block_t));
            test->block_count = block_count;
            
            const cJSON *block_item;
            int idx = 0;
            cJSON_ArrayForEach(block_item, blocks) {
                test_block_t *block = &test->blocks[idx++];
                
                // Block header
                const cJSON *block_header = json_get_object(block_item, "blockHeader");
                if (block_header) {
                    parse_block_header(block_header, &block->header);
                }
                
                // Block RLP
                const char *rlp_str;
                if (json_get_string(block_item, "rlp", &rlp_str)) {
                    block->rlp = parse_hex_alloc(rlp_str, &block->rlp_len);
                }
                
                // Transactions (array of RLP)
                const cJSON *txs = json_get_array(block_item, "transactions");
                if (txs) {
                    int tx_count = cJSON_GetArraySize(txs);
                    if (tx_count > 0) {
                        block->transactions = calloc(tx_count, sizeof(uint8_t*));
                        block->tx_len = calloc(tx_count, sizeof(size_t));
                        block->tx_count = tx_count;
                        
                        const cJSON *tx_item;
                        int tx_idx = 0;
                        cJSON_ArrayForEach(tx_item, txs) {
                            if (cJSON_IsString(tx_item)) {
                                block->transactions[tx_idx] = 
                                    parse_hex_alloc(tx_item->valuestring, &block->tx_len[tx_idx]);
                                tx_idx++;
                            }
                        }
                    }
                }
                
                // Uncles (array of RLP)
                const cJSON *uncles = json_get_array(block_item, "uncleHeaders");
                if (uncles) {
                    int uncle_count = cJSON_GetArraySize(uncles);
                    if (uncle_count > 0) {
                        block->uncles = calloc(uncle_count, sizeof(uint8_t*));
                        block->uncle_len = calloc(uncle_count, sizeof(size_t));
                        block->uncle_count = uncle_count;
                        
                        const cJSON *uncle_item;
                        int uncle_idx = 0;
                        cJSON_ArrayForEach(uncle_item, uncles) {
                            if (cJSON_IsString(uncle_item)) {
                                block->uncles[uncle_idx] = 
                                    parse_hex_alloc(uncle_item->valuestring, &block->uncle_len[uncle_idx]);
                                uncle_idx++;
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Last block hash
    const char *last_hash;
    if (json_get_string(test_case, "lastblockhash", &last_hash)) {
        parse_hash(last_hash, &test->last_block_hash);
    }
    
    // Chain config
    const cJSON *config = json_get_object(test_case, "config");
    if (config) {
        const char *chain_id;
        if (json_get_string(config, "chainid", &chain_id)) {
            parse_uint256(chain_id, &test->chain_id);
        }
    }
    
    cJSON_Delete(root);
    *out = test;
    return true;
}

//==============================================================================
// State Test Parsing
//==============================================================================

bool parse_state_test(const char *filepath, state_test_t **out) {
    if (!filepath || !out) return false;
    
    cJSON *root = load_json_file(filepath);
    if (!root) return false;
    
    // State tests have one test case per file
    const cJSON *test_case = root->child;
    if (!test_case) {
        cJSON_Delete(root);
        return false;
    }
    
    state_test_t *test = calloc(1, sizeof(state_test_t));
    if (!test) {
        cJSON_Delete(root);
        return false;
    }
    
    // Test name
    if (test_case->string) {
        test->name = strdup(test_case->string);
    }
    
    // Environment
    const cJSON *env = json_get_object(test_case, "env");
    if (env) {
        parse_environment(env, &test->env);
    }
    
    // Pre-state
    const cJSON *pre = json_get_object(test_case, "pre");
    if (pre) {
        parse_account_map(pre, &test->pre_state, &test->pre_state_count);
    }
    
    // Transaction
    const cJSON *tx = json_get_object(test_case, "transaction");
    if (tx) {
        parse_transaction(tx, &test->transaction);
    }
    
    // Post-conditions (by fork)
    const cJSON *post = json_get_object(test_case, "post");
    if (post) {
        int fork_count = cJSON_GetArraySize(post);
        if (fork_count > 0) {
            test->post = calloc(fork_count, sizeof(*test->post));
            test->post_count = fork_count;
            
            const cJSON *fork_item;
            int fork_idx = 0;
            cJSON_ArrayForEach(fork_item, post) {
                if (!fork_item->string) continue;
                
                test->post[fork_idx].fork_name = strdup(fork_item->string);
                
                // Each fork can have multiple post-conditions
                if (cJSON_IsArray(fork_item)) {
                    int cond_count = cJSON_GetArraySize(fork_item);
                    test->post[fork_idx].conditions = calloc(cond_count, sizeof(test_post_condition_t));
                    test->post[fork_idx].condition_count = cond_count;
                    
                    const cJSON *cond_item;
                    int cond_idx = 0;
                    cJSON_ArrayForEach(cond_item, fork_item) {
                        test_post_condition_t *cond = &test->post[fork_idx].conditions[cond_idx++];
                        
                        const char *hash_str;
                        if (json_get_string(cond_item, "hash", &hash_str)) {
                            parse_hash(hash_str, &cond->state_root);
                        }
                        
                        if (json_get_string(cond_item, "logs", &hash_str)) {
                            parse_hash(hash_str, &cond->logs_hash);
                        }
                        
                        if (json_get_string(cond_item, "txbytes", &hash_str)) {
                            cond->tx_bytes = parse_hex_alloc(hash_str, &cond->tx_bytes_len);
                        }
                        
                        // Exception expectation
                        const char *exception_str;
                        if (json_get_string(cond_item, "expectException", &exception_str)) {
                            cond->expect_exception = strdup(exception_str);
                        } else {
                            cond->expect_exception = NULL;
                        }
                        
                        // Indexes
                        const cJSON *indexes = json_get_object(cond_item, "indexes");
                        if (indexes) {
                            const cJSON *idx;
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "data")) && cJSON_IsNumber(idx)) {
                                cond->data_index = idx->valueint;
                            }
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "gas")) && cJSON_IsNumber(idx)) {
                                cond->gas_index = idx->valueint;
                            }
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "value")) && cJSON_IsNumber(idx)) {
                                cond->value_index = idx->valueint;
                            }
                        }
                    }
                }
                fork_idx++;
            }
        }
    }
    
    cJSON_Delete(root);
    *out = test;
    return true;
}

/**
 * Parse a single state test from a JSON object (not a file)
 * Used when a file contains multiple test objects
 */
bool parse_state_test_from_json(const cJSON *test_obj, const char *test_name, state_test_t **out) {
    if (!test_obj || !test_name || !out) return false;
    
    state_test_t *test = calloc(1, sizeof(state_test_t));
    if (!test) return false;
    
    // Set test name
    test->name = strdup(test_name);
    
    // Environment
    const cJSON *env = json_get_object(test_obj, "env");
    if (env) {
        parse_environment(env, &test->env);
    }
    
    // Pre-state
    const cJSON *pre = json_get_object(test_obj, "pre");
    if (pre) {
        parse_account_map(pre, &test->pre_state, &test->pre_state_count);
    }
    
    // Transaction
    const cJSON *tx = json_get_object(test_obj, "transaction");
    if (tx) {
        parse_transaction(tx, &test->transaction);
    }
    
    // Post-conditions (by fork)
    const cJSON *post = json_get_object(test_obj, "post");
    if (post) {
        int fork_count = cJSON_GetArraySize(post);
        if (fork_count > 0) {
            test->post = calloc(fork_count, sizeof(*test->post));
            test->post_count = fork_count;
            
            const cJSON *fork_item;
            int fork_idx = 0;
            cJSON_ArrayForEach(fork_item, post) {
                if (!fork_item->string) continue;
                
                test->post[fork_idx].fork_name = strdup(fork_item->string);
                
                // Each fork can have multiple post-conditions
                if (cJSON_IsArray(fork_item)) {
                    int cond_count = cJSON_GetArraySize(fork_item);
                    test->post[fork_idx].conditions = calloc(cond_count, sizeof(test_post_condition_t));
                    test->post[fork_idx].condition_count = cond_count;
                    
                    const cJSON *cond_item;
                    int cond_idx = 0;
                    cJSON_ArrayForEach(cond_item, fork_item) {
                        test_post_condition_t *cond = &test->post[fork_idx].conditions[cond_idx++];
                        
                        const char *hash_str;
                        if (json_get_string(cond_item, "hash", &hash_str)) {
                            parse_hash(hash_str, &cond->state_root);
                        }
                        
                        if (json_get_string(cond_item, "logs", &hash_str)) {
                            parse_hash(hash_str, &cond->logs_hash);
                        }
                        
                        if (json_get_string(cond_item, "txbytes", &hash_str)) {
                            cond->tx_bytes = parse_hex_alloc(hash_str, &cond->tx_bytes_len);
                        }
                        
                        // Exception expectation
                        const char *exception_str;
                        if (json_get_string(cond_item, "expectException", &exception_str)) {
                            cond->expect_exception = strdup(exception_str);
                        } else {
                            cond->expect_exception = NULL;
                        }
                        
                        // Indexes
                        const cJSON *indexes = json_get_object(cond_item, "indexes");
                        if (indexes) {
                            const cJSON *idx;
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "data")) && cJSON_IsNumber(idx)) {
                                cond->data_index = idx->valueint;
                            }
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "gas")) && cJSON_IsNumber(idx)) {
                                cond->gas_index = idx->valueint;
                            }
                            if ((idx = cJSON_GetObjectItemCaseSensitive(indexes, "value")) && cJSON_IsNumber(idx)) {
                                cond->value_index = idx->valueint;
                            }
                        }
                    }
                }
                fork_idx++;
            }
        }
    }
    
    *out = test;
    return true;
}

//==============================================================================
// Transaction Test Parsing
//==============================================================================

bool parse_transaction_test(const char *filepath, transaction_test_t **out) {
    if (!filepath || !out) return false;
    
    cJSON *root = load_json_file(filepath);
    if (!root) return false;
    
    // Transaction tests have one test case per file
    const cJSON *test_case = root->child;
    if (!test_case) {
        cJSON_Delete(root);
        return false;
    }
    
    transaction_test_t *test = calloc(1, sizeof(transaction_test_t));
    if (!test) {
        cJSON_Delete(root);
        return false;
    }
    
    // Test name
    if (test_case->string) {
        test->name = strdup(test_case->string);
    }
    
    // Transaction bytes
    const char *txbytes;
    if (json_get_string(test_case, "txbytes", &txbytes)) {
        test->tx_bytes = parse_hex_alloc(txbytes, &test->tx_bytes_len);
    }
    
    // Results by fork
    const cJSON *result = json_get_object(test_case, "result");
    if (result) {
        int fork_count = cJSON_GetArraySize(result);
        if (fork_count > 0) {
            test->results = calloc(fork_count, sizeof(tx_test_result_t));
            test->result_count = fork_count;
            
            const cJSON *fork_item;
            int idx = 0;
            cJSON_ArrayForEach(fork_item, result) {
                if (!fork_item->string) continue;
                
                tx_test_result_t *res = &test->results[idx++];
                res->fork_name = strdup(fork_item->string);
                
                // Intrinsic gas
                const char *gas_str;
                if (json_get_string(fork_item, "intrinsicGas", &gas_str)) {
                    parse_uint256(gas_str, &res->intrinsic_gas);
                }
                
                // Exception (if invalid)
                const char *exception;
                if (json_get_string(fork_item, "exception", &exception)) {
                    res->exception = strdup(exception);
                }
            }
        }
    }
    
    // Metadata
    const cJSON *info = json_get_object(test_case, "_info");
    if (info) {
        const char *str;
        if (json_get_string(info, "description", &str)) {
            test->description = strdup(str);
        }
        if (json_get_string(info, "url", &str)) {
            test->url = strdup(str);
        }
    }
    
    cJSON_Delete(root);
    *out = test;
    return true;
}
