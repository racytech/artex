/**
 * JSON Test Parser - Block and Transaction Parsing
 */

#include "test_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//==============================================================================
// Block Header Parsing
//==============================================================================

bool parse_block_header(const cJSON *json, test_block_header_t *out) {
    if (!json || !out) return false;
    
    memset(out, 0, sizeof(*out));
    
    const char *str;
    
    // Required fields
    if (json_get_string(json, "parentHash", &str)) {
        if (!parse_hash(str, &out->parent_hash)) return false;
    }
    
    if (json_get_string(json, "uncleHash", &str)) {
        if (!parse_hash(str, &out->uncle_hash)) return false;
    }
    
    if (json_get_string(json, "coinbase", &str)) {
        if (!parse_address(str, &out->coinbase)) return false;
    }
    
    if (json_get_string(json, "stateRoot", &str)) {
        if (!parse_hash(str, &out->state_root)) return false;
    }
    
    if (json_get_string(json, "transactionsTrie", &str)) {
        if (!parse_hash(str, &out->transactions_trie)) return false;
    }
    
    if (json_get_string(json, "receiptTrie", &str)) {
        if (!parse_hash(str, &out->receipt_trie)) return false;
    }
    
    if (json_get_string(json, "bloom", &str)) {
        if (!parse_bloom(str, out->bloom)) return false;
    }
    
    if (json_get_string(json, "difficulty", &str)) {
        if (!parse_uint256(str, &out->difficulty)) return false;
    }
    
    if (json_get_string(json, "number", &str)) {
        if (!parse_uint256(str, &out->number)) return false;
    }
    
    if (json_get_string(json, "gasLimit", &str)) {
        if (!parse_uint256(str, &out->gas_limit)) return false;
    }
    
    if (json_get_string(json, "gasUsed", &str)) {
        if (!parse_uint256(str, &out->gas_used)) return false;
    }
    
    if (json_get_string(json, "timestamp", &str)) {
        if (!parse_uint256(str, &out->timestamp)) return false;
    }
    
    if (json_get_string(json, "extraData", &str)) {
        out->extra_data = parse_hex_alloc(str, &out->extra_data_len);
    }
    
    if (json_get_string(json, "mixHash", &str)) {
        if (!parse_hash(str, &out->mix_hash)) return false;
    }
    
    if (json_get_string(json, "nonce", &str)) {
        if (!parse_uint64(str, &out->nonce)) return false;
    }
    
    if (json_get_string(json, "hash", &str)) {
        if (!parse_hash(str, &out->hash)) return false;
    }
    
    // Optional fields (post-EIP)
    if (json_get_string(json, "baseFeePerGas", &str)) {
        parse_uint256(str, &out->base_fee);
    }
    
    if (json_get_string(json, "withdrawalsRoot", &str)) {
        parse_hash(str, &out->withdrawals_root);
    }
    
    if (json_get_string(json, "blobGasUsed", &str)) {
        parse_uint256(str, &out->blob_gas_used);
    }
    
    if (json_get_string(json, "excessBlobGas", &str)) {
        parse_uint256(str, &out->excess_blob_gas);
    }
    
    if (json_get_string(json, "parentBeaconBlockRoot", &str)) {
        parse_hash(str, &out->parent_beacon_block_root);
    }
    
    return true;
}

//==============================================================================
// Transaction Parsing
//==============================================================================

bool parse_transaction(const cJSON *json, test_transaction_t *out) {
    if (!json || !out) return false;
    
    memset(out, 0, sizeof(*out));
    
    const char *str;
    
    // Nonce
    if (json_get_string(json, "nonce", &str)) {
        if (!parse_uint256(str, &out->nonce)) return false;
    }
    
    // Gas price (legacy transactions)
    if (json_get_string(json, "gasPrice", &str)) {
        if (!parse_uint256(str, &out->gas_price)) return false;
    }
    
    // EIP-1559 gas fields
    if (json_get_string(json, "maxFeePerGas", &str)) {
        if (!parse_uint256(str, &out->max_fee_per_gas)) return false;
    }
    
    if (json_get_string(json, "maxPriorityFeePerGas", &str)) {
        if (!parse_uint256(str, &out->max_priority_fee_per_gas)) return false;
    }
    
    // Gas limit (can be array for multiple test cases)
    const cJSON *gas_limit = cJSON_GetObjectItemCaseSensitive(json, "gasLimit");
    if (gas_limit) {
        if (cJSON_IsArray(gas_limit)) {
            int count = cJSON_GetArraySize(gas_limit);
            out->gas_limit = calloc(count, sizeof(uint256_t));
            if (!out->gas_limit) return false;
            
            out->gas_limit_count = count;
            const cJSON *item;
            int idx = 0;
            cJSON_ArrayForEach(item, gas_limit) {
                if (cJSON_IsString(item)) {
                    parse_uint256(item->valuestring, &out->gas_limit[idx++]);
                }
            }
        } else if (cJSON_IsString(gas_limit)) {
            out->gas_limit = calloc(1, sizeof(uint256_t));
            if (!out->gas_limit) return false;
            out->gas_limit_count = 1;
            parse_uint256(gas_limit->valuestring, &out->gas_limit[0]);
        }
    }
    
    // To address
    if (json_get_string(json, "to", &str)) {
        if (strlen(str) == 0 || strlen(str) == 2 || strcmp(str, "0x") == 0) {
            // Empty address = contract creation
            out->is_create = true;
            memset(&out->to, 0, sizeof(out->to));
        } else {
            out->is_create = false;
            if (!parse_address(str, &out->to)) return false;
        }
    }
    
    // Value (can be array for multiple test cases)
    const cJSON *value = cJSON_GetObjectItemCaseSensitive(json, "value");
    if (value) {
        if (cJSON_IsArray(value)) {
            int count = cJSON_GetArraySize(value);
            out->value = calloc(count, sizeof(uint256_t));
            if (!out->value) return false;
            
            out->value_count = count;
            const cJSON *item;
            int idx = 0;
            cJSON_ArrayForEach(item, value) {
                if (cJSON_IsString(item)) {
                    parse_uint256(item->valuestring, &out->value[idx++]);
                }
            }
        } else if (cJSON_IsString(value)) {
            out->value = calloc(1, sizeof(uint256_t));
            if (!out->value) return false;
            out->value_count = 1;
            parse_uint256(value->valuestring, &out->value[0]);
        }
    }
    
    // Data (can be array for multiple test cases)
    const cJSON *data = cJSON_GetObjectItemCaseSensitive(json, "data");
    if (data) {
        if (cJSON_IsArray(data)) {
            int count = cJSON_GetArraySize(data);
            out->data = calloc(count, sizeof(uint8_t*));
            out->data_len = calloc(count, sizeof(size_t));
            if (!out->data || !out->data_len) {
                free(out->data);
                free(out->data_len);
                return false;
            }
            
            out->data_count = count;
            const cJSON *item;
            int idx = 0;
            cJSON_ArrayForEach(item, data) {
                if (cJSON_IsString(item)) {
                    out->data[idx] = parse_hex_alloc(item->valuestring, &out->data_len[idx]);
                    idx++;
                }
            }
        } else if (cJSON_IsString(data)) {
            out->data = calloc(1, sizeof(uint8_t*));
            out->data_len = calloc(1, sizeof(size_t));
            if (!out->data || !out->data_len) {
                free(out->data);
                free(out->data_len);
                return false;
            }
            out->data_count = 1;
            out->data[0] = parse_hex_alloc(data->valuestring, &out->data_len[0]);
        }
    }
    
    // Sender
    if (json_get_string(json, "sender", &str)) {
        if (!parse_address(str, &out->sender)) return false;
    }
    
    // Secret key
    if (json_get_string(json, "secretKey", &str)) {
        parse_hex_string(str, out->secret_key, 32);
    }
    
    // Signature (v, r, s) - for pre-signed transactions
    if (json_get_string(json, "v", &str)) {
        parse_uint256(str, &out->v);
    }
    if (json_get_string(json, "r", &str)) {
        parse_uint256(str, &out->r);
    }
    if (json_get_string(json, "s", &str)) {
        parse_uint256(str, &out->s);
    }
    
    // Access lists (EIP-2930) - can be array for multiple test cases
    const cJSON *access_lists = cJSON_GetObjectItemCaseSensitive(json, "accessLists");
    if (access_lists && cJSON_IsArray(access_lists)) {
        int count = cJSON_GetArraySize(access_lists);
        out->access_lists = calloc(count, sizeof(test_access_list_t));
        if (!out->access_lists) return false;
        
        out->access_lists_count = count;
        const cJSON *list_item;
        int idx = 0;
        cJSON_ArrayForEach(list_item, access_lists) {
            if (cJSON_IsArray(list_item)) {
                // Parse each access list entry
                int entry_count = cJSON_GetArraySize(list_item);
                out->access_lists[idx].entries = calloc(entry_count, sizeof(test_access_list_entry_t));
                if (!out->access_lists[idx].entries) return false;
                
                out->access_lists[idx].entries_count = entry_count;
                const cJSON *entry;
                int entry_idx = 0;
                cJSON_ArrayForEach(entry, list_item) {
                    // Parse address
                    const char *addr_str;
                    if (json_get_string(entry, "address", &addr_str)) {
                        parse_address(addr_str, &out->access_lists[idx].entries[entry_idx].address);
                    }
                    
                    // Parse storage keys
                    const cJSON *storage_keys = cJSON_GetObjectItemCaseSensitive(entry, "storageKeys");
                    if (storage_keys && cJSON_IsArray(storage_keys)) {
                        int key_count = cJSON_GetArraySize(storage_keys);
                        out->access_lists[idx].entries[entry_idx].storage_keys = calloc(key_count, sizeof(uint256_t));
                        if (!out->access_lists[idx].entries[entry_idx].storage_keys) return false;
                        
                        out->access_lists[idx].entries[entry_idx].storage_keys_count = key_count;
                        const cJSON *key_item;
                        int key_idx = 0;
                        cJSON_ArrayForEach(key_item, storage_keys) {
                            if (cJSON_IsString(key_item)) {
                                parse_uint256(key_item->valuestring, &out->access_lists[idx].entries[entry_idx].storage_keys[key_idx++]);
                            }
                        }
                    }
                    
                    entry_idx++;
                }
            }
            idx++;
        }
    }
    
    // EIP-4844 blob fields
    if (json_get_string(json, "maxFeePerBlobGas", &str)) {
        parse_uint256(str, &out->max_fee_per_blob_gas);
    }

    const cJSON *blob_hashes = cJSON_GetObjectItemCaseSensitive(json, "blobVersionedHashes");
    if (blob_hashes && cJSON_IsArray(blob_hashes)) {
        int count = cJSON_GetArraySize(blob_hashes);
        if (count > 0) {
            out->blob_versioned_hashes = calloc(count, sizeof(hash_t));
            if (!out->blob_versioned_hashes) return false;
            out->blob_versioned_hashes_count = count;
            int idx = 0;
            const cJSON *item;
            cJSON_ArrayForEach(item, blob_hashes) {
                if (cJSON_IsString(item)) {
                    parse_hash(item->valuestring, &out->blob_versioned_hashes[idx++]);
                }
            }
        }
    }

    // EIP-7702 authorization list
    const cJSON *auth_list = cJSON_GetObjectItemCaseSensitive(json, "authorizationList");
    if (auth_list && cJSON_IsArray(auth_list)) {
        out->has_authorization_list = true;
        int count = cJSON_GetArraySize(auth_list);
        if (count > 0) {
            out->authorization_list = calloc(count, sizeof(test_authorization_t));
            if (!out->authorization_list) return false;
            out->authorization_list_count = count;
            int idx = 0;
            const cJSON *item;
            cJSON_ArrayForEach(item, auth_list) {
                if (cJSON_IsObject(item)) {
                    test_authorization_t *auth = &out->authorization_list[idx];
                    if (json_get_string(item, "chainId", &str))
                        parse_uint256(str, &auth->chain_id);
                    if (json_get_string(item, "address", &str))
                        parse_address(str, &auth->address);
                    if (json_get_string(item, "nonce", &str)) {
                        uint256_t nonce_u256;
                        parse_uint256(str, &nonce_u256);
                        auth->nonce = uint256_to_uint64(&nonce_u256);
                    }
                    if (json_get_string(item, "yParity", &str)) {
                        uint256_t yp;
                        parse_uint256(str, &yp);
                        auth->y_parity = (uint8_t)uint256_to_uint64(&yp);
                    } else if (json_get_string(item, "v", &str)) {
                        uint256_t v;
                        parse_uint256(str, &v);
                        auth->y_parity = (uint8_t)uint256_to_uint64(&v);
                    }
                    if (json_get_string(item, "r", &str))
                        parse_uint256(str, &auth->r);
                    if (json_get_string(item, "s", &str))
                        parse_uint256(str, &auth->s);
                    if (json_get_string(item, "signer", &str))
                        parse_address(str, &auth->signer);
                    idx++;
                }
            }
        }
    }

    return true;
}

//==============================================================================
// Environment Parsing
//==============================================================================

bool parse_environment(const cJSON *json, test_env_t *out) {
    if (!json || !out) return false;
    
    memset(out, 0, sizeof(*out));
    
    const char *str;
    
    if (json_get_string(json, "currentCoinbase", &str)) {
        parse_address(str, &out->coinbase);
    }
    
    if (json_get_string(json, "currentGasLimit", &str)) {
        parse_uint256(str, &out->gas_limit);
    }
    
    if (json_get_string(json, "currentNumber", &str)) {
        parse_uint256(str, &out->number);
    }
    
    if (json_get_string(json, "currentTimestamp", &str)) {
        parse_uint256(str, &out->timestamp);
    }
    
    if (json_get_string(json, "currentDifficulty", &str)) {
        parse_uint256(str, &out->difficulty);
    }
    
    if (json_get_string(json, "currentBaseFee", &str)) {
        parse_uint256(str, &out->base_fee);
    }

    if (json_get_string(json, "currentRandom", &str)) {
        parse_hash(str, &out->prev_randao);
    }

    if (json_get_string(json, "currentExcessBlobGas", &str)) {
        parse_uint256(str, &out->excess_blob_gas);
    }

    if (json_get_string(json, "previousHash", &str)) {
        parse_hash(str, &out->previous_hash);
        out->has_previous_hash = true;
    }

    return true;
}
