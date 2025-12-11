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
    
    // Gas price
    if (json_get_string(json, "gasPrice", &str)) {
        if (!parse_uint256(str, &out->gas_price)) return false;
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
    
    return true;
}
