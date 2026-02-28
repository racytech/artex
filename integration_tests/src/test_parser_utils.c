/**
 * JSON Test Parser - Utilities Implementation
 */

#include "test_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

//==============================================================================
// Hex Parsing Utilities
//==============================================================================

static int hex_char_to_int(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

size_t parse_hex_string(const char *hex_str, uint8_t *out, size_t max_len) {
    if (!hex_str || !out) return 0;
    
    // Skip "0x" prefix if present
    if (hex_str[0] == '0' && (hex_str[1] == 'x' || hex_str[1] == 'X')) {
        hex_str += 2;
    }
    
    size_t hex_len = strlen(hex_str);
    if (hex_len == 0) return 0;
    
    // Handle odd-length hex strings (prepend '0')
    size_t byte_len = (hex_len + 1) / 2;
    if (byte_len > max_len) return 0;
    
    size_t out_idx = 0;
    size_t hex_idx = 0;
    
    // If odd length, process first nibble separately
    if (hex_len % 2 == 1) {
        int val = hex_char_to_int(hex_str[0]);
        if (val < 0) return 0;
        out[out_idx++] = (uint8_t)val;
        hex_idx = 1;
    }
    
    // Process pairs of hex digits
    while (hex_idx < hex_len) {
        int high = hex_char_to_int(hex_str[hex_idx]);
        int low = hex_char_to_int(hex_str[hex_idx + 1]);
        
        if (high < 0 || low < 0) return 0;
        
        out[out_idx++] = (uint8_t)((high << 4) | low);
        hex_idx += 2;
    }
    
    return out_idx;
}

uint8_t *parse_hex_alloc(const char *hex_str, size_t *out_len) {
    if (!hex_str) return NULL;
    
    // Skip "0x" prefix
    const char *p = hex_str;
    if (p[0] == '0' && (p[1] == 'x' || p[1] == 'X')) {
        p += 2;
    }
    
    size_t hex_len = strlen(p);
    if (hex_len == 0) {
        if (out_len) *out_len = 0;
        return NULL;
    }
    
    size_t byte_len = (hex_len + 1) / 2;
    uint8_t *buf = malloc(byte_len);
    if (!buf) return NULL;
    
    size_t actual_len = parse_hex_string(hex_str, buf, byte_len);
    if (actual_len == 0) {
        free(buf);
        return NULL;
    }
    
    if (out_len) *out_len = actual_len;
    return buf;
}

bool parse_uint256(const char *hex_str, uint256_t *out) {
    if (!hex_str || !out) return false;
    
    uint8_t buf[32] = {0};
    size_t len = parse_hex_string(hex_str, buf, 32);
    
    // Convert to uint256 (big-endian)
    *out = uint256_from_bytes(buf, len);
    return true;
}

bool parse_uint64(const char *hex_str, uint64_t *out) {
    if (!hex_str || !out) return false;
    
    uint8_t buf[8] = {0};
    size_t len = parse_hex_string(hex_str, buf, 8);
    if (len == 0) {
        *out = 0;
        return true;
    }
    
    // Convert big-endian bytes to uint64
    *out = 0;
    for (size_t i = 0; i < len; i++) {
        *out = (*out << 8) | buf[i];
    }
    
    return true;
}

bool parse_address(const char *hex_str, address_t *out) {
    if (!hex_str || !out) return false;
    
    uint8_t buf[20] = {0};
    size_t len = parse_hex_string(hex_str, buf, 20);
    
    if (len > 0 && len <= 20) {
        // Pad with zeros on the left if needed
        memset(out->bytes, 0, 20);
        memcpy(out->bytes + (20 - len), buf, len);
        return true;
    }
    
    return false;
}

bool parse_hash(const char *hex_str, hash_t *out) {
    if (!hex_str || !out) return false;
    
    uint8_t buf[32] = {0};
    size_t len = parse_hex_string(hex_str, buf, 32);
    
    if (len > 0 && len <= 32) {
        // Pad with zeros on the left if needed
        memset(out->bytes, 0, 32);
        memcpy(out->bytes + (32 - len), buf, len);
        return true;
    }
    
    return false;
}

bool parse_bloom(const char *hex_str, uint8_t out[256]) {
    if (!hex_str || !out) return false;
    
    size_t len = parse_hex_string(hex_str, out, 256);
    
    // Pad with zeros if needed
    if (len < 256) {
        memmove(out + (256 - len), out, len);
        memset(out, 0, 256 - len);
    }
    
    return len > 0;
}

//==============================================================================
// JSON Helper Functions
//==============================================================================

cJSON *load_json_file(const char *filepath) {
    FILE *f = fopen(filepath, "r");
    if (!f) {
        fprintf(stderr, "Failed to open file: %s\n", filepath);
        return NULL;
    }
    
    // Get file size
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    
    if (fsize <= 0) {
        fclose(f);
        return NULL;
    }
    
    // Read file
    char *content = malloc(fsize + 1);
    if (!content) {
        fclose(f);
        return NULL;
    }
    
    size_t read_size = fread(content, 1, fsize, f);
    fclose(f);
    
    content[read_size] = '\0';
    
    // Parse JSON
    cJSON *json = cJSON_Parse(content);
    free(content);
    
    if (!json) {
        const char *error_ptr = cJSON_GetErrorPtr();
        if (error_ptr) {
            fprintf(stderr, "JSON parse error: %s\n", error_ptr);
        }
    }
    
    return json;
}

bool json_get_string(const cJSON *json, const char *key, const char **out) {
    if (!json || !key) return false;
    
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(json, key);
    if (!item || !cJSON_IsString(item)) return false;
    
    if (out) *out = item->valuestring;
    return true;
}

const cJSON *json_get_object(const cJSON *json, const char *key) {
    if (!json || !key) return NULL;
    
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(json, key);
    if (!item || !cJSON_IsObject(item)) return NULL;
    
    return item;
}

const cJSON *json_get_array(const cJSON *json, const char *key) {
    if (!json || !key) return NULL;
    
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(json, key);
    if (!item || !cJSON_IsArray(item)) return NULL;
    
    return item;
}

//==============================================================================
// Storage Parsing
//==============================================================================

bool parse_storage(const cJSON *json, 
                   test_storage_entry_t **out_storage,
                   size_t *out_count) {
    if (!json || !out_storage || !out_count) return false;
    
    if (!cJSON_IsObject(json)) return false;
    
    int count = cJSON_GetArraySize(json);
    if (count == 0) {
        *out_storage = NULL;
        *out_count = 0;
        return true;
    }
    
    test_storage_entry_t *storage = 
        calloc(count, sizeof(*storage));
    if (!storage) return false;
    
    int idx = 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, json) {
        if (!item->string || !cJSON_IsString(item)) {
            free(storage);
            return false;
        }
        
        if (!parse_uint256(item->string, &storage[idx].key) ||
            !parse_uint256(item->valuestring, &storage[idx].value)) {
            free(storage);
            return false;
        }
        idx++;
    }
    
    *out_storage = storage;
    *out_count = count;
    return true;
}

//==============================================================================
// Account Parsing
//==============================================================================

bool parse_account(const cJSON *json, const address_t *addr, test_account_t *out) {
    if (!json || !addr || !out) return false;
    
    memset(out, 0, sizeof(*out));
    out->address = *addr;
    
    // Parse nonce
    const char *nonce_str;
    if (json_get_string(json, "nonce", &nonce_str)) {
        if (!parse_uint256(nonce_str, &out->nonce)) return false;
    }
    
    // Parse balance
    const char *balance_str;
    if (json_get_string(json, "balance", &balance_str)) {
        if (!parse_uint256(balance_str, &out->balance)) return false;
    }
    
    // Parse code
    const char *code_str;
    if (json_get_string(json, "code", &code_str)) {
        out->code = parse_hex_alloc(code_str, &out->code_len);
        if (!out->code && strlen(code_str) > 2) return false; // "0x" is valid empty code
    }
    
    // Parse storage
    const cJSON *storage_obj = json_get_object(json, "storage");
    if (storage_obj) {
        if (!parse_storage(storage_obj, &out->storage, &out->storage_count)) {
            free(out->code);
            return false;
        }
    }
    
    return true;
}

bool parse_account_map(const cJSON *json, test_account_t **out_accounts, size_t *out_count) {
    if (!json || !out_accounts || !out_count) return false;
    
    if (!cJSON_IsObject(json)) return false;
    
    int count = cJSON_GetArraySize(json);
    if (count == 0) {
        *out_accounts = NULL;
        *out_count = 0;
        return true;
    }
    
    test_account_t *accounts = calloc(count, sizeof(*accounts));
    if (!accounts) return false;
    
    int idx = 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, json) {
        if (!item->string) {
            // Cleanup on error
            for (int i = 0; i < idx; i++) {
                test_account_free(&accounts[i]);
            }
            free(accounts);
            return false;
        }
        
        address_t addr;
        if (!parse_address(item->string, &addr)) {
            for (int i = 0; i < idx; i++) {
                test_account_free(&accounts[i]);
            }
            free(accounts);
            return false;
        }
        
        if (!parse_account(item, &addr, &accounts[idx])) {
            for (int i = 0; i < idx; i++) {
                test_account_free(&accounts[i]);
            }
            free(accounts);
            return false;
        }
        idx++;
    }
    
    *out_accounts = accounts;
    *out_count = count;
    return true;
}
