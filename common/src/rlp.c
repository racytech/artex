#include "../include/rlp.h"
#include <stdlib.h>
#include <string.h>

// Helper: Encode length
static bytes_t encode_length(size_t len, uint8_t offset) {
    bytes_t result = bytes_new();
    
    if (len < 56) {
        // Short length: single byte
        uint8_t byte = offset + len;
        bytes_append(&result, &byte, 1);
    } else {
        // Long length: length of length, then length
        // Calculate bytes needed for length
        size_t len_bytes = 0;
        size_t temp = len;
        while (temp > 0) {
            len_bytes++;
            temp >>= 8;
        }
        
        // First byte: offset + length_of_length
        // Note: offset is already 0xb7 or 0xf7, don't add 55 again
        uint8_t first = offset + len_bytes;
        bytes_append(&result, &first, 1);
        
        // Then length bytes (big-endian)
        for (int i = len_bytes - 1; i >= 0; i--) {
            uint8_t byte = (len >> (i * 8)) & 0xFF;
            bytes_append(&result, &byte, 1);
        }
    }
    
    return result;
}

// Create RLP string item from bytes
rlp_item_t* rlp_string(const uint8_t* data, size_t len) {
    rlp_item_t* item = (rlp_item_t*)malloc(sizeof(rlp_item_t));
    if (!item) return NULL;
    
    item->type = RLP_TYPE_STRING;
    item->data.string = bytes_from_data(data, len);
    
    return item;
}

// Create RLP string item from bytes_t
rlp_item_t* rlp_string_from_bytes(const bytes_t* bytes) {
    if (!bytes) return NULL;
    return rlp_string(bytes->data, bytes->len);
}

// Create RLP string from uint64
rlp_item_t* rlp_uint64(uint64_t value) {
    if (value == 0) {
        // Zero is encoded as empty string
        return rlp_string(NULL, 0);
    }
    
    // Encode as big-endian, removing leading zeros
    uint8_t buf[8];
    int start = 0;
    for (int i = 0; i < 8; i++) {
        buf[i] = (value >> (56 - i * 8)) & 0xFF;
        if (start == i && buf[i] == 0) {
            start = i + 1;
        }
    }
    
    return rlp_string(buf + start, 8 - start);
}

// Create empty RLP list
rlp_item_t* rlp_list_new(void) {
    rlp_item_t* item = (rlp_item_t*)malloc(sizeof(rlp_item_t));
    if (!item) return NULL;
    
    item->type = RLP_TYPE_LIST;
    item->data.list.items = NULL;
    item->data.list.count = 0;
    
    return item;
}

// Append item to list
bool rlp_list_append(rlp_item_t* list, rlp_item_t* item) {
    if (!list || list->type != RLP_TYPE_LIST || !item) {
        return false;
    }
    
    size_t new_count = list->data.list.count + 1;
    rlp_item_t** new_items = (rlp_item_t**)realloc(
        list->data.list.items,
        new_count * sizeof(rlp_item_t*)
    );
    
    if (!new_items) return false;
    
    new_items[list->data.list.count] = item;
    list->data.list.items = new_items;
    list->data.list.count = new_count;
    
    return true;
}

// Free RLP item
void rlp_item_free(rlp_item_t* item) {
    if (!item) return;
    
    if (item->type == RLP_TYPE_STRING) {
        bytes_free(&item->data.string);
    } else if (item->type == RLP_TYPE_LIST) {
        for (size_t i = 0; i < item->data.list.count; i++) {
            rlp_item_free(item->data.list.items[i]);
        }
        free(item->data.list.items);
    }
    
    free(item);
}

// Encode RLP item to bytes
bytes_t rlp_encode(const rlp_item_t* item) {
    bytes_t result = bytes_new();
    
    if (!item) return result;
    
    if (item->type == RLP_TYPE_STRING) {
        const bytes_t* str = &item->data.string;
        
        if (str->len == 1 && str->data[0] < 0x80) {
            // Single byte [0x00, 0x7f]: encode as itself
            bytes_append(&result, str->data, 1);
        } else if (str->len <= 55) {
            // String 0-55 bytes: 0x80 + length, then data
            uint8_t prefix = 0x80 + str->len;
            bytes_append(&result, &prefix, 1);
            bytes_append(&result, str->data, str->len);
        } else {
            // String >55 bytes: 0xb7 + length_of_length, length, then data
            bytes_t len_encoding = encode_length(str->len, 0xb7);
            bytes_append(&result, len_encoding.data, len_encoding.len);
            bytes_free(&len_encoding);
            bytes_append(&result, str->data, str->len);
        }
    } else if (item->type == RLP_TYPE_LIST) {
        // First, encode all items and calculate total length
        bytes_t* encoded_items = (bytes_t*)malloc(item->data.list.count * sizeof(bytes_t));
        size_t total_len = 0;
        
        for (size_t i = 0; i < item->data.list.count; i++) {
            encoded_items[i] = rlp_encode(item->data.list.items[i]);
            total_len += encoded_items[i].len;
        }
        
        // Encode list prefix
        if (total_len <= 55) {
            // List 0-55 bytes: 0xc0 + length
            uint8_t prefix = 0xc0 + total_len;
            bytes_append(&result, &prefix, 1);
        } else {
            // List >55 bytes: 0xf7 + length_of_length, length
            bytes_t len_encoding = encode_length(total_len, 0xf7);
            bytes_append(&result, len_encoding.data, len_encoding.len);
            bytes_free(&len_encoding);
        }
        
        // Append all encoded items
        for (size_t i = 0; i < item->data.list.count; i++) {
            bytes_append(&result, encoded_items[i].data, encoded_items[i].len);
            bytes_free(&encoded_items[i]);
        }
        
        free(encoded_items);
    }
    
    return result;
}

// Helper: Decode length from bytes
static bool decode_length(const uint8_t* data, size_t data_len, size_t* out_len, size_t* out_header_len) {
    if (data_len == 0) return false;
    
    uint8_t prefix = data[0];
    
    if (prefix <= 0xb7) {
        // Short string: length is in the prefix
        *out_len = prefix - 0x80;
        *out_header_len = 1;
        return true;
    } else if (prefix <= 0xbf) {
        // Long string
        size_t len_of_len = prefix - 0xb7;
        if (data_len < 1 + len_of_len) return false;
        
        *out_len = 0;
        for (size_t i = 0; i < len_of_len; i++) {
            *out_len = (*out_len << 8) | data[1 + i];
        }
        *out_header_len = 1 + len_of_len;
        return true;
    } else if (prefix <= 0xf7) {
        // Short list
        *out_len = prefix - 0xc0;
        *out_header_len = 1;
        return true;
    } else {
        // Long list
        size_t len_of_len = prefix - 0xf7;
        if (data_len < 1 + len_of_len) return false;
        
        *out_len = 0;
        for (size_t i = 0; i < len_of_len; i++) {
            *out_len = (*out_len << 8) | data[1 + i];
        }
        *out_header_len = 1 + len_of_len;
        return true;
    }
}

// Decode RLP bytes to item
rlp_item_t* rlp_decode(const uint8_t* data, size_t len) {
    if (!data || len == 0) return NULL;
    
    uint8_t prefix = data[0];
    
    // Single byte [0x00, 0x7f]
    if (prefix < 0x80) {
        return rlp_string(data, 1);
    }
    
    // String
    if (prefix <= 0xbf) {
        size_t str_len, header_len;
        if (!decode_length(data, len, &str_len, &header_len)) {
            return NULL;
        }
        
        if (header_len + str_len > len) return NULL;
        
        return rlp_string(data + header_len, str_len);
    }
    
    // List (prefix 0xc0..0xff)
    {
        size_t list_len, header_len;
        if (!decode_length(data, len, &list_len, &header_len)) {
            return NULL;
        }
        
        if (header_len + list_len > len) return NULL;
        
        rlp_item_t* list = rlp_list_new();
        if (!list) return NULL;
        
        // Decode all items in the list
        size_t pos = header_len;
        size_t end = header_len + list_len;
        
        while (pos < end) {
            rlp_item_t* item = rlp_decode(data + pos, end - pos);
            if (!item) {
                rlp_item_free(list);
                return NULL;
            }
            
            // Calculate item size to advance position
            bytes_t encoded = rlp_encode(item);
            pos += encoded.len;
            bytes_free(&encoded);
            
            rlp_list_append(list, item);
        }
        
        return list;
    }
    
    return NULL;
}

// Decode RLP bytes_t to item
rlp_item_t* rlp_decode_bytes(const bytes_t* bytes) {
    if (!bytes) return NULL;
    return rlp_decode(bytes->data, bytes->len);
}

// Get item type
rlp_type_t rlp_get_type(const rlp_item_t* item) {
    return item ? item->type : RLP_TYPE_STRING;
}

// Get string data
const bytes_t* rlp_get_string(const rlp_item_t* item) {
    if (!item || item->type != RLP_TYPE_STRING) {
        return NULL;
    }
    return &item->data.string;
}

// Get list count
size_t rlp_get_list_count(const rlp_item_t* item) {
    if (!item || item->type != RLP_TYPE_LIST) {
        return 0;
    }
    return item->data.list.count;
}

// Get list item at index
const rlp_item_t* rlp_get_list_item(const rlp_item_t* item, size_t index) {
    if (!item || item->type != RLP_TYPE_LIST || index >= item->data.list.count) {
        return NULL;
    }
    return item->data.list.items[index];
}

// Helper: Encode empty string
bytes_t rlp_encode_empty_string(void) {
    bytes_t result = bytes_new();
    uint8_t byte = 0x80;
    bytes_append(&result, &byte, 1);
    return result;
}

// Helper: Encode single byte
bytes_t rlp_encode_byte(uint8_t byte) {
    bytes_t result = bytes_new();
    if (byte < 0x80) {
        bytes_append(&result, &byte, 1);
    } else {
        uint8_t prefix = 0x81;
        bytes_append(&result, &prefix, 1);
        bytes_append(&result, &byte, 1);
    }
    return result;
}

// Helper: Encode bytes directly
bytes_t rlp_encode_bytes(const uint8_t* data, size_t len) {
    rlp_item_t* item = rlp_string(data, len);
    bytes_t result = rlp_encode(item);
    rlp_item_free(item);
    return result;
}

// Helper: Encode uint64 directly
bytes_t rlp_encode_uint64_direct(uint64_t value) {
    rlp_item_t* item = rlp_uint64(value);
    bytes_t result = rlp_encode(item);
    rlp_item_free(item);
    return result;
}
