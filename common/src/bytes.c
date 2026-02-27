#include "bytes.h"
#include <string.h>
#include <stdlib.h>

bytes_t bytes_new(void) {
    bytes_t b;
    b.data = NULL;
    b.len = 0;
    b.capacity = 0;
    return b;
}

bytes_t bytes_with_capacity(size_t capacity) {
    bytes_t b;
    b.data = capacity > 0 ? (uint8_t*)malloc(capacity) : NULL;
    b.len = 0;
    b.capacity = b.data ? capacity : 0;
    return b;
}

bytes_t bytes_from_data(const uint8_t* data, size_t len) {
    bytes_t b = bytes_with_capacity(len);
    if (b.data && data && len > 0) {
        memcpy(b.data, data, len);
        b.len = len;
    }
    return b;
}

static int hex_char_to_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static char value_to_hex_char(int val) {
    if (val < 10) return '0' + val;
    return 'a' + (val - 10);
}

bool bytes_from_hex(const char* hex, bytes_t* out) {
    if (!hex || !out) return false;
    
    // Initialize output to empty
    *out = bytes_new();
    
    // Skip 0x prefix if present
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
        hex += 2;
    }
    
    size_t hex_len = strlen(hex);
    if (hex_len % 2 != 0) return false; // Must be even
    
    size_t byte_len = hex_len / 2;
    if (byte_len > 0) {
        *out = bytes_with_capacity(byte_len);
        if (!out->data) return false;
    }
    
    for (size_t i = 0; i < byte_len; i++) {
        int high = hex_char_to_value(hex[i * 2]);
        int low = hex_char_to_value(hex[i * 2 + 1]);
        
        if (high < 0 || low < 0) {
            bytes_free(out);
            *out = bytes_new();
            return false;
        }
        
        out->data[i] = (high << 4) | low;
    }
    
    out->len = byte_len;
    return true;
}

void bytes_free(bytes_t* b) {
    if (b && b->data) {
        free(b->data);
        b->data = NULL;
        b->len = 0;
        b->capacity = 0;
    }
}

char* bytes_to_hex(const bytes_t* b) {
    if (!b || b->len == 0) {
        char* result = (char*)malloc(3); // "0x" + null
        if (result) {
            strcpy(result, "0x");
        }
        return result;
    }
    
    size_t hex_len = 2 + b->len * 2 + 1; // "0x" + hex chars + null
    char* hex = (char*)malloc(hex_len);
    if (!hex) return NULL;
    
    hex[0] = '0';
    hex[1] = 'x';
    
    for (size_t i = 0; i < b->len; i++) {
        hex[2 + i * 2] = value_to_hex_char(b->data[i] >> 4);
        hex[2 + i * 2 + 1] = value_to_hex_char(b->data[i] & 0x0F);
    }
    
    hex[hex_len - 1] = '\0';
    return hex;
}

bool bytes_reserve(bytes_t* b, size_t new_capacity) {
    if (!b) return false;
    if (new_capacity <= b->capacity) return true;
    
    // realloc(NULL, size) is equivalent to malloc(size)
    uint8_t* new_data = (uint8_t*)realloc(b->data, new_capacity);
    if (!new_data) return false;
    
    b->data = new_data;
    b->capacity = new_capacity;
    return true;
}

bool bytes_resize(bytes_t* b, size_t new_len) {
    if (!b) return false;
    
    if (new_len > b->capacity) {
        if (!bytes_reserve(b, new_len)) return false;
    }
    
    // Zero-initialize new bytes
    if (new_len > b->len) {
        memset(b->data + b->len, 0, new_len - b->len);
    }
    
    b->len = new_len;
    return true;
}

bool bytes_append(bytes_t* b, const uint8_t* data, size_t len) {
    if (!b || !data) return true; // Allow appending 0 bytes
    if (len == 0) return true;
    
    size_t new_len = b->len + len;
    if (new_len > b->capacity) {
        // Grow capacity by 1.5x or to new_len, whichever is larger
        size_t new_capacity = b->capacity == 0 ? 16 : b->capacity;
        while (new_capacity < new_len) {
            new_capacity = new_capacity + new_capacity / 2;
        }
        if (!bytes_reserve(b, new_capacity)) return false;
    }
    
    memcpy(b->data + b->len, data, len);
    b->len = new_len;
    return true;
}

bool bytes_push(bytes_t* b, uint8_t byte) {
    return bytes_append(b, &byte, 1);
}

void bytes_clear(bytes_t* b) {
    if (b) {
        b->len = 0;
    }
}

bytes_t bytes_clone(const bytes_t* b) {
    if (!b || !b->data) {
        return bytes_new();
    }
    return bytes_from_data(b->data, b->len);
}

bool bytes_equal(const bytes_t* a, const bytes_t* b) {
    if (!a || !b) return false;
    if (a->len != b->len) return false;
    if (a->len == 0) return true;
    return memcmp(a->data, b->data, a->len) == 0;
}

bool bytes_is_empty(const bytes_t* b) {
    return !b || b->len == 0;
}
