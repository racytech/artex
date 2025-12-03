#include "address.h"
#include <string.h>
#include <ctype.h>

address_t address_from_bytes(const uint8_t* data) {
    address_t addr;
    memcpy(addr.bytes, data, ADDRESS_SIZE);
    return addr;
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

bool address_from_hex(const char* hex, address_t* out) {
    if (!hex || !out) return false;
    
    // Skip 0x prefix if present
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
        hex += 2;
    }
    
    size_t hex_len = strlen(hex);
    if (hex_len != ADDRESS_SIZE * 2) return false;
    
    for (size_t i = 0; i < ADDRESS_SIZE; i++) {
        int high = hex_char_to_value(hex[i * 2]);
        int low = hex_char_to_value(hex[i * 2 + 1]);
        
        if (high < 0 || low < 0) return false;
        
        out->bytes[i] = (high << 4) | low;
    }
    
    return true;
}

address_t address_zero(void) {
    address_t addr;
    memset(addr.bytes, 0, ADDRESS_SIZE);
    return addr;
}

void address_to_hex(const address_t* addr, char* out) {
    out[0] = '0';
    out[1] = 'x';
    
    for (size_t i = 0; i < ADDRESS_SIZE; i++) {
        out[2 + i * 2] = value_to_hex_char(addr->bytes[i] >> 4);
        out[2 + i * 2 + 1] = value_to_hex_char(addr->bytes[i] & 0x0F);
    }
    
    out[2 + ADDRESS_SIZE * 2] = '\0';
}

bool address_equal(const address_t* a, const address_t* b) {
    return memcmp(a->bytes, b->bytes, ADDRESS_SIZE) == 0;
}

bool address_is_zero(const address_t* addr) {
    for (size_t i = 0; i < ADDRESS_SIZE; i++) {
        if (addr->bytes[i] != 0) return false;
    }
    return true;
}

void address_copy(address_t* dest, const address_t* src) {
    memcpy(dest->bytes, src->bytes, ADDRESS_SIZE);
}

void address_from_uint256(const void* uint256_ptr, address_t* out) {
    if (!uint256_ptr || !out) return;
    
    // uint256_t is stored in little-endian (lowest bytes first)
    // Address is the lowest 20 bytes of the uint256_t
    const uint8_t* bytes = (const uint8_t*)uint256_ptr;
    memcpy(out->bytes, bytes, ADDRESS_SIZE);
}
