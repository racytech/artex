#include "hash.h"
#include "keccak256.h"
#include <string.h>
#include <stdlib.h>

// Ethereum standard empty hashes
// Empty code hash = Keccak256("")
const hash_t HASH_EMPTY_CODE = {
    .bytes = {0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
              0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
              0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
              0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70}
};

// Empty trie root = Keccak256(RLP("")) = Keccak256(0x80)
const hash_t HASH_EMPTY_STORAGE = {
    .bytes = {0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
              0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
              0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
              0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21}
};

hash_t hash_from_bytes(const uint8_t* data) {
    hash_t h;
    memcpy(h.bytes, data, HASH_SIZE);
    return h;
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

bool hash_from_hex(const char* hex, hash_t* out) {
    if (!hex || !out) return false;
    
    // Skip 0x prefix if present
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
        hex += 2;
    }
    
    size_t hex_len = strlen(hex);
    if (hex_len != HASH_SIZE * 2) return false;
    
    for (size_t i = 0; i < HASH_SIZE; i++) {
        int high = hex_char_to_value(hex[i * 2]);
        int low = hex_char_to_value(hex[i * 2 + 1]);
        
        if (high < 0 || low < 0) return false;
        
        out->bytes[i] = (high << 4) | low;
    }
    
    return true;
}

hash_t hash_zero(void) {
    hash_t h;
    memset(h.bytes, 0, HASH_SIZE);
    return h;
}

void hash_to_hex(const hash_t* h, char* out) {
    out[0] = '0';
    out[1] = 'x';
    
    for (size_t i = 0; i < HASH_SIZE; i++) {
        out[2 + i * 2] = value_to_hex_char(h->bytes[i] >> 4);
        out[2 + i * 2 + 1] = value_to_hex_char(h->bytes[i] & 0x0F);
    }
    
    out[2 + HASH_SIZE * 2] = '\0';
}

bool hash_equal(const hash_t* a, const hash_t* b) {
    return memcmp(a->bytes, b->bytes, HASH_SIZE) == 0;
}

bool hash_is_zero(const hash_t* h) {
    for (size_t i = 0; i < HASH_SIZE; i++) {
        if (h->bytes[i] != 0) return false;
    }
    return true;
}

void hash_copy(hash_t* dest, const hash_t* src) {
    memcpy(dest->bytes, src->bytes, HASH_SIZE);
}

hash_t hash_keccak256(const uint8_t* data, size_t len) {
    hash_t result;
    SHA3_CTX ctx;
    
    keccak_init(&ctx);
    
    if (data && len > 0) {
        // Process data in chunks since keccak_update takes uint16_t size
        size_t remaining = len;
        const uint8_t* ptr = data;
        
        while (remaining > 0) {
            uint16_t chunk = (remaining > UINT16_MAX) ? UINT16_MAX : (uint16_t)remaining;
            keccak_update(&ctx, ptr, chunk);
            ptr += chunk;
            remaining -= chunk;
        }
    }
    
    keccak_final(&ctx, result.bytes);
    
    return result;
}
