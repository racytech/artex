#ifndef ART_COMMON_HASH_H
#define ART_COMMON_HASH_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Keccak-256 hash is 32 bytes
#define HASH_SIZE 32

typedef struct {
    uint8_t bytes[HASH_SIZE];
} hash_t;

// Ethereum standard empty hashes
// Empty code hash = Keccak256("")
extern const hash_t HASH_EMPTY_CODE;

// Empty storage root = Keccak256(RLP([]))
extern const hash_t HASH_EMPTY_STORAGE;

// Create hash from bytes (copies data)
hash_t hash_from_bytes(const uint8_t* data);

// Create hash from hex string (with or without 0x prefix)
// Returns true on success, false on invalid input
bool hash_from_hex(const char* hex, hash_t* out);

// Create zero hash
hash_t hash_zero(void);

// Convert hash to hex string (with 0x prefix)
// Buffer must be at least 67 bytes (0x + 64 hex chars + null terminator)
void hash_to_hex(const hash_t* h, char* out);

// Compare two hashes
bool hash_equal(const hash_t* a, const hash_t* b);

// Check if hash is zero
bool hash_is_zero(const hash_t* h);

// Copy hash
void hash_copy(hash_t* dest, const hash_t* src);

// Compute Keccak-256 hash of data
hash_t hash_keccak256(const uint8_t* data, size_t len);

#ifdef __cplusplus
}
#endif

#endif // ART_COMMON_HASH_H
