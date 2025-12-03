#ifndef ART_COMMON_ADDRESS_H
#define ART_COMMON_ADDRESS_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Ethereum address is 20 bytes
#define ADDRESS_SIZE 20

typedef struct {
    uint8_t bytes[ADDRESS_SIZE];
} address_t;

// Create address from bytes (copies data)
address_t address_from_bytes(const uint8_t* data);

// Create address from hex string (with or without 0x prefix)
// Returns true on success, false on invalid input
bool address_from_hex(const char* hex, address_t* out);

// Create zero address
address_t address_zero(void);

// Convert address to hex string (with 0x prefix)
// Buffer must be at least 43 bytes (0x + 40 hex chars + null terminator)
void address_to_hex(const address_t* addr, char* out);

// Compare two addresses
bool address_equal(const address_t* a, const address_t* b);

// Check if address is zero
bool address_is_zero(const address_t* addr);

// Copy address
void address_copy(address_t* dest, const address_t* src);

// Convert uint256_t to address (takes lowest 20 bytes)
void address_from_uint256(const void* uint256_ptr, address_t* out);

#ifdef __cplusplus
}
#endif

#endif // ART_COMMON_ADDRESS_H
