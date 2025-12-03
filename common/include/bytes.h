#ifndef ART_COMMON_BYTES_H
#define ART_COMMON_BYTES_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Variable-length byte array
typedef struct {
    uint8_t* data;
    size_t len;
    size_t capacity;
} bytes_t;

// Create empty bytes
bytes_t bytes_new(void);

// Create bytes with initial capacity
bytes_t bytes_with_capacity(size_t capacity);

// Create bytes from existing data (copies data)
bytes_t bytes_from_data(const uint8_t* data, size_t len);

// Create bytes from hex string (with or without 0x prefix)
// Returns true on success, false on invalid input
bool bytes_from_hex(const char* hex, bytes_t* out);

// Free bytes memory
void bytes_free(bytes_t* b);

// Convert bytes to hex string (with 0x prefix)
// Returns allocated string that must be freed by caller
char* bytes_to_hex(const bytes_t* b);

// Reserve capacity (may reallocate)
bool bytes_reserve(bytes_t* b, size_t new_capacity);

// Resize bytes (may reallocate, new bytes are zero-initialized)
bool bytes_resize(bytes_t* b, size_t new_len);

// Append data to bytes
bool bytes_append(bytes_t* b, const uint8_t* data, size_t len);

// Append single byte
bool bytes_push(bytes_t* b, uint8_t byte);

// Clear bytes (sets length to 0, keeps capacity)
void bytes_clear(bytes_t* b);

// Clone bytes (deep copy)
bytes_t bytes_clone(const bytes_t* b);

// Compare two bytes
bool bytes_equal(const bytes_t* a, const bytes_t* b);

// Check if bytes is empty
bool bytes_is_empty(const bytes_t* b);

#ifdef __cplusplus
}
#endif

#endif // ART_COMMON_BYTES_H
