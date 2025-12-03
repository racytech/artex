#ifndef ART_COMMON_RLP_H
#define ART_COMMON_RLP_H

#include "bytes.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * RLP (Recursive Length Prefix) Encoding
 * 
 * Ethereum's serialization format for encoding arbitrarily nested arrays
 * of binary data. RLP is used for encoding trie nodes, transactions, blocks, etc.
 * 
 * Encoding rules:
 * - Single byte [0x00, 0x7f]: encoded as itself
 * - String 0-55 bytes: 0x80 + length, then data
 * - String >55 bytes: 0xb7 + length_of_length, length, then data
 * - List 0-55 bytes: 0xc0 + length, then concatenated encoded items
 * - List >55 bytes: 0xf7 + length_of_length, length, then concatenated encoded items
 */

// RLP item type
typedef enum {
    RLP_TYPE_STRING,    // Byte string
    RLP_TYPE_LIST       // List of items
} rlp_type_t;

// Forward declaration
typedef struct rlp_item_t rlp_item_t;

// RLP item (can be string or list)
struct rlp_item_t {
    rlp_type_t type;
    union {
        bytes_t string;                 // For string type
        struct {
            rlp_item_t** items;         // Array of item pointers
            size_t count;               // Number of items
        } list;                         // For list type
    } data;
};

// Create RLP string item from bytes
rlp_item_t* rlp_string(const uint8_t* data, size_t len);

// Create RLP string item from bytes_t
rlp_item_t* rlp_string_from_bytes(const bytes_t* bytes);

// Create RLP string from uint64
rlp_item_t* rlp_uint64(uint64_t value);

// Create empty RLP list
rlp_item_t* rlp_list_new(void);

// Append item to list (takes ownership of item)
bool rlp_list_append(rlp_item_t* list, rlp_item_t* item);

// Free RLP item (recursively frees all nested items)
void rlp_item_free(rlp_item_t* item);

// Encode RLP item to bytes
bytes_t rlp_encode(const rlp_item_t* item);

// Decode RLP bytes to item
rlp_item_t* rlp_decode(const uint8_t* data, size_t len);

// Decode RLP bytes_t to item
rlp_item_t* rlp_decode_bytes(const bytes_t* bytes);

// Get item type
rlp_type_t rlp_get_type(const rlp_item_t* item);

// Get string data (returns NULL if not a string)
const bytes_t* rlp_get_string(const rlp_item_t* item);

// Get list count (returns 0 if not a list)
size_t rlp_get_list_count(const rlp_item_t* item);

// Get list item at index (returns NULL if not a list or invalid index)
const rlp_item_t* rlp_get_list_item(const rlp_item_t* item, size_t index);

// Helper: Encode empty string
bytes_t rlp_encode_empty_string(void);

// Helper: Encode single byte
bytes_t rlp_encode_byte(uint8_t byte);

// Helper: Encode bytes directly (without creating item)
bytes_t rlp_encode_bytes(const uint8_t* data, size_t len);

// Helper: Encode uint64 directly (without creating item)
bytes_t rlp_encode_uint64_direct(uint64_t value);

#ifdef __cplusplus
}
#endif

#endif // ART_COMMON_RLP_H
