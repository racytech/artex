/**
 * EVM Memory - Expandable Byte Array
 *
 * The EVM provides a volatile, byte-addressed memory that can be expanded
 * during execution. Memory is word-addressed (32-byte chunks) but can be
 * accessed at byte granularity.
 *
 * Memory properties:
 * - Byte-addressable
 * - Expandable (grows on demand)
 * - Volatile (cleared between calls)
 * - Gas cost increases quadratically with size
 * - Initially all zeros
 */

#ifndef ART_EVM_MEMORY_H
#define ART_EVM_MEMORY_H

#include "uint256.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

//==============================================================================
// Constants
//==============================================================================

#define EVM_MEMORY_WORD_SIZE 32 // Word size in bytes

//==============================================================================
// Memory Structure
//==============================================================================

/**
 * EVM memory structure
 */
typedef struct evm_memory_t
{
    uint8_t *data; // Memory data (dynamically allocated)
    size_t size;   // Current size in bytes
    size_t capacity; // Allocated capacity in bytes
} evm_memory_t;

//==============================================================================
// Lifecycle
//==============================================================================

/**
 * Create a new memory instance
 *
 * @return Memory instance, or NULL on failure
 */
evm_memory_t *evm_memory_create(void);

/**
 * Destroy memory and free resources
 *
 * @param mem Memory to destroy
 */
void evm_memory_destroy(evm_memory_t *mem);

/**
 * Reset memory to empty state
 * Clears all data but keeps allocated capacity
 *
 * @param mem Memory to reset
 */
void evm_memory_reset(evm_memory_t *mem);

/**
 * Clear memory (alias for reset)
 * Sets size to 0 without deallocating
 *
 * @param mem Memory to clear
 */
void evm_memory_clear(evm_memory_t *mem);

//==============================================================================
// Memory Access
//==============================================================================

/**
 * Read a single byte from memory
 *
 * @param mem Memory instance
 * @param offset Byte offset to read from
 * @param value Output parameter for byte value
 * @return true on success, false on error
 */
bool evm_memory_read_byte(evm_memory_t *mem, uint64_t offset, uint8_t *value);

/**
 * Write a single byte to memory
 * Expands memory if necessary
 *
 * @param mem Memory instance
 * @param offset Byte offset to write to
 * @param value Byte value to write
 * @return true on success, false on allocation failure
 */
bool evm_memory_write_byte(evm_memory_t *mem, uint64_t offset, uint8_t value);

/**
 * Read a 256-bit word from memory (32 bytes)
 *
 * @param mem Memory instance
 * @param offset Byte offset to read from
 * @param value Output parameter for word value
 * @return true on success, false on error
 */
bool evm_memory_read_word(evm_memory_t *mem, uint64_t offset, uint256_t *value);

/**
 * Write a 256-bit word to memory (32 bytes)
 * Expands memory if necessary
 *
 * @param mem Memory instance
 * @param offset Byte offset to write to
 * @param value Word value to write
 * @return true on success, false on allocation failure
 */
bool evm_memory_write_word(evm_memory_t *mem, uint64_t offset, const uint256_t *value);

/**
 * Read a byte range from memory
 *
 * @param mem Memory instance
 * @param offset Byte offset to start reading from
 * @param data Output buffer
 * @param size Number of bytes to read
 * @return true on success, false on error
 */
bool evm_memory_read(evm_memory_t *mem, uint64_t offset, uint8_t *data, size_t size);

/**
 * Write a byte range to memory
 * Expands memory if necessary
 *
 * @param mem Memory instance
 * @param offset Byte offset to start writing to
 * @param data Data to write
 * @param size Number of bytes to write
 * @return true on success, false on allocation failure
 */
bool evm_memory_write(evm_memory_t *mem, uint64_t offset, const uint8_t *data, size_t size);

//==============================================================================
// Memory Expansion
//==============================================================================

/**
 * Expand memory to accommodate the given offset + size
 * Memory is expanded in 32-byte word increments
 *
 * @param mem Memory instance
 * @param offset Starting offset
 * @param size Number of bytes needed
 * @return true on success, false on allocation failure
 */
bool evm_memory_expand_slow(evm_memory_t *mem, uint64_t offset, size_t size);

/**
 * Inline fast path: skip function call when memory is already large enough
 */
static inline bool evm_memory_expand(evm_memory_t *mem, uint64_t offset, size_t size) {
    if (size == 0) return true;
    uint64_t end = offset + size;
    if (__builtin_expect(end < offset, 0)) return false; /* overflow */
    size_t new_size = (end + EVM_MEMORY_WORD_SIZE - 1) & ~(size_t)(EVM_MEMORY_WORD_SIZE - 1);
    if (__builtin_expect(new_size <= mem->size, 1)) return true;
    return evm_memory_expand_slow(mem, offset, size);
}

/**
 * Ensure memory has at least the given size
 * Expands if necessary, rounds up to word boundary
 *
 * @param mem Memory instance
 * @param min_size Minimum size in bytes
 * @return true on success, false on allocation failure
 */
bool evm_memory_ensure_size(evm_memory_t *mem, size_t min_size);

//==============================================================================
// Memory State
//==============================================================================

/**
 * Get current memory size in bytes
 *
 * @param mem Memory instance
 * @return Current size
 */
size_t evm_memory_size(const evm_memory_t *mem);

/**
 * Get current memory size in 32-byte words
 *
 * @param mem Memory instance
 * @return Current size in words (rounded up)
 */
size_t evm_memory_size_words(const evm_memory_t *mem);

/**
 * Check if memory is empty
 *
 * @param mem Memory instance
 * @return true if memory size is 0
 */
bool evm_memory_is_empty(const evm_memory_t *mem);

//==============================================================================
// Gas Cost Calculation
//==============================================================================

/**
 * Calculate gas cost for memory expansion (inline for interpreter hot path)
 * Uses the Ethereum memory expansion formula:
 *   cost = (memory_size_word^2 / 512) + (3 * memory_size_word)
 */
static inline uint64_t evm_memory_expansion_cost(size_t current_size, size_t new_size)
{
    if (new_size <= current_size)
        return 0;

    // Guard against word-rounding overflow
    if (new_size > SIZE_MAX - (EVM_MEMORY_WORD_SIZE - 1))
        return (uint64_t)1 << 62;

    size_t current_words = (current_size + EVM_MEMORY_WORD_SIZE - 1) / EVM_MEMORY_WORD_SIZE;
    size_t new_words = (new_size + EVM_MEMORY_WORD_SIZE - 1) / EVM_MEMORY_WORD_SIZE;

    uint64_t current_cost = 3 * current_words + ((current_words * current_words) >> 9);
    uint64_t new_cost = 3 * new_words + ((new_words * new_words) >> 9);

    return new_cost - current_cost;
}

/**
 * Calculate gas cost for accessing memory at offset with given size (inline)
 */
static inline uint64_t evm_memory_access_cost(const evm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem || size == 0)
        return 0;

    if (offset > UINT64_MAX - size)
        return (uint64_t)1 << 62;

    size_t end = offset + size;
    // Guard against word-rounding overflow: end + 31 must not wrap
    if (end > UINT64_MAX - (EVM_MEMORY_WORD_SIZE - 1))
        return (uint64_t)1 << 62;

    size_t new_size = (end + EVM_MEMORY_WORD_SIZE - 1) & ~(size_t)(EVM_MEMORY_WORD_SIZE - 1);

    return evm_memory_expansion_cost(mem->size, new_size);
}

//==============================================================================
// Utility Functions
//==============================================================================

/**
 * Copy memory range to another location within memory
 * Handles overlapping ranges correctly
 *
 * @param mem Memory instance
 * @param dest_offset Destination offset
 * @param src_offset Source offset
 * @param size Number of bytes to copy
 * @return true on success, false on allocation failure
 */
bool evm_memory_copy(evm_memory_t *mem, uint64_t dest_offset, uint64_t src_offset, size_t size);

/**
 * Get direct pointer to memory at offset
 * WARNING: Pointer may be invalidated by subsequent memory operations
 *
 * @param mem Memory instance
 * @param offset Byte offset
 * @return Pointer to memory at offset, or NULL if out of bounds
 */
const uint8_t *evm_memory_get_ptr(const evm_memory_t *mem, uint64_t offset);

/**
 * Get mutable pointer to memory at offset
 * Expands memory if necessary
 * WARNING: Pointer may be invalidated by subsequent memory operations
 *
 * @param mem Memory instance
 * @param offset Byte offset
 * @param size Size needed at this offset
 * @return Pointer to memory at offset, or NULL on allocation failure
 */
uint8_t *evm_memory_get_mut_ptr(evm_memory_t *mem, uint64_t offset, size_t size);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_MEMORY_H */
