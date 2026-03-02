/**
 * VM Memory — Expandable Byte Array
 *
 * Byte-addressable, expandable linear memory. Cleared between calls.
 * Gas cost increases quadratically with size. Initially all zeros.
 */

#ifndef ART_VM_MEMORY_H
#define ART_VM_MEMORY_H

#include "uint256.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VM_MEMORY_WORD_SIZE 32

typedef struct vm_memory {
    uint8_t *data;
    size_t   size;      // current size in bytes (word-aligned)
    size_t   capacity;  // allocated capacity
} vm_memory_t;

// Lifecycle
vm_memory_t *vm_memory_create(void);
void         vm_memory_destroy(vm_memory_t *mem);
void         vm_memory_reset(vm_memory_t *mem);

// Byte access
bool vm_memory_read_byte(vm_memory_t *mem, uint64_t offset, uint8_t *value);
bool vm_memory_write_byte(vm_memory_t *mem, uint64_t offset, uint8_t value);

// Word access (32 bytes, big-endian)
bool vm_memory_read_word(vm_memory_t *mem, uint64_t offset, uint256_t *value);
bool vm_memory_write_word(vm_memory_t *mem, uint64_t offset, const uint256_t *value);

// Bulk access
bool vm_memory_read(vm_memory_t *mem, uint64_t offset, uint8_t *data, size_t size);
bool vm_memory_write(vm_memory_t *mem, uint64_t offset, const uint8_t *data, size_t size);

// Expansion
bool vm_memory_expand(vm_memory_t *mem, uint64_t offset, size_t size);
bool vm_memory_ensure_size(vm_memory_t *mem, size_t min_size);

// State
size_t vm_memory_size(const vm_memory_t *mem);
size_t vm_memory_size_words(const vm_memory_t *mem);
bool   vm_memory_is_empty(const vm_memory_t *mem);

// Gas cost
uint64_t vm_memory_expansion_cost(size_t current_size, size_t new_size);
uint64_t vm_memory_access_cost(const vm_memory_t *mem, uint64_t offset, size_t size);

// Utilities
bool           vm_memory_copy(vm_memory_t *mem, uint64_t dest, uint64_t src, size_t size);
const uint8_t *vm_memory_get_ptr(const vm_memory_t *mem, uint64_t offset);
uint8_t       *vm_memory_get_mut_ptr(vm_memory_t *mem, uint64_t offset, size_t size);

#ifdef __cplusplus
}
#endif

#endif /* ART_VM_MEMORY_H */
