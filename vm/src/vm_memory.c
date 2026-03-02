/**
 * VM Memory Implementation
 * Adapted from evm_memory.c with vm_memory_ prefix.
 */

#include "vm_memory.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY 1024

static inline size_t round_up_to_word(size_t size)
{
    return (size + VM_MEMORY_WORD_SIZE - 1) & ~((size_t)VM_MEMORY_WORD_SIZE - 1);
}

//==============================================================================
// Lifecycle
//==============================================================================

vm_memory_t *vm_memory_create(void)
{
    vm_memory_t *mem = (vm_memory_t *)calloc(1, sizeof(vm_memory_t));
    if (!mem) return NULL;

    mem->capacity = INITIAL_CAPACITY;
    mem->data = (uint8_t *)calloc(mem->capacity, 1);
    if (!mem->data) {
        free(mem);
        return NULL;
    }
    mem->size = 0;
    return mem;
}

void vm_memory_destroy(vm_memory_t *mem)
{
    if (!mem) return;
    free(mem->data);
    free(mem);
}

void vm_memory_reset(vm_memory_t *mem)
{
    if (!mem) return;
    mem->size = 0;
    if (mem->data)
        memset(mem->data, 0, mem->capacity);
}

//==============================================================================
// Memory Access
//==============================================================================

bool vm_memory_read_byte(vm_memory_t *mem, uint64_t offset, uint8_t *value)
{
    if (!mem || !value) return false;
    if (!vm_memory_expand(mem, offset, 1)) return false;
    *value = mem->data[offset];
    return true;
}

bool vm_memory_write_byte(vm_memory_t *mem, uint64_t offset, uint8_t value)
{
    if (!mem) return false;
    if (!vm_memory_expand(mem, offset, 1)) return false;
    mem->data[offset] = value;
    return true;
}

bool vm_memory_read_word(vm_memory_t *mem, uint64_t offset, uint256_t *value)
{
    if (!mem || !value) return false;
    if (!vm_memory_expand(mem, offset, VM_MEMORY_WORD_SIZE)) return false;
    *value = uint256_from_bytes(&mem->data[offset], VM_MEMORY_WORD_SIZE);
    return true;
}

bool vm_memory_write_word(vm_memory_t *mem, uint64_t offset, const uint256_t *value)
{
    if (!mem || !value) return false;
    if (!vm_memory_expand(mem, offset, VM_MEMORY_WORD_SIZE)) return false;
    uint256_to_bytes(value, &mem->data[offset]);
    return true;
}

bool vm_memory_read(vm_memory_t *mem, uint64_t offset, uint8_t *data, size_t size)
{
    if (!mem || !data) return false;
    if (size == 0) return true;
    if (!vm_memory_expand(mem, offset, size)) return false;
    memcpy(data, &mem->data[offset], size);
    return true;
}

bool vm_memory_write(vm_memory_t *mem, uint64_t offset, const uint8_t *data, size_t size)
{
    if (!mem || !data) return false;
    if (size == 0) return true;
    if (!vm_memory_expand(mem, offset, size)) return false;
    memcpy(&mem->data[offset], data, size);
    return true;
}

//==============================================================================
// Memory Expansion
//==============================================================================

bool vm_memory_expand(vm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem) return false;
    if (size == 0) return true;

    uint64_t end = offset + size;
    if (end < offset) return false; // overflow

    size_t new_size = round_up_to_word(end);
    if (new_size <= mem->size) return true;

    return vm_memory_ensure_size(mem, new_size);
}

bool vm_memory_ensure_size(vm_memory_t *mem, size_t min_size)
{
    if (!mem) return false;

    size_t new_size = round_up_to_word(min_size);
    if (new_size <= mem->size) return true;

    if (new_size > mem->capacity) {
        size_t new_capacity = mem->capacity;
        while (new_capacity < new_size)
            new_capacity *= 2;

        uint8_t *new_data = (uint8_t *)realloc(mem->data, new_capacity);
        if (!new_data) return false;

        memset(new_data + mem->capacity, 0, new_capacity - mem->capacity);
        mem->data = new_data;
        mem->capacity = new_capacity;
    }

    mem->size = new_size;
    return true;
}

//==============================================================================
// Memory State
//==============================================================================

size_t vm_memory_size(const vm_memory_t *mem)
{
    return mem ? mem->size : 0;
}

size_t vm_memory_size_words(const vm_memory_t *mem)
{
    if (!mem) return 0;
    return (mem->size + VM_MEMORY_WORD_SIZE - 1) / VM_MEMORY_WORD_SIZE;
}

bool vm_memory_is_empty(const vm_memory_t *mem)
{
    return mem ? (mem->size == 0) : true;
}

//==============================================================================
// Gas Cost Calculation
//==============================================================================

uint64_t vm_memory_expansion_cost(size_t current_size, size_t new_size)
{
    if (new_size <= current_size) return 0;

    size_t cw = (current_size + VM_MEMORY_WORD_SIZE - 1) / VM_MEMORY_WORD_SIZE;
    size_t nw = (new_size + VM_MEMORY_WORD_SIZE - 1) / VM_MEMORY_WORD_SIZE;

    uint64_t cc = 3 * cw + (cw * cw) / 512;
    uint64_t nc = 3 * nw + (nw * nw) / 512;
    return nc - cc;
}

uint64_t vm_memory_access_cost(const vm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem || size == 0) return 0;
    size_t new_size = round_up_to_word(offset + size);
    return vm_memory_expansion_cost(mem->size, new_size);
}

//==============================================================================
// Utilities
//==============================================================================

bool vm_memory_copy(vm_memory_t *mem, uint64_t dest, uint64_t src, size_t size)
{
    if (!mem) return false;
    if (size == 0) return true;

    uint64_t max_off = (dest > src) ? dest : src;
    if (!vm_memory_expand(mem, max_off, size)) return false;

    memmove(&mem->data[dest], &mem->data[src], size);
    return true;
}

const uint8_t *vm_memory_get_ptr(const vm_memory_t *mem, uint64_t offset)
{
    if (!mem || offset >= mem->size) return NULL;
    return &mem->data[offset];
}

uint8_t *vm_memory_get_mut_ptr(vm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem) return NULL;
    if (!vm_memory_expand(mem, offset, size)) return NULL;
    return &mem->data[offset];
}
