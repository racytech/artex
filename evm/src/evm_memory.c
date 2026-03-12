/**
 * EVM Memory Implementation
 */

#include "evm_memory.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY 1024 // Start with 1KB

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * Round up to next word boundary (32 bytes)
 */
static inline size_t round_up_to_word(size_t size)
{
    return (size + EVM_MEMORY_WORD_SIZE - 1) & ~(EVM_MEMORY_WORD_SIZE - 1);
}

//==============================================================================
// Lifecycle
//==============================================================================

evm_memory_t *evm_memory_create(void)
{
    evm_memory_t *mem = (evm_memory_t *)calloc(1, sizeof(evm_memory_t));
    if (!mem)
    {
        LOG_EVM_ERROR("Failed to allocate memory structure");
        return NULL;
    }

    mem->capacity = INITIAL_CAPACITY;
    mem->data = (uint8_t *)calloc(mem->capacity, 1);
    if (!mem->data)
    {
        LOG_EVM_ERROR("Failed to allocate memory data");
        free(mem);
        return NULL;
    }

    mem->size = 0;

    LOG_EVM_DEBUG("Created EVM memory with initial capacity %zu bytes", mem->capacity);
    return mem;
}

void evm_memory_destroy(evm_memory_t *mem)
{
    if (!mem)
        return;

    if (mem->data)
    {
        free(mem->data);
    }

    free(mem);
    LOG_EVM_DEBUG("Destroyed EVM memory");
}

void evm_memory_reset(evm_memory_t *mem)
{
    if (!mem)
        return;

    mem->size = 0;
    // Zero out the memory for security
    if (mem->data)
    {
        memset(mem->data, 0, mem->capacity);
    }
}

void evm_memory_clear(evm_memory_t *mem)
{
    // Just an alias for reset
    evm_memory_reset(mem);
}

//==============================================================================
// Memory Access
//==============================================================================

bool evm_memory_read_byte(evm_memory_t *mem, uint64_t offset, uint8_t *value)
{
    if (!mem || !value)
    {
        LOG_EVM_DEBUG("Invalid parameters");
        return false;
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, 1))
    {
        return false;
    }

    *value = mem->data[offset];
    return true;
}

bool evm_memory_write_byte(evm_memory_t *mem, uint64_t offset, uint8_t value)
{
    if (!mem)
    {
        LOG_EVM_DEBUG("Invalid memory");
        return false;
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, 1))
    {
        return false;
    }

    mem->data[offset] = value;
    return true;
}

bool evm_memory_read_word(evm_memory_t *mem, uint64_t offset, uint256_t *value)
{
    if (!mem || !value)
    {
        LOG_EVM_DEBUG("Invalid parameters");
        return false;
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, EVM_MEMORY_WORD_SIZE))
    {
        return false;
    }

    // Read 32 bytes in big-endian order
    *value = uint256_from_bytes(&mem->data[offset], EVM_MEMORY_WORD_SIZE);
    return true;
}

bool evm_memory_write_word(evm_memory_t *mem, uint64_t offset, const uint256_t *value)
{
    if (!mem || !value)
    {
        LOG_EVM_DEBUG("Invalid parameters");
        return false;
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, EVM_MEMORY_WORD_SIZE))
    {
        return false;
    }

    // Write 32 bytes in big-endian order
    uint256_to_bytes(value, &mem->data[offset]);
    return true;
}

bool evm_memory_read(evm_memory_t *mem, uint64_t offset, uint8_t *data, size_t size)
{
    if (!mem || !data)
    {
        LOG_EVM_DEBUG("Invalid parameters");
        return false;
    }

    if (size == 0)
    {
        return true; // Nothing to read
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, size))
    {
        return false;
    }

    memcpy(data, &mem->data[offset], size);
    return true;
}

bool evm_memory_write(evm_memory_t *mem, uint64_t offset, const uint8_t *data, size_t size)
{
    if (!mem || !data)
    {
        LOG_EVM_DEBUG("Invalid parameters");
        return false;
    }

    if (size == 0)
    {
        return true; // Nothing to write
    }

    // Expand memory if needed
    if (!evm_memory_expand(mem, offset, size))
    {
        return false;
    }

    memcpy(&mem->data[offset], data, size);
    return true;
}

//==============================================================================
// Memory Expansion
//==============================================================================

bool evm_memory_expand_slow(evm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem)
    {
        LOG_EVM_DEBUG("Invalid memory");
        return false;
    }

    if (size == 0)
    {
        return true;
    }

    // Check for overflow
    uint64_t end = offset + size;
    if (end < offset)
    {
        LOG_EVM_DEBUG("Memory offset overflow");
        return false;
    }

    // Round up to word boundary
    size_t new_size = round_up_to_word(end);

    // Already large enough
    if (new_size <= mem->size)
    {
        return true;
    }

    return evm_memory_ensure_size(mem, new_size);
}

bool evm_memory_ensure_size(evm_memory_t *mem, size_t min_size)
{
    if (!mem)
    {
        LOG_EVM_DEBUG("Invalid memory");
        return false;
    }

    // Round up to word boundary
    size_t new_size = round_up_to_word(min_size);

    // Already large enough
    if (new_size <= mem->size)
    {
        return true;
    }

    // Expand capacity if needed
    if (new_size > mem->capacity)
    {
        size_t new_capacity = mem->capacity;
        while (new_capacity < new_size)
        {
            new_capacity *= 2;
        }

        uint8_t *new_data = (uint8_t *)realloc(mem->data, new_capacity);
        if (!new_data)
        {
            LOG_EVM_ERROR("Failed to expand memory capacity to %zu bytes", new_capacity);
            return false;
        }

        // Zero out new memory
        memset(new_data + mem->capacity, 0, new_capacity - mem->capacity);

        mem->data = new_data;
        mem->capacity = new_capacity;

        LOG_EVM_DEBUG("Expanded memory capacity to %zu bytes", new_capacity);
    }

    // Update size
    mem->size = new_size;
    return true;
}

//==============================================================================
// Memory State
//==============================================================================

size_t evm_memory_size(const evm_memory_t *mem)
{
    return mem ? mem->size : 0;
}

size_t evm_memory_size_words(const evm_memory_t *mem)
{
    if (!mem)
        return 0;

    return (mem->size + EVM_MEMORY_WORD_SIZE - 1) / EVM_MEMORY_WORD_SIZE;
}

bool evm_memory_is_empty(const evm_memory_t *mem)
{
    return mem ? (mem->size == 0) : true;
}

//==============================================================================
// Gas Cost Calculation
//==============================================================================

// evm_memory_expansion_cost and evm_memory_access_cost are now
// static inline in evm_memory.h for interpreter hot-path inlining.

//==============================================================================
// Utility Functions
//==============================================================================

bool evm_memory_copy(evm_memory_t *mem, uint64_t dest_offset, uint64_t src_offset, size_t size)
{
    if (!mem)
    {
        LOG_EVM_DEBUG("Invalid memory");
        return false;
    }

    if (size == 0)
    {
        return true;
    }

    // Expand memory to accommodate both source and destination
    uint64_t max_offset = (dest_offset > src_offset) ? dest_offset : src_offset;
    if (!evm_memory_expand(mem, max_offset, size))
    {
        return false;
    }

    // Use memmove to handle overlapping regions
    memmove(&mem->data[dest_offset], &mem->data[src_offset], size);
    return true;
}

const uint8_t *evm_memory_get_ptr(const evm_memory_t *mem, uint64_t offset)
{
    if (!mem || offset >= mem->size)
    {
        return NULL;
    }

    return &mem->data[offset];
}

uint8_t *evm_memory_get_mut_ptr(evm_memory_t *mem, uint64_t offset, size_t size)
{
    if (!mem)
    {
        return NULL;
    }

    // Expand if necessary
    if (!evm_memory_expand(mem, offset, size))
    {
        return NULL;
    }

    return &mem->data[offset];
}
