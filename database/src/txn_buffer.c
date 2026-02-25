/*
 * Transaction Buffer Implementation
 *
 * Uses a flat arena for key/value data — zero per-op malloc/free.
 */

#include "txn_buffer.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_INITIAL_CAPACITY 16
#define DEFAULT_ARENA_SIZE       4096

// ============================================================================
// Arena Helper
// ============================================================================

/**
 * Bump-allocate `size` bytes from the arena. Grows with realloc if needed.
 * Returns offset into arena, or (size_t)-1 on failure.
 */
static size_t arena_alloc(txn_buffer_t *buffer, size_t size) {
    size_t needed = buffer->arena_used + size;
    if (needed > buffer->arena_cap) {
        size_t new_cap = buffer->arena_cap * 2;
        while (new_cap < needed) new_cap *= 2;
        uint8_t *new_arena = realloc(buffer->arena, new_cap);
        if (!new_arena) {
            LOG_ERROR("Failed to grow arena");
            return (size_t)-1;
        }
        buffer->arena = new_arena;
        buffer->arena_cap = new_cap;
    }
    size_t off = buffer->arena_used;
    buffer->arena_used = needed;
    return off;
}

// ============================================================================
// Lifecycle
// ============================================================================

txn_buffer_t *txn_buffer_create(uint64_t txn_id, size_t initial_capacity) {
    if (initial_capacity == 0) {
        initial_capacity = DEFAULT_INITIAL_CAPACITY;
    }

    txn_buffer_t *buffer = calloc(1, sizeof(txn_buffer_t));
    if (!buffer) {
        LOG_ERROR("Failed to allocate transaction buffer");
        return NULL;
    }

    buffer->operations = calloc(initial_capacity, sizeof(txn_operation_t));
    if (!buffer->operations) {
        LOG_ERROR("Failed to allocate operations array");
        free(buffer);
        return NULL;
    }

    buffer->arena = malloc(DEFAULT_ARENA_SIZE);
    if (!buffer->arena) {
        LOG_ERROR("Failed to allocate arena");
        free(buffer->operations);
        free(buffer);
        return NULL;
    }

    buffer->num_ops = 0;
    buffer->capacity = initial_capacity;
    buffer->txn_id = txn_id;
    buffer->arena_used = 0;
    buffer->arena_cap = DEFAULT_ARENA_SIZE;

    return buffer;
}

void txn_buffer_destroy(txn_buffer_t *buffer) {
    if (!buffer) {
        return;
    }

    free(buffer->arena);
    free(buffer->operations);
    free(buffer);
}

void txn_buffer_clear(txn_buffer_t *buffer) {
    if (!buffer) {
        return;
    }

    buffer->num_ops = 0;
    buffer->arena_used = 0;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Grow operations array if needed
 */
static bool ensure_capacity(txn_buffer_t *buffer) {
    if (buffer->num_ops < buffer->capacity) {
        return true;
    }

    size_t new_capacity = buffer->capacity * 2;
    txn_operation_t *new_ops = realloc(buffer->operations,
                                       new_capacity * sizeof(txn_operation_t));
    if (!new_ops) {
        LOG_ERROR("Failed to grow operations array");
        return false;
    }

    buffer->operations = new_ops;
    buffer->capacity = new_capacity;

    return true;
}

// ============================================================================
// Operations
// ============================================================================

bool txn_buffer_add_insert(txn_buffer_t *buffer,
                           const uint8_t *key, size_t key_len,
                           const void *value, size_t value_len) {
    if (!buffer || !key || !value) {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    if (!ensure_capacity(buffer)) {
        return false;
    }

    size_t key_off = arena_alloc(buffer, key_len);
    if (key_off == (size_t)-1) return false;
    memcpy(buffer->arena + key_off, key, key_len);

    size_t value_off = arena_alloc(buffer, value_len);
    if (value_off == (size_t)-1) return false;
    memcpy(buffer->arena + value_off, value, value_len);

    txn_operation_t *op = &buffer->operations[buffer->num_ops];
    op->type = TXN_OP_INSERT;
    op->key_off = key_off;
    op->key_len = key_len;
    op->value_off = value_off;
    op->value_len = value_len;

    buffer->num_ops++;

    return true;
}

bool txn_buffer_add_delete(txn_buffer_t *buffer,
                           const uint8_t *key, size_t key_len) {
    if (!buffer || !key) {
        LOG_ERROR("Invalid parameters");
        return false;
    }

    if (!ensure_capacity(buffer)) {
        return false;
    }

    size_t key_off = arena_alloc(buffer, key_len);
    if (key_off == (size_t)-1) return false;
    memcpy(buffer->arena + key_off, key, key_len);

    txn_operation_t *op = &buffer->operations[buffer->num_ops];
    op->type = TXN_OP_DELETE;
    op->key_off = key_off;
    op->key_len = key_len;
    op->value_off = 0;
    op->value_len = 0;

    buffer->num_ops++;

    return true;
}

size_t txn_buffer_size(const txn_buffer_t *buffer) {
    return buffer ? buffer->num_ops : 0;
}

bool txn_buffer_is_empty(const txn_buffer_t *buffer) {
    return !buffer || buffer->num_ops == 0;
}
