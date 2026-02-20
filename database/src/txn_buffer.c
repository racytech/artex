/*
 * Transaction Buffer Implementation
 */

#include "txn_buffer.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_INITIAL_CAPACITY 16

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
    
    buffer->num_ops = 0;
    buffer->capacity = initial_capacity;
    buffer->txn_id = txn_id;
    
    return buffer;
}

void txn_buffer_destroy(txn_buffer_t *buffer) {
    if (!buffer) {
        return;
    }
    
    // Free all operation data
    for (size_t i = 0; i < buffer->num_ops; i++) {
        free(buffer->operations[i].key);
        free(buffer->operations[i].value);
    }
    
    free(buffer->operations);
    free(buffer);
}

void txn_buffer_clear(txn_buffer_t *buffer) {
    if (!buffer) {
        return;
    }
    
    // Free all operation data
    for (size_t i = 0; i < buffer->num_ops; i++) {
        free(buffer->operations[i].key);
        free(buffer->operations[i].value);
        buffer->operations[i].key = NULL;
        buffer->operations[i].value = NULL;
    }
    
    buffer->num_ops = 0;
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
    
    // Allocate and copy key
    uint8_t *key_copy = malloc(key_len);
    if (!key_copy) {
        LOG_ERROR("Failed to allocate key copy");
        return false;
    }
    memcpy(key_copy, key, key_len);
    
    // Allocate and copy value
    uint8_t *value_copy = malloc(value_len);
    if (!value_copy) {
        LOG_ERROR("Failed to allocate value copy");
        free(key_copy);
        return false;
    }
    memcpy(value_copy, value, value_len);
    
    // Add operation
    txn_operation_t *op = &buffer->operations[buffer->num_ops];
    op->type = TXN_OP_INSERT;
    op->key = key_copy;
    op->key_len = key_len;
    op->value = value_copy;
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
    
    // Allocate and copy key
    uint8_t *key_copy = malloc(key_len);
    if (!key_copy) {
        LOG_ERROR("Failed to allocate key copy");
        return false;
    }
    memcpy(key_copy, key, key_len);
    
    // Add operation
    txn_operation_t *op = &buffer->operations[buffer->num_ops];
    op->type = TXN_OP_DELETE;
    op->key = key_copy;
    op->key_len = key_len;
    op->value = NULL;
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
