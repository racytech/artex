/*
 * Transaction Buffer - Atomic Multi-Key Updates
 * 
 * Buffers insert/delete operations during a transaction, applying them
 * atomically on commit or discarding on abort.
 * 
 * This provides true atomicity for multi-key updates without requiring
 * MVCC versioning infrastructure.
 */

#ifndef TXN_BUFFER_H
#define TXN_BUFFER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

// ============================================================================
// Types
// ============================================================================

typedef enum {
    TXN_OP_INSERT,
    TXN_OP_DELETE
} txn_op_type_t;

/**
 * Single buffered operation
 */
typedef struct {
    txn_op_type_t type;
    uint8_t *key;
    size_t key_len;
    uint8_t *value;      // NULL for delete operations
    size_t value_len;    // 0 for delete operations
} txn_operation_t;

/**
 * Transaction buffer
 * 
 * Stores all pending operations for an active transaction.
 * Operations are applied to the tree only on commit.
 */
typedef struct txn_buffer {
    txn_operation_t *operations;  // Dynamic array of operations
    size_t num_ops;               // Number of operations
    size_t capacity;              // Allocated capacity
    uint64_t txn_id;              // Associated transaction ID
} txn_buffer_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new transaction buffer
 * 
 * @param txn_id Transaction ID
 * @param initial_capacity Initial capacity (default: 16)
 * @return Buffer instance, or NULL on failure
 */
txn_buffer_t *txn_buffer_create(uint64_t txn_id, size_t initial_capacity);

/**
 * Destroy buffer and free all resources
 * 
 * @param buffer Buffer to destroy
 */
void txn_buffer_destroy(txn_buffer_t *buffer);

/**
 * Clear all operations from buffer (used on abort)
 * 
 * @param buffer Buffer to clear
 */
void txn_buffer_clear(txn_buffer_t *buffer);

// ============================================================================
// Operations
// ============================================================================

/**
 * Add insert operation to buffer
 * 
 * @param buffer Transaction buffer
 * @param key Key bytes
 * @param key_len Key length
 * @param value Value bytes
 * @param value_len Value length
 * @return true on success, false on failure
 */
bool txn_buffer_add_insert(txn_buffer_t *buffer,
                           const uint8_t *key, size_t key_len,
                           const void *value, size_t value_len);

/**
 * Add delete operation to buffer
 * 
 * @param buffer Transaction buffer
 * @param key Key bytes
 * @param key_len Key length
 * @return true on success, false on failure
 */
bool txn_buffer_add_delete(txn_buffer_t *buffer,
                           const uint8_t *key, size_t key_len);

/**
 * Get number of operations in buffer
 * 
 * @param buffer Transaction buffer
 * @return Number of pending operations
 */
size_t txn_buffer_size(const txn_buffer_t *buffer);

/**
 * Check if buffer is empty
 * 
 * @param buffer Transaction buffer
 * @return true if no operations buffered
 */
bool txn_buffer_is_empty(const txn_buffer_t *buffer);

#endif // TXN_BUFFER_H
