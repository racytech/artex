#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Data Layer — hash_store + code.dat.
 *
 * Architecture:
 *   dl_put/dl_delete  → hash_store directly (mmap'd, zero-copy)
 *   dl_get            → hash_store (single lookup)
 *   dl_put_code       → code.dat + hash_store
 *   dl_get_code       → hash_store → code.dat
 *   dl_merge          → no-op (kept for API compat, writes are immediate)
 *
 * hash_store: zero-RAM mmap'd hash table. Writes land directly in
 * memory-mapped slots — no intermediate buffer or copy needed.
 *
 * Opaque handle — struct defined in data_layer.c.
 */

typedef struct data_layer data_layer_t;

typedef struct {
    uint64_t index_keys;      // hash_store count (committed entries)
    uint32_t code_count;       // code_store entries
    uint64_t code_file_size;   // code.dat file size in bytes
} dl_stats_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new data layer backed by hash_store.
 * dir: directory for hash_store shard files.
 * code_path: path for code.dat (NULL to disable code store).
 * key_size: fixed key length (e.g., 32 for accounts, 64 for storage).
 * slot_size: hash_store slot size (e.g., 128 for accounts, 64 for storage).
 * shard_capacity: slots per shard (power of 2, e.g., 1<<24 = 16M).
 * Returns NULL on failure.
 */
data_layer_t *dl_create(const char *dir, const char *code_path,
                         uint32_t key_size, uint32_t slot_size,
                         uint64_t shard_capacity);

/**
 * Open an existing data layer from hash_store directory.
 * Reads hash_store metadata (key_size/slot_size stored on disk).
 * code_path: path for code.dat (NULL if no code store).
 * Returns NULL on failure.
 */
data_layer_t *dl_open(const char *dir, const char *code_path);

/**
 * Destroy the data layer and free all resources.
 * Syncs hash_store before closing. Does NOT remove files.
 */
void dl_destroy(data_layer_t *dl);

// ============================================================================
// Per-block Operations
// ============================================================================

/**
 * Write a key-value pair directly to hash_store.
 * len must be <= hash_store max_value.
 */
bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len);

/**
 * Delete a key from hash_store (tombstone).
 */
bool dl_delete(data_layer_t *dl, const uint8_t *key);

/**
 * Read a key from hash_store.
 * Returns false if not found or deleted.
 */
bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len);

// ============================================================================
// Code Operations (bypass write buffer — direct to code.dat + hash_store)
// ============================================================================

/**
 * Store contract bytecode. Content-addressed: if key already exists in
 * hash_store, this is a no-op (dedup). Writes directly to code.dat.
 * Returns false on failure or if code store is not configured.
 */
bool dl_put_code(data_layer_t *dl, const uint8_t *key,
                 const void *bytecode, uint32_t len);

/**
 * Read contract bytecode by key. out_len receives the bytecode length.
 * Returns false if not found or if key points to state (not code).
 */
bool dl_get_code(data_layer_t *dl, const uint8_t *key,
                 void *out, uint32_t *out_len);

/**
 * Get bytecode length for a key without reading the data.
 * Returns 0 if not found or if key points to state.
 */
uint32_t dl_code_length(data_layer_t *dl, const uint8_t *key);

// ============================================================================
// Merge
// ============================================================================

/**
 * No-op — writes go directly to hash_store. Kept for API compatibility.
 * Always returns 0.
 */
uint64_t dl_merge(data_layer_t *dl);

// ============================================================================
// Checkpoint / Recovery
// ============================================================================

/**
 * Sync hash_store to disk (flush dirty shard headers + metadata).
 * Data is already on disk via mmap — this just ensures consistency.
 * Returns false on failure.
 */
bool dl_checkpoint(data_layer_t *dl);

// ============================================================================
// Diagnostics
// ============================================================================

/**
 * Get current stats snapshot.
 */
dl_stats_t dl_stats(const data_layer_t *dl);

#endif // DATA_LAYER_H
