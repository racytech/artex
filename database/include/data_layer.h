#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Data Layer — Write buffer + hash_store + code.dat.
 *
 * Architecture:
 *   dl_put/dl_delete  → write buffer (mem_art, in-memory)
 *   dl_merge          → flush buffer to hash_store (mmap'd, already on disk)
 *   dl_get            → buffer → hash_store (direct, no indirection)
 *   dl_put_code       → direct to code.dat + hash_store (bypasses buffer)
 *   dl_get_code       → hash_store → code.dat (read path)
 *
 * Hash store value encoding:
 *   byte 0 = 0x00 → state entry, bytes 1..len = actual value
 *   byte 0 = 0x01 → code entry, bytes 1..5 = code_store index (uint32_t)
 *
 * Replaces compact_art (100 B/key RAM) + state_store (state.dat) with
 * hash_store (zero RAM, values inline in mmap'd slots).
 * Checkpoint = hash_store_sync (data already on disk).
 * Recovery = hash_store_open (no index.dat rebuild).
 *
 * Opaque handle — struct defined in data_layer.c.
 */

typedef struct data_layer data_layer_t;

typedef struct {
    uint64_t index_keys;      // hash_store count (committed entries)
    uint64_t buffer_entries;   // write buffer size (pending entries)
    uint64_t total_merged;     // lifetime merged entries
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
 * Write a key-value pair to the buffer (memory only).
 * len must be <= hash_store max_value - 1 (1 byte reserved for type prefix).
 */
bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len);

/**
 * Mark a key as deleted in the buffer (tombstone).
 */
bool dl_delete(data_layer_t *dl, const uint8_t *key);

/**
 * Read a key. Checks buffer first, then hash_store.
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
 * Flush write buffer to hash_store. Call after each block.
 * Returns number of entries processed.
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
// Dirty Key Extraction (for MPT commitment — pre-merge)
// ============================================================================

typedef struct {
    uint8_t  **keys;        // sorted keys (owned)
    uint8_t  **values;      // value bytes; NULL for deletes (owned)
    size_t    *value_lens;  // value lengths; 0 for deletes
    size_t     count;
} dl_dirty_set_t;

/**
 * Extract sorted dirty keys + values from write buffer.
 * Must be called BEFORE dl_merge() (merge clears the buffer).
 * Returns false on allocation failure or empty buffer.
 */
bool dl_extract_dirty(data_layer_t *dl, dl_dirty_set_t *out);

/**
 * Free all memory owned by a dirty set.
 */
void dl_dirty_set_free(dl_dirty_set_t *ds);

// ============================================================================
// Diagnostics
// ============================================================================

/**
 * Get current stats snapshot.
 */
dl_stats_t dl_stats(const data_layer_t *dl);

#endif // DATA_LAYER_H
