#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Data Layer — Write buffer + compact_art index + state.dat + code.dat.
 *
 * Architecture:
 *   dl_put/dl_delete  → write buffer (mem_art, in-memory)
 *   dl_merge          → flush buffer to compact_art index + state.dat
 *   dl_get            → buffer → index → disk (read path)
 *   dl_put_code       → direct to code.dat + index (bypasses buffer)
 *   dl_get_code       → index → code.dat (read path)
 *
 * Ref encoding in compact_art (4-byte value):
 *   bit 31 = 0 → state_store slot index
 *   bit 31 = 1 → code_store entry index
 *
 * Merge is cheap (~17ms at 100M keys): memory ops + pwrite to page cache.
 * No fdatasync — that's a checkpoint concern (Stage 3).
 *
 * Opaque handle — struct defined in data_layer.c.
 */

typedef struct data_layer data_layer_t;

typedef struct {
    uint64_t index_keys;      // compact_art size (committed entries)
    uint64_t buffer_entries;   // write buffer size (pending entries)
    uint64_t total_merged;     // lifetime merged entries
    uint32_t free_slots;       // state_store free list size
    uint32_t code_count;       // code_store entries
    uint64_t code_file_size;   // code.dat file size in bytes
} dl_stats_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new data layer. Truncates state and code files at paths.
 * key_size: fixed key length (e.g., 32 for keccak256 hashes)
 * value_size: compact_art value size (e.g., 4 for uint32_t slot refs)
 * code_path: path for code.dat (NULL to disable code store)
 * Returns NULL on failure.
 */
data_layer_t *dl_create(const char *state_path, const char *code_path,
                         uint32_t key_size, uint32_t value_size);

/**
 * Destroy the data layer and free all resources.
 * Does NOT unlink the state/code files.
 */
void dl_destroy(data_layer_t *dl);

// ============================================================================
// Per-block Operations
// ============================================================================

/**
 * Write a key-value pair to the buffer (memory only).
 * len must be <= 62 bytes (STATE_STORE_MAX_VALUE).
 */
bool dl_put(data_layer_t *dl, const uint8_t *key,
            const void *value, uint16_t len);

/**
 * Mark a key as deleted in the buffer (tombstone).
 */
bool dl_delete(data_layer_t *dl, const uint8_t *key);

/**
 * Read a key. Checks buffer first, then index → disk.
 * Returns false if not found or deleted.
 */
bool dl_get(data_layer_t *dl, const uint8_t *key,
            void *out_value, uint16_t *out_len);

// ============================================================================
// Code Operations (bypass write buffer — direct to code.dat + index)
// ============================================================================

/**
 * Store contract bytecode. Content-addressed: if key already exists in index,
 * this is a no-op (dedup). Writes directly to code.dat, bypasses buffer.
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
 * Flush write buffer to index + state.dat. Call after each block.
 * Returns number of entries processed.
 * Does NOT fdatasync — caller handles durability at checkpoint time.
 */
uint64_t dl_merge(data_layer_t *dl);

// ============================================================================
// Checkpoint / Recovery
// ============================================================================

/**
 * Checkpoint current state to index file (atomic rename).
 * Serializes compact_art index + state_store free list + code_store entries.
 * Returns false on I/O error.
 */
bool dl_checkpoint(data_layer_t *dl, const char *index_path,
                   uint64_t block_number);

/**
 * Open from existing checkpoint (recovery path).
 * Rebuilds compact_art from index.dat, restores free list and code entries.
 * out_block_number receives the checkpoint block number.
 * Returns NULL on failure.
 */
data_layer_t *dl_open(const char *state_path, const char *code_path,
                       const char *index_path,
                       uint32_t key_size, uint32_t value_size,
                       uint64_t *out_block_number);

// ============================================================================
// Cursor (for MPT commitment — ih_cursor_t adapter)
// ============================================================================

#include "intermediate_hashes.h"

typedef struct dl_cursor dl_cursor_t;

/**
 * Create a cursor over the committed index (post-merge state).
 * Returns actual values (not slot refs) via state_store reads.
 * Skips code refs (bit 31 set). Caller must destroy when done.
 */
dl_cursor_t *dl_cursor_create(data_layer_t *dl);
void         dl_cursor_destroy(dl_cursor_t *cursor);

/**
 * Get an ih_cursor_t interface backed by this cursor.
 * The returned struct is valid for the lifetime of the dl_cursor.
 */
ih_cursor_t  dl_cursor_as_ih(dl_cursor_t *cursor);

// ============================================================================
// Dirty Key Extraction (for MPT commitment — pre-merge)
// ============================================================================

typedef struct {
    uint8_t  **keys;        // sorted 32-byte keys (owned)
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
