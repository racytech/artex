#ifndef DATA_LAYER_H
#define DATA_LAYER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Data Layer — Write buffer + compact_art index + state.dat storage.
 *
 * Architecture:
 *   dl_put/dl_delete → write buffer (mem_art, in-memory)
 *   dl_merge         → flush buffer to compact_art index + state.dat
 *   dl_get           → buffer → index → disk (read path)
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
} dl_stats_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new data layer. Truncates state file at path.
 * key_size: fixed key length (e.g., 32 for keccak256 hashes)
 * value_size: compact_art value size (e.g., 4 for uint32_t slot refs)
 * Returns NULL on failure.
 */
data_layer_t *dl_create(const char *state_path,
                         uint32_t key_size, uint32_t value_size);

/**
 * Destroy the data layer and free all resources.
 * Does NOT unlink the state file.
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
// Merge
// ============================================================================

/**
 * Flush write buffer to index + state.dat. Call after each block.
 * Returns number of entries processed.
 * Does NOT fdatasync — caller handles durability at checkpoint time.
 */
uint64_t dl_merge(data_layer_t *dl);

// ============================================================================
// Diagnostics
// ============================================================================

/**
 * Get current stats snapshot.
 */
dl_stats_t dl_stats(const data_layer_t *dl);

#endif // DATA_LAYER_H
