#ifndef FLAT_STORE_H
#define FLAT_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "compact_art.h"

/**
 * ART Store — ART-indexed persistent record store with size classes.
 *
 * Combines compact_art index with a flat data file.
 * Fixed-size keys, variable-length records using size classes.
 *
 * Data file layout:
 *   Header (4096 bytes, page-aligned):
 *     magic[4]:           "FLST"
 *     version[4]:         2
 *     key_size[4]:        fixed key length
 *     max_record_size[4]: maximum record length
 *     num_classes[4]:     number of size classes
 *     live_count[4]:      occupied slots
 *     data_size[8]:       total bytes used in data region
 *     slot_sizes[32]:     up to 8 size classes (uint32_t each)
 *     free_counts[32]:    free list counts per class (uint32_t each)
 *     reserved + free offsets: rest of page
 *
 *   Data region (offset 4096):
 *     Slots packed sequentially, variable size per class:
 *       [4B slot_header][key_size B key][record data][padding to class slot_size]
 *
 *     Slot header (packed uint32_t):
 *       bits 0-2:   class_idx (0-7)
 *       bits 3-13:  data_len (0-2047, actual record bytes; 0 = free)
 *       bits 14-31: reserved
 *
 * In-memory: compact_art (key -> uint64_t byte offset into data region).
 * On open: sequential scan rebuilds ART index + free lists.
 */

typedef struct flat_store flat_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new flat_store, creating/truncating the file at path.
 * key_size:        fixed key length in bytes (e.g., 32).
 * max_record_size: maximum record length in bytes. Determines size classes.
 * Returns NULL on failure.
 */
flat_store_t *flat_store_create(const char *path, uint32_t key_size,
                               uint32_t max_record_size);

/**
 * Open an existing flat_store from file.
 * Reads header, scans all slots, rebuilds ART index + free lists.
 * Returns NULL on failure or corrupt file.
 */
flat_store_t *flat_store_open(const char *path);

/**
 * Close file, destroy ART, free all resources.
 * Does NOT remove the file.
 */
void flat_store_destroy(flat_store_t *store);

/* =========================================================================
 * Operations
 * ========================================================================= */

/**
 * Insert or update a variable-length record.
 * key:        exactly key_size bytes.
 * record:     record_len bytes of payload.
 * record_len: actual record size (must be <= max_record_size).
 * If key exists and fits in current slot: overwrites in place.
 * If key exists but needs a larger class: frees old slot, allocates new.
 * If new: allocates a slot from the smallest fitting class.
 */
bool flat_store_put(flat_store_t *store, const uint8_t *key,
                    const void *record, uint32_t record_len);

/**
 * Look up a record by key.
 * out:      buffer to receive record data.
 * buf_size: size of out buffer.
 * out_len:  receives actual record length (may be NULL).
 * Returns false if key not found.
 */
bool flat_store_get(const flat_store_t *store, const uint8_t *key,
                    void *out, uint32_t buf_size, uint32_t *out_len);

/**
 * Delete a record by key.
 * Returns true if found and deleted, false if not found.
 */
bool flat_store_delete(flat_store_t *store, const uint8_t *key);

/**
 * Check if a key exists. Pure in-memory, zero I/O.
 */
bool flat_store_contains(const flat_store_t *store, const uint8_t *key);

/**
 * Batch insert/update with variable-length records.
 * keys:        packed array of key_size-byte keys.
 * records:     packed array of record data (each record_lens[i] bytes, concatenated).
 * record_lens: array of record lengths.
 * count:       number of entries.
 */
bool flat_store_batch_put(flat_store_t *store, const uint8_t *keys,
                           const void *records, const uint32_t *record_lens,
                           uint32_t count);

/* =========================================================================
 * Stats
 * ========================================================================= */

/** Number of occupied records. */
uint32_t flat_store_count(const flat_store_t *store);

/** Key size in bytes. */
uint32_t flat_store_key_size(const flat_store_t *store);

/** Maximum record size in bytes. */
uint32_t flat_store_max_record_size(const flat_store_t *store);

/* =========================================================================
 * Internal Access (for art_mpt integration)
 * ========================================================================= */

/** Get the compact_art index (non-owning pointer). */
compact_art_t *flat_store_get_art(flat_store_t *store);

/**
 * Read the raw record for a compact_art leaf value.
 * leaf_val: pointer to the leaf value in the ART (uint64_t offset).
 * buf/buf_size: output buffer.
 * Returns actual record length, or 0 on error.
 */
uint32_t flat_store_read_leaf_record(const flat_store_t *store,
                                      const void *leaf_val,
                                      uint8_t *buf, uint32_t buf_size);

/* =========================================================================
 * Deferred Buffer
 * ========================================================================= */

/**
 * Flush deferred (in-memory) writes to the data file.
 * Updates compact_art leaves to point to file offsets.
 * Call at checkpoint/evict time.
 */
void flat_store_flush_deferred(flat_store_t *store);

/** Discard all deferred writes without flushing. */
void flat_store_clear_deferred(flat_store_t *store);

/* =========================================================================
 * Durability
 * ========================================================================= */

/**
 * Flush deferred buffer + write header.
 */
void flat_store_sync(flat_store_t *store);

#endif /* FLAT_STORE_H */
