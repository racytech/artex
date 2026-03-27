#ifndef FLAT_STORE_H
#define FLAT_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * ART Store — ART-indexed persistent record store.
 *
 * Combines flat_index) with a flat data file.
 * Fixed-size keys and fixed-size records.
 *
 * Data file layout:
 *   Header (64 bytes):
 *     magic[4]:       "FLST"
 *     version[4]:     1
 *     key_size[4]:    fixed key length
 *     record_size[4]: fixed record length
 *     slot_count[4]:  total allocated slots
 *     live_count[4]:  occupied slots
 *     reserved[40]
 *
 *   Slot array (offset 64):
 *     slot[i] = [1B flags][key_size B key][record_size B data]
 *     flags: 0x00 = free, 0x01 = occupied
 *     slot_size = 1 + key_size + record_size
 *
 * In-memory: flat_index).
 * On open: sequential scan rebuilds ART index + free list.
 */

typedef struct flat_store flat_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new flat_store, creating/truncating the file at path.
 * key_size:    fixed key length in bytes (e.g., 31 for stems, 32 for keys).
 * record_size: fixed record length in bytes (user payload).
 * Returns NULL on failure.
 */
flat_store_t *flat_store_create(const char *path, uint32_t key_size,
                               uint32_t record_size);

/**
 * Open an existing flat_store from file.
 * Reads header, scans all slots, rebuilds ART index + free list.
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
 * Insert or update a record.
 * key:    exactly key_size bytes.
 * record: exactly record_size bytes.
 * If key exists: overwrites the record data in place.
 * If new: allocates a slot (free list or append), writes key+data.
 */
bool flat_store_put(flat_store_t *store, const uint8_t *key,
                    const void *record);

/**
 * Look up a record by key.
 * out: buffer of at least record_size bytes.
 * Returns false if key not found.
 */
bool flat_store_get(const flat_store_t *store, const uint8_t *key,
                    void *out);

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
 * Batch insert/update, sorted by slot offset for sequential page access.
 * keys: packed array of key_size-byte keys.
 * records: packed array of record_size-byte records.
 */
bool flat_store_batch_put(flat_store_t *store, const uint8_t *keys,
                           const void *records, uint32_t count);

/* =========================================================================
 * Stats
 * ========================================================================= */

/** Number of occupied records. */
uint32_t flat_store_count(const flat_store_t *store);

/** Total allocated slots (occupied + free). */
uint32_t flat_store_slot_count(const flat_store_t *store);

/** Key size in bytes. */
uint32_t flat_store_key_size(const flat_store_t *store);

/** Record size in bytes. */
uint32_t flat_store_record_size(const flat_store_t *store);

/* =========================================================================
 * Durability
 * ========================================================================= */

/**
 * Write header and fsync the data file.
 */
void flat_store_sync(flat_store_t *store);

#endif /* FLAT_STORE_H */
