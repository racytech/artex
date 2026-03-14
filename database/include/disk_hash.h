#ifndef DISK_HASH_H
#define DISK_HASH_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Disk Hash — Zero-RAM disk-backed hash table.
 *
 * Bucket-based open hashing with pread/pwrite. Only ~48 bytes of
 * metadata kept in memory; all data lives on disk. Supports batch
 * get/put with internal sorting by bucket for sequential I/O.
 *
 * Data file layout:
 *   Header (4096 bytes — page-aligned):
 *     magic[4]:            "DHSH"
 *     version[4]:          1
 *     key_size[4]:         fixed key length
 *     record_size[4]:      fixed record length
 *     bucket_count[8]:     number of primary buckets
 *     entry_count[8]:      live entries
 *     overflow_count[8]:   overflow buckets allocated
 *     slots_per_bucket[4]: computed: (4096 - 8) / slot_size
 *     reserved[4020]:      zeroed
 *
 *   Bucket array (offset 4096, each bucket 4096 bytes):
 *     bucket_header[8]:    count[2] + tombstone_count[2] + overflow_id[4]
 *     slots[]:             each = [1B flags][key_size B key][record_size B data]
 *     Slot flags: 0x00 = empty, 0x01 = occupied, 0x02 = tombstone
 *
 *   Overflow buckets appended after primary region (same layout).
 *
 * Hash: MurmurHash3 64-bit finalizer on first 8 bytes of key.
 * Capacity: bucket_count = capacity_hint / (slots_per_bucket * 0.75).
 * File created sparse via ftruncate (only written pages consume disk).
 */

typedef struct disk_hash disk_hash_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new disk_hash, creating/truncating the file at path.
 * key_size:      fixed key length in bytes (e.g. 32).
 * record_size:   fixed record length in bytes.
 * capacity_hint: expected number of entries (determines bucket count).
 * Returns NULL on failure.
 */
disk_hash_t *disk_hash_create(const char *path, uint32_t key_size,
                               uint32_t record_size, uint64_t capacity_hint);

/**
 * Open an existing disk_hash from file.
 * Reads and validates header. No scan needed (zero RAM index).
 * Returns NULL on failure or corrupt file.
 */
disk_hash_t *disk_hash_open(const char *path);

/**
 * Close file descriptor, free the struct.
 * Does NOT remove the file.
 */
void disk_hash_destroy(disk_hash_t *dh);

/**
 * Clear all entries in-place without file recreation.
 * Zeros all buckets, resets entry/overflow counts, rewrites header.
 * Much faster than destroy + create for repeated test use.
 */
void disk_hash_clear(disk_hash_t *dh);

/* =========================================================================
 * Single Operations
 * ========================================================================= */

/**
 * Insert or update a record.
 * key:    exactly key_size bytes.
 * record: exactly record_size bytes.
 * If key exists: overwrites the record in place.
 * If new: inserts into an empty or tombstoned slot. Allocates overflow
 * bucket if the primary bucket (and its chain) are full.
 */
bool disk_hash_put(disk_hash_t *dh, const uint8_t *key, const void *record);

/**
 * Look up a record by key.
 * out: buffer of at least record_size bytes.
 * Returns false if key not found.
 */
bool disk_hash_get(const disk_hash_t *dh, const uint8_t *key, void *out);

/**
 * Delete a record by key (sets tombstone).
 * Returns true if found and deleted, false if not found.
 */
bool disk_hash_delete(disk_hash_t *dh, const uint8_t *key);

/**
 * Check if a key exists. Same I/O as get, but no record copy.
 */
bool disk_hash_contains(const disk_hash_t *dh, const uint8_t *key);

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

/**
 * Batch lookup. keys and records are flat contiguous arrays:
 *   keys[i]    at offset i * key_size
 *   records[i] at offset i * record_size
 * found[i] is set to true/false for each key.
 * Internally sorts by bucket for sequential disk I/O.
 * Returns number of keys found.
 */
uint32_t disk_hash_batch_get(const disk_hash_t *dh,
                              const uint8_t *keys, void *records,
                              bool *found, uint32_t count);

/**
 * Batch insert/update. keys and records are flat contiguous arrays.
 * Internally sorts by bucket for sequential disk I/O.
 * Returns true on success.
 */
bool disk_hash_batch_put(disk_hash_t *dh,
                          const uint8_t *keys, const void *records,
                          uint32_t count);

/* =========================================================================
 * Stats
 * ========================================================================= */

/** Number of live entries. */
uint64_t disk_hash_count(const disk_hash_t *dh);

/** Total capacity (bucket_count * slots_per_bucket). */
uint64_t disk_hash_capacity(const disk_hash_t *dh);

/* =========================================================================
 * Durability
 * ========================================================================= */

/**
 * Write header (entry_count, overflow_count) and fsync the file.
 */
void disk_hash_sync(disk_hash_t *dh);

#endif /* DISK_HASH_H */
