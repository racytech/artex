#ifndef DISK_TABLE_H
#define DISK_TABLE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Disk Table — Memory-mapped hash table optimized for pre-hashed keys.
 *
 * Designed for keccak256 / Pedersen keys where the first 8 bytes are
 * already uniformly distributed. No hash function — raw key bytes are
 * used directly as the bucket index via bitmask (power-of-2 buckets).
 *
 * Data file layout:
 *   Header (4096 bytes — page-aligned):
 *     magic[4]:            "DTBL"
 *     version[4]:          1
 *     key_size[4]:         fixed key length (>= 8)
 *     record_size[4]:      fixed record length
 *     bucket_count[8]:     number of primary buckets (power of 2)
 *     bucket_mask[8]:      bucket_count - 1
 *     entry_count[8]:      live entries
 *     overflow_count[8]:   overflow buckets allocated
 *     slots_per_bucket[4]: computed: (4096 - 16) / slot_size
 *     dirty[1]:            crash recovery flag
 *     reserved[11]:        zeroed
 *
 *   Bucket array (offset 4096, each bucket 4096 bytes):
 *     bucket_header[16]:   count[2] + tombstone_count[2] + pad[4] + overflow_id[8]
 *     slots[]:             each = [1B flags][1B fingerprint][key][record]
 *
 *   Slot flags: 0x00 = empty, 0x01 = occupied, 0x02 = tombstone
 *   Fingerprint: key[8] — cheap pre-filter before full memcmp.
 *
 * Differences from disk_hash:
 *   - No hash function: keys are pre-hashed, first 8 bytes used directly
 *   - Power-of-2 bucket count: bitmask instead of modulo (~35 cycles saved)
 *   - Fingerprint byte: skip most 32-byte memcmps on scan
 *   - 64-bit overflow_id: no truncation at >4B buckets
 *   - madvise hints on batch operations
 */

typedef struct disk_table disk_table_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

disk_table_t *disk_table_create(const char *path, uint32_t key_size,
                                uint32_t record_size, uint64_t capacity_hint);

disk_table_t *disk_table_open(const char *path);

void disk_table_destroy(disk_table_t *dt);

void disk_table_clear(disk_table_t *dt);

/* =========================================================================
 * Single Operations
 * ========================================================================= */

bool disk_table_put(disk_table_t *dt, const uint8_t *key, const void *record);

bool disk_table_get(const disk_table_t *dt, const uint8_t *key, void *out);

bool disk_table_delete(disk_table_t *dt, const uint8_t *key);

bool disk_table_contains(const disk_table_t *dt, const uint8_t *key);

/* =========================================================================
 * Batch Operations
 * ========================================================================= */

uint32_t disk_table_batch_get(const disk_table_t *dt,
                              const uint8_t *keys, void *records,
                              bool *found, uint32_t count);

bool disk_table_batch_put(disk_table_t *dt,
                          const uint8_t *keys, const void *records,
                          uint32_t count);

/* =========================================================================
 * Stats
 * ========================================================================= */

uint64_t disk_table_count(const disk_table_t *dt);

uint64_t disk_table_capacity(const disk_table_t *dt);

/* =========================================================================
 * Iteration
 * ========================================================================= */

typedef void (*disk_table_key_cb_t)(const uint8_t *key, void *user_data);
void disk_table_foreach_key(const disk_table_t *dt, disk_table_key_cb_t cb,
                            void *user_data);

/* =========================================================================
 * Prefetch
 * ========================================================================= */

/** Prefetch the bucket page for a key into OS page cache (non-blocking). */
void disk_table_prefetch(const disk_table_t *dt, const uint8_t *key);

/* =========================================================================
 * Durability
 * ========================================================================= */

void disk_table_sync(disk_table_t *dt);

#endif /* DISK_TABLE_H */
