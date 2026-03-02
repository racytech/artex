#ifndef HASH_STORE_H
#define HASH_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Hash Store — Zero-RAM file-backed hash table with configurable key/slot sizes.
 *
 * Keys are keccak256 hashes (uniformly distributed), so the first 8 bytes
 * of the key serve as both the hash and the fingerprint.
 *
 * Slot layout (slot_size bytes):
 *   [8B fingerprint][1B flags][1B value_len][key_size B key][value][padding]
 *   max_value = slot_size - 10 - key_size
 *
 * Full key stored in each slot for exact match — no silent collisions.
 *
 * Configurations:
 *   Accounts:  key_size=32, slot_size=128 (max_value=86, fits 75B type+account)
 *   Storage:   key_size=64, slot_size=128 (max_value=54, fits 33B type+value)
 *
 * Sharded architecture (extendible hashing):
 *   - Multiple fixed-size shard files instead of one growing file.
 *   - Top K bits of fingerprint route to a shard via a directory.
 *   - Bottom bits address within the shard (linear probing).
 *   - When a shard hits 75% load, split ONLY that shard.
 *   - No full-table resize — only the full shard is rehashed.
 *
 * File layout:
 *   {dir}/meta.dat          — directory + shard metadata
 *   {dir}/shard_NNNN.dat    — individual shard files (mmap'd)
 */

#define HASH_STORE_HEADER_SIZE    64
#define HASH_STORE_FINGERPRINT    8

#define HASH_STORE_FLAG_EMPTY     0x00
#define HASH_STORE_FLAG_OCCUPIED  0x01
#define HASH_STORE_FLAG_TOMBSTONE 0x02

typedef struct hash_store hash_store_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new sharded hash store in the given directory.
 * Directory is created if it does not exist.
 * shard_capacity: fixed number of slots per shard (power of 2).
 * key_size: fixed key length in bytes (must be >= 8).
 * slot_size: bytes per slot (must be >= 10 + key_size + 1; max_value = slot_size - 10 - key_size).
 * Returns NULL on failure.
 */
hash_store_t *hash_store_create(const char *dir, uint64_t shard_capacity,
                                 uint32_t key_size, uint32_t slot_size);

/**
 * Open an existing sharded hash store from directory.
 * Reads metadata (including key_size/slot_size) and opens all shard files.
 * Returns NULL on failure or invalid metadata.
 */
hash_store_t *hash_store_open(const char *dir);

/**
 * Close all file descriptors and free resources. Does NOT remove files.
 */
void hash_store_destroy(hash_store_t *hs);

// ============================================================================
// Operations
// ============================================================================

/**
 * Insert or update a key-value pair.
 * key must be exactly key_size bytes.
 * len must be <= max_value (slot_size - 10 - key_size).
 * May trigger a shard split if load factor exceeds 75%.
 * Returns false on I/O error or split failure.
 */
bool hash_store_put(hash_store_t *hs, const uint8_t *key,
                    const void *value, uint8_t len);

/**
 * Look up a key. out_len receives the stored value length.
 * Returns false if not found.
 */
bool hash_store_get(const hash_store_t *hs, const uint8_t *key,
                    void *out_value, uint8_t *out_len);

/**
 * Check if a key exists.
 */
bool hash_store_contains(const hash_store_t *hs, const uint8_t *key);

/**
 * Delete a key (sets tombstone). Returns true if found and deleted.
 */
bool hash_store_delete(hash_store_t *hs, const uint8_t *key);

// ============================================================================
// Stats
// ============================================================================

/** Number of occupied entries across all shards. */
uint64_t hash_store_count(const hash_store_t *hs);

/** Total slot capacity across all shards. */
uint64_t hash_store_capacity(const hash_store_t *hs);

/** Number of tombstone slots across all shards. */
uint64_t hash_store_tombstones(const hash_store_t *hs);

/** Number of shard files. */
uint32_t hash_store_num_shards(const hash_store_t *hs);

/** Global depth of the extendible hashing directory. */
uint32_t hash_store_global_depth(const hash_store_t *hs);

/** Key size in bytes. */
uint32_t hash_store_key_size(const hash_store_t *hs);

/** Slot size in bytes. */
uint32_t hash_store_slot_size(const hash_store_t *hs);

/** Maximum value length (slot_size - 10 - key_size). */
uint32_t hash_store_max_value(const hash_store_t *hs);

// ============================================================================
// Durability
// ============================================================================

/**
 * Flush all shard headers + metadata to disk.
 */
void hash_store_sync(hash_store_t *hs);

#endif // HASH_STORE_H
