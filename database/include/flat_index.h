#ifndef FLAT_INDEX_H
#define FLAT_INDEX_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/**
 * Flat Index — Compact open-addressing hash table.
 *
 * Maps fixed-size keys (keccak256 hashes) to uint32_t slot IDs.
 * 12 bytes per slot: 8-byte tag + 4-byte offset.
 *
 * Keys must be >= 16 bytes (first 8 bytes = bucket, next 8 = tag).
 * Linear probing, power-of-2 capacity, auto-grow at 70% load.
 *
 * Rehash requires caller to provide keys (read from data file).
 */

typedef struct {
    uint64_t *tags;      /* verification tag (key[8..15]), 0 = empty */
    uint32_t *offsets;   /* slot_id + 1 (0 = empty) */
    uint32_t  capacity;  /* power of 2 */
    uint32_t  mask;      /* capacity - 1 */
    uint32_t  count;     /* live entries */
} flat_index_t;

#define FLAT_INDEX_INITIAL_CAP 1024
#define FLAT_INDEX_LOAD_PCT    70

bool flat_index_init(flat_index_t *idx, uint32_t capacity);
void flat_index_destroy(flat_index_t *idx);

/**
 * Look up a key. Returns pointer to stored offset+1, or NULL if not found.
 * Caller must subtract 1 to get the actual slot_id.
 */
const uint32_t *flat_index_get(const flat_index_t *idx, const uint8_t *key);

/**
 * Insert or update. Returns true on success.
 * If load factor exceeded, returns false — caller must grow and re-insert.
 */
bool flat_index_put(flat_index_t *idx, const uint8_t *key, uint32_t slot_id);

/** Delete a key. Returns true if found and deleted. */
bool flat_index_delete(flat_index_t *idx, const uint8_t *key);

/** Check if key exists (no I/O). */
bool flat_index_contains(const flat_index_t *idx, const uint8_t *key);

/** Check if index needs growing (>70% load). */
bool flat_index_needs_grow(const flat_index_t *idx);

/**
 * Grow to new_capacity. Caller must re-insert all entries after this
 * (the old entries are discarded). Returns true on success.
 */
bool flat_index_grow(flat_index_t *idx, uint32_t new_capacity);

#endif /* FLAT_INDEX_H */
