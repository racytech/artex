#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Bloom Filter — probabilistic set membership test.
 *
 * Uses double hashing (h1 + i*h2) from MurmurHash3 64-bit finalizer.
 * False positives possible; false negatives impossible.
 *
 * Sizing guide (k=7 hash functions):
 *   capacity     bits        RAM
 *   1M nodes     ~10M       ~1.2 MB
 *   10M nodes    ~96M       ~12 MB
 *   50M nodes    ~480M      ~57 MB
 *
 * Not thread-safe — caller must synchronize.
 */

typedef struct bloom_filter bloom_filter_t;

/**
 * Create a bloom filter sized for `capacity` elements with target
 * false positive rate `fpr` (e.g. 0.01 for 1%).
 * Returns NULL on failure.
 */
bloom_filter_t *bloom_filter_create(uint64_t capacity, double fpr);

/** Free the bloom filter. */
void bloom_filter_destroy(bloom_filter_t *bf);

/**
 * Add a key to the filter.
 * key_len: number of bytes in key.
 */
void bloom_filter_add(bloom_filter_t *bf, const uint8_t *key, size_t key_len);

/**
 * Test if a key might be in the set.
 * Returns false  → definitely not in set.
 * Returns true   → possibly in set (check disk_table to confirm).
 */
bool bloom_filter_maybe_contains(const bloom_filter_t *bf,
                                  const uint8_t *key, size_t key_len);

/** Reset all bits to zero (keeps allocated memory). */
void bloom_filter_clear(bloom_filter_t *bf);

/** Number of bits set (for diagnostics). */
uint64_t bloom_filter_popcount(const bloom_filter_t *bf);

/** Total memory used in bytes (bit array + struct). */
size_t bloom_filter_memory(const bloom_filter_t *bf);

#endif /* BLOOM_FILTER_H */
