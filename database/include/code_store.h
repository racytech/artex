#ifndef CODE_STORE_H
#define CODE_STORE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Code Store — Content-Addressed, Append-Only, Deduplicated.
 *
 * Stores Ethereum contract bytecode keyed by code hash (32 bytes).
 * Backed by two files:
 *   - Index:  disk_hash mapping code_hash (32B) → {offset, length} (12B)
 *   - Data:   append-only flat file of raw code bytes
 *
 * Properties:
 *   - Content-addressed: same code → same hash → stored once (free dedup)
 *   - Write-once: code is immutable after deployment, never updated/deleted
 *   - Variable-length: codes range from 0 to ~24KB
 *   - Thread-safe: reads are lock-free (pread), writes serialized by mutex
 *
 * File layout (data file):
 *   [Header: 4096 bytes]
 *     magic[4] "CDST" | version[4] 1 | data_size[8] | reserved[4080]
 *   [Code region: offset 4096+]
 *     raw bytes, appended sequentially, no framing
 *
 * Crash safety:
 *   - Data written before index entry (orphaned bytes on crash = harmless)
 *   - Index inherits disk_hash crash safety (dirty flag + recovery)
 */

typedef struct code_store code_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new code store.
 * Creates two files: <path>.idx (disk_hash index) and <path>.dat (data).
 * capacity_hint: expected number of unique codes (sizes disk_hash).
 * Returns NULL on failure.
 */
code_store_t *code_store_create(const char *path, uint64_t capacity_hint);

/**
 * Open an existing code store from <path>.idx and <path>.dat.
 * Returns NULL on failure or corrupt files.
 */
code_store_t *code_store_open(const char *path);

/**
 * Close files, free struct. Does NOT remove files.
 */
void code_store_destroy(code_store_t *cs);

/* =========================================================================
 * Operations
 * ========================================================================= */

/**
 * Store code bytes keyed by code_hash.
 * If code_hash already exists, this is a no-op (deduplication).
 * code_len may be 0 (empty code).
 * Returns true on success.
 */
bool code_store_put(code_store_t *cs, const uint8_t code_hash[32],
                    const uint8_t *code, uint32_t code_len);

/**
 * Retrieve code by hash.
 * buf: caller-provided buffer of buf_len bytes.
 * Returns actual code length.
 *   - If code not found: returns 0.
 *   - If buf_len < code length: returns required size, buf untouched.
 *   - Otherwise: copies code into buf, returns code length.
 */
uint32_t code_store_get(const code_store_t *cs, const uint8_t code_hash[32],
                        uint8_t *buf, uint32_t buf_len);

/**
 * Check if code_hash exists in the store.
 */
bool code_store_contains(const code_store_t *cs, const uint8_t code_hash[32]);

/**
 * Get code length without reading data. Returns 0 if not found.
 */
uint32_t code_store_get_size(const code_store_t *cs, const uint8_t code_hash[32]);

/* =========================================================================
 * Durability
 * ========================================================================= */

/**
 * Sync both index and data file to disk.
 */
void code_store_sync(code_store_t *cs);

/* =========================================================================
 * Stats
 * ========================================================================= */

/**
 * Number of unique codes stored.
 */
uint64_t code_store_count(const code_store_t *cs);

#endif /* CODE_STORE_H */
