#ifndef BLOCK_STORE_H
#define BLOCK_STORE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Block Store — Lightweight on-disk block index.
 *
 * Append-only flat file storing per-block metadata for:
 *   - BLOCKHASH opcode (lookup by block number)
 *   - Restart recovery (rebuild 256-block ring buffer)
 *   - Reorg ancestor search (walk parent_hash chain)
 *
 * Each record is 80 bytes:
 *   block_number  [8]
 *   block_hash    [32]
 *   parent_hash   [32]
 *   timestamp     [8]
 *
 * File layout:
 *   [header: 64 bytes]  magic, version, count, padding
 *   [record 0]
 *   [record 1]
 *   ...
 *
 * Memory-mapped for zero-copy reads. Writes go through pwrite + remap.
 * Indexed by block_number for O(1) lookup (records stored in order).
 */

#define BLOCK_STORE_RECORD_SIZE 80

typedef struct block_store block_store_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create a new block store at the given path, truncating any existing file.
 * Returns NULL on failure.
 */
block_store_t *block_store_create(const char *path);

/**
 * Open an existing block store. Returns NULL if file doesn't exist or is
 * corrupt. The file is memory-mapped for fast reads.
 */
block_store_t *block_store_open(const char *path);

/**
 * Close the block store, unmap memory, close file descriptor.
 */
void block_store_destroy(block_store_t *bs);

/* =========================================================================
 * Write
 * ========================================================================= */

/**
 * Append a block record. Records must be appended in block_number order.
 * Returns false on I/O error or out-of-order insert.
 */
bool block_store_put(block_store_t *bs,
                      uint64_t block_number,
                      const uint8_t block_hash[32],
                      const uint8_t parent_hash[32],
                      uint64_t timestamp);

/**
 * Sync the file to disk (fdatasync). Call at checkpoint time.
 */
void block_store_sync(block_store_t *bs);

/* =========================================================================
 * Read
 * ========================================================================= */

/**
 * Look up a block hash by block number.
 * Returns true if found, copies hash to out.
 * O(1) — direct offset calculation.
 */
bool block_store_get_hash(const block_store_t *bs,
                           uint64_t block_number,
                           uint8_t out[32]);

/**
 * Look up a parent hash by block number.
 * Returns true if found, copies parent_hash to out.
 */
bool block_store_get_parent(const block_store_t *bs,
                             uint64_t block_number,
                             uint8_t out[32]);

/**
 * Look up a block's timestamp by block number.
 * Returns 0 if not found.
 */
uint64_t block_store_get_timestamp(const block_store_t *bs,
                                    uint64_t block_number);

/**
 * Get the number of records stored.
 */
uint64_t block_store_count(const block_store_t *bs);

/**
 * Get the highest block number stored. Returns 0 if empty.
 */
uint64_t block_store_highest(const block_store_t *bs);

/**
 * Get the lowest block number stored. Returns UINT64_MAX if empty.
 */
uint64_t block_store_lowest(const block_store_t *bs);

/**
 * Populate a 256-entry block hash ring buffer from the last N blocks.
 * Fills hashes[block_number % 256] for each block in range.
 * Used to rebuild the BLOCKHASH ring buffer on restart.
 *
 * @param head_block  The current head block number
 * @param hashes      Output array of 256 hashes (caller-allocated)
 * @return            Number of entries populated (up to 256)
 */
uint32_t block_store_fill_ring(const block_store_t *bs,
                                uint64_t head_block,
                                uint8_t hashes[256][32]);

/**
 * Truncate all records above the given block number.
 * Used for reorg: discard blocks on the old fork.
 * Returns true on success.
 */
bool block_store_truncate(block_store_t *bs, uint64_t keep_up_to);

#ifdef __cplusplus
}
#endif

#endif /* BLOCK_STORE_H */
