#ifndef STORAGE_FILE_H
#define STORAGE_FILE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/**
 * Packed Storage File — per-account storage persistence.
 *
 * Stores storage slots as packed sections, one per account:
 *   [slot_hash[32] + value_be[32]] × slot_count
 *
 * Each account's section is identified by an offset + slot_count stored
 * in the account record. The file is append-only during a checkpoint
 * window and compacted on eviction (old sections become dead space,
 * reclaimed by rewriting).
 *
 * File format:
 *   Header: "STOR" magic (4) + version (4) + reserved (8) = 16 bytes
 *   Data:   packed sections, each = slot_count * 64 bytes
 *
 * Thread safety: NONE. Single-threaded access only.
 */

#define STORAGE_SLOT_SIZE 64  /* slot_hash[32] + value_be[32] */

typedef struct storage_file storage_file_t;

/** Create or open a storage file at the given path. */
storage_file_t *storage_file_create(const char *path);

/** Close and free. */
void storage_file_destroy(storage_file_t *sf);

/**
 * Write a packed section for an account's storage.
 * slots: array of [slot_hash[32] + value_be[32]] pairs.
 * slot_count: number of pairs.
 * Returns the file offset where the section was written, or UINT64_MAX on error.
 */
uint64_t storage_file_write_section(storage_file_t *sf,
                                     const uint8_t *slots,
                                     uint32_t slot_count);

/**
 * Read a packed section into a caller-provided buffer.
 * offset: file offset returned by write_section.
 * slot_count: number of pairs to read.
 * out: buffer of at least slot_count * 64 bytes.
 * Returns true on success.
 */
bool storage_file_read_section(const storage_file_t *sf,
                                uint64_t offset, uint32_t slot_count,
                                uint8_t *out);

/**
 * Reset the file — truncate to header only.
 * Called after compaction or on clean start.
 */
void storage_file_reset(storage_file_t *sf);

/** Current file size in bytes. */
uint64_t storage_file_size(const storage_file_t *sf);

#endif /* STORAGE_FILE_H */
