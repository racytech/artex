#ifndef CHECKPOINT_H
#define CHECKPOINT_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "compact_art.h"
#include "state_store.h"
#include "code_store.h"

/**
 * Checkpoint — Serialize/restore compact_art index + state_store free list
 * + code_store entries to/from a single index.dat file.
 *
 * Format:
 *   [4096B header]
 *   [num_entries x (key_size + value_size) bytes: sorted key+ref pairs]
 *   [free_count x 4B: free list slots]
 *   [code_count x 12B: code entries {offset(8B), length(4B)}]
 *
 * Write procedure: temp file -> fdatasync -> atomic rename.
 * CRC32 covers everything after the header.
 *
 * Internal module — public API is on data_layer (dl_checkpoint, dl_open).
 */

#define CHECKPOINT_MAGIC    "ARTIDX01"
#define CHECKPOINT_VERSION  1
#define CHECKPOINT_HEADER_SIZE 4096

typedef struct __attribute__((packed)) {
    char     magic[8];          // "ARTIDX01"
    uint32_t version;           // 1
    uint64_t block_number;
    uint64_t num_entries;       // compact_art size
    uint32_t key_size;          // 32
    uint32_t value_size;        // 4
    uint32_t next_slot;         // state_store allocator position
    uint32_t free_count;        // state_store free list size
    uint32_t code_count;        // code_store entry count
    uint64_t code_file_size;    // code.dat file size
    uint32_t checksum;          // CRC32 of everything after header
    uint8_t  _pad[CHECKPOINT_HEADER_SIZE - 8 - 4 - 8 - 8 - 4 - 4 - 4 - 4 - 4 - 8 - 4];
} checkpoint_header_t;

// ============================================================================
// CRC32
// ============================================================================

uint32_t crc32_update(uint32_t crc, const void *data, size_t len);

// ============================================================================
// Write / Load
// ============================================================================

/**
 * Write checkpoint: iterate compact_art, serialize to temp file, atomic rename.
 * code may be NULL if code store is not configured.
 */
bool checkpoint_write(const char *path, uint64_t block_number,
                      const compact_art_t *index,
                      const state_store_t *store,
                      const code_store_t *code);

/**
 * Load checkpoint: rebuild compact_art, restore free list + code entries.
 * code may be NULL if code store is not configured.
 * Returns false on failure (bad magic, CRC mismatch, I/O error).
 */
bool checkpoint_load(const char *path, uint64_t *out_block_number,
                     compact_art_t *index,
                     state_store_t *store,
                     code_store_t *code);

// ============================================================================
// Slim Meta Sidecar (free list + code entries only, no key-value pairs)
// ============================================================================

#define CHECKPOINT_META_MAGIC    "ARTMET01"
#define CHECKPOINT_META_VERSION  1

typedef struct __attribute__((packed)) {
    char     magic[8];          // "ARTMET01"
    uint32_t version;           // 1
    uint64_t block_number;
    uint32_t next_slot;         // state_store allocator position
    uint32_t free_count;        // state_store free list size
    uint32_t code_count;        // code_store entry count
    uint64_t code_file_size;    // code.dat file size
    uint32_t checksum;          // CRC32 of everything after header
    uint8_t  _pad[CHECKPOINT_HEADER_SIZE - 8 - 4 - 8 - 4 - 4 - 4 - 8 - 4];
} checkpoint_meta_header_t;

/**
 * Write slim meta sidecar: state_store free list + code_store entries.
 * No key-value pairs (those are persisted in nibble_trie).
 * code may be NULL if code store is not configured.
 */
bool checkpoint_meta_write(const char *path, uint64_t block_number,
                           const state_store_t *store,
                           const code_store_t *code);

/**
 * Load slim meta sidecar: restore free list + code entries.
 * code may be NULL if code store is not configured.
 */
bool checkpoint_meta_load(const char *path, uint64_t *out_block_number,
                          state_store_t *store,
                          code_store_t *code);

#endif // CHECKPOINT_H
