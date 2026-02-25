/**
 * Database Backup - Online Snapshot Export/Import
 *
 * Exports a consistent snapshot of the persistent ART to a portable
 * binary backup file while the database remains online. Uses the
 * iterator (snapshot isolation) for export and batch insert for import.
 *
 * Binary format:
 *   [Header 32B] [Entry1] [Entry2] ... [EntryN] [Footer 16B]
 *
 * Each entry:
 *   key_len(4) + value_len(4) + key_bytes + value_bytes
 */

#ifndef DB_BACKUP_H
#define DB_BACKUP_H

#include "data_art.h"
#include <stdint.h>
#include <stdbool.h>

// ============================================================================
// Constants
// ============================================================================

#define DB_BACKUP_MAGIC         0x41525442  // "ARTB"
#define DB_BACKUP_FOOTER_MAGIC  0x42545241  // "BTRA"
#define DB_BACKUP_VERSION       1

// ============================================================================
// On-disk Structures
// ============================================================================

typedef struct __attribute__((packed)) {
    uint32_t magic;           // DB_BACKUP_MAGIC
    uint32_t version;         // DB_BACKUP_VERSION
    uint64_t entry_count;     // number of key-value pairs
    uint64_t timestamp;       // seconds since epoch (time of export)
    uint32_t flags;           // bits 0-15: reserved, bits 16-31: key_size
    uint32_t header_checksum; // CRC32-C of first 28 bytes
} db_backup_header_t;

typedef struct __attribute__((packed)) {
    uint32_t magic;           // DB_BACKUP_FOOTER_MAGIC
    uint64_t entry_count;     // must match header entry_count
    uint32_t data_checksum;   // CRC32-C of all entry bytes
} db_backup_footer_t;

// ============================================================================
// Backup Info (user-facing metadata)
// ============================================================================

typedef struct {
    uint64_t entry_count;
    uint64_t timestamp;
    uint32_t version;
    uint32_t flags;
    uint32_t key_size;
} db_backup_info_t;

// ============================================================================
// API
// ============================================================================

/**
 * Export entire database to backup file (consistent snapshot, non-blocking).
 *
 * Creates an iterator (captures committed root atomically), walks all
 * entries in sorted key order, writes each key-value pair sequentially
 * with CRC32-C checksum. The tree remains fully operational during export.
 *
 * @param tree        ART tree to export
 * @param backup_path Path for output file (created/overwritten)
 * @return true on success, false on failure
 */
bool db_backup_export(data_art_tree_t *tree, const char *backup_path);

/**
 * Import backup file into tree (batch insert).
 *
 * Reads backup file, validates header + footer + checksums, then inserts
 * all entries using batch insert (transactional, in batches of 1000).
 *
 * @param tree        Target ART tree
 * @param backup_path Path to backup file
 * @return true on success, false on failure
 */
bool db_backup_import(data_art_tree_t *tree, const char *backup_path);

/**
 * Read backup metadata without importing.
 *
 * @param backup_path Path to backup file
 * @param info_out    Output metadata
 * @return true on success, false if file is invalid
 */
bool db_backup_info(const char *backup_path, db_backup_info_t *info_out);

#endif // DB_BACKUP_H
