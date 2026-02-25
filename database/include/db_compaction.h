/**
 * Database Compaction - In-Place Defragmentation
 *
 * Maintenance operation to compact the append-only database in-place.
 * Dead pages accumulate as delete/update operations create new versions.
 *
 * In-place compaction (per data file):
 * 1. Scans page index for live pages, sorted by file_offset
 * 2. Moves live pages forward (to lower offsets) closing gaps
 * 3. ftruncate() to reclaim space at end of file
 * 4. Updates page index with new offsets
 *
 * Key properties:
 * - No temporary files or extra disk space needed
 * - Pages only move to lower offsets (write cursor always behind read cursor)
 * - Crash-safe via compaction journal (replay on recovery)
 * - Compressed pages copied as raw bytes (no decompress/recompress)
 */

#ifndef DB_COMPACTION_H
#define DB_COMPACTION_H

#include "page_manager.h"

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Compaction Configuration
// ============================================================================

typedef struct {
    bool dry_run;                    // If true, analyze only (don't modify)
    bool verbose;                    // Enable detailed logging
    uint64_t max_duration_sec;       // Abort if exceeds this (0 = no limit)
} compaction_config_t;

static const compaction_config_t COMPACTION_DEFAULT_CONFIG = {
    .dry_run = false,
    .verbose = false,
    .max_duration_sec = 0,
};

// ============================================================================
// Compaction Statistics
// ============================================================================

typedef struct {
    // Input state
    uint64_t total_pages_before;
    uint64_t live_pages;
    uint64_t dead_pages;
    uint64_t file_size_before;

    // Output state
    uint64_t total_pages_after;
    uint64_t file_size_after;
    uint64_t space_reclaimed;

    // Per-file breakdown
    uint64_t old_data_files;
    uint64_t new_data_files;

    // Operation stats
    uint64_t pages_moved;            // Pages that changed offset
    uint64_t bytes_read;
    uint64_t bytes_written;
    uint64_t duration_ms;

    // Outcome
    bool success;
    const char *error_message;
} compaction_stats_t;

// ============================================================================
// Compaction Journal (crash safety)
// ============================================================================

#define COMPACTION_JOURNAL_MAGIC   0x434A524E  // "CJRN"
#define COMPACTION_JOURNAL_VERSION 1

typedef enum {
    COMPACTION_STATE_COMPACTING = 1,  // Moves in progress
    COMPACTION_STATE_DONE       = 2,  // All moves + truncate + index update complete
} compaction_state_t;

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t state;          // compaction_state_t
    uint32_t file_idx;       // which data file is being compacted
    uint32_t num_entries;    // number of page move entries
    uint64_t final_file_size; // file size after compaction (for ftruncate)
    uint32_t crc;            // CRC of header fields before this
    uint32_t padding;
} __attribute__((packed)) compaction_journal_header_t;

typedef struct {
    uint64_t page_id;
    uint64_t old_offset;
    uint64_t new_offset;
    uint32_t compressed_size;
    uint32_t padding;
} __attribute__((packed)) compaction_journal_entry_t;

// ============================================================================
// Compaction Operations
// ============================================================================

/**
 * Analyze database fragmentation (read-only).
 */
bool db_compaction_analyze(page_manager_t *pm, compaction_stats_t *stats_out);

/**
 * Run in-place compaction.
 *
 * Moves live pages forward within each data file, closing gaps
 * left by dead pages, then truncates. Uses a journal for crash safety.
 */
bool db_compaction_run(page_manager_t *pm,
                       const compaction_config_t *config,
                       compaction_stats_t *stats_out);

/**
 * Check if compaction is recommended (fragmentation > 30%).
 */
bool db_compaction_is_recommended(page_manager_t *pm);

/**
 * Recover from interrupted compaction.
 *
 * Checks for compaction.journal in db_path. If found with
 * COMPACTING state, replays all page moves and completes
 * the operation. Called during page_manager startup.
 *
 * @param pm Page manager instance
 * @return true on success (or no journal found)
 */
bool db_compaction_recover(page_manager_t *pm);

// ============================================================================
// Utilities
// ============================================================================

void db_compaction_print_stats(const compaction_stats_t *stats);
double db_compaction_fragmentation_pct(const compaction_stats_t *stats);

#ifdef __cplusplus
}
#endif

#endif // DB_COMPACTION_H
