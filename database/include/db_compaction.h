/**
 * Database Compaction - Defragmentation and Space Reclamation
 * 
 * User-facing maintenance operation to compact the append-only database.
 * With append-only writes (see COMPRESSION.md), dead pages accumulate over time
 * as delete/update operations create new versions. Dead pages are marked with
 * PAGE_GC_FLAG_DEAD but remain on disk consuming space.
 * 
 * Compaction reorganizes the database:
 * 1. Identifies all live pages (ref_count > 0) from page index
 * 2. Copies live pages to NEW data file(s) in packed contiguous layout
 * 3. Builds new page index with updated file offsets
 * 4. Atomically switches to new data files + index
 * 5. Deletes old data files to reclaim disk space
 * 
 * Key differences for append-only storage:
 * - Cannot overwrite pages in place (variable compressed sizes)
 * - Must create entirely new data files
 * - Page IDs remain the same, only file offsets change
 * - Atomic cutover using metadata file switch
 * 
 * This is a **heavy operation** - typically run:
 * - During maintenance windows (weekly/monthly)
 * - When fragmentation exceeds 30-50%
 * - When dead_pages × avg_page_size > threshold (e.g., 10GB wasted)
 * 
 * Operation is **transactional** - either completes fully or rolls back.
 */

#ifndef DB_COMPACTION_H
#define DB_COMPACTION_H

#include "data_art.h"
#include "page_manager.h"

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Compaction Configuration
// ============================================================================

/**
 * Compaction configuration
 */
typedef struct {
    bool dry_run;                    // If true, analyze only (don't modify)
    bool verbose;                    // Enable detailed logging
    uint64_t batch_size;             // Pages to process per batch (for progress)
    
    // Recompression policy
    bool recompress_cold;            // Promote cold pages to higher compression
    bool recompress_warm;            // Recompress warm pages with latest algorithm
    
    // Safety limits
    bool allow_concurrent_reads;     // Allow reads during compaction (MVCC)
    uint64_t max_duration_sec;       // Abort if exceeds this (0 = no limit)
    uint64_t max_temp_space_mb;      // Max temp disk space for new files (0 = no limit)
} compaction_config_t;

/**
 * Default compaction configuration
 */
static const compaction_config_t COMPACTION_DEFAULT_CONFIG = {
    .dry_run = false,
    .verbose = false,
    .batch_size = 1000,
    .recompress_cold = true,         // Opportunistically improve compression
    .recompress_warm = false,        // Keep existing compression
    .allow_concurrent_reads = false, // Safer: exclusive lock
    .max_duration_sec = 0,           // No time limit
    .max_temp_space_mb = 0,          // No space limit
};

// ============================================================================
// Compaction Statistics
// ============================================================================

/**
 * Compaction results and statistics
 */
typedef struct {
    // Input state
    uint64_t total_pages_before;     // Total pages in old index
    uint64_t live_pages;             // Pages with ref_count > 0
    uint64_t dead_pages;             // Pages with ref_count = 0
    uint64_t file_size_before;       // Total old data file sizes
    
    // Output state
    uint64_t total_pages_after;      // Pages in new index (should = live_pages)
    uint64_t file_size_after;        // Total new data file sizes
    uint64_t space_reclaimed;        // Bytes freed (before - after)
    
    // Per-file breakdown
    uint64_t old_data_files;         // Number of old data files
    uint64_t new_data_files;         // Number of new data files
    
    // Operation stats
    uint64_t pages_copied;           // Pages written to new files
    uint64_t pages_recompressed;     // Pages compressed with different tier
    uint64_t bytes_read;             // Total bytes read from old files
    uint64_t bytes_written;          // Total bytes written to new files
    uint64_t duration_ms;            // Time taken in milliseconds
    
    // Outcome
    bool success;                    // Operation succeeded
    const char *error_message;       // Error details if failed
} compaction_stats_t;

// ============================================================================
// Compaction Operations
// ============================================================================

/**
 * Analyze database fragmentation (dry run)
 * 
 * Scans page index to determine how many pages are live vs dead,
 * and calculates space that could be reclaimed by compaction.
 * 
 * Does NOT modify the database - safe to run anytime.
 * 
 * For append-only storage:
 * - Scans page index entries
 * - Counts pages with PAGE_GC_FLAG_DEAD
 * - Sums compressed_size of dead pages
 * - Calculates fragmentation percentage
 * 
 * @param tree ART tree to analyze
 * @param stats_out Statistics output
 * @return true on success
 */
bool db_compaction_analyze(data_art_tree_t *tree, compaction_stats_t *stats_out);

/**
 * Compact database (full operation)
 * 
 * DANGER: Heavy operation that rewrites the entire database.
 * 
 * Process for append-only storage:
 * 1. Acquire exclusive lock (or allow concurrent reads if configured)
 * 2. Scan page index to identify live pages (ref_count > 0, no DEAD flag)
 * 3. Create new temporary data files (pages_XXXXXX.dat.tmp)
 * 4. Copy live pages to new files in packed order:
 *    - Decompress from old location
 *    - Recompress if tier changed (e.g., promote cold → warm)
 *    - Write to new file sequentially
 *    - Track new file offset in temp index
 * 5. Build new page index with updated file offsets
 * 6. Write new page index (pages.idx.tmp)
 * 7. Atomic cutover:
 *    - Rename pages.idx.tmp → pages.idx
 *    - Rename pages_XXXXXX.dat.tmp → pages_XXXXXX.dat
 *    - Update metadata file
 * 8. Delete old data files
 * 9. Release lock
 * 
 * Key properties:
 * - Page IDs unchanged (tree structure unmodified)
 * - Only file offsets updated in page index
 * - Atomic switch ensures crash safety
 * - Old files kept until cutover succeeds
 * 
 * Transactional: If operation fails, database remains in valid state
 * (old files still intact, temp files deleted).
 * 
 * Typical usage:
 * ```c
 * compaction_stats_t stats;
 * compaction_config_t config = COMPACTION_DEFAULT_CONFIG;
 * config.verbose = true;
 * 
 * if (!db_compaction_run(tree, &config, &stats)) {
 *     fprintf(stderr, "Compaction failed: %s\n", stats.error_message);
 * } else {
 *     printf("Reclaimed %lu MB (%.1f%% reduction)\n", 
 *            stats.space_reclaimed / (1024*1024),
 *            stats.fragmentation_pct);
 * }
 * ```
 * 
 * @param tree ART tree to compact
 * @param config Compaction configuration
 * @param stats_out Statistics output
 * @return true on success
 */
bool db_compaction_run(data_art_tree_t *tree, 
                       const compaction_config_t *config,
                       compaction_stats_t *stats_out);

/**
 * Check if compaction is recommended
 * 
 * Heuristics for append-only storage:
 * - More than 30% dead pages → recommend
 * - Dead space >10GB → recommend  
 * - File size >2x live data size → recommend
 * - After heavy delete workload → recommend
 * 
 * @param tree ART tree to check
 * @return true if compaction recommended
 */
bool db_compaction_is_recommended(data_art_tree_t *tree);

/**
 * Estimate compaction duration
 * 
 * Rough estimate based on:
 * - Number of live pages
 * - Average compressed page size
 * - Disk sequential I/O speed (~200 MB/s typical)
 * - Decompression + recompression overhead
 * 
 * Formula: duration ≈ (live_pages × avg_size × 2) / disk_speed + cpu_overhead
 * 
 * @param tree ART tree
 * @return Estimated duration in seconds
 */
uint64_t db_compaction_estimate_duration(data_art_tree_t *tree);

// ============================================================================
// Advanced Operations
// ============================================================================

/**
 * Incremental compaction (process in batches)
 * 
 * For very large databases, process compaction in chunks
 * to avoid long exclusive locks. Each batch processes N pages
 * and updates progress.
 * 
 * Note: With append-only storage, this is more complex because
 * we need to maintain consistency across multiple partial writes.
 * Consider using snapshot isolation or multi-version concurrency.
 * 
 * @param tree ART tree
 * @param batch_size Pages per batch
 * @param stats_out Accumulated statistics
 * @return true if batch completed, false if more work remains
 */
bool db_compaction_run_incremental(data_art_tree_t *tree,
                                   uint64_t batch_size,
                                   compaction_stats_t *stats_out);

/**
 * Online compaction (allow reads during compaction)
 * 
 * Uses MVCC to allow concurrent reads while compaction is in progress:
 * - Readers use old data files + old page index
 * - Compactor creates new data files + new index in background
 * - Atomic cutover when complete (metadata file switch)
 * - Incremental buffer pool invalidation
 * 
 * More complex but allows zero-downtime compaction for production systems.
 * 
 * @param tree ART tree
 * @param config Configuration
 * @param stats_out Statistics
 * @return true on success
 */
bool db_compaction_run_online(data_art_tree_t *tree,
                              const compaction_config_t *config,
                              compaction_stats_t *stats_out);

// ============================================================================
// Utilities
// ============================================================================

/**
 * Print compaction statistics
 * 
 * @param stats Statistics to print
 */
void db_compaction_print_stats(const compaction_stats_t *stats);

/**
 * Calculate fragmentation percentage
 * 
 * @param stats Statistics from analysis
 * @return Fragmentation percentage (0-100)
 */
double db_compaction_fragmentation_pct(const compaction_stats_t *stats);

#ifdef __cplusplus
}
#endif

#endif // DB_COMPACTION_H
