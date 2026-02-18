/**
 * Database Compaction - Defragmentation and Space Reclamation
 * 
 * User-facing maintenance operation to compact the database file.
 * Even with page GC (ref counting + free list), the database file
 * can become fragmented with freed pages scattered throughout.
 * 
 * Compaction reorganizes the database:
 * 1. Identifies all live (reachable) pages from tree root
 * 2. Copies live pages to beginning of file (contiguous layout)
 * 3. Updates all node references to new page locations
 * 4. Truncates file to remove freed space at end
 * 
 * This is a **heavy operation** - typically run:
 * - During maintenance windows
 * - When disk usage is high
 * - Periodically (weekly/monthly)
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
    
    // Safety limits
    bool allow_concurrent_access;    // Allow reads during compaction
    uint64_t max_duration_sec;       // Abort if exceeds this (0 = no limit)
} compaction_config_t;

/**
 * Default compaction configuration
 */
static const compaction_config_t COMPACTION_DEFAULT_CONFIG = {
    .dry_run = false,
    .verbose = false,
    .batch_size = 1000,
    .allow_concurrent_access = false,  // Safer: exclusive lock
    .max_duration_sec = 0,             // No time limit
};

// ============================================================================
// Compaction Statistics
// ============================================================================

/**
 * Compaction results and statistics
 */
typedef struct {
    // Input state
    uint64_t total_pages_before;     // Total allocated pages
    uint64_t live_pages;             // Reachable pages
    uint64_t dead_pages;             // Unreachable pages
    uint64_t file_size_before;       // File size in bytes
    
    // Output state
    uint64_t total_pages_after;      // Total pages after compaction
    uint64_t file_size_after;        // File size after truncation
    uint64_t space_reclaimed;        // Bytes freed
    
    // Operation stats
    uint64_t pages_moved;            // Pages copied to new location
    uint64_t references_updated;     // Node references rewritten
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
 * Walks the tree to determine how many pages are live vs dead,
 * and estimates space that could be reclaimed.
 * 
 * Does NOT modify the database - safe to run anytime.
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
 * Process:
 * 1. Acquire exclusive lock (or allow concurrent reads if configured)
 * 2. Walk tree to identify all live pages
 * 3. Build page remapping table (old_id → new_id)
 * 4. Copy live pages to temporary location or beginning of file
 * 5. Update all node references to new page IDs
 * 6. Update root pointer
 * 7. Truncate file to remove freed space
 * 8. Release lock
 * 
 * Transactional: If operation fails, database remains in valid state.
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
 *     printf("Reclaimed %lu MB\n", stats.space_reclaimed / (1024*1024));
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
 * Heuristics:
 * - More than 30% dead pages → recommend
 * - Free list has >10,000 entries → recommend
 * - File size >2x live data size → recommend
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
 * - Disk I/O speed
 * - CPU for reference rewriting
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
 * to avoid long exclusive locks.
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
 * More complex: use copy-on-write to allow concurrent reads
 * while compaction is in progress.
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
