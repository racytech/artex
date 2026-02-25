/**
 * Page Garbage Collection - Reference Counting for Append-Only Storage
 * 
 * Manages page-level garbage collection within a single tree version.
 * Solves the copy-on-write page leak problem where delete operations
 * create new page versions but don't free old unreachable pages.
 * 
 * IMPORTANT: Works with append-only variable-size storage (see COMPRESSION.md)
 * - Pages are appended to data files with variable compressed sizes
 * - Cannot reuse old page locations (different sizes)
 * - Instead: Mark pages as "dead" and reclaim space during compaction
 * 
 * Two-part system:
 * 1. Reference counting: Track which pages are reachable from tree root
 * 2. Dead page tracking: Mark unreachable pages for future compaction
 * 
 * Key operations:
 * - Increment ref count when page is linked (insert, update)
 * - Decrement ref count when page is unlinked (delete, shrink)
 * - Mark pages with ref_count=0 as "dead" in page index
 * - Compaction reclaims space from dead pages (see db_compaction.h)
 */

#ifndef PAGE_GC_H
#define PAGE_GC_H

#include "page_manager.h"

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Page Reference Counting
// ============================================================================

/**
 * Page metadata with reference counting
 * 
 * Extended page index entry that includes reference count for GC.
 * ref_count tracks how many tree nodes reference this page.
 * 
 * This extends the page_index_entry_t from COMPRESSION.md with GC metadata.
 */
typedef struct {
    // From COMPRESSION.md page_index_entry_t
    uint64_t page_id;
    uint32_t file_idx;           // Which data file (pages_XXXXX.dat)
    uint64_t file_offset;        // Physical byte offset in data file
    uint32_t compressed_size;    // Bytes on disk
    uint8_t compression_type;    // NONE, LZ4, ZSTD_5, ZSTD_19
    uint32_t checksum;
    uint64_t version;
    
    // GC extensions
    uint32_t ref_count;          // Number of references to this page
    uint32_t flags;              // Flags: DEAD, PINNED, etc.
    uint64_t last_access_time;   // For compression tier decisions
} page_gc_metadata_t;

// Flags for page GC state
#define PAGE_GC_FLAG_DEAD   (1 << 0)  // ref_count=0, eligible for compaction
#define PAGE_GC_FLAG_PINNED (1 << 1)  // Pinned in buffer pool, don't compress

/**
 * Initialize reference counting for a page
 * 
 * Sets ref_count to 1 (caller holds initial reference).
 * Called when allocating a new page.
 * 
 * @param pm Page manager
 * @param page_id Page to initialize
 * @return true on success
 */
bool page_gc_init_ref(page_manager_t *pm, uint64_t page_id);

/**
 * Increment page reference count
 * 
 * Called when:
 * - Inserting a child pointer to this page
 * - Creating a new reference during tree operations
 * 
 * Thread-safe using atomic operations.
 * 
 * @param pm Page manager
 * @param page_id Page to increment
 * @return New reference count, or 0 on error
 */
uint32_t page_gc_incref(page_manager_t *pm, uint64_t page_id);

/**
 * Decrement page reference count
 * 
 * Called when:
 * - Removing a child pointer during delete
 * - Updating a pointer during copy-on-write
 * - Shrinking a node (old version unreachable)
 * 
 * If ref_count reaches 0, page is added to free list automatically.
 * Thread-safe using atomic operations.
 * 
 * @param pm Page manager
 * @param page_id Page to decrement
 * @return New reference count, or 0 if freed
 */
uint32_t page_gc_decref(page_manager_t *pm, uint64_t page_id);

/**
 * Get current reference count (for debugging/stats)
 * 
 * @param pm Page manager
 * @param page_id Page to query
 * @return Current ref_count, or 0 if not found
 */
uint32_t page_gc_get_refcount(page_manager_t *pm, uint64_t page_id);

// ============================================================================
// Dead Page Tracking (replaces free list for append-only storage)
// ============================================================================

/**
 * Dead page statistics
 * 
 * Tracks pages with ref_count=0 that are consuming disk space.
 * Used to determine when compaction is needed.
 */
typedef struct {
    uint64_t total_pages;         // Total pages in index
    uint64_t live_pages;          // Pages with ref_count > 0
    uint64_t dead_pages;          // Pages with ref_count = 0
    uint64_t dead_bytes;          // Disk space wasted by dead pages
    double fragmentation_pct;     // Percentage of wasted space
} dead_page_stats_t;

/**
 * Mark page as dead (ref_count reached 0)
 * 
 * Called automatically when page ref_count reaches 0.
 * Page remains on disk but is marked as eligible for compaction.
 * 
 * Thread-safe - updates page index atomically.
 * 
 * @param pm Page manager
 * @param page_id Page to mark dead
 * @return true on success
 */
bool page_gc_mark_dead(page_manager_t *pm, uint64_t page_id);

/**
 * Check if page is dead
 * 
 * @param pm Page manager
 * @param page_id Page to check
 * @return true if page has ref_count=0
 */
bool page_gc_is_dead(page_manager_t *pm, uint64_t page_id);

/**
 * Get dead page statistics
 * 
 * Scans page index to count dead pages and calculate fragmentation.
 * Used to determine if compaction is recommended.
 * 
 * @param pm Page manager
 * @param stats_out Statistics output
 */
void page_gc_get_dead_stats(page_manager_t *pm, dead_page_stats_t *stats_out);

/**
 * Collect dead pages (list all pages with ref_count=0)
 * 
 * Returns array of page IDs that are dead (for compaction).
 * Caller must free the returned array.
 * 
 * @param pm Page manager
 * @param count_out Number of dead pages returned
 * @return Array of dead page IDs, or NULL if none
 */
uint64_t *page_gc_collect_dead_pages(page_manager_t *pm, size_t *count_out);

// ============================================================================
// Integration with Page Manager (Append-Only Model)
// ============================================================================

/**
 * Page allocation in append-only model
 * 
 * Always allocates NEW page IDs (monotonically increasing).
 * Dead pages are NOT reused - space reclaimed during compaction.
 * 
 * Usage:
 * ```c
 * // Allocate new page (always appends to end)
 * uint64_t page_id = page_manager_alloc(tree->pm, size);
 * page_gc_init_ref(tree->pm, page_id);
 * 
 * // Write page (appends to data file, updates index)
 * page_manager_write(tree->pm, page_id, data, COMPRESSION_LZ4);
 * ```
 */

/**
 * Page deallocation (decrements ref count, marks dead if zero)
 * 
 * Usage during delete/update:
 * ```c
 * // Old child being replaced
 * node_ref_t old_child = find_child(tree, node_ref, byte);
 * 
 * // Create new version (allocates new page)
 * node_ref_t new_node = update_child(tree, node_ref, byte, new_child);
 * 
 * // Decrement ref count on old version
 * page_gc_decref(tree->pm, old_child.page_id);
 * // → If ref_count reaches 0, marked as dead (PAGE_GC_FLAG_DEAD)
 * // → Space reclaimed later during compaction
 * 
 * // Buffer pool should invalidate old page to avoid cache coherency issues
 * buffer_pool_invalidate(tree->buffer_pool, old_child.page_id);
 * ```
 */

/**
 * When to run compaction
 * 
 * Check fragmentation periodically and compact when needed:
 * ```c
 * dead_page_stats_t stats;
 * page_gc_get_dead_stats(tree->pm, &stats);
 * 
 * if (stats.fragmentation_pct > 30.0) {
 *     // More than 30% wasted space - run compaction
 *     db_compaction_run(tree, &config, &compact_stats);
 * }
 * ```
 */

#ifdef __cplusplus
}
#endif

#endif // PAGE_GC_H
