/**
 * Page Garbage Collection - Reference Counting and Free List
 * 
 * Manages page-level garbage collection within a single tree version.
 * Solves the copy-on-write page leak problem where delete operations
 * create new page versions but don't free old unreachable pages.
 * 
 * Two-part system:
 * 1. Reference counting: Track which pages are reachable from tree root
 * 2. Free list: Maintain recyclable page IDs for reallocation
 * 
 * Key operations:
 * - Increment ref count when page is linked (insert, update)
 * - Decrement ref count when page is unlinked (delete, shrink)
 * - Add pages with ref_count=0 to free list
 * - Allocate from free list before extending file
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
 * Extended page header that includes reference count for GC.
 * ref_count tracks how many tree nodes reference this page.
 */
typedef struct {
    uint32_t ref_count;          // Number of references to this page
    uint32_t flags;              // Flags: DIRTY, PINNED, etc.
    uint64_t last_modified;      // Timestamp of last modification
} page_gc_metadata_t;

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
// Free List Management
// ============================================================================

/**
 * Free list node (linked list of available page IDs)
 * 
 * Stored in a dedicated metadata page or memory structure.
 * Each node contains a batch of freed page IDs.
 */
typedef struct free_list_node {
    uint64_t page_ids[63];           // Batch of free page IDs (63 × 8 = 504 bytes)
    uint32_t count;                  // Number of valid entries (0-63)
    uint32_t reserved;
    struct free_list_node *next;     // Next batch (page_id stored here)
} free_list_node_t;

/**
 * Free list manager
 * 
 * Maintains recyclable page IDs. Pages are added when ref_count
 * reaches 0, and consumed during allocation.
 */
typedef struct {
    page_manager_t *pm;              // Associated page manager
    free_list_node_t *head;          // Head of free list
    pthread_rwlock_t lock;           // Protects free list access
    
    // Statistics
    uint64_t total_freed;            // Total pages ever freed
    uint64_t total_reused;           // Total pages reused from free list
    uint64_t current_free_count;     // Current number of free pages
} free_list_t;

/**
 * Create free list manager
 * 
 * @param pm Page manager to associate with
 * @return Initialized free list, or NULL on error
 */
free_list_t *free_list_create(page_manager_t *pm);

/**
 * Destroy free list manager
 * 
 * Persists free list to disk before destruction.
 * 
 * @param fl Free list to destroy
 */
void free_list_destroy(free_list_t *fl);

/**
 * Add page to free list
 * 
 * Called automatically when page ref_count reaches 0.
 * Page is available for reallocation.
 * 
 * Thread-safe.
 * 
 * @param fl Free list
 * @param page_id Page to add
 * @return true on success
 */
bool free_list_add(free_list_t *fl, uint64_t page_id);

/**
 * Allocate page from free list
 * 
 * Returns a page ID from free list if available, otherwise 0.
 * Caller should allocate new page if this returns 0.
 * 
 * Thread-safe.
 * 
 * @param fl Free list
 * @return Page ID from free list, or 0 if empty
 */
uint64_t free_list_pop(free_list_t *fl);

/**
 * Check if free list is empty
 * 
 * @param fl Free list
 * @return true if no free pages available
 */
bool free_list_is_empty(free_list_t *fl);

/**
 * Get free list statistics
 * 
 * @param fl Free list
 * @param total_freed_out Total pages ever freed
 * @param total_reused_out Total pages reused
 * @param current_free_out Current free page count
 */
void free_list_stats(free_list_t *fl, 
                     uint64_t *total_freed_out,
                     uint64_t *total_reused_out, 
                     uint64_t *current_free_out);

/**
 * Persist free list to disk
 * 
 * Writes free list to special metadata page(s).
 * Called during checkpoint or shutdown.
 * 
 * @param fl Free list
 * @return true on success
 */
bool free_list_persist(free_list_t *fl);

/**
 * Load free list from disk
 * 
 * Reads free list from metadata page(s).
 * Called during database open.
 * 
 * @param fl Free list
 * @return true on success
 */
bool free_list_load(free_list_t *fl);

// ============================================================================
// Integration with Page Manager
// ============================================================================

/**
 * Modified page allocation that checks free list first
 * 
 * Usage:
 * ```c
 * uint64_t page_id = free_list_pop(tree->free_list);
 * if (page_id == 0) {
 *     page_id = page_manager_alloc(tree->pm, size);
 * }
 * page_gc_init_ref(tree->pm, page_id);
 * ```
 */

/**
 * Page deallocation (decrements ref count, possibly frees)
 * 
 * Usage during delete/update:
 * ```c
 * // Old child being replaced
 * node_ref_t old_child = find_child(tree, node_ref, byte);
 * 
 * // Create new version
 * node_ref_t new_node = update_child(tree, node_ref, byte, new_child);
 * 
 * // Decrement ref count on old version
 * page_gc_decref(tree->pm, old_child.page_id);
 * // → If ref_count reaches 0, automatically added to free list
 * ```
 */

#ifdef __cplusplus
}
#endif

#endif // PAGE_GC_H
