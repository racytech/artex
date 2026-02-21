/*
 * Buffer Pool - LRU Cache for Pages
 * 
 * Manages in-memory cache of disk pages with:
 * - Hash table (uthash) for O(1) page_id → frame lookup
 * - LRU doubly-linked list for eviction policy
 * - Pin/unpin mechanism to prevent eviction of active pages
 * - Dirty page tracking for checkpoint flushing
 * 
 * Design goals:
 * - Fast lookup: O(1) average case
 * - Simple eviction: LRU tail removal
 * - Thread-safe: rwlock protection
 * - Memory bounded: Fixed capacity, evict when full
 */

#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include "page_manager.h"
#include "uthash.h"

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

// ============================================================================
// Buffer Frame - Cached Page in Memory
// ============================================================================

/**
 * Buffer frame: Represents a single page cached in memory
 * 
 * Layout:
 * - page_id: Key for hash table lookup (MUST be first for uthash)
 * - page: Actual 4KB page data
 * - LRU pointers: Doubly-linked list for eviction
 * - State: dirty flag, pin count, access time
 * - Hash handle: uthash bookkeeping
 */
typedef struct buffer_frame {
    uint64_t page_id;              // Key (MUST be first field for uthash)
    
    page_t page;                   // 4KB page data
    
    // LRU doubly-linked list
    struct buffer_frame *lru_prev;
    struct buffer_frame *lru_next;
    
    // State management
    bool is_dirty;                 // Modified but not flushed to disk
    uint32_t pin_count;            // Reference count (0 = can evict)
    uint64_t last_access_time;     // Timestamp for LRU policy
    uint64_t load_time;            // When page was loaded (for stats)
    
    UT_hash_handle hh;             // uthash handle (internal)
} buffer_frame_t;

// ============================================================================
// Buffer Pool - Main Cache Structure
// ============================================================================

/**
 * Buffer pool configuration
 */
typedef struct {
    size_t capacity;               // Max number of frames (default: 10000)
    bool enable_statistics;        // Track cache hits/misses (default: true)
} buffer_pool_config_t;

/**
 * Buffer pool statistics
 */
typedef struct {
    // Cache performance
    uint64_t cache_hits;           // Successful lookups
    uint64_t cache_misses;         // Page not in cache
    double cache_hit_rate;         // hits / (hits + misses)
    
    // Eviction stats
    uint64_t evictions;            // Total pages evicted
    uint64_t evictions_clean;      // Clean pages evicted
    uint64_t evictions_dirty;      // Dirty pages evicted (flushed first)
    
    // Memory usage
    size_t num_frames;             // Current frames in cache
    size_t num_dirty;              // Dirty frames count
    size_t num_pinned;             // Pinned frames count
    size_t capacity;               // Max capacity
    double utilization;            // num_frames / capacity
    
    // Performance metrics
    uint64_t total_loads;          // Pages loaded from disk
    uint64_t total_flushes;        // Pages flushed to disk
    uint64_t pins;                 // Total pin operations
    uint64_t unpins;               // Total unpin operations
} buffer_pool_stats_t;

/**
 * Buffer pool: In-memory cache of disk pages
 */
typedef struct {
    // Hash table: page_id → buffer_frame_t
    buffer_frame_t *frames_hash;   // uthash table
    
    // LRU eviction list (doubly-linked)
    buffer_frame_t *lru_head;      // Most recently used
    buffer_frame_t *lru_tail;      // Least recently used (evict here)
    
    // Configuration
    buffer_pool_config_t config;
    
    // Statistics
    buffer_pool_stats_t stats;
    
    // Thread safety
    pthread_rwlock_t lock;         // Protects entire buffer pool
    
    // Page manager for disk I/O
    page_manager_t *page_manager;
} buffer_pool_t;

// ============================================================================
// Buffer Pool Operations
// ============================================================================

/**
 * Create a new buffer pool
 * 
 * @param config Configuration (NULL for defaults)
 * @param page_manager Page manager for disk I/O
 * @return Buffer pool instance, or NULL on failure
 */
buffer_pool_t *buffer_pool_create(const buffer_pool_config_t *config,
                                   page_manager_t *page_manager);

/**
 * Destroy buffer pool and free all resources
 * 
 * Flushes all dirty pages before destruction.
 * 
 * @param bp Buffer pool to destroy
 */
void buffer_pool_destroy(buffer_pool_t *bp);

/**
 * Get a page from the buffer pool
 * 
 * If page is not in cache:
 * - Load from disk via page_manager
 * - Add to cache (may evict LRU page if full)
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to retrieve
 * @return Pointer to page in cache, or NULL on error
 */
page_t *buffer_pool_get(buffer_pool_t *bp, uint64_t page_id);

/**
 * Get and pin a page atomically
 * 
 * Like buffer_pool_get(), but pins the page before releasing the buffer pool lock.
 * This prevents the page from being evicted between get and pin.
 * Must call buffer_pool_unpin() when done with the page.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to retrieve
 * @return Pointer to page in cache, or NULL on error
 */
page_t *buffer_pool_get_pinned(buffer_pool_t *bp, uint64_t page_id);

/**
 * Pin a page to prevent eviction
 * 
 * Increments pin count. Pinned pages (pin_count > 0) cannot be evicted.
 * Must call buffer_pool_unpin() when done.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to pin
 * @return true on success, false if page not in cache
 */
bool buffer_pool_pin(buffer_pool_t *bp, uint64_t page_id);

/**
 * Unpin a page to allow eviction
 * 
 * Decrements pin count. When pin_count reaches 0, page becomes eligible
 * for eviction.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to unpin
 * @return true on success, false if page not in cache or already unpinned
 */
bool buffer_pool_unpin(buffer_pool_t *bp, uint64_t page_id);

/**
 * Mark a page as dirty (modified)
 * 
 * Dirty pages are flushed to disk during checkpoint or eviction.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to mark dirty
 * @return true on success, false if page not in cache
 */
bool buffer_pool_mark_dirty(buffer_pool_t *bp, uint64_t page_id);

/**
 * Flush a specific page to disk
 * 
 * Writes page to disk and clears dirty flag.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to flush
 * @return true on success, false on error
 */
bool buffer_pool_flush_page(buffer_pool_t *bp, uint64_t page_id);

/**
 * Flush all dirty pages to disk
 * 
 * Used during checkpoint to persist all modifications.
 * Iterates through all frames and writes dirty pages.
 * 
 * @param bp Buffer pool
 * @return Number of pages flushed, or -1 on error
 */
int buffer_pool_flush_all(buffer_pool_t *bp);

/**
 * Evict a specific page from cache
 * 
 * Removes page from cache. If dirty, flushes first.
 * Fails if page is pinned.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to evict
 * @return true on success, false if page pinned or error
 */
bool buffer_pool_evict(buffer_pool_t *bp, uint64_t page_id);

/**
 * Check if a page is in the buffer pool
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to check
 * @return true if in cache, false otherwise
 */
bool buffer_pool_contains(buffer_pool_t *bp, uint64_t page_id);

/**
 * Clear all pages from the buffer pool
 * 
 * Flushes all dirty pages and removes all entries from cache.
 * Use this after operations that invalidate the entire cache
 * (e.g., delete with copy-on-write semantics).
 * 
 * @param bp Buffer pool
 * @return Number of pages cleared, or -1 on error
 */
int buffer_pool_clear(buffer_pool_t *bp);

/**
 * Invalidate a page in the cache after external write
 * 
 * Use this when a page is written to disk outside the buffer pool
 * (e.g., via direct page_manager_write). This ensures cache coherency
 * by removing the stale cached copy.
 * 
 * WARNING: Only call this when you're certain no pointers to this
 * page are still in use. Otherwise, use buffer_pool_reload instead.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to invalidate
 * @return true on success (or if page not in cache), false on error
 */
bool buffer_pool_invalidate(buffer_pool_t *bp, uint64_t page_id);

/**
 * Reload a page from disk after external write
 * 
 * Use this when a page is written to disk outside the buffer pool
 * to refresh the cached copy with the latest data from disk.
 * Safer than invalidate when pointers to the page might still be in use.
 * 
 * @param bp Buffer pool
 * @param page_id Page ID to reload
 * @return true on success, false if page not in cache or reload failed
 */
bool buffer_pool_reload(buffer_pool_t *bp, uint64_t page_id);

// ============================================================================
// Statistics & Monitoring
// ============================================================================

/**
 * Get buffer pool statistics
 * 
 * @param bp Buffer pool
 * @param stats_out Output statistics structure
 */
void buffer_pool_get_stats(buffer_pool_t *bp, buffer_pool_stats_t *stats_out);

/**
 * Print buffer pool statistics (for debugging)
 * 
 * @param bp Buffer pool
 */
void buffer_pool_print_stats(buffer_pool_t *bp);

/**
 * Reset statistics counters
 * 
 * @param bp Buffer pool
 */
void buffer_pool_reset_stats(buffer_pool_t *bp);

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Get default buffer pool configuration
 * 
 * @return Default configuration
 */
buffer_pool_config_t buffer_pool_default_config(void);

/**
 * Get current memory usage in bytes
 * 
 * @param bp Buffer pool
 * @return Memory usage (approximate)
 */
size_t buffer_pool_memory_usage(buffer_pool_t *bp);

#endif // BUFFER_POOL_H
