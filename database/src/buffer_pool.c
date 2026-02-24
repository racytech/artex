/*
 * Buffer Pool Implementation
 * 
 * LRU cache for disk pages with:
 * - uthash for O(1) lookup
 * - Doubly-linked list for LRU eviction
 * - Pin/unpin reference counting
 * - Dirty page tracking and flushing
 */

#include "buffer_pool.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>

// ============================================================================
// Internal Helper Functions
// ============================================================================

/**
 * Get current timestamp for LRU tracking
 */
static inline uint64_t get_timestamp(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * Remove frame from LRU list (does not free frame)
 */
static void lru_remove(buffer_pool_t *bp, buffer_frame_t *frame) {
    if (frame->lru_prev) {
        frame->lru_prev->lru_next = frame->lru_next;
    } else {
        // Frame was head
        bp->lru_head = frame->lru_next;
    }
    
    if (frame->lru_next) {
        frame->lru_next->lru_prev = frame->lru_prev;
    } else {
        // Frame was tail
        bp->lru_tail = frame->lru_prev;
    }
    
    frame->lru_prev = NULL;
    frame->lru_next = NULL;
}

/**
 * Add frame to head of LRU list (most recently used)
 */
static void lru_add_head(buffer_pool_t *bp, buffer_frame_t *frame) {
    frame->lru_next = bp->lru_head;
    frame->lru_prev = NULL;
    
    if (bp->lru_head) {
        bp->lru_head->lru_prev = frame;
    } else {
        // List was empty
        bp->lru_tail = frame;
    }
    
    bp->lru_head = frame;
    frame->last_access_time = get_timestamp();
}

/**
 * Move frame to head of LRU list (mark as recently used)
 */
static void lru_touch(buffer_pool_t *bp, buffer_frame_t *frame) {
    if (frame == bp->lru_head) {
        // Already at head, just update timestamp
        frame->last_access_time = get_timestamp();
        return;
    }
    
    lru_remove(bp, frame);
    lru_add_head(bp, frame);
}

/**
 * Find frame in hash table
 */
static buffer_frame_t *find_frame(buffer_pool_t *bp, uint64_t page_id) {
    buffer_frame_t *frame = NULL;
    HASH_FIND(hh, bp->frames_hash, &page_id, sizeof(uint64_t), frame);
    return frame;
}

/**
 * Add frame to hash table
 */
static void add_frame(buffer_pool_t *bp, buffer_frame_t *frame) {
    HASH_ADD(hh, bp->frames_hash, page_id, sizeof(uint64_t), frame);
}

/**
 * Remove frame from hash table (does not free frame)
 */
static void remove_frame(buffer_pool_t *bp, buffer_frame_t *frame) {
    HASH_DEL(bp->frames_hash, frame);
}

/**
 * Flush a dirty frame to disk
 */
static bool flush_frame(buffer_pool_t *bp, buffer_frame_t *frame) {
    if (!frame->is_dirty) {
        return true;
    }
    
    // Set page_id in header before writing
    frame->page.header.page_id = frame->page_id;
    
    // Compute checksum for modified data
    page_compute_checksum(&frame->page);
    
    page_result_t result = page_manager_write(bp->page_manager, &frame->page);
    if (result != PAGE_SUCCESS) {
        LOG_ERROR("Failed to flush page %lu to disk", frame->page_id);
        return false;
    }
    
    frame->is_dirty = false;
    bp->stats.total_flushes++;
    if (bp->stats.num_dirty > 0) {
        bp->stats.num_dirty--;
    }
    
    return true;
}

/**
 * Evict the LRU (tail) frame from cache
 * 
 * @return true if eviction succeeded, false if no evictable frame found
 */
static bool evict_lru_frame(buffer_pool_t *bp) {
    // Find first unpinned frame from tail
    buffer_frame_t *frame = bp->lru_tail;
    
    while (frame) {
        if (frame->pin_count == 0) {
            // Found evictable frame
            
            // Flush if dirty
            if (frame->is_dirty) {
                if (!flush_frame(bp, frame)) {
                    return false;
                }
                bp->stats.evictions_dirty++;
            } else {
                bp->stats.evictions_clean++;
            }
            
            // Remove from LRU list and hash table
            lru_remove(bp, frame);
            remove_frame(bp, frame);
            
            // Update stats
            bp->stats.evictions++;
            if (frame->pin_count > 0 && bp->stats.num_pinned > 0) {
                bp->stats.num_pinned--;
            }
            
            // Free frame
            free(frame);
            
            return true;
        }
        
        frame = frame->lru_prev;
    }
    
    // All frames are pinned
    LOG_ERROR("Buffer pool full: all frames are pinned (capacity=%zu)", 
              bp->config.capacity);
    return false;
}

/**
 * Allocate and initialize a new frame
 */
static buffer_frame_t *create_frame(uint64_t page_id) {
    buffer_frame_t *frame = calloc(1, sizeof(buffer_frame_t));
    if (!frame) {
        LOG_ERROR("Failed to allocate buffer frame");
        return NULL;
    }
    
    frame->page_id = page_id;
    frame->is_dirty = false;
    frame->pin_count = 0;
    frame->last_access_time = get_timestamp();
    frame->load_time = frame->last_access_time;
    frame->lru_prev = NULL;
    frame->lru_next = NULL;
    
    return frame;
}

/**
 * Load page from disk into new frame
 */
static buffer_frame_t *load_page(buffer_pool_t *bp, uint64_t page_id) {
    buffer_frame_t *frame = create_frame(page_id);
    if (!frame) {
        return NULL;
    }
    
    // Read page from disk
    page_result_t result = page_manager_read(bp->page_manager, page_id, &frame->page);
    if (result == PAGE_ERROR_IO) {
        // Only create an empty fallback if the page was actually allocated.
        // Pages beyond next_page_id were never allocated — return NULL.
        if (page_id >= bp->page_manager->allocator->next_page_id) {
            LOG_DEBUG("Page %lu was never allocated (next_page_id=%lu)",
                      page_id, bp->page_manager->allocator->next_page_id);
            free(frame);
            return NULL;
        }
        // Page was allocated but not yet written to disk
        // This is normal in concurrent scenarios - initialize as empty page
        LOG_DEBUG("Page %lu not on disk yet, initializing as new page", page_id);
        memset(&frame->page, 0, sizeof(page_t));
        frame->page.header.page_id = page_id;
        frame->page.header.version = 0;
        frame->page.header.free_offset = PAGE_HEADER_SIZE;
        frame->page.header.num_nodes = 0;
        frame->page.header.fragmented_bytes = 0;
        frame->page.header.compression_type = 0;
        frame->page.header.compressed_size = 0;
        frame->page.header.uncompressed_size = PAGE_SIZE;
        frame->page.header.flags = 0;
        frame->is_dirty = true;  // Mark dirty so it gets written on flush
        bp->stats.total_loads++;
        return frame;
    }
    
    if (result != PAGE_SUCCESS) {
        LOG_ERROR("Failed to read page %lu from disk", page_id);
        free(frame);
        return NULL;
    }
    
    bp->stats.total_loads++;
    
    return frame;
}

// ============================================================================
// Public API Implementation
// ============================================================================

buffer_pool_config_t buffer_pool_default_config(void) {
    return (buffer_pool_config_t){
        .capacity = 10000,
        .enable_statistics = true,
    };
}

buffer_pool_t *buffer_pool_create(const buffer_pool_config_t *config,
                                   page_manager_t *page_manager) {
    if (!page_manager) {
        LOG_ERROR("Page manager is required");
        return NULL;
    }
    
    buffer_pool_t *bp = calloc(1, sizeof(buffer_pool_t));
    if (!bp) {
        LOG_ERROR("Failed to allocate buffer pool");
        return NULL;
    }
    
    // Initialize configuration
    if (config) {
        bp->config = *config;
    } else {
        bp->config = buffer_pool_default_config();
    }
    
    // Initialize hash table and LRU list
    bp->frames_hash = NULL;
    bp->lru_head = NULL;
    bp->lru_tail = NULL;
    
    // Initialize statistics
    memset(&bp->stats, 0, sizeof(buffer_pool_stats_t));
    bp->stats.capacity = bp->config.capacity;
    
    // Initialize lock
    if (pthread_rwlock_init(&bp->lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize buffer pool lock");
        free(bp);
        return NULL;
    }
    
    // Store page manager reference
    bp->page_manager = page_manager;
    
    LOG_INFO("Buffer pool created: capacity=%zu, stats=%s",
             bp->config.capacity,
             bp->config.enable_statistics ? "enabled" : "disabled");
    
    return bp;
}

void buffer_pool_destroy(buffer_pool_t *bp) {
    if (!bp) {
        return;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    // Flush all dirty pages
    LOG_INFO("Flushing dirty pages before buffer pool destruction...");
    int flushed = buffer_pool_flush_all(bp);
    LOG_INFO("Flushed %d pages", flushed);
    
    // Free all frames
    buffer_frame_t *frame, *tmp;
    HASH_ITER(hh, bp->frames_hash, frame, tmp) {
        HASH_DEL(bp->frames_hash, frame);
        free(frame);
    }
    
    pthread_rwlock_unlock(&bp->lock);
    pthread_rwlock_destroy(&bp->lock);
    
    free(bp);
    
    LOG_INFO("Buffer pool destroyed");
}

page_t *buffer_pool_get(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return NULL;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    // Check if page is in cache
    buffer_frame_t *frame = find_frame(bp, page_id);
    
    if (frame) {
        // Cache hit
        if (bp->config.enable_statistics) {
            bp->stats.cache_hits++;
        }
        
        // Move to head of LRU list
        lru_touch(bp, frame);
        
        pthread_rwlock_unlock(&bp->lock);
        return &frame->page;
    }
    
    // Cache miss - need to load from disk
    if (bp->config.enable_statistics) {
        bp->stats.cache_misses++;
    }
    
    // Check if we need to evict
    size_t current_size = HASH_COUNT(bp->frames_hash);
    if (current_size >= bp->config.capacity) {
        if (!evict_lru_frame(bp)) {
            pthread_rwlock_unlock(&bp->lock);
            return NULL;
        }
    }
    
    // Load page from disk
    frame = load_page(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return NULL;
    }
    
    // Add to cache
    add_frame(bp, frame);
    lru_add_head(bp, frame);
    
    // Update stats
    bp->stats.num_frames = HASH_COUNT(bp->frames_hash);
    
    pthread_rwlock_unlock(&bp->lock);
    return &frame->page;
}

page_t *buffer_pool_get_pinned(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return NULL;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    // Check if page is in cache
    buffer_frame_t *frame = find_frame(bp, page_id);
    
    if (frame) {
        // Cache hit - pin it before releasing lock
        if (bp->config.enable_statistics) {
            bp->stats.cache_hits++;
        }
        
        // Move to head of LRU list
        lru_touch(bp, frame);
        
        // Pin the frame atomically
        if (frame->pin_count == 0) {
            bp->stats.num_pinned++;
        }
        frame->pin_count++;
        bp->stats.pins++;
        
        pthread_rwlock_unlock(&bp->lock);
        return &frame->page;
    }
    
    // Cache miss - need to load from disk
    if (bp->config.enable_statistics) {
        bp->stats.cache_misses++;
    }
    
    // Check if we need to evict
    size_t current_size = HASH_COUNT(bp->frames_hash);
    if (current_size >= bp->config.capacity) {
        if (!evict_lru_frame(bp)) {
            pthread_rwlock_unlock(&bp->lock);
            return NULL;
        }
    }
    
    // Load page from disk
    frame = load_page(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return NULL;
    }
    
    // Add to cache
    add_frame(bp, frame);
    lru_add_head(bp, frame);
    
    // Update stats
    bp->stats.num_frames = HASH_COUNT(bp->frames_hash);
    
    // Pin the newly loaded frame before releasing lock
    if (frame->pin_count == 0) {
        bp->stats.num_pinned++;
    }
    frame->pin_count++;
    bp->stats.pins++;
    
    pthread_rwlock_unlock(&bp->lock);
    return &frame->page;
}

bool buffer_pool_pin(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    if (frame->pin_count == 0) {
        bp->stats.num_pinned++;
    }
    
    frame->pin_count++;
    bp->stats.pins++;
    
    pthread_rwlock_unlock(&bp->lock);
    return true;
}

bool buffer_pool_unpin(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame || frame->pin_count == 0) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    frame->pin_count--;
    bp->stats.unpins++;
    
    if (frame->pin_count == 0 && bp->stats.num_pinned > 0) {
        bp->stats.num_pinned--;
    }
    
    pthread_rwlock_unlock(&bp->lock);
    return true;
}

bool buffer_pool_mark_dirty(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    if (!frame->is_dirty) {
        frame->is_dirty = true;
        bp->stats.num_dirty++;
    }
    
    pthread_rwlock_unlock(&bp->lock);
    return true;
}

bool buffer_pool_flush_page(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    bool result = flush_frame(bp, frame);
    
    pthread_rwlock_unlock(&bp->lock);
    return result;
}

int buffer_pool_flush_all(buffer_pool_t *bp) {
    if (!bp) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    int flushed = 0;
    buffer_frame_t *frame, *tmp;
    
    HASH_ITER(hh, bp->frames_hash, frame, tmp) {
        if (frame->is_dirty) {
            if (flush_frame(bp, frame)) {
                flushed++;
            }
        }
    }
    
    pthread_rwlock_unlock(&bp->lock);
    return flushed;
}

int buffer_pool_clear(buffer_pool_t *bp) {
    if (!bp) {
        return -1;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    int cleared = 0;
    buffer_frame_t *frame, *tmp;
    
    HASH_ITER(hh, bp->frames_hash, frame, tmp) {
        // Flush if dirty
        if (frame->is_dirty) {
            flush_frame(bp, frame);
        }
        
        // Remove from LRU list
        if (frame->lru_prev) {
            frame->lru_prev->lru_next = frame->lru_next;
        } else {
            bp->lru_head = frame->lru_next;
        }
        
        if (frame->lru_next) {
            frame->lru_next->lru_prev = frame->lru_prev;
        } else {
            bp->lru_tail = frame->lru_prev;
        }
        
        // Remove from hash table
        HASH_DEL(bp->frames_hash, frame);
        
        // Free frame
        free(frame);
        cleared++;
    }
    
    pthread_rwlock_unlock(&bp->lock);
    return cleared;
}

bool buffer_pool_evict(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    // Cannot evict pinned page
    if (frame->pin_count > 0) {
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    // Flush if dirty
    if (frame->is_dirty) {
        if (!flush_frame(bp, frame)) {
            pthread_rwlock_unlock(&bp->lock);
            return false;
        }
        bp->stats.evictions_dirty++;
    } else {
        bp->stats.evictions_clean++;
    }
    
    // Remove from cache
    lru_remove(bp, frame);
    remove_frame(bp, frame);
    free(frame);
    
    // Update stats
    bp->stats.evictions++;
    bp->stats.num_frames = HASH_COUNT(bp->frames_hash);
    
    pthread_rwlock_unlock(&bp->lock);
    return true;
}

bool buffer_pool_contains(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return false;
    }
    
    pthread_rwlock_rdlock(&bp->lock);
    buffer_frame_t *frame = find_frame(bp, page_id);
    pthread_rwlock_unlock(&bp->lock);
    
    return frame != NULL;
}

bool buffer_pool_invalidate(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return true;  // No buffer pool, nothing to invalidate
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        // Page not in cache, nothing to do
        LOG_DEBUG("[BP_INVALIDATE] Page %lu not in cache (already evicted)", page_id);
        pthread_rwlock_unlock(&bp->lock);
        return true;
    }
    
    LOG_DEBUG("[BP_INVALIDATE] Found page %lu in cache (dirty=%d, pin_count=%u)",
              page_id, frame->is_dirty, frame->pin_count);
    
    // Check if page is pinned
    if (frame->pin_count > 0) {
        LOG_WARN("Cannot invalidate pinned page %lu (pin_count=%u)", 
                 page_id, frame->pin_count);
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    // Remove from LRU list and hash table
    lru_remove(bp, frame);
    remove_frame(bp, frame);
    free(frame);
    
    // Update stats
    bp->stats.num_frames = HASH_COUNT(bp->frames_hash);
    
    pthread_rwlock_unlock(&bp->lock);
    
    LOG_DEBUG("[BP_INVALIDATE] Successfully removed page %lu from buffer pool", page_id);
    return true;
}

bool buffer_pool_reload(buffer_pool_t *bp, uint64_t page_id) {
    if (!bp) {
        return true;  // No buffer pool, nothing to reload
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    buffer_frame_t *frame = find_frame(bp, page_id);
    if (!frame) {
        // Page not in cache, nothing to reload
        pthread_rwlock_unlock(&bp->lock);
        return true;
    }
    
    // Reload page from disk
    page_result_t result = page_manager_read(bp->page_manager, page_id, &frame->page);
    if (result != PAGE_SUCCESS) {
        LOG_ERROR("Failed to reload page %lu from disk", page_id);
        pthread_rwlock_unlock(&bp->lock);
        return false;
    }
    
    // Clear dirty flag since we just loaded fresh data
    frame->is_dirty = false;
    
    // Update last access time
    frame->last_access_time = (uint64_t)time(NULL);
    
    pthread_rwlock_unlock(&bp->lock);
    
    LOG_DEBUG("Reloaded page %lu from disk into buffer pool", page_id);
    return true;
}

void buffer_pool_get_stats(buffer_pool_t *bp, buffer_pool_stats_t *stats_out) {
    if (!bp || !stats_out) {
        return;
    }
    
    pthread_rwlock_rdlock(&bp->lock);
    
    // Copy stats
    *stats_out = bp->stats;
    
    // Calculate derived metrics
    stats_out->num_frames = HASH_COUNT(bp->frames_hash);
    stats_out->utilization = (double)stats_out->num_frames / stats_out->capacity;
    
    uint64_t total_accesses = stats_out->cache_hits + stats_out->cache_misses;
    if (total_accesses > 0) {
        stats_out->cache_hit_rate = (double)stats_out->cache_hits / total_accesses;
    } else {
        stats_out->cache_hit_rate = 0.0;
    }
    
    pthread_rwlock_unlock(&bp->lock);
}

void buffer_pool_print_stats(buffer_pool_t *bp) {
    if (!bp) {
        return;
    }
    
    buffer_pool_stats_t stats;
    buffer_pool_get_stats(bp, &stats);
    
    printf("\n=== Buffer Pool Statistics ===\n");
    printf("Cache Performance:\n");
    printf("  Hits:          %lu\n", stats.cache_hits);
    printf("  Misses:        %lu\n", stats.cache_misses);
    printf("  Hit Rate:      %.2f%%\n", stats.cache_hit_rate * 100.0);
    
    printf("\nEviction:\n");
    printf("  Total:         %lu\n", stats.evictions);
    printf("  Clean:         %lu\n", stats.evictions_clean);
    printf("  Dirty:         %lu\n", stats.evictions_dirty);
    
    printf("\nMemory Usage:\n");
    printf("  Frames:        %zu / %zu\n", stats.num_frames, stats.capacity);
    printf("  Utilization:   %.2f%%\n", stats.utilization * 100.0);
    printf("  Dirty:         %zu\n", stats.num_dirty);
    printf("  Pinned:        %zu\n", stats.num_pinned);
    
    printf("\nI/O Operations:\n");
    printf("  Loads:         %lu\n", stats.total_loads);
    printf("  Flushes:       %lu\n", stats.total_flushes);
    
    printf("\nPin/Unpin:\n");
    printf("  Pins:          %lu\n", stats.pins);
    printf("  Unpins:        %lu\n", stats.unpins);
    printf("==============================\n\n");
}

void buffer_pool_reset_stats(buffer_pool_t *bp) {
    if (!bp) {
        return;
    }
    
    pthread_rwlock_wrlock(&bp->lock);
    
    // Keep capacity, reset counters
    size_t capacity = bp->stats.capacity;
    memset(&bp->stats, 0, sizeof(buffer_pool_stats_t));
    bp->stats.capacity = capacity;
    
    pthread_rwlock_unlock(&bp->lock);
}

size_t buffer_pool_memory_usage(buffer_pool_t *bp) {
    if (!bp) {
        return 0;
    }
    
    pthread_rwlock_rdlock(&bp->lock);
    
    // Approximate memory usage
    size_t num_frames = HASH_COUNT(bp->frames_hash);
    size_t usage = sizeof(buffer_pool_t) +
                   num_frames * sizeof(buffer_frame_t);
    
    pthread_rwlock_unlock(&bp->lock);
    
    return usage;
}
