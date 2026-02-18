/**
 * Page Garbage Collection Implementation
 * 
 * Reference counting and dead page tracking for append-only storage.
 * Works with variable-size compressed pages and page index architecture.
 */

#include "page_gc.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

// ============================================================================
// Reference Counting
// ============================================================================

bool page_gc_init_ref(page_manager_t *pm, uint64_t page_id) {
    // Find page in index
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        LOG_ERROR("Page %lu not found in index", page_id);
        return false;
    }
    
    // Initialize ref_count to 1 (caller holds initial reference)
    __atomic_store_n(&meta->ref_count, 1, __ATOMIC_RELEASE);
    
    // Clear any flags
    meta->flags = 0;
    
    // Set last access time
    meta->last_access_time = (uint64_t)time(NULL);
    
    LOG_DEBUG("Initialized ref_count=1 for page %lu", page_id);
    return true;
}

uint32_t page_gc_incref(page_manager_t *pm, uint64_t page_id) {
    // Find page in index
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        LOG_ERROR("Page %lu not found in index", page_id);
        return 0;
    }
    
    // Atomic increment
    uint32_t new_count = __atomic_add_fetch(&meta->ref_count, 1, __ATOMIC_ACQ_REL);
    
    // Update last access time
    meta->last_access_time = (uint64_t)time(NULL);
    
    LOG_DEBUG("Incremented ref_count for page %lu: %u -> %u", 
              page_id, new_count - 1, new_count);
    
    return new_count;
}

uint32_t page_gc_decref(page_manager_t *pm, uint64_t page_id) {
    // Find page in index
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        LOG_ERROR("Page %lu not found in index", page_id);
        return 0;
    }
    
    // Check current count
    uint32_t old_count = __atomic_load_n(&meta->ref_count, __ATOMIC_ACQUIRE);
    if (old_count == 0) {
        LOG_WARN("Attempted to decref page %lu with ref_count=0", page_id);
        return 0;
    }
    
    // Atomic decrement
    uint32_t new_count = __atomic_sub_fetch(&meta->ref_count, 1, __ATOMIC_ACQ_REL);
    
    LOG_DEBUG("Decremented ref_count for page %lu: %u -> %u", 
              page_id, old_count, new_count);
    
    // If reached zero, mark as dead
    if (new_count == 0) {
        page_gc_mark_dead(pm, page_id);
    }
    
    return new_count;
}

uint32_t page_gc_get_refcount(page_manager_t *pm, uint64_t page_id) {
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        return 0;
    }
    
    return __atomic_load_n(&meta->ref_count, __ATOMIC_ACQUIRE);
}

// ============================================================================
// Dead Page Tracking
// ============================================================================

bool page_gc_mark_dead(page_manager_t *pm, uint64_t page_id) {
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        LOG_ERROR("Page %lu not found in index", page_id);
        return false;
    }
    
    // Verify ref_count is actually 0
    uint32_t ref_count = __atomic_load_n(&meta->ref_count, __ATOMIC_ACQUIRE);
    if (ref_count != 0) {
        LOG_WARN("Attempted to mark page %lu as dead with ref_count=%u", 
                 page_id, ref_count);
        return false;
    }
    
    // Set DEAD flag
    meta->flags |= PAGE_GC_FLAG_DEAD;
    
    LOG_INFO("Marked page %lu as dead (size=%u bytes)", 
             page_id, meta->compressed_size);
    
    // Update statistics in page manager
    page_manager_update_dead_stats(pm, meta->compressed_size);
    
    return true;
}

bool page_gc_is_dead(page_manager_t *pm, uint64_t page_id) {
    page_gc_metadata_t *meta = page_manager_get_metadata(pm, page_id);
    if (!meta) {
        return false;
    }
    
    return (meta->flags & PAGE_GC_FLAG_DEAD) != 0;
}

void page_gc_get_dead_stats(page_manager_t *pm, dead_page_stats_t *stats_out) {
    if (!stats_out) {
        return;
    }
    
    memset(stats_out, 0, sizeof(dead_page_stats_t));
    
    // Scan page index to count live/dead pages
    size_t num_pages = page_manager_get_num_pages(pm);
    stats_out->total_pages = num_pages;
    
    for (size_t i = 0; i < num_pages; i++) {
        page_gc_metadata_t *meta = page_manager_get_metadata_by_index(pm, i);
        if (!meta) {
            continue;
        }
        
        if (meta->flags & PAGE_GC_FLAG_DEAD) {
            stats_out->dead_pages++;
            stats_out->dead_bytes += meta->compressed_size;
        } else {
            stats_out->live_pages++;
        }
    }
    
    // Calculate fragmentation percentage
    uint64_t total_bytes = page_manager_get_total_file_size(pm);
    if (total_bytes > 0) {
        stats_out->fragmentation_pct = 
            (double)stats_out->dead_bytes / total_bytes * 100.0;
    }
    
    LOG_INFO("Dead page stats: total=%lu, live=%lu, dead=%lu, "
             "dead_bytes=%lu, fragmentation=%.1f%%",
             stats_out->total_pages,
             stats_out->live_pages,
             stats_out->dead_pages,
             stats_out->dead_bytes,
             stats_out->fragmentation_pct);
}

uint64_t *page_gc_collect_dead_pages(page_manager_t *pm, size_t *count_out) {
    if (!count_out) {
        return NULL;
    }
    
    *count_out = 0;
    
    // First pass: count dead pages
    size_t num_pages = page_manager_get_num_pages(pm);
    size_t dead_count = 0;
    
    for (size_t i = 0; i < num_pages; i++) {
        page_gc_metadata_t *meta = page_manager_get_metadata_by_index(pm, i);
        if (meta && (meta->flags & PAGE_GC_FLAG_DEAD)) {
            dead_count++;
        }
    }
    
    if (dead_count == 0) {
        LOG_INFO("No dead pages found");
        return NULL;
    }
    
    // Allocate array for dead page IDs
    uint64_t *dead_pages = malloc(dead_count * sizeof(uint64_t));
    if (!dead_pages) {
        LOG_ERROR("Failed to allocate memory for %zu dead pages", dead_count);
        return NULL;
    }
    
    // Second pass: collect dead page IDs
    size_t idx = 0;
    for (size_t i = 0; i < num_pages; i++) {
        page_gc_metadata_t *meta = page_manager_get_metadata_by_index(pm, i);
        if (meta && (meta->flags & PAGE_GC_FLAG_DEAD)) {
            dead_pages[idx++] = meta->page_id;
        }
    }
    
    *count_out = dead_count;
    
    LOG_INFO("Collected %zu dead pages for compaction", dead_count);
    
    return dead_pages;
}
