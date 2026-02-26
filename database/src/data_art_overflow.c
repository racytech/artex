/*
 * Persistent ART - Overflow Pages, Versioning, and Utilities
 * 
 * Implements:
 * - Overflow page operations for large values
 * - Versioning and snapshot support
 * - Persistence and recovery
 * - Statistics and debugging utilities
 * 
 * This file complements data_art_core.c which handles the main tree operations.
 */

#include "data_art.h"
#include "db_error.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Forward declarations from data_art_core.c
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern void *data_art_lock_node_mut(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_unlock_node_mut(data_art_tree_t *tree);

// ============================================================================
// Overflow Page Operations
// ============================================================================

bool data_art_read_overflow_value(data_art_tree_t *tree, 
                                   const data_art_leaf_t *leaf,
                                   void *value_out) {
    if (!tree || !leaf || !value_out) {
        LOG_ERROR("Invalid parameters");
        return false;
    }
    
    if (!(leaf->flags & LEAF_FLAG_OVERFLOW)) {
        // No overflow, just copy inline data
        const uint8_t *value_start = leaf_key(leaf) + tree->key_size;
        memcpy(value_out, value_start, leaf->value_len);
        return true;
    }

    // Copy inline portion first
    const uint8_t *value_start = leaf_key(leaf) + tree->key_size;
    size_t inline_value_size = (PAGE_SIZE - PAGE_HEADER_SIZE) - sizeof(data_art_leaf_t) - 8 - tree->key_size;
    memcpy(value_out, value_start, inline_value_size);

    // Traverse overflow chain to get remainder (direct mmap access)
    uint8_t *out_ptr = (uint8_t *)value_out + inline_value_size;
    size_t remaining = leaf->value_len - inline_value_size;
    uint64_t current_page = leaf_overflow_page(leaf);

    while (current_page != 0 && remaining > 0) {
        tree->overflow_chain_reads++;

        const page_t *page = mmap_storage_get_page(tree->mmap_storage, current_page);
        const data_art_overflow_t *overflow = (const data_art_overflow_t *)page->data;

        size_t to_copy = (overflow->data_len < remaining) ?
                         overflow->data_len : remaining;
        memcpy(out_ptr, overflow->data, to_copy);

        out_ptr += to_copy;
        remaining -= to_copy;
        current_page = overflow->next_page;
    }
    
    if (remaining > 0) {
        LOG_ERROR("Overflow chain ended prematurely, %zu bytes remaining", remaining);
        return false;
    }
    
    return true;
}

uint64_t data_art_write_overflow_value(data_art_tree_t *tree,
                                        const void *value,
                                        size_t value_len,
                                        size_t inline_size) {
    if (!tree || !value) {
        LOG_ERROR("Invalid parameters");
        return 0;
    }

    if (value_len <= inline_size) {
        return 0;
    }

    size_t remaining = value_len - inline_size;
    size_t data_per_page = sizeof(((data_art_overflow_t *)0)->data);

    // Count pages needed
    size_t num_pages = (remaining + data_per_page - 1) / data_per_page;

    // Phase 1: Allocate all pages upfront into a small stack array (max ~6 for 24KB)
    // For values > 24KB we'd need 7+ pages; use a reasonable stack limit.
    #define MAX_OVERFLOW_STACK 16
    uint64_t page_ids_stack[MAX_OVERFLOW_STACK];
    uint64_t *page_ids = page_ids_stack;
    if (num_pages > MAX_OVERFLOW_STACK) {
        page_ids = malloc(num_pages * sizeof(uint64_t));
        if (!page_ids) {
            LOG_ERROR("Failed to allocate overflow page_ids array");
            return 0;
        }
    }

    for (size_t i = 0; i < num_pages; i++) {
        node_ref_t ref = data_art_alloc_node(tree, sizeof(data_art_overflow_t));
        if (node_ref_is_null(ref)) {
            LOG_ERROR("Failed to allocate overflow page %zu/%zu", i, num_pages);
            if (page_ids != page_ids_stack) free(page_ids);
            return 0;
        }
        page_ids[i] = node_ref_page_id(ref);
        tree->overflow_pages_allocated++;
    }

    // Phase 2: Write forward — next_page is already known, write directly to mmap
    const uint8_t *data_ptr = (const uint8_t *)value + inline_size;

    for (size_t i = 0; i < num_pages; i++) {
        size_t to_copy = (remaining < data_per_page) ? remaining : data_per_page;

        // Get direct mmap pointer (marks page dirty)
        data_art_overflow_t *ovf = (data_art_overflow_t *)
            data_art_lock_node_mut(tree, node_ref_make(page_ids[i], 0));

        ovf->type = DATA_NODE_OVERFLOW;
        memset(ovf->padding, 0, sizeof(ovf->padding));
        ovf->next_page = (i + 1 < num_pages) ? page_ids[i + 1] : 0;
        ovf->data_len = (uint32_t)to_copy;
        memcpy(ovf->data, data_ptr, to_copy);

        data_art_unlock_node_mut(tree);

        data_ptr += to_copy;
        remaining -= to_copy;
    }

    uint64_t first_page = page_ids[0];
    if (page_ids != page_ids_stack) free(page_ids);
    return first_page;
}

size_t data_art_free_overflow_chain(data_art_tree_t *tree, uint64_t first_page) {
    if (!tree || first_page == 0) {
        return 0;
    }
    
    size_t freed = 0;
    uint64_t current_page = first_page;
    
    while (current_page != 0) {
        const page_t *page = mmap_storage_get_page(tree->mmap_storage, current_page);
        const data_art_overflow_t *overflow = (const data_art_overflow_t *)page->data;
        uint64_t next_page = overflow->next_page;
        
        // Release current page using reference counting
        extern void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);
        data_art_release_page(tree, node_ref_make(current_page, 0));
        freed++;
        
        LOG_DEBUG("[FREE_OVERFLOW] Released overflow page %lu (next=%lu)", 
                 current_page, next_page);
        
        current_page = next_page;
    }
    
    LOG_DEBUG("[FREE_OVERFLOW_CHAIN] Freed %zu overflow pages starting from %lu",
             freed, first_page);
    
    return freed;
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

bool data_art_flush(data_art_tree_t *tree) {
    if (!tree) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree is NULL");
        return false;
    }

    return mmap_storage_sync(tree->mmap_storage);
}

node_ref_t data_art_get_root(const data_art_tree_t *tree) {
    return tree ? tree->root : NULL_NODE_REF;
}

// ============================================================================
// Statistics & Debugging
// ============================================================================

void data_art_get_stats(const data_art_tree_t *tree, data_art_stats_t *stats) {
    if (!tree || !stats) {
        return;
    }
    
    memset(stats, 0, sizeof(data_art_stats_t));
    
    stats->num_entries = tree->size;
    stats->version = tree->version;
    stats->nodes_allocated = tree->nodes_allocated;
    stats->nodes_copied = tree->nodes_copied;
    stats->cache_hits = tree->cache_hits;
    stats->cache_misses = tree->cache_misses;
    
    // Calculate hit rate
    uint64_t total_accesses = tree->cache_hits + tree->cache_misses;
    stats->cache_hit_rate = (total_accesses > 0) ? 
        (double)tree->cache_hits / total_accesses : 0.0;
    
    // Overflow statistics
    stats->overflow_pages_allocated = tree->overflow_pages_allocated;
    stats->overflow_chain_reads = tree->overflow_chain_reads;
    
    // TODO: Traverse tree to count node types and calculate accurate stats
    // For now, these fields will be 0
    stats->num_node4 = 0;
    stats->num_node16 = 0;
    stats->num_node48 = 0;
    stats->num_node256 = 0;
    stats->num_leaves = 0;
    stats->num_leaves_with_overflow = 0;
    stats->total_overflow_bytes = 0;

    // Slot allocator statistics
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        stats->slot_pages_created[i] = tree->slot_pages_created[i];
        stats->slot_allocs[i] = tree->slot_allocs[i];
        stats->slot_frees[i] = tree->slot_frees[i];
    }
    stats->slot_hint_hits = tree->slot_hint_hits;
    stats->slot_hint_misses = tree->slot_hint_misses;
    stats->dedicated_pages_created = tree->dedicated_pages_created;
    stats->pages_reused = tree->pages_reused;
}

void data_art_print_stats(const data_art_tree_t *tree) {
    if (!tree) {
        return;
    }
    
    data_art_stats_t stats;
    data_art_get_stats(tree, &stats);
    
    printf("\n=== Persistent ART Statistics ===\n");
    printf("Entries:           %zu\n", stats.num_entries);
    printf("Version:           %lu\n", stats.version);
    printf("Nodes allocated:   %lu\n", stats.nodes_allocated);
    printf("Nodes copied (CoW): %lu\n", stats.nodes_copied);
    printf("\n--- Cache Performance ---\n");
    printf("Cache hits:        %lu\n", stats.cache_hits);
    printf("Cache misses:      %lu\n", stats.cache_misses);
    printf("Hit rate:          %.2f%%\n", stats.cache_hit_rate * 100.0);
    printf("\n--- Overflow Pages ---\n");
    printf("Overflow pages:    %lu\n", stats.overflow_pages_allocated);
    printf("Chain reads:       %lu\n", stats.overflow_chain_reads);
    printf("\n--- Node Distribution ---\n");
    printf("NODE_4:            %zu\n", stats.num_node4);
    printf("NODE_16:           %zu\n", stats.num_node16);
    printf("NODE_48:           %zu\n", stats.num_node48);
    printf("NODE_256:          %zu\n", stats.num_node256);
    printf("Leaves:            %zu\n", stats.num_leaves);
    printf("  With overflow:   %zu\n", stats.num_leaves_with_overflow);
    printf("===================================\n\n");
}

bool data_art_verify(data_art_tree_t *tree) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return false;
    }
    
    // TODO: Implement tree integrity verification:
    // - Check all node types are valid
    // - Verify parent-child relationships
    // - Check that all page references are valid
    // - Verify overflow chains are properly terminated
    // - Check path compression invariants
    
    LOG_ERROR("data_art_verify not yet implemented");
    return false;
}
