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
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
        const uint8_t *value_start = leaf->data + leaf->key_len;
        size_t value_size = leaf->value_len;
        memcpy(value_out, value_start, value_size);
        return true;
    }
    
    // Copy inline portion first
    const uint8_t *value_start = leaf->data + leaf->key_len;
    size_t inline_value_size = leaf->inline_data_len - leaf->key_len;
    memcpy(value_out, value_start, inline_value_size);
    
    // Traverse overflow chain to get remainder
    uint8_t *out_ptr = (uint8_t *)value_out + inline_value_size;
    size_t remaining = leaf->value_len - inline_value_size;
    uint64_t current_page = leaf->overflow_page;
    
    while (current_page != 0 && remaining > 0) {
        tree->overflow_chain_reads++;
        
        // Load overflow page
        const data_art_overflow_t *overflow = 
            (const data_art_overflow_t *)data_art_load_node(tree, 
                (node_ref_t){.page_id = current_page, .offset = 0});
        
        if (!overflow) {
            LOG_ERROR("Failed to load overflow page %lu", current_page);
            return false;
        }
        
        if (overflow->type != DATA_NODE_OVERFLOW) {
            LOG_ERROR("Invalid overflow page type: %d", overflow->type);
            return false;
        }
        
        // Copy data from this overflow page
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
        // Everything fits inline, no overflow needed
        return 0;
    }
    
    const uint8_t *data_ptr = (const uint8_t *)value + inline_size;
    size_t remaining = value_len - inline_size;
    
    uint64_t first_page = 0;
    uint64_t prev_page = 0;
    
    while (remaining > 0) {
        // Allocate overflow page - use fixed overflow structure size
        size_t overflow_size = sizeof(data_art_overflow_t);
        node_ref_t ref = data_art_alloc_node(tree, overflow_size);
        if (node_ref_is_null(ref)) {
            LOG_ERROR("Failed to allocate overflow page");
            return 0;
        }
        
        tree->overflow_pages_allocated++;
        
        if (first_page == 0) {
            first_page = ref.page_id;
        }
        
        // Create overflow page structure
        data_art_overflow_t overflow;
        overflow.type = DATA_NODE_OVERFLOW;
        memset(overflow.padding, 0, sizeof(overflow.padding));
        overflow.next_page = 0;  // Will be updated if more pages needed
        
        // Copy data
        size_t to_copy = (remaining < sizeof(overflow.data)) ? 
                         remaining : sizeof(overflow.data);
        overflow.data_len = to_copy;
        memcpy(overflow.data, data_ptr, to_copy);
        
        // Zero remaining space for cleaner debugging
        if (to_copy < sizeof(overflow.data)) {
            memset(overflow.data + to_copy, 0, sizeof(overflow.data) - to_copy);
        }
        
        // Write overflow page
        if (!data_art_write_node(tree, ref, &overflow, sizeof(overflow))) {
            LOG_ERROR("Failed to write overflow page");
            return 0;
        }
        
        // Link previous page to this one
        if (prev_page != 0) {
            // Update previous page's next_page pointer
            // CRITICAL: Load and COPY immediately before any other operations
            // that might call data_art_load_node and invalidate the pointer
            const data_art_overflow_t *prev_overflow = 
                (const data_art_overflow_t *)data_art_load_node(tree,
                    (node_ref_t){.page_id = prev_page, .offset = 0});
            
            if (prev_overflow) {
                // IMPORTANT: Copy immediately to avoid stale pointer issues
                data_art_overflow_t updated = *prev_overflow;
                // Now safe to do other operations that might call data_art_load_node
                updated.next_page = ref.page_id;
                data_art_write_node(tree, 
                    (node_ref_t){.page_id = prev_page, .offset = 0},
                    &updated, sizeof(updated));
            }
        }
        
        prev_page = ref.page_id;
        data_ptr += to_copy;
        remaining -= to_copy;
    }
    
    return first_page;
}

size_t data_art_free_overflow_chain(data_art_tree_t *tree, uint64_t first_page) {
    if (!tree || first_page == 0) {
        return 0;
    }
    
    size_t freed = 0;
    uint64_t current_page = first_page;
    
    while (current_page != 0) {
        // Load overflow page to get next pointer
        const data_art_overflow_t *overflow = 
            (const data_art_overflow_t *)data_art_load_node(tree,
                (node_ref_t){.page_id = current_page, .offset = 0});
        
        if (!overflow) {
            LOG_ERROR("Failed to load overflow page %lu during free", current_page);
            break;
        }
        
        // Verify it's actually an overflow page
        if (overflow->type != DATA_NODE_OVERFLOW) {
            LOG_ERROR("Expected overflow page at %lu but got type %u", 
                     current_page, overflow->type);
            break;
        }
        
        uint64_t next_page = overflow->next_page;
        
        // Release current page using reference counting
        extern void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);
        data_art_release_page(tree, (node_ref_t){.page_id = current_page, .offset = 0});
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
// Versioning & Snapshots
// ============================================================================

uint64_t data_art_snapshot(data_art_tree_t *tree) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return 0;
    }
    
    // Enable CoW if not already enabled
    if (!tree->cow_enabled) {
        tree->cow_enabled = true;
        LOG_INFO("Enabled copy-on-write for versioning");
    }
    
    uint64_t snapshot_version = tree->version;
    
    // Add to active versions array
    size_t new_size = tree->num_active_versions + 1;
    uint64_t *new_array = realloc(tree->active_versions, 
                                   new_size * sizeof(uint64_t));
    if (!new_array) {
        LOG_ERROR("Failed to allocate active versions array");
        return 0;
    }
    
    tree->active_versions = new_array;
    tree->active_versions[tree->num_active_versions] = snapshot_version;
    tree->num_active_versions = new_size;
    
    // Increment version for future modifications
    tree->version++;
    
    LOG_INFO("Created snapshot version %lu", snapshot_version);
    
    return snapshot_version;
}

void data_art_release_version(data_art_tree_t *tree, uint64_t version) {
    if (!tree || !tree->active_versions) {
        return;
    }
    
    // Find and remove version from active array
    for (size_t i = 0; i < tree->num_active_versions; i++) {
        if (tree->active_versions[i] == version) {
            // Shift remaining versions down
            memmove(&tree->active_versions[i], 
                    &tree->active_versions[i + 1],
                    (tree->num_active_versions - i - 1) * sizeof(uint64_t));
            tree->num_active_versions--;
            
            LOG_INFO("Released snapshot version %lu", version);
            
            // If no active versions, disable CoW
            if (tree->num_active_versions == 0) {
                free(tree->active_versions);
                tree->active_versions = NULL;
                tree->cow_enabled = false;
                LOG_INFO("Disabled copy-on-write (no active snapshots)");
            }
            
            return;
        }
    }
    
    LOG_ERROR("Version %lu not found in active versions", version);
}

// ============================================================================
// Persistence & Recovery
// ============================================================================

bool data_art_flush(data_art_tree_t *tree) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return false;
    }
    
    if (!tree->buffer_pool) {
        // No buffer pool, assume direct writes are already persistent
        return true;
    }
    
    // Flush all dirty pages in buffer pool
    if (!buffer_pool_flush_all(tree->buffer_pool)) {
        LOG_ERROR("Failed to flush buffer pool");
        return false;
    }
    
    LOG_INFO("Flushed ART tree to disk");
    return true;
}

data_art_tree_t *data_art_load(page_manager_t *page_manager,
                                 buffer_pool_t *buffer_pool,
                                 size_t key_size,
                                 node_ref_t root_ref) {
    if (!page_manager) {
        LOG_ERROR("page_manager cannot be NULL");
        return NULL;
    }
    
    data_art_tree_t *tree = data_art_create(page_manager, buffer_pool, NULL, key_size);
    if (!tree) {
        return NULL;
    }
    
    tree->root = root_ref;

    // Publish loaded root for lock-free readers
    extern void data_art_publish_root(data_art_tree_t *tree);
    data_art_publish_root(tree);

    // TODO: Traverse tree to calculate size and statistics
    // For now, assume metadata will be provided separately

    LOG_INFO("Loaded ART tree from disk (root page=%lu, offset=%u)",
             root_ref.page_id, root_ref.offset);

    return tree;
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
