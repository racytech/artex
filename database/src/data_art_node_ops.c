/*
 * Persistent ART - Node Growth and Shrinking Operations
 * 
 * Implements node capacity transitions:
 * - Growth: NODE_4 → NODE_16 → NODE_48 → NODE_256
 * - Shrinking: NODE_256 → NODE_48 → NODE_16 → NODE_4
 */

#include "data_art.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>

// Forward declarations
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                                  const void *node, size_t size);
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);
extern void *data_art_lock_node_mut(data_art_tree_t *tree, node_ref_t ref);
extern void data_art_unlock_node_mut(data_art_tree_t *tree);
extern bool data_art_write_partial(data_art_tree_t *tree, node_ref_t ref,
                                    size_t node_offset, const void *data, size_t len);
extern bool data_art_copy_node(data_art_tree_t *tree, node_ref_t dst, node_ref_t src, size_t size);

// NODE48_EMPTY is defined in data_art.h

// Forward declarations for add_child functions
static node_ref_t add_child_node4(data_art_tree_t *tree, node_ref_t node_ref,
                                   const data_art_node4_t *n4, uint8_t byte, node_ref_t child_ref);
static node_ref_t add_child_node16(data_art_tree_t *tree, node_ref_t node_ref,
                                    const data_art_node16_t *n16, uint8_t byte, node_ref_t child_ref);
static node_ref_t add_child_node48(data_art_tree_t *tree, node_ref_t node_ref,
                                    const data_art_node48_t *n48, uint8_t byte, node_ref_t child_ref);
static node_ref_t add_child_node256(data_art_tree_t *tree, node_ref_t node_ref,
                                     const data_art_node256_t *n256, uint8_t byte, node_ref_t child_ref);

// ============================================================================
// Node Growth Functions
// ============================================================================

/**
 * Grow NODE_4 to NODE_16
 */
static node_ref_t grow_node4_to_node16(data_art_tree_t *tree, node_ref_t old_ref,
                                        const data_art_node4_t *n4) {
    // Allocate new NODE_16
    node_ref_t new_ref = data_art_alloc_node(tree, sizeof(data_art_node16_t));
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate NODE_16");
        return NULL_NODE_REF;
    }
    
    // Create NODE_16 structure
    data_art_node16_t n16;
    memset(&n16, 0, sizeof(n16));
    
    n16.type = DATA_NODE_16;
    n16.num_children = n4->num_children;
    n16.partial_len = n4->partial_len;
    memcpy(n16.partial, n4->partial, 10);
    
    // Copy children (sorted)
    for (int i = 0; i < n4->num_children; i++) {
        n16.keys[i] = n4->keys[i];
        n16.child_page_ids[i] = n4->child_page_ids[i];
        n16.child_offsets[i] = n4->child_offsets[i];
    }
    
    // Write to disk
    if (!data_art_write_node(tree, new_ref, &n16, sizeof(n16))) {
        LOG_ERROR("Failed to write NODE_16");
        return NULL_NODE_REF;
    }
    
    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, old_ref);
    
    LOG_DEBUG("Grew NODE_4 to NODE_16");
    return new_ref;
}

/**
 * Grow NODE_16 to NODE_48
 */
static node_ref_t grow_node16_to_node48(data_art_tree_t *tree, node_ref_t old_ref,
                                         const data_art_node16_t *n16) {
    // Allocate new NODE_48
    node_ref_t new_ref = data_art_alloc_node(tree, sizeof(data_art_node48_t));
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate NODE_48");
        return NULL_NODE_REF;
    }
    
    // Create NODE_48 structure
    data_art_node48_t n48;
    memset(&n48, 0, sizeof(n48));
    
    n48.type = DATA_NODE_48;
    n48.num_children = n16->num_children;
    n48.partial_len = n16->partial_len;
    memcpy(n48.partial, n16->partial, 10);
    
    // Initialize index array to empty
    memset(n48.keys, NODE48_EMPTY, 256);
    
    // Copy children using index array
    for (int i = 0; i < n16->num_children; i++) {
        uint8_t byte = n16->keys[i];
        n48.keys[byte] = i;  // Index into child arrays
        n48.child_page_ids[i] = n16->child_page_ids[i];
        n48.child_offsets[i] = n16->child_offsets[i];
    }
    
    // Write to disk
    if (!data_art_write_node(tree, new_ref, &n48, sizeof(n48))) {
        LOG_ERROR("Failed to write NODE_48");
        return NULL_NODE_REF;
    }
    
    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, old_ref);
    
    LOG_DEBUG("Grew NODE_16 to NODE_48");
    return new_ref;
}

/**
 * Grow NODE_48 to NODE_256
 */
static node_ref_t grow_node48_to_node256(data_art_tree_t *tree, node_ref_t old_ref,
                                          const data_art_node48_t *n48) {
    // Allocate new NODE_256
    node_ref_t new_ref = data_art_alloc_node(tree, sizeof(data_art_node256_t));
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate NODE_256");
        return NULL_NODE_REF;
    }
    
    // Create NODE_256 structure
    data_art_node256_t n256;
    memset(&n256, 0, sizeof(n256));
    
    n256.type = DATA_NODE_256;
    n256.num_children = n48->num_children;
    n256.partial_len = n48->partial_len;
    memcpy(n256.partial, n48->partial, 10);
    
    // Copy children to direct index
    for (int i = 0; i < 256; i++) {
        uint8_t idx = n48->keys[i];
        if (idx != NODE48_EMPTY) {
            n256.child_page_ids[i] = n48->child_page_ids[idx];
            n256.child_offsets[i] = n48->child_offsets[idx];
        }
    }
    
    // Write to disk
    if (!data_art_write_node(tree, new_ref, &n256, sizeof(n256))) {
        LOG_ERROR("Failed to write NODE_256");
        return NULL_NODE_REF;
    }
    
    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, old_ref);
    
    LOG_DEBUG("Grew NODE_48 to NODE_256");
    return new_ref;
}

// ============================================================================
// Add/Remove Child with Growth/Shrinking
// ============================================================================

/**
 * Add child to NODE_4 (grow to NODE_16 if full)
 */
static node_ref_t add_child_node4(data_art_tree_t *tree, node_ref_t node_ref,
                                   const data_art_node4_t *n4, uint8_t byte, node_ref_t child_ref) {
    // Check for duplicate key first
    for (int i = 0; i < n4->num_children; i++) {
        if (n4->keys[i] == byte) {
            LOG_ERROR("BUG: Attempted to add duplicate child byte=0x%02x to NODE_4 at page=%lu (already has %u children)", 
                     byte, node_ref.page_id, n4->num_children);
            LOG_ERROR("  Existing child at index %d points to page=%lu", i, n4->child_page_ids[i]);
            LOG_ERROR("  New child would point to page=%lu", child_ref.page_id);
            // Return error to prevent corruption
            return NULL_NODE_REF;
        }
    }
    
    // If full, grow to NODE_16
    if (n4->num_children >= 4) {
        node_ref_t n16_ref = grow_node4_to_node16(tree, node_ref, n4);
        if (node_ref_is_null(n16_ref)) {
            return NULL_NODE_REF;
        }
        
        // Load the new NODE_16 and add child to it
        const data_art_node16_t *n16 = 
            (const data_art_node16_t *)data_art_load_node(tree, n16_ref);
        if (!n16) {
            return NULL_NODE_REF;
        }
        
        return add_child_node16(tree, n16_ref, n16, byte, child_ref);
    }
    
    // Add to NODE_4 (keep sorted)
    data_art_node4_t new_n4 = *n4;
    
    // Find insertion position
    int pos = 0;
    while (pos < new_n4.num_children && new_n4.keys[pos] < byte) {
        pos++;
    }
    
    // Shift elements
    if (pos < new_n4.num_children) {
        memmove(&new_n4.keys[pos + 1], &new_n4.keys[pos], 
                new_n4.num_children - pos);
        memmove(&new_n4.child_page_ids[pos + 1], &new_n4.child_page_ids[pos],
                (new_n4.num_children - pos) * sizeof(uint64_t));
        memmove(&new_n4.child_offsets[pos + 1], &new_n4.child_offsets[pos],
                (new_n4.num_children - pos) * sizeof(uint32_t));
    }
    
    // Insert
    new_n4.keys[pos] = byte;
    new_n4.child_page_ids[pos] = child_ref.page_id;
    new_n4.child_offsets[pos] = child_ref.offset;
    new_n4.num_children++;
    
    // CoW: allocate new page (hint = old page for same-page COW), write there, release old
    node_ref_t new_ref = data_art_alloc_node_hint(tree, sizeof(data_art_node4_t), node_ref.page_id);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate new NODE_4 for CoW");
        return NULL_NODE_REF;
    }
    if (!data_art_write_node(tree, new_ref, &new_n4, sizeof(new_n4))) {
        LOG_ERROR("Failed to write NODE_4");
        return NULL_NODE_REF;
    }
    data_art_release_page(tree, node_ref);

    return new_ref;
}

/**
 * Add child to NODE_16 (grow to NODE_48 if full)
 */
static node_ref_t add_child_node16(data_art_tree_t *tree, node_ref_t node_ref,
                                    const data_art_node16_t *n16, uint8_t byte, node_ref_t child_ref) {
    // If full, grow to NODE_48
    if (n16->num_children >= 16) {
        node_ref_t n48_ref = grow_node16_to_node48(tree, node_ref, n16);
        if (node_ref_is_null(n48_ref)) {
            return NULL_NODE_REF;
        }
        
        // Load the new NODE_48 and add child to it
        const data_art_node48_t *n48 = 
            (const data_art_node48_t *)data_art_load_node(tree, n48_ref);
        if (!n48) {
            return NULL_NODE_REF;
        }
        
        return add_child_node48(tree, n48_ref, n48, byte, child_ref);
    }
    
    // Add to NODE_16 (keep sorted)
    data_art_node16_t new_n16 = *n16;
    
    // Check for duplicate key - this should never happen in normal operation
    for (int i = 0; i < n16->num_children; i++) {
        if (n16->keys[i] == byte) {
            LOG_ERROR("BUG: Attempted to add duplicate child byte=0x%02x to NODE_16 at page=%lu "
                     "(num_children=%u)",
                     byte, node_ref.page_id, n16->num_children);
            return NULL_NODE_REF;
        }
    }
    
    // Find insertion position
    int pos = 0;
    while (pos < new_n16.num_children && new_n16.keys[pos] < byte) {
        pos++;
    }
    
    // Shift elements
    if (pos < new_n16.num_children) {
        memmove(&new_n16.keys[pos + 1], &new_n16.keys[pos], 
                new_n16.num_children - pos);
        memmove(&new_n16.child_page_ids[pos + 1], &new_n16.child_page_ids[pos],
                (new_n16.num_children - pos) * sizeof(uint64_t));
        memmove(&new_n16.child_offsets[pos + 1], &new_n16.child_offsets[pos],
                (new_n16.num_children - pos) * sizeof(uint32_t));
    }
    
    // Insert
    new_n16.keys[pos] = byte;
    new_n16.child_page_ids[pos] = child_ref.page_id;
    new_n16.child_offsets[pos] = child_ref.offset;
    new_n16.num_children++;
    
    // CoW: allocate new page (hint = old page for same-page COW), write there, release old
    node_ref_t new_ref = data_art_alloc_node_hint(tree, sizeof(data_art_node16_t), node_ref.page_id);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate new NODE_16 for CoW");
        return NULL_NODE_REF;
    }
    if (!data_art_write_node(tree, new_ref, &new_n16, sizeof(new_n16))) {
        LOG_ERROR("Failed to write NODE_16");
        return NULL_NODE_REF;
    }
    data_art_release_page(tree, node_ref);

    return new_ref;
}

/**
 * Add child to NODE_48 (grow to NODE_256 if full)
 */
static node_ref_t add_child_node48(data_art_tree_t *tree, node_ref_t node_ref,
                                    const data_art_node48_t *n48, uint8_t byte, node_ref_t child_ref) {
    // If full, grow to NODE_256
    if (n48->num_children >= 48) {
        node_ref_t n256_ref = grow_node48_to_node256(tree, node_ref, n48);
        if (node_ref_is_null(n256_ref)) {
            return NULL_NODE_REF;
        }
        
        // Load the new NODE_256 and add child to it
        const data_art_node256_t *n256 = 
            (const data_art_node256_t *)data_art_load_node(tree, n256_ref);
        if (!n256) {
            return NULL_NODE_REF;
        }
        
        return add_child_node256(tree, n256_ref, n256, byte, child_ref);
    }
    
    // Add to NODE_48
    data_art_node48_t new_n48 = *n48;
    
    // Check for duplicate key - this should never happen in normal operation
    if (n48->keys[byte] != 255) {
        LOG_ERROR("BUG: Attempted to add duplicate child byte=0x%02x to NODE_48 at page=%lu "
                 "(num_children=%u, slot=%u)",
                 byte, node_ref.page_id, n48->num_children, n48->keys[byte]);
        return NULL_NODE_REF;
    }
    
    // Find empty slot
    int slot = 0;
    while (slot < 48) {
        bool used = false;
        for (int i = 0; i < 256; i++) {
            if (new_n48.keys[i] == slot) {
                used = true;
                break;
            }
        }
        if (!used) break;
        slot++;
    }
    
    // Set index and child
    new_n48.keys[byte] = slot;
    new_n48.child_page_ids[slot] = child_ref.page_id;
    new_n48.child_offsets[slot] = child_ref.offset;
    new_n48.num_children++;
    
    // CoW: allocate new page (hint = old page for same-page COW), write there, release old
    node_ref_t new_ref = data_art_alloc_node_hint(tree, sizeof(data_art_node48_t), node_ref.page_id);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate new NODE_48 for CoW");
        return NULL_NODE_REF;
    }
    if (!data_art_write_node(tree, new_ref, &new_n48, sizeof(new_n48))) {
        LOG_ERROR("Failed to write NODE_48");
        return NULL_NODE_REF;
    }
    data_art_release_page(tree, node_ref);

    return new_ref;
}

/**
 * Add child to NODE_256 (no growth needed)
 */
static node_ref_t add_child_node256(data_art_tree_t *tree, node_ref_t node_ref,
                                     const data_art_node256_t *n256, uint8_t byte, node_ref_t child_ref) {
    if (n256->child_page_ids[byte] != 0) {
        LOG_ERROR("BUG: Attempted to add duplicate child byte=0x%02x to NODE_256 at page=%lu "
                 "(existing child at page=%lu)",
                 byte, node_ref.page_id, n256->child_page_ids[byte]);
        return NULL_NODE_REF;
    }

    // Optimized CoW: copy whole node mmap-to-mmap, patch 13 bytes (num_children + child ref)
    node_ref_t new_ref = data_art_alloc_node_hint(tree, sizeof(data_art_node256_t), node_ref.page_id);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate new NODE_256 for CoW");
        return NULL_NODE_REF;
    }

    data_art_copy_node(tree, new_ref, node_ref, sizeof(data_art_node256_t));

    uint8_t new_count = n256->num_children + 1;
    data_art_write_partial(tree, new_ref, offsetof(data_art_node256_t, num_children),
                           &new_count, sizeof(uint8_t));
    data_art_write_partial(tree, new_ref,
                           offsetof(data_art_node256_t, child_page_ids) + byte * sizeof(uint64_t),
                           &child_ref.page_id, sizeof(uint64_t));
    data_art_write_partial(tree, new_ref,
                           offsetof(data_art_node256_t, child_offsets) + byte * sizeof(uint32_t),
                           &child_ref.offset, sizeof(uint32_t));

    data_art_release_page(tree, node_ref);
    return new_ref;
}

// ============================================================================
// Public API (exported for use in data_art_insert.c)
// ============================================================================

/**
 * Add child with automatic node growth
 */
node_ref_t data_art_add_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, node_ref_t child_ref) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return NULL_NODE_REF;
    }

    uint8_t type = *(const uint8_t *)node;

    switch (type) {
        case DATA_NODE_4:
            return add_child_node4(tree, node_ref,
                                   (const data_art_node4_t *)node, byte, child_ref);
        case DATA_NODE_16:
            return add_child_node16(tree, node_ref,
                                    (const data_art_node16_t *)node, byte, child_ref);
        case DATA_NODE_48:
            return add_child_node48(tree, node_ref,
                                    (const data_art_node48_t *)node, byte, child_ref);
        case DATA_NODE_256:
            return add_child_node256(tree, node_ref,
                                     (const data_art_node256_t *)node, byte, child_ref);
        default:
            LOG_ERROR("Invalid node type for add_child: %d", type);
            return NULL_NODE_REF;
    }
}

/**
 * In-place add child — mutate node directly in mmap.
 * Caller must hold write_lock as wrlock (exclusive).
 * Returns same node_ref on success (no CoW propagation), or new ref if growth occurred.
 */
node_ref_t data_art_add_child_inplace(data_art_tree_t *tree, node_ref_t node_ref,
                                       uint8_t byte, node_ref_t child_ref) {
    // First load to determine type and check capacity
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) return NULL_NODE_REF;

    uint8_t type = *(const uint8_t *)node;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n4 = (const data_art_node4_t *)node;
            if (n4->num_children >= 4) {
                // Growth needed: fall back to CoW (alloc new NODE_16)
                return add_child_node4(tree, node_ref, n4, byte, child_ref);
            }
            // In-place add to NODE_4
            void *mut = data_art_lock_node_mut(tree, node_ref);
            data_art_node4_t *m = (data_art_node4_t *)mut;
            int pos = 0;
            while (pos < m->num_children && m->keys[pos] < byte) pos++;
            if (pos < m->num_children) {
                memmove(&m->keys[pos + 1], &m->keys[pos], m->num_children - pos);
                memmove(&m->child_page_ids[pos + 1], &m->child_page_ids[pos],
                        (m->num_children - pos) * sizeof(uint64_t));
                memmove(&m->child_offsets[pos + 1], &m->child_offsets[pos],
                        (m->num_children - pos) * sizeof(uint32_t));
            }
            m->keys[pos] = byte;
            m->child_page_ids[pos] = child_ref.page_id;
            m->child_offsets[pos] = child_ref.offset;
            m->num_children++;
            data_art_unlock_node_mut(tree);
            tree->inplace_mutations++;
            return node_ref;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n16 = (const data_art_node16_t *)node;
            if (n16->num_children >= 16) {
                return add_child_node16(tree, node_ref, n16, byte, child_ref);
            }
            void *mut = data_art_lock_node_mut(tree, node_ref);
            data_art_node16_t *m = (data_art_node16_t *)mut;
            int pos = 0;
            while (pos < m->num_children && m->keys[pos] < byte) pos++;
            if (pos < m->num_children) {
                memmove(&m->keys[pos + 1], &m->keys[pos], m->num_children - pos);
                memmove(&m->child_page_ids[pos + 1], &m->child_page_ids[pos],
                        (m->num_children - pos) * sizeof(uint64_t));
                memmove(&m->child_offsets[pos + 1], &m->child_offsets[pos],
                        (m->num_children - pos) * sizeof(uint32_t));
            }
            m->keys[pos] = byte;
            m->child_page_ids[pos] = child_ref.page_id;
            m->child_offsets[pos] = child_ref.offset;
            m->num_children++;
            data_art_unlock_node_mut(tree);
            tree->inplace_mutations++;
            return node_ref;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n48 = (const data_art_node48_t *)node;
            if (n48->num_children >= 48) {
                return add_child_node48(tree, node_ref, n48, byte, child_ref);
            }
            void *mut = data_art_lock_node_mut(tree, node_ref);
            data_art_node48_t *m = (data_art_node48_t *)mut;
            // Find free slot
            uint8_t slot = 0;
            for (; slot < 48; slot++) {
                bool used = false;
                for (int i = 0; i < 256; i++) {
                    if (m->keys[i] == slot) { used = true; break; }
                }
                if (!used) break;
            }
            m->keys[byte] = slot;
            m->child_page_ids[slot] = child_ref.page_id;
            m->child_offsets[slot] = child_ref.offset;
            m->num_children++;
            data_art_unlock_node_mut(tree);
            tree->inplace_mutations++;
            return node_ref;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n256 = (const data_art_node256_t *)node;
            if (n256->child_page_ids[byte] != 0) {
                LOG_ERROR("BUG: duplicate child byte=0x%02x in NODE_256 inplace", byte);
                return NULL_NODE_REF;
            }
            void *mut = data_art_lock_node_mut(tree, node_ref);
            data_art_node256_t *m = (data_art_node256_t *)mut;
            m->child_page_ids[byte] = child_ref.page_id;
            m->child_offsets[byte] = child_ref.offset;
            m->num_children++;
            data_art_unlock_node_mut(tree);
            tree->inplace_mutations++;
            return node_ref;
        }
        default:
            LOG_ERROR("Invalid node type for add_child_inplace: %d", type);
            return NULL_NODE_REF;
    }
}
