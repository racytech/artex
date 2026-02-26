/**
 * Persistent ART - Delete Operations
 * 
 * Handles recursive deletion with node shrinking:
 * - NODE_256 -> NODE_48 (at 48 children)
 * - NODE_48 -> NODE_16 (at 16 children)
 * - NODE_16 -> NODE_4 (at 4 children)
 * - NODE_4 -> collapse to single child or leaf
 * 
 * KNOWN LIMITATION:
 * Delete operations use copy-on-write semantics, creating new page versions
 * but NOT freeing the old pages. This causes:
 * 1. Disk space leaks (old pages remain allocated)
 * 2. Potential cache coherency issues with buffer pool
 * 
 * TODO: Implement proper page garbage collection to:
 * - Track reference counts for pages
 * - Free unreachable pages after delete
 * - Compact database file periodically
 * 
 * For now, delete operations work but may be slower and use more disk space
 * than necessary. Insert and search operations are fully optimized.
 */

#include "data_art.h"
#include "mvcc.h"
#include "txn_buffer.h"
#include "db_error.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>

// Forward declarations - functions we need from data_art_core.c and other files
// These need to be made non-static in their respective files
extern const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);
extern bool leaf_matches(const data_art_leaf_t *leaf, const uint8_t *key, size_t key_len);
extern node_ref_t find_child(data_art_tree_t *tree, node_ref_t node_ref, uint8_t byte);
extern size_t get_node_size(data_art_node_type_t node_type);
extern node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);
extern bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref, const void *data, size_t size);
extern void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);
extern void data_art_reset_arena(void);
extern void data_art_publish_root(data_art_tree_t *tree);

// Thread-local: when true, skip MVCC logical deletes (inplace mode)
static __thread bool tls_delete_skip_mvcc = false;

// Forward declarations
static node_ref_t delete_recursive(data_art_tree_t *tree, node_ref_t node_ref,
                                   const uint8_t *key, size_t key_len, size_t depth,
                                   bool *deleted);
static node_ref_t remove_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, bool *did_remove);
static node_ref_t update_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, node_ref_t new_child_ref);
static node_ref_t try_shrink_node(data_art_tree_t *tree, node_ref_t node_ref);// ============================================================================
// Public API
// ============================================================================

bool data_art_delete(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree or key is NULL");
        return false;
    }

    // Validate key size matches tree's configured size
    if (key_len != tree->key_size) {
        DB_ERROR(DB_ERROR_INVALID_ARG,
            "key size mismatch: expected %zu, got %zu", tree->key_size, key_len);
        return false;
    }

    // If in transaction, buffer the operation instead of applying immediately
    thread_txn_context_t *ctx = get_txn_context();
    if (ctx && ctx->txn_buffer) {
        return txn_buffer_add_delete(ctx->txn_buffer, key, key_len);
    }

    if (node_ref_is_null(tree->root)) {
        DB_ERROR(DB_ERROR_NOT_FOUND, "empty tree");
        return false;  // Empty tree, nothing to delete
    }
    
    // Reset thread-local arena — each delete starts fresh
    data_art_reset_arena();

    // Acquire write lock to serialize all write operations (one writer at a time)
    pthread_rwlock_wrlock(&tree->write_lock);

    // Auto-commit MVCC transaction setup
    uint64_t auto_txn_id = 0;
    bool auto_commit = (tree->current_txn_id == 0);
    bool skip_mvcc = false;

    if (auto_commit) {
        if (tree->mvcc_manager &&
            mvcc_has_active_snapshots(tree->mvcc_manager)) {
            // Snapshots active: full MVCC required
            if (!mvcc_begin_txn(tree->mvcc_manager, &auto_txn_id)) {
                pthread_rwlock_unlock(&tree->write_lock);
                DB_ERROR(DB_ERROR_OUT_OF_MEMORY, "failed to begin auto-commit txn");
                return false;
            }
            tree->current_txn_id = auto_txn_id;
        } else {
            // No snapshots: skip full MVCC machinery
            skip_mvcc = true;
            tree->version++;
            tree->current_txn_id = tree->version;
        }
    }

    tls_delete_skip_mvcc = skip_mvcc;

    bool deleted = false;
    node_ref_t new_root = delete_recursive(tree, tree->root, key, key_len, 0, &deleted);

    if (deleted) {
        tree->root = new_root;
        tree->size--;

        if (auto_commit && !skip_mvcc && tree->mvcc_manager) {
            mvcc_commit_txn(tree->mvcc_manager, auto_txn_id);
        }

        data_art_publish_root(tree);
    } else {
        if (auto_commit && !skip_mvcc && tree->mvcc_manager) {
            mvcc_abort_txn(tree->mvcc_manager, auto_txn_id);
        }
        DB_ERROR(DB_ERROR_NOT_FOUND, "key not found");
    }
    tree->current_txn_id = 0;

    pthread_rwlock_unlock(&tree->write_lock);
    return deleted;
}

// ============================================================================
// Internal Delete (for optimized commit path)
// ============================================================================

/**
 * Internal delete — called from commit path with write_lock already held.
 * No locking, no auto-commit MVCC, no WAL logging, no root publication.
 * Caller is responsible for: write_lock, MVCC txn, WAL logging, root publish.
 * Returns true if key was deleted, false if not found.
 */
bool data_art_delete_internal(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                               bool inplace) {
    if (!tree || !key) return false;
    if (key_len != tree->key_size) return false;
    if (node_ref_is_null(tree->root)) return false;

    data_art_reset_arena();
    tls_delete_skip_mvcc = inplace;

    bool deleted = false;
    node_ref_t new_root = delete_recursive(tree, tree->root, key, key_len, 0, &deleted);

    if (deleted) {
        tree->root = new_root;
        tree->size--;
    }
    return deleted;
}

// ============================================================================
// Recursive Delete
// ============================================================================

static node_ref_t delete_recursive(data_art_tree_t *tree, node_ref_t node_ref,
                                   const uint8_t *key, size_t key_len, size_t depth,
                                   bool *deleted) {
    *deleted = false;
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node at page=%lu, offset=%u",
                 node_ref_page_id(node_ref), node_ref_offset(node_ref));
        return node_ref;
    }
    
    uint8_t type = *(const uint8_t *)node;
    
    // If we've reached a leaf, check if it matches
    if (type == DATA_NODE_LEAF) {
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
        
        if (leaf_matches(leaf, key, key_len)) {
            // Check if already deleted (prevents double-deletion bug)
            if (leaf->xmax != 0) {
                LOG_DEBUG("[DELETE_LEAF] Key already deleted (xmax=%lu), skipping", leaf->xmax);
                *deleted = false;
                return node_ref;
            }
            
            // MVCC logical deletion: Set xmax instead of physical removal
            // Only use logical deletes when there are active snapshots that need
            // to see the old version. Otherwise, use physical deletion for efficiency.
            //
            // Logical delete when:
            // 1. MVCC manager exists
            // 2. There are active snapshots (concurrent readers need versions preserved)
            // 3. The leaf was created by a different transaction (not in-flight creation)
            bool should_do_logical_delete = !tls_delete_skip_mvcc &&
                                           tree->current_txn_id > 0 &&
                                           leaf->xmin != tree->current_txn_id;
            
            if (should_do_logical_delete) {
                // Create modified copy of leaf with xmax set
                size_t leaf_size = leaf_total_size(leaf, tree->key_size);
                data_art_leaf_t *modified_leaf = malloc(leaf_size);
                if (!modified_leaf) {
                    LOG_ERROR("Failed to allocate modified leaf");
                    return node_ref;
                }
                
                // Copy entire leaf
                memcpy(modified_leaf, leaf, leaf_size);
                
                // Set xmax to mark as deleted
                modified_leaf->xmax = tree->current_txn_id;
                
                // Write modified leaf back (CoW, hint = old page for same-page COW)
                node_ref_t new_ref = data_art_alloc_node_hint(tree, leaf_size, node_ref_page_id(node_ref));
                if (node_ref_is_null(new_ref)) {
                    LOG_ERROR("Failed to allocate new leaf page");
                    free(modified_leaf);
                    return node_ref;
                }
                
                if (!data_art_write_node(tree, new_ref, modified_leaf, leaf_size)) {
                    LOG_ERROR("Failed to write modified leaf");
                    free(modified_leaf);
                    return node_ref;
                }
                
                free(modified_leaf);
                *deleted = true;
                
                LOG_DEBUG("[DELETE_LEAF] Logical deletion: set xmax=%lu on leaf (xmin=%lu)",
                         tree->current_txn_id, leaf->xmin);
                
                return new_ref;  // Return modified leaf ref
            }
            
            // Physical deletion for:
            // 1. Non-MVCC mode
            // 2. Outside transaction
            // 3. Deleting a leaf created in the same transaction
            // 4. Regular transactions without active snapshots
            *deleted = true;
            
            // Free overflow pages if this leaf has any
            if (leaf->flags & LEAF_FLAG_OVERFLOW) {
                uint64_t ofp = leaf_overflow_page(leaf);
                LOG_DEBUG("[DELETE_LEAF] Freeing overflow chain starting at page=%lu", ofp);
                extern size_t data_art_free_overflow_chain(data_art_tree_t *tree, uint64_t first_overflow_page);
                data_art_free_overflow_chain(tree, ofp);
            }
            
            // Free the leaf page itself
            data_art_release_page(tree, node_ref);
            
            // Return NULL to indicate this node should be removed
            return NULL_NODE_REF;
        }
        
        // Not a match, nothing to delete
        return node_ref;
    }
    
    // Inner node — check compressed prefix, then descend to child

    // Path compression: verify prefix match before child dispatch
    uint8_t plen = node_partial_len(node);
    if (plen > 0) {
        if (depth + plen > key_len ||
            memcmp(node_partial(node), key + depth, plen) != 0) {
            return node_ref;  // Key doesn't match prefix, not in tree
        }
        depth += plen;
    }

    // Get next byte to search
    uint8_t search_byte = (depth < key_len) ? key[depth] : 0x00;
    size_t next_depth = (depth < key_len) ? depth + 1 : depth;
    
    // Find child BEFORE recursion (uses data_art_load_node internally, might corrupt temp buffer)
    node_ref_t child_ref = find_child(tree, node_ref, search_byte);
    if (node_ref_is_null(child_ref)) {
        return node_ref;  // Child doesn't exist, key not in tree
    }
    
    // Recursively delete in child
    // WARNING: This will corrupt the `node` pointer in temp_pages!
    node_ref_t new_child = delete_recursive(tree, child_ref, key, key_len, next_depth, deleted);
    
    if (*deleted) {
        if (node_ref_is_null(new_child)) {
            // Child was removed, remove it from this node
            bool did_remove;
            node_ref_t updated = remove_child(tree, node_ref, search_byte, &did_remove);
            
            // Try to shrink the node if needed
            return try_shrink_node(tree, updated);
        } else {
            // Child was updated, update the pointer
            return update_child(tree, node_ref, search_byte, new_child);
        }
    }
    
    return node_ref;
}

// ============================================================================
// Update Child Pointer in Node
// ============================================================================

static node_ref_t update_child(data_art_tree_t *tree, node_ref_t node_ref,
                               uint8_t byte, node_ref_t new_child_ref) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node for update_child");
        return node_ref;
    }
    
    uint8_t type = *(const uint8_t *)node;
    size_t node_size = get_node_size(type);
    
    // Create a copy of the node
    void *new_node = malloc(node_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for updated node");
        return node_ref;
    }
    
    memcpy(new_node, node, node_size);
    
    // Update the child pointer based on node type
    switch (type) {
        case DATA_NODE_4: {
            data_art_node4_t *n4 = (data_art_node4_t *)new_node;
            for (int i = 0; i < n4->num_children; i++) {
                if (n4->keys[i] == byte) {
                    n4->children[i] = new_child_ref;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_16: {
            data_art_node16_t *n16 = (data_art_node16_t *)new_node;
            for (int i = 0; i < n16->num_children; i++) {
                if (n16->keys[i] == byte) {
                    n16->children[i] = new_child_ref;
                    break;
                }
            }
            break;
        }
        case DATA_NODE_48: {
            data_art_node48_t *n48 = (data_art_node48_t *)new_node;
            int index = n48->keys[byte];
            if (index != 255) {  // 255 means empty slot
                n48->children[index] = new_child_ref;
            }
            break;
        }
        case DATA_NODE_256: {
            data_art_node256_t *n256 = (data_art_node256_t *)new_node;
            n256->children[byte] = new_child_ref;
            break;
        }
        default:
            LOG_ERROR("Cannot update child on node type %d", type);
            free(new_node);
            return node_ref;
    }
    
    // Allocate new page and write the node (hint = old page for same-page COW)
    node_ref_t new_ref = data_art_alloc_node_hint(tree, node_size, node_ref_page_id(node_ref));
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate page for updated node");
        free(new_node);
        return node_ref;
    }

    if (!data_art_write_node(tree, new_ref, new_node, node_size)) {
        LOG_ERROR("Failed to write updated node");
        free(new_node);
        return node_ref;
    }

    free(new_node);

    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, node_ref);

    return new_ref;
}

// ============================================================================
// Remove Child from Node
// ============================================================================

static node_ref_t remove_child(data_art_tree_t *tree, node_ref_t node_ref, 
                               uint8_t byte, bool *did_remove) {
    *did_remove = false;
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_ERROR("Failed to load node");
        return node_ref;
    }
    
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t type = node_bytes[0];
    uint8_t num_children = node_bytes[1];
    
    // Find the child to remove
    int child_pos = -1;
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n4 = (const data_art_node4_t *)node;
            for (int i = 0; i < num_children; i++) {
                if (n4->keys[i] == byte) {
                    child_pos = i;
                    break;
                }
            }
            break;
        }
        
        case DATA_NODE_16: {
            const data_art_node16_t *n16 = (const data_art_node16_t *)node;
            for (int i = 0; i < num_children; i++) {
                if (n16->keys[i] == byte) {
                    child_pos = i;
                    break;
                }
            }
            break;
        }
        
        case DATA_NODE_48: {
            const data_art_node48_t *n48 = (const data_art_node48_t *)node;
            uint8_t pos = n48->keys[byte];
            if (pos != 0xFF) {
                child_pos = pos;
            }
            break;
        }
        
        case DATA_NODE_256: {
            const data_art_node256_t *n256 = (const data_art_node256_t *)node;

            if (n256->children[byte] == 0) {
                // Child doesn't exist
                return node_ref;
            }

            child_pos = byte;
            break;
        }
        
        default:
            LOG_ERROR("Invalid node type: %d", type);
            return node_ref;
    }
    
    if (child_pos == -1) {
        // Child not found
        return node_ref;
    }
    
    // Create a new node without this child
    size_t node_size = get_node_size(type);
    void *new_node = malloc(node_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for new node");
        return node_ref;
    }
    
    memcpy(new_node, node, node_size);
    uint8_t *new_bytes = (uint8_t *)new_node;
    
    // Decrease child count
    new_bytes[1] = num_children - 1;
    
    // Remove the child based on node type
    switch (type) {
        case DATA_NODE_4: {
            data_art_node4_t *n4 = (data_art_node4_t *)new_node;

            // Shift keys and children to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n4->keys[i] = n4->keys[i + 1];
                n4->children[i] = n4->children[i + 1];
            }

            // Clear the last slot
            n4->keys[num_children - 1] = 0;
            n4->children[num_children - 1] = 0;
            break;
        }
        
        case DATA_NODE_16: {
            data_art_node16_t *n16 = (data_art_node16_t *)new_node;

            // Shift keys and children to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n16->keys[i] = n16->keys[i + 1];
                n16->children[i] = n16->children[i + 1];
            }

            // Clear the last slot
            n16->keys[num_children - 1] = 0;
            n16->children[num_children - 1] = 0;
            break;
        }
        
        case DATA_NODE_48: {
            data_art_node48_t *n48 = (data_art_node48_t *)new_node;

            // Mark index slot as empty
            n48->keys[byte] = 0xFF;

            // Shift children down to fill the gap
            for (int i = child_pos; i < num_children - 1; i++) {
                n48->children[i] = n48->children[i + 1];
            }

            // Clear the last slot
            n48->children[num_children - 1] = 0;
            
            // Update all index entries that pointed after the removed position
            for (int i = 0; i < 256; i++) {
                if (n48->keys[i] != 0xFF && n48->keys[i] > child_pos) {
                    n48->keys[i]--;
                }
            }
            break;
        }
        
        case DATA_NODE_256: {
            data_art_node256_t *n256 = (data_art_node256_t *)new_node;

            // Simply zero out the child pointer
            n256->children[byte] = 0;
            break;
        }
    }
    
    // Allocate new page for modified node (hint = old page for same-page COW)
    node_ref_t new_ref = data_art_alloc_node_hint(tree, node_size, node_ref_page_id(node_ref));
    if (node_ref_is_null(new_ref)) {
        free(new_node);
        LOG_ERROR("Failed to allocate page for modified node");
        return node_ref;
    }

    if (!data_art_write_node(tree, new_ref, new_node, node_size)) {
        free(new_node);
        LOG_ERROR("Failed to write modified node");
        return node_ref;
    }

    free(new_node);
    *did_remove = true;

    LOG_DEBUG("[REMOVE_CHILD] Created new node at page=%lu, releasing old page=%lu",
              node_ref_page_id(new_ref), node_ref_page_id(node_ref));
    
    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, node_ref);
    
    return new_ref;
}

// ============================================================================
// Node Shrinking
// ============================================================================

static node_ref_t try_shrink_node(data_art_tree_t *tree, node_ref_t node_ref) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return node_ref;
    }
    
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t type = node_bytes[0];
    uint8_t num_children = node_bytes[1];
    
    // Determine if we should shrink
    bool should_shrink = false;
    uint8_t new_type = type;
    
    switch (type) {
        case DATA_NODE_256:
            if (num_children <= 48) {
                should_shrink = true;
                new_type = DATA_NODE_48;
            }
            break;
            
        case DATA_NODE_48:
            if (num_children <= 16) {
                should_shrink = true;
                new_type = DATA_NODE_16;
            }
            break;
            
        case DATA_NODE_16:
            if (num_children <= 4) {
                should_shrink = true;
                new_type = DATA_NODE_4;
            }
            break;
            
        case DATA_NODE_4:
            if (num_children == 1) {
                const data_art_node4_t *n4 = (const data_art_node4_t *)node;
                node_ref_t child_ref = n4->children[0];
                const void *child = data_art_load_node(tree, child_ref);
                if (!child) break;
                uint8_t child_type = *(const uint8_t *)child;

                if (child_type == DATA_NODE_LEAF) {
                    // Collapse to leaf directly.
                    // NOTE: intentionally do NOT release the Node4 page here.
                    // MVCC snapshots may still reference the old tree structure.
                    return child_ref;
                }

                // Inner-node child: merge prefix.
                // new_prefix = this.partial + dispatch_byte + child.partial
                uint8_t child_plen = node_partial_len(child);
                uint8_t new_plen = n4->partial_len + 1 + child_plen;
                if (new_plen <= ART_MAX_PREFIX) {
                    // Build merged prefix
                    uint8_t merged[ART_MAX_PREFIX];
                    memcpy(merged, n4->partial, n4->partial_len);
                    merged[n4->partial_len] = n4->keys[0];  // dispatch byte
                    memcpy(merged + n4->partial_len + 1, node_partial(child), child_plen);

                    // CoW copy of child with extended prefix
                    size_t child_size = get_node_size(child_type);
                    void *new_child = malloc(child_size);
                    if (!new_child) break;
                    memcpy(new_child, child, child_size);

                    // Set new prefix in the copy
                    ((uint8_t *)new_child)[2] = new_plen;
                    memcpy((uint8_t *)new_child + 3, merged, new_plen);

                    node_ref_t new_ref = data_art_alloc_node(tree, child_size);
                    if (node_ref_is_null(new_ref)) {
                        free(new_child);
                        break;
                    }
                    if (!data_art_write_node(tree, new_ref, new_child, child_size)) {
                        free(new_child);
                        break;
                    }
                    free(new_child);

                    // Don't release old Node4 or old child — MVCC snapshots may reference them
                    return new_ref;
                }
                // Combined prefix too long (shouldn't happen for 32-byte keys), keep Node4
            }
            break;
    }
    
    if (!should_shrink) {
        return node_ref;
    }
    
    // Shrink the node
    LOG_DEBUG("Shrinking node from type %d to type %d", type, new_type);
    
    // Allocate new smaller node
    size_t new_size = get_node_size(new_type);
    void *new_node = calloc(1, new_size);
    if (!new_node) {
        LOG_ERROR("Failed to allocate memory for shrunk node");
        return node_ref;
    }
    
    uint8_t *new_bytes = (uint8_t *)new_node;
    
    // Copy header: type + num_children + partial fields
    new_bytes[0] = new_type;
    new_bytes[1] = num_children;
    new_bytes[2] = node_partial_len(node);
    memcpy(new_bytes + 3, node_partial(node), node_partial_len(node));

    // Copy children based on conversion
    if (type == DATA_NODE_256 && new_type == DATA_NODE_48) {
        // NODE_256 -> NODE_48
        const data_art_node256_t *old_n256 = (const data_art_node256_t *)node;
        data_art_node48_t *new_n48 = (data_art_node48_t *)new_node;

        memset(new_n48->keys, 0xFF, 256);

        int pos = 0;
        for (int i = 0; i < 256; i++) {
            if (old_n256->children[i] != 0) {
                new_n48->keys[i] = pos;
                new_n48->children[pos] = old_n256->children[i];
                pos++;
            }
        }
    } else if (type == DATA_NODE_48 && new_type == DATA_NODE_16) {
        // NODE_48 -> NODE_16
        const data_art_node48_t *old_n48 = (const data_art_node48_t *)node;
        data_art_node16_t *new_n16 = (data_art_node16_t *)new_node;

        int pos = 0;
        for (int i = 0; i < 256; i++) {
            if (old_n48->keys[i] != 0xFF) {
                uint8_t child_idx = old_n48->keys[i];
                new_n16->keys[pos] = i;
                new_n16->children[pos] = old_n48->children[child_idx];
                pos++;
            }
        }
    } else if (type == DATA_NODE_16 && new_type == DATA_NODE_4) {
        // NODE_16 -> NODE_4
        const data_art_node16_t *old_n16 = (const data_art_node16_t *)node;
        data_art_node4_t *new_n4 = (data_art_node4_t *)new_node;

        for (int i = 0; i < num_children; i++) {
            new_n4->keys[i] = old_n16->keys[i];
            new_n4->children[i] = old_n16->children[i];
        }
    }
    
    // Allocate page for new node
    node_ref_t new_ref = data_art_alloc_node(tree, new_size);
    if (node_ref_is_null(new_ref)) {
        free(new_node);
        LOG_ERROR("Failed to allocate page for shrunk node");
        return node_ref;
    }
    
    if (!data_art_write_node(tree, new_ref, new_node, new_size)) {
        free(new_node);
        LOG_ERROR("Failed to write shrunk node");
        return node_ref;
    }
    
    free(new_node);
    
    LOG_DEBUG("Successfully shrunk node from type %d to type %d", type, new_type);
    
    // Release old page - decrement ref count and mark dead if 0
    data_art_release_page(tree, node_ref);
    
    return new_ref;
}
