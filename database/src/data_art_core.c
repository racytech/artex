/*
 * Persistent ART - Core Operations
 * 
 * Implements the main tree operations for the disk-backed adaptive radix tree:
 * - Tree lifecycle (create, destroy)
 * - Basic operations (insert, get, delete, contains)
 * - Node management (alloc, load, write, CoW)
 * 
 * This file handles the core ART logic, while data_art_overflow.c handles
 * overflow pages, versioning, persistence, and statistics.
 */

#include "data_art.h"
#include "txn_buffer.h"
#include "mvcc.h"
#include "page_gc.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

// ============================================================================
// Thread-Local Arena for Safe Node Reads
// ============================================================================
//
// data_art_load_node() previously returned raw pointers into buffer pool pages.
// Under concurrent load, pages could be evicted (and freed) while threads still
// held pointers — a use-after-free causing checksum corruption.
//
// Fix: pin the page, copy node data to this thread-local arena, unpin immediately.
// The returned pointer is to stable arena memory, immune to buffer pool eviction.
// Arena is reset at the start of each top-level operation (get/insert/delete).

#define TLS_ARENA_SIZE (256 * 1024)  // 256KB per thread — fits ~80 Node256 or thousands of small nodes
static __thread uint8_t tls_arena[TLS_ARENA_SIZE];
static __thread size_t  tls_arena_offset = 0;

static void tls_arena_reset(void) {
    tls_arena_offset = 0;
}

static void *tls_arena_alloc(size_t size) {
    size = (size + 7) & ~7;  // 8-byte align
    if (tls_arena_offset + size > TLS_ARENA_SIZE) {
        return NULL;
    }
    void *ptr = tls_arena + tls_arena_offset;
    tls_arena_offset += size;
    return ptr;
}

// Non-static wrapper so data_art_insert.c and data_art_delete.c can reset the arena
void data_art_reset_arena(void) {
    tls_arena_reset();
}

// Thread-local transaction context
typedef struct {
    data_art_tree_t *tree;           // Tree this transaction belongs to
    uint64_t txn_id;                 // Transaction ID
    txn_buffer_t *txn_buffer;        // Buffer for pending operations
} thread_txn_context_t;

static pthread_key_t txn_context_key;
static pthread_once_t txn_context_key_once = PTHREAD_ONCE_INIT;

// Initialize thread-local storage key
static void make_txn_context_key() {
    pthread_key_create(&txn_context_key, free);
}

// Get thread-local transaction context
static thread_txn_context_t *get_txn_context() {
    pthread_once(&txn_context_key_once, make_txn_context_key);
    return (thread_txn_context_t *)pthread_getspecific(txn_context_key);
}

// Set thread-local transaction context
static void set_txn_context(thread_txn_context_t *ctx) {
    pthread_once(&txn_context_key_once, make_txn_context_key);
    pthread_setspecific(txn_context_key, ctx);
}

// Snapshot handle structure - each thread owns one
struct data_art_snapshot {
    mvcc_snapshot_t *mvcc_snapshot;  // Underlying MVCC snapshot
    uint64_t txn_id;                 // Transaction ID for this snapshot
};

// ============================================================================
// Helper Functions - Node Size Calculation
// ============================================================================

/**
 * Get the fixed size of a node based on its type
 */
size_t get_node_size(data_art_node_type_t type) {
    switch (type) {
        case DATA_NODE_4:   return sizeof(data_art_node4_t);
        case DATA_NODE_16:  return sizeof(data_art_node16_t);
        case DATA_NODE_48:  return sizeof(data_art_node48_t);
        case DATA_NODE_256: return sizeof(data_art_node256_t);
        case DATA_NODE_OVERFLOW: return sizeof(data_art_overflow_t);
        case DATA_NODE_LEAF: 
            // Leaf size is variable, caller must specify
            LOG_ERROR("get_node_size called for LEAF without size");
            return 0;
        default:
            LOG_ERROR("Unknown node type: %d", type);
            return 0;
    }
}

/**
 * Get the size of a node by inspecting its in-memory data.
 * Unlike get_node_size(), this handles variable-size leaves.
 */
static size_t data_art_node_size_from_data(const void *node_data) {
    uint8_t type = *(const uint8_t *)node_data;
    switch (type) {
        case DATA_NODE_4:      return sizeof(data_art_node4_t);
        case DATA_NODE_16:     return sizeof(data_art_node16_t);
        case DATA_NODE_48:     return sizeof(data_art_node48_t);
        case DATA_NODE_256:    return sizeof(data_art_node256_t);
        case DATA_NODE_OVERFLOW: return sizeof(data_art_overflow_t);
        case DATA_NODE_LEAF: {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node_data;
            return sizeof(data_art_leaf_t) + leaf->inline_data_len;
        }
        default:
            LOG_ERROR("Unknown node type %u in data_art_node_size_from_data", type);
            return PAGE_SIZE - PAGE_HEADER_SIZE;  // safe fallback: copy max possible
    }
}

// ============================================================================
// Tree Lifecycle
// ============================================================================

data_art_tree_t *data_art_create(page_manager_t *page_manager,
                                   buffer_pool_t *buffer_pool,
                                   wal_t *wal,
                                   size_t key_size) {
    if (!page_manager) {
        LOG_ERROR("page_manager cannot be NULL");
        return NULL;
    }
    
    // Validate key_size for Ethereum (20-byte addresses, 32-byte hashes)
    if (key_size != 20 && key_size != 32) {
        LOG_ERROR("Invalid key_size: %zu (must be 20 or 32 for Ethereum)", key_size);
        return NULL;
    }
    
    data_art_tree_t *tree = calloc(1, sizeof(data_art_tree_t));
    if (!tree) {
        LOG_ERROR("Failed to allocate tree structure");
        return NULL;
    }
    
    tree->page_manager = page_manager;
    tree->buffer_pool = buffer_pool;
    tree->wal = wal;
    tree->root = NULL_NODE_REF;
    tree->version = 1;
    tree->size = 0;
    tree->key_size = key_size;
    tree->max_depth = key_size + 1;  // +1 for 0x00 terminator at leaf level
    tree->current_txn_id = 0;        // No active transaction
    tree->txn_buffer = NULL;         // No transaction buffer initially
    
    // Initialize write lock with writer-preference to prevent writer starvation.
    // Default pthread_rwlock is reader-preferred: with many concurrent readers,
    // a waiting writer can be starved indefinitely. Writer-preference queues new
    // rdlocks behind a pending wrlock, bounding writer wait time.
    pthread_rwlockattr_t rwlock_attr;
    pthread_rwlockattr_init(&rwlock_attr);
    pthread_rwlockattr_setkind_np(&rwlock_attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    if (pthread_rwlock_init(&tree->write_lock, &rwlock_attr) != 0) {
        LOG_ERROR("Failed to initialize write_lock");
        pthread_rwlockattr_destroy(&rwlock_attr);
        free(tree);
        return NULL;
    }
    pthread_rwlockattr_destroy(&rwlock_attr);
    
    // Initialize MVCC manager
    tree->mvcc_manager = mvcc_manager_create();
    if (!tree->mvcc_manager) {
        LOG_ERROR("Failed to create MVCC manager");
        pthread_rwlock_destroy(&tree->write_lock);
        free(tree);
        return NULL;
    }
    
    tree->cow_enabled = false;
    tree->active_versions = NULL;
    tree->num_active_versions = 0;
    
    // Initialize statistics
    tree->nodes_allocated = 0;
    tree->nodes_copied = 0;
    tree->cache_hits = 0;
    tree->cache_misses = 0;
    tree->overflow_pages_allocated = 0;
    tree->overflow_chain_reads = 0;
    
    LOG_INFO("Created persistent ART tree (key_size=%zu, max_depth=%zu, buffer_pool=%s, wal=%s)", 
             key_size, tree->max_depth, 
             buffer_pool ? "enabled" : "disabled",
             wal ? "enabled" : "disabled");
    
    return tree;
}

void data_art_destroy(data_art_tree_t *tree) {
    if (!tree) {
        return;
    }
    
    // Destroy write lock
    pthread_rwlock_destroy(&tree->write_lock);
    
    // Destroy MVCC manager
    if (tree->mvcc_manager) {
        mvcc_manager_destroy(tree->mvcc_manager);
        tree->mvcc_manager = NULL;
    }
    
    // Free active versions array if allocated
    if (tree->active_versions) {
        free(tree->active_versions);
    }
    
    // Note: We do NOT delete pages from disk - they persist
    // Use data_art_drop() if you want to remove all data
    
    LOG_INFO("Destroyed ART tree (size=%zu, nodes=%lu)", 
             tree->size, tree->nodes_allocated);
    
    free(tree);
}

// ============================================================================
// Basic Tree Operations - Inline Helpers
// ============================================================================

size_t data_art_size(const data_art_tree_t *tree) {
    return tree ? tree->size : 0;
}

bool data_art_is_empty(const data_art_tree_t *tree) {
    return tree ? node_ref_is_null(tree->root) : true;
}

// ============================================================================
// Node Management - Allocation
// ============================================================================

/**
 * Simple allocation strategy: allocate one node per page
 * 
 * TODO: Implement page-level free space tracking for packing multiple
 * small nodes into a single page. For now, we use 1 node = 1 page for
 * simplicity and to avoid fragmentation.
 */
node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL_NODE_REF;
    }
    
    // Page data size is PAGE_SIZE - PAGE_HEADER_SIZE
    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    
    if (size == 0 || size > max_size) {
        LOG_ERROR("Invalid node size: %zu (max=%zu)", size, max_size);
        return NULL_NODE_REF;
    }
    
    // Allocate a new page for this node
    uint64_t page_id = page_manager_alloc(tree->page_manager, size);
    if (page_id == 0) {
        LOG_ERROR("Failed to allocate page for node");
        return NULL_NODE_REF;
    }
    
    // Initialize reference count to 1 (caller holds initial reference)
    if (!page_gc_init_ref(tree->page_manager, page_id)) {
        LOG_ERROR("Failed to initialize ref count for page %lu", page_id);
        // Note: page is allocated but orphaned - will be cleaned up during compaction
        return NULL_NODE_REF;
    }
    
    tree->nodes_allocated++;
    
    // Node starts at offset 0 in the page
    node_ref_t ref = {.page_id = page_id, .offset = 0};
    
    return ref;
}

/**
 * Release reference to an old page
 * 
 * Called when a page is no longer referenced by the tree (e.g., after
 * copy-on-write creates a new version). Decrements ref count and invalidates
 * buffer pool entry if ref count reaches 0.
 * 
 * @param tree ART tree
 * @param old_ref Reference to old page
 */
void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref) {
    if (!tree || node_ref_is_null(old_ref)) {
        return;
    }
    
    uint32_t ref_count = page_gc_decref(tree->page_manager, old_ref.page_id);
    
    LOG_DEBUG("[RELEASE_PAGE] page=%lu | ref_count after decrement=%u",
              old_ref.page_id, ref_count);
    
    // If ref count reached 0, invalidate buffer pool entry
    if (ref_count == 0) {
        LOG_DEBUG("[RELEASE_PAGE] page=%lu reached ref_count=0, invalidating from buffer pool",
                  old_ref.page_id);
        buffer_pool_invalidate(tree->buffer_pool, old_ref.page_id);
        LOG_DEBUG("[RELEASE_PAGE] page=%lu marked as dead and removed from buffer pool",
                  old_ref.page_id);
    }
}

// ============================================================================
// Node Management - Load/Write
// ============================================================================

const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL;
    }
    
    if (node_ref_is_null(ref)) {
        return NULL;
    }
    
    // Buffer pool is required for correct operation
    // The old temp_pages fallback had corruption issues with deep recursion
    if (!tree->buffer_pool) {
        LOG_ERROR("Buffer pool is required but not configured");
        return NULL;
    }
    
    // Pin the page so it can't be evicted while we copy
    page_t *page = buffer_pool_get_pinned(tree->buffer_pool, ref.page_id);
    if (!page) {
        LOG_ERROR("Failed to load page %lu from buffer pool", ref.page_id);
        tree->cache_misses++;
        return NULL;
    }

    tree->cache_hits++;

    // Copy node data to thread-local arena so the pointer remains valid
    // even after the buffer pool page is evicted/freed
    const void *src = page->data + ref.offset;
    size_t node_size = data_art_node_size_from_data(src);

    void *copy = tls_arena_alloc(node_size);
    if (!copy) {
        LOG_ERROR("TLS arena exhausted (%zu bytes used, need %zu more)",
                  tls_arena_offset, node_size);
        buffer_pool_unpin(tree->buffer_pool, ref.page_id);
        return NULL;
    }
    memcpy(copy, src, node_size);

    // Unpin immediately — caller uses the arena copy, not the page
    buffer_pool_unpin(tree->buffer_pool, ref.page_id);
    return copy;
}

bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                         const void *node, size_t size) {
    if (!tree || !node) {
        LOG_ERROR("Invalid parameters: tree=%p, node=%p", (void*)tree, node);
        return false;
    }
    
    if (node_ref_is_null(ref)) {
        LOG_ERROR("Cannot write to NULL node reference");
        return false;
    }
    
    // Buffer pool is required
    if (!tree->buffer_pool) {
        LOG_ERROR("Buffer pool is required but not configured");
        return false;
    }
    
    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    if (size > max_size) {
        LOG_ERROR("Node size %zu exceeds page capacity %zu", size, max_size);
        return false;
    }
    
    // Try to get page from buffer pool (and pin it atomically)
    page_t *page = buffer_pool_get_pinned(tree->buffer_pool, ref.page_id);
    bool page_is_temp = false;
    page_t *temp_page_allocated = NULL;
    
    if (!page) {
        // Page doesn't exist in buffer pool yet - might be a new page
        // Allocate temporary page on heap for writing, then reload into buffer pool
        temp_page_allocated = malloc(sizeof(page_t));
        if (!temp_page_allocated) {
            LOG_ERROR("Failed to allocate temporary page");
            return false;
        }
        
        memset(temp_page_allocated, 0, sizeof(*temp_page_allocated));
        
        // Try to read existing page data
        page_result_t result = page_manager_read(tree->page_manager, ref.page_id, temp_page_allocated);
        if (result != PAGE_SUCCESS) {
            // Page doesn't exist yet - already zeroed
            temp_page_allocated->header.page_id = ref.page_id;
            LOG_TRACE("Page %lu doesn't exist yet, using zero-initialized page", ref.page_id);
        } else {
            LOG_TRACE("Successfully read existing page %lu from disk", ref.page_id);
        }
        page = temp_page_allocated;
        page_is_temp = true;
    }
    
    // Copy node data to page at offset
    LOG_DEBUG("[WRITE_NODE] Copying %zu bytes to page=%lu offset=%u", size, ref.page_id, ref.offset);
    memcpy(page->data + ref.offset, node, size);
    
    // Debug: Log what we're about to write
    uint8_t node_type = *(const uint8_t *)node;
    if (node_type == 0) {  // DATA_NODE_LEAF
        const data_art_leaf_t *debug_leaf = (const data_art_leaf_t *)node;
        LOG_TRACE("About to write leaf to page=%lu: key_len=%u, value_len=%u, size=%zu",
                  ref.page_id, debug_leaf->key_len, debug_leaf->value_len, size);
        LOG_DEBUG("[WRITE_NODE] Leaf in page buffer: type=%u flags=0x%02x key_len=%u value_len=%u",
                  debug_leaf->type, debug_leaf->flags, debug_leaf->key_len, debug_leaf->value_len);
    }
    
    // Compute checksum before writing
    page_compute_checksum(page);
    
    if (!page_is_temp) {
        // Page is in buffer pool - mark as dirty for later flush
        buffer_pool_mark_dirty(tree->buffer_pool, ref.page_id);
        
        // Unpin the page now that we're done modifying it
        buffer_pool_unpin(tree->buffer_pool, ref.page_id);
    } else {
        // Temp page for new allocation - write directly and reload into buffer pool
        LOG_DEBUG("[WRITE_NODE] Writing new page=%lu to disk", ref.page_id);
        page_result_t result = page_manager_write(tree->page_manager, page);
        if (result != PAGE_SUCCESS) {
            LOG_ERROR("Failed to write page %lu", ref.page_id);
            if (temp_page_allocated) {
                free(temp_page_allocated);
            }
            return false;
        }
        LOG_DEBUG("[WRITE_NODE] Page %lu written to disk successfully", ref.page_id);
        
        // Reload into buffer pool cache for subsequent reads
        buffer_pool_reload(tree->buffer_pool, ref.page_id);
        LOG_DEBUG("[WRITE_NODE] Reloaded page %lu into buffer pool cache", ref.page_id);
        
        // Sync to ensure data is persisted
        page_manager_sync(tree->page_manager);
        LOG_DEBUG("[WRITE_NODE] Page %lu synced to disk", ref.page_id);
    }
    
    // Free temporary page if allocated
    if (temp_page_allocated) {
        free(temp_page_allocated);
    }
    
    return true;
}

// ============================================================================
// Copy-on-Write (CoW) Support
// ============================================================================

node_ref_t data_art_cow_node(data_art_tree_t *tree, node_ref_t ref) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL_NODE_REF;
    }
    
    if (node_ref_is_null(ref)) {
        return NULL_NODE_REF;
    }
    
    // Load the original node
    const void *original = data_art_load_node(tree, ref);
    if (!original) {
        LOG_ERROR("Failed to load node for CoW");
        return NULL_NODE_REF;
    }
    
    // Determine node type and size
    uint8_t type = *(const uint8_t *)original;
    size_t size = 0;
    
    if (type == DATA_NODE_LEAF) {
        // Leaf nodes have variable size
        const data_art_leaf_t *leaf = (const data_art_leaf_t *)original;
        size = sizeof(data_art_leaf_t) + leaf->inline_data_len;
    } else {
        size = get_node_size(type);
        if (size == 0) {
            LOG_ERROR("Failed to determine size for node type %d", type);
            return NULL_NODE_REF;
        }
    }
    
    // Allocate new node
    node_ref_t new_ref = data_art_alloc_node(tree, size);
    if (node_ref_is_null(new_ref)) {
        LOG_ERROR("Failed to allocate node for CoW copy");
        return NULL_NODE_REF;
    }
    
    // Copy data to new node
    if (!data_art_write_node(tree, new_ref, original, size)) {
        LOG_ERROR("Failed to write CoW copy");
        // TODO: Free allocated node
        return NULL_NODE_REF;
    }
    
    tree->nodes_copied++;
    
    return new_ref;
}

// ============================================================================
// Helper Functions - Node Navigation
// ============================================================================

/**
 * Check if a node reference points to a leaf
 */
static bool is_leaf_ref(data_art_tree_t *tree, node_ref_t ref) {
    if (node_ref_is_null(ref)) {
        return false;
    }
    
    const void *node = data_art_load_node(tree, ref);
    if (!node) {
        return false;
    }
    
    uint8_t type = *(const uint8_t *)node;
    return type == DATA_NODE_LEAF;
}

/**
 * Find child node reference by byte key
 */
node_ref_t find_child(data_art_tree_t *tree, node_ref_t node_ref, uint8_t byte) {
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        LOG_INFO("find_child: failed to load node at page=%lu offset=%u",
                 node_ref.page_id, node_ref.offset);
        return NULL_NODE_REF;
    }
    
    uint8_t type = *(const uint8_t *)node;
    LOG_INFO("find_child: looking for byte=0x%02x in node type=%d at page=%lu", 
             byte, type, node_ref.page_id);
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            // Debug: log available keys
            char keys_str[32] = {0};
            for (int i = 0; i < n->num_children && i < 4; i++) {
                char temp[8];
                snprintf(temp, sizeof(temp), "0x%02x ", n->keys[i]);
                strcat(keys_str, temp);
            }
            LOG_INFO("find_child NODE_4: looking for 0x%02x, available keys: %s", byte, keys_str);
            
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    return (node_ref_t){.page_id = n->child_page_ids[i], 
                                       .offset = n->child_offsets[i]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            // Debug: log available keys
            char keys_str[128] = {0};
            for (int i = 0; i < n->num_children && i < 16; i++) {
                char temp[8];
                snprintf(temp, sizeof(temp), "0x%02x ", n->keys[i]);
                strcat(keys_str, temp);
            }
            LOG_INFO("find_child NODE_16: looking for 0x%02x, num_children=%d, available keys: %s", 
                     byte, n->num_children, keys_str);
            
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    return (node_ref_t){.page_id = n->child_page_ids[i], 
                                       .offset = n->child_offsets[i]};
                }
            }
            return NULL_NODE_REF;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            uint8_t idx = n->keys[byte];
            if (idx == 255) return NULL_NODE_REF;  // Empty slot
            return (node_ref_t){.page_id = n->child_page_ids[idx], 
                               .offset = n->child_offsets[idx]};
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            uint64_t page_id = n->child_page_ids[byte];
            if (page_id == 0) return NULL_NODE_REF;
            return (node_ref_t){.page_id = page_id, 
                               .offset = n->child_offsets[byte]};
        }
        default:
            return NULL_NODE_REF;
    }
}

/**
 * Find any leaf descendant of a node (for lazy expansion prefix verification)
 */
static const data_art_leaf_t* find_any_leaf_for_prefix(data_art_tree_t *tree, node_ref_t node_ref) {
    if (node_ref_is_null(node_ref)) {
        return NULL;
    }
    
    const void *node = data_art_load_node(tree, node_ref);
    if (!node) {
        return NULL;
    }
    
    uint8_t type = *(const uint8_t *)node;
    
    // If it's a leaf, return it
    if (type == DATA_NODE_LEAF) {
        return (const data_art_leaf_t *)node;
    }
    
    // Otherwise, recurse to first child
    node_ref_t child_ref = NULL_NODE_REF;
    
    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            if (n->num_children > 0) {
                child_ref = (node_ref_t){.page_id = n->child_page_ids[0],
                                        .offset = n->child_offsets[0]};
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            if (n->num_children > 0) {
                child_ref = (node_ref_t){.page_id = n->child_page_ids[0],
                                        .offset = n->child_offsets[0]};
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            // Find first non-empty slot
            for (int i = 0; i < 256; i++) {
                if (n->keys[i] != 255) {  // NODE48_EMPTY
                    uint8_t idx = n->keys[i];
                    child_ref = (node_ref_t){.page_id = n->child_page_ids[idx],
                                            .offset = n->child_offsets[idx]};
                    break;
                }
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            // Find first non-null child
            for (int i = 0; i < 256; i++) {
                if (n->child_page_ids[i] != 0) {
                    child_ref = (node_ref_t){.page_id = n->child_page_ids[i],
                                            .offset = n->child_offsets[i]};
                    break;
                }
            }
            break;
        }
    }
    
    return find_any_leaf_for_prefix(tree, child_ref);
}

/**
 * Check prefix match (path compression) with lazy expansion support
 * For search operations, we use optimistic matching for lazy expansion
 */
int check_prefix(data_art_tree_t *tree, node_ref_t node_ref,
                       const void *node, const uint8_t *key, 
                       size_t key_len, size_t depth) {
    const uint8_t *node_bytes = (const uint8_t *)node;
    uint8_t partial_len = node_bytes[2];  // Offset of partial_len field
    const uint8_t *partial = node_bytes + 4;  // Offset of partial array
    
    int max_cmp = (partial_len < 10) ? partial_len : 10;
    
    // Check inline portion
    for (int i = 0; i < max_cmp; i++) {
        if (depth + i >= key_len) return i;
        if (partial[i] != key[depth + i]) return i;
    }
    
    // For lazy expansion (partial_len > 10):
    // We optimistically assume the remaining bytes match.
    // The final leaf comparison will verify the full key.
    // This avoids the cost of traversing to a leaf just for prefix verification.
    return partial_len;
}

/**
 * Check if leaf matches key
 */
bool leaf_matches(const data_art_leaf_t *leaf, const uint8_t *key, size_t key_len) {
    if (leaf->key_len != key_len) {
        return false;
    }
    return memcmp(leaf->data, key, key_len) == 0;
}

// ============================================================================
// Core Operations - Search
// ============================================================================

// Internal get with snapshot support
static const void *data_art_get_internal(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                         size_t *value_len, mvcc_snapshot_t *snapshot, uint64_t snapshot_txn_id) {
    if (!tree || !key) {
        LOG_ERROR("Invalid parameters");
        return NULL;
    }

    // Reset thread-local arena — each get operation starts fresh
    tls_arena_reset();

    // Validate key size matches tree's configured size
    if (key_len != tree->key_size) {
        LOG_ERROR("Key size mismatch: expected %zu bytes, got %zu bytes",
                  tree->key_size, key_len);
        return NULL;
    }
    
    node_ref_t current = tree->root;

    if (node_ref_is_null(current)) {
        return NULL;  // Empty tree
    }

    size_t depth = 0;
    
    while (!node_ref_is_null(current)) {
        const void *node = data_art_load_node(tree, current);
        if (!node) {
            LOG_ERROR("Failed to load node at page=%lu, offset=%u", 
                     current.page_id, current.offset);
            return NULL;
        }
        
        uint8_t type = *(const uint8_t *)node;
        
        // Check if leaf
        if (type == DATA_NODE_LEAF) {
            const data_art_leaf_t *leaf = (const data_art_leaf_t *)node;
            LOG_DEBUG("Found LEAF at page=%lu: key_len=%u, value_len=%u, flags=0x%02x, xmin=%lu, xmax=%lu",
                      current.page_id, leaf->key_len, leaf->value_len, leaf->flags, leaf->xmin, leaf->xmax);
            
            // Walk version chain to find visible version
            node_ref_t version_ref = current;
            const data_art_leaf_t *visible_leaf = NULL;
            int chain_length = 0;
            const int MAX_CHAIN_LENGTH = 1000;  // Prevent infinite loops
            
            LOG_TRACE("Starting version chain walk from page=%lu offset=%u", version_ref.page_id, version_ref.offset);
            
            // Start with the leaf we already loaded
            const data_art_leaf_t *candidate = leaf;
            
            while (chain_length < MAX_CHAIN_LENGTH) {
                if (!candidate || candidate->type != DATA_NODE_LEAF) {
                    LOG_ERROR("Invalid version chain: candidate=%p, type=%u", 
                              (void*)candidate, 
                              candidate ? candidate->type : 255);
                    break;
                }
                
                LOG_TRACE("Checking version chain[%d]: page=%lu offset=%u xmin=%lu xmax=%lu",
                         chain_length, version_ref.page_id, version_ref.offset, candidate->xmin, candidate->xmax);
                
                // MVCC visibility check
                bool visible = true;
                if (snapshot && tree->mvcc_manager) {
                    visible = mvcc_is_visible(tree->mvcc_manager, snapshot,
                                              candidate->xmin, candidate->xmax, snapshot_txn_id);
                    if (!visible) {
                        LOG_TRACE("Version not visible to snapshot (xmin=%lu, xmax=%lu)",
                                 candidate->xmin, candidate->xmax);
                    }
                } else if (candidate->xmax != 0) {
                    // No snapshot but leaf is deleted - not visible
                    visible = false;
                    LOG_TRACE("Version is deleted (xmax=%lu), not visible", candidate->xmax);
                }
                
                if (visible) {
                    visible_leaf = candidate;
                    LOG_DEBUG("Found visible version at chain[%d]: xmin=%lu xmax=%lu", 
                             chain_length, candidate->xmin, candidate->xmax);
                    break;
                }
                
                // Move to previous version
                if (node_ref_is_null(candidate->prev_version)) {
                    LOG_DEBUG("End of version chain (no prev_version)");
                    break;
                }
                
                version_ref = candidate->prev_version;
                chain_length++;
                
                // Load next version in chain
                candidate = (const data_art_leaf_t *)data_art_load_node(tree, version_ref);
            }
            
            if (chain_length >= MAX_CHAIN_LENGTH) {
                LOG_ERROR("Version chain too long (>%d), possible corruption", MAX_CHAIN_LENGTH);
                return NULL;
            }
            
            if (!visible_leaf) {
                LOG_DEBUG("No visible version found in chain (length=%d)", chain_length);
                return NULL;  // No visible version in chain
            }
            
            leaf = visible_leaf;  // Use the visible version
            
            // Debug: Verify the leaf structure makes sense
            if (leaf->value_len > 1024) {  // Suspiciously large
                LOG_ERROR("CORRUPTION DETECTED: leaf at page=%lu offset=%u has suspiciously large value_len=%u, key_len=%u",
                          current.page_id, current.offset, leaf->value_len, leaf->key_len);
                LOG_ERROR("Leaf dump: type=%u flags=0x%02x, overflow_page=%lu, inline_data_len=%u",
                          leaf->type, leaf->flags, leaf->overflow_page, leaf->inline_data_len);
                // Dump raw bytes of entire leaf header
                const uint8_t *raw = (const uint8_t *)leaf;
                LOG_ERROR("Raw bytes [0-23]: %02x %02x %02x %02x | %02x %02x %02x %02x | %02x %02x %02x %02x | %02x %02x %02x %02x %02x %02x %02x %02x | %02x %02x %02x %02x",
                          raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                          raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
                          raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22], raw[23]);
                LOG_ERROR("Field breakdown: type@0=%02x flags@1=%02x pad@2-3=%02x%02x | key_len@4-7=%02x%02x%02x%02x | value_len@8-11=%02x%02x%02x%02x | overflow@12-19=%02x%02x%02x%02x%02x%02x%02x%02x | inline_len@20-23=%02x%02x%02x%02x",
                          raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                          raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
                          raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22], raw[23]);
            }
            
            if (leaf_matches(leaf, key, key_len)) {
                if (value_len) {
                    *value_len = leaf->value_len;
                }
                LOG_TRACE("Leaf matches! Returning value_len=%u", leaf->value_len);
                
                // Allocate buffer for value (caller must free)
                void *value_copy = malloc(leaf->value_len);
                if (!value_copy) {
                    LOG_ERROR("Failed to allocate memory for value");
                    return NULL;
                }
                
                // Handle overflow if needed
                if (leaf->flags & LEAF_FLAG_OVERFLOW) {
                    // Read full value from overflow chain
                    if (!data_art_read_overflow_value(tree, leaf, value_copy)) {
                        free(value_copy);
                        LOG_ERROR("Failed to read overflow value");
                        return NULL;
                    }
                    return value_copy;
                }
                
                // Copy inline value
                memcpy(value_copy, leaf->data + leaf->key_len, leaf->value_len);
                return value_copy;
            }
            return NULL;
        }
        
        // Check compressed path
        const uint8_t *node_bytes = (const uint8_t *)node;
        uint8_t partial_len = node_bytes[2];
        
        if (partial_len > 0) {
            LOG_INFO("Checking prefix: node at page=%lu has partial_len=%u, current depth=%zu", 
                     current.page_id, partial_len, depth);
            
            int prefix_match = check_prefix(tree, current, node, key, key_len, depth);
            
            // Check inline portion (up to 10 bytes)
            int expected_match = (partial_len < 10) ? partial_len : 10;
            
            LOG_INFO("Prefix check result: prefix_match=%d, expected_match=%d", 
                     prefix_match, expected_match);
            
            if (prefix_match < expected_match) {
                // Show what mismatched - use hex for clarity
                char key_hex[64];
                char node_hex[64];
                int show_bytes = (expected_match < 10) ? expected_match : 10;
                
                for (int i = 0; i < show_bytes; i++) {
                    if (depth + i < key_len) {
                        snprintf(key_hex + i*3, 4, "%02x ", key[depth + i]);
                    } else {
                        snprintf(key_hex + i*3, 4, "?? ");
                    }
                    snprintf(node_hex + i*3, 4, "%02x ", node_bytes[4 + i]);
                }
                
                LOG_INFO("PREFIX MISMATCH at depth=%zu (matched %d/%d bytes)", 
                         depth, prefix_match, expected_match);
                LOG_INFO("  Key bytes:  %s", key_hex);
                LOG_INFO("  Node bytes: %s", node_hex);
                
                return NULL;  // Prefix mismatch in inline portion
            }
            
            // For lazy expansion (partial_len > 10), verify full prefix via leaf
            if (partial_len > 10) {
                // Need to check remaining bytes - traverse to any leaf
                // But we'll do this optimistically and verify at the leaf level
                // The leaf comparison will catch any mismatch in bytes 10+
            }
            
            depth += partial_len;  // Skip the full compressed path
        }
        
        // Get next byte to search
        // Fixed-size keys optimization: check exhaustion only when needed
        uint8_t byte;
        if (depth < key_len) {
            byte = key[depth];
            depth++;  // Move past the byte we just used for lookup
        } else {
            // Key exhausted, look for NULL byte child (leaf level)
            byte = 0x00;
        }
        
        current = find_child(tree, current, byte);
        if (node_ref_is_null(current)) {
            // Debug: child not found
            char key_str[64];
            snprintf(key_str, sizeof(key_str), "%.*s", (int)(key_len < 40 ? key_len : 40), key);
            LOG_INFO("Child lookup failed: key='%s', depth=%zu, byte=0x%02x('%c')", 
                     key_str, depth, byte, (byte >= 32 && byte < 127) ? byte : '?');
        } else {
            LOG_INFO("Child found, advancing to page=%lu offset=%u", 
                     current.page_id, current.offset);
        }
    }
    
    LOG_INFO("Exited search loop, current is null, returning NULL");
    return NULL;  // Not found
}

// Public API: Get with snapshot
// Readers acquire rdlock to prevent torn reads from in-place parent updates.
// Writer-preference rwlock ensures waiting writers are not starved by readers.
const void *data_art_get_snapshot(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                                   size_t *value_len, data_art_snapshot_t *snapshot) {
    pthread_rwlock_rdlock(&tree->write_lock);
    const void *result;
    if (snapshot) {
        result = data_art_get_internal(tree, key, key_len, value_len,
                                        snapshot->mvcc_snapshot, snapshot->txn_id);
    } else {
        result = data_art_get_internal(tree, key, key_len, value_len, NULL, 0);
    }
    pthread_rwlock_unlock(&tree->write_lock);
    return result;
}

// Legacy API: Get without snapshot (reads latest committed)
const void *data_art_get(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                         size_t *value_len) {
    return data_art_get_snapshot(tree, key, key_len, value_len, NULL);
}

bool data_art_contains(data_art_tree_t *tree, const uint8_t *key, size_t key_len) {
    size_t len;
    return data_art_get(tree, key, key_len, &len) != NULL;
}

// ============================================================================
// NOTE: Insert and Delete operations are implemented in separate files:
// - data_art_insert.c (recursive insert with node growth)
// - data_art_delete.c (recursive delete with node shrinking - TODO)
// 
// Node growth/shrinking operations are in:
// - data_art_node_ops.c (add_child, remove_child, node transitions)
// ============================================================================

// ============================================================================
// Transaction Support
// ============================================================================

bool data_art_begin_txn(data_art_tree_t *tree, uint64_t *txn_id_out) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot begin transaction: WAL not enabled");
        return false;
    }
    
    // Check if this thread already has an active transaction
    thread_txn_context_t *ctx = get_txn_context();
    if (ctx && ctx->txn_buffer) {
        LOG_ERROR("Transaction already active for this thread (txn_id=%lu)", ctx->txn_id);
        return false;
    }
    
    // Allocate transaction ID from MVCC manager (unified ID space)
    uint64_t txn_id;
    if (!mvcc_begin_txn(tree->mvcc_manager, &txn_id)) {
        LOG_ERROR("Failed to begin MVCC transaction");
        return false;
    }
    
    // Log to WAL
    if (!wal_log_begin_txn(tree->wal, txn_id, NULL)) {
        LOG_ERROR("Failed to log transaction to WAL");
        mvcc_abort_txn(tree->mvcc_manager, txn_id);
        return false;
    }
    
    // Create transaction buffer
    txn_buffer_t *buffer = txn_buffer_create(txn_id, 16);
    if (!buffer) {
        LOG_ERROR("Failed to create transaction buffer");
        wal_log_abort_txn(tree->wal, txn_id, NULL);
        mvcc_abort_txn(tree->mvcc_manager, txn_id);
        return false;
    }
    
    // Create or update thread-local context
    if (!ctx) {
        ctx = malloc(sizeof(thread_txn_context_t));
        if (!ctx) {
            LOG_ERROR("Failed to allocate transaction context");
            txn_buffer_destroy(buffer);
            wal_log_abort_txn(tree->wal, txn_id, NULL);
            mvcc_abort_txn(tree->mvcc_manager, txn_id);
            return false;
        }
        set_txn_context(ctx);
    }
    
    ctx->tree = tree;
    ctx->txn_id = txn_id;
    ctx->txn_buffer = buffer;
    
    // Also set on tree for backward compatibility (single-threaded access)
    tree->current_txn_id = txn_id;
    tree->txn_buffer = buffer;
    
    if (txn_id_out) {
        *txn_id_out = txn_id;
    }
    
    LOG_INFO("Began transaction %lu (MVCC-allocated) for thread %p", txn_id, (void*)pthread_self());
    return true;
}

bool data_art_commit_txn(data_art_tree_t *tree) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot commit transaction: WAL not enabled");
        return false;
    }
    
    // Get thread-local transaction context
    thread_txn_context_t *ctx = get_txn_context();
    if (!ctx || !ctx->txn_buffer) {
        LOG_ERROR("No active transaction to commit for this thread");
        return false;
    }
    
    if (ctx->tree != tree) {
        LOG_ERROR("Transaction belongs to different tree");
        return false;
    }
    
    uint64_t txn_id = ctx->txn_id;
    txn_buffer_t *buffer = ctx->txn_buffer;
    
    // Apply all buffered operations to the tree
    for (size_t i = 0; i < buffer->num_ops; i++) {
        txn_operation_t *op = &buffer->operations[i];
        
        // Temporarily clear txn_buffer to prevent recursive buffering
        ctx->txn_buffer = NULL;
        tree->txn_buffer = NULL;
        
        bool success;
        if (op->type == TXN_OP_INSERT) {
            success = data_art_insert(tree, op->key, op->key_len,
                                     op->value, op->value_len);
        } else { // TXN_OP_DELETE
            // Delete may return false if key was already deleted by a
            // concurrent transaction — this is expected, not an error.
            success = data_art_delete(tree, op->key, op->key_len);
            if (!success) {
                LOG_DEBUG("Delete for key not found during commit (concurrent delete) — skipping");
                success = true;
            }
        }

        // Restore buffer
        ctx->txn_buffer = buffer;
        tree->txn_buffer = buffer;

        if (!success) {
            LOG_ERROR("Failed to apply operation %zu during commit", i);
            // Cleanup and abort
            txn_buffer_destroy(buffer);
            ctx->txn_buffer = NULL;
            tree->txn_buffer = NULL;
            tree->current_txn_id = 0;
            mvcc_abort_txn(tree->mvcc_manager, txn_id);
            return false;
        }
    }
    
    // Log commit to WAL
    if (!wal_log_commit_txn(tree->wal, txn_id, NULL)) {
        LOG_ERROR("Failed to commit transaction %lu to WAL", txn_id);
        // Operations already applied - durability failure
        txn_buffer_destroy(buffer);
        ctx->txn_buffer = NULL;
        tree->txn_buffer = NULL;
        tree->current_txn_id = 0;
        return false;
    }
    
    // Commit MVCC transaction
    if (!mvcc_commit_txn(tree->mvcc_manager, txn_id)) {
        LOG_ERROR("Failed to commit MVCC transaction %lu", txn_id);
    }
    
    // Clean up
    txn_buffer_destroy(buffer);
    ctx->txn_buffer = NULL;
    tree->txn_buffer = NULL;
    tree->current_txn_id = 0;
    
    LOG_INFO("Committed transaction %lu for thread %p", txn_id, (void*)pthread_self());
    return true;
}

bool data_art_abort_txn(data_art_tree_t *tree) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot abort transaction: WAL not enabled");
        return false;
    }
    
    // Get thread-local transaction context
    thread_txn_context_t *ctx = get_txn_context();
    if (!ctx || !ctx->txn_buffer) {
        LOG_ERROR("No active transaction to abort for this thread");
        return false;
    }
    
    if (ctx->tree != tree) {
        LOG_ERROR("Transaction belongs to different tree");
        return false;
    }
    
    uint64_t txn_id = ctx->txn_id;
    size_t num_ops = txn_buffer_size(ctx->txn_buffer);
    
    // Discard all buffered operations (no tree modification)
    txn_buffer_destroy(ctx->txn_buffer);
    ctx->txn_buffer = NULL;
    tree->txn_buffer = NULL;
    
    // Log abort to WAL
    if (!wal_log_abort_txn(tree->wal, txn_id, NULL)) {
        LOG_ERROR("Failed to abort transaction %lu", txn_id);
        tree->current_txn_id = 0;
        return false;
    }
    
    // Abort MVCC transaction
    if (!mvcc_abort_txn(tree->mvcc_manager, txn_id)) {
        LOG_ERROR("Failed to abort MVCC transaction %lu", txn_id);
    }
    
    tree->current_txn_id = 0;
    
    LOG_INFO("Aborted transaction %lu (discarded %zu operations) for thread %p", 
             txn_id, num_ops, (void*)pthread_self());
    return true;
}

// ============================================================================
// Snapshot API
// ============================================================================

data_art_snapshot_t *data_art_begin_snapshot(data_art_tree_t *tree) {
    if (!tree || !tree->mvcc_manager) {
        LOG_ERROR("Cannot begin snapshot: MVCC manager not initialized");
        return NULL;
    }
    
    // Allocate snapshot handle
    data_art_snapshot_t *snapshot = malloc(sizeof(data_art_snapshot_t));
    if (!snapshot) {
        LOG_ERROR("Failed to allocate snapshot handle");
        return NULL;
    }
    
    // Allocate a transaction ID for the snapshot
    if (!mvcc_begin_txn(tree->mvcc_manager, &snapshot->txn_id)) {
        LOG_ERROR("Failed to begin MVCC transaction for snapshot");
        free(snapshot);
        return NULL;
    }
    
    // Create MVCC snapshot
    snapshot->mvcc_snapshot = mvcc_snapshot_create(tree->mvcc_manager);
    if (!snapshot->mvcc_snapshot) {
        LOG_ERROR("Failed to create MVCC snapshot");
        mvcc_abort_txn(tree->mvcc_manager, snapshot->txn_id);
        free(snapshot);
        return NULL;
    }
    
    LOG_DEBUG("Started snapshot %lu (txn_id=%lu, xmin=%lu, xmax=%lu, %zu active txns)",
              snapshot->mvcc_snapshot->snapshot_id,
              snapshot->txn_id,
              snapshot->mvcc_snapshot->xmin,
              snapshot->mvcc_snapshot->xmax,
              snapshot->mvcc_snapshot->num_active);
    
    return snapshot;
}

void data_art_end_snapshot(data_art_tree_t *tree, data_art_snapshot_t *snapshot) {
    if (!tree || !snapshot) {
        return;
    }
    
    LOG_DEBUG("Ending snapshot %lu", snapshot->mvcc_snapshot->snapshot_id);
    
    // Release MVCC snapshot
    mvcc_snapshot_release(tree->mvcc_manager, snapshot->mvcc_snapshot);
    
    // Commit the snapshot's transaction
    if (snapshot->txn_id > 0) {
        mvcc_commit_txn(tree->mvcc_manager, snapshot->txn_id);
    }
    
    // Free the handle
    free(snapshot);
}

// ============================================================================
// Checkpoint and Recovery
// ============================================================================

bool data_art_checkpoint(data_art_tree_t *tree, uint64_t *checkpoint_lsn_out) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot checkpoint: WAL not enabled");
        return false;
    }
    
    // Flush all dirty pages to disk
    if (!data_art_flush(tree)) {
        LOG_ERROR("Failed to flush tree pages during checkpoint");
        return false;
    }
    
    // Log checkpoint with current tree state
    uint64_t lsn;
    if (!wal_log_checkpoint(tree->wal, 
                           tree->root.page_id,
                           tree->root.offset,
                           tree->size,
                           tree->nodes_allocated,  // Use nodes allocated as proxy
                           &lsn)) {
        LOG_ERROR("Failed to log checkpoint to WAL");
        return false;
    }
    
    if (checkpoint_lsn_out) {
        *checkpoint_lsn_out = lsn;
    }
    
    LOG_INFO("Created checkpoint at LSN %lu (root=%lu:%u, size=%zu)",
             lsn, tree->root.page_id, tree->root.offset, tree->size);
    
    return true;
}

// Callback for WAL replay
static bool apply_wal_entry(void *context, const wal_entry_header_t *header, const void *payload) {
    data_art_tree_t *tree = (data_art_tree_t *)context;
    
    switch (header->entry_type) {
        case WAL_ENTRY_INSERT: {
            const wal_insert_payload_t *insert = (const wal_insert_payload_t *)payload;
            const uint8_t *key = insert->data;
            const uint8_t *value = insert->data + insert->key_len;
            
            // Note: insert may fail if key already exists - that's ok during replay
            data_art_insert(tree, key, insert->key_len, value, insert->value_len);
            break;
        }
        
        case WAL_ENTRY_DELETE: {
            const wal_delete_payload_t *del = (const wal_delete_payload_t *)payload;
            const uint8_t *key = del->key;
            
            // Note: delete may fail if key doesn't exist - that's ok during replay
            data_art_delete(tree, key, del->key_len);
            break;
        }
        
        case WAL_ENTRY_BEGIN_TXN:
        case WAL_ENTRY_COMMIT_TXN:
        case WAL_ENTRY_ABORT_TXN:
            // Transaction markers - already handled by WAL replay logic
            break;
            
        case WAL_ENTRY_CHECKPOINT: {
            const wal_checkpoint_payload_t *ckpt = (const wal_checkpoint_payload_t *)payload;
            
            // Restore tree state from checkpoint
            tree->root.page_id = ckpt->root_page_id;
            tree->root.offset = ckpt->root_offset;
            tree->size = ckpt->tree_size;
            
            LOG_INFO("Restored checkpoint: root=%lu:%u, size=%zu",
                     tree->root.page_id, tree->root.offset, tree->size);
            break;
        }
        
        default:
            LOG_WARN("Unknown WAL entry type: %u", header->entry_type);
            break;
    }
    
    return true;  // Continue replay
}

int64_t data_art_recover(data_art_tree_t *tree, uint64_t start_lsn) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot recover: WAL not enabled");
        return -1;
    }
    
    LOG_INFO("Starting recovery from LSN %lu", start_lsn);
    
    // Replay WAL entries to rebuild tree state
    // start_lsn == 0 means replay from beginning
    uint64_t end_lsn = UINT64_MAX;  // Replay to end
    
    int64_t entries = wal_replay(tree->wal, start_lsn, end_lsn, tree, apply_wal_entry);
    
    if (entries < 0) {
        LOG_ERROR("Recovery failed during WAL replay");
        return -1;
    }
    
    LOG_INFO("Recovery complete: replayed %ld entries, tree size=%zu",
             entries, tree->size);
    
    return entries;
}
