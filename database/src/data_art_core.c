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
#include "db_error.h"
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

// ============================================================================
// Lock-Free Read Helpers
// ============================================================================

// Publish the current tree->root so lock-free readers can see it.
// Called after auto-commit, commit_txn, or tree initialization.
void data_art_publish_root(data_art_tree_t *tree) {
    atomic_store_explicit(&tree->committed_root_page_id,
                          tree->root.page_id, memory_order_release);
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
    
    // Initialize write lock (mutex). Readers are lock-free via atomic committed root,
    // so only writers need serialization.
    if (pthread_mutex_init(&tree->write_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize write_lock");
        free(tree);
        return NULL;
    }
    
    // Initialize MVCC manager
    tree->mvcc_manager = mvcc_manager_create();
    if (!tree->mvcc_manager) {
        LOG_ERROR("Failed to create MVCC manager");
        pthread_mutex_destroy(&tree->write_lock);
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
    
    // Initialize committed root for lock-free readers (empty tree → page_id 0)
    atomic_store_explicit(&tree->committed_root_page_id, 0, memory_order_release);

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
    pthread_mutex_destroy(&tree->write_lock);
    
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
    // With lock-free reads, snapshots may still be traversing old pages.
    // Releasing/invalidating pages here would cause readers to see corrupted data.
    // Old pages are left alive; epoch-based GC will reclaim them when no snapshot
    // can reference them.
    (void)tree;
    (void)old_ref;
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
// NOTE: Search, Insert and Delete operations are implemented in separate files:
// - data_art_search.c (tree traversal, get, contains)
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
        DB_ERROR(DB_ERROR_INVALID_ARG, "WAL not enabled");
        return false;
    }

    // Get thread-local transaction context
    thread_txn_context_t *ctx = get_txn_context();
    if (!ctx || !ctx->txn_buffer) {
        DB_ERROR(DB_ERROR_TXN_NOT_FOUND, "no active transaction");
        return false;
    }

    if (ctx->tree != tree) {
        DB_ERROR(DB_ERROR_TXN_CONFLICT, "transaction belongs to different tree");
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
            DB_ERROR(DB_ERROR_IO, "failed to apply operation %zu", i);
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
        DB_ERROR(DB_ERROR_IO, "WAL commit failed (txn_id=%lu)", txn_id);
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

    // Publish final root for lock-free readers
    data_art_publish_root(tree);

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
        DB_ERROR(DB_ERROR_INVALID_ARG, "WAL not enabled");
        return false;
    }

    // Get thread-local transaction context
    thread_txn_context_t *ctx = get_txn_context();
    if (!ctx || !ctx->txn_buffer) {
        DB_ERROR(DB_ERROR_TXN_NOT_FOUND, "no active transaction");
        return false;
    }

    if (ctx->tree != tree) {
        DB_ERROR(DB_ERROR_TXN_CONFLICT, "transaction belongs to different tree");
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
        DB_ERROR(DB_ERROR_IO, "WAL abort failed (txn_id=%lu)", txn_id);
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
    
    // Capture committed root at snapshot creation time (lock-free)
    snapshot->root_page_id = atomic_load_explicit(&tree->committed_root_page_id,
                                                   memory_order_acquire);

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

// ============================================================================
// Transaction-Aware WAL Replay
//
// Two-pass recovery:
//   Pass 1: Scan WAL for BEGIN_TXN/COMMIT_TXN markers to identify uncommitted txns
//   Pass 2: Replay entries, skipping INSERT/DELETE from uncommitted transactions
//
// Auto-commit operations (no BEGIN_TXN in WAL) are always applied.
// ============================================================================

// Dynamic array append for txn_id tracking
static void txn_id_append(uint64_t **arr, size_t *count, size_t *cap, uint64_t id) {
    if (*count >= *cap) {
        *cap = (*cap == 0) ? 16 : *cap * 2;
        *arr = realloc(*arr, *cap * sizeof(uint64_t));
    }
    (*arr)[(*count)++] = id;
}

// Pass 1 context: collect BEGIN and COMMIT txn_ids
typedef struct {
    uint64_t *begun;        size_t begun_count, begun_cap;
    uint64_t *committed;    size_t committed_count, committed_cap;
} txn_scan_ctx_t;

static bool scan_txn_markers(void *ctx, const wal_entry_header_t *header, const void *payload) {
    (void)payload;
    txn_scan_ctx_t *scan = (txn_scan_ctx_t *)ctx;
    if (header->entry_type == WAL_ENTRY_BEGIN_TXN)
        txn_id_append(&scan->begun, &scan->begun_count, &scan->begun_cap, header->txn_id);
    else if (header->entry_type == WAL_ENTRY_COMMIT_TXN)
        txn_id_append(&scan->committed, &scan->committed_count, &scan->committed_cap, header->txn_id);
    return true;
}

// Pass 2 context: apply entries, skipping uncommitted transactions
typedef struct {
    data_art_tree_t *tree;
    uint64_t *uncommitted;
    size_t uncommitted_count;
    int64_t skipped;
} txn_apply_ctx_t;

static bool is_uncommitted_txn(uint64_t txn_id, const txn_apply_ctx_t *ctx) {
    for (size_t i = 0; i < ctx->uncommitted_count; i++)
        if (ctx->uncommitted[i] == txn_id) return true;
    return false;
}

static bool apply_wal_entry_txn_aware(void *context, const wal_entry_header_t *header, const void *payload) {
    txn_apply_ctx_t *ctx = (txn_apply_ctx_t *)context;

    switch (header->entry_type) {
        case WAL_ENTRY_INSERT: {
            if (is_uncommitted_txn(header->txn_id, ctx)) { ctx->skipped++; break; }
            const wal_insert_payload_t *insert = (const wal_insert_payload_t *)payload;
            const uint8_t *key = insert->data;
            const uint8_t *value = insert->data + insert->key_len;
            data_art_insert(ctx->tree, key, insert->key_len, value, insert->value_len);
            break;
        }

        case WAL_ENTRY_DELETE: {
            if (is_uncommitted_txn(header->txn_id, ctx)) { ctx->skipped++; break; }
            const wal_delete_payload_t *del = (const wal_delete_payload_t *)payload;
            data_art_delete(ctx->tree, del->key, del->key_len);
            break;
        }

        case WAL_ENTRY_CHECKPOINT: {
            const wal_checkpoint_payload_t *ckpt = (const wal_checkpoint_payload_t *)payload;
            ctx->tree->root.page_id = ckpt->root_page_id;
            ctx->tree->root.offset = ckpt->root_offset;
            ctx->tree->size = ckpt->tree_size;
            LOG_INFO("Restored checkpoint: root=%lu:%u, size=%zu",
                     ckpt->root_page_id, ckpt->root_offset, ctx->tree->size);
            break;
        }

        case WAL_ENTRY_BEGIN_TXN:
        case WAL_ENTRY_COMMIT_TXN:
        case WAL_ENTRY_ABORT_TXN:
        case WAL_ENTRY_NOOP:
            break;

        default:
            LOG_WARN("Unknown WAL entry type: %u", header->entry_type);
            break;
    }

    return true;
}

int64_t data_art_recover(data_art_tree_t *tree, uint64_t start_lsn) {
    if (!tree || !tree->wal) {
        LOG_ERROR("Cannot recover: WAL not enabled");
        return -1;
    }

    LOG_INFO("Starting recovery from LSN %lu", start_lsn);

    wal_t *saved_wal = tree->wal;
    mvcc_manager_t *saved_mvcc = tree->mvcc_manager;
    uint64_t end_lsn = UINT64_MAX;

    // Pass 1: Scan WAL for transaction markers
    // Note: scan may return -1 on corruption, but we proceed with whatever
    // markers were collected. Txns with BEGIN but no COMMIT (due to corruption
    // truncating the WAL) are correctly treated as uncommitted.
    txn_scan_ctx_t scan = {0};
    wal_replay(saved_wal, start_lsn, end_lsn, &scan, scan_txn_markers);

    // Build uncommitted set = begun txns that never committed
    uint64_t *uncommitted = NULL;
    size_t uncommitted_count = 0, uncommitted_cap = 0;
    for (size_t i = 0; i < scan.begun_count; i++) {
        bool found_commit = false;
        for (size_t j = 0; j < scan.committed_count; j++) {
            if (scan.begun[i] == scan.committed[j]) { found_commit = true; break; }
        }
        if (!found_commit)
            txn_id_append(&uncommitted, &uncommitted_count, &uncommitted_cap, scan.begun[i]);
    }
    free(scan.begun);
    free(scan.committed);

    if (uncommitted_count > 0)
        LOG_WARN("Recovery: %zu uncommitted transactions will be skipped", uncommitted_count);

    // Pass 2: Apply entries with transaction awareness
    txn_apply_ctx_t apply = {
        .tree = tree,
        .uncommitted = uncommitted,
        .uncommitted_count = uncommitted_count,
        .skipped = 0
    };

    // Disable WAL and MVCC to prevent re-logging and unnecessary overhead
    tree->wal = NULL;
    tree->mvcc_manager = NULL;

    int64_t entries = wal_replay(saved_wal, start_lsn, end_lsn, &apply, apply_wal_entry_txn_aware);

    tree->wal = saved_wal;
    tree->mvcc_manager = saved_mvcc;
    free(uncommitted);

    if (entries < 0) {
        LOG_ERROR("Recovery failed during WAL replay");
        return -1;
    }

    LOG_INFO("Recovery complete: %ld entries replayed, %ld skipped from %zu uncommitted txns, tree size=%zu",
             entries, apply.skipped, uncommitted_count, tree->size);

    // Publish recovered root for lock-free readers
    data_art_publish_root(tree);

    return entries;
}
