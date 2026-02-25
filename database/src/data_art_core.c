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

// Forward declarations
static void drain_pending_frees(data_art_tree_t *tree);
void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);

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
    // Store offset first (readers read page_id first with acquire)
    atomic_store_explicit(&tree->committed_root_offset,
                          tree->root.offset, memory_order_relaxed);
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
// Slot Page Bitmap Helpers
// ============================================================================

/**
 * Find first free (0) bit in bitmap, searching up to 'count' bits.
 * Returns bit index, or -1 if all occupied.
 */
static int bitmap_find_free(const uint8_t *bitmap, int count) {
    for (int byte_idx = 0; byte_idx < (count + 7) / 8; byte_idx++) {
        uint8_t b = bitmap[byte_idx];
        if (b == 0xFF) continue;  // All 8 bits set
        for (int bit = 0; bit < 8; bit++) {
            int idx = byte_idx * 8 + bit;
            if (idx >= count) return -1;
            if (!(b & (1u << bit))) return idx;
        }
    }
    return -1;
}

static inline void bitmap_set(uint8_t *bitmap, int idx) {
    bitmap[idx / 8] |= (1u << (idx % 8));
}

static inline void bitmap_clear(uint8_t *bitmap, int idx) {
    bitmap[idx / 8] &= ~(1u << (idx % 8));
}

static inline bool bitmap_test(const uint8_t *bitmap, int idx) {
    return (bitmap[idx / 8] & (1u << (idx % 8))) != 0;
}

// ============================================================================
// Slot Allocator
// ============================================================================

/**
 * Initialize slot class sizes based on node struct sizes and tree key_size.
 */
static void slot_allocator_init(data_art_tree_t *tree) {
    // Default leaf inline size: key_size + 32 (typical Ethereum value)
    size_t leaf_size = sizeof(data_art_leaf_t) + tree->key_size + 32;

    struct { uint8_t type; size_t raw_size; } classes[] = {
        { DATA_NODE_4,    sizeof(data_art_node4_t) },
        { DATA_NODE_16,   sizeof(data_art_node16_t) },
        { DATA_NODE_48,   sizeof(data_art_node48_t) },
        { DATA_NODE_256,  sizeof(data_art_node256_t) },
        { DATA_NODE_LEAF, leaf_size },
    };

    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        uint16_t aligned = (uint16_t)ALIGN8(classes[i].raw_size);
        uint16_t slots = (uint16_t)(SLOT_AREA_SIZE / aligned);
        if (slots > 64) slots = 64;  // Bitmap is 8 bytes = 64 bits
        if (slots == 0) slots = 1;

        tree->slot_classes[i].node_type = classes[i].type;
        tree->slot_classes[i].slot_size = aligned;
        tree->slot_classes[i].slots_per_page = slots;
        tree->slot_classes[i].current_page_id = 0;

        LOG_DEBUG("Slot class %d: type=%u raw_size=%zu slot_size=%u slots/page=%u",
                  i, classes[i].type, classes[i].raw_size, aligned, slots);
    }
}

/**
 * Map a node size to a slot class index.
 * Returns -1 if the size exceeds all slot classes (use dedicated page).
 */
static int size_to_slot_class(data_art_tree_t *tree, size_t size) {
    int best = -1;
    uint16_t best_size = UINT16_MAX;
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        // Skip single-slot classes — no benefit from multi-node-per-page
        if (tree->slot_classes[i].slots_per_page < 2) continue;
        if (size <= tree->slot_classes[i].slot_size &&
            tree->slot_classes[i].slot_size < best_size) {
            best = i;
            best_size = tree->slot_classes[i].slot_size;
        }
    }
    return best;
}

/**
 * Read slot page header from a page in the buffer pool.
 */
static void slot_page_read_header(const page_t *page, slot_page_header_t *hdr) {
    memcpy(hdr, page->data, sizeof(slot_page_header_t));
}

/**
 * Write slot page header to a page in the buffer pool.
 */
static void slot_page_write_header(page_t *page, const slot_page_header_t *hdr) {
    memcpy(page->data, hdr, sizeof(slot_page_header_t));
}

/**
 * Allocate a slot on the given page. Returns offset within page->data, or 0 on failure.
 * The page must be pinned and will be marked dirty.
 */
static uint32_t slot_page_alloc_slot(page_t *page, const slot_class_t *cls) {
    slot_page_header_t hdr;
    slot_page_read_header(page, &hdr);

    if (hdr.used_count >= hdr.slot_count) {
        return 0;  // Page full
    }

    int idx = bitmap_find_free(hdr.bitmap, hdr.slot_count);
    if (idx < 0) return 0;

    bitmap_set(hdr.bitmap, idx);
    hdr.used_count++;
    slot_page_write_header(page, &hdr);

    return (uint32_t)(SLOT_PAGE_HEADER_SIZE + idx * cls->slot_size);
}

/**
 * Free a slot on the given page. Returns new used_count.
 * The page must be pinned and will be marked dirty.
 */
static uint16_t slot_page_free_slot(page_t *page, uint32_t offset, const slot_class_t *cls) {
    slot_page_header_t hdr;
    slot_page_read_header(page, &hdr);

    int idx = (offset - SLOT_PAGE_HEADER_SIZE) / cls->slot_size;
    if (idx < 0 || idx >= hdr.slot_count) {
        LOG_ERROR("Invalid slot index %d (offset=%u, slot_size=%u)", idx, offset, cls->slot_size);
        return hdr.used_count;
    }

    bitmap_clear(hdr.bitmap, idx);
    if (hdr.used_count > 0) hdr.used_count--;
    slot_page_write_header(page, &hdr);

    return hdr.used_count;
}

/**
 * Initialize a new page as a slot page for the given class.
 * The page must be from buffer_pool_insert_new (zero-initialized, pinned).
 */
static void slot_page_init(page_t *page, const slot_class_t *cls) {
    slot_page_header_t hdr = {
        .node_type = cls->node_type,
        .reserved = 0,
        .slot_size = cls->slot_size,
        .slot_count = cls->slots_per_page,
        .used_count = 0,
        .bitmap = {0},
    };
    slot_page_write_header(page, &hdr);
}

/**
 * Pop a page ID from the reuse pool. Returns 0 if empty.
 * Re-initializes ref count and clears stale flags on the page.
 */
static uint64_t reuse_pool_pop(data_art_tree_t *tree) {
    if (tree->reuse_pool_count == 0) return 0;
    uint64_t page_id = tree->reuse_pool[--tree->reuse_pool_count];
    // Re-init ref count and clear dead/stale flags
    page_gc_init_ref(tree->page_manager, page_id);
    tree->pages_reused++;
    return page_id;
}

/**
 * Allocate a slot from the slot allocator for the given class.
 * Returns a node_ref_t with {page_id, offset}, or NULL_NODE_REF on failure.
 */
static node_ref_t slot_alloc(data_art_tree_t *tree, int class_idx) {
    slot_class_t *cls = &tree->slot_classes[class_idx];

    // Try current page first
    if (cls->current_page_id != 0) {
        page_t *page = buffer_pool_get_pinned(tree->buffer_pool, cls->current_page_id);
        if (page) {
            uint32_t offset = slot_page_alloc_slot(page, cls);
            if (offset != 0) {
                buffer_pool_dirty_unpin(tree->buffer_pool, cls->current_page_id);
                tree->slot_allocs[class_idx]++;
                return (node_ref_t){.page_id = cls->current_page_id, .offset = offset};
            }
            // Page full — unpin and fall through to allocate new page
            buffer_pool_unpin(tree->buffer_pool, cls->current_page_id);
        }
        cls->current_page_id = 0;
    }

    // Try reuse pool first, then allocate fresh
    uint64_t page_id = reuse_pool_pop(tree);
    if (page_id == 0) {
        page_id = page_manager_alloc(tree->page_manager, PAGE_SIZE);
        if (page_id == 0) {
            LOG_ERROR("slot_alloc: page_manager_alloc failed");
            return NULL_NODE_REF;
        }
        page_gc_init_ref(tree->page_manager, page_id);
    }

    // Insert into buffer pool as a fresh page
    page_t *page = buffer_pool_insert_new(tree->buffer_pool, page_id);
    if (!page) {
        LOG_ERROR("slot_alloc: buffer_pool_insert_new failed for page %lu", page_id);
        return NULL_NODE_REF;
    }

    // Initialize slot page header
    slot_page_init(page, cls);

    // Allocate first slot
    uint32_t offset = slot_page_alloc_slot(page, cls);
    buffer_pool_dirty_unpin(tree->buffer_pool, page_id);

    cls->current_page_id = page_id;
    tree->nodes_allocated++;
    tree->slot_pages_created[class_idx]++;
    tree->slot_allocs[class_idx]++;

    return (node_ref_t){.page_id = page_id, .offset = offset};
}

/**
 * Allocate a slot with a hint page — tries to reuse a slot on the hint page first.
 * Used for same-page COW: when replacing a node, the old node's page often has a
 * free slot (from a previous COW free). Allocating there keeps related nodes on the
 * same page, improving cache locality and reducing page count.
 */
static node_ref_t slot_alloc_hint(data_art_tree_t *tree, int class_idx, uint64_t hint_page_id) {
    slot_class_t *cls = &tree->slot_classes[class_idx];

    // Try hint page first (if it's a valid slot page of the right class)
    if (hint_page_id != 0 && hint_page_id != cls->current_page_id) {
        page_t *page = buffer_pool_get_pinned(tree->buffer_pool, hint_page_id);
        if (page) {
            slot_page_header_t hdr;
            slot_page_read_header(page, &hdr);
            // Verify it's the right class and has free slots
            if (hdr.node_type == cls->node_type && hdr.used_count < hdr.slot_count) {
                uint32_t offset = slot_page_alloc_slot(page, cls);
                if (offset != 0) {
                    buffer_pool_dirty_unpin(tree->buffer_pool, hint_page_id);
                    tree->slot_hint_hits++;
                    tree->slot_allocs[class_idx]++;
                    return (node_ref_t){.page_id = hint_page_id, .offset = offset};
                }
            }
            buffer_pool_unpin(tree->buffer_pool, hint_page_id);
        }
    }

    // Hint didn't work — fall through to normal allocation
    tree->slot_hint_misses++;
    return slot_alloc(tree, class_idx);
}

/**
 * Allocate a node on a dedicated page (one node per page).
 * Used for Node256, overflow pages, and oversized leaves.
 */
static node_ref_t alloc_dedicated_page(data_art_tree_t *tree, size_t size) {
    // Try reuse pool first
    uint64_t page_id = reuse_pool_pop(tree);
    if (page_id == 0) {
        // No reusable pages — allocate fresh
        page_id = page_manager_alloc(tree->page_manager, size);
        if (page_id == 0) {
            LOG_ERROR("alloc_dedicated_page: page_manager_alloc failed");
            return NULL_NODE_REF;
        }
        if (!page_gc_init_ref(tree->page_manager, page_id)) {
            LOG_ERROR("alloc_dedicated_page: page_gc_init_ref failed for page %lu", page_id);
            return NULL_NODE_REF;
        }
    }

    tree->nodes_allocated++;
    tree->dedicated_pages_created++;
    return (node_ref_t){.page_id = page_id, .offset = 0};
}

/**
 * Free a slot and potentially the entire page if it becomes empty.
 */
static void slot_free(data_art_tree_t *tree, uint64_t page_id, uint32_t offset) {
    if (offset == 0) {
        // Legacy/dedicated page — free whole page
        data_art_release_page(tree, (node_ref_t){.page_id = page_id, .offset = 0});
        return;
    }

    page_t *page = buffer_pool_get_pinned(tree->buffer_pool, page_id);
    if (!page) {
        LOG_ERROR("slot_free: failed to get page %lu", page_id);
        return;
    }

    // Read header to determine class
    slot_page_header_t hdr;
    slot_page_read_header(page, &hdr);

    // Find the matching class
    slot_class_t *cls = NULL;
    int class_idx = -1;
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        if (tree->slot_classes[i].node_type == hdr.node_type) {
            cls = &tree->slot_classes[i];
            class_idx = i;
            break;
        }
    }

    if (!cls) {
        LOG_ERROR("slot_free: unknown node_type %u on page %lu", hdr.node_type, page_id);
        buffer_pool_unpin(tree->buffer_pool, page_id);
        return;
    }

    if (class_idx >= 0) tree->slot_frees[class_idx]++;
    uint16_t remaining = slot_page_free_slot(page, offset, cls);
    buffer_pool_dirty_unpin(tree->buffer_pool, page_id);

    if (remaining == 0) {
        // Page is empty — reset current_page_id if it matches
        if (cls->current_page_id == page_id) {
            cls->current_page_id = 0;
        }
        // Add to pending free list (whole page)
        data_art_release_page(tree, (node_ref_t){.page_id = page_id, .offset = 0});
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
    
    // Initialize committed root for lock-free readers (empty tree → page_id 0, offset 0)
    atomic_store_explicit(&tree->committed_root_page_id, 0, memory_order_release);
    atomic_store_explicit(&tree->committed_root_offset, 0, memory_order_release);

    // Initialize slot allocator for multi-node-per-page packing
    slot_allocator_init(tree);

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

    // Drain any remaining pending frees (no readers at destroy time)
    drain_pending_frees(tree);
    free(tree->pending_free_pages);

    // Drain reuse pool — mark pages as dead so compaction can reclaim them
    for (size_t i = 0; i < tree->reuse_pool_count; i++) {
        page_manager_free(tree->page_manager, tree->reuse_pool[i]);
    }
    free(tree->reuse_pool);

    LOG_INFO("Destroyed ART tree (size=%zu, nodes=%lu, pages_reused=%lu)",
             tree->size, tree->nodes_allocated, tree->pages_reused);

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

    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    if (size == 0 || size > max_size) {
        LOG_ERROR("Invalid node size: %zu (max=%zu)", size, max_size);
        return NULL_NODE_REF;
    }

    // Try slot allocation if buffer pool is available
    if (tree->buffer_pool) {
        int class_idx = size_to_slot_class(tree, size);
        if (class_idx >= 0) {
            return slot_alloc(tree, class_idx);
        }
    }

    // Fallback: dedicated page (overflow, oversized, or no buffer pool)
    return alloc_dedicated_page(tree, size);
}

/**
 * Allocate space for a new node with a hint page for same-page COW.
 *
 * When COW'ing a node, pass the old node's page_id as hint.  The allocator
 * tries the hint page first — if it's the right size class and has a free
 * slot, the new copy lands on the same page as its siblings, improving
 * cache locality and reducing total page count.
 *
 * Falls through to normal allocation if the hint page doesn't work.
 */
node_ref_t data_art_alloc_node_hint(data_art_tree_t *tree, size_t size, uint64_t hint_page_id) {
    if (!tree) {
        LOG_ERROR("Tree is NULL");
        return NULL_NODE_REF;
    }

    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    if (size == 0 || size > max_size) {
        LOG_ERROR("Invalid node size: %zu (max=%zu)", size, max_size);
        return NULL_NODE_REF;
    }

    // Try slot allocation with hint if buffer pool is available
    if (tree->buffer_pool) {
        int class_idx = size_to_slot_class(tree, size);
        if (class_idx >= 0) {
            if (hint_page_id != 0) {
                return slot_alloc_hint(tree, class_idx, hint_page_id);
            }
            return slot_alloc(tree, class_idx);
        }
    }

    // Fallback: dedicated page (overflow, oversized, or no buffer pool)
    return alloc_dedicated_page(tree, size);
}

/**
 * Drain the deferred free list — actually free pages to the allocator.
 *
 * Safe to call when no concurrent readers can reference these pages:
 *  - No MVCC manager (single-threaded / recovery)
 *  - No active snapshots
 */
static void drain_pending_frees(data_art_tree_t *tree) {
    if (tree->pending_free_count == 0) return;

    for (size_t i = 0; i < tree->pending_free_count; i++) {
        uint64_t page_id = tree->pending_free_pages[i];

        // Evict from buffer pool so reuse gets a fresh frame
        if (tree->buffer_pool) {
            buffer_pool_invalidate(tree->buffer_pool, page_id);
        }

        // Push to reuse pool instead of page_manager_free
        if (tree->reuse_pool_count >= tree->reuse_pool_capacity) {
            size_t new_cap = tree->reuse_pool_capacity == 0 ? 64 : tree->reuse_pool_capacity * 2;
            uint64_t *new_pool = realloc(tree->reuse_pool, new_cap * sizeof(uint64_t));
            if (!new_pool) {
                // Fallback: free to page manager (loses reuse opportunity)
                page_manager_free(tree->page_manager, page_id);
                continue;
            }
            tree->reuse_pool = new_pool;
            tree->reuse_pool_capacity = new_cap;
        }
        tree->reuse_pool[tree->reuse_pool_count++] = page_id;
    }

    LOG_DEBUG("Drained %zu pending frees to reuse pool (pool size=%zu)",
              tree->pending_free_count, tree->reuse_pool_count);
    tree->pending_free_count = 0;
}

/**
 * Try to drain pending frees if it's safe (no active readers).
 */
static void try_drain_pending_frees(data_art_tree_t *tree) {
    // No MVCC manager → single-threaded or recovery, always safe
    if (!tree->mvcc_manager) {
        drain_pending_frees(tree);
        return;
    }
    // MVCC present but no active snapshots → safe to drain
    if (!mvcc_has_active_snapshots(tree->mvcc_manager)) {
        drain_pending_frees(tree);
    }
}

/**
 * Release reference to an old page
 *
 * Called when a page is no longer referenced by the tree (e.g., after
 * copy-on-write creates a new version). Adds the page to a deferred free
 * list. Pages are actually returned to the allocator when it's safe
 * (no concurrent readers can reference them).
 *
 * @param tree ART tree
 * @param old_ref Reference to old page
 */
void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref) {
    if (!tree || node_ref_is_null(old_ref)) return;

    // Slot-level free: non-zero offset means node is on a multi-node page
    if (old_ref.offset != 0 && tree->buffer_pool) {
        slot_free(tree, old_ref.page_id, old_ref.offset);
        return;
    }

    // Whole-page free (dedicated pages or legacy offset=0 pages)
    if (tree->pending_free_count >= tree->pending_free_capacity) {
        size_t new_cap = tree->pending_free_capacity == 0 ? 64 : tree->pending_free_capacity * 2;
        uint64_t *new_list = realloc(tree->pending_free_pages, new_cap * sizeof(uint64_t));
        if (!new_list) {
            LOG_ERROR("Failed to grow pending free list");
            return;
        }
        tree->pending_free_pages = new_list;
        tree->pending_free_capacity = new_cap;
    }

    tree->pending_free_pages[tree->pending_free_count++] = old_ref.page_id;

    // Opportunistically drain if safe
    try_drain_pending_frees(tree);
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
    
    // Get page from buffer pool — either existing (cache hit) or new allocation
    page_t *page = buffer_pool_get_pinned(tree->buffer_pool, ref.page_id);

    if (!page) {
        // Page not in buffer pool — insert a fresh frame directly.
        // No disk I/O: the page was just allocated, WAL handles durability.
        page = buffer_pool_insert_new(tree->buffer_pool, ref.page_id);
        if (!page) {
            LOG_ERROR("Failed to insert new page %lu into buffer pool", ref.page_id);
            return false;
        }
    }

    // Copy node data to page at offset
    memcpy(page->data + ref.offset, node, size);

    // No checksum here — page_manager_write() computes it at flush time
    // (after stamping write_counter for torn-write detection)

    // Mark dirty and unpin (single hash lookup + lock)
    buffer_pool_dirty_unpin(tree->buffer_pool, ref.page_id);

    return true;
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
    size_t num_ops = buffer->num_ops;

    // === OPTIMIZED BATCH COMMIT: single lock, single root publish, single fsync ===
    pthread_mutex_lock(&tree->write_lock);

    // Save rollback point in case any operation fails
    node_ref_t saved_root = tree->root;
    size_t saved_size = tree->size;

    // Apply all buffered operations using internal functions (no per-op lock/publish)
    for (size_t i = 0; i < num_ops; i++) {
        txn_operation_t *op = &buffer->operations[i];
        bool success;

        if (op->type == TXN_OP_INSERT) {
            success = data_art_insert_internal(tree, op->key, op->key_len,
                                                op->value, op->value_len);
            if (success) {
                if (!wal_log_insert(tree->wal, txn_id, op->key, op->key_len,
                                    op->value, op->value_len, NULL)) {
                    LOG_ERROR("Failed to log insert to WAL");
                }
            }
        } else { // TXN_OP_DELETE
            success = data_art_delete_internal(tree, op->key, op->key_len);
            if (success) {
                if (!wal_log_delete(tree->wal, txn_id, op->key, op->key_len, NULL)) {
                    LOG_ERROR("Failed to log delete to WAL");
                }
            } else {
                // Delete not found — not an error in batch context
                LOG_DEBUG("Delete for key not found during commit — skipping");
                success = true;
            }
        }

        if (!success) {
            // Rollback: restore root and size
            tree->root = saved_root;
            tree->size = saved_size;
            pthread_mutex_unlock(&tree->write_lock);

            DB_ERROR(DB_ERROR_IO, "failed to apply operation %zu", i);
            txn_buffer_destroy(buffer);
            ctx->txn_buffer = NULL;
            tree->txn_buffer = NULL;
            tree->current_txn_id = 0;
            mvcc_abort_txn(tree->mvcc_manager, txn_id);
            return false;
        }
    }

    // WAL commit marker + single fsync
    if (!wal_log_commit_txn(tree->wal, txn_id, NULL)) {
        // Rollback on WAL failure
        tree->root = saved_root;
        tree->size = saved_size;
        pthread_mutex_unlock(&tree->write_lock);

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

    // Single root publication for the entire batch
    data_art_publish_root(tree);

    pthread_mutex_unlock(&tree->write_lock);

    // Clean up
    txn_buffer_destroy(buffer);
    ctx->txn_buffer = NULL;
    tree->txn_buffer = NULL;
    tree->current_txn_id = 0;

    LOG_INFO("Committed transaction %lu (%zu ops) for thread %p",
             txn_id, num_ops, (void*)pthread_self());
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
    snapshot->root_offset = atomic_load_explicit(&tree->committed_root_offset,
                                                  memory_order_relaxed);

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
    
    // Drain deferred page frees before checkpointing
    // Safe because checkpoint is called under write lock and represents a quiesce point
    drain_pending_frees(tree);

    // Flush all dirty pages to disk
    if (!data_art_flush(tree)) {
        LOG_ERROR("Failed to flush tree pages during checkpoint");
        return false;
    }

    // Log checkpoint with current tree state
    uint64_t next_pid = page_manager_get_next_page_id(tree->page_manager);
    uint64_t lsn;
    if (!wal_log_checkpoint(tree->wal,
                           tree->root.page_id,
                           tree->root.offset,
                           tree->size,
                           next_pid,
                           &lsn)) {
        LOG_ERROR("Failed to log checkpoint to WAL");
        return false;
    }

    // Persist allocator metadata and page index atomically
    page_manager_save_metadata(tree->page_manager, lsn);
    page_manager_save_index(tree->page_manager);

    if (checkpoint_lsn_out) {
        *checkpoint_lsn_out = lsn;
    }

    LOG_INFO("Created checkpoint at LSN %lu (root=%lu:%u, size=%zu, next_page_id=%lu)",
             lsn, tree->root.page_id, tree->root.offset, tree->size, next_pid);

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
    bool full_replay;  // true when start_lsn == 0 (full from-scratch rebuild)
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
            if (!ctx->full_replay) {
                // Incremental recovery: restore root/size from checkpoint
                ctx->tree->root.page_id = ckpt->root_page_id;
                ctx->tree->root.offset = ckpt->root_offset;
                ctx->tree->size = ckpt->tree_size;
            }
            // Always restore allocator state — ensures next_page_id doesn't regress
            if (ckpt->next_page_id > 0)
                page_manager_set_next_page_id(ctx->tree->page_manager, ckpt->next_page_id);
            LOG_INFO("Checkpoint entry: root=%lu:%u, size=%zu, next_page_id=%lu%s",
                     ckpt->root_page_id, ckpt->root_offset, (size_t)ckpt->tree_size,
                     ckpt->next_page_id,
                     ctx->full_replay ? " (root/size skipped — full replay)" : "");
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
        .skipped = 0,
        .full_replay = (start_lsn == 0)
    };

    // Disable WAL and MVCC to prevent re-logging and unnecessary overhead
    tree->wal = NULL;
    tree->mvcc_manager = NULL;

    // For full replay: clear page index — it will be rebuilt from WAL operations
    if (start_lsn == 0 && tree->page_manager) {
        page_index_clear(tree->page_manager);
    }

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
