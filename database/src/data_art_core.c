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
#include "db_error.h"
#include "logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>

// Forward declarations
void drain_pending_frees(data_art_tree_t *tree);
void data_art_release_page(data_art_tree_t *tree, node_ref_t old_ref);

// ============================================================================
// Stub: data_art_reset_arena (no-op — arena removed, mmap pointers are stable)
// ============================================================================
//
// With fixed VA reservation, base never moves.  data_art_load_node() returns
// direct mmap pointers — no arena copy needed, no resize_lock needed.

void data_art_reset_arena(void) {
    /* no-op: kept for API compatibility until callers are cleaned up */
}

// ============================================================================
// Lock-Free Read Helpers
// ============================================================================

// Publish the current tree->root so lock-free readers can see it.
// Called after auto-commit, commit_txn, or tree initialization.
void data_art_publish_root(data_art_tree_t *tree) {
    atomic_store_explicit(&tree->committed_root, tree->root, memory_order_release);
}

// Thread-local transaction context (struct defined in data_art.h)
static pthread_key_t txn_context_key;
static pthread_once_t txn_context_key_once = PTHREAD_ONCE_INIT;

static void make_txn_context_key(void) {
    pthread_key_create(&txn_context_key, free);
}

thread_txn_context_t *get_txn_context(void) {
    pthread_once(&txn_context_key_once, make_txn_context_key);
    return (thread_txn_context_t *)pthread_getspecific(txn_context_key);
}

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


// ============================================================================
// Slot Page Bitmap Helpers (64-bit word scan via ctzll)
// ============================================================================

/**
 * Find first free (0) bit in bitmap, searching up to 'count' bits.
 * Returns bit index, or -1 if all occupied.
 *
 * Uses 64-bit word scan with __builtin_ctzll — one or two iterations
 * for up to 128 slots (all current classes fit in 1 word: max 55 slots).
 */
static inline int bitmap_find_free(const uint8_t *bitmap, int count) {
    const uint64_t *words = (const uint64_t *)bitmap;
    int remaining = count;

    for (int w = 0; remaining > 0; w++, remaining -= 64) {
        uint64_t word = words[w];
        /* Mask off bits beyond count in the last word */
        int bits_here = remaining < 64 ? remaining : 64;
        if (bits_here < 64)
            word |= ~((1ULL << bits_here) - 1);  /* force out-of-range bits to 1 */
        uint64_t free_bits = ~word;
        if (free_bits)
            return w * 64 + __builtin_ctzll(free_bits);
    }
    return -1;
}

static inline void bitmap_set(uint8_t *bitmap, int idx) {
    ((uint64_t *)bitmap)[idx / 64] |= (1ULL << (idx % 64));
}

static inline void bitmap_clear(uint8_t *bitmap, int idx) {
    ((uint64_t *)bitmap)[idx / 64] &= ~(1ULL << (idx % 64));
}

static inline bool bitmap_test(const uint8_t *bitmap, int idx) {
    return (((const uint64_t *)bitmap)[idx / 64] >> (idx % 64)) & 1;
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
        if (slots > 128) slots = 128;  // Bitmap is 16 bytes = 128 bits
        if (slots == 0) slots = 1;

        tree->slot_classes[i].node_type = classes[i].type;
        tree->slot_classes[i].slot_size = aligned;
        tree->slot_classes[i].slots_per_page = slots;
        tree->slot_classes[i].current_page_id = 0;
        tree->slot_classes[i].partial_pages = NULL;
        tree->slot_classes[i].partial_count = 0;
        tree->slot_classes[i].partial_capacity = 0;

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
 * Read slot page header from a page (copy to stack).
 */
static void slot_page_read_header(const page_t *page, slot_page_header_t *hdr) {
    memcpy(hdr, page->data, sizeof(slot_page_header_t));
}

/**
 * Get a direct pointer to the slot page header on the mmap page.
 * Caller must hold write_lock (header is modified in-place).
 */
static inline slot_page_header_t *slot_page_header_mut(page_t *page) {
    return (slot_page_header_t *)page->data;
}

static inline const slot_page_header_t *slot_page_header_ref(const page_t *page) {
    return (const slot_page_header_t *)page->data;
}

/**
 * Allocate a slot on the given page. Returns offset within page->data, or 0 on failure.
 * Works in-place on the mmap page — no memcpy round-trip.
 */
static uint32_t slot_page_alloc_slot(page_t *page, const slot_class_t *cls) {
    slot_page_header_t *hdr = slot_page_header_mut(page);

    if (hdr->used_count >= hdr->slot_count)
        return 0;

    int idx = bitmap_find_free(hdr->bitmap, hdr->slot_count);
    if (idx < 0) return 0;

    bitmap_set(hdr->bitmap, idx);
    hdr->used_count++;

    return (uint32_t)(SLOT_PAGE_HEADER_SIZE + idx * cls->slot_size);
}

/**
 * Free a slot on the given page. Returns new used_count.
 * Works in-place on the mmap page — no memcpy round-trip.
 */
static uint16_t slot_page_free_slot(page_t *page, uint32_t offset, const slot_class_t *cls) {
    slot_page_header_t *hdr = slot_page_header_mut(page);

    int idx = (offset - SLOT_PAGE_HEADER_SIZE) / cls->slot_size;
    if (idx < 0 || idx >= hdr->slot_count) {
        LOG_ERROR("Invalid slot index %d (offset=%u, slot_size=%u)", idx, offset, cls->slot_size);
        return hdr->used_count;
    }

    bitmap_clear(hdr->bitmap, idx);
    if (hdr->used_count > 0) hdr->used_count--;

    return hdr->used_count;
}

/**
 * Initialize a new page as a slot page for the given class.
 * The page must be freshly allocated (zero-initialized by mmap).
 */
static void slot_page_init(page_t *page, const slot_class_t *cls) {
    slot_page_header_t *hdr = slot_page_header_mut(page);
    hdr->node_type = cls->node_type;
    hdr->reserved = 0;
    hdr->slot_size = cls->slot_size;
    hdr->slot_count = cls->slots_per_page;
    hdr->used_count = 0;
    memset(hdr->bitmap, 0, sizeof(hdr->bitmap));
}

/**
 * Pop a page ID from the reuse pool. Returns 0 if empty.
 * Re-initializes ref count and clears stale flags on the page.
 */
static uint64_t reuse_pool_pop(data_art_tree_t *tree) {
    if (tree->reuse_pool_count == 0) return 0;
    uint64_t page_id = tree->reuse_pool[--tree->reuse_pool_count];
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
        page_t *page = mmap_storage_get_page(tree->mmap_storage, cls->current_page_id);
        uint32_t offset = slot_page_alloc_slot(page, cls);
        if (offset != 0) {
            tree->slot_allocs[class_idx]++;
            mmap_storage_mark_dirty(tree->mmap_storage, cls->current_page_id);
            return node_ref_make(cls->current_page_id, offset);
        }
        cls->current_page_id = 0;
    }

    // Try partial-page list: reuse pages that had slots freed
    while (cls->partial_count > 0) {
        uint64_t partial_id = cls->partial_pages[--cls->partial_count];
        page_t *ppage = mmap_storage_get_page(tree->mmap_storage, partial_id);
        const slot_page_header_t *phdr = slot_page_header_ref(ppage);
        if (phdr->node_type == cls->node_type && phdr->used_count < phdr->slot_count) {
            uint32_t offset = slot_page_alloc_slot(ppage, cls);
            if (offset != 0) {
                cls->current_page_id = partial_id;
                tree->slot_allocs[class_idx]++;
                mmap_storage_mark_dirty(tree->mmap_storage, partial_id);
                return node_ref_make(partial_id, offset);
            }
        }
        // Page was full or wrong type — skip it
    }

    // Try reuse pool (whole freed pages), then allocate fresh
    uint64_t page_id = reuse_pool_pop(tree);
    if (page_id == 0) {
        page_id = mmap_storage_alloc_page(tree->mmap_storage);
        if (page_id == 0) {
            LOG_ERROR("slot_alloc: alloc_page failed");
            return NULL_NODE_REF;
        }
    } else {
        // Reused page — zero the data area for clean state
        page_t *page = mmap_storage_get_page(tree->mmap_storage, page_id);
        memset(page->data, 0, sizeof(page->data));
    }

    page_t *page = mmap_storage_get_page(tree->mmap_storage, page_id);
    slot_page_init(page, cls);

    uint32_t offset = slot_page_alloc_slot(page, cls);

    cls->current_page_id = page_id;
    tree->nodes_allocated++;
    tree->slot_pages_created[class_idx]++;
    tree->slot_allocs[class_idx]++;
    mmap_storage_mark_dirty(tree->mmap_storage, page_id);

    return node_ref_make(page_id, offset);
}

/**
 * Allocate a slot with a hint page — tries to reuse a slot on the hint page first.
 * Used for same-page COW: when replacing a node, the old node's page often has a
 * free slot (from a previous COW free). Allocating there keeps related nodes on the
 * same page, improving cache locality and reducing page count.
 */
static node_ref_t slot_alloc_hint(data_art_tree_t *tree, int class_idx, uint64_t hint_page_id) {
    slot_class_t *cls = &tree->slot_classes[class_idx];

    if (hint_page_id != 0 && hint_page_id != cls->current_page_id) {
        page_t *page = mmap_storage_get_page(tree->mmap_storage, hint_page_id);
        const slot_page_header_t *hdr = slot_page_header_ref(page);
        if (hdr->node_type == cls->node_type && hdr->used_count < hdr->slot_count) {
            uint32_t offset = slot_page_alloc_slot(page, cls);
            if (offset != 0) {
                tree->slot_hint_hits++;
                tree->slot_allocs[class_idx]++;
                mmap_storage_mark_dirty(tree->mmap_storage, hint_page_id);
                return node_ref_make(hint_page_id, offset);
            }
        }
    }

    tree->slot_hint_misses++;
    return slot_alloc(tree, class_idx);
}

/**
 * Allocate a node on a dedicated page (one node per page).
 * Used for Node256, overflow pages, and oversized leaves.
 */
static node_ref_t alloc_dedicated_page(data_art_tree_t *tree) {
    // Try reuse pool first
    uint64_t page_id = reuse_pool_pop(tree);
    if (page_id == 0) {
        page_id = mmap_storage_alloc_page(tree->mmap_storage);
        if (page_id == 0) {
            LOG_ERROR("alloc_dedicated_page: alloc_page failed");
            return NULL_NODE_REF;
        }
    }

    tree->nodes_allocated++;
    tree->dedicated_pages_created++;
    return node_ref_make(page_id, 0);
}

/**
 * Free a slot and potentially the entire page if it becomes empty.
 */
static void slot_free(data_art_tree_t *tree, uint64_t page_id, uint32_t offset) {
    if (offset == 0) {
        // Dedicated page — free whole page
        data_art_release_page(tree, node_ref_make(page_id, 0));
        return;
    }

    page_t *page = mmap_storage_get_page(tree->mmap_storage, page_id);

    // Read header to determine class (in-place)
    const slot_page_header_t *hdr = slot_page_header_ref(page);

    // Find the matching class
    slot_class_t *cls = NULL;
    int class_idx = -1;
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        if (tree->slot_classes[i].node_type == hdr->node_type) {
            cls = &tree->slot_classes[i];
            class_idx = i;
            break;
        }
    }

    if (!cls) {
        LOG_ERROR("slot_free: unknown node_type %u on page %lu", hdr->node_type, page_id);
        return;
    }

    if (class_idx >= 0) tree->slot_frees[class_idx]++;
    uint16_t remaining = slot_page_free_slot(page, offset, cls);
    mmap_storage_mark_dirty(tree->mmap_storage, page_id);

    if (remaining == 0) {
        // Page is empty — reset current_page_id if it matches
        if (cls->current_page_id == page_id) {
            cls->current_page_id = 0;
        }
        // Remove from partial list if present
        for (size_t i = 0; i < cls->partial_count; i++) {
            if (cls->partial_pages[i] == page_id) {
                cls->partial_pages[i] = cls->partial_pages[--cls->partial_count];
                break;
            }
        }
        // Add to pending free list (whole page)
        data_art_release_page(tree, node_ref_make(page_id, 0));
    } else if (page_id != cls->current_page_id) {
        // Page has free slots and isn't the current page — add to partial list
        // (only if not already present)
        bool found = false;
        for (size_t i = 0; i < cls->partial_count; i++) {
            if (cls->partial_pages[i] == page_id) { found = true; break; }
        }
        if (!found) {
            if (cls->partial_count >= cls->partial_capacity) {
                size_t new_cap = cls->partial_capacity == 0 ? 16
                               : cls->partial_capacity * 2;
                uint64_t *new_list = realloc(cls->partial_pages,
                                             new_cap * sizeof(uint64_t));
                if (new_list) {
                    cls->partial_pages = new_list;
                    cls->partial_capacity = new_cap;
                }
            }
            if (cls->partial_count < cls->partial_capacity) {
                cls->partial_pages[cls->partial_count++] = page_id;
            }
        }
    }
}

// ============================================================================
// Tree Lifecycle
// ============================================================================

/**
 * Common initialization for tree struct fields.
 */
static bool data_art_init_common(data_art_tree_t *tree, size_t key_size) {
    tree->root = NULL_NODE_REF;
    tree->version = 1;
    tree->size = 0;
    tree->key_size = key_size;
    tree->max_depth = key_size + 1;
    tree->current_txn_id = 0;

    // Writer-preferred rwlock: when a writer is waiting, new readers block
    // until the writer completes. Prevents writer starvation under heavy
    // concurrent read load (e.g., many JSON-RPC readers starving the block
    // executor). Existing readers finish normally before the writer proceeds.
    pthread_rwlockattr_t rwattr;
    pthread_rwlockattr_init(&rwattr);
    pthread_rwlockattr_setkind_np(&rwattr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    if (pthread_rwlock_init(&tree->write_lock, &rwattr) != 0) {
        LOG_ERROR("Failed to initialize write_lock");
        pthread_rwlockattr_destroy(&rwattr);
        return false;
    }
    pthread_rwlockattr_destroy(&rwattr);

    tree->mvcc_manager = mvcc_manager_create();
    if (!tree->mvcc_manager) {
        LOG_ERROR("Failed to create MVCC manager");
        pthread_rwlock_destroy(&tree->write_lock);
        return false;
    }

    tree->cow_enabled = false;
    tree->active_versions = NULL;
    tree->num_active_versions = 0;

    atomic_store_explicit(&tree->committed_root, NULL_NODE_REF, memory_order_release);

    slot_allocator_init(tree);
    return true;
}

data_art_tree_t *data_art_create(const char *path, size_t key_size) {
    if (!path) {
        LOG_ERROR("path cannot be NULL");
        return NULL;
    }

    if (key_size != 20 && key_size != 32) {
        LOG_ERROR("Invalid key_size: %zu (must be 20 or 32)", key_size);
        return NULL;
    }

    mmap_storage_t *ms = mmap_storage_create(path, MMAP_DEFAULT_INITIAL_PAGES);
    if (!ms) {
        LOG_ERROR("Failed to create mmap storage at %s", path);
        return NULL;
    }

    data_art_tree_t *tree = calloc(1, sizeof(data_art_tree_t));
    if (!tree) {
        mmap_storage_destroy(ms);
        return NULL;
    }

    tree->mmap_storage = ms;

    if (!data_art_init_common(tree, key_size)) {
        mmap_storage_destroy(ms);
        free(tree);
        return NULL;
    }

    // Persist key_size to header for reopen
    mmap_storage_checkpoint(ms, 0, 0, 0, key_size);

    LOG_INFO("Created ART tree (key_size=%zu, path=%s)", key_size, path);
    return tree;
}

data_art_tree_t *data_art_open(const char *path, size_t key_size) {
    if (!path) {
        LOG_ERROR("path cannot be NULL");
        return NULL;
    }

    mmap_storage_t *ms = mmap_storage_open(path);
    if (!ms) {
        LOG_ERROR("Failed to open mmap storage at %s", path);
        return NULL;
    }

    // Load header
    uint64_t root_page_id;
    uint32_t root_offset;
    uint64_t tree_size, stored_key_size;

    if (!mmap_storage_load_header(ms, &root_page_id, &root_offset,
                                   &tree_size, &stored_key_size)) {
        LOG_ERROR("Failed to load mmap header from %s", path);
        mmap_storage_destroy(ms);
        return NULL;
    }

    if (stored_key_size != key_size) {
        LOG_ERROR("Key size mismatch: file has %lu, requested %zu",
                  stored_key_size, key_size);
        mmap_storage_destroy(ms);
        return NULL;
    }

    data_art_tree_t *tree = calloc(1, sizeof(data_art_tree_t));
    if (!tree) {
        mmap_storage_destroy(ms);
        return NULL;
    }

    tree->mmap_storage = ms;

    if (!data_art_init_common(tree, key_size)) {
        mmap_storage_destroy(ms);
        free(tree);
        return NULL;
    }

    // Restore tree state from header
    tree->root = node_ref_make(root_page_id, root_offset);
    tree->size = tree_size;

    // Publish committed root for lock-free readers
    atomic_store_explicit(&tree->committed_root, tree->root, memory_order_release);

    LOG_INFO("Opened ART tree (key_size=%zu, size=%lu, root=%lu:%u, path=%s)",
             key_size, tree_size, node_ref_page_id(tree->root), node_ref_offset(tree->root), path);
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

    // Drain any remaining pending frees (no readers at destroy time)
    drain_pending_frees(tree);
    free(tree->pending_free_pages);
    free(tree->pending_slot_frees);

    // Free partial-page lists
    for (int i = 0; i < NUM_SLOT_CLASSES; i++) {
        free(tree->slot_classes[i].partial_pages);
    }

    // Crash-safe checkpoint, then destroy storage
    mmap_storage_checkpoint(tree->mmap_storage,
                             node_ref_page_id(tree->root), node_ref_offset(tree->root),
                             tree->size, tree->key_size);
    free(tree->reuse_pool);
    mmap_storage_destroy(tree->mmap_storage);

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

    // Try slot allocation
    int class_idx = size_to_slot_class(tree, size);
    if (class_idx >= 0) {
        return slot_alloc(tree, class_idx);
    }

    // Fallback: dedicated page (overflow, oversized)
    return alloc_dedicated_page(tree);
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

    // Try slot allocation with hint
    int class_idx = size_to_slot_class(tree, size);
    if (class_idx >= 0) {
        if (hint_page_id != 0) {
            return slot_alloc_hint(tree, class_idx, hint_page_id);
        }
        return slot_alloc(tree, class_idx);
    }

    // Fallback: dedicated page (overflow, oversized)
    return alloc_dedicated_page(tree);
}

/**
 * Drain the deferred free list — actually free pages to the allocator.
 *
 * Safe to call when no concurrent readers can reference these pages:
 *  - No MVCC manager (single-threaded / recovery)
 *  - No active snapshots
 */
void drain_pending_frees(data_art_tree_t *tree) {
    if (tree->pending_free_count == 0 && tree->pending_slot_free_count == 0) return;
    if (tree->draining_pending) return;  // Prevent recursion
    tree->draining_pending = true;

    // Phase 1: Drain deferred slot frees (may generate whole-page frees)
    for (size_t i = 0; i < tree->pending_slot_free_count; i++) {
        slot_free(tree, tree->pending_slot_frees[i].page_id,
                        tree->pending_slot_frees[i].offset);
    }
    tree->pending_slot_free_count = 0;

    // Phase 2: Drain whole-page frees (may include pages emptied in phase 1)
    for (size_t i = 0; i < tree->pending_free_count; i++) {
        uint64_t page_id = tree->pending_free_pages[i];

        // Push to reuse pool
        if (tree->reuse_pool_count >= tree->reuse_pool_capacity) {
            size_t new_cap = tree->reuse_pool_capacity == 0 ? 64 : tree->reuse_pool_capacity * 2;
            uint64_t *new_pool = realloc(tree->reuse_pool, new_cap * sizeof(uint64_t));
            if (!new_pool) {
                continue;  // Drop this page (loses reuse opportunity)
            }
            tree->reuse_pool = new_pool;
            tree->reuse_pool_capacity = new_cap;
        }
        tree->reuse_pool[tree->reuse_pool_count++] = page_id;
    }

    LOG_DEBUG("Drained %zu pending frees to reuse pool (pool size=%zu)",
              tree->pending_free_count, tree->reuse_pool_count);
    tree->pending_free_count = 0;

    tree->draining_pending = false;
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
    if (node_ref_offset(old_ref) != 0) {
        // Safe to free immediately if no MVCC or no active snapshots
        if (!tree->mvcc_manager || !mvcc_has_active_snapshots(tree->mvcc_manager)) {
            slot_free(tree, node_ref_page_id(old_ref), node_ref_offset(old_ref));
        } else {
            // Defer: snapshots may still reference this slot
            if (tree->pending_slot_free_count >= tree->pending_slot_free_capacity) {
                size_t new_cap = tree->pending_slot_free_capacity == 0 ? 64
                               : tree->pending_slot_free_capacity * 2;
                typeof(tree->pending_slot_frees) new_list =
                    realloc(tree->pending_slot_frees,
                            new_cap * sizeof(tree->pending_slot_frees[0]));
                if (!new_list) {
                    LOG_ERROR("Failed to grow pending slot free list");
                    return;
                }
                tree->pending_slot_frees = new_list;
                tree->pending_slot_free_capacity = new_cap;
            }
            tree->pending_slot_frees[tree->pending_slot_free_count].page_id =
                node_ref_page_id(old_ref);
            tree->pending_slot_frees[tree->pending_slot_free_count].offset =
                node_ref_offset(old_ref);
            tree->pending_slot_free_count++;
        }
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

    tree->pending_free_pages[tree->pending_free_count++] = node_ref_page_id(old_ref);

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

    /* Direct mmap pointer — base is stable (fixed VA reservation) */
    page_t *page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(ref));
    return page->data + node_ref_offset(ref);
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

    size_t max_size = PAGE_SIZE - PAGE_HEADER_SIZE;
    if (size > max_size) {
        LOG_ERROR("Node size %zu exceeds page capacity %zu", size, max_size);
        return false;
    }

    page_t *page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(ref));
    memcpy(page->data + node_ref_offset(ref), node, size);
    mmap_storage_mark_dirty(tree->mmap_storage, node_ref_page_id(ref));
    return true;
}

// ============================================================================
// In-Place Mutation Helpers
// ============================================================================

void *data_art_lock_node_mut(data_art_tree_t *tree, node_ref_t ref) {
    mmap_storage_mark_dirty(tree->mmap_storage, node_ref_page_id(ref));
    page_t *page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(ref));
    return page->data + node_ref_offset(ref);
}

void data_art_unlock_node_mut(data_art_tree_t *tree) {
    (void)tree;  /* no-op: no resize_lock to release */
}

bool data_art_write_partial(data_art_tree_t *tree, node_ref_t ref,
                            size_t node_offset, const void *data, size_t len) {
    if (!tree || !data || node_ref_is_null(ref)) return false;
    page_t *page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(ref));
    memcpy(page->data + node_ref_offset(ref) + node_offset, data, len);
    mmap_storage_mark_dirty(tree->mmap_storage, node_ref_page_id(ref));
    return true;
}

bool data_art_copy_node(data_art_tree_t *tree, node_ref_t dst, node_ref_t src, size_t size) {
    if (!tree || node_ref_is_null(dst) || node_ref_is_null(src)) return false;
    page_t *src_page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(src));
    page_t *dst_page = mmap_storage_get_page(tree->mmap_storage, node_ref_page_id(dst));
    memcpy(dst_page->data + node_ref_offset(dst), src_page->data + node_ref_offset(src), size);
    mmap_storage_mark_dirty(tree->mmap_storage, node_ref_page_id(dst));
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
    if (!tree) {
        LOG_ERROR("Cannot begin transaction: tree is NULL");
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

    // Create transaction buffer
    txn_buffer_t *buffer = txn_buffer_create(txn_id, 16);
    if (!buffer) {
        LOG_ERROR("Failed to create transaction buffer");
        mvcc_abort_txn(tree->mvcc_manager, txn_id);
        return false;
    }

    // Create or update thread-local context
    if (!ctx) {
        ctx = malloc(sizeof(thread_txn_context_t));
        if (!ctx) {
            LOG_ERROR("Failed to allocate transaction context");
            txn_buffer_destroy(buffer);
            mvcc_abort_txn(tree->mvcc_manager, txn_id);
            return false;
        }
        set_txn_context(ctx);
    }

    ctx->tree = tree;
    ctx->txn_id = txn_id;
    ctx->txn_buffer = buffer;

    if (txn_id_out) {
        *txn_id_out = txn_id;
    }

    LOG_INFO("Began transaction %lu for thread %p", txn_id, (void*)pthread_self());
    return true;
}

bool data_art_commit_txn(data_art_tree_t *tree) {
    if (!tree) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree is NULL");
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

    // === OPTIMIZED BATCH COMMIT: single lock, single root publish ===
    pthread_rwlock_wrlock(&tree->write_lock);

    // Set current_txn_id under write_lock so internal functions see it
    tree->current_txn_id = txn_id;

    // Use in-place mutations when no snapshots need to see old tree versions
    bool inplace = !mvcc_has_active_snapshots(tree->mvcc_manager);

    // Hint: pre-grow mmap to reduce resize frequency during commit.
    mmap_storage_ensure_capacity(tree->mmap_storage,
                                 tree->mmap_storage->next_page_id + num_ops * 4);

    // Save rollback point in case any operation fails
    node_ref_t saved_root = tree->root;
    size_t saved_size = tree->size;

    // Apply all buffered operations using internal functions (no per-op lock/publish)
    for (size_t i = 0; i < num_ops; i++) {
        txn_operation_t *op = &buffer->operations[i];
        bool success;

        uint8_t *key = buffer->arena + op->key_off;
        if (op->type == TXN_OP_INSERT) {
            void *value = buffer->arena + op->value_off;
            success = data_art_insert_internal(tree, key, op->key_len,
                                                value, op->value_len, inplace);
        } else { // TXN_OP_DELETE
            success = data_art_delete_internal(tree, key, op->key_len, inplace);
            if (!success) {
                // Delete not found — not an error in batch context
                LOG_DEBUG("Delete for key not found during commit — skipping");
                success = true;
            }
        }

        if (!success) {
            // Rollback: restore root and size
            tree->root = saved_root;
            tree->size = saved_size;
            pthread_rwlock_unlock(&tree->write_lock);

            DB_ERROR(DB_ERROR_IO, "failed to apply operation %zu", i);
            tree->current_txn_id = 0;
            txn_buffer_destroy(buffer);
            ctx->txn_buffer = NULL;
            mvcc_abort_txn(tree->mvcc_manager, txn_id);
            return false;
        }
    }

    // Commit MVCC transaction
    if (!mvcc_commit_txn(tree->mvcc_manager, txn_id)) {
        LOG_ERROR("Failed to commit MVCC transaction %lu", txn_id);
    }

    // Single root publication for the entire batch
    data_art_publish_root(tree);

    tree->current_txn_id = 0;
    pthread_rwlock_unlock(&tree->write_lock);

    // Clean up
    txn_buffer_destroy(buffer);
    ctx->txn_buffer = NULL;

    LOG_INFO("Committed transaction %lu (%zu ops) for thread %p",
             txn_id, num_ops, (void*)pthread_self());
    return true;
}

bool data_art_abort_txn(data_art_tree_t *tree) {
    if (!tree) {
        DB_ERROR(DB_ERROR_INVALID_ARG, "tree is NULL");
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

    // Abort MVCC transaction
    if (!mvcc_abort_txn(tree->mvcc_manager, txn_id)) {
        LOG_ERROR("Failed to abort MVCC transaction %lu", txn_id);
    }

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
    
    // Capture committed root AND register MVCC snapshot atomically.
    // Both must happen under rdlock to prevent the race where a writer sees
    // no active snapshots (via mvcc_has_active_snapshots) and uses in-place
    // mutations on nodes that this snapshot's root references.
    pthread_rwlock_rdlock(&tree->write_lock);
    snapshot->mvcc_snapshot = mvcc_snapshot_create(tree->mvcc_manager);
    snapshot->root = atomic_load_explicit(&tree->committed_root, memory_order_acquire);
    pthread_rwlock_unlock(&tree->write_lock);

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
    if (!tree) {
        LOG_ERROR("Cannot checkpoint: tree is NULL");
        return false;
    }

    // Only drain if no active snapshots — snapshots may still reference old nodes
    try_drain_pending_frees(tree);

    if (!mmap_storage_checkpoint(tree->mmap_storage,
                                  node_ref_page_id(tree->root),
                                  node_ref_offset(tree->root),
                                  tree->size, tree->key_size)) {
        LOG_ERROR("checkpoint: mmap_storage_checkpoint failed");
        return false;
    }

    if (checkpoint_lsn_out) *checkpoint_lsn_out = 0;
    LOG_INFO("Checkpoint (root=%lu:%u, size=%zu)",
             node_ref_page_id(tree->root), node_ref_offset(tree->root), tree->size);
    return true;
}

// ============================================================================
// Hot Page Pinning (mlock)
// ============================================================================

#include <sys/mman.h>

/**
 * Get child refs from an inner node for BFS traversal.
 * Returns number of children written to out_children[].
 */
static int mlock_get_children(const void *node, node_ref_t *out_children, int max) {
    uint8_t type = *(const uint8_t *)node;
    int count = 0;

    switch (type) {
        case DATA_NODE_4: {
            const data_art_node4_t *n = (const data_art_node4_t *)node;
            for (int i = 0; i < n->num_children && count < max; i++) {
                if (!node_ref_is_null(n->children[i]))
                    out_children[count++] = n->children[i];
            }
            break;
        }
        case DATA_NODE_16: {
            const data_art_node16_t *n = (const data_art_node16_t *)node;
            for (int i = 0; i < n->num_children && count < max; i++) {
                if (!node_ref_is_null(n->children[i]))
                    out_children[count++] = n->children[i];
            }
            break;
        }
        case DATA_NODE_48: {
            const data_art_node48_t *n = (const data_art_node48_t *)node;
            for (int i = 0; i < 48 && count < max; i++) {
                if (!node_ref_is_null(n->children[i]))
                    out_children[count++] = n->children[i];
            }
            break;
        }
        case DATA_NODE_256: {
            const data_art_node256_t *n = (const data_art_node256_t *)node;
            for (int i = 0; i < 256 && count < max; i++) {
                if (!node_ref_is_null(n->children[i]))
                    out_children[count++] = n->children[i];
            }
            break;
        }
        default:
            break;
    }
    return count;
}

int64_t data_art_mlock_hot_pages(data_art_tree_t *tree, size_t budget_bytes,
                                  data_art_mlock_result_t *result) {
    if (!tree || !tree->mmap_storage) return -1;

    node_ref_t root = atomic_load_explicit(&tree->committed_root, memory_order_acquire);
    if (node_ref_is_null(root)) return 0;

    mmap_storage_t *ms = tree->mmap_storage;

    /* Track which pages we've already locked to avoid duplicates.
     * Use a simple bitmap — 1 bit per page.  At 56 GB / 4KB = 14M pages
     * that's ~1.75 MB for the bitmap, acceptable. */
    uint64_t total_pages = ms->next_page_id;
    size_t bitmap_words = (total_pages + 63) / 64;
    uint64_t *locked_bitmap = calloc(bitmap_words, sizeof(uint64_t));
    if (!locked_bitmap) return -1;

    /* BFS queue — start with root, expand children of inner nodes.
     * Max queue size: 256 children per node × depth.  Use a dynamic array. */
    size_t queue_cap = 4096;
    size_t queue_head = 0, queue_tail = 0;
    node_ref_t *queue = malloc(queue_cap * sizeof(node_ref_t));
    if (!queue) { free(locked_bitmap); return -1; }

    queue[queue_tail++] = root;

    size_t pages_locked = 0;
    size_t bytes_locked = 0;
    size_t inner_nodes = 0;
    size_t leaves_skipped = 0;

    node_ref_t children_buf[256];

    while (queue_head < queue_tail) {
        /* Check budget */
        if (budget_bytes > 0 && bytes_locked >= budget_bytes)
            break;

        node_ref_t ref = queue[queue_head++];
        if (node_ref_is_null(ref)) continue;

        uint64_t page_id = node_ref_page_id(ref);
        if (page_id == 0 || page_id >= total_pages) continue;

        /* Skip if already locked */
        uint64_t word_idx = page_id / 64;
        uint64_t bit_mask = 1ULL << (page_id % 64);
        if (locked_bitmap[word_idx] & bit_mask) {
            /* Page already locked — still need to explore children */
            const void *node = data_art_load_node(tree, ref);
            if (!node) continue;
            uint8_t type = *(const uint8_t *)node;
            if (type >= DATA_NODE_LEAF) continue;

            int nchildren = mlock_get_children(node, children_buf, 256);
            /* Grow queue if needed */
            if (queue_tail + (size_t)nchildren > queue_cap) {
                queue_cap = queue_cap * 2 + (size_t)nchildren;
                node_ref_t *nq = realloc(queue, queue_cap * sizeof(node_ref_t));
                if (!nq) break;
                queue = nq;
            }
            for (int i = 0; i < nchildren; i++)
                queue[queue_tail++] = children_buf[i];
            continue;
        }

        /* Load the node */
        const void *node = data_art_load_node(tree, ref);
        if (!node) continue;

        uint8_t type = *(const uint8_t *)node;

        /* Skip leaves — they're cold after insertion */
        if (type >= DATA_NODE_LEAF) {
            leaves_skipped++;
            continue;
        }

        /* Lock this page */
        void *page_addr = (void *)(ms->base + page_id * PAGE_SIZE);
        if (mlock(page_addr, PAGE_SIZE) == 0) {
            locked_bitmap[word_idx] |= bit_mask;
            pages_locked++;
            bytes_locked += PAGE_SIZE;
        }

        inner_nodes++;

        /* Enqueue children */
        int nchildren = mlock_get_children(node, children_buf, 256);

        /* Grow queue if needed */
        if (queue_tail + (size_t)nchildren > queue_cap) {
            queue_cap = queue_cap * 2 + (size_t)nchildren;
            node_ref_t *nq = realloc(queue, queue_cap * sizeof(node_ref_t));
            if (!nq) break;
            queue = nq;
        }

        for (int i = 0; i < nchildren; i++)
            queue[queue_tail++] = children_buf[i];
    }

    free(queue);
    free(locked_bitmap);

    if (result) {
        result->pages_locked = pages_locked;
        result->bytes_locked = bytes_locked;
        result->inner_nodes_visited = inner_nodes;
        result->leaves_skipped = leaves_skipped;
    }

    LOG_INFO("mlock_hot_pages: locked %zu pages (%.1f MB), "
             "visited %zu inner nodes, skipped %zu leaves",
             pages_locked, bytes_locked / (1024.0 * 1024.0),
             inner_nodes, leaves_skipped);

    return (int64_t)pages_locked;
}

bool data_art_munlock_all(data_art_tree_t *tree) {
    if (!tree || !tree->mmap_storage) return false;
    mmap_storage_t *ms = tree->mmap_storage;
    if (!ms->base || ms->mapped_size == 0) return false;

    int rc = munlock(ms->base, ms->mapped_size);
    if (rc != 0) {
        LOG_ERROR("munlock_all: munlock(%p, %zu) failed: %s",
                  (void *)ms->base, ms->mapped_size, strerror(errno));
        return false;
    }
    return true;
}
