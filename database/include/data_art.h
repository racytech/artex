/*
 * Persistent Adaptive Radix Tree (data_art)
 *
 * DESIGNED FOR FIXED-SIZE KEYS (e.g., 20-byte addresses, 32-byte hashes)
 * This implementation does NOT support variable-length keys or prefix relationships.
 * All keys must have the same length (e.g., Ethereum: 20 bytes for addresses, 32 bytes for storage keys).
 *
 * Disk-backed ART implementation with:
 * - Packed page references (page_id + offset in a single uint64_t)
 * - Copy-on-Write (CoW) for MVCC
 * - mmap-backed storage for persistence
 * - Serialized node structures for persistence
 *
 * Key differences from mem_art:
 * - Uses node_ref_t (packed uint64_t) instead of pointers
 * - All nodes serialized to fixed-size structures
 * - Backed by mmap_storage (memory-mapped file)
 * - Supports versioning via MVCC snapshots
 */

#ifndef DATA_ART_H
#define DATA_ART_H

#include "mmap_storage.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

// Forward declarations
typedef struct txn_buffer txn_buffer_t;
typedef struct mvcc_manager mvcc_manager_t;
typedef struct mvcc_snapshot mvcc_snapshot_t;

// Thread-local transaction context (one per thread, allows multi-writer)
typedef struct {
    struct data_art_tree *tree;  // Tree this transaction belongs to
    uint64_t txn_id;             // Transaction ID
    txn_buffer_t *txn_buffer;    // Buffer for pending operations
} thread_txn_context_t;

// Get the calling thread's active transaction context (NULL if none)
thread_txn_context_t *get_txn_context(void);

// ============================================================================
// Node Reference (Packed Page-based addressing)
// ============================================================================

/**
 * Reference to a node stored on disk.
 *
 * Packed into a single uint64_t for compact storage in node child arrays:
 *   bits [63:OFFSET_BITS] = page_id
 *   bits [OFFSET_BITS-1:0] = offset within page
 *
 * OFFSET_BITS is derived from PAGE_SIZE so the packing adapts automatically.
 * A ref of 0 represents NULL (page_id=0 is reserved for the file header).
 */
typedef uint64_t node_ref_t;

// Number of bits needed for intra-page offset (log2(PAGE_SIZE))
// 4KB=12, 8KB=13, 16KB=14, 64KB=16
#define NODE_REF_OFFSET_BITS (__builtin_ctz(PAGE_SIZE))
#define NODE_REF_OFFSET_MASK ((1ULL << NODE_REF_OFFSET_BITS) - 1)

#define NULL_NODE_REF ((node_ref_t)0)

static inline node_ref_t node_ref_make(uint64_t page_id, uint32_t offset) {
    return (page_id << NODE_REF_OFFSET_BITS) | (offset & NODE_REF_OFFSET_MASK);
}

static inline uint64_t node_ref_page_id(node_ref_t ref) {
    return ref >> NODE_REF_OFFSET_BITS;
}

static inline uint32_t node_ref_offset(node_ref_t ref) {
    return (uint32_t)(ref & NODE_REF_OFFSET_MASK);
}

static inline bool node_ref_is_null(node_ref_t ref) {
    return ref == 0;
}

static inline bool node_ref_equals(node_ref_t a, node_ref_t b) {
    return a == b;
}

// ============================================================================
// Node Types
// ============================================================================

typedef enum {
    DATA_NODE_4   = 0,  // Up to 4 children
    DATA_NODE_16  = 1,  // Up to 16 children
    DATA_NODE_48  = 2,  // Up to 48 children (indexed)
    DATA_NODE_256 = 3,  // Up to 256 children (direct array)
    DATA_NODE_LEAF = 4,  // Leaf node containing key-value pair
    DATA_NODE_OVERFLOW = 5  // Overflow page for large values
} data_art_node_type_t;

// Path compression: max compressed prefix bytes stored in each inner node.
// Covers both 20-byte (Ethereum address) and 32-byte (hash) key sizes.
#define ART_MAX_PREFIX 32

// Generic accessor for partial_len — always at byte offset 2 in all inner node types.
static inline uint8_t node_partial_len(const void *node) {
    return ((const uint8_t *)node)[2];
}
// node_partial() defined after struct declarations (needs offsetof).

// ============================================================================
// Slot Page Infrastructure (Multi-Node-Per-Page)
// ============================================================================

/**
 * Number of node size classes for slot allocation.
 * One per node type: Node4, Node16, Node48, Node256, Leaf
 */
#define NUM_SLOT_CLASSES 5

// Size class indices (match data_art_node_type_t values)
#define SLOT_CLASS_NODE4    0
#define SLOT_CLASS_NODE16   1
#define SLOT_CLASS_NODE48   2
#define SLOT_CLASS_NODE256  3
#define SLOT_CLASS_LEAF     4

// Slot page header size (bytes, stored at start of page data area)
#define SLOT_PAGE_HEADER_SIZE 24

// Usable slot area per page (after page header + slot header + tail marker)
// = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_PAGE_HEADER_SIZE - 4 (tail marker)
#define SLOT_AREA_SIZE (PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_PAGE_HEADER_SIZE - 4)

// Dedicated page marker (node_type value for non-slotted pages)
#define SLOT_PAGE_DEDICATED 0xFF

// Align size up to 8-byte boundary
#define ALIGN8(x) (((x) + 7) & ~7)

/**
 * Slot page header — stored at page->data[0..23]
 *
 * Tracks which slots are occupied via a bitmap.
 * Max 128 slots per page (16-byte bitmap).
 */
typedef struct {
    uint8_t  node_type;       // DATA_NODE_4..DATA_NODE_LEAF, or SLOT_PAGE_DEDICATED
    uint8_t  reserved;
    uint16_t slot_size;       // Size of each slot in bytes (8-byte aligned)
    uint16_t slot_count;      // Total slots available in this page
    uint16_t used_count;      // Currently occupied slots
    uint8_t  bitmap[16];      // Occupancy bitmap (bit N = slot N occupied)
} __attribute__((packed)) slot_page_header_t;

/**
 * Per-type allocator state — tracks "current page" for each size class
 */
#define SLOT_PARTIAL_LIST_CAP 64  // Max pages in partial-free list per class
typedef struct {
    uint64_t current_page_id;   // Page currently accepting allocations (0 = none)
    uint16_t slot_size;         // Aligned slot size for this class
    uint16_t slots_per_page;    // Pre-computed max slots
    uint8_t  node_type;         // DATA_NODE_4, DATA_NODE_16, etc.

    // Partial-page free list: pages with free slots available for reuse.
    // When a slot is freed on a non-current page, that page is added here.
    // slot_alloc() checks this list before allocating a fresh page.
    uint64_t *partial_pages;    // Array of page IDs with free slots
    size_t    partial_count;    // Number of pages in partial list
    size_t    partial_capacity; // Capacity of partial_pages array
} slot_class_t;

// ============================================================================
// On-Disk Node Structures (Fixed Size, Packed)
// ============================================================================

// Leaf flags
#define LEAF_FLAG_NONE     0x00
#define LEAF_FLAG_OVERFLOW 0x01  // Has overflow pages for large value

// Leaf header size (bytes): type(1)+flags(1)+value_len(4)+xmin(8)+xmax(8)+prev_version(8) = 30
#define LEAF_HEADER_SIZE 30

// Maximum inline data size — derived from PAGE_SIZE so it scales automatically.
#define MAX_INLINE_DATA (PAGE_SIZE - PAGE_HEADER_SIZE - LEAF_HEADER_SIZE)

/**
 * NODE_4: Up to 4 children
 *
 * Size: 71 bytes (fixed)
 * Layout: type(1) + num_children(1) + partial_len(1) + keys(4) + children(32) + partial(32)
 *
 * Hot path (type + num_children + partial_len + keys + children) = 39 bytes,
 * fits in 1 cache line.  partial[] is cold, only loaded when partial_len > 0.
 */
typedef struct {
    uint8_t type;               // DATA_NODE_4
    uint8_t num_children;       // 0-4
    uint8_t partial_len;        // compressed prefix length (0 = none)
    uint8_t keys[4];            // Child keys

    // Children stored as packed page references (page_id << 12 | offset)
    uint64_t children[4];       // 32 bytes

    uint8_t partial[ART_MAX_PREFIX]; // compressed key bytes (cold)
} __attribute__((packed)) data_art_node4_t;

/**
 * NODE_16: Up to 16 children
 *
 * Size: 179 bytes (fixed)
 */
typedef struct {
    uint8_t type;               // DATA_NODE_16
    uint8_t num_children;       // 0-16
    uint8_t partial_len;        // compressed prefix length
    uint8_t keys[16];

    uint64_t children[16];      // 128 bytes

    uint8_t partial[ART_MAX_PREFIX]; // cold
} __attribute__((packed)) data_art_node16_t;

/**
 * NODE_48: Up to 48 children (indexed)
 *
 * Size: 642 bytes (fixed)
 * Uses index array: keys[byte_value] = child_slot (0-47, NODE48_EMPTY=empty)
 */
#define NODE48_EMPTY 255

typedef struct {
    uint8_t type;               // DATA_NODE_48
    uint8_t num_children;       // 0-48
    uint8_t partial_len;        // compressed prefix length
    uint8_t keys[256];          // Index: byte_value → child_slot

    uint64_t children[48];      // 384 bytes

    uint8_t partial[ART_MAX_PREFIX]; // cold
} __attribute__((packed)) data_art_node48_t;

/**
 * NODE_256: Up to 256 children (direct mapping)
 *
 * Size: 2050 bytes (~50% of 4KB page)
 * Direct array: children[byte_value] = child_ref
 */
typedef struct {
    uint8_t type;               // DATA_NODE_256
    uint8_t num_children;       // 0-256
    uint8_t partial_len;        // compressed prefix length

    uint64_t children[256];     // 2048 bytes

    uint8_t partial[ART_MAX_PREFIX]; // cold
} __attribute__((packed)) data_art_node256_t;

// Type-aware accessor for partial[] — offset varies per node type since
// partial[] is stored at the end of each struct (cold, after keys/children).
static inline const uint8_t *node_partial(const void *node) {
    uint8_t type = ((const uint8_t *)node)[0];
    switch (type) {
        case DATA_NODE_4:   return ((const data_art_node4_t *)node)->partial;
        case DATA_NODE_16:  return ((const data_art_node16_t *)node)->partial;
        case DATA_NODE_48:  return ((const data_art_node48_t *)node)->partial;
        case DATA_NODE_256: return ((const data_art_node256_t *)node)->partial;
        default: return NULL;
    }
}

// Returns byte offset of partial[] within a node of the given type.
static inline size_t node_partial_offset(uint8_t type) {
    switch (type) {
        case DATA_NODE_4:   return offsetof(data_art_node4_t, partial);
        case DATA_NODE_16:  return offsetof(data_art_node16_t, partial);
        case DATA_NODE_48:  return offsetof(data_art_node48_t, partial);
        case DATA_NODE_256: return offsetof(data_art_node256_t, partial);
        default: return 0;
    }
}

/**
 * Leaf node: Stores key-value pair with MVCC versioning
 *
 * Fixed-size keys: key_len is always tree->key_size, not stored in leaf.
 *
 * For small values (key_size + value_len <= MAX_INLINE_DATA):
 *   - Data stored inline in data[] array
 *   - overflow_page = 0
 *
 * For large values (key_size + value_len > MAX_INLINE_DATA):
 *   - Key stored inline in data[]
 *   - Value prefix stored inline (as much as fits)
 *   - Remaining value stored in overflow pages
 *   - overflow_page points to first overflow page
 *
 * MVCC fields:
 *   - xmin: Transaction ID that created this version
 *   - xmax: Transaction ID that deleted/superseded this version (0 = current)
 *   - prev_version: Link to previous version of same key (for version chains)
 *
 * Version chains:
 *   Tree always points to LATEST version. Each version links to older version.
 *   Example: Latest(xmin=3) -> Middle(xmin=2) -> Oldest(xmin=1) -> NULL
 *   Snapshots walk chain to find visible version based on xmin/xmax.
 *
 * Size: 44 + inline_data_len (variable)
 * Layout: type(1) + flags(1) + pad(2) + value_len(4) + overflow_page(8) +
 *         inline_data_len(4) + xmin(8) + xmax(8) + prev_version(8) + data[...]
 */
typedef struct {
    uint8_t type;               // DATA_NODE_LEAF
    uint8_t flags;              // LEAF_FLAG_OVERFLOW if value spans pages
    uint32_t value_len;         // Total value length (may span overflow pages)
    uint64_t xmin;              // Transaction that created this version
    uint64_t xmax;              // Transaction that deleted/superseded this version (0 = current)
    node_ref_t prev_version;    // Previous version of same key (0 = no older version)
    uint8_t data[];             // Non-overflow: [key][value]
                                // Overflow: [overflow_page_id:8B][key][value_prefix]
} __attribute__((packed)) data_art_leaf_t;

// Leaf accessor helpers — encapsulate overflow_page-in-data[] layout
static inline const uint8_t *leaf_key(const data_art_leaf_t *leaf) {
    return leaf->data + ((leaf->flags & LEAF_FLAG_OVERFLOW) ? 8 : 0);
}
static inline uint8_t *leaf_key_mut(data_art_leaf_t *leaf) {
    return leaf->data + ((leaf->flags & LEAF_FLAG_OVERFLOW) ? 8 : 0);
}
static inline uint64_t leaf_overflow_page(const data_art_leaf_t *leaf) {
    if (!(leaf->flags & LEAF_FLAG_OVERFLOW)) return 0;
    uint64_t pg;
    memcpy(&pg, leaf->data, sizeof(pg));
    return pg;
}
static inline size_t leaf_total_size(const data_art_leaf_t *leaf, size_t key_size) {
    if (leaf->flags & LEAF_FLAG_OVERFLOW)
        return PAGE_SIZE - PAGE_HEADER_SIZE;  // overflow leaf fills entire page
    return sizeof(data_art_leaf_t) + key_size + leaf->value_len;
}

/**
 * Overflow page: Continuation storage for large values
 *
 * When a value exceeds MAX_INLINE_DATA, the remainder is stored in a
 * linked list of overflow pages. Each page fills the remaining page space.
 *
 * Overflow header: type(1) + pad(3) + next_page(8) + data_len(4) = 16 bytes
 * Data area: PAGE_SIZE - PAGE_HEADER_SIZE - 16 bytes
 */
#define OVERFLOW_HEADER_SIZE 16
#define OVERFLOW_DATA_SIZE (PAGE_SIZE - PAGE_HEADER_SIZE - OVERFLOW_HEADER_SIZE)

typedef struct {
    uint8_t type;               // DATA_NODE_OVERFLOW
    uint8_t padding[3];
    uint64_t next_page;         // Next overflow page ID (0 = last page)
    uint32_t data_len;          // Valid bytes in data[]
    uint8_t data[OVERFLOW_DATA_SIZE];  // Continuation data — fills remaining page space
} __attribute__((packed)) data_art_overflow_t;

// ============================================================================
// Tree Structure
// ============================================================================

/**
 * Persistent ART tree
 *
 * Manages disk-backed adaptive radix tree with CoW and versioning.
 */
typedef struct data_art_tree {
    // Storage backend
    mmap_storage_t *mmap_storage;

    // Current tree state
    node_ref_t root;             // Root node reference
    uint64_t version;            // Current version number
    size_t size;                 // Number of key-value pairs
    size_t key_size;             // Fixed key size (20 or 32 bytes for Ethereum)
    size_t max_depth;            // Precomputed: key_size + 1 (for 0x00 terminator)

    // Transaction support (current_txn_id is only set under write_lock)
    uint64_t current_txn_id;     // Active writer's txn ID (0 = no active writer)

    // Concurrency control
    pthread_rwlock_t write_lock;  // Writers: wrlock. Readers: rdlock (coordinates with in-place mutation).

    // Lock-free read support: readers load committed root atomically, no lock needed
    _Atomic uint64_t committed_root;  // packed node_ref_t

    // MVCC support
    mvcc_manager_t *mvcc_manager; // MVCC manager for snapshot isolation

    // Versioning (CoW support)
    bool cow_enabled;            // Enable copy-on-write
    uint64_t *active_versions;   // Array of active version IDs
    size_t num_active_versions;

    // Deferred page freeing (safe page reuse under concurrent reads)
    uint64_t *pending_free_pages;     // Pages waiting to be freed
    size_t pending_free_count;        // Number of pages in pending list
    size_t pending_free_capacity;     // Capacity of pending list

    // Deferred slot freeing (safe slot reuse under concurrent reads)
    struct { uint64_t page_id; uint32_t offset; } *pending_slot_frees;
    size_t pending_slot_free_count;
    size_t pending_slot_free_capacity;
    bool draining_pending;            // Guard against recursive drain

    // Slot allocator (multi-node-per-page)
    slot_class_t slot_classes[NUM_SLOT_CLASSES];

    // Page reuse pool (recycled page IDs from drained pending frees)
    uint64_t *reuse_pool;
    size_t reuse_pool_count;
    size_t reuse_pool_capacity;

    // Statistics
    uint64_t nodes_allocated;
    uint64_t nodes_copied;       // CoW copies
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t overflow_pages_allocated;  // Number of overflow pages created
    uint64_t overflow_chain_reads;      // Number of overflow chain traversals

    // Slot allocator statistics
    uint64_t slot_pages_created[NUM_SLOT_CLASSES];  // Pages created per class
    uint64_t slot_allocs[NUM_SLOT_CLASSES];          // Slot allocations per class
    uint64_t slot_frees[NUM_SLOT_CLASSES];           // Slot frees per class
    uint64_t slot_hint_hits;                         // Hint page had a free slot
    uint64_t slot_hint_misses;                       // Hint page full or wrong class
    uint64_t dedicated_pages_created;                // Fallback dedicated page allocs
    uint64_t pages_reused;                           // Pages recycled from reuse pool
    uint64_t inplace_mutations;                      // In-place mutations (skipped CoW)
    uint64_t last_compact_page_count;                // next_page_id after last compaction
} data_art_tree_t;

// ============================================================================
// Core Operations
// ============================================================================

/**
 * Create a new persistent ART tree backed by mmap storage.
 *
 * Uses a memory-mapped file for persistence. Durability via msync.
 *
 * @param path      File path for the mmap data file
 * @param key_size  Fixed size for all keys (20 or 32)
 * @return Tree instance, or NULL on failure
 */
data_art_tree_t *data_art_create(const char *path, size_t key_size);

/**
 * Open an existing mmap-backed ART tree.
 *
 * @param path      Path to existing mmap data file
 * @param key_size  Fixed key size (must match the file)
 * @return Tree instance, or NULL on failure
 */
data_art_tree_t *data_art_open(const char *path, size_t key_size);

/**
 * Destroy tree and free resources
 *
 * Does NOT delete data from disk. Use data_art_drop() to remove all data.
 *
 * @param tree Tree to destroy
 */
void data_art_destroy(data_art_tree_t *tree);

/**
 * Insert or update a key-value pair
 *
 * @param tree Tree instance
 * @param key Byte array key
 * @param key_len Length of key in bytes
 * @param value Value data
 * @param value_len Length of value in bytes
 * @return true on success, false on failure
 */
bool data_art_insert(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                      const void *value, size_t value_len);

/**
 * Retrieve value by key
 *
 * @param tree Tree instance
 * @param key Byte array key
 * @param key_len Length of key
 * @param value_len Output parameter for value length (can be NULL)
 * @return Pointer to value if found, NULL otherwise
 *         Note: Returned pointer may be invalidated by subsequent operations
 */
const void *data_art_get(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                         size_t *value_len);

/**
 * Get value into caller-supplied buffer (zero allocation).
 *
 * @param tree Tree instance
 * @param key Byte array key
 * @param key_len Length of key
 * @param value_buf Buffer to copy value into
 * @param buf_size Size of value_buf
 * @param value_len Output: actual value length (can be NULL)
 * @return true if found, false otherwise
 */
bool data_art_get_into(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                       void *value_buf, size_t buf_size, size_t *value_len);

/**
 * Delete a key-value pair
 *
 * @param tree Tree instance
 * @param key Byte array key
 * @param key_len Length of key
 * @return true if key was deleted, false if not found
 */
bool data_art_delete(data_art_tree_t *tree, const uint8_t *key, size_t key_len);

/**
 * Check if key exists
 *
 * @param tree Tree instance
 * @param key Byte array key
 * @param key_len Length of key
 * @return true if key exists, false otherwise
 */
bool data_art_contains(data_art_tree_t *tree, const uint8_t *key, size_t key_len);

/**
 * Get number of key-value pairs
 *
 * @param tree Tree instance
 * @return Number of entries
 */
size_t data_art_size(const data_art_tree_t *tree);

/**
 * Check if tree is empty
 *
 * @param tree Tree instance
 * @return true if empty
 */
bool data_art_is_empty(const data_art_tree_t *tree);

// ============================================================================
// Node Operations (Internal)
// ============================================================================

/**
 * Load a node from disk into memory
 *
 * Reads from mmap and copies to thread-local arena for safety.
 *
 * @param tree Tree instance
 * @param ref Node reference
 * @return Pointer to node in memory (TLS arena copy), NULL on error
 */
const void *data_art_load_node(data_art_tree_t *tree, node_ref_t ref);

/**
 * Allocate space for a new node
 *
 * Finds a page with sufficient free space and returns a node reference.
 *
 * @param tree Tree instance
 * @param size Size of node in bytes
 * @return Node reference, or NULL_NODE_REF on failure
 */
node_ref_t data_art_alloc_node(data_art_tree_t *tree, size_t size);

/**
 * Allocate space for a new node with a hint page for same-page COW.
 *
 * Tries to allocate on hint_page_id first (if it's the right class with
 * free slots), falling back to normal allocation otherwise.
 *
 * @param tree Tree instance
 * @param size Size of node in bytes
 * @param hint_page_id Page to try first (0 = no hint, same as alloc_node)
 * @return Node reference, or NULL_NODE_REF on failure
 */
node_ref_t data_art_alloc_node_hint(data_art_tree_t *tree, size_t size, uint64_t hint_page_id);

/**
 * Write a node to disk
 *
 * Serializes node and writes to the page referenced by ref.
 *
 * @param tree Tree instance
 * @param ref Node reference
 * @param node Node data to write
 * @param size Size of node in bytes
 * @return true on success, false on failure
 */
bool data_art_write_node(data_art_tree_t *tree, node_ref_t ref,
                         const void *node, size_t size);

/**
 * Internal insert — for optimized commit path (lock held, no auto-commit/publish)
 * Caller must hold write_lock, manage MVCC txn, and root publication.
 */
bool data_art_insert_internal(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                               const void *value, size_t value_len, bool inplace);

/**
 * Internal delete — for optimized commit path (lock held, no auto-commit/publish)
 * Returns true if key was deleted, false if not found.
 */
bool data_art_delete_internal(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                               bool inplace);

// ============================================================================
// Overflow Page Operations
// ============================================================================

/**
 * Read a large value from overflow pages
 *
 * Traverses the overflow page chain and reconstructs the full value.
 * Allocates memory for the complete value.
 *
 * @param tree Tree instance
 * @param leaf Leaf node with overflow flag set
 * @param value_out Output buffer (allocated by caller, size >= leaf->value_len)
 * @return true on success, false on error
 */
bool data_art_read_overflow_value(data_art_tree_t *tree,
                                   const data_art_leaf_t *leaf,
                                   void *value_out);

/**
 * Write a large value to overflow pages
 *
 * Creates a chain of overflow pages to store value data that doesn't
 * fit inline in the leaf node.
 *
 * @param tree Tree instance
 * @param value Value data to write
 * @param value_len Total value length
 * @param inline_size How much was stored inline in leaf
 * @return First overflow page ID, or 0 on failure
 */
uint64_t data_art_write_overflow_value(data_art_tree_t *tree,
                                        const void *value,
                                        size_t value_len,
                                        size_t inline_size);

/**
 * Free overflow page chain
 *
 * Releases all overflow pages in the chain starting from first_page.
 * Called when deleting a leaf or updating a value.
 *
 * @param tree Tree instance
 * @param first_page First overflow page ID
 * @return Number of pages freed
 */
size_t data_art_free_overflow_chain(data_art_tree_t *tree, uint64_t first_page);

// ============================================================================
// Persistence & Recovery
// ============================================================================

/**
 * Flush all dirty pages to disk
 *
 * Forces all modified pages to be written. Called during checkpoint.
 *
 * @param tree Tree instance
 * @return true on success, false on failure
 */
bool data_art_flush(data_art_tree_t *tree);

/**
 * Start non-blocking writeback of dirty pages.
 *
 * Tells the kernel to start flushing dirty mmap pages to disk without
 * waiting for completion.  Call this after committing a transaction when
 * the writer will be idle (e.g., between Ethereum blocks).  By the time
 * the next checkpoint runs, most pages will already be on disk, reducing
 * checkpoint blocking time from hundreds of milliseconds to ~10-20ms.
 *
 * This is a hint — the kernel may ignore it.  No-op on non-Linux platforms.
 *
 * @param tree Tree instance
 */
void data_art_start_writeback(data_art_tree_t *tree);

/**
 * Incrementally flush dirty pages to disk (blocking).
 *
 * Syncs up to max_pages dirty pages via msync, clearing their dirty bits.
 * Call after each transaction commit to spread I/O between checkpoints.
 *
 * @param tree      Tree instance
 * @param max_pages Maximum number of pages to sync
 * @return Number of pages actually synced
 */
size_t data_art_flush_dirty(data_art_tree_t *tree, size_t max_pages);

/**
 * Get current root reference (for persistence)
 *
 * @param tree Tree instance
 * @return Root node reference
 */
node_ref_t data_art_get_root(const data_art_tree_t *tree);

// ============================================================================
// Transaction Support
// ============================================================================

/**
 * Begin a new transaction
 *
 * @param tree Tree instance
 * @param txn_id_out Output parameter for transaction ID
 * @return true on success
 */
bool data_art_begin_txn(data_art_tree_t *tree, uint64_t *txn_id_out);

/**
 * Commit the current transaction
 *
 * @param tree Tree instance
 * @return true on success
 */
bool data_art_commit_txn(data_art_tree_t *tree);

/**
 * Abort the current transaction
 *
 * @param tree Tree instance
 * @return true on success
 */
bool data_art_abort_txn(data_art_tree_t *tree);

// ============================================================================
// Batch Operations
// ============================================================================

/**
 * Batch operation type for mixed insert/delete batches
 */
typedef struct {
    enum { BATCH_OP_INSERT, BATCH_OP_DELETE } type;
    const uint8_t *key;
    size_t key_len;
    const void *value;      // NULL for delete
    size_t value_len;       // 0 for delete
} data_art_batch_op_t;

/**
 * Atomic batch insert — inserts multiple key-value pairs in a single transaction.
 *
 * Wraps all inserts in BEGIN -> N inserts -> COMMIT atomically.
 * If any insert fails, the entire batch is aborted (no partial application).
 *
 * @param tree Tree instance
 * @param keys Array of key pointers
 * @param key_lens Array of key lengths (must all equal tree->key_size)
 * @param values Array of value pointers
 * @param value_lens Array of value lengths
 * @param count Number of entries to insert
 * @return true if all inserts committed, false on failure (batch aborted)
 */
bool data_art_insert_batch(data_art_tree_t *tree,
                           const uint8_t **keys, const size_t *key_lens,
                           const void **values, const size_t *value_lens,
                           size_t count);

/**
 * Atomic batch of mixed insert/delete operations.
 *
 * Same transactional semantics as insert_batch but supports both
 * inserts and deletes in a single atomic batch.
 *
 * @param tree Tree instance
 * @param ops Array of batch operations
 * @param count Number of operations
 * @return true if all operations committed, false on failure (batch aborted)
 */
bool data_art_batch(data_art_tree_t *tree,
                    const data_art_batch_op_t *ops, size_t count);

// ============================================================================
// MVCC Snapshot Support
// ============================================================================

// Snapshot handle - each thread owns one
typedef struct data_art_snapshot {
    mvcc_snapshot_t *mvcc_snapshot;  // Underlying MVCC snapshot
    uint64_t txn_id;                 // Transaction ID for this snapshot
    node_ref_t root;                 // Committed root at snapshot creation time
} data_art_snapshot_t;

/**
 * Create a snapshot for consistent read operations
 *
 * Returns a snapshot handle that captures the current transaction state.
 * Each thread should have its own snapshot for concurrent reads.
 * The handle must be freed with data_art_end_snapshot().
 *
 * @param tree Tree instance
 * @return Snapshot handle, or NULL on failure
 */
data_art_snapshot_t *data_art_begin_snapshot(data_art_tree_t *tree);

/**
 * Release a snapshot
 *
 * Releases the snapshot and frees its resources.
 *
 * @param tree Tree instance
 * @param snapshot Snapshot handle to release
 */
void data_art_end_snapshot(data_art_tree_t *tree, data_art_snapshot_t *snapshot);

/**
 * Get value with snapshot isolation
 *
 * @param tree Tree instance
 * @param key Key to search for
 * @param key_len Length of key
 * @param value_len Output: length of value (if found)
 * @param snapshot Snapshot to use (NULL = read latest committed)
 * @return Pointer to value, or NULL if not found
 */
const void *data_art_get_snapshot(data_art_tree_t *tree, const uint8_t *key, size_t key_len,
                                   size_t *value_len, data_art_snapshot_t *snapshot);

// ============================================================================
// Checkpoint and Recovery
// ============================================================================

/**
 * Create a checkpoint of the current tree state
 *
 * Saves tree header and syncs mmap to disk.
 *
 * @param tree Tree instance
 * @param checkpoint_lsn_out Output parameter (unused, set to 0)
 * @return true on success
 */
bool data_art_checkpoint(data_art_tree_t *tree, uint64_t *checkpoint_lsn_out);

// ============================================================================
// Iterator Support
// ============================================================================

/**
 * Persistent ART iterator — enumerates all keys in lexicographic order.
 *
 * Captures a snapshot of the committed root at creation time, providing
 * a consistent view even under concurrent writes. Uses stack-based DFS.
 *
 * Usage:
 *   data_art_iterator_t *it = data_art_iterator_create(tree);
 *   while (data_art_iterator_next(it)) {
 *       size_t klen, vlen;
 *       const uint8_t *key = data_art_iterator_key(it, &klen);
 *       const void *val = data_art_iterator_value(it, &vlen);
 *       // use key, val ...
 *   }
 *   data_art_iterator_destroy(it);
 */
typedef struct data_art_iterator data_art_iterator_t;

data_art_iterator_t *data_art_iterator_create(data_art_tree_t *tree);
bool data_art_iterator_next(data_art_iterator_t *iter);
const uint8_t *data_art_iterator_key(const data_art_iterator_t *iter, size_t *key_len);
const void *data_art_iterator_value(const data_art_iterator_t *iter, size_t *value_len);
bool data_art_iterator_done(const data_art_iterator_t *iter);
void data_art_iterator_destroy(data_art_iterator_t *iter);

/**
 * Seek to the first key >= target key.
 *
 * Repositions the iterator so the next key/value accessors return the first
 * entry whose key is >= the given key. Enables efficient range scans:
 *
 *   data_art_iterator_seek(it, start_key, key_len);
 *   while (!data_art_iterator_done(it)) {
 *       // process key/value ...
 *       data_art_iterator_next(it);
 *   }
 *
 * @param iter Iterator instance
 * @param key Target key to seek to
 * @param key_len Length of the target key
 * @return true if positioned at a valid entry, false if no key >= target
 */
bool data_art_iterator_seek(data_art_iterator_t *iter,
                            const uint8_t *key, size_t key_len);

/**
 * Create a prefix iterator — iterates only keys starting with the given prefix.
 *
 * Uses seek to jump to the first matching key, then stops when prefix diverges.
 * If prefix is NULL or prefix_len is 0, behaves like data_art_iterator_create().
 *
 * @param tree Tree instance
 * @param prefix Prefix bytes to match
 * @param prefix_len Length of prefix
 * @return Iterator, or NULL on failure
 */
data_art_iterator_t *data_art_iterator_create_prefix(
    data_art_tree_t *tree, const uint8_t *prefix, size_t prefix_len);

// ============================================================================
// Online File Compaction
// ============================================================================

/**
 * Compaction result statistics
 */
typedef struct {
    uint64_t pages_before;      // Total pages before compaction
    uint64_t pages_after;       // Total pages after compaction
    uint64_t pages_freed;       // pages_before - pages_after
    uint64_t nodes_relocated;   // Number of nodes moved to front pages
    uint64_t live_pages;        // Live pages (reachable from root)
} data_art_compact_result_t;

/**
 * Compact the data file by relocating live nodes to the front and truncating.
 *
 * Requires exclusive write access and no active MVCC snapshots.
 * After compaction, the file shrinks to fit only the live data.
 *
 * @param tree   Tree instance
 * @param result Output stats (can be NULL)
 * @return true on success
 */
bool data_art_compact(data_art_tree_t *tree, data_art_compact_result_t *result);

/**
 * Compact the data file if the dead-page ratio exceeds a threshold.
 *
 * Estimates dead pages from the reuse pool + pending free list relative to
 * total allocated pages.  If the ratio exceeds `threshold` (0.0-1.0),
 * runs full compaction.  Safe to call frequently — no-op when below threshold.
 *
 * @param tree      Tree instance
 * @param threshold Dead-page ratio threshold (e.g., 0.3 = compact when >30% dead)
 * @param result    Output stats (can be NULL)
 * @return true if compaction ran and succeeded (or was skipped), false on error
 */
bool data_art_compact_if_needed(data_art_tree_t *tree, double threshold,
                                 data_art_compact_result_t *result);

// ============================================================================
// Statistics & Debugging
// ============================================================================

/**
 * Tree statistics
 */
typedef struct {
    size_t num_entries;
    uint64_t version;
    uint64_t nodes_allocated;
    uint64_t nodes_copied;
    uint64_t cache_hits;
    uint64_t cache_misses;
    double cache_hit_rate;

    // Node type distribution
    size_t num_node4;
    size_t num_node16;
    size_t num_node48;
    size_t num_node256;
    size_t num_leaves;

    // Overflow statistics
    size_t num_leaves_with_overflow;  // Leaves using overflow pages
    uint64_t overflow_pages_allocated;  // Total overflow pages created
    uint64_t overflow_chain_reads;      // Overflow chain traversals
    size_t total_overflow_bytes;        // Total bytes in overflow storage

    // Slot allocator statistics
    uint64_t slot_pages_created[NUM_SLOT_CLASSES];
    uint64_t slot_allocs[NUM_SLOT_CLASSES];
    uint64_t slot_frees[NUM_SLOT_CLASSES];
    uint64_t slot_hint_hits;
    uint64_t slot_hint_misses;
    uint64_t dedicated_pages_created;
    uint64_t pages_reused;
} data_art_stats_t;

/**
 * Get tree statistics
 *
 * @param tree Tree instance
 * @param stats Output statistics structure
 */
void data_art_get_stats(const data_art_tree_t *tree, data_art_stats_t *stats);

/**
 * Print tree statistics (for debugging)
 *
 * @param tree Tree instance
 */
void data_art_print_stats(const data_art_tree_t *tree);

/**
 * Verify tree integrity
 *
 * Checks for corruption and invariant violations.
 *
 * @param tree Tree instance
 * @return true if tree is valid, false if corrupted
 */
bool data_art_verify(data_art_tree_t *tree);

#endif // DATA_ART_H
