/*
 * Persistent Adaptive Radix Tree (data_art)
 * 
 * DESIGNED FOR FIXED-SIZE KEYS (e.g., 20-byte addresses, 32-byte hashes)
 * This implementation does NOT support variable-length keys or prefix relationships.
 * All keys must have the same length (e.g., Ethereum: 20 bytes for addresses, 32 bytes for storage keys).
 * 
 * Disk-backed ART implementation with:
 * - Page references instead of pointers
 * - Copy-on-Write (CoW) for MVCC
 * - Buffer pool integration for caching
 * - Serialized node structures for persistence
 * 
 * Key differences from mem_art:
 * - Uses node_ref_t (page_id, offset) instead of pointers
 * - All nodes serialized to fixed-size structures
 * - Integrated with page_manager and buffer_pool
 * - Supports versioning and crash recovery
 */

#ifndef DATA_ART_H
#define DATA_ART_H

#include "page_manager.h"
#include "buffer_pool.h"
#include "wal.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

// Forward declarations
typedef struct txn_buffer txn_buffer_t;
typedef struct mvcc_manager mvcc_manager_t;
typedef struct mvcc_snapshot mvcc_snapshot_t;

// ============================================================================
// Node Reference (Page-based addressing)
// ============================================================================

/**
 * Reference to a node stored on disk
 * 
 * Instead of memory pointers, we use (page_id, offset) pairs to reference
 * nodes. This allows persistence and enables buffer pool caching.
 */
typedef struct {
    uint64_t page_id;    // Which page contains this node (0 = NULL)
    uint32_t offset;     // Offset within page (0-4095)
} node_ref_t;

#define NULL_NODE_REF ((node_ref_t){.page_id = 0, .offset = 0})

static inline bool node_ref_is_null(node_ref_t ref) {
    return ref.page_id == 0;
}

static inline bool node_ref_equals(node_ref_t a, node_ref_t b) {
    return a.page_id == b.page_id && a.offset == b.offset;
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

// ============================================================================
// On-Disk Node Structures (Fixed Size, Packed)
// ============================================================================

// Leaf flags
#define LEAF_FLAG_NONE     0x00
#define LEAF_FLAG_OVERFLOW 0x01  // Has overflow pages for large value

// Maximum inline data size — derived from PAGE_SIZE so it scales automatically.
// Available = page data area - leaf header (56 bytes including struct padding)
#define LEAF_HEADER_SIZE 56
#define MAX_INLINE_DATA (PAGE_SIZE - PAGE_HEADER_SIZE - LEAF_HEADER_SIZE)

/**
 * NODE_4: Up to 4 children
 * 
 * Size: 70 bytes (fixed)
 * Layout: type(1) + num_children(1) + partial_len(1) + pad(1) + partial(10) +
 *         keys(4) + child_page_ids(32) + child_offsets(16)
 */
typedef struct {
    uint8_t type;               // DATA_NODE_4
    uint8_t num_children;       // 0-4
    uint8_t partial_len;        // Length of compressed path
    uint8_t padding1;           // Alignment
    uint8_t partial[10];        // Compressed path (max 10 bytes)
    uint8_t keys[4];            // Child keys
    
    // Children stored as page references
    uint64_t child_page_ids[4];   // 32 bytes
    uint32_t child_offsets[4];    // 16 bytes
} __attribute__((packed)) data_art_node4_t;

/**
 * NODE_16: Up to 16 children
 * 
 * Size: 240 bytes (fixed)
 */
typedef struct {
    uint8_t type;               // DATA_NODE_16
    uint8_t num_children;       // 0-16
    uint8_t partial_len;
    uint8_t padding1;
    uint8_t partial[10];
    uint8_t keys[16];
    uint8_t padding2[2];        // Alignment
    
    uint64_t child_page_ids[16];  // 128 bytes
    uint32_t child_offsets[16];   // 64 bytes
} __attribute__((packed)) data_art_node16_t;

/**
 * NODE_48: Up to 48 children (indexed)
 * 
 * Size: 658 bytes (fixed)
 * Uses index array: keys[byte_value] = child_slot (0-47, NODE48_EMPTY=empty)
 */
#define NODE48_EMPTY 255

typedef struct {
    uint8_t type;               // DATA_NODE_48
    uint8_t num_children;       // 0-48
    uint8_t partial_len;
    uint8_t padding1;
    uint8_t partial[10];
    uint8_t keys[256];          // Index: byte_value → child_slot
    uint8_t padding2[2];
    
    uint64_t child_page_ids[48];  // 384 bytes
    uint32_t child_offsets[48];   // 192 bytes
} __attribute__((packed)) data_art_node48_t;

/**
 * NODE_256: Up to 256 children (direct mapping)
 * 
 * Size: 3088 bytes (~75% of 4KB page)
 * Direct array: children[byte_value] = child_ref
 */
typedef struct {
    uint8_t type;               // DATA_NODE_256
    uint8_t num_children;       // 0-256
    uint8_t partial_len;
    uint8_t padding1;
    uint8_t partial[10];
    uint8_t padding2[2];
    
    uint64_t child_page_ids[256];   // 2048 bytes
    uint32_t child_offsets[256];    // 1024 bytes
} __attribute__((packed)) data_art_node256_t;

/**
 * Leaf node: Stores key-value pair with MVCC versioning
 * 
 * For small values (key_len + value_len <= MAX_INLINE_DATA):
 *   - Data stored inline in data[] array
 *   - overflow_page = 0
 * 
 * For large values (key_len + value_len > MAX_INLINE_DATA):
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
 * Size: 52 + inline_data_len (variable) [+12 bytes for prev_version]
 * Layout: type(1) + flags(1) + pad(2) + key_len(4) + value_len(4) +
 *         overflow_page(8) + inline_data_len(4) + xmin(8) + xmax(8) +
 *         prev_version(12) + data[...]
 */
typedef struct {
    uint8_t type;               // DATA_NODE_LEAF
    uint8_t flags;              // LEAF_FLAG_OVERFLOW if value spans pages
    uint8_t padding[2];
    uint32_t key_len;           // Length of key
    uint32_t value_len;         // Total value length (may span overflow pages)
    uint64_t overflow_page;     // First overflow page ID (0 if none)
    uint32_t inline_data_len;   // Bytes stored inline in data[]
    uint64_t xmin;              // Transaction that created this version
    uint64_t xmax;              // Transaction that deleted/superseded this version (0 = current)
    node_ref_t prev_version;    // Previous version of same key (NULL_NODE_REF = no older version)
    uint8_t data[];             // key + value (or value prefix if overflow)
} __attribute__((packed)) data_art_leaf_t;

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
    page_manager_t *page_manager;
    buffer_pool_t *buffer_pool;
    wal_t *wal;                  // Write-ahead log for durability
    
    // Current tree state
    node_ref_t root;             // Root node reference
    uint64_t version;            // Current version number
    size_t size;                 // Number of key-value pairs
    size_t key_size;             // Fixed key size (20 or 32 bytes for Ethereum)
    size_t max_depth;            // Precomputed: key_size + 1 (for 0x00 terminator)
    
    // Transaction support
    uint64_t current_txn_id;     // Active transaction ID (0 = no transaction)
    struct txn_buffer *txn_buffer; // Buffer for pending operations (NULL if not in transaction)
    
    // Concurrency control
    pthread_mutex_t write_lock;  // Serializes all write operations (one writer at a time)

    // Lock-free read support: readers load committed root atomically, no lock needed
    _Atomic uint64_t committed_root_page_id;

    // MVCC support
    mvcc_manager_t *mvcc_manager; // MVCC manager for snapshot isolation
    
    // Versioning (CoW support)
    bool cow_enabled;            // Enable copy-on-write
    uint64_t *active_versions;   // Array of active version IDs
    size_t num_active_versions;
    
    // Statistics
    uint64_t nodes_allocated;
    uint64_t nodes_copied;       // CoW copies
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t overflow_pages_allocated;  // Number of overflow pages created
    uint64_t overflow_chain_reads;      // Number of overflow chain traversals
} data_art_tree_t;

// ============================================================================
// Core Operations
// ============================================================================

/**
 * Create a new persistent ART tree with fixed-size keys
 * 
 * @param page_manager Page manager for disk I/O
 * @param buffer_pool Buffer pool for caching (optional, can be NULL)
 * @param wal Write-ahead log for durability (optional, can be NULL)
 * @param key_size Fixed size for all keys (must be 20 or 32 for Ethereum)
 * @return Tree instance, or NULL on failure
 */
data_art_tree_t *data_art_create(page_manager_t *page_manager,
                                   buffer_pool_t *buffer_pool,
                                   wal_t *wal,
                                   size_t key_size);

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
 * Uses buffer pool if available, otherwise direct page manager access.
 * 
 * @param tree Tree instance
 * @param ref Node reference
 * @return Pointer to node in memory (cached), NULL on error
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
 * Write a node to disk
 * 
 * Serializes node and writes to the page referenced by ref.
 * Marks page as dirty in buffer pool.
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
 * Copy a node for CoW (Copy-on-Write)
 * 
 * Creates a new copy of the node at a different location.
 * Used for MVCC versioning.
 * 
 * @param tree Tree instance
 * @param ref Original node reference
 * @return New node reference, or NULL_NODE_REF on failure
 */
node_ref_t data_art_cow_node(data_art_tree_t *tree, node_ref_t ref);

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
// Versioning & Snapshots
// ============================================================================

/**
 * Create a snapshot of the current tree state
 * 
 * Returns the current version ID. This version will remain accessible
 * until explicitly released.
 * 
 * @param tree Tree instance
 * @return Version ID (snapshot handle)
 */
uint64_t data_art_snapshot(data_art_tree_t *tree);

/**
 * Release a snapshot version
 * 
 * Allows garbage collection of old versions.
 * 
 * @param tree Tree instance
 * @param version Version ID to release
 */
void data_art_release_version(data_art_tree_t *tree, uint64_t version);

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
 * Load tree from disk
 * 
 * Recovers tree state from persisted root reference.
 * 
 * @param page_manager Page manager
 * @param buffer_pool Buffer pool (optional)
 * @param key_size Fixed key size for the tree (20 or 32)
 * @param root_ref Root node reference from metadata
 * @return Tree instance, or NULL on failure
 */
data_art_tree_t *data_art_load(page_manager_t *page_manager,
                                 buffer_pool_t *buffer_pool,
                                 size_t key_size,
                                 node_ref_t root_ref);

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
// MVCC Snapshot Support
// ============================================================================

// Snapshot handle - each thread owns one
typedef struct data_art_snapshot {
    mvcc_snapshot_t *mvcc_snapshot;  // Underlying MVCC snapshot
    uint64_t txn_id;                 // Transaction ID for this snapshot
    uint64_t root_page_id;           // Committed root at snapshot creation time
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
 * Flushes all dirty pages and logs checkpoint to WAL, enabling
 * truncation of old WAL segments.
 * 
 * @param tree Tree instance
 * @param checkpoint_lsn_out Output parameter for checkpoint LSN (optional)
 * @return true on success
 */
bool data_art_checkpoint(data_art_tree_t *tree, uint64_t *checkpoint_lsn_out);

/**
 * Recover tree state from WAL after crash
 * 
 * Replays WAL entries from last checkpoint to rebuild tree state.
 * Should be called after data_art_create() on startup.
 * 
 * @param tree Tree instance
 * @param start_lsn LSN to start recovery from (0 = from beginning)
 * @return Number of entries recovered, or -1 on error
 */
int64_t data_art_recover(data_art_tree_t *tree, uint64_t start_lsn);

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
