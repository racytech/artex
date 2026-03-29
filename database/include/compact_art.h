#ifndef COMPACT_ART_H
#define COMPACT_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Compact Adaptive Radix Tree (compact_art)
 *
 * Space-efficient in-memory ART for fixed-size keys and values.
 * Designed as a pure index: 32-byte keccak256 keys -> 4-byte opaque values.
 *
 * Optimizations:
 *   1. Two-pool allocator — contiguous mmap for leaves + nodes (no malloc overhead)
 *   2. 4-byte child refs — uint32_t offset/index instead of 8-byte void* pointers
 *   3. Fixed-size fields — no per-leaf key_len/value_len
 *   4. Compact inner node headers — uint8_t type/num_children/partial_len
 *   5. Node32 — fills gap between Node16 and Node48 for ~30-child nodes
 *
 * FIXED-SIZE KEYS ONLY. All keys must be the same length.
 * MEMORY-ONLY — no persistence. Data lost on process exit.
 */

// ============================================================================
// Pool Allocator (contiguous mmap, bump-allocated)
// ============================================================================

typedef struct {
    uint8_t *base;
    size_t reserved;    // virtual bytes (MAP_NORESERVE)
    size_t used;        // committed bytes
} compact_pool_t;

// ============================================================================
// Compact Ref — 4-byte encoded pointer (node offset or leaf index)
// ============================================================================

typedef uint32_t compact_ref_t;

#define COMPACT_REF_NULL      ((compact_ref_t)0)
#define COMPACT_REF_LEAF_BIT  ((compact_ref_t)0x80000000u)

#define COMPACT_IS_LEAF_REF(r)  ((r) & COMPACT_REF_LEAF_BIT)
#define COMPACT_LEAF_INDEX(r)   ((r) & 0x7FFFFFFFu)
#define COMPACT_MAKE_LEAF_REF(idx) ((compact_ref_t)(idx) | COMPACT_REF_LEAF_BIT)

// ============================================================================
// Node Types
// ============================================================================

typedef enum {
    COMPACT_NODE_4   = 0,
    COMPACT_NODE_16  = 1,
    COMPACT_NODE_32  = 2,
    COMPACT_NODE_48  = 3,
    COMPACT_NODE_256 = 4,
} compact_node_type_t;

#define COMPACT_NODE4_MAX   4
#define COMPACT_NODE16_MAX  16
#define COMPACT_NODE32_MAX  32
#define COMPACT_NODE48_MAX  48
#define COMPACT_NODE256_MAX 256

// Maximum prefix bytes stored inline
#define COMPACT_MAX_PREFIX 8

// Node48 empty index marker
#define COMPACT_NODE48_EMPTY 255

// Node flags (stored in the former _pad byte)
#define COMPACT_NODE_FLAG_DIRTY  0x01   // subtree modified, hash needs recomputation

// ============================================================================
// Compact Inner Node Structures
// ============================================================================

// With compact_ref_t (uint32_t) children, alignment requirement drops from
// 8 bytes (void*) to 4 bytes (uint32_t). Padding is just 1 byte per node.
// The `flags` byte (formerly _pad) carries per-node state bits (e.g., dirty).

/**
 * Node4: Up to 4 children
 * Size: 32 bytes (was 48 with void* children)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[4];
    uint8_t flags;
    compact_ref_t children[4];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node4_t;

/**
 * Node16: Up to 16 children
 * Size: 92 bytes (was 160 with void* children)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[16];
    uint8_t flags;
    compact_ref_t children[16];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node16_t;

/**
 * Node32: Up to 32 children
 * Size: 172 bytes (was 304 with void* children)
 * Lookup: 2x SSE compare over sorted keys
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[32];
    uint8_t flags;
    compact_ref_t children[32];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node32_t;

/**
 * Node48: Up to 48 children (indexed lookup)
 * Size: 460 bytes (was 656 with void* children)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t index[256];
    uint8_t flags;
    compact_ref_t children[48];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node48_t;

/**
 * Node256: Up to 256 children (direct indexing)
 * Size: 1036 bytes (was 2064 with void* children)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;   // 0 means 256 (wraps)
    uint8_t partial_len;
    uint8_t flags;
    compact_ref_t children[256];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node256_t;

// ============================================================================
// Key Fetch Callback (compact leaf mode)
// ============================================================================

/* Callback to fetch full key from external storage (for compact leaf mode).
 * Called during insert when leaf splitting needs the existing leaf's full key.
 * value: pointer to the stored leaf value (e.g., slot_id or node_record_t)
 * key_out: buffer of key_size bytes to fill with the full key
 * user_data: opaque context (e.g., pointer to flat_store)
 * Returns true on success. */
typedef bool (*compact_art_key_fetch_t)(const void *value, uint8_t *key_out, void *user_data);

// ============================================================================
// Tree Structure
// ============================================================================

typedef struct compact_art compact_art_t;
typedef struct compact_art_iterator compact_art_iterator_t;

#define COMPACT_NODE_TYPE_COUNT 5

/* Per-type free list for recycling abandoned inner nodes */
typedef struct {
    compact_ref_t *refs;
    uint32_t count;
    uint32_t cap;
} compact_free_list_t;

struct compact_art {
    compact_ref_t root;         // 0 = empty tree
    size_t size;                // Number of key-value pairs
    uint32_t key_size;          // Fixed key size for traversal (e.g., 32, 64)
    uint32_t value_size;        // Fixed value size (e.g., 4)
    uint32_t leaf_key_size;     // Bytes stored in leaf for verification (key_size or 8)
    uint32_t leaf_size;         // leaf_key_size + value_size (cached)
    uint32_t leaf_count;        // Number of leaves allocated
    compact_pool_t nodes;       // Inner node pool (variable-size, 8-byte aligned)
    compact_pool_t leaves;      // Leaf pool (fixed-size slots)
    compact_free_list_t free_nodes[COMPACT_NODE_TYPE_COUNT]; // Recycled inner nodes
    compact_art_key_fetch_t key_fetch;  /* fetch full key for split (compact mode only) */
    void *key_fetch_ctx;                /* user context for key_fetch */
};

// ============================================================================
// Public API
// ============================================================================

/**
 * Initialize a compact ART tree with fixed key and value sizes.
 * If compact_leaves is true, leaves store only an 8-byte verification
 * hash instead of the full key (saves memory at negligible collision risk).
 */
bool compact_art_init(compact_art_t *tree, uint32_t key_size, uint32_t value_size,
                       bool compact_leaves,
                       compact_art_key_fetch_t key_fetch, void *key_fetch_ctx);

/**
 * Destroy the tree and free all pool memory.
 */
void compact_art_destroy(compact_art_t *tree);

/**
 * Insert or update a key-value pair.
 * key must be exactly tree->key_size bytes.
 * value must be exactly tree->value_size bytes.
 */
bool compact_art_insert(compact_art_t *tree, const uint8_t *key, const void *value);

/**
 * Look up a key. Returns pointer to value (inside leaf pool) or NULL.
 * Returned pointer is valid until tree is destroyed.
 */
const void *compact_art_get(const compact_art_t *tree, const uint8_t *key);

/**
 * Check if a key exists.
 */
bool compact_art_contains(const compact_art_t *tree, const uint8_t *key);

/**
 * Delete a key. Returns true if found and deleted.
 */
bool compact_art_delete(compact_art_t *tree, const uint8_t *key);

/**
 * Get number of entries.
 */
size_t compact_art_size(const compact_art_t *tree);

/**
 * Check if tree is empty.
 */
bool compact_art_is_empty(const compact_art_t *tree);

/**
 * Navigate to the subtree rooted at the given key prefix.
 * Follows `prefix_len` bytes from the root, returning the compact_ref_t
 * of the node (or leaf) at that depth.
 * Returns COMPACT_REF_NULL if the prefix does not exist.
 * `depth_out` receives the actual byte depth reached.
 */
compact_ref_t compact_art_find_subtree(const compact_art_t *tree,
                                        const uint8_t *prefix,
                                        uint32_t prefix_len,
                                        uint32_t *depth_out);

/** Get memory usage: nodes pool + leaves pool (committed bytes). */
static inline size_t compact_art_memory_usage(const compact_art_t *tree) {
    return tree ? tree->nodes.used + tree->leaves.used : 0;
}

// ============================================================================
// Iterator (ordered traversal)
// ============================================================================

compact_art_iterator_t *compact_art_iterator_create(const compact_art_t *tree);
bool compact_art_iterator_next(compact_art_iterator_t *iter);
const uint8_t *compact_art_iterator_key(const compact_art_iterator_t *iter);
const void *compact_art_iterator_value(const compact_art_iterator_t *iter);
bool compact_art_iterator_done(const compact_art_iterator_t *iter);
void compact_art_iterator_destroy(compact_art_iterator_t *iter);

/**
 * Seek iterator to the first entry with key >= the given key (lower bound).
 * Returns true if positioned at a valid entry, false if all entries < key.
 * After seek, call iterator_next() to retrieve the current entry and advance.
 * Can be called at any point to re-position the iterator.
 */
bool compact_art_iterator_seek(compact_art_iterator_t *iter, const uint8_t *key);

#endif // COMPACT_ART_H
