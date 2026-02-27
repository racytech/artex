#ifndef COMPACT_ART_H
#define COMPACT_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Compact Adaptive Radix Tree (compact_art)
 *
 * Space-efficient in-memory ART for fixed-size keys and values.
 * Designed as a pure index: 32-byte keccak256 keys → 12-byte (offset+length) values.
 *
 * Optimizations over mem_art (~97 B/key → ~59 B/key target):
 *   1. Arena allocator — eliminates malloc overhead (16 B/allocation)
 *   2. Tagged pointers — LSB marks leaf, removes type field from leaf
 *   3. Fixed-size fields — no per-leaf key_len/value_len
 *   4. Compact inner node headers — uint8_t type/num_children/partial_len
 *   5. Node32 — fills gap between Node16 and Node48 for ~30-child nodes
 *
 * FIXED-SIZE KEYS ONLY. All keys must be the same length.
 * MEMORY-ONLY — no persistence. Data lost on process exit.
 */

// ============================================================================
// Arena Allocator
// ============================================================================

#define COMPACT_ARENA_SLAB_SIZE (64ULL * 1024 * 1024)  // 64 MB slabs

typedef struct {
    uint8_t *base;
    size_t capacity;
    size_t used;
} compact_arena_slab_t;

typedef struct {
    compact_arena_slab_t *slabs;
    size_t num_slabs;
    size_t slab_cap;       // allocated capacity of slabs array
    size_t slab_size;      // size of each slab (COMPACT_ARENA_SLAB_SIZE)
} compact_arena_t;

// ============================================================================
// Node Types
// ============================================================================

typedef enum {
    COMPACT_NODE_4   = 0,
    COMPACT_NODE_16  = 1,
    COMPACT_NODE_32  = 2,   // New: fills gap between Node16 and Node48
    COMPACT_NODE_48  = 3,
    COMPACT_NODE_256 = 4,
} compact_node_type_t;

// Maximum children per node type
#define COMPACT_NODE4_MAX   4
#define COMPACT_NODE16_MAX  16
#define COMPACT_NODE32_MAX  32
#define COMPACT_NODE48_MAX  48
#define COMPACT_NODE256_MAX 256

// Maximum prefix bytes stored inline
#define COMPACT_MAX_PREFIX 8

// Node48 empty index marker
#define COMPACT_NODE48_EMPTY 255

// ============================================================================
// Tagged Pointers (Leaf Detection)
// ============================================================================

// Arena allocations are ≥4-byte aligned, so LSB is always 0 for real pointers.
// We use LSB=1 to mark leaf pointers.
#define COMPACT_IS_LEAF(p)    ((uintptr_t)(p) & 1)
#define COMPACT_MAKE_LEAF(p)  ((void *)((uintptr_t)(p) | 1))
#define COMPACT_GET_LEAF(p)   ((void *)((uintptr_t)(p) & ~(uintptr_t)1))

// ============================================================================
// Compact Leaf (no header, fixed-size key + value)
// ============================================================================

// Leaf is just raw bytes: key[key_size] followed by value[value_size].
// Accessed via COMPACT_GET_LEAF(ptr), then:
//   key   = (uint8_t *)leaf
//   value = (uint8_t *)leaf + tree->key_size

// ============================================================================
// Compact Inner Node Structures (cold partial at end)
// ============================================================================

// Header fields use uint8_t (was uint32_t/enum in mem_art).
// Structs are NOT packed — children[] must be pointer-aligned to avoid
// unaligned access warnings. The ~5 bytes of alignment padding per node
// is negligible (~85 MB across 17M inner nodes).

/**
 * Node4: Up to 4 children
 * Size: ~80 bytes (with alignment padding)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[4];
    uint8_t _pad[1];           // align children to 8 bytes
    void *children[4];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node4_t;

/**
 * Node16: Up to 16 children
 * Size: ~192 bytes (with alignment padding)
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[16];
    uint8_t _pad[5];           // align children to 8 bytes
    void *children[16];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node16_t;

/**
 * Node32: Up to 32 children (NEW — fills gap between Node16 and Node48)
 * Size: ~336 bytes (vs Node48's ~696 bytes)
 * Lookup: 2x SSE compare over sorted keys
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[32];
    uint8_t _pad[5];           // align children to 8 bytes
    void *children[32];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node32_t;

/**
 * Node48: Up to 48 children (indexed lookup)
 * Size: ~696 bytes
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t index[256];
    uint8_t _pad[5];           // align children to 8 bytes
    void *children[48];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node48_t;

/**
 * Node256: Up to 256 children (direct indexing)
 * Size: ~2088 bytes
 */
typedef struct {
    uint8_t type;
    uint8_t num_children;   // 0 means 256 (wraps)
    uint8_t partial_len;
    uint8_t _pad[5];           // align children to 8 bytes
    void *children[256];
    uint8_t partial[COMPACT_MAX_PREFIX];
} compact_node256_t;

// ============================================================================
// Tree Structure
// ============================================================================

typedef struct compact_art compact_art_t;
typedef struct compact_art_iterator compact_art_iterator_t;

struct compact_art {
    void *root;                 // Tagged pointer (LSB=1 for leaf)
    size_t size;                // Number of key-value pairs
    uint32_t key_size;          // Fixed key size (e.g., 32)
    uint32_t value_size;        // Fixed value size (e.g., 12)
    compact_arena_t arena;      // Bump allocator for all nodes + leaves
};

// ============================================================================
// Public API
// ============================================================================

/**
 * Initialize a compact ART tree with fixed key and value sizes.
 */
bool compact_art_init(compact_art_t *tree, uint32_t key_size, uint32_t value_size);

/**
 * Destroy the tree and free all arena memory.
 */
void compact_art_destroy(compact_art_t *tree);

/**
 * Insert or update a key-value pair.
 * key must be exactly tree->key_size bytes.
 * value must be exactly tree->value_size bytes.
 */
bool compact_art_insert(compact_art_t *tree, const uint8_t *key, const void *value);

/**
 * Look up a key. Returns pointer to value (inside arena) or NULL.
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

// ============================================================================
// Iterator (ordered traversal)
// ============================================================================

/**
 * Create an iterator for lexicographic traversal.
 */
compact_art_iterator_t *compact_art_iterator_create(const compact_art_t *tree);

/**
 * Move to next entry. Returns false when done.
 */
bool compact_art_iterator_next(compact_art_iterator_t *iter);

/**
 * Get current key (tree->key_size bytes).
 */
const uint8_t *compact_art_iterator_key(const compact_art_iterator_t *iter);

/**
 * Get current value (tree->value_size bytes).
 */
const void *compact_art_iterator_value(const compact_art_iterator_t *iter);

/**
 * Check if iteration is complete.
 */
bool compact_art_iterator_done(const compact_art_iterator_t *iter);

/**
 * Destroy iterator.
 */
void compact_art_iterator_destroy(compact_art_iterator_t *iter);

#endif // COMPACT_ART_H
