#ifndef BITMAP_ART_H
#define BITMAP_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Bitmap ART (bitmap_art)
 *
 * COW file-backed ART with a single node type: bitmap + packed children.
 * Same file layout and commit protocol as persistent_art, different node
 * representation for comparison.
 *
 * ONE node struct: 256-bit bitmap (32 bytes) + packed children[].
 * O(1) child lookup via hardware popcount.
 * Variable-size nodes — size adapts naturally with each COW copy.
 * No growth/shrink transitions.
 *
 * FIXED-SIZE KEYS ONLY. All keys must be the same length.
 */

// ============================================================================
// Ref Encoding (4 bytes)
// ============================================================================

typedef uint32_t bart_ref_t;

#define BART_REF_NULL         ((bart_ref_t)0)
#define BART_REF_LEAF_BIT     ((bart_ref_t)0x80000000u)
#define BART_IS_LEAF_REF(r)   ((r) & BART_REF_LEAF_BIT)
#define BART_LEAF_INDEX(r)    ((r) & 0x7FFFFFFFu)
#define BART_MAKE_LEAF_REF(i) ((bart_ref_t)(i) | BART_REF_LEAF_BIT)

// ============================================================================
// Tree Structure
// ============================================================================

typedef struct bitmap_art bitmap_art_t;
typedef struct bart_iterator bart_iterator_t;

struct bitmap_art {
    int fd;
    uint8_t *node_base;     // mmap at file offset 8192
    uint8_t *leaf_base;     // mmap at file offset 8192 + NODE_POOL_MAX

    uint32_t root;
    uint64_t size;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t leaf_size;     // key_size + value_size

    size_t   node_used;     // bytes used in node pool
    uint32_t leaf_count;    // leaves allocated

    // Committed state snapshot (for rollback)
    uint32_t committed_root;
    uint64_t committed_size;
    size_t   committed_node_used;
    uint32_t committed_leaf_count;

    uint64_t generation;
    int      active_meta;   // 0 or 1
};

// ============================================================================
// Lifecycle
// ============================================================================

bool bart_open(bitmap_art_t *tree, const char *path,
               uint32_t key_size, uint32_t value_size);
void bart_close(bitmap_art_t *tree);

// ============================================================================
// COW Mutations (pending until commit)
// ============================================================================

bool bart_insert(bitmap_art_t *tree, const uint8_t *key, const void *value);
bool bart_delete(bitmap_art_t *tree, const uint8_t *key);

// ============================================================================
// Reads
// ============================================================================

const void *bart_get(const bitmap_art_t *tree, const uint8_t *key);
bool        bart_contains(const bitmap_art_t *tree, const uint8_t *key);
size_t      bart_size(const bitmap_art_t *tree);

// ============================================================================
// Persistence
// ============================================================================

bool bart_commit(bitmap_art_t *tree);
void bart_rollback(bitmap_art_t *tree);

// ============================================================================
// Iterator (sorted order)
// ============================================================================

bart_iterator_t *bart_iterator_create(const bitmap_art_t *tree);
bool             bart_iterator_next(bart_iterator_t *iter);
const uint8_t   *bart_iterator_key(const bart_iterator_t *iter);
const void      *bart_iterator_value(const bart_iterator_t *iter);
bool             bart_iterator_done(const bart_iterator_t *iter);
void             bart_iterator_destroy(bart_iterator_t *iter);
bool             bart_iterator_seek(bart_iterator_t *iter, const uint8_t *key);

#endif // BITMAP_ART_H
