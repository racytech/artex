#ifndef PERSISTENT_ART_H
#define PERSISTENT_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * Persistent Adaptive Radix Tree (persistent_art)
 *
 * COW file-backed ART for fixed-size keys and values.
 * Single file, mmap'd data, dual-meta atomic commit.
 *
 * Node types: Node16 (SSE2), Node64 (4×SSE2), Node256 (direct).
 * COW mutations — never modify committed data, always copy to new locations.
 * Dual meta pages — atomic commit with generation counter + CRC32C.
 * Instant recovery — read highest-generation valid meta → done.
 *
 * FIXED-SIZE KEYS ONLY. All keys must be the same length.
 */

// ============================================================================
// Ref Encoding (4 bytes)
// ============================================================================

typedef uint32_t part_ref_t;

#define PART_REF_NULL         ((part_ref_t)0)
#define PART_REF_LEAF_BIT     ((part_ref_t)0x80000000u)
#define PART_IS_LEAF_REF(r)   ((r) & PART_REF_LEAF_BIT)
#define PART_LEAF_INDEX(r)    ((r) & 0x7FFFFFFFu)
#define PART_MAKE_LEAF_REF(i) ((part_ref_t)(i) | PART_REF_LEAF_BIT)

// ============================================================================
// Tree Structure
// ============================================================================

typedef struct persistent_art persistent_art_t;
typedef struct part_iterator part_iterator_t;

struct persistent_art {
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

bool part_open(persistent_art_t *tree, const char *path,
               uint32_t key_size, uint32_t value_size);
void part_close(persistent_art_t *tree);

// ============================================================================
// COW Mutations (pending until commit)
// ============================================================================

bool part_insert(persistent_art_t *tree, const uint8_t *key, const void *value);
bool part_delete(persistent_art_t *tree, const uint8_t *key);

// ============================================================================
// Reads
// ============================================================================

const void *part_get(const persistent_art_t *tree, const uint8_t *key);
bool        part_contains(const persistent_art_t *tree, const uint8_t *key);
size_t      part_size(const persistent_art_t *tree);

// ============================================================================
// Persistence
// ============================================================================

bool part_commit(persistent_art_t *tree);
void part_rollback(persistent_art_t *tree);

// ============================================================================
// Iterator (sorted order)
// ============================================================================

part_iterator_t *part_iterator_create(const persistent_art_t *tree);
bool             part_iterator_next(part_iterator_t *iter);
const uint8_t   *part_iterator_key(const part_iterator_t *iter);
const void      *part_iterator_value(const part_iterator_t *iter);
bool             part_iterator_done(const part_iterator_t *iter);
void             part_iterator_destroy(part_iterator_t *iter);
bool             part_iterator_seek(part_iterator_t *iter, const uint8_t *key);

#endif // PERSISTENT_ART_H
