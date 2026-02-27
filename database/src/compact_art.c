#include "compact_art.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <emmintrin.h>  // SSE2

/**
 * Compact ART Implementation — 4-byte child refs
 *
 * Space-efficient in-memory ART for fixed-size keys.
 *
 * Key design:
 * - Two contiguous mmap pools: leaves (fixed-size slots) + nodes (bump-allocated)
 * - 4-byte compact_ref_t instead of 8-byte void* pointers
 *   bit 31 = 1: leaf ref (index into leaf pool)
 *   bit 31 = 0: node ref (byte_offset/8 into node pool), 0 = NULL
 * - Packed inner node structs (uint8_t header fields, uint32_t children)
 * - Node32 with 2x SSE lookup
 */

// ============================================================================
// Pool Allocator
// ============================================================================

// Default virtual reservations (MAP_NORESERVE — virtual only, demand-paged)
#define COMPACT_NODE_POOL_RESERVE  (16ULL * 1024 * 1024 * 1024)  // 16 GB
#define COMPACT_LEAF_POOL_RESERVE  (64ULL * 1024 * 1024 * 1024)  // 64 GB

#ifndef MAP_NORESERVE
#define MAP_NORESERVE 0
#endif

static bool pool_init(compact_pool_t *pool, size_t reserve_bytes) {
    void *mem = mmap(NULL, reserve_bytes,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                     -1, 0);
    if (mem == MAP_FAILED) {
        // Fallback: try 1/4 size
        reserve_bytes /= 4;
        mem = mmap(NULL, reserve_bytes,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                   -1, 0);
        if (mem == MAP_FAILED) return false;
    }
    pool->base = mem;
    pool->reserved = reserve_bytes;
    pool->used = 0;
    return true;
}

static void pool_destroy(compact_pool_t *pool) {
    if (pool->base) {
        munmap(pool->base, pool->reserved);
        pool->base = NULL;
    }
    pool->reserved = 0;
    pool->used = 0;
}

// ============================================================================
// Ref Resolution
// ============================================================================

// Resolve a node ref to a raw pointer.
// Caller guarantees ref is not a leaf ref and not NULL.
static inline void *node_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->nodes.base + (size_t)ref * 8;
}

// Resolve a leaf ref to raw leaf data (key || value).
static inline uint8_t *leaf_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->leaves.base +
           (size_t)COMPACT_LEAF_INDEX(ref) * tree->leaf_size;
}

// ============================================================================
// Leaf Helpers
// ============================================================================

static inline compact_ref_t alloc_leaf(compact_art_t *tree,
                                        const uint8_t *key,
                                        const void *value) {
    uint32_t idx = tree->leaf_count;
    size_t offset = (size_t)idx * tree->leaf_size;
    if (offset + tree->leaf_size > tree->leaves.reserved)
        return COMPACT_REF_NULL;

    tree->leaf_count++;
    if (offset + tree->leaf_size > tree->leaves.used)
        tree->leaves.used = offset + tree->leaf_size;

    uint8_t *leaf = tree->leaves.base + offset;
    memcpy(leaf, key, tree->key_size);
    memcpy(leaf + tree->key_size, value, tree->value_size);

    return COMPACT_MAKE_LEAF_REF(idx);
}

static inline const uint8_t *leaf_key(const compact_art_t *tree,
                                       compact_ref_t ref) {
    return leaf_ptr(tree, ref);
}

static inline const void *leaf_value(const compact_art_t *tree,
                                      compact_ref_t ref) {
    return leaf_ptr(tree, ref) + tree->key_size;
}

static inline bool leaf_matches(const compact_art_t *tree,
                                 compact_ref_t ref,
                                 const uint8_t *key) {
    return memcmp(leaf_key(tree, ref), key, tree->key_size) == 0;
}

static inline void leaf_set_value(const compact_art_t *tree,
                                   compact_ref_t ref,
                                   const void *value) {
    uint8_t *leaf = leaf_ptr(tree, ref);
    memcpy(leaf + tree->key_size, value, tree->value_size);
}

// ============================================================================
// Inner Node Allocation
// ============================================================================

static compact_ref_t alloc_node(compact_art_t *tree, compact_node_type_t type) {
    size_t size;
    switch (type) {
        case COMPACT_NODE_4:   size = sizeof(compact_node4_t);   break;
        case COMPACT_NODE_16:  size = sizeof(compact_node16_t);  break;
        case COMPACT_NODE_32:  size = sizeof(compact_node32_t);  break;
        case COMPACT_NODE_48:  size = sizeof(compact_node48_t);  break;
        case COMPACT_NODE_256: size = sizeof(compact_node256_t); break;
        default: return COMPACT_REF_NULL;
    }

    // 8-byte aligned bump allocation
    compact_pool_t *pool = &tree->nodes;
    size_t aligned = (pool->used + 7) & ~(size_t)7;
    if (aligned + size > pool->reserved) return COMPACT_REF_NULL;
    pool->used = aligned + size;

    void *node = pool->base + aligned;
    memset(node, 0, size);
    ((uint8_t *)node)[0] = (uint8_t)type;

    if (type == COMPACT_NODE_48) {
        compact_node48_t *n48 = node;
        memset(n48->index, COMPACT_NODE48_EMPTY, 256);
    }

    return (compact_ref_t)(aligned / 8);
}

// ============================================================================
// Node Accessors
// ============================================================================

static inline compact_node_type_t node_type(const void *node) {
    return (compact_node_type_t)(((const uint8_t *)node)[0]);
}

static inline uint8_t node_num_children(const void *node) {
    return ((const uint8_t *)node)[1];
}

static inline uint8_t node_partial_len(const void *node) {
    return ((const uint8_t *)node)[2];
}

static inline const uint8_t *node_partial(const void *node) {
    switch (node_type(node)) {
        case COMPACT_NODE_4:   return ((const compact_node4_t *)node)->partial;
        case COMPACT_NODE_16:  return ((const compact_node16_t *)node)->partial;
        case COMPACT_NODE_32:  return ((const compact_node32_t *)node)->partial;
        case COMPACT_NODE_48:  return ((const compact_node48_t *)node)->partial;
        case COMPACT_NODE_256: return ((const compact_node256_t *)node)->partial;
        default: return NULL;
    }
}

static inline uint8_t *node_partial_mut(void *node) {
    switch (node_type(node)) {
        case COMPACT_NODE_4:   return ((compact_node4_t *)node)->partial;
        case COMPACT_NODE_16:  return ((compact_node16_t *)node)->partial;
        case COMPACT_NODE_32:  return ((compact_node32_t *)node)->partial;
        case COMPACT_NODE_48:  return ((compact_node48_t *)node)->partial;
        case COMPACT_NODE_256: return ((compact_node256_t *)node)->partial;
        default: return NULL;
    }
}

// ============================================================================
// find_minimum_leaf — leftmost leaf in subtree (for optimistic prefix check)
// ============================================================================

static compact_ref_t find_minimum_leaf(const compact_art_t *tree,
                                        compact_ref_t ref) {
    while (ref != COMPACT_REF_NULL && !COMPACT_IS_LEAF_REF(ref)) {
        void *node = node_ptr(tree, ref);
        switch (node_type(node)) {
            case COMPACT_NODE_4:
                ref = ((compact_node4_t *)node)->children[0];
                break;
            case COMPACT_NODE_16:
                ref = ((compact_node16_t *)node)->children[0];
                break;
            case COMPACT_NODE_32:
                ref = ((compact_node32_t *)node)->children[0];
                break;
            case COMPACT_NODE_48: {
                compact_node48_t *n = node;
                for (int i = 0; i < 256; i++) {
                    if (n->index[i] != COMPACT_NODE48_EMPTY) {
                        ref = n->children[n->index[i]];
                        break;
                    }
                }
                break;
            }
            case COMPACT_NODE_256: {
                compact_node256_t *n = node;
                for (int i = 0; i < 256; i++) {
                    if (n->children[i] != COMPACT_REF_NULL) {
                        ref = n->children[i];
                        break;
                    }
                }
                break;
            }
            default:
                return COMPACT_REF_NULL;
        }
    }
    return ref;
}

// ============================================================================
// check_prefix — compare compressed path against key (optimistic)
//
// Compares stored prefix bytes first (up to COMPACT_MAX_PREFIX).
// If prefix is longer, compares remaining bytes against a leaf key.
// ============================================================================

static int check_prefix(const compact_art_t *tree, compact_ref_t ref,
                        const void *node, const uint8_t *key,
                        uint32_t key_size, size_t depth) {
    uint8_t plen = node_partial_len(node);
    const uint8_t *partial = node_partial(node);
    int max_cmp = plen;
    if (depth + (size_t)max_cmp > key_size) {
        max_cmp = (int)key_size - (int)depth;
    }

    // Compare stored bytes (up to COMPACT_MAX_PREFIX)
    int stored = max_cmp < COMPACT_MAX_PREFIX ? max_cmp : COMPACT_MAX_PREFIX;
    for (int idx = 0; idx < stored; idx++) {
        if (partial[idx] != key[depth + idx]) return idx;
    }

    // If prefix extends beyond stored bytes, compare against leaf key
    if (max_cmp > COMPACT_MAX_PREFIX) {
        compact_ref_t min_leaf = find_minimum_leaf(tree, ref);
        if (min_leaf == COMPACT_REF_NULL) return COMPACT_MAX_PREFIX;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int idx = COMPACT_MAX_PREFIX; idx < max_cmp; idx++) {
            if (lk[depth + idx] != key[depth + idx]) return idx;
        }
    }

    return max_cmp;
}

// ============================================================================
// find_child — find child ref for a given byte
// ============================================================================

static compact_ref_t *find_child_ptr(const compact_art_t *tree,
                                      compact_ref_t node_ref, uint8_t byte) {
    void *node = node_ptr(tree, node_ref);
    switch (node_type(node)) {
        case COMPACT_NODE_4: {
            compact_node4_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) return &n->children[i];
            }
            return NULL;
        }
        case COMPACT_NODE_16: {
            compact_node16_t *n = node;
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)n->keys));
            int mask = _mm_movemask_epi8(cmp) & ((1 << n->num_children) - 1);
            if (mask) return &n->children[__builtin_ctz(mask)];
            return NULL;
        }
        case COMPACT_NODE_32: {
            compact_node32_t *n = node;
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp1 = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)n->keys));
            __m128i cmp2 = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)(n->keys + 16)));
            int mask1, mask2;
            if (n->num_children <= 16) {
                mask1 = _mm_movemask_epi8(cmp1) & ((1 << n->num_children) - 1);
                mask2 = 0;
            } else {
                mask1 = _mm_movemask_epi8(cmp1);
                mask2 = _mm_movemask_epi8(cmp2) &
                         ((1 << (n->num_children - 16)) - 1);
            }
            if (mask1) return &n->children[__builtin_ctz(mask1)];
            if (mask2) return &n->children[16 + __builtin_ctz(mask2)];
            return NULL;
        }
        case COMPACT_NODE_48: {
            compact_node48_t *n = node;
            uint8_t idx = n->index[byte];
            if (idx == COMPACT_NODE48_EMPTY) return NULL;
            return &n->children[idx];
        }
        case COMPACT_NODE_256: {
            compact_node256_t *n = node;
            if (n->children[byte] == COMPACT_REF_NULL) return NULL;
            return &n->children[byte];
        }
        default:
            return NULL;
    }
}

static compact_ref_t find_child(const compact_art_t *tree,
                                 compact_ref_t node_ref, uint8_t byte) {
    compact_ref_t *ptr = find_child_ptr(tree, node_ref, byte);
    return ptr ? *ptr : COMPACT_REF_NULL;
}

// ============================================================================
// replace_child — replace child ref in-place
// ============================================================================

static void replace_child(const compact_art_t *tree, compact_ref_t node_ref,
                           uint8_t byte, compact_ref_t new_child) {
    compact_ref_t *ptr = find_child_ptr(tree, node_ref, byte);
    if (ptr) *ptr = new_child;
}

// ============================================================================
// add_child — add a new child, growing node if full
// ============================================================================

static compact_ref_t add_child(compact_art_t *tree, compact_ref_t node_ref,
                                uint8_t byte, compact_ref_t child);

// Copy header (partial_len + partial) from old node to new node.
// Only copies up to COMPACT_MAX_PREFIX stored bytes.
static void copy_header(void *dst, const void *src) {
    uint8_t plen = ((const uint8_t *)src)[2];
    ((uint8_t *)dst)[2] = plen;
    uint8_t store = plen < COMPACT_MAX_PREFIX ? plen : COMPACT_MAX_PREFIX;
    memcpy(node_partial_mut(dst), node_partial(src), store);
}

static compact_ref_t add_child(compact_art_t *tree, compact_ref_t node_ref,
                                uint8_t byte, compact_ref_t child) {
    void *node = node_ptr(tree, node_ref);
    switch (node_type(node)) {
        case COMPACT_NODE_4: {
            compact_node4_t *n = node;
            if (n->num_children < COMPACT_NODE4_MAX) {
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i],
                            n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(compact_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node16
            compact_ref_t n16_ref = alloc_node(tree, COMPACT_NODE_16);
            if (n16_ref == COMPACT_REF_NULL) return node_ref;
            // Re-resolve after alloc (pool is contiguous, no move, but good hygiene)
            n = node_ptr(tree, node_ref);
            compact_node16_t *n16 = node_ptr(tree, n16_ref);
            copy_header(n16, n);
            memcpy(n16->keys, n->keys, COMPACT_NODE4_MAX);
            memcpy(n16->children, n->children,
                   COMPACT_NODE4_MAX * sizeof(compact_ref_t));
            n16->num_children = COMPACT_NODE4_MAX;
            return add_child(tree, n16_ref, byte, child);
        }

        case COMPACT_NODE_16: {
            compact_node16_t *n = node;
            if (n->num_children < COMPACT_NODE16_MAX) {
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i],
                            n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(compact_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node32
            compact_ref_t n32_ref = alloc_node(tree, COMPACT_NODE_32);
            if (n32_ref == COMPACT_REF_NULL) return node_ref;
            n = node_ptr(tree, node_ref);
            compact_node32_t *n32 = node_ptr(tree, n32_ref);
            copy_header(n32, n);
            memcpy(n32->keys, n->keys, COMPACT_NODE16_MAX);
            memcpy(n32->children, n->children,
                   COMPACT_NODE16_MAX * sizeof(compact_ref_t));
            n32->num_children = COMPACT_NODE16_MAX;
            return add_child(tree, n32_ref, byte, child);
        }

        case COMPACT_NODE_32: {
            compact_node32_t *n = node;
            if (n->num_children < COMPACT_NODE32_MAX) {
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i],
                            n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(compact_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node48
            compact_ref_t n48_ref = alloc_node(tree, COMPACT_NODE_48);
            if (n48_ref == COMPACT_REF_NULL) return node_ref;
            n = node_ptr(tree, node_ref);
            compact_node48_t *n48 = node_ptr(tree, n48_ref);
            copy_header(n48, n);
            for (int i = 0; i < COMPACT_NODE32_MAX; i++) {
                n48->index[n->keys[i]] = i;
                n48->children[i] = n->children[i];
            }
            n48->num_children = COMPACT_NODE32_MAX;
            return add_child(tree, n48_ref, byte, child);
        }

        case COMPACT_NODE_48: {
            compact_node48_t *n = node;
            if (n->num_children < COMPACT_NODE48_MAX) {
                int slot;
                for (slot = 0; slot < COMPACT_NODE48_MAX; slot++) {
                    if (n->children[slot] == COMPACT_REF_NULL) break;
                }
                n->index[byte] = (uint8_t)slot;
                n->children[slot] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node256
            compact_ref_t n256_ref = alloc_node(tree, COMPACT_NODE_256);
            if (n256_ref == COMPACT_REF_NULL) return node_ref;
            n = node_ptr(tree, node_ref);
            compact_node256_t *n256 = node_ptr(tree, n256_ref);
            copy_header(n256, n);
            for (int i = 0; i < 256; i++) {
                if (n->index[i] != COMPACT_NODE48_EMPTY) {
                    n256->children[i] = n->children[n->index[i]];
                }
            }
            n256->num_children = n->num_children;
            return add_child(tree, n256_ref, byte, child);
        }

        case COMPACT_NODE_256: {
            compact_node256_t *n = node;
            if (n->children[byte] == COMPACT_REF_NULL) {
                n->num_children++;
            }
            n->children[byte] = child;
            return node_ref;
        }

        default:
            return node_ref;
    }
}

// ============================================================================
// remove_child — remove a child, shrinking node if needed
// ============================================================================

static compact_ref_t remove_child(compact_art_t *tree, compact_ref_t node_ref,
                                   uint8_t byte) {
    void *node = node_ptr(tree, node_ref);
    switch (node_type(node)) {
        case COMPACT_NODE_4: {
            compact_node4_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(compact_ref_t));
                    n->num_children--;
                    return node_ref;
                }
            }
            break;
        }
        case COMPACT_NODE_16: {
            compact_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(compact_ref_t));
                    n->num_children--;
                    if (n->num_children <= COMPACT_NODE4_MAX) {
                        compact_ref_t n4_ref = alloc_node(tree, COMPACT_NODE_4);
                        if (n4_ref != COMPACT_REF_NULL) {
                            n = node_ptr(tree, node_ref);
                            compact_node4_t *n4 = node_ptr(tree, n4_ref);
                            copy_header(n4, n);
                            n4->num_children = n->num_children;
                            memcpy(n4->keys, n->keys, n->num_children);
                            memcpy(n4->children, n->children,
                                   n->num_children * sizeof(compact_ref_t));
                            return n4_ref;
                        }
                    }
                    return node_ref;
                }
            }
            break;
        }
        case COMPACT_NODE_32: {
            compact_node32_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(compact_ref_t));
                    n->num_children--;
                    if (n->num_children <= COMPACT_NODE16_MAX) {
                        compact_ref_t n16_ref = alloc_node(tree, COMPACT_NODE_16);
                        if (n16_ref != COMPACT_REF_NULL) {
                            n = node_ptr(tree, node_ref);
                            compact_node16_t *n16 = node_ptr(tree, n16_ref);
                            copy_header(n16, n);
                            n16->num_children = n->num_children;
                            memcpy(n16->keys, n->keys, n->num_children);
                            memcpy(n16->children, n->children,
                                   n->num_children * sizeof(compact_ref_t));
                            return n16_ref;
                        }
                    }
                    return node_ref;
                }
            }
            break;
        }
        case COMPACT_NODE_48: {
            compact_node48_t *n = node;
            uint8_t idx = n->index[byte];
            if (idx != COMPACT_NODE48_EMPTY) {
                n->children[idx] = COMPACT_REF_NULL;
                n->index[byte] = COMPACT_NODE48_EMPTY;
                n->num_children--;
                if (n->num_children <= COMPACT_NODE32_MAX) {
                    compact_ref_t n32_ref = alloc_node(tree, COMPACT_NODE_32);
                    if (n32_ref != COMPACT_REF_NULL) {
                        n = node_ptr(tree, node_ref);
                        compact_node32_t *n32 = node_ptr(tree, n32_ref);
                        copy_header(n32, n);
                        n32->num_children = 0;
                        for (int i = 0; i < 256; i++) {
                            if (n->index[i] != COMPACT_NODE48_EMPTY) {
                                int pos = n32->num_children;
                                n32->keys[pos] = (uint8_t)i;
                                n32->children[pos] = n->children[n->index[i]];
                                n32->num_children++;
                            }
                        }
                        return n32_ref;
                    }
                }
                return node_ref;
            }
            break;
        }
        case COMPACT_NODE_256: {
            compact_node256_t *n = node;
            n->children[byte] = COMPACT_REF_NULL;
            n->num_children--;
            if (n->num_children <= COMPACT_NODE48_MAX) {
                compact_ref_t n48_ref = alloc_node(tree, COMPACT_NODE_48);
                if (n48_ref != COMPACT_REF_NULL) {
                    n = node_ptr(tree, node_ref);
                    compact_node48_t *n48 = node_ptr(tree, n48_ref);
                    copy_header(n48, n);
                    n48->num_children = 0;
                    for (int i = 0; i < 256; i++) {
                        if (n->children[i] != COMPACT_REF_NULL) {
                            n48->index[i] = n48->num_children;
                            n48->children[n48->num_children] = n->children[i];
                            n48->num_children++;
                        }
                    }
                    return n48_ref;
                }
            }
            return node_ref;
        }
        default:
            break;
    }
    return node_ref;
}

// ============================================================================
// search — iterative lookup
// ============================================================================

static const void *search(const compact_art_t *tree, compact_ref_t ref,
                          const uint8_t *key, size_t depth) {
    while (ref != COMPACT_REF_NULL) {
        if (COMPACT_IS_LEAF_REF(ref)) {
            if (leaf_matches(tree, ref, key)) {
                return leaf_value(tree, ref);
            }
            return NULL;
        }

        void *node = node_ptr(tree, ref);

        // Check compressed path (optimistic: uses leaf for long prefixes)
        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            int prefix_len = check_prefix(tree, ref, node, key,
                                           tree->key_size, depth);
            if (prefix_len != plen) return NULL;
            depth += plen;
        }

        uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
        compact_ref_t child = find_child(tree, ref, byte);
        if (child == COMPACT_REF_NULL) return NULL;

        if (depth >= tree->key_size) {
            if (COMPACT_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
                return leaf_value(tree, child);
            }
            return NULL;
        }

        ref = child;
        depth++;
    }
    return NULL;
}

// ============================================================================
// insert_recursive
// ============================================================================

static compact_ref_t insert_recursive(compact_art_t *tree, compact_ref_t ref,
                                       const uint8_t *key, size_t depth,
                                       const void *value, bool *inserted) {
    // Base case: empty slot
    if (ref == COMPACT_REF_NULL) {
        *inserted = true;
        return alloc_leaf(tree, key, value);
    }

    // Leaf: match or split
    if (COMPACT_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            leaf_set_value(tree, ref, value);
            *inserted = false;
            return ref;
        }

        // Keys differ — create new inner node
        *inserted = true;
        compact_ref_t new_leaf = alloc_leaf(tree, key, value);
        if (new_leaf == COMPACT_REF_NULL) return ref;

        const uint8_t *existing_key = leaf_key(tree, ref);

        // Find common prefix from current depth
        size_t limit = tree->key_size;
        size_t prefix_len = 0;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == existing_key[depth + prefix_len]) {
            prefix_len++;
        }

        compact_ref_t new_node_ref = alloc_node(tree, COMPACT_NODE_4);
        if (new_node_ref == COMPACT_REF_NULL) return ref;

        compact_node4_t *new_node = node_ptr(tree, new_node_ref);
        new_node->partial_len = (uint8_t)prefix_len;
        if (prefix_len > 0) {
            size_t store = prefix_len < COMPACT_MAX_PREFIX
                           ? prefix_len : COMPACT_MAX_PREFIX;
            memcpy(new_node->partial, key + depth, store);
        }

        size_t new_depth = depth + prefix_len;
        uint8_t new_byte = (new_depth < tree->key_size) ? key[new_depth] : 0x00;
        uint8_t old_byte = (new_depth < tree->key_size) ?
                            existing_key[new_depth] : 0x00;

        compact_ref_t result = add_child(tree, new_node_ref, new_byte, new_leaf);
        result = add_child(tree, result, old_byte, ref);
        return result;
    }

    // Inner node: check compressed path
    void *node = node_ptr(tree, ref);
    uint8_t plen = node_partial_len(node);
    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);

        if (prefix_len < plen) {
            // Prefix mismatch — split
            *inserted = true;

            // Get the mismatch byte from the existing subtree.
            // If mismatch is within stored bytes, read from partial.
            // Otherwise, find a leaf and read from the leaf key.
            uint8_t *partial = node_partial_mut(node);
            uint8_t old_byte;
            const uint8_t *leaf_k = NULL;
            if (prefix_len < COMPACT_MAX_PREFIX) {
                old_byte = partial[prefix_len];
            } else {
                compact_ref_t min_leaf = find_minimum_leaf(tree, ref);
                leaf_k = leaf_key(tree, min_leaf);
                old_byte = leaf_k[depth + prefix_len];
            }

            compact_ref_t new_node_ref = alloc_node(tree, COMPACT_NODE_4);
            if (new_node_ref == COMPACT_REF_NULL) return ref;

            // Re-resolve after alloc (node pool doesn't move, but be safe)
            node = node_ptr(tree, ref);
            partial = node_partial_mut(node);

            compact_node4_t *new_node = node_ptr(tree, new_node_ref);
            new_node->partial_len = (uint8_t)prefix_len;
            {
                size_t store = (size_t)prefix_len < COMPACT_MAX_PREFIX
                               ? (size_t)prefix_len : COMPACT_MAX_PREFIX;
                memcpy(new_node->partial, partial, store);
            }

            // Update old node's prefix: remove first (prefix_len + 1) bytes
            uint8_t new_partial_len = plen - (prefix_len + 1);
            if (prefix_len + 1 < COMPACT_MAX_PREFIX) {
                // Some stored bytes remain valid — shift them
                size_t shift = (size_t)prefix_len + 1;
                size_t avail = COMPACT_MAX_PREFIX - shift;
                size_t keep = (size_t)new_partial_len < avail
                              ? (size_t)new_partial_len : avail;
                memmove(partial, partial + shift, keep);
            } else {
                // All stored bytes consumed — repopulate from leaf
                if (!leaf_k) {
                    compact_ref_t min_leaf = find_minimum_leaf(tree, ref);
                    leaf_k = leaf_key(tree, min_leaf);
                }
                size_t store = (size_t)new_partial_len < COMPACT_MAX_PREFIX
                               ? (size_t)new_partial_len : COMPACT_MAX_PREFIX;
                memcpy(partial, leaf_k + depth + prefix_len + 1, store);
            }
            ((uint8_t *)node)[2] = new_partial_len;

            compact_ref_t leaf = alloc_leaf(tree, key, value);
            if (leaf == COMPACT_REF_NULL) return ref;

            uint8_t new_byte = (depth + prefix_len < tree->key_size) ?
                                key[depth + prefix_len] : 0x00;

            compact_ref_t result = add_child(tree, new_node_ref, old_byte, ref);
            result = add_child(tree, result, new_byte, leaf);
            return result;
        }

        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    compact_ref_t child = find_child(tree, ref, byte);

    if (child != COMPACT_REF_NULL) {
        if (depth >= tree->key_size) {
            if (COMPACT_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
                leaf_set_value(tree, child, value);
                *inserted = false;
            }
            return ref;
        }
        compact_ref_t new_child = insert_recursive(tree, child, key, depth + 1,
                                                    value, inserted);
        if (new_child != child) {
            replace_child(tree, ref, byte, new_child);
        }
    } else {
        *inserted = true;
        compact_ref_t leaf = alloc_leaf(tree, key, value);
        if (leaf != COMPACT_REF_NULL) {
            ref = add_child(tree, ref, byte, leaf);
        }
    }

    return ref;
}

// ============================================================================
// delete_recursive
// ============================================================================

static compact_ref_t delete_recursive(compact_art_t *tree, compact_ref_t ref,
                                       const uint8_t *key, size_t depth,
                                       bool *deleted) {
    if (ref == COMPACT_REF_NULL) return COMPACT_REF_NULL;

    if (COMPACT_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            *deleted = true;
            return COMPACT_REF_NULL;
        }
        return ref;
    }

    void *node = node_ptr(tree, ref);

    // Check compressed path
    uint8_t plen = node_partial_len(node);
    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);
        if (prefix_len != plen) return ref;
        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    compact_ref_t child = find_child(tree, ref, byte);
    if (child == COMPACT_REF_NULL) return ref;

    if (depth >= tree->key_size) {
        if (COMPACT_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
            *deleted = true;
            ref = remove_child(tree, ref, byte);
            // Re-resolve after remove_child (may have returned different ref)
            node = node_ptr(tree, ref);
        }
        // Fall through to path collapse check if we deleted
        if (!*deleted) return ref;
    } else {
        compact_ref_t new_child = delete_recursive(tree, child, key,
                                                    depth + 1, deleted);

        if (new_child != child) {
            if (new_child == COMPACT_REF_NULL) {
                ref = remove_child(tree, ref, byte);
            } else {
                replace_child(tree, ref, byte, new_child);
            }
        }
    }

    // Path collapse: if Node4 has exactly 1 child, merge with child
    if (!COMPACT_IS_LEAF_REF(ref) && ref != COMPACT_REF_NULL) {
        node = node_ptr(tree, ref);
        if (node_type(node) == COMPACT_NODE_4 &&
            node_num_children(node) == 1) {
            compact_node4_t *n4 = node;
            compact_ref_t only_child = n4->children[0];

            if (COMPACT_IS_LEAF_REF(only_child)) {
                return only_child;
            }

            // Merge prefixes: parent_partial + byte + child_partial
            // Use leaf key to reconstruct full prefix safely
            void *child_node = node_ptr(tree, only_child);
            uint8_t child_plen = node_partial_len(child_node);
            uint32_t new_len = (uint32_t)n4->partial_len + 1 + child_plen;
            if (new_len <= 255) {
                compact_ref_t min_leaf = find_minimum_leaf(tree, only_child);
                const uint8_t *lk = leaf_key(tree, min_leaf);
                uint8_t *child_partial = node_partial_mut(child_node);
                size_t store = (size_t)new_len < COMPACT_MAX_PREFIX
                               ? (size_t)new_len : COMPACT_MAX_PREFIX;
                // depth is where the parent node sits; merged prefix
                // covers key bytes [depth .. depth+new_len-1]
                memcpy(child_partial, lk + depth, store);
                ((uint8_t *)child_node)[2] = (uint8_t)new_len;
                return only_child;
            }
        }
    }

    return ref;
}

// ============================================================================
// Public API
// ============================================================================

bool compact_art_init(compact_art_t *tree, uint32_t key_size,
                      uint32_t value_size) {
    if (!tree || key_size == 0) return false;

    tree->root = COMPACT_REF_NULL;
    tree->size = 0;
    tree->key_size = key_size;
    tree->value_size = value_size;
    tree->leaf_size = key_size + value_size;
    tree->leaf_count = 0;

    if (!pool_init(&tree->nodes, COMPACT_NODE_POOL_RESERVE)) return false;
    // Skip offset 0 so ref=0 means NULL
    tree->nodes.used = 8;

    // Leaf pool: reserve enough for ~1.8B leaves
    size_t leaf_reserve = (size_t)tree->leaf_size * (1UL << 31);
    if (leaf_reserve > COMPACT_LEAF_POOL_RESERVE)
        leaf_reserve = COMPACT_LEAF_POOL_RESERVE;

    if (!pool_init(&tree->leaves, leaf_reserve)) {
        pool_destroy(&tree->nodes);
        return false;
    }

    return true;
}

void compact_art_destroy(compact_art_t *tree) {
    if (!tree) return;
    pool_destroy(&tree->nodes);
    pool_destroy(&tree->leaves);
    tree->root = COMPACT_REF_NULL;
    tree->size = 0;
    tree->leaf_count = 0;
}

bool compact_art_insert(compact_art_t *tree, const uint8_t *key,
                        const void *value) {
    if (!tree || !key) return false;

    bool inserted = false;
    tree->root = insert_recursive(tree, tree->root, key, 0, value, &inserted);

    if (inserted) tree->size++;
    return tree->root != COMPACT_REF_NULL;
}

const void *compact_art_get(const compact_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return NULL;
    return search(tree, tree->root, key, 0);
}

bool compact_art_contains(const compact_art_t *tree, const uint8_t *key) {
    return compact_art_get(tree, key) != NULL;
}

bool compact_art_delete(compact_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return false;

    bool deleted = false;
    tree->root = delete_recursive(tree, tree->root, key, 0, &deleted);

    if (deleted) tree->size--;
    return deleted;
}

size_t compact_art_size(const compact_art_t *tree) {
    return tree ? tree->size : 0;
}

bool compact_art_is_empty(const compact_art_t *tree) {
    return tree ? (tree->size == 0) : true;
}

// ============================================================================
// Iterator
// ============================================================================

typedef struct {
    const uint8_t *key;
    const void *value;
    bool done;
    bool started;
    struct {
        compact_ref_t ref;
        int child_idx;
    } stack[64];
    int depth;
} compact_iter_state_t;

struct compact_art_iterator {
    const compact_art_t *tree;
    compact_iter_state_t state;
};

compact_art_iterator_t *compact_art_iterator_create(const compact_art_t *tree) {
    if (!tree) return NULL;

    compact_art_iterator_t *iter = malloc(sizeof(compact_art_iterator_t));
    if (!iter) return NULL;

    iter->tree = tree;
    memset(&iter->state, 0, sizeof(compact_iter_state_t));
    iter->state.done = (tree->root == COMPACT_REF_NULL);
    iter->state.depth = -1;

    return iter;
}

bool compact_art_iterator_next(compact_art_iterator_t *iter) {
    if (!iter) return false;

    compact_iter_state_t *s = &iter->state;
    if (s->done) return false;

    // First call — push root
    if (!s->started) {
        s->started = true;
        if (iter->tree->root == COMPACT_REF_NULL) {
            s->done = true;
            return false;
        }
        s->depth = 0;
        s->stack[0].ref = iter->tree->root;
        s->stack[0].child_idx = -1;
    }

    while (s->depth >= 0) {
        compact_ref_t ref = s->stack[s->depth].ref;

        if (COMPACT_IS_LEAF_REF(ref)) {
            s->key = leaf_key(iter->tree, ref);
            s->value = leaf_value(iter->tree, ref);
            s->depth--;
            return true;
        }

        int *ci = &s->stack[s->depth].child_idx;
        (*ci)++;

        void *node = node_ptr(iter->tree, ref);
        compact_ref_t next_child = COMPACT_REF_NULL;

        switch (node_type(node)) {
            case COMPACT_NODE_4: {
                compact_node4_t *n = node;
                if (*ci < n->num_children)
                    next_child = n->children[*ci];
                break;
            }
            case COMPACT_NODE_16: {
                compact_node16_t *n = node;
                if (*ci < n->num_children)
                    next_child = n->children[*ci];
                break;
            }
            case COMPACT_NODE_32: {
                compact_node32_t *n = node;
                if (*ci < n->num_children)
                    next_child = n->children[*ci];
                break;
            }
            case COMPACT_NODE_48: {
                compact_node48_t *n = node;
                while (*ci < 256) {
                    uint8_t idx = n->index[*ci];
                    if (idx != COMPACT_NODE48_EMPTY) {
                        next_child = n->children[idx];
                        break;
                    }
                    (*ci)++;
                }
                break;
            }
            case COMPACT_NODE_256: {
                compact_node256_t *n = node;
                while (*ci < 256) {
                    if (n->children[*ci] != COMPACT_REF_NULL) {
                        next_child = n->children[*ci];
                        break;
                    }
                    (*ci)++;
                }
                break;
            }
            default:
                break;
        }

        if (next_child != COMPACT_REF_NULL) {
            s->depth++;
            if (s->depth >= 64) {
                s->done = true;
                return false;
            }
            s->stack[s->depth].ref = next_child;
            s->stack[s->depth].child_idx = -1;
        } else {
            s->depth--;
        }
    }

    s->done = true;
    return false;
}

const uint8_t *compact_art_iterator_key(const compact_art_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.key;
}

const void *compact_art_iterator_value(const compact_art_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.value;
}

bool compact_art_iterator_done(const compact_art_iterator_t *iter) {
    if (!iter) return true;
    return iter->state.done;
}

void compact_art_iterator_destroy(compact_art_iterator_t *iter) {
    free(iter);
}
