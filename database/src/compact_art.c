#include "compact_art.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <emmintrin.h>  // SSE2

/**
 * Compact ART Implementation
 *
 * Space-efficient in-memory ART for fixed-size keys.
 *
 * Key differences from mem_art:
 * - Arena allocator (no malloc overhead per node/leaf)
 * - Tagged pointers (LSB=1 marks leaf, no type field in leaf)
 * - No per-leaf key_len/value_len (fixed at tree level)
 * - Packed inner node structs (uint8_t header fields)
 * - New Node32 type (2x SSE lookup, fills Node16–Node48 gap)
 */

// ============================================================================
// Arena Allocator
// ============================================================================

static bool arena_init(compact_arena_t *arena, size_t slab_size) {
    arena->slab_size = slab_size;
    arena->num_slabs = 0;
    arena->slab_cap = 16;
    arena->slabs = malloc(arena->slab_cap * sizeof(compact_arena_slab_t));
    if (!arena->slabs) return false;
    return true;
}

static void arena_destroy(compact_arena_t *arena) {
    for (size_t i = 0; i < arena->num_slabs; i++) {
        munmap(arena->slabs[i].base, arena->slabs[i].capacity);
    }
    free(arena->slabs);
    arena->slabs = NULL;
    arena->num_slabs = 0;
}

static bool arena_grow(compact_arena_t *arena) {
    if (arena->num_slabs >= arena->slab_cap) {
        size_t new_cap = arena->slab_cap * 2;
        compact_arena_slab_t *new_slabs = realloc(arena->slabs,
            new_cap * sizeof(compact_arena_slab_t));
        if (!new_slabs) return false;
        arena->slabs = new_slabs;
        arena->slab_cap = new_cap;
    }

    void *mem = mmap(NULL, arena->slab_size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) return false;

    compact_arena_slab_t *slab = &arena->slabs[arena->num_slabs];
    slab->base = mem;
    slab->capacity = arena->slab_size;
    slab->used = 0;
    arena->num_slabs++;
    return true;
}

// Bump-allocate from arena.
// align: required alignment (must be power of 2). Use 8 for inner nodes, 4 for leaves.
static void *arena_alloc(compact_arena_t *arena, size_t size, size_t align) {
    // Try current slab
    if (arena->num_slabs > 0) {
        compact_arena_slab_t *slab = &arena->slabs[arena->num_slabs - 1];
        size_t aligned = (slab->used + align - 1) & ~(align - 1);
        if (aligned + size <= slab->capacity) {
            slab->used = aligned + size;
            return slab->base + aligned;
        }
    }

    // Need new slab
    size_t needed = size > arena->slab_size ? size : arena->slab_size;
    size_t saved = arena->slab_size;
    arena->slab_size = needed;
    if (!arena_grow(arena)) {
        arena->slab_size = saved;
        return NULL;
    }
    arena->slab_size = saved;

    compact_arena_slab_t *slab = &arena->slabs[arena->num_slabs - 1];
    size_t aligned = (slab->used + align - 1) & ~(align - 1);
    slab->used = aligned + size;
    return slab->base + aligned;
}

// ============================================================================
// Leaf Helpers
// ============================================================================

// Leaf is raw bytes: key[key_size] + value[value_size], arena-allocated.
// Pointer is tagged with LSB=1.

static inline void *alloc_leaf(compact_art_t *tree, const uint8_t *key,
                               const void *value) {
    size_t leaf_size = tree->key_size + tree->value_size;
    // 4-byte align: leaves are raw bytes accessed via memcpy, no pointer members.
    // Tagged pointer uses LSB, so 2-byte align minimum. Use 4 to save vs 8-byte.
    uint8_t *leaf = arena_alloc(&tree->arena, leaf_size, 4);
    if (!leaf) return NULL;

    memcpy(leaf, key, tree->key_size);
    memcpy(leaf + tree->key_size, value, tree->value_size);

    return COMPACT_MAKE_LEAF(leaf);
}

static inline const uint8_t *leaf_key(const void *tagged_ptr) {
    return (const uint8_t *)COMPACT_GET_LEAF(tagged_ptr);
}

static inline const void *leaf_value(const compact_art_t *tree,
                                     const void *tagged_ptr) {
    return (const uint8_t *)COMPACT_GET_LEAF(tagged_ptr) + tree->key_size;
}

static inline bool leaf_matches(const compact_art_t *tree,
                                const void *tagged_ptr,
                                const uint8_t *key) {
    return memcmp(leaf_key(tagged_ptr), key, tree->key_size) == 0;
}

// Update leaf value in-place (arena memory is mutable)
static inline void leaf_set_value(const compact_art_t *tree,
                                  void *tagged_ptr,
                                  const void *value) {
    uint8_t *leaf = (uint8_t *)COMPACT_GET_LEAF(tagged_ptr);
    memcpy(leaf + tree->key_size, value, tree->value_size);
}

// ============================================================================
// Inner Node Allocation
// ============================================================================

static void *alloc_node(compact_art_t *tree, compact_node_type_t type) {
    size_t size;
    switch (type) {
        case COMPACT_NODE_4:   size = sizeof(compact_node4_t);   break;
        case COMPACT_NODE_16:  size = sizeof(compact_node16_t);  break;
        case COMPACT_NODE_32:  size = sizeof(compact_node32_t);  break;
        case COMPACT_NODE_48:  size = sizeof(compact_node48_t);  break;
        case COMPACT_NODE_256: size = sizeof(compact_node256_t); break;
        default: return NULL;
    }

    // 8-byte align: inner nodes contain void* children that need pointer alignment
    void *node = arena_alloc(&tree->arena, size, 8);
    if (!node) return NULL;

    // Zero the entire node
    memset(node, 0, size);
    ((uint8_t *)node)[0] = (uint8_t)type;  // Set type

    // Node48: initialize index to EMPTY
    if (type == COMPACT_NODE_48) {
        compact_node48_t *n48 = node;
        memset(n48->index, COMPACT_NODE48_EMPTY, 256);
    }

    return node;
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
    // partial[] is at the end of each struct, offset depends on type
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
// check_prefix — compare compressed path against key
// ============================================================================

static int check_prefix(const void *node, const uint8_t *key,
                        uint32_t key_size, size_t depth) {
    uint8_t plen = node_partial_len(node);
    const uint8_t *partial = node_partial(node);
    int max_cmp = plen;
    if (depth + max_cmp > key_size) {
        max_cmp = (int)key_size - (int)depth;
    }

    if (memcmp(partial, key + depth, max_cmp) == 0) {
        return max_cmp;
    }

    for (int idx = 0; idx < max_cmp; idx++) {
        if (partial[idx] != key[depth + idx]) return idx;
    }
    return max_cmp;
}

// ============================================================================
// find_child — find child pointer for a given byte
// ============================================================================

static void **find_child_ptr(void *node, uint8_t byte) {
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
            // Two SSE compares over sorted 32-byte key array
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
            if (!n->children[byte]) return NULL;
            return &n->children[byte];
        }
        default:
            return NULL;
    }
}

static void *find_child(void *node, uint8_t byte) {
    void **ptr = find_child_ptr(node, byte);
    return ptr ? *ptr : NULL;
}

// ============================================================================
// replace_child — replace child pointer in-place
// ============================================================================

static void replace_child(void *node, uint8_t byte, void *new_child) {
    void **ptr = find_child_ptr(node, byte);
    if (ptr) *ptr = new_child;
}

// ============================================================================
// add_child — add a new child, growing node if full
// ============================================================================

static void *add_child(compact_art_t *tree, void *node, uint8_t byte,
                        void *child);

// Copy header (partial_len + partial) from old node to new node
static void copy_header(void *dst, const void *src) {
    ((uint8_t *)dst)[2] = ((const uint8_t *)src)[2];  // partial_len
    memcpy(node_partial_mut(dst), node_partial(src),
           node_partial_len(src));
}

static void *add_child(compact_art_t *tree, void *node, uint8_t byte,
                        void *child) {
    switch (node_type(node)) {
        case COMPACT_NODE_4: {
            compact_node4_t *n = node;
            if (n->num_children < COMPACT_NODE4_MAX) {
                // Insert sorted
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i],
                            n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(void *));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node;
            }
            // Grow to Node16
            compact_node16_t *n16 = alloc_node(tree, COMPACT_NODE_16);
            if (!n16) return node;
            copy_header(n16, n);
            memcpy(n16->keys, n->keys, COMPACT_NODE4_MAX);
            memcpy(n16->children, n->children,
                   COMPACT_NODE4_MAX * sizeof(void *));
            n16->num_children = COMPACT_NODE4_MAX;
            // Arena: old node is abandoned (not freed)
            return add_child(tree, n16, byte, child);
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
                            (n->num_children - i) * sizeof(void *));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node;
            }
            // Grow to Node32
            compact_node32_t *n32 = alloc_node(tree, COMPACT_NODE_32);
            if (!n32) return node;
            copy_header(n32, n);
            memcpy(n32->keys, n->keys, COMPACT_NODE16_MAX);
            memcpy(n32->children, n->children,
                   COMPACT_NODE16_MAX * sizeof(void *));
            n32->num_children = COMPACT_NODE16_MAX;
            return add_child(tree, n32, byte, child);
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
                            (n->num_children - i) * sizeof(void *));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node;
            }
            // Grow to Node48
            compact_node48_t *n48 = alloc_node(tree, COMPACT_NODE_48);
            if (!n48) return node;
            copy_header(n48, n);
            for (int i = 0; i < COMPACT_NODE32_MAX; i++) {
                n48->index[n->keys[i]] = i;
                n48->children[i] = n->children[i];
            }
            n48->num_children = COMPACT_NODE32_MAX;
            return add_child(tree, n48, byte, child);
        }

        case COMPACT_NODE_48: {
            compact_node48_t *n = node;
            if (n->num_children < COMPACT_NODE48_MAX) {
                // Find free slot
                int slot;
                for (slot = 0; slot < COMPACT_NODE48_MAX; slot++) {
                    if (!n->children[slot]) break;
                }
                n->index[byte] = (uint8_t)slot;
                n->children[slot] = child;
                n->num_children++;
                return node;
            }
            // Grow to Node256
            compact_node256_t *n256 = alloc_node(tree, COMPACT_NODE_256);
            if (!n256) return node;
            copy_header(n256, n);
            for (int i = 0; i < 256; i++) {
                if (n->index[i] != COMPACT_NODE48_EMPTY) {
                    n256->children[i] = n->children[n->index[i]];
                }
            }
            n256->num_children = n->num_children;
            return add_child(tree, n256, byte, child);
        }

        case COMPACT_NODE_256: {
            compact_node256_t *n = node;
            if (!n->children[byte]) {
                n->num_children++;
            }
            n->children[byte] = child;
            return node;
        }

        default:
            return node;
    }
}

// ============================================================================
// remove_child — remove a child, shrinking node if needed
// ============================================================================

static void *remove_child(compact_art_t *tree, void *node, uint8_t byte) {
    switch (node_type(node)) {
        case COMPACT_NODE_4: {
            compact_node4_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(void *));
                    n->num_children--;
                    return node;
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
                            (n->num_children - i - 1) * sizeof(void *));
                    n->num_children--;
                    // Shrink Node16 → Node4
                    if (n->num_children <= COMPACT_NODE4_MAX) {
                        compact_node4_t *n4 = alloc_node(tree, COMPACT_NODE_4);
                        if (n4) {
                            copy_header(n4, n);
                            n4->num_children = n->num_children;
                            memcpy(n4->keys, n->keys, n->num_children);
                            memcpy(n4->children, n->children,
                                   n->num_children * sizeof(void *));
                            return n4;
                        }
                    }
                    return node;
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
                            (n->num_children - i - 1) * sizeof(void *));
                    n->num_children--;
                    // Shrink Node32 → Node16
                    if (n->num_children <= COMPACT_NODE16_MAX) {
                        compact_node16_t *n16 = alloc_node(tree, COMPACT_NODE_16);
                        if (n16) {
                            copy_header(n16, n);
                            n16->num_children = n->num_children;
                            memcpy(n16->keys, n->keys, n->num_children);
                            memcpy(n16->children, n->children,
                                   n->num_children * sizeof(void *));
                            return n16;
                        }
                    }
                    return node;
                }
            }
            break;
        }
        case COMPACT_NODE_48: {
            compact_node48_t *n = node;
            uint8_t idx = n->index[byte];
            if (idx != COMPACT_NODE48_EMPTY) {
                n->children[idx] = NULL;
                n->index[byte] = COMPACT_NODE48_EMPTY;
                n->num_children--;
                // Shrink Node48 → Node32
                if (n->num_children <= COMPACT_NODE32_MAX) {
                    compact_node32_t *n32 = alloc_node(tree, COMPACT_NODE_32);
                    if (n32) {
                        copy_header(n32, n);
                        n32->num_children = 0;
                        for (int i = 0; i < 256; i++) {
                            if (n->index[i] != COMPACT_NODE48_EMPTY) {
                                // Insert sorted
                                int pos = n32->num_children;
                                n32->keys[pos] = (uint8_t)i;
                                n32->children[pos] = n->children[n->index[i]];
                                n32->num_children++;
                            }
                        }
                        return n32;
                    }
                }
                return node;
            }
            break;
        }
        case COMPACT_NODE_256: {
            compact_node256_t *n = node;
            n->children[byte] = NULL;
            n->num_children--;
            // Shrink Node256 → Node48
            if (n->num_children <= COMPACT_NODE48_MAX) {
                compact_node48_t *n48 = alloc_node(tree, COMPACT_NODE_48);
                if (n48) {
                    copy_header(n48, n);
                    n48->num_children = 0;
                    for (int i = 0; i < 256; i++) {
                        if (n->children[i]) {
                            n48->index[i] = n48->num_children;
                            n48->children[n48->num_children] = n->children[i];
                            n48->num_children++;
                        }
                    }
                    return n48;
                }
            }
            return node;
        }
        default:
            break;
    }
    return node;
}

// ============================================================================
// search — iterative lookup
// ============================================================================

static const void *search(const compact_art_t *tree, const void *node,
                          const uint8_t *key, size_t depth) {
    while (node) {
        if (COMPACT_IS_LEAF(node)) {
            if (leaf_matches(tree, node, key)) {
                return leaf_value(tree, node);
            }
            return NULL;
        }

        // Check compressed path
        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            int prefix_len = check_prefix(node, key, tree->key_size, depth);
            if (prefix_len != plen) return NULL;
            depth += plen;
        }

        uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
        void *child = find_child((void *)node, byte);
        if (!child) return NULL;

        if (depth >= tree->key_size) {
            if (COMPACT_IS_LEAF(child) && leaf_matches(tree, child, key)) {
                return leaf_value(tree, child);
            }
            return NULL;
        }

        node = child;
        depth++;
    }
    return NULL;
}

// ============================================================================
// insert_recursive
// ============================================================================

static void *insert_recursive(compact_art_t *tree, void *node,
                               const uint8_t *key, size_t depth,
                               const void *value, bool *inserted) {
    // Base case: empty slot
    if (!node) {
        *inserted = true;
        return alloc_leaf(tree, key, value);
    }

    // Leaf: match or split
    if (COMPACT_IS_LEAF(node)) {
        if (leaf_matches(tree, node, key)) {
            // Update value in-place
            leaf_set_value(tree, node, value);
            *inserted = false;
            return node;
        }

        // Keys differ — create new inner node
        *inserted = true;
        void *new_leaf = alloc_leaf(tree, key, value);
        if (!new_leaf) return node;

        const uint8_t *existing_key = leaf_key(node);

        // Find common prefix from current depth
        size_t limit = tree->key_size;
        size_t prefix_len = 0;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == existing_key[depth + prefix_len]) {
            prefix_len++;
        }

        compact_node4_t *new_node = alloc_node(tree, COMPACT_NODE_4);
        if (!new_node) return node;

        new_node->partial_len = (uint8_t)prefix_len;
        if (prefix_len > 0) {
            memcpy(new_node->partial, key + depth, prefix_len);
        }

        size_t new_depth = depth + prefix_len;
        uint8_t new_byte = (new_depth < tree->key_size) ? key[new_depth] : 0x00;
        uint8_t old_byte = (new_depth < tree->key_size) ?
                            existing_key[new_depth] : 0x00;

        void *result = add_child(tree, new_node, new_byte, new_leaf);
        result = add_child(tree, result, old_byte, node);
        return result;
    }

    // Inner node: check compressed path
    uint8_t plen = node_partial_len(node);
    if (plen > 0) {
        int prefix_len = check_prefix(node, key, tree->key_size, depth);

        if (prefix_len < plen) {
            // Prefix mismatch — split
            *inserted = true;

            uint8_t *partial = node_partial_mut(node);
            uint8_t old_byte = partial[prefix_len];

            compact_node4_t *new_node = alloc_node(tree, COMPACT_NODE_4);
            if (!new_node) return node;

            new_node->partial_len = (uint8_t)prefix_len;
            memcpy(new_node->partial, partial, prefix_len);

            // Shift old node's prefix
            uint8_t new_partial_len = plen - (prefix_len + 1);
            memmove(partial, partial + prefix_len + 1, new_partial_len);
            ((uint8_t *)node)[2] = new_partial_len;  // Set partial_len

            void *leaf = alloc_leaf(tree, key, value);
            if (!leaf) return node;

            uint8_t new_byte = (depth + prefix_len < tree->key_size) ?
                                key[depth + prefix_len] : 0x00;

            void *result = add_child(tree, new_node, old_byte, node);
            result = add_child(tree, result, new_byte, leaf);
            return result;
        }

        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    void *child = find_child(node, byte);

    if (child) {
        if (depth >= tree->key_size) {
            // Key consumed, child should be matching leaf
            if (COMPACT_IS_LEAF(child) && leaf_matches(tree, child, key)) {
                leaf_set_value(tree, child, value);
                *inserted = false;
            }
            return node;
        }
        void *new_child = insert_recursive(tree, child, key, depth + 1,
                                            value, inserted);
        if (new_child != child) {
            replace_child(node, byte, new_child);
        }
    } else {
        *inserted = true;
        void *leaf = alloc_leaf(tree, key, value);
        if (leaf) {
            node = add_child(tree, node, byte, leaf);
        }
    }

    return node;
}

// ============================================================================
// delete_recursive
// ============================================================================

static void *delete_recursive(compact_art_t *tree, void *node,
                               const uint8_t *key, size_t depth,
                               bool *deleted) {
    if (!node) return NULL;

    if (COMPACT_IS_LEAF(node)) {
        if (leaf_matches(tree, node, key)) {
            *deleted = true;
            // Arena: leaf memory is not reclaimed
            return NULL;
        }
        return node;
    }

    // Check compressed path
    uint8_t plen = node_partial_len(node);
    if (plen > 0) {
        int prefix_len = check_prefix(node, key, tree->key_size, depth);
        if (prefix_len != plen) return node;
        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    void *child = find_child(node, byte);
    if (!child) return node;

    if (depth >= tree->key_size) {
        if (COMPACT_IS_LEAF(child) && leaf_matches(tree, child, key)) {
            *deleted = true;
            node = remove_child(tree, node, byte);
            return node;
        }
        return node;
    }

    void *new_child = delete_recursive(tree, child, key, depth + 1, deleted);

    if (new_child != child) {
        if (!new_child) {
            node = remove_child(tree, node, byte);
        } else {
            replace_child(node, byte, new_child);
        }
    }

    // Path collapse: if Node4 has exactly 1 child, merge with child
    if (!COMPACT_IS_LEAF(node) && node_type(node) == COMPACT_NODE_4 &&
        node_num_children(node) == 1) {
        compact_node4_t *n4 = node;
        void *only_child = n4->children[0];
        uint8_t only_byte = n4->keys[0];

        if (COMPACT_IS_LEAF(only_child)) {
            return only_child;
        }

        // Merge prefixes: node->partial + byte + child->partial
        uint8_t child_plen = node_partial_len(only_child);
        uint32_t new_len = n4->partial_len + 1 + child_plen;
        if (new_len <= COMPACT_MAX_PREFIX) {
            uint8_t *child_partial = node_partial_mut(only_child);
            memmove(child_partial + n4->partial_len + 1,
                    child_partial, child_plen);
            memcpy(child_partial, n4->partial, n4->partial_len);
            child_partial[n4->partial_len] = only_byte;
            ((uint8_t *)only_child)[2] = (uint8_t)new_len;
            return only_child;
        }
    }

    return node;
}

// ============================================================================
// Public API
// ============================================================================

bool compact_art_init(compact_art_t *tree, uint32_t key_size,
                      uint32_t value_size) {
    if (!tree || key_size == 0) return false;

    tree->root = NULL;
    tree->size = 0;
    tree->key_size = key_size;
    tree->value_size = value_size;

    return arena_init(&tree->arena, COMPACT_ARENA_SLAB_SIZE);
}

void compact_art_destroy(compact_art_t *tree) {
    if (!tree) return;
    arena_destroy(&tree->arena);
    tree->root = NULL;
    tree->size = 0;
}

bool compact_art_insert(compact_art_t *tree, const uint8_t *key,
                        const void *value) {
    if (!tree || !key) return false;

    bool inserted = false;
    tree->root = insert_recursive(tree, tree->root, key, 0, value, &inserted);

    if (inserted) tree->size++;
    return tree->root != NULL;
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
        void *node;
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
    iter->state.done = (tree->root == NULL);
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
        if (!iter->tree->root) {
            s->done = true;
            return false;
        }
        s->depth = 0;
        s->stack[0].node = iter->tree->root;
        s->stack[0].child_idx = -1;
    }

    while (s->depth >= 0) {
        void *node = s->stack[s->depth].node;

        if (COMPACT_IS_LEAF(node)) {
            s->key = leaf_key(node);
            s->value = leaf_value(iter->tree, node);
            s->depth--;
            return true;
        }

        int *ci = &s->stack[s->depth].child_idx;
        (*ci)++;

        void *next_child = NULL;

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
                    next_child = n->children[*ci];
                    if (next_child) break;
                    (*ci)++;
                }
                break;
            }
            default:
                break;
        }

        if (next_child) {
            s->depth++;
            if (s->depth >= 64) {
                s->done = true;
                return false;
            }
            s->stack[s->depth].node = next_child;
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
