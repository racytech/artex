#include "mem_art.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <emmintrin.h>  // SSE2

/**
 * In-Memory ART — Arena-Allocated Implementation
 *
 * Bump arena allocator: all nodes and leaves allocated from a single
 * contiguous buffer. No per-node malloc overhead. O(1) destroy.
 *
 * Node layout: 3-byte header, 4-byte child refs,
 * 8-byte inline prefix, Node32 with 2×SSE lookup.
 *
 * Dead space (from node growth or value updates) is not reclaimed —
 * the arena resets entirely on mem_art_destroy.
 */

// ============================================================================
// Constants
// ============================================================================

#define MEM_ARENA_INITIAL_CAP (64 * 1024)  // 64 KB

#define MEM_NODE4_MAX   4
#define MEM_NODE16_MAX  16
#define MEM_NODE32_MAX  32
#define MEM_NODE48_MAX  48
#define MEM_NODE256_MAX 256

#define MEM_MAX_PREFIX  8
#define MEM_NODE48_EMPTY 255

// ============================================================================
// Node Types
// ============================================================================

typedef enum {
    MEM_NODE_4   = 0,
    MEM_NODE_16  = 1,
    MEM_NODE_32  = 2,
    MEM_NODE_48  = 3,
    MEM_NODE_256 = 4,
} mem_node_type_t;

// ============================================================================
// Compact Node Structures (3-byte header + data + 8-byte partial)
// ============================================================================

typedef struct {
    uint8_t type;           // mem_node_type_t
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[4];
    uint8_t _pad[1];
    mem_ref_t children[4];
    uint8_t partial[MEM_MAX_PREFIX];
} mem_node4_t;  // 32 bytes

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[16];
    uint8_t _pad[1];
    mem_ref_t children[16];
    uint8_t partial[MEM_MAX_PREFIX];
} mem_node16_t;  // 92 bytes

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[32];
    uint8_t _pad[1];
    mem_ref_t children[32];
    uint8_t partial[MEM_MAX_PREFIX];
} mem_node32_t;  // 172 bytes

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t index[256];
    uint8_t _pad[1];
    mem_ref_t children[48];
    uint8_t partial[MEM_MAX_PREFIX];
} mem_node48_t;  // 460 bytes

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t _pad[1];
    mem_ref_t children[256];
    uint8_t partial[MEM_MAX_PREFIX];
} mem_node256_t;  // 1036 bytes

// ============================================================================
// Leaf Structure (variable-length, in same arena)
// ============================================================================

typedef struct {
    uint16_t key_len;
    uint16_t value_len;
    uint8_t data[];  // key bytes then value bytes
} mem_leaf_t;

// ============================================================================
// Arena Allocator
// ============================================================================

static mem_ref_t arena_alloc(mem_art_t *tree, size_t bytes, bool is_leaf) {
    // 16-byte alignment (required for struct values with __uint128_t fields)
    size_t aligned = (tree->arena_used + 15) & ~(size_t)15;
    if (aligned + bytes > tree->arena_cap) {
        size_t new_cap = tree->arena_cap * 2;
        while (aligned + bytes > new_cap) new_cap *= 2;
        uint8_t *new_arena = realloc(tree->arena, new_cap);
        if (!new_arena) return MEM_REF_NULL;
        tree->arena = new_arena;
        tree->arena_cap = new_cap;
    }
    tree->arena_used = aligned + bytes;
    memset(tree->arena + aligned, 0, bytes);
    mem_ref_t ref = (mem_ref_t)aligned;
    if (is_leaf) ref |= MEM_REF_LEAF_BIT;
    return ref;
}

// ============================================================================
// Ref Resolution
// ============================================================================

static inline void *ref_ptr(const mem_art_t *tree, mem_ref_t ref) {
    return tree->arena + (ref & 0x7FFFFFFFu);
}

// ============================================================================
// Node Accessors
// ============================================================================

static inline mem_node_type_t node_type(const void *node) {
    return (mem_node_type_t)(((const uint8_t *)node)[0]);
}

static inline uint8_t node_num_children(const void *node) {
    return ((const uint8_t *)node)[1];
}

static inline uint8_t node_partial_len(const void *node) {
    return ((const uint8_t *)node)[2];
}

static inline void set_num_children(void *node, uint8_t n) {
    ((uint8_t *)node)[1] = n;
}

static inline void set_partial_len(void *node, uint8_t n) {
    ((uint8_t *)node)[2] = n;
}

static inline uint8_t *node_partial(void *node) {
    switch (node_type(node)) {
        case MEM_NODE_4:   return ((mem_node4_t *)node)->partial;
        case MEM_NODE_16:  return ((mem_node16_t *)node)->partial;
        case MEM_NODE_32:  return ((mem_node32_t *)node)->partial;
        case MEM_NODE_48:  return ((mem_node48_t *)node)->partial;
        case MEM_NODE_256: return ((mem_node256_t *)node)->partial;
        default: return NULL;
    }
}

// ============================================================================
// Leaf Helpers
// ============================================================================

static inline size_t leaf_value_offset(size_t key_len, size_t value_len) {
    if (value_len == 0) return sizeof(mem_leaf_t) + key_len;
    // Align value start to 16 bytes for struct values with __uint128_t
    size_t hdr_key = sizeof(mem_leaf_t) + key_len;
    return (hdr_key + 15) & ~(size_t)15;
}

static mem_ref_t alloc_leaf(mem_art_t *tree, const uint8_t *key, size_t key_len,
                            const void *value, size_t value_len) {
    size_t val_off = leaf_value_offset(key_len, value_len);
    size_t total = val_off + value_len;
    mem_ref_t ref = arena_alloc(tree, total, true);
    if (ref == MEM_REF_NULL) return MEM_REF_NULL;

    mem_leaf_t *leaf = ref_ptr(tree, ref);
    leaf->key_len = (uint16_t)key_len;
    leaf->value_len = (uint16_t)value_len;
    memcpy(leaf->data, key, key_len);
    if (value_len > 0) {
        uint8_t *base = (uint8_t *)leaf;
        memcpy(base + val_off, value, value_len);
    }
    return ref;
}

static inline mem_leaf_t *leaf_ptr(const mem_art_t *tree, mem_ref_t ref) {
    return (mem_leaf_t *)ref_ptr(tree, ref);
}

static inline const uint8_t *leaf_key(const mem_art_t *tree, mem_ref_t ref) {
    return leaf_ptr(tree, ref)->data;
}

static inline const void *leaf_value(const mem_art_t *tree, mem_ref_t ref,
                                     size_t *value_len) {
    mem_leaf_t *leaf = leaf_ptr(tree, ref);
    if (value_len) *value_len = leaf->value_len;
    size_t val_off = leaf_value_offset(leaf->key_len, leaf->value_len);
    return (const uint8_t *)leaf + val_off;
}

static inline bool leaf_matches(const mem_art_t *tree, mem_ref_t ref,
                                const uint8_t *key, size_t key_len) {
    mem_leaf_t *leaf = leaf_ptr(tree, ref);
    if (leaf->key_len != key_len) return false;
    return memcmp(leaf->data, key, key_len) == 0;
}

// ============================================================================
// Node Allocation
// ============================================================================

static mem_ref_t alloc_node(mem_art_t *tree, mem_node_type_t type) {
    size_t size;
    switch (type) {
        case MEM_NODE_4:   size = sizeof(mem_node4_t);   break;
        case MEM_NODE_16:  size = sizeof(mem_node16_t);  break;
        case MEM_NODE_32:  size = sizeof(mem_node32_t);  break;
        case MEM_NODE_48:  size = sizeof(mem_node48_t);  break;
        case MEM_NODE_256: size = sizeof(mem_node256_t); break;
        default: return MEM_REF_NULL;
    }

    mem_ref_t ref = arena_alloc(tree, size, false);
    if (ref == MEM_REF_NULL) return MEM_REF_NULL;

    void *node = ref_ptr(tree, ref);
    ((uint8_t *)node)[0] = (uint8_t)type;

    if (type == MEM_NODE_48) {
        mem_node48_t *n48 = node;
        memset(n48->index, MEM_NODE48_EMPTY, 256);
    }

    return ref;
}

// ============================================================================
// find_minimum_leaf — leftmost leaf in subtree (for optimistic prefix check)
// ============================================================================

static mem_ref_t find_minimum_leaf(const mem_art_t *tree, mem_ref_t ref) {
    while (ref != MEM_REF_NULL && !MEM_IS_LEAF(ref)) {
        void *node = ref_ptr(tree, ref);
        switch (node_type(node)) {
            case MEM_NODE_4:
                ref = ((mem_node4_t *)node)->children[0];
                break;
            case MEM_NODE_16:
                ref = ((mem_node16_t *)node)->children[0];
                break;
            case MEM_NODE_32:
                ref = ((mem_node32_t *)node)->children[0];
                break;
            case MEM_NODE_48: {
                mem_node48_t *n = node;
                for (int i = 0; i < 256; i++) {
                    if (n->index[i] != MEM_NODE48_EMPTY) {
                        ref = n->children[n->index[i]];
                        goto next_iter;
                    }
                }
                return MEM_REF_NULL;
            }
            case MEM_NODE_256: {
                mem_node256_t *n = node;
                for (int i = 0; i < 256; i++) {
                    if (n->children[i] != MEM_REF_NULL) {
                        ref = n->children[i];
                        goto next_iter;
                    }
                }
                return MEM_REF_NULL;
            }
            default:
                return MEM_REF_NULL;
        }
        next_iter:;
    }
    return ref;
}

// ============================================================================
// check_prefix — optimistic prefix comparison
// ============================================================================

static int check_prefix(const mem_art_t *tree, mem_ref_t node_ref,
                        const void *node, const uint8_t *key,
                        size_t key_len, size_t depth) {
    uint8_t plen = node_partial_len(node);
    const uint8_t *partial = ((uint8_t *)node_partial((void *)node));
    int max_cmp = plen;
    if (depth + (size_t)max_cmp > key_len) {
        max_cmp = (int)key_len - (int)depth;
    }

    // Compare stored bytes (up to MEM_MAX_PREFIX)
    int stored = max_cmp < MEM_MAX_PREFIX ? max_cmp : MEM_MAX_PREFIX;
    for (int idx = 0; idx < stored; idx++) {
        if (partial[idx] != key[depth + idx]) return idx;
    }

    // If prefix extends beyond stored bytes, compare against leaf key
    if (max_cmp > MEM_MAX_PREFIX) {
        mem_ref_t min_leaf = find_minimum_leaf(tree, node_ref);
        if (min_leaf == MEM_REF_NULL) return MEM_MAX_PREFIX;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int idx = MEM_MAX_PREFIX; idx < max_cmp; idx++) {
            if (lk[depth + idx] != key[depth + idx]) return idx;
        }
    }

    return max_cmp;
}

// ============================================================================
// find_child — returns pointer to child slot in parent node
// ============================================================================

static mem_ref_t *find_child_ptr(const mem_art_t *tree, mem_ref_t node_ref,
                                 uint8_t byte) {
    void *node = ref_ptr(tree, node_ref);
    switch (node_type(node)) {
        case MEM_NODE_4: {
            mem_node4_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) return &n->children[i];
            }
            return NULL;
        }
        case MEM_NODE_16: {
            mem_node16_t *n = node;
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)n->keys));
            int mask = _mm_movemask_epi8(cmp) & ((1 << n->num_children) - 1);
            if (mask) return &n->children[__builtin_ctz(mask)];
            return NULL;
        }
        case MEM_NODE_32: {
            mem_node32_t *n = node;
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
        case MEM_NODE_48: {
            mem_node48_t *n = node;
            uint8_t idx = n->index[byte];
            if (idx == MEM_NODE48_EMPTY) return NULL;
            return &n->children[idx];
        }
        case MEM_NODE_256: {
            mem_node256_t *n = node;
            if (n->children[byte] == MEM_REF_NULL) return NULL;
            return &n->children[byte];
        }
        default:
            return NULL;
    }
}

// ============================================================================
// add_child — add a child to a node, growing if needed
// Returns new node ref (may differ if node grew)
// ============================================================================

static mem_ref_t add_child(mem_art_t *tree, mem_ref_t node_ref,
                           uint8_t byte, mem_ref_t child);

// Copy node header (partial_len + partial bytes) from src to dst
static void copy_header(void *dst, const void *src) {
    set_partial_len(dst, node_partial_len(src));
    uint8_t plen = node_partial_len(src);
    if (plen > 0) {
        uint8_t cpy = plen < MEM_MAX_PREFIX ? plen : MEM_MAX_PREFIX;
        memcpy(node_partial(dst), node_partial((void *)src), cpy);
    }
}

static mem_ref_t add_child(mem_art_t *tree, mem_ref_t node_ref,
                           uint8_t byte, mem_ref_t child) {
    void *node = ref_ptr(tree, node_ref);
    switch (node_type(node)) {
        case MEM_NODE_4: {
            mem_node4_t *n = node;
            if (n->num_children < MEM_NODE4_MAX) {
                // Find sorted insertion position
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i], n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(mem_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node16
            mem_ref_t new_ref = alloc_node(tree, MEM_NODE_16);
            if (new_ref == MEM_REF_NULL) return node_ref;
            // Re-resolve pointers after potential realloc
            n = ref_ptr(tree, node_ref);
            mem_node16_t *n16 = ref_ptr(tree, new_ref);
            copy_header(n16, n);
            memcpy(n16->keys, n->keys, MEM_NODE4_MAX);
            memcpy(n16->children, n->children, MEM_NODE4_MAX * sizeof(mem_ref_t));
            n16->num_children = MEM_NODE4_MAX;
            return add_child(tree, new_ref, byte, child);
        }

        case MEM_NODE_16: {
            mem_node16_t *n = node;
            if (n->num_children < MEM_NODE16_MAX) {
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i], n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(mem_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node32
            mem_ref_t new_ref = alloc_node(tree, MEM_NODE_32);
            if (new_ref == MEM_REF_NULL) return node_ref;
            n = ref_ptr(tree, node_ref);
            mem_node32_t *n32 = ref_ptr(tree, new_ref);
            copy_header(n32, n);
            memcpy(n32->keys, n->keys, MEM_NODE16_MAX);
            memcpy(n32->children, n->children, MEM_NODE16_MAX * sizeof(mem_ref_t));
            n32->num_children = MEM_NODE16_MAX;
            return add_child(tree, new_ref, byte, child);
        }

        case MEM_NODE_32: {
            mem_node32_t *n = node;
            if (n->num_children < MEM_NODE32_MAX) {
                int i;
                for (i = 0; i < n->num_children; i++) {
                    if (byte < n->keys[i]) break;
                }
                if (i < n->num_children) {
                    memmove(&n->keys[i + 1], &n->keys[i], n->num_children - i);
                    memmove(&n->children[i + 1], &n->children[i],
                            (n->num_children - i) * sizeof(mem_ref_t));
                }
                n->keys[i] = byte;
                n->children[i] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node48
            mem_ref_t new_ref = alloc_node(tree, MEM_NODE_48);
            if (new_ref == MEM_REF_NULL) return node_ref;
            n = ref_ptr(tree, node_ref);
            mem_node48_t *n48 = ref_ptr(tree, new_ref);
            copy_header(n48, n);
            for (int i = 0; i < MEM_NODE32_MAX; i++) {
                n48->index[n->keys[i]] = i;
                n48->children[i] = n->children[i];
            }
            n48->num_children = MEM_NODE32_MAX;
            return add_child(tree, new_ref, byte, child);
        }

        case MEM_NODE_48: {
            mem_node48_t *n = node;
            if (n->num_children < MEM_NODE48_MAX) {
                // Find free slot
                int slot;
                for (slot = 0; slot < MEM_NODE48_MAX; slot++) {
                    if (n->children[slot] == MEM_REF_NULL) break;
                }
                n->index[byte] = (uint8_t)slot;
                n->children[slot] = child;
                n->num_children++;
                return node_ref;
            }
            // Grow to Node256
            mem_ref_t new_ref = alloc_node(tree, MEM_NODE_256);
            if (new_ref == MEM_REF_NULL) return node_ref;
            n = ref_ptr(tree, node_ref);
            mem_node256_t *n256 = ref_ptr(tree, new_ref);
            copy_header(n256, n);
            for (int i = 0; i < 256; i++) {
                if (n->index[i] != MEM_NODE48_EMPTY) {
                    n256->children[i] = n->children[n->index[i]];
                }
            }
            n256->num_children = n->num_children;
            return add_child(tree, new_ref, byte, child);
        }

        case MEM_NODE_256: {
            mem_node256_t *n = node;
            if (n->children[byte] == MEM_REF_NULL) {
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
// Returns new node ref (may differ if node shrank)
// ============================================================================

static mem_ref_t remove_child(mem_art_t *tree, mem_ref_t node_ref, uint8_t byte) {
    void *node = ref_ptr(tree, node_ref);
    switch (node_type(node)) {
        case MEM_NODE_4: {
            mem_node4_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1], n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(mem_ref_t));
                    n->num_children--;
                    return node_ref;
                }
            }
            return node_ref;
        }
        case MEM_NODE_16: {
            mem_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1], n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(mem_ref_t));
                    n->num_children--;
                    // Shrink Node16 → Node4
                    if (n->num_children <= MEM_NODE4_MAX) {
                        mem_ref_t new_ref = alloc_node(tree, MEM_NODE_4);
                        if (new_ref == MEM_REF_NULL) return node_ref;
                        n = ref_ptr(tree, node_ref);
                        mem_node4_t *n4 = ref_ptr(tree, new_ref);
                        copy_header(n4, n);
                        n4->num_children = n->num_children;
                        memcpy(n4->keys, n->keys, n->num_children);
                        memcpy(n4->children, n->children,
                               n->num_children * sizeof(mem_ref_t));
                        return new_ref;
                    }
                    return node_ref;
                }
            }
            return node_ref;
        }
        case MEM_NODE_32: {
            mem_node32_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1], n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(mem_ref_t));
                    n->num_children--;
                    // Shrink Node32 → Node16
                    if (n->num_children <= MEM_NODE16_MAX) {
                        mem_ref_t new_ref = alloc_node(tree, MEM_NODE_16);
                        if (new_ref == MEM_REF_NULL) return node_ref;
                        n = ref_ptr(tree, node_ref);
                        mem_node16_t *n16 = ref_ptr(tree, new_ref);
                        copy_header(n16, n);
                        n16->num_children = n->num_children;
                        memcpy(n16->keys, n->keys, n->num_children);
                        memcpy(n16->children, n->children,
                               n->num_children * sizeof(mem_ref_t));
                        return new_ref;
                    }
                    return node_ref;
                }
            }
            return node_ref;
        }
        case MEM_NODE_48: {
            mem_node48_t *n = node;
            uint8_t idx = n->index[byte];
            if (idx != MEM_NODE48_EMPTY) {
                n->children[idx] = MEM_REF_NULL;
                n->index[byte] = MEM_NODE48_EMPTY;
                n->num_children--;
                // Shrink Node48 → Node32
                if (n->num_children <= MEM_NODE32_MAX) {
                    mem_ref_t new_ref = alloc_node(tree, MEM_NODE_32);
                    if (new_ref == MEM_REF_NULL) return node_ref;
                    n = ref_ptr(tree, node_ref);
                    mem_node32_t *n32 = ref_ptr(tree, new_ref);
                    copy_header(n32, n);
                    n32->num_children = 0;
                    for (int i = 0; i < 256; i++) {
                        if (n->index[i] != MEM_NODE48_EMPTY) {
                            n32->keys[n32->num_children] = (uint8_t)i;
                            n32->children[n32->num_children] = n->children[n->index[i]];
                            n32->num_children++;
                        }
                    }
                    return new_ref;
                }
            }
            return node_ref;
        }
        case MEM_NODE_256: {
            mem_node256_t *n = node;
            if (n->children[byte] != MEM_REF_NULL) {
                n->children[byte] = MEM_REF_NULL;
                n->num_children--;
                // Shrink Node256 → Node48
                if (n->num_children <= MEM_NODE48_MAX) {
                    mem_ref_t new_ref = alloc_node(tree, MEM_NODE_48);
                    if (new_ref == MEM_REF_NULL) return node_ref;
                    n = ref_ptr(tree, node_ref);
                    mem_node48_t *n48 = ref_ptr(tree, new_ref);
                    copy_header(n48, n);
                    n48->num_children = 0;
                    for (int i = 0; i < 256; i++) {
                        if (n->children[i] != MEM_REF_NULL) {
                            n48->index[i] = (uint8_t)n48->num_children;
                            n48->children[n48->num_children] = n->children[i];
                            n48->num_children++;
                        }
                    }
                    return new_ref;
                }
            }
            return node_ref;
        }
        default:
            return node_ref;
    }
}

// ============================================================================
// search — iterative key lookup
// ============================================================================

static const void *search(const mem_art_t *tree, mem_ref_t ref,
                          const uint8_t *key, size_t key_len, size_t depth,
                          size_t *value_len) {
    while (ref != MEM_REF_NULL) {
        if (MEM_IS_LEAF(ref)) {
            if (leaf_matches(tree, ref, key, key_len)) {
                return leaf_value(tree, ref, value_len);
            }
            return NULL;
        }

        void *node = ref_ptr(tree, ref);

        // Check compressed path
        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            int prefix_len = check_prefix(tree, ref, node, key, key_len, depth);
            if (prefix_len != (int)plen) return NULL;
            depth += plen;
        }

        // Find child for next byte (use 0x00 if key is consumed)
        uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
        mem_ref_t *child_ptr = find_child_ptr(tree, ref, byte);
        if (!child_ptr) return NULL;
        ref = *child_ptr;
        depth++;
    }
    return NULL;
}

// ============================================================================
// insert_recursive
// ============================================================================

static mem_ref_t insert_recursive(mem_art_t *tree, mem_ref_t ref,
                                  const uint8_t *key, size_t key_len,
                                  size_t depth, const void *value,
                                  size_t value_len, bool *inserted,
                                  mem_ref_t *out_leaf_ref) {
    // Base case: empty slot → create leaf
    if (ref == MEM_REF_NULL) {
        *inserted = true;
        mem_ref_t leaf = alloc_leaf(tree, key, key_len, value, value_len);
        if (out_leaf_ref) *out_leaf_ref = leaf;
        return leaf;
    }

    // Leaf node
    if (MEM_IS_LEAF(ref)) {
        if (leaf_matches(tree, ref, key, key_len)) {
            // Update existing leaf — allocate new (old becomes dead arena space)
            mem_ref_t new_leaf = alloc_leaf(tree, key, key_len, value, value_len);
            if (new_leaf == MEM_REF_NULL) return ref;
            *inserted = false;
            if (out_leaf_ref) *out_leaf_ref = new_leaf;
            return new_leaf;
        }

        // Keys differ — create new branch node
        *inserted = true;
        mem_ref_t new_leaf = alloc_leaf(tree, key, key_len, value, value_len);
        if (new_leaf == MEM_REF_NULL) return ref;
        if (out_leaf_ref) *out_leaf_ref = new_leaf;

        // Find longest common prefix
        const uint8_t *lk = leaf_key(tree, ref);
        mem_leaf_t *old_leaf = leaf_ptr(tree, ref);
        size_t old_key_len = old_leaf->key_len;

        size_t limit = (key_len < old_key_len) ? key_len : old_key_len;
        size_t prefix_len = 0;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == lk[depth + prefix_len]) {
            prefix_len++;
        }

        mem_ref_t new_node_ref = alloc_node(tree, MEM_NODE_4);
        if (new_node_ref == MEM_REF_NULL) return ref;

        // Re-resolve after potential realloc
        lk = leaf_key(tree, ref);
        old_leaf = leaf_ptr(tree, ref);

        void *new_node = ref_ptr(tree, new_node_ref);
        set_partial_len(new_node, (uint8_t)(prefix_len < MEM_MAX_PREFIX ?
                        prefix_len : MEM_MAX_PREFIX));
        if (prefix_len > 0) {
            uint8_t cpy = prefix_len < MEM_MAX_PREFIX ?
                          (uint8_t)prefix_len : MEM_MAX_PREFIX;
            memcpy(node_partial(new_node), key + depth, cpy);
        }
        // Store full prefix length in partial_len (for optimistic check)
        set_partial_len(new_node, (uint8_t)prefix_len);

        size_t new_depth = depth + prefix_len;
        uint8_t new_key_byte = (new_depth < key_len) ? key[new_depth] : 0x00;
        uint8_t old_key_byte = (new_depth < old_key_len) ? lk[new_depth] : 0x00;

        new_node_ref = add_child(tree, new_node_ref, new_key_byte, new_leaf);
        new_node_ref = add_child(tree, new_node_ref, old_key_byte, ref);

        return new_node_ref;
    }

    // Inner node — check compressed path
    void *node = ref_ptr(tree, ref);
    uint8_t plen = node_partial_len(node);

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key, key_len, depth);

        if (prefix_len < (int)plen) {
            // Prefix mismatch — split the node
            *inserted = true;

            // Get the discriminating byte for old node
            uint8_t old_byte;
            if (prefix_len < MEM_MAX_PREFIX) {
                old_byte = node_partial(node)[prefix_len];
            } else {
                // Beyond stored prefix — get from leaf
                mem_ref_t min_leaf = find_minimum_leaf(tree, ref);
                old_byte = leaf_key(tree, min_leaf)[depth + prefix_len];
            }

            mem_ref_t new_node_ref = alloc_node(tree, MEM_NODE_4);
            if (new_node_ref == MEM_REF_NULL) return ref;

            // Re-resolve after potential realloc
            node = ref_ptr(tree, ref);

            void *new_node = ref_ptr(tree, new_node_ref);

            // New node gets the matching prefix
            set_partial_len(new_node, (uint8_t)prefix_len);
            if (prefix_len > 0) {
                uint8_t cpy = prefix_len < MEM_MAX_PREFIX ?
                              (uint8_t)prefix_len : MEM_MAX_PREFIX;
                memcpy(node_partial(new_node), node_partial(node), cpy);
            }

            // Shift old node's prefix: skip matched prefix + discriminating byte
            uint8_t new_plen = plen - (prefix_len + 1);
            uint8_t to_store = new_plen < MEM_MAX_PREFIX ? new_plen : MEM_MAX_PREFIX;

            if (prefix_len + 1 < MEM_MAX_PREFIX) {
                // Some stored bytes survive the shift
                uint8_t surviving = MEM_MAX_PREFIX - (prefix_len + 1);
                if (surviving > to_store) surviving = to_store;
                memmove(node_partial(node),
                        node_partial(node) + prefix_len + 1, surviving);
                // Fill remaining inline slots from leaf if needed
                if (surviving < to_store) {
                    mem_ref_t min_leaf = find_minimum_leaf(tree, ref);
                    const uint8_t *lk = leaf_key(tree, min_leaf);
                    node = ref_ptr(tree, ref);
                    memcpy(node_partial(node) + surviving,
                           lk + depth + prefix_len + 1 + surviving,
                           to_store - surviving);
                }
            } else if (to_store > 0) {
                // Entire stored prefix consumed — reload from leaf
                mem_ref_t min_leaf = find_minimum_leaf(tree, ref);
                const uint8_t *lk = leaf_key(tree, min_leaf);
                node = ref_ptr(tree, ref);
                memcpy(node_partial(node), lk + depth + prefix_len + 1, to_store);
            }
            set_partial_len(node, new_plen);

            // Create leaf for new key
            mem_ref_t leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
            if (leaf_ref == MEM_REF_NULL) return ref;
            if (out_leaf_ref) *out_leaf_ref = leaf_ref;

            uint8_t new_byte = (depth + prefix_len < key_len) ?
                               key[depth + prefix_len] : 0x00;

            new_node_ref = add_child(tree, new_node_ref, old_byte, ref);
            new_node_ref = add_child(tree, new_node_ref, new_byte, leaf_ref);

            return new_node_ref;
        }

        depth += plen;
    }

    // Use 0x00 as terminator if key is consumed
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    mem_ref_t *child_ptr = find_child_ptr(tree, ref, byte);

    if (child_ptr) {
        // Child exists — recurse or update in-place
        if (depth >= key_len) {
            // Key is consumed, child should be a leaf — update it
            mem_ref_t old_child = *child_ptr;
            if (MEM_IS_LEAF(old_child) && leaf_matches(tree, old_child, key, key_len)) {
                mem_ref_t new_leaf = alloc_leaf(tree, key, key_len, value, value_len);
                if (new_leaf != MEM_REF_NULL) {
                    child_ptr = find_child_ptr(tree, ref, byte);
                    *child_ptr = new_leaf;
                    *inserted = false;
                    if (out_leaf_ref) *out_leaf_ref = new_leaf;
                }
            }
            return ref;
        }
        mem_ref_t old_child = *child_ptr;
        mem_ref_t new_child = insert_recursive(tree, old_child, key, key_len,
                                               depth + 1, value, value_len,
                                               inserted, out_leaf_ref);
        if (new_child != old_child) {
            // Re-resolve child_ptr after potential realloc
            child_ptr = find_child_ptr(tree, ref, byte);
            *child_ptr = new_child;
        }
    } else {
        *inserted = true;
        mem_ref_t leaf_ref = alloc_leaf(tree, key, key_len, value, value_len);
        if (leaf_ref != MEM_REF_NULL) {
            if (out_leaf_ref) *out_leaf_ref = leaf_ref;
            ref = add_child(tree, ref, byte, leaf_ref);
        }
    }

    return ref;
}

// ============================================================================
// delete_recursive
// ============================================================================

static mem_ref_t delete_recursive(mem_art_t *tree, mem_ref_t ref,
                                  const uint8_t *key, size_t key_len,
                                  size_t depth, bool *deleted) {
    if (ref == MEM_REF_NULL) return MEM_REF_NULL;

    if (MEM_IS_LEAF(ref)) {
        if (leaf_matches(tree, ref, key, key_len)) {
            *deleted = true;
            return MEM_REF_NULL;
        }
        return ref;
    }

    void *node = ref_ptr(tree, ref);
    uint8_t plen = node_partial_len(node);

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key, key_len, depth);
        if (prefix_len != (int)plen) return ref;
        depth += plen;
    }

    // Use 0x00 as terminator if key is consumed
    uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
    mem_ref_t *child_ptr = find_child_ptr(tree, ref, byte);
    if (!child_ptr) return ref;

    // If key is consumed, child should be the leaf to delete
    if (depth >= key_len) {
        mem_ref_t old_child = *child_ptr;
        if (MEM_IS_LEAF(old_child) && leaf_matches(tree, old_child, key, key_len)) {
            *deleted = true;
            ref = remove_child(tree, ref, byte);
            goto check_collapse;
        }
        return ref;
    }

    mem_ref_t old_child = *child_ptr;
    mem_ref_t new_child = delete_recursive(tree, old_child, key, key_len,
                                           depth + 1, deleted);

    if (new_child != old_child) {
        if (new_child == MEM_REF_NULL) {
            // Child was deleted
            ref = remove_child(tree, ref, byte);
        } else {
            // Child replaced (shouldn't happen in delete, but handle it)
            child_ptr = find_child_ptr(tree, ref, byte);
            if (child_ptr) *child_ptr = new_child;
        }
    }

check_collapse:
    // Path collapse: if Node4 has exactly 1 child, merge with that child
    node = ref_ptr(tree, ref);
    if (!MEM_IS_LEAF(ref) && node_type(node) == MEM_NODE_4 &&
        node_num_children(node) == 1) {
        mem_node4_t *n4 = node;
        mem_ref_t only_child = n4->children[0];
        uint8_t only_byte = n4->keys[0];

        if (MEM_IS_LEAF(only_child)) {
            // Single leaf child — return the leaf directly
            return only_child;
        }

        // Merge prefixes: parent partial + discriminating byte + child partial
        void *child_node = ref_ptr(tree, only_child);
        uint8_t child_plen = node_partial_len(child_node);
        uint32_t new_plen = (uint32_t)plen + 1 + child_plen;

        if (new_plen <= 255) {
            // Build merged prefix
            // We need to construct the full new prefix. For the inline portion:
            uint8_t new_partial[MEM_MAX_PREFIX];
            uint8_t *parent_partial = node_partial(node);

            // Bytes from parent's prefix
            int pos = 0;
            uint8_t parent_inline = plen < MEM_MAX_PREFIX ? plen : MEM_MAX_PREFIX;
            for (int i = 0; i < parent_inline && pos < MEM_MAX_PREFIX; i++, pos++) {
                new_partial[pos] = parent_partial[i];
            }
            // If parent prefix extends beyond stored, get from leaf
            if (plen > MEM_MAX_PREFIX && pos < MEM_MAX_PREFIX) {
                mem_ref_t min_leaf = find_minimum_leaf(tree, only_child);
                if (min_leaf != MEM_REF_NULL) {
                    const uint8_t *lk = leaf_key(tree, min_leaf);
                    child_node = ref_ptr(tree, only_child);
                    for (int i = MEM_MAX_PREFIX; i < (int)plen && pos < MEM_MAX_PREFIX; i++, pos++) {
                        new_partial[pos] = lk[depth - plen + i];
                    }
                }
            }
            // Discriminating byte
            if (pos < MEM_MAX_PREFIX) {
                new_partial[pos++] = only_byte;
            }
            // Child's prefix bytes
            uint8_t *child_partial = node_partial(child_node);
            uint8_t child_inline = child_plen < MEM_MAX_PREFIX ? child_plen : MEM_MAX_PREFIX;
            for (int i = 0; i < child_inline && pos < MEM_MAX_PREFIX; i++, pos++) {
                new_partial[pos] = child_partial[i];
            }

            set_partial_len(child_node, (uint8_t)new_plen);
            memcpy(node_partial(child_node), new_partial,
                   pos < MEM_MAX_PREFIX ? pos : MEM_MAX_PREFIX);

            return only_child;
        }
    }

    return ref;
}

// ============================================================================
// Public API
// ============================================================================

bool mem_art_init(mem_art_t *tree) {
    if (!tree) return false;
    tree->root = MEM_REF_NULL;
    tree->size = 0;
    tree->arena = malloc(MEM_ARENA_INITIAL_CAP);
    if (!tree->arena) return false;
    tree->arena_cap = MEM_ARENA_INITIAL_CAP;
    tree->arena_used = 4;  // Reserve offset 0 as MEM_REF_NULL
    return true;
}

void mem_art_destroy(mem_art_t *tree) {
    if (!tree) return;
    free(tree->arena);
    tree->arena = NULL;
    tree->arena_cap = 0;
    tree->arena_used = 0;
    tree->root = MEM_REF_NULL;
    tree->size = 0;
}

bool mem_art_insert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                    const void *value, size_t value_len) {
    if (!tree || !key || key_len == 0) return false;

    bool inserted = false;
    mem_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                          value, value_len, &inserted, NULL);
    if (new_root == MEM_REF_NULL && tree->root != MEM_REF_NULL) return false;

    tree->root = new_root;
    if (inserted) tree->size++;
    return true;
}

bool mem_art_insert_check(mem_art_t *tree, const uint8_t *key, size_t key_len,
                          const void *value, size_t value_len, bool *was_new) {
    if (!tree || !key || key_len == 0) return false;

    bool inserted = false;
    mem_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                          value, value_len, &inserted, NULL);
    if (new_root == MEM_REF_NULL && tree->root != MEM_REF_NULL) return false;

    tree->root = new_root;
    if (inserted) tree->size++;
    if (was_new) *was_new = inserted;
    return true;
}

const void *mem_art_get(const mem_art_t *tree, const uint8_t *key,
                        size_t key_len, size_t *value_len) {
    if (!tree || !key || key_len == 0) return NULL;
    return search(tree, tree->root, key, key_len, 0, value_len);
}

void *mem_art_get_mut(mem_art_t *tree, const uint8_t *key,
                      size_t key_len, size_t *value_len) {
    return (void *)mem_art_get(tree, key, key_len, value_len);
}

bool mem_art_delete(mem_art_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key || key_len == 0) return false;

    bool deleted = false;
    tree->root = delete_recursive(tree, tree->root, key, key_len, 0, &deleted);
    if (deleted) tree->size--;
    return deleted;
}

void *mem_art_upsert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                     const void *value, size_t value_len) {
    if (!tree || !key || key_len == 0) return NULL;

    bool inserted = false;
    mem_ref_t leaf_ref = MEM_REF_NULL;
    mem_ref_t new_root = insert_recursive(tree, tree->root, key, key_len, 0,
                                          value, value_len, &inserted,
                                          &leaf_ref);
    if (new_root == MEM_REF_NULL && tree->root != MEM_REF_NULL) return NULL;

    tree->root = new_root;
    if (inserted) tree->size++;

    if (leaf_ref == MEM_REF_NULL) return NULL;
    return (void *)leaf_value(tree, leaf_ref, NULL);
}

void mem_art_prefetch(const mem_art_t *tree, const uint8_t *key, size_t key_len) {
    if (!tree || !key || key_len == 0) return;
    mem_ref_t ref = tree->root;
    size_t depth = 0;
    while (ref != MEM_REF_NULL) {
        if (MEM_IS_LEAF(ref)) {
            /* Prefetch leaf value data */
            void *leaf = ref_ptr(tree, ref);
            __builtin_prefetch(leaf, 0, 1);
            return;
        }
        void *node = ref_ptr(tree, ref);
        __builtin_prefetch(node, 0, 1);

        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            int prefix_len = check_prefix(tree, ref, node, key, key_len, depth);
            if (prefix_len != (int)plen) return;
            depth += plen;
        }

        uint8_t byte = (depth < key_len) ? key[depth] : 0x00;
        mem_ref_t *child_ptr = find_child_ptr(tree, ref, byte);
        if (!child_ptr) return;
        ref = *child_ptr;
        depth++;
    }
}

bool mem_art_contains(const mem_art_t *tree, const uint8_t *key, size_t key_len) {
    return mem_art_get(tree, key, key_len, NULL) != NULL;
}

size_t mem_art_size(const mem_art_t *tree) {
    return tree ? tree->size : 0;
}

bool mem_art_is_empty(const mem_art_t *tree) {
    return tree ? (tree->size == 0) : true;
}

// ============================================================================
// Iterator
// ============================================================================

typedef struct {
    const uint8_t *key;
    size_t key_len;
    const void *value;
    size_t value_len;
    bool done;
    bool started;
    struct {
        mem_ref_t ref;
        int child_idx;
    } stack[64];
    int depth;
} mem_iterator_state_t;

mem_art_iterator_t *mem_art_iterator_create(const mem_art_t *tree) {
    if (!tree) return NULL;

    mem_art_iterator_t *iter = malloc(sizeof(mem_art_iterator_t));
    if (!iter) return NULL;

    mem_iterator_state_t *state = malloc(sizeof(mem_iterator_state_t));
    if (!state) {
        free(iter);
        return NULL;
    }

    iter->tree = (mem_art_t *)tree;
    iter->internal = state;

    memset(state, 0, sizeof(mem_iterator_state_t));
    state->done = (tree->root == MEM_REF_NULL);
    state->depth = -1;

    return iter;
}

bool mem_art_iterator_next(mem_art_iterator_t *iter) {
    if (!iter || !iter->internal) return false;

    mem_iterator_state_t *state = iter->internal;
    if (state->done) return false;

    // First call — initialize stack with root
    if (!state->started) {
        state->started = true;
        if (iter->tree->root == MEM_REF_NULL) {
            state->done = true;
            return false;
        }
        state->depth = 0;
        state->stack[0].ref = iter->tree->root;
        state->stack[0].child_idx = -1;
    }

    // Depth-first traversal to find next leaf
    while (state->depth >= 0) {
        mem_ref_t ref = state->stack[state->depth].ref;

        // If this is a leaf, return it
        if (MEM_IS_LEAF(ref)) {
            mem_leaf_t *leaf = leaf_ptr(iter->tree, ref);
            state->key = leaf->data;
            state->key_len = leaf->key_len;
            state->value_len = leaf->value_len;
            size_t val_off = leaf_value_offset(leaf->key_len, leaf->value_len);
            state->value = (const uint8_t *)leaf + val_off;

            // Pop this leaf from stack for next iteration
            state->depth--;
            return true;
        }

        // Inner node — find next child to visit
        void *node = ref_ptr(iter->tree, ref);
        int *child_idx = &state->stack[state->depth].child_idx;
        (*child_idx)++;

        mem_ref_t next_child = MEM_REF_NULL;

        switch (node_type(node)) {
            case MEM_NODE_4: {
                mem_node4_t *n = node;
                if (*child_idx < n->num_children) {
                    next_child = n->children[*child_idx];
                }
                break;
            }
            case MEM_NODE_16: {
                mem_node16_t *n = node;
                if (*child_idx < n->num_children) {
                    next_child = n->children[*child_idx];
                }
                break;
            }
            case MEM_NODE_32: {
                mem_node32_t *n = node;
                if (*child_idx < n->num_children) {
                    next_child = n->children[*child_idx];
                }
                break;
            }
            case MEM_NODE_48: {
                mem_node48_t *n = node;
                while (*child_idx < 256) {
                    uint8_t idx = n->index[*child_idx];
                    if (idx != MEM_NODE48_EMPTY) {
                        next_child = n->children[idx];
                        break;
                    }
                    (*child_idx)++;
                }
                break;
            }
            case MEM_NODE_256: {
                mem_node256_t *n = node;
                while (*child_idx < 256) {
                    if (n->children[*child_idx] != MEM_REF_NULL) {
                        next_child = n->children[*child_idx];
                        break;
                    }
                    (*child_idx)++;
                }
                break;
            }
            default:
                break;
        }

        if (next_child != MEM_REF_NULL) {
            state->depth++;
            if (state->depth >= 64) {
                state->done = true;
                return false;
            }
            state->stack[state->depth].ref = next_child;
            state->stack[state->depth].child_idx = -1;
        } else {
            state->depth--;
        }
    }

    state->done = true;
    return false;
}

const uint8_t *mem_art_iterator_key(const mem_art_iterator_t *iter,
                                    size_t *key_len) {
    if (!iter || !iter->internal) return NULL;
    mem_iterator_state_t *state = iter->internal;
    if (state->done) return NULL;
    if (key_len) *key_len = state->key_len;
    return state->key;
}

const void *mem_art_iterator_value(const mem_art_iterator_t *iter,
                                   size_t *value_len) {
    if (!iter || !iter->internal) return NULL;
    mem_iterator_state_t *state = iter->internal;
    if (state->done) return NULL;
    if (value_len) *value_len = state->value_len;
    return state->value;
}

bool mem_art_iterator_done(const mem_art_iterator_t *iter) {
    if (!iter || !iter->internal) return true;
    mem_iterator_state_t *state = iter->internal;
    return state->done;
}

void mem_art_iterator_destroy(mem_art_iterator_t *iter) {
    if (!iter) return;
    free(iter->internal);
    free(iter);
}

void mem_art_foreach(const mem_art_t *tree, mem_art_callback_t callback,
                     void *user_data) {
    if (!tree || !callback) return;

    mem_art_iterator_t *iter = mem_art_iterator_create(tree);
    if (!iter) return;

    while (mem_art_iterator_next(iter)) {
        size_t key_len, value_len;
        const uint8_t *key = mem_art_iterator_key(iter, &key_len);
        const void *value = mem_art_iterator_value(iter, &value_len);

        if (key && value) {
            if (!callback(key, key_len, value, value_len, user_data)) {
                break;
            }
        }
    }

    mem_art_iterator_destroy(iter);
}
