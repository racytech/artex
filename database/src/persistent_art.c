#include "../include/persistent_art.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <emmintrin.h>  // SSE2
#include <nmmintrin.h>  // SSE4.2 (CRC32C)

// ============================================================================
// Constants
// ============================================================================

#define PART_META_SIZE        4096
#define PART_META_A_OFFSET    0
#define PART_META_B_OFFSET    4096
#define PART_DATA_OFFSET      8192

#define PART_NODE_POOL_MAX    (16ULL * 1024 * 1024 * 1024)  // 16 GB
#define PART_LEAF_POOL_OFFSET (PART_DATA_OFFSET + PART_NODE_POOL_MAX)
#define PART_LEAF_POOL_MAX    (64ULL * 1024 * 1024 * 1024)  // 64 GB
#define PART_TOTAL_FILE_SIZE  (PART_LEAF_POOL_OFFSET + PART_LEAF_POOL_MAX)

#define PART_MAX_PREFIX       8
#define PART_META_MAGIC       0x545241545241500AULL  // "PART_ART\n"

// ============================================================================
// Node Types
// ============================================================================

typedef enum {
    PART_NODE_16  = 0,
    PART_NODE_64  = 1,
    PART_NODE_256 = 2,
} part_node_type_t;

#define PART_NODE16_MAX   16
#define PART_NODE64_MAX   64
#define PART_NODE256_MAX  256

// ============================================================================
// Node Structures
// ============================================================================

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[16];
    uint8_t _pad[1];
    uint32_t children[16];
    uint8_t partial[PART_MAX_PREFIX];
} part_node16_t;

typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t keys[64];
    uint8_t _pad[1];
    uint32_t children[64];
    uint8_t partial[PART_MAX_PREFIX];
} part_node64_t;

typedef struct {
    uint8_t type;
    uint8_t num_children;   // 0 means 256
    uint8_t partial_len;
    uint8_t _pad[1];
    uint32_t children[256];
    uint8_t partial[PART_MAX_PREFIX];
} part_node256_t;

// ============================================================================
// Meta Page
// ============================================================================

typedef struct {
    uint64_t magic;
    uint64_t generation;
    uint64_t size;
    uint64_t node_used;
    uint32_t root;
    uint32_t leaf_count;
    uint32_t key_size;
    uint32_t value_size;
    uint32_t crc32c;
} part_meta_t;

#define PART_META_CRC_LEN  (offsetof(part_meta_t, crc32c))

// ============================================================================
// CRC32C (hardware-accelerated via SSE4.2)
// ============================================================================

static uint32_t part_crc32c(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t c = ~(uint64_t)0;
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, p, 8);
        c = _mm_crc32_u64(c, val);
        p += 8;
        len -= 8;
    }
    while (len > 0) {
        c = _mm_crc32_u8((uint32_t)c, *p);
        p++;
        len--;
    }
    return ~(uint32_t)c;
}

// ============================================================================
// Node Accessors
// ============================================================================

static inline part_node_type_t node_type(const void *node) {
    return (part_node_type_t)(((const uint8_t *)node)[0]);
}

static inline uint8_t node_num_children(const void *node) {
    return ((const uint8_t *)node)[1];
}

static inline uint8_t node_partial_len(const void *node) {
    return ((const uint8_t *)node)[2];
}

static inline const uint8_t *node_partial(const void *node) {
    switch (node_type(node)) {
        case PART_NODE_16:  return ((const part_node16_t *)node)->partial;
        case PART_NODE_64:  return ((const part_node64_t *)node)->partial;
        case PART_NODE_256: return ((const part_node256_t *)node)->partial;
        default: return NULL;
    }
}

static inline uint8_t *node_partial_mut(void *node) {
    switch (node_type(node)) {
        case PART_NODE_16:  return ((part_node16_t *)node)->partial;
        case PART_NODE_64:  return ((part_node64_t *)node)->partial;
        case PART_NODE_256: return ((part_node256_t *)node)->partial;
        default: return NULL;
    }
}

static void copy_header(void *dst, const void *src) {
    uint8_t plen = ((const uint8_t *)src)[2];
    ((uint8_t *)dst)[2] = plen;
    uint8_t store = plen < PART_MAX_PREFIX ? plen : PART_MAX_PREFIX;
    memcpy(node_partial_mut(dst), node_partial(src), store);
}

static size_t node_size_for_type(part_node_type_t type) {
    switch (type) {
        case PART_NODE_16:  return sizeof(part_node16_t);
        case PART_NODE_64:  return sizeof(part_node64_t);
        case PART_NODE_256: return sizeof(part_node256_t);
        default: return 0;
    }
}

// ============================================================================
// Ref Resolution
// ============================================================================

static inline void *node_ptr(const persistent_art_t *tree, part_ref_t ref) {
    return tree->node_base + (size_t)ref * 8;
}

static inline uint8_t *leaf_ptr(const persistent_art_t *tree, part_ref_t ref) {
    return tree->leaf_base +
           (size_t)PART_LEAF_INDEX(ref) * tree->leaf_size;
}

static inline const uint8_t *leaf_key(const persistent_art_t *tree,
                                       part_ref_t ref) {
    return leaf_ptr(tree, ref);
}

static inline const void *leaf_value(const persistent_art_t *tree,
                                      part_ref_t ref) {
    return leaf_ptr(tree, ref) + tree->key_size;
}

static inline bool leaf_matches(const persistent_art_t *tree,
                                 part_ref_t ref, const uint8_t *key) {
    return memcmp(leaf_key(tree, ref), key, tree->key_size) == 0;
}

// ============================================================================
// Allocation (bump, COW-safe — never reused in v1)
// ============================================================================

static part_ref_t alloc_node(persistent_art_t *tree, part_node_type_t type) {
    size_t size = node_size_for_type(type);
    if (size == 0) return PART_REF_NULL;

    // 8-byte aligned bump
    size_t aligned = (tree->node_used + 7) & ~(size_t)7;
    if (aligned + size > PART_NODE_POOL_MAX) return PART_REF_NULL;

    void *node = tree->node_base + aligned;
    memset(node, 0, size);
    ((uint8_t *)node)[0] = (uint8_t)type;

    tree->node_used = aligned + size;
    return (part_ref_t)(aligned / 8);
}

static part_ref_t alloc_leaf(persistent_art_t *tree,
                              const uint8_t *key, const void *value) {
    uint32_t idx = tree->leaf_count;
    size_t offset = (size_t)idx * tree->leaf_size;
    if (offset + tree->leaf_size > PART_LEAF_POOL_MAX)
        return PART_REF_NULL;

    tree->leaf_count++;
    uint8_t *leaf = tree->leaf_base + offset;
    memcpy(leaf, key, tree->key_size);
    memcpy(leaf + tree->key_size, value, tree->value_size);

    return PART_MAKE_LEAF_REF(idx);
}

// ============================================================================
// COW Copy Node
// ============================================================================

static part_ref_t cow_copy_node(persistent_art_t *tree, part_ref_t ref) {
    void *old = node_ptr(tree, ref);
    part_node_type_t type = node_type(old);
    size_t size = node_size_for_type(type);

    size_t aligned = (tree->node_used + 7) & ~(size_t)7;
    if (aligned + size > PART_NODE_POOL_MAX) return PART_REF_NULL;

    void *new_node = tree->node_base + aligned;
    memcpy(new_node, old, size);

    tree->node_used = aligned + size;
    return (part_ref_t)(aligned / 8);
}

// ============================================================================
// find_child — find child ref for a given byte
// ============================================================================

static part_ref_t find_child(const persistent_art_t *tree,
                              part_ref_t node_ref, uint8_t byte) {
    void *node = node_ptr(tree, node_ref);
    switch (node_type(node)) {
        case PART_NODE_16: {
            part_node16_t *n = node;
            __m128i key_vec = _mm_set1_epi8((char)byte);
            __m128i cmp = _mm_cmpeq_epi8(key_vec,
                            _mm_loadu_si128((__m128i *)n->keys));
            int mask = _mm_movemask_epi8(cmp) & ((1 << n->num_children) - 1);
            if (mask) return n->children[__builtin_ctz(mask)];
            return PART_REF_NULL;
        }
        case PART_NODE_64: {
            part_node64_t *n = node;
            __m128i key_vec = _mm_set1_epi8((char)byte);
            for (int chunk = 0; chunk < 4; chunk++) {
                int off = chunk * 16;
                int rem = n->num_children - off;
                if (rem <= 0) break;
                __m128i cmp = _mm_cmpeq_epi8(key_vec,
                                _mm_loadu_si128((__m128i *)(n->keys + off)));
                int mask = _mm_movemask_epi8(cmp);
                if (rem < 16) mask &= (1 << rem) - 1;
                if (mask) return n->children[off + __builtin_ctz(mask)];
            }
            return PART_REF_NULL;
        }
        case PART_NODE_256: {
            part_node256_t *n = node;
            return n->children[byte];  // PART_REF_NULL if absent
        }
        default:
            return PART_REF_NULL;
    }
}

// ============================================================================
// find_minimum_leaf — leftmost leaf in subtree
// ============================================================================

static part_ref_t find_minimum_leaf(const persistent_art_t *tree,
                                     part_ref_t ref) {
    while (ref != PART_REF_NULL && !PART_IS_LEAF_REF(ref)) {
        void *node = node_ptr(tree, ref);
        switch (node_type(node)) {
            case PART_NODE_16:
                ref = ((part_node16_t *)node)->children[0];
                break;
            case PART_NODE_64:
                ref = ((part_node64_t *)node)->children[0];
                break;
            case PART_NODE_256: {
                part_node256_t *n = node;
                ref = PART_REF_NULL;
                for (int i = 0; i < 256; i++) {
                    if (n->children[i] != PART_REF_NULL) {
                        ref = n->children[i];
                        break;
                    }
                }
                break;
            }
            default:
                return PART_REF_NULL;
        }
    }
    return ref;
}

// ============================================================================
// check_prefix — compare compressed path against key (optimistic)
// ============================================================================

static int check_prefix(const persistent_art_t *tree, part_ref_t ref,
                         const void *node, const uint8_t *key,
                         uint32_t key_size, size_t depth) {
    uint8_t plen = node_partial_len(node);
    const uint8_t *partial = node_partial(node);
    int max_cmp = plen;
    if (depth + (size_t)max_cmp > key_size)
        max_cmp = (int)key_size - (int)depth;

    int stored = max_cmp < PART_MAX_PREFIX ? max_cmp : PART_MAX_PREFIX;
    for (int idx = 0; idx < stored; idx++) {
        if (partial[idx] != key[depth + idx]) return idx;
    }

    if (max_cmp > PART_MAX_PREFIX) {
        part_ref_t min_leaf = find_minimum_leaf(tree, ref);
        if (min_leaf == PART_REF_NULL) return PART_MAX_PREFIX;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int idx = PART_MAX_PREFIX; idx < max_cmp; idx++) {
            if (lk[depth + idx] != key[depth + idx]) return idx;
        }
    }

    return max_cmp;
}

// ============================================================================
// Node mutation helpers (operate on freshly allocated copies)
// ============================================================================

// Add child in sorted position (Node16/Node64) or direct (Node256).
static void node_add_child_inplace(void *node, uint8_t byte, part_ref_t child) {
    switch (node_type(node)) {
        case PART_NODE_16: {
            part_node16_t *n = node;
            int i;
            for (i = 0; i < n->num_children; i++) {
                if (byte < n->keys[i]) break;
            }
            if (i < n->num_children) {
                memmove(&n->keys[i + 1], &n->keys[i], n->num_children - i);
                memmove(&n->children[i + 1], &n->children[i],
                        (n->num_children - i) * sizeof(uint32_t));
            }
            n->keys[i] = byte;
            n->children[i] = child;
            n->num_children++;
            break;
        }
        case PART_NODE_64: {
            part_node64_t *n = node;
            int i;
            for (i = 0; i < n->num_children; i++) {
                if (byte < n->keys[i]) break;
            }
            if (i < n->num_children) {
                memmove(&n->keys[i + 1], &n->keys[i], n->num_children - i);
                memmove(&n->children[i + 1], &n->children[i],
                        (n->num_children - i) * sizeof(uint32_t));
            }
            n->keys[i] = byte;
            n->children[i] = child;
            n->num_children++;
            break;
        }
        case PART_NODE_256: {
            part_node256_t *n = node;
            if (n->children[byte] == PART_REF_NULL)
                n->num_children++;
            n->children[byte] = child;
            break;
        }
    }
}

// Remove child by key byte (in-place on a fresh copy).
static void node_remove_child_inplace(void *node, uint8_t byte) {
    switch (node_type(node)) {
        case PART_NODE_16: {
            part_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(uint32_t));
                    n->num_children--;
                    return;
                }
            }
            break;
        }
        case PART_NODE_64: {
            part_node64_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    memmove(&n->keys[i], &n->keys[i + 1],
                            n->num_children - i - 1);
                    memmove(&n->children[i], &n->children[i + 1],
                            (n->num_children - i - 1) * sizeof(uint32_t));
                    n->num_children--;
                    return;
                }
            }
            break;
        }
        case PART_NODE_256: {
            part_node256_t *n = node;
            if (n->children[byte] != PART_REF_NULL) {
                n->children[byte] = PART_REF_NULL;
                n->num_children--;
            }
            break;
        }
    }
}

// Find the single remaining child after removing `exclude_byte`.
static void find_remaining_child(const void *node, uint8_t exclude_byte,
                                  part_ref_t *out_child, uint8_t *out_byte) {
    *out_child = PART_REF_NULL;
    *out_byte = 0;
    switch (node_type(node)) {
        case PART_NODE_16: {
            const part_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] != exclude_byte) {
                    *out_child = n->children[i];
                    *out_byte = n->keys[i];
                    return;
                }
            }
            break;
        }
        case PART_NODE_64: {
            const part_node64_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] != exclude_byte) {
                    *out_child = n->children[i];
                    *out_byte = n->keys[i];
                    return;
                }
            }
            break;
        }
        case PART_NODE_256: {
            const part_node256_t *n = node;
            for (int b = 0; b < 256; b++) {
                if (b != exclude_byte && n->children[b] != PART_REF_NULL) {
                    *out_child = n->children[b];
                    *out_byte = (uint8_t)b;
                    return;
                }
            }
            break;
        }
    }
}

// ============================================================================
// COW Helpers
// ============================================================================

// COW: copy node, replace one child.
static part_ref_t cow_set_child(persistent_art_t *tree, part_ref_t ref,
                                 uint8_t byte, part_ref_t new_child) {
    part_ref_t new_ref = cow_copy_node(tree, ref);
    if (new_ref == PART_REF_NULL) return ref;

    void *node = node_ptr(tree, new_ref);
    switch (node_type(node)) {
        case PART_NODE_16: {
            part_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    n->children[i] = new_child;
                    return new_ref;
                }
            }
            break;
        }
        case PART_NODE_64: {
            part_node64_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] == byte) {
                    n->children[i] = new_child;
                    return new_ref;
                }
            }
            break;
        }
        case PART_NODE_256: {
            part_node256_t *n = node;
            n->children[byte] = new_child;
            return new_ref;
        }
    }
    return ref;
}

// COW: copy node, add new child (grow if full).
static part_ref_t cow_add_child(persistent_art_t *tree, part_ref_t ref,
                                 uint8_t byte, part_ref_t child) {
    void *old = node_ptr(tree, ref);
    part_node_type_t type = node_type(old);
    uint8_t nc = node_num_children(old);

    // Check if room in current type
    int max_c;
    switch (type) {
        case PART_NODE_16:  max_c = PART_NODE16_MAX; break;
        case PART_NODE_64:  max_c = PART_NODE64_MAX; break;
        case PART_NODE_256: max_c = PART_NODE256_MAX; break;
        default: return ref;
    }

    if (nc < max_c) {
        // Room available: copy and add
        part_ref_t new_ref = cow_copy_node(tree, ref);
        if (new_ref == PART_REF_NULL) return ref;
        node_add_child_inplace(node_ptr(tree, new_ref), byte, child);
        return new_ref;
    }

    // Need to grow
    part_node_type_t new_type;
    switch (type) {
        case PART_NODE_16:  new_type = PART_NODE_64; break;
        case PART_NODE_64:  new_type = PART_NODE_256; break;
        default: return ref;  // Node256 is max
    }

    part_ref_t new_ref = alloc_node(tree, new_type);
    if (new_ref == PART_REF_NULL) return ref;

    // Re-resolve old after alloc (mmap is stable, but be consistent)
    old = node_ptr(tree, ref);
    void *new_node = node_ptr(tree, new_ref);
    copy_header(new_node, old);

    // Copy children from old to new
    if (type == PART_NODE_16 && new_type == PART_NODE_64) {
        part_node16_t *n16 = old;
        part_node64_t *n64 = new_node;
        memcpy(n64->keys, n16->keys, n16->num_children);
        memcpy(n64->children, n16->children,
               n16->num_children * sizeof(uint32_t));
        n64->num_children = n16->num_children;
    } else if (type == PART_NODE_64 && new_type == PART_NODE_256) {
        part_node64_t *n64 = old;
        part_node256_t *n256 = new_node;
        for (int i = 0; i < n64->num_children; i++) {
            n256->children[n64->keys[i]] = n64->children[i];
        }
        n256->num_children = n64->num_children;
    }

    node_add_child_inplace(new_node, byte, child);
    return new_ref;
}

// Path collapse: merge parent prefix + byte + child prefix.
static part_ref_t do_path_collapse(persistent_art_t *tree, const void *parent,
                                    uint8_t byte, part_ref_t child,
                                    size_t depth) {
    (void)byte;
    if (PART_IS_LEAF_REF(child)) return child;

    part_ref_t cow_child = cow_copy_node(tree, child);
    if (cow_child == PART_REF_NULL) return child;

    void *child_node = node_ptr(tree, cow_child);
    uint8_t parent_plen = node_partial_len(parent);
    uint8_t child_plen = node_partial_len(child_node);
    uint32_t new_plen = (uint32_t)parent_plen + 1 + child_plen;

    if (new_plen <= 255) {
        part_ref_t min_leaf = find_minimum_leaf(tree, cow_child);
        const uint8_t *lk = leaf_key(tree, min_leaf);
        uint8_t *cp = node_partial_mut(child_node);
        size_t store = (size_t)new_plen < PART_MAX_PREFIX
                       ? (size_t)new_plen : PART_MAX_PREFIX;
        memcpy(cp, lk + depth, store);
        ((uint8_t *)child_node)[2] = (uint8_t)new_plen;
    }

    return cow_child;
}

// COW: copy node, remove one child (shrink if appropriate, collapse if 1 left).
static part_ref_t cow_remove_child(persistent_art_t *tree, part_ref_t ref,
                                    uint8_t byte, size_t depth) {
    void *old = node_ptr(tree, ref);
    part_node_type_t type = node_type(old);
    uint8_t nc = node_num_children(old);
    uint8_t new_nc = nc - 1;

    // Path collapse: exactly 1 child remaining
    if (new_nc == 1) {
        part_ref_t remaining;
        uint8_t remaining_byte;
        find_remaining_child(old, byte, &remaining, &remaining_byte);
        return do_path_collapse(tree, old, remaining_byte, remaining, depth);
    }

    // Shrink Node256 → Node64
    if (type == PART_NODE_256 && new_nc <= PART_NODE64_MAX) {
        part_ref_t new_ref = alloc_node(tree, PART_NODE_64);
        if (new_ref == PART_REF_NULL) goto fallback;
        part_node256_t *n256 = (part_node256_t *)old;
        part_node64_t *n64 = node_ptr(tree, new_ref);
        copy_header(n64, old);
        n64->num_children = 0;
        for (int b = 0; b < 256; b++) {
            if (b == byte) continue;
            if (n256->children[b] != PART_REF_NULL) {
                n64->keys[n64->num_children] = (uint8_t)b;
                n64->children[n64->num_children] = n256->children[b];
                n64->num_children++;
            }
        }
        return new_ref;
    }

    // Shrink Node64 → Node16
    if (type == PART_NODE_64 && new_nc <= PART_NODE16_MAX) {
        part_ref_t new_ref = alloc_node(tree, PART_NODE_16);
        if (new_ref == PART_REF_NULL) goto fallback;
        part_node64_t *n64 = (part_node64_t *)old;
        part_node16_t *n16 = node_ptr(tree, new_ref);
        copy_header(n16, old);
        n16->num_children = 0;
        for (int i = 0; i < n64->num_children; i++) {
            if (n64->keys[i] == byte) continue;
            n16->keys[n16->num_children] = n64->keys[i];
            n16->children[n16->num_children] = n64->children[i];
            n16->num_children++;
        }
        return new_ref;
    }

fallback: {
        // No shrink: copy and remove in-place
        part_ref_t new_ref = cow_copy_node(tree, ref);
        if (new_ref == PART_REF_NULL) return ref;
        node_remove_child_inplace(node_ptr(tree, new_ref), byte);
        return new_ref;
    }
}

// ============================================================================
// COW Insert (recursive)
// ============================================================================

static part_ref_t cow_insert(persistent_art_t *tree, part_ref_t ref,
                              const uint8_t *key, size_t depth,
                              const void *value, bool *inserted) {
    // Empty slot
    if (ref == PART_REF_NULL) {
        *inserted = true;
        return alloc_leaf(tree, key, value);
    }

    // Leaf
    if (PART_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            // Update — allocate new leaf (old becomes dead)
            *inserted = false;
            return alloc_leaf(tree, key, value);
        }

        // Split — create new inner node with two leaves
        *inserted = true;
        part_ref_t new_leaf = alloc_leaf(tree, key, value);
        if (new_leaf == PART_REF_NULL) return ref;

        const uint8_t *existing_key = leaf_key(tree, ref);

        size_t prefix_len = 0;
        size_t limit = tree->key_size;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == existing_key[depth + prefix_len]) {
            prefix_len++;
        }

        part_ref_t new_node_ref = alloc_node(tree, PART_NODE_16);
        if (new_node_ref == PART_REF_NULL) return ref;

        part_node16_t *nn = node_ptr(tree, new_node_ref);
        nn->partial_len = (uint8_t)prefix_len;
        if (prefix_len > 0) {
            size_t store = prefix_len < PART_MAX_PREFIX
                           ? prefix_len : PART_MAX_PREFIX;
            memcpy(nn->partial, key + depth, store);
        }

        size_t nd = depth + prefix_len;
        uint8_t nb = (nd < tree->key_size) ? key[nd] : 0x00;
        uint8_t ob = (nd < tree->key_size) ? existing_key[nd] : 0x00;

        node_add_child_inplace(nn, nb, new_leaf);
        node_add_child_inplace(nn, ob, ref);  // reuse old leaf ref
        return new_node_ref;
    }

    // Inner node
    void *node = node_ptr(tree, ref);
    uint8_t plen = node_partial_len(node);

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);

        if (prefix_len < plen) {
            // Prefix mismatch — split
            *inserted = true;

            const uint8_t *partial = node_partial(node);
            uint8_t old_byte;
            const uint8_t *leaf_k = NULL;
            if (prefix_len < PART_MAX_PREFIX) {
                old_byte = partial[prefix_len];
            } else {
                part_ref_t min_leaf = find_minimum_leaf(tree, ref);
                leaf_k = leaf_key(tree, min_leaf);
                old_byte = leaf_k[depth + prefix_len];
            }

            // New split node
            part_ref_t split_ref = alloc_node(tree, PART_NODE_16);
            if (split_ref == PART_REF_NULL) return ref;

            part_node16_t *split = node_ptr(tree, split_ref);
            split->partial_len = (uint8_t)prefix_len;
            {
                size_t store = (size_t)prefix_len < PART_MAX_PREFIX
                               ? (size_t)prefix_len : PART_MAX_PREFIX;
                memcpy(split->partial, key + depth, store);
            }

            // COW-copy old node with shortened prefix
            part_ref_t cow_ref = cow_copy_node(tree, ref);
            if (cow_ref == PART_REF_NULL) return ref;

            void *cow_node = node_ptr(tree, cow_ref);
            uint8_t *cow_partial = node_partial_mut(cow_node);
            uint8_t new_partial_len = plen - (prefix_len + 1);
            if (prefix_len + 1 < PART_MAX_PREFIX) {
                size_t shift = (size_t)prefix_len + 1;
                size_t avail = PART_MAX_PREFIX - shift;
                size_t keep = (size_t)new_partial_len < avail
                              ? (size_t)new_partial_len : avail;
                memmove(cow_partial, cow_partial + shift, keep);
            } else {
                if (!leaf_k) {
                    part_ref_t ml = find_minimum_leaf(tree, cow_ref);
                    leaf_k = leaf_key(tree, ml);
                }
                size_t s = (size_t)new_partial_len < PART_MAX_PREFIX
                           ? (size_t)new_partial_len : PART_MAX_PREFIX;
                memcpy(cow_partial, leaf_k + depth + prefix_len + 1, s);
            }
            ((uint8_t *)cow_node)[2] = new_partial_len;

            // New leaf
            part_ref_t leaf = alloc_leaf(tree, key, value);
            if (leaf == PART_REF_NULL) return ref;

            uint8_t new_byte = (depth + prefix_len < tree->key_size) ?
                                key[depth + prefix_len] : 0x00;

            // Re-resolve split node (node_ptr is stable with mmap)
            split = node_ptr(tree, split_ref);
            node_add_child_inplace(split, old_byte, cow_ref);
            node_add_child_inplace(split, new_byte, leaf);

            return split_ref;
        }

        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    part_ref_t child = find_child(tree, ref, byte);

    if (child != PART_REF_NULL) {
        if (depth >= tree->key_size) {
            if (PART_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
                *inserted = false;
                part_ref_t nl = alloc_leaf(tree, key, value);
                if (nl == PART_REF_NULL) return ref;
                return cow_set_child(tree, ref, byte, nl);
            }
            return ref;
        }

        part_ref_t new_child = cow_insert(tree, child, key, depth + 1,
                                           value, inserted);
        if (new_child != child) {
            return cow_set_child(tree, ref, byte, new_child);
        }
        return ref;
    } else {
        *inserted = true;
        part_ref_t leaf = alloc_leaf(tree, key, value);
        if (leaf == PART_REF_NULL) return ref;
        return cow_add_child(tree, ref, byte, leaf);
    }
}

// ============================================================================
// COW Delete (recursive)
// ============================================================================

static part_ref_t cow_delete(persistent_art_t *tree, part_ref_t ref,
                              const uint8_t *key, size_t depth,
                              bool *deleted) {
    if (ref == PART_REF_NULL) return PART_REF_NULL;

    if (PART_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            *deleted = true;
            return PART_REF_NULL;
        }
        return ref;
    }

    void *node = node_ptr(tree, ref);
    uint8_t plen = node_partial_len(node);

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);
        if (prefix_len != plen) return ref;
        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    part_ref_t child = find_child(tree, ref, byte);
    if (child == PART_REF_NULL) return ref;

    if (depth >= tree->key_size) {
        if (PART_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
            *deleted = true;
            return cow_remove_child(tree, ref, byte, depth);
        }
        return ref;
    }

    part_ref_t new_child = cow_delete(tree, child, key, depth + 1, deleted);

    if (new_child != child) {
        if (new_child == PART_REF_NULL) {
            return cow_remove_child(tree, ref, byte, depth);
        } else {
            return cow_set_child(tree, ref, byte, new_child);
        }
    }

    return ref;
}

// ============================================================================
// search — iterative lookup
// ============================================================================

static const void *search(const persistent_art_t *tree, part_ref_t ref,
                           const uint8_t *key, size_t depth) {
    while (ref != PART_REF_NULL) {
        if (PART_IS_LEAF_REF(ref)) {
            if (leaf_matches(tree, ref, key))
                return leaf_value(tree, ref);
            return NULL;
        }

        void *node = node_ptr(tree, ref);
        uint8_t plen = node_partial_len(node);
        if (plen > 0) {
            int prefix_len = check_prefix(tree, ref, node, key,
                                           tree->key_size, depth);
            if (prefix_len != plen) return NULL;
            depth += plen;
        }

        uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
        part_ref_t child = find_child(tree, ref, byte);
        if (child == PART_REF_NULL) return NULL;

        if (depth >= tree->key_size) {
            if (PART_IS_LEAF_REF(child) && leaf_matches(tree, child, key))
                return leaf_value(tree, child);
            return NULL;
        }

        ref = child;
        depth++;
    }
    return NULL;
}

// ============================================================================
// Meta Page Read/Write
// ============================================================================

static bool meta_read(int fd, int page, part_meta_t *meta) {
    off_t offset = (page == 0) ? PART_META_A_OFFSET : PART_META_B_OFFSET;
    uint8_t buf[PART_META_SIZE];

    ssize_t r = pread(fd, buf, PART_META_SIZE, offset);
    if (r != PART_META_SIZE) return false;

    memcpy(meta, buf, sizeof(part_meta_t));

    if (meta->magic != PART_META_MAGIC) return false;

    uint32_t expected = part_crc32c(meta, PART_META_CRC_LEN);
    if (meta->crc32c != expected) return false;

    return true;
}

static bool meta_write(int fd, int page, const part_meta_t *meta) {
    off_t offset = (page == 0) ? PART_META_A_OFFSET : PART_META_B_OFFSET;
    uint8_t buf[PART_META_SIZE];
    memset(buf, 0, PART_META_SIZE);
    memcpy(buf, meta, sizeof(part_meta_t));

    ssize_t w = pwrite(fd, buf, PART_META_SIZE, offset);
    return w == PART_META_SIZE;
}

// ============================================================================
// Lifecycle
// ============================================================================

bool part_open(persistent_art_t *tree, const char *path,
               uint32_t key_size, uint32_t value_size) {
    if (!tree || !path || key_size == 0) return false;
    memset(tree, 0, sizeof(*tree));
    tree->fd = -1;
    tree->key_size = key_size;
    tree->value_size = value_size;
    tree->leaf_size = key_size + value_size;

    bool created = false;
    int fd = open(path, O_RDWR, 0644);
    if (fd < 0) {
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) return false;
        created = true;
    }

    // Ensure file is large enough (sparse)
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < (off_t)PART_TOTAL_FILE_SIZE) {
        if (ftruncate(fd, (off_t)PART_TOTAL_FILE_SIZE) != 0) {
            close(fd);
            return false;
        }
    }

    // mmap node pool
    tree->node_base = mmap(NULL, PART_NODE_POOL_MAX,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, PART_DATA_OFFSET);
    if (tree->node_base == MAP_FAILED) {
        close(fd);
        return false;
    }

    // mmap leaf pool
    tree->leaf_base = mmap(NULL, PART_LEAF_POOL_MAX,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, PART_LEAF_POOL_OFFSET);
    if (tree->leaf_base == MAP_FAILED) {
        munmap(tree->node_base, PART_NODE_POOL_MAX);
        close(fd);
        return false;
    }

    tree->fd = fd;

    if (created || file_size == 0) {
        // Initialize new tree
        tree->root = PART_REF_NULL;
        tree->size = 0;
        tree->node_used = 8;   // skip offset 0 so ref=0 means NULL
        tree->leaf_count = 0;
        tree->generation = 0;
        tree->active_meta = 0;

        // Write initial meta to page A
        part_meta_t meta = {0};
        meta.magic = PART_META_MAGIC;
        meta.generation = 1;
        meta.root = PART_REF_NULL;
        meta.size = 0;
        meta.node_used = 8;
        meta.leaf_count = 0;
        meta.key_size = key_size;
        meta.value_size = value_size;
        meta.crc32c = part_crc32c(&meta, PART_META_CRC_LEN);

        if (!meta_write(fd, 0, &meta)) {
            part_close(tree);
            return false;
        }
        if (fdatasync(fd) != 0) {
            part_close(tree);
            return false;
        }

        tree->generation = 1;
        tree->active_meta = 0;
    } else {
        // Recovery: read both meta pages, pick highest valid generation
        part_meta_t meta_a = {0}, meta_b = {0};
        bool a_valid = meta_read(fd, 0, &meta_a);
        bool b_valid = meta_read(fd, 1, &meta_b);

        part_meta_t *active = NULL;
        int active_page = -1;

        if (a_valid && b_valid) {
            if (meta_a.generation >= meta_b.generation) {
                active = &meta_a; active_page = 0;
            } else {
                active = &meta_b; active_page = 1;
            }
        } else if (a_valid) {
            active = &meta_a; active_page = 0;
        } else if (b_valid) {
            active = &meta_b; active_page = 1;
        } else {
            part_close(tree);
            return false;
        }

        // Validate key/value sizes
        if (active->key_size != key_size || active->value_size != value_size) {
            part_close(tree);
            return false;
        }

        tree->root = active->root;
        tree->size = active->size;
        tree->node_used = (size_t)active->node_used;
        tree->leaf_count = active->leaf_count;
        tree->generation = active->generation;
        tree->active_meta = active_page;
    }

    // Snapshot committed state
    tree->committed_root = tree->root;
    tree->committed_size = tree->size;
    tree->committed_node_used = tree->node_used;
    tree->committed_leaf_count = tree->leaf_count;

    return true;
}

void part_close(persistent_art_t *tree) {
    if (!tree) return;
    if (tree->leaf_base && tree->leaf_base != MAP_FAILED)
        munmap(tree->leaf_base, PART_LEAF_POOL_MAX);
    if (tree->node_base && tree->node_base != MAP_FAILED)
        munmap(tree->node_base, PART_NODE_POOL_MAX);
    if (tree->fd >= 0) close(tree->fd);
    memset(tree, 0, sizeof(*tree));
    tree->fd = -1;
}

// ============================================================================
// Public API
// ============================================================================

bool part_insert(persistent_art_t *tree, const uint8_t *key,
                  const void *value) {
    if (!tree || !key || !value) return false;

    bool inserted = false;
    tree->root = cow_insert(tree, tree->root, key, 0, value, &inserted);
    if (inserted) tree->size++;
    return tree->root != PART_REF_NULL;
}

bool part_delete(persistent_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return false;

    bool deleted = false;
    tree->root = cow_delete(tree, tree->root, key, 0, &deleted);
    if (deleted) tree->size--;
    return deleted;
}

const void *part_get(const persistent_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return NULL;
    return search(tree, tree->root, key, 0);
}

bool part_contains(const persistent_art_t *tree, const uint8_t *key) {
    return part_get(tree, key) != NULL;
}

size_t part_size(const persistent_art_t *tree) {
    return tree ? tree->size : 0;
}

// ============================================================================
// Commit / Rollback
// ============================================================================

bool part_commit(persistent_art_t *tree) {
    if (!tree || tree->fd < 0) return false;

    // Phase 1: flush all dirty mmap'd data pages
    if (fdatasync(tree->fd) != 0) return false;

    // Phase 2: write meta to inactive page
    int inactive = 1 - tree->active_meta;
    tree->generation++;

    part_meta_t meta = {0};
    meta.magic = PART_META_MAGIC;
    meta.generation = tree->generation;
    meta.root = tree->root;
    meta.size = tree->size;
    meta.node_used = (uint64_t)tree->node_used;
    meta.leaf_count = tree->leaf_count;
    meta.key_size = tree->key_size;
    meta.value_size = tree->value_size;
    meta.crc32c = part_crc32c(&meta, PART_META_CRC_LEN);

    if (!meta_write(tree->fd, inactive, &meta)) {
        tree->generation--;
        return false;
    }

    // Phase 3: flush meta page
    if (fdatasync(tree->fd) != 0) {
        tree->generation--;
        return false;
    }

    // Phase 4: update committed state
    tree->active_meta = inactive;
    tree->committed_root = tree->root;
    tree->committed_size = tree->size;
    tree->committed_node_used = tree->node_used;
    tree->committed_leaf_count = tree->leaf_count;

    return true;
}

void part_rollback(persistent_art_t *tree) {
    if (!tree) return;
    tree->root = tree->committed_root;
    tree->size = tree->committed_size;
    tree->node_used = tree->committed_node_used;
    tree->leaf_count = tree->committed_leaf_count;
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
        part_ref_t ref;
        int child_idx;
    } stack[64];
    int depth;
} part_iter_state_t;

struct part_iterator {
    const persistent_art_t *tree;
    part_iter_state_t state;
};

part_iterator_t *part_iterator_create(const persistent_art_t *tree) {
    if (!tree) return NULL;

    part_iterator_t *iter = malloc(sizeof(part_iterator_t));
    if (!iter) return NULL;

    iter->tree = tree;
    memset(&iter->state, 0, sizeof(part_iter_state_t));
    iter->state.done = (tree->root == PART_REF_NULL);
    iter->state.depth = -1;

    return iter;
}

bool part_iterator_next(part_iterator_t *iter) {
    if (!iter) return false;

    part_iter_state_t *s = &iter->state;
    if (s->done) return false;

    if (!s->started) {
        s->started = true;
        if (iter->tree->root == PART_REF_NULL) {
            s->done = true;
            return false;
        }
        s->depth = 0;
        s->stack[0].ref = iter->tree->root;
        s->stack[0].child_idx = -1;
    }

    while (s->depth >= 0) {
        part_ref_t ref = s->stack[s->depth].ref;

        if (PART_IS_LEAF_REF(ref)) {
            s->key = leaf_key(iter->tree, ref);
            s->value = leaf_value(iter->tree, ref);
            s->depth--;
            return true;
        }

        int *ci = &s->stack[s->depth].child_idx;
        (*ci)++;

        void *node = node_ptr(iter->tree, ref);
        part_ref_t next_child = PART_REF_NULL;

        switch (node_type(node)) {
            case PART_NODE_16: {
                part_node16_t *n = node;
                if (*ci < n->num_children)
                    next_child = n->children[*ci];
                break;
            }
            case PART_NODE_64: {
                part_node64_t *n = node;
                if (*ci < n->num_children)
                    next_child = n->children[*ci];
                break;
            }
            case PART_NODE_256: {
                part_node256_t *n = node;
                while (*ci < 256) {
                    if (n->children[*ci] != PART_REF_NULL) {
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

        if (next_child != PART_REF_NULL) {
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

const uint8_t *part_iterator_key(const part_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.key;
}

const void *part_iterator_value(const part_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.value;
}

bool part_iterator_done(const part_iterator_t *iter) {
    return iter ? iter->state.done : true;
}

void part_iterator_destroy(part_iterator_t *iter) {
    free(iter);
}

// ============================================================================
// Iterator Seek (lower-bound)
// ============================================================================

static int compare_prefix_seek(const persistent_art_t *tree, part_ref_t ref,
                                const void *node, const uint8_t *key,
                                uint32_t key_size, size_t depth) {
    uint8_t plen = node_partial_len(node);
    const uint8_t *partial = node_partial(node);
    int cmp_len = plen;
    if (depth + (size_t)cmp_len > key_size)
        cmp_len = (int)key_size - (int)depth;

    int stored = cmp_len < PART_MAX_PREFIX ? cmp_len : PART_MAX_PREFIX;
    for (int i = 0; i < stored; i++) {
        if (partial[i] != key[depth + i])
            return (int)partial[i] - (int)key[depth + i];
    }

    if (cmp_len > PART_MAX_PREFIX) {
        part_ref_t min_leaf = find_minimum_leaf(tree, ref);
        if (min_leaf == PART_REF_NULL) return 0;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int i = PART_MAX_PREFIX; i < cmp_len; i++) {
            if (lk[depth + i] != key[depth + i])
                return (int)lk[depth + i] - (int)key[depth + i];
        }
    }

    return 0;
}

static part_ref_t find_ge_child(const persistent_art_t *tree,
                                 part_ref_t node_ref,
                                 uint8_t target,
                                 int *out_child_idx,
                                 uint8_t *out_actual_byte) {
    void *node = node_ptr(tree, node_ref);
    switch (node_type(node)) {
        case PART_NODE_16: {
            part_node16_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] >= target) {
                    *out_child_idx = i;
                    *out_actual_byte = n->keys[i];
                    return n->children[i];
                }
            }
            return PART_REF_NULL;
        }
        case PART_NODE_64: {
            part_node64_t *n = node;
            for (int i = 0; i < n->num_children; i++) {
                if (n->keys[i] >= target) {
                    *out_child_idx = i;
                    *out_actual_byte = n->keys[i];
                    return n->children[i];
                }
            }
            return PART_REF_NULL;
        }
        case PART_NODE_256: {
            part_node256_t *n = node;
            for (int b = target; b < 256; b++) {
                if (n->children[b] != PART_REF_NULL) {
                    *out_child_idx = b;
                    *out_actual_byte = (uint8_t)b;
                    return n->children[b];
                }
            }
            return PART_REF_NULL;
        }
        default:
            return PART_REF_NULL;
    }
}

static bool descend_to_min_leaf(const persistent_art_t *tree,
                                 part_iter_state_t *s,
                                 part_ref_t ref) {
    while (ref != PART_REF_NULL) {
        if (PART_IS_LEAF_REF(ref)) {
            s->depth++;
            if (s->depth >= 64) { s->done = true; return false; }
            s->stack[s->depth].ref = ref;
            s->stack[s->depth].child_idx = -1;
            s->key = leaf_key(tree, ref);
            s->value = leaf_value(tree, ref);
            return true;
        }

        void *node = node_ptr(tree, ref);
        int first_idx = -1;
        part_ref_t first_child = PART_REF_NULL;

        switch (node_type(node)) {
            case PART_NODE_16: {
                part_node16_t *n = node;
                if (n->num_children > 0) {
                    first_idx = 0;
                    first_child = n->children[0];
                }
                break;
            }
            case PART_NODE_64: {
                part_node64_t *n = node;
                if (n->num_children > 0) {
                    first_idx = 0;
                    first_child = n->children[0];
                }
                break;
            }
            case PART_NODE_256: {
                part_node256_t *n = node;
                for (int b = 0; b < 256; b++) {
                    if (n->children[b] != PART_REF_NULL) {
                        first_idx = b;
                        first_child = n->children[b];
                        break;
                    }
                }
                break;
            }
            default:
                return false;
        }

        if (first_child == PART_REF_NULL) return false;

        s->depth++;
        if (s->depth >= 64) { s->done = true; return false; }
        s->stack[s->depth].ref = ref;
        s->stack[s->depth].child_idx = first_idx;
        ref = first_child;
    }
    return false;
}

bool part_iterator_seek(part_iterator_t *iter, const uint8_t *key) {
    if (!iter || !key) return false;

    const persistent_art_t *tree = iter->tree;
    part_iter_state_t *s = &iter->state;

    memset(s, 0, sizeof(part_iter_state_t));
    s->depth = -1;
    s->started = true;

    if (tree->root == PART_REF_NULL) {
        s->done = true;
        return false;
    }

    part_ref_t ref = tree->root;
    size_t depth = 0;

    while (ref != PART_REF_NULL) {
        if (PART_IS_LEAF_REF(ref)) {
            const uint8_t *lk = leaf_key(tree, ref);
            if (memcmp(lk, key, tree->key_size) >= 0) {
                s->depth++;
                if (s->depth >= 64) { s->done = true; return false; }
                s->stack[s->depth].ref = ref;
                s->stack[s->depth].child_idx = -1;
                s->key = lk;
                s->value = leaf_value(tree, ref);
                return true;
            }
            goto backtrack;
        }

        void *node = node_ptr(tree, ref);
        uint8_t plen = node_partial_len(node);

        if (plen > 0) {
            int cmp = compare_prefix_seek(tree, ref, node, key,
                                           tree->key_size, depth);
            if (cmp > 0) return descend_to_min_leaf(tree, s, ref);
            if (cmp < 0) goto backtrack;
            depth += plen;
        }

        if (depth >= tree->key_size)
            return descend_to_min_leaf(tree, s, ref);

        int child_idx;
        uint8_t actual_byte;
        part_ref_t child = find_ge_child(tree, ref, key[depth],
                                          &child_idx, &actual_byte);

        if (child == PART_REF_NULL) goto backtrack;

        s->depth++;
        if (s->depth >= 64) { s->done = true; return false; }
        s->stack[s->depth].ref = ref;
        s->stack[s->depth].child_idx = child_idx;

        if (actual_byte > key[depth])
            return descend_to_min_leaf(tree, s, child);

        depth++;
        ref = child;
        continue;

    backtrack:
        while (s->depth >= 0) {
            part_ref_t parent_ref = s->stack[s->depth].ref;
            int ci = s->stack[s->depth].child_idx;
            int next_ci = ci + 1;
            part_ref_t next_child = PART_REF_NULL;

            void *pnode = node_ptr(tree, parent_ref);
            switch (node_type(pnode)) {
                case PART_NODE_16: {
                    part_node16_t *n = pnode;
                    if (next_ci < n->num_children)
                        next_child = n->children[next_ci];
                    break;
                }
                case PART_NODE_64: {
                    part_node64_t *n = pnode;
                    if (next_ci < n->num_children)
                        next_child = n->children[next_ci];
                    break;
                }
                case PART_NODE_256: {
                    part_node256_t *n = pnode;
                    for (int b = next_ci; b < 256; b++) {
                        if (n->children[b] != PART_REF_NULL) {
                            next_child = n->children[b];
                            next_ci = b;
                            break;
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            if (next_child != PART_REF_NULL) {
                s->stack[s->depth].child_idx = next_ci;
                return descend_to_min_leaf(tree, s, next_child);
            }

            s->depth--;
        }

        s->done = true;
        return false;
    }

    s->done = true;
    return false;
}
