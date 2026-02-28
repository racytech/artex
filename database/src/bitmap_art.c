#include "../include/bitmap_art.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <nmmintrin.h>  // SSE4.2 (CRC32C)

// ============================================================================
// Constants
// ============================================================================

#define BART_META_SIZE        4096
#define BART_META_A_OFFSET    0
#define BART_META_B_OFFSET    4096
#define BART_DATA_OFFSET      8192

#define BART_NODE_POOL_MAX    (16ULL * 1024 * 1024 * 1024)  // 16 GB
#define BART_LEAF_POOL_OFFSET (BART_DATA_OFFSET + BART_NODE_POOL_MAX)
#define BART_LEAF_POOL_MAX    (64ULL * 1024 * 1024 * 1024)  // 64 GB
#define BART_TOTAL_FILE_SIZE  (BART_LEAF_POOL_OFFSET + BART_LEAF_POOL_MAX)

#define BART_MAX_PREFIX       8
#define BART_META_MAGIC       0x5452415F54524142ULL  // "BART_ART"

// ============================================================================
// Node Structure (single type — bitmap + packed children)
// ============================================================================

typedef struct {
    uint16_t num_children;              // 2
    uint8_t  partial_len;               // 1
    uint8_t  partial[BART_MAX_PREFIX];  // 8
    uint8_t  _pad[1];                   // 1 (align bitmap to 4 bytes)
    uint32_t bitmap[8];                 // 32 (256 bits)
    uint32_t children[];                // 4 × num_children (flexible array)
} bart_node_t;

// Header size: 2+1+8+1+32 = 44 bytes
#define BART_NODE_HEADER_SIZE  44

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
} bart_meta_t;

#define BART_META_CRC_LEN  (offsetof(bart_meta_t, crc32c))

// ============================================================================
// CRC32C (hardware-accelerated via SSE4.2)
// ============================================================================

static uint32_t bart_crc32c(const void *data, size_t len) {
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
// Bitmap Helpers (256-bit bitmap as uint32_t[8])
// ============================================================================

static inline bool bitmap_test(const uint32_t *bm, uint8_t byte) {
    return (bm[byte >> 5] >> (byte & 31)) & 1;
}

static inline void bitmap_set(uint32_t *bm, uint8_t byte) {
    bm[byte >> 5] |= (1u << (byte & 31));
}

static inline void bitmap_clear(uint32_t *bm, uint8_t byte) {
    bm[byte >> 5] &= ~(1u << (byte & 31));
}

// Number of set bits before position `byte` (rank = index into children[]).
static inline int bitmap_rank(const uint32_t *bm, uint8_t byte) {
    int rank = 0;
    int word = byte >> 5;
    for (int i = 0; i < word; i++)
        rank += __builtin_popcount(bm[i]);
    uint32_t mask = (1u << (byte & 31)) - 1;
    rank += __builtin_popcount(bm[word] & mask);
    return rank;
}

// Next set bit >= start, or 256 if none.
static inline int bitmap_next(const uint32_t *bm, int start) {
    if (start >= 256) return 256;
    int word = start >> 5;
    int bit = start & 31;
    uint32_t w = bm[word] & ~((1u << bit) - 1);
    while (word < 8) {
        if (w) return (word << 5) + __builtin_ctz(w);
        word++;
        w = (word < 8) ? bm[word] : 0;
    }
    return 256;
}

// First set bit, or 256 if empty.
static inline int bitmap_first(const uint32_t *bm) {
    return bitmap_next(bm, 0);
}

// ============================================================================
// Ref Resolution
// ============================================================================

static inline bart_node_t *node_ptr(const bitmap_art_t *tree, bart_ref_t ref) {
    return (bart_node_t *)(tree->node_base + (size_t)ref * 8);
}

static inline uint8_t *leaf_ptr(const bitmap_art_t *tree, bart_ref_t ref) {
    return tree->leaf_base +
           (size_t)BART_LEAF_INDEX(ref) * tree->leaf_size;
}

static inline const uint8_t *leaf_key(const bitmap_art_t *tree,
                                       bart_ref_t ref) {
    return leaf_ptr(tree, ref);
}

static inline const void *leaf_value(const bitmap_art_t *tree,
                                      bart_ref_t ref) {
    return leaf_ptr(tree, ref) + tree->key_size;
}

static inline bool leaf_matches(const bitmap_art_t *tree,
                                 bart_ref_t ref, const uint8_t *key) {
    return memcmp(leaf_key(tree, ref), key, tree->key_size) == 0;
}

// ============================================================================
// Allocation (bump, COW-safe — never reused in v1)
// ============================================================================

static inline size_t node_alloc_size(uint16_t num_children) {
    size_t size = BART_NODE_HEADER_SIZE +
                  (size_t)num_children * sizeof(uint32_t);
    return (size + 7) & ~(size_t)7;  // 8-byte aligned
}

static bart_ref_t alloc_node(bitmap_art_t *tree, uint16_t num_children) {
    size_t size = node_alloc_size(num_children);

    size_t aligned = (tree->node_used + 7) & ~(size_t)7;
    if (aligned + size > BART_NODE_POOL_MAX) return BART_REF_NULL;

    void *node = tree->node_base + aligned;
    memset(node, 0, size);

    tree->node_used = aligned + size;
    return (bart_ref_t)(aligned / 8);
}

static bart_ref_t alloc_leaf(bitmap_art_t *tree,
                              const uint8_t *key, const void *value) {
    uint32_t idx = tree->leaf_count;
    size_t offset = (size_t)idx * tree->leaf_size;
    if (offset + tree->leaf_size > BART_LEAF_POOL_MAX)
        return BART_REF_NULL;

    tree->leaf_count++;
    uint8_t *leaf = tree->leaf_base + offset;
    memcpy(leaf, key, tree->key_size);
    memcpy(leaf + tree->key_size, value, tree->value_size);

    return BART_MAKE_LEAF_REF(idx);
}

// ============================================================================
// Node Accessors
// ============================================================================

static inline bart_ref_t find_child(const bitmap_art_t *tree,
                                     bart_ref_t node_ref, uint8_t byte) {
    bart_node_t *n = node_ptr(tree, node_ref);
    if (!bitmap_test(n->bitmap, byte)) return BART_REF_NULL;
    return n->children[bitmap_rank(n->bitmap, byte)];
}

static bart_ref_t find_minimum_leaf(const bitmap_art_t *tree,
                                     bart_ref_t ref) {
    while (ref != BART_REF_NULL && !BART_IS_LEAF_REF(ref)) {
        bart_node_t *n = node_ptr(tree, ref);
        int first = bitmap_first(n->bitmap);
        if (first >= 256) return BART_REF_NULL;
        ref = n->children[0];  // first child is always at array index 0
    }
    return ref;
}

// ============================================================================
// check_prefix — compare compressed path against key (optimistic)
// ============================================================================

static int check_prefix(const bitmap_art_t *tree, bart_ref_t ref,
                         const bart_node_t *node, const uint8_t *key,
                         uint32_t key_size, size_t depth) {
    uint8_t plen = node->partial_len;
    int max_cmp = plen;
    if (depth + (size_t)max_cmp > key_size)
        max_cmp = (int)key_size - (int)depth;

    int stored = max_cmp < BART_MAX_PREFIX ? max_cmp : BART_MAX_PREFIX;
    for (int idx = 0; idx < stored; idx++) {
        if (node->partial[idx] != key[depth + idx]) return idx;
    }

    if (max_cmp > BART_MAX_PREFIX) {
        bart_ref_t min_leaf = find_minimum_leaf(tree, ref);
        if (min_leaf == BART_REF_NULL) return BART_MAX_PREFIX;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int idx = BART_MAX_PREFIX; idx < max_cmp; idx++) {
            if (lk[depth + idx] != key[depth + idx]) return idx;
        }
    }

    return max_cmp;
}

// ============================================================================
// COW Helpers
// ============================================================================

// COW: copy node, replace one child.
static bart_ref_t cow_set_child(bitmap_art_t *tree, bart_ref_t ref,
                                 uint8_t byte, bart_ref_t new_child) {
    bart_node_t *old = node_ptr(tree, ref);
    uint16_t nc = old->num_children;

    bart_ref_t new_ref = alloc_node(tree, nc);
    if (new_ref == BART_REF_NULL) return ref;

    old = node_ptr(tree, ref);  // re-resolve after alloc
    bart_node_t *nn = node_ptr(tree, new_ref);

    size_t copy_size = BART_NODE_HEADER_SIZE + (size_t)nc * sizeof(uint32_t);
    memcpy(nn, old, copy_size);

    int idx = bitmap_rank(nn->bitmap, byte);
    nn->children[idx] = new_child;

    return new_ref;
}

// COW: copy node, add new child.
static bart_ref_t cow_add_child(bitmap_art_t *tree, bart_ref_t ref,
                                 uint8_t byte, bart_ref_t child) {
    bart_node_t *old = node_ptr(tree, ref);
    uint16_t nc = old->num_children;

    bart_ref_t new_ref = alloc_node(tree, nc + 1);
    if (new_ref == BART_REF_NULL) return ref;

    old = node_ptr(tree, ref);  // re-resolve
    bart_node_t *nn = node_ptr(tree, new_ref);

    // Copy header
    nn->num_children = nc + 1;
    nn->partial_len = old->partial_len;
    memcpy(nn->partial, old->partial, BART_MAX_PREFIX);
    memcpy(nn->bitmap, old->bitmap, sizeof(nn->bitmap));

    // Set the new bit and compute insertion rank
    bitmap_set(nn->bitmap, byte);
    int rank = bitmap_rank(nn->bitmap, byte);

    // Copy children with insertion at rank position
    memcpy(nn->children, old->children, (size_t)rank * sizeof(uint32_t));
    nn->children[rank] = child;
    memcpy(nn->children + rank + 1, old->children + rank,
           (size_t)(nc - rank) * sizeof(uint32_t));

    return new_ref;
}

// Path collapse: merge parent prefix + byte + child prefix.
static bart_ref_t do_path_collapse(bitmap_art_t *tree,
                                    const bart_node_t *parent,
                                    bart_ref_t child, size_t depth) {
    if (BART_IS_LEAF_REF(child)) return child;

    // COW-copy child node (same size)
    bart_node_t *old_child = node_ptr(tree, child);
    uint16_t cc = old_child->num_children;

    bart_ref_t cow_child = alloc_node(tree, cc);
    if (cow_child == BART_REF_NULL) return child;

    old_child = node_ptr(tree, child);  // re-resolve
    bart_node_t *cn = node_ptr(tree, cow_child);
    size_t copy_size = BART_NODE_HEADER_SIZE + (size_t)cc * sizeof(uint32_t);
    memcpy(cn, old_child, copy_size);

    uint8_t parent_plen = parent->partial_len;
    uint8_t child_plen = cn->partial_len;
    uint32_t new_plen = (uint32_t)parent_plen + 1 + child_plen;

    if (new_plen <= 255) {
        bart_ref_t min_leaf = find_minimum_leaf(tree, cow_child);
        const uint8_t *lk = leaf_key(tree, min_leaf);
        size_t store = (size_t)new_plen < BART_MAX_PREFIX
                       ? (size_t)new_plen : BART_MAX_PREFIX;
        memcpy(cn->partial, lk + depth - parent_plen, store);
        cn->partial_len = (uint8_t)new_plen;
    }

    return cow_child;
}

// COW: copy node, remove one child (collapse if 1 left).
static bart_ref_t cow_remove_child(bitmap_art_t *tree, bart_ref_t ref,
                                    uint8_t byte, size_t depth) {
    bart_node_t *old = node_ptr(tree, ref);
    uint16_t nc = old->num_children;

    // Path collapse: exactly 1 child remaining
    if (nc == 2) {
        int rank = bitmap_rank(old->bitmap, byte);
        bart_ref_t remaining = old->children[rank == 0 ? 1 : 0];
        return do_path_collapse(tree, old, remaining, depth);
    }

    bart_ref_t new_ref = alloc_node(tree, nc - 1);
    if (new_ref == BART_REF_NULL) return ref;

    old = node_ptr(tree, ref);  // re-resolve
    bart_node_t *nn = node_ptr(tree, new_ref);

    int rank = bitmap_rank(old->bitmap, byte);

    nn->num_children = nc - 1;
    nn->partial_len = old->partial_len;
    memcpy(nn->partial, old->partial, BART_MAX_PREFIX);
    memcpy(nn->bitmap, old->bitmap, sizeof(nn->bitmap));
    bitmap_clear(nn->bitmap, byte);

    // Copy children, skipping the removed one
    memcpy(nn->children, old->children, (size_t)rank * sizeof(uint32_t));
    memcpy(nn->children + rank, old->children + rank + 1,
           (size_t)(nc - rank - 1) * sizeof(uint32_t));

    return new_ref;
}

// ============================================================================
// COW Insert (recursive)
// ============================================================================

static bart_ref_t cow_insert(bitmap_art_t *tree, bart_ref_t ref,
                              const uint8_t *key, size_t depth,
                              const void *value, bool *inserted) {
    // Empty slot
    if (ref == BART_REF_NULL) {
        *inserted = true;
        return alloc_leaf(tree, key, value);
    }

    // Leaf
    if (BART_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            // Update — allocate new leaf (old becomes dead)
            *inserted = false;
            return alloc_leaf(tree, key, value);
        }

        // Split — create new inner node with two leaves
        *inserted = true;
        bart_ref_t new_leaf = alloc_leaf(tree, key, value);
        if (new_leaf == BART_REF_NULL) return ref;

        const uint8_t *existing_key = leaf_key(tree, ref);

        size_t prefix_len = 0;
        size_t limit = tree->key_size;
        while (depth + prefix_len < limit &&
               key[depth + prefix_len] == existing_key[depth + prefix_len]) {
            prefix_len++;
        }

        bart_ref_t new_node_ref = alloc_node(tree, 2);
        if (new_node_ref == BART_REF_NULL) return ref;

        bart_node_t *nn = node_ptr(tree, new_node_ref);
        nn->num_children = 2;
        nn->partial_len = (uint8_t)prefix_len;
        if (prefix_len > 0) {
            size_t store = prefix_len < BART_MAX_PREFIX
                           ? prefix_len : BART_MAX_PREFIX;
            memcpy(nn->partial, key + depth, store);
        }

        size_t nd = depth + prefix_len;
        uint8_t nb = (nd < tree->key_size) ? key[nd] : 0x00;
        uint8_t ob = (nd < tree->key_size) ? existing_key[nd] : 0x00;

        bitmap_set(nn->bitmap, nb);
        bitmap_set(nn->bitmap, ob);

        int rank_nb = bitmap_rank(nn->bitmap, nb);
        int rank_ob = bitmap_rank(nn->bitmap, ob);
        nn->children[rank_nb] = new_leaf;
        nn->children[rank_ob] = ref;  // reuse old leaf ref

        return new_node_ref;
    }

    // Inner node
    bart_node_t *node = node_ptr(tree, ref);
    uint8_t plen = node->partial_len;

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);

        if (prefix_len < plen) {
            // Prefix mismatch — split
            *inserted = true;

            uint8_t old_byte;
            const uint8_t *leaf_k = NULL;
            if (prefix_len < BART_MAX_PREFIX) {
                old_byte = node->partial[prefix_len];
            } else {
                bart_ref_t min_leaf = find_minimum_leaf(tree, ref);
                leaf_k = leaf_key(tree, min_leaf);
                old_byte = leaf_k[depth + prefix_len];
            }

            // New split node with 2 children
            bart_ref_t split_ref = alloc_node(tree, 2);
            if (split_ref == BART_REF_NULL) return ref;

            bart_node_t *split = node_ptr(tree, split_ref);
            split->num_children = 2;
            split->partial_len = (uint8_t)prefix_len;
            {
                size_t store = (size_t)prefix_len < BART_MAX_PREFIX
                               ? (size_t)prefix_len : BART_MAX_PREFIX;
                memcpy(split->partial, key + depth, store);
            }

            // COW-copy old node with shortened prefix
            node = node_ptr(tree, ref);  // re-resolve
            uint16_t old_nc = node->num_children;

            bart_ref_t cow_ref = alloc_node(tree, old_nc);
            if (cow_ref == BART_REF_NULL) return ref;

            node = node_ptr(tree, ref);  // re-resolve
            bart_node_t *cow_node = node_ptr(tree, cow_ref);
            size_t copy_size = BART_NODE_HEADER_SIZE +
                               (size_t)old_nc * sizeof(uint32_t);
            memcpy(cow_node, node, copy_size);

            uint8_t new_partial_len = plen - (prefix_len + 1);
            if (prefix_len + 1 < BART_MAX_PREFIX) {
                size_t shift = (size_t)prefix_len + 1;
                size_t avail = BART_MAX_PREFIX - shift;
                size_t keep = (size_t)new_partial_len < avail
                              ? (size_t)new_partial_len : avail;
                memmove(cow_node->partial, cow_node->partial + shift, keep);
            } else {
                if (!leaf_k) {
                    bart_ref_t ml = find_minimum_leaf(tree, cow_ref);
                    leaf_k = leaf_key(tree, ml);
                }
                size_t s = (size_t)new_partial_len < BART_MAX_PREFIX
                           ? (size_t)new_partial_len : BART_MAX_PREFIX;
                memcpy(cow_node->partial, leaf_k + depth + prefix_len + 1, s);
            }
            cow_node->partial_len = new_partial_len;

            // New leaf
            bart_ref_t leaf = alloc_leaf(tree, key, value);
            if (leaf == BART_REF_NULL) return ref;

            uint8_t new_byte = (depth + prefix_len < tree->key_size) ?
                                key[depth + prefix_len] : 0x00;

            // Re-resolve split node
            split = node_ptr(tree, split_ref);
            bitmap_set(split->bitmap, old_byte);
            bitmap_set(split->bitmap, new_byte);

            int rank_old = bitmap_rank(split->bitmap, old_byte);
            int rank_new = bitmap_rank(split->bitmap, new_byte);
            split->children[rank_old] = cow_ref;
            split->children[rank_new] = leaf;

            return split_ref;
        }

        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    bart_ref_t child = find_child(tree, ref, byte);

    if (child != BART_REF_NULL) {
        if (depth >= tree->key_size) {
            if (BART_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
                *inserted = false;
                bart_ref_t nl = alloc_leaf(tree, key, value);
                if (nl == BART_REF_NULL) return ref;
                return cow_set_child(tree, ref, byte, nl);
            }
            return ref;
        }

        bart_ref_t new_child = cow_insert(tree, child, key, depth + 1,
                                           value, inserted);
        if (new_child != child) {
            return cow_set_child(tree, ref, byte, new_child);
        }
        return ref;
    } else {
        *inserted = true;
        bart_ref_t leaf = alloc_leaf(tree, key, value);
        if (leaf == BART_REF_NULL) return ref;
        return cow_add_child(tree, ref, byte, leaf);
    }
}

// ============================================================================
// COW Delete (recursive)
// ============================================================================

static bart_ref_t cow_delete(bitmap_art_t *tree, bart_ref_t ref,
                              const uint8_t *key, size_t depth,
                              bool *deleted) {
    if (ref == BART_REF_NULL) return BART_REF_NULL;

    if (BART_IS_LEAF_REF(ref)) {
        if (leaf_matches(tree, ref, key)) {
            *deleted = true;
            return BART_REF_NULL;
        }
        return ref;
    }

    bart_node_t *node = node_ptr(tree, ref);
    uint8_t plen = node->partial_len;

    if (plen > 0) {
        int prefix_len = check_prefix(tree, ref, node, key,
                                       tree->key_size, depth);
        if (prefix_len != plen) return ref;
        depth += plen;
    }

    uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
    bart_ref_t child = find_child(tree, ref, byte);
    if (child == BART_REF_NULL) return ref;

    if (depth >= tree->key_size) {
        if (BART_IS_LEAF_REF(child) && leaf_matches(tree, child, key)) {
            *deleted = true;
            return cow_remove_child(tree, ref, byte, depth);
        }
        return ref;
    }

    bart_ref_t new_child = cow_delete(tree, child, key, depth + 1, deleted);

    if (new_child != child) {
        if (new_child == BART_REF_NULL) {
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

static const void *search(const bitmap_art_t *tree, bart_ref_t ref,
                           const uint8_t *key, size_t depth) {
    while (ref != BART_REF_NULL) {
        if (BART_IS_LEAF_REF(ref)) {
            if (leaf_matches(tree, ref, key))
                return leaf_value(tree, ref);
            return NULL;
        }

        bart_node_t *node = node_ptr(tree, ref);
        uint8_t plen = node->partial_len;
        if (plen > 0) {
            int prefix_len = check_prefix(tree, ref, node, key,
                                           tree->key_size, depth);
            if (prefix_len != plen) return NULL;
            depth += plen;
        }

        uint8_t byte = (depth < tree->key_size) ? key[depth] : 0x00;
        bart_ref_t child = find_child(tree, ref, byte);
        if (child == BART_REF_NULL) return NULL;

        if (depth >= tree->key_size) {
            if (BART_IS_LEAF_REF(child) && leaf_matches(tree, child, key))
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

static bool meta_read(int fd, int page, bart_meta_t *meta) {
    off_t offset = (page == 0) ? BART_META_A_OFFSET : BART_META_B_OFFSET;
    uint8_t buf[BART_META_SIZE];

    ssize_t r = pread(fd, buf, BART_META_SIZE, offset);
    if (r != BART_META_SIZE) return false;

    memcpy(meta, buf, sizeof(bart_meta_t));

    if (meta->magic != BART_META_MAGIC) return false;

    uint32_t expected = bart_crc32c(meta, BART_META_CRC_LEN);
    if (meta->crc32c != expected) return false;

    return true;
}

static bool meta_write(int fd, int page, const bart_meta_t *meta) {
    off_t offset = (page == 0) ? BART_META_A_OFFSET : BART_META_B_OFFSET;
    uint8_t buf[BART_META_SIZE];
    memset(buf, 0, BART_META_SIZE);
    memcpy(buf, meta, sizeof(bart_meta_t));

    ssize_t w = pwrite(fd, buf, BART_META_SIZE, offset);
    return w == BART_META_SIZE;
}

// ============================================================================
// Lifecycle
// ============================================================================

bool bart_open(bitmap_art_t *tree, const char *path,
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
    if (file_size < (off_t)BART_TOTAL_FILE_SIZE) {
        if (ftruncate(fd, (off_t)BART_TOTAL_FILE_SIZE) != 0) {
            close(fd);
            return false;
        }
    }

    // mmap node pool
    tree->node_base = mmap(NULL, BART_NODE_POOL_MAX,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, BART_DATA_OFFSET);
    if (tree->node_base == MAP_FAILED) {
        close(fd);
        return false;
    }

    // mmap leaf pool
    tree->leaf_base = mmap(NULL, BART_LEAF_POOL_MAX,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, BART_LEAF_POOL_OFFSET);
    if (tree->leaf_base == MAP_FAILED) {
        munmap(tree->node_base, BART_NODE_POOL_MAX);
        close(fd);
        return false;
    }

    tree->fd = fd;

    if (created || file_size == 0) {
        // Initialize new tree
        tree->root = BART_REF_NULL;
        tree->size = 0;
        tree->node_used = 8;   // skip offset 0 so ref=0 means NULL
        tree->leaf_count = 0;
        tree->generation = 0;
        tree->active_meta = 0;

        // Write initial meta to page A
        bart_meta_t meta = {0};
        meta.magic = BART_META_MAGIC;
        meta.generation = 1;
        meta.root = BART_REF_NULL;
        meta.size = 0;
        meta.node_used = 8;
        meta.leaf_count = 0;
        meta.key_size = key_size;
        meta.value_size = value_size;
        meta.crc32c = bart_crc32c(&meta, BART_META_CRC_LEN);

        if (!meta_write(fd, 0, &meta)) {
            bart_close(tree);
            return false;
        }
        if (fdatasync(fd) != 0) {
            bart_close(tree);
            return false;
        }

        tree->generation = 1;
        tree->active_meta = 0;
    } else {
        // Recovery: read both meta pages, pick highest valid generation
        bart_meta_t meta_a = {0}, meta_b = {0};
        bool a_valid = meta_read(fd, 0, &meta_a);
        bool b_valid = meta_read(fd, 1, &meta_b);

        bart_meta_t *active = NULL;
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
            bart_close(tree);
            return false;
        }

        // Validate key/value sizes
        if (active->key_size != key_size || active->value_size != value_size) {
            bart_close(tree);
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

void bart_close(bitmap_art_t *tree) {
    if (!tree) return;
    if (tree->leaf_base && tree->leaf_base != MAP_FAILED)
        munmap(tree->leaf_base, BART_LEAF_POOL_MAX);
    if (tree->node_base && tree->node_base != MAP_FAILED)
        munmap(tree->node_base, BART_NODE_POOL_MAX);
    if (tree->fd >= 0) close(tree->fd);
    memset(tree, 0, sizeof(*tree));
    tree->fd = -1;
}

// ============================================================================
// Public API
// ============================================================================

bool bart_insert(bitmap_art_t *tree, const uint8_t *key,
                  const void *value) {
    if (!tree || !key || !value) return false;

    bool inserted = false;
    tree->root = cow_insert(tree, tree->root, key, 0, value, &inserted);
    if (inserted) tree->size++;
    return tree->root != BART_REF_NULL;
}

bool bart_delete(bitmap_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return false;

    bool deleted = false;
    tree->root = cow_delete(tree, tree->root, key, 0, &deleted);
    if (deleted) tree->size--;
    return deleted;
}

const void *bart_get(const bitmap_art_t *tree, const uint8_t *key) {
    if (!tree || !key) return NULL;
    return search(tree, tree->root, key, 0);
}

bool bart_contains(const bitmap_art_t *tree, const uint8_t *key) {
    return bart_get(tree, key) != NULL;
}

size_t bart_size(const bitmap_art_t *tree) {
    return tree ? tree->size : 0;
}

// ============================================================================
// Commit / Rollback
// ============================================================================

bool bart_commit(bitmap_art_t *tree) {
    if (!tree || tree->fd < 0) return false;

    // Phase 1: flush all dirty mmap'd data pages
    if (fdatasync(tree->fd) != 0) return false;

    // Phase 2: write meta to inactive page
    int inactive = 1 - tree->active_meta;
    tree->generation++;

    bart_meta_t meta = {0};
    meta.magic = BART_META_MAGIC;
    meta.generation = tree->generation;
    meta.root = tree->root;
    meta.size = tree->size;
    meta.node_used = (uint64_t)tree->node_used;
    meta.leaf_count = tree->leaf_count;
    meta.key_size = tree->key_size;
    meta.value_size = tree->value_size;
    meta.crc32c = bart_crc32c(&meta, BART_META_CRC_LEN);

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

void bart_rollback(bitmap_art_t *tree) {
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
        bart_ref_t ref;
        int child_idx;  // bitmap byte index (0-255), -1 = not started
    } stack[64];
    int depth;
} bart_iter_state_t;

struct bart_iterator {
    const bitmap_art_t *tree;
    bart_iter_state_t state;
};

bart_iterator_t *bart_iterator_create(const bitmap_art_t *tree) {
    if (!tree) return NULL;

    bart_iterator_t *iter = malloc(sizeof(bart_iterator_t));
    if (!iter) return NULL;

    iter->tree = tree;
    memset(&iter->state, 0, sizeof(bart_iter_state_t));
    iter->state.done = (tree->root == BART_REF_NULL);
    iter->state.depth = -1;

    return iter;
}

bool bart_iterator_next(bart_iterator_t *iter) {
    if (!iter) return false;

    bart_iter_state_t *s = &iter->state;
    if (s->done) return false;

    if (!s->started) {
        s->started = true;
        if (iter->tree->root == BART_REF_NULL) {
            s->done = true;
            return false;
        }
        s->depth = 0;
        s->stack[0].ref = iter->tree->root;
        s->stack[0].child_idx = -1;
    }

    while (s->depth >= 0) {
        bart_ref_t ref = s->stack[s->depth].ref;

        if (BART_IS_LEAF_REF(ref)) {
            s->key = leaf_key(iter->tree, ref);
            s->value = leaf_value(iter->tree, ref);
            s->depth--;
            return true;
        }

        bart_node_t *node = node_ptr(iter->tree, ref);
        int *ci = &s->stack[s->depth].child_idx;

        // Find next set bit after current child_idx
        int next_byte = bitmap_next(node->bitmap, *ci + 1);

        if (next_byte < 256) {
            *ci = next_byte;
            int arr_idx = bitmap_rank(node->bitmap, (uint8_t)next_byte);
            bart_ref_t next_child = node->children[arr_idx];

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

const uint8_t *bart_iterator_key(const bart_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.key;
}

const void *bart_iterator_value(const bart_iterator_t *iter) {
    if (!iter || iter->state.done) return NULL;
    return iter->state.value;
}

bool bart_iterator_done(const bart_iterator_t *iter) {
    return iter ? iter->state.done : true;
}

void bart_iterator_destroy(bart_iterator_t *iter) {
    free(iter);
}

// ============================================================================
// Iterator Seek (lower-bound)
// ============================================================================

static int compare_prefix_seek(const bitmap_art_t *tree, bart_ref_t ref,
                                const bart_node_t *node, const uint8_t *key,
                                uint32_t key_size, size_t depth) {
    uint8_t plen = node->partial_len;
    int cmp_len = plen;
    if (depth + (size_t)cmp_len > key_size)
        cmp_len = (int)key_size - (int)depth;

    int stored = cmp_len < BART_MAX_PREFIX ? cmp_len : BART_MAX_PREFIX;
    for (int i = 0; i < stored; i++) {
        if (node->partial[i] != key[depth + i])
            return (int)node->partial[i] - (int)key[depth + i];
    }

    if (cmp_len > BART_MAX_PREFIX) {
        bart_ref_t min_leaf = find_minimum_leaf(tree, ref);
        if (min_leaf == BART_REF_NULL) return 0;
        const uint8_t *lk = leaf_key(tree, min_leaf);
        for (int i = BART_MAX_PREFIX; i < cmp_len; i++) {
            if (lk[depth + i] != key[depth + i])
                return (int)lk[depth + i] - (int)key[depth + i];
        }
    }

    return 0;
}

// Find child with byte >= target.
static bart_ref_t find_ge_child(const bitmap_art_t *tree,
                                 bart_ref_t node_ref,
                                 uint8_t target,
                                 int *out_child_idx) {
    bart_node_t *n = node_ptr(tree, node_ref);
    int byte = bitmap_next(n->bitmap, target);
    if (byte >= 256) return BART_REF_NULL;
    *out_child_idx = byte;
    return n->children[bitmap_rank(n->bitmap, (uint8_t)byte)];
}

static bool descend_to_min_leaf(const bitmap_art_t *tree,
                                 bart_iter_state_t *s,
                                 bart_ref_t ref) {
    while (ref != BART_REF_NULL) {
        if (BART_IS_LEAF_REF(ref)) {
            // Don't push leaf — just set key/value.
            // Stack stays at parent so next() advances past this leaf.
            s->key = leaf_key(tree, ref);
            s->value = leaf_value(tree, ref);
            return true;
        }

        bart_node_t *n = node_ptr(tree, ref);
        int first = bitmap_first(n->bitmap);
        if (first >= 256) return false;

        s->depth++;
        if (s->depth >= 64) { s->done = true; return false; }
        s->stack[s->depth].ref = ref;
        s->stack[s->depth].child_idx = first;

        ref = n->children[0];  // first child always at array index 0
    }
    return false;
}

bool bart_iterator_seek(bart_iterator_t *iter, const uint8_t *key) {
    if (!iter || !key) return false;

    const bitmap_art_t *tree = iter->tree;
    bart_iter_state_t *s = &iter->state;

    memset(s, 0, sizeof(bart_iter_state_t));
    s->depth = -1;
    s->started = true;

    if (tree->root == BART_REF_NULL) {
        s->done = true;
        return false;
    }

    bart_ref_t ref = tree->root;
    size_t depth = 0;

    while (ref != BART_REF_NULL) {
        if (BART_IS_LEAF_REF(ref)) {
            const uint8_t *lk = leaf_key(tree, ref);
            if (memcmp(lk, key, tree->key_size) >= 0) {
                // Don't push leaf — just set key/value.
                // Stack stays at parent so next() advances past this leaf.
                s->key = lk;
                s->value = leaf_value(tree, ref);
                return true;
            }
            goto backtrack;
        }

        bart_node_t *node = node_ptr(tree, ref);
        uint8_t plen = node->partial_len;

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
        bart_ref_t child = find_ge_child(tree, ref, key[depth], &child_idx);

        if (child == BART_REF_NULL) goto backtrack;

        s->depth++;
        if (s->depth >= 64) { s->done = true; return false; }
        s->stack[s->depth].ref = ref;
        s->stack[s->depth].child_idx = child_idx;

        if ((uint8_t)child_idx > key[depth])
            return descend_to_min_leaf(tree, s, child);

        depth++;
        ref = child;
        continue;

    backtrack:
        while (s->depth >= 0) {
            bart_ref_t parent_ref = s->stack[s->depth].ref;
            int ci = s->stack[s->depth].child_idx;

            bart_node_t *pnode = node_ptr(tree, parent_ref);
            int next_byte = bitmap_next(pnode->bitmap, ci + 1);

            if (next_byte < 256) {
                s->stack[s->depth].child_idx = next_byte;
                int arr_idx = bitmap_rank(pnode->bitmap, (uint8_t)next_byte);
                return descend_to_min_leaf(tree, s, pnode->children[arr_idx]);
            }

            s->depth--;
        }

        s->done = true;
        return false;
    }

    s->done = true;
    return false;
}
