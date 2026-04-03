/**
 * Hashed ART — Adaptive Radix Trie with embedded MPT hash cache.
 *
 * Purpose-built for keccak-hashed 32-byte keys:
 *   - No path compression (keccak keys are uniformly distributed)
 *   - Fixed value_size (set at init)
 *   - Each inner node embeds a 32-byte MPT hash cache
 *   - Dirty flag: when set, cached hash is stale and must be recomputed
 *   - MPT root hash computed directly — no separate art_mpt + cache
 */

#include "hashed_art.h"
#include "hash.h"
#include <stdlib.h>
#include <string.h>
#include <immintrin.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE       32
#define NODE48_EMPTY    255
#define NODE_DIRTY      0x01

#define NODE_4          0
#define NODE_16         1
#define NODE_48         2
#define NODE_256        3

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* =========================================================================
 * Node structures
 *
 * Header: type(1) num_children(1) flags(1) _pad(1) = 4 bytes
 * Followed by keys/index, children, and hash[32] cache.
 * ========================================================================= */

typedef struct {
    uint8_t    type, num_children, flags, _pad;
    uint8_t    keys[4];
    hart_ref_t children[4];
    uint8_t    hash[32];
} node4_t;

typedef struct {
    uint8_t    type, num_children, flags, _pad;
    uint8_t    keys[16];
    hart_ref_t children[16];
    uint8_t    hash[32];
} node16_t;

typedef struct {
    uint8_t    type, num_children, flags, _pad;
    uint8_t    index[256];
    hart_ref_t children[48];
    uint8_t    hash[32];
} node48_t;

typedef struct {
    uint8_t    type, num_children, flags, _pad;
    hart_ref_t children[256];
    uint8_t    hash[32];
} node256_t;

/* Leaf is raw bytes in arena: key[32] followed by value[value_size]. */

/* =========================================================================
 * Arena allocator
 * ========================================================================= */

static hart_ref_t arena_alloc(hart_t *t, size_t bytes, bool is_leaf) {
    size_t aligned = (t->arena_used + 15) & ~(size_t)15;
    if (aligned + bytes > t->arena_cap) {
        size_t nc = t->arena_cap ? t->arena_cap * 2 : 4096;
        while (aligned + bytes > nc) nc *= 2;
        uint8_t *na = realloc(t->arena, nc);
        if (!na) return HART_REF_NULL;
        t->arena = na;
        t->arena_cap = nc;
    }
    t->arena_used = aligned + bytes;
    memset(t->arena + aligned, 0, bytes);
    hart_ref_t ref = (hart_ref_t)(aligned >> 4);
    if (is_leaf) ref |= 0x80000000u;
    return ref;
}

static inline void *ref_ptr(const hart_t *t, hart_ref_t ref) {
    return t->arena + ((size_t)(ref & 0x7FFFFFFFu) << 4);
}

/* =========================================================================
 * Node helpers
 * ========================================================================= */

static inline uint8_t node_type(const void *n) { return *(const uint8_t *)n; }

static inline void mark_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    ((uint8_t *)ref_ptr(t, ref))[2] |= NODE_DIRTY;
}

static inline bool is_node_dirty(const hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return true;
    return (((const uint8_t *)ref_ptr(t, ref))[2] & NODE_DIRTY) != 0;
}

static inline void clear_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    ((uint8_t *)ref_ptr(t, ref))[2] &= ~NODE_DIRTY;
}

static inline const uint8_t *node_hash_ptr(const void *n) {
    switch (node_type(n)) {
    case NODE_4:   return ((const node4_t *)n)->hash;
    case NODE_16:  return ((const node16_t *)n)->hash;
    case NODE_48:  return ((const node48_t *)n)->hash;
    case NODE_256: return ((const node256_t *)n)->hash;
    default: return NULL;
    }
}

static inline uint8_t *node_hash_mut(void *n) {
    switch (node_type(n)) {
    case NODE_4:   return ((node4_t *)n)->hash;
    case NODE_16:  return ((node16_t *)n)->hash;
    case NODE_48:  return ((node48_t *)n)->hash;
    case NODE_256: return ((node256_t *)n)->hash;
    default: return NULL;
    }
}

/* =========================================================================
 * Leaf helpers
 * ========================================================================= */

static inline const uint8_t *leaf_key(const hart_t *t, hart_ref_t ref) {
    return (const uint8_t *)ref_ptr(t, ref);
}

static inline const void *leaf_value(const hart_t *t, hart_ref_t ref) {
    return (const uint8_t *)ref_ptr(t, ref) + KEY_SIZE;
}

static inline bool leaf_matches(const hart_t *t, hart_ref_t ref, const uint8_t key[32]) {
    return memcmp(leaf_key(t, ref), key, KEY_SIZE) == 0;
}

static hart_ref_t alloc_leaf(hart_t *t, const uint8_t key[32], const void *value) {
    size_t total = KEY_SIZE + t->value_size;
    hart_ref_t ref = arena_alloc(t, total, true);
    if (ref == HART_REF_NULL) return ref;
    uint8_t *p = ref_ptr(t, ref);
    memcpy(p, key, KEY_SIZE);
    memcpy(p + KEY_SIZE, value, t->value_size);
    return ref;
}

/* =========================================================================
 * Child management
 * ========================================================================= */

static hart_ref_t *find_child_ptr(const hart_t *t, hart_ref_t nref, uint8_t byte) {
    void *n = ref_ptr(t, nref);
    switch (node_type(n)) {
    case NODE_4: {
        node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (nn->keys[i] == byte) return &nn->children[i];
        return NULL;
    }
    case NODE_16: {
        node16_t *nn = n;
        __m128i kv = _mm_set1_epi8((char)byte);
        __m128i cmp = _mm_cmpeq_epi8(kv, _mm_loadu_si128((__m128i *)nn->keys));
        int mask = _mm_movemask_epi8(cmp) & ((1 << nn->num_children) - 1);
        if (mask) return &nn->children[__builtin_ctz(mask)];
        return NULL;
    }
    case NODE_48: {
        node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        return (idx != NODE48_EMPTY) ? &nn->children[idx] : NULL;
    }
    case NODE_256: {
        node256_t *nn = n;
        return nn->children[byte] ? &nn->children[byte] : NULL;
    }
    }
    return NULL;
}

static hart_ref_t alloc_node(hart_t *t, uint8_t type) {
    size_t sz;
    switch (type) {
    case NODE_4:   sz = sizeof(node4_t); break;
    case NODE_16:  sz = sizeof(node16_t); break;
    case NODE_48:  sz = sizeof(node48_t); break;
    case NODE_256: sz = sizeof(node256_t); break;
    default: return HART_REF_NULL;
    }
    hart_ref_t ref = arena_alloc(t, sz, false);
    if (ref == HART_REF_NULL) return ref;
    void *n = ref_ptr(t, ref);
    *(uint8_t *)n = type;
    ((uint8_t *)n)[2] = NODE_DIRTY; /* born dirty */
    if (type == NODE_48)
        memset(((node48_t *)n)->index, NODE48_EMPTY, 256);
    return ref;
}

static hart_ref_t add_child(hart_t *t, hart_ref_t nref, uint8_t byte, hart_ref_t child) {
    void *n = ref_ptr(t, nref);
    switch (node_type(n)) {
    case NODE_4: {
        node4_t *nn = n;
        if (nn->num_children < 4) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos, nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos, (nn->num_children - pos) * sizeof(hart_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node16 */
        hart_ref_t new_ref = alloc_node(t, NODE_16);
        if (new_ref == HART_REF_NULL) return nref;
        nn = ref_ptr(t, nref);
        node16_t *nn16 = ref_ptr(t, new_ref);
        memcpy(nn16->keys, nn->keys, 4);
        memcpy(nn16->children, nn->children, 4 * sizeof(hart_ref_t));
        nn16->num_children = 4;
        int pos = 0;
        while (pos < 4 && nn16->keys[pos] < byte) pos++;
        memmove(nn16->keys + pos + 1, nn16->keys + pos, 4 - pos);
        memmove(nn16->children + pos + 1, nn16->children + pos, (4 - pos) * sizeof(hart_ref_t));
        nn16->keys[pos] = byte;
        nn16->children[pos] = child;
        nn16->num_children = 5;
        return new_ref;
    }
    case NODE_16: {
        node16_t *nn = n;
        if (nn->num_children < 16) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos, nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos, (nn->num_children - pos) * sizeof(hart_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node48 */
        hart_ref_t new_ref = alloc_node(t, NODE_48);
        if (new_ref == HART_REF_NULL) return nref;
        nn = ref_ptr(t, nref);
        node48_t *nn48 = ref_ptr(t, new_ref);
        for (int i = 0; i < 16; i++) {
            nn48->index[nn->keys[i]] = (uint8_t)i;
            nn48->children[i] = nn->children[i];
        }
        nn48->index[byte] = 16;
        nn48->children[16] = child;
        nn48->num_children = 17;
        return new_ref;
    }
    case NODE_48: {
        node48_t *nn = n;
        if (nn->num_children < 48) {
            /* Find a free slot (may have holes from delete) */
            uint8_t slot = 0;
            for (; slot < 48; slot++)
                if (nn->children[slot] == HART_REF_NULL) break;
            nn->index[byte] = slot;
            nn->children[slot] = child;
            nn->num_children++;
            return nref;
        }
        /* Grow to node256 */
        hart_ref_t new_ref = alloc_node(t, NODE_256);
        if (new_ref == HART_REF_NULL) return nref;
        nn = ref_ptr(t, nref);
        node256_t *nn256 = ref_ptr(t, new_ref);
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY)
                nn256->children[i] = nn->children[nn->index[i]];
        }
        nn256->children[byte] = child;
        nn256->num_children = nn->num_children + 1;
        return new_ref;
    }
    case NODE_256: {
        node256_t *nn = n;
        nn->children[byte] = child;
        nn->num_children++;
        return nref;
    }
    }
    return nref;
}

/* =========================================================================
 * Insert
 * ========================================================================= */

static hart_ref_t insert_recursive(hart_t *t, hart_ref_t ref,
                                    const uint8_t key[32], const void *value,
                                    size_t depth, bool *inserted) {
    if (ref == HART_REF_NULL) {
        *inserted = true;
        return alloc_leaf(t, key, value);
    }

    if (HART_IS_LEAF(ref)) {
        if (leaf_matches(t, ref, key)) {
            memcpy((uint8_t *)ref_ptr(t, ref) + KEY_SIZE, value, t->value_size);
            *inserted = false;
            return ref;
        }
        /* Split: find first differing byte, create chain of Node4s */
        *inserted = true;
        hart_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == HART_REF_NULL) return ref;

        const uint8_t *old_key = leaf_key(t, ref);
        size_t diff = depth;
        while (diff < KEY_SIZE && old_key[diff] == key[diff]) diff++;

        /* Build chain from diff back to depth */
        hart_ref_t new_node = alloc_node(t, NODE_4);
        if (new_node == HART_REF_NULL) return ref;
        old_key = leaf_key(t, ref); /* re-resolve */
        node4_t *nn = ref_ptr(t, new_node);
        uint8_t old_byte = old_key[diff];
        uint8_t new_byte = key[diff];
        if (old_byte < new_byte) {
            nn->keys[0] = old_byte; nn->children[0] = ref;
            nn->keys[1] = new_byte; nn->children[1] = new_leaf;
        } else {
            nn->keys[0] = new_byte; nn->children[0] = new_leaf;
            nn->keys[1] = old_byte; nn->children[1] = ref;
        }
        nn->num_children = 2;

        /* Wrap with single-child Node4s for each shared byte */
        hart_ref_t result = new_node;
        for (size_t d = diff; d > depth; d--) {
            hart_ref_t wrapper = alloc_node(t, NODE_4);
            if (wrapper == HART_REF_NULL) return ref;
            old_key = leaf_key(t, ref); /* re-resolve */
            node4_t *w = ref_ptr(t, wrapper);
            w->keys[0] = old_key[d - 1];
            w->children[0] = result;
            w->num_children = 1;
            result = wrapper;
        }
        return result;
    }

    /* Inner node — dispatch on key byte */
    mark_dirty(t, ref);

    uint8_t byte = key[depth];
    hart_ref_t *child_ptr = find_child_ptr(t, ref, byte);

    if (child_ptr) {
        hart_ref_t old_child = *child_ptr;
        hart_ref_t new_child = insert_recursive(t, old_child, key, value, depth + 1, inserted);
        if (new_child != old_child) {
            child_ptr = find_child_ptr(t, ref, byte);
            *child_ptr = new_child;
        }
    } else {
        *inserted = true;
        hart_ref_t leaf = alloc_leaf(t, key, value);
        if (leaf != HART_REF_NULL)
            ref = add_child(t, ref, byte, leaf);
    }
    return ref;
}

bool hart_insert(hart_t *t, const uint8_t key[32], const void *value) {
    if (!t || !key || !value) return false;
    bool inserted = false;
    t->root = insert_recursive(t, t->root, key, value, 0, &inserted);
    if (inserted) t->size++;
    return true;
}

/* =========================================================================
 * Delete
 * ========================================================================= */

static hart_ref_t remove_child(hart_t *t, hart_ref_t nref, uint8_t byte) {
    void *n = ref_ptr(t, nref);
    switch (node_type(n)) {
    case NODE_4: {
        node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1], nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(hart_ref_t));
                nn->num_children--;
                return nref;
            }
        }
        return nref;
    }
    case NODE_16: {
        node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1], nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(hart_ref_t));
                nn->num_children--;
                if (nn->num_children <= 4) {
                    hart_ref_t new_ref = alloc_node(t, NODE_4);
                    if (new_ref == HART_REF_NULL) return nref;
                    nn = ref_ptr(t, nref);
                    node4_t *n4 = ref_ptr(t, new_ref);
                    n4->num_children = nn->num_children;
                    memcpy(n4->keys, nn->keys, nn->num_children);
                    memcpy(n4->children, nn->children,
                           nn->num_children * sizeof(hart_ref_t));
                    return new_ref;
                }
                return nref;
            }
        }
        return nref;
    }
    case NODE_48: {
        node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        if (idx != NODE48_EMPTY) {
            nn->children[idx] = HART_REF_NULL;
            nn->index[byte] = NODE48_EMPTY;
            nn->num_children--;
            if (nn->num_children <= 16) {
                hart_ref_t new_ref = alloc_node(t, NODE_16);
                if (new_ref == HART_REF_NULL) return nref;
                nn = ref_ptr(t, nref);
                node16_t *n16 = ref_ptr(t, new_ref);
                n16->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->index[i] != NODE48_EMPTY) {
                        n16->keys[n16->num_children] = (uint8_t)i;
                        n16->children[n16->num_children] = nn->children[nn->index[i]];
                        n16->num_children++;
                    }
                }
                return new_ref;
            }
        }
        return nref;
    }
    case NODE_256: {
        node256_t *nn = n;
        if (nn->children[byte] != HART_REF_NULL) {
            nn->children[byte] = HART_REF_NULL;
            nn->num_children--;
            if (nn->num_children <= 48) {
                hart_ref_t new_ref = alloc_node(t, NODE_48);
                if (new_ref == HART_REF_NULL) return nref;
                nn = ref_ptr(t, nref);
                node48_t *n48 = ref_ptr(t, new_ref);
                n48->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->children[i] != HART_REF_NULL) {
                        n48->index[i] = (uint8_t)n48->num_children;
                        n48->children[n48->num_children] = nn->children[i];
                        n48->num_children++;
                    }
                }
                return new_ref;
            }
        }
        return nref;
    }
    }
    return nref;
}

static hart_ref_t delete_recursive(hart_t *t, hart_ref_t ref,
                                    const uint8_t key[32],
                                    size_t depth, bool *deleted) {
    if (ref == HART_REF_NULL) return HART_REF_NULL;

    if (HART_IS_LEAF(ref)) {
        if (leaf_matches(t, ref, key)) {
            *deleted = true;
            return HART_REF_NULL;
        }
        return ref;
    }

    uint8_t byte = key[depth];
    hart_ref_t *child_ptr = find_child_ptr(t, ref, byte);
    if (!child_ptr) return ref;

    mark_dirty(t, ref);

    hart_ref_t old_child = *child_ptr;
    hart_ref_t new_child = delete_recursive(t, old_child, key, depth + 1, deleted);

    if (new_child != old_child) {
        if (new_child == HART_REF_NULL) {
            ref = remove_child(t, ref, byte);
        } else {
            child_ptr = find_child_ptr(t, ref, byte);
            if (child_ptr) *child_ptr = new_child;
        }
    }

    /* Collapse empty nodes; promote lone leaf children */
    if (!HART_IS_LEAF(ref)) {
        void *node = ref_ptr(t, ref);
        if (node_type(node) == NODE_4) {
            node4_t *n4 = (node4_t *)node;
            if (n4->num_children == 0) return HART_REF_NULL;
            if (n4->num_children == 1 && HART_IS_LEAF(n4->children[0]))
                return n4->children[0];
        }
    }
    return ref;
}

bool hart_delete(hart_t *t, const uint8_t key[32]) {
    if (!t || !key) return false;
    bool deleted = false;
    t->root = delete_recursive(t, t->root, key, 0, &deleted);
    if (deleted) t->size--;
    return deleted;
}

/* =========================================================================
 * Get / Contains
 * ========================================================================= */

const void *hart_get(const hart_t *t, const uint8_t key[32]) {
    if (!t) return NULL;
    hart_ref_t ref = t->root;
    size_t depth = 0;
    while (ref != HART_REF_NULL) {
        if (HART_IS_LEAF(ref))
            return leaf_matches(t, ref, key) ? leaf_value(t, ref) : NULL;
        hart_ref_t *child = find_child_ptr(t, ref, key[depth]);
        if (!child) return NULL;
        ref = *child;
        depth++;
    }
    return NULL;
}

bool hart_contains(const hart_t *t, const uint8_t key[32]) {
    return hart_get(t, key) != NULL;
}

/* =========================================================================
 * Mark path dirty
 * ========================================================================= */

bool hart_mark_path_dirty(hart_t *t, const uint8_t key[32]) {
    if (!t) return false;
    hart_ref_t ref = t->root;
    size_t depth = 0;
    while (ref != HART_REF_NULL) {
        if (HART_IS_LEAF(ref))
            return leaf_matches(t, ref, key);
        mark_dirty(t, ref);
        hart_ref_t *child = find_child_ptr(t, ref, key[depth]);
        if (!child) return false;
        ref = *child;
        depth++;
    }
    return false;
}

/* =========================================================================
 * MPT Root Hash — embedded hash version
 * ========================================================================= */

/* RLP helpers */
typedef struct { uint8_t data[1024]; size_t len; } rlp_buf_t;
static void rbuf_reset(rlp_buf_t *b) { b->len = 0; }
static bool rbuf_append(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (b->len + n > sizeof(b->data)) return false;
    if (n > 0) memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}
static bool rbuf_encode_bytes(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80) return rbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return rbuf_append(b, &pfx, 1) && (n > 0 ? rbuf_append(b, d, n) : true);
    }
    size_t ll = 1; size_t tmp = n; while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t hdr[6]; hdr[0] = 0xb7 + (uint8_t)ll;
    for (size_t i = ll; i > 0; i--) { hdr[i] = (uint8_t)(n & 0xFF); n >>= 8; }
    return rbuf_append(b, hdr, 1 + ll) && rbuf_append(b, d, n);
}
static bool rbuf_list_wrap(rlp_buf_t *out, const rlp_buf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return rbuf_append(out, &pfx, 1) && rbuf_append(out, payload->data, payload->len);
    }
    size_t ll = 1; size_t tmp = payload->len; while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    rbuf_append(out, &pfx, 1);
    uint8_t len_be[4]; size_t plen = payload->len;
    for (size_t i = ll; i > 0; i--) { len_be[i-1] = (uint8_t)(plen & 0xFF); plen >>= 8; }
    return rbuf_append(out, len_be, ll) && rbuf_append(out, payload->data, payload->len);
}

static void hart_keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    hash_t h = hash_keccak256(data, len);
    memcpy(out, h.bytes, 32);
}

static size_t hex_prefix_encode(const uint8_t *nibbles, size_t nib_len, bool is_leaf, uint8_t *out) {
    uint8_t prefix = (is_leaf ? 2 : 0) + (nib_len % 2 ? 1 : 0);
    size_t pos = 0;
    if (nib_len % 2) {
        out[pos++] = (prefix << 4) | nibbles[0];
        nibbles++; nib_len--;
    } else {
        out[pos++] = prefix << 4;
    }
    for (size_t i = 0; i < nib_len; i += 2)
        out[pos++] = (nibbles[i] << 4) | nibbles[i+1];
    return pos;
}

static size_t hash_ref(hart_t *t, hart_ref_t ref, size_t byte_depth,
                        hart_encode_t encode, void *ctx,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out);

static size_t rlp_to_hashref(const uint8_t *rlp, size_t rlp_len, uint8_t *out) {
    if (rlp_len < 32) { memcpy(out, rlp, rlp_len); return rlp_len; }
    hart_keccak(rlp, rlp_len, out);
    return 32;
}

static size_t encode_child_ref(const uint8_t *child_rlp, size_t child_len, uint8_t *out) {
    if (child_len == 0) { out[0] = 0x80; return 1; }
    if (child_len < 32) { memcpy(out, child_rlp, child_len); return child_len; }
    if (child_len == 32) { out[0] = 0xa0; memcpy(out+1, child_rlp, 32); return 33; }
    out[0] = 0xa0; hart_keccak(child_rlp, child_len, out+1); return 33;
}

static size_t encode_leaf_node(hart_t *t, hart_ref_t leaf_ref, size_t byte_depth,
                                hart_encode_t encode, void *ctx,
                                const uint8_t *nib_prefix, size_t nib_prefix_len,
                                uint8_t *rlp_out) {
    const uint8_t *key = leaf_key(t, leaf_ref);

    uint8_t path[128];
    size_t path_len = 0;
    if (nib_prefix_len > 0) { memcpy(path, nib_prefix, nib_prefix_len); path_len = nib_prefix_len; }
    for (size_t i = byte_depth; i < KEY_SIZE; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(path, path_len, true, hp);

    uint8_t value_rlp[256];
    uint32_t value_len = encode(key, leaf_value(t, leaf_ref), value_rlp, ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t encode_extension(const uint8_t *nibbles, size_t nib_len,
                                const uint8_t *child_rlp, size_t child_len,
                                uint8_t *rlp_out) {
    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(nibbles, nib_len, false, hp);
    uint8_t cref[33];
    size_t cref_len = encode_child_ref(child_rlp, child_len, cref);
    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_append(&payload, cref, cref_len);
    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t build_branch_rlp(uint8_t slots[16][33], uint8_t slot_lens[16],
                                uint8_t *rlp_out) {
    uint8_t payload[600];
    size_t plen = 0;
    for (int i = 0; i < 16; i++) {
        if (slot_lens[i] == 0) {
            payload[plen++] = 0x80;
        } else if (slot_lens[i] < 32) {
            memcpy(payload + plen, slots[i], slot_lens[i]);
            plen += slot_lens[i];
        } else {
            payload[plen++] = 0xa0;
            memcpy(payload + plen, slots[i], 32);
            plen += 32;
        }
    }
    payload[plen++] = 0x80; /* value slot */

    uint8_t encoded[610];
    size_t elen;
    if (plen <= 55) {
        encoded[0] = 0xc0 + (uint8_t)plen;
        memcpy(encoded + 1, payload, plen);
        elen = 1 + plen;
    } else {
        size_t ll = 1; size_t tmp = plen; while (tmp > 255) { ll++; tmp >>= 8; }
        encoded[0] = 0xf7 + (uint8_t)ll;
        size_t p = plen;
        for (size_t i = ll; i > 0; i--) { encoded[i] = (uint8_t)(p & 0xFF); p >>= 8; }
        memcpy(encoded + 1 + ll, payload, plen);
        elen = 1 + ll + plen;
    }
    return rlp_to_hashref(encoded, elen, rlp_out);
}

/* Forward declaration */
static size_t hash_ref(hart_t *t, hart_ref_t ref, size_t byte_depth,
                        hart_encode_t encode, void *ctx,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out);

static size_t hash_lo_group(hart_t *t, const uint8_t *lo_keys,
                             const hart_ref_t *lo_refs, int count,
                             size_t next_depth, hart_encode_t encode, void *ctx,
                             uint8_t *rlp_out) {
    uint8_t slots[16][33];
    uint8_t slot_lens[16] = {0};
    for (int j = 0; j < count; j++) {
        slot_lens[lo_keys[j]] = (uint8_t)
            hash_ref(t, lo_refs[j], next_depth, encode, ctx,
                     NULL, 0, slots[lo_keys[j]]);
    }
    return build_branch_rlp(slots, slot_lens, rlp_out);
}

static size_t hash_ref(hart_t *t, hart_ref_t ref, size_t byte_depth,
                        hart_encode_t encode, void *ctx,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out) {
    if (ref == HART_REF_NULL) return 0;

    if (HART_IS_LEAF(ref))
        return encode_leaf_node(t, ref, byte_depth, encode, ctx, nib_prefix, nib_prefix_len, rlp_out);

    if (!is_node_dirty(t, ref)) {
        const uint8_t *cached = node_hash_ptr(ref_ptr(t, ref));
        if (cached) {
            if (nib_prefix_len > 0)
                return encode_extension(nib_prefix, nib_prefix_len, cached, 32, rlp_out);
            memcpy(rlp_out, cached, 32);
            return 32;
        }
    }

    void *n = ref_ptr(t, ref);
    uint8_t lo_keys[16][16];
    hart_ref_t lo_refs[16][16];
    uint8_t gcounts[16] = {0};
    int nchildren = 0;
    switch (node_type(n)) {
    case NODE_4: { node4_t *nn = n; for (int i = 0; i < nn->num_children; i++) { uint8_t hi = nn->keys[i]>>4, lo = nn->keys[i]&0xF; int g = gcounts[hi]++; lo_keys[hi][g] = lo; lo_refs[hi][g] = nn->children[i]; } nchildren = nn->num_children; break; }
    case NODE_16: { node16_t *nn = n; for (int i = 0; i < nn->num_children; i++) { uint8_t hi = nn->keys[i]>>4, lo = nn->keys[i]&0xF; int g = gcounts[hi]++; lo_keys[hi][g] = lo; lo_refs[hi][g] = nn->children[i]; } nchildren = nn->num_children; break; }
    case NODE_48: { node48_t *nn = n; for (int i = 0; i < 256; i++) if (nn->index[i]!=NODE48_EMPTY) { uint8_t hi=i>>4, lo=i&0xF; int g=gcounts[hi]++; lo_keys[hi][g]=lo; lo_refs[hi][g]=nn->children[nn->index[i]]; nchildren++; } break; }
    case NODE_256: { node256_t *nn = n; for (int i = 0; i < 256; i++) if (nn->children[i]) { uint8_t hi=i>>4, lo=i&0xF; int g=gcounts[hi]++; lo_keys[hi][g]=lo; lo_refs[hi][g]=nn->children[i]; nchildren++; } break; }
    }

    size_t next_depth = byte_depth + 1;
    int hi_occupied = 0, single_hi = -1;
    for (int i = 0; i < 16; i++) if (gcounts[i] > 0) { hi_occupied++; single_hi = i; }

    if (nchildren == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) { memcpy(pfx, nib_prefix, nib_prefix_len); pfx_len = nib_prefix_len; }
        pfx[pfx_len++] = (uint8_t)single_hi; pfx[pfx_len++] = lo_keys[single_hi][0];
        return hash_ref(t, lo_refs[single_hi][0], next_depth, encode, ctx, pfx, pfx_len, rlp_out);
    }
    if (hi_occupied == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) { memcpy(pfx, nib_prefix, nib_prefix_len); pfx_len = nib_prefix_len; }
        pfx[pfx_len++] = (uint8_t)single_hi;
        if (gcounts[single_hi] == 1) {
            pfx[pfx_len++] = lo_keys[single_hi][0];
            return hash_ref(t, lo_refs[single_hi][0], next_depth, encode, ctx, pfx, pfx_len, rlp_out);
        }
        uint8_t lo_hash[33];
        size_t lo_len = hash_lo_group(t, lo_keys[single_hi], lo_refs[single_hi], gcounts[single_hi], next_depth, encode, ctx, lo_hash);
        return encode_extension(pfx, pfx_len, lo_hash, lo_len, rlp_out);
    }

    uint8_t hi_slots[16][33]; uint8_t hi_slot_lens[16] = {0};
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;
        if (gcounts[hi] == 1) {
            uint8_t lo_nib = lo_keys[hi][0];
            hi_slot_lens[hi] = (uint8_t)hash_ref(t, lo_refs[hi][0], next_depth, encode, ctx, &lo_nib, 1, hi_slots[hi]);
        } else {
            hi_slot_lens[hi] = (uint8_t)hash_lo_group(t, lo_keys[hi], lo_refs[hi], gcounts[hi], next_depth, encode, ctx, hi_slots[hi]);
        }
    }
    uint8_t node_hash[33];
    size_t node_hash_len = build_branch_rlp(hi_slots, hi_slot_lens, node_hash);
    n = ref_ptr(t, ref);
    memcpy(node_hash_mut(n), node_hash, 32);
    clear_dirty(t, ref);
    if (nib_prefix_len > 0) return encode_extension(nib_prefix, nib_prefix_len, node_hash, node_hash_len, rlp_out);
    memcpy(rlp_out, node_hash, node_hash_len);
    return node_hash_len;
}

void hart_root_hash(hart_t *t, hart_encode_t encode, void *ctx, uint8_t out[32]) {
    if (!t || t->root == HART_REF_NULL) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }
    uint8_t rlp[1024];
    size_t len = hash_ref(t, t->root, 0, encode, ctx, NULL, 0, rlp);
    if (len == 32) {
        memcpy(out, rlp, 32);
    } else if (len > 0 && len < 32) {
        hart_keccak(rlp, len, out);
    } else {
        memcpy(out, EMPTY_ROOT, 32);
    }
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

bool hart_init(hart_t *t, uint16_t value_size) {
    return hart_init_cap(t, value_size, 4096);
}

bool hart_init_cap(hart_t *t, uint16_t value_size, size_t initial_cap) {
    memset(t, 0, sizeof(*t));
    t->value_size = value_size;
    if (initial_cap < 256) initial_cap = 256;
    t->arena = malloc(initial_cap);
    if (!t->arena) return false;
    t->arena_cap = initial_cap;
    t->arena_used = 16;
    return true;
}

void hart_destroy(hart_t *t) {
    if (!t) return;
    free(t->arena);
    memset(t, 0, sizeof(*t));
}

size_t hart_size(const hart_t *t) { return t ? t->size : 0; }

/* =========================================================================
 * Iterator — DFS stack-based traversal
 * ========================================================================= */

#define ITER_STACK_SIZE 64

typedef struct {
    hart_ref_t ref;
    int        child_idx;
} iter_frame_t;

struct hart_iter {
    const hart_t *tree;
    iter_frame_t  stack[ITER_STACK_SIZE];
    int           top;
    hart_ref_t    current_leaf;
};

hart_iter_t *hart_iter_create(const hart_t *t) {
    if (!t) return NULL;
    hart_iter_t *it = malloc(sizeof(hart_iter_t));
    if (!it) return NULL;
    it->tree = t;
    it->top = 0;
    it->current_leaf = HART_REF_NULL;
    if (t->root != HART_REF_NULL) {
        it->stack[0].ref = t->root;
        it->stack[0].child_idx = HART_IS_LEAF(t->root) ? -1 : 0;
        it->top = 1;
    }
    return it;
}

bool hart_iter_next(hart_iter_t *it) {
    if (!it) return false;

    while (it->top > 0) {
        iter_frame_t *f = &it->stack[it->top - 1];

        if (HART_IS_LEAF(f->ref)) {
            it->current_leaf = f->ref;
            it->top--;
            return true;
        }

        const void *n = ref_ptr(it->tree, f->ref);
        switch (node_type(n)) {
        case NODE_4: {
            const node4_t *nn = (const node4_t *)n;
            if (f->child_idx >= nn->num_children) { it->top--; break; }
            hart_ref_t child = nn->children[f->child_idx++];
            if (it->top < ITER_STACK_SIZE) {
                it->stack[it->top].ref = child;
                it->stack[it->top].child_idx = HART_IS_LEAF(child) ? -1 : 0;
                it->top++;
            }
            break;
        }
        case NODE_16: {
            const node16_t *nn = (const node16_t *)n;
            if (f->child_idx >= nn->num_children) { it->top--; break; }
            hart_ref_t child = nn->children[f->child_idx++];
            if (it->top < ITER_STACK_SIZE) {
                it->stack[it->top].ref = child;
                it->stack[it->top].child_idx = HART_IS_LEAF(child) ? -1 : 0;
                it->top++;
            }
            break;
        }
        case NODE_48: {
            const node48_t *nn = (const node48_t *)n;
            while (f->child_idx < 256 && nn->index[f->child_idx] == NODE48_EMPTY)
                f->child_idx++;
            if (f->child_idx >= 256) { it->top--; break; }
            hart_ref_t child = nn->children[nn->index[f->child_idx++]];
            if (it->top < ITER_STACK_SIZE) {
                it->stack[it->top].ref = child;
                it->stack[it->top].child_idx = HART_IS_LEAF(child) ? -1 : 0;
                it->top++;
            }
            break;
        }
        case NODE_256: {
            const node256_t *nn = (const node256_t *)n;
            while (f->child_idx < 256 && nn->children[f->child_idx] == HART_REF_NULL)
                f->child_idx++;
            if (f->child_idx >= 256) { it->top--; break; }
            hart_ref_t child = nn->children[f->child_idx++];
            if (it->top < ITER_STACK_SIZE) {
                it->stack[it->top].ref = child;
                it->stack[it->top].child_idx = HART_IS_LEAF(child) ? -1 : 0;
                it->top++;
            }
            break;
        }
        default:
            it->top--;
            break;
        }
    }
    return false;
}

const uint8_t *hart_iter_key(const hart_iter_t *it) {
    if (!it || it->current_leaf == HART_REF_NULL) return NULL;
    return leaf_key(it->tree, it->current_leaf);
}

const void *hart_iter_value(const hart_iter_t *it) {
    if (!it || it->current_leaf == HART_REF_NULL) return NULL;
    return leaf_value(it->tree, it->current_leaf);
}

void hart_iter_destroy(hart_iter_t *it) {
    free(it);
}
