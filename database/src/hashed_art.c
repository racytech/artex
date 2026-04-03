/**
 * Hashed ART — Adaptive Radix Trie with embedded MPT hash cache.
 *
 * Key differences from mem_art:
 *   - Fixed 32-byte keys (no variable key_len)
 *   - Fixed value_size (set at init, 4 or 32 bytes)
 *   - Each inner node has 32-byte hash + hash_len embedded
 *   - Dirty flag on inner nodes: hash is stale when dirty
 *   - MPT root hash computed directly — no separate art_mpt + cache
 *   - Optimized for keccak-hashed keys (short/zero shared prefixes)
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
#define MAX_PREFIX      8
#define NODE_DIRTY      0x01

#define NODE_4          0
#define NODE_16         1
#define NODE_48         2
#define NODE_256        3

#define NODE48_EMPTY    255

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* =========================================================================
 * Node structures — each embeds hash[32] + hash_len
 * ========================================================================= */

typedef struct {
    uint8_t    type, num_children, partial_len, flags, hash_len;
    uint8_t    keys[4];
    hart_ref_t children[4];
    uint8_t    partial[MAX_PREFIX];
    uint8_t    hash[32];
} node4_t;

typedef struct {
    uint8_t    type, num_children, partial_len, flags, hash_len;
    uint8_t    keys[16];
    hart_ref_t children[16];
    uint8_t    partial[MAX_PREFIX];
    uint8_t    hash[32];
} node16_t;

typedef struct {
    uint8_t    type, num_children, partial_len, flags, hash_len;
    uint8_t    index[256];
    hart_ref_t children[48];
    uint8_t    partial[MAX_PREFIX];
    uint8_t    hash[32];
} node48_t;

typedef struct {
    uint8_t    type, num_children, partial_len, flags, hash_len;
    hart_ref_t children[256];
    uint8_t    partial[MAX_PREFIX];
    uint8_t    hash[32];
} node256_t;

/* Leaf is raw bytes in arena: key[32] followed by value[value_size].
 * No struct header needed — accessed via ref_ptr directly. */

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
static inline uint8_t node_partial_len(const void *n) { return ((const uint8_t *)n)[2]; }

static inline void set_partial_len(void *n, uint8_t len) {
    ((uint8_t *)n)[2] = len;
}

static inline uint8_t *node_flags_ptr(void *n) {
    return &((uint8_t *)n)[3]; /* flags at offset 3 in all node types */
}

static inline void mark_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    *node_flags_ptr(ref_ptr(t, ref)) |= NODE_DIRTY;
}

static inline bool is_node_dirty(const hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return true;
    return (*node_flags_ptr(ref_ptr(t, ref)) & NODE_DIRTY) != 0;
}

static inline void clear_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    *node_flags_ptr(ref_ptr(t, ref)) &= ~NODE_DIRTY;
}

static inline const uint8_t *node_partial(const void *n) {
    switch (node_type(n)) {
    case NODE_4:   return ((const node4_t *)n)->partial;
    case NODE_16:  return ((const node16_t *)n)->partial;
    case NODE_48:  return ((const node48_t *)n)->partial;
    case NODE_256: return ((const node256_t *)n)->partial;
    default: return NULL;
    }
}

static inline uint8_t *node_partial_mut(void *n) {
    switch (node_type(n)) {
    case NODE_4:   return ((node4_t *)n)->partial;
    case NODE_16:  return ((node16_t *)n)->partial;
    case NODE_48:  return ((node48_t *)n)->partial;
    case NODE_256: return ((node256_t *)n)->partial;
    default: return NULL;
    }
}

/* Embedded hash access */
static inline void node_get_hash(const void *n, uint8_t *data, uint8_t *len) {
    const uint8_t *h; uint8_t hl;
    switch (node_type(n)) {
    case NODE_4:   h = ((const node4_t *)n)->hash;   hl = ((const node4_t *)n)->hash_len; break;
    case NODE_16:  h = ((const node16_t *)n)->hash;  hl = ((const node16_t *)n)->hash_len; break;
    case NODE_48:  h = ((const node48_t *)n)->hash;  hl = ((const node48_t *)n)->hash_len; break;
    case NODE_256: h = ((const node256_t *)n)->hash; hl = ((const node256_t *)n)->hash_len; break;
    default: *len = 0; return;
    }
    *len = hl;
    if (hl > 0) memcpy(data, h, hl);
}

static inline void node_set_hash(void *n, const uint8_t *data, uint8_t len) {
    switch (node_type(n)) {
    case NODE_4:   memcpy(((node4_t *)n)->hash, data, len);   ((node4_t *)n)->hash_len = len; break;
    case NODE_16:  memcpy(((node16_t *)n)->hash, data, len);  ((node16_t *)n)->hash_len = len; break;
    case NODE_48:  memcpy(((node48_t *)n)->hash, data, len);  ((node48_t *)n)->hash_len = len; break;
    case NODE_256: memcpy(((node256_t *)n)->hash, data, len); ((node256_t *)n)->hash_len = len; break;
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
    *node_flags_ptr(n) = NODE_DIRTY; /* born dirty */
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
        nn = ref_ptr(t, nref); /* re-resolve after alloc */
        node16_t *nn16 = ref_ptr(t, new_ref);
        memcpy(nn16->keys, nn->keys, 4);
        memcpy(nn16->children, nn->children, 4 * sizeof(hart_ref_t));
        nn16->num_children = 4;
        nn16->partial_len = nn->partial_len;
        memcpy(nn16->partial, nn->partial, MAX_PREFIX);
        /* Insert new child */
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
        nn48->partial_len = nn->partial_len;
        memcpy(nn48->partial, nn->partial, MAX_PREFIX);
        return new_ref;
    }
    case NODE_48: {
        node48_t *nn = n;
        if (nn->num_children < 48) {
            nn->index[byte] = nn->num_children;
            nn->children[nn->num_children] = child;
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
        nn256->partial_len = nn->partial_len;
        memcpy(nn256->partial, nn->partial, MAX_PREFIX);
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
 * Prefix helpers
 * ========================================================================= */

static int check_prefix(const hart_t *t, hart_ref_t nref, const void *n,
                         const uint8_t key[32], size_t depth) {
    uint8_t plen = node_partial_len(n);
    const uint8_t *partial = node_partial(n);
    int max_cmp = plen < MAX_PREFIX ? plen : MAX_PREFIX;
    int idx = 0;
    for (; idx < max_cmp; idx++)
        if (partial[idx] != key[depth + idx]) return idx;
    if (plen > MAX_PREFIX) {
        /* Recover full prefix from leftmost leaf */
        hart_ref_t r = nref;
        while (!HART_IS_LEAF(r)) {
            void *nn = ref_ptr(t, r);
            switch (node_type(nn)) {
            case NODE_4:   r = ((node4_t *)nn)->children[0]; break;
            case NODE_16:  r = ((node16_t *)nn)->children[0]; break;
            case NODE_48: {
                node48_t *n48 = nn;
                for (int i = 0; i < 256; i++)
                    if (n48->index[i] != NODE48_EMPTY) { r = n48->children[n48->index[i]]; break; }
                break;
            }
            case NODE_256: {
                node256_t *n256 = nn;
                for (int i = 0; i < 256; i++)
                    if (n256->children[i]) { r = n256->children[i]; break; }
                break;
            }
            default: return idx;
            }
        }
        const uint8_t *lk = leaf_key(t, r);
        for (; idx < (int)plen; idx++)
            if (lk[depth + idx] != key[depth + idx]) return idx;
    }
    return idx;
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
            /* Update in place */
            memcpy((uint8_t *)ref_ptr(t, ref) + KEY_SIZE, value, t->value_size);
            *inserted = false;
            return ref;
        }
        /* Split: create branch with old leaf + new leaf */
        *inserted = true;
        hart_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == HART_REF_NULL) return ref;

        const uint8_t *old_key = leaf_key(t, ref);
        size_t prefix_len = 0;
        while (depth + prefix_len < KEY_SIZE &&
               old_key[depth + prefix_len] == key[depth + prefix_len])
            prefix_len++;

        hart_ref_t new_node = alloc_node(t, NODE_4);
        if (new_node == HART_REF_NULL) return ref;
        node4_t *nn = ref_ptr(t, new_node);
        nn->partial_len = (uint8_t)(prefix_len < MAX_PREFIX ? prefix_len : MAX_PREFIX);
        /* Re-resolve after alloc */
        old_key = leaf_key(t, ref);
        memcpy(nn->partial, old_key + depth, nn->partial_len);
        set_partial_len(nn, (uint8_t)prefix_len);

        uint8_t old_byte = old_key[depth + prefix_len];
        uint8_t new_byte = key[depth + prefix_len];
        if (old_byte < new_byte) {
            nn->keys[0] = old_byte; nn->children[0] = ref;
            nn->keys[1] = new_byte; nn->children[1] = new_leaf;
        } else {
            nn->keys[0] = new_byte; nn->children[0] = new_leaf;
            nn->keys[1] = old_byte; nn->children[1] = ref;
        }
        nn->num_children = 2;
        return new_node;
    }

    /* Inner node */
    void *n = ref_ptr(t, ref);
    uint8_t plen = node_partial_len(n);

    if (plen > 0) {
        int prefix_match = check_prefix(t, ref, n, key, depth);
        if (prefix_match < (int)plen) {
            /* Partial mismatch — split this node */
            *inserted = true;
            hart_ref_t new_node = alloc_node(t, NODE_4);
            if (new_node == HART_REF_NULL) return ref;

            hart_ref_t new_leaf = alloc_leaf(t, key, value);
            if (new_leaf == HART_REF_NULL) return ref;

            n = ref_ptr(t, ref); /* re-resolve */
            node4_t *nn = ref_ptr(t, new_node);

            /* New node gets the common prefix */
            uint8_t cpy = prefix_match < MAX_PREFIX ? (uint8_t)prefix_match : MAX_PREFIX;
            memcpy(nn->partial, node_partial(n), cpy);
            set_partial_len(nn, (uint8_t)prefix_match);

            /* Old node's prefix shortened */
            uint8_t old_byte;
            if (plen <= MAX_PREFIX) {
                old_byte = node_partial(n)[prefix_match];
                uint8_t new_plen = plen - prefix_match - 1;
                memmove(node_partial_mut(n), node_partial(n) + prefix_match + 1,
                        new_plen < MAX_PREFIX ? new_plen : MAX_PREFIX);
                set_partial_len(n, new_plen);
            } else {
                /* Recover from leaf */
                hart_ref_t r = ref;
                while (!HART_IS_LEAF(r)) {
                    void *rn = ref_ptr(t, r);
                    switch (node_type(rn)) {
                    case NODE_4:   r = ((node4_t *)rn)->children[0]; break;
                    case NODE_16:  r = ((node16_t *)rn)->children[0]; break;
                    case NODE_48: {
                        node48_t *n48 = rn;
                        for (int i = 0; i < 256; i++)
                            if (n48->index[i] != NODE48_EMPTY) { r = n48->children[n48->index[i]]; break; }
                        break;
                    }
                    case NODE_256: {
                        node256_t *n256 = rn;
                        for (int i = 0; i < 256; i++)
                            if (n256->children[i]) { r = n256->children[i]; break; }
                        break;
                    }
                    default: return ref;
                    }
                }
                const uint8_t *lk = leaf_key(t, r);
                old_byte = lk[depth + prefix_match];
                uint8_t new_plen = plen - prefix_match - 1;
                uint8_t to_store = new_plen < MAX_PREFIX ? new_plen : MAX_PREFIX;
                memcpy(node_partial_mut(ref_ptr(t, ref)), lk + depth + prefix_match + 1, to_store);
                set_partial_len(ref_ptr(t, ref), new_plen);
            }

            uint8_t new_byte = key[depth + prefix_match];
            if (old_byte < new_byte) {
                nn->keys[0] = old_byte; nn->children[0] = ref;
                nn->keys[1] = new_byte; nn->children[1] = new_leaf;
            } else {
                nn->keys[0] = new_byte; nn->children[0] = new_leaf;
                nn->keys[1] = old_byte; nn->children[1] = ref;
            }
            nn->num_children = 2;
            return new_node;
        }
        depth += plen;
    }

    /* Mark this inner node dirty */
    mark_dirty(t, ref);

    uint8_t byte = (depth < KEY_SIZE) ? key[depth] : 0;
    hart_ref_t *child_ptr = find_child_ptr(t, ref, byte);

    if (child_ptr) {
        hart_ref_t old_child = *child_ptr;
        hart_ref_t new_child = insert_recursive(t, old_child, key, value, depth + 1, inserted);
        if (new_child != old_child) {
            child_ptr = find_child_ptr(t, ref, byte); /* re-resolve after realloc */
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
 * Delete (stub — to be ported when needed)
 * ========================================================================= */

bool hart_delete(hart_t *t, const uint8_t key[32]) {
    (void)t; (void)key;
    /* TODO: port from mem_art delete_recursive with path collapse */
    return false;
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
        void *n = ref_ptr(t, ref);
        uint8_t plen = node_partial_len(n);
        if (plen > 0) {
            int match = check_prefix(t, ref, n, key, depth);
            if (match != (int)plen) return NULL;
            depth += plen;
        }
        uint8_t byte = (depth < KEY_SIZE) ? key[depth] : 0;
        hart_ref_t *child = find_child_ptr(t, ref, byte);
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
        void *n = ref_ptr(t, ref);
        uint8_t plen = node_partial_len(n);
        if (plen > 0) {
            int match = check_prefix(t, ref, n, key, depth);
            if (match != (int)plen) return false;
            depth += plen;
        }
        uint8_t byte = (depth < KEY_SIZE) ? key[depth] : 0;
        hart_ref_t *child = find_child_ptr(t, ref, byte);
        if (!child) return false;
        ref = *child;
        depth++;
    }
    return false;
}

/* =========================================================================
 * MPT Root Hash — embedded hash version
 *
 * Same algorithm as art_mpt but reads/writes hash from/to the node
 * struct instead of a separate cache array.
 * ========================================================================= */

/* RLP helpers (same as art_mpt) */
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

/* Forward declaration */
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

/* Reconstruct full partial from leftmost leaf */
static void reconstruct_partial(hart_t *t, hart_ref_t ref, size_t byte_depth,
                                 uint8_t plen, uint8_t *out) {
    hart_ref_t r = ref;
    while (!HART_IS_LEAF(r)) {
        void *n = ref_ptr(t, r);
        switch (node_type(n)) {
        case NODE_4:   r = ((node4_t *)n)->children[0]; break;
        case NODE_16:  r = ((node16_t *)n)->children[0]; break;
        case NODE_48: {
            node48_t *n48 = n;
            for (int i = 0; i < 256; i++)
                if (n48->index[i] != NODE48_EMPTY) { r = n48->children[n48->index[i]]; break; }
            break;
        }
        case NODE_256: {
            node256_t *n256 = n;
            for (int i = 0; i < 256; i++)
                if (n256->children[i]) { r = n256->children[i]; break; }
            break;
        }
        default: return;
        }
    }
    memcpy(out, leaf_key(t, r) + byte_depth, plen);
}

static size_t hash_ref(hart_t *t, hart_ref_t ref, size_t byte_depth,
                        hart_encode_t encode, void *ctx,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out) {
    if (ref == HART_REF_NULL) return 0;

    if (HART_IS_LEAF(ref))
        return encode_leaf_node(t, ref, byte_depth, encode, ctx, nib_prefix, nib_prefix_len, rlp_out);

    /* Cache check — read embedded hash if clean */
    if (!is_node_dirty(t, ref)) {
        void *n = ref_ptr(t, ref);
        uint8_t cached_len;
        uint8_t cached[32];
        node_get_hash(n, cached, &cached_len);
        if (cached_len > 0) {
            /* Build extension if this node has a partial */
            uint8_t plen = node_partial_len(n);
            const uint8_t *partial = node_partial(n);
            uint8_t full_partial[64];
            if (plen > MAX_PREFIX) {
                reconstruct_partial(t, ref, byte_depth, plen, full_partial);
                partial = full_partial;
            }
            uint8_t full_pfx[128];
            size_t full_pfx_len = 0;
            if (nib_prefix_len > 0) { memcpy(full_pfx, nib_prefix, nib_prefix_len); full_pfx_len = nib_prefix_len; }
            for (size_t i = 0; i < plen; i++) {
                full_pfx[full_pfx_len++] = (partial[i] >> 4) & 0x0F;
                full_pfx[full_pfx_len++] =  partial[i]       & 0x0F;
            }
            if (full_pfx_len > 0)
                return encode_extension(full_pfx, full_pfx_len, cached, cached_len, rlp_out);
            memcpy(rlp_out, cached, cached_len);
            return cached_len;
        }
    }

    /* Compute: inner node hash */
    void *n = ref_ptr(t, ref);
    uint8_t plen = node_partial_len(n);
    const uint8_t *partial = node_partial(n);
    uint8_t full_partial[64];
    if (plen > MAX_PREFIX) {
        reconstruct_partial(t, ref, byte_depth, plen, full_partial);
        partial = full_partial;
    }

    uint8_t prefix[128];
    size_t prefix_len = 0;
    if (nib_prefix_len > 0) { memcpy(prefix, nib_prefix, nib_prefix_len); prefix_len = nib_prefix_len; }
    for (size_t i = 0; i < plen; i++) {
        prefix[prefix_len++] = (partial[i] >> 4) & 0x0F;
        prefix[prefix_len++] =  partial[i]       & 0x0F;
    }

    size_t next_depth = byte_depth + plen + 1;
    size_t child_pos = byte_depth + plen;

    /* Collect children */
    uint8_t child_keys[256];
    hart_ref_t child_refs[256];
    int nchildren = 0;
    switch (node_type(n)) {
    case NODE_4: {
        node4_t *nn = n;
        nchildren = nn->num_children;
        memcpy(child_keys, nn->keys, nchildren);
        memcpy(child_refs, nn->children, nchildren * sizeof(hart_ref_t));
        break;
    }
    case NODE_16: {
        node16_t *nn = n;
        nchildren = nn->num_children;
        memcpy(child_keys, nn->keys, nchildren);
        memcpy(child_refs, nn->children, nchildren * sizeof(hart_ref_t));
        break;
    }
    case NODE_48: {
        node48_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY) {
                child_keys[nchildren] = (uint8_t)i;
                child_refs[nchildren] = nn->children[nn->index[i]];
                nchildren++;
            }
        }
        break;
    }
    case NODE_256: {
        node256_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->children[i]) {
                child_keys[nchildren] = (uint8_t)i;
                child_refs[nchildren] = nn->children[i];
                nchildren++;
            }
        }
        break;
    }
    }

    if (nchildren == 1) {
        prefix[prefix_len++] = child_keys[0] >> 4;
        prefix[prefix_len++] = child_keys[0] & 0x0F;
        return hash_ref(t, child_refs[0], next_depth, encode, ctx, prefix, prefix_len, rlp_out);
    }

    /* Branch node: group by high nibble */
    typedef struct { uint8_t lo; hart_ref_t ref; } lo_entry_t;
    lo_entry_t groups[16][16];
    int gcounts[16] = {0};
    for (int i = 0; i < nchildren; i++) {
        uint8_t hi = child_keys[i] >> 4;
        uint8_t lo = child_keys[i] & 0x0F;
        groups[hi][gcounts[hi]].lo = lo;
        groups[hi][gcounts[hi]].ref = child_refs[i];
        gcounts[hi]++;
    }

    rlp_buf_t branch; rbuf_reset(&branch);
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) {
            uint8_t empty = 0x80;
            rbuf_append(&branch, &empty, 1);
        } else if (gcounts[hi] == 1) {
            uint8_t sub_prefix[128];
            size_t sub_len = 0;
            sub_prefix[sub_len++] = groups[hi][0].lo;
            uint8_t child_rlp[1024];
            size_t cl = hash_ref(t, groups[hi][0].ref, next_depth, encode, ctx, sub_prefix, sub_len, child_rlp);
            uint8_t cref[33];
            size_t crl = encode_child_ref(child_rlp, cl, cref);
            rbuf_append(&branch, cref, crl);
        } else {
            /* Sub-branch for this high nibble */
            rlp_buf_t sub_branch; rbuf_reset(&sub_branch);
            uint8_t used[16] = {0};
            for (int j = 0; j < gcounts[hi]; j++) used[groups[hi][j].lo] = 1;
            for (int lo = 0; lo < 16; lo++) {
                if (!used[lo]) {
                    uint8_t empty = 0x80;
                    rbuf_append(&sub_branch, &empty, 1);
                } else {
                    hart_ref_t cref = HART_REF_NULL;
                    for (int j = 0; j < gcounts[hi]; j++)
                        if (groups[hi][j].lo == (uint8_t)lo) { cref = groups[hi][j].ref; break; }
                    uint8_t child_rlp[1024];
                    size_t cl = hash_ref(t, cref, next_depth, encode, ctx, NULL, 0, child_rlp);
                    uint8_t cr[33];
                    size_t crl = encode_child_ref(child_rlp, cl, cr);
                    rbuf_append(&sub_branch, cr, crl);
                }
            }
            uint8_t empty = 0x80;
            rbuf_append(&sub_branch, &empty, 1); /* branch value */
            rlp_buf_t sub_enc; rbuf_reset(&sub_enc);
            rbuf_list_wrap(&sub_enc, &sub_branch);
            uint8_t cr[33];
            size_t crl = encode_child_ref(sub_enc.data, sub_enc.len, cr);
            rbuf_append(&branch, cr, crl);
        }
    }
    uint8_t empty = 0x80;
    rbuf_append(&branch, &empty, 1); /* branch value */

    rlp_buf_t node_rlp; rbuf_reset(&node_rlp);
    rbuf_list_wrap(&node_rlp, &branch);

    uint8_t node_hash[32];
    size_t node_hash_len = rlp_to_hashref(node_rlp.data, node_rlp.len, node_hash);

    /* Store hash in node */
    n = ref_ptr(t, ref); /* re-resolve */
    node_set_hash(n, node_hash, (uint8_t)node_hash_len);
    clear_dirty(t, ref);

    /* Wrap with extension if needed */
    if (prefix_len > 0)
        return encode_extension(prefix, prefix_len, node_hash, node_hash_len, rlp_out);
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
 * Iterator (stub — to be ported when needed)
 * ========================================================================= */

hart_iter_t *hart_iter_create(const hart_t *t) { (void)t; return NULL; }
bool hart_iter_next(hart_iter_t *it) { (void)it; return false; }
const uint8_t *hart_iter_key(const hart_iter_t *it) { (void)it; return NULL; }
const void *hart_iter_value(const hart_iter_t *it) { (void)it; return NULL; }
void hart_iter_destroy(hart_iter_t *it) { (void)it; }
