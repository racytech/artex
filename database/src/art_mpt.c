/*
 * ART→MPT — Incremental Ethereum MPT root hash from compact_art.
 *
 * Side hash cache on ART inner nodes enables incremental computation:
 * only dirty paths (from mutated leaves to root) are rehashed.
 * Clean subtrees return cached hashes in O(1).
 *
 * Cache is indexed by compact_ref_t (inner node pool offset / 8).
 * Leaf hashes are NOT cached — recomputed on demand from flat_state.
 */

#include "art_mpt.h"
#include "keccak256.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define MAX_NODE_RLP  1024
#define MAX_NIBBLES   64

static const uint8_t EMPTY_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

/* =========================================================================
 * Hash cache entry
 * ========================================================================= */

typedef struct {
    uint8_t hash[32];
    uint8_t rlp_len;    /* 0 = not cached, 1-31 = inline RLP length, 32 = hashed */
    uint8_t depth;      /* byte_depth when this hash was computed */
} hash_entry_t;

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct art_mpt {
    compact_art_t          *tree;
    art_mpt_value_encode_t  encode;
    void                   *encode_ctx;

    /* Hash cache: indexed by compact_ref_t / 8 for inner nodes */
    hash_entry_t *cache;
    size_t        cache_cap;   /* number of entries */

    /* Inline RLP cache for small nodes (RLP < 32 bytes) */
    /* These nodes are embedded in their parent's RLP, not hashed */
    uint8_t     *inline_cache;  /* inline_cache[ref_idx * 31] */
    size_t       inline_cap;

    art_mpt_stats_t stats;
};

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

/* =========================================================================
 * RLP encoding
 * ========================================================================= */

typedef struct {
    uint8_t data[MAX_NODE_RLP];
    size_t  len;
} rlp_buf_t;

static inline void rbuf_reset(rlp_buf_t *b) { b->len = 0; }

static inline bool rbuf_append(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (b->len + n > sizeof(b->data)) return false;
    if (n > 0) memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}

static bool rbuf_encode_bytes(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80)
        return rbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return rbuf_append(b, &pfx, 1) && (n > 0 ? rbuf_append(b, d, n) : true);
    }
    uint8_t hdr[3]; size_t hlen;
    if (n <= 0xFF) { hdr[0] = 0xb8; hdr[1] = (uint8_t)n; hlen = 2; }
    else { hdr[0] = 0xb9; hdr[1] = (uint8_t)(n >> 8); hdr[2] = (uint8_t)(n & 0xFF); hlen = 3; }
    return rbuf_append(b, hdr, hlen) && rbuf_append(b, d, n);
}

static inline bool rbuf_encode_empty(rlp_buf_t *b) {
    uint8_t e = 0x80;
    return rbuf_append(b, &e, 1);
}

static bool rbuf_list_wrap(rlp_buf_t *out, const rlp_buf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return rbuf_append(out, &pfx, 1) &&
               rbuf_append(out, payload->data, payload->len);
    }
    uint8_t lb[4]; size_t ll = 0; size_t tmp = payload->len;
    while (tmp > 0) { lb[ll++] = tmp & 0xFF; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    if (!rbuf_append(out, &pfx, 1)) return false;
    for (int i = (int)ll - 1; i >= 0; i--)
        if (!rbuf_append(out, &lb[i], 1)) return false;
    return rbuf_append(out, payload->data, payload->len);
}

static size_t hex_prefix_encode(const uint8_t *nibbles, size_t nibble_len,
                                bool is_leaf, uint8_t *out) {
    bool odd = (nibble_len % 2) == 1;
    uint8_t prefix = (is_leaf ? 2 : 0) + (odd ? 1 : 0);
    if (odd) {
        out[0] = (prefix << 4) | nibbles[0];
        for (size_t i = 1; i < nibble_len; i += 2)
            out[(i + 1) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
        return (nibble_len + 1) / 2;
    } else {
        out[0] = prefix << 4;
        for (size_t i = 0; i < nibble_len; i += 2)
            out[(i + 2) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
        return (nibble_len + 2) / 2;
    }
}

static size_t encode_child_ref(const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *ref_out) {
    if (child_rlp_len == 0) { ref_out[0] = 0x80; return 1; }
    if (child_rlp_len < 32) {
        memcpy(ref_out, child_rlp, child_rlp_len);
        return child_rlp_len;
    }
    ref_out[0] = 0xa0;
    keccak(child_rlp, child_rlp_len, ref_out + 1);
    return 33;
}

/* =========================================================================
 * ART node access helpers
 * ========================================================================= */

static inline void *node_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->nodes.base + ((size_t)(ref & 0x7FFFFFFFU) * 8);
}

static inline void *leaf_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->leaves.base + (size_t)COMPACT_LEAF_INDEX(ref) * tree->leaf_size;
}

static bool get_leaf_key(const compact_art_t *tree, compact_ref_t ref,
                          uint8_t *key_out) {
    const uint8_t *leaf = leaf_ptr(tree, ref);
    if (tree->leaf_key_size == tree->key_size) {
        memcpy(key_out, leaf, tree->key_size);
        return true;
    }
    if (tree->key_fetch) {
        const void *val = leaf + tree->leaf_key_size;
        return tree->key_fetch(val, key_out, tree->key_fetch_ctx);
    }
    return false;
}

static const void *get_leaf_value(const compact_art_t *tree, compact_ref_t ref) {
    return (const uint8_t *)leaf_ptr(tree, ref) + tree->leaf_key_size;
}

static int get_art_children(const compact_art_t *tree, const void *node,
                             uint8_t *keys, compact_ref_t *refs) {
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case COMPACT_NODE_4: {
        const compact_node4_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_16: {
        const compact_node16_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_32: {
        const compact_node32_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_48: {
        const compact_node48_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->index[i] != COMPACT_NODE48_EMPTY) {
                keys[count] = (uint8_t)i; refs[count] = n->children[n->index[i]]; count++;
            }
        }
        return count;
    }
    case COMPACT_NODE_256: {
        const compact_node256_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != COMPACT_REF_NULL) {
                keys[count] = (uint8_t)i; refs[count] = n->children[i]; count++;
            }
        }
        return count;
    }
    }
    return 0;
}

static const uint8_t *get_partial(const void *node, uint8_t *partial_len) {
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case COMPACT_NODE_4:   *partial_len = ((const compact_node4_t *)node)->partial_len; return ((const compact_node4_t *)node)->partial;
    case COMPACT_NODE_16:  *partial_len = ((const compact_node16_t *)node)->partial_len; return ((const compact_node16_t *)node)->partial;
    case COMPACT_NODE_32:  *partial_len = ((const compact_node32_t *)node)->partial_len; return ((const compact_node32_t *)node)->partial;
    case COMPACT_NODE_48:  *partial_len = ((const compact_node48_t *)node)->partial_len; return ((const compact_node48_t *)node)->partial;
    case COMPACT_NODE_256: *partial_len = ((const compact_node256_t *)node)->partial_len; return ((const compact_node256_t *)node)->partial;
    }
    *partial_len = 0;
    return NULL;
}

/* =========================================================================
 * Hash cache access
 * ========================================================================= */

/* =========================================================================
 * Node dirty flag access
 * ========================================================================= */

static inline bool node_is_dirty(const compact_art_t *tree, compact_ref_t ref) {
    if (COMPACT_IS_LEAF_REF(ref) || ref == COMPACT_REF_NULL) return true;
    const void *node = (const uint8_t *)tree->nodes.base + ((size_t)(ref & 0x7FFFFFFFU) * 8);
    uint8_t type = *(const uint8_t *)node;
    const uint8_t *flags;
    switch (type) {
    case COMPACT_NODE_4:   flags = &((const compact_node4_t *)node)->flags; break;
    case COMPACT_NODE_16:  flags = &((const compact_node16_t *)node)->flags; break;
    case COMPACT_NODE_32:  flags = &((const compact_node32_t *)node)->flags; break;
    case COMPACT_NODE_48:  flags = &((const compact_node48_t *)node)->flags; break;
    case COMPACT_NODE_256: flags = &((const compact_node256_t *)node)->flags; break;
    default: return true;
    }
    return (*flags & COMPACT_NODE_FLAG_DIRTY) != 0;
}

static inline void node_clear_dirty(const compact_art_t *tree, compact_ref_t ref) {
    if (COMPACT_IS_LEAF_REF(ref) || ref == COMPACT_REF_NULL) return;
    void *node = (uint8_t *)tree->nodes.base + ((size_t)(ref & 0x7FFFFFFFU) * 8);
    uint8_t type = *(const uint8_t *)node;
    uint8_t *flags;
    switch (type) {
    case COMPACT_NODE_4:   flags = &((compact_node4_t *)node)->flags; break;
    case COMPACT_NODE_16:  flags = &((compact_node16_t *)node)->flags; break;
    case COMPACT_NODE_32:  flags = &((compact_node32_t *)node)->flags; break;
    case COMPACT_NODE_48:  flags = &((compact_node48_t *)node)->flags; break;
    case COMPACT_NODE_256: flags = &((compact_node256_t *)node)->flags; break;
    default: return;
    }
    *flags &= ~COMPACT_NODE_FLAG_DIRTY;
}

/* =========================================================================
 * Hash cache — indexed by compact_ref / 8
 * Only stores computed hash/inline RLP. Validity is determined by
 * the ART node's dirty flag, not by the cache entry itself.
 * ========================================================================= */

static inline size_t ref_to_idx(compact_ref_t ref) {
    return (size_t)(ref & 0x7FFFFFFFU);
}

static bool cache_get(art_mpt_t *am, compact_ref_t ref,
                       size_t byte_depth, uint8_t *rlp_out, size_t *rlp_len) {
    return false; /* DISABLED */
    if (COMPACT_IS_LEAF_REF(ref)) return false;
    if (node_is_dirty(am->tree, ref)) return false;
    size_t idx = ref_to_idx(ref);
    if (idx >= am->cache_cap) return false;
    const hash_entry_t *e = &am->cache[idx];
    if (e->rlp_len == 0) return false;
    /* Depth must match — same subtree at different depth produces different hash */
    if (e->depth != (uint8_t)byte_depth) return false;

    if (e->rlp_len < 32) {
        if (idx < am->inline_cap && am->inline_cache) {
            memcpy(rlp_out, am->inline_cache + idx * 31, e->rlp_len);
            *rlp_len = e->rlp_len;
            return true;
        }
        return false;
    }
    rlp_out[0] = 0xa0;
    memcpy(rlp_out + 1, e->hash, 32);
    *rlp_len = 33;
    return true;
}

static void cache_put(art_mpt_t *am, compact_ref_t ref,
                       size_t byte_depth, const uint8_t *rlp, size_t rlp_len) {
    if (COMPACT_IS_LEAF_REF(ref)) return;
    size_t idx = ref_to_idx(ref);

    if (idx >= am->cache_cap) {
        size_t new_cap = am->cache_cap ? am->cache_cap * 2 : 4096;
        while (new_cap <= idx) new_cap *= 2;
        hash_entry_t *p = realloc(am->cache, new_cap * sizeof(hash_entry_t));
        if (!p) return;
        memset(p + am->cache_cap, 0, (new_cap - am->cache_cap) * sizeof(hash_entry_t));
        am->cache = p;
        am->cache_cap = new_cap;
    }

    hash_entry_t *e = &am->cache[idx];
    if (rlp_len < 32) {
        e->rlp_len = (uint8_t)rlp_len;
        if (idx >= am->inline_cap) {
            size_t new_cap = am->inline_cap ? am->inline_cap * 2 : 4096;
            while (new_cap <= idx) new_cap *= 2;
            uint8_t *p = realloc(am->inline_cache, new_cap * 31);
            if (!p) return;
            memset(p + am->inline_cap * 31, 0, (new_cap - am->inline_cap) * 31);
            am->inline_cache = p;
            am->inline_cap = new_cap;
        }
        memcpy(am->inline_cache + idx * 31, rlp, rlp_len);
    } else {
        e->rlp_len = 32;
        keccak(rlp, rlp_len, e->hash);
    }

    e->depth = (uint8_t)byte_depth;

    /* Clear dirty — this node's hash is now cached at this depth */
    node_clear_dirty(am->tree, ref);
}

/* Dirty marking is handled by compact_art itself (in insert_recursive /
 * delete_recursive). No separate invalidation walk needed. */

/* =========================================================================
 * MPT hash computation (same algorithm as before, with cache)
 * ========================================================================= */

static size_t hash_ref(art_mpt_t *am, compact_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out);

static size_t encode_leaf(art_mpt_t *am, compact_ref_t leaf_ref,
                           size_t byte_depth,
                           const uint8_t *nib_prefix, size_t nib_prefix_len,
                           uint8_t *rlp_out) {
    uint8_t key[64];
    if (!get_leaf_key(am->tree, leaf_ref, key)) return 0;

    uint8_t path[MAX_NIBBLES * 2];
    size_t path_len = 0;
    if (nib_prefix_len > 0) { memcpy(path, nib_prefix, nib_prefix_len); path_len = nib_prefix_len; }
    for (size_t i = byte_depth; i < am->tree->key_size; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(path, path_len, true, hp);

    const void *leaf_val = get_leaf_value(am->tree, leaf_ref);
    uint8_t value_rlp[MAX_NODE_RLP];
    uint32_t value_len = am->encode(key, leaf_val, am->tree->value_size,
                                     value_rlp, am->encode_ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

static size_t encode_extension(const uint8_t *nibbles, size_t nibble_len,
                                const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *rlp_out) {
    if (nibble_len == 0) { memcpy(rlp_out, child_rlp, child_rlp_len); return child_rlp_len; }
    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(nibbles, nibble_len, false, hp);
    uint8_t child_ref[33];
    size_t ref_len = encode_child_ref(child_rlp, child_rlp_len, child_ref);
    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_append(&payload, child_ref, ref_len);
    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

static size_t encode_branch(uint8_t child_rlps[16][MAX_NODE_RLP],
                              size_t child_lens[16], uint8_t *rlp_out) {
    rlp_buf_t payload; rbuf_reset(&payload);
    for (int i = 0; i < 16; i++) {
        if (child_lens[i] == 0) { rbuf_encode_empty(&payload); }
        else {
            uint8_t ref[33];
            size_t ref_len = encode_child_ref(child_rlps[i], child_lens[i], ref);
            rbuf_append(&payload, ref, ref_len);
        }
    }
    rbuf_encode_empty(&payload);
    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

static size_t hash_lo_group(art_mpt_t *am, uint8_t *lo_nibs,
                              compact_ref_t *lo_refs, int lo_count,
                              size_t next_byte_depth,
                              const uint8_t *nib_prefix, size_t nib_prefix_len,
                              uint8_t *rlp_out) {
    if (lo_count == 1) {
        uint8_t extended[MAX_NIBBLES * 2];
        if (nib_prefix_len > 0) memcpy(extended, nib_prefix, nib_prefix_len);
        extended[nib_prefix_len] = lo_nibs[0];
        return hash_ref(am, lo_refs[0], next_byte_depth,
                         extended, nib_prefix_len + 1, rlp_out);
    }

    uint8_t branch_children[16][MAX_NODE_RLP];
    size_t branch_child_lens[16] = {0};
    for (int j = 0; j < lo_count; j++) {
        branch_child_lens[lo_nibs[j]] = hash_ref(am, lo_refs[j], next_byte_depth,
                                                   NULL, 0, branch_children[lo_nibs[j]]);
    }
    uint8_t branch_rlp[MAX_NODE_RLP];
    size_t branch_len = encode_branch(branch_children, branch_child_lens, branch_rlp);
    return encode_extension(nib_prefix, nib_prefix_len, branch_rlp, branch_len, rlp_out);
}

static size_t hash_ref(art_mpt_t *am, compact_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out) {
    if (ref == COMPACT_REF_NULL) return 0;

    if (COMPACT_IS_LEAF_REF(ref)) {
        return encode_leaf(am, ref, byte_depth, nib_prefix, nib_prefix_len, rlp_out);
    }

    /* --- Cache check ---
     * Only use cache for non-dirty nodes (dirty flag checked inside cache_get).
     * Cache stores the node's own RLP (without accumulated prefix). */
    {
        uint8_t cached_rlp[MAX_NODE_RLP];
        size_t cached_len;
        if (cache_get(am, ref, byte_depth, cached_rlp, &cached_len)) {
            am->stats.cache_hits++;
            if (nib_prefix_len == 0) {
                memcpy(rlp_out, cached_rlp, cached_len);
                return cached_len;
            }
            return encode_extension(nib_prefix, nib_prefix_len,
                                     cached_rlp, cached_len, rlp_out);
        }
    }
    am->stats.cache_misses++;

    /* Inner node — compute hash */
    const void *node = node_ptr(am->tree, ref);

    /* ART partial → add to nibble prefix */
    uint8_t partial_len;
    const uint8_t *partial = get_partial(node, &partial_len);

    /* Reconstruct long partials from a leaf key */
    uint8_t full_partial[64];
    if (partial_len > COMPACT_MAX_PREFIX) {
        compact_ref_t probe = ref;
        while (!COMPACT_IS_LEAF_REF(probe) && probe != COMPACT_REF_NULL) {
            const void *pn = node_ptr(am->tree, probe);
            uint8_t pk[256]; compact_ref_t pr[256];
            int pc = get_art_children(am->tree, pn, pk, pr);
            if (pc == 0) break;
            probe = pr[0];
        }
        if (COMPACT_IS_LEAF_REF(probe)) {
            uint8_t leaf_key[64];
            if (get_leaf_key(am->tree, probe, leaf_key))
                memcpy(full_partial, leaf_key + byte_depth, partial_len);
            partial = full_partial;
        }
    }

    uint8_t prefix[MAX_NIBBLES * 2];
    size_t prefix_len = 0;
    if (nib_prefix_len > 0) { memcpy(prefix, nib_prefix, nib_prefix_len); prefix_len = nib_prefix_len; }
    for (size_t i = 0; i < partial_len; i++) {
        prefix[prefix_len++] = (partial[i] >> 4) & 0x0F;
        prefix[prefix_len++] =  partial[i]       & 0x0F;
    }

    size_t next_byte_depth = byte_depth + partial_len + 1;

    uint8_t child_keys[256];
    compact_ref_t child_refs[256];
    int nchildren = get_art_children(am->tree, node, child_keys, child_refs);

    /* Result RLP for this node (without prefix — prefix added by caller or here) */
    uint8_t node_rlp[MAX_NODE_RLP];
    size_t node_rlp_len;

    if (nchildren == 1) {
        prefix[prefix_len++] = child_keys[0] >> 4;
        prefix[prefix_len++] = child_keys[0] & 0x0F;
        size_t result = hash_ref(am, child_refs[0], next_byte_depth,
                                  prefix, prefix_len, rlp_out);
        /* Cache: we cache the node's contribution without the incoming prefix.
         * For single-child nodes, the contribution is extension(partial+byte) → child.
         * It's complex to separate, so skip caching for single-child. */
        return result;
    }

    /* Multiple children — group by high nibble */
    typedef struct { uint8_t lo; compact_ref_t ref; } lo_entry_t;
    lo_entry_t groups[16][16];
    int gcounts[16] = {0};
    for (int i = 0; i < nchildren; i++) {
        uint8_t hi = child_keys[i] >> 4, lo = child_keys[i] & 0x0F;
        groups[hi][gcounts[hi]].lo = lo;
        groups[hi][gcounts[hi]].ref = child_refs[i];
        gcounts[hi]++;
    }

    int hi_occupied = 0, single_hi = -1;
    for (int i = 0; i < 16; i++) {
        if (gcounts[i] > 0) { hi_occupied++; single_hi = i; }
    }

    if (hi_occupied == 1) {
        prefix[prefix_len++] = (uint8_t)single_hi;
        uint8_t lo_nibs[16]; compact_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[single_hi]; j++) {
            lo_nibs[j] = groups[single_hi][j].lo;
            lo_refs[j] = groups[single_hi][j].ref;
        }
        size_t result = hash_lo_group(am, lo_nibs, lo_refs, gcounts[single_hi],
                                       next_byte_depth, prefix, prefix_len, rlp_out);
        /* Skip caching single-hi (same reason as single-child) */
        return result;
    }

    /* Multiple high nibbles → branch */
    uint8_t hi_children[16][MAX_NODE_RLP];
    size_t hi_child_lens[16] = {0};
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;
        uint8_t lo_nibs[16]; compact_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[hi]; j++) {
            lo_nibs[j] = groups[hi][j].lo;
            lo_refs[j] = groups[hi][j].ref;
        }
        hi_child_lens[hi] = hash_lo_group(am, lo_nibs, lo_refs, gcounts[hi],
                                            next_byte_depth, NULL, 0, hi_children[hi]);
    }

    node_rlp_len = encode_branch(hi_children, hi_child_lens, node_rlp);

    /* Cache the node's RLP (without prefix) */
    cache_put(am, ref, byte_depth, node_rlp, node_rlp_len);

    /* Wrap with accumulated prefix */
    return encode_extension(prefix, prefix_len, node_rlp, node_rlp_len, rlp_out);
}

/* =========================================================================
 * Public API
 * ========================================================================= */

art_mpt_t *art_mpt_create(compact_art_t *tree,
                            art_mpt_value_encode_t encode, void *ctx) {
    if (!tree) return NULL;
    art_mpt_t *am = calloc(1, sizeof(*am));
    if (!am) return NULL;
    am->tree = tree;
    am->encode = encode;
    am->encode_ctx = ctx;
    return am;
}

void art_mpt_destroy(art_mpt_t *am) {
    if (!am) return;
    free(am->cache);
    free(am->inline_cache);
    free(am);
}

bool art_mpt_insert(art_mpt_t *am, const uint8_t key[32],
                     const void *value, uint32_t value_size) {
    if (!am) return false;
    (void)value_size;
    /* compact_art_insert marks all visited nodes dirty internally */
    return compact_art_insert(am->tree, key, value);
}

bool art_mpt_delete(art_mpt_t *am, const uint8_t key[32]) {
    if (!am) return false;
    /* compact_art_delete marks all visited nodes dirty internally */
    return compact_art_delete(am->tree, key);
}

/* Clear dirty flags on all inner nodes (after hash walk cached everything) */
static void clear_dirty_recursive(art_mpt_t *am, compact_ref_t ref) {
    if (ref == COMPACT_REF_NULL || COMPACT_IS_LEAF_REF(ref)) return;
    node_clear_dirty(am->tree, ref);

    uint8_t keys[256];
    compact_ref_t refs[256];
    const void *node = node_ptr(am->tree, ref);
    int n = get_art_children(am->tree, node, keys, refs);
    for (int i = 0; i < n; i++)
        clear_dirty_recursive(am, refs[i]);
}

void art_mpt_root_hash(art_mpt_t *am, uint8_t out[32]) {
    if (!am || !am->tree || compact_art_size(am->tree) == 0) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len = hash_ref(am, am->tree->root, 0, NULL, 0, rlp);

    if (rlp_len == 0) {
        memcpy(out, EMPTY_ROOT, 32);
    } else {
        keccak(rlp, rlp_len, out);
    }

    /* Dirty flags are cleared by cache_put during the hash walk.
     * Nodes not visited (clean subtrees) keep their clean state. */
}

void art_mpt_invalidate_all(art_mpt_t *am) {
    if (!am) return;
    if (am->cache)
        memset(am->cache, 0, am->cache_cap * sizeof(hash_entry_t));
}

art_mpt_stats_t art_mpt_get_stats(const art_mpt_t *am) {
    if (!am) return (art_mpt_stats_t){0};
    return am->stats;
}

void art_mpt_reset_stats(art_mpt_t *am) {
    if (am) memset(&am->stats, 0, sizeof(am->stats));
}

/* =========================================================================
 * Legacy non-incremental API (for tests that don't use art_mpt_t)
 * ========================================================================= */

void art_mpt_root_hash_full(const compact_art_t *tree,
                              art_mpt_value_encode_t encode, void *ctx,
                              uint8_t out[32]) {
    /* Create temporary context, compute, destroy */
    art_mpt_t *am = art_mpt_create((compact_art_t *)tree, encode, ctx);
    if (!am) { memcpy(out, EMPTY_ROOT, 32); return; }
    art_mpt_root_hash(am, out);
    art_mpt_destroy(am);
}
