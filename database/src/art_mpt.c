/*
 * ART→MPT — Incremental Ethereum MPT root hash from any ART tree.
 *
 * Uses art_iface_t vtable to abstract over compact_art and mem_art.
 * Side hash cache on ART inner nodes enables incremental computation:
 * only dirty paths (from mutated leaves to root) are rehashed.
 * Clean subtrees return cached hashes in O(1).
 *
 * Cache is indexed by art_ref_t (inner node offset).
 * Leaf hashes are NOT cached — recomputed on demand.
 */

#include "art_mpt.h"
#include "art_iface.h"
#include "keccak256.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define MAX_NODE_RLP  1024
#define MAX_NIBBLES   128   /* supports up to 64-byte keys */

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
    uint8_t data[32];   /* inline RLP (len < 32) or keccak hash (len == 32) */
    uint8_t len;        /* 0 = not cached, 1-31 = inline, 32 = hash */
    uint8_t depth;      /* byte_depth when computed */
} hash_entry_t;

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct art_mpt {
    art_iface_t             iface;
    art_mpt_value_encode_t  encode;
    void                   *encode_ctx;

    hash_entry_t *cache;
    size_t        cache_cap;

    bool             no_cache;
    uint32_t         mpt_key_offset;
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

static size_t rlp_to_hashref(const uint8_t *rlp, size_t rlp_len, uint8_t *out) {
    if (rlp_len < 32) { memcpy(out, rlp, rlp_len); return rlp_len; }
    keccak(rlp, rlp_len, out);
    return 32;
}

static size_t encode_child_ref(const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *ref_out) {
    if (child_rlp_len == 0) { ref_out[0] = 0x80; return 1; }
    if (child_rlp_len < 32) { memcpy(ref_out, child_rlp, child_rlp_len); return child_rlp_len; }
    if (child_rlp_len == 32) { ref_out[0] = 0xa0; memcpy(ref_out + 1, child_rlp, 32); return 33; }
    ref_out[0] = 0xa0;
    keccak(child_rlp, child_rlp_len, ref_out + 1);
    return 33;
}

/* =========================================================================
 * Hash cache — indexed by ref offset
 * ========================================================================= */

static inline size_t ref_to_idx(art_ref_t ref) {
    return (size_t)(ref & 0x7FFFFFFFu);
}

static bool cache_get(art_mpt_t *am, art_ref_t ref,
                       size_t byte_depth, uint8_t *out, size_t *out_len) {
    if (ART_IS_LEAF(ref)) return false;
    if (am->iface.is_dirty(am->iface.ctx, ref)) return false;
    size_t idx = ref_to_idx(ref);
    if (idx >= am->cache_cap) return false;
    const hash_entry_t *e = &am->cache[idx];
    if (e->len == 0) return false;
    if (e->depth != (uint8_t)byte_depth) return false;
    memcpy(out, e->data, e->len);
    *out_len = e->len;
    return true;
}

static void cache_put(art_mpt_t *am, art_ref_t ref,
                       size_t byte_depth, const uint8_t *data, size_t data_len) {
    if (ART_IS_LEAF(ref)) return;
    if (am->no_cache) return;
    if (data_len == 0 || data_len > 32) return;

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
    memcpy(e->data, data, data_len);
    e->len = (uint8_t)data_len;
    e->depth = (uint8_t)byte_depth;

    am->iface.clear_dirty(am->iface.ctx, ref);
}

/* =========================================================================
 * Long partial reconstruction — when partial_len > max_prefix,
 * walk down to a descendant leaf and read the full key.
 * ========================================================================= */

static void reconstruct_partial(art_mpt_t *am, art_ref_t ref,
                                 size_t byte_depth, uint8_t partial_len,
                                 uint8_t *out) {
    /* Walk to leftmost leaf descendant */
    art_ref_t probe = ref;
    while (!ART_IS_LEAF(probe) && probe != ART_REF_NULL) {
        void *pn = am->iface.node_ptr(am->iface.ctx, probe);
        uint8_t pk[256]; art_ref_t pr[256];
        int pc = am->iface.node_children(am->iface.ctx, pn, pk, pr);
        if (pc == 0) break;
        probe = pr[0];
    }
    if (ART_IS_LEAF(probe)) {
        uint8_t lk[64];
        if (am->iface.leaf_key(am->iface.ctx, probe, lk))
            memcpy(out, lk + byte_depth, partial_len);
    }
}

/* =========================================================================
 * MPT hash computation
 * ========================================================================= */

static size_t hash_ref(art_mpt_t *am, art_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out);

static size_t encode_leaf(art_mpt_t *am, art_ref_t leaf_ref,
                           size_t byte_depth,
                           const uint8_t *nib_prefix, size_t nib_prefix_len,
                           uint8_t *rlp_out) {
    uint8_t key[64];
    if (!am->iface.leaf_key(am->iface.ctx, leaf_ref, key)) return 0;

    uint8_t path[MAX_NIBBLES * 2];
    size_t path_len = 0;
    if (nib_prefix_len > 0) { memcpy(path, nib_prefix, nib_prefix_len); path_len = nib_prefix_len; }
    size_t path_start = byte_depth > am->mpt_key_offset ? byte_depth : am->mpt_key_offset;
    for (size_t i = path_start; i < am->iface.key_size; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(path, path_len, true, hp);

    const void *leaf_val = am->iface.leaf_value(am->iface.ctx, leaf_ref);
    uint8_t value_rlp[MAX_NODE_RLP];
    uint32_t value_len = am->encode(key, leaf_val, am->iface.value_size,
                                     value_rlp, am->encode_ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return rlp_to_hashref(encoded.data, encoded.len, rlp_out);
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
    return rlp_to_hashref(encoded.data, encoded.len, rlp_out);
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
    return rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t hash_lo_group(art_mpt_t *am, uint8_t *lo_nibs,
                              art_ref_t *lo_refs, int lo_count,
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

static size_t hash_ref(art_mpt_t *am, art_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out) {
    if (ref == ART_REF_NULL) return 0;

    if (ART_IS_LEAF(ref)) {
        return encode_leaf(am, ref, byte_depth, nib_prefix, nib_prefix_len, rlp_out);
    }

    /* --- Cache check --- */
    {
        uint8_t cached_rlp[MAX_NODE_RLP];
        size_t cached_len;
        if (cache_get(am, ref, byte_depth, cached_rlp, &cached_len)) {
            am->stats.cache_hits++;

            const void *cnode = am->iface.node_ptr(am->iface.ctx, ref);
            uint8_t cpartial_len;
            const uint8_t *cpartial = am->iface.node_partial(am->iface.ctx, cnode, &cpartial_len);

            uint8_t cfull_partial[64];
            if (cpartial_len > am->iface.max_prefix) {
                reconstruct_partial(am, ref, byte_depth, cpartial_len, cfull_partial);
                cpartial = cfull_partial;
            }

            uint8_t full_pfx[MAX_NIBBLES * 2];
            size_t full_pfx_len = 0;
            if (nib_prefix_len > 0) {
                memcpy(full_pfx, nib_prefix, nib_prefix_len);
                full_pfx_len = nib_prefix_len;
            }
            for (size_t i = 0; i < cpartial_len; i++) {
                if (byte_depth + i >= am->mpt_key_offset) {
                    full_pfx[full_pfx_len++] = (cpartial[i] >> 4) & 0x0F;
                    full_pfx[full_pfx_len++] =  cpartial[i]       & 0x0F;
                }
            }

            return encode_extension(full_pfx, full_pfx_len,
                                     cached_rlp, cached_len, rlp_out);
        }
    }
    am->stats.cache_misses++;

    /* Inner node — compute hash */
    const void *node = am->iface.node_ptr(am->iface.ctx, ref);

    uint8_t partial_len;
    const uint8_t *partial = am->iface.node_partial(am->iface.ctx, node, &partial_len);

    uint8_t full_partial[64];
    if (partial_len > am->iface.max_prefix) {
        reconstruct_partial(am, ref, byte_depth, partial_len, full_partial);
        partial = full_partial;
    }

    uint8_t prefix[MAX_NIBBLES * 2];
    size_t prefix_len = 0;
    if (nib_prefix_len > 0) { memcpy(prefix, nib_prefix, nib_prefix_len); prefix_len = nib_prefix_len; }
    for (size_t i = 0; i < partial_len; i++) {
        if (byte_depth + i >= am->mpt_key_offset) {
            prefix[prefix_len++] = (partial[i] >> 4) & 0x0F;
            prefix[prefix_len++] =  partial[i]       & 0x0F;
        }
    }

    size_t next_byte_depth = byte_depth + partial_len + 1;
    size_t child_byte_pos = byte_depth + partial_len;

    uint8_t child_keys[256];
    art_ref_t child_refs[256];
    int nchildren = am->iface.node_children(am->iface.ctx, node, child_keys, child_refs);

    if (nchildren == 1) {
        if (child_byte_pos >= am->mpt_key_offset) {
            prefix[prefix_len++] = child_keys[0] >> 4;
            prefix[prefix_len++] = child_keys[0] & 0x0F;
        }
        return hash_ref(am, child_refs[0], next_byte_depth,
                          prefix, prefix_len, rlp_out);
    }

    /* Multiple children — group by high nibble */
    typedef struct { uint8_t lo; art_ref_t ref; } lo_entry_t;
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
        if (child_byte_pos >= am->mpt_key_offset)
            prefix[prefix_len++] = (uint8_t)single_hi;
        uint8_t lo_nibs[16]; art_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[single_hi]; j++) {
            lo_nibs[j] = groups[single_hi][j].lo;
            lo_refs[j] = groups[single_hi][j].ref;
        }
        return hash_lo_group(am, lo_nibs, lo_refs, gcounts[single_hi],
                              next_byte_depth, prefix, prefix_len, rlp_out);
    }

    /* Multiple high nibbles → branch */
    uint8_t hi_children[16][MAX_NODE_RLP];
    size_t hi_child_lens[16] = {0};
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;
        uint8_t lo_nibs[16]; art_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[hi]; j++) {
            lo_nibs[j] = groups[hi][j].lo;
            lo_refs[j] = groups[hi][j].ref;
        }
        hi_child_lens[hi] = hash_lo_group(am, lo_nibs, lo_refs, gcounts[hi],
                                            next_byte_depth, NULL, 0, hi_children[hi]);
    }

    uint8_t node_rlp[MAX_NODE_RLP];
    size_t node_rlp_len = encode_branch(hi_children, hi_child_lens, node_rlp);

    cache_put(am, ref, byte_depth, node_rlp, node_rlp_len);

    return encode_extension(prefix, prefix_len, node_rlp, node_rlp_len, rlp_out);
}

/* =========================================================================
 * Public API
 * ========================================================================= */

art_mpt_t *art_mpt_create_iface(art_iface_t iface,
                                  art_mpt_value_encode_t encode, void *ctx) {
    if (!iface.ctx) return NULL;
    art_mpt_t *am = calloc(1, sizeof(*am));
    if (!am) return NULL;
    am->iface = iface;
    am->encode = encode;
    am->encode_ctx = ctx;
    return am;
}

void art_mpt_set_no_cache(art_mpt_t *am, bool disable) {
    if (am) am->no_cache = disable;
}

art_mpt_t *art_mpt_create(compact_art_t *tree,
                            art_mpt_value_encode_t encode, void *ctx) {
    if (!tree) return NULL;
    return art_mpt_create_iface(art_iface_compact(tree), encode, ctx);
}

void art_mpt_destroy(art_mpt_t *am) {
    if (!am) return;
    free(am->cache);
    free(am);
}

bool art_mpt_insert(art_mpt_t *am, const uint8_t key[32],
                     const void *value, uint32_t value_size) {
    if (!am) return false;
    (void)value_size;
    return am->iface.insert(am->iface.ctx, key, value);
}

bool art_mpt_delete(art_mpt_t *am, const uint8_t key[32]) {
    if (!am) return false;
    return am->iface.del(am->iface.ctx, key);
}

void art_mpt_root_hash(art_mpt_t *am, uint8_t out[32]) {
    if (!am || am->iface.size(am->iface.ctx) == 0) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    art_ref_t root = am->iface.root(am->iface.ctx);
    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len = hash_ref(am, root, 0, NULL, 0, rlp);

    if (rlp_len == 0) {
        memcpy(out, EMPTY_ROOT, 32);
    } else if (rlp_len == 32) {
        memcpy(out, rlp, 32);
    } else {
        keccak(rlp, rlp_len, out);
    }
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
 * Subtree hash — compute MPT root for a prefix-identified subtree
 * ========================================================================= */

void art_mpt_subtree_hash(art_mpt_t *am,
                            const uint8_t *prefix, uint32_t prefix_len,
                            uint8_t out[32]) {
    if (!am || !prefix || !am->iface.find_subtree) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    uint32_t depth_out;
    art_ref_t subtree = am->iface.find_subtree(am->iface.ctx, prefix,
                                                 prefix_len, &depth_out);
    if (subtree == ART_REF_NULL) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    uint32_t saved_offset = am->mpt_key_offset;
    am->mpt_key_offset = prefix_len;

    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len = hash_ref(am, subtree, depth_out, NULL, 0, rlp);

    am->mpt_key_offset = saved_offset;

    if (rlp_len == 0) {
        memcpy(out, EMPTY_ROOT, 32);
    } else if (rlp_len == 32) {
        memcpy(out, rlp, 32);
    } else {
        keccak(rlp, rlp_len, out);
    }
}

/* =========================================================================
 * Legacy non-incremental API
 * ========================================================================= */

void art_mpt_root_hash_full(const compact_art_t *tree,
                              art_mpt_value_encode_t encode, void *ctx,
                              uint8_t out[32]) {
    art_mpt_t *am = art_mpt_create((compact_art_t *)tree, encode, ctx);
    if (!am) { memcpy(out, EMPTY_ROOT, 32); return; }
    am->no_cache = true;
    art_mpt_root_hash(am, out);
    art_mpt_destroy(am);
}
