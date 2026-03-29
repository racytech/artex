/*
 * ART→MPT Hash — compute Ethereum MPT root hash by walking compact_art.
 *
 * ART uses byte-level branching. MPT uses nibble-level (4-bit).
 * Each ART inner node maps to two levels of MPT branch nodes.
 * ART partial keys map to MPT extension nodes (doubled in nibbles).
 *
 * The walk is recursive: at each ART node, we produce the MPT RLP
 * encoding and hash it. Leaves get their value from a callback.
 */

#include "art_mpt.h"
#include "keccak256.h"

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
 * Helpers
 * ========================================================================= */

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

/* =========================================================================
 * RLP encoding (same helpers as mpt_trie/mpt_arena)
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
    uint8_t hdr[3];
    size_t hlen;
    if (n <= 0xFF) {
        hdr[0] = 0xb8; hdr[1] = (uint8_t)n; hlen = 2;
    } else {
        hdr[0] = 0xb9; hdr[1] = (uint8_t)(n >> 8); hdr[2] = (uint8_t)(n & 0xFF); hlen = 3;
    }
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
    uint8_t lb[4];
    size_t ll = 0;
    size_t tmp = payload->len;
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

/* =========================================================================
 * Child reference encoding
 *
 * A child reference in MPT is either:
 *   - 0x80 (empty)
 *   - inline RLP (< 32 bytes, raw bytes embedded)
 *   - 0xa0 + keccak256(RLP) (33 bytes, hash reference)
 * ========================================================================= */

/* Encode a child's RLP as a reference suitable for embedding in a parent.
 * child_rlp/child_rlp_len: the child's full RLP encoding.
 * ref_out: buffer for the reference (at least 33 bytes).
 * Returns reference length. */
static size_t encode_child_ref(const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *ref_out) {
    if (child_rlp_len == 0) {
        ref_out[0] = 0x80;
        return 1;
    }
    if (child_rlp_len < 32) {
        /* Inline */
        memcpy(ref_out, child_rlp, child_rlp_len);
        return child_rlp_len;
    }
    /* Hash reference */
    ref_out[0] = 0xa0;
    keccak(child_rlp, child_rlp_len, ref_out + 1);
    return 33;
}

/* =========================================================================
 * ART node access helpers
 * ========================================================================= */

typedef struct {
    const compact_art_t *tree;
    art_mpt_value_encode_t encode;
    void *ctx;
} walk_ctx_t;

static inline void *node_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->nodes.base + ((size_t)(ref & 0x7FFFFFFFU) * 8);
}

static inline void *leaf_ptr(const compact_art_t *tree, compact_ref_t ref) {
    return tree->leaves.base + (size_t)COMPACT_LEAF_INDEX(ref) * tree->leaf_size;
}

/* Get the full key for a leaf. In compact_leaves mode, only 8-byte hash
 * is stored — we'd need key_fetch. For non-compact mode, key is inline. */
static bool get_leaf_key(const compact_art_t *tree, compact_ref_t ref,
                          uint8_t *key_out) {
    const uint8_t *leaf = leaf_ptr(tree, ref);
    if (tree->leaf_key_size == tree->key_size) {
        /* Full key stored in leaf */
        memcpy(key_out, leaf, tree->key_size);
        return true;
    }
    /* Compact leaves — need key_fetch callback */
    if (tree->key_fetch) {
        const void *val = leaf + tree->leaf_key_size;
        return tree->key_fetch(val, key_out, tree->key_fetch_ctx);
    }
    return false;
}

static const void *get_leaf_value(const compact_art_t *tree, compact_ref_t ref) {
    const uint8_t *leaf = leaf_ptr(tree, ref);
    return leaf + tree->leaf_key_size;
}

/* Get children of an ART node: fills keys[] and refs[] arrays.
 * Returns number of children. */
static int get_art_children(const compact_art_t *tree, const void *node,
                             uint8_t *keys, compact_ref_t *refs) {
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case COMPACT_NODE_4: {
        const compact_node4_t *n = node;
        for (int i = 0; i < n->num_children; i++) {
            keys[i] = n->keys[i];
            refs[i] = n->children[i];
        }
        return n->num_children;
    }
    case COMPACT_NODE_16: {
        const compact_node16_t *n = node;
        for (int i = 0; i < n->num_children; i++) {
            keys[i] = n->keys[i];
            refs[i] = n->children[i];
        }
        return n->num_children;
    }
    case COMPACT_NODE_32: {
        const compact_node32_t *n = node;
        for (int i = 0; i < n->num_children; i++) {
            keys[i] = n->keys[i];
            refs[i] = n->children[i];
        }
        return n->num_children;
    }
    case COMPACT_NODE_48: {
        const compact_node48_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->index[i] != COMPACT_NODE48_EMPTY) {
                keys[count] = (uint8_t)i;
                refs[count] = n->children[n->index[i]];
                count++;
            }
        }
        return count;
    }
    case COMPACT_NODE_256: {
        const compact_node256_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != COMPACT_REF_NULL) {
                keys[count] = (uint8_t)i;
                refs[count] = n->children[i];
                count++;
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
 * MPT hash computation from ART structure
 *
 * Core approach: at each ART inner node, collect all children and decompose
 * into nibble-level MPT structure. Accumulate a nibble prefix to avoid
 * creating non-canonical consecutive extensions.
 *
 * hash_ref(ref, byte_depth, nib_prefix, nib_prefix_len) → RLP
 *
 * The nib_prefix accumulates nibbles from:
 *   - ART partial bytes (×2 nibbles each)
 *   - Single-child high-nibble collapse (1 nibble)
 *   - Single-child low-nibble collapse (1 nibble)
 *
 * When we hit a branch (≥2 children at either nibble level), the accumulated
 * prefix becomes an extension, and the branch is encoded normally.
 * When we hit a leaf, the prefix is prepended to the leaf's remaining path.
 * ========================================================================= */

/* Forward declaration */
static size_t hash_ref(walk_ctx_t *wc, compact_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out);

/* Encode an MPT leaf node with a nibble prefix prepended to its path */
static size_t encode_leaf(walk_ctx_t *wc, compact_ref_t leaf_ref,
                           size_t byte_depth,
                           const uint8_t *nib_prefix, size_t nib_prefix_len,
                           uint8_t *rlp_out) {
    uint8_t key[64];
    if (!get_leaf_key(wc->tree, leaf_ref, key))
        return 0;

    /* Build full nibble path: prefix + remaining key nibbles */
    uint8_t path[MAX_NIBBLES * 2];
    size_t path_len = 0;

    /* Prefix nibbles */
    if (nib_prefix_len > 0) {
        memcpy(path, nib_prefix, nib_prefix_len);
        path_len = nib_prefix_len;
    }

    /* Remaining nibbles from byte_depth onward */
    size_t key_size = wc->tree->key_size;
    for (size_t i = byte_depth; i < key_size; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    /* Hex-prefix encode */
    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(path, path_len, true, hp);

    /* Get value RLP */
    const void *leaf_val = get_leaf_value(wc->tree, leaf_ref);
    uint8_t value_rlp[MAX_NODE_RLP];
    uint32_t value_len = wc->encode(key, leaf_val, wc->tree->value_size,
                                     value_rlp, wc->ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);

    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

/* Encode an MPT extension + child */
static size_t encode_extension(const uint8_t *nibbles, size_t nibble_len,
                                const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *rlp_out) {
    if (nibble_len == 0) {
        memcpy(rlp_out, child_rlp, child_rlp_len);
        return child_rlp_len;
    }

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

/* Encode an MPT branch: [child_ref × 16, empty_value] */
static size_t encode_branch(uint8_t child_rlps[16][MAX_NODE_RLP],
                              size_t child_lens[16],
                              uint8_t *rlp_out) {
    rlp_buf_t payload; rbuf_reset(&payload);

    for (int i = 0; i < 16; i++) {
        if (child_lens[i] == 0) {
            rbuf_encode_empty(&payload);
        } else {
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

/* Build MPT structure for a set of (low_nibble, child_ref) entries.
 * If only 1 entry: append the low nibble to prefix and recurse.
 * If multiple: create MPT branch over low nibbles. */
static size_t hash_lo_group(walk_ctx_t *wc, uint8_t *lo_nibs,
                              compact_ref_t *lo_refs, int lo_count,
                              size_t next_byte_depth,
                              const uint8_t *nib_prefix, size_t nib_prefix_len,
                              uint8_t *rlp_out) {
    if (lo_count == 1) {
        /* Single low nibble → extend the prefix, recurse */
        uint8_t extended[MAX_NIBBLES * 2];
        memcpy(extended, nib_prefix, nib_prefix_len);
        extended[nib_prefix_len] = lo_nibs[0];
        return hash_ref(wc, lo_refs[0], next_byte_depth,
                         extended, nib_prefix_len + 1, rlp_out);
    }

    /* Multiple low nibbles → MPT branch.
     * First, flush any accumulated prefix as an extension. */
    uint8_t branch_children[16][MAX_NODE_RLP];
    size_t branch_child_lens[16] = {0};

    for (int j = 0; j < lo_count; j++) {
        uint8_t lo = lo_nibs[j];
        branch_child_lens[lo] = hash_ref(wc, lo_refs[j], next_byte_depth,
                                          NULL, 0, branch_children[lo]);
    }

    uint8_t branch_rlp[MAX_NODE_RLP];
    size_t branch_len = encode_branch(branch_children, branch_child_lens,
                                       branch_rlp);

    /* Wrap with prefix extension if any */
    return encode_extension(nib_prefix, nib_prefix_len,
                             branch_rlp, branch_len, rlp_out);
}

/* Hash an ART subtrie. nib_prefix is the accumulated nibble path from
 * collapsed single-child branches + partials + single-child lo groups. */
static size_t hash_ref(walk_ctx_t *wc, compact_ref_t ref, size_t byte_depth,
                        const uint8_t *nib_prefix, size_t nib_prefix_len,
                        uint8_t *rlp_out) {
    if (ref == COMPACT_REF_NULL) return 0;

    if (COMPACT_IS_LEAF_REF(ref)) {
        return encode_leaf(wc, ref, byte_depth,
                            nib_prefix, nib_prefix_len, rlp_out);
    }

    /* Inner node */
    const void *node = node_ptr(wc->tree, ref);

    /* ART partial → add to nibble prefix.
     * ART only stores up to COMPACT_MAX_PREFIX bytes inline. If partial_len
     * exceeds that, we must reconstruct the full prefix from a leaf key. */
    uint8_t partial_len;
    const uint8_t *partial = get_partial(node, &partial_len);

    uint8_t full_partial[64]; /* max key_size */
    if (partial_len > COMPACT_MAX_PREFIX) {
        /* Find any leaf under this node to get the full prefix bytes */
        compact_ref_t probe = ref;
        while (!COMPACT_IS_LEAF_REF(probe)) {
            const void *pn = node_ptr(wc->tree, probe);
            uint8_t pk[256]; compact_ref_t pr[256];
            int pc = get_art_children(wc->tree, pn, pk, pr);
            if (pc == 0) break;
            uint8_t ppl;
            (void)get_partial(pn, &ppl);
            probe = pr[0]; /* follow first child */
        }
        if (COMPACT_IS_LEAF_REF(probe)) {
            uint8_t leaf_key[64];
            if (get_leaf_key(wc->tree, probe, leaf_key)) {
                memcpy(full_partial, leaf_key + byte_depth, partial_len);
                partial = full_partial;
            }
        }
    }

    uint8_t prefix[MAX_NIBBLES * 2];
    size_t prefix_len = 0;
    if (nib_prefix_len > 0) {
        memcpy(prefix, nib_prefix, nib_prefix_len);
        prefix_len = nib_prefix_len;
    }
    for (size_t i = 0; i < partial_len; i++) {
        prefix[prefix_len++] = (partial[i] >> 4) & 0x0F;
        prefix[prefix_len++] =  partial[i]       & 0x0F;
    }

    size_t next_byte_depth = byte_depth + partial_len + 1;

    /* Get children */
    uint8_t child_keys[256];
    compact_ref_t child_refs[256];
    int nchildren = get_art_children(wc->tree, node, child_keys, child_refs);

    if (nchildren == 1) {
        /* Single child → decompose byte into 2 nibbles, extend prefix */
        prefix[prefix_len++] = child_keys[0] >> 4;
        prefix[prefix_len++] = child_keys[0] & 0x0F;
        return hash_ref(wc, child_refs[0], next_byte_depth,
                         prefix, prefix_len, rlp_out);
    }

    /* Multiple children → group by high nibble */
    typedef struct { uint8_t lo; compact_ref_t ref; } lo_entry_t;
    lo_entry_t groups[16][16];
    int gcounts[16] = {0};

    for (int i = 0; i < nchildren; i++) {
        uint8_t hi = child_keys[i] >> 4;
        uint8_t lo = child_keys[i] & 0x0F;
        groups[hi][gcounts[hi]].lo = lo;
        groups[hi][gcounts[hi]].ref = child_refs[i];
        gcounts[hi]++;
    }

    /* Count occupied high nibbles */
    int hi_occupied = 0;
    int single_hi = -1;
    for (int i = 0; i < 16; i++) {
        if (gcounts[i] > 0) { hi_occupied++; single_hi = i; }
    }

    if (hi_occupied == 1) {
        /* All children share the same high nibble → extend prefix */
        prefix[prefix_len++] = (uint8_t)single_hi;

        uint8_t lo_nibs[16];
        compact_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[single_hi]; j++) {
            lo_nibs[j] = groups[single_hi][j].lo;
            lo_refs[j] = groups[single_hi][j].ref;
        }

        return hash_lo_group(wc, lo_nibs, lo_refs, gcounts[single_hi],
                              next_byte_depth, prefix, prefix_len, rlp_out);
    }

    /* Multiple high nibbles → MPT branch at high-nibble level.
     * Flush prefix as extension before the branch. */
    uint8_t hi_children[16][MAX_NODE_RLP];
    size_t hi_child_lens[16] = {0};

    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;

        uint8_t lo_nibs[16];
        compact_ref_t lo_refs[16];
        for (int j = 0; j < gcounts[hi]; j++) {
            lo_nibs[j] = groups[hi][j].lo;
            lo_refs[j] = groups[hi][j].ref;
        }

        /* For each high nibble group: resolve low nibbles.
         * No prefix here — each group starts fresh at the low-nibble level. */
        hi_child_lens[hi] = hash_lo_group(wc, lo_nibs, lo_refs, gcounts[hi],
                                            next_byte_depth, NULL, 0,
                                            hi_children[hi]);
    }

    uint8_t branch_rlp[MAX_NODE_RLP];
    size_t branch_len = encode_branch(hi_children, hi_child_lens, branch_rlp);

    /* Wrap with accumulated prefix */
    return encode_extension(prefix, prefix_len, branch_rlp, branch_len, rlp_out);
}

/* =========================================================================
 * Public API
 * ========================================================================= */

void art_mpt_root_hash(const compact_art_t *tree,
                        art_mpt_value_encode_t encode, void *ctx,
                        uint8_t out[32]) {
    if (!tree || compact_art_size(tree) == 0) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    walk_ctx_t wc = { .tree = tree, .encode = encode, .ctx = ctx };

    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len = hash_ref(&wc, tree->root, 0, NULL, 0, rlp);

    if (rlp_len == 0) {
        memcpy(out, EMPTY_ROOT, 32);
    } else if (rlp_len < 32) {
        keccak(rlp, rlp_len, out);
    } else {
        keccak(rlp, rlp_len, out);
    }
}
