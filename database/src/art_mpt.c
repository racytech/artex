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
 * The core recursion:
 *   hash_subtrie(ref, depth, nibbles_accumulated) → RLP bytes
 *
 * ART byte at position `depth` maps to MPT nibbles at depth*2 and depth*2+1.
 * ART partial bytes map to MPT extension nibbles (each byte = 2 nibbles).
 *
 * At each ART inner node:
 *   1. If partial_len > 0: wrap in MPT extension (partial bytes → nibbles)
 *   2. Collect children by byte. Group by high nibble.
 *   3. For each high nibble group:
 *      - If single entry: the low nibble leads directly to child
 *      - If multiple: create MPT branch for low nibbles
 *   4. Create MPT branch for high nibbles (16 slots)
 *
 * At each ART leaf:
 *   - Remaining key nibbles form the leaf path
 *   - Value from callback
 *   - Encode as MPT leaf: RLP([hex_prefix(path, leaf=true), value_rlp])
 * ========================================================================= */

/* Forward declaration */
static size_t hash_ref(walk_ctx_t *wc, compact_ref_t ref,
                        size_t depth, uint8_t *rlp_out);

/* Encode an MPT leaf node: [hex_prefix(path, is_leaf=true), value_rlp] */
static size_t encode_leaf(walk_ctx_t *wc, compact_ref_t leaf_ref,
                           size_t byte_depth, uint8_t *rlp_out) {
    uint8_t key[64]; /* max key_size */
    if (!get_leaf_key(wc->tree, leaf_ref, key))
        return 0;

    /* Remaining nibbles: from byte_depth onward */
    uint8_t nibbles[MAX_NIBBLES];
    size_t key_size = wc->tree->key_size;
    for (size_t i = 0; i < key_size; i++) {
        nibbles[i * 2]     = (key[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] =  key[i]       & 0x0F;
    }
    size_t nib_depth = byte_depth * 2;
    size_t remaining = key_size * 2 - nib_depth;

    /* Hex-prefix encode the path */
    uint8_t hp[33];
    size_t hp_len = hex_prefix_encode(nibbles + nib_depth, remaining, true, hp);

    /* Get RLP-encoded value via callback */
    const void *leaf_val = get_leaf_value(wc->tree, leaf_ref);
    uint8_t value_rlp[MAX_NODE_RLP];
    uint32_t value_len = wc->encode(key, leaf_val, wc->tree->value_size,
                                     value_rlp, wc->ctx);
    if (value_len == 0) return 0;

    /* Encode leaf: list([hp_path, value_rlp]) */
    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);

    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

/* Encode an MPT extension node: [hex_prefix(nibbles, is_leaf=false), child_ref] */
static size_t encode_extension(const uint8_t *nibbles, size_t nibble_len,
                                const uint8_t *child_rlp, size_t child_rlp_len,
                                uint8_t *rlp_out) {
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

/* Encode an MPT branch node: [child_ref × 16, empty_value] */
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
    rbuf_encode_empty(&payload); /* value slot — always empty */

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);

    memcpy(rlp_out, encoded.data, encoded.len);
    return encoded.len;
}

/* Hash an ART subtrie rooted at `ref` (inner node or leaf).
 * byte_depth: current byte position in the key.
 * Writes the node's full RLP to rlp_out, returns length. */
static size_t hash_ref(walk_ctx_t *wc, compact_ref_t ref,
                        size_t byte_depth, uint8_t *rlp_out) {
    if (ref == COMPACT_REF_NULL) return 0;

    if (COMPACT_IS_LEAF_REF(ref)) {
        return encode_leaf(wc, ref, byte_depth, rlp_out);
    }

    /* Inner node */
    const void *node = node_ptr(wc->tree, ref);

    /* Get partial key (ART path compression → MPT extension) */
    uint8_t partial_len;
    const uint8_t *partial = get_partial(node, &partial_len);
    size_t effective_depth = byte_depth + partial_len;

    /* Get children */
    uint8_t child_keys[256];
    compact_ref_t child_refs[256];
    int nchildren = get_art_children(wc->tree, node, child_keys, child_refs);

    /* Map ART byte-level children to MPT nibble-level.
     * Each child byte X maps to high_nibble = X>>4, low_nibble = X&0xF.
     * Group by high nibble: each group becomes a low-nibble MPT branch.
     * Then the 16 high-nibble groups form the top-level MPT branch. */

    /* Collect per high-nibble groups */
    typedef struct { uint8_t low_nib; compact_ref_t ref; } low_entry_t;
    low_entry_t groups[16][16]; /* [high_nib][entries] */
    int group_counts[16] = {0};

    for (int i = 0; i < nchildren; i++) {
        uint8_t hi = child_keys[i] >> 4;
        uint8_t lo = child_keys[i] & 0x0F;
        int gc = group_counts[hi];
        groups[hi][gc].low_nib = lo;
        groups[hi][gc].ref = child_refs[i];
        group_counts[hi]++;
    }

    /* Build top-level MPT branch: 16 high-nibble slots */
    uint8_t top_rlps[16][MAX_NODE_RLP];
    size_t top_lens[16] = {0};

    for (int hi = 0; hi < 16; hi++) {
        if (group_counts[hi] == 0) continue;

        if (group_counts[hi] == 1) {
            /* Single child at this high nibble → extension [lo_nibble] + child
             * Or if child is a leaf at the next depth, the low nibble is part
             * of the leaf path. We encode as: extension(1 nibble) → child */
            uint8_t lo = groups[hi][0].low_nib;
            compact_ref_t child_ref = groups[hi][0].ref;

            uint8_t child_rlp[MAX_NODE_RLP];
            size_t child_len = hash_ref(wc, child_ref,
                                         effective_depth + 1, child_rlp);
            if (child_len == 0) continue;

            /* Wrap with 1-nibble extension for the low nibble */
            top_lens[hi] = encode_extension(&lo, 1, child_rlp, child_len,
                                             top_rlps[hi]);
        } else {
            /* Multiple children at this high nibble → MPT branch for low nibbles */
            uint8_t lo_rlps[16][MAX_NODE_RLP];
            size_t lo_lens[16] = {0};

            for (int j = 0; j < group_counts[hi]; j++) {
                uint8_t lo = groups[hi][j].low_nib;
                uint8_t child_rlp[MAX_NODE_RLP];
                size_t child_len = hash_ref(wc, groups[hi][j].ref,
                                             effective_depth + 1, child_rlp);
                if (child_len > 0) {
                    memcpy(lo_rlps[lo], child_rlp, child_len);
                    lo_lens[lo] = child_len;
                }
            }

            top_lens[hi] = encode_branch(lo_rlps, lo_lens, top_rlps[hi]);
        }
    }

    /* Encode the top-level MPT branch (high nibbles) */
    uint8_t branch_rlp[MAX_NODE_RLP];
    size_t branch_len;

    /* Check if there's only one high nibble occupied → collapse to extension */
    int occupied = 0, single_hi = -1;
    for (int i = 0; i < 16; i++) {
        if (top_lens[i] > 0) { occupied++; single_hi = i; }
    }

    if (occupied == 1) {
        /* Single high nibble → this is just the child with the high nibble
         * prepended as extension. But top_rlps[single_hi] already has the
         * low nibble extension. We need to merge. */
        uint8_t nib = (uint8_t)single_hi;
        branch_rlp[0] = 0; /* placeholder */
        branch_len = encode_extension(&nib, 1, top_rlps[single_hi],
                                       top_lens[single_hi], branch_rlp);
    } else {
        branch_len = encode_branch(top_rlps, top_lens, branch_rlp);
    }

    /* If ART node has partial key, wrap in MPT extension */
    if (partial_len > 0) {
        uint8_t ext_nibbles[COMPACT_MAX_PREFIX * 2];
        size_t ext_nib_len = 0;
        for (size_t i = 0; i < partial_len; i++) {
            ext_nibbles[ext_nib_len++] = (partial[i] >> 4) & 0x0F;
            ext_nibbles[ext_nib_len++] =  partial[i]       & 0x0F;
        }
        return encode_extension(ext_nibbles, ext_nib_len,
                                 branch_rlp, branch_len, rlp_out);
    }

    memcpy(rlp_out, branch_rlp, branch_len);
    return branch_len;
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
    size_t rlp_len = hash_ref(&wc, tree->root, 0, rlp);

    if (rlp_len == 0) {
        memcpy(out, EMPTY_ROOT, 32);
    } else if (rlp_len < 32) {
        keccak(rlp, rlp_len, out);
    } else {
        keccak(rlp, rlp_len, out);
    }
}
