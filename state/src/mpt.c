#include "mpt.h"
#include "keccak256.h"
#include <stdlib.h>
#include <string.h>

/**
 * Batch MPT Root Computation
 *
 * Computes the Ethereum Merkle Patricia Trie root hash from sorted key-value
 * pairs. Stack-allocated RLP buffers, zero heap allocation in the hot path.
 */

//==============================================================================
// Nibble / Hex-prefix utilities
//==============================================================================

static void bytes_to_nibbles(const uint8_t *bytes, size_t byte_len,
                             uint8_t *nibbles) {
    for (size_t i = 0; i < byte_len; i++) {
        nibbles[i * 2]     = (bytes[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] =  bytes[i]       & 0x0F;
    }
}

/**
 * Hex-prefix encoding (Ethereum Yellow Paper, Appendix C).
 *
 * Encodes a nibble path with a flag indicating leaf vs extension
 * and whether the path has odd or even length.
 */
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

//==============================================================================
// Keccak-256 helper
//==============================================================================

static void keccak256(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

//==============================================================================
// Stack-based RLP buffer (768 bytes — fits branch nodes, ~532 bytes max)
//==============================================================================

typedef struct {
    uint8_t data[768];
    size_t len;
} rlp_sbuf_t;

static inline void sbuf_reset(rlp_sbuf_t *b) { b->len = 0; }

static inline bool sbuf_append(rlp_sbuf_t *b, const uint8_t *d, size_t n) {
    if (n == 0) return true;
    if (b->len + n > sizeof(b->data)) return false;
    memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}

static inline bool sbuf_encode_bytes(rlp_sbuf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80)
        return sbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return sbuf_append(b, &pfx, 1) && (n > 0 ? sbuf_append(b, d, n) : true);
    }
    // Long string (> 55 bytes)
    uint8_t hdr[3];
    size_t hlen;
    if (n <= 0xFF) {
        hdr[0] = 0xb8; hdr[1] = (uint8_t)n; hlen = 2;
    } else {
        hdr[0] = 0xb9; hdr[1] = (uint8_t)(n >> 8); hdr[2] = (uint8_t)(n & 0xFF); hlen = 3;
    }
    return sbuf_append(b, hdr, hlen) && sbuf_append(b, d, n);
}

static inline bool sbuf_encode_empty(rlp_sbuf_t *b) {
    uint8_t e = 0x80;
    return sbuf_append(b, &e, 1);
}

static inline bool sbuf_list_wrap(rlp_sbuf_t *out, const rlp_sbuf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return sbuf_append(out, &pfx, 1) &&
               sbuf_append(out, payload->data, payload->len);
    }
    uint8_t lb[4];
    size_t ll = 0;
    size_t tmp = payload->len;
    while (tmp > 0) { lb[ll++] = tmp & 0xFF; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    if (!sbuf_append(out, &pfx, 1)) return false;
    for (int i = (int)ll - 1; i >= 0; i--)
        if (!sbuf_append(out, &lb[i], 1)) return false;
    return sbuf_append(out, payload->data, payload->len);
}

//==============================================================================
// Hash helpers
//==============================================================================

static bool hash_is_empty(const hash_t *h) {
    for (int i = 0; i < 32; i++)
        if (h->bytes[i] != 0) return false;
    return true;
}

//==============================================================================
// Node hash computation (stack-allocated)
//==============================================================================

/** Leaf: RLP([hex_prefix(suffix, true), value]) */
static bool leaf_hash(const uint8_t *suffix, size_t suffix_len,
                      const uint8_t *value, size_t value_len,
                      hash_t *out) {
    uint8_t encoded_path[33];
    size_t encoded_len = hex_prefix_encode(suffix, suffix_len, true, encoded_path);

    rlp_sbuf_t payload; sbuf_reset(&payload);
    sbuf_encode_bytes(&payload, encoded_path, encoded_len);
    sbuf_encode_bytes(&payload, value, value_len);

    rlp_sbuf_t node; sbuf_reset(&node);
    sbuf_list_wrap(&node, &payload);

    keccak256(node.data, node.len, out->bytes);
    return true;
}

/** Extension: RLP([hex_prefix(path, false), child_hash]) */
static bool extension_hash(const uint8_t *path, size_t path_len,
                           const hash_t *child, hash_t *out) {
    uint8_t encoded_path[33];
    size_t encoded_len = hex_prefix_encode(path, path_len, false, encoded_path);

    rlp_sbuf_t payload; sbuf_reset(&payload);
    sbuf_encode_bytes(&payload, encoded_path, encoded_len);
    sbuf_encode_bytes(&payload, child->bytes, 32);

    rlp_sbuf_t node; sbuf_reset(&node);
    sbuf_list_wrap(&node, &payload);

    keccak256(node.data, node.len, out->bytes);
    return true;
}

/** Branch: RLP([child0..child15, empty_value]) */
static bool branch_hash(const hash_t children[16], hash_t *out) {
    rlp_sbuf_t payload; sbuf_reset(&payload);

    for (int i = 0; i < 16; i++) {
        if (hash_is_empty(&children[i]))
            sbuf_encode_empty(&payload);
        else
            sbuf_encode_bytes(&payload, children[i].bytes, 32);
    }
    sbuf_encode_empty(&payload);  // value slot — always empty for state/storage tries

    rlp_sbuf_t node; sbuf_reset(&node);
    sbuf_list_wrap(&node, &payload);

    keccak256(node.data, node.len, out->bytes);
    return true;
}

//==============================================================================
// Recursive Patricia trie builder
//==============================================================================

typedef struct {
    uint8_t nibbles[64];      // pre-expanded from 32-byte key
    const uint8_t *value;
    size_t value_len;
} batch_leaf_t;

static bool build_subtrie(batch_leaf_t *leaves, size_t start, size_t end,
                          size_t depth, hash_t *out) {
    size_t count = end - start;

    if (count == 0) {
        const uint8_t empty_rlp[] = {0x80};
        keccak256(empty_rlp, sizeof(empty_rlp), out->bytes);
        return true;
    }

    if (count == 1) {
        return leaf_hash(&leaves[start].nibbles[depth], 64 - depth,
                         leaves[start].value, leaves[start].value_len, out);
    }

    // Find common prefix among all leaves at current depth
    size_t common_len = 64 - depth;
    for (size_t i = start + 1; i < end && common_len > 0; i++) {
        for (size_t j = 0; j < common_len; j++) {
            if (leaves[start].nibbles[depth + j] != leaves[i].nibbles[depth + j]) {
                common_len = j;
                break;
            }
        }
    }

    // Extension node if all leaves share a prefix
    if (common_len > 0) {
        hash_t child;
        if (!build_subtrie(leaves, start, end, depth + common_len, &child))
            return false;
        return extension_hash(&leaves[start].nibbles[depth], common_len, &child, out);
    }

    // Branch node — group by nibble at current depth
    hash_t children[16];
    memset(children, 0, sizeof(children));

    size_t i = start;
    while (i < end) {
        uint8_t nibble = leaves[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && leaves[group_end].nibbles[depth] == nibble)
            group_end++;

        if (!build_subtrie(leaves, i, group_end, depth + 1, &children[nibble]))
            return false;

        i = group_end;
    }

    return branch_hash(children, out);
}

//==============================================================================
// Public API
//==============================================================================

static int compare_entries(const void *a, const void *b) {
    return memcmp(((const mpt_batch_entry_t *)a)->key,
                  ((const mpt_batch_entry_t *)b)->key, 32);
}

bool mpt_compute_root_batch(mpt_batch_entry_t *entries, size_t count,
                            hash_t *out_root) {
    if (!out_root) return false;

    if (count == 0) {
        // Empty trie root = keccak256(0x80)
        const uint8_t empty_rlp[] = {0x80};
        keccak256(empty_rlp, sizeof(empty_rlp), out_root->bytes);
        return true;
    }
    if (!entries) return false;

    // Sort by 32-byte key (byte lex == nibble lex)
    qsort(entries, count, sizeof(mpt_batch_entry_t), compare_entries);

    // Expand keys to nibbles
    batch_leaf_t *leaves = malloc(count * sizeof(batch_leaf_t));
    if (!leaves) return false;

    for (size_t i = 0; i < count; i++) {
        bytes_to_nibbles(entries[i].key, 32, leaves[i].nibbles);
        leaves[i].value     = entries[i].value;
        leaves[i].value_len = entries[i].value_len;
    }

    bool ok = build_subtrie(leaves, 0, count, 0, out_root);

    free(leaves);
    return ok;
}

//==============================================================================
// Unsecured trie (variable-length raw keys)
//==============================================================================

typedef struct {
    uint8_t nibbles[64];      // max 32 bytes = 64 nibbles
    size_t  nibble_count;
    const uint8_t *value;
    size_t value_len;
} batch_leaf_var_t;

static bool build_subtrie_var(batch_leaf_var_t *leaves, size_t start, size_t end,
                               size_t depth, hash_t *out) {
    size_t count = end - start;

    if (count == 0) {
        const uint8_t empty_rlp[] = {0x80};
        keccak256(empty_rlp, sizeof(empty_rlp), out->bytes);
        return true;
    }

    if (count == 1) {
        size_t remaining = leaves[start].nibble_count - depth;
        return leaf_hash(&leaves[start].nibbles[depth], remaining,
                         leaves[start].value, leaves[start].value_len, out);
    }

    /* Find minimum remaining nibble count across all leaves */
    size_t min_remaining = leaves[start].nibble_count - depth;
    for (size_t i = start + 1; i < end; i++) {
        size_t rem = leaves[i].nibble_count - depth;
        if (rem < min_remaining) min_remaining = rem;
    }

    /* Find common prefix (bounded by shortest remaining path) */
    size_t common_len = min_remaining;
    for (size_t i = start + 1; i < end && common_len > 0; i++) {
        for (size_t j = 0; j < common_len; j++) {
            if (leaves[start].nibbles[depth + j] != leaves[i].nibbles[depth + j]) {
                common_len = j;
                break;
            }
        }
    }

    if (common_len > 0) {
        hash_t child;
        if (!build_subtrie_var(leaves, start, end, depth + common_len, &child))
            return false;
        return extension_hash(&leaves[start].nibbles[depth], common_len, &child, out);
    }

    /* Branch node */
    hash_t children[16];
    memset(children, 0, sizeof(children));

    size_t i = start;
    while (i < end) {
        uint8_t nibble = leaves[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && leaves[group_end].nibbles[depth] == nibble)
            group_end++;

        if (!build_subtrie_var(leaves, i, group_end, depth + 1, &children[nibble]))
            return false;

        i = group_end;
    }

    return branch_hash(children, out);
}

static int compare_unsecured_entries(const void *a, const void *b) {
    const mpt_unsecured_entry_t *ea = (const mpt_unsecured_entry_t *)a;
    const mpt_unsecured_entry_t *eb = (const mpt_unsecured_entry_t *)b;
    size_t min_len = ea->key_len < eb->key_len ? ea->key_len : eb->key_len;
    int cmp = min_len > 0 ? memcmp(ea->key, eb->key, min_len) : 0;
    if (cmp != 0) return cmp;
    return (ea->key_len < eb->key_len) ? -1 :
           (ea->key_len > eb->key_len) ?  1 : 0;
}

bool mpt_compute_root_unsecured(mpt_unsecured_entry_t *entries,
                                 size_t count, hash_t *out_root) {
    if (!out_root) return false;

    if (count == 0) {
        const uint8_t empty_rlp[] = {0x80};
        keccak256(empty_rlp, sizeof(empty_rlp), out_root->bytes);
        return true;
    }
    if (!entries) return false;

    /* Sort by raw key bytes */
    qsort(entries, count, sizeof(mpt_unsecured_entry_t),
          compare_unsecured_entries);

    /* Expand keys to nibbles */
    batch_leaf_var_t *leaves = malloc(count * sizeof(batch_leaf_var_t));
    if (!leaves) return false;

    for (size_t i = 0; i < count; i++) {
        if (entries[i].key_len > 32) {
            free(leaves);
            return false;
        }
        bytes_to_nibbles(entries[i].key, entries[i].key_len,
                         leaves[i].nibbles);
        leaves[i].nibble_count = entries[i].key_len * 2;
        leaves[i].value        = entries[i].value;
        leaves[i].value_len    = entries[i].value_len;
    }

    bool ok = build_subtrie_var(leaves, 0, count, 0, out_root);

    free(leaves);
    return ok;
}
