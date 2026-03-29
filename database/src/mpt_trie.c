/*
 * MPT Trie — In-Memory Merkle Patricia Trie (structure only).
 *
 * Native tree: nodes are C structs linked by pointers.
 * No RLP storage, no hash index, no arena. Leaf nodes store only
 * the keccak256 of the value — actual data lives in flat_state.
 *
 * Hashing is lazy: mutations mark paths dirty, root_hash() walks
 * only dirty paths bottom-up to recompute keccak hashes.
 */

#include "mpt_trie.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define MAX_NIBBLES    64
#define MAX_NODE_RLP   1024

static const uint8_t EMPTY_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

/* =========================================================================
 * Node types
 * ========================================================================= */

typedef enum {
    NODE_BRANCH,
    NODE_EXTENSION,
    NODE_LEAF,
} node_type_t;

typedef struct trie_node trie_node_t;

struct trie_node {
    node_type_t type;
    bool        dirty;       /* needs rehash */
    uint8_t     hash[32];    /* cached keccak — valid when !dirty */
    bool        hash_valid;  /* hash has been computed at least once */
    union {
        struct {
            trie_node_t *children[16];
        } branch;
        struct {
            uint8_t     *path;      /* nibble array (heap) */
            uint8_t      path_len;
            trie_node_t *child;
        } ext;
        struct {
            uint8_t     *path;      /* nibble array (heap) */
            uint8_t      path_len;
            uint8_t      value_rlp[MAX_NODE_RLP]; /* full leaf value RLP */
            uint16_t     value_len;
        } leaf;
    };
};

struct mpt_trie {
    trie_node_t *root;
    uint64_t     node_count;
};

/* =========================================================================
 * Node allocation
 * ========================================================================= */

static trie_node_t *node_alloc(node_type_t type) {
    trie_node_t *n = calloc(1, sizeof(*n));
    if (!n) return NULL;
    /* Extra safety: ensure all fields are zero */
    n->type = type;
    n->dirty = true;
    return n;
}

static void node_free(trie_node_t *n) {
    if (!n) return;
    switch (n->type) {
    case NODE_BRANCH:
        for (int i = 0; i < 16; i++)
            node_free(n->branch.children[i]);
        break;
    case NODE_EXTENSION:
        free(n->ext.path);
        node_free(n->ext.child);
        break;
    case NODE_LEAF:
        free(n->leaf.path);
        break;
    }
    free(n);
}

static uint64_t node_count_recursive(const trie_node_t *n) {
    if (!n) return 0;
    uint64_t c = 1;
    if (n->type == NODE_BRANCH) {
        for (int i = 0; i < 16; i++)
            c += node_count_recursive(n->branch.children[i]);
    } else if (n->type == NODE_EXTENSION) {
        c += node_count_recursive(n->ext.child);
    }
    return c;
}

static uint64_t leaf_count_recursive(const trie_node_t *n) {
    if (!n) return 0;
    if (n->type == NODE_LEAF) return 1;
    if (n->type == NODE_BRANCH) {
        uint64_t c = 0;
        for (int i = 0; i < 16; i++)
            c += leaf_count_recursive(n->branch.children[i]);
        return c;
    }
    if (n->type == NODE_EXTENSION)
        return leaf_count_recursive(n->ext.child);
    return 0;
}

static uint64_t dirty_count_recursive(const trie_node_t *n) {
    if (!n) return 0;
    uint64_t c = n->dirty ? 1 : 0;
    if (n->type == NODE_BRANCH) {
        for (int i = 0; i < 16; i++)
            c += dirty_count_recursive(n->branch.children[i]);
    } else if (n->type == NODE_EXTENSION) {
        c += dirty_count_recursive(n->ext.child);
    }
    return c;
}

static uint64_t memory_recursive(const trie_node_t *n) {
    if (!n) return 0;
    uint64_t m = sizeof(trie_node_t);
    switch (n->type) {
    case NODE_BRANCH:
        for (int i = 0; i < 16; i++)
            m += memory_recursive(n->branch.children[i]);
        break;
    case NODE_EXTENSION:
        m += n->ext.path_len;
        m += memory_recursive(n->ext.child);
        break;
    case NODE_LEAF:
        m += n->leaf.path_len;
        break;
    }
    return m;
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    SHA3_CTX ctx;
    keccak_init(&ctx);
    keccak_update(&ctx, data, (uint16_t)len);
    keccak_final(&ctx, out);
}

static void bytes_to_nibbles(const uint8_t *bytes, size_t byte_len,
                             uint8_t *nibbles) {
    for (size_t i = 0; i < byte_len; i++) {
        nibbles[i * 2]     = (bytes[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] =  bytes[i]       & 0x0F;
    }
}

static uint8_t *nibbles_dup(const uint8_t *src, size_t len) {
    if (len == 0) return NULL;
    uint8_t *p = malloc(len);
    if (p) memcpy(p, src, len);
    return p;
}

/* Count non-empty children */
static int branch_child_count(const trie_node_t *n) {
    int c = 0;
    for (int i = 0; i < 16; i++)
        if (n->branch.children[i]) c++;
    return c;
}

/* Find single non-empty child index, -1 if != 1 */
static int branch_single_child(const trie_node_t *n) {
    int idx = -1;
    for (int i = 0; i < 16; i++) {
        if (n->branch.children[i]) {
            if (idx >= 0) return -1;
            idx = i;
        }
    }
    return idx;
}

/* Mark a node and propagate dirty up (caller's responsibility at call site) */
static void mark_dirty(trie_node_t *n) {
    if (n) n->dirty = true;
}

/* =========================================================================
 * Node constructors
 * ========================================================================= */

static trie_node_t *make_leaf(const uint8_t *path, size_t path_len,
                               const uint8_t *value_rlp, size_t value_len) {
    trie_node_t *n = node_alloc(NODE_LEAF);
    if (!n) return NULL;
    n->leaf.path = nibbles_dup(path, path_len);
    n->leaf.path_len = (uint8_t)path_len;
    if (value_len > MAX_NODE_RLP) value_len = MAX_NODE_RLP;
    memcpy(n->leaf.value_rlp, value_rlp, value_len);
    n->leaf.value_len = (uint16_t)value_len;
    return n;
}

static trie_node_t *make_extension(const uint8_t *path, size_t path_len,
                                    trie_node_t *child) {
    if (path_len == 0) return child;
    trie_node_t *n = node_alloc(NODE_EXTENSION);
    if (!n) return NULL;
    n->ext.path = nibbles_dup(path, path_len);
    n->ext.path_len = (uint8_t)path_len;
    n->ext.child = child;
    return n;
}

static trie_node_t *make_branch(void) {
    return node_alloc(NODE_BRANCH);
}

/* Split a leaf at a divergence point. Both old and new key share
 * `match` nibbles of the leaf's path, then diverge. */
static trie_node_t *split_leaf_and_insert(
    const uint8_t *old_suffix, size_t old_suffix_len,
    const uint8_t *old_value, size_t old_value_len,
    const uint8_t *new_nibbles, size_t depth,
    const uint8_t *new_value, size_t new_value_len,
    size_t match)
{
    trie_node_t *branch = make_branch();
    if (!branch) return NULL;

    /* Old leaf → child under old_suffix[match] */
    uint8_t old_nibble = old_suffix[match];
    branch->branch.children[old_nibble] = make_leaf(
        old_suffix + match + 1, old_suffix_len - match - 1,
        old_value, old_value_len);

    /* New leaf → child under new_nibbles[depth + match] */
    uint8_t new_nibble = new_nibbles[depth + match];
    branch->branch.children[new_nibble] = make_leaf(
        new_nibbles + depth + match + 1,
        MAX_NIBBLES - depth - match - 1,
        new_value, new_value_len);

    /* Wrap with common prefix extension if match > 0 */
    if (match > 0)
        return make_extension(old_suffix, match, branch);
    return branch;
}

/* Split an extension at divergence. */
static trie_node_t *split_ext_and_insert(
    const uint8_t *ext_path, size_t ext_path_len,
    trie_node_t *ext_child,
    const uint8_t *nibbles, size_t depth,
    const uint8_t *value_rlp, size_t value_len,
    size_t match)
{
    trie_node_t *branch = make_branch();
    if (!branch) return NULL;

    /* Extension remainder */
    uint8_t old_nibble = ext_path[match];
    size_t remaining = ext_path_len - match - 1;
    if (remaining > 0) {
        branch->branch.children[old_nibble] = make_extension(
            ext_path + match + 1, remaining, ext_child);
    } else {
        branch->branch.children[old_nibble] = ext_child;
    }

    /* New key */
    uint8_t new_nibble = nibbles[depth + match];
    branch->branch.children[new_nibble] = make_leaf(
        nibbles + depth + match + 1,
        MAX_NIBBLES - depth - match - 1,
        value_rlp, value_len);

    if (match > 0)
        return make_extension(ext_path, match, branch);
    return branch;
}

/* Core recursive insert — clean version */
static trie_node_t *do_insert(trie_node_t *node, const uint8_t *nibbles,
                               size_t depth, const uint8_t *value_rlp,
                               size_t value_len) {
    if (!node) {
        return make_leaf(nibbles + depth, MAX_NIBBLES - depth,
                         value_rlp, value_len);
    }

    switch (node->type) {
    case NODE_LEAF: {
        size_t match = 0;
        while (match < node->leaf.path_len &&
               node->leaf.path[match] == nibbles[depth + match])
            match++;

        if (match == node->leaf.path_len &&
            node->leaf.path_len == MAX_NIBBLES - depth) {
            /* Same key — update in place */
            if (value_len > MAX_NODE_RLP) value_len = MAX_NODE_RLP;
            memcpy(node->leaf.value_rlp, value_rlp, value_len);
            node->leaf.value_len = (uint16_t)value_len;
            node->dirty = true;
            return node;
        }

        /* Save leaf data, then free node */
        uint8_t saved_path[MAX_NIBBLES];
        size_t saved_path_len = node->leaf.path_len;
        memcpy(saved_path, node->leaf.path, saved_path_len);
        uint8_t saved_value[MAX_NODE_RLP];
        size_t saved_value_len = node->leaf.value_len;
        memcpy(saved_value, node->leaf.value_rlp, saved_value_len);

        free(node->leaf.path);
        free(node);

        return split_leaf_and_insert(
            saved_path, saved_path_len, saved_value, saved_value_len,
            nibbles, depth, value_rlp, value_len, match);
    }

    case NODE_EXTENSION: {
        size_t match = 0;
        while (match < node->ext.path_len &&
               node->ext.path[match] == nibbles[depth + match])
            match++;

        if (match == node->ext.path_len) {
            /* Full match — recurse */
            node->ext.child = do_insert(node->ext.child, nibbles,
                                         depth + match, value_rlp, value_len);
            node->dirty = true;
            return node;
        }

        /* Save extension data, free node (NOT child — reused) */
        uint8_t saved_path[MAX_NIBBLES];
        size_t saved_path_len = node->ext.path_len;
        memcpy(saved_path, node->ext.path, saved_path_len);
        trie_node_t *saved_child = node->ext.child;

        free(node->ext.path);
        node->ext.child = NULL;  /* prevent double-free */
        free(node);

        return split_ext_and_insert(
            saved_path, saved_path_len, saved_child,
            nibbles, depth, value_rlp, value_len, match);
    }

    case NODE_BRANCH: {
        uint8_t nibble = nibbles[depth];
        node->branch.children[nibble] = do_insert(
            node->branch.children[nibble], nibbles, depth + 1,
            value_rlp, value_len);
        node->dirty = true;
        return node;
    }
    }

    return node;
}

/* =========================================================================
 * Trie remove
 * ========================================================================= */

/* Collapse a branch with single child into extension or merged node */
static trie_node_t *collapse_branch(trie_node_t *branch) {
    int idx = branch_single_child(branch);
    if (idx < 0) return branch; /* shouldn't happen */

    trie_node_t *child = branch->branch.children[idx];
    branch->branch.children[idx] = NULL;

    /* Free the branch shell */
    free(branch);

    uint8_t prefix = (uint8_t)idx;

    if (child->type == NODE_LEAF) {
        /* branch(1 nibble) + leaf(N) → leaf(1+N) */
        uint8_t *new_path = malloc(1 + child->leaf.path_len);
        if (!new_path) return child;
        new_path[0] = prefix;
        memcpy(new_path + 1, child->leaf.path, child->leaf.path_len);
        free(child->leaf.path);
        child->leaf.path = new_path;
        child->leaf.path_len++;
        child->dirty = true;
        return child;
    }

    if (child->type == NODE_EXTENSION) {
        /* branch(1 nibble) + ext(M) → ext(1+M) */
        uint8_t *new_path = malloc(1 + child->ext.path_len);
        if (!new_path) return child;
        new_path[0] = prefix;
        memcpy(new_path + 1, child->ext.path, child->ext.path_len);
        free(child->ext.path);
        child->ext.path = new_path;
        child->ext.path_len++;
        child->dirty = true;
        return child;
    }

    /* Child is a branch — wrap with 1-nibble extension */
    return make_extension(&prefix, 1, child);
}

/* Recursive remove. Returns new subtrie root (NULL if deleted). */
static trie_node_t *do_remove(trie_node_t *node, const uint8_t *nibbles,
                               size_t depth) {
    if (!node) return NULL;

    switch (node->type) {
    case NODE_LEAF: {
        /* Check path match */
        if (node->leaf.path_len != MAX_NIBBLES - depth)
            return node; /* different key length — no match */
        for (size_t i = 0; i < node->leaf.path_len; i++) {
            if (node->leaf.path[i] != nibbles[depth + i])
                return node; /* mismatch */
        }
        /* Match — delete this leaf */
        free(node->leaf.path);
        free(node);
        return NULL;
    }

    case NODE_EXTENSION: {
        /* Match extension path */
        for (size_t i = 0; i < node->ext.path_len; i++) {
            if (nibbles[depth + i] != node->ext.path[i])
                return node; /* key doesn't go through this extension */
        }

        trie_node_t *new_child = do_remove(node->ext.child, nibbles,
                                            depth + node->ext.path_len);
        if (new_child == node->ext.child)
            return node; /* nothing changed */

        if (!new_child) {
            /* Child deleted — remove extension too */
            free(node->ext.path);
            node->ext.child = NULL;
            free(node);
            return NULL;
        }

        node->ext.child = new_child;
        node->dirty = true;

        /* If child became a leaf or extension, merge paths */
        if (new_child->type == NODE_LEAF) {
            uint8_t *merged = malloc(node->ext.path_len + new_child->leaf.path_len);
            if (merged) {
                memcpy(merged, node->ext.path, node->ext.path_len);
                memcpy(merged + node->ext.path_len, new_child->leaf.path,
                       new_child->leaf.path_len);
                free(new_child->leaf.path);
                new_child->leaf.path = merged;
                new_child->leaf.path_len += node->ext.path_len;
                new_child->dirty = true;
                free(node->ext.path);
                node->ext.child = NULL;
                free(node);
                return new_child;
            }
        }
        if (new_child->type == NODE_EXTENSION) {
            uint8_t *merged = malloc(node->ext.path_len + new_child->ext.path_len);
            if (merged) {
                memcpy(merged, node->ext.path, node->ext.path_len);
                memcpy(merged + node->ext.path_len, new_child->ext.path,
                       new_child->ext.path_len);
                free(new_child->ext.path);
                new_child->ext.path = merged;
                new_child->ext.path_len += node->ext.path_len;
                new_child->dirty = true;
                free(node->ext.path);
                node->ext.child = NULL;
                free(node);
                return new_child;
            }
        }

        return node;
    }

    case NODE_BRANCH: {
        uint8_t nibble = nibbles[depth];
        trie_node_t *old_child = node->branch.children[nibble];
        trie_node_t *new_child = do_remove(old_child, nibbles, depth + 1);

        if (new_child == old_child)
            return node; /* nothing changed */

        node->branch.children[nibble] = new_child;
        node->dirty = true;

        int count = branch_child_count(node);
        if (count == 0) {
            free(node);
            return NULL;
        }
        if (count == 1) {
            return collapse_branch(node);
        }
        return node;
    }
    }

    return node;
}

/* =========================================================================
 * RLP encoding for hashing
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
 * Hash computation (bottom-up, dirty-only)
 *
 * Each node's RLP is: list([children...] or [path, child/value]).
 * If RLP < 32 bytes, the node is inlined in its parent's RLP.
 * If RLP >= 32 bytes, keccak(RLP) is used as the reference.
 * ========================================================================= */

/* Encode a child reference: either inline RLP or keccak hash.
 * Returns the encoded bytes in `out`, sets `out_len`. */
static void encode_child_ref(trie_node_t *child,
                              uint8_t *out, size_t *out_len);

/* Compute hash of a node, encode its RLP. Sets node->hash.
 * Returns RLP in `rlp_out` with length `rlp_len`.
 * Only recomputes if node->dirty. */
static void hash_node(trie_node_t *node, uint8_t *rlp_out, size_t *rlp_len) {
    if (!node) {
        /* Empty → 0x80 */
        rlp_out[0] = 0x80;
        *rlp_len = 1;
        return;
    }

    if (!node->dirty && node->hash_valid) {
        /* Already computed — encode as hash reference */
        /* But caller needs the full RLP to decide inline vs hash.
         * We don't store old RLP, so we must recompute. Actually,
         * for correctness we need to always produce the RLP.
         * The hash_valid flag is used for the parent's decision:
         * if this node's RLP >= 32, parent uses the cached hash.
         *
         * For now, always recompute. We can optimize later by
         * caching whether a node is inline-sized. */
    }

    switch (node->type) {
    case NODE_LEAF: {
        uint8_t hp[33];
        size_t hp_len = hex_prefix_encode(node->leaf.path,
                                           node->leaf.path_len, true, hp);
        rlp_buf_t payload; rbuf_reset(&payload);
        rbuf_encode_bytes(&payload, hp, hp_len);
        rbuf_encode_bytes(&payload, node->leaf.value_rlp, node->leaf.value_len);

        rlp_buf_t encoded; rbuf_reset(&encoded);
        rbuf_list_wrap(&encoded, &payload);

        memcpy(rlp_out, encoded.data, encoded.len);
        *rlp_len = encoded.len;

        if (encoded.len >= 32) {
            keccak(encoded.data, encoded.len, node->hash);
            node->hash_valid = true;
        }
        node->dirty = false;
        return;
    }

    case NODE_EXTENSION: {
        uint8_t hp[33];
        size_t hp_len = hex_prefix_encode(node->ext.path,
                                           node->ext.path_len, false, hp);

        /* Encode child */
        uint8_t child_ref[MAX_NODE_RLP];
        size_t child_ref_len;
        encode_child_ref(node->ext.child, child_ref, &child_ref_len);

        rlp_buf_t payload; rbuf_reset(&payload);
        rbuf_encode_bytes(&payload, hp, hp_len);
        rbuf_append(&payload, child_ref, child_ref_len);

        rlp_buf_t encoded; rbuf_reset(&encoded);
        rbuf_list_wrap(&encoded, &payload);

        memcpy(rlp_out, encoded.data, encoded.len);
        *rlp_len = encoded.len;

        if (encoded.len >= 32) {
            keccak(encoded.data, encoded.len, node->hash);
            node->hash_valid = true;
        }
        node->dirty = false;
        return;
    }

    case NODE_BRANCH: {
        rlp_buf_t payload; rbuf_reset(&payload);

        for (int i = 0; i < 16; i++) {
            if (!node->branch.children[i]) {
                rbuf_encode_empty(&payload);
            } else {
                uint8_t child_ref[MAX_NODE_RLP];
                size_t child_ref_len;
                encode_child_ref(node->branch.children[i],
                                 child_ref, &child_ref_len);
                rbuf_append(&payload, child_ref, child_ref_len);
            }
        }
        rbuf_encode_empty(&payload); /* value slot */

        rlp_buf_t encoded; rbuf_reset(&encoded);
        rbuf_list_wrap(&encoded, &payload);

        memcpy(rlp_out, encoded.data, encoded.len);
        *rlp_len = encoded.len;

        if (encoded.len >= 32) {
            keccak(encoded.data, encoded.len, node->hash);
            node->hash_valid = true;
        }
        node->dirty = false;
        return;
    }
    }
}

static void encode_child_ref(trie_node_t *child,
                              uint8_t *out, size_t *out_len) {
    if (!child) {
        out[0] = 0x80;
        *out_len = 1;
        return;
    }

    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len;
    hash_node(child, rlp, &rlp_len);

    if (rlp_len < 32) {
        /* Inline */
        memcpy(out, rlp, rlp_len);
        *out_len = rlp_len;
    } else {
        /* Hash reference: 0xa0 + 32-byte hash */
        out[0] = 0xa0;
        memcpy(out + 1, child->hash, 32);
        *out_len = 33;
    }
}

/* =========================================================================
 * Public API
 * ========================================================================= */

mpt_trie_t *mpt_trie_create(void) {
    mpt_trie_t *t = calloc(1, sizeof(*t));
    return t;
}

void mpt_trie_destroy(mpt_trie_t *t) {
    if (!t) return;
    node_free(t->root);
    /* Temporarily: zero the root pointer to detect stale use */
    t->root = NULL;
    free(t);
}

/* Debug: leak-based destroy (skip node_free) */
void mpt_trie_destroy_leak(mpt_trie_t *t) {
    if (!t) return;
    t->root = NULL;
    free(t);
}

void mpt_trie_reset(mpt_trie_t *t) {
    if (!t) return;
    node_free(t->root);
    t->root = NULL;
    t->node_count = 0;
}

bool mpt_trie_insert(mpt_trie_t *t, const uint8_t key[32],
                      const uint8_t *value_rlp, size_t value_len) {
    if (!t) return false;
    uint8_t nibbles[MAX_NIBBLES];
    bytes_to_nibbles(key, 32, nibbles);
    t->root = do_insert(t->root, nibbles, 0, value_rlp, value_len);
    return t->root != NULL;
}

bool mpt_trie_remove(mpt_trie_t *t, const uint8_t key[32]) {
    if (!t) return false;
    uint8_t nibbles[MAX_NIBBLES];
    bytes_to_nibbles(key, 32, nibbles);
    t->root = do_remove(t->root, nibbles, 0);
    return true;
}

void mpt_trie_root_hash(mpt_trie_t *t, uint8_t out[32]) {
    if (!t || !t->root) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }

    uint8_t rlp[MAX_NODE_RLP];
    size_t rlp_len;
    hash_node(t->root, rlp, &rlp_len);

    if (rlp_len < 32) {
        /* Root is inline — hash the RLP to get root hash */
        keccak(rlp, rlp_len, out);
    } else {
        memcpy(out, t->root->hash, 32);
    }
}

mpt_trie_stats_t mpt_trie_stats(const mpt_trie_t *t) {
    mpt_trie_stats_t s = {0};
    if (!t) return s;
    s.node_count = node_count_recursive(t->root);
    s.leaf_count = leaf_count_recursive(t->root);
    s.dirty_count = dirty_count_recursive(t->root);
    s.memory_bytes = memory_recursive(t->root) + sizeof(mpt_trie_t);
    return s;
}
