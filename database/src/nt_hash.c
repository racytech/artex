#include "../include/nt_hash.h"
#include "../include/nibble_trie.h"
#include "hash.h"
#include "rlp.h"
#include "bytes.h"

#include <stdlib.h>
#include <string.h>

/* ========================================================================
 * nibble_trie internal structures (must match nibble_trie.c layout)
 * ======================================================================== */

#define NT_BRANCH_SLOT_SIZE 64
#define NT_EXT_SLOT_SIZE    40

typedef struct {
    uint32_t children[16];
} nt_branch_t;

typedef struct {
    uint8_t  skip_len;
    uint8_t  nibbles[32];
    uint8_t  _pad[3];
    uint32_t child;
} nt_extension_t;

/* ========================================================================
 * Pointer resolution (same logic as nibble_trie.c)
 * ======================================================================== */

static inline nt_branch_t *branch_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return (nt_branch_t *)(t->branches.base + (size_t)idx * NT_BRANCH_SLOT_SIZE);
}

static inline nt_extension_t *ext_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return (nt_extension_t *)(t->extensions.base + (size_t)idx * NT_EXT_SLOT_SIZE);
}

static inline uint8_t *leaf_key_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return t->leaves.base + (size_t)idx * t->leaves.slot_size;
}

static inline void *leaf_value_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    return leaf_key_ptr(t, ref) + NT_KEY_SIZE;
}

/* ========================================================================
 * Nibble helpers
 * ======================================================================== */

static inline uint8_t ext_nibble(const nt_extension_t *ext, int i) {
    return (ext->nibbles[i / 2] >> (4 * (1 - (i & 1)))) & 0x0F;
}

static void key_to_nibbles(const uint8_t *key, size_t key_len,
                            uint8_t *nibbles) {
    for (size_t i = 0; i < key_len; i++) {
        nibbles[i * 2]     = (key[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] = key[i] & 0x0F;
    }
}

/* ========================================================================
 * Hex-Prefix Encoding (Yellow Paper Appendix C)
 * ======================================================================== */

static bytes_t hex_prefix_encode(const uint8_t *nibbles, size_t count,
                                  bool is_leaf) {
    uint8_t flag = is_leaf ? 2 : 0;
    bool odd = (count % 2) != 0;
    size_t out_len = odd ? (count + 1) / 2 : count / 2 + 1;

    bytes_t result = bytes_with_capacity(out_len);
    bytes_resize(&result, out_len);

    if (odd) {
        result.data[0] = ((flag | 1) << 4) | nibbles[0];
        for (size_t i = 1; i < count; i += 2)
            result.data[(i + 1) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
    } else {
        result.data[0] = (flag << 4);
        for (size_t i = 0; i < count; i += 2)
            result.data[i / 2 + 1] = (nibbles[i] << 4) | nibbles[i + 1];
    }

    return result;
}

/* ========================================================================
 * RLP Helpers
 * ======================================================================== */

static bytes_t rlp_wrap_list(const uint8_t *payload, size_t payload_len) {
    bytes_t result = bytes_new();

    if (payload_len <= 55) {
        bytes_push(&result, (uint8_t)(0xC0 + payload_len));
    } else {
        size_t len_bytes = 0;
        size_t temp = payload_len;
        while (temp > 0) { len_bytes++; temp >>= 8; }
        bytes_push(&result, (uint8_t)(0xF7 + len_bytes));
        for (int i = (int)len_bytes - 1; i >= 0; i--)
            bytes_push(&result, (uint8_t)((payload_len >> (i * 8)) & 0xFF));
    }

    bytes_append(&result, payload, payload_len);
    return result;
}

/* ========================================================================
 * Node References
 * ======================================================================== */

typedef struct {
    uint8_t data[32];
    uint8_t len;        /* 32 = hash, 1-31 = inline RLP, 0 = empty */
} node_ref_t;

static node_ref_t make_node_ref(const uint8_t *rlp, size_t rlp_len) {
    node_ref_t ref = {0};

    if (rlp_len == 0) return ref;

    if (rlp_len >= 32) {
        hash_t h = hash_keccak256(rlp, rlp_len);
        memcpy(ref.data, h.bytes, 32);
        ref.len = 32;
    } else {
        memcpy(ref.data, rlp, rlp_len);
        ref.len = (uint8_t)rlp_len;
    }

    return ref;
}

static void append_child_ref(bytes_t *payload, const node_ref_t *ref) {
    if (ref->len == 0) {
        bytes_push(payload, 0x80);  /* RLP empty string */
    } else if (ref->len == 32) {
        bytes_push(payload, 0xa0);  /* RLP 32-byte string */
        bytes_append(payload, ref->data, 32);
    } else {
        bytes_append(payload, ref->data, ref->len);  /* inline RLP */
    }
}

/* ========================================================================
 * Node Hashing
 * ======================================================================== */

static node_ref_t nref_hash_leaf(const uint8_t *nibbles, int depth,
                                  int total_nibbles,
                                  const uint8_t *value, size_t value_len) {
    int remaining = total_nibbles - depth;
    bytes_t hp = hex_prefix_encode(nibbles + depth, remaining, true);

    bytes_t payload = bytes_new();

    bytes_t hp_rlp = rlp_encode_bytes(hp.data, hp.len);
    bytes_append(&payload, hp_rlp.data, hp_rlp.len);
    bytes_free(&hp_rlp);
    bytes_free(&hp);

    bytes_t val_rlp = rlp_encode_bytes(value, value_len);
    bytes_append(&payload, val_rlp.data, val_rlp.len);
    bytes_free(&val_rlp);

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len);
    bytes_free(&encoded);
    return ref;
}

static node_ref_t nref_hash_branch(const node_ref_t children[16]) {
    bytes_t payload = bytes_with_capacity(600);

    for (int i = 0; i < 16; i++)
        append_child_ref(&payload, &children[i]);

    /* 17th element: empty (no value at branch in this trie) */
    bytes_push(&payload, 0x80);

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len);
    bytes_free(&encoded);
    return ref;
}

static node_ref_t nref_hash_extension(const uint8_t *nibbles, int count,
                                       const node_ref_t *child) {
    bytes_t hp = hex_prefix_encode(nibbles, count, false);

    bytes_t payload = bytes_new();

    bytes_t hp_rlp = rlp_encode_bytes(hp.data, hp.len);
    bytes_append(&payload, hp_rlp.data, hp_rlp.len);
    bytes_free(&hp_rlp);
    bytes_free(&hp);

    append_child_ref(&payload, child);

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len);
    bytes_free(&encoded);
    return ref;
}

/* ========================================================================
 * Hash cache helpers
 * ======================================================================== */

static bool ensure_cache_capacity(nt_hash_cache_t *cache, uint32_t count) {
    if (count <= cache->capacity) return true;
    uint32_t new_cap = count * 2;
    nt_node_hash_t *new_entries = calloc(new_cap, sizeof(nt_node_hash_t));
    if (!new_entries) return false;
    if (cache->entries) {
        memcpy(new_entries, cache->entries,
               cache->capacity * sizeof(nt_node_hash_t));
        free(cache->entries);
    }
    cache->entries = new_entries;
    cache->capacity = new_cap;
    return true;
}

static inline nt_node_hash_t *get_hash_cache(nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);

    if (NT_IS_BRANCH(ref) && idx < t->branch_cache.capacity)
        return &t->branch_cache.entries[idx];
    if (NT_IS_EXTENSION(ref) && idx < t->ext_cache.capacity)
        return &t->ext_cache.entries[idx];
    if (NT_IS_LEAF(ref) && idx < t->leaf_cache.capacity)
        return &t->leaf_cache.entries[idx];

    return NULL;
}

/* ========================================================================
 * Recursive Tree Walk (with hash caching)
 * ======================================================================== */

static node_ref_t hash_node(nibble_trie_t *t, nt_ref_t ref, int depth) {
    node_ref_t empty = {.len = 0};

    if (ref == NT_REF_NULL) return empty;

    /* Check cache */
    nt_node_hash_t *cached = get_hash_cache(t, ref);
    if (cached && cached->len > 0) {
        node_ref_t result = {.len = cached->len};
        memcpy(result.data, cached->data, cached->len);
        return result;
    }

    node_ref_t result;

    if (NT_IS_LEAF(ref)) {
        uint8_t *key = leaf_key_ptr(t, ref);
        uint8_t *val = leaf_value_ptr(t, ref);
        uint8_t nibbles[NT_MAX_NIBBLES];
        key_to_nibbles(key, NT_KEY_SIZE, nibbles);
        result = nref_hash_leaf(nibbles, depth, NT_MAX_NIBBLES,
                                val, t->value_size);
    } else if (NT_IS_EXTENSION(ref)) {
        nt_extension_t *ext = ext_ptr(t, ref);
        uint8_t ext_nibs[NT_MAX_NIBBLES];
        for (int i = 0; i < ext->skip_len; i++)
            ext_nibs[i] = ext_nibble(ext, i);

        node_ref_t child_ref = hash_node(t, ext->child, depth + ext->skip_len);
        result = nref_hash_extension(ext_nibs, ext->skip_len, &child_ref);
    } else if (NT_IS_BRANCH(ref)) {
        nt_branch_t *b = branch_ptr(t, ref);
        node_ref_t children[16];
        for (int i = 0; i < 16; i++)
            children[i] = hash_node(t, b->children[i], depth + 1);
        result = nref_hash_branch(children);
    } else {
        return empty;
    }

    /* Store in cache */
    if (cached) {
        memcpy(cached->data, result.data, result.len);
        cached->len = result.len;
    }

    return result;
}

/* ========================================================================
 * Public API
 * ======================================================================== */

hash_t nt_root_hash(nibble_trie_t *t) {
    if (!t || t->root == NT_REF_NULL)
        return HASH_EMPTY_STORAGE;

    /* Pre-allocate caches to arena sizes so hash_node never grows mid-walk */
    ensure_cache_capacity(&t->branch_cache, t->branches.count);
    ensure_cache_capacity(&t->ext_cache, t->extensions.count);
    ensure_cache_capacity(&t->leaf_cache, t->leaves.count);

    node_ref_t root_ref = hash_node(t, t->root, 0);

    /* Root node is always hashed (even if inline RLP < 32 bytes) */
    if (root_ref.len > 0 && root_ref.len < 32) {
        return hash_keccak256(root_ref.data, root_ref.len);
    } else if (root_ref.len == 32) {
        hash_t h;
        memcpy(h.bytes, root_ref.data, 32);
        return h;
    }

    return HASH_EMPTY_STORAGE;
}
