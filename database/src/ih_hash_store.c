#include "ih_hash_store.h"
#include "hash_store.h"
#include "hash.h"
#include "rlp.h"
#include "bytes.h"
#include <stdlib.h>
#include <string.h>

/**
 * Intermediate Hash Table — hash_store Backend
 *
 * Same algorithm as intermediate_hashes.c but backed by hash_store.
 * Key changes from compact_art version:
 *   - ih_store → hash_store_put
 *   - ih_load  → hash_store_get (copies to buffer, not pointer)
 *   - ih_delete_prefix → removed (stale entries are safe to leave)
 *   - Full rebuild → destroy + recreate hash_store
 */

/* =========================================================================
 * Internal Types
 * ========================================================================= */

typedef struct {
    uint8_t data[32];
    uint8_t len;        /* 32 = hash, 1-31 = inline RLP, 0 = empty */
} node_ref_t;

typedef struct {
    node_ref_t ref;
    size_t branch_depth; /* +1 encoded: 0 = leaf, >0 = branch at depth-1 */
} subtree_result_t;

struct ihs_state {
    hash_store_t *store;
    char         *dir;
    hash_t        root;
    bool          initialized;
    size_t        root_branch_depth;
};

/* ih_tree value layout:
 *   [2B bitmap][1B ref_len][32B ref_data][16B child_depths]
 */
#define IH_KEY_SIZE    33
#define IH_VALUE_SIZE  51
#define IHS_SLOT_SIZE  96   /* 8+1+1+33+51+2 = 96 */
#define IHS_SHARD_CAP  65536

/* =========================================================================
 * Nibble Helpers
 * ========================================================================= */

static void key_to_nibbles(const uint8_t *key, size_t key_len,
                           uint8_t *nibbles) {
    for (size_t i = 0; i < key_len; i++) {
        nibbles[i * 2]     = (key[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] = key[i] & 0x0F;
    }
}

/* =========================================================================
 * Hex-Prefix Encoding (Yellow Paper Appendix C)
 * ========================================================================= */

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

/* =========================================================================
 * RLP List Wrapping
 * ========================================================================= */

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

/* =========================================================================
 * Node Reference Construction
 * ========================================================================= */

static node_ref_t make_node_ref(const uint8_t *rlp, size_t rlp_len,
                                bool force_hash) {
    node_ref_t ref = {0};
    if (rlp_len == 0) return ref;

    if (rlp_len >= 32 || force_hash) {
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
        bytes_push(payload, 0x80);
    } else if (ref->len == 32) {
        bytes_push(payload, 0xa0);
        bytes_append(payload, ref->data, 32);
    } else {
        bytes_append(payload, ref->data, ref->len);
    }
}

/* =========================================================================
 * Node Hashing
 * ========================================================================= */

static node_ref_t hash_leaf(const uint8_t *nibbles, size_t depth,
                            size_t total_nibbles,
                            const uint8_t *value, size_t value_len) {
    size_t remaining = total_nibbles - depth;
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

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

static node_ref_t hash_branch(const node_ref_t children[16],
                               const uint8_t *value, size_t value_len) {
    bytes_t payload = bytes_with_capacity(600);

    for (int i = 0; i < 16; i++)
        append_child_ref(&payload, &children[i]);

    if (value && value_len > 0) {
        bytes_t val_rlp = rlp_encode_bytes(value, value_len);
        bytes_append(&payload, val_rlp.data, val_rlp.len);
        bytes_free(&val_rlp);
    } else {
        bytes_push(&payload, 0x80);
    }

    bytes_t encoded = rlp_wrap_list(payload.data, payload.len);
    bytes_free(&payload);

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

static node_ref_t hash_extension(const uint8_t *nibbles, size_t count,
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

    node_ref_t ref = make_node_ref(encoded.data, encoded.len, false);
    bytes_free(&encoded);
    return ref;
}

/* =========================================================================
 * ih_tree Helpers (hash_store backend)
 * ========================================================================= */

static void make_ih_key(const uint8_t *nibbles, size_t count,
                        uint8_t out[IH_KEY_SIZE]) {
    memset(out, 0, IH_KEY_SIZE);
    for (size_t i = 0; i < count; i += 2) {
        uint8_t hi = nibbles[i];
        uint8_t lo = (i + 1 < count) ? nibbles[i + 1] : 0;
        out[i / 2] = (hi << 4) | lo;
    }
    out[32] = (uint8_t)count;
}

static void ih_store(ihs_state_t *ih, const uint8_t *nibbles, size_t count,
                     uint16_t bitmap, const node_ref_t *branch_ref,
                     const uint8_t child_depths[16]) {
    uint8_t key[IH_KEY_SIZE];
    make_ih_key(nibbles, count, key);

    uint8_t value[IH_VALUE_SIZE];
    memcpy(value, &bitmap, 2);
    value[2] = branch_ref->len;
    memcpy(value + 3, branch_ref->data, 32);
    memcpy(value + 35, child_depths, 16);

    hash_store_put(ih->store, key, value, IH_VALUE_SIZE);
}

static bool ih_load(const ihs_state_t *ih, const uint8_t *nibbles,
                    size_t count, uint16_t *out_bitmap, node_ref_t *out_ref,
                    uint8_t out_child_depths[16]) {
    uint8_t key[IH_KEY_SIZE];
    make_ih_key(nibbles, count, key);

    uint8_t buf[IH_VALUE_SIZE];
    uint8_t len;
    if (!hash_store_get(ih->store, key, buf, &len))
        return false;

    memcpy(out_bitmap, buf, 2);
    if (out_ref) {
        out_ref->len = buf[2];
        memcpy(out_ref->data, buf + 3, 32);
    }
    if (out_child_depths) {
        memcpy(out_child_depths, buf + 35, 16);
    }
    return true;
}

/* =========================================================================
 * Core: Recursive Build
 * ========================================================================= */

static size_t find_branch_depth(const uint8_t *const *nibble_arrays,
                                const size_t *nibble_lens,
                                size_t from, size_t to,
                                size_t start_depth) {
    size_t depth = start_depth;
    size_t min_len = nibble_lens[from] < nibble_lens[to - 1]
                     ? nibble_lens[from] : nibble_lens[to - 1];
    while (depth < min_len) {
        if (nibble_arrays[from][depth] != nibble_arrays[to - 1][depth])
            break;
        depth++;
    }
    return depth;
}

static void find_nibble_range(const uint8_t *const *nibble_arrays,
                              const size_t *nibble_lens,
                              size_t from, size_t to, size_t depth,
                              uint8_t target,
                              size_t *out_from, size_t *out_to) {
    size_t lo = from;
    while (lo < to) {
        if (nibble_lens[lo] > depth && nibble_arrays[lo][depth] == target)
            break;
        if (nibble_lens[lo] <= depth) { lo++; continue; }
        if (nibble_arrays[lo][depth] < target) { lo++; continue; }
        break;
    }

    if (lo >= to || nibble_lens[lo] <= depth || nibble_arrays[lo][depth] != target) {
        *out_from = *out_to = lo;
        return;
    }

    size_t hi = lo;
    while (hi < to && nibble_lens[hi] > depth && nibble_arrays[hi][depth] == target)
        hi++;

    *out_from = lo;
    *out_to = hi;
}

static subtree_result_t build_subtree(ihs_state_t *ih,
                                      const uint8_t *const *nibble_arrays,
                                      const size_t *nibble_lens,
                                      const uint8_t *const *values,
                                      const size_t *value_lens,
                                      size_t from, size_t to,
                                      size_t depth) {
    subtree_result_t empty = {{{0}, 0}, 0};

    if (from >= to) return empty;

    /* Single key → leaf */
    if (to - from == 1) {
        subtree_result_t r = {
            hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                      values[from], value_lens[from]),
            0
        };
        return r;
    }

    /* Multiple keys: check for terminating key */
    const uint8_t *branch_value = NULL;
    size_t branch_value_len = 0;
    size_t data_from = from;

    if (nibble_lens[from] == depth) {
        branch_value = values[from];
        branch_value_len = value_lens[from];
        data_from = from + 1;
    }

    if (data_from >= to) {
        subtree_result_t r = {
            hash_leaf(nibble_arrays[from], depth, nibble_lens[from],
                      values[from], value_lens[from]),
            0
        };
        return r;
    }

    /* Find divergence point */
    size_t branch_depth;
    if (branch_value) {
        branch_depth = depth;
    } else {
        branch_depth = find_branch_depth(nibble_arrays, nibble_lens,
                                         data_from, to, depth);
    }

    /* Partition by nibble at branch_depth */
    node_ref_t children[16] = {{{0}, 0}};
    uint16_t bitmap = 0;
    uint8_t child_depths[16] = {0};

    for (uint8_t n = 0; n < 16; n++) {
        size_t sub_from, sub_to;
        find_nibble_range(nibble_arrays, nibble_lens,
                          data_from, to, branch_depth, n,
                          &sub_from, &sub_to);
        if (sub_from < sub_to) {
            subtree_result_t cr = build_subtree(ih, nibble_arrays, nibble_lens,
                                                values, value_lens,
                                                sub_from, sub_to,
                                                branch_depth + 1);
            children[n] = cr.ref;
            child_depths[n] = (uint8_t)cr.branch_depth;
            bitmap |= (1 << n);
        }
    }

    node_ref_t branch_ref = hash_branch(children, branch_value, branch_value_len);
    ih_store(ih, nibble_arrays[data_from], branch_depth, bitmap,
             &branch_ref, child_depths);

    if (branch_depth > depth) {
        node_ref_t ext_ref = hash_extension(
            nibble_arrays[data_from] + depth, branch_depth - depth,
            &branch_ref);
        subtree_result_t r = {ext_ref, branch_depth + 1};
        return r;
    }

    subtree_result_t r = {branch_ref, branch_depth + 1};
    return r;
}

/* =========================================================================
 * Public API
 * ========================================================================= */

ihs_state_t *ihs_create(const char *dir) {
    if (!dir) return NULL;

    ihs_state_t *ih = calloc(1, sizeof(ihs_state_t));
    if (!ih) return NULL;

    ih->dir = strdup(dir);
    if (!ih->dir) { free(ih); return NULL; }

    ih->store = hash_store_create(dir, IHS_SHARD_CAP, IH_KEY_SIZE, IHS_SLOT_SIZE);
    if (!ih->store) { free(ih->dir); free(ih); return NULL; }

    ih->root = HASH_EMPTY_STORAGE;
    ih->initialized = true;
    return ih;
}

ihs_state_t *ihs_open(const char *dir) {
    if (!dir) return NULL;

    ihs_state_t *ih = calloc(1, sizeof(ihs_state_t));
    if (!ih) return NULL;

    ih->dir = strdup(dir);
    if (!ih->dir) { free(ih); return NULL; }

    ih->store = hash_store_open(dir);
    if (!ih->store) { free(ih->dir); free(ih); return NULL; }

    ih->root = HASH_EMPTY_STORAGE;
    ih->initialized = true;
    return ih;
}

void ihs_destroy(ihs_state_t *ih) {
    if (!ih) return;
    if (ih->store) hash_store_destroy(ih->store);
    free(ih->dir);
    free(ih);
}

void ihs_sync(ihs_state_t *ih) {
    if (ih && ih->store) hash_store_sync(ih->store);
}

/* Helper: destroy and recreate the hash_store (for full rebuilds) */
static bool ihs_reset_store(ihs_state_t *ih) {
    hash_store_destroy(ih->store);
    ih->store = hash_store_create(ih->dir, IHS_SHARD_CAP, IH_KEY_SIZE, IHS_SLOT_SIZE);
    return ih->store != NULL;
}

static hash_t finalize_root(node_ref_t root_ref) {
    hash_t root;
    if (root_ref.len > 0 && root_ref.len < 32) {
        hash_t h = hash_keccak256(root_ref.data, root_ref.len);
        memcpy(root.bytes, h.bytes, 32);
    } else if (root_ref.len == 32) {
        memcpy(root.bytes, root_ref.data, 32);
    } else {
        root = HASH_EMPTY_STORAGE;
    }
    return root;
}

hash_t ihs_build(ihs_state_t *ih,
                  const uint8_t *const *keys,
                  const uint8_t *const *values,
                  const uint16_t *value_lens,
                  size_t count) {
    if (!ih) return HASH_EMPTY_STORAGE;

    /* Reset store */
    if (!ihs_reset_store(ih)) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    if (count == 0) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    /* Convert keys to nibbles */
    uint8_t (*nibble_arrays)[64] = malloc(count * 64);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));
    size_t *val_lens = malloc(count * sizeof(size_t));

    if (!nibble_arrays || !nibble_lens || !nibble_ptrs || !val_lens) {
        free(nibble_arrays); free(nibble_lens);
        free(nibble_ptrs); free(val_lens);
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        key_to_nibbles(keys[i], 32, nibble_arrays[i]);
        nibble_lens[i] = 64;
        nibble_ptrs[i] = nibble_arrays[i];
        val_lens[i] = value_lens[i];
    }

    subtree_result_t root_result = build_subtree(ih,
        (const uint8_t *const *)nibble_ptrs,
        nibble_lens, values, val_lens,
        0, count, 0);
    ih->root_branch_depth = root_result.branch_depth;
    ih->root = finalize_root(root_result.ref);

    free(nibble_arrays);
    free(nibble_lens);
    free(nibble_ptrs);
    free(val_lens);

    return ih->root;
}

hash_t ihs_build_varlen(ihs_state_t *ih,
                         const uint8_t *const *keys,
                         const size_t *key_lens,
                         const uint8_t *const *values,
                         const size_t *value_lens,
                         size_t count) {
    if (!ih) return HASH_EMPTY_STORAGE;

    if (!ihs_reset_store(ih)) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    if (count == 0) {
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    size_t max_nibbles = 0;
    for (size_t i = 0; i < count; i++) {
        size_t nlen = key_lens[i] * 2;
        if (nlen > max_nibbles) max_nibbles = nlen;
    }

    uint8_t *nibble_storage = calloc(count, max_nibbles);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));

    if (!nibble_storage || !nibble_lens || !nibble_ptrs) {
        free(nibble_storage); free(nibble_lens); free(nibble_ptrs);
        ih->root = HASH_EMPTY_STORAGE;
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        nibble_ptrs[i] = nibble_storage + i * max_nibbles;
        key_to_nibbles(keys[i], key_lens[i],
                       (uint8_t *)(nibble_storage + i * max_nibbles));
        nibble_lens[i] = key_lens[i] * 2;
    }

    subtree_result_t root_result = build_subtree(ih,
        nibble_ptrs, nibble_lens,
        values, value_lens,
        0, count, 0);
    ih->root_branch_depth = root_result.branch_depth;
    ih->root = finalize_root(root_result.ref);

    free(nibble_storage);
    free(nibble_lens);
    free(nibble_ptrs);

    return ih->root;
}

/* =========================================================================
 * Incremental Update Helpers
 * ========================================================================= */

static void nibbles_to_key(const uint8_t *nibbles, size_t nibble_count,
                           uint8_t out_key[32]) {
    memset(out_key, 0, 32);
    size_t i;
    for (i = 0; i + 1 < nibble_count; i += 2)
        out_key[i / 2] = (nibbles[i] << 4) | nibbles[i + 1];
    if (nibble_count % 2 != 0)
        out_key[nibble_count / 2] = nibbles[nibble_count - 1] << 4;
}

static bool nibbles_match_prefix(const uint8_t *key_nibbles, size_t key_nib_len,
                                 const uint8_t *prefix, size_t prefix_len) {
    if (key_nib_len < prefix_len) return false;
    for (size_t i = 0; i < prefix_len; i++) {
        if (key_nibbles[i] != prefix[i]) return false;
    }
    return true;
}

static subtree_result_t get_clean_child_ref(
        ihs_state_t *ih,
        uint8_t *path,
        size_t branch_depth,
        uint8_t child_nibble,
        uint8_t child_branch_depth,
        ih_cursor_t *cursor) {
    subtree_result_t empty = {{{0}, 0}, 0};

    path[branch_depth] = child_nibble;
    size_t child_prefix_len = branch_depth + 1;

    uint8_t seek_key[32];
    nibbles_to_key(path, child_prefix_len, seek_key);

    if (!cursor->seek(cursor->ctx, seek_key, 32))
        return empty;
    if (!cursor->valid(cursor->ctx))
        return empty;

    size_t klen, vlen;
    const uint8_t *k = cursor->key(cursor->ctx, &klen);
    const uint8_t *v = cursor->value(cursor->ctx, &vlen);
    if (!k) return empty;

    uint8_t knibbles[64];
    key_to_nibbles(k, klen, knibbles);

    if (!nibbles_match_prefix(knibbles, klen * 2, path, child_prefix_len))
        return empty;

    if (child_branch_depth == 0) {
        node_ref_t ref = hash_leaf(knibbles, child_prefix_len,
                                   klen * 2, v, vlen);
        subtree_result_t r = {ref, 0};
        return r;
    }

    size_t actual_depth = child_branch_depth - 1;
    memcpy(path + child_prefix_len, knibbles + child_prefix_len,
           actual_depth - child_prefix_len);

    uint16_t bitmap;
    node_ref_t branch_ref;
    if (!ih_load(ih, knibbles, actual_depth, &bitmap, &branch_ref, NULL))
        return empty;

    if (actual_depth > child_prefix_len) {
        node_ref_t ext_ref = hash_extension(
            knibbles + child_prefix_len,
            actual_depth - child_prefix_len,
            &branch_ref);
        subtree_result_t r = {ext_ref, child_branch_depth};
        return r;
    }

    subtree_result_t r = {branch_ref, child_branch_depth};
    return r;
}

/*
 * Rebuild a subtree from cursor data.
 * Key difference from compact_art version: no ih_delete_prefix.
 * For full rebuild (path_len == 0): destroy + recreate hash_store.
 * For subtree rebuild (path_len > 0): stale entries left in place.
 */
static subtree_result_t rebuild_from_cursor(
        ihs_state_t *ih,
        size_t depth,
        ih_cursor_t *cursor,
        const uint8_t *path,
        size_t path_len) {
    subtree_result_t empty = {{{0}, 0}, 0};

    if (path_len == 0) {
        /* Full rebuild from root — clear entire store */
        if (!ihs_reset_store(ih))
            return empty;
    }
    /* path_len > 0: skip prefix deletion — stale entries are safe */

    uint8_t seek_key[32];
    nibbles_to_key(path, path_len, seek_key);

    if (!cursor->seek(cursor->ctx, seek_key, 32))
        return empty;

    /* Collect all keys matching the prefix */
    size_t cap = 64, count = 0;
    uint8_t *nibble_storage = malloc(cap * 64);
    size_t *nibble_lens = malloc(cap * sizeof(size_t));
    uint8_t *value_storage = NULL;
    size_t value_storage_cap = 0, value_storage_used = 0;
    size_t *value_offsets = malloc(cap * sizeof(size_t));
    size_t *value_lens = malloc(cap * sizeof(size_t));

    if (!nibble_storage || !nibble_lens || !value_offsets || !value_lens) {
        free(nibble_storage); free(nibble_lens);
        free(value_offsets); free(value_lens);
        free(value_storage);
        return empty;
    }

    while (cursor->valid(cursor->ctx)) {
        size_t klen, vlen;
        const uint8_t *k = cursor->key(cursor->ctx, &klen);
        const uint8_t *v = cursor->value(cursor->ctx, &vlen);
        if (!k) break;

        uint8_t knibs[64];
        key_to_nibbles(k, klen, knibs);

        if (!nibbles_match_prefix(knibs, klen * 2, path, path_len))
            break;

        if (count >= cap) {
            cap *= 2;
            nibble_storage = realloc(nibble_storage, cap * 64);
            nibble_lens = realloc(nibble_lens, cap * sizeof(size_t));
            value_offsets = realloc(value_offsets, cap * sizeof(size_t));
            value_lens = realloc(value_lens, cap * sizeof(size_t));
        }

        memcpy(nibble_storage + count * 64, knibs, 64);
        nibble_lens[count] = klen * 2;

        if (value_storage_used + vlen > value_storage_cap) {
            value_storage_cap = (value_storage_cap + vlen) * 2;
            value_storage = realloc(value_storage, value_storage_cap);
        }
        value_offsets[count] = value_storage_used;
        memcpy(value_storage + value_storage_used, v, vlen);
        value_storage_used += vlen;
        value_lens[count] = vlen;

        count++;
        cursor->next(cursor->ctx);
    }

    if (count == 0) {
        free(nibble_storage); free(nibble_lens);
        free(value_offsets); free(value_lens);
        free(value_storage);
        return empty;
    }

    const uint8_t **nib_ptrs = malloc(count * sizeof(uint8_t *));
    const uint8_t **val_ptrs = malloc(count * sizeof(uint8_t *));
    for (size_t i = 0; i < count; i++) {
        nib_ptrs[i] = nibble_storage + i * 64;
        val_ptrs[i] = value_storage + value_offsets[i];
    }

    subtree_result_t result = build_subtree(ih,
        (const uint8_t *const *)nib_ptrs, nibble_lens,
        (const uint8_t *const *)val_ptrs, value_lens,
        0, count, depth);

    free(nib_ptrs);
    free(val_ptrs);
    free(nibble_storage);
    free(nibble_lens);
    free(value_offsets);
    free(value_lens);
    free(value_storage);

    return result;
}

/* =========================================================================
 * Core: Incremental Update (recursive)
 * ========================================================================= */

static subtree_result_t update_subtree(
        ihs_state_t *ih,
        const uint8_t *const *dirty_nibs,
        const size_t *dirty_nib_lens,
        const uint8_t *const *dirty_vals,
        const size_t *dirty_vlens,
        size_t from, size_t to,
        size_t depth,
        size_t stored_depth,
        ih_cursor_t *cursor,
        uint8_t *path) {
    subtree_result_t empty = {{{0}, 0}, 0};

    if (from >= to) return empty;

    /* Case 1: No existing branch — rebuild from cursor */
    if (stored_depth == 0) {
        return rebuild_from_cursor(ih, depth, cursor, path, depth);
    }

    size_t branch_depth = stored_depth - 1;

    /* Case 2: Extension check */
    if (branch_depth > depth) {
        uint8_t seek_key[32];
        nibbles_to_key(path, depth, seek_key);

        bool got_ext = false;
        if (cursor->seek(cursor->ctx, seek_key, 32) &&
            cursor->valid(cursor->ctx)) {
            size_t klen;
            const uint8_t *k = cursor->key(cursor->ctx, &klen);
            if (k) {
                uint8_t knibs[64];
                key_to_nibbles(k, klen, knibs);
                if (nibbles_match_prefix(knibs, klen * 2, path, depth)) {
                    memcpy(path + depth, knibs + depth, branch_depth - depth);
                    got_ext = true;
                }
            }
        }

        if (!got_ext)
            return rebuild_from_cursor(ih, depth, cursor, path, depth);

        for (size_t i = from; i < to; i++) {
            for (size_t d = depth; d < branch_depth; d++) {
                if (dirty_nibs[i][d] != path[d])
                    return rebuild_from_cursor(ih, depth, cursor, path, depth);
            }
        }
    }

    /* Case 3: Load existing branch */
    uint16_t old_bitmap = 0;
    node_ref_t old_ref;
    uint8_t old_child_depths[16] = {0};
    memset(&old_ref, 0, sizeof(old_ref));

    if (!ih_load(ih, path, branch_depth, &old_bitmap, &old_ref, old_child_depths))
        return rebuild_from_cursor(ih, depth, cursor, path, depth);

    /* Case 4: Partition dirty keys and process each child */
    node_ref_t children[16];
    memset(children, 0, sizeof(children));
    uint16_t new_bitmap = 0;
    uint8_t new_child_depths[16] = {0};

    for (uint8_t n = 0; n < 16; n++) {
        size_t sub_from, sub_to;
        find_nibble_range(dirty_nibs, dirty_nib_lens,
                          from, to, branch_depth, n,
                          &sub_from, &sub_to);

        bool is_dirty = (sub_from < sub_to);
        bool existed = (old_bitmap & (1 << n)) != 0;

        if (!is_dirty && !existed) continue;

        if (!is_dirty && existed) {
            subtree_result_t cr = get_clean_child_ref(
                ih, path, branch_depth, n,
                old_child_depths[n], cursor);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
            if (cr.ref.len > 0) new_bitmap |= (1 << n);
            continue;
        }

        path[branch_depth] = n;

        if (existed && old_child_depths[n] > 0) {
            subtree_result_t cr = update_subtree(
                ih, dirty_nibs, dirty_nib_lens,
                dirty_vals, dirty_vlens,
                sub_from, sub_to,
                branch_depth + 1,
                old_child_depths[n],
                cursor, path);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
        } else {
            subtree_result_t cr = rebuild_from_cursor(
                ih, branch_depth + 1, cursor,
                path, branch_depth + 1);
            children[n] = cr.ref;
            new_child_depths[n] = (uint8_t)cr.branch_depth;
        }

        if (children[n].len > 0) {
            new_bitmap |= (1 << n);
        } else {
            new_child_depths[n] = 0;
        }
    }

    /* Case 5: Check for structural collapse */
    int popcount = 0;
    for (int i = 0; i < 16; i++) {
        if (new_bitmap & (1 << i)) popcount++;
    }

    if (popcount == 0) return empty;

    if (popcount == 1) {
        return rebuild_from_cursor(ih, depth, cursor, path, depth);
    }

    /* Case 6: Re-hash branch */
    node_ref_t branch_ref = hash_branch(children, NULL, 0);
    ih_store(ih, path, branch_depth, new_bitmap, &branch_ref, new_child_depths);

    if (branch_depth > depth) {
        node_ref_t ext_ref = hash_extension(
            path + depth, branch_depth - depth, &branch_ref);
        subtree_result_t r = {ext_ref, branch_depth + 1};
        return r;
    }

    subtree_result_t r = {branch_ref, branch_depth + 1};
    return r;
}

/* =========================================================================
 * Public: Incremental Update
 * ========================================================================= */

hash_t ihs_update(ihs_state_t *ih,
                   const uint8_t *const *dirty_keys,
                   const uint8_t *const *dirty_vals,
                   const size_t *dirty_vlens,
                   size_t count,
                   ih_cursor_t *cursor) {
    if (!ih || !ih->initialized) return HASH_EMPTY_STORAGE;
    if (count == 0) return ih->root;

    uint8_t (*nibble_arrays)[64] = malloc(count * 64);
    size_t *nibble_lens = malloc(count * sizeof(size_t));
    const uint8_t **nibble_ptrs = malloc(count * sizeof(uint8_t *));

    if (!nibble_arrays || !nibble_lens || !nibble_ptrs) {
        free(nibble_arrays); free(nibble_lens); free(nibble_ptrs);
        return ih->root;
    }

    for (size_t i = 0; i < count; i++) {
        key_to_nibbles(dirty_keys[i], 32, nibble_arrays[i]);
        nibble_lens[i] = 64;
        nibble_ptrs[i] = nibble_arrays[i];
    }

    uint8_t path[65] = {0};

    subtree_result_t root_result = update_subtree(
        ih,
        (const uint8_t *const *)nibble_ptrs,
        nibble_lens,
        dirty_vals, dirty_vlens,
        0, count,
        0,
        ih->root_branch_depth,
        cursor, path);

    ih->root_branch_depth = root_result.branch_depth;
    ih->root = finalize_root(root_result.ref);

    free(nibble_arrays);
    free(nibble_lens);
    free(nibble_ptrs);

    return ih->root;
}

hash_t ihs_root(const ihs_state_t *ih) {
    if (!ih) return HASH_EMPTY_STORAGE;
    return ih->root;
}

size_t ihs_entry_count(const ihs_state_t *ih) {
    if (!ih || !ih->store) return 0;
    return (size_t)hash_store_count(ih->store);
}
