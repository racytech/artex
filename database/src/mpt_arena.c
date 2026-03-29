/*
 * MPT Arena — In-Memory Merkle Patricia Trie with Incremental Updates.
 *
 * All node RLP lives in a memory arena. compact_art index maps
 * node_hash(32B) → packed record (offset + length + refcount).
 * Writes go directly to arena + index — no deferred buffer, no flush.
 *
 * The trie walk logic (update_subtrie, merge_branch, merge_leaf,
 * merge_extension, build_fresh, collapse_branch) is carried over
 * from mpt_store.c with minimal changes (mpt_store_t → mpt_arena_t,
 * load_node_rlp reads from arena instead of mmap'd file).
 */

#include "mpt_arena.h"
#include "compact_art.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define NODE_HASH_SIZE     32
#define MAX_NIBBLES        64
#define MAX_NODE_RLP       1024
#define DIRTY_INIT_CAP     256

static const uint8_t EMPTY_ROOT[32] = {
    0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
    0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
    0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
    0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
};

/* =========================================================================
 * Arena: growing memory buffer for node RLP data
 * ========================================================================= */

#define ARENA_INIT_SIZE  (1ULL << 20)   /* 1 MB */

typedef struct {
    uint8_t *data;
    size_t   used;
    size_t   capacity;
} arena_t;

static bool arena_init(arena_t *a) {
    a->data = malloc(ARENA_INIT_SIZE);
    if (!a->data) return false;
    a->used = 0;
    a->capacity = ARENA_INIT_SIZE;
    return true;
}

static void arena_destroy(arena_t *a) {
    free(a->data);
    a->data = NULL;
    a->used = a->capacity = 0;
}

static void arena_reset(arena_t *a) {
    a->used = 0;
}

/* Append data, return offset. Returns (size_t)-1 on failure. */
static size_t arena_append(arena_t *a, const uint8_t *data, size_t len) {
    if (a->used + len > a->capacity) {
        size_t new_cap = a->capacity * 2;
        while (new_cap < a->used + len)
            new_cap *= 2;
        uint8_t *p = realloc(a->data, new_cap);
        if (!p) return (size_t)-1;
        a->data = p;
        a->capacity = new_cap;
    }
    size_t offset = a->used;
    memcpy(a->data + offset, data, len);
    a->used += len;
    return offset;
}

/* =========================================================================
 * Node record: packed into 8 bytes in compact_art value
 *   bits 0-39:   offset (40 bits = 1TB addressable)
 *   bits 40-50:  length (11 bits = max 2047)
 *   bits 51-63:  refcount (13 bits = max 8191)
 * ========================================================================= */

typedef uint64_t node_record_t;

static inline node_record_t rec_pack(uint64_t offset, uint32_t length,
                                      uint32_t refcount) {
    if (refcount > 0x1FFF) refcount = 0x1FFF;
    return (offset & 0xFFFFFFFFFFULL) |
           ((uint64_t)(length & 0x7FF) << 40) |
           ((uint64_t)(refcount & 0x1FFF) << 51);
}

static inline uint64_t rec_offset(node_record_t r)   { return r & 0xFFFFFFFFFFULL; }
static inline uint32_t rec_length(node_record_t r)   { return (r >> 40) & 0x7FF; }
static inline uint32_t rec_refcount(node_record_t r) { return (r >> 51) & 0x1FFF; }

_Static_assert(sizeof(node_record_t) == 8, "node_record_t must be 8 bytes");

/* =========================================================================
 * Dirty entry (staged update/delete)
 * ========================================================================= */

typedef struct {
    uint8_t  nibbles[MAX_NIBBLES];
    uint8_t *value;
    size_t   value_len;
} dirty_entry_t;

/* =========================================================================
 * Node reference: hash OR inline RLP (< 32 bytes)
 * ========================================================================= */

typedef enum { REF_EMPTY, REF_HASH, REF_INLINE } ref_type_t;

typedef struct {
    ref_type_t type;
    union {
        uint8_t hash[32];
        struct { uint8_t data[31]; uint8_t len; } raw;
    };
} node_ref_t;

/* =========================================================================
 * Decoded node (transient, used during trie walks)
 * ========================================================================= */

typedef enum {
    MPT_NODE_BRANCH,
    MPT_NODE_EXTENSION,
    MPT_NODE_LEAF,
} mpt_node_type_t;

typedef struct {
    mpt_node_type_t type;
    union {
        struct {
            node_ref_t children[16];
        } branch;
        struct {
            uint8_t   path[MAX_NIBBLES];
            uint8_t   path_len;
            node_ref_t child;
        } extension;
        struct {
            uint8_t   path[MAX_NIBBLES];
            uint8_t   path_len;
            uint8_t  *value;
            size_t    value_len;
        } leaf;
    };
} mpt_node_t;

/* =========================================================================
 * Main struct
 * ========================================================================= */

struct mpt_arena {
    compact_art_t    index;
    arena_t          arena;
    uint8_t          root_hash[32];

    dirty_entry_t   *dirty;
    size_t           dirty_count;
    size_t           dirty_cap;
    bool             batch_active;

    /* Page-based arena for dirty entry values */
    uint8_t        **val_pages;
    size_t           val_page_count;
    size_t           val_page_cap;
    size_t           val_page_used;
#define VAL_PAGE_SIZE 65536

    bool             shared;

    mpt_arena_commit_stats_t cstats;
};

/* =========================================================================
 * compact_art index wrappers
 * ========================================================================= */

static inline bool idx_get(const mpt_arena_t *ma, const uint8_t hash[32],
                            node_record_t *rec) {
    const void *val = compact_art_get(&ma->index, hash);
    if (!val) return false;
    memcpy(rec, val, sizeof(*rec));
    return true;
}

static inline bool idx_put(mpt_arena_t *ma, const uint8_t hash[32],
                            const node_record_t *rec) {
    return compact_art_insert(&ma->index, hash, rec);
}

static inline bool idx_delete(mpt_arena_t *ma, const uint8_t hash[32]) {
    return compact_art_delete(&ma->index, hash);
}

/* =========================================================================
 * Timing
 * ========================================================================= */

static inline uint64_t cstat_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
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

/* =========================================================================
 * RLP encoding helpers
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

/* =========================================================================
 * Hex-prefix encoding (Yellow Paper, Appendix C)
 * ========================================================================= */

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
 * Hex-prefix decoding
 * ========================================================================= */

static size_t hex_prefix_decode(const uint8_t *encoded, size_t enc_len,
                                uint8_t *nibbles, bool *is_leaf) {
    if (enc_len == 0) { *is_leaf = false; return 0; }
    uint8_t first = encoded[0] >> 4;
    *is_leaf = (first >= 2);
    bool odd = (first & 1) != 0;

    size_t out = 0;
    if (odd) {
        nibbles[out++] = encoded[0] & 0x0F;
    }
    for (size_t i = 1; i < enc_len; i++) {
        nibbles[out++] = (encoded[i] >> 4) & 0x0F;
        nibbles[out++] =  encoded[i]       & 0x0F;
    }
    return out;
}

/* =========================================================================
 * RLP decoding helpers
 * ========================================================================= */

typedef struct {
    const uint8_t *data;
    size_t len;
} rlp_slice_t;

static size_t rlp_decode_header(const uint8_t *buf, size_t buf_len,
                                size_t *data_len, bool *is_list) {
    if (buf_len == 0) return 0;
    uint8_t b = buf[0];

    if (b < 0x80) {
        *data_len = 1; *is_list = false; return 0;
    }
    if (b <= 0xb7) {
        *data_len = b - 0x80; *is_list = false; return 1;
    }
    if (b <= 0xbf) {
        size_t ll = b - 0xb7;
        if (1 + ll > buf_len) return 0;
        size_t len = 0;
        for (size_t i = 0; i < ll; i++)
            len = (len << 8) | buf[1 + i];
        *data_len = len; *is_list = false; return 1 + ll;
    }
    if (b <= 0xf7) {
        *data_len = b - 0xc0; *is_list = true; return 1;
    }
    size_t ll = b - 0xf7;
    if (1 + ll > buf_len) return 0;
    size_t len = 0;
    for (size_t i = 0; i < ll; i++)
        len = (len << 8) | buf[1 + i];
    *data_len = len; *is_list = true; return 1 + ll;
}

static size_t rlp_list_items(const uint8_t *list_data, size_t list_len,
                             rlp_slice_t *items, size_t max_items) {
    size_t pos = 0, count = 0;
    while (pos < list_len && count < max_items) {
        size_t data_len;
        bool is_list;
        uint8_t b = list_data[pos];

        if (b < 0x80) {
            items[count].data = &list_data[pos];
            items[count].len = 1;
            pos += 1;
        } else {
            size_t hdr_off = rlp_decode_header(&list_data[pos], list_len - pos,
                                               &data_len, &is_list);
            size_t total;
            if (b < 0x80) {
                total = 1;
            } else {
                total = hdr_off + data_len;
            }
            items[count].data = &list_data[pos];
            items[count].len = total;
            pos += total;
        }
        count++;
    }
    return count;
}

/* =========================================================================
 * Node decoding
 * ========================================================================= */

static bool decode_ref(const rlp_slice_t *item, node_ref_t *ref) {
    if (item->len == 1 && item->data[0] == 0x80) {
        ref->type = REF_EMPTY;
        return true;
    }
    if (item->len == 33 && item->data[0] == 0xa0) {
        ref->type = REF_HASH;
        memcpy(ref->hash, item->data + 1, 32);
        return true;
    }
    if (item->len < 32) {
        ref->type = REF_INLINE;
        memcpy(ref->raw.data, item->data, item->len);
        ref->raw.len = (uint8_t)item->len;
        return true;
    }
    return false;
}

static bool decode_node(const uint8_t *rlp, size_t rlp_len, mpt_node_t *node) {
    size_t data_len;
    bool is_list;
    size_t hdr = rlp_decode_header(rlp, rlp_len, &data_len, &is_list);
    if (!is_list || hdr == 0) return false;

    const uint8_t *list_data = rlp + hdr;

    rlp_slice_t items[17];
    size_t n_items = rlp_list_items(list_data, data_len, items, 17);

    if (n_items == 17) {
        /* Branch node */
        node->type = MPT_NODE_BRANCH;
        for (int i = 0; i < 16; i++) {
            if (!decode_ref(&items[i], &node->branch.children[i]))
                return false;
        }
        return true;
    }

    if (n_items == 2) {
        /* Leaf or extension */
        size_t path_data_len;
        bool path_is_list;
        size_t path_hdr = rlp_decode_header(items[0].data, items[0].len,
                                            &path_data_len, &path_is_list);
        const uint8_t *path_bytes = items[0].data + path_hdr;

        bool is_leaf;
        uint8_t nibbles[MAX_NIBBLES];
        size_t nibble_len = hex_prefix_decode(path_bytes, path_data_len,
                                              nibbles, &is_leaf);

        if (is_leaf) {
            node->type = MPT_NODE_LEAF;
            memcpy(node->leaf.path, nibbles, nibble_len);
            node->leaf.path_len = (uint8_t)nibble_len;

            size_t val_data_len;
            bool val_is_list;
            size_t val_hdr = rlp_decode_header(items[1].data, items[1].len,
                                               &val_data_len, &val_is_list);
            if (val_is_list) {
                node->leaf.value = (uint8_t *)items[1].data;
                node->leaf.value_len = items[1].len;
            } else {
                if (val_hdr == 0 && items[1].len == 1 && items[1].data[0] < 0x80) {
                    node->leaf.value = (uint8_t *)items[1].data;
                    node->leaf.value_len = 1;
                } else {
                    node->leaf.value = (uint8_t *)(items[1].data + val_hdr);
                    node->leaf.value_len = val_data_len;
                }
            }
            return true;
        }

        /* Extension */
        node->type = MPT_NODE_EXTENSION;
        memcpy(node->extension.path, nibbles, nibble_len);
        node->extension.path_len = (uint8_t)nibble_len;
        return decode_ref(&items[1], &node->extension.child);
    }

    return false;
}

/* =========================================================================
 * Value page arena (for dirty entry values during batch)
 * ========================================================================= */

static uint8_t *val_alloc(mpt_arena_t *ma, size_t len) {
    if (ma->val_page_count == 0 || ma->val_page_used + len > VAL_PAGE_SIZE) {
        if (ma->val_page_count >= ma->val_page_cap) {
            size_t new_cap = ma->val_page_cap ? ma->val_page_cap * 2 : 4;
            uint8_t **pp = realloc(ma->val_pages, new_cap * sizeof(uint8_t *));
            if (!pp) return NULL;
            ma->val_pages = pp;
            ma->val_page_cap = new_cap;
        }
        uint8_t *page = malloc(VAL_PAGE_SIZE);
        if (!page) return NULL;
        ma->val_pages[ma->val_page_count++] = page;
        ma->val_page_used = 0;
    }
    uint8_t *p = ma->val_pages[ma->val_page_count - 1] + ma->val_page_used;
    ma->val_page_used += len;
    return p;
}

/* =========================================================================
 * Core node operations (the parts that differ from mpt_store)
 * ========================================================================= */

/* Load node RLP from arena. Returns length, 0 = not found. */
static size_t load_node_rlp(const mpt_arena_t *ma, const uint8_t hash[32],
                             uint8_t *buf) {
    uint64_t _t0 = cstat_now();
    mpt_arena_commit_stats_t *cs = (mpt_arena_commit_stats_t *)&ma->cstats;

    node_record_t rec;
    if (!idx_get(ma, hash, &rec)) {
        cs->load_ns += (double)(cstat_now() - _t0);
        return 0;
    }

    uint32_t len = rec_length(rec);
    uint64_t off = rec_offset(rec);
    if (len == 0 || len > MAX_NODE_RLP || off + len > ma->arena.used) {
        cs->load_ns += (double)(cstat_now() - _t0);
        return 0;
    }

    memcpy(buf, ma->arena.data + off, len);
    cs->load_ns += (double)(cstat_now() - _t0);
    cs->nodes_loaded++;
    return len;
}

/* Write node to arena + index. Returns true, sets out_hash. */
static bool write_node(mpt_arena_t *ma, const uint8_t *rlp, size_t rlp_len,
                       uint8_t out_hash[32]) {
    uint64_t _t0 = cstat_now();
    keccak(rlp, rlp_len, out_hash);
    ma->cstats.keccak_ns += (double)(cstat_now() - _t0);
    ma->cstats.nodes_hashed++;

    /* Check for duplicates */
    node_record_t rec;
    if (idx_get(ma, out_hash, &rec)) {
        if (ma->shared) {
            uint32_t rc = rec_refcount(rec);
            if (rc < 0x1FFF) {
                rec = rec_pack(rec_offset(rec), rec_length(rec), rc + 1);
                idx_put(ma, out_hash, &rec);
            }
        }
        ma->cstats.check_hits++;
        return true;
    }

    /* Append to arena */
    size_t offset = arena_append(&ma->arena, rlp, rlp_len);
    if (offset == (size_t)-1) return false;

    rec = rec_pack(offset, (uint32_t)rlp_len, 1);
    idx_put(ma, out_hash, &rec);
    return true;
}

/* Delete node: decrement refcount, free if zero. */
static void delete_node(mpt_arena_t *ma, const uint8_t hash[32]) {
    if (!ma->shared) {
        /* Non-shared: just remove from index.
         * Arena space is not reclaimed (becomes a hole). */
        idx_delete(ma, hash);
        ma->cstats.deletes++;
        return;
    }

    /* Shared: decrement refcount */
    node_record_t rec;
    if (!idx_get(ma, hash, &rec)) return;

    uint32_t rc = rec_refcount(rec);
    if (rc > 1) {
        rec = rec_pack(rec_offset(rec), rec_length(rec), rc - 1);
        idx_put(ma, hash, &rec);
    } else {
        idx_delete(ma, hash);
    }
    ma->cstats.deletes++;
}

/* =========================================================================
 * Node RLP encoding + construction
 * ========================================================================= */

static bool encode_child_ref(rlp_buf_t *buf, const node_ref_t *ref) {
    switch (ref->type) {
    case REF_EMPTY:
        return rbuf_encode_empty(buf);
    case REF_HASH:
        return rbuf_encode_bytes(buf, ref->hash, 32);
    case REF_INLINE:
        return rbuf_append(buf, ref->raw.data, ref->raw.len);
    }
    return false;
}

static node_ref_t make_branch(mpt_arena_t *ma, const node_ref_t children[16]) {
    node_ref_t result = { .type = REF_EMPTY };

    rlp_buf_t payload; rbuf_reset(&payload);
    for (int i = 0; i < 16; i++) {
        if (!encode_child_ref(&payload, &children[i]))
            return result;
    }
    rbuf_encode_empty(&payload); /* value slot — always empty */

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ma, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

static node_ref_t make_leaf(mpt_arena_t *ma, const uint8_t *suffix,
                            size_t suffix_len, const uint8_t *value,
                            size_t value_len) {
    node_ref_t result = { .type = REF_EMPTY };

    uint8_t encoded_path[33];
    size_t enc_len = hex_prefix_encode(suffix, suffix_len, true, encoded_path);

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, encoded_path, enc_len);
    rbuf_encode_bytes(&payload, value, value_len);

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ma, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

static node_ref_t make_extension(mpt_arena_t *ma, const uint8_t *path,
                                 size_t path_len, const node_ref_t *child) {
    node_ref_t result = { .type = REF_EMPTY };

    if (path_len == 0) return *child;

    uint8_t encoded_path[33];
    size_t enc_len = hex_prefix_encode(path, path_len, false, encoded_path);

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, encoded_path, enc_len);
    encode_child_ref(&payload, child);

    rlp_buf_t node; rbuf_reset(&node);
    if (!rbuf_list_wrap(&node, &payload))
        return result;

    if (node.len < 32) {
        result.type = REF_INLINE;
        memcpy(result.raw.data, node.data, node.len);
        result.raw.len = (uint8_t)node.len;
    } else {
        result.type = REF_HASH;
        if (!write_node(ma, node.data, node.len, result.hash))
            result.type = REF_EMPTY;
    }
    return result;
}

/* =========================================================================
 * Core trie update algorithm
 * (Same logic as mpt_store.c, just uses mpt_arena_t instead)
 * ========================================================================= */

static node_ref_t update_subtrie(mpt_arena_t *ma, const node_ref_t *current,
                                 dirty_entry_t *entries, size_t start,
                                 size_t end, size_t depth);

static bool load_from_ref(const mpt_arena_t *ma, const node_ref_t *ref,
                          uint8_t *buf, size_t *buf_len, mpt_node_t *node) {
    if (ref->type == REF_EMPTY) return false;

    if (ref->type == REF_INLINE) {
        memcpy(buf, ref->raw.data, ref->raw.len);
        *buf_len = ref->raw.len;
        return decode_node(buf, ref->raw.len, node);
    }

    *buf_len = load_node_rlp(ma, ref->hash, buf);
    if (*buf_len == 0) return false;
    return decode_node(buf, *buf_len, node);
}

static int count_children(const node_ref_t children[16]) {
    int n = 0;
    for (int i = 0; i < 16; i++)
        if (children[i].type != REF_EMPTY) n++;
    return n;
}

static int single_child_index(const node_ref_t children[16]) {
    int idx = -1;
    for (int i = 0; i < 16; i++) {
        if (children[i].type != REF_EMPTY) {
            if (idx >= 0) return -1;
            idx = i;
        }
    }
    return idx;
}

static void delete_ref(mpt_arena_t *ma, const node_ref_t *ref) {
    if (ref->type == REF_HASH)
        delete_node(ma, ref->hash);
}

static node_ref_t collapse_branch(mpt_arena_t *ma, node_ref_t children[16]) {
    int idx = single_child_index(children);
    if (idx < 0) return make_branch(ma, children);

    node_ref_t child_ref = children[idx];
    uint8_t prefix_nibble = (uint8_t)idx;

    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t child_node;

    if (!load_from_ref(ma, &child_ref, buf, &buf_len, &child_node)) {
        return make_extension(ma, &prefix_nibble, 1, &child_ref);
    }

    delete_ref(ma, &child_ref);

    if (child_node.type == MPT_NODE_LEAF) {
        uint8_t merged_path[MAX_NIBBLES];
        merged_path[0] = prefix_nibble;
        memcpy(merged_path + 1, child_node.leaf.path, child_node.leaf.path_len);
        return make_leaf(ma, merged_path, 1 + child_node.leaf.path_len,
                         child_node.leaf.value, child_node.leaf.value_len);
    }

    if (child_node.type == MPT_NODE_EXTENSION) {
        uint8_t merged_path[MAX_NIBBLES];
        merged_path[0] = prefix_nibble;
        memcpy(merged_path + 1, child_node.extension.path,
               child_node.extension.path_len);
        return make_extension(ma, merged_path,
                              1 + child_node.extension.path_len,
                              &child_node.extension.child);
    }

    node_ref_t new_child = make_branch(ma, child_node.branch.children);
    return make_extension(ma, &prefix_nibble, 1, &new_child);
}

static node_ref_t build_fresh(mpt_arena_t *ma, dirty_entry_t *entries,
                              size_t start, size_t end, size_t depth) {
    node_ref_t empty = { .type = REF_EMPTY };

    size_t live = 0;
    for (size_t i = start; i < end; i++)
        if (entries[i].value != NULL) live++;
    if (live == 0) return empty;

    if (depth >= MAX_NIBBLES) {
        return make_leaf(ma, NULL, 0,
                         entries[start].value, entries[start].value_len);
    }

    if (live < end - start) {
        size_t w = start;
        for (size_t r = start; r < end; r++) {
            if (entries[r].value != NULL) {
                if (w != r) {
                    entries[w] = entries[r];
                    entries[r].value = NULL;
                }
                w++;
            }
        }
        end = start + live;
    }

    if (live == 1) {
        return make_leaf(ma, &entries[start].nibbles[depth],
                         MAX_NIBBLES - depth,
                         entries[start].value, entries[start].value_len);
    }

    size_t common_len = MAX_NIBBLES - depth;
    for (size_t i = start + 1; i < end && common_len > 0; i++) {
        for (size_t j = 0; j < common_len; j++) {
            if (entries[start].nibbles[depth + j] != entries[i].nibbles[depth + j]) {
                common_len = j;
                break;
            }
        }
    }

    if (common_len > 0) {
        node_ref_t child = build_fresh(ma, entries, start, end,
                                       depth + common_len);
        if (child.type == REF_EMPTY) return empty;
        return make_extension(ma, &entries[start].nibbles[depth],
                              common_len, &child);
    }

    node_ref_t children[16];
    for (int i = 0; i < 16; i++) children[i].type = REF_EMPTY;

    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && entries[group_end].nibbles[depth] == nibble)
            group_end++;
        children[nibble] = build_fresh(ma, entries, i, group_end, depth + 1);
        i = group_end;
    }

    return make_branch(ma, children);
}

static node_ref_t merge_branch(mpt_arena_t *ma, const node_ref_t *old_ref,
                                const mpt_node_t *branch,
                                dirty_entry_t *entries, size_t start,
                                size_t end, size_t depth) {
    node_ref_t children[16];
    memcpy(children, branch->branch.children, sizeof(children));
    bool changed = false;

    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth];
        size_t group_end = i + 1;
        while (group_end < end && entries[group_end].nibbles[depth] == nibble)
            group_end++;

        node_ref_t old_child = children[nibble];
        node_ref_t new_child = update_subtrie(ma, &old_child, entries,
                                              i, group_end, depth + 1);

        if (old_child.type != new_child.type ||
            (new_child.type == REF_HASH &&
             memcmp(old_child.hash, new_child.hash, 32) != 0) ||
            (new_child.type == REF_INLINE &&
             (old_child.raw.len != new_child.raw.len ||
              memcmp(old_child.raw.data, new_child.raw.data, new_child.raw.len) != 0))) {
            children[nibble] = new_child;
            changed = true;
        }

        i = group_end;
    }

    if (!changed) return *old_ref;

    delete_ref(ma, old_ref);

    int non_empty = count_children(children);
    if (non_empty == 0) return (node_ref_t){ .type = REF_EMPTY };
    if (non_empty == 1) return collapse_branch(ma, children);
    return make_branch(ma, children);
}

static node_ref_t merge_leaf(mpt_arena_t *ma, const node_ref_t *old_ref,
                              const mpt_node_t *leaf,
                              dirty_entry_t *entries, size_t start,
                              size_t end, size_t depth) {
    const uint8_t *leaf_suffix = leaf->leaf.path;
    size_t leaf_suffix_len = leaf->leaf.path_len;

    if (end - start == 1) {
        bool match = (leaf_suffix_len == MAX_NIBBLES - depth);
        if (match) {
            for (size_t j = 0; j < leaf_suffix_len; j++) {
                if (entries[start].nibbles[depth + j] != leaf_suffix[j]) {
                    match = false;
                    break;
                }
            }
        }

        if (match) {
            delete_ref(ma, old_ref);
            if (entries[start].value == NULL)
                return (node_ref_t){ .type = REF_EMPTY };
            return make_leaf(ma, leaf_suffix, leaf_suffix_len,
                             entries[start].value, entries[start].value_len);
        }
    }

    size_t total = (end - start) + 1;
    dirty_entry_t stack_buf[8];
    dirty_entry_t *merged = (total <= 8) ? stack_buf
                            : malloc(total * sizeof(*merged));
    if (!merged) return *old_ref;

    delete_ref(ma, old_ref);

    memset(merged[0].nibbles, 0, MAX_NIBBLES);
    memcpy(merged[0].nibbles, entries[start].nibbles, depth);
    memcpy(merged[0].nibbles + depth, leaf_suffix, leaf_suffix_len);
    merged[0].value = (uint8_t *)leaf->leaf.value;
    merged[0].value_len = leaf->leaf.value_len;

    for (size_t i = 0; i < end - start; i++)
        merged[1 + i] = entries[start + i];

    for (size_t i = 1; i < total; i++) {
        if (memcmp(merged[0].nibbles, merged[i].nibbles, MAX_NIBBLES) == 0) {
            merged[0] = merged[total - 1];
            total--;
            break;
        }
    }

    for (size_t i = 1; i < total; i++) {
        dirty_entry_t tmp = merged[i];
        size_t j = i;
        while (j > 0 && memcmp(tmp.nibbles, merged[j - 1].nibbles, MAX_NIBBLES) < 0) {
            merged[j] = merged[j - 1];
            j--;
        }
        merged[j] = tmp;
    }

    node_ref_t result = build_fresh(ma, merged, 0, total, depth);
    if (merged != stack_buf) free(merged);
    return result;
}

static node_ref_t merge_extension(mpt_arena_t *ma, const node_ref_t *old_ref,
                                   const mpt_node_t *ext,
                                   dirty_entry_t *entries, size_t start,
                                   size_t end, size_t depth) {
    const uint8_t *ext_path = ext->extension.path;
    size_t ext_len = ext->extension.path_len;

    size_t shared = ext_len;
    for (size_t i = start; i < end; i++) {
        for (size_t j = 0; j < shared; j++) {
            if (entries[i].nibbles[depth + j] != ext_path[j]) {
                shared = j;
                break;
            }
        }
    }

    if (shared == ext_len) {
        node_ref_t old_child = ext->extension.child;
        node_ref_t new_child = update_subtrie(ma, &old_child, entries,
                                              start, end, depth + ext_len);

        bool same = (old_child.type == new_child.type);
        if (same && new_child.type == REF_HASH)
            same = (memcmp(old_child.hash, new_child.hash, 32) == 0);
        if (same && new_child.type == REF_INLINE)
            same = (old_child.raw.len == new_child.raw.len &&
                    memcmp(old_child.raw.data, new_child.raw.data, new_child.raw.len) == 0);

        if (same) return *old_ref;

        delete_ref(ma, old_ref);

        if (new_child.type == REF_EMPTY)
            return (node_ref_t){ .type = REF_EMPTY };

        uint8_t buf[MAX_NODE_RLP];
        size_t buf_len;
        mpt_node_t child_node;
        if (load_from_ref(ma, &new_child, buf, &buf_len, &child_node)) {
            if (child_node.type == MPT_NODE_LEAF) {
                delete_ref(ma, &new_child);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, ext_len);
                memcpy(mp + ext_len, child_node.leaf.path,
                       child_node.leaf.path_len);
                return make_leaf(ma, mp, ext_len + child_node.leaf.path_len,
                                child_node.leaf.value, child_node.leaf.value_len);
            }
            if (child_node.type == MPT_NODE_EXTENSION) {
                delete_ref(ma, &new_child);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, ext_len);
                memcpy(mp + ext_len, child_node.extension.path,
                       child_node.extension.path_len);
                return make_extension(ma, mp,
                                      ext_len + child_node.extension.path_len,
                                      &child_node.extension.child);
            }
        }

        return make_extension(ma, ext_path, ext_len, &new_child);
    }

    /* Extension path diverges — split */
    delete_ref(ma, old_ref);

    node_ref_t children[16];
    for (int i = 0; i < 16; i++) children[i].type = REF_EMPTY;

    uint8_t old_nibble = ext_path[shared];
    size_t remaining = ext_len - shared - 1;
    if (remaining > 0) {
        children[old_nibble] = make_extension(ma, &ext_path[shared + 1],
                                              remaining, &ext->extension.child);
    } else {
        children[old_nibble] = ext->extension.child;
    }

    size_t i = start;
    while (i < end) {
        uint8_t nibble = entries[i].nibbles[depth + shared];
        size_t group_end = i + 1;
        while (group_end < end &&
               entries[group_end].nibbles[depth + shared] == nibble)
            group_end++;

        if (nibble == old_nibble) {
            children[nibble] = update_subtrie(ma, &children[nibble], entries,
                                              i, group_end, depth + shared + 1);
        } else {
            children[nibble] = build_fresh(ma, entries, i, group_end,
                                           depth + shared + 1);
        }
        i = group_end;
    }

    int non_empty = count_children(children);
    if (non_empty == 0) return (node_ref_t){ .type = REF_EMPTY };

    node_ref_t subtrie_ref;
    if (non_empty == 1) {
        subtrie_ref = collapse_branch(ma, children);
    } else {
        subtrie_ref = make_branch(ma, children);
    }

    if (shared > 0) {
        if (subtrie_ref.type == REF_EMPTY) return subtrie_ref;

        uint8_t buf2[MAX_NODE_RLP];
        size_t buf2_len;
        mpt_node_t sub_node;
        if (load_from_ref(ma, &subtrie_ref, buf2, &buf2_len, &sub_node)) {
            if (sub_node.type == MPT_NODE_LEAF) {
                delete_ref(ma, &subtrie_ref);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, shared);
                memcpy(mp + shared, sub_node.leaf.path,
                       sub_node.leaf.path_len);
                return make_leaf(ma, mp, shared + sub_node.leaf.path_len,
                                 sub_node.leaf.value, sub_node.leaf.value_len);
            }
            if (sub_node.type == MPT_NODE_EXTENSION) {
                delete_ref(ma, &subtrie_ref);
                uint8_t mp[MAX_NIBBLES];
                memcpy(mp, ext_path, shared);
                memcpy(mp + shared, sub_node.extension.path,
                       sub_node.extension.path_len);
                return make_extension(ma, mp,
                                      shared + sub_node.extension.path_len,
                                      &sub_node.extension.child);
            }
        }
        return make_extension(ma, ext_path, shared, &subtrie_ref);
    }

    return subtrie_ref;
}

static node_ref_t update_subtrie(mpt_arena_t *ma, const node_ref_t *current,
                                 dirty_entry_t *entries, size_t start,
                                 size_t end, size_t depth) {
    if (start >= end) return *current;

    if (current->type == REF_EMPTY)
        return build_fresh(ma, entries, start, end, depth);

    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t node;

    if (!load_from_ref(ma, current, buf, &buf_len, &node)) {
        if (current->type == REF_HASH)
            ma->cstats.lost_nodes++;
        return build_fresh(ma, entries, start, end, depth);
    }

    switch (node.type) {
    case MPT_NODE_BRANCH:
        return merge_branch(ma, current, &node, entries, start, end, depth);
    case MPT_NODE_LEAF:
        return merge_leaf(ma, current, &node, entries, start, end, depth);
    case MPT_NODE_EXTENSION:
        return merge_extension(ma, current, &node, entries, start, end, depth);
    }

    return (node_ref_t){ .type = REF_EMPTY };
}

/* =========================================================================
 * Dirty entry sort
 * ========================================================================= */

static int dirty_cmp(const void *a, const void *b) {
    const uint8_t *na = ((const dirty_entry_t *)a)->nibbles;
    const uint8_t *nb = ((const dirty_entry_t *)b)->nibbles;
    uint64_t pa, pb;
    memcpy(&pa, na, 8);
    memcpy(&pb, nb, 8);
    if (pa != pb) {
        pa = __builtin_bswap64(pa);
        pb = __builtin_bswap64(pb);
        return pa < pb ? -1 : 1;
    }
    return memcmp(na + 8, nb + 8, MAX_NIBBLES - 8);
}

/* =========================================================================
 * Public API
 * ========================================================================= */

/* key_fetch callback for compact_art compact_leaves mode */
static bool arena_key_fetch(const void *value, uint8_t *key_out, void *ctx) {
    mpt_arena_t *ma = (mpt_arena_t *)ctx;
    node_record_t rec;
    memcpy(&rec, value, sizeof(rec));
    uint32_t len = rec_length(rec);
    uint64_t off = rec_offset(rec);
    if (len > 0 && len <= MAX_NODE_RLP && off + len <= ma->arena.used) {
        keccak(ma->arena.data + off, len, key_out);
        return true;
    }
    return false;
}

mpt_arena_t *mpt_arena_create(void) {
    mpt_arena_t *ma = calloc(1, sizeof(*ma));
    if (!ma) return NULL;

    if (!compact_art_init(&ma->index, NODE_HASH_SIZE, sizeof(node_record_t),
                          true, arena_key_fetch, ma)) {
        free(ma);
        return NULL;
    }

    if (!arena_init(&ma->arena)) {
        compact_art_destroy(&ma->index);
        free(ma);
        return NULL;
    }

    memcpy(ma->root_hash, EMPTY_ROOT, 32);
    return ma;
}

void mpt_arena_destroy(mpt_arena_t *ma) {
    if (!ma) return;
    compact_art_destroy(&ma->index);
    arena_destroy(&ma->arena);
    free(ma->dirty);
    for (size_t i = 0; i < ma->val_page_count; i++)
        free(ma->val_pages[i]);
    free(ma->val_pages);
    free(ma);
}

void mpt_arena_reset(mpt_arena_t *ma) {
    if (!ma) return;
    compact_art_destroy(&ma->index);
    compact_art_init(&ma->index, NODE_HASH_SIZE, sizeof(node_record_t),
                     true, arena_key_fetch, ma);
    arena_reset(&ma->arena);
    memcpy(ma->root_hash, EMPTY_ROOT, 32);
    ma->dirty_count = 0;
    ma->batch_active = false;
    ma->val_page_used = 0;
    memset(&ma->cstats, 0, sizeof(ma->cstats));
}

void mpt_arena_root(const mpt_arena_t *ma, uint8_t out[32]) {
    if (!ma) { memset(out, 0, 32); return; }
    memcpy(out, ma->root_hash, 32);
}

void mpt_arena_set_root(mpt_arena_t *ma, const uint8_t root[32]) {
    if (!ma) return;
    memcpy(ma->root_hash, root, 32);
}

void mpt_arena_set_shared(mpt_arena_t *ma, bool shared) {
    if (!ma) return;
    ma->shared = shared;
}

bool mpt_arena_begin_batch(mpt_arena_t *ma) {
    if (!ma || ma->batch_active) return false;
    ma->batch_active = true;
    ma->dirty_count = 0;
    ma->val_page_used = 0;
    return true;
}

bool mpt_arena_update(mpt_arena_t *ma, const uint8_t key[32],
                       const uint8_t *value, size_t value_len) {
    if (!ma || !ma->batch_active) return false;

    if (ma->dirty_count >= ma->dirty_cap) {
        size_t new_cap = ma->dirty_cap ? ma->dirty_cap * 2 : DIRTY_INIT_CAP;
        dirty_entry_t *p = realloc(ma->dirty, new_cap * sizeof(dirty_entry_t));
        if (!p) return false;
        ma->dirty = p;
        ma->dirty_cap = new_cap;
    }

    dirty_entry_t *e = &ma->dirty[ma->dirty_count++];
    bytes_to_nibbles(key, 32, e->nibbles);

    uint8_t *val_copy = val_alloc(ma, value_len);
    if (!val_copy) { ma->dirty_count--; return false; }
    memcpy(val_copy, value, value_len);
    e->value = val_copy;
    e->value_len = value_len;
    return true;
}

bool mpt_arena_delete(mpt_arena_t *ma, const uint8_t key[32]) {
    if (!ma || !ma->batch_active) return false;

    if (ma->dirty_count >= ma->dirty_cap) {
        size_t new_cap = ma->dirty_cap ? ma->dirty_cap * 2 : DIRTY_INIT_CAP;
        dirty_entry_t *p = realloc(ma->dirty, new_cap * sizeof(dirty_entry_t));
        if (!p) return false;
        ma->dirty = p;
        ma->dirty_cap = new_cap;
    }

    dirty_entry_t *e = &ma->dirty[ma->dirty_count++];
    bytes_to_nibbles(key, 32, e->nibbles);
    e->value = NULL;
    e->value_len = 0;
    return true;
}

bool mpt_arena_commit_batch(mpt_arena_t *ma) {
    if (!ma || !ma->batch_active) return false;

    ma->cstats.commits++;

    if (ma->dirty_count == 0) {
        ma->batch_active = false;
        return true;
    }

    /* Sort dirty entries by nibbles */
    uint64_t _sort0 = cstat_now();
    if (ma->dirty_count > 1)
        qsort(ma->dirty, ma->dirty_count, sizeof(dirty_entry_t), dirty_cmp);
    ma->cstats.sort_ns += (double)(cstat_now() - _sort0);

    /* Build current root ref */
    node_ref_t root_ref;
    if (memcmp(ma->root_hash, EMPTY_ROOT, 32) == 0) {
        root_ref.type = REF_EMPTY;
    } else {
        root_ref.type = REF_HASH;
        memcpy(root_ref.hash, ma->root_hash, 32);
    }

    /* Recursively update the trie */
    uint64_t _enc0 = cstat_now();
    double _keccak_before = ma->cstats.keccak_ns;
    double _load_before   = ma->cstats.load_ns;

    node_ref_t new_root = update_subtrie(ma, &root_ref, ma->dirty,
                                         0, ma->dirty_count, 0);

    double _enc_total = (double)(cstat_now() - _enc0);
    ma->cstats.encode_ns += _enc_total - (ma->cstats.keccak_ns - _keccak_before)
                             - (ma->cstats.load_ns - _load_before);

    /* Update root hash */
    if (new_root.type == REF_HASH) {
        memcpy(ma->root_hash, new_root.hash, 32);
    } else if (new_root.type == REF_EMPTY) {
        memcpy(ma->root_hash, EMPTY_ROOT, 32);
    } else {
        /* Inline root — store as regular node so it can be loaded later */
        uint8_t root_hash[32];
        if (write_node(ma, new_root.raw.data, new_root.raw.len, root_hash)) {
            memcpy(ma->root_hash, root_hash, 32);
        } else {
            keccak(new_root.raw.data, new_root.raw.len, ma->root_hash);
        }
    }

    /* Reset dirty state */
    ma->dirty_count = 0;
    ma->val_page_used = 0;
    if (ma->val_page_count > 1) {
        for (size_t i = 1; i < ma->val_page_count; i++)
            free(ma->val_pages[i]);
        ma->val_page_count = 1;
    }
    ma->batch_active = false;
    return true;
}

void mpt_arena_discard_batch(mpt_arena_t *ma) {
    if (!ma) return;
    ma->dirty_count = 0;
    ma->val_page_used = 0;
    ma->batch_active = false;
}

/* =========================================================================
 * Point lookup
 * ========================================================================= */

uint32_t mpt_arena_get(const mpt_arena_t *ma, const uint8_t key[32],
                        uint8_t *buf, uint32_t buf_len) {
    if (!ma || memcmp(ma->root_hash, EMPTY_ROOT, 32) == 0) return 0;

    uint8_t nibbles[MAX_NIBBLES];
    bytes_to_nibbles(key, 32, nibbles);

    node_ref_t current;
    current.type = REF_HASH;
    memcpy(current.hash, ma->root_hash, 32);

    size_t depth = 0;
    uint8_t node_buf[MAX_NODE_RLP];

    while (depth < MAX_NIBBLES) {
        size_t node_len;
        mpt_node_t node;
        if (!load_from_ref(ma, &current, node_buf, &node_len, &node))
            return 0;

        switch (node.type) {
        case MPT_NODE_BRANCH: {
            uint8_t nibble = nibbles[depth];
            current = node.branch.children[nibble];
            if (current.type == REF_EMPTY) return 0;
            depth++;
            break;
        }
        case MPT_NODE_EXTENSION: {
            for (size_t i = 0; i < node.extension.path_len; i++) {
                if (depth + i >= MAX_NIBBLES ||
                    nibbles[depth + i] != node.extension.path[i])
                    return 0;
            }
            depth += node.extension.path_len;
            current = node.extension.child;
            break;
        }
        case MPT_NODE_LEAF: {
            for (size_t i = 0; i < node.leaf.path_len; i++) {
                if (depth + i >= MAX_NIBBLES ||
                    nibbles[depth + i] != node.leaf.path[i])
                    return 0;
            }
            uint32_t vlen = (uint32_t)node.leaf.value_len;
            if (buf && buf_len >= vlen)
                memcpy(buf, node.leaf.value, vlen);
            return vlen;
        }
        }
    }
    return 0;
}

/* =========================================================================
 * Leaf walking
 * ========================================================================= */

static bool walk_node(const mpt_arena_t *ma, const node_ref_t *ref,
                      mpt_arena_leaf_cb_t cb, void *ctx) {
    if (ref->type == REF_EMPTY) return true;

    uint8_t buf[MAX_NODE_RLP];
    size_t buf_len;
    mpt_node_t node;
    if (!load_from_ref(ma, ref, buf, &buf_len, &node)) return true;

    switch (node.type) {
    case MPT_NODE_BRANCH:
        for (int i = 0; i < 16; i++) {
            if (!walk_node(ma, &node.branch.children[i], cb, ctx))
                return false;
        }
        return true;
    case MPT_NODE_EXTENSION:
        return walk_node(ma, &node.extension.child, cb, ctx);
    case MPT_NODE_LEAF:
        return cb(node.leaf.value, node.leaf.value_len, ctx);
    }
    return true;
}

bool mpt_arena_walk_leaves(const mpt_arena_t *ma, mpt_arena_leaf_cb_t cb,
                            void *user_data) {
    if (!ma || memcmp(ma->root_hash, EMPTY_ROOT, 32) == 0) return true;

    node_ref_t root;
    root.type = REF_HASH;
    memcpy(root.hash, ma->root_hash, 32);
    return walk_node(ma, &root, cb, user_data);
}

/* =========================================================================
 * Stats
 * ========================================================================= */

void mpt_arena_reset_commit_stats(mpt_arena_t *ma) {
    if (ma) memset(&ma->cstats, 0, sizeof(ma->cstats));
}

mpt_arena_commit_stats_t mpt_arena_get_commit_stats(const mpt_arena_t *ma) {
    if (!ma) return (mpt_arena_commit_stats_t){0};
    return ma->cstats;
}

mpt_arena_stats_t mpt_arena_stats(const mpt_arena_t *ma) {
    mpt_arena_stats_t st = {0};
    if (!ma) return st;
    st.node_count = compact_art_size(&ma->index);
    st.arena_bytes = ma->arena.used;
    st.arena_capacity = ma->arena.capacity;
    return st;
}

/* =========================================================================
 * Snapshot (TODO — implement when needed)
 * ========================================================================= */

bool mpt_arena_snapshot_write(const mpt_arena_t *ma, const char *path) {
    (void)ma; (void)path;
    return false; /* not yet implemented */
}

mpt_arena_t *mpt_arena_snapshot_load(const char *path) {
    (void)path;
    return NULL; /* not yet implemented */
}
