#include "../include/mpt.h"
#include "hash.h"
#include "rlp.h"
#include "bytes.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <nmmintrin.h>  /* SSE4.2 CRC32C */

/*
 * MPT Trie — mmap'd nibble trie for Ethereum MPT root hash computation
 *
 * Two pools in a single sparse file:
 *   Node pool: 128B slots (branches + extensions)
 *   Leaf pool: 160B slots (key + hash + value)
 *
 * Each node caches its keccak256 hash. Dirty flags propagate from
 * modified leaves to root. mpt_root() walks only dirty nodes.
 */

/* ========================================================================
 * Constants
 * ======================================================================== */

#define META_SIZE           4096
#define META_A_OFFSET       0
#define META_B_OFFSET       4096
#define DATA_OFFSET         8192

#define NODE_SLOT_SIZE      128
#define NODE_POOL_MAX       (32ULL * 1024 * 1024 * 1024)

#define LEAF_POOL_OFFSET    (DATA_OFFSET + NODE_POOL_MAX)
#define LEAF_SLOT_SIZE      160
#define LEAF_POOL_MAX       (32ULL * 1024 * 1024 * 1024)

#define TOTAL_FILE_SIZE     (LEAF_POOL_OFFSET + LEAF_POOL_MAX)

#define META_MAGIC          0x4D50545F54524945ULL  /* "MPT_TRIE" */

#define MAX_EXT_NIBBLES     118  /* 59 bytes × 2 nibbles/byte */

/* ========================================================================
 * Ref encoding (32-bit)
 * ======================================================================== */

typedef uint32_t ref_t;

#define REF_NULL            ((ref_t)0)
#define TYPE_MASK           ((ref_t)0xC0000000u)
#define TYPE_BRANCH         ((ref_t)0xC0000000u)
#define TYPE_EXTENSION      ((ref_t)0x80000000u)
#define TYPE_LEAF           ((ref_t)0x40000000u)
#define INDEX_MASK          ((ref_t)0x3FFFFFFFu)

#define REF_TYPE(r)         ((r) & TYPE_MASK)
#define REF_INDEX(r)        ((r) & INDEX_MASK)
#define IS_BRANCH(r)        (REF_TYPE(r) == TYPE_BRANCH)
#define IS_EXTENSION(r)     (REF_TYPE(r) == TYPE_EXTENSION)
#define IS_LEAF(r)          (REF_TYPE(r) == TYPE_LEAF)

#define MAKE_BRANCH(i)      ((ref_t)(i) | TYPE_BRANCH)
#define MAKE_EXTENSION(i)   ((ref_t)(i) | TYPE_EXTENSION)
#define MAKE_LEAF(i)        ((ref_t)(i) | TYPE_LEAF)

/* ========================================================================
 * Node structures
 * ======================================================================== */

/* Branch node (128B node pool slot) */
typedef struct {
    uint32_t children[16];          /* 64B: child refs */
    uint8_t  hash[32];             /* 32B: cached keccak256 */
    uint8_t  dirty;                /* 1B: needs rehashing */
    uint8_t  _pad[31];
} mpt_branch_t;

/* Extension node (128B node pool slot) */
typedef struct {
    uint8_t  skip_len;             /* number of nibbles (1-118) */
    uint8_t  nibbles[59];          /* packed path (2 nibbles/byte) */
    uint32_t child;                /* ref to next node */
    uint8_t  hash[32];            /* cached hash */
    uint8_t  dirty;
    uint8_t  _pad[31];
} mpt_extension_t;

/* Node pool slot union */
typedef union {
    mpt_branch_t    branch;
    mpt_extension_t ext;
    uint8_t         raw[NODE_SLOT_SIZE];
} node_slot_t;

_Static_assert(sizeof(mpt_branch_t) == 128, "branch must be 128 bytes");
_Static_assert(sizeof(mpt_extension_t) == 128, "extension must be 128 bytes");

/* Leaf node (160B leaf pool slot) */
typedef struct {
    uint8_t  key[MPT_KEY_SIZE];    /* 32B: full key */
    uint8_t  hash[32];            /* 32B: cached keccak256 */
    uint8_t  vlen;                /* 1B: value length */
    uint8_t  value[MPT_MAX_VALUE]; /* 86B: raw value */
    uint8_t  dirty;               /* 1B: needs rehashing */
    uint8_t  _pad[8];
} mpt_leaf_t;

_Static_assert(sizeof(mpt_leaf_t) == LEAF_SLOT_SIZE, "leaf must be 160 bytes");

/* ========================================================================
 * Meta page
 * ======================================================================== */

typedef struct {
    uint64_t magic;
    uint64_t generation;
    uint64_t size;                 /* key count */
    uint32_t root;                 /* root ref */
    uint32_t node_count;
    uint32_t leaf_count;
    uint32_t _reserved;
    uint32_t crc32c;
    uint32_t _pad;
} mpt_meta_t;

#define META_CRC_LEN  (offsetof(mpt_meta_t, crc32c))

/* ========================================================================
 * Tree struct
 * ======================================================================== */

struct mpt {
    int       fd;
    uint8_t  *node_base;           /* mmap'd node pool */
    uint8_t  *leaf_base;           /* mmap'd leaf pool */

    ref_t     root;
    uint64_t  size;
    uint32_t  node_count;
    uint32_t  leaf_count;

    uint64_t  generation;
    int       active_meta;         /* 0 or 1 */

    bool      root_dirty;          /* root hash needs recomputation */
    hash_t    cached_root;         /* last computed root hash */
};

/* ========================================================================
 * CRC32C (hardware SSE4.2)
 * ======================================================================== */

static uint32_t compute_crc32c(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t c = ~(uint64_t)0;
    while (len >= 8) {
        uint64_t val;
        memcpy(&val, p, 8);
        c = _mm_crc32_u64(c, val);
        p += 8;
        len -= 8;
    }
    while (len > 0) {
        c = _mm_crc32_u8((uint32_t)c, *p);
        p++;
        len--;
    }
    return ~(uint32_t)c;
}

/* ========================================================================
 * Nibble helpers
 * ======================================================================== */

static inline uint8_t key_nibble(const uint8_t *data, int i) {
    return (data[i / 2] >> (4 * (1 - (i & 1)))) & 0x0F;
}

static inline void set_nibble(uint8_t *data, int i, uint8_t val) {
    int byte_idx = i / 2;
    if (i & 1)
        data[byte_idx] = (data[byte_idx] & 0xF0) | (val & 0x0F);
    else
        data[byte_idx] = (data[byte_idx] & 0x0F) | ((val & 0x0F) << 4);
}

static void key_to_nibbles(const uint8_t *key, uint8_t *nibbles) {
    for (int i = 0; i < MPT_KEY_SIZE; i++) {
        nibbles[i * 2]     = (key[i] >> 4) & 0x0F;
        nibbles[i * 2 + 1] = key[i] & 0x0F;
    }
}

/* ========================================================================
 * Pointer resolution
 * ======================================================================== */

static inline mpt_branch_t *branch_ptr(const mpt_t *m, ref_t ref) {
    return &((node_slot_t *)(m->node_base +
             (size_t)REF_INDEX(ref) * NODE_SLOT_SIZE))->branch;
}

static inline mpt_extension_t *ext_ptr(const mpt_t *m, ref_t ref) {
    return &((node_slot_t *)(m->node_base +
             (size_t)REF_INDEX(ref) * NODE_SLOT_SIZE))->ext;
}

static inline mpt_leaf_t *leaf_ptr(const mpt_t *m, ref_t ref) {
    return (mpt_leaf_t *)(m->leaf_base +
            (size_t)REF_INDEX(ref) * LEAF_SLOT_SIZE);
}

/* ========================================================================
 * Allocation (bump allocators, index 0 reserved for NULL)
 * ======================================================================== */

static ref_t alloc_branch(mpt_t *m) {
    uint32_t idx = m->node_count;
    if (idx > INDEX_MASK) return REF_NULL;
    if ((size_t)(idx + 1) * NODE_SLOT_SIZE > NODE_POOL_MAX) return REF_NULL;
    m->node_count++;
    node_slot_t *s = (node_slot_t *)(m->node_base +
                      (size_t)idx * NODE_SLOT_SIZE);
    memset(s, 0, NODE_SLOT_SIZE);
    return MAKE_BRANCH(idx);
}

static ref_t alloc_extension(mpt_t *m) {
    uint32_t idx = m->node_count;
    if (idx > INDEX_MASK) return REF_NULL;
    if ((size_t)(idx + 1) * NODE_SLOT_SIZE > NODE_POOL_MAX) return REF_NULL;
    m->node_count++;
    node_slot_t *s = (node_slot_t *)(m->node_base +
                      (size_t)idx * NODE_SLOT_SIZE);
    memset(s, 0, NODE_SLOT_SIZE);
    return MAKE_EXTENSION(idx);
}

static ref_t alloc_leaf(mpt_t *m, const uint8_t *key,
                        const uint8_t *value, uint8_t vlen) {
    uint32_t idx = m->leaf_count;
    if (idx > INDEX_MASK) return REF_NULL;
    if ((size_t)(idx + 1) * LEAF_SLOT_SIZE > LEAF_POOL_MAX) return REF_NULL;
    m->leaf_count++;
    mpt_leaf_t *l = (mpt_leaf_t *)(m->leaf_base +
                     (size_t)idx * LEAF_SLOT_SIZE);
    memset(l, 0, LEAF_SLOT_SIZE);
    memcpy(l->key, key, MPT_KEY_SIZE);
    l->vlen = vlen;
    if (vlen > 0 && value)
        memcpy(l->value, value, vlen);
    l->dirty = 1;
    return MAKE_LEAF(idx);
}

/* ========================================================================
 * Extension construction helpers
 * ======================================================================== */

/* Build extension from key nibbles at key[depth..depth+count-1] → child */
static ref_t make_ext_from_key(mpt_t *m, const uint8_t *key,
                               int depth, int count, ref_t child) {
    if (count == 0) return child;

    if (count > MAX_EXT_NIBBLES) {
        ref_t inner = make_ext_from_key(m, key,
                        depth + MAX_EXT_NIBBLES,
                        count - MAX_EXT_NIBBLES, child);
        if (inner == REF_NULL) return REF_NULL;
        count = MAX_EXT_NIBBLES;
        child = inner;
    }

    ref_t ref = alloc_extension(m);
    if (ref == REF_NULL) return REF_NULL;

    mpt_extension_t *ext = ext_ptr(m, ref);
    ext->skip_len = (uint8_t)count;
    ext->child = child;
    ext->dirty = 1;
    for (int i = 0; i < count; i++)
        set_nibble(ext->nibbles, i, key_nibble(key, depth + i));
    return ref;
}

/* Build extension from saved nibbles buffer */
static ref_t make_ext_from_buf(mpt_t *m, const uint8_t *saved,
                               int start, int count, ref_t child) {
    if (count == 0) return child;

    if (count > MAX_EXT_NIBBLES) {
        ref_t inner = make_ext_from_buf(m, saved,
                        start + MAX_EXT_NIBBLES,
                        count - MAX_EXT_NIBBLES, child);
        if (inner == REF_NULL) return REF_NULL;
        count = MAX_EXT_NIBBLES;
        child = inner;
    }

    ref_t ref = alloc_extension(m);
    if (ref == REF_NULL) return REF_NULL;

    mpt_extension_t *ext = ext_ptr(m, ref);
    ext->skip_len = (uint8_t)count;
    ext->child = child;
    ext->dirty = 1;
    for (int i = 0; i < count; i++)
        set_nibble(ext->nibbles, i, key_nibble(saved, start + i));
    return ref;
}

/* Single nibble extension */
static ref_t make_ext_single(mpt_t *m, uint8_t nib, ref_t child) {
    ref_t ref = alloc_extension(m);
    if (ref == REF_NULL) return REF_NULL;

    mpt_extension_t *ext = ext_ptr(m, ref);
    ext->skip_len = 1;
    ext->child = child;
    ext->dirty = 1;
    set_nibble(ext->nibbles, 0, nib);
    return ref;
}

/* ========================================================================
 * Insert (recursive, no COW — all nodes mutable)
 * ======================================================================== */

static ref_t do_insert(mpt_t *m, ref_t ref, const uint8_t *key,
                       int depth, const uint8_t *value, uint8_t vlen,
                       bool *inserted) {
    /* Case 1: empty slot */
    if (ref == REF_NULL) {
        *inserted = true;
        return alloc_leaf(m, key, value, vlen);
    }

    /* Case 2: leaf */
    if (IS_LEAF(ref)) {
        mpt_leaf_t *l = leaf_ptr(m, ref);

        if (memcmp(l->key, key, MPT_KEY_SIZE) == 0) {
            /* Update existing */
            *inserted = false;
            l->vlen = vlen;
            if (vlen > 0 && value)
                memcpy(l->value, value, vlen);
            else
                memset(l->value, 0, MPT_MAX_VALUE);
            l->dirty = 1;
            return ref;
        }

        /* Different key — find diverging nibble */
        *inserted = true;
        uint8_t saved_key[MPT_KEY_SIZE];
        uint8_t saved_val[MPT_MAX_VALUE];
        uint8_t saved_vlen = l->vlen;
        memcpy(saved_key, l->key, MPT_KEY_SIZE);
        memcpy(saved_val, l->value, saved_vlen);

        int diff = depth;
        while (diff < MPT_NUM_NIBBLES &&
               key_nibble(key, diff) == key_nibble(saved_key, diff))
            diff++;

        /* Allocate new leaf for incoming key */
        ref_t new_leaf = alloc_leaf(m, key, value, vlen);
        if (new_leaf == REF_NULL) return ref;

        /* Re-allocate old leaf (it may have moved due to alloc) */
        ref_t old_leaf = alloc_leaf(m, saved_key, saved_val, saved_vlen);
        if (old_leaf == REF_NULL) return ref;

        /* Branch at divergence point */
        ref_t br = alloc_branch(m);
        if (br == REF_NULL) return ref;

        mpt_branch_t *b = branch_ptr(m, br);
        b->children[key_nibble(key, diff)] = new_leaf;
        b->children[key_nibble(saved_key, diff)] = old_leaf;
        b->dirty = 1;

        /* Wrap in extension for shared prefix */
        int prefix_len = diff - depth;
        if (prefix_len > 0)
            return make_ext_from_key(m, key, depth, prefix_len, br);
        return br;
    }

    /* Case 3: extension */
    if (IS_EXTENSION(ref)) {
        mpt_extension_t *ext = ext_ptr(m, ref);
        int skip = ext->skip_len;

        /* Compare extension nibbles against key */
        int match = 0;
        while (match < skip &&
               key_nibble(ext->nibbles, match) == key_nibble(key, depth + match))
            match++;

        if (match == skip) {
            /* Full match — recurse into child */
            ref_t old_child = ext->child;
            ref_t new_child = do_insert(m, old_child, key,
                                        depth + skip, value, vlen, inserted);
            /* Always mark dirty: branches are mutated in-place (same ref)
             * so we can't use ref comparison to detect changes. */
            ext_ptr(m, ref)->child = new_child;
            ext_ptr(m, ref)->dirty = 1;
            return ref;
        }

        /* Partial match — split extension */
        *inserted = true;

        /* Save extension data before allocs */
        uint8_t saved_nibbles[59];
        memcpy(saved_nibbles, ext->nibbles, 59);
        int saved_skip = skip;
        ref_t saved_child = ext->child;

        /* New leaf */
        ref_t new_leaf = alloc_leaf(m, key, value, vlen);
        if (new_leaf == REF_NULL) return ref;

        /* Branch at mismatch */
        ref_t br = alloc_branch(m);
        if (br == REF_NULL) return ref;

        uint8_t nib_key = key_nibble(key, depth + match);
        uint8_t nib_ext = key_nibble(saved_nibbles, match);

        /* Tail of old extension */
        int tail_len = saved_skip - match - 1;
        ref_t tail_ref;
        if (tail_len > 0) {
            tail_ref = make_ext_from_buf(m, saved_nibbles,
                                         match + 1, tail_len, saved_child);
            if (tail_ref == REF_NULL) return ref;
        } else {
            tail_ref = saved_child;
        }

        mpt_branch_t *b = branch_ptr(m, br);
        b->children[nib_key] = new_leaf;
        b->children[nib_ext] = tail_ref;
        b->dirty = 1;

        /* Prefix extension */
        if (match > 0)
            return make_ext_from_buf(m, saved_nibbles, 0, match, br);
        return br;
    }

    /* Case 4: branch */
    mpt_branch_t *b = branch_ptr(m, ref);
    uint8_t nib = key_nibble(key, depth);
    ref_t child = b->children[nib];

    ref_t new_child = do_insert(m, child, key, depth + 1, value, vlen, inserted);
    b = branch_ptr(m, ref);  /* re-resolve after allocs */
    b->children[nib] = new_child;
    b->dirty = 1;
    return ref;
}

/* ========================================================================
 * Delete (recursive)
 * ======================================================================== */

static ref_t do_delete(mpt_t *m, ref_t ref, const uint8_t *key,
                       int depth, bool *deleted) {
    if (ref == REF_NULL) {
        *deleted = false;
        return REF_NULL;
    }

    /* Leaf */
    if (IS_LEAF(ref)) {
        mpt_leaf_t *l = leaf_ptr(m, ref);
        if (memcmp(l->key, key, MPT_KEY_SIZE) == 0) {
            *deleted = true;
            return REF_NULL;
        }
        *deleted = false;
        return ref;
    }

    /* Extension */
    if (IS_EXTENSION(ref)) {
        mpt_extension_t *ext = ext_ptr(m, ref);
        int skip = ext->skip_len;

        for (int i = 0; i < skip; i++) {
            if (key_nibble(ext->nibbles, i) != key_nibble(key, depth + i)) {
                *deleted = false;
                return ref;
            }
        }

        ref_t old_child = ext->child;
        ref_t new_child = do_delete(m, old_child, key, depth + skip, deleted);
        if (!*deleted) return ref;

        if (new_child == REF_NULL)
            return REF_NULL;

        /* Child became leaf — drop extension, leaf moves up in depth */
        if (IS_LEAF(new_child)) {
            leaf_ptr(m, new_child)->dirty = 1;
            return new_child;
        }

        /* Child became extension — merge */
        if (IS_EXTENSION(new_child)) {
            ext = ext_ptr(m, ref);
            uint8_t our_nibs[59];
            memcpy(our_nibs, ext->nibbles, 59);
            int our_skip = ext->skip_len;

            mpt_extension_t *child_ext = ext_ptr(m, new_child);
            int child_skip = child_ext->skip_len;
            ref_t child_child = child_ext->child;
            uint8_t child_nibs[59];
            memcpy(child_nibs, child_ext->nibbles, 59);

            int total = our_skip + child_skip;
            uint8_t merged[MPT_NUM_NIBBLES / 2];
            memset(merged, 0, sizeof(merged));
            for (int i = 0; i < our_skip; i++)
                set_nibble(merged, i, key_nibble(our_nibs, i));
            for (int i = 0; i < child_skip; i++)
                set_nibble(merged, our_skip + i, key_nibble(child_nibs, i));
            return make_ext_from_buf(m, merged, 0, total, child_child);
        }

        /* Child still a branch — update extension */
        ext_ptr(m, ref)->child = new_child;
        ext_ptr(m, ref)->dirty = 1;
        return ref;
    }

    /* Branch */
    mpt_branch_t *b = branch_ptr(m, ref);
    uint8_t nib = key_nibble(key, depth);
    ref_t child = b->children[nib];

    if (child == REF_NULL) {
        *deleted = false;
        return ref;
    }

    ref_t new_child = do_delete(m, child, key, depth + 1, deleted);
    if (!*deleted) return ref;

    b = branch_ptr(m, ref);
    b->children[nib] = new_child;

    /* Count remaining children */
    int remaining = 0;
    int last_nib = -1;
    for (int i = 0; i < 16; i++) {
        if (b->children[i] != REF_NULL) {
            remaining++;
            last_nib = i;
        }
    }

    if (remaining == 0)
        return REF_NULL;

    if (remaining == 1) {
        ref_t sole = b->children[last_nib];

        /* Leaf floats up — depth changes, cached hash is stale */
        if (IS_LEAF(sole)) {
            leaf_ptr(m, sole)->dirty = 1;
            return sole;
        }

        /* Extension — prepend nibble */
        if (IS_EXTENSION(sole)) {
            mpt_extension_t *ce = ext_ptr(m, sole);
            int child_skip = ce->skip_len;
            ref_t child_child = ce->child;
            uint8_t child_nibs[59];
            memcpy(child_nibs, ce->nibbles, 59);

            int total = 1 + child_skip;
            uint8_t merged[MPT_NUM_NIBBLES / 2];
            memset(merged, 0, sizeof(merged));
            set_nibble(merged, 0, (uint8_t)last_nib);
            for (int i = 0; i < child_skip; i++)
                set_nibble(merged, 1 + i, key_nibble(child_nibs, i));
            return make_ext_from_buf(m, merged, 0, total, child_child);
        }

        /* Branch child — wrap in 1-nibble extension */
        return make_ext_single(m, (uint8_t)last_nib, sole);
    }

    /* >= 2 remaining */
    b->dirty = 1;
    return ref;
}

/* ========================================================================
 * MPT Hash Computation — Ethereum-compatible RLP + Keccak256
 * ======================================================================== */

/* node_ref: 32-byte hash or inline RLP (< 32 bytes) */
typedef struct {
    uint8_t data[32];
    uint8_t len;        /* 32=hash, 1-31=inline, 0=empty */
} node_ref_t;

/* Hex-Prefix encoding (Yellow Paper Appendix C) */
static void hex_prefix_encode(const uint8_t *nibbles, size_t count,
                              bool is_leaf,
                              uint8_t *out, size_t *out_len) {
    uint8_t flag = is_leaf ? 2 : 0;
    bool odd = (count % 2) != 0;
    size_t len = odd ? (count + 1) / 2 : count / 2 + 1;

    if (odd) {
        out[0] = ((flag | 1) << 4) | nibbles[0];
        for (size_t i = 1; i < count; i += 2)
            out[(i + 1) / 2] = (nibbles[i] << 4) | nibbles[i + 1];
    } else {
        out[0] = (flag << 4);
        for (size_t i = 0; i < count; i += 2)
            out[i / 2 + 1] = (nibbles[i] << 4) | nibbles[i + 1];
    }
    *out_len = len;
}

/* RLP encode a byte string into buf, return bytes written */
static size_t rlp_encode_string(const uint8_t *data, size_t len,
                                uint8_t *buf) {
    if (len == 1 && data[0] <= 0x7F) {
        buf[0] = data[0];
        return 1;
    }
    if (len <= 55) {
        buf[0] = (uint8_t)(0x80 + len);
        memcpy(buf + 1, data, len);
        return 1 + len;
    }
    /* Long string */
    size_t len_bytes = 0;
    size_t temp = len;
    while (temp > 0) { len_bytes++; temp >>= 8; }
    buf[0] = (uint8_t)(0xB7 + len_bytes);
    for (int i = (int)len_bytes - 1; i >= 0; i--)
        buf[1 + ((int)len_bytes - 1 - i)] = (uint8_t)((len >> (i * 8)) & 0xFF);
    memcpy(buf + 1 + len_bytes, data, len);
    return 1 + len_bytes + len;
}

/* Write RLP list header for given payload length, return header bytes */
static size_t rlp_list_header(size_t payload_len, uint8_t *buf) {
    if (payload_len <= 55) {
        buf[0] = (uint8_t)(0xC0 + payload_len);
        return 1;
    }
    size_t len_bytes = 0;
    size_t temp = payload_len;
    while (temp > 0) { len_bytes++; temp >>= 8; }
    buf[0] = (uint8_t)(0xF7 + len_bytes);
    for (int i = (int)len_bytes - 1; i >= 0; i--)
        buf[1 + ((int)len_bytes - 1 - i)] = (uint8_t)((payload_len >> (i * 8)) & 0xFF);
    return 1 + len_bytes;
}

/* Append child ref to payload buffer */
static size_t append_child_ref(uint8_t *buf, const node_ref_t *ref) {
    if (ref->len == 0) {
        buf[0] = 0x80;  /* empty string */
        return 1;
    }
    if (ref->len == 32) {
        buf[0] = 0xA0;  /* 32-byte string prefix */
        memcpy(buf + 1, ref->data, 32);
        return 33;
    }
    /* Inline RLP */
    memcpy(buf, ref->data, ref->len);
    return ref->len;
}

/* Make node_ref from RLP: hash if >= 32 bytes, inline otherwise */
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

/* ========================================================================
 * Recursive hash recomputation (dirty walk)
 * ======================================================================== */

static node_ref_t recompute(mpt_t *m, ref_t ref, int depth, bool force_hash);

/* Hash a leaf node */
static node_ref_t hash_leaf_node(mpt_t *m, ref_t ref, int depth,
                                 bool force_hash) {
    mpt_leaf_t *l = leaf_ptr(m, ref);
    if (!l->dirty) {
        node_ref_t r;
        memcpy(r.data, l->hash, 32);
        r.len = 32;
        return r;
    }

    /* remaining path nibbles */
    uint8_t nibbles[MPT_NUM_NIBBLES];
    key_to_nibbles(l->key, nibbles);
    size_t remaining = MPT_NUM_NIBBLES - depth;

    /* hex-prefix encode */
    uint8_t hp[33];
    size_t hp_len;
    hex_prefix_encode(nibbles + depth, remaining, true, hp, &hp_len);

    /* Build list payload: [hp_rlp, value_rlp] */
    uint8_t payload[256];
    size_t pos = 0;
    pos += rlp_encode_string(hp, hp_len, payload + pos);
    pos += rlp_encode_string(l->value, l->vlen, payload + pos);

    /* Wrap in list */
    uint8_t encoded[270];
    size_t hdr = rlp_list_header(pos, encoded);
    memcpy(encoded + hdr, payload, pos);
    size_t total = hdr + pos;

    node_ref_t result = make_node_ref(encoded, total, force_hash);

    /* Cache hash */
    if (result.len == 32)
        memcpy(l->hash, result.data, 32);
    l->dirty = 0;
    return result;
}

/* Hash a branch node */
static node_ref_t hash_branch_node(mpt_t *m, ref_t ref, int depth,
                                   bool force_hash) {
    mpt_branch_t *b = branch_ptr(m, ref);
    if (!b->dirty) {
        node_ref_t r;
        memcpy(r.data, b->hash, 32);
        r.len = 32;
        return r;
    }

    /* Compute child refs */
    node_ref_t children[16];
    for (int i = 0; i < 16; i++) {
        if (b->children[i] == REF_NULL) {
            children[i].len = 0;
        } else {
            children[i] = recompute(m, b->children[i], depth + 1, false);
            b = branch_ptr(m, ref);  /* re-resolve (recompute may alloc) */
        }
    }

    /* Build payload: 16 children + empty value */
    uint8_t payload[700];
    size_t pos = 0;
    for (int i = 0; i < 16; i++)
        pos += append_child_ref(payload + pos, &children[i]);
    payload[pos++] = 0x80;  /* empty value */

    uint8_t encoded[710];
    size_t hdr = rlp_list_header(pos, encoded);
    memcpy(encoded + hdr, payload, pos);
    size_t total = hdr + pos;

    node_ref_t result = make_node_ref(encoded, total, force_hash);

    b = branch_ptr(m, ref);
    if (result.len == 32)
        memcpy(b->hash, result.data, 32);
    b->dirty = 0;
    return result;
}

/* Hash an extension node */
static node_ref_t hash_ext_node(mpt_t *m, ref_t ref, int depth,
                                bool force_hash) {
    mpt_extension_t *ext = ext_ptr(m, ref);
    if (!ext->dirty) {
        node_ref_t r;
        memcpy(r.data, ext->hash, 32);
        r.len = 32;
        return r;
    }

    /* Unpack extension nibbles */
    uint8_t nibs[MAX_EXT_NIBBLES];
    for (int i = 0; i < ext->skip_len; i++)
        nibs[i] = key_nibble(ext->nibbles, i);

    /* Compute child ref */
    node_ref_t child_ref = recompute(m, ext->child,
                                     depth + ext->skip_len, false);

    /* hex-prefix encode (is_leaf = false) */
    ext = ext_ptr(m, ref);  /* re-resolve */
    uint8_t hp[60];
    size_t hp_len;
    hex_prefix_encode(nibs, ext->skip_len, false, hp, &hp_len);

    /* Build payload: [hp_rlp, child_ref] */
    uint8_t payload[128];
    size_t pos = 0;
    pos += rlp_encode_string(hp, hp_len, payload + pos);
    pos += append_child_ref(payload + pos, &child_ref);

    uint8_t encoded[140];
    size_t hdr = rlp_list_header(pos, encoded);
    memcpy(encoded + hdr, payload, pos);
    size_t total = hdr + pos;

    node_ref_t result = make_node_ref(encoded, total, force_hash);

    ext = ext_ptr(m, ref);
    if (result.len == 32)
        memcpy(ext->hash, result.data, 32);
    ext->dirty = 0;
    return result;
}

/* Main recompute dispatcher */
static node_ref_t recompute(mpt_t *m, ref_t ref, int depth, bool force_hash) {
    if (ref == REF_NULL) {
        node_ref_t r = {0};
        return r;
    }

    if (IS_LEAF(ref))
        return hash_leaf_node(m, ref, depth, force_hash);
    if (IS_BRANCH(ref))
        return hash_branch_node(m, ref, depth, force_hash);
    if (IS_EXTENSION(ref))
        return hash_ext_node(m, ref, depth, force_hash);

    node_ref_t r = {0};
    return r;
}

/* ========================================================================
 * Meta page read/write
 * ======================================================================== */

static bool meta_read(int fd, int page, mpt_meta_t *meta) {
    off_t offset = (page == 0) ? META_A_OFFSET : META_B_OFFSET;
    uint8_t buf[META_SIZE];

    ssize_t r = pread(fd, buf, META_SIZE, offset);
    if (r != META_SIZE) return false;

    memcpy(meta, buf, sizeof(mpt_meta_t));
    if (meta->magic != META_MAGIC) return false;

    uint32_t expected = compute_crc32c(meta, META_CRC_LEN);
    if (meta->crc32c != expected) return false;
    return true;
}

static bool meta_write(int fd, int page, const mpt_meta_t *meta) {
    off_t offset = (page == 0) ? META_A_OFFSET : META_B_OFFSET;
    uint8_t buf[META_SIZE];
    memset(buf, 0, META_SIZE);
    memcpy(buf, meta, sizeof(mpt_meta_t));

    ssize_t w = pwrite(fd, buf, META_SIZE, offset);
    return w == META_SIZE;
}

/* ========================================================================
 * Public API: Lifecycle
 * ======================================================================== */

mpt_t *mpt_create(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return NULL;

    if (ftruncate(fd, (off_t)TOTAL_FILE_SIZE) != 0) {
        close(fd);
        return NULL;
    }

    uint8_t *node_base = mmap(NULL, NODE_POOL_MAX,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd, DATA_OFFSET);
    if (node_base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    uint8_t *leaf_base = mmap(NULL, LEAF_POOL_MAX,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd, LEAF_POOL_OFFSET);
    if (leaf_base == MAP_FAILED) {
        munmap(node_base, NODE_POOL_MAX);
        close(fd);
        return NULL;
    }

    mpt_t *m = calloc(1, sizeof(mpt_t));
    if (!m) {
        munmap(leaf_base, LEAF_POOL_MAX);
        munmap(node_base, NODE_POOL_MAX);
        close(fd);
        return NULL;
    }

    m->fd = fd;
    m->node_base = node_base;
    m->leaf_base = leaf_base;
    m->root = REF_NULL;
    m->size = 0;
    m->node_count = 1;  /* skip index 0 (NULL) */
    m->leaf_count = 1;
    m->generation = 0;
    m->active_meta = 0;
    m->root_dirty = true;
    m->cached_root = hash_zero();

    /* Write initial meta */
    mpt_meta_t meta = {0};
    meta.magic = META_MAGIC;
    meta.generation = 0;
    meta.size = 0;
    meta.root = REF_NULL;
    meta.node_count = 1;
    meta.leaf_count = 1;
    meta.crc32c = compute_crc32c(&meta, META_CRC_LEN);
    meta_write(fd, 0, &meta);
    meta_write(fd, 1, &meta);
    fdatasync(fd);

    return m;
}

mpt_t *mpt_open(const char *path) {
    if (!path) return NULL;

    int fd = open(path, O_RDWR, 0644);
    if (fd < 0) return NULL;

    /* Read both meta pages, pick highest valid generation */
    mpt_meta_t meta_a = {0}, meta_b = {0};
    bool ok_a = meta_read(fd, 0, &meta_a);
    bool ok_b = meta_read(fd, 1, &meta_b);

    if (!ok_a && !ok_b) {
        close(fd);
        return NULL;
    }

    int active;
    mpt_meta_t *meta;
    if (ok_a && ok_b)
        active = (meta_b.generation > meta_a.generation) ? 1 : 0;
    else
        active = ok_a ? 0 : 1;
    meta = (active == 0) ? &meta_a : &meta_b;

    /* Ensure file size */
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < (off_t)TOTAL_FILE_SIZE) {
        if (ftruncate(fd, (off_t)TOTAL_FILE_SIZE) != 0) {
            close(fd);
            return NULL;
        }
    }

    uint8_t *node_base = mmap(NULL, NODE_POOL_MAX,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd, DATA_OFFSET);
    if (node_base == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    uint8_t *leaf_base = mmap(NULL, LEAF_POOL_MAX,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, fd, LEAF_POOL_OFFSET);
    if (leaf_base == MAP_FAILED) {
        munmap(node_base, NODE_POOL_MAX);
        close(fd);
        return NULL;
    }

    mpt_t *m = calloc(1, sizeof(mpt_t));
    if (!m) {
        munmap(leaf_base, LEAF_POOL_MAX);
        munmap(node_base, NODE_POOL_MAX);
        close(fd);
        return NULL;
    }

    m->fd = fd;
    m->node_base = node_base;
    m->leaf_base = leaf_base;
    m->root = meta->root;
    m->size = meta->size;
    m->node_count = meta->node_count;
    m->leaf_count = meta->leaf_count;
    m->generation = meta->generation;
    m->active_meta = active;
    m->root_dirty = false;  /* assume committed state has valid hashes */
    m->cached_root = hash_zero();

    return m;
}

void mpt_close(mpt_t *m) {
    if (!m) return;
    if (m->leaf_base && m->leaf_base != MAP_FAILED)
        munmap(m->leaf_base, LEAF_POOL_MAX);
    if (m->node_base && m->node_base != MAP_FAILED)
        munmap(m->node_base, NODE_POOL_MAX);
    if (m->fd >= 0)
        close(m->fd);
    free(m);
}

/* ========================================================================
 * Public API: Mutations
 * ======================================================================== */

bool mpt_put(mpt_t *m, const uint8_t key[MPT_KEY_SIZE],
             const uint8_t *value, uint8_t vlen) {
    if (!m || !key || vlen > MPT_MAX_VALUE) return false;
    bool inserted = false;
    m->root = do_insert(m, m->root, key, 0, value, vlen, &inserted);
    if (inserted) m->size++;
    m->root_dirty = true;
    return true;
}

bool mpt_delete(mpt_t *m, const uint8_t key[MPT_KEY_SIZE]) {
    if (!m || !key) return false;
    bool deleted = false;
    m->root = do_delete(m, m->root, key, 0, &deleted);
    if (deleted) {
        m->size--;
        m->root_dirty = true;
    }
    return deleted;
}

/* ========================================================================
 * Public API: Root Hash
 * ======================================================================== */

hash_t mpt_root(mpt_t *m) {
    if (!m) return hash_zero();

    if (m->root == REF_NULL) {
        /* Empty trie: keccak256(RLP("")) = keccak256(0x80) */
        uint8_t empty_rlp = 0x80;
        m->cached_root = hash_keccak256(&empty_rlp, 1);
        m->root_dirty = false;
        return m->cached_root;
    }

    /* Force hash at root (root is always hashed, never inlined) */
    node_ref_t root_ref = recompute(m, m->root, 0, true);
    (void)root_ref;  /* hash already cached in node */

    /* Read hash from root node */
    if (IS_LEAF(m->root))
        memcpy(m->cached_root.bytes, leaf_ptr(m, m->root)->hash, 32);
    else if (IS_BRANCH(m->root))
        memcpy(m->cached_root.bytes, branch_ptr(m, m->root)->hash, 32);
    else if (IS_EXTENSION(m->root))
        memcpy(m->cached_root.bytes, ext_ptr(m, m->root)->hash, 32);

    m->root_dirty = false;
    return m->cached_root;
}

/* ========================================================================
 * Public API: Persistence
 * ======================================================================== */

bool mpt_commit(mpt_t *m) {
    if (!m) return false;

    /* 1. Flush pool data */
    fdatasync(m->fd);

    /* 2. Write meta to inactive page */
    int inactive = 1 - m->active_meta;
    m->generation++;

    mpt_meta_t meta = {0};
    meta.magic = META_MAGIC;
    meta.generation = m->generation;
    meta.size = m->size;
    meta.root = m->root;
    meta.node_count = m->node_count;
    meta.leaf_count = m->leaf_count;
    meta.crc32c = compute_crc32c(&meta, META_CRC_LEN);

    if (!meta_write(m->fd, inactive, &meta))
        return false;

    /* 3. Flush meta */
    fdatasync(m->fd);

    /* 4. Flip active */
    m->active_meta = inactive;
    return true;
}

/* ========================================================================
 * Public API: Stats
 * ======================================================================== */

uint64_t mpt_size(const mpt_t *m) {
    return m ? m->size : 0;
}

uint32_t mpt_nodes(const mpt_t *m) {
    return m ? m->node_count : 0;
}

uint32_t mpt_leaves(const mpt_t *m) {
    return m ? m->leaf_count : 0;
}
