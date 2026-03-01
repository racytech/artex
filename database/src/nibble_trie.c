#include "../include/nibble_trie.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <nmmintrin.h>  /* SSE4.2 (CRC32C) */

/* ========================================================================
 * Constants
 * ======================================================================== */

#define NT_META_SIZE        4096
#define NT_META_A_OFFSET    0
#define NT_META_B_OFFSET    4096
#define NT_DATA_OFFSET      8192

#define NT_NODE_SLOT_SIZE   64
#define NT_NODE_POOL_MAX    (32ULL * 1024 * 1024 * 1024)  /* 32 GB */
#define NT_LEAF_POOL_OFFSET (NT_DATA_OFFSET + NT_NODE_POOL_MAX)
#define NT_LEAF_POOL_MAX    (32ULL * 1024 * 1024 * 1024)  /* 32 GB */
#define NT_TOTAL_FILE_SIZE  (NT_LEAF_POOL_OFFSET + NT_LEAF_POOL_MAX)

#define NT_META_MAGIC       0x4952545F4C424E55ULL  /* "UNBL_TRI" */

#define NT_KEY_SIZE         32
#define NT_NUM_NIBBLES      64  /* 32 bytes × 2 nibbles/byte */

/* ========================================================================
 * Node structures (all 64 bytes, in the node pool)
 * ======================================================================== */

typedef struct {
    uint32_t children[16];       /* index by nibble, 0 = empty */
} nt_branch_t;

typedef struct {
    uint8_t  skip_len;           /* number of nibbles (1-63) */
    uint8_t  nibbles[59];        /* packed 2/byte, high nibble first */
    uint32_t child;              /* ref to next node (always a branch) */
} nt_extension_t;

typedef union {
    nt_branch_t    branch;
    nt_extension_t extension;
    uint8_t        raw[NT_NODE_SLOT_SIZE];
} nt_node_slot_t;

_Static_assert(sizeof(nt_branch_t)    == 64, "branch must be 64 bytes");
_Static_assert(sizeof(nt_extension_t) == 64, "extension must be 64 bytes");
_Static_assert(sizeof(nt_node_slot_t) == 64, "node slot must be 64 bytes");

/* Leaf layout (in the leaf pool, variable size):
 *   [key: NT_KEY_SIZE bytes][value: value_size bytes]
 * Total: leaf_slot_size = (NT_KEY_SIZE + value_size + 7) & ~7
 */

/* ========================================================================
 * Meta page
 * ======================================================================== */

typedef struct {
    uint64_t magic;
    uint64_t generation;
    uint64_t size;               /* number of key-value pairs */
    uint32_t root;               /* root ref (with type bits) */
    uint32_t node_count;         /* node slots allocated */
    uint32_t leaf_count;         /* leaf slots allocated */
    uint32_t value_size;         /* value size in bytes */
    uint32_t crc32c;
    uint32_t _pad;
} nt_meta_t;

#define NT_META_CRC_LEN  (offsetof(nt_meta_t, crc32c))

/* ========================================================================
 * CRC32C (hardware-accelerated via SSE4.2)
 * ======================================================================== */

static uint32_t nt_crc32c(const void *data, size_t len) {
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

static inline uint8_t ext_nibble(const nt_extension_t *ext, int i) {
    return key_nibble(ext->nibbles, i);
}

static inline void set_nibble(uint8_t *data, int i, uint8_t val) {
    int byte_idx = i / 2;
    if (i & 1) {
        data[byte_idx] = (data[byte_idx] & 0xF0) | (val & 0x0F);
    } else {
        data[byte_idx] = (data[byte_idx] & 0x0F) | ((val & 0x0F) << 4);
    }
}

/* ========================================================================
 * Pointer resolution (separate pools)
 * ======================================================================== */

static inline nt_branch_t *branch_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return &((nt_node_slot_t *)(t->node_base +
             (size_t)idx * NT_NODE_SLOT_SIZE))->branch;
}

static inline nt_extension_t *ext_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return &((nt_node_slot_t *)(t->node_base +
             (size_t)idx * NT_NODE_SLOT_SIZE))->extension;
}

static inline uint8_t *leaf_key_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    return t->leaf_base + (size_t)idx * t->leaf_slot_size;
}

static inline void *leaf_value_ptr(const nibble_trie_t *t, nt_ref_t ref) {
    return leaf_key_ptr(t, ref) + NT_KEY_SIZE;
}

/* ========================================================================
 * Allocation (separate bump allocators, index 0 reserved for NULL)
 * ======================================================================== */

static uint32_t alloc_node_slot(nibble_trie_t *t) {
    uint32_t idx = t->node_count;
    if (idx > NT_INDEX_MASK) return 0;
    if ((size_t)(idx + 1) * NT_NODE_SLOT_SIZE > NT_NODE_POOL_MAX) return 0;

    t->node_count++;
    nt_node_slot_t *s = (nt_node_slot_t *)(t->node_base +
                          (size_t)idx * NT_NODE_SLOT_SIZE);
    memset(s, 0, NT_NODE_SLOT_SIZE);
    return idx;
}

static uint32_t alloc_leaf_slot(nibble_trie_t *t) {
    uint32_t idx = t->leaf_count;
    if (idx > NT_INDEX_MASK) return 0;
    if ((size_t)(idx + 1) * t->leaf_slot_size > NT_LEAF_POOL_MAX) return 0;

    t->leaf_count++;
    uint8_t *s = t->leaf_base + (size_t)idx * t->leaf_slot_size;
    memset(s, 0, t->leaf_slot_size);
    return idx;
}

static nt_ref_t alloc_leaf(nibble_trie_t *t,
                            const uint8_t *key, const void *value) {
    uint32_t idx = alloc_leaf_slot(t);
    if (idx == 0) return NT_REF_NULL;

    uint8_t *lk = t->leaf_base + (size_t)idx * t->leaf_slot_size;
    memcpy(lk, key, NT_KEY_SIZE);
    memcpy(lk + NT_KEY_SIZE, value, t->value_size);
    return NT_MAKE_LEAF_REF(idx);
}

static nt_ref_t alloc_branch(nibble_trie_t *t) {
    uint32_t idx = alloc_node_slot(t);
    if (idx == 0) return NT_REF_NULL;
    return NT_MAKE_BRANCH_REF(idx);
}

static nt_ref_t alloc_extension_raw(nibble_trie_t *t) {
    uint32_t idx = alloc_node_slot(t);
    if (idx == 0) return NT_REF_NULL;
    return NT_MAKE_EXTENSION_REF(idx);
}

/* ========================================================================
 * Dirty check: allocated after last commit → safe to mutate in place
 * ======================================================================== */

static inline bool is_dirty(const nibble_trie_t *t, nt_ref_t ref) {
    if (NT_IS_LEAF(ref))
        return NT_REF_INDEX(ref) >= t->committed_leaf_count;
    else
        return NT_REF_INDEX(ref) >= t->committed_node_count;
}

/* ========================================================================
 * Extension construction helpers
 * ======================================================================== */

/* Build extension from key nibbles at key[depth..depth+count-1] -> child */
static nt_ref_t make_extension_from_key(nibble_trie_t *t,
                                         const uint8_t *key, int depth,
                                         int count, nt_ref_t child) {
    if (count == 0) return child;

    nt_ref_t ref = alloc_extension_raw(t);
    if (ref == NT_REF_NULL) return NT_REF_NULL;

    nt_extension_t *ext = ext_ptr(t, ref);
    ext->skip_len = (uint8_t)count;
    ext->child = child;
    for (int i = 0; i < count; i++)
        set_nibble(ext->nibbles, i, key_nibble(key, depth + i));

    return ref;
}

/* Build extension from saved nibbles buffer at offset start for count -> child */
static nt_ref_t make_extension_from_buf(nibble_trie_t *t,
                                         const uint8_t *saved_nibbles,
                                         int start, int count,
                                         nt_ref_t child) {
    if (count == 0) return child;

    nt_ref_t ref = alloc_extension_raw(t);
    if (ref == NT_REF_NULL) return NT_REF_NULL;

    nt_extension_t *ext = ext_ptr(t, ref);
    ext->skip_len = (uint8_t)count;
    ext->child = child;
    for (int i = 0; i < count; i++)
        set_nibble(ext->nibbles, i, key_nibble(saved_nibbles, start + i));

    return ref;
}

/* Build 1-nibble extension: [nib] -> child */
static nt_ref_t make_extension_single(nibble_trie_t *t,
                                       uint8_t nib, nt_ref_t child) {
    nt_ref_t ref = alloc_extension_raw(t);
    if (ref == NT_REF_NULL) return NT_REF_NULL;

    nt_extension_t *ext = ext_ptr(t, ref);
    ext->skip_len = 1;
    ext->child = child;
    set_nibble(ext->nibbles, 0, nib);
    return ref;
}

/* ========================================================================
 * COW Insert (recursive)
 * ======================================================================== */

static nt_ref_t cow_insert(nibble_trie_t *t, nt_ref_t ref,
                            const uint8_t *key, int depth,
                            const void *value, bool *inserted) {
    /* Case 1: empty slot -> alloc leaf */
    if (ref == NT_REF_NULL) {
        *inserted = true;
        return alloc_leaf(t, key, value);
    }

    /* Case 2: leaf */
    if (NT_IS_LEAF(ref)) {
        uint8_t *existing_key = leaf_key_ptr(t, ref);

        if (memcmp(existing_key, key, NT_KEY_SIZE) == 0) {
            /* Update existing key */
            *inserted = false;
            if (is_dirty(t, ref)) {
                memcpy(leaf_value_ptr(t, ref), value, t->value_size);
                return ref;
            }
            return alloc_leaf(t, key, value);
        }

        /* Different key — find first differing nibble */
        *inserted = true;

        /* Save existing key before allocs */
        uint8_t saved_key[NT_KEY_SIZE];
        memcpy(saved_key, existing_key, NT_KEY_SIZE);

        int diff = depth;
        while (diff < NT_NUM_NIBBLES &&
               key_nibble(key, diff) == key_nibble(saved_key, diff))
            diff++;

        /* Allocate new leaf */
        nt_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == NT_REF_NULL) return ref;

        /* Allocate branch at the diff point */
        nt_ref_t br_ref = alloc_branch(t);
        if (br_ref == NT_REF_NULL) return ref;

        nt_branch_t *br = branch_ptr(t, br_ref);
        br->children[key_nibble(key, diff)] = new_leaf;
        br->children[key_nibble(saved_key, diff)] = ref;  /* old leaf */

        /* Wrap in extension for shared prefix */
        int prefix_len = diff - depth;
        if (prefix_len > 0)
            return make_extension_from_key(t, key, depth, prefix_len, br_ref);
        return br_ref;
    }

    /* Case 3: extension */
    if (NT_IS_EXTENSION(ref)) {
        nt_extension_t *ext = ext_ptr(t, ref);
        int skip = ext->skip_len;

        /* Compare extension nibbles against key */
        int match = 0;
        while (match < skip &&
               ext_nibble(ext, match) == key_nibble(key, depth + match))
            match++;

        if (match == skip) {
            /* Full match — recurse into child */
            nt_ref_t old_child = ext->child;
            nt_ref_t new_child = cow_insert(t, old_child, key,
                                             depth + skip, value, inserted);
            if (new_child == old_child) return ref;

            /* Dirty: mutate in place */
            if (is_dirty(t, ref)) {
                ext_ptr(t, ref)->child = new_child;
                return ref;
            }

            /* COW copy extension with updated child */
            ext = ext_ptr(t, ref);  /* re-resolve */
            nt_ref_t new_ext = alloc_extension_raw(t);
            if (new_ext == NT_REF_NULL) return ref;

            ext = ext_ptr(t, ref);  /* re-resolve */
            nt_extension_t *ne = ext_ptr(t, new_ext);
            memcpy(ne, ext, NT_NODE_SLOT_SIZE);
            ne->child = new_child;
            return new_ext;
        }

        /* Partial match — split extension */
        *inserted = true;

        /* Save extension data before allocs */
        uint8_t saved_nibbles[59];
        memcpy(saved_nibbles, ext->nibbles, 59);
        int saved_skip = skip;
        nt_ref_t saved_child = ext->child;

        /* Allocate new leaf for inserted key */
        nt_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == NT_REF_NULL) return ref;

        /* Create branch at mismatch point */
        nt_ref_t br_ref = alloc_branch(t);
        if (br_ref == NT_REF_NULL) return ref;

        uint8_t nib_key = key_nibble(key, depth + match);
        uint8_t nib_ext = key_nibble(saved_nibbles, match);

        /* Tail of old extension: nibbles[match+1..skip-1] -> saved_child */
        int tail_len = saved_skip - match - 1;
        nt_ref_t tail_ref;
        if (tail_len > 0) {
            tail_ref = make_extension_from_buf(t, saved_nibbles,
                                                match + 1, tail_len,
                                                saved_child);
            if (tail_ref == NT_REF_NULL) return ref;
        } else {
            tail_ref = saved_child;
        }

        /* Set branch children */
        nt_branch_t *br = branch_ptr(t, br_ref);  /* re-resolve */
        br->children[nib_key] = new_leaf;
        br->children[nib_ext] = tail_ref;

        /* Prefix extension: nibbles[0..match-1] -> branch */
        if (match > 0)
            return make_extension_from_buf(t, saved_nibbles, 0, match, br_ref);
        return br_ref;
    }

    /* Case 4: branch */
    nt_branch_t *br = branch_ptr(t, ref);
    uint8_t nib = key_nibble(key, depth);
    nt_ref_t child = br->children[nib];

    nt_ref_t new_child = cow_insert(t, child, key, depth + 1, value, inserted);
    if (new_child == child) return ref;

    /* Dirty: mutate in place */
    if (is_dirty(t, ref)) {
        branch_ptr(t, ref)->children[nib] = new_child;
        return ref;
    }

    /* COW copy branch with updated child */
    nt_ref_t new_ref = alloc_branch(t);
    if (new_ref == NT_REF_NULL) return ref;

    br = branch_ptr(t, ref);  /* re-resolve */
    nt_branch_t *nb = branch_ptr(t, new_ref);
    memcpy(nb, br, NT_NODE_SLOT_SIZE);
    nb->children[nib] = new_child;
    return new_ref;
}

/* ========================================================================
 * COW Delete (recursive)
 * ======================================================================== */

static nt_ref_t cow_delete(nibble_trie_t *t, nt_ref_t ref,
                            const uint8_t *key, int depth, bool *deleted) {
    /* Case 1: empty */
    if (ref == NT_REF_NULL) {
        *deleted = false;
        return NT_REF_NULL;
    }

    /* Case 2: leaf */
    if (NT_IS_LEAF(ref)) {
        if (memcmp(leaf_key_ptr(t, ref), key, NT_KEY_SIZE) == 0) {
            *deleted = true;
            return NT_REF_NULL;
        }
        *deleted = false;
        return ref;
    }

    /* Case 3: extension */
    if (NT_IS_EXTENSION(ref)) {
        nt_extension_t *ext = ext_ptr(t, ref);
        int skip = ext->skip_len;

        /* Check nibble match */
        for (int i = 0; i < skip; i++) {
            if (ext_nibble(ext, i) != key_nibble(key, depth + i)) {
                *deleted = false;
                return ref;
            }
        }

        /* Full match — recurse */
        nt_ref_t old_child = ext->child;
        nt_ref_t new_child = cow_delete(t, old_child, key,
                                         depth + skip, deleted);
        if (!*deleted) return ref;
        if (new_child == old_child) return ref;

        /* Child gone — extension dead too */
        if (new_child == NT_REF_NULL)
            return NT_REF_NULL;

        /* Child became leaf — drop extension, leaf floats up */
        if (NT_IS_LEAF(new_child))
            return new_child;

        /* Child became extension — merge */
        if (NT_IS_EXTENSION(new_child)) {
            ext = ext_ptr(t, ref);  /* re-resolve */
            uint8_t our_nibs[59];
            memcpy(our_nibs, ext->nibbles, 59);
            int our_skip = ext->skip_len;

            nt_extension_t *child_ext = ext_ptr(t, new_child);
            int child_skip = child_ext->skip_len;
            nt_ref_t child_child = child_ext->child;
            uint8_t child_nibs[59];
            memcpy(child_nibs, child_ext->nibbles, 59);

            int total = our_skip + child_skip;
            nt_ref_t merged = alloc_extension_raw(t);
            if (merged == NT_REF_NULL) return ref;

            nt_extension_t *me = ext_ptr(t, merged);
            me->skip_len = (uint8_t)total;
            me->child = child_child;
            for (int i = 0; i < our_skip; i++)
                set_nibble(me->nibbles, i, key_nibble(our_nibs, i));
            for (int i = 0; i < child_skip; i++)
                set_nibble(me->nibbles, our_skip + i,
                           key_nibble(child_nibs, i));
            return merged;
        }

        /* Child is still a branch — update extension */
        if (is_dirty(t, ref)) {
            ext_ptr(t, ref)->child = new_child;
            return ref;
        }

        ext = ext_ptr(t, ref);  /* re-resolve */
        nt_ref_t new_ext = alloc_extension_raw(t);
        if (new_ext == NT_REF_NULL) return ref;

        ext = ext_ptr(t, ref);  /* re-resolve */
        nt_extension_t *ne = ext_ptr(t, new_ext);
        memcpy(ne, ext, NT_NODE_SLOT_SIZE);
        ne->child = new_child;
        return new_ext;
    }

    /* Case 4: branch */
    nt_branch_t *br = branch_ptr(t, ref);
    uint8_t nib = key_nibble(key, depth);
    nt_ref_t child = br->children[nib];

    if (child == NT_REF_NULL) {
        *deleted = false;
        return ref;
    }

    nt_ref_t new_child = cow_delete(t, child, key, depth + 1, deleted);
    if (!*deleted) return ref;

    /* Count remaining children */
    br = branch_ptr(t, ref);  /* re-resolve */
    int remaining = 0;
    int last_nib = -1;
    for (int i = 0; i < 16; i++) {
        nt_ref_t c = (i == nib) ? new_child : br->children[i];
        if (c != NT_REF_NULL) {
            remaining++;
            last_nib = i;
        }
    }

    if (remaining == 0)
        return NT_REF_NULL;

    if (remaining == 1) {
        /* Collapse — only one child left */
        nt_ref_t sole = (last_nib == nib) ? new_child
                                          : br->children[last_nib];

        /* Leaf floats up */
        if (NT_IS_LEAF(sole))
            return sole;

        /* Extension — prepend this nibble */
        if (NT_IS_EXTENSION(sole)) {
            nt_extension_t *child_ext = ext_ptr(t, sole);
            int child_skip = child_ext->skip_len;
            nt_ref_t child_child = child_ext->child;
            uint8_t child_nibs[59];
            memcpy(child_nibs, child_ext->nibbles, 59);

            nt_ref_t merged = alloc_extension_raw(t);
            if (merged == NT_REF_NULL) return ref;

            nt_extension_t *me = ext_ptr(t, merged);
            me->skip_len = (uint8_t)(1 + child_skip);
            me->child = child_child;
            set_nibble(me->nibbles, 0, (uint8_t)last_nib);
            for (int i = 0; i < child_skip; i++)
                set_nibble(me->nibbles, 1 + i, key_nibble(child_nibs, i));
            return merged;
        }

        /* Branch — wrap in 1-nibble extension */
        return make_extension_single(t, (uint8_t)last_nib, sole);
    }

    /* >= 2 remaining — update branch */
    if (is_dirty(t, ref)) {
        branch_ptr(t, ref)->children[nib] = new_child;
        return ref;
    }

    nt_ref_t new_ref = alloc_branch(t);
    if (new_ref == NT_REF_NULL) return ref;

    br = branch_ptr(t, ref);  /* re-resolve */
    nt_branch_t *nb = branch_ptr(t, new_ref);
    memcpy(nb, br, NT_NODE_SLOT_SIZE);
    nb->children[nib] = new_child;
    return new_ref;
}

/* ========================================================================
 * Search (iterative)
 * ======================================================================== */

const void *nt_get(const nibble_trie_t *t, const uint8_t *key) {
    if (!t || !key) return NULL;

    nt_ref_t ref = t->root;
    int depth = 0;

    while (ref != NT_REF_NULL) {
        if (NT_IS_LEAF(ref)) {
            if (memcmp(leaf_key_ptr(t, ref), key, NT_KEY_SIZE) == 0)
                return leaf_value_ptr(t, ref);
            return NULL;
        }

        if (NT_IS_EXTENSION(ref)) {
            nt_extension_t *ext = ext_ptr(t, ref);
            for (int i = 0; i < ext->skip_len; i++) {
                if (ext_nibble(ext, i) != key_nibble(key, depth + i))
                    return NULL;
            }
            depth += ext->skip_len;
            ref = ext->child;
            continue;
        }

        /* Branch */
        nt_branch_t *br = branch_ptr(t, ref);
        uint8_t nib = key_nibble(key, depth);
        ref = br->children[nib];
        depth++;
    }

    return NULL;
}

/* ========================================================================
 * Public API
 * ======================================================================== */

bool nt_insert(nibble_trie_t *t, const uint8_t *key, const void *value) {
    if (!t || !key || !value) return false;
    bool inserted = false;
    t->root = cow_insert(t, t->root, key, 0, value, &inserted);
    if (inserted) t->size++;
    return true;
}

bool nt_delete(nibble_trie_t *t, const uint8_t *key) {
    if (!t || !key) return false;
    bool deleted = false;
    t->root = cow_delete(t, t->root, key, 0, &deleted);
    if (deleted) t->size--;
    return deleted;
}

bool nt_contains(const nibble_trie_t *t, const uint8_t *key) {
    return nt_get(t, key) != NULL;
}

size_t nt_size(const nibble_trie_t *t) {
    return t ? t->size : 0;
}

/* ========================================================================
 * Meta page read/write
 * ======================================================================== */

static bool meta_read(int fd, int page, nt_meta_t *meta) {
    off_t offset = (page == 0) ? NT_META_A_OFFSET : NT_META_B_OFFSET;
    uint8_t buf[NT_META_SIZE];

    ssize_t r = pread(fd, buf, NT_META_SIZE, offset);
    if (r != NT_META_SIZE) return false;

    memcpy(meta, buf, sizeof(nt_meta_t));
    if (meta->magic != NT_META_MAGIC) return false;

    uint32_t expected = nt_crc32c(meta, NT_META_CRC_LEN);
    if (meta->crc32c != expected) return false;

    return true;
}

static bool meta_write(int fd, int page, const nt_meta_t *meta) {
    off_t offset = (page == 0) ? NT_META_A_OFFSET : NT_META_B_OFFSET;
    uint8_t buf[NT_META_SIZE];
    memset(buf, 0, NT_META_SIZE);
    memcpy(buf, meta, sizeof(nt_meta_t));

    ssize_t w = pwrite(fd, buf, NT_META_SIZE, offset);
    return w == NT_META_SIZE;
}

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

bool nt_open(nibble_trie_t *t, const char *path, uint32_t value_size) {
    if (!t || !path || value_size == 0) return false;
    memset(t, 0, sizeof(*t));
    t->fd = -1;
    t->value_size = value_size;
    t->leaf_slot_size = (NT_KEY_SIZE + value_size + 7) & ~(uint32_t)7;

    bool created = false;
    int fd = open(path, O_RDWR, 0644);
    if (fd < 0) {
        fd = open(path, O_RDWR | O_CREAT, 0644);
        if (fd < 0) return false;
        created = true;
    }

    /* Ensure file is large enough (sparse) */
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < (off_t)NT_TOTAL_FILE_SIZE) {
        if (ftruncate(fd, (off_t)NT_TOTAL_FILE_SIZE) != 0) {
            close(fd);
            return false;
        }
    }

    /* mmap node pool */
    t->node_base = mmap(NULL, NT_NODE_POOL_MAX,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, NT_DATA_OFFSET);
    if (t->node_base == MAP_FAILED) {
        close(fd);
        return false;
    }

    /* mmap leaf pool */
    t->leaf_base = mmap(NULL, NT_LEAF_POOL_MAX,
                         PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd, NT_LEAF_POOL_OFFSET);
    if (t->leaf_base == MAP_FAILED) {
        munmap(t->node_base, NT_NODE_POOL_MAX);
        close(fd);
        return false;
    }

    t->fd = fd;

    if (created || file_size == 0) {
        /* Initialize new tree */
        t->root = NT_REF_NULL;
        t->size = 0;
        t->node_count = 1;     /* skip index 0 so ref=0 means NULL */
        t->leaf_count = 1;     /* skip index 0 so ref=0 means NULL */
        t->generation = 0;
        t->active_meta = 0;

        nt_meta_t meta = {0};
        meta.magic = NT_META_MAGIC;
        meta.generation = 1;
        meta.root = NT_REF_NULL;
        meta.size = 0;
        meta.node_count = 1;
        meta.leaf_count = 1;
        meta.value_size = value_size;
        meta.crc32c = nt_crc32c(&meta, NT_META_CRC_LEN);

        if (!meta_write(fd, 0, &meta)) {
            nt_close(t);
            return false;
        }
        if (fdatasync(fd) != 0) {
            nt_close(t);
            return false;
        }

        t->generation = 1;
        t->active_meta = 0;
    } else {
        /* Recovery: read both meta pages, pick highest valid generation */
        nt_meta_t meta_a = {0}, meta_b = {0};
        bool a_valid = meta_read(fd, 0, &meta_a);
        bool b_valid = meta_read(fd, 1, &meta_b);

        nt_meta_t *active = NULL;
        int active_page = -1;

        if (a_valid && b_valid) {
            if (meta_a.generation >= meta_b.generation) {
                active = &meta_a; active_page = 0;
            } else {
                active = &meta_b; active_page = 1;
            }
        } else if (a_valid) {
            active = &meta_a; active_page = 0;
        } else if (b_valid) {
            active = &meta_b; active_page = 1;
        } else {
            nt_close(t);
            return false;
        }

        /* Validate value_size matches */
        if (active->value_size != value_size) {
            nt_close(t);
            return false;
        }

        t->root = active->root;
        t->size = active->size;
        t->node_count = active->node_count;
        t->leaf_count = active->leaf_count;
        t->generation = active->generation;
        t->active_meta = active_page;
    }

    /* Snapshot committed state */
    t->committed_root = t->root;
    t->committed_size = t->size;
    t->committed_node_count = t->node_count;
    t->committed_leaf_count = t->leaf_count;

    return true;
}

void nt_close(nibble_trie_t *t) {
    if (!t) return;
    if (t->leaf_base && t->leaf_base != MAP_FAILED)
        munmap(t->leaf_base, NT_LEAF_POOL_MAX);
    if (t->node_base && t->node_base != MAP_FAILED)
        munmap(t->node_base, NT_NODE_POOL_MAX);
    if (t->fd >= 0) close(t->fd);
    memset(t, 0, sizeof(*t));
    t->fd = -1;
}

/* ========================================================================
 * Commit / Rollback
 * ======================================================================== */

bool nt_commit(nibble_trie_t *t) {
    if (!t || t->fd < 0) return false;

    /* Phase 1: flush all dirty mmap'd data pages */
    if (fdatasync(t->fd) != 0) return false;

    /* Phase 2: write meta to inactive page */
    int inactive = 1 - t->active_meta;
    t->generation++;

    nt_meta_t meta = {0};
    meta.magic = NT_META_MAGIC;
    meta.generation = t->generation;
    meta.root = t->root;
    meta.size = t->size;
    meta.node_count = t->node_count;
    meta.leaf_count = t->leaf_count;
    meta.value_size = t->value_size;
    meta.crc32c = nt_crc32c(&meta, NT_META_CRC_LEN);

    if (!meta_write(t->fd, inactive, &meta)) {
        t->generation--;
        return false;
    }

    /* Phase 3: flush meta page */
    if (fdatasync(t->fd) != 0) {
        t->generation--;
        return false;
    }

    /* Phase 4: update committed state */
    t->active_meta = inactive;
    t->committed_root = t->root;
    t->committed_size = t->size;
    t->committed_node_count = t->node_count;
    t->committed_leaf_count = t->leaf_count;

    return true;
}

void nt_rollback(nibble_trie_t *t) {
    if (!t) return;
    t->root = t->committed_root;
    t->size = t->committed_size;
    t->node_count = t->committed_node_count;
    t->leaf_count = t->committed_leaf_count;
}

/* ========================================================================
 * Iterator
 * ======================================================================== */

typedef struct {
    const uint8_t *key;
    const void *value;
    bool done;
    bool started;
    struct {
        nt_ref_t ref;       /* branch ref */
        int      nibble;    /* current nibble (0-15) */
    } stack[65];
    int depth;
} nt_iter_state_t;

struct nt_iterator {
    const nibble_trie_t *tree;
    nt_iter_state_t state;
};

/* Descend from ref to minimum (leftmost) leaf.
 * Pushes branches onto stack; extensions are transparent. */
static bool descend_to_min(const nibble_trie_t *t,
                            nt_iter_state_t *s, nt_ref_t ref) {
    while (ref != NT_REF_NULL) {
        if (NT_IS_LEAF(ref)) {
            s->key = leaf_key_ptr(t, ref);
            s->value = leaf_value_ptr(t, ref);
            return true;
        }

        if (NT_IS_EXTENSION(ref)) {
            ref = ext_ptr(t, ref)->child;
            continue;
        }

        /* Branch — push and descend to first non-NULL child */
        nt_branch_t *br = branch_ptr(t, ref);
        int first = -1;
        for (int n = 0; n < 16; n++) {
            if (br->children[n] != NT_REF_NULL) {
                first = n;
                break;
            }
        }
        if (first < 0) return false;

        s->depth++;
        if (s->depth >= 65) { s->done = true; return false; }
        s->stack[s->depth].ref = ref;
        s->stack[s->depth].nibble = first;

        ref = br->children[first];
    }
    return false;
}

nt_iterator_t *nt_iterator_create(const nibble_trie_t *t) {
    if (!t) return NULL;
    nt_iterator_t *it = calloc(1, sizeof(nt_iterator_t));
    if (!it) return NULL;
    it->tree = t;
    it->state.depth = -1;
    return it;
}

bool nt_iterator_next(nt_iterator_t *it) {
    if (!it) return false;
    nt_iter_state_t *s = &it->state;
    if (s->done) return false;

    if (!s->started) {
        s->started = true;
        if (it->tree->root == NT_REF_NULL) {
            s->done = true;
            return false;
        }
        return descend_to_min(it->tree, s, it->tree->root);
    }

    /* Advance: backtrack and find next child */
    while (s->depth >= 0) {
        nt_ref_t bref = s->stack[s->depth].ref;
        int *nib = &s->stack[s->depth].nibble;
        nt_branch_t *br = branch_ptr(it->tree, bref);

        for (int n = *nib + 1; n < 16; n++) {
            if (br->children[n] != NT_REF_NULL) {
                *nib = n;
                return descend_to_min(it->tree, s, br->children[n]);
            }
        }

        s->depth--;
    }

    s->done = true;
    return false;
}

const uint8_t *nt_iterator_key(const nt_iterator_t *it) {
    return it ? it->state.key : NULL;
}

const void *nt_iterator_value(const nt_iterator_t *it) {
    return it ? it->state.value : NULL;
}

bool nt_iterator_done(const nt_iterator_t *it) {
    return !it || it->state.done;
}

void nt_iterator_destroy(nt_iterator_t *it) {
    free(it);
}

/* ========================================================================
 * Iterator seek (lower-bound: first key >= target)
 * ======================================================================== */

bool nt_iterator_seek(nt_iterator_t *it, const uint8_t *key) {
    if (!it || !key) return false;

    const nibble_trie_t *t = it->tree;
    nt_iter_state_t *s = &it->state;

    memset(s, 0, sizeof(nt_iter_state_t));
    s->depth = -1;
    s->started = true;

    if (t->root == NT_REF_NULL) {
        s->done = true;
        return false;
    }

    nt_ref_t ref = t->root;
    int depth = 0;

    while (ref != NT_REF_NULL) {
        if (NT_IS_LEAF(ref)) {
            const uint8_t *lk = leaf_key_ptr(t, ref);
            if (memcmp(lk, key, NT_KEY_SIZE) >= 0) {
                s->key = lk;
                s->value = leaf_value_ptr(t, ref);
                return true;
            }
            goto backtrack;
        }

        if (NT_IS_EXTENSION(ref)) {
            nt_extension_t *ext = ext_ptr(t, ref);
            int skip = ext->skip_len;
            int cmp = 0;

            for (int i = 0; i < skip && cmp == 0; i++) {
                int en = ext_nibble(ext, i);
                int kn = key_nibble(key, depth + i);
                cmp = en - kn;
            }

            if (cmp > 0)
                return descend_to_min(t, s, ref);
            if (cmp < 0)
                goto backtrack;

            /* Full match */
            depth += skip;
            ref = ext->child;
            continue;
        }

        /* Branch */
        {
            nt_branch_t *br = branch_ptr(t, ref);
            uint8_t target = key_nibble(key, depth);

            int found = -1;
            for (int n = target; n < 16; n++) {
                if (br->children[n] != NT_REF_NULL) {
                    found = n;
                    break;
                }
            }

            if (found < 0) goto backtrack;

            s->depth++;
            if (s->depth >= 65) { s->done = true; return false; }
            s->stack[s->depth].ref = ref;
            s->stack[s->depth].nibble = found;

            if (found > target)
                return descend_to_min(t, s, br->children[found]);

            depth++;
            ref = br->children[found];
            continue;
        }

    backtrack:
        while (s->depth >= 0) {
            nt_ref_t pref = s->stack[s->depth].ref;
            int ci = s->stack[s->depth].nibble;
            nt_branch_t *pbr = branch_ptr(t, pref);

            for (int n = ci + 1; n < 16; n++) {
                if (pbr->children[n] != NT_REF_NULL) {
                    s->stack[s->depth].nibble = n;
                    return descend_to_min(t, s, pbr->children[n]);
                }
            }
            s->depth--;
        }

        s->done = true;
        return false;
    }

    s->done = true;
    return false;
}
