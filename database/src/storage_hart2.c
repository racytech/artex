/**
 * Storage Hart v2 — per-account ART backed by hart_pool slab allocator.
 *
 * Same ART algorithm as v1 (storage_hart.c), but:
 *   - Pool is hart_pool_t (anonymous mmap, slab chain per hart)
 *   - Refs are 64-bit pool byte offsets; bit 63 = leaf flag
 *   - Intra-hart per-size-class freelist (128 B step, 16 classes) replaces
 *     the 5 per-type heads. A freed node4 (~64 B → class 0) can be reused
 *     for a leaf (~80 B → class 0), killing the cross-type waste.
 *   - Slabs are never relocated; node refs stay valid for the node's life
 *   - No file I/O: hart_pool is MAP_ANONYMOUS; save/load is external
 *
 * Warming invariant: refs are reused after free+reuse → warming cache must
 * key on (resource_idx, hashed_key), not refs. This file does not touch
 * the warming cache; the invariant is a contract on callers.
 */

#define _GNU_SOURCE

#include "storage_hart2.h"
#include "hash.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <immintrin.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE         32
#define VALUE_SIZE       32
#define LEAF_SIZE        64           /* key[32] + value[32] */

#define NODE_DIRTY       0x01
#define NODE_4           0
#define NODE_16          1
#define NODE_48          2
#define NODE_256         3
#define NODE48_EMPTY     255

/* Ref encoding: 64-bit pool offset + leaf flag in bit 63. */
typedef uint64_t sh_ref_t;

#define SH_REF_NULL     ((sh_ref_t)0)
#define SH_LEAF_BIT     ((sh_ref_t)1 << 63)
#define SH_IS_LEAF(r)   (((r) & SH_LEAF_BIT) != 0)
#define SH_REF_OFF(r)   ((hart_pool_ref_t)((r) & ~SH_LEAF_BIT))

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* =========================================================================
 * Node structures
 * ========================================================================= */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  keys[4];
    sh_ref_t children[4];
    uint8_t  hash[32];
} sh_node4_t;    /* 4 + 4 + 32 + 32 = 72 B */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  keys[16];
    sh_ref_t children[16];
    uint8_t  hash[32];
} sh_node16_t;   /* 4 + 16 + 128 + 32 = 180 B */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    uint8_t  index[256];
    sh_ref_t children[48];
    uint8_t  hash[32];
} sh_node48_t;   /* 4 + 256 + 384 + 32 = 676 B */

typedef struct {
    uint8_t  type, num_children, flags, _pad;
    sh_ref_t children[256];
    uint8_t  hash[32];
} sh_node256_t;  /* 4 + 2048 + 32 = 2084 B */

static uint32_t node_size_of(int type) {
    switch (type) {
    case NODE_4:   return sizeof(sh_node4_t);
    case NODE_16:  return sizeof(sh_node16_t);
    case NODE_48:  return sizeof(sh_node48_t);
    case NODE_256: return sizeof(sh_node256_t);
    default:       return LEAF_SIZE;
    }
}

/* Per-type freelist head (same scheme as v1). type = -1 → leaf. */
static hart_pool_ref_t *sh_free_list_for_type(storage_hart_t *sh, int type) {
    switch (type) {
    case NODE_4:   return &sh->free_node4;
    case NODE_16:  return &sh->free_node16;
    case NODE_48:  return &sh->free_node48;
    case NODE_256: return &sh->free_node256;
    default:       return &sh->free_leaf;  /* -1 == leaf */
    }
}

/* =========================================================================
 * Allocation helpers
 * ========================================================================= */

/* Resolve a (non-leaf) ref to a pointer. Caller must not have a pool-grow
 * event pending since last resolution. */
static inline void *sh_ref_ptr(const hart_pool_t *pool, sh_ref_t ref) {
    return hart_pool_ptr(pool, SH_REF_OFF(ref));
}

static inline void sh_prefetch_ref(const hart_pool_t *pool, sh_ref_t ref) {
    if (ref != SH_REF_NULL) {
        const void *p = hart_pool_ptr(pool, SH_REF_OFF(ref));
        _mm_prefetch((const char *)p, _MM_HINT_T0);
    }
}

/* Try per-type freelist first, else allocate from the slab chain at the
 * type's natural size. type = -1 means leaf. Same-size-per-type guarantees
 * no aliasing on reuse — pop gives a slot of exactly the right size. */
static sh_ref_t sh_alloc_raw(hart_pool_t *pool, storage_hart_t *sh,
                             int type, bool is_leaf) {
    uint32_t sz = node_size_of(type);
    hart_pool_ref_t *head = sh_free_list_for_type(sh, type);
    if (*head != HART_POOL_REF_NULL) {
        hart_pool_ref_t off = *head;
        hart_pool_ref_t *slot = hart_pool_ptr(pool, off);
        *head = *slot;  /* pop: next ref lives in first 8 bytes of slot */
        memset(slot, 0, sz);
        sh_ref_t ref = (sh_ref_t)off;
        if (is_leaf) ref |= SH_LEAF_BIT;
        return ref;
    }

    uint32_t out_cap = 0;
    hart_pool_ref_t off = hart_pool_alloc(pool, &sh->slab, sz, &out_cap);
    if (off == HART_POOL_REF_NULL) return SH_REF_NULL;
    memset(hart_pool_ptr(pool, off), 0, sz);
    sh_ref_t ref = (sh_ref_t)off;
    if (is_leaf) ref |= SH_LEAF_BIT;
    return ref;
}

/* Push a freed node/leaf onto its type-specific freelist. */
static void sh_free_raw(hart_pool_t *pool, storage_hart_t *sh,
                        sh_ref_t ref, int type) {
    if (ref == SH_REF_NULL) return;
    hart_pool_ref_t off = SH_REF_OFF(ref);
    hart_pool_ref_t *head = sh_free_list_for_type(sh, type);
    hart_pool_ref_t *slot = hart_pool_ptr(pool, off);
    *slot = *head;
    *head = off;
}

static sh_ref_t sh_alloc_node(hart_pool_t *pool, storage_hart_t *sh, int type) {
    sh_ref_t ref = sh_alloc_raw(pool, sh, type, false);
    if (ref == SH_REF_NULL) return ref;
    uint8_t *p = sh_ref_ptr(pool, ref);
    p[0] = (uint8_t)type;
    p[2] = NODE_DIRTY;  /* new node is dirty */
    /* node48 wants index[] filled with NODE48_EMPTY */
    if (type == NODE_48) {
        sh_node48_t *nn = (sh_node48_t *)p;
        memset(nn->index, NODE48_EMPTY, sizeof(nn->index));
    }
    return ref;
}

static sh_ref_t sh_alloc_leaf(hart_pool_t *pool, storage_hart_t *sh,
                              const uint8_t key[32], const uint8_t value[32]) {
    sh_ref_t ref = sh_alloc_raw(pool, sh, -1 /* leaf */, true);
    if (ref == SH_REF_NULL) return ref;
    uint8_t *p = sh_ref_ptr(pool, ref);
    memcpy(p, key, KEY_SIZE);
    memcpy(p + KEY_SIZE, value, VALUE_SIZE);
    return ref;
}

/* =========================================================================
 * Node / leaf helpers
 * ========================================================================= */

static inline uint8_t sh_node_type(const void *n) {
    return *(const uint8_t *)n;
}

static inline void sh_mark_dirty(const hart_pool_t *pool, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return;
    ((uint8_t *)sh_ref_ptr(pool, ref))[2] |= NODE_DIRTY;
}

static inline bool sh_is_node_dirty(const hart_pool_t *pool, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return true;
    return (((const uint8_t *)sh_ref_ptr(pool, ref))[2] & NODE_DIRTY) != 0;
}

static inline void sh_clear_dirty(const hart_pool_t *pool, sh_ref_t ref) {
    if (SH_IS_LEAF(ref) || ref == SH_REF_NULL) return;
    ((uint8_t *)sh_ref_ptr(pool, ref))[2] &= ~NODE_DIRTY;
}

static inline const uint8_t *sh_node_hash_ptr(const void *n) {
    switch (sh_node_type(n)) {
    case NODE_4:   return ((const sh_node4_t *)n)->hash;
    case NODE_16:  return ((const sh_node16_t *)n)->hash;
    case NODE_48:  return ((const sh_node48_t *)n)->hash;
    case NODE_256: return ((const sh_node256_t *)n)->hash;
    default: return NULL;
    }
}

static inline uint8_t *sh_node_hash_mut(void *n) {
    switch (sh_node_type(n)) {
    case NODE_4:   return ((sh_node4_t *)n)->hash;
    case NODE_16:  return ((sh_node16_t *)n)->hash;
    case NODE_48:  return ((sh_node48_t *)n)->hash;
    case NODE_256: return ((sh_node256_t *)n)->hash;
    default: return NULL;
    }
}

static inline const uint8_t *sh_leaf_key(const hart_pool_t *pool, sh_ref_t ref) {
    return (const uint8_t *)sh_ref_ptr(pool, ref);
}

static inline const uint8_t *sh_leaf_value(const hart_pool_t *pool, sh_ref_t ref) {
    return (const uint8_t *)sh_ref_ptr(pool, ref) + KEY_SIZE;
}

static inline bool sh_leaf_matches(const hart_pool_t *pool, sh_ref_t ref,
                                   const uint8_t key[32]) {
    return memcmp(sh_leaf_key(pool, ref), key, KEY_SIZE) == 0;
}

/* =========================================================================
 * Child management
 * ========================================================================= */

static sh_ref_t *sh_find_child_ptr(const hart_pool_t *pool,
                                   sh_ref_t nref, uint8_t byte) {
    void *n = sh_ref_ptr(pool, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (nn->keys[i] == byte) return &nn->children[i];
        return NULL;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (nn->keys[i] == byte) return &nn->children[i];
        return NULL;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        if (idx == NODE48_EMPTY) return NULL;
        return &nn->children[idx];
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        if (nn->children[byte] == SH_REF_NULL) return NULL;
        return &nn->children[byte];
    }
    }
    return NULL;
}

static sh_ref_t sh_add_child(hart_pool_t *pool, storage_hart_t *sh,
                             sh_ref_t nref, uint8_t byte, sh_ref_t child) {
    void *n = sh_ref_ptr(pool, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        if (nn->num_children < 4) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos,
                    nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos,
                    (nn->num_children - pos) * sizeof(sh_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_16);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, nref);
        sh_node16_t *nn16 = sh_ref_ptr(pool, new_ref);
        memcpy(nn16->keys, nn->keys, 4);
        memcpy(nn16->children, nn->children, 4 * sizeof(sh_ref_t));
        nn16->num_children = 4;
        int pos = 0;
        while (pos < 4 && nn16->keys[pos] < byte) pos++;
        memmove(nn16->keys + pos + 1, nn16->keys + pos, 4 - pos);
        memmove(nn16->children + pos + 1, nn16->children + pos,
                (4 - pos) * sizeof(sh_ref_t));
        nn16->keys[pos] = byte;
        nn16->children[pos] = child;
        nn16->num_children = 5;
        sh_free_raw(pool, sh, nref, NODE_4);
        return new_ref;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        if (nn->num_children < 16) {
            int pos = 0;
            while (pos < nn->num_children && nn->keys[pos] < byte) pos++;
            memmove(nn->keys + pos + 1, nn->keys + pos,
                    nn->num_children - pos);
            memmove(nn->children + pos + 1, nn->children + pos,
                    (nn->num_children - pos) * sizeof(sh_ref_t));
            nn->keys[pos] = byte;
            nn->children[pos] = child;
            nn->num_children++;
            return nref;
        }
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_48);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, nref);
        sh_node48_t *nn48 = sh_ref_ptr(pool, new_ref);
        for (int i = 0; i < 16; i++) {
            nn48->index[nn->keys[i]] = (uint8_t)i;
            nn48->children[i] = nn->children[i];
        }
        nn48->index[byte] = 16;
        nn48->children[16] = child;
        nn48->num_children = 17;
        sh_free_raw(pool, sh, nref, NODE_16);
        return new_ref;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        if (nn->num_children < 48) {
            uint8_t slot = 0;
            for (; slot < 48; slot++)
                if (nn->children[slot] == SH_REF_NULL) break;
            nn->index[byte] = slot;
            nn->children[slot] = child;
            nn->num_children++;
            return nref;
        }
        sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_256);
        if (new_ref == SH_REF_NULL) return nref;
        nn = sh_ref_ptr(pool, nref);
        sh_node256_t *nn256 = sh_ref_ptr(pool, new_ref);
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY)
                nn256->children[i] = nn->children[nn->index[i]];
        }
        nn256->children[byte] = child;
        nn256->num_children = nn->num_children + 1;
        sh_free_raw(pool, sh, nref, NODE_48);
        return new_ref;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        nn->children[byte] = child;
        nn->num_children++;
        return nref;
    }
    }
    return nref;
}

/* =========================================================================
 * Insert / delete (recursive)
 * ========================================================================= */

static sh_ref_t sh_insert_recursive(hart_pool_t *pool, storage_hart_t *sh,
                                    sh_ref_t ref,
                                    const uint8_t key[32],
                                    const uint8_t value[32],
                                    size_t depth, bool *inserted) {
    if (ref == SH_REF_NULL) {
        *inserted = true;
        return sh_alloc_leaf(pool, sh, key, value);
    }

    if (SH_IS_LEAF(ref)) {
        if (sh_leaf_matches(pool, ref, key)) {
            memcpy((uint8_t *)sh_ref_ptr(pool, ref) + KEY_SIZE,
                   value, VALUE_SIZE);
            *inserted = false;
            return ref;
        }
        *inserted = true;
        sh_ref_t new_leaf = sh_alloc_leaf(pool, sh, key, value);
        if (new_leaf == SH_REF_NULL) return ref;

        const uint8_t *old_key = sh_leaf_key(pool, ref);
        size_t diff = depth;
        while (diff < KEY_SIZE && old_key[diff] == key[diff]) diff++;

        sh_ref_t new_node = sh_alloc_node(pool, sh, NODE_4);
        if (new_node == SH_REF_NULL) return ref;
        old_key = sh_leaf_key(pool, ref);
        sh_node4_t *nn = sh_ref_ptr(pool, new_node);
        uint8_t old_byte = old_key[diff];
        uint8_t new_byte = key[diff];
        if (old_byte < new_byte) {
            nn->keys[0] = old_byte; nn->children[0] = ref;
            nn->keys[1] = new_byte; nn->children[1] = new_leaf;
        } else {
            nn->keys[0] = new_byte; nn->children[0] = new_leaf;
            nn->keys[1] = old_byte; nn->children[1] = ref;
        }
        nn->num_children = 2;

        sh_ref_t result = new_node;
        for (size_t d = diff; d > depth; d--) {
            sh_ref_t wrapper = sh_alloc_node(pool, sh, NODE_4);
            if (wrapper == SH_REF_NULL) return ref;
            old_key = sh_leaf_key(pool, ref);
            sh_node4_t *w = sh_ref_ptr(pool, wrapper);
            w->keys[0] = old_key[d - 1];
            w->children[0] = result;
            w->num_children = 1;
            result = wrapper;
        }
        return result;
    }

    sh_mark_dirty(pool, ref);

    uint8_t byte = key[depth];
    sh_ref_t *child_ptr = sh_find_child_ptr(pool, ref, byte);

    if (child_ptr) {
        sh_ref_t old_child = *child_ptr;
        sh_prefetch_ref(pool, old_child);
        sh_ref_t new_child = sh_insert_recursive(pool, sh, old_child,
                                                  key, value, depth + 1,
                                                  inserted);
        if (new_child != old_child) {
            child_ptr = sh_find_child_ptr(pool, ref, byte);
            *child_ptr = new_child;
        }
    } else {
        *inserted = true;
        sh_ref_t leaf = sh_alloc_leaf(pool, sh, key, value);
        if (leaf != SH_REF_NULL)
            ref = sh_add_child(pool, sh, ref, byte, leaf);
    }
    return ref;
}

static sh_ref_t sh_remove_child(hart_pool_t *pool, storage_hart_t *sh,
                                sh_ref_t nref, uint8_t byte) {
    void *n = sh_ref_ptr(pool, nref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1],
                        nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(sh_ref_t));
                nn->num_children--;
                return nref;
            }
        }
        return nref;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            if (nn->keys[i] == byte) {
                memmove(&nn->keys[i], &nn->keys[i+1],
                        nn->num_children - i - 1);
                memmove(&nn->children[i], &nn->children[i+1],
                        (nn->num_children - i - 1) * sizeof(sh_ref_t));
                nn->num_children--;
                if (nn->num_children <= 4) {
                    sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_4);
                    if (new_ref == SH_REF_NULL) return nref;
                    nn = sh_ref_ptr(pool, nref);
                    sh_node4_t *n4 = sh_ref_ptr(pool, new_ref);
                    n4->num_children = nn->num_children;
                    memcpy(n4->keys, nn->keys, nn->num_children);
                    memcpy(n4->children, nn->children,
                           nn->num_children * sizeof(sh_ref_t));
                    sh_free_raw(pool, sh, nref, NODE_16);
                    return new_ref;
                }
                return nref;
            }
        }
        return nref;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        if (idx != NODE48_EMPTY) {
            nn->children[idx] = SH_REF_NULL;
            nn->index[byte] = NODE48_EMPTY;
            nn->num_children--;
            if (nn->num_children <= 16) {
                sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_16);
                if (new_ref == SH_REF_NULL) return nref;
                nn = sh_ref_ptr(pool, nref);
                sh_node16_t *n16 = sh_ref_ptr(pool, new_ref);
                n16->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->index[i] != NODE48_EMPTY) {
                        n16->keys[n16->num_children] = (uint8_t)i;
                        n16->children[n16->num_children] =
                            nn->children[nn->index[i]];
                        n16->num_children++;
                    }
                }
                sh_free_raw(pool, sh, nref, NODE_48);
                return new_ref;
            }
        }
        return nref;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        if (nn->children[byte] != SH_REF_NULL) {
            nn->children[byte] = SH_REF_NULL;
            nn->num_children--;
            if (nn->num_children <= 48) {
                sh_ref_t new_ref = sh_alloc_node(pool, sh, NODE_48);
                if (new_ref == SH_REF_NULL) return nref;
                nn = sh_ref_ptr(pool, nref);
                sh_node48_t *n48 = sh_ref_ptr(pool, new_ref);
                n48->num_children = 0;
                for (int i = 0; i < 256; i++) {
                    if (nn->children[i] != SH_REF_NULL) {
                        n48->index[i] = (uint8_t)n48->num_children;
                        n48->children[n48->num_children] = nn->children[i];
                        n48->num_children++;
                    }
                }
                sh_free_raw(pool, sh, nref, NODE_256);
                return new_ref;
            }
        }
        return nref;
    }
    }
    return nref;
}

static sh_ref_t sh_delete_recursive(hart_pool_t *pool, storage_hart_t *sh,
                                    sh_ref_t ref,
                                    const uint8_t key[32],
                                    size_t depth, bool *deleted) {
    if (ref == SH_REF_NULL) return SH_REF_NULL;

    if (SH_IS_LEAF(ref)) {
        if (sh_leaf_matches(pool, ref, key)) {
            *deleted = true;
            sh_free_raw(pool, sh, ref, -1);
            return SH_REF_NULL;
        }
        return ref;
    }

    uint8_t byte = key[depth];
    sh_ref_t *child_ptr = sh_find_child_ptr(pool, ref, byte);
    if (!child_ptr) return ref;

    sh_mark_dirty(pool, ref);

    sh_ref_t old_child = *child_ptr;
    sh_prefetch_ref(pool, old_child);
    sh_ref_t new_child = sh_delete_recursive(pool, sh, old_child,
                                              key, depth + 1, deleted);

    if (new_child != old_child) {
        if (new_child == SH_REF_NULL) {
            ref = sh_remove_child(pool, sh, ref, byte);
        } else {
            child_ptr = sh_find_child_ptr(pool, ref, byte);
            if (child_ptr) *child_ptr = new_child;
        }
    }

    if (!SH_IS_LEAF(ref)) {
        void *node = sh_ref_ptr(pool, ref);
        if (sh_node_type(node) == NODE_4) {
            sh_node4_t *n4 = (sh_node4_t *)node;
            if (n4->num_children == 0) {
                sh_free_raw(pool, sh, ref, NODE_4);
                return SH_REF_NULL;
            }
            if (n4->num_children == 1 && SH_IS_LEAF(n4->children[0]))
                return n4->children[0];
        }
    }
    return ref;
}

/* =========================================================================
 * Public API: get / put / del / clear / reserve
 * ========================================================================= */

bool storage_hart_get(const hart_pool_t *pool,
                      const storage_hart_t *sh,
                      const uint8_t key[32], uint8_t val[32]) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) return false;

    sh_ref_t ref = sh->root_ref;
    size_t depth = 0;
    while (ref != SH_REF_NULL) {
        if (SH_IS_LEAF(ref)) {
            if (sh_leaf_matches(pool, ref, key)) {
                memcpy(val, sh_leaf_value(pool, ref), VALUE_SIZE);
                return true;
            }
            return false;
        }
        sh_ref_t *child = sh_find_child_ptr(pool, ref, key[depth]);
        if (!child) return false;
        ref = *child;
        sh_prefetch_ref(pool, ref);
        depth++;
    }
    return false;
}

bool storage_hart_put(hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32], const uint8_t val[32]) {
    if (!pool || !sh || !key || !val) return false;

    bool inserted = false;
    sh->root_ref = sh_insert_recursive(pool, sh, sh->root_ref,
                                        key, val, 0, &inserted);
    if (inserted) sh->count++;
    return true;
}

void storage_hart_del(hart_pool_t *pool, storage_hart_t *sh,
                      const uint8_t key[32]) {
    if (!pool || !sh || !key) return;
    if (sh->root_ref == SH_REF_NULL) return;

    bool deleted = false;
    sh->root_ref = sh_delete_recursive(pool, sh, sh->root_ref,
                                        key, 0, &deleted);
    if (deleted && sh->count > 0) sh->count--;
}

void storage_hart_clear(hart_pool_t *pool, storage_hart_t *sh) {
    if (!pool || !sh) return;
    hart_pool_free_slabs(pool, &sh->slab);
    memset(sh, 0, sizeof(*sh));
}

void storage_hart_reserve(hart_pool_t *pool, storage_hart_t *sh,
                          uint32_t expected_entries) {
    /* In v2, slabs grow geometrically on demand. Nothing to pre-reserve.
     * Kept for API compatibility. */
    (void)pool; (void)sh; (void)expected_entries;
}

/* =========================================================================
 * MPT Root Hash — identical to v1 but pool type / ref type changed
 * ========================================================================= */

typedef struct { uint8_t data[1024]; size_t len; } rlp_buf_t;

static void rbuf_reset(rlp_buf_t *b) { b->len = 0; }

static bool rbuf_append(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (b->len + n > sizeof(b->data)) return false;
    if (n > 0) memcpy(b->data + b->len, d, n);
    b->len += n;
    return true;
}

static bool rbuf_encode_bytes(rlp_buf_t *b, const uint8_t *d, size_t n) {
    if (n == 1 && d[0] < 0x80) return rbuf_append(b, d, 1);
    if (n <= 55) {
        uint8_t pfx = 0x80 + (uint8_t)n;
        return rbuf_append(b, &pfx, 1) && (n > 0 ? rbuf_append(b, d, n) : true);
    }
    size_t ll = 1; size_t tmp = n;
    while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t hdr[6]; hdr[0] = 0xb7 + (uint8_t)ll;
    tmp = n;
    for (size_t i = ll; i > 0; i--) { hdr[i] = (uint8_t)(tmp & 0xFF); tmp >>= 8; }
    return rbuf_append(b, hdr, 1 + ll) && rbuf_append(b, d, n);
}

static bool rbuf_list_wrap(rlp_buf_t *out, const rlp_buf_t *payload) {
    if (payload->len <= 55) {
        uint8_t pfx = 0xc0 + (uint8_t)payload->len;
        return rbuf_append(out, &pfx, 1) &&
               rbuf_append(out, payload->data, payload->len);
    }
    size_t ll = 1; size_t tmp = payload->len;
    while (tmp > 255) { ll++; tmp >>= 8; }
    uint8_t pfx = 0xf7 + (uint8_t)ll;
    rbuf_append(out, &pfx, 1);
    uint8_t len_be[4]; size_t plen = payload->len;
    for (size_t i = ll; i > 0; i--) {
        len_be[i-1] = (uint8_t)(plen & 0xFF); plen >>= 8;
    }
    return rbuf_append(out, len_be, ll) &&
           rbuf_append(out, payload->data, payload->len);
}

static void sh_keccak(const uint8_t *data, size_t len, uint8_t out[32]) {
    hash_t h = hash_keccak256(data, len);
    memcpy(out, h.bytes, 32);
}

static size_t sh_hex_prefix_encode(const uint8_t *nibbles, size_t nib_len,
                                   bool is_leaf, uint8_t *out) {
    uint8_t prefix = (is_leaf ? 2 : 0) + (nib_len % 2 ? 1 : 0);
    size_t pos = 0;
    if (nib_len % 2) {
        out[pos++] = (prefix << 4) | nibbles[0];
        nibbles++; nib_len--;
    } else {
        out[pos++] = prefix << 4;
    }
    for (size_t i = 0; i < nib_len; i += 2)
        out[pos++] = (nibbles[i] << 4) | nibbles[i+1];
    return pos;
}

static size_t sh_hash_ref(hart_pool_t *pool, storage_hart_t *sh,
                          sh_ref_t ref, size_t byte_depth,
                          storage_hart_encode_t encode, void *ctx,
                          const uint8_t *nib_prefix, size_t nib_prefix_len,
                          uint8_t *rlp_out);

static size_t sh_rlp_to_hashref(const uint8_t *rlp, size_t rlp_len,
                                uint8_t *out) {
    if (rlp_len < 32) { memcpy(out, rlp, rlp_len); return rlp_len; }
    sh_keccak(rlp, rlp_len, out);
    return 32;
}

static size_t sh_encode_child_ref(const uint8_t *child_rlp, size_t child_len,
                                  uint8_t *out) {
    if (child_len == 0) { out[0] = 0x80; return 1; }
    if (child_len < 32) { memcpy(out, child_rlp, child_len); return child_len; }
    if (child_len == 32) {
        out[0] = 0xa0; memcpy(out + 1, child_rlp, 32); return 33;
    }
    out[0] = 0xa0; sh_keccak(child_rlp, child_len, out + 1); return 33;
}

static size_t sh_encode_leaf_node(hart_pool_t *pool, storage_hart_t *sh,
                                  sh_ref_t leaf_ref, size_t byte_depth,
                                  storage_hart_encode_t encode, void *ctx,
                                  const uint8_t *nib_prefix,
                                  size_t nib_prefix_len,
                                  uint8_t *rlp_out) {
    (void)sh;
    const uint8_t *key = sh_leaf_key(pool, leaf_ref);

    uint8_t path[128];
    size_t path_len = 0;
    if (nib_prefix_len > 0) {
        memcpy(path, nib_prefix, nib_prefix_len);
        path_len = nib_prefix_len;
    }
    for (size_t i = byte_depth; i < KEY_SIZE; i++) {
        path[path_len++] = (key[i] >> 4) & 0x0F;
        path[path_len++] =  key[i]       & 0x0F;
    }

    uint8_t hp[33];
    size_t hp_len = sh_hex_prefix_encode(path, path_len, true, hp);

    uint8_t value_rlp[256];
    uint32_t value_len = encode(key, sh_leaf_value(pool, leaf_ref),
                                value_rlp, ctx);
    if (value_len == 0) return 0;

    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_encode_bytes(&payload, value_rlp, value_len);

    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return sh_rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t sh_encode_extension(const uint8_t *nibbles, size_t nib_len,
                                  const uint8_t *child_rlp, size_t child_len,
                                  uint8_t *rlp_out) {
    uint8_t hp[33];
    size_t hp_len = sh_hex_prefix_encode(nibbles, nib_len, false, hp);
    uint8_t cref[33];
    size_t cref_len = sh_encode_child_ref(child_rlp, child_len, cref);
    rlp_buf_t payload; rbuf_reset(&payload);
    rbuf_encode_bytes(&payload, hp, hp_len);
    rbuf_append(&payload, cref, cref_len);
    rlp_buf_t encoded; rbuf_reset(&encoded);
    rbuf_list_wrap(&encoded, &payload);
    return sh_rlp_to_hashref(encoded.data, encoded.len, rlp_out);
}

static size_t sh_build_branch_rlp(uint8_t slots[16][33],
                                  uint8_t slot_lens[16],
                                  uint8_t *rlp_out) {
    uint8_t payload[600];
    size_t plen = 0;
    for (int i = 0; i < 16; i++) {
        if (slot_lens[i] == 0) {
            payload[plen++] = 0x80;
        } else if (slot_lens[i] < 32) {
            memcpy(payload + plen, slots[i], slot_lens[i]);
            plen += slot_lens[i];
        } else {
            payload[plen++] = 0xa0;
            memcpy(payload + plen, slots[i], 32);
            plen += 32;
        }
    }
    payload[plen++] = 0x80;

    uint8_t encoded[610];
    size_t elen;
    if (plen <= 55) {
        encoded[0] = 0xc0 + (uint8_t)plen;
        memcpy(encoded + 1, payload, plen);
        elen = 1 + plen;
    } else {
        size_t ll = 1; size_t tmp = plen;
        while (tmp > 255) { ll++; tmp >>= 8; }
        encoded[0] = 0xf7 + (uint8_t)ll;
        size_t p = plen;
        for (size_t i = ll; i > 0; i--) {
            encoded[i] = (uint8_t)(p & 0xFF); p >>= 8;
        }
        memcpy(encoded + 1 + ll, payload, plen);
        elen = 1 + ll + plen;
    }
    return sh_rlp_to_hashref(encoded, elen, rlp_out);
}

static size_t sh_hash_lo_group(hart_pool_t *pool, storage_hart_t *sh,
                               const uint8_t *lo_keys,
                               const sh_ref_t *lo_refs, int count,
                               size_t next_depth,
                               storage_hart_encode_t encode, void *ctx,
                               uint8_t *rlp_out) {
    uint8_t slots[16][33];
    uint8_t slot_lens[16] = {0};
    for (int j = 0; j < count; j++) {
        if (j + 1 < count)
            sh_prefetch_ref(pool, lo_refs[j + 1]);
        slot_lens[lo_keys[j]] = (uint8_t)
            sh_hash_ref(pool, sh, lo_refs[j], next_depth, encode, ctx,
                        NULL, 0, slots[lo_keys[j]]);
    }
    return sh_build_branch_rlp(slots, slot_lens, rlp_out);
}

static size_t sh_hash_ref(hart_pool_t *pool, storage_hart_t *sh,
                          sh_ref_t ref, size_t byte_depth,
                          storage_hart_encode_t encode, void *ctx,
                          const uint8_t *nib_prefix, size_t nib_prefix_len,
                          uint8_t *rlp_out) {
    if (ref == SH_REF_NULL) return 0;

    if (SH_IS_LEAF(ref))
        return sh_encode_leaf_node(pool, sh, ref, byte_depth, encode, ctx,
                                   nib_prefix, nib_prefix_len, rlp_out);

    if (!sh_is_node_dirty(pool, ref)) {
        const uint8_t *cached = sh_node_hash_ptr(sh_ref_ptr(pool, ref));
        if (cached) {
            if (nib_prefix_len > 0)
                return sh_encode_extension(nib_prefix, nib_prefix_len,
                                           cached, 32, rlp_out);
            memcpy(rlp_out, cached, 32);
            return 32;
        }
    }

    void *n = sh_ref_ptr(pool, ref);
    uint8_t lo_keys[16][16];
    sh_ref_t lo_refs[16][16];
    uint8_t gcounts[16] = {0};
    int nchildren = 0;

    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            uint8_t hi = nn->keys[i] >> 4, lo = nn->keys[i] & 0xF;
            int g = gcounts[hi]++;
            lo_keys[hi][g] = lo;
            lo_refs[hi][g] = nn->children[i];
        }
        nchildren = nn->num_children;
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++) {
            uint8_t hi = nn->keys[i] >> 4, lo = nn->keys[i] & 0xF;
            int g = gcounts[hi]++;
            lo_keys[hi][g] = lo;
            lo_refs[hi][g] = nn->children[i];
        }
        nchildren = nn->num_children;
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->index[i] != NODE48_EMPTY) {
                uint8_t hi = i >> 4, lo = i & 0xF;
                int g = gcounts[hi]++;
                lo_keys[hi][g] = lo;
                lo_refs[hi][g] = nn->children[nn->index[i]];
                nchildren++;
            }
        }
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++) {
            if (nn->children[i]) {
                uint8_t hi = i >> 4, lo = i & 0xF;
                int g = gcounts[hi]++;
                lo_keys[hi][g] = lo;
                lo_refs[hi][g] = nn->children[i];
                nchildren++;
            }
        }
        break;
    }
    }

    size_t next_depth = byte_depth + 1;
    int hi_occupied = 0, single_hi = -1;
    for (int i = 0; i < 16; i++) {
        if (gcounts[i] > 0) { hi_occupied++; single_hi = i; }
    }

    if (nchildren == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) {
            memcpy(pfx, nib_prefix, nib_prefix_len);
            pfx_len = nib_prefix_len;
        }
        pfx[pfx_len++] = (uint8_t)single_hi;
        pfx[pfx_len++] = lo_keys[single_hi][0];
        return sh_hash_ref(pool, sh, lo_refs[single_hi][0], next_depth,
                           encode, ctx, pfx, pfx_len, rlp_out);
    }
    if (hi_occupied == 1) {
        uint8_t pfx[128]; size_t pfx_len = 0;
        if (nib_prefix_len > 0) {
            memcpy(pfx, nib_prefix, nib_prefix_len);
            pfx_len = nib_prefix_len;
        }
        pfx[pfx_len++] = (uint8_t)single_hi;
        if (gcounts[single_hi] == 1) {
            pfx[pfx_len++] = lo_keys[single_hi][0];
            return sh_hash_ref(pool, sh, lo_refs[single_hi][0], next_depth,
                               encode, ctx, pfx, pfx_len, rlp_out);
        }
        uint8_t lo_hash[33];
        size_t lo_len = sh_hash_lo_group(pool, sh, lo_keys[single_hi],
                                          lo_refs[single_hi],
                                          gcounts[single_hi],
                                          next_depth, encode, ctx, lo_hash);
        return sh_encode_extension(pfx, pfx_len, lo_hash, lo_len, rlp_out);
    }

    uint8_t hi_slots[16][33];
    uint8_t hi_slot_lens[16] = {0};
    for (int hi = 0; hi < 16; hi++) {
        if (gcounts[hi] == 0) continue;
        for (int next = hi + 1; next < 16; next++) {
            if (gcounts[next] > 0) {
                for (int j = 0; j < gcounts[next]; j++)
                    sh_prefetch_ref(pool, lo_refs[next][j]);
                break;
            }
        }
        if (gcounts[hi] == 1) {
            uint8_t lo_nib = lo_keys[hi][0];
            hi_slot_lens[hi] = (uint8_t)
                sh_hash_ref(pool, sh, lo_refs[hi][0], next_depth,
                            encode, ctx, &lo_nib, 1, hi_slots[hi]);
        } else {
            hi_slot_lens[hi] = (uint8_t)
                sh_hash_lo_group(pool, sh, lo_keys[hi], lo_refs[hi],
                                 gcounts[hi], next_depth, encode, ctx,
                                 hi_slots[hi]);
        }
    }

    uint8_t node_hash[33];
    size_t node_hash_len = sh_build_branch_rlp(hi_slots, hi_slot_lens,
                                                node_hash);
    n = sh_ref_ptr(pool, ref);
    memcpy(sh_node_hash_mut(n), node_hash, 32);
    sh_clear_dirty(pool, ref);

    if (nib_prefix_len > 0)
        return sh_encode_extension(nib_prefix, nib_prefix_len,
                                   node_hash, node_hash_len, rlp_out);
    memcpy(rlp_out, node_hash, node_hash_len);
    return node_hash_len;
}

void storage_hart_root_hash(hart_pool_t *pool, storage_hart_t *sh,
                            storage_hart_encode_t encode,
                            void *ctx, uint8_t out[32]) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) {
        memcpy(out, EMPTY_ROOT, 32);
        return;
    }
    uint8_t rlp[1024];
    size_t len = sh_hash_ref(pool, sh, sh->root_ref, 0, encode, ctx,
                             NULL, 0, rlp);
    if (len == 32) {
        memcpy(out, rlp, 32);
    } else if (len > 0 && len < 32) {
        sh_keccak(rlp, len, out);
    } else {
        memcpy(out, EMPTY_ROOT, 32);
    }
}

/* =========================================================================
 * Invalidate / mark dirty
 * ========================================================================= */

static void sh_invalidate_recursive(const hart_pool_t *pool, sh_ref_t ref) {
    if (ref == SH_REF_NULL || SH_IS_LEAF(ref)) return;
    sh_mark_dirty(pool, ref);
    void *n = sh_ref_ptr(pool, ref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            sh_invalidate_recursive(pool, nn->children[i]);
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            sh_invalidate_recursive(pool, nn->children[i]);
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->index[i] != NODE48_EMPTY)
                sh_invalidate_recursive(pool, nn->children[nn->index[i]]);
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->children[i])
                sh_invalidate_recursive(pool, nn->children[i]);
        break;
    }
    }
}

void storage_hart_invalidate(hart_pool_t *pool, storage_hart_t *sh) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) return;
    sh_invalidate_recursive(pool, sh->root_ref);
}

void storage_hart_mark_dirty(hart_pool_t *pool, storage_hart_t *sh,
                             const uint8_t key[32]) {
    if (!pool || !sh || sh->root_ref == SH_REF_NULL) return;
    sh_ref_t ref = sh->root_ref;
    size_t depth = 0;
    while (ref != SH_REF_NULL) {
        if (SH_IS_LEAF(ref)) return;
        sh_mark_dirty(pool, ref);
        sh_ref_t *child = sh_find_child_ptr(pool, ref, key[depth]);
        if (!child) return;
        ref = *child;
        depth++;
    }
}

/* =========================================================================
 * Iteration
 * ========================================================================= */

static bool sh_foreach_recursive(const hart_pool_t *pool, sh_ref_t ref,
                                 storage_hart_iter_cb cb, void *ctx) {
    if (ref == SH_REF_NULL) return true;

    if (SH_IS_LEAF(ref)) {
        const uint8_t *key = sh_leaf_key(pool, ref);
        const uint8_t *val = sh_leaf_value(pool, ref);
        return cb(key, val, ctx);
    }

    void *n = sh_ref_ptr(pool, ref);
    switch (sh_node_type(n)) {
    case NODE_4: {
        sh_node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (!sh_foreach_recursive(pool, nn->children[i], cb, ctx))
                return false;
        break;
    }
    case NODE_16: {
        sh_node16_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (!sh_foreach_recursive(pool, nn->children[i], cb, ctx))
                return false;
        break;
    }
    case NODE_48: {
        sh_node48_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->index[i] != NODE48_EMPTY)
                if (!sh_foreach_recursive(pool, nn->children[nn->index[i]],
                                          cb, ctx))
                    return false;
        break;
    }
    case NODE_256: {
        sh_node256_t *nn = n;
        for (int i = 0; i < 256; i++)
            if (nn->children[i])
                if (!sh_foreach_recursive(pool, nn->children[i], cb, ctx))
                    return false;
        break;
    }
    }
    return true;
}

void storage_hart_foreach(const hart_pool_t *pool, const storage_hart_t *sh,
                          storage_hart_iter_cb cb, void *ctx) {
    if (!pool || !sh || !cb) return;
    if (sh->root_ref == SH_REF_NULL) return;
    sh_foreach_recursive(pool, sh->root_ref, cb, ctx);
}
