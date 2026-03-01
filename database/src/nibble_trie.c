#include "../include/nibble_trie.h"

#include <stdlib.h>
#include <string.h>

/* ========================================================================
 * Constants
 * ======================================================================== */

#define NT_BRANCH_SLOT_SIZE 64
#define NT_EXT_SLOT_SIZE    40
#define NT_INITIAL_BRANCHES 1024
#define NT_INITIAL_EXTS     1024
#define NT_INITIAL_LEAVES   1024

/* ========================================================================
 * Node structures
 * ======================================================================== */

typedef struct {
    uint32_t children[16];              /* 64B */
} nt_branch_t;

typedef struct {
    uint8_t  skip_len;                  /*  1B: nibble count (1-63) */
    uint8_t  nibbles[32];               /* 32B: packed nibbles (max 64) */
    uint8_t  _pad[3];                   /*  3B: alignment */
    uint32_t child;                     /*  4B: child ref */
} nt_extension_t;                       /* 40B total */

_Static_assert(sizeof(nt_branch_t)    == 64, "branch must be 64 bytes");
_Static_assert(sizeof(nt_extension_t) == 40, "extension must be 40 bytes");

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
 * Arena allocator (bump + intrusive free list)
 * ======================================================================== */

static bool arena_init(nt_arena_t *a, uint32_t slot_size, uint32_t initial_cap) {
    a->slot_size = slot_size;
    a->count = 1;       /* skip index 0 so ref=0 means NULL */
    a->capacity = initial_cap;
    a->free_head = 0;
    a->base = malloc((size_t)initial_cap * slot_size);
    if (!a->base) return false;
    memset(a->base, 0, (size_t)slot_size); /* zero slot 0 */
    return true;
}

static void arena_destroy(nt_arena_t *a) {
    free(a->base);
    memset(a, 0, sizeof(*a));
}

static void arena_clear(nt_arena_t *a) {
    a->count = 1;
    a->free_head = 0;
}

static uint32_t arena_alloc(nt_arena_t *a) {
    /* Try free list first */
    if (a->free_head != 0) {
        uint32_t idx = a->free_head;
        uint32_t *slot = (uint32_t *)(a->base + (size_t)idx * a->slot_size);
        a->free_head = *slot;
        memset(slot, 0, a->slot_size);
        return idx;
    }

    /* Bump allocate */
    if (a->count >= a->capacity) {
        uint32_t new_cap = a->capacity * 2;
        uint8_t *new_base = realloc(a->base, (size_t)new_cap * a->slot_size);
        if (!new_base) return 0;
        a->base = new_base;
        a->capacity = new_cap;
    }
    uint32_t idx = a->count++;
    memset(a->base + (size_t)idx * a->slot_size, 0, a->slot_size);
    return idx;
}

static void arena_free(nt_arena_t *a, uint32_t idx) {
    uint32_t *slot = (uint32_t *)(a->base + (size_t)idx * a->slot_size);
    *slot = a->free_head;
    a->free_head = idx;
}

static void arena_shrink_to_fit(nt_arena_t *a) {
    if (a->count >= a->capacity || a->count <= 1) return;
    uint8_t *new_base = realloc(a->base, (size_t)a->count * a->slot_size);
    if (new_base) {
        a->base = new_base;
        a->capacity = a->count;
    }
}

static bool arena_reserve(nt_arena_t *a, uint32_t min_capacity) {
    if (a->capacity >= min_capacity) return true;
    uint8_t *new_base = realloc(a->base, (size_t)min_capacity * a->slot_size);
    if (!new_base) return false;
    a->base = new_base;
    a->capacity = min_capacity;
    return true;
}

/* ========================================================================
 * Pointer resolution
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
 * Hash cache invalidation
 * ======================================================================== */

static inline void invalidate_hash(nibble_trie_t *t, nt_ref_t ref) {
    uint32_t idx = NT_REF_INDEX(ref);
    if (NT_IS_BRANCH(ref) && idx < t->branch_cache.capacity)
        t->branch_cache.entries[idx].len = 0;
    else if (NT_IS_EXTENSION(ref) && idx < t->ext_cache.capacity)
        t->ext_cache.entries[idx].len = 0;
    else if (NT_IS_LEAF(ref) && idx < t->leaf_cache.capacity)
        t->leaf_cache.entries[idx].len = 0;
}

/* ========================================================================
 * Allocation helpers
 * ======================================================================== */

static nt_ref_t alloc_leaf(nibble_trie_t *t,
                            const uint8_t *key, const void *value) {
    uint32_t idx = arena_alloc(&t->leaves);
    if (idx == 0) return NT_REF_NULL;

    uint8_t *lk = t->leaves.base + (size_t)idx * t->leaves.slot_size;
    memcpy(lk, key, NT_KEY_SIZE);
    memcpy(lk + NT_KEY_SIZE, value, t->value_size);
    nt_ref_t ref = NT_MAKE_LEAF_REF(idx);
    invalidate_hash(t, ref);  /* clear stale cache from recycled slot */
    return ref;
}

static nt_ref_t alloc_branch(nibble_trie_t *t) {
    uint32_t idx = arena_alloc(&t->branches);
    if (idx == 0) return NT_REF_NULL;
    nt_ref_t ref = NT_MAKE_BRANCH_REF(idx);
    invalidate_hash(t, ref);  /* clear stale cache from recycled slot */
    return ref;
}

static nt_ref_t alloc_extension_raw(nibble_trie_t *t) {
    uint32_t idx = arena_alloc(&t->extensions);
    if (idx == 0) return NT_REF_NULL;
    nt_ref_t ref = NT_MAKE_EXTENSION_REF(idx);
    invalidate_hash(t, ref);  /* clear stale cache from recycled slot */
    return ref;
}

/* ========================================================================
 * Extension construction helpers
 * ======================================================================== */

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
 * Insert (recursive, in-place mutation)
 * ======================================================================== */

static nt_ref_t do_insert(nibble_trie_t *t, nt_ref_t ref,
                           const uint8_t *key, int depth,
                           const void *value, bool *inserted) {
    /* Empty slot -> alloc leaf */
    if (ref == NT_REF_NULL) {
        *inserted = true;
        return alloc_leaf(t, key, value);
    }

    /* Leaf */
    if (NT_IS_LEAF(ref)) {
        uint8_t *existing_key = leaf_key_ptr(t, ref);

        if (memcmp(existing_key, key, NT_KEY_SIZE) == 0) {
            *inserted = false;
            memcpy(leaf_value_ptr(t, ref), value, t->value_size);
            invalidate_hash(t, ref);
            return ref;
        }

        *inserted = true;

        uint8_t saved_key[NT_KEY_SIZE];
        memcpy(saved_key, existing_key, NT_KEY_SIZE);

        int diff = depth;
        while (diff < NT_MAX_NIBBLES &&
               key_nibble(key, diff) == key_nibble(saved_key, diff))
            diff++;

        nt_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == NT_REF_NULL) return ref;

        nt_ref_t br_ref = alloc_branch(t);
        if (br_ref == NT_REF_NULL) return ref;

        nt_branch_t *br = branch_ptr(t, br_ref);
        br->children[key_nibble(key, diff)] = new_leaf;
        br->children[key_nibble(saved_key, diff)] = ref;
        invalidate_hash(t, ref);  /* leaf moved deeper — depth-dependent hash */

        int prefix_len = diff - depth;
        if (prefix_len > 0)
            return make_extension_from_key(t, key, depth, prefix_len, br_ref);
        return br_ref;
    }

    /* Extension */
    if (NT_IS_EXTENSION(ref)) {
        nt_extension_t *ext = ext_ptr(t, ref);
        int skip = ext->skip_len;

        int match = 0;
        while (match < skip &&
               ext_nibble(ext, match) == key_nibble(key, depth + match))
            match++;

        if (match == skip) {
            nt_ref_t old_child = ext->child;
            nt_ref_t new_child = do_insert(t, old_child, key,
                                            depth + skip, value, inserted);
            if (new_child != old_child)
                ext_ptr(t, ref)->child = new_child;
            invalidate_hash(t, ref);
            return ref;
        }

        /* Partial match — split */
        *inserted = true;

        uint8_t saved_nibbles[32];
        memcpy(saved_nibbles, ext->nibbles, 32);
        int saved_skip = skip;
        nt_ref_t saved_child = ext->child;

        /* Free the old extension slot being split */
        arena_free(&t->extensions, NT_REF_INDEX(ref));

        nt_ref_t new_leaf = alloc_leaf(t, key, value);
        if (new_leaf == NT_REF_NULL) return ref;

        nt_ref_t br_ref = alloc_branch(t);
        if (br_ref == NT_REF_NULL) return ref;

        uint8_t nib_key = key_nibble(key, depth + match);
        uint8_t nib_ext = key_nibble(saved_nibbles, match);

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

        nt_branch_t *br = branch_ptr(t, br_ref);
        br->children[nib_key] = new_leaf;
        br->children[nib_ext] = tail_ref;

        if (match > 0)
            return make_extension_from_buf(t, saved_nibbles, 0, match, br_ref);
        return br_ref;
    }

    /* Branch */
    nt_branch_t *br = branch_ptr(t, ref);
    uint8_t nib = key_nibble(key, depth);
    nt_ref_t child = br->children[nib];

    nt_ref_t new_child = do_insert(t, child, key, depth + 1, value, inserted);
    branch_ptr(t, ref)->children[nib] = new_child;
    invalidate_hash(t, ref);
    return ref;
}

/* ========================================================================
 * Delete (recursive, in-place mutation)
 * ======================================================================== */

static nt_ref_t do_delete(nibble_trie_t *t, nt_ref_t ref,
                           const uint8_t *key, int depth, bool *deleted) {
    if (ref == NT_REF_NULL) {
        *deleted = false;
        return NT_REF_NULL;
    }

    /* Leaf */
    if (NT_IS_LEAF(ref)) {
        if (memcmp(leaf_key_ptr(t, ref), key, NT_KEY_SIZE) == 0) {
            *deleted = true;
            arena_free(&t->leaves, NT_REF_INDEX(ref));
            return NT_REF_NULL;
        }
        *deleted = false;
        return ref;
    }

    /* Extension */
    if (NT_IS_EXTENSION(ref)) {
        nt_extension_t *ext = ext_ptr(t, ref);
        int skip = ext->skip_len;

        for (int i = 0; i < skip; i++) {
            if (ext_nibble(ext, i) != key_nibble(key, depth + i)) {
                *deleted = false;
                return ref;
            }
        }

        nt_ref_t old_child = ext->child;
        nt_ref_t new_child = do_delete(t, old_child, key,
                                        depth + skip, deleted);
        if (!*deleted) return ref;
        if (new_child == old_child) {
            invalidate_hash(t, ref);
            return ref;
        }

        if (new_child == NT_REF_NULL) {
            arena_free(&t->extensions, NT_REF_INDEX(ref));
            return NT_REF_NULL;
        }

        if (NT_IS_LEAF(new_child)) {
            arena_free(&t->extensions, NT_REF_INDEX(ref));
            invalidate_hash(t, new_child);  /* leaf moved up — depth-dependent hash */
            return new_child;
        }

        /* Child became extension — merge */
        if (NT_IS_EXTENSION(new_child)) {
            ext = ext_ptr(t, ref);
            uint8_t tmp[NT_MAX_NIBBLES];
            int our_skip = ext->skip_len;
            for (int i = 0; i < our_skip; i++)
                tmp[i] = ext_nibble(ext, i);

            nt_extension_t *child_ext = ext_ptr(t, new_child);
            int child_skip = child_ext->skip_len;
            nt_ref_t child_child = child_ext->child;
            for (int i = 0; i < child_skip; i++)
                tmp[our_skip + i] = ext_nibble(child_ext, i);

            int total = our_skip + child_skip;

            /* Pack tmp nibbles into a byte buffer */
            uint8_t packed[(NT_MAX_NIBBLES + 1) / 2];
            memset(packed, 0, sizeof(packed));
            for (int i = 0; i < total; i++)
                set_nibble(packed, i, tmp[i]);

            /* Free both old extensions before allocating new one */
            arena_free(&t->extensions, NT_REF_INDEX(ref));
            arena_free(&t->extensions, NT_REF_INDEX(new_child));

            return make_extension_from_buf(t, packed, 0, total, child_child);
        }

        /* Child is still a branch — update */
        ext_ptr(t, ref)->child = new_child;
        invalidate_hash(t, ref);
        return ref;
    }

    /* Branch */
    nt_branch_t *br = branch_ptr(t, ref);
    uint8_t nib = key_nibble(key, depth);
    nt_ref_t child = br->children[nib];

    if (child == NT_REF_NULL) {
        *deleted = false;
        return ref;
    }

    nt_ref_t new_child = do_delete(t, child, key, depth + 1, deleted);
    if (!*deleted) return ref;

    /* Count remaining children */
    br = branch_ptr(t, ref);
    int remaining = 0;
    int last_nib = -1;
    for (int i = 0; i < 16; i++) {
        nt_ref_t c = (i == nib) ? new_child : br->children[i];
        if (c != NT_REF_NULL) {
            remaining++;
            last_nib = i;
        }
    }

    if (remaining == 0) {
        arena_free(&t->branches, NT_REF_INDEX(ref));
        return NT_REF_NULL;
    }

    if (remaining == 1) {
        nt_ref_t sole = (last_nib == nib) ? new_child
                                          : br->children[last_nib];

        if (NT_IS_LEAF(sole)) {
            arena_free(&t->branches, NT_REF_INDEX(ref));
            invalidate_hash(t, sole);  /* leaf moved up — depth-dependent hash */
            return sole;
        }

        /* Extension — prepend this nibble */
        if (NT_IS_EXTENSION(sole)) {
            nt_extension_t *child_ext = ext_ptr(t, sole);
            int child_skip = child_ext->skip_len;
            nt_ref_t child_child = child_ext->child;

            uint8_t tmp[NT_MAX_NIBBLES];
            tmp[0] = (uint8_t)last_nib;
            for (int i = 0; i < child_skip; i++)
                tmp[1 + i] = ext_nibble(child_ext, i);

            int total = 1 + child_skip;
            uint8_t packed[(NT_MAX_NIBBLES + 1) / 2];
            memset(packed, 0, sizeof(packed));
            for (int i = 0; i < total; i++)
                set_nibble(packed, i, tmp[i]);

            arena_free(&t->branches, NT_REF_INDEX(ref));
            arena_free(&t->extensions, NT_REF_INDEX(sole));

            return make_extension_from_buf(t, packed, 0, total, child_child);
        }

        /* Branch — wrap in 1-nibble extension */
        arena_free(&t->branches, NT_REF_INDEX(ref));
        return make_extension_single(t, (uint8_t)last_nib, sole);
    }

    /* >= 2 remaining — update branch */
    branch_ptr(t, ref)->children[nib] = new_child;
    invalidate_hash(t, ref);
    return ref;
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

bool nt_init(nibble_trie_t *t, uint32_t value_size) {
    if (!t || value_size == 0) return false;
    memset(t, 0, sizeof(*t));
    t->value_size = value_size;

    uint32_t leaf_slot_size = (NT_KEY_SIZE + value_size + 7) & ~(uint32_t)7;

    if (!arena_init(&t->branches, NT_BRANCH_SLOT_SIZE, NT_INITIAL_BRANCHES))
        return false;
    if (!arena_init(&t->extensions, NT_EXT_SLOT_SIZE, NT_INITIAL_EXTS)) {
        arena_destroy(&t->branches);
        return false;
    }
    if (!arena_init(&t->leaves, leaf_slot_size, NT_INITIAL_LEAVES)) {
        arena_destroy(&t->branches);
        arena_destroy(&t->extensions);
        return false;
    }

    return true;
}

void nt_destroy(nibble_trie_t *t) {
    if (!t) return;
    arena_destroy(&t->branches);
    arena_destroy(&t->extensions);
    arena_destroy(&t->leaves);
    free(t->branch_cache.entries);
    free(t->ext_cache.entries);
    free(t->leaf_cache.entries);
    memset(t, 0, sizeof(*t));
}

void nt_clear(nibble_trie_t *t) {
    if (!t) return;
    arena_clear(&t->branches);
    arena_clear(&t->extensions);
    arena_clear(&t->leaves);
    if (t->branch_cache.entries)
        memset(t->branch_cache.entries, 0, t->branch_cache.capacity * sizeof(nt_node_hash_t));
    if (t->ext_cache.entries)
        memset(t->ext_cache.entries, 0, t->ext_cache.capacity * sizeof(nt_node_hash_t));
    if (t->leaf_cache.entries)
        memset(t->leaf_cache.entries, 0, t->leaf_cache.capacity * sizeof(nt_node_hash_t));
    t->root = NT_REF_NULL;
    t->size = 0;
}

static void hash_cache_shrink(nt_hash_cache_t *c, uint32_t count) {
    if (count == 0) {
        free(c->entries);
        c->entries = NULL;
        c->capacity = 0;
    } else if (count < c->capacity) {
        nt_node_hash_t *new_entries = realloc(c->entries, count * sizeof(nt_node_hash_t));
        if (new_entries) {
            c->entries = new_entries;
            c->capacity = count;
        }
    }
}

void nt_shrink_to_fit(nibble_trie_t *t) {
    if (!t) return;
    arena_shrink_to_fit(&t->branches);
    arena_shrink_to_fit(&t->extensions);
    arena_shrink_to_fit(&t->leaves);
    hash_cache_shrink(&t->branch_cache, t->branches.count);
    hash_cache_shrink(&t->ext_cache, t->extensions.count);
    hash_cache_shrink(&t->leaf_cache, t->leaves.count);
}

static bool hash_cache_reserve(nt_hash_cache_t *c, uint32_t cap) {
    if (cap <= c->capacity) return true;
    nt_node_hash_t *new_entries = calloc(cap, sizeof(nt_node_hash_t));
    if (!new_entries) return false;
    if (c->entries) {
        memcpy(new_entries, c->entries, c->capacity * sizeof(nt_node_hash_t));
        free(c->entries);
    }
    c->entries = new_entries;
    c->capacity = cap;
    return true;
}

bool nt_reserve(nibble_trie_t *t, size_t expected_keys) {
    if (!t) return false;

    uint32_t leaf_cap = (uint32_t)(expected_keys + 1);  /* +1 for sentinel */
    /* Heuristic: ~keys/5 branches and ~keys/5 extensions for random keys */
    uint32_t internal_cap = (uint32_t)(expected_keys / 5 + 1);

    if (!arena_reserve(&t->branches, internal_cap)) return false;
    if (!arena_reserve(&t->extensions, internal_cap)) return false;
    if (!arena_reserve(&t->leaves, leaf_cap)) return false;
    if (!hash_cache_reserve(&t->branch_cache, internal_cap)) return false;
    if (!hash_cache_reserve(&t->ext_cache, internal_cap)) return false;
    if (!hash_cache_reserve(&t->leaf_cache, leaf_cap)) return false;
    return true;
}

bool nt_insert(nibble_trie_t *t, const uint8_t *key, const void *value) {
    if (!t || !key || !value) return false;
    bool inserted = false;
    t->root = do_insert(t, t->root, key, 0, value, &inserted);
    if (inserted) t->size++;
    return true;
}

bool nt_delete(nibble_trie_t *t, const uint8_t *key) {
    if (!t || !key) return false;
    bool deleted = false;
    t->root = do_delete(t, t->root, key, 0, &deleted);
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
 * Iterator
 * ======================================================================== */

typedef struct {
    const uint8_t *key;
    const void *value;
    bool done;
    bool started;
    struct {
        nt_ref_t ref;
        int      nibble;
    } stack[NT_MAX_NIBBLES + 1];
    int depth;
} nt_iter_state_t;

struct nt_iterator {
    const nibble_trie_t *tree;
    nt_iter_state_t state;
};

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
        if (s->depth >= NT_MAX_NIBBLES + 1) { s->done = true; return false; }
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
            if (s->depth >= NT_MAX_NIBBLES + 1) { s->done = true; return false; }
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
