/**
 * Hashed ART — Adaptive Radix Trie with embedded MPT hash cache.
 *
 * Key differences from mem_art:
 *   - Fixed 32-byte keys (no variable key_len)
 *   - Fixed value_size (set at init, 4 or 32 bytes)
 *   - Each inner node has 32-byte hash + hash_len embedded
 *   - Dirty flag on inner nodes: hash is stale when dirty
 *   - MPT root hash computed directly — no separate art_mpt + cache
 *   - Optimized for keccak-hashed keys (short/zero shared prefixes)
 */

#include "hashed_art.h"
#include "keccak256.h"
#include <stdlib.h>
#include <string.h>
#include <immintrin.h>  /* SSE2 for node16/32 key search */

/* =========================================================================
 * Constants
 * ========================================================================= */

#define KEY_SIZE      32
#define MAX_PREFIX     8   /* keccak keys rarely share more than 1-2 bytes */
#define NODE_DIRTY     0x01

#define NODE_4         0
#define NODE_16        1
#define NODE_48        2
#define NODE_256       3

#define NODE48_EMPTY   255

static const uint8_t EMPTY_ROOT[32] = {
    0x56,0xe8,0x1f,0x17,0x1b,0xcc,0x55,0xa6,
    0xff,0x83,0x45,0xe6,0x92,0xc0,0xf8,0x6e,
    0x5b,0x48,0xe0,0x1b,0x99,0x6c,0xad,0xc0,
    0x01,0x62,0x2f,0xb5,0xe3,0x63,0xb4,0x21
};

/* =========================================================================
 * Node structures — each embeds hash[32] + hash_len
 * ========================================================================= */

typedef struct {
    uint8_t   type, num_children, partial_len, flags, hash_len;
    uint8_t   keys[4];
    hart_ref_t children[4];
    uint8_t   partial[MAX_PREFIX];
    uint8_t   hash[32];
} node4_t;

typedef struct {
    uint8_t   type, num_children, partial_len, flags, hash_len;
    uint8_t   keys[16];
    hart_ref_t children[16];
    uint8_t   partial[MAX_PREFIX];
    uint8_t   hash[32];
} node16_t;

typedef struct {
    uint8_t   type, num_children, partial_len, flags, hash_len;
    uint8_t   keys[48];     /* unused slots beyond num_children */
    uint8_t   index[256];
    hart_ref_t children[48];
    uint8_t   partial[MAX_PREFIX];
    uint8_t   hash[32];
} node48_t;

typedef struct {
    uint8_t   type, num_children, partial_len, flags, hash_len;
    hart_ref_t children[256];
    uint8_t   partial[MAX_PREFIX];
    uint8_t   hash[32];
} node256_t;

typedef struct {
    uint8_t  data[];  /* key[32] then value[value_size] */
} leaf_t;

/* =========================================================================
 * Arena allocator
 * ========================================================================= */

static hart_ref_t arena_alloc(hart_t *t, size_t bytes, bool is_leaf) {
    size_t aligned = (t->arena_used + 15) & ~(size_t)15;
    if (aligned + bytes > t->arena_cap) {
        size_t nc = t->arena_cap ? t->arena_cap * 2 : 4096;
        while (aligned + bytes > nc) nc *= 2;
        uint8_t *na = realloc(t->arena, nc);
        if (!na) return HART_REF_NULL;
        t->arena = na;
        t->arena_cap = nc;
    }
    t->arena_used = aligned + bytes;
    memset(t->arena + aligned, 0, bytes);
    hart_ref_t ref = (hart_ref_t)(aligned >> 4);
    if (is_leaf) ref |= 0x80000000u;
    return ref;
}

static inline void *ref_ptr(const hart_t *t, hart_ref_t ref) {
    return t->arena + ((size_t)(ref & 0x7FFFFFFFu) << 4);
}

/* =========================================================================
 * Node helpers
 * ========================================================================= */

static inline uint8_t node_type(const void *n) { return *(const uint8_t *)n; }

static inline uint8_t *node_flags(void *n) {
    switch (node_type(n)) {
    case NODE_4:   return &((node4_t *)n)->flags;
    case NODE_16:  return &((node16_t *)n)->flags;
    case NODE_48:  return &((node48_t *)n)->flags;
    case NODE_256: return &((node256_t *)n)->flags;
    default: return NULL;
    }
}

static inline void mark_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    uint8_t *f = node_flags(ref_ptr(t, ref));
    if (f) *f |= NODE_DIRTY;
}

static inline bool is_dirty(const hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return true;
    const uint8_t *f = node_flags(ref_ptr(t, ref));
    return f ? (*f & NODE_DIRTY) : true;
}

static inline void clear_dirty(hart_t *t, hart_ref_t ref) {
    if (HART_IS_LEAF(ref) || ref == HART_REF_NULL) return;
    uint8_t *f = node_flags(ref_ptr(t, ref));
    if (f) *f &= ~NODE_DIRTY;
}

/* Get/set embedded hash */
static inline uint8_t *node_hash(void *n, uint8_t *hash_len_out) {
    switch (node_type(n)) {
    case NODE_4:   *hash_len_out = ((node4_t *)n)->hash_len;   return ((node4_t *)n)->hash;
    case NODE_16:  *hash_len_out = ((node16_t *)n)->hash_len;  return ((node16_t *)n)->hash;
    case NODE_48:  *hash_len_out = ((node48_t *)n)->hash_len;  return ((node48_t *)n)->hash;
    case NODE_256: *hash_len_out = ((node256_t *)n)->hash_len; return ((node256_t *)n)->hash;
    default: *hash_len_out = 0; return NULL;
    }
}

static inline void set_node_hash(void *n, const uint8_t *data, uint8_t len) {
    switch (node_type(n)) {
    case NODE_4:   memcpy(((node4_t *)n)->hash, data, len);   ((node4_t *)n)->hash_len = len; break;
    case NODE_16:  memcpy(((node16_t *)n)->hash, data, len);  ((node16_t *)n)->hash_len = len; break;
    case NODE_48:  memcpy(((node48_t *)n)->hash, data, len);  ((node48_t *)n)->hash_len = len; break;
    case NODE_256: memcpy(((node256_t *)n)->hash, data, len); ((node256_t *)n)->hash_len = len; break;
    }
}

static inline uint8_t node_partial_len(const void *n) {
    return ((const uint8_t *)n)[2];  /* partial_len at offset 2 in all types */
}

static inline const uint8_t *node_partial(const void *n) {
    switch (node_type(n)) {
    case NODE_4:   return ((const node4_t *)n)->partial;
    case NODE_16:  return ((const node16_t *)n)->partial;
    case NODE_48:  return ((const node48_t *)n)->partial;
    case NODE_256: return ((const node256_t *)n)->partial;
    default: return NULL;
    }
}

/* Leaf access — fixed 32-byte key, value_size bytes after */
static inline const uint8_t *leaf_key(const hart_t *t, hart_ref_t ref) {
    return ((const leaf_t *)ref_ptr(t, ref))->data;
}

static inline const void *leaf_value(const hart_t *t, hart_ref_t ref) {
    return ((const leaf_t *)ref_ptr(t, ref))->data + KEY_SIZE;
}

static inline bool leaf_matches(const hart_t *t, hart_ref_t ref, const uint8_t key[32]) {
    return memcmp(leaf_key(t, ref), key, KEY_SIZE) == 0;
}

static hart_ref_t alloc_leaf(hart_t *t, const uint8_t key[32], const void *value) {
    size_t total = KEY_SIZE + t->value_size;
    /* Align total to 16 bytes */
    total = (total + 15) & ~(size_t)15;
    hart_ref_t ref = arena_alloc(t, total, true);
    if (ref == HART_REF_NULL) return ref;
    uint8_t *p = ref_ptr(t, ref);
    memcpy(p, key, KEY_SIZE);
    memcpy(p + KEY_SIZE, value, t->value_size);
    return ref;
}

/* =========================================================================
 * Child lookup — find_child_ptr returns pointer to child ref slot
 * ========================================================================= */

static hart_ref_t *find_child_ptr(const hart_t *t, hart_ref_t node_ref, uint8_t byte) {
    void *n = ref_ptr(t, node_ref);
    switch (node_type(n)) {
    case NODE_4: {
        node4_t *nn = n;
        for (int i = 0; i < nn->num_children; i++)
            if (nn->keys[i] == byte) return &nn->children[i];
        return NULL;
    }
    case NODE_16: {
        node16_t *nn = n;
        __m128i key_vec = _mm_set1_epi8((char)byte);
        __m128i cmp = _mm_cmpeq_epi8(key_vec, _mm_loadu_si128((__m128i *)nn->keys));
        int mask = _mm_movemask_epi8(cmp) & ((1 << nn->num_children) - 1);
        if (mask) return &nn->children[__builtin_ctz(mask)];
        return NULL;
    }
    case NODE_48: {
        node48_t *nn = n;
        uint8_t idx = nn->index[byte];
        return (idx != NODE48_EMPTY) ? &nn->children[idx] : NULL;
    }
    case NODE_256: {
        node256_t *nn = n;
        return nn->children[byte] ? &nn->children[byte] : NULL;
    }
    }
    return NULL;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

bool hart_init(hart_t *t, uint16_t value_size) {
    return hart_init_cap(t, value_size, 4096);
}

bool hart_init_cap(hart_t *t, uint16_t value_size, size_t initial_cap) {
    memset(t, 0, sizeof(*t));
    t->value_size = value_size;
    if (initial_cap < 256) initial_cap = 256;
    t->arena = malloc(initial_cap);
    if (!t->arena) return false;
    t->arena_cap = initial_cap;
    t->arena_used = 16;  /* offset 0 = NULL ref */
    return true;
}

void hart_destroy(hart_t *t) {
    if (!t) return;
    free(t->arena);
    memset(t, 0, sizeof(*t));
}

size_t hart_size(const hart_t *t) { return t ? t->size : 0; }

/* =========================================================================
 * TODO: insert, delete, get, contains, mark_path_dirty, root_hash, iterator
 * These follow the same algorithms as mem_art but with:
 *   - Fixed 32-byte keys (no key_len parameter)
 *   - mark_dirty sets NODE_DIRTY and clears hash_len on inner nodes
 *   - root_hash reads embedded hash when clean, computes when dirty
 * ========================================================================= */

const void *hart_get(const hart_t *t, const uint8_t key[32]) {
    if (!t) return NULL;
    hart_ref_t ref = t->root;
    size_t depth = 0;
    while (ref != HART_REF_NULL) {
        if (HART_IS_LEAF(ref))
            return leaf_matches(t, ref, key) ? leaf_value(t, ref) : NULL;
        void *n = ref_ptr(t, ref);
        uint8_t plen = node_partial_len(n);
        if (plen > 0) {
            const uint8_t *partial = node_partial(n);
            int cmp = plen < MAX_PREFIX ? plen : MAX_PREFIX;
            for (int i = 0; i < cmp; i++)
                if (partial[i] != key[depth + i]) return NULL;
            /* For prefix > MAX_PREFIX, trust the structure (verified on insert) */
            depth += plen;
        }
        uint8_t byte = (depth < KEY_SIZE) ? key[depth] : 0;
        hart_ref_t *child = find_child_ptr(t, ref, byte);
        if (!child) return NULL;
        ref = *child;
        depth++;
    }
    return NULL;
}

bool hart_contains(const hart_t *t, const uint8_t key[32]) {
    return hart_get(t, key) != NULL;
}

/* Stub implementations — to be filled in */
bool hart_insert(hart_t *t, const uint8_t key[32], const void *value) {
    (void)t; (void)key; (void)value;
    /* TODO: port from mem_art insert_recursive with fixed KEY_SIZE */
    return false;
}

bool hart_delete(hart_t *t, const uint8_t key[32]) {
    (void)t; (void)key;
    /* TODO: port from mem_art delete_recursive */
    return false;
}

bool hart_mark_path_dirty(hart_t *t, const uint8_t key[32]) {
    if (!t) return false;
    hart_ref_t ref = t->root;
    size_t depth = 0;
    while (ref != HART_REF_NULL) {
        if (HART_IS_LEAF(ref))
            return leaf_matches(t, ref, key);
        mark_dirty(t, ref);
        void *n = ref_ptr(t, ref);
        uint8_t plen = node_partial_len(n);
        if (plen > 0) depth += plen;
        uint8_t byte = (depth < KEY_SIZE) ? key[depth] : 0;
        hart_ref_t *child = find_child_ptr(t, ref, byte);
        if (!child) return false;
        ref = *child;
        depth++;
    }
    return false;
}

void hart_root_hash(hart_t *t, hart_encode_t encode, void *ctx, uint8_t out[32]) {
    (void)t; (void)encode; (void)ctx;
    /* TODO: port from art_mpt hash_ref but read/write hash from node */
    memcpy(out, EMPTY_ROOT, 32);
}

hart_iter_t *hart_iter_create(const hart_t *t) { (void)t; return NULL; }
bool hart_iter_next(hart_iter_t *it) { (void)it; return false; }
const uint8_t *hart_iter_key(const hart_iter_t *it) { (void)it; return NULL; }
const void *hart_iter_value(const hart_iter_t *it) { (void)it; return NULL; }
void hart_iter_destroy(hart_iter_t *it) { (void)it; }
