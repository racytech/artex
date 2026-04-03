/**
 * art_iface implementation for mem_art_t.
 *
 * Wraps mem_art internals for art_mpt hash computation.
 * Requires dirty flags in mem_art nodes (_pad → flags, MEM_NODE_FLAG_DIRTY).
 */

#include "art_iface.h"
#include "mem_art.h"
#include <string.h>

/* =========================================================================
 * mem_art internal types — must match mem_art.c definitions
 * ========================================================================= */

/* Node type enum (same values as mem_art.c) */
#define MI_NODE_4    0
#define MI_NODE_16   1
#define MI_NODE_32   2
#define MI_NODE_48   3
#define MI_NODE_256  4

#define MI_MAX_PREFIX    8
#define MI_NODE48_EMPTY  255
#define MI_NODE_FLAG_DIRTY 0x01

typedef uint32_t mi_ref_t;

/* Node structs — must match mem_art.c layout exactly */
typedef struct {
    uint8_t type, num_children, partial_len;
    uint8_t keys[4];
    uint8_t flags;
    mi_ref_t children[4];
    uint8_t partial[MI_MAX_PREFIX];
} mi_node4_t;

typedef struct {
    uint8_t type, num_children, partial_len;
    uint8_t keys[16];
    uint8_t flags;
    mi_ref_t children[16];
    uint8_t partial[MI_MAX_PREFIX];
} mi_node16_t;

typedef struct {
    uint8_t type, num_children, partial_len;
    uint8_t keys[32];
    uint8_t flags;
    mi_ref_t children[32];
    uint8_t partial[MI_MAX_PREFIX];
} mi_node32_t;

typedef struct {
    uint8_t type, num_children, partial_len;
    uint8_t index[256];
    uint8_t flags;
    mi_ref_t children[48];
    uint8_t partial[MI_MAX_PREFIX];
} mi_node48_t;

typedef struct {
    uint8_t type, num_children, partial_len;
    uint8_t flags;
    mi_ref_t children[256];
    uint8_t partial[MI_MAX_PREFIX];
} mi_node256_t;

typedef struct {
    uint16_t key_len;
    uint16_t value_len;
    uint8_t data[];
} mi_leaf_t;

/* =========================================================================
 * Helpers
 * ========================================================================= */

static inline void *mi_ref_ptr(const mem_art_t *tree, mi_ref_t ref) {
    return tree->arena + ((size_t)(ref & 0x7FFFFFFFu) << 4);  /* ref * 16 */
}

static inline art_iface_mem_ctx_t *get_ctx(const void *ctx) {
    return (art_iface_mem_ctx_t *)ctx;
}

/* Must match leaf_value_offset in mem_art.c — value is 16-byte aligned */
static inline size_t mi_leaf_value_offset(size_t key_len, size_t value_len) {
    if (value_len == 0) return sizeof(mi_leaf_t) + key_len;
    size_t hdr_key = sizeof(mi_leaf_t) + key_len;
    return (hdr_key + 15) & ~(size_t)15;
}

/* =========================================================================
 * Vtable functions
 * ========================================================================= */

static art_ref_t mi_root(const void *ctx) {
    return get_ctx(ctx)->tree->root;
}

static size_t mi_size(const void *ctx) {
    return get_ctx(ctx)->tree->size;
}

static bool mi_leaf_key(const void *ctx, art_ref_t ref, uint8_t *key_out) {
    const mem_art_t *tree = get_ctx(ctx)->tree;
    const mi_leaf_t *leaf = mi_ref_ptr(tree, ref);
    memcpy(key_out, leaf->data, leaf->key_len);
    return true;
}

static const void *mi_leaf_value(const void *ctx, art_ref_t ref) {
    const art_iface_mem_ctx_t *mc = get_ctx(ctx);
    const mi_leaf_t *leaf = mi_ref_ptr(mc->tree, ref);
    size_t val_off = mi_leaf_value_offset(leaf->key_len, leaf->value_len);
    return (const uint8_t *)leaf + val_off;
}

static void *mi_node_ptr(const void *ctx, art_ref_t ref) {
    return mi_ref_ptr(get_ctx(ctx)->tree, ref);
}

static int mi_node_children(const void *ctx, const void *node,
                             uint8_t *keys, art_ref_t *refs) {
    (void)ctx;
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case MI_NODE_4: {
        const mi_node4_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case MI_NODE_16: {
        const mi_node16_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case MI_NODE_32: {
        const mi_node32_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case MI_NODE_48: {
        const mi_node48_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->index[i] != MI_NODE48_EMPTY) {
                keys[count] = (uint8_t)i;
                refs[count] = n->children[n->index[i]];
                count++;
            }
        }
        return count;
    }
    case MI_NODE_256: {
        const mi_node256_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != 0) {
                keys[count] = (uint8_t)i;
                refs[count] = n->children[i];
                count++;
            }
        }
        return count;
    }
    }
    return 0;
}

static const uint8_t *mi_node_partial(const void *ctx, const void *node,
                                       uint8_t *partial_len) {
    (void)ctx;
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case MI_NODE_4:   *partial_len = ((const mi_node4_t *)node)->partial_len;   return ((const mi_node4_t *)node)->partial;
    case MI_NODE_16:  *partial_len = ((const mi_node16_t *)node)->partial_len;  return ((const mi_node16_t *)node)->partial;
    case MI_NODE_32:  *partial_len = ((const mi_node32_t *)node)->partial_len;  return ((const mi_node32_t *)node)->partial;
    case MI_NODE_48:  *partial_len = ((const mi_node48_t *)node)->partial_len;  return ((const mi_node48_t *)node)->partial;
    case MI_NODE_256: *partial_len = ((const mi_node256_t *)node)->partial_len; return ((const mi_node256_t *)node)->partial;
    }
    *partial_len = 0;
    return NULL;
}

static uint8_t *get_flags_ptr(const void *node) {
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case MI_NODE_4:   return &((mi_node4_t *)node)->flags;
    case MI_NODE_16:  return &((mi_node16_t *)node)->flags;
    case MI_NODE_32:  return &((mi_node32_t *)node)->flags;
    case MI_NODE_48:  return &((mi_node48_t *)node)->flags;
    case MI_NODE_256: return &((mi_node256_t *)node)->flags;
    }
    return NULL;
}

static bool mi_is_dirty(const void *ctx, art_ref_t ref) {
    if (ART_IS_LEAF(ref) || ref == ART_REF_NULL) return true;
    const void *node = mi_ref_ptr(get_ctx(ctx)->tree, ref);
    const uint8_t *flags = get_flags_ptr(node);
    return flags ? (*flags & MI_NODE_FLAG_DIRTY) != 0 : true;
}

static void mi_clear_dirty(void *ctx, art_ref_t ref) {
    if (ART_IS_LEAF(ref) || ref == ART_REF_NULL) return;
    void *node = mi_ref_ptr(get_ctx(ctx)->tree, ref);
    uint8_t *flags = get_flags_ptr(node);
    if (flags) *flags &= ~MI_NODE_FLAG_DIRTY;
}

static bool mi_insert(void *ctx, const uint8_t *key, const void *value) {
    art_iface_mem_ctx_t *mc = ctx;
    return mem_art_insert(mc->tree, key, mc->key_size, value, mc->value_size);
}

static bool mi_delete(void *ctx, const uint8_t *key) {
    art_iface_mem_ctx_t *mc = ctx;
    return mem_art_delete(mc->tree, key, mc->key_size);
}

/* =========================================================================
 * Constructor
 * ========================================================================= */

art_iface_t art_iface_mem(art_iface_mem_ctx_t *ctx) {
    return (art_iface_t){
        .ctx          = ctx,
        .key_size     = ctx->key_size,
        .value_size   = ctx->value_size,
        .max_prefix   = MI_MAX_PREFIX,
        .root         = mi_root,
        .size         = mi_size,
        .leaf_key     = mi_leaf_key,
        .leaf_value   = mi_leaf_value,
        .node_ptr     = mi_node_ptr,
        .node_children = mi_node_children,
        .node_partial = mi_node_partial,
        .is_dirty     = mi_is_dirty,
        .clear_dirty  = mi_clear_dirty,
        .insert       = mi_insert,
        .del          = mi_delete,
        .find_subtree = NULL,  /* not needed for per-account trees */
    };
}
