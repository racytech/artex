/**
 * art_iface implementation for compact_art_t.
 *
 * Thin wrappers around compact_art internals — node/leaf access,
 * dirty flags, children iteration, insert/delete.
 */

#include "art_iface.h"
#include "compact_art.h"
#include <string.h>

/* =========================================================================
 * Helpers (same as in art_mpt.c, moved here)
 * ========================================================================= */

static inline void *ca_node_ptr(const compact_art_t *tree, art_ref_t ref) {
    return tree->nodes.base + ((size_t)(ref & 0x7FFFFFFFu) * 8);
}

static inline void *ca_leaf_ptr(const compact_art_t *tree, art_ref_t ref) {
    return tree->leaves.base + (size_t)COMPACT_LEAF_INDEX(ref) * tree->leaf_size;
}

/* =========================================================================
 * Vtable functions
 * ========================================================================= */

static art_ref_t ci_root(const void *ctx) {
    return ((const compact_art_t *)ctx)->root;
}

static size_t ci_size(const void *ctx) {
    return ((const compact_art_t *)ctx)->size;
}

static bool ci_leaf_key(const void *ctx, art_ref_t ref, uint8_t *key_out) {
    const compact_art_t *tree = ctx;
    const uint8_t *leaf = ca_leaf_ptr(tree, ref);
    if (tree->leaf_key_size == tree->key_size) {
        memcpy(key_out, leaf, tree->key_size);
        return true;
    }
    if (tree->key_fetch) {
        const void *val = leaf + tree->leaf_key_size;
        return tree->key_fetch(val, key_out, tree->key_fetch_ctx);
    }
    return false;
}

static const void *ci_leaf_value(const void *ctx, art_ref_t ref) {
    const compact_art_t *tree = ctx;
    return (const uint8_t *)ca_leaf_ptr(tree, ref) + tree->leaf_key_size;
}

static void *ci_node_ptr(const void *ctx, art_ref_t ref) {
    return ca_node_ptr((const compact_art_t *)ctx, ref);
}

static int ci_node_children(const void *ctx, const void *node,
                             uint8_t *keys, art_ref_t *refs) {
    (void)ctx;
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case COMPACT_NODE_4: {
        const compact_node4_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_16: {
        const compact_node16_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_32: {
        const compact_node32_t *n = node;
        for (int i = 0; i < n->num_children; i++) { keys[i] = n->keys[i]; refs[i] = n->children[i]; }
        return n->num_children;
    }
    case COMPACT_NODE_48: {
        const compact_node48_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->index[i] != COMPACT_NODE48_EMPTY) {
                keys[count] = (uint8_t)i;
                refs[count] = n->children[n->index[i]];
                count++;
            }
        }
        return count;
    }
    case COMPACT_NODE_256: {
        const compact_node256_t *n = node;
        int count = 0;
        for (int i = 0; i < 256; i++) {
            if (n->children[i] != COMPACT_REF_NULL) {
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

static const uint8_t *ci_node_partial(const void *ctx, const void *node,
                                       uint8_t *partial_len) {
    (void)ctx;
    uint8_t type = *(const uint8_t *)node;
    switch (type) {
    case COMPACT_NODE_4:   *partial_len = ((const compact_node4_t *)node)->partial_len;   return ((const compact_node4_t *)node)->partial;
    case COMPACT_NODE_16:  *partial_len = ((const compact_node16_t *)node)->partial_len;  return ((const compact_node16_t *)node)->partial;
    case COMPACT_NODE_32:  *partial_len = ((const compact_node32_t *)node)->partial_len;  return ((const compact_node32_t *)node)->partial;
    case COMPACT_NODE_48:  *partial_len = ((const compact_node48_t *)node)->partial_len;  return ((const compact_node48_t *)node)->partial;
    case COMPACT_NODE_256: *partial_len = ((const compact_node256_t *)node)->partial_len; return ((const compact_node256_t *)node)->partial;
    }
    *partial_len = 0;
    return NULL;
}

static bool ci_is_dirty(const void *ctx, art_ref_t ref) {
    if (ART_IS_LEAF(ref) || ref == ART_REF_NULL) return true;
    const compact_art_t *tree = ctx;
    const void *node = ca_node_ptr(tree, ref);
    uint8_t type = *(const uint8_t *)node;
    const uint8_t *flags;
    switch (type) {
    case COMPACT_NODE_4:   flags = &((const compact_node4_t *)node)->flags; break;
    case COMPACT_NODE_16:  flags = &((const compact_node16_t *)node)->flags; break;
    case COMPACT_NODE_32:  flags = &((const compact_node32_t *)node)->flags; break;
    case COMPACT_NODE_48:  flags = &((const compact_node48_t *)node)->flags; break;
    case COMPACT_NODE_256: flags = &((const compact_node256_t *)node)->flags; break;
    default: return true;
    }
    return (*flags & COMPACT_NODE_FLAG_DIRTY) != 0;
}

static void ci_clear_dirty(void *ctx, art_ref_t ref) {
    if (ART_IS_LEAF(ref) || ref == ART_REF_NULL) return;
    compact_art_t *tree = ctx;
    void *node = ca_node_ptr(tree, ref);
    uint8_t type = *(const uint8_t *)node;
    uint8_t *flags;
    switch (type) {
    case COMPACT_NODE_4:   flags = &((compact_node4_t *)node)->flags; break;
    case COMPACT_NODE_16:  flags = &((compact_node16_t *)node)->flags; break;
    case COMPACT_NODE_32:  flags = &((compact_node32_t *)node)->flags; break;
    case COMPACT_NODE_48:  flags = &((compact_node48_t *)node)->flags; break;
    case COMPACT_NODE_256: flags = &((compact_node256_t *)node)->flags; break;
    default: return;
    }
    *flags &= ~COMPACT_NODE_FLAG_DIRTY;
}

static bool ci_insert(void *ctx, const uint8_t *key, const void *value) {
    return compact_art_insert((compact_art_t *)ctx, key, value);
}

static bool ci_delete(void *ctx, const uint8_t *key) {
    return compact_art_delete((compact_art_t *)ctx, key);
}

static art_ref_t ci_find_subtree(const void *ctx, const uint8_t *prefix,
                                  uint32_t prefix_len, uint32_t *depth_out) {
    return compact_art_find_subtree((const compact_art_t *)ctx,
                                    prefix, prefix_len, depth_out);
}

/* =========================================================================
 * Constructor
 * ========================================================================= */

art_iface_t art_iface_compact(struct compact_art *tree) {
    return (art_iface_t){
        .ctx          = tree,
        .key_size     = tree->key_size,
        .value_size   = tree->value_size,
        .max_prefix   = COMPACT_MAX_PREFIX,
        .root         = ci_root,
        .size         = ci_size,
        .leaf_key     = ci_leaf_key,
        .leaf_value   = ci_leaf_value,
        .node_ptr     = ci_node_ptr,
        .node_children = ci_node_children,
        .node_partial = ci_node_partial,
        .is_dirty     = ci_is_dirty,
        .clear_dirty  = ci_clear_dirty,
        .insert       = ci_insert,
        .del          = ci_delete,
        .find_subtree = ci_find_subtree,
    };
}
