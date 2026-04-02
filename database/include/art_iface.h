#ifndef ART_IFACE_H
#define ART_IFACE_H

/**
 * Abstract ART tree interface for art_mpt.
 *
 * Allows art_mpt to compute MPT root hashes over any ART implementation
 * (compact_art or mem_art) through a vtable of function pointers.
 *
 * Both compact_art and mem_art use uint32_t refs with bit 31 as leaf marker
 * and 0 as null, so the ref type is unified.
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef uint32_t art_ref_t;

#define ART_REF_NULL      ((art_ref_t)0)
#define ART_REF_LEAF_BIT  ((art_ref_t)0x80000000u)
#define ART_IS_LEAF(r)    ((r) & ART_REF_LEAF_BIT)

typedef struct art_iface {
    void *ctx;   /* opaque — points to compact_art_t* or mem_art wrapper */

    uint32_t key_size;
    uint32_t value_size;
    uint32_t max_prefix;   /* COMPACT_MAX_PREFIX=8, MEM_MAX_PREFIX=20 */

    /* Tree-level */
    art_ref_t (*root)(const void *ctx);
    size_t    (*size)(const void *ctx);

    /* Leaf access */
    bool        (*leaf_key)(const void *ctx, art_ref_t ref, uint8_t *key_out);
    const void *(*leaf_value)(const void *ctx, art_ref_t ref);

    /* Inner node access — ref must be a non-leaf, non-null inner node ref */
    void       *(*node_ptr)(const void *ctx, art_ref_t ref);
    int         (*node_children)(const void *ctx, const void *node,
                                 uint8_t *keys_out, art_ref_t *refs_out);
    const uint8_t *(*node_partial)(const void *ctx, const void *node,
                                    uint8_t *partial_len_out);

    /* Dirty flags */
    bool (*is_dirty)(const void *ctx, art_ref_t ref);
    void (*clear_dirty)(void *ctx, art_ref_t ref);

    /* Mutation (marks path dirty internally) */
    bool (*insert)(void *ctx, const uint8_t *key, const void *value);
    bool (*del)(void *ctx, const uint8_t *key);

    /* Subtree navigation (for 64-byte key mode — per-account storage roots).
     * NULL if not supported (per-account trees don't need this). */
    art_ref_t (*find_subtree)(const void *ctx, const uint8_t *prefix,
                               uint32_t prefix_len, uint32_t *depth_out);
} art_iface_t;

/* Backend constructors (defined in respective .c files) */
struct compact_art;
struct mem_art;

art_iface_t art_iface_compact(struct compact_art *tree);

/* mem_art wrapper — needs fixed key/value size since mem_art API is variable-length */
typedef struct {
    struct mem_art *tree;
    uint32_t key_size;
    uint32_t value_size;
} art_iface_mem_ctx_t;

art_iface_t art_iface_mem(art_iface_mem_ctx_t *ctx);

#endif /* ART_IFACE_H */
