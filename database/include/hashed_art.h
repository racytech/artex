#ifndef HASHED_ART_H
#define HASHED_ART_H

/**
 * Hashed ART — Adaptive Radix Trie with embedded MPT hash cache.
 *
 * Purpose-built for Ethereum state:
 *   - Fixed 32-byte keys (keccak hashes of addresses/slots)
 *   - Fixed value sizes (4 bytes for acct_index, 32 bytes for storage)
 *   - Each inner node embeds a 32-byte MPT hash + dirty flag
 *   - No separate hash cache allocation
 *   - No art_iface indirection layer
 *   - Bump arena allocator (same as mem_art)
 *
 * Usage:
 *   hart_t tree;
 *   hart_init(&tree, 4);         // 4-byte values (acct_index)
 *   hart_insert(&tree, key, &idx);
 *   hart_root_hash(&tree, encode_cb, ctx, root_out);
 *   hart_destroy(&tree);
 */

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef uint32_t hart_ref_t;

#define HART_REF_NULL   ((hart_ref_t)0)
#define HART_IS_LEAF(r) ((r) & 0x80000000u)

/**
 * MPT leaf encode callback.
 * key:      32-byte leaf key
 * leaf_val: pointer to stored value (value_size bytes)
 * rlp_out:  buffer to write encoded RLP (max 256 bytes)
 * Returns RLP length, or 0 on error.
 */
typedef uint32_t (*hart_encode_t)(const uint8_t key[32],
                                   const void *leaf_val,
                                   uint8_t *rlp_out,
                                   void *ctx);

typedef struct {
    hart_ref_t root;
    size_t     size;          /* number of key-value pairs */
    uint8_t   *arena;
    size_t     arena_used;
    size_t     arena_cap;
    uint16_t   value_size;    /* fixed: 4 or 32 */
    /* Per-type free lists — arena offsets, 0 = empty list.
     * Dead nodes are recycled before appending new ones. */
    uint32_t   free_node4;
    uint32_t   free_node16;
    uint32_t   free_node48;
    uint32_t   free_node256;
    uint32_t   free_leaf;
} hart_t;

/* Lifecycle */
bool hart_init(hart_t *t, uint16_t value_size);
bool hart_init_cap(hart_t *t, uint16_t value_size, size_t initial_cap);
void hart_destroy(hart_t *t);

/* Shrink arena to fit used data. Returns bytes freed (0 if nothing to shrink). */
size_t hart_trim(hart_t *t);

/* Arena-direct dump/load — serialize hart as header + raw arena bytes.
 * With freelist enabled, arenas have no dead space — dump is compact. */
typedef struct {
    hart_ref_t root;
    uint32_t   size;
    uint32_t   arena_used;
    uint16_t   value_size;
    /* Free list heads — preserved across dump/load */
    uint32_t   free_node4, free_node16, free_node48, free_node256, free_leaf;
    uint16_t   _pad;
} hart_dump_hdr_t;

size_t hart_dump_size(const hart_t *t);
void   hart_dump(const hart_t *t, uint8_t *buf);
bool   hart_load(hart_t *t, const uint8_t *buf, size_t buf_len);

/* Operations */
bool        hart_insert(hart_t *t, const uint8_t key[32], const void *value);
bool        hart_delete(hart_t *t, const uint8_t key[32]);
const void *hart_get(const hart_t *t, const uint8_t key[32]);
bool        hart_contains(const hart_t *t, const uint8_t key[32]);
size_t      hart_size(const hart_t *t);

/* Mark path from root to key as dirty (for external modifications) */
bool hart_mark_path_dirty(hart_t *t, const uint8_t key[32]);

/* Check if tree has any dirty nodes (root node dirty = tree needs rehashing) */
bool hart_is_dirty(const hart_t *t);

/* Force all nodes dirty — invalidates all cached hashes */
void hart_invalidate_all(hart_t *t);

/* Compute MPT root hash. Only rehashes dirty subtrees.
 * Clean subtrees use the hash embedded in the node. */
void hart_root_hash(hart_t *t, hart_encode_t encode, void *ctx, uint8_t out[32]);

/* Iterator */
typedef struct hart_iter hart_iter_t;
hart_iter_t    *hart_iter_create(const hart_t *t);
bool            hart_iter_next(hart_iter_t *it);
const uint8_t  *hart_iter_key(const hart_iter_t *it);
const void     *hart_iter_value(const hart_iter_t *it);
void            hart_iter_destroy(hart_iter_t *it);

#endif /* HASHED_ART_H */
