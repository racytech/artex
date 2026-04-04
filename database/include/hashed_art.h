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
} hart_t;

/* Lifecycle */
bool hart_init(hart_t *t, uint16_t value_size);
bool hart_init_cap(hart_t *t, uint16_t value_size, size_t initial_cap);
void hart_destroy(hart_t *t);

/* Operations */
bool        hart_insert(hart_t *t, const uint8_t key[32], const void *value);
bool        hart_delete(hart_t *t, const uint8_t key[32]);
const void *hart_get(const hart_t *t, const uint8_t key[32]);
bool        hart_contains(const hart_t *t, const uint8_t key[32]);
size_t      hart_size(const hart_t *t);

/* Mark path from root to key as dirty (for external modifications) */
bool hart_mark_path_dirty(hart_t *t, const uint8_t key[32]);

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
