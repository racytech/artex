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

/* Operations */
bool        hart_insert(hart_t *t, const uint8_t key[32], const void *value);
bool        hart_delete(hart_t *t, const uint8_t key[32]);
bool        hart_delete_get(hart_t *t, const uint8_t key[32], void *out_value);
const void *hart_get(const hart_t *t, const uint8_t key[32]);
bool        hart_contains(const hart_t *t, const uint8_t key[32]);
size_t      hart_size(const hart_t *t);

/* Mark path from root to key as dirty (for external modifications) */
bool hart_mark_path_dirty(hart_t *t, const uint8_t key[32]);

/* Check if tree has any dirty nodes (root node dirty = tree needs rehashing) */
bool hart_is_dirty(const hart_t *t);

/* Force all nodes dirty — invalidates all cached hashes */
void hart_invalidate_all(hart_t *t);

/* Parallel variant of hart_invalidate_all — splits root's 16 hi-nibble
 * groups across 4 threads, same structure as hart_root_hash_parallel.
 * Falls back to serial for small roots (NODE_4 / NODE_16 / leaf). Safe:
 * each subtree's dirty bits are independent, no shared mutable state. */
void hart_invalidate_all_parallel(hart_t *t);

/* Count internal (non-leaf, non-null) nodes reachable from the root, and
 * how many of them currently have a clean (non-dirty) hash cache. After
 * hart_invalidate_all / hart_invalidate_all_parallel, `*clean_out` should
 * be 0 — used by tests to verify full-tree coverage of the parallel path. */
uint32_t hart_count_internal_nodes(const hart_t *t, uint32_t *clean_out);

/* Compute MPT root hash. Only rehashes dirty subtrees.
 * Clean subtrees use the hash embedded in the node. */
void hart_root_hash(hart_t *t, hart_encode_t encode, void *ctx, uint8_t out[32]);

/* Parallel root hash — splits root's 16 hi-nibble groups across 4 threads. */
void hart_root_hash_parallel(hart_t *t, hart_encode_t encode, void *ctx, uint8_t out[32]);

/* Iterator */
typedef struct hart_iter hart_iter_t;
hart_iter_t    *hart_iter_create(const hart_t *t);
bool            hart_iter_next(hart_iter_t *it);
const uint8_t  *hart_iter_key(const hart_iter_t *it);
const void     *hart_iter_value(const hart_iter_t *it);
void            hart_iter_destroy(hart_iter_t *it);

/* Pre-order DFS walk over every reachable node (internal + leaf).
 * Child traversal is key-sorted (same order as the root-hash walk and the
 * leaf iterator), so the emitted node sequence is deterministic given the
 * tree's key set. For internal nodes, hash32 points at the cached 32-byte
 * MPT hash embedded in the node — call hart_root_hash first so hashes are
 * valid; dirty internals expose stale bytes. For leaves, hash32 is NULL. */
typedef void (*hart_walk_cb)(hart_ref_t ref,
                              int depth,
                              const uint8_t *hash32,
                              bool is_leaf,
                              void *user);
void hart_walk_dfs(const hart_t *t, hart_walk_cb cb, void *user);

/* Count internal nodes that carry a meaningful cached hash — i.e. those
 * whose RLP is a real branch in the MPT (≥2 children, ≥2 distinct
 * hi-nibbles). Single-child / single-hi-group internals are folded into
 * their parent's extension prefix and never get a cache write, so they
 * must NOT be persisted. This count matches what hart_install_dfs_hashes
 * expects and what hart_walk_persistable_hashes will emit. */
uint32_t hart_count_persistable_hashes(const hart_t *t);

/* Pre-order DFS walk over branch-producing internals only, in the same
 * order as hart_install_dfs_hashes consumes. Caller must have computed
 * the root first so the cached hashes are valid. */
typedef void (*hart_persist_cb)(const uint8_t hash32[32], void *user);
void hart_walk_persistable_hashes(const hart_t *t,
                                   hart_persist_cb cb, void *user);

/* Install cached hashes onto branch-producing internal nodes in DFS order.
 * Tree must already be fully built (all keys inserted). `count` must equal
 * hart_count_persistable_hashes(t) — mismatch returns false without
 * touching the tree. Branch-producing nodes get the supplied hash and are
 * marked clean; passthrough internals stay dirty so the next root
 * computation walks through them naturally (fast: their children are
 * already cached). */
bool hart_install_dfs_hashes(hart_t *t, const uint8_t *stream, uint32_t count);

#endif /* HASHED_ART_H */
