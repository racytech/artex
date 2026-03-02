#ifndef MPT_H
#define MPT_H

#include "hash.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/*
 * MPT Trie — Stripped-down nibble trie for Merkle Patricia Trie hash computation
 *
 * An mmap'd nibble-level trie that IS the Ethereum MPT structure. Each node
 * caches a keccak256 hash. Per-block: insert/delete dirty keys, then walk
 * dirty paths bottom-up to compute the root hash.
 *
 * Node types (mirroring Ethereum MPT):
 *   Branch    — 16 children + cached hash (128B node pool slot)
 *   Extension — nibble path + child ref + cached hash (128B node pool slot)
 *   Leaf      — full key + value + cached hash (192B leaf pool slot)
 *
 * File layout (single mmap'd file, sparse):
 *   [0, 4KB)           Meta A
 *   [4KB, 8KB)         Meta B
 *   [8KB, 32GB+8KB)    Node pool (128B slots, branches + extensions)
 *   [32GB+8KB, ...)    Leaf pool (192B slots)
 *
 * Default keys are 32 bytes (64 nibbles). Use mpt_create_ex for other sizes.
 * Values are variable up to max_value bytes (stored in leaf for rehashing).
 */

#define MPT_KEY_SIZE     32
#define MPT_NUM_NIBBLES  64
#define MPT_MAX_VALUE    120  /* fits RLP([nonce, balance, storageRoot, codeHash]) */

typedef struct mpt mpt_t;

/* Lifecycle */
mpt_t   *mpt_create(const char *path);
mpt_t   *mpt_create_ex(const char *path, uint32_t key_size, uint32_t max_value);
mpt_t   *mpt_open(const char *path);
void     mpt_close(mpt_t *m);

/* Reset trie to empty (reuse without file I/O) */
void     mpt_clear(mpt_t *m);

/* Mutations (mark path dirty) */
bool     mpt_put(mpt_t *m, const uint8_t *key,
                 const uint8_t *value, uint8_t vlen);
bool     mpt_delete(mpt_t *m, const uint8_t *key);

/* Lookup */
bool     mpt_get(mpt_t *m, const uint8_t *key,
                 uint8_t *value_out, uint8_t *vlen_out);

/* Root hash (walks dirty nodes bottom-up, caches result) */
hash_t   mpt_root(mpt_t *m);

/* Subtree root hash — navigate trie by prefix nibbles, compute hash */
hash_t   mpt_subtree_root(mpt_t *m, const uint8_t *prefix_key,
                          uint32_t prefix_nibbles);

/* Persistence (fdatasync + dual meta page) */
bool     mpt_commit(mpt_t *m);

/* Stats */
uint64_t mpt_size(const mpt_t *m);    /* key count */
uint32_t mpt_nodes(const mpt_t *m);   /* allocated node slots */
uint32_t mpt_leaves(const mpt_t *m);  /* allocated leaf slots */

#endif /* MPT_H */
