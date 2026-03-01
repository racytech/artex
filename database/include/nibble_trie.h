#ifndef NIBBLE_TRIE_H
#define NIBBLE_TRIE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* ========================================================================
 * Nibble Trie — in-memory 16-way trie for MPT root computation
 *
 * Pure in-memory, arena-allocated. No file I/O, no persistence.
 * Structure matches Ethereum's hex-prefix trie (branch/extension/leaf).
 *
 * Fixed 32-byte keys (keccak256 hashes). Value size configurable at init.
 *
 * Ref encoding (32-bit):
 *   Bits 31-30 : type tag
 *     11 = branch     (0xC0000000)
 *     10 = extension  (0x80000000)
 *     01 = leaf       (0x40000000)
 *     00 = NULL       (ref == 0)
 *   Bits 29-0  : pool index
 * ======================================================================== */

#define NT_KEY_SIZE         32
#define NT_MAX_NIBBLES      64   /* NT_KEY_SIZE * 2 */

typedef uint32_t nt_ref_t;

#define NT_REF_NULL         ((nt_ref_t)0)

#define NT_TYPE_MASK        ((nt_ref_t)0xC0000000u)
#define NT_TYPE_BRANCH      ((nt_ref_t)0xC0000000u)
#define NT_TYPE_EXTENSION   ((nt_ref_t)0x80000000u)
#define NT_TYPE_LEAF        ((nt_ref_t)0x40000000u)

#define NT_INDEX_MASK       ((nt_ref_t)0x3FFFFFFFu)

#define NT_REF_TYPE(r)      ((r) & NT_TYPE_MASK)
#define NT_REF_INDEX(r)     ((r) & NT_INDEX_MASK)

#define NT_IS_BRANCH(r)     (NT_REF_TYPE(r) == NT_TYPE_BRANCH)
#define NT_IS_EXTENSION(r)  (NT_REF_TYPE(r) == NT_TYPE_EXTENSION)
#define NT_IS_LEAF(r)       (NT_REF_TYPE(r) == NT_TYPE_LEAF)

#define NT_MAKE_BRANCH_REF(i)    ((nt_ref_t)(i) | NT_TYPE_BRANCH)
#define NT_MAKE_EXTENSION_REF(i) ((nt_ref_t)(i) | NT_TYPE_EXTENSION)
#define NT_MAKE_LEAF_REF(i)      ((nt_ref_t)(i) | NT_TYPE_LEAF)

/* ========================================================================
 * Arena — growable bump allocator with free list
 * ======================================================================== */

typedef struct {
    uint8_t  *base;      /* heap allocation */
    uint32_t  count;     /* slots used (high-water mark) */
    uint32_t  capacity;  /* slots allocated */
    uint32_t  slot_size; /* bytes per slot */
    uint32_t  free_head; /* intrusive free list head (0 = empty) */
} nt_arena_t;

/* ========================================================================
 * Hash cache — per-node cached node_ref_t for incremental nt_root_hash
 * ======================================================================== */

typedef struct {
    uint8_t data[32];  /* cached hash or inline RLP */
    uint8_t len;       /* 32=hash, 1-31=inline, 0=dirty/uncached */
    uint8_t _pad;
} nt_node_hash_t;      /* 34 bytes */

typedef struct {
    nt_node_hash_t *entries;  /* growable array, indexed by pool index */
    uint32_t capacity;
} nt_hash_cache_t;

/* ========================================================================
 * Tree structure
 * ======================================================================== */

typedef struct nibble_trie nibble_trie_t;
typedef struct nt_iterator nt_iterator_t;

struct nibble_trie {
    nt_arena_t branches;     /* 64B slots */
    nt_arena_t extensions;   /* 40B slots */
    nt_arena_t leaves;       /* (NT_KEY_SIZE + value_size) per slot */

    uint32_t  value_size;

    nt_ref_t  root;
    uint64_t  size;      /* number of key-value pairs */

    /* Per-node hash cache for incremental nt_root_hash */
    nt_hash_cache_t branch_cache;
    nt_hash_cache_t ext_cache;
    nt_hash_cache_t leaf_cache;
};

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

bool nt_init(nibble_trie_t *t, uint32_t value_size);
void nt_destroy(nibble_trie_t *t);

/* Reset to empty without freeing arena memory (reuse allocations) */
void nt_clear(nibble_trie_t *t);

/* Shrink arenas to exactly fit current contents. Call after bulk loading. */
void nt_shrink_to_fit(nibble_trie_t *t);

/* Pre-allocate arenas for expected number of keys. Avoids repeated doubling. */
bool nt_reserve(nibble_trie_t *t, size_t expected_keys);

/* ========================================================================
 * Mutations (in-place, no COW)
 * ======================================================================== */

bool nt_insert(nibble_trie_t *t, const uint8_t *key, const void *value);
bool nt_delete(nibble_trie_t *t, const uint8_t *key);

/* ========================================================================
 * Reads
 * ======================================================================== */

const void    *nt_get(const nibble_trie_t *t, const uint8_t *key);
bool           nt_contains(const nibble_trie_t *t, const uint8_t *key);
size_t         nt_size(const nibble_trie_t *t);

/* ========================================================================
 * Iterator (sorted nibble order = sorted key order)
 * ======================================================================== */

nt_iterator_t *nt_iterator_create(const nibble_trie_t *t);
bool           nt_iterator_next(nt_iterator_t *it);
const uint8_t *nt_iterator_key(const nt_iterator_t *it);
const void    *nt_iterator_value(const nt_iterator_t *it);
bool           nt_iterator_done(const nt_iterator_t *it);
void           nt_iterator_destroy(nt_iterator_t *it);
bool           nt_iterator_seek(nt_iterator_t *it, const uint8_t *key);

#endif /* NIBBLE_TRIE_H */
