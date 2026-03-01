#ifndef NIBBLE_TRIE_H
#define NIBBLE_TRIE_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* ========================================================================
 * Ref encoding (32-bit)
 *
 *   Bits 31-30 : type tag
 *     11 = branch     (0xC0000000)
 *     10 = extension  (0x80000000)
 *     01 = leaf       (0x40000000)
 *     00 = NULL       (ref == 0)
 *   Bits 29-0  : pool index
 *     branch/extension → node pool (64-byte slots)
 *     leaf             → leaf pool (key_size + value_size, 8-byte aligned)
 * ======================================================================== */

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
 * Tree structure
 * ======================================================================== */

typedef struct nibble_trie nibble_trie_t;
typedef struct nt_iterator nt_iterator_t;

struct nibble_trie {
    int       fd;
    uint8_t  *node_base;            /* mmap'd node pool (branches + extensions, 64B slots) */
    uint8_t  *leaf_base;            /* mmap'd leaf pool (key + value, variable slot size) */

    uint32_t  key_size;             /* key size in bytes (set at open, e.g. 32 or 64) */
    uint32_t  value_size;           /* value size in bytes (set at open) */
    uint32_t  leaf_slot_size;       /* key_size + value_size, 8-byte aligned */

    nt_ref_t  root;
    uint64_t  size;                 /* number of key-value pairs */
    uint32_t  node_count;           /* node slots allocated (branches + extensions) */
    uint32_t  leaf_count;           /* leaf slots allocated */

    /* committed-state snapshot (for rollback) */
    nt_ref_t  committed_root;
    uint64_t  committed_size;
    uint32_t  committed_node_count;
    uint32_t  committed_leaf_count;

    uint64_t  generation;
    int       active_meta;          /* 0 or 1 */
};

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

bool nt_open(nibble_trie_t *t, const char *path, uint32_t key_size, uint32_t value_size);
void nt_close(nibble_trie_t *t);

/* ========================================================================
 * COW mutations (pending until commit)
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
 * Persistence
 * ======================================================================== */

bool nt_commit(nibble_trie_t *t);
void nt_rollback(nibble_trie_t *t);

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
