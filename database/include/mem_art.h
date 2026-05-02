// SPDX-License-Identifier: LGPL-3.0-or-later
// Copyright (C) 2026 artex contributors

#ifndef MEM_ART_H
#define MEM_ART_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/**
 * In-Memory Adaptive Radix Tree (ART) — Arena-Allocated
 *
 * Space-efficient write buffer and EVM state cache for the data layer.
 * Every state-touching opcode (SLOAD/SSTORE/BALANCE) hits this tree.
 *
 * Optimizations:
 *   1. Bump arena allocator (no per-node malloc overhead, O(1) destroy)
 *   2. 4-byte mem_ref_t instead of 8-byte pointers
 *   3. Compact 3-byte node headers + 8-byte inline prefix
 *   4. Node32 fills gap between Node16 and Node48
 *   5. SSE2 for Node16/Node32 key lookup
 *
 * DESIGNED FOR FIXED-SIZE KEYS (20-byte addresses, 32-byte hashes).
 * Variable-length values supported (tombstones, RLP-encoded state).
 * MEMORY-ONLY — no persistence. Resets per block via destroy+init.
 */

// ============================================================================
// Ref type — 4-byte encoded pointer (offset into arena)
// ============================================================================

typedef uint32_t mem_ref_t;

#define MEM_REF_NULL      ((mem_ref_t)0)
#define MEM_REF_LEAF_BIT  ((mem_ref_t)0x80000000u)

#define MEM_IS_LEAF(r)       ((r) & MEM_REF_LEAF_BIT)
#define MEM_LEAF_OFFSET(r)   ((r) & 0x7FFFFFFFu)
#define MEM_MAKE_LEAF(off)   ((mem_ref_t)(off) | MEM_REF_LEAF_BIT)

// ============================================================================
// Tree structure
// ============================================================================

typedef struct mem_art mem_art_t;
typedef struct mem_art_iterator mem_art_iterator_t;

struct mem_art {
    mem_ref_t root;       // 0 = empty tree
    size_t size;          // Number of key-value pairs
    uint8_t *arena;       // Bump-allocated arena
    size_t arena_used;    // Next free byte offset (starts at 4, offset 0 = NULL)
    size_t arena_cap;     // Arena capacity in bytes
};

struct mem_art_iterator {
    mem_art_t *tree;
    void *internal;
};

// ============================================================================
// Public API (unchanged signatures)
// ============================================================================

bool mem_art_init(mem_art_t *tree);
bool mem_art_init_cap(mem_art_t *tree, size_t initial_cap);
void mem_art_destroy(mem_art_t *tree);

bool mem_art_insert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len);

/** Insert and report whether the key was new (single traversal).
 *  Returns false on allocation failure. Sets *was_new = true if inserted,
 *  false if key already existed (value updated in place). */
bool mem_art_insert_check(mem_art_t *tree, const uint8_t *key, size_t key_len,
                          const void *value, size_t value_len, bool *was_new);

const void *mem_art_get(const mem_art_t *tree, const uint8_t *key, size_t key_len,
                    size_t *value_len);

void *mem_art_get_mut(mem_art_t *tree, const uint8_t *key, size_t key_len,
                      size_t *value_len);

/** Insert (or update) and return mutable pointer to the stored value.
 *  Combines insert + get_mut in a single call (one tree traversal for insert,
 *  one for pointer lookup — but avoids caller doing two separate calls). */
void *mem_art_upsert(mem_art_t *tree, const uint8_t *key, size_t key_len,
                     const void *value, size_t value_len);

bool mem_art_delete(mem_art_t *tree, const uint8_t *key, size_t key_len);

/** Walk from root to the leaf matching key, set dirty flags on all inner nodes.
 *  No allocation, no value change, no structural modification.
 *  Returns true if the key was found. */
bool mem_art_mark_path_dirty(mem_art_t *tree, const uint8_t *key, size_t key_len);
bool mem_art_contains(const mem_art_t *tree, const uint8_t *key, size_t key_len);

/** Prefetch ART tree path for key into CPU cache (non-blocking). */
void mem_art_prefetch(const mem_art_t *tree, const uint8_t *key, size_t key_len);
size_t mem_art_size(const mem_art_t *tree);
bool mem_art_is_empty(const mem_art_t *tree);

// Iterator
mem_art_iterator_t *mem_art_iterator_create(const mem_art_t *tree);
bool mem_art_iterator_next(mem_art_iterator_t *iter);
const uint8_t *mem_art_iterator_key(const mem_art_iterator_t *iter, size_t *key_len);
const void *mem_art_iterator_value(const mem_art_iterator_t *iter, size_t *value_len);
bool mem_art_iterator_done(const mem_art_iterator_t *iter);
void mem_art_iterator_destroy(mem_art_iterator_t *iter);

// Foreach callback
typedef bool (*mem_art_callback_t)(const uint8_t *key, size_t key_len,
                                const void *value, size_t value_len,
                                void *user_data);

void mem_art_foreach(const mem_art_t *tree, mem_art_callback_t callback, void *user_data);

#endif // MEM_ART_H
