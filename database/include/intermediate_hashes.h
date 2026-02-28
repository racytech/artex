#ifndef INTERMEDIATE_HASHES_H
#define INTERMEDIATE_HASHES_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "hash.h"

/**
 * Intermediate Hash Table — MPT Commitment via Flat Hash Storage
 *
 * Stores intermediate Merkle Patricia Trie hashes in a separate compact_art
 * instance, completely decoupled from the data layer ART. The data ART owns
 * key-value data; this table owns only 32-byte hashes at branch node positions.
 *
 * Architecture: Erigon-style (see PROOF_GENERATION.md, INTERMEDIATE_HASHES.md).
 * No trie nodes in memory. No value duplication.
 *
 * Phase 1: ih_build (full computation from sorted keys).
 * Phase 2 (future): ih_update (incremental per-block update).
 */

typedef struct ih_state ih_state_t;

// ============================================================================
// Lifecycle
// ============================================================================

/** Create empty intermediate hash state. Returns NULL on failure. */
ih_state_t *ih_create(void);

/** Destroy and free all resources. */
void ih_destroy(ih_state_t *ih);

// ============================================================================
// Full Build
// ============================================================================

/**
 * Build the full MPT from sorted 32-byte keys and raw values.
 *
 * keys:       array of `count` pointers to 32-byte keys (sorted ascending)
 * values:     array of `count` pointers to raw value bytes
 * value_lens: array of `count` value lengths
 * count:      number of entries (0 = empty trie)
 *
 * Values are raw bytes — the function handles all RLP encoding internally.
 * Returns the 32-byte state root hash.
 * Clears any previous state before building.
 */
hash_t ih_build(ih_state_t *ih,
                const uint8_t *const *keys,
                const uint8_t *const *values,
                const uint16_t *value_lens,
                size_t count);

/**
 * Build MPT from variable-length keys (for Ethereum test vectors).
 * Same semantics as ih_build but keys can have different lengths.
 */
hash_t ih_build_varlen(ih_state_t *ih,
                       const uint8_t *const *keys,
                       const size_t *key_lens,
                       const uint8_t *const *values,
                       const size_t *value_lens,
                       size_t count);

// ============================================================================
// Incremental Update
// ============================================================================

/**
 * Cursor interface for reading data keys during ih_update.
 *
 * The cursor abstracts read access to the data layer's sorted key-value store.
 * ih_update uses seek + next to iterate only the key ranges it needs
 * (clean leaf children of dirty branches), avoiding a full scan.
 *
 * All keys/values must remain valid until the next seek/next call or cursor
 * destruction.
 */
typedef struct {
    void *ctx;  // opaque context (e.g., data layer iterator)

    /** Position cursor at first entry with key >= seek_key. */
    bool (*seek)(void *ctx, const uint8_t *seek_key, size_t key_len);

    /** Advance to next entry. Returns false if no more entries. */
    bool (*next)(void *ctx);

    /** Check if cursor is positioned at a valid entry. */
    bool (*valid)(void *ctx);

    /** Get current key (NULL if !valid). */
    const uint8_t *(*key)(void *ctx, size_t *out_len);

    /** Get current value (NULL if !valid). */
    const uint8_t *(*value)(void *ctx, size_t *out_len);
} ih_cursor_t;

/**
 * Incremental MPT update for a block's dirty keys.
 *
 * Recomputes only the subtrees affected by changed keys, reusing cached
 * branch hashes from ih_tree for clean subtrees.
 *
 * dirty_keys:  array of `count` pointers to 32-byte keys (sorted ascending)
 * dirty_vals:  array of `count` pointers to new values (NULL = delete)
 * dirty_vlens: array of `count` value lengths (0 for deletes)
 * count:       number of dirty keys this block
 * cursor:      read cursor over the full data layer (post-merge state)
 *
 * Precondition: ih_build() must have been called once (initial sync).
 * The data layer must already contain the merged dirty keys.
 *
 * Returns the new 32-byte state root hash.
 */
hash_t ih_update(ih_state_t *ih,
                 const uint8_t *const *dirty_keys,
                 const uint8_t *const *dirty_vals,
                 const size_t *dirty_vlens,
                 size_t count,
                 ih_cursor_t *cursor);

// ============================================================================
// Accessors
// ============================================================================

/** Get current root hash (no recomputation). */
hash_t ih_root(const ih_state_t *ih);

/** Number of intermediate hash entries stored (branch nodes). */
size_t ih_entry_count(const ih_state_t *ih);

#endif // INTERMEDIATE_HASHES_H
