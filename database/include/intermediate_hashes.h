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
// Accessors
// ============================================================================

/** Get current root hash (no recomputation). */
hash_t ih_root(const ih_state_t *ih);

/** Number of intermediate hash entries stored (branch nodes). */
size_t ih_entry_count(const ih_state_t *ih);

#endif // INTERMEDIATE_HASHES_H
