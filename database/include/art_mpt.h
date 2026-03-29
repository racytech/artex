#ifndef ART_MPT_H
#define ART_MPT_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include "compact_art.h"

/**
 * ART→MPT Hash — compute Ethereum MPT root hash from a compact_art tree.
 *
 * Walks the ART structure directly and maps it to MPT semantics:
 *   - ART byte-level branching → two levels of MPT nibble branching
 *   - ART partial keys → MPT extension nodes
 *   - ART leaves → MPT leaf nodes (path + RLP value)
 *
 * The leaf's RLP value is provided by a callback (value_encode) since
 * compact_art stores raw data, not RLP-encoded MPT leaf values.
 *
 * Non-incremental version: recomputes entire trie hash.
 * For 250M accounts: ~5-10 seconds. For per-account storage tries
 * (typically < 1000 entries): microseconds.
 */

/**
 * Callback to produce the RLP-encoded value for an MPT leaf.
 *
 * key:      full 32-byte key (the ART leaf's key).
 * leaf_val: pointer to the compact_art leaf value data.
 * val_size: size of the leaf value data.
 * rlp_out:  buffer to write RLP-encoded value (max 1024 bytes).
 * Returns the RLP length written, or 0 on failure.
 */
typedef uint32_t (*art_mpt_value_encode_t)(const uint8_t *key,
                                            const void *leaf_val,
                                            uint32_t val_size,
                                            uint8_t *rlp_out,
                                            void *user_ctx);

/**
 * Compute the MPT root hash from a compact_art tree.
 *
 * tree:     the compact_art to walk.
 * key_size: bytes per key (must match tree->key_size, typically 32).
 * encode:   callback to RLP-encode each leaf's value.
 * ctx:      user context passed to encode callback.
 * out:      receives the 32-byte MPT root hash.
 *
 * If the tree is empty, out is set to EMPTY_ROOT (keccak256(0x80)).
 */
void art_mpt_root_hash(const compact_art_t *tree,
                        art_mpt_value_encode_t encode, void *ctx,
                        uint8_t out[32]);

#endif /* ART_MPT_H */
