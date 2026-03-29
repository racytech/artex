#ifndef STORAGE_TRIE_H
#define STORAGE_TRIE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Storage Trie — Per-account storage MPT roots from a shared compact_art.
 *
 * Wraps art_mpt with subtree hash computation over a 64-byte-keyed
 * compact_art (addr_hash[32] || slot_hash[32]).
 *
 * Does NOT own the compact_art — that belongs to flat_state.
 * The flat_store is used to read slot values for RLP encoding.
 *
 * Usage:
 *   storage_trie_t *st = storage_trie_create(
 *       flat_state_storage_art(fs), flat_state_storage_store(fs));
 *   // ... flat_state_put_storage / flat_state_delete_storage ...
 *   storage_trie_root(st, addr_hash, root_out);
 *   storage_trie_destroy(st);
 */

typedef struct storage_trie storage_trie_t;
typedef struct compact_art compact_art_t;
typedef struct flat_store flat_store_t;

storage_trie_t *storage_trie_create(compact_art_t *storage_art,
                                      flat_store_t *storage_store);
void storage_trie_destroy(storage_trie_t *st);

/**
 * Compute storage root for one account.
 * addr_hash: 32-byte keccak256(address).
 * out: receives 32-byte MPT root hash (EMPTY_ROOT if no storage).
 */
void storage_trie_root(storage_trie_t *st,
                         const uint8_t addr_hash[32],
                         uint8_t out[32]);

/** Invalidate all cached hashes. Use after bulk load or external mutation. */
void storage_trie_invalidate_all(storage_trie_t *st);

#endif /* STORAGE_TRIE_H */
