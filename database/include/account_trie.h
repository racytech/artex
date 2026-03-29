#ifndef ACCOUNT_TRIE_H
#define ACCOUNT_TRIE_H

#include <stdint.h>
#include <stdbool.h>

/**
 * Account Trie — MPT root hash from flat_state's account compact_art.
 *
 * Wraps art_mpt over the 32-byte-keyed account compact_art.
 * The encode callback reads compressed account records from flat_store's
 * mmap'd data file and produces Ethereum account RLP:
 *   [nonce, balance, storage_root, code_hash]
 *
 * Does NOT own the compact_art — that belongs to flat_state.
 */

typedef struct account_trie account_trie_t;
typedef struct compact_art compact_art_t;
typedef struct flat_store flat_store_t;

account_trie_t *account_trie_create(compact_art_t *account_art,
                                      flat_store_t *account_store);
void account_trie_destroy(account_trie_t *at);

/**
 * Compute account trie root hash (incremental).
 * Only rehashes accounts whose data changed since the last call.
 */
void account_trie_root(account_trie_t *at, uint8_t out[32]);

/** Invalidate all cached hashes. */
void account_trie_invalidate_all(account_trie_t *at);

#endif /* ACCOUNT_TRIE_H */
