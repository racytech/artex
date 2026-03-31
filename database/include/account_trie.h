#ifndef ACCOUNT_TRIE_H
#define ACCOUNT_TRIE_H

#include <stdint.h>
#include <stdbool.h>
#include "state_meta.h"

/**
 * Account Trie — MPT root hash from flat_state's account compact_art.
 *
 * Encode callback reads from meta pool (overlay entries) or flat_store (disk).
 */

typedef struct account_trie account_trie_t;
typedef struct compact_art compact_art_t;
typedef struct flat_store flat_store_t;

account_trie_t *account_trie_create(compact_art_t *account_art,
                                      flat_store_t *account_store,
                                      account_meta_pool_t *meta_pool);
void account_trie_destroy(account_trie_t *at);
void account_trie_root(account_trie_t *at, uint8_t out[32]);
void account_trie_invalidate_all(account_trie_t *at);

#endif /* ACCOUNT_TRIE_H */
