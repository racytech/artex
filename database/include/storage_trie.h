#ifndef STORAGE_TRIE_H
#define STORAGE_TRIE_H

#include <stdint.h>
#include <stdbool.h>
#include "state_meta.h"

/**
 * Storage Trie — Per-account storage MPT roots from a shared compact_art.
 *
 * Encode callback reads from meta pool (overlay entries) or flat_store (disk).
 */

typedef struct storage_trie storage_trie_t;
typedef struct compact_art compact_art_t;
typedef struct flat_store flat_store_t;

storage_trie_t *storage_trie_create(compact_art_t *storage_art,
                                      flat_store_t *storage_store,
                                      slot_meta_pool_t *meta_pool);
void storage_trie_destroy(storage_trie_t *st);

void storage_trie_root(storage_trie_t *st,
                         const uint8_t addr_hash[32],
                         uint8_t out[32]);
void storage_trie_invalidate_all(storage_trie_t *st);

#endif /* STORAGE_TRIE_H */
