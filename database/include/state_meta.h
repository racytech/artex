#ifndef STATE_META_H
#define STATE_META_H

/**
 * State meta types — cached account and slot structures.
 *
 * Shared between state_overlay.c (owner) and encode callbacks
 * (account_trie.c, storage_trie.c) so the trie can read typed
 * fields directly from the meta pool.
 */

#include "uint256.h"
#include "hash.h"
#include "address.h"
#include <stdbool.h>
#include <stdint.h>

#define STATE_META_SLOT_KEY_SIZE 52  /* addr[20] + slot_be[32] */

typedef struct {
    uint64_t   nonce;
    uint256_t  balance;
    bool dirty;
    bool block_dirty;
    bool existed;
    bool mpt_dirty;
    bool storage_dirty;
    bool storage_cleared;
    bool has_code;
    bool created;
    bool self_destructed;
    bool code_dirty;
    bool block_code_dirty;
    uint8_t   *code;
    uint32_t   code_size;
    address_t  addr;
    hash_t     code_hash;
    hash_t     storage_root;
    hash_t     addr_hash;
#ifdef ENABLE_HISTORY
    uint64_t   original_nonce;
    uint256_t  original_balance;
    hash_t     original_code_hash;
    bool       block_self_destructed;
    bool       block_created;
    bool       block_accessed;
#endif
} cached_account_t;

typedef struct {
    uint8_t   key[STATE_META_SLOT_KEY_SIZE];
    uint256_t original;
    uint256_t current;
    bool dirty;
    bool block_dirty;
    bool mpt_dirty;
    hash_t slot_hash;
#ifdef ENABLE_HISTORY
    uint256_t block_original;
#endif
} cached_slot_t;

typedef struct {
    cached_account_t *entries;
    uint32_t          capacity;
} account_meta_pool_t;

typedef struct {
    cached_slot_t    *entries;
    uint32_t          capacity;
} slot_meta_pool_t;

#endif /* STATE_META_H */
