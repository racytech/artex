#ifndef STATE_ACCOUNT_H
#define STATE_ACCOUNT_H

/**
 * Account types for the in-memory state.
 *
 * Two structs, two vectors:
 *   account_t  — compact (80 bytes), every account
 *   resource_t — full (heap pointers), only accounts with code/storage (~3%)
 *
 * account_t.resource_idx points into the resource vector (0 = none).
 */

#include "uint256.h"
#include "hash.h"
#include "address.h"
#include <stdint.h>

/* =========================================================================
 * Flags — packed into uint16_t
 * ========================================================================= */

#define ACCT_EXISTED         (1 << 0)
#define ACCT_DIRTY           (1 << 1)
#define ACCT_CODE_DIRTY      (1 << 2)
#define ACCT_STORAGE_DIRTY   (1 << 3)
#define ACCT_STORAGE_CLEARED (1 << 4)
#define ACCT_CREATED         (1 << 5)
#define ACCT_SELF_DESTRUCTED (1 << 6)
#define ACCT_HAS_CODE        (1 << 7)
#define ACCT_MPT_DIRTY       (1 << 8)
#define ACCT_BLOCK_DIRTY     (1 << 9)

/* =========================================================================
 * account_t — compact, every account (80 bytes)
 * ========================================================================= */

typedef struct {
    uint256_t balance;          /* 32 bytes — 16-byte aligned, first */
    uint64_t  nonce;            /*  8 bytes */
    uint64_t  last_access_block;/*  8 bytes */
    address_t addr;             /* 20 bytes */
    uint16_t  flags;            /*  2 bytes */
    uint32_t  resource_idx;     /*  4 bytes — index into resource_t[], 0=none */
} account_t;

/* =========================================================================
 * resource_t — only for accounts with code and/or storage
 * ========================================================================= */

typedef struct {
    hash_t    code_hash;        /* 32 bytes */
    hash_t    storage_root;     /* 32 bytes — root of this account's storage trie */
    uint8_t  *code;             /*  8 bytes (heap, loaded on demand) */
    uint32_t  code_size;        /*  4 bytes */
    uint32_t  _pad;             /*  4 bytes */
} resource_t;

/* =========================================================================
 * Flag helpers
 * ========================================================================= */

static inline int  acct_has_flag(const account_t *a, uint16_t f) { return (a->flags & f) != 0; }
static inline void acct_set_flag(account_t *a, uint16_t f)       { a->flags |= f; }
static inline void acct_clear_flag(account_t *a, uint16_t f)     { a->flags &= (uint16_t)~f; }

static inline int acct_is_empty(const account_t *a) {
    return a->nonce == 0 &&
           uint256_is_zero(&a->balance) &&
           !acct_has_flag(a, ACCT_HAS_CODE);
}

#endif /* STATE_ACCOUNT_H */
