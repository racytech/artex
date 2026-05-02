// SPDX-License-Identifier: LGPL-3.0-or-later
// Copyright (C) 2026 artex contributors

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
#include "storage_hart2.h"
#include <stdint.h>

/* =========================================================================
 * Flags — packed into uint16_t
 * ========================================================================= */

#define ACCT_EXISTED         (1 << 0)  /* account is in the state trie */
#define ACCT_DIRTY           (1 << 1)  /* nonce/balance/code changed this tx */
#define ACCT_STORAGE_DIRTY   (1 << 2)  /* storage changed this block */
#define ACCT_CREATED         (1 << 3)  /* CREATE'd this tx */
#define ACCT_SELF_DESTRUCTED (1 << 4)  /* SELFDESTRUCT this tx */
#define ACCT_HAS_CODE        (1 << 5)  /* account has non-empty code */
#define ACCT_IN_BLK_DIRTY    (1 << 6)  /* already in blk_dirty list this block */

/* =========================================================================
 * account_t — compact, every account (80 bytes)
 * ========================================================================= */

/* TODO(perf): reuse last_access_block's 8B slot for an inline storage hart
 * root ref. last_access_block is dead — only written (state.c:460, 490) for
 * an eviction scheme that was removed (OS swap handles cold pages now).
 * Repurposing it saves one indirection on SLOAD:
 *   before: accounts[] → resources[resource_idx] → r.storage.root_ref
 *   after:  accounts[] → a.storage_root_ref
 * Combined with per-account slabs in storage_hart pool (see
 * docs/storage_hart_pool_design.md), turns a cold SLOAD from ~3 page faults
 * into ~1. Zero size cost — account_t stays 80B. Requires keeping
 * a.storage_root_ref and r.storage.root_ref in sync — single write site, add
 * invariant assert. */
typedef struct {
    uint256_t balance;          /* 32 bytes — 16-byte aligned, first */
    uint64_t  nonce;            /*  8 bytes */
    uint64_t  last_access_block;/*  8 bytes — DEAD, see TODO above */
    address_t addr;             /* 20 bytes */
    uint16_t  flags;            /*  2 bytes */
    uint32_t  resource_idx;     /*  4 bytes — index into resource_t[], 0=none */
} account_t;

/* =========================================================================
 * resource_t — only for accounts with code and/or storage
 * ========================================================================= */

typedef struct {
    hash_t          code_hash;     /* 32 bytes */
    hash_t          storage_root;  /* 32 bytes */
    uint8_t        *code;          /*  8 bytes (heap, loaded on demand) */
    uint32_t        code_size;     /*  4 bytes */
    uint8_t        *jumpdest_bitmap; /* cached JUMPDEST bitmap (1 bit per byte) */
    storage_hart_t  storage;       /* per-account storage trie (mmap-backed) */
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
