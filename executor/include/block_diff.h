#ifndef ART_EXECUTOR_BLOCK_DIFF_H
#define ART_EXECUTOR_BLOCK_DIFF_H

/**
 * Block Diff Types — per-block state change records.
 *
 * Always available (not gated by ENABLE_HISTORY). Used for:
 *   - Block rollback / chain reorgs (in-memory)
 *   - State history disk persistence (optional, ENABLE_HISTORY)
 *   - Forward/reverse replay
 */

#include "address.h"
#include "hash.h"
#include "uint256.h"
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Account flags */
#define ACCT_DIFF_CREATED      (1 << 0)
#define ACCT_DIFF_DESTRUCTED   (1 << 1)
#define ACCT_DIFF_TOUCHED      (1 << 2)
#define ACCT_DIFF_FINAL_DIRTY  (1 << 3)

/* Field bitmask — which account fields changed */
#define FIELD_NONCE     (1 << 0)
#define FIELD_BALANCE   (1 << 1)
#define FIELD_CODE_HASH (1 << 2)

/** Single storage slot change. */
typedef struct {
    uint256_t slot;
    uint256_t value;        /* new value (forward replay) */
    uint256_t old_value;    /* previous value (reverse replay / undo) */
} slot_diff_t;

/** Per-address diff group: account fields + storage slots. */
typedef struct {
    address_t   addr;
    uint8_t     flags;       /* ACCT_DIFF_CREATED | ACCT_DIFF_DESTRUCTED */
    uint8_t     field_mask;  /* FIELD_NONCE | FIELD_BALANCE | FIELD_CODE_HASH */
    uint64_t    nonce;       /* new nonce (valid if field_mask & FIELD_NONCE) */
    uint64_t    old_nonce;   /* previous nonce (for undo) */
    uint256_t   balance;     /* new balance (valid if field_mask & FIELD_BALANCE) */
    uint256_t   old_balance; /* previous balance (for undo) */
    hash_t      code_hash;   /* new code_hash (valid if field_mask & FIELD_CODE_HASH) */
    hash_t      old_code_hash; /* previous code_hash (for undo) */
    slot_diff_t *slots;      /* heap-allocated array */
    uint16_t    slot_count;
} addr_diff_t;

/** Per-block diff: array of address groups. */
typedef struct block_diff_t {
    uint64_t     block_number;
    addr_diff_t *groups;        /* heap-allocated array */
    uint16_t     group_count;
} block_diff_t;

/** Free a block_diff_t's heap allocations (not the struct itself). */
void block_diff_free(block_diff_t *diff);

/** Deep-copy a block_diff_t (allocates new groups + slots arrays). */
void block_diff_clone(const block_diff_t *src, block_diff_t *dst);

/* ── Revert API (always available) ───────────────────────────────────── */

struct evm_state;

/**
 * Revert a single block diff (reverse replay / undo).
 * Writes old values to undo the block's state changes.
 * Caller must invalidate cached hashes after revert.
 */
void block_diff_revert(struct evm_state *es, const block_diff_t *diff);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_DIFF_H */
