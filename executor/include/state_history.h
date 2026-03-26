#ifndef ART_EXECUTOR_STATE_HISTORY_H
#define ART_EXECUTOR_STATE_HISTORY_H

#include "address.h"
#include "hash.h"
#include "uint256.h"
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * State History: per-block diff tracking (v3 — grouped, new-values-only)
 *
 * Records per-block state changes into an append-only log. Used for
 * debugging (diff comparison, bisection) and as a feed for verkle
 * tree construction.
 *
 * Architecture:
 *   block_executor (producer) → SPSC ring → consumer thread → disk
 *
 * The executor never blocks: if the ring is full, the diff is dropped.
 *
 * v3 format improvements over v2:
 *   - New values only (old values reconstructed from block N-1 when needed)
 *   - Grouped by address (account fields + storage slots in one entry)
 *   - Field bitmask (skip unchanged account fields on disk)
 *
 * File format (v3):
 *   .idx — 16-byte header (magic + version + first_block) + 16-byte entries
 *   .dat — variable-length records, each with CRC32C trailer
 *   Record: header(16) + addr_groups(variable) + crc32(4)
 *
 * On-disk record layout:
 *   header:  block_number(8) + record_len(4) + group_count(2) + reserved(2)
 *   per group:
 *     addr(20) + flags(1) + field_mask(1) + slot_count(2) = 24 bytes fixed
 *     [nonce(8)]        if field_mask & FIELD_NONCE
 *     [balance(32)]     if field_mask & FIELD_BALANCE
 *     [code_hash(32)]   if field_mask & FIELD_CODE_HASH
 *     [slot(32) + value(32)] × slot_count
 *
 * On reopen, the tail is validated by walking backwards and checking CRC.
 * Corrupt trailing records are truncated.
 * ========================================================================= */

/* ── Per-block diff types ─────────────────────────────────────────────── */

/* Account flags */
#define ACCT_DIFF_CREATED      (1 << 0)
#define ACCT_DIFF_DESTRUCTED   (1 << 1)
#define ACCT_DIFF_TOUCHED      (1 << 2)  /* touched this block but no value change */
#define ACCT_DIFF_FINAL_DIRTY  (1 << 3)  /* block_dirty was true at diff collection */

/* Field bitmask — which account fields changed */
#define FIELD_NONCE     (1 << 0)
#define FIELD_BALANCE   (1 << 1)
#define FIELD_CODE_HASH (1 << 2)

/** Single storage slot change (new value only). */
typedef struct {
    uint256_t slot;
    uint256_t value;        /* new value */
} slot_diff_t;

/** Per-address diff group: account fields + storage slots. */
typedef struct {
    address_t   addr;
    uint8_t     flags;       /* ACCT_DIFF_CREATED | ACCT_DIFF_DESTRUCTED */
    uint8_t     field_mask;  /* FIELD_NONCE | FIELD_BALANCE | FIELD_CODE_HASH */
    uint64_t    nonce;       /* new nonce (valid if field_mask & FIELD_NONCE) */
    uint256_t   balance;     /* new balance (valid if field_mask & FIELD_BALANCE) */
    hash_t      code_hash;   /* new code_hash (valid if field_mask & FIELD_CODE_HASH) */
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

/* ── SPSC ring buffer for diffs ───────────────────────────────────────── */

#define DIFF_RING_CAP 512  /* must be power of 2 */

typedef struct {
    block_diff_t   slots[DIFF_RING_CAP];
    atomic_size_t  head;    /* written by producer only */
    atomic_size_t  tail;    /* written by consumer only */
    pthread_mutex_t mtx;
    pthread_cond_t  not_empty;
} diff_ring_t;

/* ── Opaque handle ────────────────────────────────────────────────────── */

typedef struct state_history state_history_t;

/* ── Lifecycle ────────────────────────────────────────────────────────── */

/**
 * Create state history tracker. Opens/creates files at dir_path.
 * Spawns consumer thread. Returns NULL on failure.
 */
state_history_t *state_history_create(const char *dir_path);

/** Stop consumer thread, flush remaining diffs, close files. */
void state_history_destroy(state_history_t *sh);

/* ── Producer API (called from block executor, non-blocking) ──────────── */

/**
 * Collect diff from evm_state dirty entries and push to ring buffer.
 * Non-blocking: drops diff if ring is full (logs warning).
 * Called between finalize() and compute_state_root_ex().
 */
struct evm_state;
void state_history_capture(state_history_t *sh, struct evm_state *es,
                            uint64_t block_number);

/**
 * Push a pre-built diff to the ring buffer. Non-blocking: drops if full.
 * The ring takes ownership of diff->groups (and nested slots) on success.
 */
void state_history_push(state_history_t *sh, block_diff_t *diff);

/* ── Query API (thread-safe, read-only against consumer) ──────────────── */

/**
 * Read diff for a single block. Caller must free via block_diff_free().
 * Returns false if block not found.
 */
bool state_history_get_diff(const state_history_t *sh,
                             uint64_t block_number,
                             block_diff_t *out);

/**
 * Get range of available block numbers.
 * Returns false if no blocks recorded.
 */
bool state_history_range(const state_history_t *sh,
                          uint64_t *first, uint64_t *last);

/** Get disk usage in bytes (dat file size). */
uint64_t state_history_disk_bytes(const state_history_t *sh);

/** Get number of blocks recorded. */
uint64_t state_history_block_count(const state_history_t *sh);

/* ── Truncation (for checkpoint resume) ──────────────────────────────── */

/**
 * Truncate history to keep only blocks up to `last_block`.
 * Used on checkpoint resume to discard entries beyond the checkpoint.
 * Not thread-safe with consumer — call before pushing new diffs.
 */
void state_history_truncate(state_history_t *sh, uint64_t last_block);

/* ── Forward reconstruction API ──────────────────────────────────────── */

/**
 * Apply a single block diff to an evm_state (forward reconstruction).
 * Sets nonce/balance/code_hash and storage slots from the diff's new values.
 * Handles ACCT_DIFF_CREATED (create account) and ACCT_DIFF_DESTRUCTED
 * (zero nonce/balance/code_hash, clear cached storage).
 */
struct evm_state;
void state_history_apply_diff(struct evm_state *es, const block_diff_t *diff);

/**
 * Replay diffs from first_block..last_block onto evm_state.
 * Reads each diff from the history files and applies it via apply_diff.
 * Returns the number of blocks successfully applied (0 on failure).
 */
uint64_t state_history_replay(state_history_t *sh,
                               struct evm_state *es,
                               uint64_t first_block,
                               uint64_t last_block);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_STATE_HISTORY_H */
