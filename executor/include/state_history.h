#ifndef ART_EXECUTOR_STATE_HISTORY_H
#define ART_EXECUTOR_STATE_HISTORY_H

#include "address.h"
#include "hash.h"
#include "uint256.h"
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* =========================================================================
 * State History: per-block diff tracking
 *
 * Records account and storage changes for every block into an append-only
 * log. Used for debugging (diff comparison, bisection) and as a feed for
 * verkle tree construction.
 *
 * Architecture:
 *   block_executor (producer) → SPSC ring → consumer thread → disk
 *
 * The executor never blocks: if the ring is full, the diff is dropped.
 *
 * File format (v2):
 *   .idx — 16-byte header (magic + version + first_block) + 16-byte entries
 *   .dat — variable-length records, each with CRC32C trailer
 *   Record: header(16) + account_diffs(165 each) + storage_diffs(116 each) + crc32(4)
 *
 * On reopen, the tail is validated by walking backwards and checking CRC.
 * Corrupt trailing records are truncated. fdatasync every 256 blocks, so
 * worst-case crash loss is ~256 blocks of diffs.
 *
 * Disk usage (measured from 200K early blocks, extrapolated):
 *   Early chain (0-4.3M):   ~350 bytes/block  →  ~1.5 GB
 *   Mid chain (4.3M-15.5M): ~40 KB/block      →  ~440 GB
 *   Post-merge (15.5M+):    ~160 KB/block      →  ~750 GB
 *   Full chain total:        ~1.2 TB uncompressed
 *
 * Future size reduction options (not yet implemented):
 *   1. Per-record compression (zstd/lz4): 2-4x reduction → 300-600 GB.
 *      Balances share leading bytes, code_hash often unchanged, lots of zeros.
 *   2. Compact encoding: bitmask for changed fields, skip unchanged code_hash
 *      (currently 64 bytes per account even when identical). Could halve
 *      account diff size.
 *   3. Pruning: keep only last N blocks (e.g. 100K ≈ 2 weeks post-merge
 *      → ~16 GB). Sufficient for debugging. Truncate old entries from head.
 *   4. Delta encoding: store balance diff instead of full old+new (32+32 bytes
 *      → typically 8-16 bytes).
 * ========================================================================= */

/* ── Per-block diff types ─────────────────────────────────────────────── */

#define ACCT_DIFF_CREATED      (1 << 0)
#define ACCT_DIFF_DESTRUCTED   (1 << 1)

typedef struct {
    address_t addr;
    uint64_t  old_nonce,  new_nonce;
    uint256_t old_balance, new_balance;
    hash_t    old_code_hash, new_code_hash;
    uint8_t   flags;       /* ACCT_DIFF_CREATED | ACCT_DIFF_DESTRUCTED */
} account_diff_t;

typedef struct {
    address_t addr;
    uint256_t slot;
    uint256_t old_value, new_value;
} storage_diff_t;

typedef struct block_diff_t {
    uint64_t        block_number;
    account_diff_t *accounts;        /* heap-allocated array */
    uint32_t        account_count;
    storage_diff_t *storage;         /* heap-allocated array */
    uint32_t        storage_count;
} block_diff_t;

/** Free a block_diff_t's heap allocations (not the struct itself). */
void block_diff_free(block_diff_t *diff);

/* ── SPSC ring buffer for diffs ───────────────────────────────────────── */

#define DIFF_RING_CAP 512  /* must be power of 2 */

typedef struct {
    block_diff_t   slots[DIFF_RING_CAP];
    atomic_size_t  head;    /* written by producer only */
    atomic_size_t  tail;    /* written by consumer only */
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

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_STATE_HISTORY_H */
