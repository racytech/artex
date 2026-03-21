#ifndef SYNC_H
#define SYNC_H

#include <stdint.h>
#include <stdbool.h>
#include "hash.h"
#include "block.h"
#include "block_executor.h"
#include "fork.h"
#include "evm_state.h"

/**
 * Sync Engine — block execution + validation.
 *
 * Owns the full state lifecycle (evm_state, evm, verkle_state).
 * Callers provide decoded block headers and bodies from any source
 * (era1 files, Engine API, p2p, etc).
 *
 * Usage:
 *   1. sync_create(config)              — create engine
 *   2. sync_load_genesis(path, hash)    — load genesis state
 *   3. sync_execute_block(...)          — execute + validate per block
 *   4. sync_destroy()                   — cleanup
 */

// ============================================================================
// Types
// ============================================================================

typedef struct {
    const chain_config_t *chain_config;   /* mainnet, sepolia, etc. (NOT owned) */

    /* Persistent store paths (NULL = disable that backend) */
    const char *verkle_value_dir;
    const char *verkle_commit_dir;
    const char *mpt_path;
    const char *code_store_path;   /* contract bytecode store (NULL = no code persistence) */

    /* State history (per-block diff tracking, NULL = disabled) */
    const char *history_dir;

    /* Verkle builder (background verkle state construction, NULL = disabled) */
    const char *verkle_builder_value_dir;
    const char *verkle_builder_commit_dir;

    /* Behavior */
    uint32_t checkpoint_interval;   /* validate root + flush every N blocks (0 = never) */
    bool     validate_state_root;   /* compare computed root against header */
#ifdef ENABLE_DEBUG
    bool     no_evict;              /* skip cache eviction at interval boundaries */
#endif
} sync_config_t;

typedef enum {
    SYNC_OK = 0,
    SYNC_GAS_MISMATCH,
    SYNC_ROOT_MISMATCH,
} sync_error_t;

typedef struct {
    bool         ok;            /* true if gas + root matched */
    sync_error_t error;         /* type of mismatch (if !ok) */
    uint64_t     gas_used;      /* total gas consumed in block */
    size_t       tx_count;      /* number of transactions */
    uint64_t     expected_gas;  /* from header */
    uint64_t     actual_gas;    /* from execution */
    hash_t       expected_root; /* from header */
    hash_t       actual_root;   /* from computation */
    uint32_t     transfer_count;/* simple ETH transfers */
    uint32_t     call_count;    /* contract calls / creates */

    /* Per-tx receipts (only populated on gas mismatch, caller must free) */
    tx_receipt_t *receipts;
    size_t        receipt_count;
} sync_block_result_t;

typedef struct {
    uint64_t last_block;
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
} sync_status_t;

/* Opaque handles */
typedef struct sync sync_t;
struct evm;
struct evm_state;

// ============================================================================
// Lifecycle
// ============================================================================

/** Create a sync engine. Returns NULL on failure. */
sync_t *sync_create(const sync_config_t *config);

/** Destroy sync engine. */
void sync_destroy(sync_t *sync);

// ============================================================================
// Genesis
// ============================================================================

/**
 * Load genesis state from a JSON file.
 * Format: { "addr_hex": { "balance": "0x..." }, ... }
 *
 * genesis_hash: stored in block_hashes[0] for BLOCKHASH opcode.
 *               Pass NULL if unknown.
 *
 * Must be called before execute_block.
 * Returns false on parse error or if genesis was already loaded.
 */
bool sync_load_genesis(sync_t *sync, const char *genesis_json_path,
                       const hash_t *genesis_hash);

// ============================================================================
// Resume from existing state
// ============================================================================

/**
 * Resume from an existing MPT state at `last_block`.
 * Skips genesis loading — assumes the MPT files already contain valid state.
 * Populates the block hash ring from the provided array.
 *
 * block_hashes: array of up to 256 hashes for blocks leading up to last_block.
 *               Entry [i] = hash of block (last_block - count + 1 + i).
 * count: number of hashes in the array (at most 256).
 *
 * Must be called instead of sync_load_genesis.
 */
bool sync_resume(sync_t *sync, uint64_t last_block,
                 const hash_t *block_hashes, size_t count);

// ============================================================================
// Block Execution
// ============================================================================

/**
 * Execute a single decoded block.
 *
 * block_hash: keccak256 of the RLP-encoded header (for BLOCKHASH ring buffer).
 *
 * Fills *result with execution outcome. Returns false only on fatal/internal
 * error (not on validation mismatch — check result->ok for that).
 */
bool sync_execute_block(sync_t *sync,
                        const block_header_t *header,
                        const block_body_t *body,
                        const hash_t *block_hash,
                        sync_block_result_t *result);

// ============================================================================
// Live Mode (per-block validation for CL sync)
// ============================================================================

/**
 * Execute a block with immediate per-block validation.
 *
 * Unlike sync_execute_block() (which defers root validation to interval
 * boundaries), this validates gas + state root immediately and flushes
 * state synchronously. Designed for CL-driven sync where each newPayload
 * needs a VALID/INVALID response.
 */
bool sync_execute_block_live(sync_t *sync,
                              const block_header_t *header,
                              const block_body_t *body,
                              const hash_t *block_hash,
                              sync_block_result_t *result);

/** Switch between batch and live modes. */
void sync_set_live_mode(sync_t *sync, bool live);

/** Get the EVM state instance owned by the sync engine. */
struct evm_state *sync_get_state(const sync_t *sync);

// ============================================================================
// Query
// ============================================================================

/**
 * Get a block hash from the 256-entry ring buffer.
 * Returns true if the block number is within the window.
 */
bool sync_get_block_hash(const sync_t *sync, uint64_t block_number, hash_t *out);

/** Get current sync status (counters, last block). */
sync_status_t sync_get_status(const sync_t *sync);

/** Get cache/store statistics from the underlying evm_state. */
evm_state_stats_t sync_get_state_stats(const sync_t *sync);

/** History stats (only valid when ENABLE_HISTORY is on). */
typedef struct {
    uint64_t blocks;
    double   disk_mb;
} sync_history_stats_t;

/** Get state history disk usage stats. Returns zeros if history is disabled. */
sync_history_stats_t sync_get_history_stats(const sync_t *sync);

/** Truncate state history to keep only blocks up to last_block. */
void sync_truncate_history(sync_t *sync, uint64_t last_block);

#endif /* SYNC_H */
