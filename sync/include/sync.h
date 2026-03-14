#ifndef SYNC_H
#define SYNC_H

#include <stdint.h>
#include <stdbool.h>
#include "hash.h"
#include "block.h"
#include "fork.h"
#include "evm_state.h"

/**
 * Sync Engine — reusable block execution + validation + checkpointing.
 *
 * Owns the full state lifecycle (evm_state, evm, verkle_state).
 * Callers provide decoded block headers and bodies from any source
 * (era1 files, Engine API, p2p, etc).
 *
 * Usage:
 *   1. sync_create(config)              — create engine, resume from checkpoint
 *   2. sync_load_genesis(path, hash)    — load genesis (skip if resumed)
 *   3. sync_execute_block(...)          — execute + validate per block
 *   4. sync_checkpoint()                — force save (e.g. on SIGINT)
 *   5. sync_destroy()                   — final checkpoint + cleanup
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

    /* Checkpoint file path (NULL = no checkpointing) */
    const char *checkpoint_path;

    /* Behavior */
    uint32_t checkpoint_interval;   /* auto-save every N blocks (0 = never) */
    bool     validate_state_root;   /* compare computed root against header */
#ifdef ENABLE_DEBUG
    bool     no_evict;              /* skip cache eviction at checkpoints */
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
} sync_block_result_t;

typedef struct {
    uint64_t last_block;
    uint64_t total_gas;
    uint64_t blocks_ok;
    uint64_t blocks_fail;
} sync_status_t;

/* Opaque handle */
typedef struct sync sync_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a sync engine.
 * If checkpoint_path is set and a valid checkpoint exists, state is restored
 * automatically. Returns NULL on failure.
 */
sync_t *sync_create(const sync_config_t *config);

/**
 * Destroy sync engine. Saves a final checkpoint if blocks were processed
 * since the last checkpoint and blocks_fail == 0.
 */
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
 * Must be called before execute_block if not resuming from checkpoint.
 * Returns false on parse error or if genesis was already loaded/resumed.
 */
bool sync_load_genesis(sync_t *sync, const char *genesis_json_path,
                       const hash_t *genesis_hash);

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
// Checkpoint
// ============================================================================

/** Force a checkpoint save now. Returns false if save fails. */
bool sync_checkpoint(sync_t *sync);

/** Returns the block number from which we resumed (0 if fresh start). */
uint64_t sync_resumed_from(const sync_t *sync);

// ============================================================================
// Status
// ============================================================================

/** Get current sync status (counters, last block). */
sync_status_t sync_get_status(const sync_t *sync);

/** Get cache/store statistics from the underlying evm_state. */
evm_state_stats_t sync_get_state_stats(const sync_t *sync);

#ifdef ENABLE_DEBUG
struct evm_state *sync_get_state(const sync_t *sync);
#endif

#endif /* SYNC_H */
