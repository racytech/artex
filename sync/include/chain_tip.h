#ifndef CHAIN_TIP_H
#define CHAIN_TIP_H

#include <stdint.h>
#include <stdbool.h>
#include "hash.h"
#include "engine_types.h"
#include "engine_store.h"
#include "sync.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Chain Tip — Orchestration layer between Engine API and Sync Engine.
 *
 * Owns fork choice state (head, safe, finalized) and mediates all
 * block execution.  The Engine API handlers delegate to chain_tip
 * instead of calling block_execute() directly.
 *
 * Two modes of operation:
 *   1. Batch mode (era1 replay): blocks fed via sync_execute_block(),
 *      root validated at checkpoint boundaries, background MPT flush.
 *   2. Live mode (CL sync): blocks fed via chain_tip_new_payload(),
 *      root validated per block, synchronous MPT flush.
 *
 * The transition happens when era1 replay reaches The Merge and the
 * CL begins sending newPayload + forkchoiceUpdated.
 */

/* =========================================================================
 * Configuration
 * ========================================================================= */

typedef struct {
    /* Sync engine config (chain_tip creates the sync engine) */
    sync_config_t sync_config;

    /* Fork choice persistence (NULL = no persistence) */
    const char *tip_state_path;

    /* Paris (The Merge) block number — triggers transition to live mode */
    uint64_t merge_block;

    /* Verbose logging */
    bool verbose;
} chain_tip_config_t;

/* =========================================================================
 * Opaque Handle
 * ========================================================================= */

typedef struct chain_tip chain_tip_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/**
 * Create chain tip. Creates the underlying sync engine.
 * Resumes from checkpoint if available. Loads persisted fork choice state.
 * Returns NULL on failure.
 */
chain_tip_t *chain_tip_create(const chain_tip_config_t *config);

/**
 * Destroy chain tip. Persists fork choice state, saves final checkpoint,
 * destroys the underlying sync engine.
 */
void chain_tip_destroy(chain_tip_t *tip);

/* =========================================================================
 * Genesis
 * ========================================================================= */

/**
 * Load genesis state. Delegates to sync_load_genesis().
 * Must be called before any block execution if not resuming.
 */
bool chain_tip_load_genesis(chain_tip_t *tip, const char *genesis_json_path,
                             const hash_t *genesis_hash);

/* =========================================================================
 * Era1 Batch Mode (pre-merge)
 * ========================================================================= */

/**
 * Execute a decoded block in batch mode (era1 replay).
 * Delegates directly to sync_execute_block(). Root validation and
 * checkpointing happen at the sync engine's configured interval.
 */
bool chain_tip_execute_batch(chain_tip_t *tip,
                              const block_header_t *header,
                              const block_body_t *body,
                              const hash_t *block_hash,
                              sync_block_result_t *result);

/**
 * Transition from batch mode to live CL sync.
 * Flushes all state to disk, saves checkpoint, switches sync engine
 * to per-block validation mode. Call after era1 replay reaches merge_block.
 */
bool chain_tip_go_live(chain_tip_t *tip);

/**
 * Returns true if the chain tip is in live (CL-driven) mode.
 */
bool chain_tip_is_live(const chain_tip_t *tip);

/* =========================================================================
 * Live Mode — Engine API (post-merge)
 * ========================================================================= */

/**
 * Process engine_newPayloadV1..V4.
 *
 * Validates the payload, executes the block against the sync engine,
 * verifies state root, and returns a payload_status_t for the JSON-RPC
 * response.
 *
 * The payload is stored in the engine_store for future reference
 * (e.g., reorgs, getPayload).
 *
 * @param tip       Chain tip handle
 * @param payload   Decoded execution payload from JSON-RPC
 * @param version   Engine API version (V1..V4)
 * @return          Payload status (VALID, INVALID, SYNCING, etc.)
 */
payload_status_t chain_tip_new_payload(chain_tip_t *tip,
                                        const execution_payload_t *payload,
                                        engine_version_t version);

/**
 * Process engine_forkchoiceUpdatedV1..V3.
 *
 * Updates head/safe/finalized block pointers. If head changes and is
 * not a descendant of the current head, triggers a reorg.
 *
 * If payload_attributes is non-NULL, starts building a new block
 * (for validators — sets pending payload for getPayload).
 *
 * @param tip       Chain tip handle
 * @param fc        Fork choice state from CL
 * @param attrs     Payload attributes (NULL if not building a block)
 * @param version   Engine API version (V1..V3)
 * @return          Payload status for the current head
 */
payload_status_t chain_tip_forkchoice_updated(chain_tip_t *tip,
                                               const forkchoice_state_t *fc,
                                               const payload_attributes_t *attrs,
                                               engine_version_t version);

/* =========================================================================
 * Accessors
 * ========================================================================= */

/** Get the underlying sync engine (for direct access when needed). */
sync_t *chain_tip_get_sync(const chain_tip_t *tip);

/** Get the engine store (for getPayload, block lookups). */
engine_store_t *chain_tip_get_store(const chain_tip_t *tip);

/** Get current sync status from the underlying engine. */
sync_status_t chain_tip_get_status(const chain_tip_t *tip);

/** Force a checkpoint save. */
bool chain_tip_checkpoint(chain_tip_t *tip);

/** Get the current head block number (0 if no head set). */
uint64_t chain_tip_head_number(const chain_tip_t *tip);

/** Get the current head block hash. Returns false if no head. */
bool chain_tip_head_hash(const chain_tip_t *tip, hash_t *out);

/** Get the finalized block number (0 if not yet finalized). */
uint64_t chain_tip_finalized_number(const chain_tip_t *tip);

#ifdef __cplusplus
}
#endif

#endif /* CHAIN_TIP_H */
