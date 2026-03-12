#ifndef ENGINE_STORE_H
#define ENGINE_STORE_H

#include "engine_types.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine Store — In-memory block storage and fork choice state.
 *
 * Stores validated execution payloads by block hash.
 * Tracks fork choice (head, safe, finalized).
 * Maintains a block hash ring buffer for BLOCKHASH opcode.
 * Prunes old blocks when finalized advances.
 */

#define ENGINE_STORE_MAX_BLOCKS 1024

typedef struct {
    execution_payload_t payload;
    bool                occupied;
    bool                valid;      /* true if execution was successful */
} engine_stored_block_t;

typedef struct {
    /* Block storage (hash-indexed via linear scan — good enough for ~1K) */
    engine_stored_block_t blocks[ENGINE_STORE_MAX_BLOCKS];
    int                   block_count;

    /* Fork choice state */
    uint8_t head_hash[32];
    uint8_t safe_hash[32];
    uint8_t finalized_hash[32];
    bool    has_head;

    /* Block number ring buffer for BLOCKHASH (last 256) */
    uint8_t  blockhash_ring[256][32];
    uint64_t blockhash_lowest;  /* lowest block number recorded */
    uint64_t blockhash_highest; /* highest block number recorded */
    bool     blockhash_active;  /* true after first record */

    /* Pending payload build (for getPayload) */
    bool                has_pending;
    uint64_t            pending_payload_id;
    execution_payload_t pending_payload;
} engine_store_t;

/** Create a new empty store. */
engine_store_t *engine_store_create(void);

/** Destroy the store and free all payloads. */
void engine_store_destroy(engine_store_t *store);

/** Store a validated payload. Returns false if store full. */
bool engine_store_put(engine_store_t *store,
                      const execution_payload_t *payload,
                      bool valid);

/** Look up a payload by block hash. Returns NULL if not found. */
const engine_stored_block_t *engine_store_get(const engine_store_t *store,
                                               const uint8_t hash[32]);

/** Check if a block hash is known. */
bool engine_store_has(const engine_store_t *store, const uint8_t hash[32]);

/** Update fork choice pointers. */
void engine_store_set_forkchoice(engine_store_t *store,
                                  const uint8_t head[32],
                                  const uint8_t safe[32],
                                  const uint8_t finalized[32]);

/** Record a block hash in the ring buffer. */
void engine_store_record_blockhash(engine_store_t *store,
                                    uint64_t block_number,
                                    const uint8_t hash[32]);

/** Look up block hash by number (for BLOCKHASH opcode). Returns false if not in range. */
bool engine_store_get_blockhash(const engine_store_t *store,
                                 uint64_t block_number,
                                 uint8_t out[32]);

/** Set pending payload for getPayload. */
void engine_store_set_pending(engine_store_t *store,
                               uint64_t payload_id,
                               const execution_payload_t *payload);

/** Get and clear pending payload. Returns false if no pending build. */
bool engine_store_take_pending(engine_store_t *store,
                                uint64_t payload_id,
                                execution_payload_t *out);

/** Prune blocks older than finalized. */
void engine_store_prune(engine_store_t *store);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_STORE_H */
