/*
 * Engine Store — In-memory block storage and fork choice state.
 *
 * Simple linear-scan storage. At ~1 block/12s, even 1024 blocks
 * covers ~3.4 hours which is more than enough for fork choice.
 */

#include "engine_store.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

engine_store_t *engine_store_create(void) {
    engine_store_t *store = calloc(1, sizeof(*store));
    return store;
}

void engine_store_destroy(engine_store_t *store) {
    if (!store) return;
    for (int i = 0; i < ENGINE_STORE_MAX_BLOCKS; i++) {
        if (store->blocks[i].occupied)
            execution_payload_free(&store->blocks[i].payload);
    }
    if (store->has_pending)
        execution_payload_free(&store->pending_payload);
    free(store);
}

/* =========================================================================
 * Block Storage
 * ========================================================================= */

static int find_slot(const engine_store_t *store, const uint8_t hash[32]) {
    for (int i = 0; i < ENGINE_STORE_MAX_BLOCKS; i++) {
        if (store->blocks[i].occupied &&
            memcmp(store->blocks[i].payload.block_hash, hash, 32) == 0)
            return i;
    }
    return -1;
}

static int find_free_slot(const engine_store_t *store) {
    for (int i = 0; i < ENGINE_STORE_MAX_BLOCKS; i++) {
        if (!store->blocks[i].occupied)
            return i;
    }
    return -1;
}

/* Check if a hash matches any of the protected fork choice hashes. */
static bool is_protected(const engine_store_t *store, const uint8_t hash[32]) {
    if (!store->has_head) return false;
    return memcmp(hash, store->head_hash, 32) == 0 ||
           memcmp(hash, store->safe_hash, 32) == 0 ||
           memcmp(hash, store->finalized_hash, 32) == 0;
}

/* Evict the oldest non-protected block. Returns freed slot index or -1. */
static int evict_oldest(engine_store_t *store) {
    int best = -1;
    uint64_t best_number = UINT64_MAX;
    for (int i = 0; i < ENGINE_STORE_MAX_BLOCKS; i++) {
        if (!store->blocks[i].occupied) continue;
        if (is_protected(store, store->blocks[i].payload.block_hash)) continue;
        if (store->blocks[i].payload.block_number < best_number) {
            best_number = store->blocks[i].payload.block_number;
            best = i;
        }
    }
    if (best < 0) return -1;

    execution_payload_free(&store->blocks[best].payload);
    store->blocks[best].occupied = false;
    store->block_count--;
    return best;
}

bool engine_store_put(engine_store_t *store,
                      const execution_payload_t *payload,
                      bool valid) {
    if (!store || !payload) return false;

    /* Check if already stored */
    int existing = find_slot(store, payload->block_hash);
    if (existing >= 0) {
        store->blocks[existing].valid = valid;
        return true;
    }

    int slot = find_free_slot(store);
    if (slot < 0) {
        /* Store full — evict oldest non-protected block */
        slot = evict_oldest(store);
        if (slot < 0) {
            fprintf(stderr, "engine_store: full, all blocks protected\n");
            return false;
        }
    }

    /* Deep copy — caller retains ownership and must free independently */
    execution_payload_deep_copy(&store->blocks[slot].payload, payload);
    store->blocks[slot].occupied = true;
    store->blocks[slot].valid = valid;
    store->block_count++;
    return true;
}

const engine_stored_block_t *engine_store_get(const engine_store_t *store,
                                               const uint8_t hash[32]) {
    if (!store) return NULL;
    int idx = find_slot(store, hash);
    return idx >= 0 ? &store->blocks[idx] : NULL;
}

bool engine_store_has(const engine_store_t *store, const uint8_t hash[32]) {
    return find_slot(store, hash) >= 0;
}

/* =========================================================================
 * Fork Choice
 * ========================================================================= */

void engine_store_set_forkchoice(engine_store_t *store,
                                  const uint8_t head[32],
                                  const uint8_t safe[32],
                                  const uint8_t finalized[32]) {
    if (!store) return;
    memcpy(store->head_hash, head, 32);
    memcpy(store->safe_hash, safe, 32);
    memcpy(store->finalized_hash, finalized, 32);
    store->has_head = true;
}

/* =========================================================================
 * Block Hash Ring Buffer
 * ========================================================================= */

void engine_store_record_blockhash(engine_store_t *store,
                                    uint64_t block_number,
                                    const uint8_t hash[32]) {
    if (!store) return;
    int idx = (int)(block_number % 256);
    memcpy(store->blockhash_ring[idx], hash, 32);

    if (!store->blockhash_active) {
        store->blockhash_lowest = block_number;
        store->blockhash_highest = block_number;
        store->blockhash_active = true;
    } else {
        if (block_number > store->blockhash_highest)
            store->blockhash_highest = block_number;
        /* Slide lowest forward if we've wrapped past 256 */
        if (store->blockhash_highest - store->blockhash_lowest >= 256)
            store->blockhash_lowest = store->blockhash_highest - 255;
    }
}

bool engine_store_get_blockhash(const engine_store_t *store,
                                 uint64_t block_number,
                                 uint8_t out[32]) {
    if (!store || !store->blockhash_active) return false;
    if (block_number < store->blockhash_lowest) return false;
    if (block_number > store->blockhash_highest) return false;

    int idx = (int)(block_number % 256);
    memcpy(out, store->blockhash_ring[idx], 32);
    return true;
}

/* =========================================================================
 * Pending Payload
 * ========================================================================= */

void engine_store_set_pending(engine_store_t *store,
                               uint64_t payload_id,
                               const execution_payload_t *payload) {
    if (!store || !payload) return;
    if (store->has_pending)
        execution_payload_free(&store->pending_payload);
    execution_payload_deep_copy(&store->pending_payload, payload);
    store->pending_payload_id = payload_id;
    store->has_pending = true;
}

bool engine_store_take_pending(engine_store_t *store,
                                uint64_t payload_id,
                                execution_payload_t *out) {
    if (!store || !store->has_pending) return false;
    if (store->pending_payload_id != payload_id) return false;

    *out = store->pending_payload;
    memset(&store->pending_payload, 0, sizeof(store->pending_payload));
    store->has_pending = false;
    return true;
}

/* =========================================================================
 * Pruning
 * ========================================================================= */

void engine_store_prune(engine_store_t *store) {
    if (!store || !store->has_head) return;

    /* Find finalized block number */
    int fin_idx = find_slot(store, store->finalized_hash);
    if (fin_idx < 0) return;
    uint64_t fin_number = store->blocks[fin_idx].payload.block_number;

    /* Remove blocks older than finalized (keep finalized itself) */
    for (int i = 0; i < ENGINE_STORE_MAX_BLOCKS; i++) {
        if (store->blocks[i].occupied &&
            store->blocks[i].payload.block_number < fin_number) {
            execution_payload_free(&store->blocks[i].payload);
            store->blocks[i].occupied = false;
            store->block_count--;
        }
    }
}
