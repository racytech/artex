#ifndef WITNESS_GAS_H
#define WITNESS_GAS_H

#include "mem_art.h"
#include <stdint.h>
#include <stdbool.h>

/**
 * EIP-4762 Verkle Witness Gas Tracker
 *
 * Tracks accessed/edited subtrees and leaves to compute witness gas costs.
 * Each access_event charges gas only on FIRST access of a stem or key.
 * Sets are per-block (reset by witness_gas_reset at block boundaries).
 * NOT journaled — witness gas is not reverted on REVERT (per EIP spec).
 */

/* EIP-4762 witness gas constants */
#define WITNESS_BRANCH_COST   1900
#define WITNESS_CHUNK_COST     200
#define SUBTREE_EDIT_COST     3000
#define CHUNK_EDIT_COST        500
#define CHUNK_FILL_COST       6200

typedef struct {
    mem_art_t accessed_subtrees;  /* key = stem[31] */
    mem_art_t accessed_leaves;    /* key = verkle_key[32] */
    mem_art_t edited_subtrees;    /* key = stem[31] */
    mem_art_t edited_leaves;      /* key = verkle_key[32] */
} witness_gas_t;

/** Initialize all four tracking sets. */
void witness_gas_init(witness_gas_t *wg);

/** Destroy all four tracking sets. */
void witness_gas_destroy(witness_gas_t *wg);

/** Reset all four tracking sets (per-transaction boundary). */
void witness_gas_reset(witness_gas_t *wg);

/**
 * Compute witness gas for an access event.
 *
 * @param wg             Witness gas tracker
 * @param key            Full 32-byte verkle key (key[0:31] = stem)
 * @param is_write       true if this is a write access
 * @param value_was_empty true if the tree value at this key was zero/absent
 *                        (only relevant for writes — triggers CHUNK_FILL_COST)
 * @return Gas to charge (0 if all components already accessed/edited)
 */
uint64_t witness_gas_access_event(witness_gas_t *wg,
                                   const uint8_t key[32],
                                   bool is_write,
                                   bool value_was_empty);

#endif /* WITNESS_GAS_H */
