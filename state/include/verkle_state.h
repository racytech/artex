#ifndef VERKLE_STATE_H
#define VERKLE_STATE_H

#include "verkle.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verkle State — Typed Interface over Verkle Tree
 *
 * Provides account-level operations (nonce, balance, code, storage) that
 * map to raw 32-byte key-value operations in the verkle tree via
 * Pedersen hash key derivation.
 *
 * This is the interface the execution layer calls.
 */

typedef struct {
    verkle_tree_t *tree;   /* owned */
} verkle_state_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create an empty verkle state. */
verkle_state_t *verkle_state_create(void);

/** Destroy state and free tree. */
void verkle_state_destroy(verkle_state_t *vs);

/* =========================================================================
 * Version (uint8 → 32-byte LE slot)
 * ========================================================================= */

uint8_t verkle_state_get_version(verkle_state_t *vs,
                                 const uint8_t addr[20]);
void    verkle_state_set_version(verkle_state_t *vs,
                                 const uint8_t addr[20],
                                 uint8_t version);

/* =========================================================================
 * Nonce (uint64 → 32-byte LE slot)
 * ========================================================================= */

uint64_t verkle_state_get_nonce(verkle_state_t *vs,
                                const uint8_t addr[20]);
void     verkle_state_set_nonce(verkle_state_t *vs,
                                const uint8_t addr[20],
                                uint64_t nonce);

/* =========================================================================
 * Balance (raw 32-byte LE value)
 * ========================================================================= */

void verkle_state_get_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              uint8_t balance[32]);
void verkle_state_set_balance(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t balance[32]);

/* =========================================================================
 * Code Hash (raw 32 bytes)
 * ========================================================================= */

void verkle_state_get_code_hash(verkle_state_t *vs,
                                const uint8_t addr[20],
                                uint8_t hash[32]);
void verkle_state_set_code_hash(verkle_state_t *vs,
                                const uint8_t addr[20],
                                const uint8_t hash[32]);

/* =========================================================================
 * Code Size (uint64 → 32-byte LE slot)
 * ========================================================================= */

uint64_t verkle_state_get_code_size(verkle_state_t *vs,
                                    const uint8_t addr[20]);
void     verkle_state_set_code_size(verkle_state_t *vs,
                                    const uint8_t addr[20],
                                    uint64_t size);

/* =========================================================================
 * Storage (raw 32-byte values, slot is uint256 LE)
 * ========================================================================= */

void verkle_state_get_storage(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t slot[32],
                              uint8_t value[32]);
void verkle_state_set_storage(verkle_state_t *vs,
                              const uint8_t addr[20],
                              const uint8_t slot[32],
                              const uint8_t value[32]);

/* =========================================================================
 * Account Existence
 * ========================================================================= */

/** Returns true if any header field is set for this address. */
bool verkle_state_exists(verkle_state_t *vs, const uint8_t addr[20]);

/* =========================================================================
 * Root
 * ========================================================================= */

/** Get the state root hash (serialized root commitment, 32 bytes). */
void verkle_state_root_hash(const verkle_state_t *vs, uint8_t out[32]);

#ifdef __cplusplus
}
#endif

#endif /* VERKLE_STATE_H */
