#ifndef VERKLE_STATE_H
#define VERKLE_STATE_H

#include "verkle.h"
#include "verkle_flat.h"
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
 * Backend-agnostic: works with either in-memory tree or disk-backed flat
 * updater. The typed API is identical regardless of backend.
 */

typedef enum {
    VS_BACKEND_TREE,
    VS_BACKEND_FLAT,
} vs_backend_type_t;

typedef struct {
    vs_backend_type_t type;
    union {
        verkle_tree_t *tree;   /* owned, for VS_BACKEND_TREE */
        verkle_flat_t *flat;   /* owned, for VS_BACKEND_FLAT */
    };
} verkle_state_t;

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

/** Create an empty verkle state (in-memory tree backend). */
verkle_state_t *verkle_state_create(void);

/** Create a flat-backed verkle state (new stores). */
verkle_state_t *verkle_state_create_flat(const char *value_dir,
                                          const char *commit_dir);

/** Open an existing flat-backed verkle state. */
verkle_state_t *verkle_state_open_flat(const char *value_dir,
                                        const char *commit_dir);

/** Destroy state and free backend. */
void verkle_state_destroy(verkle_state_t *vs);

/* =========================================================================
 * Backend Accessors
 * ========================================================================= */

/** Get the underlying tree (NULL if flat backend). */
verkle_tree_t *verkle_state_get_tree(verkle_state_t *vs);

/** Get the underlying flat updater (NULL if tree backend). */
verkle_flat_t *verkle_state_get_flat(verkle_state_t *vs);

/* =========================================================================
 * Block Operations (flat backend; no-op for tree backend)
 * ========================================================================= */

/** Begin a new block. Required before set() for flat backend. */
bool verkle_state_begin_block(verkle_state_t *vs, uint64_t block_number);

/** Commit the current block (flat: groups + incremental update). */
bool verkle_state_commit_block(verkle_state_t *vs);

/** Revert the current/most recent block. */
bool verkle_state_revert_block(verkle_state_t *vs);

/** Flush flat stores to disk. No-op for tree backend. */
void verkle_state_sync(verkle_state_t *vs);

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
 * Code (bytecode split into 32-byte chunks, domain 3)
 * ========================================================================= */

/** Store contract bytecode (splits into 32-byte chunks, sets code_size).
 *  Does NOT set code_hash — caller must set it separately.
 *  Returns false on error. */
bool verkle_state_set_code(verkle_state_t *vs,
                           const uint8_t addr[20],
                           const uint8_t *bytecode,
                           uint64_t len);

/** Read contract bytecode into `out` (max `max_len` bytes).
 *  Returns actual code length (0 if no code). */
uint64_t verkle_state_get_code(verkle_state_t *vs,
                               const uint8_t addr[20],
                               uint8_t *out,
                               uint64_t max_len);

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
