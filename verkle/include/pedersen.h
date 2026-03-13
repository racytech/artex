#ifndef PEDERSEN_H
#define PEDERSEN_H

#include "banderwagon.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Pedersen Vector Commitment
 *
 * Commits to a vector of up to 256 scalars using a fixed CRS
 * (Common Reference String) of 256 Banderwagon points:
 *
 *   C = v_0 * G_0 + v_1 * G_1 + ... + v_255 * G_255
 *
 * Key property — O(1) update:
 *   C_new = C_old + (v'_i - v_i) * G_i
 *
 * CRS generated deterministically from seed "eth_verkle_oct_2021"
 * using SHA256 + try-and-increment (Ethereum verkle spec).
 */

#define PEDERSEN_WIDTH 256

/** Initialize CRS points (idempotent, thread-unsafe).
 *  Called automatically by pedersen_* functions, but can be
 *  called explicitly at startup. */
void pedersen_init(void);

/** Get CRS point G_i (0 <= index < 256). */
const banderwagon_point_t *pedersen_get_crs(size_t index);

/** Commit to a vector of scalars: out = sum(scalars[i] * G_i).
 *  Each scalar is 32 bytes little-endian. count <= 256.
 *  Unused slots (count < 256) are implicitly zero. */
void pedersen_commit(banderwagon_point_t *out,
                     const uint8_t (*scalars)[32],
                     size_t count);

/** Update commitment: out = old + delta * G_index.
 *  delta is 32 bytes little-endian. */
void pedersen_update(banderwagon_point_t *out,
                     const banderwagon_point_t *old_commitment,
                     size_t index,
                     const uint8_t delta[32]);

/** Compute delta = new_val - old_val (mod Fr) for use in update.
 *  All values are 32 bytes little-endian. */
void pedersen_scalar_diff(uint8_t delta[32],
                          const uint8_t new_val[32],
                          const uint8_t old_val[32]);

#ifdef __cplusplus
}
#endif

#endif /* PEDERSEN_H */
