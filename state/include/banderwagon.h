#ifndef BANDERWAGON_H
#define BANDERWAGON_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "blst.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Banderwagon Curve Arithmetic
 *
 * Banderwagon is a prime-order group constructed as a quotient of the
 * Bandersnatch curve (twisted Edwards form: ax^2 + y^2 = 1 + dx^2y^2).
 * It identifies points (x,y) and (-x,-y), yielding effective prime order.
 *
 * The base field Fp is the BLS12-381 scalar field, so we use blst_fr
 * for all coordinate arithmetic (optimized asm).
 *
 * Parameters:
 *   a = -5
 *   d = 0x6389c12633c267cbc66e3bf86be3b6d8cb66677177e54f92b369f2f5188d58e7
 *   Fp = BLS12-381 scalar field (255 bits)
 *   Fr = Bandersnatch subgroup order (253 bits)
 */

/* =========================================================================
 * Types
 * ========================================================================= */

/** Base field element (Bandersnatch Fp = BLS12-381 scalar field). */
typedef blst_fr fp_t;

/**
 * Curve point in extended twisted Edwards coordinates.
 *   affine: x = X/Z, y = Y/Z
 *   auxiliary: T = X*Y/Z
 */
typedef struct {
    fp_t X, Y, Z, T;
} banderwagon_point_t;

/* =========================================================================
 * Constants
 * ========================================================================= */

extern fp_t FP_ZERO;
extern fp_t FP_ONE;
extern fp_t BANDERSNATCH_A;   /* -5 mod p */
extern fp_t BANDERSNATCH_D;

extern banderwagon_point_t BANDERWAGON_IDENTITY;   /* (0, 1, 1, 0) */
extern banderwagon_point_t BANDERWAGON_GENERATOR;

/** Bandersnatch prime-order subgroup modulus r (32 bytes, little-endian). */
extern const uint8_t FR_MODULUS[32];

/** Initialize all constants (idempotent, thread-unsafe).
 *  Called automatically by banderwagon_* functions, but can be
 *  called explicitly at startup to ensure constants are ready. */
void banderwagon_init(void);

/* =========================================================================
 * Fp Wrappers (thin wrappers over blst_fr)
 * ========================================================================= */

void fp_add(fp_t *out, const fp_t *a, const fp_t *b);
void fp_sub(fp_t *out, const fp_t *a, const fp_t *b);
void fp_mul(fp_t *out, const fp_t *a, const fp_t *b);
void fp_sqr(fp_t *out, const fp_t *a);
void fp_neg(fp_t *out, const fp_t *a);
void fp_inv(fp_t *out, const fp_t *a);
bool fp_eq(const fp_t *a, const fp_t *b);
bool fp_is_zero(const fp_t *a);

void fp_from_uint64(fp_t *out, uint64_t v);
void fp_from_bytes_le(fp_t *out, const uint8_t bytes[32]);
void fp_to_bytes_le(uint8_t out[32], const fp_t *a);
void fp_from_bytes_be(fp_t *out, const uint8_t bytes[32]);
void fp_to_bytes_be(uint8_t out[32], const fp_t *a);

/* =========================================================================
 * Point Operations
 * ========================================================================= */

/** Unified addition on extended twisted Edwards coordinates. */
void banderwagon_add(banderwagon_point_t *out,
                     const banderwagon_point_t *p,
                     const banderwagon_point_t *q);

/** Dedicated doubling (faster than add(p, p)). */
void banderwagon_double(banderwagon_point_t *out,
                        const banderwagon_point_t *p);

/** Negation: in Banderwagon, neg(x,y) = (-x, y) in affine. */
void banderwagon_neg(banderwagon_point_t *out,
                     const banderwagon_point_t *p);

/** Scalar multiplication: out = scalar * p.
 *  scalar is 32 bytes little-endian. */
void banderwagon_scalar_mul(banderwagon_point_t *out,
                            const banderwagon_point_t *p,
                            const uint8_t scalar[32]);

/** Multi-scalar multiplication: out = sum(scalars[i] * points[i]).
 *  Each scalar is 32 bytes little-endian. Naive impl for now. */
void banderwagon_msm(banderwagon_point_t *out,
                     const banderwagon_point_t *points,
                     const uint8_t (*scalars)[32],
                     size_t count);

/** Check if point is the identity element. */
bool banderwagon_is_identity(const banderwagon_point_t *p);

/**
 * Banderwagon equality check.
 * Points (x,y) and (-x,-y) are identified, so:
 *   P1 == P2  iff  X1*Y2 == X2*Y1  (in projective coords)
 */
bool banderwagon_eq(const banderwagon_point_t *p,
                    const banderwagon_point_t *q);

/**
 * Map curve point to scalar field element.
 * Returns X/Y (base field division), then reduced mod Fr.
 * Output is 32 bytes little-endian.
 * Identity maps to 0.
 */
void banderwagon_map_to_field(uint8_t out[32],
                              const banderwagon_point_t *p);

/**
 * Serialize point to 32 bytes (compressed).
 * Format: sign(Y) * X in big-endian.
 */
void banderwagon_serialize(uint8_t out[32],
                           const banderwagon_point_t *p);

/**
 * Deserialize 32 bytes to point.
 * Returns false if bytes don't represent a valid Banderwagon point.
 */
bool banderwagon_deserialize(banderwagon_point_t *out,
                             const uint8_t bytes[32]);

/** Check that point lies on the Bandersnatch curve. */
bool banderwagon_is_on_curve(const banderwagon_point_t *p);

#ifdef __cplusplus
}
#endif

#endif /* BANDERWAGON_H */
