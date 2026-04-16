/**
 * BN256/alt_bn128 Elliptic Curve Arithmetic
 *
 * Implements field and curve operations for the alt_bn128 (BN254) curve
 * used by Ethereum precompiles 0x06-0x08 (EIP-196/197).
 *
 * Curve: y² = x³ + 3 over Fp
 * p = 21888242871839275222246405745257275088696311157297823662689037894645226208583
 * r = 21888242871839275222246405745257275088548364400416034343698204186575808495617
 *
 * Built on mini-gmp for big integer arithmetic.
 */

#ifndef BN256_H
#define BN256_H

#include "gmp.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

//==============================================================================
// Field Types
//==============================================================================

// Fp: base field element mod p
typedef struct {
    mpz_t v;
} bn256_fp_t;

// Fp2 = Fp[u] / (u² + 1)
// Element: a + b*u
typedef struct {
    bn256_fp_t a;  // real part
    bn256_fp_t b;  // imaginary part (coefficient of u)
} bn256_fp2_t;

// Fp6 = Fp2[v] / (v³ - ξ) where ξ = 9 + u
// Element: a + b*v + c*v²
typedef struct {
    bn256_fp2_t a;
    bn256_fp2_t b;
    bn256_fp2_t c;
} bn256_fp6_t;

// Fp12 = Fp6[w] / (w² - v)
// Element: a + b*w
typedef struct {
    bn256_fp6_t a;
    bn256_fp6_t b;
} bn256_fp12_t;

//==============================================================================
// Curve Point Types (Jacobian coordinates)
//==============================================================================

// G1: point on y² = x³ + 3 over Fp
// Jacobian: (X, Y, Z) represents affine (X/Z², Y/Z³)
// Point at infinity: Z == 0
typedef struct {
    bn256_fp_t x;
    bn256_fp_t y;
    bn256_fp_t z;
} bn256_g1_t;

// G2: point on twist curve y² = x³ + 3/ξ over Fp2
typedef struct {
    bn256_fp2_t x;
    bn256_fp2_t y;
    bn256_fp2_t z;
} bn256_g2_t;

//==============================================================================
// G1 Operations (for precompiles 0x06, 0x07)
//==============================================================================

// Init/clear G1 point (manages mpz_t memory)
void bn256_g1_init(bn256_g1_t *pt);
void bn256_g1_clear(bn256_g1_t *pt);

// Unmarshal G1 point from 64 bytes (big-endian x, y).
// Returns 0 on success, -1 if coordinates >= p or point not on curve.
// Input (0,0) is treated as point at infinity.
int bn256_g1_unmarshal(bn256_g1_t *pt, const uint8_t input[64]);

// Marshal G1 point to 64 bytes (big-endian x, y).
// Point at infinity marshals as (0, 0).
void bn256_g1_marshal(uint8_t output[64], const bn256_g1_t *pt);

// r = a + b (point addition)
void bn256_g1_add(bn256_g1_t *r, const bn256_g1_t *a, const bn256_g1_t *b);

// r = scalar * pt (scalar multiplication, scalar is 32-byte big-endian)
void bn256_g1_scalar_mul(bn256_g1_t *r, const bn256_g1_t *pt,
                         const uint8_t scalar[32]);

//==============================================================================
// G2 Operations (for pairing precompile 0x08)
//==============================================================================

void bn256_g2_init(bn256_g2_t *pt);
void bn256_g2_clear(bn256_g2_t *pt);

// Unmarshal G2 point from 128 bytes.
// Ethereum encoding: x_imag(32) | x_real(32) | y_imag(32) | y_real(32)
// Returns 0 on success, -1 on invalid input.
int bn256_g2_unmarshal(bn256_g2_t *pt, const uint8_t input[128]);

//==============================================================================
// Pairing (for precompile 0x08)
//==============================================================================

// Check if product of e(g1[i], g2[i]) == 1 in GT.
// Returns 1 if pairing check passes, 0 if fails, -1 on error.
// Points must already be initialized and unmarshaled.
int bn256_pairing_check(const bn256_g1_t *g1_points,
                        const bn256_g2_t *g2_points,
                        size_t n);

#endif /* BN256_H */
