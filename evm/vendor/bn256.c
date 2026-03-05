/**
 * BN256/alt_bn128 Elliptic Curve Arithmetic
 * Implementation built on mini-gmp.
 */

#include "bn256.h"
#include <string.h>
#include <stdlib.h>

//==============================================================================
// Constants (lazy-initialized)
//==============================================================================

static bool bn256_initialized = false;
static mpz_t bn256_p;  // base field prime
static mpz_t bn256_r;  // scalar field order

static void bn256_ensure_init(void)
{
    if (bn256_initialized)
        return;
    mpz_init_set_str(bn256_p,
        "21888242871839275222246405745257275088696311157297823662689037894645226208583", 10);
    mpz_init_set_str(bn256_r,
        "21888242871839275222246405745257275088548364400416034343698204186575808495617", 10);
    bn256_initialized = true;
}

//==============================================================================
// Fp Arithmetic
//==============================================================================

static void fp_init(bn256_fp_t *a)
{
    mpz_init(a->v);
}

static void fp_clear(bn256_fp_t *a)
{
    mpz_clear(a->v);
}

static void fp_copy(bn256_fp_t *dst, const bn256_fp_t *src)
{
    mpz_set(dst->v, src->v);
}

static bool fp_is_zero(const bn256_fp_t *a)
{
    return mpz_sgn(a->v) == 0;
}

static bool fp_equal(const bn256_fp_t *a, const bn256_fp_t *b)
{
    return mpz_cmp(a->v, b->v) == 0;
}

static void fp_set_ui(bn256_fp_t *a, unsigned long val)
{
    mpz_set_ui(a->v, val);
}

static void fp_add(bn256_fp_t *r, const bn256_fp_t *a, const bn256_fp_t *b)
{
    mpz_add(r->v, a->v, b->v);
    if (mpz_cmp(r->v, bn256_p) >= 0)
        mpz_sub(r->v, r->v, bn256_p);
}

static void fp_sub(bn256_fp_t *r, const bn256_fp_t *a, const bn256_fp_t *b)
{
    mpz_sub(r->v, a->v, b->v);
    if (mpz_sgn(r->v) < 0)
        mpz_add(r->v, r->v, bn256_p);
}

static void fp_mul(bn256_fp_t *r, const bn256_fp_t *a, const bn256_fp_t *b)
{
    mpz_mul(r->v, a->v, b->v);
    mpz_mod(r->v, r->v, bn256_p);
}

static void fp_neg(bn256_fp_t *r, const bn256_fp_t *a)
{
    if (mpz_sgn(a->v) == 0)
        mpz_set_ui(r->v, 0);
    else
        mpz_sub(r->v, bn256_p, a->v);
}

static void fp_inv(bn256_fp_t *r, const bn256_fp_t *a)
{
    mpz_invert(r->v, a->v, bn256_p);
}

static void fp_mul_ui(bn256_fp_t *r, const bn256_fp_t *a, unsigned long b)
{
    mpz_mul_ui(r->v, a->v, b);
    mpz_mod(r->v, r->v, bn256_p);
}

static void fp_square(bn256_fp_t *r, const bn256_fp_t *a)
{
    mpz_mul(r->v, a->v, a->v);
    mpz_mod(r->v, r->v, bn256_p);
}

// Import 32 bytes big-endian into Fp. Returns -1 if value >= p.
static int fp_from_bytes(bn256_fp_t *a, const uint8_t bytes[32])
{
    mpz_import(a->v, 32, 1, 1, 1, 0, bytes);
    if (mpz_cmp(a->v, bn256_p) >= 0)
        return -1;
    return 0;
}

// Export Fp to 32 bytes big-endian, left-padded with zeros.
static void fp_to_bytes(uint8_t bytes[32], const bn256_fp_t *a)
{
    memset(bytes, 0, 32);
    size_t count = 0;
    if (mpz_sgn(a->v) != 0)
    {
        uint8_t tmp[32];
        mpz_export(tmp, &count, 1, 1, 1, 0, a->v);
        if (count > 32) count = 32;
        memcpy(bytes + (32 - count), tmp, count);
    }
}

//==============================================================================
// Fp2 Arithmetic: a + b*u, where u² = -1
//==============================================================================

static void fp2_init(bn256_fp2_t *a)
{
    fp_init(&a->a);
    fp_init(&a->b);
}

static void fp2_clear(bn256_fp2_t *a)
{
    fp_clear(&a->a);
    fp_clear(&a->b);
}

static void fp2_copy(bn256_fp2_t *dst, const bn256_fp2_t *src)
{
    fp_copy(&dst->a, &src->a);
    fp_copy(&dst->b, &src->b);
}

static bool fp2_is_zero(const bn256_fp2_t *a)
{
    return fp_is_zero(&a->a) && fp_is_zero(&a->b);
}

static bool fp2_equal(const bn256_fp2_t *a, const bn256_fp2_t *b)
{
    return fp_equal(&a->a, &b->a) && fp_equal(&a->b, &b->b);
}

static void fp2_set_zero(bn256_fp2_t *a)
{
    fp_set_ui(&a->a, 0);
    fp_set_ui(&a->b, 0);
}

static void fp2_add(bn256_fp2_t *r, const bn256_fp2_t *a, const bn256_fp2_t *b)
{
    fp_add(&r->a, &a->a, &b->a);
    fp_add(&r->b, &a->b, &b->b);
}

static void fp2_sub(bn256_fp2_t *r, const bn256_fp2_t *a, const bn256_fp2_t *b)
{
    fp_sub(&r->a, &a->a, &b->a);
    fp_sub(&r->b, &a->b, &b->b);
}

static void fp2_neg(bn256_fp2_t *r, const bn256_fp2_t *a)
{
    fp_neg(&r->a, &a->a);
    fp_neg(&r->b, &a->b);
}

// (a + bu)(c + du) = (ac - bd) + (ad + bc)u
static void fp2_mul(bn256_fp2_t *r, const bn256_fp2_t *a, const bn256_fp2_t *b)
{
    bn256_fp_t ac, bd, ad, bc;
    fp_init(&ac); fp_init(&bd); fp_init(&ad); fp_init(&bc);

    fp_mul(&ac, &a->a, &b->a);
    fp_mul(&bd, &a->b, &b->b);
    fp_mul(&ad, &a->a, &b->b);
    fp_mul(&bc, &a->b, &b->a);

    fp_sub(&r->a, &ac, &bd);   // real = ac - bd
    fp_add(&r->b, &ad, &bc);   // imag = ad + bc

    fp_clear(&ac); fp_clear(&bd); fp_clear(&ad); fp_clear(&bc);
}

static void fp2_square(bn256_fp2_t *r, const bn256_fp2_t *a)
{
    // (a + bu)² = (a² - b²) + 2ab*u
    bn256_fp_t t1, t2;
    fp_init(&t1); fp_init(&t2);

    fp_add(&t1, &a->a, &a->b);   // a + b
    fp_sub(&t2, &a->a, &a->b);   // a - b
    fp_mul(&r->b, &a->a, &a->b); // ab
    fp_add(&r->b, &r->b, &r->b); // 2ab
    fp_mul(&r->a, &t1, &t2);     // (a+b)(a-b) = a² - b²

    fp_clear(&t1); fp_clear(&t2);
}

// Multiply by scalar from Fp
static void fp2_mul_scalar(bn256_fp2_t *r, const bn256_fp2_t *a, const bn256_fp_t *s)
{
    fp_mul(&r->a, &a->a, s);
    fp_mul(&r->b, &a->b, s);
}

// Inverse: 1/(a + bu) = (a - bu) / (a² + b²)
static void fp2_inv(bn256_fp2_t *r, const bn256_fp2_t *a)
{
    bn256_fp_t norm, a2, b2;
    fp_init(&norm); fp_init(&a2); fp_init(&b2);

    fp_square(&a2, &a->a);
    fp_square(&b2, &a->b);
    fp_add(&norm, &a2, &b2);   // a² + b² (since u² = -1)
    fp_inv(&norm, &norm);

    fp_mul(&r->a, &a->a, &norm);
    fp_neg(&r->b, &a->b);
    fp_mul(&r->b, &r->b, &norm);

    fp_clear(&norm); fp_clear(&a2); fp_clear(&b2);
}

// Multiply by ξ = 9 + u: (9+u)(a+bu) = (9a - b) + (a + 9b)u
static void fp2_mul_xi(bn256_fp2_t *r, const bn256_fp2_t *a)
{
    bn256_fp_t t1, t2;
    fp_init(&t1); fp_init(&t2);

    fp_mul_ui(&t1, &a->a, 9);   // 9a
    fp_sub(&t1, &t1, &a->b);    // 9a - b

    fp_mul_ui(&t2, &a->b, 9);   // 9b
    fp_add(&t2, &t2, &a->a);    // a + 9b

    fp_copy(&r->a, &t1);
    fp_copy(&r->b, &t2);

    fp_clear(&t1); fp_clear(&t2);
}

// Conjugate: (a, -b)
static void fp2_conj(bn256_fp2_t *r, const bn256_fp2_t *a)
{
    fp_copy(&r->a, &a->a);
    fp_neg(&r->b, &a->b);
}

//==============================================================================
// G1 Curve Operations: y² = x³ + 3 over Fp, Jacobian coords
//==============================================================================

void bn256_g1_init(bn256_g1_t *pt)
{
    fp_init(&pt->x);
    fp_init(&pt->y);
    fp_init(&pt->z);
}

void bn256_g1_clear(bn256_g1_t *pt)
{
    fp_clear(&pt->x);
    fp_clear(&pt->y);
    fp_clear(&pt->z);
}

static void g1_set_infinity(bn256_g1_t *pt)
{
    fp_set_ui(&pt->x, 0);
    fp_set_ui(&pt->y, 1);
    fp_set_ui(&pt->z, 0);
}

static bool g1_is_infinity(const bn256_g1_t *pt)
{
    return fp_is_zero(&pt->z);
}

static void g1_copy(bn256_g1_t *dst, const bn256_g1_t *src)
{
    fp_copy(&dst->x, &src->x);
    fp_copy(&dst->y, &src->y);
    fp_copy(&dst->z, &src->z);
}

int bn256_g1_unmarshal(bn256_g1_t *pt, const uint8_t input[64])
{
    bn256_ensure_init();

    if (fp_from_bytes(&pt->x, input) != 0)
        return -1;
    if (fp_from_bytes(&pt->y, input + 32) != 0)
        return -1;

    // (0, 0) = point at infinity
    if (fp_is_zero(&pt->x) && fp_is_zero(&pt->y))
    {
        g1_set_infinity(pt);
        return 0;
    }

    // Check on curve: y² = x³ + 3
    bn256_fp_t lhs, rhs, x3;
    fp_init(&lhs); fp_init(&rhs); fp_init(&x3);

    fp_square(&lhs, &pt->y);        // y²
    fp_square(&x3, &pt->x);
    fp_mul(&x3, &x3, &pt->x);       // x³
    fp_set_ui(&rhs, 3);
    fp_add(&rhs, &x3, &rhs);        // x³ + 3

    bool on_curve = fp_equal(&lhs, &rhs);

    fp_clear(&lhs); fp_clear(&rhs); fp_clear(&x3);

    if (!on_curve)
        return -1;

    // Set Z = 1 (affine to Jacobian)
    fp_set_ui(&pt->z, 1);
    return 0;
}

void bn256_g1_marshal(uint8_t output[64], const bn256_g1_t *pt)
{
    bn256_ensure_init();

    if (g1_is_infinity(pt))
    {
        memset(output, 0, 64);
        return;
    }

    // Convert Jacobian to affine: x = X/Z², y = Y/Z³
    bn256_fp_t z_inv, z2, z3, ax, ay;
    fp_init(&z_inv); fp_init(&z2); fp_init(&z3);
    fp_init(&ax); fp_init(&ay);

    fp_inv(&z_inv, &pt->z);
    fp_square(&z2, &z_inv);
    fp_mul(&z3, &z2, &z_inv);
    fp_mul(&ax, &pt->x, &z2);
    fp_mul(&ay, &pt->y, &z3);

    fp_to_bytes(output, &ax);
    fp_to_bytes(output + 32, &ay);

    fp_clear(&z_inv); fp_clear(&z2); fp_clear(&z3);
    fp_clear(&ax); fp_clear(&ay);
}

// Point doubling in Jacobian coordinates
// https://hyperelliptic.org/EFD/g1p/auto-shortw-jacobian-0.html#doubling-dbl-2009-l
static void g1_double(bn256_g1_t *r, const bn256_g1_t *p)
{
    if (g1_is_infinity(p))
    {
        g1_set_infinity(r);
        return;
    }

    bn256_fp_t A, B, C, D, E, F;
    fp_init(&A); fp_init(&B); fp_init(&C);
    fp_init(&D); fp_init(&E); fp_init(&F);

    fp_square(&A, &p->x);          // A = X1²
    fp_square(&B, &p->y);          // B = Y1²
    fp_square(&C, &B);             // C = B² = Y1⁴

    // D = 2*((X1+B)² - A - C)
    fp_add(&D, &p->x, &B);
    fp_square(&D, &D);
    fp_sub(&D, &D, &A);
    fp_sub(&D, &D, &C);
    fp_add(&D, &D, &D);

    fp_add(&E, &A, &A);
    fp_add(&E, &E, &A);            // E = 3*A = 3*X1²

    fp_square(&F, &E);             // F = E²

    // X3 = F - 2*D
    fp_sub(&r->x, &F, &D);
    fp_sub(&r->x, &r->x, &D);

    // Y3 = E*(D - X3) - 8*C
    fp_sub(&r->y, &D, &r->x);
    fp_mul(&r->y, &E, &r->y);
    fp_add(&C, &C, &C);            // 2C
    fp_add(&C, &C, &C);            // 4C
    fp_add(&C, &C, &C);            // 8C
    fp_sub(&r->y, &r->y, &C);

    // Z3 = 2*Y1*Z1
    fp_mul(&r->z, &p->y, &p->z);
    fp_add(&r->z, &r->z, &r->z);

    fp_clear(&A); fp_clear(&B); fp_clear(&C);
    fp_clear(&D); fp_clear(&E); fp_clear(&F);
}

// Point addition in Jacobian coordinates
// https://hyperelliptic.org/EFD/g1p/auto-shortw-jacobian-0.html#addition-add-2007-bl
void bn256_g1_add(bn256_g1_t *r, const bn256_g1_t *p, const bn256_g1_t *q)
{
    bn256_ensure_init();

    if (g1_is_infinity(p)) { g1_copy(r, q); return; }
    if (g1_is_infinity(q)) { g1_copy(r, p); return; }

    bn256_fp_t Z1Z1, Z2Z2, U1, U2, S1, S2, H, I, J, rr, V;
    fp_init(&Z1Z1); fp_init(&Z2Z2); fp_init(&U1); fp_init(&U2);
    fp_init(&S1); fp_init(&S2); fp_init(&H); fp_init(&I);
    fp_init(&J); fp_init(&rr); fp_init(&V);

    fp_square(&Z1Z1, &p->z);        // Z1Z1 = Z1²
    fp_square(&Z2Z2, &q->z);        // Z2Z2 = Z2²

    fp_mul(&U1, &p->x, &Z2Z2);      // U1 = X1*Z2Z2
    fp_mul(&U2, &q->x, &Z1Z1);      // U2 = X2*Z1Z1

    fp_mul(&S1, &p->y, &q->z);
    fp_mul(&S1, &S1, &Z2Z2);        // S1 = Y1*Z2*Z2Z2

    fp_mul(&S2, &q->y, &p->z);
    fp_mul(&S2, &S2, &Z1Z1);        // S2 = Y2*Z1*Z1Z1

    fp_sub(&H, &U2, &U1);           // H = U2 - U1
    fp_sub(&rr, &S2, &S1);          // r = S2 - S1

    // Check if points are the same or negation
    if (fp_is_zero(&H))
    {
        if (fp_is_zero(&rr))
        {
            // P == Q, use doubling
            fp_clear(&Z1Z1); fp_clear(&Z2Z2); fp_clear(&U1); fp_clear(&U2);
            fp_clear(&S1); fp_clear(&S2); fp_clear(&H); fp_clear(&I);
            fp_clear(&J); fp_clear(&rr); fp_clear(&V);
            g1_double(r, p);
            return;
        }
        // P == -Q, result is infinity
        fp_clear(&Z1Z1); fp_clear(&Z2Z2); fp_clear(&U1); fp_clear(&U2);
        fp_clear(&S1); fp_clear(&S2); fp_clear(&H); fp_clear(&I);
        fp_clear(&J); fp_clear(&rr); fp_clear(&V);
        g1_set_infinity(r);
        return;
    }

    fp_add(&I, &H, &H);
    fp_square(&I, &I);              // I = (2*H)²

    fp_mul(&J, &H, &I);            // J = H*I

    fp_add(&rr, &rr, &rr);          // r = 2*(S2 - S1)

    fp_mul(&V, &U1, &I);            // V = U1*I

    // X3 = r² - J - 2*V
    fp_square(&r->x, &rr);
    fp_sub(&r->x, &r->x, &J);
    fp_sub(&r->x, &r->x, &V);
    fp_sub(&r->x, &r->x, &V);

    // Y3 = r*(V - X3) - 2*S1*J
    fp_sub(&r->y, &V, &r->x);
    fp_mul(&r->y, &rr, &r->y);
    fp_mul(&S1, &S1, &J);
    fp_add(&S1, &S1, &S1);
    fp_sub(&r->y, &r->y, &S1);

    // Z3 = ((Z1+Z2)² - Z1Z1 - Z2Z2)*H
    fp_add(&r->z, &p->z, &q->z);
    fp_square(&r->z, &r->z);
    fp_sub(&r->z, &r->z, &Z1Z1);
    fp_sub(&r->z, &r->z, &Z2Z2);
    fp_mul(&r->z, &r->z, &H);

    fp_clear(&Z1Z1); fp_clear(&Z2Z2); fp_clear(&U1); fp_clear(&U2);
    fp_clear(&S1); fp_clear(&S2); fp_clear(&H); fp_clear(&I);
    fp_clear(&J); fp_clear(&rr); fp_clear(&V);
}

void bn256_g1_scalar_mul(bn256_g1_t *r, const bn256_g1_t *pt,
                         const uint8_t scalar[32])
{
    bn256_ensure_init();

    g1_set_infinity(r);

    // Check for zero scalar
    bool all_zero = true;
    for (int i = 0; i < 32; i++)
    {
        if (scalar[i] != 0) { all_zero = false; break; }
    }
    if (all_zero || g1_is_infinity(pt))
        return;

    // Double-and-add, MSB first
    bn256_g1_t tmp;
    bn256_g1_init(&tmp);

    bool started = false;
    for (int i = 0; i < 256; i++)
    {
        int byte_idx = i / 8;
        int bit_idx = 7 - (i % 8);
        int bit = (scalar[byte_idx] >> bit_idx) & 1;

        if (started)
        {
            g1_double(&tmp, r);
            g1_copy(r, &tmp);
        }

        if (bit)
        {
            if (!started)
            {
                g1_copy(r, pt);
                started = true;
            }
            else
            {
                bn256_g1_add(&tmp, r, pt);
                g1_copy(r, &tmp);
            }
        }
    }

    bn256_g1_clear(&tmp);
}

//==============================================================================
// Fp6 Arithmetic: a + b*v + c*v², where v³ = ξ (ξ = 9 + u)
//==============================================================================

static void fp6_init(bn256_fp6_t *a)
{
    fp2_init(&a->a);
    fp2_init(&a->b);
    fp2_init(&a->c);
}

static void fp6_clear(bn256_fp6_t *a)
{
    fp2_clear(&a->a);
    fp2_clear(&a->b);
    fp2_clear(&a->c);
}

static void fp6_copy(bn256_fp6_t *dst, const bn256_fp6_t *src)
{
    fp2_copy(&dst->a, &src->a);
    fp2_copy(&dst->b, &src->b);
    fp2_copy(&dst->c, &src->c);
}

static void fp6_set_zero(bn256_fp6_t *a)
{
    fp2_set_zero(&a->a);
    fp2_set_zero(&a->b);
    fp2_set_zero(&a->c);
}

static bool fp6_is_zero(const bn256_fp6_t *a)
{
    return fp2_is_zero(&a->a) && fp2_is_zero(&a->b) && fp2_is_zero(&a->c);
}

static void fp6_add(bn256_fp6_t *r, const bn256_fp6_t *a, const bn256_fp6_t *b)
{
    fp2_add(&r->a, &a->a, &b->a);
    fp2_add(&r->b, &a->b, &b->b);
    fp2_add(&r->c, &a->c, &b->c);
}

static void fp6_sub(bn256_fp6_t *r, const bn256_fp6_t *a, const bn256_fp6_t *b)
{
    fp2_sub(&r->a, &a->a, &b->a);
    fp2_sub(&r->b, &a->b, &b->b);
    fp2_sub(&r->c, &a->c, &b->c);
}

static void fp6_neg(bn256_fp6_t *r, const bn256_fp6_t *a)
{
    fp2_neg(&r->a, &a->a);
    fp2_neg(&r->b, &a->b);
    fp2_neg(&r->c, &a->c);
}

// Fp6 multiplication (schoolbook with v³ = ξ reduction)
// (a0 + a1*v + a2*v²)(b0 + b1*v + b2*v²)
static void fp6_mul(bn256_fp6_t *r, const bn256_fp6_t *a, const bn256_fp6_t *b)
{
    bn256_fp2_t t0, t1, t2, tmp1, tmp2;
    fp2_init(&t0); fp2_init(&t1); fp2_init(&t2);
    fp2_init(&tmp1); fp2_init(&tmp2);

    fp2_mul(&t0, &a->a, &b->a);   // t0 = a0*b0
    fp2_mul(&t1, &a->b, &b->b);   // t1 = a1*b1
    fp2_mul(&t2, &a->c, &b->c);   // t2 = a2*b2

    // c0 = t0 + ξ*((a1+a2)(b1+b2) - t1 - t2)
    fp2_add(&tmp1, &a->b, &a->c);
    fp2_add(&tmp2, &b->b, &b->c);
    fp2_mul(&r->a, &tmp1, &tmp2);
    fp2_sub(&r->a, &r->a, &t1);
    fp2_sub(&r->a, &r->a, &t2);
    fp2_mul_xi(&r->a, &r->a);
    fp2_add(&r->a, &r->a, &t0);

    // c1 = (a0+a1)(b0+b1) - t0 - t1 + ξ*t2
    fp2_add(&tmp1, &a->a, &a->b);
    fp2_add(&tmp2, &b->a, &b->b);
    fp2_mul(&r->b, &tmp1, &tmp2);
    fp2_sub(&r->b, &r->b, &t0);
    fp2_sub(&r->b, &r->b, &t1);
    fp2_mul_xi(&tmp1, &t2);
    fp2_add(&r->b, &r->b, &tmp1);

    // c2 = (a0+a2)(b0+b2) - t0 - t2 + t1
    fp2_add(&tmp1, &a->a, &a->c);
    fp2_add(&tmp2, &b->a, &b->c);
    fp2_mul(&r->c, &tmp1, &tmp2);
    fp2_sub(&r->c, &r->c, &t0);
    fp2_sub(&r->c, &r->c, &t2);
    fp2_add(&r->c, &r->c, &t1);

    fp2_clear(&t0); fp2_clear(&t1); fp2_clear(&t2);
    fp2_clear(&tmp1); fp2_clear(&tmp2);
}

static void fp6_square(bn256_fp6_t *r, const bn256_fp6_t *a)
{
    bn256_fp2_t t0, t1, t2, t3, t4;
    fp2_init(&t0); fp2_init(&t1); fp2_init(&t2);
    fp2_init(&t3); fp2_init(&t4);

    fp2_square(&t0, &a->a);       // a0²
    fp2_mul(&t1, &a->a, &a->b);
    fp2_add(&t1, &t1, &t1);       // 2*a0*a1
    fp2_sub(&t2, &a->a, &a->b);
    fp2_add(&t2, &t2, &a->c);
    fp2_square(&t2, &t2);          // (a0 - a1 + a2)²
    fp2_mul(&t3, &a->b, &a->c);
    fp2_add(&t3, &t3, &t3);       // 2*a1*a2
    fp2_square(&t4, &a->c);       // a2²

    // c0 = t0 + ξ*t3
    fp2_mul_xi(&r->a, &t3);
    fp2_add(&r->a, &r->a, &t0);

    // c1 = t1 + ξ*t4
    fp2_mul_xi(&r->b, &t4);
    fp2_add(&r->b, &r->b, &t1);

    // c2 = t1 + t2 + t3 - t0 - t4
    fp2_add(&r->c, &t1, &t2);
    fp2_add(&r->c, &r->c, &t3);
    fp2_sub(&r->c, &r->c, &t0);
    fp2_sub(&r->c, &r->c, &t4);

    fp2_clear(&t0); fp2_clear(&t1); fp2_clear(&t2);
    fp2_clear(&t3); fp2_clear(&t4);
}

// Multiply by τ (= v): shift and reduce
// v*(a + bv + cv²) = ξc + av + bv²
static void fp6_mul_tau(bn256_fp6_t *r, const bn256_fp6_t *a)
{
    bn256_fp2_t tmp;
    fp2_init(&tmp);

    fp2_mul_xi(&tmp, &a->c);  // ξ*c
    fp2_copy(&r->c, &a->b);   // new c = b
    fp2_copy(&r->b, &a->a);   // new b = a
    fp2_copy(&r->a, &tmp);    // new a = ξ*c

    fp2_clear(&tmp);
}

static void fp6_inv(bn256_fp6_t *r, const bn256_fp6_t *a)
{
    // Using the formula for cubic extension inversion
    bn256_fp2_t A, B, C, t, F;
    fp2_init(&A); fp2_init(&B); fp2_init(&C);
    fp2_init(&t); fp2_init(&F);

    // A = a0² - a1*a2*ξ  (actually: A = a² - b*c*ξ... using the standard naming)
    // Wait: for Fp6 = Fp2[v]/(v³ - ξ), the inverse of (a + bv + cv²) needs:
    // A = a² - bc*ξ
    // B = c²*ξ - ab
    // C = b² - ac
    // F = aA + ξ(cB + bC)
    // inv = (A + Bv + Cv²) / F

    fp2_square(&A, &a->a);
    fp2_mul(&t, &a->b, &a->c);
    fp2_mul_xi(&t, &t);
    fp2_sub(&A, &A, &t);        // A = a² - bc*ξ

    fp2_square(&B, &a->c);
    fp2_mul_xi(&B, &B);
    fp2_mul(&t, &a->a, &a->b);
    fp2_sub(&B, &B, &t);        // B = c²*ξ - ab

    fp2_square(&C, &a->b);
    fp2_mul(&t, &a->a, &a->c);
    fp2_sub(&C, &C, &t);        // C = b² - ac

    // F = a*A + ξ*(c*B + b*C)
    fp2_mul(&F, &a->c, &B);
    fp2_mul(&t, &a->b, &C);
    fp2_add(&F, &F, &t);
    fp2_mul_xi(&F, &F);
    fp2_mul(&t, &a->a, &A);
    fp2_add(&F, &F, &t);

    fp2_inv(&F, &F);

    fp2_mul(&r->a, &A, &F);
    fp2_mul(&r->b, &B, &F);
    fp2_mul(&r->c, &C, &F);

    fp2_clear(&A); fp2_clear(&B); fp2_clear(&C);
    fp2_clear(&t); fp2_clear(&F);
}

// Multiply Fp6 element by Fp2 scalar
static void fp6_mul_scalar_fp2(bn256_fp6_t *r, const bn256_fp6_t *a, const bn256_fp2_t *s)
{
    fp2_mul(&r->a, &a->a, s);
    fp2_mul(&r->b, &a->b, s);
    fp2_mul(&r->c, &a->c, s);
}

//==============================================================================
// Fp12 Arithmetic: a + b*w, where w² = v
//==============================================================================

static void fp12_init(bn256_fp12_t *a)
{
    fp6_init(&a->a);
    fp6_init(&a->b);
}

static void fp12_clear(bn256_fp12_t *a)
{
    fp6_clear(&a->a);
    fp6_clear(&a->b);
}

static void fp12_copy(bn256_fp12_t *dst, const bn256_fp12_t *src)
{
    fp6_copy(&dst->a, &src->a);
    fp6_copy(&dst->b, &src->b);
}

static void fp12_set_one(bn256_fp12_t *a)
{
    fp6_set_zero(&a->a);
    fp6_set_zero(&a->b);
    fp_set_ui(&a->a.a.a, 1);
}

static bool fp12_is_one(const bn256_fp12_t *a)
{
    bn256_fp12_t one;
    fp12_init(&one);
    fp12_set_one(&one);

    bool result = fp2_equal(&a->a.a, &one.a.a) &&
                  fp2_is_zero(&a->a.b) && fp2_is_zero(&a->a.c) &&
                  fp6_is_zero(&a->b);

    fp12_clear(&one);
    return result;
}

static void fp12_mul(bn256_fp12_t *r, const bn256_fp12_t *a, const bn256_fp12_t *b)
{
    // (a0 + a1*w)(b0 + b1*w) = (a0*b0 + a1*b1*v) + (a0*b1 + a1*b0)*w
    // where w² = v, so a1*b1*w² = a1*b1*v → use fp6_mul_tau
    bn256_fp6_t t0, t1, tmp;
    fp6_init(&t0); fp6_init(&t1); fp6_init(&tmp);

    fp6_mul(&t0, &a->a, &b->a);     // a0*b0
    fp6_mul(&t1, &a->b, &b->b);     // a1*b1

    // r.b = (a0+a1)(b0+b1) - t0 - t1
    fp6_add(&r->b, &a->a, &a->b);
    fp6_add(&tmp, &b->a, &b->b);
    fp6_mul(&r->b, &r->b, &tmp);
    fp6_sub(&r->b, &r->b, &t0);
    fp6_sub(&r->b, &r->b, &t1);

    // r.a = t0 + t1*v (tau)
    fp6_mul_tau(&tmp, &t1);
    fp6_add(&r->a, &t0, &tmp);

    fp6_clear(&t0); fp6_clear(&t1); fp6_clear(&tmp);
}

static void fp12_square(bn256_fp12_t *r, const bn256_fp12_t *a)
{
    // (a + bw)² = (a² + b²*v) + 2ab*w
    bn256_fp6_t t0, t1, ab;
    fp6_init(&t0); fp6_init(&t1); fp6_init(&ab);

    fp6_square(&t0, &a->a);     // a²
    fp6_square(&t1, &a->b);     // b²
    fp6_mul(&ab, &a->a, &a->b); // ab

    // r.b = 2*ab
    fp6_add(&r->b, &ab, &ab);

    // r.a = a² + b²*v
    fp6_mul_tau(&t1, &t1);
    fp6_add(&r->a, &t0, &t1);

    fp6_clear(&t0); fp6_clear(&t1); fp6_clear(&ab);
}

// Conjugate in Fp12: (a, -b)
static void fp12_conj(bn256_fp12_t *r, const bn256_fp12_t *a)
{
    fp6_copy(&r->a, &a->a);
    fp6_neg(&r->b, &a->b);
}

static void fp12_inv(bn256_fp12_t *r, const bn256_fp12_t *a)
{
    // 1/(a + bw) = (a - bw) / (a² - b²*v)
    bn256_fp6_t t0, t1, denom;
    fp6_init(&t0); fp6_init(&t1); fp6_init(&denom);

    fp6_square(&t0, &a->a);     // a²
    fp6_square(&t1, &a->b);     // b²
    fp6_mul_tau(&t1, &t1);      // b²*v
    fp6_sub(&denom, &t0, &t1);  // a² - b²*v
    fp6_inv(&denom, &denom);

    fp6_mul(&r->a, &a->a, &denom);
    fp6_neg(&r->b, &a->b);
    fp6_mul(&r->b, &r->b, &denom);

    fp6_clear(&t0); fp6_clear(&t1); fp6_clear(&denom);
}

// Exponentiation by square-and-multiply
static void fp12_exp(bn256_fp12_t *r, const bn256_fp12_t *base, const mpz_t exp)
{
    fp12_set_one(r);

    if (mpz_sgn(exp) == 0)
        return;

    bn256_fp12_t tmp;
    fp12_init(&tmp);

    mp_bitcnt_t bits = mpz_sizeinbase(exp, 2);
    bool started = false;

    for (mp_bitcnt_t i = bits; i > 0; i--)
    {
        if (started)
        {
            fp12_square(&tmp, r);
            fp12_copy(r, &tmp);
        }

        if (mpz_tstbit(exp, i - 1))
        {
            if (!started)
            {
                fp12_copy(r, base);
                started = true;
            }
            else
            {
                fp12_mul(&tmp, r, base);
                fp12_copy(r, &tmp);
            }
        }
    }

    fp12_clear(&tmp);
}

//==============================================================================
// Frobenius endomorphism for Fp12
// These constants are precomputed from p^k applied to the tower construction
//==============================================================================

// Frobenius constants (lazy-initialized)
static bool frobenius_initialized = false;
static bn256_fp2_t frob_xi_to_p_minus_1_over_6;    // ξ^((p-1)/6)
static bn256_fp2_t frob_xi_to_p_minus_1_over_3;    // ξ^((p-1)/3)
static bn256_fp2_t frob_xi_to_p_minus_1_over_2;    // ξ^((p-1)/2)
static bn256_fp2_t frob_xi_to_2p_minus_2_over_3;   // ξ^((2p-2)/3)
static bn256_fp2_t frob_xi_to_p_sq_minus_1_over_6; // ξ^((p²-1)/6)
static bn256_fp2_t frob_xi_to_p_sq_minus_1_over_3; // ξ^((p²-1)/3)

static void frobenius_ensure_init(void)
{
    if (frobenius_initialized)
        return;
    bn256_ensure_init();

    // ξ = 9 + u in Fp2
    // We need ξ^((p-1)/k) for various k
    // These are computed by exponentiating ξ in Fp2

    // ξ^((p-1)/6)
    fp2_init(&frob_xi_to_p_minus_1_over_6);
    mpz_t exp;
    mpz_init(exp);

    // (p-1)/6
    mpz_sub_ui(exp, bn256_p, 1);
    mpz_tdiv_q_ui(exp, exp, 6);

    // Compute ξ^exp in Fp2 using repeated squaring
    // ξ = 9 + u → a=9, b=1
    // We need a general Fp2 exponentiation
    // For this we'll use a simple square-and-multiply

    // Helper: Fp2 exp by mpz_t
    bn256_fp2_t base, result, tmp;
    fp2_init(&base); fp2_init(&result); fp2_init(&tmp);

    fp_set_ui(&base.a, 9);
    fp_set_ui(&base.b, 1);

    // result = 1
    fp_set_ui(&result.a, 1);
    fp_set_ui(&result.b, 0);

    mp_bitcnt_t bits = mpz_sizeinbase(exp, 2);
    bool started = false;
    for (mp_bitcnt_t i = bits; i > 0; i--)
    {
        if (started)
        {
            fp2_square(&tmp, &result);
            fp2_copy(&result, &tmp);
        }
        if (mpz_tstbit(exp, i - 1))
        {
            if (!started)
            {
                fp2_copy(&result, &base);
                started = true;
            }
            else
            {
                fp2_mul(&tmp, &result, &base);
                fp2_copy(&result, &tmp);
            }
        }
    }
    fp2_copy(&frob_xi_to_p_minus_1_over_6, &result);

    // ξ^((p-1)/3) = (ξ^((p-1)/6))²
    fp2_init(&frob_xi_to_p_minus_1_over_3);
    fp2_square(&frob_xi_to_p_minus_1_over_3, &frob_xi_to_p_minus_1_over_6);

    // ξ^((p-1)/2) = (ξ^((p-1)/6))³
    fp2_init(&frob_xi_to_p_minus_1_over_2);
    fp2_mul(&frob_xi_to_p_minus_1_over_2, &frob_xi_to_p_minus_1_over_3,
            &frob_xi_to_p_minus_1_over_6);

    // ξ^((2p-2)/3) = (ξ^((p-1)/3))²  ... wait, (2p-2)/3 = 2*(p-1)/3
    fp2_init(&frob_xi_to_2p_minus_2_over_3);
    fp2_square(&frob_xi_to_2p_minus_2_over_3, &frob_xi_to_p_minus_1_over_3);

    // ξ^((p²-1)/6)
    // Since p ≡ 1 (mod 6), (p²-1)/6 = (p-1)(p+1)/6
    // ξ^((p²-1)/6) = ξ^((p-1)/6 * (p+1))
    // But it's easier to just compute directly
    fp2_init(&frob_xi_to_p_sq_minus_1_over_6);
    mpz_t p_sq;
    mpz_init(p_sq);
    mpz_mul(p_sq, bn256_p, bn256_p);
    mpz_sub_ui(exp, p_sq, 1);
    mpz_tdiv_q_ui(exp, exp, 6);

    fp_set_ui(&result.a, 1);
    fp_set_ui(&result.b, 0);
    started = false;
    bits = mpz_sizeinbase(exp, 2);
    for (mp_bitcnt_t i = bits; i > 0; i--)
    {
        if (started)
        {
            fp2_square(&tmp, &result);
            fp2_copy(&result, &tmp);
        }
        if (mpz_tstbit(exp, i - 1))
        {
            if (!started)
            {
                fp2_copy(&result, &base);
                started = true;
            }
            else
            {
                fp2_mul(&tmp, &result, &base);
                fp2_copy(&result, &tmp);
            }
        }
    }
    fp2_copy(&frob_xi_to_p_sq_minus_1_over_6, &result);

    // ξ^((p²-1)/3) = (ξ^((p²-1)/6))²
    fp2_init(&frob_xi_to_p_sq_minus_1_over_3);
    fp2_square(&frob_xi_to_p_sq_minus_1_over_3, &frob_xi_to_p_sq_minus_1_over_6);

    mpz_clear(exp);
    mpz_clear(p_sq);
    fp2_clear(&base); fp2_clear(&result); fp2_clear(&tmp);

    frobenius_initialized = true;
}

// Frobenius endomorphism: raise to power p
// For Fp12 = Fp6[w]/(w²-v), Fp6 = Fp2[v]/(v³-ξ):
// φ_p(a0 + a1*v + a2*v² + (b0 + b1*v + b2*v²)*w)
// Apply conjugation on Fp2 components, then multiply by Frobenius constants
static void fp12_frobenius(bn256_fp12_t *r, const bn256_fp12_t *a)
{
    frobenius_ensure_init();

    // Apply Fp2 conjugation to each coefficient, then multiply by powers of ξ^((p-1)/6)
    // r.a.a = conj(a.a.a)
    fp2_conj(&r->a.a, &a->a.a);

    // r.a.b = conj(a.a.b) * ξ^((p-1)/3)
    fp2_conj(&r->a.b, &a->a.b);
    fp2_mul(&r->a.b, &r->a.b, &frob_xi_to_p_minus_1_over_3);

    // r.a.c = conj(a.a.c) * ξ^(2(p-1)/3)
    fp2_conj(&r->a.c, &a->a.c);
    fp2_mul(&r->a.c, &r->a.c, &frob_xi_to_2p_minus_2_over_3);

    // r.b.a = conj(a.b.a) * ξ^((p-1)/6)
    fp2_conj(&r->b.a, &a->b.a);
    fp2_mul(&r->b.a, &r->b.a, &frob_xi_to_p_minus_1_over_6);

    // r.b.b = conj(a.b.b) * ξ^((p-1)/2)
    fp2_conj(&r->b.b, &a->b.b);
    fp2_mul(&r->b.b, &r->b.b, &frob_xi_to_p_minus_1_over_2);

    // r.b.c = conj(a.b.c) * ξ^(5(p-1)/6)
    // ξ^(5(p-1)/6) = ξ^((p-1)/2) * ξ^((p-1)/3)
    bn256_fp2_t t;
    fp2_init(&t);
    fp2_mul(&t, &frob_xi_to_p_minus_1_over_2, &frob_xi_to_p_minus_1_over_3);
    fp2_conj(&r->b.c, &a->b.c);
    fp2_mul(&r->b.c, &r->b.c, &t);
    fp2_clear(&t);
}

// Frobenius squared (raise to p²)
static void fp12_frobenius_p2(bn256_fp12_t *r, const bn256_fp12_t *a)
{
    frobenius_ensure_init();

    // For p², conjugation is identity (conj(conj(x)) = x), but we multiply
    // by ξ^(k*(p²-1)/6) for each component

    fp2_copy(&r->a.a, &a->a.a);

    fp2_mul(&r->a.b, &a->a.b, &frob_xi_to_p_sq_minus_1_over_3);

    // ξ^(2*(p²-1)/6) = (ξ^((p²-1)/6))² = ... let's compute
    // Actually ξ^(2*(p²-1)/6) = ξ^((p²-1)/3)
    bn256_fp2_t t;
    fp2_init(&t);
    fp2_square(&t, &frob_xi_to_p_sq_minus_1_over_3);
    // Wait, that would be ξ^(2*(p²-1)/3), not what we want.
    // ξ^(2*(p²-1)/6) = ξ^((p²-1)/3) — which is frob_xi_to_p_sq_minus_1_over_3
    // No wait: 2*(p²-1)/6 = (p²-1)/3. Yes.
    fp2_mul(&r->a.c, &a->a.c, &frob_xi_to_p_sq_minus_1_over_3);
    // Hmm, but a.c is coefficient of v², so the Frobenius factor is ξ^(2*(p²-1)/6) = ξ^((p²-1)/3).
    // Actually we need to be more careful. Let me reconsider.

    // For Fp6 = Fp2[v]/(v³ - ξ):
    // φ_{p²}(v) = v * ξ^((p²-1)/3)
    // φ_{p²}(v²) = v² * ξ^(2*(p²-1)/3)
    // So a.b (coeff of v) gets multiplied by ξ^((p²-1)/3)
    // And a.c (coeff of v²) gets multiplied by ξ^(2*(p²-1)/3)
    // ξ^(2*(p²-1)/3) = (ξ^((p²-1)/3))²
    fp2_square(&t, &frob_xi_to_p_sq_minus_1_over_3);
    fp2_mul(&r->a.c, &a->a.c, &t);

    // For the w part (coeff of w in Fp12 = Fp6[w]/(w²-v)):
    // φ_{p²}(w) = w * ξ^((p²-1)/6)
    // So b.a (coeff of w) gets ξ^((p²-1)/6)
    fp2_mul(&r->b.a, &a->b.a, &frob_xi_to_p_sq_minus_1_over_6);

    // b.b (coeff of v*w) gets ξ^((p²-1)/6) * ξ^((p²-1)/3) = ξ^((p²-1)/2)
    bn256_fp2_t half;
    fp2_init(&half);
    fp2_mul(&half, &frob_xi_to_p_sq_minus_1_over_6, &frob_xi_to_p_sq_minus_1_over_3);
    fp2_mul(&r->b.b, &a->b.b, &half);

    // b.c (coeff of v²*w) gets ξ^((p²-1)/6) * ξ^(2*(p²-1)/3) = ξ^(5*(p²-1)/6)
    bn256_fp2_t five_sixths;
    fp2_init(&five_sixths);
    fp2_mul(&five_sixths, &half, &frob_xi_to_p_sq_minus_1_over_3);
    fp2_mul(&r->b.c, &a->b.c, &five_sixths);

    fp2_clear(&t);
    fp2_clear(&half);
    fp2_clear(&five_sixths);
}

//==============================================================================
// G2 Curve Operations: y² = x³ + twist_b over Fp2
// twist_b = 3 / ξ = 3 / (9+u)
//==============================================================================

static bool twist_b_initialized = false;
static bn256_fp2_t twist_b;

static void twist_b_ensure_init(void)
{
    if (twist_b_initialized)
        return;
    bn256_ensure_init();

    fp2_init(&twist_b);
    // twist_b = 3 / (9 + u)
    bn256_fp2_t xi;
    fp2_init(&xi);
    fp_set_ui(&xi.a, 9);
    fp_set_ui(&xi.b, 1);
    fp2_inv(&twist_b, &xi);
    bn256_fp_t three;
    fp_init(&three);
    fp_set_ui(&three, 3);
    fp2_mul_scalar(&twist_b, &twist_b, &three);
    fp_clear(&three);
    fp2_clear(&xi);

    twist_b_initialized = true;
}

void bn256_g2_init(bn256_g2_t *pt)
{
    fp2_init(&pt->x);
    fp2_init(&pt->y);
    fp2_init(&pt->z);
}

void bn256_g2_clear(bn256_g2_t *pt)
{
    fp2_clear(&pt->x);
    fp2_clear(&pt->y);
    fp2_clear(&pt->z);
}

static void g2_set_infinity(bn256_g2_t *pt)
{
    fp2_set_zero(&pt->x);
    fp_set_ui(&pt->y.a, 1);
    fp_set_ui(&pt->y.b, 0);
    fp2_set_zero(&pt->z);
}

static bool g2_is_infinity(const bn256_g2_t *pt)
{
    return fp2_is_zero(&pt->z);
}

static void g2_copy(bn256_g2_t *dst, const bn256_g2_t *src)
{
    fp2_copy(&dst->x, &src->x);
    fp2_copy(&dst->y, &src->y);
    fp2_copy(&dst->z, &src->z);
}

int bn256_g2_unmarshal(bn256_g2_t *pt, const uint8_t input[128])
{
    bn256_ensure_init();
    twist_b_ensure_init();

    // Ethereum encoding: x_imag(32) | x_real(32) | y_imag(32) | y_real(32)
    if (fp_from_bytes(&pt->x.b, input) != 0)       return -1;  // x imaginary
    if (fp_from_bytes(&pt->x.a, input + 32) != 0)   return -1;  // x real
    if (fp_from_bytes(&pt->y.b, input + 64) != 0)   return -1;  // y imaginary
    if (fp_from_bytes(&pt->y.a, input + 96) != 0)   return -1;  // y real

    // All zeros = point at infinity
    if (fp2_is_zero(&pt->x) && fp2_is_zero(&pt->y))
    {
        g2_set_infinity(pt);
        return 0;
    }

    // Check on twist curve: y² = x³ + twist_b
    bn256_fp2_t lhs, rhs, x2, x3;
    fp2_init(&lhs); fp2_init(&rhs); fp2_init(&x2); fp2_init(&x3);

    fp2_square(&lhs, &pt->y);
    fp2_square(&x2, &pt->x);
    fp2_mul(&x3, &x2, &pt->x);
    fp2_add(&rhs, &x3, &twist_b);

    bool on_curve = fp2_equal(&lhs, &rhs);

    fp2_clear(&lhs); fp2_clear(&rhs); fp2_clear(&x2); fp2_clear(&x3);

    if (!on_curve)
        return -1;

    // Set Z = 1
    fp_set_ui(&pt->z.a, 1);
    fp_set_ui(&pt->z.b, 0);
    return 0;
}

// G2 point doubling (Jacobian, same formulas as G1 but over Fp2)
static void g2_double(bn256_g2_t *r, const bn256_g2_t *p)
{
    if (g2_is_infinity(p))
    {
        g2_set_infinity(r);
        return;
    }

    bn256_fp2_t A, B, C, D, E, F;
    fp2_init(&A); fp2_init(&B); fp2_init(&C);
    fp2_init(&D); fp2_init(&E); fp2_init(&F);

    fp2_square(&A, &p->x);
    fp2_square(&B, &p->y);
    fp2_square(&C, &B);

    fp2_add(&D, &p->x, &B);
    fp2_square(&D, &D);
    fp2_sub(&D, &D, &A);
    fp2_sub(&D, &D, &C);
    fp2_add(&D, &D, &D);

    fp2_add(&E, &A, &A);
    fp2_add(&E, &E, &A);   // 3A

    fp2_square(&F, &E);

    fp2_sub(&r->x, &F, &D);
    fp2_sub(&r->x, &r->x, &D);

    fp2_sub(&r->y, &D, &r->x);
    fp2_mul(&r->y, &E, &r->y);
    fp2_add(&C, &C, &C);
    fp2_add(&C, &C, &C);
    fp2_add(&C, &C, &C);
    fp2_sub(&r->y, &r->y, &C);

    fp2_mul(&r->z, &p->y, &p->z);
    fp2_add(&r->z, &r->z, &r->z);

    fp2_clear(&A); fp2_clear(&B); fp2_clear(&C);
    fp2_clear(&D); fp2_clear(&E); fp2_clear(&F);
}

// G2 point addition (Jacobian over Fp2)
static void g2_add(bn256_g2_t *r, const bn256_g2_t *p, const bn256_g2_t *q)
{
    if (g2_is_infinity(p)) { g2_copy(r, q); return; }
    if (g2_is_infinity(q)) { g2_copy(r, p); return; }

    bn256_fp2_t Z1Z1, Z2Z2, U1, U2, S1, S2, H, I, J, rr, V;
    fp2_init(&Z1Z1); fp2_init(&Z2Z2); fp2_init(&U1); fp2_init(&U2);
    fp2_init(&S1); fp2_init(&S2); fp2_init(&H); fp2_init(&I);
    fp2_init(&J); fp2_init(&rr); fp2_init(&V);

    fp2_square(&Z1Z1, &p->z);
    fp2_square(&Z2Z2, &q->z);
    fp2_mul(&U1, &p->x, &Z2Z2);
    fp2_mul(&U2, &q->x, &Z1Z1);
    fp2_mul(&S1, &p->y, &q->z);
    fp2_mul(&S1, &S1, &Z2Z2);
    fp2_mul(&S2, &q->y, &p->z);
    fp2_mul(&S2, &S2, &Z1Z1);

    fp2_sub(&H, &U2, &U1);
    fp2_sub(&rr, &S2, &S1);

    if (fp2_is_zero(&H))
    {
        fp2_clear(&Z1Z1); fp2_clear(&Z2Z2); fp2_clear(&U1); fp2_clear(&U2);
        fp2_clear(&S1); fp2_clear(&S2); fp2_clear(&H); fp2_clear(&I);
        fp2_clear(&J); fp2_clear(&rr); fp2_clear(&V);
        if (fp2_is_zero(&rr))
            g2_double(r, p);
        else
            g2_set_infinity(r);
        return;
    }

    fp2_add(&I, &H, &H);
    fp2_square(&I, &I);
    fp2_mul(&J, &H, &I);
    fp2_add(&rr, &rr, &rr);
    fp2_mul(&V, &U1, &I);

    fp2_square(&r->x, &rr);
    fp2_sub(&r->x, &r->x, &J);
    fp2_sub(&r->x, &r->x, &V);
    fp2_sub(&r->x, &r->x, &V);

    fp2_sub(&r->y, &V, &r->x);
    fp2_mul(&r->y, &rr, &r->y);
    fp2_mul(&S1, &S1, &J);
    fp2_add(&S1, &S1, &S1);
    fp2_sub(&r->y, &r->y, &S1);

    fp2_add(&r->z, &p->z, &q->z);
    fp2_square(&r->z, &r->z);
    fp2_sub(&r->z, &r->z, &Z1Z1);
    fp2_sub(&r->z, &r->z, &Z2Z2);
    fp2_mul(&r->z, &r->z, &H);

    fp2_clear(&Z1Z1); fp2_clear(&Z2Z2); fp2_clear(&U1); fp2_clear(&U2);
    fp2_clear(&S1); fp2_clear(&S2); fp2_clear(&H); fp2_clear(&I);
    fp2_clear(&J); fp2_clear(&rr); fp2_clear(&V);
}

//==============================================================================
// Optimal Ate Pairing
// Reference: go-ethereum crypto/bn256/cloudflare/optimalAte.go
//==============================================================================

// Line function evaluation structures
// A "line result" is a sparse Fp12 element
typedef struct {
    bn256_fp2_t a;  // coefficient
    bn256_fp2_t b;  // coefficient
    bn256_fp2_t c;  // coefficient
} line_result_t;

static void line_init(line_result_t *l)
{
    fp2_init(&l->a);
    fp2_init(&l->b);
    fp2_init(&l->c);
}

static void line_clear(line_result_t *l)
{
    fp2_clear(&l->a);
    fp2_clear(&l->b);
    fp2_clear(&l->c);
}

// Multiply Fp12 by a sparse line function result
// This is more efficient than full Fp12 multiplication
static void fp12_mul_line(bn256_fp12_t *r, const bn256_fp12_t *a, const line_result_t *l)
{
    // The line function result represents an element of Fp12 where
    // most coefficients are zero. We exploit this sparsity.
    // Line result maps to: (l.a, 0, 0) + (l.b, l.c, 0)*w in Fp6 + Fp6*w representation
    // i.e., a6_0 = (l.a, 0, 0) and a6_1 = (l.b, l.c, 0)

    bn256_fp6_t b6;
    fp6_init(&b6);
    fp2_copy(&b6.a, &l->b);
    fp2_copy(&b6.b, &l->c);
    fp2_set_zero(&b6.c);

    bn256_fp6_t a6;
    fp6_init(&a6);
    fp2_copy(&a6.a, &l->a);
    fp2_set_zero(&a6.b);
    fp2_set_zero(&a6.c);

    // Now multiply (a->a + a->b * w) * (a6 + b6 * w)
    bn256_fp6_t t0, t1, tmp;
    fp6_init(&t0); fp6_init(&t1); fp6_init(&tmp);

    fp6_mul(&t0, &a->a, &a6);
    fp6_mul(&t1, &a->b, &b6);

    fp6_add(&r->b, &a->a, &a->b);
    fp6_add(&tmp, &a6, &b6);
    fp6_mul(&r->b, &r->b, &tmp);
    fp6_sub(&r->b, &r->b, &t0);
    fp6_sub(&r->b, &r->b, &t1);

    fp6_mul_tau(&tmp, &t1);
    fp6_add(&r->a, &t0, &tmp);

    fp6_clear(&a6); fp6_clear(&b6);
    fp6_clear(&t0); fp6_clear(&t1); fp6_clear(&tmp);
}

// Line function for doubling: tangent at T evaluated at Q
// Updates T to 2T, returns line coefficients
static void line_func_double(line_result_t *l, bn256_g2_t *T, const bn256_g1_t *Q)
{
    // This computes the tangent line at T on the twist curve,
    // evaluates it at Q (a G1 point), and doubles T.

    bn256_fp2_t A, B, C, D, E, G, tmp;
    fp2_init(&A); fp2_init(&B); fp2_init(&C); fp2_init(&D);
    fp2_init(&E); fp2_init(&G); fp2_init(&tmp);

    // A = (T.x * T.y) / 2
    fp2_mul(&A, &T->x, &T->y);
    // Divide by 2: multiply by inverse of 2
    bn256_fp_t two_inv;
    fp_init(&two_inv);
    fp_set_ui(&two_inv, 2);
    fp_inv(&two_inv, &two_inv);
    fp2_mul_scalar(&A, &A, &two_inv);
    fp_clear(&two_inv);

    // B = T.y²
    fp2_square(&B, &T->y);

    // C = T.z²
    fp2_square(&C, &T->z);

    // D = 3 * twist_b * C  (twist_b = 3/(9+u))
    twist_b_ensure_init();
    fp2_mul(&D, &twist_b, &C);
    fp2_add(&tmp, &D, &D);
    fp2_add(&D, &tmp, &D);  // D = 3 * twist_b * C

    // E = unused, let's call D as it is = 3b'C
    // Following go-ethereum's lineFunctionDouble more carefully:

    // G = (B + D) / 2
    fp2_add(&G, &B, &D);
    bn256_fp_t half;
    fp_init(&half);
    fp_set_ui(&half, 2);
    fp_inv(&half, &half);
    fp2_mul_scalar(&G, &G, &half);
    fp_clear(&half);

    // T.x = A * (B - D)
    fp2_sub(&tmp, &B, &D);
    fp2_mul(&T->x, &A, &tmp);

    // T.y = G² - 3*D²
    fp2_square(&T->y, &G);
    fp2_square(&tmp, &D);
    bn256_fp2_t three_d2;
    fp2_init(&three_d2);
    fp2_add(&three_d2, &tmp, &tmp);
    fp2_add(&three_d2, &three_d2, &tmp);  // 3*D²
    fp2_sub(&T->y, &T->y, &three_d2);

    // T.z = B * T.z_old * ... wait
    // Actually: T.z_new = B * T.z_old (the original T.z before we modify it)
    // But we already used C = T.z². Let me track T.z_old.
    // T.z_new = 2 * T.y_old * T.z_old ... but we need original y and z
    // Let me re-read the algorithm more carefully.

    // Actually, let me just use the standard doubling + line eval approach:
    // The tangent line at T = (X, Y, Z) on the twist is:
    // l(Q) is evaluated using Q's affine coordinates

    // Let me use a cleaner formulation.
    // For the tangent at T in Jacobian:
    // lambda = 3*X²/(2*Y*Z) in the affine case
    // Line: l(xQ, yQ) = yQ*Z_T³ - Y_T - lambda*(xQ*Z_T² - X_T)
    // Which simplifies to specific coefficients for the sparse Fp12 representation.

    // Following Section 3 of "High-Speed Software Implementation of the Optimal Ate Pairing over Barreto-Naehrig Curves"

    // Line coefficients for the tangent at T evaluated at Q = (xQ, yQ):
    // l.a = -2*Y*Z * yQ       (in Fp2, scaled by Fp element yQ)
    // l.b = 3*X² * xQ         (in Fp2, scaled by Fp element xQ)
    // l.c = 3*b'*Z² - Y²      (pure Fp2)

    // But we also need to update T to 2T.
    // Let's just do the doubling and line evaluation separately but efficiently.

    fp2_clear(&A); fp2_clear(&B); fp2_clear(&C); fp2_clear(&D);
    fp2_clear(&E); fp2_clear(&G); fp2_clear(&tmp); fp2_clear(&three_d2);

    // OK, let me restart with a cleaner implementation.
    // I'll compute the line coefficients and double T at the same time.

    bn256_fp2_t X2, Y2, Z2, XY, YZ, XZ;
    fp2_init(&X2); fp2_init(&Y2); fp2_init(&Z2);
    fp2_init(&XY); fp2_init(&YZ); fp2_init(&XZ);

    fp2_square(&X2, &T->x);     // X²
    fp2_square(&Y2, &T->y);     // Y²
    fp2_square(&Z2, &T->z);     // Z²

    // Line coefficients (evaluated at Q with affine coords xQ, yQ in Fp):
    // We represent them as elements that will multiply into Fp12 sparsely.

    // a_coeff = -2*Y*Z  (will be multiplied by yQ from G1)
    fp2_mul(&l->a, &T->y, &T->z);
    fp2_add(&l->a, &l->a, &l->a);
    fp2_neg(&l->a, &l->a);
    // Scale by yQ (Fp element)
    fp2_mul_scalar(&l->a, &l->a, &Q->y);

    // b_coeff = 3*X²  (will be multiplied by xQ from G1)
    fp2_add(&l->b, &X2, &X2);
    fp2_add(&l->b, &l->b, &X2);  // 3*X²
    fp2_mul_scalar(&l->b, &l->b, &Q->x);

    // c_coeff = 3*b'*Z² - Y²
    twist_b_ensure_init();
    fp2_mul(&l->c, &twist_b, &Z2);
    bn256_fp2_t three_bz2;
    fp2_init(&three_bz2);
    fp2_add(&three_bz2, &l->c, &l->c);
    fp2_add(&three_bz2, &three_bz2, &l->c);  // 3*b'*Z²
    fp2_sub(&l->c, &three_bz2, &Y2);

    // Now double T using standard Jacobian doubling for y² = x³ + b' curve
    // (a = 0 for BN curves)

    // Use the efficient doubling:
    // A = X*Y/2
    fp2_mul(&XY, &T->x, &T->y);
    fp_set_ui(&two_inv, 2);
    fp_init(&two_inv);
    fp_set_ui(&two_inv, 2);
    fp_inv(&two_inv, &two_inv);
    fp2_mul_scalar(&XY, &XY, &two_inv);
    fp_clear(&two_inv);

    // D = 3*b'*Z²  (already computed as three_bz2)
    // X3 = XY * (Y² - 3*b'*Z²) = A * (Y² - D)
    bn256_fp2_t Y2_minus_D;
    fp2_init(&Y2_minus_D);
    fp2_sub(&Y2_minus_D, &Y2, &three_bz2);
    fp2_mul(&T->x, &XY, &Y2_minus_D);

    // Z3 = Y² * Z²  ... wait, actually
    // For a=0 curve doubling:
    // Z3 = 2*Y*Z  ... wait that's not right either for Jacobian.
    // Standard Jacobian doubling for a=0:
    // M = 3*X²
    // S = 4*X*Y²
    // X3 = M² - 2S
    // Y3 = M*(S-X3) - 8*Y⁴
    // Z3 = 2*Y*Z

    // Let me just use the standard formulas directly.
    bn256_fp2_t M, S, Y4;
    fp2_init(&M); fp2_init(&S); fp2_init(&Y4);

    // M = 3*X²
    fp2_add(&M, &X2, &X2);
    fp2_add(&M, &M, &X2);

    // S = 4*X*Y²
    fp2_mul(&S, &T->x, &Y2);
    // Wait, T->x was already modified above. This is a bug.
    // I need to save original values before modifying T.
    // Let me restructure.

    fp2_clear(&M); fp2_clear(&S); fp2_clear(&Y4);
    fp2_clear(&Y2_minus_D); fp2_clear(&three_bz2);
    fp2_clear(&X2); fp2_clear(&Y2); fp2_clear(&Z2);
    fp2_clear(&XY); fp2_clear(&YZ); fp2_clear(&XZ);

    // ---- CLEAN RESTART of line_func_double ----
    // Save original T coordinates
    bn256_fp2_t ox, oy, oz;
    fp2_init(&ox); fp2_init(&oy); fp2_init(&oz);
    fp2_copy(&ox, &T->x);
    fp2_copy(&oy, &T->y);
    fp2_copy(&oz, &T->z);

    bn256_fp2_t ox2, oy2, oz2;
    fp2_init(&ox2); fp2_init(&oy2); fp2_init(&oz2);
    fp2_square(&ox2, &ox);
    fp2_square(&oy2, &oy);
    fp2_square(&oz2, &oz);

    // Line coefficients
    // a = -2*Y*Z * yQ
    fp2_mul(&l->a, &oy, &oz);
    fp2_add(&l->a, &l->a, &l->a);
    fp2_neg(&l->a, &l->a);
    fp2_mul_scalar(&l->a, &l->a, &Q->y);

    // b = 3*X² * xQ
    fp2_add(&l->b, &ox2, &ox2);
    fp2_add(&l->b, &l->b, &ox2);
    fp2_mul_scalar(&l->b, &l->b, &Q->x);

    // c = 3*b'*Z² - Y²
    twist_b_ensure_init();
    bn256_fp2_t bpz2;
    fp2_init(&bpz2);
    fp2_mul(&bpz2, &twist_b, &oz2);
    fp2_add(&l->c, &bpz2, &bpz2);
    fp2_add(&l->c, &l->c, &bpz2);  // 3*b'*Z²
    fp2_sub(&l->c, &l->c, &oy2);

    // Double T using standard Jacobian (a=0)
    // M = 3*X²
    bn256_fp2_t dM, dS, dY4;
    fp2_init(&dM); fp2_init(&dS); fp2_init(&dY4);

    fp2_add(&dM, &ox2, &ox2);
    fp2_add(&dM, &dM, &ox2);  // 3X²

    // S = 4*X*Y²
    fp2_mul(&dS, &ox, &oy2);
    fp2_add(&dS, &dS, &dS);
    fp2_add(&dS, &dS, &dS);  // 4*X*Y²

    // X3 = M² - 2S
    fp2_square(&T->x, &dM);
    fp2_sub(&T->x, &T->x, &dS);
    fp2_sub(&T->x, &T->x, &dS);

    // Z3 = 2*Y*Z
    fp2_mul(&T->z, &oy, &oz);
    fp2_add(&T->z, &T->z, &T->z);

    // Y3 = M*(S - X3) - 8*Y⁴
    fp2_square(&dY4, &oy2);  // Y⁴
    fp2_sub(&T->y, &dS, &T->x);
    fp2_mul(&T->y, &dM, &T->y);
    fp2_add(&dY4, &dY4, &dY4);  // 2Y⁴
    fp2_add(&dY4, &dY4, &dY4);  // 4Y⁴
    fp2_add(&dY4, &dY4, &dY4);  // 8Y⁴
    fp2_sub(&T->y, &T->y, &dY4);

    fp2_clear(&ox); fp2_clear(&oy); fp2_clear(&oz);
    fp2_clear(&ox2); fp2_clear(&oy2); fp2_clear(&oz2);
    fp2_clear(&bpz2);
    fp2_clear(&dM); fp2_clear(&dS); fp2_clear(&dY4);
}

// Line function for addition: line through T and Q2 evaluated at Q1
// Updates T to T + Q2
static void line_func_add(line_result_t *l, bn256_g2_t *T,
                          const bn256_g2_t *Q2, const bn256_g1_t *Q1)
{
    // Save T coordinates
    bn256_fp2_t t_z2;
    fp2_init(&t_z2);
    fp2_square(&t_z2, &T->z);

    // U = Q2.x * T.z² - T.x
    bn256_fp2_t U, t1;
    fp2_init(&U); fp2_init(&t1);
    fp2_mul(&U, &Q2->x, &t_z2);
    fp2_sub(&U, &U, &T->x);  // U = Q2.x*Tz² - Tx

    // V = Q2.y * T.z³ - T.y
    bn256_fp2_t V;
    fp2_init(&V);
    fp2_mul(&V, &T->z, &t_z2);  // T.z³
    fp2_mul(&V, &Q2->y, &V);
    fp2_sub(&V, &V, &T->y);  // V = Q2.y*Tz³ - Ty

    // Line coefficients:
    // a = V * T.z * yQ1  (negated for the line equation)
    // b = -U * xQ1
    // c = U * T.y - V * T.x  (evaluated using original T)

    // Actually, the line through T and Q2 evaluated at the G1 point Q1:
    // In the twist setting:
    // l(Q1) encodes as a sparse Fp12 element with:
    // a = (Q2.y*Tz³ - Ty) * Tz * yQ1   ... this needs careful derivation

    // Let me use a simpler approach: compute chord line coefficients
    // l(xQ1, yQ1) = V*Tz*(yQ1) - U*(xQ1) + (U*Ty - V*Tx*Tz^{-1}*...)
    // This is getting complicated. Let me follow go-ethereum more directly.

    // go-ethereum lineFunctionAdd:
    // r0 = T.z² * Q2.y - T.y
    // r1 = T.z² * Q2.x - T.x
    // (r0 is what I called V, r1 is what I called U)

    // a = r1 (the slope denominator, times twist)
    // b = -r0 (the slope numerator, times twist)
    // c = ...

    // Actually from go-ethereum optimalAte.go lineFunctionAdd:
    // The return values a, b, c form the line evaluation
    // a = r1 ← this is U above
    // b = -r0 ← this is -V above
    // c = r0*T.x - r1*T.y (using original T)

    // Then these get combined with Q1 coordinates when multiplied into the accumulator.

    // Line eval components:
    // l.a = U * yQ1  (will be placed in the Fp12 multiplication correctly)
    // l.b = -V * xQ1
    // l.c = V*Tx - U*Ty

    fp2_mul_scalar(&l->a, &U, &Q1->y);
    fp2_neg(&t1, &V);
    fp2_mul_scalar(&l->b, &t1, &Q1->x);
    fp2_mul(&l->c, &V, &T->x);
    fp2_mul(&t1, &U, &T->y);
    fp2_sub(&l->c, &l->c, &t1);

    // Now update T = T + Q2 using addition formulas
    // We have U = Q2.x*Tz² - Tx and V = Q2.y*Tz³ - Ty
    // This is equivalent to the H and r values in standard addition

    bn256_fp2_t U2, H3, S;
    fp2_init(&U2); fp2_init(&H3); fp2_init(&S);

    fp2_square(&U2, &U);        // U²
    fp2_mul(&H3, &U2, &U);     // U³

    // S = T.x * U²
    fp2_mul(&S, &T->x, &U2);

    // T.x = V² - U³ - 2*S
    fp2_square(&T->x, &V);
    fp2_sub(&T->x, &T->x, &H3);
    fp2_sub(&T->x, &T->x, &S);
    fp2_sub(&T->x, &T->x, &S);

    // T.y = V*(S - T.x) - T.y*U³
    fp2_sub(&t1, &S, &T->x);
    fp2_mul(&t1, &V, &t1);
    fp2_mul(&T->y, &T->y, &H3);
    fp2_sub(&T->y, &t1, &T->y);

    // T.z = T.z * U
    fp2_mul(&T->z, &T->z, &U);

    fp2_clear(&t_z2); fp2_clear(&U); fp2_clear(&t1); fp2_clear(&V);
    fp2_clear(&U2); fp2_clear(&H3); fp2_clear(&S);
}

// The BN parameter u and the loop parameter 6u+2
// u = 4965661367071055456
// 6u+2 = 29793968203157093138  -- this is the value used in the Miller loop
// Its NAF representation determines the loop structure.
// We encode 6u+2 in binary with sign bits.
// 6u+2 = 0x19d797039be763ba8 (hex)
// In binary: the loop iterates from MSB to LSB

// Actually, the standard approach uses the binary representation of |6u+2|
// with the sign of each non-zero bit recorded.
// 6u+2 = 29793968203157093138
// The loop parameter in go-ethereum is stored as:
// {0, 0, 0, 1, 0, 1, 0, -1, 0, 0, 1, -1, 0, 0, 1, 0,
//  0, 1, 1, 0, -1, 0, 0, 1, 0, -1, 0, 0, 0, 0, 1, 1,
//  1, 0, 0, -1, 0, 0, 1, 0, 0, 0, 0, 0, -1, 0, 0, 1,
//  1, 0, 0, -1, 0, 0, 0, 1, 1, 0, -1, 0, 0, 1, 0, 1, 1}
// Length 65, iterated from index 64 down to 0

static const int8_t loop_count[] = {
    0, 0, 0, 1, 0, 1, 0, -1, 0, 0, 1, -1, 0, 0, 1, 0,
    0, 1, 1, 0, -1, 0, 0, 1, 0, -1, 0, 0, 0, 0, 1, 1,
    1, 0, 0, -1, 0, 0, 1, 0, 0, 0, 0, 0, -1, 0, 0, 1,
    1, 0, 0, -1, 0, 0, 0, 1, 1, 0, -1, 0, 0, 1, 0, 1, 1,
};
static const int loop_count_len = 65;

// Miller loop: compute f_{6u+2, Q}(P) for one (P, Q) pair
static void miller_loop(bn256_fp12_t *result, const bn256_g1_t *P, const bn256_g2_t *Q)
{
    fp12_set_one(result);

    if (g1_is_infinity(P) || g2_is_infinity(Q))
        return;

    // We need Q in affine for the line functions
    // Q should already be in affine (Z=1) from unmarshal

    // T starts as Q
    bn256_g2_t T;
    bn256_g2_init(&T);
    g2_copy(&T, Q);

    line_result_t line;
    line_init(&line);

    bn256_fp12_t tmp;
    fp12_init(&tmp);

    // Iterate from loop_count_len-2 down to 0 (MSB is always 1, start with T=Q)
    for (int i = loop_count_len - 2; i >= 0; i--)
    {
        fp12_square(&tmp, result);
        fp12_copy(result, &tmp);

        // Doubling step
        line_func_double(&line, &T, P);
        fp12_mul_line(&tmp, result, &line);
        fp12_copy(result, &tmp);

        if (loop_count[i] == 1)
        {
            // Addition step with Q
            line_func_add(&line, &T, Q, P);
            fp12_mul_line(&tmp, result, &line);
            fp12_copy(result, &tmp);
        }
        else if (loop_count[i] == -1)
        {
            // Addition step with -Q
            bn256_g2_t negQ;
            bn256_g2_init(&negQ);
            g2_copy(&negQ, Q);
            fp2_neg(&negQ.y, &negQ.y);

            line_func_add(&line, &T, &negQ, P);
            fp12_mul_line(&tmp, result, &line);
            fp12_copy(result, &tmp);

            bn256_g2_clear(&negQ);
        }
    }

    // Final steps: two more additions with Frobenius-transformed Q
    // Q1 = π_p(Q), Q2 = π_{p²}(Q)
    // These involve applying the p-power Frobenius to Q's coordinates

    // Q1 = (conjugate(Q.x) * ξ^((p-1)/3), conjugate(Q.y) * ξ^((p-1)/2))
    frobenius_ensure_init();
    bn256_g2_t Q1, Q2;
    bn256_g2_init(&Q1);
    bn256_g2_init(&Q2);

    fp2_conj(&Q1.x, &Q->x);
    fp2_mul(&Q1.x, &Q1.x, &frob_xi_to_p_minus_1_over_3);
    fp2_conj(&Q1.y, &Q->y);
    fp2_mul(&Q1.y, &Q1.y, &frob_xi_to_p_minus_1_over_2);
    fp_set_ui(&Q1.z.a, 1);
    fp_set_ui(&Q1.z.b, 0);

    // Q2 = (Q.x * ξ^((p²-1)/3), -Q.y * ξ^((p²-1)/2))
    // Note: conjugate twice = identity, so no conjugate for p²
    fp2_mul(&Q2.x, &Q->x, &frob_xi_to_p_sq_minus_1_over_3);
    // ξ^((p²-1)/2) = ξ^((p²-1)/6) * ξ^((p²-1)/3)
    bn256_fp2_t xi_p2_half;
    fp2_init(&xi_p2_half);
    fp2_mul(&xi_p2_half, &frob_xi_to_p_sq_minus_1_over_6, &frob_xi_to_p_sq_minus_1_over_3);
    fp2_mul(&Q2.y, &Q->y, &xi_p2_half);
    fp2_neg(&Q2.y, &Q2.y);
    fp_set_ui(&Q2.z.a, 1);
    fp_set_ui(&Q2.z.b, 0);

    // Add Q1
    line_func_add(&line, &T, &Q1, P);
    fp12_mul_line(&tmp, result, &line);
    fp12_copy(result, &tmp);

    // Add Q2
    line_func_add(&line, &T, &Q2, P);
    fp12_mul_line(&tmp, result, &line);
    fp12_copy(result, &tmp);

    fp2_clear(&xi_p2_half);
    bn256_g2_clear(&Q1);
    bn256_g2_clear(&Q2);
    bn256_g2_clear(&T);
    line_clear(&line);
    fp12_clear(&tmp);
}

// Final exponentiation: f^((p¹² - 1) / r)
// Decomposed as:
//   easy part: f^(p⁶ - 1) * f^(p² + 1)
//   hard part: f^((p⁴ - p² + 1) / r)  using BN-specific formula
static void final_exponentiation(bn256_fp12_t *result, const bn256_fp12_t *f)
{
    // Easy part 1: f^(p⁶ - 1)
    // f^(p⁶) = conjugate(f) for Fp12 = Fp6[w]/(w²-v)
    bn256_fp12_t t0, t1, t2, t3, fp, fp2_val, fp3, fu, fu2, fu3;
    fp12_init(&t0); fp12_init(&t1); fp12_init(&t2); fp12_init(&t3);
    fp12_init(&fp); fp12_init(&fp2_val); fp12_init(&fp3);
    fp12_init(&fu); fp12_init(&fu2); fp12_init(&fu3);

    fp12_conj(&t0, f);         // f^(p⁶) = conj(f)
    fp12_inv(&t1, f);          // f^(-1)
    fp12_mul(&t0, &t0, &t1);   // f^(p⁶ - 1)

    // Easy part 2: (f^(p⁶-1))^(p² + 1)
    fp12_frobenius_p2(&t1, &t0);  // t0^(p²)
    fp12_mul(&t0, &t0, &t1);      // t0^(p² + 1)

    // Now t0 = f^((p⁶-1)(p²+1))

    // Hard part: t0^((p⁴ - p² + 1) / r)
    // Using the BN-specific algorithm from go-ethereum finalExponentiation
    // This uses the decomposition:
    // (p⁴ - p² + 1)/r = (p³ + p²(6u²+1) + p(-36u³-18u²-12u-1) + (-36u³-18u²-6u-1)) · (1/r')
    // But in practice, we use the formula involving exponentiations by u.

    // u = 4965661367071055456 (the BN parameter)
    mpz_t u_val;
    mpz_init_set_str(u_val, "4965661367071055456", 10);

    // Compute various powers of t0 by u
    fp12_exp(&fu, &t0, u_val);      // t0^u
    fp12_exp(&fu2, &fu, u_val);     // t0^(u²)
    fp12_exp(&fu3, &fu2, u_val);    // t0^(u³)

    // Frobenius maps
    fp12_frobenius(&fp, &t0);       // t0^p
    fp12_frobenius(&t1, &fu);       // fu^p (t0^(u*p))
    fp12_frobenius(&t2, &fu2);      // fu2^p (t0^(u²*p))

    bn256_fp12_t fp2p, fp3p, fu2p, fu3p;
    fp12_init(&fp2p); fp12_init(&fp3p); fp12_init(&fu2p); fp12_init(&fu3p);

    fp12_frobenius_p2(&fp2_val, &t0);  // t0^(p²)
    fp12_frobenius(&fp3, &fp2_val);    // t0^(p³) = (t0^(p²))^p

    fp12_frobenius_p2(&fu2p, &fu);     // fu^(p²)
    fp12_frobenius(&fu3p, &fu2);       // fu2^p

    // y0 = fp * fp2 * fp3
    bn256_fp12_t y0, y1, y2, y3, y4, y5, y6;
    fp12_init(&y0); fp12_init(&y1); fp12_init(&y2);
    fp12_init(&y3); fp12_init(&y4); fp12_init(&y5); fp12_init(&y6);

    fp12_mul(&y0, &fp, &fp2_val);
    fp12_mul(&y0, &y0, &fp3);

    // y1 = conj(t0)  (t0^(-1) in the unitary group)
    fp12_conj(&y1, &t0);

    // y2 = fu2^(p²) = fu2p ... actually this is t2 computed above
    // Let me be more careful with the formula.
    // Following go-ethereum's finalExponentiation:

    // y5 = conj(fu2)
    fp12_conj(&y5, &fu2);

    // y3 = conj(frobenius(fu))  ... that's conj of t1 from above
    fp12_conj(&y3, &t1);

    // y4 = conj(fu * fu2p)
    fp12_mul(&y4, &fu, &fu2p);
    fp12_conj(&y4, &y4);

    // y6 = conj(fu3 * fu3p)
    fp12_mul(&y6, &fu3, &fu3p);
    fp12_conj(&y6, &y6);

    // t0 = y6^2 * y4 * y5
    fp12_square(&t1, &y6);
    fp12_mul(&t1, &t1, &y4);
    fp12_mul(&t1, &t1, &y5);

    // t1 = t0 * y3 * t0^2  ← confusing naming, let me use different vars
    // Following more carefully:
    // In go-ethereum:
    // t0 = y6² · y4 · y5
    // t1 = y3 · y5 · t0
    // t0 = t0 · y2
    // t1 = t1² · t0
    // t1 = t1²
    // t0 = t1 · y1
    // t1 = t1 · y0
    // t0 = t0² · t1

    bn256_fp12_t T0, T1;
    fp12_init(&T0); fp12_init(&T1);

    // T0 = y6² · y4 · y5
    fp12_square(&T0, &y6);
    fp12_mul(&T0, &T0, &y4);
    fp12_mul(&T0, &T0, &y5);

    // T1 = y3 · y5 · T0
    fp12_mul(&T1, &y3, &y5);
    fp12_mul(&T1, &T1, &T0);

    // T0 = T0 · y2 (which is t2 = fu2^p from above)
    fp12_mul(&T0, &T0, &t2);

    // T1 = T1² · T0
    fp12_square(&T1, &T1);
    fp12_mul(&T1, &T1, &T0);

    // T1 = T1²
    fp12_square(&T1, &T1);

    // T0 = T1 · y1
    fp12_mul(&T0, &T1, &y1);

    // T1 = T1 · y0
    fp12_mul(&T1, &T1, &y0);

    // result = T0² · T1
    fp12_square(&T0, &T0);
    fp12_mul(result, &T0, &T1);

    mpz_clear(u_val);
    fp12_clear(&t0); fp12_clear(&t1); fp12_clear(&t2); fp12_clear(&t3);
    fp12_clear(&fp); fp12_clear(&fp2_val); fp12_clear(&fp3);
    fp12_clear(&fu); fp12_clear(&fu2); fp12_clear(&fu3);
    fp12_clear(&fp2p); fp12_clear(&fp3p); fp12_clear(&fu2p); fp12_clear(&fu3p);
    fp12_clear(&y0); fp12_clear(&y1); fp12_clear(&y2);
    fp12_clear(&y3); fp12_clear(&y4); fp12_clear(&y5); fp12_clear(&y6);
    fp12_clear(&T0); fp12_clear(&T1);
}

int bn256_pairing_check(const bn256_g1_t *g1_points,
                        const bn256_g2_t *g2_points,
                        size_t n)
{
    bn256_ensure_init();

    if (n == 0)
        return 1;  // empty pairing check passes

    // Accumulate Miller loops
    bn256_fp12_t acc, ml;
    fp12_init(&acc);
    fp12_init(&ml);
    fp12_set_one(&acc);

    for (size_t i = 0; i < n; i++)
    {
        if (g1_is_infinity(&g1_points[i]) || g2_is_infinity(&g2_points[i]))
            continue;  // e(O, Q) = 1, contributes nothing

        miller_loop(&ml, &g1_points[i], &g2_points[i]);
        bn256_fp12_t tmp;
        fp12_init(&tmp);
        fp12_mul(&tmp, &acc, &ml);
        fp12_copy(&acc, &tmp);
        fp12_clear(&tmp);
    }

    // Final exponentiation
    bn256_fp12_t result;
    fp12_init(&result);
    final_exponentiation(&result, &acc);

    int check = fp12_is_one(&result) ? 1 : 0;

    fp12_clear(&acc);
    fp12_clear(&ml);
    fp12_clear(&result);

    return check;
}
