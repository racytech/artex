#include "banderwagon.h"
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Byte Constants (used during lazy initialization)
 * ========================================================================= */

/* Bandersnatch curve parameter d (big-endian) */
static const uint8_t D_BYTES_BE[32] = {
    0x63, 0x89, 0xc1, 0x26, 0x33, 0xc2, 0x67, 0xcb,
    0xc6, 0x6e, 0x3b, 0xf8, 0x6b, 0xe3, 0xb6, 0xd8,
    0xcb, 0x66, 0x67, 0x71, 0x77, 0xe5, 0x4f, 0x92,
    0xb3, 0x69, 0xf2, 0xf5, 0x18, 0x8d, 0x58, 0xe7,
};

/* Generator X coordinate (big-endian) */
static const uint8_t GEN_X_BE[32] = {
    0x29, 0xc1, 0x32, 0xcc, 0x2c, 0x0b, 0x34, 0xc5,
    0x74, 0x37, 0x11, 0x77, 0x7b, 0xbe, 0x42, 0xf3,
    0x2b, 0x79, 0xc0, 0x22, 0xad, 0x99, 0x84, 0x65,
    0xe1, 0xe7, 0x18, 0x66, 0xa2, 0x52, 0xae, 0x18,
};

/* Generator Y coordinate (big-endian) */
static const uint8_t GEN_Y_BE[32] = {
    0x2a, 0x6c, 0x66, 0x9e, 0xda, 0x12, 0x3e, 0x0f,
    0x15, 0x7d, 0x8b, 0x50, 0xba, 0xdc, 0xd5, 0x86,
    0x35, 0x8c, 0xad, 0x81, 0xee, 0xe4, 0x64, 0x60,
    0x5e, 0x31, 0x67, 0xb6, 0xcc, 0x97, 0x41, 0x66,
};

/* Bandersnatch scalar field modulus r (little-endian) */
const uint8_t FR_MODULUS[32] = {
    0xe1, 0xe7, 0x76, 0x28, 0xb5, 0x06, 0xfd, 0x74,
    0x71, 0x04, 0x19, 0x74, 0x00, 0x87, 0x8f, 0xff,
    0x00, 0x76, 0x68, 0x02, 0x02, 0x76, 0xce, 0x0c,
    0x52, 0x5f, 0x67, 0xca, 0xd4, 0x69, 0xfb, 0x1c,
};

/*
 * (p-1)/2 in uint64 limbs (LE) for lex_largest check.
 * p = 0x73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001
 */
static const uint64_t HALF_P[4] = {
    0x7fffffff80000000ULL,
    0xa9ded2017fff2dffULL,
    0x199cec0404d0ec02ULL,
    0x39f6d3a994cebea4ULL,
};

/*
 * Tonelli-Shanks parameters for sqrt in Fp.
 * p - 1 = 2^32 * Q  where Q is odd.
 */
#define SQRT_S 32

/* Q = (p-1) >> 32, as uint64 LE limbs */
static const uint64_t SQRT_Q[4] = {
    0xfffe5bfefffffffFULL,
    0x09a1d80553bda402ULL,
    0x299d7d483339d808ULL,
    0x0000000073eda753ULL,
};

/* (Q+1)/2, as uint64 LE limbs */
static const uint64_t SQRT_Q_PLUS1_DIV2[4] = {
    0x7fff2dff80000000ULL,
    0x04d0ec02a9ded201ULL,
    0x94cebea4199cec04ULL,
    0x0000000039f6d3a9ULL,
};

/* =========================================================================
 * Globals (initialized lazily)
 * ========================================================================= */

fp_t FP_ZERO;
fp_t FP_ONE;
fp_t BANDERSNATCH_A;
fp_t BANDERSNATCH_D;
banderwagon_point_t BANDERWAGON_IDENTITY;
banderwagon_point_t BANDERWAGON_GENERATOR;

static bool bw_initialized = false;

/* Forward declaration — needs fp wrappers */
static void bw_init(void);

static inline void ensure_init(void) {
    if (__builtin_expect(!bw_initialized, 0)) bw_init();
}

/* =========================================================================
 * Fp Wrappers
 * ========================================================================= */

void banderwagon_init(void) {
    ensure_init();
}

void fp_add(fp_t *out, const fp_t *a, const fp_t *b) {
    blst_fr_add(out, a, b);
}

void fp_sub(fp_t *out, const fp_t *a, const fp_t *b) {
    blst_fr_sub(out, a, b);
}

void fp_mul(fp_t *out, const fp_t *a, const fp_t *b) {
    blst_fr_mul(out, a, b);
}

void fp_sqr(fp_t *out, const fp_t *a) {
    blst_fr_sqr(out, a);
}

void fp_neg(fp_t *out, const fp_t *a) {
    blst_fr_cneg(out, a, true);
}

void fp_inv(fp_t *out, const fp_t *a) {
    blst_fr_inverse(out, a);
}

bool fp_eq(const fp_t *a, const fp_t *b) {
    return a->l[0] == b->l[0] && a->l[1] == b->l[1] &&
           a->l[2] == b->l[2] && a->l[3] == b->l[3];
}

bool fp_is_zero(const fp_t *a) {
    return (a->l[0] | a->l[1] | a->l[2] | a->l[3]) == 0;
}

void fp_from_uint64(fp_t *out, uint64_t v) {
    uint64_t limbs[4] = { v, 0, 0, 0 };
    blst_fr_from_uint64(out, limbs);
}

void fp_from_bytes_le(fp_t *out, const uint8_t bytes[32]) {
    blst_scalar s;
    blst_scalar_from_lendian(&s, bytes);
    blst_fr_from_scalar(out, &s);
}

void fp_to_bytes_le(uint8_t out[32], const fp_t *a) {
    blst_scalar s;
    blst_scalar_from_fr(&s, a);
    blst_lendian_from_scalar(out, &s);
}

void fp_from_bytes_be(fp_t *out, const uint8_t bytes[32]) {
    blst_scalar s;
    blst_scalar_from_bendian(&s, bytes);
    blst_fr_from_scalar(out, &s);
}

void fp_to_bytes_be(uint8_t out[32], const fp_t *a) {
    blst_scalar s;
    blst_scalar_from_fr(&s, a);
    blst_bendian_from_scalar(out, &s);
}

/* =========================================================================
 * Internal Fp Helpers
 * ========================================================================= */

/** Exponentiation: out = base^exp, exp as 4 uint64 limbs (LE). */
static void fp_pow(fp_t *out, const fp_t *base, const uint64_t exp[4]) {
    fp_t result, b;
    ensure_init();
    result = FP_ONE;
    b = *base;

    for (int i = 0; i < 4; i++) {
        uint64_t e = exp[i];
        for (int j = 0; j < 64; j++) {
            if (e & 1)
                fp_mul(&result, &result, &b);
            fp_sqr(&b, &b);
            e >>= 1;
        }
    }
    *out = result;
}

/**
 * Tonelli-Shanks square root in Fp.
 * Returns true if a is a QR and sets *out to sqrt(a).
 * Returns false if a is not a QR.
 *
 * Uses S=32, QNR=7 for the BLS12-381 scalar field.
 */
static bool fp_sqrt(fp_t *out, const fp_t *a) {
    ensure_init();

    if (fp_is_zero(a)) {
        *out = FP_ZERO;
        return true;
    }

    /* g = 7 (quadratic non-residue for BLS12-381 scalar field) */
    fp_t g;
    fp_from_uint64(&g, 7);

    /* z = g^Q */
    fp_t z;
    fp_pow(&z, &g, SQRT_Q);

    /* t = a^Q */
    fp_t t;
    fp_pow(&t, a, SQRT_Q);

    /* r = a^((Q+1)/2) */
    fp_t r;
    fp_pow(&r, a, SQRT_Q_PLUS1_DIV2);

    uint32_t m = SQRT_S;
    fp_t c = z;

    for (int iter = 0; iter < 256; iter++) {
        if (fp_eq(&t, &FP_ONE)) {
            *out = r;
            return true;
        }

        /* Find least i such that t^(2^i) == 1 */
        uint32_t i;
        fp_t tmp = t;
        for (i = 1; i < m; i++) {
            fp_sqr(&tmp, &tmp);
            if (fp_eq(&tmp, &FP_ONE))
                break;
        }

        if (i >= m)
            return false;  /* not a QR */

        /* b = c^(2^(m-i-1)) */
        fp_t b = c;
        for (uint32_t j = 0; j < m - i - 1; j++)
            fp_sqr(&b, &b);

        m = i;
        fp_sqr(&c, &b);      /* c = b^2       */
        fp_mul(&t, &t, &c);  /* t = t * b^2   */
        fp_mul(&r, &r, &b);  /* r = r * b     */
    }

    return false;  /* not a QR (exhausted iterations) */
}

/**
 * Check if field element is "lexicographically largest",
 * i.e., its canonical representation > (p-1)/2.
 */
static bool fp_is_lex_largest(const fp_t *a) {
    uint64_t limbs[4];
    blst_uint64_from_fr(limbs, a);

    /* Compare limbs against HALF_P (MSB first) */
    for (int i = 3; i >= 0; i--) {
        if (limbs[i] > HALF_P[i]) return true;
        if (limbs[i] < HALF_P[i]) return false;
    }
    return false;  /* equal to (p-1)/2 — not strictly greater */
}

/* =========================================================================
 * Lazy Initialization
 * ========================================================================= */

static void bw_init(void) {
    if (bw_initialized) return;

    /* FP_ZERO */
    uint64_t zero[4] = {0, 0, 0, 0};
    blst_fr_from_uint64(&FP_ZERO, zero);

    /* FP_ONE */
    uint64_t one[4] = {1, 0, 0, 0};
    blst_fr_from_uint64(&FP_ONE, one);

    /* BANDERSNATCH_A = -5 mod p */
    fp_t five;
    fp_from_uint64(&five, 5);
    fp_neg(&BANDERSNATCH_A, &five);

    /* BANDERSNATCH_D */
    fp_from_bytes_be(&BANDERSNATCH_D, D_BYTES_BE);

    /* BANDERWAGON_IDENTITY = (0, 1, 1, 0) */
    BANDERWAGON_IDENTITY.X = FP_ZERO;
    BANDERWAGON_IDENTITY.Y = FP_ONE;
    BANDERWAGON_IDENTITY.Z = FP_ONE;
    BANDERWAGON_IDENTITY.T = FP_ZERO;

    /* BANDERWAGON_GENERATOR */
    fp_from_bytes_be(&BANDERWAGON_GENERATOR.X, GEN_X_BE);
    fp_from_bytes_be(&BANDERWAGON_GENERATOR.Y, GEN_Y_BE);
    BANDERWAGON_GENERATOR.Z = FP_ONE;
    fp_mul(&BANDERWAGON_GENERATOR.T,
           &BANDERWAGON_GENERATOR.X,
           &BANDERWAGON_GENERATOR.Y);

    bw_initialized = true;
}

/* =========================================================================
 * Point Operations
 * ========================================================================= */

/**
 * Unified addition for twisted Edwards curve ax^2 + y^2 = 1 + dx^2y^2
 * in extended coordinates (X, Y, Z, T) where T = X*Y/Z.
 *
 * Formula (https://hyperelliptic.org/EFD/g1p/auto-twisted-extended.html):
 *   A = X1*X2, B = Y1*Y2, C = T1*d*T2, D = Z1*Z2
 *   E = (X1+Y1)*(X2+Y2) - A - B
 *   F = D - C, G = D + C, H = B - a*A
 *   X3 = E*F, Y3 = G*H, T3 = E*H, Z3 = F*G
 */
void banderwagon_add(banderwagon_point_t *out,
                     const banderwagon_point_t *p,
                     const banderwagon_point_t *q)
{
    ensure_init();

    fp_t A, B, C, D, E, F, G, H;
    fp_t tmp1, tmp2;

    fp_mul(&A, &p->X, &q->X);                     /* A = X1*X2        */
    fp_mul(&B, &p->Y, &q->Y);                     /* B = Y1*Y2        */
    fp_mul(&C, &p->T, &q->T);
    fp_mul(&C, &C, &BANDERSNATCH_D);               /* C = T1*d*T2      */
    fp_mul(&D, &p->Z, &q->Z);                     /* D = Z1*Z2        */

    fp_add(&tmp1, &p->X, &p->Y);                  /* tmp1 = X1+Y1     */
    fp_add(&tmp2, &q->X, &q->Y);                  /* tmp2 = X2+Y2     */
    fp_mul(&E, &tmp1, &tmp2);                      /* E = (X1+Y1)(X2+Y2) */
    fp_sub(&E, &E, &A);
    fp_sub(&E, &E, &B);                            /* E = E - A - B    */

    fp_sub(&F, &D, &C);                            /* F = D - C        */
    fp_add(&G, &D, &C);                            /* G = D + C        */

    /* H = B - a*A.  Since a = -5, a*A = -5A, so H = B + 5A. */
    fp_mul(&tmp1, &A, &BANDERSNATCH_A);            /* tmp1 = a*A       */
    fp_sub(&H, &B, &tmp1);                         /* H = B - a*A      */

    fp_mul(&out->X, &E, &F);                      /* X3 = E*F         */
    fp_mul(&out->Y, &G, &H);                      /* Y3 = G*H         */
    fp_mul(&out->T, &E, &H);                      /* T3 = E*H         */
    fp_mul(&out->Z, &F, &G);                      /* Z3 = F*G         */
}

/**
 * Dedicated doubling for twisted Edwards in extended coordinates.
 *
 * Formula:
 *   A = X1^2, B = Y1^2, C = 2*Z1^2
 *   D = a*A, E = (X1+Y1)^2 - A - B
 *   G = D + B, F = G - C, H = D - B
 *   X3 = E*F, Y3 = G*H, T3 = E*H, Z3 = F*G
 */
void banderwagon_double(banderwagon_point_t *out,
                        const banderwagon_point_t *p)
{
    ensure_init();

    fp_t A, B, C, D, E, F, G, H;
    fp_t tmp;

    fp_sqr(&A, &p->X);                            /* A = X1^2         */
    fp_sqr(&B, &p->Y);                            /* B = Y1^2         */
    fp_sqr(&C, &p->Z);
    fp_add(&C, &C, &C);                            /* C = 2*Z1^2       */

    fp_mul(&D, &A, &BANDERSNATCH_A);               /* D = a*A          */

    fp_add(&tmp, &p->X, &p->Y);
    fp_sqr(&E, &tmp);
    fp_sub(&E, &E, &A);
    fp_sub(&E, &E, &B);                            /* E = (X+Y)^2-A-B */

    fp_add(&G, &D, &B);                            /* G = D + B        */
    fp_sub(&F, &G, &C);                            /* F = G - C        */
    fp_sub(&H, &D, &B);                            /* H = D - B        */

    fp_mul(&out->X, &E, &F);                      /* X3 = E*F         */
    fp_mul(&out->Y, &G, &H);                      /* Y3 = G*H         */
    fp_mul(&out->T, &E, &H);                      /* T3 = E*H         */
    fp_mul(&out->Z, &F, &G);                      /* Z3 = F*G         */
}

/**
 * Negation in twisted Edwards: -(X, Y, Z, T) = (-X, Y, Z, -T)
 */
void banderwagon_neg(banderwagon_point_t *out,
                     const banderwagon_point_t *p)
{
    fp_neg(&out->X, &p->X);
    out->Y = p->Y;
    out->Z = p->Z;
    fp_neg(&out->T, &p->T);
}

/* =========================================================================
 * Scalar Multiplication
 * ========================================================================= */

/**
 * Double-and-add scalar multiplication (MSB to LSB).
 * scalar is 32 bytes little-endian.
 */
void banderwagon_scalar_mul(banderwagon_point_t *out,
                            const banderwagon_point_t *p,
                            const uint8_t scalar[32])
{
    ensure_init();

    *out = BANDERWAGON_IDENTITY;

    /* Find the highest set bit */
    int top_byte = 31;
    while (top_byte >= 0 && scalar[top_byte] == 0) top_byte--;
    if (top_byte < 0) return;  /* scalar is zero */

    int top_bit = 7;
    while (top_bit >= 0 && !(scalar[top_byte] & (1 << top_bit))) top_bit--;

    /* Process bits from MSB to LSB */
    for (int i = top_byte; i >= 0; i--) {
        int start_bit = (i == top_byte) ? top_bit : 7;
        for (int j = start_bit; j >= 0; j--) {
            banderwagon_double(out, out);
            if (scalar[i] & (1 << j))
                banderwagon_add(out, out, p);
        }
    }
}

/**
 * Extract `num_bits` bits from a 32-byte LE scalar starting at `bit_start`.
 */
static unsigned int extract_scalar_bits(const uint8_t scalar[32],
                                        int bit_start, int num_bits)
{
    unsigned int result = 0;
    for (int b = 0; b < num_bits; b++) {
        int pos = bit_start + b;
        if (pos >= 256) break;
        if (scalar[pos >> 3] & (1u << (pos & 7)))
            result |= (1u << b);
    }
    return result;
}

/**
 * Check if a 32-byte scalar is all zeros.
 */
static bool scalar_is_zero(const uint8_t s[32]) {
    uint64_t acc = 0;
    for (int i = 0; i < 4; i++)
        acc |= ((const uint64_t *)s)[i];
    return acc == 0;
}

/**
 * Multi-scalar multiplication: out = sum(scalars[i] * points[i]).
 * Each scalar is 32 bytes little-endian.
 *
 * Uses Pippenger's bucket method for large inputs (~5-10x faster than naive
 * for 256 points). Falls back to naive for small counts.
 *
 * Algorithm (MSB-to-LSB Horner evaluation):
 *   1. Choose window size c ≈ log2(n)
 *   2. For each window w from highest to 0:
 *      a. result <<= c  (c doublings)
 *      b. Distribute points into 2^c-1 buckets by scalar window bits
 *      c. Compute weighted bucket sum via running-sum technique
 *      d. result += window_sum
 */
void banderwagon_msm(banderwagon_point_t *out,
                     const banderwagon_point_t *points,
                     const uint8_t (*scalars)[32],
                     size_t count)
{
    ensure_init();
    *out = BANDERWAGON_IDENTITY;

    if (count == 0) return;

    /* Pre-filter non-zero scalars to skip empty slots (common in sparse leaves) */
    size_t *nz = malloc(count * sizeof(size_t));
    if (!nz) return;  /* OOM fallback: identity */
    size_t nz_count = 0;

    for (size_t i = 0; i < count; i++) {
        if (!scalar_is_zero(scalars[i]))
            nz[nz_count++] = i;
    }

    if (nz_count == 0) {
        free(nz);
        return;  /* all scalars zero → identity */
    }

    /* For very small non-zero count, use naive scalar_mul */
    if (nz_count <= 4) {
        for (size_t k = 0; k < nz_count; k++) {
            size_t i = nz[k];
            banderwagon_point_t tmp;
            banderwagon_scalar_mul(&tmp, &points[i], scalars[i]);
            banderwagon_add(out, out, &tmp);
        }
        free(nz);
        return;
    }

    /* Choose window size: c ≈ floor(log2(nz_count)), capped at 16 */
    int c = 1;
    while ((1UL << (c + 1)) <= nz_count && c < 16) c++;

    int num_buckets = (1 << c) - 1;
    int num_windows = (253 + c - 1) / c;

    banderwagon_point_t *buckets = malloc((size_t)num_buckets *
                                          sizeof(banderwagon_point_t));
    if (!buckets) {
        /* OOM fallback: naive */
        for (size_t k = 0; k < nz_count; k++) {
            size_t i = nz[k];
            banderwagon_point_t tmp;
            banderwagon_scalar_mul(&tmp, &points[i], scalars[i]);
            banderwagon_add(out, out, &tmp);
        }
        free(nz);
        return;
    }

    /* Process windows from MSB to LSB (Horner evaluation) */
    for (int w = num_windows - 1; w >= 0; w--) {
        /* Shift accumulator left by c bits */
        for (int d = 0; d < c; d++)
            banderwagon_double(out, out);

        /* Clear buckets to identity */
        for (int j = 0; j < num_buckets; j++)
            buckets[j] = BANDERWAGON_IDENTITY;

        /* Distribute points into buckets based on window bits */
        int bit_start = w * c;
        for (size_t k = 0; k < nz_count; k++) {
            size_t i = nz[k];
            unsigned int idx = extract_scalar_bits(scalars[i], bit_start, c);
            if (idx == 0) continue;
            banderwagon_add(&buckets[idx - 1], &buckets[idx - 1], &points[i]);
        }

        /* Running-sum technique to compute weighted bucket sum:
         * window_sum = 1*bucket[0] + 2*bucket[1] + ... + num_buckets*bucket[num_buckets-1]
         */
        banderwagon_point_t running = BANDERWAGON_IDENTITY;
        banderwagon_point_t window_sum = BANDERWAGON_IDENTITY;
        for (int j = num_buckets - 1; j >= 0; j--) {
            banderwagon_add(&running, &running, &buckets[j]);
            banderwagon_add(&window_sum, &window_sum, &running);
        }

        banderwagon_add(out, out, &window_sum);
    }

    free(buckets);
    free(nz);
}

/* =========================================================================
 * Mixed Addition (extended + normalized, Z_q = 1)
 * ========================================================================= */

/**
 * Add extended point p + normalized point q (where q has Z=1).
 * Same formula as banderwagon_add but with D = Z1 (saves 1 fp_mul).
 *
 *   A = X1*X2, B = Y1*Y2, C = T1*d*T2, D = Z1
 *   E = (X1+Y1)*(X2+Y2) - A - B
 *   F = D - C, G = D + C, H = B - a*A
 *   X3 = E*F, Y3 = G*H, T3 = E*H, Z3 = F*G
 */
void banderwagon_add_mixed(banderwagon_point_t *out,
                           const banderwagon_point_t *p,
                           const banderwagon_point_norm_t *q)
{
    fp_t A, B, C, D, E, F, G, H;
    fp_t tmp1, tmp2;

    fp_mul(&A, &p->X, &q->X);                     /* A = X1*X2        */
    fp_mul(&B, &p->Y, &q->Y);                     /* B = Y1*Y2        */
    fp_mul(&C, &p->T, &q->T);
    fp_mul(&C, &C, &BANDERSNATCH_D);               /* C = T1*d*T2      */
    D = p->Z;                                      /* D = Z1 (Z2 = 1)  */

    fp_add(&tmp1, &p->X, &p->Y);                  /* tmp1 = X1+Y1     */
    fp_add(&tmp2, &q->X, &q->Y);                  /* tmp2 = X2+Y2     */
    fp_mul(&E, &tmp1, &tmp2);                      /* E = (X1+Y1)(X2+Y2) */
    fp_sub(&E, &E, &A);
    fp_sub(&E, &E, &B);                            /* E = E - A - B    */

    fp_sub(&F, &D, &C);                            /* F = D - C        */
    fp_add(&G, &D, &C);                            /* G = D + C        */

    /* H = B - a*A.  Since a = -5, a*A = -5A, so H = B + 5A. */
    fp_mul(&tmp1, &A, &BANDERSNATCH_A);            /* tmp1 = a*A       */
    fp_sub(&H, &B, &tmp1);                         /* H = B - a*A      */

    fp_mul(&out->X, &E, &F);                      /* X3 = E*F         */
    fp_mul(&out->Y, &G, &H);                      /* Y3 = G*H         */
    fp_mul(&out->T, &E, &H);                      /* T3 = E*H         */
    fp_mul(&out->Z, &F, &G);                      /* Z3 = F*G         */
}

/* =========================================================================
 * Precomputed Fixed-Base MSM
 * ========================================================================= */

/**
 * Batch-normalize an array of extended points to Z=1 using Montgomery's trick.
 * Only 1 fp_inv for the entire batch.
 */
static void batch_normalize(banderwagon_point_norm_t *out,
                            const banderwagon_point_t *points,
                            size_t n)
{
    if (n == 0) return;

    /* Accumulate products of Z coordinates */
    fp_t *products = malloc(n * sizeof(fp_t));
    products[0] = points[0].Z;
    for (size_t i = 1; i < n; i++)
        fp_mul(&products[i], &products[i - 1], &points[i].Z);

    /* Single inversion of the accumulated product */
    fp_t inv;
    fp_inv(&inv, &products[n - 1]);

    /* Walk backwards to recover individual Z inverses.
     * For each point: X_norm = X/Z, Y_norm = Y/Z, T_norm = X_norm * Y_norm */
    for (size_t i = n - 1; ; i--) {
        fp_t z_inv;
        if (i > 0) {
            fp_mul(&z_inv, &inv, &products[i - 1]);
            fp_mul(&inv, &inv, &points[i].Z);
        } else {
            z_inv = inv;
        }

        fp_mul(&out[i].X, &points[i].X, &z_inv);
        fp_mul(&out[i].Y, &points[i].Y, &z_inv);
        fp_mul(&out[i].T, &out[i].X, &out[i].Y);

        if (i == 0) break;
    }

    free(products);
}

/**
 * Build precomputed table for one base point.
 *
 * For each of 32 windows (w=8 bits):
 *   table[w][k] = (k+1) * G_shifted,  k = 0..127
 *   where G_shifted = G * 2^(w*8)
 *
 * All entries are normalized (Z=1) for mixed addition.
 */
static void precomp_build_point(banderwagon_precomp_point_t *pp,
                                const banderwagon_point_t *base)
{
    banderwagon_point_t shifted = *base;
    banderwagon_point_t tmp[PRECOMP_TABLE_SIZE];

    for (int w = 0; w < PRECOMP_NUM_WINDOWS; w++) {
        /* Build multiples: tmp[0] = 1*shifted, tmp[k] = (k+1)*shifted */
        tmp[0] = shifted;
        for (int k = 1; k < PRECOMP_TABLE_SIZE; k++)
            banderwagon_add(&tmp[k], &tmp[k - 1], &shifted);

        /* Batch-normalize to Z=1 */
        batch_normalize(pp->windows[w], tmp, PRECOMP_TABLE_SIZE);

        /* Advance shifted by 2^8 (8 doublings) */
        for (int d = 0; d < PRECOMP_WINDOW_BITS; d++)
            banderwagon_double(&shifted, &shifted);
    }
}

void banderwagon_precomp_msm_init(banderwagon_precomp_msm_t *msm,
                                   const banderwagon_point_t *bases,
                                   size_t count)
{
    msm->count = count;
    msm->points = malloc(count * sizeof(banderwagon_precomp_point_t));

    for (size_t i = 0; i < count; i++)
        precomp_build_point(&msm->points[i], &bases[i]);
}

void banderwagon_precomp_msm_free(banderwagon_precomp_msm_t *msm)
{
    if (msm->points) {
        free(msm->points);
        msm->points = NULL;
    }
    msm->count = 0;
}

/**
 * Negate a normalized point: -(X, Y, T) = (-X, Y, -T)
 */
static void negate_norm(banderwagon_point_norm_t *out,
                        const banderwagon_point_norm_t *p)
{
    fp_neg(&out->X, &p->X);
    out->Y = p->Y;
    fp_neg(&out->T, &p->T);
}

/**
 * Accumulate scalar * base into acc using precomputed windowed table.
 *
 * Uses sign trick: for each 8-bit window value v:
 *   if v == 0: skip
 *   if v < 128: acc += table[w][v-1]
 *   if v >= 128: acc -= table[w][256-v-1], carry 1 to next window
 */
void banderwagon_precomp_scalar_mul(banderwagon_point_t *acc,
                                     const banderwagon_precomp_point_t *pp,
                                     const uint8_t scalar[32])
{
    int carry = 0;

    for (int w = 0; w < PRECOMP_NUM_WINDOWS; w++) {
        int val = (w < 32) ? (int)scalar[w] + carry : carry;
        carry = 0;

        if (val == 0) continue;

        if (val == 256) {
            /* Full overflow: 256 = 0 in this window, carry 1 to next */
            carry = 1;
            continue;
        }

        if (val > 128) {
            val = 256 - val;
            carry = 1;
            banderwagon_point_norm_t neg;
            negate_norm(&neg, &pp->windows[w][val - 1]);
            banderwagon_add_mixed(acc, acc, &neg);
        } else if (val == 128) {
            /* Exactly 128: use table[w][127] directly (no sign trick needed) */
            banderwagon_add_mixed(acc, acc, &pp->windows[w][127]);
        } else {
            banderwagon_add_mixed(acc, acc, &pp->windows[w][val - 1]);
        }
    }
}

void banderwagon_precomp_msm(banderwagon_point_t *out,
                              const banderwagon_precomp_msm_t *msm,
                              const uint8_t (*scalars)[32],
                              size_t count)
{
    ensure_init();
    *out = BANDERWAGON_IDENTITY;

    size_t n = (count < msm->count) ? count : msm->count;
    for (size_t i = 0; i < n; i++) {
        if (scalar_is_zero(scalars[i])) continue;
        banderwagon_precomp_scalar_mul(out, &msm->points[i], scalars[i]);
    }
}

/* =========================================================================
 * Identity and Equality
 * ========================================================================= */

bool banderwagon_is_identity(const banderwagon_point_t *p) {
    ensure_init();
    /* Identity in extended coords: X == 0 and Y == Z (and T == 0) */
    return fp_is_zero(&p->X) && fp_eq(&p->Y, &p->Z);
}

/**
 * Banderwagon equality: identifies (x,y) with (-x,-y).
 * In projective coords: P1 == P2  iff  X1*Y2 == X2*Y1.
 */
bool banderwagon_eq(const banderwagon_point_t *p,
                    const banderwagon_point_t *q)
{
    fp_t lhs, rhs;
    fp_mul(&lhs, &p->X, &q->Y);
    fp_mul(&rhs, &q->X, &p->Y);
    return fp_eq(&lhs, &rhs);
}

/* =========================================================================
 * On-Curve Check
 * ========================================================================= */

/**
 * Verify that point lies on ax^2 + y^2 = 1 + dx^2y^2.
 * In projective: a*X^2*Z^2 + Y^2*Z^2 = Z^4 + d*X^2*Y^2
 */
bool banderwagon_is_on_curve(const banderwagon_point_t *p) {
    ensure_init();

    fp_t X2, Y2, Z2, Z4;
    fp_sqr(&X2, &p->X);
    fp_sqr(&Y2, &p->Y);
    fp_sqr(&Z2, &p->Z);
    fp_sqr(&Z4, &Z2);

    /* LHS = a*X^2*Z^2 + Y^2*Z^2 */
    fp_t lhs, aX2Z2, Y2Z2;
    fp_mul(&aX2Z2, &X2, &Z2);
    fp_mul(&aX2Z2, &aX2Z2, &BANDERSNATCH_A);
    fp_mul(&Y2Z2, &Y2, &Z2);
    fp_add(&lhs, &aX2Z2, &Y2Z2);

    /* RHS = Z^4 + d*X^2*Y^2 */
    fp_t rhs, dX2Y2;
    fp_mul(&dX2Y2, &X2, &Y2);
    fp_mul(&dX2Y2, &dX2Y2, &BANDERSNATCH_D);
    fp_add(&rhs, &Z4, &dX2Y2);

    return fp_eq(&lhs, &rhs);
}

/* =========================================================================
 * Serialization
 * ========================================================================= */

/**
 * Serialize: normalize to affine, if Y is not lex largest negate X
 * (choosing the equivalent point in Banderwagon), output X big-endian.
 */
void banderwagon_serialize(uint8_t out[32],
                           const banderwagon_point_t *p)
{
    ensure_init();

    /* Normalize to affine: x = X/Z, y = Y/Z */
    fp_t z_inv, x, y;
    fp_inv(&z_inv, &p->Z);
    fp_mul(&x, &p->X, &z_inv);
    fp_mul(&y, &p->Y, &z_inv);

    /* If y is not lex largest, negate x (Banderwagon: P ~ -P) */
    if (!fp_is_lex_largest(&y))
        fp_neg(&x, &x);

    fp_to_bytes_be(out, &x);
}

/**
 * Deserialize: read X, recover Y from curve equation, choose lex largest Y.
 * Subgroup check: verify (1 - a*x^2) is a non-zero QR.
 */
bool banderwagon_deserialize(banderwagon_point_t *out,
                             const uint8_t bytes[32])
{
    ensure_init();

    fp_t x;
    fp_from_bytes_be(&x, bytes);

    /* Compute x^2 */
    fp_t x2;
    fp_sqr(&x2, &x);

    /* numerator = 1 - a*x^2 */
    fp_t ax2, num;
    fp_mul(&ax2, &x2, &BANDERSNATCH_A);
    fp_sub(&num, &FP_ONE, &ax2);

    /* denominator = 1 - d*x^2 */
    fp_t dx2, den;
    fp_mul(&dx2, &x2, &BANDERSNATCH_D);
    fp_sub(&den, &FP_ONE, &dx2);

    /* Check denominator is non-zero */
    if (fp_is_zero(&den))
        return false;

    /* y^2 = num / den */
    fp_t den_inv, y2;
    fp_inv(&den_inv, &den);
    fp_mul(&y2, &num, &den_inv);

    /* Subgroup check: num = (1 - a*x^2) must be a non-zero QR.
     * Note: sqrt(y^2) = sqrt(num/den) succeeds when both num and den
     * are QNR (ratio is QR), but that gives a point outside the subgroup.
     * So we explicitly check num is a QR. */
    if (fp_is_zero(&num))
        return false;
    fp_t num_sqrt;
    if (!fp_sqrt(&num_sqrt, &num))
        return false;  /* num is QNR — point not in subgroup */

    /* Compute sqrt(y^2) — must succeed since num is QR */
    fp_t y;
    if (!fp_sqrt(&y, &y2))
        return false;

    /* Choose lexicographically largest Y */
    if (!fp_is_lex_largest(&y))
        fp_neg(&y, &y);

    /* Build extended point */
    out->X = x;
    out->Y = y;
    out->Z = FP_ONE;
    fp_mul(&out->T, &x, &y);

    return true;
}

/* =========================================================================
 * Map to Scalar Field
 * ========================================================================= */

/**
 * Map point to scalar field element: compute X/Y in Fp,
 * serialize to LE bytes, reduce mod Fr.
 * Identity maps to 0.
 */
void banderwagon_map_to_field(uint8_t out[32],
                              const banderwagon_point_t *p)
{
    ensure_init();

    if (banderwagon_is_identity(p)) {
        memset(out, 0, 32);
        return;
    }

    /* t = X / Y  (in projective: X * inv(Y)) */
    fp_t y_inv, t;
    fp_inv(&y_inv, &p->Y);
    fp_mul(&t, &p->X, &y_inv);

    /* Serialize to LE bytes */
    uint8_t t_bytes[32];
    fp_to_bytes_le(t_bytes, &t);

    /* Reduce mod r (Bandersnatch scalar field).
     * Since p < 2^255 and r < 2^253, p/r < 4.
     * So at most 3 subtractions needed. */
    uint64_t val[4];
    memcpy(val, t_bytes, 32);

    uint64_t r[4];
    memcpy(r, FR_MODULUS, 32);

    /* Subtract r while val >= r */
    for (int iter = 0; iter < 4; iter++) {
        /* Compare val vs r (MSB first) */
        bool ge = false;
        bool determined = false;
        for (int i = 3; i >= 0; i--) {
            if (val[i] > r[i]) { ge = true; determined = true; break; }
            if (val[i] < r[i]) { ge = false; determined = true; break; }
        }
        if (!determined) ge = true;  /* equal: val >= r */

        if (!ge) break;

        /* val -= r */
        uint64_t borrow = 0;
        for (int i = 0; i < 4; i++) {
            uint64_t sub = r[i] + borrow;
            borrow = (sub < r[i]) || (val[i] < sub) ? 1 : 0;
            val[i] -= sub;
        }
    }

    memcpy(out, val, 32);
}
