#include "pedersen.h"
#include "blst_aux.h"
#include <string.h>

/* =========================================================================
 * CRS State
 * ========================================================================= */

static banderwagon_point_t CRS[PEDERSEN_WIDTH];
static banderwagon_precomp_msm_t CRS_PRECOMP;
static bool ped_initialized = false;

static const char CRS_SEED[] = "eth_verkle_oct_2021";
#define CRS_SEED_LEN 19  /* strlen("eth_verkle_oct_2021") */

static inline void ensure_init(void) {
    if (__builtin_expect(!ped_initialized, 0)) pedersen_init();
}

/* =========================================================================
 * CRS Generation
 * ========================================================================= */

/**
 * Generate 256 CRS points using try-and-increment:
 *   hash = SHA256(seed || BE64(incrementer))
 *   try banderwagon_deserialize(hash)
 *   if valid: accept as G_i, else increment and retry
 */
void pedersen_init(void) {
    if (ped_initialized) return;

    banderwagon_init();

    uint8_t input[CRS_SEED_LEN + 8];
    memcpy(input, CRS_SEED, CRS_SEED_LEN);

    uint64_t incrementer = 0;

    for (int i = 0; i < PEDERSEN_WIDTH; i++) {
        while (1) {
            /* Encode incrementer as big-endian 8 bytes */
            input[CRS_SEED_LEN + 0] = (uint8_t)(incrementer >> 56);
            input[CRS_SEED_LEN + 1] = (uint8_t)(incrementer >> 48);
            input[CRS_SEED_LEN + 2] = (uint8_t)(incrementer >> 40);
            input[CRS_SEED_LEN + 3] = (uint8_t)(incrementer >> 32);
            input[CRS_SEED_LEN + 4] = (uint8_t)(incrementer >> 24);
            input[CRS_SEED_LEN + 5] = (uint8_t)(incrementer >> 16);
            input[CRS_SEED_LEN + 6] = (uint8_t)(incrementer >> 8);
            input[CRS_SEED_LEN + 7] = (uint8_t)(incrementer);

            uint8_t hash[32];
            blst_sha256(hash, input, sizeof(input));

            incrementer++;

            if (banderwagon_deserialize(&CRS[i], hash))
                break;
        }
    }

    /* Build precomputed MSM tables for fixed CRS points */
    banderwagon_precomp_msm_init(&CRS_PRECOMP, CRS, PEDERSEN_WIDTH);

    ped_initialized = true;
}

/* =========================================================================
 * Commitment Operations
 * ========================================================================= */

const banderwagon_point_t *pedersen_get_crs(size_t index) {
    ensure_init();
    return &CRS[index];
}

void pedersen_commit(banderwagon_point_t *out,
                     const uint8_t (*scalars)[32],
                     size_t count)
{
    ensure_init();
    banderwagon_precomp_msm(out, &CRS_PRECOMP, scalars, count);
}

void pedersen_update(banderwagon_point_t *out,
                     const banderwagon_point_t *old_commitment,
                     size_t index,
                     const uint8_t delta[32])
{
    ensure_init();

    /* Check for zero delta — skip the expensive scalar mul */
    bool all_zero = true;
    for (int i = 0; i < 32; i++) {
        if (delta[i] != 0) { all_zero = false; break; }
    }
    if (all_zero) {
        *out = *old_commitment;
        return;
    }

    banderwagon_point_t tmp = BANDERWAGON_IDENTITY;
    banderwagon_precomp_scalar_mul(&tmp, &CRS_PRECOMP.points[index], delta);
    banderwagon_add(out, old_commitment, &tmp);
}

/* =========================================================================
 * Scalar Arithmetic Helpers
 * ========================================================================= */

/**
 * Compute delta = (new_val - old_val) mod r.
 * All values are 32 bytes little-endian.
 * r = FR_MODULUS (Bandersnatch scalar field order).
 */
void pedersen_scalar_diff(uint8_t delta[32],
                          const uint8_t new_val[32],
                          const uint8_t old_val[32])
{
    ensure_init();

    /* Load as uint64 LE limbs */
    uint64_t a[4], b[4];
    memcpy(a, new_val, 32);
    memcpy(b, old_val, 32);

    uint64_t r[4];
    memcpy(r, FR_MODULUS, 32);

    /* result = a - b, with borrow */
    uint64_t result[4];
    uint64_t borrow = 0;
    for (int i = 0; i < 4; i++) {
        uint64_t sub = b[i] + borrow;
        borrow = (sub < b[i]) || (a[i] < sub) ? 1 : 0;
        result[i] = a[i] - sub;
    }

    /* If borrow, add r (wrap around mod r) */
    if (borrow) {
        uint64_t carry = 0;
        for (int i = 0; i < 4; i++) {
            uint64_t sum = result[i] + r[i] + carry;
            carry = (sum < result[i]) || (carry && sum == result[i]) ? 1 : 0;
            result[i] = sum;
        }
    }

    memcpy(delta, result, 32);
}
