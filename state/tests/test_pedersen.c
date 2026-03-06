#include "pedersen.h"
#include "blst_aux.h"
#include <stdio.h>
#include <string.h>

/* =========================================================================
 * Test Infrastructure
 * ========================================================================= */

static int tests_passed = 0;
static int tests_failed = 0;

#define ASSERT(cond, msg)                                              \
    do {                                                               \
        if (!(cond)) {                                                 \
            printf("  FAIL: %s (line %d)\n", msg, __LINE__);          \
            tests_failed++;                                            \
        } else {                                                       \
            tests_passed++;                                            \
        }                                                              \
    } while (0)

static void print_bytes(const char *label, const uint8_t *data, int len) {
    printf("  %s: ", label);
    for (int i = 0; i < len; i++) printf("%02x", data[i]);
    printf("\n");
}

/* =========================================================================
 * Phase 1: CRS Known Vectors
 * ========================================================================= */

static void test_crs_known_vectors(void) {
    printf("Phase 1: CRS known vectors\n");

    /* CRS point 0 */
    const uint8_t crs0_expected[32] = {
        0x01, 0x58, 0x7a, 0xd1, 0x33, 0x66, 0x75, 0xeb,
        0x91, 0x25, 0x50, 0xec, 0x2a, 0x28, 0xeb, 0x89,
        0x23, 0xb8, 0x24, 0xb4, 0x90, 0xdd, 0x2b, 0xa8,
        0x2e, 0x48, 0xf1, 0x45, 0x90, 0xa2, 0x98, 0xa0,
    };

    const banderwagon_point_t *g0 = pedersen_get_crs(0);
    uint8_t ser0[32];
    banderwagon_serialize(ser0, g0);
    ASSERT(memcmp(ser0, crs0_expected, 32) == 0,
           "CRS point 0 matches known vector");
    if (memcmp(ser0, crs0_expected, 32) != 0) {
        print_bytes("expected", crs0_expected, 32);
        print_bytes("got     ", ser0, 32);
    }

    /* CRS point 255 */
    const uint8_t crs255_expected[32] = {
        0x3d, 0xe2, 0xbe, 0x34, 0x6b, 0x53, 0x93, 0x95,
        0xb0, 0xc0, 0xde, 0x56, 0xa5, 0xcc, 0xca, 0x54,
        0xa3, 0x17, 0xf1, 0xb5, 0xc8, 0x01, 0x07, 0xb0,
        0x80, 0x2a, 0xf9, 0xa6, 0x22, 0x76, 0xa4, 0xd8,
    };

    const banderwagon_point_t *g255 = pedersen_get_crs(255);
    uint8_t ser255[32];
    banderwagon_serialize(ser255, g255);
    ASSERT(memcmp(ser255, crs255_expected, 32) == 0,
           "CRS point 255 matches known vector");
    if (memcmp(ser255, crs255_expected, 32) != 0) {
        print_bytes("expected", crs255_expected, 32);
        print_bytes("got     ", ser255, 32);
    }

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 2: CRS Integrity (SHA256 of all serialized points)
 * ========================================================================= */

static void test_crs_integrity(void) {
    printf("Phase 2: CRS integrity hash\n");

    /* Serialize all 256 CRS points and compute SHA256 */
    uint8_t all_serialized[256 * 32];
    for (int i = 0; i < PEDERSEN_WIDTH; i++) {
        const banderwagon_point_t *gi = pedersen_get_crs(i);
        banderwagon_serialize(&all_serialized[i * 32], gi);
    }

    uint8_t hash[32];
    blst_sha256(hash, all_serialized, sizeof(all_serialized));

    const uint8_t expected_hash[32] = {
        0x1f, 0xca, 0xea, 0x10, 0xbf, 0x24, 0xf7, 0x50,
        0x20, 0x0e, 0x06, 0xfa, 0x47, 0x3c, 0x76, 0xff,
        0x04, 0x68, 0x00, 0x72, 0x91, 0xfa, 0x54, 0x8e,
        0x2d, 0x99, 0xf0, 0x9b, 0xa9, 0x25, 0x6f, 0xdb,
    };

    ASSERT(memcmp(hash, expected_hash, 32) == 0,
           "SHA256 of all 256 CRS points matches known hash");
    if (memcmp(hash, expected_hash, 32) != 0) {
        print_bytes("expected", expected_hash, 32);
        print_bytes("got     ", hash, 32);
    }

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 3: All CRS Points On Curve
 * ========================================================================= */

static void test_crs_on_curve(void) {
    printf("Phase 3: All CRS points on curve\n");

    bool all_on_curve = true;
    for (int i = 0; i < PEDERSEN_WIDTH; i++) {
        const banderwagon_point_t *gi = pedersen_get_crs(i);
        if (!banderwagon_is_on_curve(gi)) {
            printf("  CRS point %d is NOT on curve\n", i);
            all_on_curve = false;
        }
    }
    ASSERT(all_on_curve, "all 256 CRS points are on curve");

    /* No CRS point should be identity */
    bool none_identity = true;
    for (int i = 0; i < PEDERSEN_WIDTH; i++) {
        if (banderwagon_is_identity(pedersen_get_crs(i))) {
            printf("  CRS point %d is identity\n", i);
            none_identity = false;
        }
    }
    ASSERT(none_identity, "no CRS point is identity");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 4: Commit Zero Vector
 * ========================================================================= */

static void test_commit_zero(void) {
    printf("Phase 4: Commit zero vector\n");

    uint8_t scalars[4][32];
    memset(scalars, 0, sizeof(scalars));

    banderwagon_point_t result;
    pedersen_commit(&result, scalars, 4);
    ASSERT(banderwagon_is_identity(&result),
           "commit([0,0,0,0]) == Identity");

    /* Full width zero */
    uint8_t all_zero[256][32];
    memset(all_zero, 0, sizeof(all_zero));
    pedersen_commit(&result, all_zero, 256);
    ASSERT(banderwagon_is_identity(&result),
           "commit(256 zeros) == Identity");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 5: Commit Single Value
 * ========================================================================= */

static void test_commit_single(void) {
    printf("Phase 5: Commit single value\n");

    /* commit with only scalars[0] = 1, rest zero, should equal G_0 */
    uint8_t scalars[PEDERSEN_WIDTH][32];
    memset(scalars, 0, sizeof(scalars));
    scalars[0][0] = 1;  /* scalar = 1 (LE) */

    banderwagon_point_t result;
    pedersen_commit(&result, scalars, PEDERSEN_WIDTH);
    ASSERT(banderwagon_eq(&result, pedersen_get_crs(0)),
           "commit([1,0,...]) == G_0");

    /* commit with only scalars[5] = 3 should equal 3 * G_5 */
    memset(scalars, 0, sizeof(scalars));
    scalars[5][0] = 3;

    pedersen_commit(&result, scalars, PEDERSEN_WIDTH);
    banderwagon_point_t expected;
    uint8_t three[32] = {3};
    banderwagon_scalar_mul(&expected, pedersen_get_crs(5), three);
    ASSERT(banderwagon_eq(&result, &expected),
           "commit([0,0,0,0,0,3,0,...]) == 3*G_5");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 6: Commit Linearity
 * ========================================================================= */

static void test_commit_linearity(void) {
    printf("Phase 6: Commit linearity\n");

    /* commit(a) + commit(b) == commit(a + b) */
    uint8_t a[4][32], b[4][32], ab[4][32];
    memset(a, 0, sizeof(a));
    memset(b, 0, sizeof(b));
    memset(ab, 0, sizeof(ab));

    a[0][0] = 2;  a[1][0] = 5;
    b[0][0] = 3;  b[2][0] = 7;
    ab[0][0] = 5; ab[1][0] = 5; ab[2][0] = 7;

    banderwagon_point_t ca, cb, cab, sum;
    pedersen_commit(&ca, a, 4);
    pedersen_commit(&cb, b, 4);
    pedersen_commit(&cab, ab, 4);

    banderwagon_add(&sum, &ca, &cb);
    ASSERT(banderwagon_eq(&sum, &cab),
           "commit(a) + commit(b) == commit(a + b)");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 7: Update Correctness
 * ========================================================================= */

static void test_update(void) {
    printf("Phase 7: Update correctness\n");

    /* Initial commitment with some values */
    uint8_t scalars[PEDERSEN_WIDTH][32];
    memset(scalars, 0, sizeof(scalars));
    scalars[0][0] = 10;
    scalars[1][0] = 20;
    scalars[2][0] = 30;

    banderwagon_point_t initial;
    pedersen_commit(&initial, scalars, PEDERSEN_WIDTH);

    /* Change scalars[1] from 20 to 50 */
    uint8_t new_scalars[PEDERSEN_WIDTH][32];
    memcpy(new_scalars, scalars, sizeof(scalars));
    new_scalars[1][0] = 50;

    /* Full recompute */
    banderwagon_point_t recomputed;
    pedersen_commit(&recomputed, new_scalars, PEDERSEN_WIDTH);

    /* Incremental update: delta = 50 - 20 = 30 */
    uint8_t delta[32] = {0};
    delta[0] = 30;
    banderwagon_point_t updated;
    pedersen_update(&updated, &initial, 1, delta);

    ASSERT(banderwagon_eq(&updated, &recomputed),
           "update matches full recompute");

    /* Zero delta should be a no-op */
    uint8_t zero_delta[32] = {0};
    banderwagon_point_t same;
    pedersen_update(&same, &initial, 0, zero_delta);
    ASSERT(banderwagon_eq(&same, &initial),
           "update with zero delta is no-op");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 8: Update with scalar_diff
 * ========================================================================= */

static void test_update_with_diff(void) {
    printf("Phase 8: Update with scalar_diff\n");

    /* Initial commitment */
    uint8_t scalars[PEDERSEN_WIDTH][32];
    memset(scalars, 0, sizeof(scalars));
    scalars[3][0] = 100;

    banderwagon_point_t initial;
    pedersen_commit(&initial, scalars, PEDERSEN_WIDTH);

    /* New value for slot 3 */
    uint8_t old_val[32] = {0};
    old_val[0] = 100;
    uint8_t new_val[32] = {0};
    new_val[0] = 42;

    /* Compute delta via scalar_diff */
    uint8_t delta[32];
    pedersen_scalar_diff(delta, new_val, old_val);

    /* Apply update */
    banderwagon_point_t updated;
    pedersen_update(&updated, &initial, 3, delta);

    /* Full recompute with new value */
    uint8_t new_scalars[PEDERSEN_WIDTH][32];
    memcpy(new_scalars, scalars, sizeof(scalars));
    memcpy(new_scalars[3], new_val, 32);

    banderwagon_point_t recomputed;
    pedersen_commit(&recomputed, new_scalars, PEDERSEN_WIDTH);

    ASSERT(banderwagon_eq(&updated, &recomputed),
           "update via scalar_diff matches recompute");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 9: scalar_diff edge cases
 * ========================================================================= */

static void test_scalar_diff(void) {
    printf("Phase 9: scalar_diff edge cases\n");

    /* a - a = 0 */
    uint8_t val[32] = {0};
    val[0] = 42; val[1] = 0x13;
    uint8_t delta[32];
    pedersen_scalar_diff(delta, val, val);

    uint8_t zero[32] = {0};
    ASSERT(memcmp(delta, zero, 32) == 0, "scalar_diff(a, a) == 0");

    /* a - 0 = a */
    pedersen_scalar_diff(delta, val, zero);
    ASSERT(memcmp(delta, val, 32) == 0, "scalar_diff(a, 0) == a");

    /* 0 - a should wrap around mod r */
    pedersen_scalar_diff(delta, zero, val);

    /* Verify: delta + val == 0 mod r (i.e., delta == r - val) */
    uint64_t d[4], v[4], r[4];
    memcpy(d, delta, 32);
    memcpy(v, val, 32);
    memcpy(r, FR_MODULUS, 32);

    /* d + v should equal r */
    uint64_t sum[4];
    uint64_t carry = 0;
    for (int i = 0; i < 4; i++) {
        sum[i] = d[i] + v[i] + carry;
        carry = (sum[i] < d[i]) || (carry && sum[i] == d[i]) ? 1 : 0;
    }
    ASSERT(sum[0] == r[0] && sum[1] == r[1] &&
           sum[2] == r[2] && sum[3] == r[3],
           "scalar_diff(0, a) + a == r (mod wrap)");

    printf("  OK\n\n");
}

/* =========================================================================
 * Phase 10: Multiple Updates
 * ========================================================================= */

static void test_multiple_updates(void) {
    printf("Phase 10: Multiple updates\n");

    /* Start from identity (all zeros) */
    banderwagon_point_t commitment = *pedersen_get_crs(0);
    /* Actually start from a known commitment */
    uint8_t scalars[PEDERSEN_WIDTH][32];
    memset(scalars, 0, sizeof(scalars));
    scalars[0][0] = 1;
    scalars[10][0] = 5;
    scalars[100][0] = 9;
    scalars[200][0] = 13;

    pedersen_commit(&commitment, scalars, PEDERSEN_WIDTH);

    /* Apply 4 updates */
    uint8_t delta1[32] = {0}; delta1[0] = 2;   /* slot 0: 1 → 3 */
    uint8_t delta2[32] = {0}; delta2[0] = 10;  /* slot 10: 5 → 15 */
    uint8_t delta3[32] = {0}; delta3[0] = 1;   /* slot 100: 9 → 10 */
    uint8_t delta4[32] = {0}; delta4[0] = 7;   /* slot 200: 13 → 20 */

    pedersen_update(&commitment, &commitment, 0, delta1);
    pedersen_update(&commitment, &commitment, 10, delta2);
    pedersen_update(&commitment, &commitment, 100, delta3);
    pedersen_update(&commitment, &commitment, 200, delta4);

    /* Full recompute with final values */
    uint8_t final[PEDERSEN_WIDTH][32];
    memset(final, 0, sizeof(final));
    final[0][0] = 3;
    final[10][0] = 15;
    final[100][0] = 10;
    final[200][0] = 20;

    banderwagon_point_t recomputed;
    pedersen_commit(&recomputed, final, PEDERSEN_WIDTH);

    ASSERT(banderwagon_eq(&commitment, &recomputed),
           "4 sequential updates match full recompute");

    printf("  OK\n\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Pedersen Commitment Tests ===\n\n");

    pedersen_init();

    test_crs_known_vectors();
    test_crs_integrity();
    test_crs_on_curve();
    test_commit_zero();
    test_commit_single();
    test_commit_linearity();
    test_update();
    test_update_with_diff();
    test_scalar_diff();
    test_multiple_updates();

    printf("=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
