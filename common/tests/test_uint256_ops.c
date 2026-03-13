/*
 * Comprehensive uint256 arithmetic tests.
 * Covers: mul, div, mod, mulmod, addmod, exp, sdiv, smod,
 *         to_words/from_words roundtrip, and stress tests that
 *         exercise the Knuth D carry overflow path.
 */
#include <stdio.h>
#include <string.h>
#include "uint256.h"

static int passed = 0, failed = 0;

#define CHECK(label, got_ptr, exp_ptr) do { \
    if (uint256_is_equal((got_ptr), (exp_ptr))) { \
        passed++; \
    } else { \
        failed++; \
        printf("FAIL %s:\n  expected=", (label)); uint256_print((exp_ptr)); \
        printf("\n  got=     "); uint256_print((got_ptr)); printf("\n"); \
    } \
} while(0)

#define CHECK_U64(label, got, exp) do { \
    if ((got) == (exp)) { passed++; } \
    else { failed++; printf("FAIL %s: expected %lu got %lu\n", (label), (unsigned long)(exp), (unsigned long)(got)); } \
} while(0)

#define CHECK_BOOL(label, got, exp) do { \
    if ((got) == (exp)) { passed++; } \
    else { failed++; printf("FAIL %s: expected %d got %d\n", (label), (exp), (got)); } \
} while(0)

static uint256_t H(const char *s) { return uint256_from_hex(s); }
static uint256_t U(uint64_t v) { return uint256_from_uint64(v); }

/* ================================================================
 * MUL
 * ================================================================ */
static void test_mul(void) {
    /* small */
    { uint256_t a=U(3), b=U(7), e=U(21), r=uint256_mul(&a,&b); CHECK("mul 3*7",&r,&e); }
    /* cross-term: 2^128 * 5 */
    {
        uint256_t a = {.high=1, .low=0};
        uint256_t b = U(5);
        uint256_t e = {.high=5, .low=0};
        uint256_t r = uint256_mul(&a,&b);
        CHECK("mul 2^128*5", &r, &e);
    }
    /* (2^128-1)^2 */
    {
        uint256_t a = {.high=0, .low=UINT128_MAX};
        uint256_t e = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE00000000000000000000000000000001");
        uint256_t r = uint256_mul(&a,&a);
        CHECK("mul (2^128-1)^2", &r, &e);
    }
    /* MAX * MAX = 1 (mod 2^256) */
    {
        uint256_t r = uint256_mul(&UINT256_MAX, &UINT256_MAX);
        CHECK("mul MAX*MAX", &r, &UINT256_ONE);
    }
    /* 0 * anything = 0 */
    {
        uint256_t r = uint256_mul(&UINT256_ZERO, &UINT256_MAX);
        CHECK("mul 0*MAX", &r, &UINT256_ZERO);
    }
    /* 1 * x = x */
    {
        uint256_t x = H("DEADBEEFCAFEBABE1234567890ABCDEF");
        uint256_t r = uint256_mul(&UINT256_ONE, &x);
        CHECK("mul 1*x", &r, &x);
    }
}

/* ================================================================
 * DIV / MOD — tests all 3 Knuth D fast paths
 * ================================================================ */
static void check_divmod(const char *label, const uint256_t *a, const uint256_t *b) {
    uint256_t q = uint256_div(a, b);
    uint256_t r = uint256_mod(a, b);
    uint256_t prod = uint256_mul(&q, b);
    uint256_t recon = uint256_add(&prod, &r);
    /* q*b + r == a */
    if (uint256_is_equal(&recon, a) && uint256_is_less(&r, b)) {
        passed++;
    } else {
        failed++;
        printf("FAIL divmod %s: q*b+r!=a or r>=b\n", label);
        printf("  a="); uint256_print(a);
        printf("\n  b="); uint256_print(b);
        printf("\n  q="); uint256_print(&q);
        printf("\n  r="); uint256_print(&r);
        printf("\n  q*b+r="); uint256_print(&recon);
        printf("\n");
    }
}

static void test_div_mod(void) {
    /* Path 1: both fit in 128 bits */
    { uint256_t a=U(100), b=U(7); check_divmod("100/7", &a, &b); }
    { uint256_t a=U(1), b=U(1); check_divmod("1/1", &a, &b); }

    /* Path 2: 64-bit divisor, large dividend */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = U(3);
        check_divmod("MAX/3", &a, &b);
    }
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = U(0xFFFFFFFFFFFFFFFF);
        check_divmod("MAX/(2^64-1)", &a, &b);
    }

    /* Path 3: general Knuth D — 2-limb divisor */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^128-1 */
        check_divmod("MAX/(2^128-1)", &a, &b);
    }

    /* 3-limb divisor */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^192-1 */
        check_divmod("MAX/(2^192-1)", &a, &b);
    }

    /* 4-limb divisor */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        check_divmod("MAX/(MAX-1)", &a, &b);
    }

    /* Division by 0 = 0 */
    {
        uint256_t q = uint256_div(&UINT256_MAX, &UINT256_ZERO);
        CHECK("div MAX/0", &q, &UINT256_ZERO);
    }

    /* a < b → q=0, r=a */
    {
        uint256_t a = U(5), b = U(10);
        uint256_t q = uint256_div(&a, &b);
        uint256_t r = uint256_mod(&a, &b);
        CHECK("div 5/10", &q, &UINT256_ZERO);
        CHECK("mod 5%10", &r, &a);
    }

    /* Stress: 64-bit range */
    for (uint64_t i = 1; i < 500; i++) {
        uint256_t a = U(i * 1000000007ULL);
        uint256_t b = U(i);
        check_divmod("stress64", &a, &b);
    }

    /* Stress: 128-bit range */
    for (uint64_t i = 1; i < 100; i++) {
        uint256_t a = uint256_make(0, ((uint128_t)i << 64) | (i * 0xDEADBEEFULL));
        uint256_t b = U(i * 997);
        check_divmod("stress128", &a, &b);
    }

    /* Stress: 256-bit range */
    for (uint64_t i = 1; i < 100; i++) {
        uint256_t a = uint256_make((uint128_t)i * 0x123456789ULL,
                                    ((uint128_t)i << 64) | 0xABCDEF01ULL);
        uint256_t b = uint256_make(0, (uint128_t)i * 0x987654321ULL);
        if (uint256_is_zero(&b)) continue;
        check_divmod("stress256", &a, &b);
    }

    /* Stress: divisor with limbs near MAX (triggers carry overflow path) */
    for (uint64_t i = 1; i <= 50; i++) {
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_make(
            ((uint128_t)0xFFFFFFFFFFFFFFFF << 64) | (0xFFFFFFFFFFFFFFFF - i),
            ((uint128_t)0xFFFFFFFFFFFFFFFF << 64) | 0xFFFFFFFFFFFFFFFF);
        check_divmod("stress_near_max_div", &a, &b);
    }
}

/* ================================================================
 * MULMOD — 512-bit intermediate path
 * ================================================================ */
static void test_mulmod(void) {
    /* mod = 0 → 0 */
    {
        uint256_t r = uint256_mulmod(&UINT256_MAX, &UINT256_MAX, &UINT256_ZERO);
        CHECK("mulmod mod=0", &r, &UINT256_ZERO);
    }
    /* mod = 1 → 0 */
    {
        uint256_t r = uint256_mulmod(&UINT256_MAX, &UINT256_MAX, &UINT256_ONE);
        CHECK("mulmod mod=1", &r, &UINT256_ZERO);
    }
    /* MAX*MAX % (MAX-1) = 1 */
    {
        uint256_t m = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t e = UINT256_ONE;
        uint256_t r = uint256_mulmod(&UINT256_MAX, &UINT256_MAX, &m);
        CHECK("mulmod MAX*MAX%(MAX-1)", &r, &e);
    }
    /* (MAX-1)*(MAX-2) % (MAX-3) = 2 */
    {
        uint256_t a = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t b = uint256_sub(&UINT256_MAX, &(uint256_t){.low=2,.high=0});
        uint256_t m = uint256_sub(&UINT256_MAX, &(uint256_t){.low=3,.high=0});
        uint256_t e = U(2);
        uint256_t r = uint256_mulmod(&a, &b, &m);
        CHECK("mulmod (MAX-1)*(MAX-2)%(MAX-3)", &r, &e);
    }
    /* secp256k1 modulus (n=4, s=0, near-MAX limbs — carry overflow stress) */
    {
        uint256_t a = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000000000000000000000000001");
        uint256_t b = H("FFFFFFFFFFFFFFFF0000000000000001FFFFFFFFFFFFFFFF0000000000000001");
        uint256_t m = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F");
        uint256_t e = H("FFFFFFFEFFFFFC2F00000001000003D20000000000000000000007A1000E8CD1");
        uint256_t r = uint256_mulmod(&a, &b, &m);
        CHECK("mulmod secp256k1", &r, &e);
    }
    /* 3-limb divisor (192 bits): (MAX-1)*(MAX-2) % (2^192-1) */
    {
        uint256_t a = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t b = uint256_sub(&UINT256_MAX, &(uint256_t){.low=2,.high=0});
        uint256_t m = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^192-1 = 48 F's */
        uint256_t e = H("FFFFFFFFFFFFFFFB0000000000000006");
        uint256_t r = uint256_mulmod(&a, &b, &m);
        CHECK("mulmod 3-limb div", &r, &e);
    }
    /* 2-limb divisor (128 bits) */
    {
        uint256_t a = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t b = uint256_sub(&UINT256_MAX, &(uint256_t){.low=2,.high=0});
        uint256_t m = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^128-1 */
        uint256_t e = U(2);
        uint256_t r = uint256_mulmod(&a, &b, &m);
        CHECK("mulmod 2-limb div", &r, &e);
    }
    /* 1-limb divisor */
    {
        uint256_t a = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t b = uint256_sub(&UINT256_MAX, &(uint256_t){.low=2,.high=0});
        uint256_t m = U(7);
        uint256_t e = UINT256_ZERO;
        uint256_t r = uint256_mulmod(&a, &b, &m);
        CHECK("mulmod 1-limb div", &r, &e);
    }
    /* Stress: MAX*MAX % various secp256k1-like moduli */
    {
        uint256_t e = H("000000000000000000000000000000000000000000000001000007A0000E8900");
        uint256_t m = H("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F");
        uint256_t r = uint256_mulmod(&UINT256_MAX, &UINT256_MAX, &m);
        CHECK("mulmod MAX^2%secp", &r, &e);
    }
}

/* ================================================================
 * ADDMOD
 * ================================================================ */
static void test_addmod(void) {
    /* simple */
    {
        uint256_t a=U(7), b=U(8), m=U(5), e=U(0);
        uint256_t r = uint256_addmod(&a, &b, &m);
        CHECK("addmod 7+8%5", &r, &e);
    }
    /* overflow: MAX + MAX % MAX = 0 */
    {
        uint256_t r = uint256_addmod(&UINT256_MAX, &UINT256_MAX, &UINT256_MAX);
        CHECK("addmod MAX+MAX%MAX", &r, &UINT256_ZERO);
    }
    /* overflow: (MAX-1) + (MAX-1) % MAX = MAX-2 */
    {
        uint256_t a = uint256_sub(&UINT256_MAX, &UINT256_ONE);
        uint256_t e = uint256_sub(&UINT256_MAX, &(uint256_t){.low=2,.high=0});
        uint256_t r = uint256_addmod(&a, &a, &UINT256_MAX);
        CHECK("addmod (MAX-1)*2%MAX", &r, &e);
    }
    /* MAX + 1 % MAX = 1 */
    {
        uint256_t r = uint256_addmod(&UINT256_MAX, &UINT256_ONE, &UINT256_MAX);
        CHECK("addmod MAX+1%MAX", &r, &UINT256_ONE);
    }
    /* mod = 0 → 0 */
    {
        uint256_t r = uint256_addmod(&UINT256_MAX, &UINT256_MAX, &UINT256_ZERO);
        CHECK("addmod mod=0", &r, &UINT256_ZERO);
    }
}

/* ================================================================
 * EXP
 * ================================================================ */
static void test_exp(void) {
    /* 2^255 */
    {
        uint256_t base = U(2), exp = U(255);
        uint256_t e = {.high = (uint128_t)1 << 127, .low = 0};
        uint256_t r = uint256_exp(&base, &exp);
        CHECK("exp 2^255", &r, &e);
    }
    /* 2^256 = 0 (overflow) */
    {
        uint256_t base = U(2), exp = U(256);
        uint256_t r = uint256_exp(&base, &exp);
        CHECK("exp 2^256", &r, &UINT256_ZERO);
    }
    /* 3^100 */
    {
        uint256_t base = U(3), exp = U(100);
        uint256_t e = H("5A4653CA673768565B41F775D6947D55CF3813D1");
        uint256_t r = uint256_exp(&base, &exp);
        CHECK("exp 3^100", &r, &e);
    }
    /* 7^77 */
    {
        uint256_t base = U(7), exp = U(77);
        uint256_t e = H("011F487519CDCC0C4E641A0D185EAB7C19A7C11AFDB6D1B7C28072E7");
        uint256_t r = uint256_exp(&base, &exp);
        CHECK("exp 7^77", &r, &e);
    }
    /* x^0 = 1 */
    {
        uint256_t r = uint256_exp(&UINT256_MAX, &UINT256_ZERO);
        CHECK("exp x^0", &r, &UINT256_ONE);
    }
    /* x^1 = x */
    {
        uint256_t x = H("DEADBEEF");
        uint256_t r = uint256_exp(&x, &UINT256_ONE);
        CHECK("exp x^1", &r, &x);
    }
    /* 0^x = 0 (x>0) */
    {
        uint256_t r = uint256_exp(&UINT256_ZERO, &(uint256_t){.low=5,.high=0});
        CHECK("exp 0^5", &r, &UINT256_ZERO);
    }
}

/* ================================================================
 * SDIV / SMOD
 * ================================================================ */
static void test_sdiv_smod(void) {
    uint256_t zero = UINT256_ZERO;
    /* -10 / 3 = -3 */
    {
        uint256_t ten = U(10), three = U(3);
        uint256_t neg10 = uint256_sub(&zero, &ten);
        uint256_t neg3 = uint256_sub(&zero, &three);
        uint256_t r = uint256_sdiv(&neg10, &three);
        CHECK("sdiv -10/3", &r, &neg3);
    }
    /* -10 % 3 = -1 */
    {
        uint256_t ten = U(10), three = U(3);
        uint256_t neg10 = uint256_sub(&zero, &ten);
        uint256_t neg1 = UINT256_MAX;
        uint256_t r = uint256_smod(&neg10, &three);
        CHECK("smod -10%3", &r, &neg1);
    }
    /* MIN_INT / -1 = MIN_INT (overflow case) */
    {
        uint256_t min_int = {.high = (uint128_t)1 << 127, .low = 0};
        uint256_t neg1 = UINT256_MAX;
        uint256_t r = uint256_sdiv(&min_int, &neg1);
        CHECK("sdiv MIN/-1", &r, &min_int);
    }
    /* 10 / 3 = 3 */
    {
        uint256_t ten = U(10), three = U(3), exp = U(3);
        uint256_t r = uint256_sdiv(&ten, &three);
        CHECK("sdiv 10/3", &r, &exp);
    }
    /* 10 % 3 = 1 */
    {
        uint256_t ten = U(10), three = U(3), exp = U(1);
        uint256_t r = uint256_smod(&ten, &three);
        CHECK("smod 10%3", &r, &exp);
    }
    /* -10 / -3 = 3 */
    {
        uint256_t ten = U(10), three = U(3), exp = U(3);
        uint256_t neg10 = uint256_sub(&zero, &ten);
        uint256_t neg3 = uint256_sub(&zero, &three);
        uint256_t r = uint256_sdiv(&neg10, &neg3);
        CHECK("sdiv -10/-3", &r, &exp);
    }
    /* -10 % -3 = -1 (sign follows dividend) */
    {
        uint256_t ten = U(10), three = U(3);
        uint256_t neg10 = uint256_sub(&zero, &ten);
        uint256_t neg3 = uint256_sub(&zero, &three);
        uint256_t neg1 = UINT256_MAX;
        uint256_t r = uint256_smod(&neg10, &neg3);
        CHECK("smod -10%-3", &r, &neg1);
    }
    /* sdiv/smod by 0 = 0 */
    {
        uint256_t ten = U(10);
        uint256_t r1 = uint256_sdiv(&ten, &zero);
        uint256_t r2 = uint256_smod(&ten, &zero);
        CHECK("sdiv x/0", &r1, &zero);
        CHECK("smod x%0", &r2, &zero);
    }
}

/* ================================================================
 * WORDS roundtrip
 * ================================================================ */
static void test_words(void) {
    uint256_t values[] = {
        UINT256_ZERO, UINT256_ONE, UINT256_MAX,
        U(0xDEADBEEFCAFEBABE),
        uint256_make((uint128_t)0x1234 << 64 | 0x5678, (uint128_t)0x9ABC << 64 | 0xDEF0),
    };
    for (int i = 0; i < 5; i++) {
        uint64_t w[4];
        uint256_to_words(&values[i], w);
        uint256_t back = uint256_from_words(w);
        char label[32];
        snprintf(label, sizeof(label), "words_rt_%d", i);
        CHECK(label, &back, &values[i]);
    }
    /* Verify word ordering: word[0] = lowest 64 bits */
    {
        uint256_t v = U(0x123456789ABCDEF0);
        uint64_t w[4];
        uint256_to_words(&v, w);
        CHECK_U64("words[0]", w[0], 0x123456789ABCDEF0);
        CHECK_U64("words[1]", w[1], 0);
        CHECK_U64("words[2]", w[2], 0);
        CHECK_U64("words[3]", w[3], 0);
    }
}

/* ================================================================
 * Signed comparisons
 * ================================================================ */
static void test_signed_cmp(void) {
    uint256_t neg1 = UINT256_MAX;
    uint256_t one = UINT256_ONE;
    uint256_t neg2 = uint256_sub(&UINT256_ZERO, &(uint256_t){.low=2,.high=0});

    CHECK_BOOL("slt -1 < 1", uint256_slt(&neg1, &one), true);
    CHECK_BOOL("slt 1 < -1", uint256_slt(&one, &neg1), false);
    CHECK_BOOL("sgt 1 > -1", uint256_sgt(&one, &neg1), true);
    CHECK_BOOL("slt -2 < -1", uint256_slt(&neg2, &neg1), true);
    CHECK_BOOL("sgt -1 > -2", uint256_sgt(&neg1, &neg2), true);
}

/* ================================================================
 * MAIN
 * ================================================================ */
int main(void) {
    printf("=== uint256 comprehensive ops tests ===\n\n");

    test_mul();
    test_div_mod();
    test_mulmod();
    test_addmod();
    test_exp();
    test_sdiv_smod();
    test_words();
    test_signed_cmp();

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
