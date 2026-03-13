#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "uint256.h"

static int tests_passed = 0;
static int tests_failed = 0;

static void check_div(const char *label,
                       const uint256_t *a, const uint256_t *b,
                       const uint256_t *expected_q) {
    uint256_t q = uint256_div(a, b);
    if (uint256_is_equal(&q, expected_q)) {
        tests_passed++;
    } else {
        tests_failed++;
        printf("FAIL div %s: ", label);
        printf("a="); uint256_print(a);
        printf(" b="); uint256_print(b);
        printf(" expected="); uint256_print(expected_q);
        printf(" got="); uint256_print(&q);
        printf("\n");
    }
}

static void check_mod(const char *label,
                       const uint256_t *a, const uint256_t *b,
                       const uint256_t *expected_r) {
    uint256_t r = uint256_mod(a, b);
    if (uint256_is_equal(&r, expected_r)) {
        tests_passed++;
    } else {
        tests_failed++;
        printf("FAIL mod %s: ", label);
        printf("a="); uint256_print(a);
        printf(" b="); uint256_print(b);
        printf(" expected="); uint256_print(expected_r);
        printf(" got="); uint256_print(&r);
        printf("\n");
    }
}

/* Verify q*b + r == a */
static void check_divmod_identity(const char *label,
                                   const uint256_t *a, const uint256_t *b) {
    uint256_t q = uint256_div(a, b);
    uint256_t r = uint256_mod(a, b);
    uint256_t product = uint256_mul(&q, b);
    uint256_t reconstructed = uint256_add(&product, &r);
    if (uint256_is_equal(&reconstructed, a) && uint256_is_less(&r, b)) {
        tests_passed++;
    } else {
        tests_failed++;
        printf("FAIL identity %s: ", label);
        printf("a="); uint256_print(a);
        printf(" b="); uint256_print(b);
        printf(" q="); uint256_print(&q);
        printf(" r="); uint256_print(&r);
        printf(" q*b+r="); uint256_print(&reconstructed);
        printf("\n");
    }
}

int main(void) {
    printf("=== uint256 div/mod tests ===\n");

    /* --- Basic small cases --- */
    {
        uint256_t a = uint256_from_uint64(100);
        uint256_t b = uint256_from_uint64(7);
        uint256_t eq = uint256_from_uint64(14);
        uint256_t er = uint256_from_uint64(2);
        check_div("100/7", &a, &b, &eq);
        check_mod("100%7", &a, &b, &er);
        check_divmod_identity("100,7", &a, &b);
    }

    /* Division by 1 */
    {
        uint256_t a = uint256_from_uint64(999999999);
        check_div("x/1", &a, &UINT256_ONE, &a);
        check_mod("x%1", &a, &UINT256_ONE, &UINT256_ZERO);
    }

    /* Division of 0 */
    {
        uint256_t b = uint256_from_uint64(42);
        check_div("0/x", &UINT256_ZERO, &b, &UINT256_ZERO);
        check_mod("0%x", &UINT256_ZERO, &b, &UINT256_ZERO);
    }

    /* a < b */
    {
        uint256_t a = uint256_from_uint64(5);
        uint256_t b = uint256_from_uint64(10);
        check_div("5/10", &a, &b, &UINT256_ZERO);
        check_mod("5%10", &a, &b, &a);
    }

    /* a == b */
    {
        uint256_t a = uint256_from_uint64(12345);
        check_div("x/x", &a, &a, &UINT256_ONE);
        check_mod("x%x", &a, &a, &UINT256_ZERO);
    }

    /* 64-bit divisor, large dividend */
    {
        /* (2^128 + 1) / 3 */
        uint256_t a = uint256_make((uint128_t)1, (uint128_t)1);
        uint256_t b = uint256_from_uint64(3);
        check_divmod_identity("2^128+1,3", &a, &b);
    }

    /* 128-bit divisor */
    {
        uint256_t a = uint256_from_hex("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^128-1 */
        uint256_t b = uint256_from_hex("0xFFFFFFFFFFFFFFFF"); /* 2^64-1 */
        uint256_t eq = uint256_from_hex("0x10000000000000001"); /* (2^128-1)/(2^64-1) = 2^64+1 */
        check_div("(2^128-1)/(2^64-1)", &a, &b, &eq);
        check_divmod_identity("2^128-1,2^64-1", &a, &b);
    }

    /* 256-bit / 128-bit */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_from_hex("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"); /* 2^128-1 */
        check_divmod_identity("MAX,2^128-1", &a, &b);
    }

    /* 256-bit / 256-bit (close values) */
    {
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_sub(&UINT256_MAX, &UINT256_ONE); /* MAX - 1 */
        uint256_t eq = UINT256_ONE;
        check_div("MAX/(MAX-1)", &a, &b, &eq);
        check_divmod_identity("MAX,MAX-1", &a, &b);
    }

    /* Power of 2 divisors */
    {
        uint256_t a = uint256_from_hex("0xABCDEF0123456789ABCDEF0123456789");
        uint256_t b = uint256_from_uint64(256); /* 2^8 */
        uint256_t eq = uint256_shr(&a, 8);
        check_div("x/256", &a, &b, &eq);
        check_divmod_identity("x,256", &a, &b);
    }

    /* Large both operands — EVM-style gas calculations */
    {
        uint256_t a = uint256_from_hex("0x10000000000000000000000000000000000000000000000000000000000000000");
        /* This is 2^256 which overflows to 0. Use MAX instead */
        uint256_t a2 = UINT256_MAX;
        uint256_t b = uint256_from_hex("0xDE0B6B3A7640000"); /* 10^18 (1 ETH in wei) */
        check_divmod_identity("MAX,10^18", &a2, &b);
    }

    /* Two 128-bit+ operands */
    {
        uint256_t a = uint256_make((uint128_t)0x123456789ABCDEF0ULL,
                                    (uint128_t)0xFEDCBA9876543210ULL);
        uint256_t b = uint256_make((uint128_t)0x1ULL,
                                    (uint128_t)0xFFFFFFFFFFFFFFFFULL);
        check_divmod_identity("big/big", &a, &b);
    }

    /* EVM SDIV edge case values (unsigned part) */
    {
        /* -1 in two's complement = MAX */
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_from_uint64(2);
        uint256_t eq = uint256_shr(&UINT256_MAX, 1); /* MAX/2 */
        check_div("MAX/2", &a, &b, &eq);
        check_divmod_identity("MAX,2", &a, &b);
    }

    /* Verify sdiv still works */
    {
        /* -10 / 3 should give -3 (truncates toward zero) */
        uint256_t neg10 = uint256_sub(&UINT256_ZERO, &(uint256_t){.low=10,.high=0});
        uint256_t three = uint256_from_uint64(3);
        uint256_t result = uint256_sdiv(&neg10, &three);
        uint256_t neg3 = uint256_sub(&UINT256_ZERO, &(uint256_t){.low=3,.high=0});
        if (uint256_is_equal(&result, &neg3)) {
            tests_passed++;
        } else {
            tests_failed++;
            printf("FAIL sdiv -10/3: expected "); uint256_print(&neg3);
            printf(" got "); uint256_print(&result); printf("\n");
        }
    }

    /* Verify smod still works */
    {
        /* -10 % 3 should give -1 */
        uint256_t neg10 = uint256_sub(&UINT256_ZERO, &(uint256_t){.low=10,.high=0});
        uint256_t three = uint256_from_uint64(3);
        uint256_t result = uint256_smod(&neg10, &three);
        uint256_t neg1 = UINT256_MAX; /* -1 in two's complement */
        if (uint256_is_equal(&result, &neg1)) {
            tests_passed++;
        } else {
            tests_failed++;
            printf("FAIL smod -10%%3: expected "); uint256_print(&neg1);
            printf(" got "); uint256_print(&result); printf("\n");
        }
    }

    /* Stress: many random-ish divisions */
    for (uint64_t i = 1; i < 1000; i++) {
        uint256_t a = uint256_from_uint64(i * 1000000007ULL);
        uint256_t b = uint256_from_uint64(i);
        check_divmod_identity("stress64", &a, &b);
    }

    /* Stress: 128-bit range */
    for (uint64_t i = 1; i < 100; i++) {
        uint256_t a = uint256_make(0, ((uint128_t)i << 64) | (i * 0xDEADBEEFULL));
        uint256_t b = uint256_from_uint64(i * 997);
        check_divmod_identity("stress128", &a, &b);
    }

    /* Stress: full 256-bit range */
    for (uint64_t i = 1; i < 100; i++) {
        uint256_t a = uint256_make((uint128_t)i * 0x123456789ULL,
                                    ((uint128_t)i << 64) | 0xABCDEF01ULL);
        uint256_t b = uint256_make(0, (uint128_t)i * 0x987654321ULL);
        if (uint256_is_zero(&b)) continue;
        check_divmod_identity("stress256", &a, &b);
    }

    /* Stress: large divisor (both high words set) */
    for (uint64_t i = 1; i < 50; i++) {
        uint256_t a = UINT256_MAX;
        uint256_t b = uint256_make((uint128_t)i << 60,
                                    (uint128_t)i * 0xFFFFFFFFULL);
        check_divmod_identity("stress_large_div", &a, &b);
    }

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
