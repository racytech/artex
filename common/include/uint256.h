#ifndef UINT256_INTERNAL_H
#define UINT256_INTERNAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "uint128.h"

/**
 * @brief 256-bit unsigned integer type using two 128-bit halves
 * 
 * This structure represents a 256-bit unsigned integer using two
 * native 128-bit integers for optimal performance on 64-bit systems.
 */
typedef struct {
    uint128_t low;   // Lower 128 bits
    uint128_t high;  // Upper 128 bits
} uint256_t;

// Constants
extern const uint256_t UINT256_ZERO;
extern const uint256_t UINT256_ONE;
extern const uint256_t UINT256_MAX;

// Convenience macros
#define UINT256_ZERO_INIT ((uint256_t){UINT128_ZERO, UINT128_ZERO})
#define UINT256_ONE_INIT  ((uint256_t){UINT128_ONE, UINT128_ZERO})
#define UINT256_MAX_INIT  ((uint256_t){UINT128_MAX, UINT128_MAX})

// Arithmetic operations (div/mod stay in uint256.c — complex, less frequent)
uint256_t uint256_div(const uint256_t* a, const uint256_t* b);
uint256_t uint256_mod(const uint256_t* a, const uint256_t* b);

// Inline multiplication (hot path — 257k calls in snailtracer)
static inline uint256_t uint256_mul(const uint256_t* a, const uint256_t* b) {
    uint256_t result;

    // Full 128×128→256 for low×low using 4 partial 64×64→128 products
    uint64_t al = (uint64_t)a->low, ah = (uint64_t)(a->low >> 64);
    uint64_t bl = (uint64_t)b->low, bh = (uint64_t)(b->low >> 64);

    __uint128_t p0 = (__uint128_t)al * bl;
    __uint128_t p1 = (__uint128_t)al * bh;
    __uint128_t p2 = (__uint128_t)ah * bl;
    __uint128_t p3 = (__uint128_t)ah * bh;

    // Combine: result = p3*2^128 + (p1+p2)*2^64 + p0
    __uint128_t mid = p1 + p2;
    __uint128_t carry_mid = (mid < p1) ? ((__uint128_t)1 << 64) : 0;

    result.low = p0 + (mid << 64);
    result.high = p3 + (mid >> 64) + carry_mid +
                  ((__uint128_t)(result.low < p0));

    // Cross terms: a_high*b_low + a_low*b_high (only low 128 bits matter)
    result.high += a->high * b->low + a->low * b->high;

    return result;
}

// Shift operations (stay in uint256.c — multi-branch)
uint256_t uint256_shl(const uint256_t* a, unsigned int shift);
uint256_t uint256_shr(const uint256_t* a, unsigned int shift);

// Arithmetic overflow detection
bool uint256_add_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result);
bool uint256_sub_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result);
bool uint256_mul_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result);

// Inline arithmetic (hot path)
static inline uint256_t uint256_add(const uint256_t* a, const uint256_t* b) {
    uint256_t r;
    r.low = a->low + b->low;
    r.high = a->high + b->high + (uint128_t)(r.low < a->low);
    return r;
}

static inline uint256_t uint256_sub(const uint256_t* a, const uint256_t* b) {
    uint256_t r;
    r.low = a->low - b->low;
    r.high = a->high - b->high - (uint128_t)(a->low < b->low);
    return r;
}

// Inline bitwise operations (hot path)
static inline uint256_t uint256_and(const uint256_t* a, const uint256_t* b) {
    return (uint256_t){ a->low & b->low, a->high & b->high };
}

static inline uint256_t uint256_or(const uint256_t* a, const uint256_t* b) {
    return (uint256_t){ a->low | b->low, a->high | b->high };
}

static inline uint256_t uint256_xor(const uint256_t* a, const uint256_t* b) {
    return (uint256_t){ a->low ^ b->low, a->high ^ b->high };
}

static inline uint256_t uint256_not(const uint256_t* a) {
    return (uint256_t){ ~a->low, ~a->high };
}

// Inline test functions (hot path)
static inline bool uint256_is_zero(const uint256_t* a) {
    return (a->low | a->high) == 0;
}

static inline bool uint256_is_one(const uint256_t* a) {
    return a->low == 1 && a->high == 0;
}

// Inline comparison functions (hot path)
static inline int uint256_compare(const uint256_t* a, const uint256_t* b) {
    if (a->high != b->high) return (a->high > b->high) ? 1 : -1;
    if (a->low != b->low)   return (a->low > b->low) ? 1 : -1;
    return 0;
}

static inline bool uint256_is_equal(const uint256_t* a, const uint256_t* b) {
    return a->low == b->low && a->high == b->high;
}

static inline bool uint256_is_less(const uint256_t* a, const uint256_t* b) {
    return (a->high < b->high) || (a->high == b->high && a->low < b->low);
}

static inline bool uint256_is_less_equal(const uint256_t* a, const uint256_t* b) {
    return (a->high < b->high) || (a->high == b->high && a->low <= b->low);
}

static inline bool uint256_is_greater(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low > b->low);
}

static inline bool uint256_is_greater_equal(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low >= b->low);
}

static inline bool uint256_eq(const uint256_t* a, const uint256_t* b) {
    return a->low == b->low && a->high == b->high;
}

static inline bool uint256_ne(const uint256_t* a, const uint256_t* b) {
    return a->low != b->low || a->high != b->high;
}

static inline bool uint256_lt(const uint256_t* a, const uint256_t* b) {
    return (a->high < b->high) || (a->high == b->high && a->low < b->low);
}

static inline bool uint256_le(const uint256_t* a, const uint256_t* b) {
    return (a->high < b->high) || (a->high == b->high && a->low <= b->low);
}

static inline bool uint256_gt(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low > b->low);
}

static inline bool uint256_ge(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low >= b->low);
}

// Creation and conversion functions
uint256_t uint256_from_uint64(uint64_t value);
uint256_t uint256_from_uint128(uint128_t value);
uint256_t uint256_from_hex(const char* hex_str);
uint256_t uint256_from_bytes(const uint8_t* bytes, size_t len);
uint256_t uint256_make(uint128_t high, uint128_t low);
uint64_t uint256_to_uint64(const uint256_t* a);
char* uint256_to_hex(const uint256_t* a);
void uint256_to_bytes(const uint256_t* a, uint8_t* bytes);
void uint256_to_bytes_le(const uint256_t* a, uint8_t* bytes);
uint256_t uint256_from_bytes_le(const uint8_t* bytes, size_t len);
static inline void uint256_to_words(const uint256_t* a, uint64_t words[4]) {
    words[0] = (uint64_t)a->low;
    words[1] = (uint64_t)(a->low >> 64);
    words[2] = (uint64_t)a->high;
    words[3] = (uint64_t)(a->high >> 64);
}

static inline uint256_t uint256_from_words(const uint64_t words[4]) {
    uint256_t r;
    r.low  = ((__uint128_t)words[1] << 64) | words[0];
    r.high = ((__uint128_t)words[3] << 64) | words[2];
    return r;
}

// Additional utility functions
int uint256_bit_length(const uint256_t* a);
bool uint256_get_bit(const uint256_t* a, unsigned int bit_index);
uint256_t uint256_set_bit(const uint256_t* a, unsigned int bit_index);
uint256_t uint256_clear_bit(const uint256_t* a, unsigned int bit_index);

// EVM-specific functions
uint256_t uint256_exp(const uint256_t* base, const uint256_t* exponent);
uint256_t uint256_addmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod);
uint256_t uint256_mulmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod);

// EVM signed arithmetic operations
uint256_t uint256_sdiv(const uint256_t* a, const uint256_t* b);
uint256_t uint256_smod(const uint256_t* a, const uint256_t* b);
bool uint256_slt(const uint256_t* a, const uint256_t* b);
bool uint256_sgt(const uint256_t* a, const uint256_t* b);
uint256_t uint256_sar(const uint256_t* a, unsigned int shift);
uint256_t uint256_signextend(const uint256_t* a, unsigned int byte_num);

// EVM byte operations
uint8_t uint256_byte(const uint256_t* a, unsigned int byte_index);

// Memory operations
void uint256_copy(uint256_t* dest, const uint256_t* src);
void uint256_zero(uint256_t* a);
void uint256_set(uint256_t* a, uint64_t value);

// Debug/Print functions
void uint256_print(const uint256_t* a);
void uint256_print_hex(const uint256_t* a);
void uint256_print_binary(const uint256_t* a);

#endif // UINT256_INTERNAL_H