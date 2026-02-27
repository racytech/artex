#ifndef UINT128_H
#define UINT128_H

#include <stdint.h>
#include <stdbool.h>

/**
 * @brief Native 128-bit unsigned integer wrapper
 * 
 * This header provides a thin wrapper around the native __uint128_t type
 * for use in 256-bit arithmetic operations. Requires compiler support
 * for __uint128_t (GCC 4.6+, Clang 3.0+, Intel ICC 13.0+).
 */

// Ensure __uint128_t support
#ifndef __SIZEOF_INT128__
#error "This library requires __uint128_t support. Use GCC 4.6+, Clang 3.0+, or Intel ICC 13.0+ on a 64-bit platform."
#endif

// Use native 128-bit integer support
typedef __uint128_t uint128_t;

// Constants
#define UINT128_ZERO ((__uint128_t)0)
#define UINT128_ONE ((__uint128_t)1)
#define UINT128_MAX (~(__uint128_t)0)

// Helper macros for native __uint128_t
#define uint128_low(x) ((uint64_t)(x))
#define uint128_high(x) ((uint64_t)((x) >> 64))
#define uint128_make(high, low) (((__uint128_t)(high) << 64) | (low))

// Arithmetic operations (direct native operations)
#define uint128_add(a, b) ((a) + (b))
#define uint128_sub(a, b) ((a) - (b))
#define uint128_mul(a, b) ((a) * (b))
#define uint128_and(a, b) ((a) & (b))
#define uint128_or(a, b) ((a) | (b))
#define uint128_xor(a, b) ((a) ^ (b))
#define uint128_not(a) (~(a))
#define uint128_shl(a, shift) ((a) << (shift))
#define uint128_shr(a, shift) ((a) >> (shift))

// Comparison operations
#define uint128_eq(a, b) ((a) == (b))
#define uint128_ne(a, b) ((a) != (b))
#define uint128_lt(a, b) ((a) < (b))
#define uint128_le(a, b) ((a) <= (b))
#define uint128_gt(a, b) ((a) > (b))
#define uint128_ge(a, b) ((a) >= (b))

// Test operations
#define uint128_is_zero(a) ((a) == 0)
#define uint128_is_one(a) ((a) == 1)

// Utility functions
static inline uint128_t uint128_from_uint64(uint64_t value) {
    return (__uint128_t)value;
}

static inline uint64_t uint128_to_uint64(uint128_t value) {
    return (uint64_t)value;
}

static inline void uint128_to_words(uint128_t value, uint64_t* high, uint64_t* low) {
    *low = (uint64_t)value;
    *high = (uint64_t)(value >> 64);
}

static inline uint128_t uint128_from_words(uint64_t high, uint64_t low) {
    return ((__uint128_t)high << 64) | low;
}

#endif // UINT128_H
