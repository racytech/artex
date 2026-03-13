#include "uint256.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

// Constants
const uint256_t UINT256_ZERO = {UINT128_ZERO, UINT128_ZERO};
const uint256_t UINT256_ONE = {UINT128_ONE, UINT128_ZERO};
const uint256_t UINT256_MAX = {UINT128_MAX, UINT128_MAX};

// ============================================================================
// Internal helper functions
// ============================================================================

/**
 * @brief Multiply two 128-bit integers and return a 256-bit result
 * 
 * Performs 128-bit × 128-bit multiplication returning the full 256-bit result.
 * This is more efficient than the generic 4×4 approach for our 2×2 use case.
 * 
 * @param a First 128-bit operand
 * @param b Second 128-bit operand
 * @return 256-bit result of a × b
 */
static uint256_t uint128_mul_to_uint256(uint128_t a, uint128_t b) {
    uint256_t result;
    
    // Split each 128-bit number into two 64-bit halves
    uint64_t a_low = uint128_low(a);
    uint64_t a_high = uint128_high(a);
    uint64_t b_low = uint128_low(b);
    uint64_t b_high = uint128_high(b);
    
    // Perform 4 partial multiplications using 64×64→128 operations
    uint128_t p0 = (uint128_t)a_low * b_low;     // Low × Low
    uint128_t p1 = (uint128_t)a_low * b_high;    // Low × High  
    uint128_t p2 = (uint128_t)a_high * b_low;    // High × Low
    uint128_t p3 = (uint128_t)a_high * b_high;   // High × High
    
    // Combine the partial products:
    // Result = p3 * 2^128 + (p1 + p2) * 2^64 + p0
    
    // Start with p0 (contributes to bits 0-127)
    result.low = p0;
    result.high = 0;
    
    // Add p1 shifted by 64 bits (contributes to bits 64-191)
    uint128_t p1_shifted_low = p1 << 64;
    uint128_t p1_shifted_high = p1 >> 64;
    
    result.low += p1_shifted_low;
    // Check for carry from low addition
    if (result.low < p1_shifted_low) {
        result.high += 1;  // Carry from low to high
    }
    result.high += p1_shifted_high;
    
    // Add p2 shifted by 64 bits (contributes to bits 64-191)
    uint128_t p2_shifted_low = p2 << 64;
    uint128_t p2_shifted_high = p2 >> 64;
    
    uint128_t old_low = result.low;
    result.low += p2_shifted_low;
    // Check for carry from low addition
    if (result.low < old_low) {
        result.high += 1;  // Carry from low to high
    }
    result.high += p2_shifted_high;
    
    // Add p3 shifted by 128 bits (contributes to bits 128-255)
    result.high += p3;
    
    return result;
}

// ============================================================================
// Arithmetic operations
// ============================================================================

/* uint256_mul is now inline in uint256.h */

/**
 * Internal combined divmod: *q = a / b, *r = a % b.
 * Precondition: b != 0, a > b (caller handles trivial cases).
 */
static void uint256_divmod_internal(const uint256_t *a, const uint256_t *b,
                                     uint256_t *q, uint256_t *r)
{
    /* Fast path 1: both operands fit in native 128 bits */
    if (a->high == 0 && b->high == 0) {
        q->low  = a->low / b->low;
        q->high = 0;
        r->low  = a->low % b->low;
        r->high = 0;
        return;
    }

    /* Fast path 2: divisor fits in a single 64-bit limb — schoolbook 256÷64 */
    if (b->high == 0 && (b->low >> 64) == 0) {
        uint64_t d = (uint64_t)b->low;
        uint64_t aw[4], qw[4];
        uint256_to_words(a, aw);
        uint64_t rem = 0;
        for (int i = 3; i >= 0; i--) {
            __uint128_t cur = ((__uint128_t)rem << 64) | aw[i];
            qw[i] = (uint64_t)(cur / d);
            rem   = (uint64_t)(cur % d);
        }
        *q = uint256_from_words(qw);
        r->low  = (uint128_t)rem;
        r->high = 0;
        return;
    }

    /* General: Knuth Algorithm D with base-2^64 limbs */
    uint64_t aw[4], bw[4];
    uint256_to_words(a, aw);
    uint256_to_words(b, bw);

    /* n = significant limbs in divisor (>= 2 here) */
    int n = 4;
    while (n > 0 && bw[n - 1] == 0) n--;

    int an = 4;
    while (an > 0 && aw[an - 1] == 0) an--;

    int m = an - n; /* quotient has at most m+1 limbs */
    if (m < 0) {
        *q = UINT256_ZERO;
        *r = *a;
        return;
    }

    /* Normalize: shift left so v[n-1] has its MSB set */
    unsigned s = (unsigned)__builtin_clzll(bw[n - 1]);

    uint64_t vn[4] = {0};
    if (s > 0) {
        for (int i = n - 1; i > 0; i--)
            vn[i] = (bw[i] << s) | (bw[i - 1] >> (64 - s));
        vn[0] = bw[0] << s;
    } else {
        for (int i = 0; i < n; i++) vn[i] = bw[i];
    }

    uint64_t un[5] = {0}; /* normalized dividend, up to 5 limbs */
    if (s > 0) {
        un[an] = aw[an - 1] >> (64 - s);
        for (int i = an - 1; i > 0; i--)
            un[i] = (aw[i] << s) | (aw[i - 1] >> (64 - s));
        un[0] = aw[0] << s;
    } else {
        for (int i = 0; i < an; i++) un[i] = aw[i];
    }

    uint64_t qw[4] = {0};

    for (int j = m; j >= 0; j--) {
        /* Trial quotient: q_hat ≈ (un[j+n]*B + un[j+n-1]) / vn[n-1] */
        __uint128_t num = ((__uint128_t)un[j + n] << 64) | un[j + n - 1];
        __uint128_t q_hat = num / vn[n - 1];
        __uint128_t r_hat = num % vn[n - 1];

        /* Refine (at most 2 corrections) */
        while (q_hat >= ((__uint128_t)1 << 64) ||
               (n >= 2 && q_hat * vn[n - 2] >
                   ((r_hat << 64) | un[j + n - 2]))) {
            q_hat--;
            r_hat += vn[n - 1];
            if (r_hat >= ((__uint128_t)1 << 64)) break;
        }

        /* Multiply and subtract: un[j..j+n] -= q_hat * vn[0..n-1] */
        __int128_t k = 0;
        for (int i = 0; i < n; i++) {
            __uint128_t p = (__uint128_t)q_hat * vn[i];
            __int128_t t = (__int128_t)un[j + i] - (uint64_t)p - k;
            un[j + i] = (uint64_t)t;
            k = (__int128_t)(p >> 64) - (t >> 64);
        }
        __int128_t t = (__int128_t)un[j + n] - k;
        un[j + n] = (uint64_t)t;

        qw[j] = (uint64_t)q_hat;

        /* Add back if we subtracted too much */
        if (t < 0) {
            qw[j]--;
            uint64_t carry = 0;
            for (int i = 0; i < n; i++) {
                __uint128_t sum = (__uint128_t)un[j + i] + vn[i] + carry;
                un[j + i] = (uint64_t)sum;
                carry = (uint64_t)(sum >> 64);
            }
            un[j + n] += carry;
        }
    }

    *q = uint256_from_words(qw);

    /* De-normalize remainder */
    uint64_t rw[4] = {0};
    if (s > 0) {
        for (int i = 0; i < n - 1; i++)
            rw[i] = (un[i] >> s) | (un[i + 1] << (64 - s));
        rw[n - 1] = un[n - 1] >> s;
    } else {
        for (int i = 0; i < n; i++) rw[i] = un[i];
    }
    *r = uint256_from_words(rw);
}

uint256_t uint256_div(const uint256_t* a, const uint256_t* b) {
    if (uint256_is_zero(b))             return UINT256_ZERO;
    if (uint256_is_zero(a))             return UINT256_ZERO;
    if (uint256_is_one(b))              return *a;
    if (uint256_is_less(a, b))          return UINT256_ZERO;
    if (uint256_is_equal(a, b))         return UINT256_ONE;

    uint256_t q, r;
    uint256_divmod_internal(a, b, &q, &r);
    return q;
}

uint256_t uint256_mod(const uint256_t* a, const uint256_t* b) {
    if (uint256_is_zero(b))             return UINT256_ZERO;
    if (uint256_is_zero(a))             return UINT256_ZERO;
    if (uint256_is_one(b))              return UINT256_ZERO;
    if (uint256_is_less(a, b))          return *a;
    if (uint256_is_equal(a, b))         return UINT256_ZERO;

    uint256_t q, r;
    uint256_divmod_internal(a, b, &q, &r);
    return r;
}

// ============================================================================
// Bitwise operations (shift operations only - simple ops moved back from header)
// ============================================================================

uint256_t uint256_shl(const uint256_t* a, unsigned int shift) {
    if (shift == 0) {
        return *a;
    }
    if (shift >= 256) {
        return UINT256_ZERO;
    }
    
    uint256_t result;
    if (shift >= 128) {
        result.high = uint128_shl(a->low, shift - 128);
        result.low = UINT128_ZERO;
    } else {
        result.low = uint128_shl(a->low, shift);
        result.high = uint128_shl(a->high, shift) | uint128_shr(a->low, 128 - shift);
    }
    return result;
}

uint256_t uint256_shr(const uint256_t* a, unsigned int shift) {
    if (shift == 0) {
        return *a;
    }
    if (shift >= 256) {
        return UINT256_ZERO;
    }
    
    uint256_t result;
    if (shift >= 128) {
        result.low = uint128_shr(a->high, shift - 128);
        result.high = UINT128_ZERO;
    } else {
        result.low = uint128_shr(a->low, shift) | uint128_shl(a->high, 128 - shift);
        result.high = uint128_shr(a->high, shift);
    }
    return result;
}

// ============================================================================
// Arithmetic overflow detection
// ============================================================================

bool uint256_add_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result) {
    if (!a || !b || !result) return false;
    
    // Perform addition
    result->low = a->low + b->low;
    uint128_t carry = (result->low < a->low) ? UINT128_ONE : UINT128_ZERO;
    result->high = a->high + b->high + carry;
    
    // Check for overflow in high part
    return (result->high < a->high) || (carry && result->high == a->high);
}

bool uint256_sub_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result) {
    if (!a || !b || !result) return false;
    
    // Check if b > a (underflow)
    bool underflow = uint256_is_less(a, b);
    
    // Perform subtraction
    result->low = a->low - b->low;
    uint128_t borrow = (a->low < b->low) ? UINT128_ONE : UINT128_ZERO;
    result->high = a->high - b->high - borrow;
    
    return underflow;
}

bool uint256_mul_overflow(const uint256_t* a, const uint256_t* b, uint256_t* result) {
    if (!a || !b || !result) return false;
    
    // Simple overflow check: if either operand has high bits set and the other is non-zero
    bool a_has_high = !uint128_is_zero(a->high);
    bool b_has_high = !uint128_is_zero(b->high);
    
    *result = uint256_mul(a, b);
    
    // More sophisticated overflow detection would require tracking intermediate carry bits
    return a_has_high && !uint256_is_zero(b) && b_has_high && !uint256_is_zero(a);
}

// ============================================================================
// Bitwise shift operations (kept in .c — multi-branch, less frequent)
// ============================================================================

// NOTE: add/sub, bitwise (and/or/xor/not), comparison, and test functions
// are static inline in uint256.h for zero-overhead in the EVM hot loop.

// ============================================================================
// Test functions - removed (moved to inline in header)
// ============================================================================

// ============================================================================
// Creation and conversion functions
// ============================================================================

uint256_t uint256_from_uint64(uint64_t value) {
    uint256_t result;
    result.low = uint128_from_uint64(value);
    result.high = UINT128_ZERO;
    return result;
}

uint256_t uint256_from_uint128(uint128_t value) {
    uint256_t result;
    result.low = value;
    result.high = UINT128_ZERO;
    return result;
}

uint256_t uint256_from_hex(const char* hex_str) {
    uint256_t result = {UINT128_ZERO, UINT128_ZERO};
    
    if (!hex_str) return result;
    
    // Skip "0x" prefix if present
    if (hex_str[0] == '0' && (hex_str[1] == 'x' || hex_str[1] == 'X')) {
        hex_str += 2;
    }
    
    size_t len = strlen(hex_str);
    if (len > 64) len = 64; // Max 64 hex characters for 256 bits
    
    uint64_t words[4] = {0, 0, 0, 0};
    
    for (size_t i = 0; i < len; i++) {
        char c = hex_str[len - 1 - i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) break;
        
        uint8_t digit = 0;
        if (c >= '0' && c <= '9') digit = c - '0';
        else if (c >= 'a' && c <= 'f') digit = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') digit = c - 'A' + 10;
        
        size_t word_idx = i / 16;
        size_t bit_pos = (i % 16) * 4;
        
        if (word_idx < 4) {
            words[word_idx] |= ((uint64_t)digit << bit_pos);
        }
    }
    
    return uint256_from_words(words);
}

uint256_t uint256_from_bytes(const uint8_t* bytes, size_t len) {
    uint256_t result = {UINT128_ZERO, UINT128_ZERO};

    if (!bytes || len == 0) return result;
    if (len > 32) len = 32;

    /* Fast path: full 32 bytes (MLOAD always passes 32) */
    if (len == 32) {
        uint64_t w0, w1, w2, w3;
        memcpy(&w3, bytes +  0, 8);
        memcpy(&w2, bytes +  8, 8);
        memcpy(&w1, bytes + 16, 8);
        memcpy(&w0, bytes + 24, 8);
        result.low  = ((__uint128_t)__builtin_bswap64(w1) << 64) | __builtin_bswap64(w0);
        result.high = ((__uint128_t)__builtin_bswap64(w3) << 64) | __builtin_bswap64(w2);
        return result;
    }

    /* Slow path: partial (rare — only for short from_bytes calls) */
    uint64_t words[4] = {0, 0, 0, 0};
    for (size_t i = 0; i < len; i++) {
        size_t word_idx = i / 8;
        size_t byte_pos = i % 8;
        words[word_idx] |= ((uint64_t)bytes[len - 1 - i] << (byte_pos * 8));
    }
    return uint256_from_words(words);
}

uint256_t uint256_make(uint128_t high, uint128_t low) {
    uint256_t result;
    result.high = high;
    result.low = low;
    return result;
}

uint64_t uint256_to_uint64(const uint256_t* a) {
    return uint128_to_uint64(a->low);
}

char* uint256_to_hex(const uint256_t* a) {
    if (!a) return NULL;
    
    char* hex_str = malloc(67); // 64 hex chars + "0x" + null terminator
    if (!hex_str) return NULL;
    
    uint64_t words[4];
    uint256_to_words(a, words);
     sprintf(hex_str, "0x%016" PRIx64 "%016" PRIx64 "%016" PRIx64 "%016" PRIx64, 
            words[3], words[2], words[1], words[0]);
    
    return hex_str;
}

void uint256_to_bytes(const uint256_t* a, uint8_t* bytes) {
    if (!a || !bytes) return;

    /* bswap64 each word and store in reverse order (big-endian output) */
    uint64_t w0 = (uint64_t)a->low;
    uint64_t w1 = (uint64_t)(a->low >> 64);
    uint64_t w2 = (uint64_t)a->high;
    uint64_t w3 = (uint64_t)(a->high >> 64);
    w0 = __builtin_bswap64(w0);
    w1 = __builtin_bswap64(w1);
    w2 = __builtin_bswap64(w2);
    w3 = __builtin_bswap64(w3);
    memcpy(bytes +  0, &w3, 8);
    memcpy(bytes +  8, &w2, 8);
    memcpy(bytes + 16, &w1, 8);
    memcpy(bytes + 24, &w0, 8);
}

void uint256_to_bytes_le(const uint256_t* a, uint8_t* bytes) {
    if (!a || !bytes) return;

    /* Little-endian: native byte order on x86_64 — direct copy */
    uint64_t w0 = (uint64_t)a->low;
    uint64_t w1 = (uint64_t)(a->low >> 64);
    uint64_t w2 = (uint64_t)a->high;
    uint64_t w3 = (uint64_t)(a->high >> 64);
    memcpy(bytes +  0, &w0, 8);
    memcpy(bytes +  8, &w1, 8);
    memcpy(bytes + 16, &w2, 8);
    memcpy(bytes + 24, &w3, 8);
}

uint256_t uint256_from_bytes_le(const uint8_t* bytes, size_t len) {
    uint256_t result = {UINT128_ZERO, UINT128_ZERO};

    if (!bytes || len == 0) return result;
    if (len > 32) len = 32;

    uint64_t words[4] = {0, 0, 0, 0};

    for (size_t i = 0; i < len; i++) {
        size_t word_idx = i / 8;
        size_t byte_pos = i % 8;

        if (word_idx < 4) {
            words[word_idx] |= ((uint64_t)bytes[i] << (byte_pos * 8));
        }
    }

    return uint256_from_words(words);
}

// uint256_to_words and uint256_from_words are now static inline in uint256.h

// ============================================================================
// Additional utility functions
// ============================================================================

int uint256_bit_length(const uint256_t* a) {
    if (!a || uint256_is_zero(a)) return 0;
    
    if (a->high != 0) {
        // Count bits in high part + 128 base bits from low part
        uint128_t high = a->high;
        int bit_count = 128;
        // Use builtin if available for better performance
        #ifdef __GNUC__
        bit_count += 128 - __builtin_clzll((uint64_t)(high >> 64));
        if ((high >> 64) == 0) {
            bit_count = 64 + 128 - __builtin_clzll((uint64_t)high);
        }
        #else
        while (high > 0) {
            high >>= 1;
            bit_count++;
        }
        #endif
        return bit_count;
    } else {
        // Count bits in low part only
        uint128_t low = a->low;
        #ifdef __GNUC__
        if ((low >> 64) != 0) {
            return 128 - __builtin_clzll((uint64_t)(low >> 64));
        } else {
            return 64 - __builtin_clzll((uint64_t)low);
        }
        #else
        int bit_count = 0;
        while (low > 0) {
            low >>= 1;
            bit_count++;
        }
        return bit_count;
        #endif
    }
}


uint256_t uint256_set_bit(const uint256_t* a, unsigned int bit_index) {
    uint256_t result = *a;
    if (!a || bit_index >= 256) return result;
    
    if (bit_index < 128) {
        result.low |= ((__uint128_t)1 << bit_index);
    } else {
        result.high |= ((__uint128_t)1 << (bit_index - 128));
    }
    return result;
}

uint256_t uint256_clear_bit(const uint256_t* a, unsigned int bit_index) {
    uint256_t result = *a;
    if (!a || bit_index >= 256) return result;
    
    if (bit_index < 128) {
        result.low &= ~((__uint128_t)1 << bit_index);
    } else {
        result.high &= ~((__uint128_t)1 << (bit_index - 128));
    }
    return result;
}

// ============================================================================
// EVM-specific functions
// ============================================================================

uint256_t uint256_exp(const uint256_t* base, const uint256_t* exponent) {
    if (!base || !exponent) return UINT256_ZERO;

    if (uint256_is_zero(exponent)) return UINT256_ONE;
    if (uint256_is_zero(base)) return UINT256_ZERO;

    // Find highest set bit to avoid unnecessary squarings
    int exp_bits = uint256_bit_length(exponent);

    uint256_t result = UINT256_ONE;
    uint256_t b = *base;

    // Binary exponentiation — iterate over 64-bit words directly
    uint64_t ew[4];
    uint256_to_words(exponent, ew);

    for (int bit = 0; bit < exp_bits; bit++) {
        if (ew[bit >> 6] & (1ULL << (bit & 63)))
            result = uint256_mul(&result, &b);
        if (bit + 1 < exp_bits)
            b = uint256_mul(&b, &b);
    }

    return result;
}

uint256_t uint256_addmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod) {
    if (uint256_is_zero(mod)) return UINT256_ZERO;

    /* Fast path: when mod has high word set and both operands have high words
     * <= mod's high word, we can use conditional subtraction instead of
     * expensive full division. This covers the common case of large moduli. */
    uint64_t mod_hi = (uint64_t)(mod->high >> 64);
    if (mod_hi != 0 &&
        (uint64_t)(a->high >> 64) <= mod_hi &&
        (uint64_t)(b->high >> 64) <= mod_hi) {
        /* Reduce a: if a >= mod, a -= mod */
        uint256_t an = *a;
        if (uint256_is_greater_equal(&an, mod))
            an = uint256_sub(&an, mod);

        /* Reduce b: if b >= mod, b -= mod */
        uint256_t bn = *b;
        if (uint256_is_greater_equal(&bn, mod))
            bn = uint256_sub(&bn, mod);

        uint256_t sum;
        bool overflow = uint256_add_overflow(&an, &bn, &sum);

        if (!overflow && uint256_is_less(&sum, mod))
            return sum;

        return uint256_sub(&sum, mod);
    }

    /* Slow path: full division */
    uint256_t a_mod = uint256_mod(a, mod);
    uint256_t b_mod = uint256_mod(b, mod);

    uint256_t sum;
    bool overflow = uint256_add_overflow(&a_mod, &b_mod, &sum);

    if (!overflow && uint256_is_less(&sum, mod))
        return sum;

    return uint256_sub(&sum, mod);
}

/**
 * 512-bit unsigned integer for mulmod intermediate product.
 * Represented as 8 × 64-bit limbs, little-endian.
 */
typedef struct { uint64_t w[8]; } uint512_t;

static uint512_t uint512_from_mul256(const uint256_t *a, const uint256_t *b) {
    uint64_t aw[4], bw[4];
    uint256_to_words(a, aw);
    uint256_to_words(b, bw);

    uint512_t r = {.w = {0}};
    for (int i = 0; i < 4; i++) {
        uint64_t carry = 0;
        for (int j = 0; j < 4; j++) {
            __uint128_t p = (__uint128_t)aw[i] * bw[j] + r.w[i + j] + carry;
            r.w[i + j] = (uint64_t)p;
            carry = (uint64_t)(p >> 64);
        }
        r.w[i + 4] = carry;
    }
    return r;
}

/**
 * 512-bit mod 256-bit using Knuth schoolbook division.
 * Only the remainder is needed. Divisor d has at least 1 significant limb.
 */
static uint256_t uint512_mod256(const uint512_t *num, const uint256_t *d) {
    uint64_t dw[4];
    uint256_to_words(d, dw);

    int n = 4;
    while (n > 0 && dw[n - 1] == 0) n--;

    /* Divisor fits in 64 bits — fast schoolbook 512÷64 for remainder only */
    if (n == 1) {
        uint64_t div = dw[0];
        uint64_t rem = 0;
        for (int i = 7; i >= 0; i--) {
            __uint128_t cur = ((__uint128_t)rem << 64) | num->w[i];
            rem = (uint64_t)(cur % div);
        }
        return uint256_from_uint64(rem);
    }

    /* General: Knuth Algorithm D, 512÷256 */
    unsigned s = (unsigned)__builtin_clzll(dw[n - 1]);

    uint64_t vn[4] = {0};
    if (s > 0) {
        for (int i = n - 1; i > 0; i--)
            vn[i] = (dw[i] << s) | (dw[i - 1] >> (64 - s));
        vn[0] = dw[0] << s;
    } else {
        for (int i = 0; i < n; i++) vn[i] = dw[i];
    }

    /* Normalized numerator: up to 9 data limbs + 1 sentinel for Knuth D */
    uint64_t un[10] = {0};
    if (s > 0) {
        un[8] = num->w[7] >> (64 - s);
        for (int i = 7; i > 0; i--)
            un[i] = (num->w[i] << s) | (num->w[i - 1] >> (64 - s));
        un[0] = num->w[0] << s;
    } else {
        for (int i = 0; i < 8; i++) un[i] = num->w[i];
    }

    int an = 9;
    while (an > 0 && un[an - 1] == 0) an--;

    int m = an - n;
    if (m < 0) {
        /* num < d — just return num mod 2^256 */
        uint64_t rw[4] = { num->w[0], num->w[1], num->w[2], num->w[3] };
        return uint256_from_words(rw);
    }

    for (int j = m; j >= 0; j--) {
        __uint128_t numj = ((__uint128_t)un[j + n] << 64) | un[j + n - 1];
        __uint128_t q_hat = numj / vn[n - 1];
        __uint128_t r_hat = numj % vn[n - 1];

        while (q_hat >= ((__uint128_t)1 << 64) ||
               (n >= 2 && q_hat * vn[n - 2] >
                   ((r_hat << 64) | un[j + n - 2]))) {
            q_hat--;
            r_hat += vn[n - 1];
            if (r_hat >= ((__uint128_t)1 << 64)) break;
        }

        __int128_t k = 0;
        for (int i = 0; i < n; i++) {
            __uint128_t p = (__uint128_t)q_hat * vn[i];
            __int128_t t = (__int128_t)un[j + i] - (uint64_t)p - k;
            un[j + i] = (uint64_t)t;
            k = (__int128_t)(p >> 64) - (t >> 64);
        }
        __int128_t t = (__int128_t)un[j + n] - k;
        un[j + n] = (uint64_t)t;

        if (t < 0) {
            uint64_t carry = 0;
            for (int i = 0; i < n; i++) {
                __uint128_t sum = (__uint128_t)un[j + i] + vn[i] + carry;
                un[j + i] = (uint64_t)sum;
                carry = (uint64_t)(sum >> 64);
            }
            un[j + n] += carry;
        }
    }

    /* De-normalize remainder */
    uint64_t rw[4] = {0};
    if (s > 0) {
        for (int i = 0; i < n - 1; i++)
            rw[i] = (un[i] >> s) | (un[i + 1] << (64 - s));
        rw[n - 1] = un[n - 1] >> s;
    } else {
        for (int i = 0; i < n; i++) rw[i] = un[i];
    }
    return uint256_from_words(rw);
}

uint256_t uint256_mulmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod) {
    if (uint256_is_zero(mod)) return UINT256_ZERO;
    if (uint256_is_zero(a) || uint256_is_zero(b)) return UINT256_ZERO;
    if (uint256_is_one(a)) return uint256_mod(b, mod);
    if (uint256_is_one(b)) return uint256_mod(a, mod);

    // For small values where multiplication won't overflow
    if (a->high == 0 && b->high == 0) {
        uint256_t product = uint128_mul_to_uint256(a->low, b->low);
        return uint256_mod(&product, mod);
    }

    // Full 512-bit product, then mod
    uint512_t full = uint512_from_mul256(a, b);
    return uint512_mod256(&full, mod);
}

// ============================================================================
// EVM signed arithmetic operations
// ============================================================================

uint256_t uint256_sdiv(const uint256_t* a, const uint256_t* b) {
    if (!a || !b || uint256_is_zero(b) || uint256_is_zero(a)) {
        return UINT256_ZERO; // Division by zero or 0/anything = 0
    }
    
    // Check sign bits efficiently
    bool a_negative = (a->high >> 127) & 1;
    bool b_negative = (b->high >> 127) & 1;
    
    // Handle the special overflow case: (-2^255) / (-1)
    // Check for -2^255: only MSB set (high = 0x8000000000000000, low = 0)
    // Check for -1: all bits set (high = all 1s, low = all 1s)
    if (a_negative && b_negative && 
        a->high == ((uint128_t)1 << 127) && a->low == 0 &&
        b->high == UINT128_MAX && b->low == UINT128_MAX) {
        return *a; // EVM behavior: return original value (no overflow)
    }
    
    // Convert to absolute values for division
    uint256_t abs_a, abs_b;
    if (a_negative) {
        abs_a = uint256_sub(&UINT256_ZERO, a);
    } else {
        abs_a = *a;
    }
    
    if (b_negative) {
        abs_b = uint256_sub(&UINT256_ZERO, b);
    } else {
        abs_b = *b;
    }
    
    // Perform unsigned division
    uint256_t result = uint256_div(&abs_a, &abs_b);
    
    // Apply sign: result is negative if signs differ
    if (a_negative != b_negative) {
        result = uint256_sub(&UINT256_ZERO, &result);
    }
    
    return result;
}

uint256_t uint256_smod(const uint256_t* a, const uint256_t* b) {
    if (!a || !b || uint256_is_zero(b) || uint256_is_zero(a)) {
        return UINT256_ZERO;
    }

    bool a_negative = (a->high >> 127) & 1;
    bool b_negative = (b->high >> 127) & 1;

    // (-2^255) % (-1) = 0
    if (a_negative && b_negative &&
        a->high == ((uint128_t)1 << 127) && a->low == 0 &&
        b->high == UINT128_MAX && b->low == UINT128_MAX) {
        return UINT256_ZERO;
    }

    // Convert to absolute values
    uint256_t abs_a = a_negative ? uint256_sub(&UINT256_ZERO, a) : *a;
    uint256_t abs_b = b_negative ? uint256_sub(&UINT256_ZERO, b) : *b;

    // Compute remainder directly via unsigned mod (avoids sdiv + mul + sub)
    uint256_t r = uint256_mod(&abs_a, &abs_b);

    // Result sign follows dividend sign (EVM SMOD semantics)
    return a_negative ? uint256_sub(&UINT256_ZERO, &r) : r;
}

bool uint256_slt(const uint256_t* a, const uint256_t* b) {
    if (!a || !b) return false;
    
    // Check sign bits directly for performance
    bool a_negative = (a->high >> 127) & 1;
    bool b_negative = (b->high >> 127) & 1;
    
    // If signs differ, negative is less than positive
    if (a_negative != b_negative) {
        return a_negative;
    }
    
    // Same sign - compare as unsigned (works for both positive and negative in two's complement)
    return uint256_is_less(a, b);
}

bool uint256_sgt(const uint256_t* a, const uint256_t* b) {
    if (!a || !b) return false;
    
    // Check sign bits directly for performance
    bool a_negative = (a->high >> 127) & 1;
    bool b_negative = (b->high >> 127) & 1;
    
    // If signs differ, positive is greater than negative
    if (a_negative != b_negative) {
        return b_negative;
    }
    
    // Same sign - compare as unsigned
    return uint256_is_greater(a, b);
}

uint256_t uint256_sar(const uint256_t* a, unsigned int shift) {
    if (!a || shift == 0) return *a;
    
    // Check sign bit efficiently
    bool is_negative = (a->high >> 127) & 1;
    
    if (shift >= 256) {
        return is_negative ? UINT256_MAX : UINT256_ZERO;
    }
    
    uint256_t result = uint256_shr(a, shift);
    
    // If negative, set higher bits to 1
    if (is_negative) {
        uint256_t mask = uint256_shl(&UINT256_MAX, 256 - shift);
        result = uint256_or(&result, &mask);
    }
    return result;
}

uint256_t uint256_signextend(const uint256_t* a, unsigned int byte_num) {
    if (!a || byte_num > 31) return *a;

    unsigned int bit_position = (byte_num + 1) * 8 - 1;
    bool is_negative = uint256_get_bit(a, bit_position);

    uint256_t mask = uint256_shl(&UINT256_MAX, bit_position + 1);

    if (is_negative) {
        return uint256_or(a, &mask);
    } else {
        uint256_t inverted_mask = uint256_not(&mask);
        return uint256_and(a, &inverted_mask);
    }
}

// ============================================================================
// EVM byte operations
// ============================================================================

uint8_t uint256_byte(const uint256_t* a, unsigned int byte_index) {
    if (!a || byte_index >= 32) return 0;
    
    // EVM BYTE operation: byte_index 0 is the most significant byte
    // In our little-endian representation:
    // - byte_index 0 corresponds to byte 31 in memory layout
    // - byte_index 31 corresponds to byte 0 in memory layout
    unsigned int actual_byte = 31 - byte_index;
    unsigned int word_idx = actual_byte / 8;
    unsigned int byte_pos = actual_byte % 8;
    
    uint64_t words[4];
    uint256_to_words(a, words);
    
    return (words[word_idx] >> (byte_pos * 8)) & 0xFF;
}

// ============================================================================
// Memory operations
// ============================================================================

void uint256_copy(uint256_t* dest, const uint256_t* src) {
    if (!dest || !src) return;
    dest->low = src->low;
    dest->high = src->high;
}

void uint256_zero(uint256_t* a) {
    if (!a) return;
    a->low = UINT128_ZERO;
    a->high = UINT128_ZERO;
}

void uint256_set(uint256_t* a, uint64_t value) {
    if (!a) return;
    a->low = uint128_from_uint64(value);
    a->high = UINT128_ZERO;
}

// ============================================================================
// Debug/Print functions
// ============================================================================

void uint256_print(const uint256_t* a) {
    if (!a) {
        printf("(null)");
        return;
    }
    
    uint64_t words[4];
    uint256_to_words(a, words);
    printf("0x%016" PRIx64 "%016" PRIx64 "%016" PRIx64 "%016" PRIx64, 
           words[3], words[2], words[1], words[0]);
}

void uint256_print_hex(const uint256_t* a) {
    uint256_print(a);
    printf("\n");
}

void uint256_print_binary(const uint256_t* a) {
    if (!a) {
        printf("(null)\n");
        return;
    }
    
    uint64_t words[4];
    uint256_to_words(a, words);
    
    for (int i = 3; i >= 0; i--) {
        for (int j = 63; j >= 0; j--) {
            printf("%d", (int)((words[i] >> j) & 1));
            if (j % 8 == 0) printf(" ");
        }
        printf("\n");
    }
}



bool uint256_get_bit(const uint256_t* a, unsigned int bit_index) {
    if (!a || bit_index >= 256) return false;
    
    if (bit_index < 128) {
        return (a->low >> bit_index) & 1;
    } else {
        return (a->high >> (bit_index - 128)) & 1;
    }
}
