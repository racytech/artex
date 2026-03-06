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

uint256_t uint256_add(const uint256_t* a, const uint256_t* b) {
    // Load values to local variables for better cache performance
    uint128_t a_low = a->low;
    uint128_t a_high = a->high;
    uint128_t b_low = b->low;
    uint128_t b_high = b->high;
    
    uint256_t result;
    result.low = a_low + b_low;
    
    // Check for carry: carry occurs if result.low < a_low
    uint128_t carry = (result.low < a_low) ? UINT128_ONE : UINT128_ZERO;
    result.high = a_high + b_high + carry;
    
    return result;
}

uint256_t uint256_sub(const uint256_t* a, const uint256_t* b) {
    // Load values to local variables for better cache performance
    uint128_t a_low = a->low;
    uint128_t a_high = a->high;
    uint128_t b_low = b->low;
    uint128_t b_high = b->high;
    
    uint256_t result;
    result.low = a_low - b_low;
    
    // Check for borrow: borrow occurs if a_low < b_low
    uint128_t borrow = (a_low < b_low) ? UINT128_ONE : UINT128_ZERO;
    result.high = a_high - b_high - borrow;
    
    return result;
}



uint256_t uint256_mul(const uint256_t* a, const uint256_t* b) {
    uint256_t result;
    
    // Use 2×2 multiplication approach:
    // (a_high * 2^128 + a_low) × (b_high * 2^128 + b_low)
    // = a_high*b_high*2^256 + (a_high*b_low + a_low*b_high)*2^128 + a_low*b_low
    // 
    // Since we only want the lower 256 bits, we can ignore the a_high*b_high*2^256 term
    // Result = (a_high*b_low + a_low*b_high)*2^128 + a_low*b_low
    
    // Compute a_low × b_low (contributes to all 256 bits)
    uint256_t low_product = uint128_mul_to_uint256(a->low, b->low);
    
    // Compute a_high × b_low (contributes to upper 128 bits when shifted)
    uint256_t high_low_product = uint128_mul_to_uint256(a->high, b->low);
    
    // Compute a_low × b_high (contributes to upper 128 bits when shifted)  
    uint256_t low_high_product = uint128_mul_to_uint256(a->low, b->high);
    
    // Start with a_low × b_low
    result = low_product;
    
    // Add (a_high × b_low) << 128
    // This means adding high_low_product.low to result.high
    result.high += high_low_product.low;
    // Note: high_low_product.high would contribute to bits beyond 256, so we ignore it
    
    // Add (a_low × b_high) << 128  
    // This means adding low_high_product.low to result.high
    result.high += low_high_product.low;
    // Note: low_high_product.high would contribute to bits beyond 256, so we ignore it
    
    return result;
}

uint256_t uint256_div(const uint256_t* a, const uint256_t* b) {
    if (!a || !b || uint256_is_zero(b)) {
        return UINT256_ZERO; // Division by zero
    }
    
    if (uint256_is_zero(a)) {
        return UINT256_ZERO;
    }
    
    if (uint256_is_one(b)) {
        return *a;
    }
    
    // If dividend is smaller than divisor, result is 0
    if (uint256_is_less(a, b)) {
        return UINT256_ZERO;
    }
    
    // If they're equal, result is 1
    if (uint256_is_equal(a, b)) {
        return UINT256_ONE;
    }
    
    // Long division algorithm with optimizations
    uint256_t quotient = UINT256_ZERO;
    uint256_t remainder = UINT256_ZERO;
    
    // Find the most significant bit to avoid unnecessary iterations
    int msb = uint256_bit_length(a) - 1;
    
    // Process from most significant bit to least significant bit
    for (int i = msb; i >= 0; i--) {
        // Shift remainder left by 1 bit
        remainder = uint256_shl(&remainder, 1);
        
        // Set the least significant bit of remainder to the current bit of dividend
        if (uint256_get_bit(a, i)) {
            remainder = uint256_set_bit(&remainder, 0);
        }
        
        // If remainder >= divisor, subtract divisor from remainder and set quotient bit
        if (uint256_is_greater_equal(&remainder, b)) {
            remainder = uint256_sub(&remainder, b);
            quotient = uint256_set_bit(&quotient, i);
        }
    }
    
    return quotient;
}

uint256_t uint256_mod(const uint256_t* a, const uint256_t* b) {
    // Simple modulo algorithm
    uint256_t quotient = uint256_div(a, b);
    uint256_t product = uint256_mul(&quotient, b);
    return uint256_sub(a, &product);
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
// Comparison functions (optimized implementations)
// ============================================================================

int uint256_compare(const uint256_t* a, const uint256_t* b) {
    // Compare high parts first for efficiency
    if (a->high != b->high) {
        return uint128_gt(a->high, b->high) ? 1 : -1;
    }
    // High parts equal, compare low parts
    if (a->low != b->low) {
        return uint128_gt(a->low, b->low) ? 1 : -1;
    }
    return 0;
}

bool uint256_is_equal(const uint256_t* a, const uint256_t* b) {
    return uint128_eq(a->low, b->low) && uint128_eq(a->high, b->high);
}

bool uint256_is_less(const uint256_t* a, const uint256_t* b) {
    // Optimized: avoid function call overhead
    return (a->high < b->high) || (a->high == b->high && a->low < b->low);
}

bool uint256_is_less_equal(const uint256_t* a, const uint256_t* b) {
    return (a->high < b->high) || (a->high == b->high && a->low <= b->low);
}

bool uint256_is_greater(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low > b->low);
}

bool uint256_is_greater_equal(const uint256_t* a, const uint256_t* b) {
    return (a->high > b->high) || (a->high == b->high && a->low >= b->low);
}

// ============================================================================
// Alternative comparison functions (for convenience) - optimized
// ============================================================================

bool uint256_eq(const uint256_t* a, const uint256_t* b) {
    return uint128_eq(a->low, b->low) && uint128_eq(a->high, b->high);
}

bool uint256_ne(const uint256_t* a, const uint256_t* b) {
    return !uint256_eq(a, b);
}

bool uint256_lt(const uint256_t* a, const uint256_t* b) {
    return uint256_is_less(a, b);
}

bool uint256_le(const uint256_t* a, const uint256_t* b) {
    return uint256_is_less_equal(a, b);
}

bool uint256_gt(const uint256_t* a, const uint256_t* b) {
    return uint256_is_greater(a, b);
}

bool uint256_ge(const uint256_t* a, const uint256_t* b) {
    return uint256_is_greater_equal(a, b);
}

// ============================================================================
// Test functions
// ============================================================================

bool uint256_is_zero(const uint256_t* a) {
    return uint128_is_zero(a->low) && uint128_is_zero(a->high);
}

bool uint256_is_one(const uint256_t* a) {
    return uint128_eq(a->low, UINT128_ONE) && uint128_eq(a->high, UINT128_ZERO);
}

// ============================================================================
// Bitwise operations
// ============================================================================

uint256_t uint256_and(const uint256_t* a, const uint256_t* b) {
    uint256_t result;
    result.low = uint128_and(a->low, b->low);
    result.high = uint128_and(a->high, b->high);
    return result;
}

uint256_t uint256_or(const uint256_t* a, const uint256_t* b) {
    uint256_t result;
    result.low = uint128_or(a->low, b->low);
    result.high = uint128_or(a->high, b->high);
    return result;
}

uint256_t uint256_xor(const uint256_t* a, const uint256_t* b) {
    uint256_t result;
    result.low = uint128_xor(a->low, b->low);
    result.high = uint128_xor(a->high, b->high);
    return result;
}

uint256_t uint256_not(const uint256_t* a) {
    uint256_t result;
    result.low = uint128_not(a->low);
    result.high = uint128_not(a->high);
    return result;
}

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
    if (len > 32) len = 32; // Max 32 bytes for 256 bits
    
    uint64_t words[4] = {0, 0, 0, 0};
    
    for (size_t i = 0; i < len; i++) {
        size_t word_idx = i / 8;
        size_t byte_pos = i % 8;
        
        if (word_idx < 4) {
            words[word_idx] |= ((uint64_t)bytes[len - 1 - i] << (byte_pos * 8));
        }
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

    uint64_t words[4];
    uint256_to_words(a, words);

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 8; j++) {
            bytes[31 - (i * 8 + j)] = (words[i] >> (j * 8)) & 0xFF;
        }
    }
}

void uint256_to_bytes_le(const uint256_t* a, uint8_t* bytes) {
    if (!a || !bytes) return;

    uint64_t words[4];
    uint256_to_words(a, words);

    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 8; j++) {
            bytes[i * 8 + j] = (words[i] >> (j * 8)) & 0xFF;
        }
    }
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

void uint256_to_words(const uint256_t* a, uint64_t words[4]) {
    uint64_t low_high, low_low, high_high, high_low;
    uint128_to_words(a->low, &low_high, &low_low);
    uint128_to_words(a->high, &high_high, &high_low);
    words[0] = low_low;
    words[1] = low_high;
    words[2] = high_low;
    words[3] = high_high;
}

uint256_t uint256_from_words(const uint64_t words[4]) {
    uint256_t result;
    result.low = uint128_from_words(words[1], words[0]);
    result.high = uint128_from_words(words[3], words[2]);
    return result;
}

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
    
    uint256_t result = UINT256_ONE;
    uint256_t base_copy = *base;
    uint256_t exp_copy = *exponent;
    
    // Binary exponentiation
    while (!uint256_is_zero(&exp_copy)) {
        if (uint256_get_bit(&exp_copy, 0)) {
            result = uint256_mul(&result, &base_copy);
        }
        base_copy = uint256_mul(&base_copy, &base_copy);
        exp_copy = uint256_shr(&exp_copy, 1);
    }
    
    return result;
}

uint256_t uint256_addmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod) {
    if (!a || !b || !mod || uint256_is_zero(mod)) return UINT256_ZERO;
    
    // Reduce inputs first
    uint256_t a_mod = uint256_mod(a, mod);
    uint256_t b_mod = uint256_mod(b, mod);
    
    // Try to add them
    uint256_t sum;
    bool overflow = uint256_add_overflow(&a_mod, &b_mod, &sum);
    
    if (!overflow) {
        // No overflow, just return sum % mod
        return uint256_mod(&sum, mod);
    }
    
    // Overflow occurred. Since a_mod < mod and b_mod < mod,
    // the mathematical sum a_mod + b_mod is in the range [2^256, 2*mod).
    // Since overflow occurred, we have: sum = (a_mod + b_mod) mod 2^256
    // The true mathematical sum is: a_mod + b_mod = sum + 2^256
    // We want: (a_mod + b_mod) mod mod = (sum + 2^256) mod mod
    //
    // Since a_mod + b_mod >= 2^256 and a_mod + b_mod < 2*mod,
    // and we know that 2^256 > mod (for any reasonable EVM-style mod),
    // we have (a_mod + b_mod) mod mod = (a_mod + b_mod) - mod
    //
    // So we need: (sum + 2^256) - mod = sum + (2^256 - mod)
    //
    // Since we can't easily compute 2^256 - mod directly without risking
    // overflow again, we use a different approach:
    //
    // We know that:
    // - sum = (a_mod + b_mod) mod 2^256 (the truncated result)
    // - a_mod + b_mod = sum + 2^256 (the true mathematical sum)
    // - We want (a_mod + b_mod) mod mod = (sum + 2^256) mod mod
    //
    // Since a_mod + b_mod < 2*mod and a_mod + b_mod >= 2^256 > mod,
    // the result is simply (a_mod + b_mod) - mod.
    //
    // We can compute this safely:
    // result = sum + (2^256 mod mod) - mod
    // But instead of computing 2^256 mod mod explicitly, we use the fact that:
    // if the sum overflowed, and both operands were < mod, then the
    // mathematical result modulo mod is: sum + (what we lost due to overflow) mod mod.
    //
    // The "what we lost" is 2^256, so we need (sum + 2^256) mod mod.
    // Since 2^256 ≡ 2^256 mod mod, we need: sum + (2^256 mod mod) mod mod.
    //
    // But there's a simpler way. Since both operands are < mod, their sum
    // is < 2*mod. When overflow occurs, the sum is >= 2^256. Since 2^256 > mod,
    // the result is (mathematical sum) - mod.
    //
    // We compute this by recognizing that the overflow "wraps around":
    // Since the true sum = sum + 2^256, and we want (true sum) mod mod,
    // and since true sum >= 2^256 > mod and true sum < 2*mod,
    // the result is true sum - mod = (sum + 2^256) - mod.
    //
    // Now, to compute this without overflow:
    // We want sum + 2^256 - mod.
    // Since 2^256 ≡ 0 (mod 2^256), we have 2^256 - mod ≡ -mod (mod 2^256).
    // In unsigned arithmetic, -mod = 2^256 - mod.
    // So we want sum + (2^256 - mod) = sum - mod + 2^256.
    // Since we're working mod 2^256, this becomes sum - mod.
    // But if sum < mod, this underflows, giving us sum - mod + 2^256,
    // which is exactly what we want!
    
    // Simple approach: compute sum - mod with underflow handling
    if (uint256_is_greater_equal(&sum, mod)) {
        // sum >= mod, so sum - mod doesn't underflow
        return uint256_sub(&sum, mod);
    } else {
        // sum < mod, so sum - mod would underflow to sum - mod + 2^256
        // This is exactly the correction we need!
        // In two's complement, this is handled automatically
        uint256_t result = uint256_sub(&sum, mod);
        return result;
    }
}

uint256_t uint256_mulmod(const uint256_t* a, const uint256_t* b, const uint256_t* mod) {
    if (!a || !b || !mod || uint256_is_zero(mod)) return UINT256_ZERO;
    
    // Handle simple cases
    if (uint256_is_zero(a) || uint256_is_zero(b)) return UINT256_ZERO;
    if (uint256_is_one(a)) return uint256_mod(b, mod);
    if (uint256_is_one(b)) return uint256_mod(a, mod);
    
    // For small values where multiplication won't overflow, use direct approach
    if (a->high == 0 && b->high == 0) {
        // Both values fit in 128 bits, so their product fits in 256 bits
        uint256_t product = uint128_mul_to_uint256(a->low, b->low);
        return uint256_mod(&product, mod);
    }
    
    // For EVM compatibility with large values, use the binary multiplication approach
    // but we need to be more careful. Let's use the Montgomery-style approach:
    // We compute (a * b) % mod by simulating the multiplication bit by bit.
    // 
    // The key insight is that for a*b, we can represent b in binary and compute:
    // a * b = a * (b₀ + b₁*2 + b₂*2² + ... + b₂₅₅*2²⁵⁵)
    //       = a*b₀ + a*b₁*2 + a*b₂*2² + ... + a*b₂₅₅*2²⁵⁵
    // 
    // We can compute this by iterating through the bits of b from LSB to MSB,
    // maintaining a running result and a running "shifted a" value.
    
    uint256_t result = UINT256_ZERO;
    uint256_t shifted_a = uint256_mod(a, mod);  // Start with a reduced modulo mod
    uint256_t temp_b = *b;
    
    while (!uint256_is_zero(&temp_b)) {
        // If the current bit of b is set, add the current shifted_a to result
        if (temp_b.low & 1) {
            result = uint256_addmod(&result, &shifted_a, mod);
        }
        
        // Shift to the next bit: shift b right by 1, and double shifted_a (mod mod)
        temp_b = uint256_shr(&temp_b, 1);
        if (!uint256_is_zero(&temp_b)) {  // Only compute if we'll use it
            shifted_a = uint256_addmod(&shifted_a, &shifted_a, mod);
        }
    }
    
    return result;
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
        return UINT256_ZERO; // Division by zero or 0 % anything = 0
    }
    
    // Check sign bits efficiently  
    bool a_negative = (a->high >> 127) & 1;
    bool b_negative = (b->high >> 127) & 1;
    
    // Handle the special overflow case: (-2^255) % (-1) = 0
    if (a_negative && b_negative && 
        a->high == ((uint128_t)1 << 127) && a->low == 0 &&
        b->high == UINT128_MAX && b->low == UINT128_MAX) {
        return UINT256_ZERO;
    }
    
    // EVM SMOD semantics: a - (a SDIV b) * b
    // This is more efficient than converting to absolute values twice
    uint256_t quotient = uint256_sdiv(a, b);
    uint256_t product = uint256_mul(&quotient, b);
    return uint256_sub(a, &product);
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
