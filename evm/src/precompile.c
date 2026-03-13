/**
 * Precompiled Contracts Implementation
 *
 * Implemented: ECRECOVER (0x01), SHA-256 (0x02), RIPEMD-160 (0x03), IDENTITY (0x04)
 * Stubbed: POINT_EVAL, BLS12-381
 */

#include "precompile.h"
#include <stdlib.h>
#include <string.h>
#include <openssl/sha.h>
#include <openssl/ripemd.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>
#include "keccak256.h"
#include "bn256.h"
#include "mini-gmp.h"
#include "blst.h"

// Helper: compute keccak256 of data into out (32 bytes)
static void keccak256_hash(const uint8_t *data, size_t len, uint8_t out[32])
{
    SHA3_CTX ctx;
    keccak_init(&ctx);
    // keccak_update takes uint16_t size, so chunk if needed
    while (len > 0)
    {
        uint16_t chunk = (len > 0xFFFF) ? 0xFFFF : (uint16_t)len;
        keccak_update(&ctx, data, chunk);
        data += chunk;
        len -= chunk;
    }
    keccak_final(&ctx, out);
}

//==============================================================================
// Address Helpers
//==============================================================================

static uint16_t precompile_index(const address_t *addr)
{
    for (int i = 0; i < 18; i++)
    {
        if (addr->bytes[i] != 0)
            return 0;
    }
    return ((uint16_t)addr->bytes[18] << 8) | addr->bytes[19];
}

//==============================================================================
// is_precompile
//==============================================================================

bool is_precompile(const address_t *addr, evm_fork_t fork)
{
    uint16_t idx = precompile_index(addr);
    if (idx == 0)
        return false;

    if (idx <= PRECOMPILE_IDENTITY)
        return true;

    if (idx <= PRECOMPILE_BN256_PAIRING)
        return fork >= FORK_BYZANTIUM;

    if (idx == PRECOMPILE_BLAKE2F)
        return fork >= FORK_ISTANBUL;

    if (idx == PRECOMPILE_POINT_EVAL)
        return fork >= FORK_CANCUN;

    if (idx >= PRECOMPILE_BLS_G1ADD && idx <= PRECOMPILE_BLS_MAP_G2)
        return fork >= FORK_PRAGUE;

    if (idx == PRECOMPILE_P256VERIFY)
        return fork >= FORK_OSAKA;

    return false;
}

//==============================================================================
// ECRECOVER Precompile (0x01)
//==============================================================================

// Gas: 3000 flat
// Input: 128 bytes — hash(32) | v(32) | r(32) | s(32)
// Output: 32 bytes — zero-padded recovered address (or empty on invalid)
static evm_status_t precompile_ecrecover(const uint8_t *input, size_t input_size,
                                         uint64_t *gas,
                                         uint8_t **output, size_t *output_size)
{
    if (*gas < 3000)
        return EVM_OUT_OF_GAS;
    *gas -= 3000;

    // Pad input to 128 bytes if shorter
    uint8_t padded[128];
    memset(padded, 0, 128);
    if (input_size > 0)
        memcpy(padded, input, input_size < 128 ? input_size : 128);

    // Extract fields
    const uint8_t *hash = padded;        // bytes 0..31
    const uint8_t *v_bytes = padded + 32;  // bytes 32..63
    const uint8_t *r_bytes = padded + 64;  // bytes 64..95
    const uint8_t *s_bytes = padded + 96;  // bytes 96..127

    // v must be 27 or 28 (stored as big-endian uint256, only low byte matters)
    // Check that the first 31 bytes of v are zero
    for (int i = 0; i < 31; i++)
    {
        if (v_bytes[i] != 0)
        {
            // Invalid v — return empty (success with no output)
            *output = NULL;
            *output_size = 0;
            return EVM_SUCCESS;
        }
    }
    uint8_t v = v_bytes[31];
    if (v != 27 && v != 28)
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }
    int recid = v - 27; // 0 or 1

    // Build compact signature: r(32) || s(32)
    uint8_t sig64[64];
    memcpy(sig64, r_bytes, 32);
    memcpy(sig64 + 32, s_bytes, 32);

    // Use libsecp256k1 to recover the public key
    static secp256k1_context *ctx = NULL;
    if (!ctx)
        ctx = secp256k1_context_create(SECP256K1_CONTEXT_VERIFY);

    secp256k1_ecdsa_recoverable_signature sig;
    if (!secp256k1_ecdsa_recoverable_signature_parse_compact(ctx, &sig, sig64, recid))
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }

    secp256k1_pubkey pubkey;
    if (!secp256k1_ecdsa_recover(ctx, &pubkey, &sig, hash))
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }

    // Serialize uncompressed public key (65 bytes: 0x04 || x(32) || y(32))
    uint8_t pubkey_serialized[65];
    size_t pubkey_len = 65;
    secp256k1_ec_pubkey_serialize(ctx, pubkey_serialized, &pubkey_len,
                                  &pubkey, SECP256K1_EC_UNCOMPRESSED);

    // Keccak256(pubkey[1..65]) → take last 20 bytes as address
    uint8_t pubkey_hash[32];
    keccak256_hash(pubkey_serialized + 1, 64, pubkey_hash);

    // Return as 32-byte left-padded address
    *output = calloc(32, 1);
    if (!*output)
        return EVM_INTERNAL_ERROR;
    memcpy(*output + 12, pubkey_hash + 12, 20);
    *output_size = 32;
    return EVM_SUCCESS;
}

//==============================================================================
// SHA-256 Precompile (0x02)
//==============================================================================

// Gas: 60 base + 12 per word (constant across all forks)
// Input: arbitrary bytes
// Output: 32 bytes — SHA-256 digest
static evm_status_t precompile_sha256(const uint8_t *input, size_t input_size,
                                      uint64_t *gas,
                                      uint8_t **output, size_t *output_size,
                                      evm_fork_t fork)
{
    uint64_t words = (input_size + 31) / 32;
    uint64_t cost = 60 + 12 * words;

    if (*gas < cost)
        return EVM_OUT_OF_GAS;
    *gas -= cost;

    *output = malloc(32);
    if (!*output)
        return EVM_INTERNAL_ERROR;

    SHA256(input, input_size, *output);
    *output_size = 32;
    return EVM_SUCCESS;
}

//==============================================================================
// RIPEMD-160 Precompile (0x03)
//==============================================================================

// Gas: 600 base + 120 per word (constant across all forks)
// Input: arbitrary bytes
// Output: 32 bytes — RIPEMD-160 digest left-padded to 32 bytes
static evm_status_t precompile_ripemd160(const uint8_t *input, size_t input_size,
                                         uint64_t *gas,
                                         uint8_t **output, size_t *output_size,
                                         evm_fork_t fork)
{
    uint64_t words = (input_size + 31) / 32;
    uint64_t cost = 600 + 120 * words;

    if (*gas < cost)
        return EVM_OUT_OF_GAS;
    *gas -= cost;

    uint8_t digest[RIPEMD160_DIGEST_LENGTH]; // 20 bytes
    RIPEMD160(input, input_size, digest);

    // Left-pad to 32 bytes (12 zero bytes + 20 byte digest)
    *output = calloc(32, 1);
    if (!*output)
        return EVM_INTERNAL_ERROR;
    memcpy(*output + 12, digest, 20);
    *output_size = 32;
    return EVM_SUCCESS;
}

//==============================================================================
// IDENTITY Precompile (0x04)
//==============================================================================

// Gas: 15 base + 3 per word (EIP-150+), 3 base + 1 per word (pre-EIP-150)
static evm_status_t precompile_identity(const uint8_t *input, size_t input_size,
                                        uint64_t *gas,
                                        uint8_t **output, size_t *output_size,
                                        evm_fork_t fork)
{
    uint64_t words = (input_size + 31) / 32;
    uint64_t cost = 15 + 3 * words;

    if (*gas < cost)
        return EVM_OUT_OF_GAS;
    *gas -= cost;

    if (input_size == 0)
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }

    *output = malloc(input_size);
    if (!*output)
        return EVM_INTERNAL_ERROR;

    memcpy(*output, input, input_size);
    *output_size = input_size;
    return EVM_SUCCESS;
}

//==============================================================================
// BLAKE2F Precompile (0x09)
//==============================================================================

// Gas: rounds (from input)
// Input: exactly 213 bytes — rounds(4) | h(64) | m(128) | t(16) | f(1)
// Output: 64 bytes — updated state vector h

// BLAKE2b IV constants
static const uint64_t blake2b_iv[8] = {
    0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL,
    0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
    0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL,
};

// BLAKE2b sigma permutation table
static const uint8_t blake2b_sigma[10][16] = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
    {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
    {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
    {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
    {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
    {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
    {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
};

static inline uint64_t rotr64(uint64_t x, unsigned r)
{
    return (x >> r) | (x << (64 - r));
}

static inline void blake2b_g(uint64_t v[16], size_t a, size_t b, size_t c, size_t d,
                              uint64_t x, uint64_t y)
{
    v[a] = v[a] + v[b] + x;
    v[d] = rotr64(v[d] ^ v[a], 32);
    v[c] = v[c] + v[d];
    v[b] = rotr64(v[b] ^ v[c], 24);
    v[a] = v[a] + v[b] + y;
    v[d] = rotr64(v[d] ^ v[a], 16);
    v[c] = v[c] + v[d];
    v[b] = rotr64(v[b] ^ v[c], 63);
}

// Read little-endian uint64 from bytes
static inline uint64_t load_le64(const uint8_t *p)
{
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) |
           ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) |
           ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

// Write little-endian uint64 to bytes
static inline void store_le64(uint8_t *p, uint64_t x)
{
    p[0] = (uint8_t)(x);       p[1] = (uint8_t)(x >> 8);
    p[2] = (uint8_t)(x >> 16); p[3] = (uint8_t)(x >> 24);
    p[4] = (uint8_t)(x >> 32); p[5] = (uint8_t)(x >> 40);
    p[6] = (uint8_t)(x >> 48); p[7] = (uint8_t)(x >> 56);
}

static evm_status_t precompile_blake2f(const uint8_t *input, size_t input_size,
                                        uint64_t *gas,
                                        uint8_t **output, size_t *output_size)
{
    // Input must be exactly 213 bytes
    if (input_size != 213)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Parse rounds (big-endian uint32)
    uint32_t rounds = ((uint32_t)input[0] << 24) | ((uint32_t)input[1] << 16) |
                      ((uint32_t)input[2] << 8) | (uint32_t)input[3];

    // Final block flag (must be 0 or 1)
    uint8_t f = input[212];
    if (f > 1)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Gas cost = rounds
    if (*gas < rounds)
        return EVM_OUT_OF_GAS;
    *gas -= rounds;

    // Parse h (8 x uint64, little-endian) at offset 4
    uint64_t h[8];
    for (int i = 0; i < 8; i++)
        h[i] = load_le64(input + 4 + i * 8);

    // Parse m (16 x uint64, little-endian) at offset 68
    uint64_t m[16];
    for (int i = 0; i < 16; i++)
        m[i] = load_le64(input + 68 + i * 8);

    // Parse t (2 x uint64, little-endian) at offset 196
    uint64_t t[2];
    t[0] = load_le64(input + 196);
    t[1] = load_le64(input + 204);

    // Initialize local work vector v[0..15]
    uint64_t v[16];
    for (int i = 0; i < 8; i++)
        v[i] = h[i];
    for (int i = 0; i < 8; i++)
        v[i + 8] = blake2b_iv[i];
    v[12] ^= t[0];
    v[13] ^= t[1];
    if (f)
        v[14] ^= UINT64_MAX;

    // Cryptographic mixing
    for (uint32_t i = 0; i < rounds; i++)
    {
        const uint8_t *s = blake2b_sigma[i % 10];
        blake2b_g(v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        blake2b_g(v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        blake2b_g(v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        blake2b_g(v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        blake2b_g(v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        blake2b_g(v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        blake2b_g(v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        blake2b_g(v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }

    // Finalize: XOR the two halves into h
    for (int i = 0; i < 8; i++)
        h[i] ^= v[i] ^ v[i + 8];

    // Output: 64 bytes (8 x uint64, little-endian)
    *output = malloc(64);
    if (!*output)
        return EVM_INTERNAL_ERROR;
    for (int i = 0; i < 8; i++)
        store_le64(*output + i * 8, h[i]);
    *output_size = 64;

    return EVM_SUCCESS;
}

//==============================================================================
// MODEXP Precompile (0x05) — EIP-198 / EIP-2565
//==============================================================================

// Read a 32-byte big-endian uint from input (zero-padded if short).
// Returns UINT64_MAX if the value doesn't fit in uint64_t.
static uint64_t modexp_read_len(const uint8_t *input, size_t input_size, size_t offset)
{
    uint8_t buf[32];
    memset(buf, 0, 32);
    if (offset < input_size)
    {
        size_t avail = input_size - offset;
        size_t n = avail < 32 ? avail : 32;
        memcpy(buf, input + offset, n);
    }

    // Check that the high 24 bytes are zero (value must fit in uint64_t)
    for (int i = 0; i < 24; i++)
    {
        if (buf[i] != 0)
            return UINT64_MAX;
    }
    uint64_t val = 0;
    for (int i = 24; i < 32; i++)
        val = (val << 8) | buf[i];
    return val;
}

// Find the highest set bit position (0-indexed from MSB) in a big-endian byte buffer.
// Returns 0 if all bytes are zero.
static uint64_t modexp_head_bit_len(const uint8_t *data, size_t len)
{
    for (size_t i = 0; i < len; i++)
    {
        if (data[i] != 0)
        {
            // Find highest bit in this byte
            uint8_t b = data[i];
            unsigned bits = 0;
            while (b > 0) { bits++; b >>= 1; }
            // Total bit length = bits in this byte + 8 * remaining bytes
            return (uint64_t)bits + 8 * (len - 1 - i);
        }
    }
    return 0;
}

// EIP-198 mult_complexity (Byzantium to Istanbul)
// Returns UINT64_MAX on overflow.
static uint64_t modexp_mult_complexity_eip198(uint64_t x)
{
    __uint128_t xx = (__uint128_t)x * x;
    __uint128_t result;
    if (x <= 64)
        result = xx;
    else if (x <= 1024)
        result = xx / 4 + 96 * x - 3072;
    else
        result = xx / 16 + 480 * x - 199680;
    return (result > UINT64_MAX) ? UINT64_MAX : (uint64_t)result;
}

// EIP-2565 mult_complexity (Berlin+)
// Returns UINT64_MAX on overflow.
static uint64_t modexp_mult_complexity_eip2565(uint64_t x)
{
    uint64_t words = (x + 7) / 8;
    __uint128_t result = (__uint128_t)words * words;
    return (result > UINT64_MAX) ? UINT64_MAX : (uint64_t)result;
}

// EIP-7883 mult_complexity (Osaka+)
// For x <= 32: returns 16
// For x > 32: returns 2 * berlinMultComplexity(x)
static uint64_t modexp_mult_complexity_osaka(uint64_t x)
{
    if (x <= 32)
        return 16;
    __uint128_t result = (__uint128_t)modexp_mult_complexity_eip2565(x) * 2;
    return (result > UINT64_MAX) ? UINT64_MAX : (uint64_t)result;
}

static uint64_t modexp_gas(uint64_t base_len, uint64_t exp_len, uint64_t mod_len,
                            const uint8_t *exp_head, size_t exp_head_len,
                            evm_fork_t fork)
{
    // Compute adjusted exponent length
    uint64_t adjusted_exp_len = 0;
    uint64_t head_bit_len = modexp_head_bit_len(exp_head, exp_head_len);

    if (exp_len <= 32)
    {
        // adjusted_exp_len = floor(log2(exponent)) = bit_length - 1
        adjusted_exp_len = (head_bit_len > 0) ? head_bit_len - 1 : 0;
    }
    else
    {
        // 8 * (exp_len - 32) + floor(log2(first_32_bytes))
        // Cap at UINT64_MAX to avoid overflow
        if (exp_len - 32 > UINT64_MAX / 8)
            adjusted_exp_len = UINT64_MAX;
        else {
            adjusted_exp_len = 8 * (exp_len - 32);
            if (head_bit_len > 0)
                adjusted_exp_len += head_bit_len - 1;
        }
    }

    uint64_t max_len = base_len > mod_len ? base_len : mod_len;

    if (fork >= FORK_OSAKA)
    {
        // EIP-7883: Osaka gas formula
        // multiplier = 16, minGas = 500
        uint64_t mc = modexp_mult_complexity_osaka(max_len);
        // iterationCount uses multiplier=16 for large exponents
        uint64_t iter;
        if (exp_len <= 32)
            iter = (head_bit_len > 0) ? head_bit_len - 1 : 0;
        else {
            if (exp_len - 32 > UINT64_MAX / 16)
                iter = UINT64_MAX;
            else {
                iter = 16 * (exp_len - 32);
                if (head_bit_len > 0)
                    iter += head_bit_len - 1;
            }
        }
        iter = iter > 1 ? iter : 1;
        __uint128_t gas128 = (__uint128_t)mc * iter;
        uint64_t gas = (gas128 > UINT64_MAX) ? UINT64_MAX : (uint64_t)gas128;
        return gas > 500 ? gas : 500;
    }
    else if (fork >= FORK_BERLIN)
    {
        // EIP-2565
        uint64_t mc = modexp_mult_complexity_eip2565(max_len);
        uint64_t iter = adjusted_exp_len > 1 ? adjusted_exp_len : 1;
        // Overflow-safe: use 128-bit multiply
        __uint128_t gas128 = (__uint128_t)mc * iter / 3;
        uint64_t gas = (gas128 > UINT64_MAX) ? UINT64_MAX : (uint64_t)gas128;
        return gas > 200 ? gas : 200;
    }
    else
    {
        // EIP-198
        uint64_t mc = modexp_mult_complexity_eip198(max_len);
        uint64_t iter = adjusted_exp_len > 1 ? adjusted_exp_len : 1;
        __uint128_t gas128 = (__uint128_t)mc * iter / 20;
        return (gas128 > UINT64_MAX) ? UINT64_MAX : (uint64_t)gas128;
    }
}

static evm_status_t precompile_modexp(const uint8_t *input, size_t input_size,
                                       uint64_t *gas,
                                       uint8_t **output, size_t *output_size,
                                       evm_fork_t fork)
{
    // Parse lengths (3 x 32 bytes at offset 0, 32, 64)
    uint64_t base_len = modexp_read_len(input, input_size, 0);
    uint64_t exp_len = modexp_read_len(input, input_size, 32);
    uint64_t mod_len = modexp_read_len(input, input_size, 64);

    // If base_len or mod_len overflow uint64, gas is astronomical → OOG
    if (base_len == UINT64_MAX || mod_len == UINT64_MAX)
        return EVM_OUT_OF_GAS;

    // EIP-7823 (Osaka+): reject inputs where any length exceeds 1024 bytes
    if (fork >= FORK_OSAKA &&
        (base_len > 1024 || exp_len > 1024 || mod_len > 1024))
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // exp_len overflow: gas depends on max(base_len, mod_len)
    // If max_len > 0, the gas will be huge → OOG
    // If max_len == 0, mc=0 and gas = min_gas (200 for EIP-2565)
    if (exp_len == UINT64_MAX) {
        uint64_t max_len = base_len > mod_len ? base_len : mod_len;
        if (max_len > 0)
            return EVM_OUT_OF_GAS;
        // max_len==0: gas = minimum (200 for Berlin+, 0 for pre-Berlin)
        uint64_t min_gas = (fork >= FORK_BERLIN) ? 200 : 0;
        if (*gas < min_gas)
            return EVM_OUT_OF_GAS;
        *gas -= min_gas;
        goto modexp_done;
    }

    // Get exponent head (first min(32, exp_len) bytes) for gas calculation
    size_t exp_head_len = exp_len < 32 ? (size_t)exp_len : 32;
    uint8_t exp_head[32];
    memset(exp_head, 0, 32);
    size_t exp_offset = 96 + base_len;
    if (exp_offset < input_size)
    {
        size_t avail = input_size - exp_offset;
        size_t n = avail < exp_head_len ? avail : exp_head_len;
        memcpy(exp_head, input + exp_offset, n);
    }

    // Compute gas cost
    uint64_t cost = modexp_gas(base_len, exp_len, mod_len, exp_head, exp_head_len, fork);
    if (*gas < cost)
        return EVM_OUT_OF_GAS;
    *gas -= cost;

modexp_done:
    // mod_len == 0: return empty
    if (mod_len == 0)
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }

    // Helper: safe read from input with zero-padding
    // data starts at offset 96; base at 96, exp at 96+base_len, mod at 96+base_len+exp_len
    size_t data_offset = 96;

    // Allocate buffers for base, exp, mod bytes (zero-padded)
    uint8_t *base_bytes = calloc((size_t)base_len + 1, 1);
    uint8_t *exp_bytes = calloc((size_t)exp_len + 1, 1);
    uint8_t *mod_bytes = calloc((size_t)mod_len + 1, 1);
    if ((!base_bytes && base_len > 0) || (!exp_bytes && exp_len > 0) || !mod_bytes)
    {
        free(base_bytes);
        free(exp_bytes);
        free(mod_bytes);
        return EVM_INTERNAL_ERROR;
    }

    // Copy from input (with bounds checking + zero-padding)
    size_t base_off = data_offset;
    size_t exp_off = data_offset + base_len;
    size_t mod_off = data_offset + base_len + exp_len;

    for (uint64_t i = 0; i < base_len; i++)
    {
        size_t idx = base_off + i;
        base_bytes[i] = (idx < input_size) ? input[idx] : 0;
    }
    for (uint64_t i = 0; i < exp_len; i++)
    {
        size_t idx = exp_off + i;
        exp_bytes[i] = (idx < input_size) ? input[idx] : 0;
    }
    for (uint64_t i = 0; i < mod_len; i++)
    {
        size_t idx = mod_off + i;
        mod_bytes[i] = (idx < input_size) ? input[idx] : 0;
    }

    // Import into GMP
    mpz_t b, e, m, r;
    mpz_init(b);
    mpz_init(e);
    mpz_init(m);
    mpz_init(r);

    if (base_len > 0)
        mpz_import(b, (size_t)base_len, 1, 1, 0, 0, base_bytes);
    if (exp_len > 0)
        mpz_import(e, (size_t)exp_len, 1, 1, 0, 0, exp_bytes);
    if (mod_len > 0)
        mpz_import(m, (size_t)mod_len, 1, 1, 0, 0, mod_bytes);

    // Compute: if mod == 0, result is all zeros
    if (mpz_sgn(m) == 0)
    {
        // Result is mod_len zero bytes
        *output = calloc((size_t)mod_len, 1);
        if (!*output)
        {
            mpz_clear(b); mpz_clear(e); mpz_clear(m); mpz_clear(r);
            free(base_bytes); free(exp_bytes); free(mod_bytes);
            return EVM_INTERNAL_ERROR;
        }
        *output_size = (size_t)mod_len;
    }
    else
    {
        // mpz_powm: r = b^e mod m
        // Note: mini-gmp returns 1 for exp=0 without reducing mod m,
        // so we explicitly reduce afterward
        mpz_powm(r, b, e, m);
        mpz_mod(r, r, m);

        // Export result to big-endian bytes
        *output = calloc((size_t)mod_len, 1);
        if (!*output)
        {
            mpz_clear(b); mpz_clear(e); mpz_clear(m); mpz_clear(r);
            free(base_bytes); free(exp_bytes); free(mod_bytes);
            return EVM_INTERNAL_ERROR;
        }

        if (mpz_sgn(r) != 0)
        {
            size_t count = 0;
            uint8_t *raw = (uint8_t *)mpz_export(NULL, &count, 1, 1, 0, 0, r);
            if (raw && count > 0)
            {
                // Right-align in output buffer (big-endian, zero-padded on left)
                size_t out_len = (size_t)mod_len;
                if (count <= out_len)
                    memcpy(*output + (out_len - count), raw, count);
                else
                    memcpy(*output, raw + (count - out_len), out_len);
                free(raw);
            }
        }
        *output_size = (size_t)mod_len;
    }

    mpz_clear(b);
    mpz_clear(e);
    mpz_clear(m);
    mpz_clear(r);
    free(base_bytes);
    free(exp_bytes);
    free(mod_bytes);

    return EVM_SUCCESS;
}

//==============================================================================
// BN256 ADD (0x06) — EIP-196
//==============================================================================

static evm_status_t precompile_bn256_add(const uint8_t *input, size_t input_size,
                                         uint64_t *gas, uint8_t **output,
                                         size_t *output_size, evm_fork_t fork)
{
    uint64_t required = (fork >= FORK_ISTANBUL) ? 150 : 500;
    if (*gas < required)
        return EVM_OUT_OF_GAS;
    *gas -= required;

    // Pad input to 128 bytes
    uint8_t padded[128];
    memset(padded, 0, 128);
    if (input_size > 128) input_size = 128;
    if (input_size > 0) memcpy(padded, input, input_size);

    bn256_g1_t p1, p2, result;
    bn256_g1_init(&p1);
    bn256_g1_init(&p2);
    bn256_g1_init(&result);

    if (bn256_g1_unmarshal(&p1, padded) != 0 ||
        bn256_g1_unmarshal(&p2, padded + 64) != 0)
    {
        bn256_g1_clear(&p1);
        bn256_g1_clear(&p2);
        bn256_g1_clear(&result);
        *gas = 0;
        return EVM_REVERT;
    }

    bn256_g1_add(&result, &p1, &p2);

    *output = malloc(64);
    bn256_g1_marshal(*output, &result);
    *output_size = 64;

    bn256_g1_clear(&p1);
    bn256_g1_clear(&p2);
    bn256_g1_clear(&result);
    return EVM_SUCCESS;
}

//==============================================================================
// BN256 MUL (0x07) — EIP-196
//==============================================================================

static evm_status_t precompile_bn256_mul(const uint8_t *input, size_t input_size,
                                         uint64_t *gas, uint8_t **output,
                                         size_t *output_size, evm_fork_t fork)
{
    uint64_t required = (fork >= FORK_ISTANBUL) ? 6000 : 40000;
    if (*gas < required)
        return EVM_OUT_OF_GAS;
    *gas -= required;

    // Pad input to 96 bytes
    uint8_t padded[96];
    memset(padded, 0, 96);
    if (input_size > 96) input_size = 96;
    if (input_size > 0) memcpy(padded, input, input_size);

    bn256_g1_t pt, result;
    bn256_g1_init(&pt);
    bn256_g1_init(&result);

    if (bn256_g1_unmarshal(&pt, padded) != 0)
    {
        bn256_g1_clear(&pt);
        bn256_g1_clear(&result);
        *gas = 0;
        return EVM_REVERT;
    }

    bn256_g1_scalar_mul(&result, &pt, padded + 64);

    *output = malloc(64);
    bn256_g1_marshal(*output, &result);
    *output_size = 64;

    bn256_g1_clear(&pt);
    bn256_g1_clear(&result);
    return EVM_SUCCESS;
}

//==============================================================================
// BN256 PAIRING (0x08) — EIP-197
//==============================================================================

static evm_status_t precompile_bn256_pairing(const uint8_t *input, size_t input_size,
                                             uint64_t *gas, uint8_t **output,
                                             size_t *output_size, evm_fork_t fork)
{
    // Input must be multiple of 192 bytes
    if (input_size % 192 != 0)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    size_t k = input_size / 192;

    uint64_t required = (fork >= FORK_ISTANBUL)
        ? (34000 * k + 45000)
        : (80000 * k + 100000);
    if (*gas < required)
        return EVM_OUT_OF_GAS;
    *gas -= required;

    // Empty input: pairing check passes
    *output = calloc(32, 1);
    *output_size = 32;

    if (k == 0)
    {
        (*output)[31] = 1;
        return EVM_SUCCESS;
    }

    // Unmarshal all points
    bn256_g1_t *g1s = malloc(k * sizeof(bn256_g1_t));
    bn256_g2_t *g2s = malloc(k * sizeof(bn256_g2_t));

    for (size_t i = 0; i < k; i++)
    {
        bn256_g1_init(&g1s[i]);
        bn256_g2_init(&g2s[i]);
    }

    for (size_t i = 0; i < k; i++)
    {
        const uint8_t *pair = input + i * 192;
        if (bn256_g1_unmarshal(&g1s[i], pair) != 0 ||
            bn256_g2_unmarshal(&g2s[i], pair + 64) != 0)
        {
            for (size_t j = 0; j < k; j++)
            {
                bn256_g1_clear(&g1s[j]);
                bn256_g2_clear(&g2s[j]);
            }
            free(g1s);
            free(g2s);
            free(*output);
            *output = NULL;
            *output_size = 0;
            *gas = 0;
            return EVM_REVERT;
        }
    }

    int result = bn256_pairing_check(g1s, g2s, k);

    if (result == 1)
        (*output)[31] = 1;
    // else already zeroed

    for (size_t i = 0; i < k; i++)
    {
        bn256_g1_clear(&g1s[i]);
        bn256_g2_clear(&g2s[i]);
    }
    free(g1s);
    free(g2s);

    return EVM_SUCCESS;
}

//==============================================================================
// KZG Point Evaluation Precompile (0x0A) — EIP-4844
//==============================================================================

// KZG_SETUP_G2_1: [s]₂ from Ethereum mainnet trusted setup (compressed G2)
static const uint8_t KZG_SETUP_G2_1_BYTES[96] = {
    0xb5, 0xbf, 0xd7, 0xdd, 0x8c, 0xde, 0xb1, 0x28,
    0x84, 0x3b, 0xc2, 0x87, 0x23, 0x0a, 0xf3, 0x89,
    0x26, 0x18, 0x70, 0x75, 0xcb, 0xfb, 0xef, 0xa8,
    0x10, 0x09, 0xa2, 0xce, 0x61, 0x5a, 0xc5, 0x3d,
    0x29, 0x14, 0xe5, 0x87, 0x0c, 0xb4, 0x52, 0xd2,
    0xaf, 0xaa, 0xab, 0x24, 0xf3, 0x49, 0x9f, 0x72,
    0x18, 0x5c, 0xbf, 0xee, 0x53, 0x49, 0x27, 0x14,
    0x73, 0x44, 0x29, 0xb7, 0xb3, 0x86, 0x08, 0xe2,
    0x39, 0x26, 0xc9, 0x11, 0xcc, 0xec, 0xea, 0xc9,
    0xa3, 0x68, 0x51, 0x47, 0x7b, 0xa4, 0xc6, 0x0b,
    0x08, 0x70, 0x41, 0xde, 0x62, 0x10, 0x00, 0xed,
    0xc9, 0x8e, 0xda, 0xda, 0x20, 0xc1, 0xde, 0xf2
};

// BLS_MODULUS (scalar field order r of BLS12-381, big-endian)
static const uint8_t BLS_MODULUS[32] = {
    0x73, 0xed, 0xa7, 0x53, 0x29, 0x9d, 0x7d, 0x48,
    0x33, 0x39, 0xd8, 0x08, 0x09, 0xa1, 0xd8, 0x05,
    0x53, 0xbd, 0xa4, 0x02, 0xff, 0xfe, 0x5b, 0xfe,
    0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01
};

// Gas: 50,000 fixed
// Input: 192 bytes — versioned_hash(32) | z(32) | y(32) | commitment(48) | proof(48)
// Output: 64 bytes — FIELD_ELEMENTS_PER_BLOB(32) | BLS_MODULUS(32)
static evm_status_t precompile_point_eval(const uint8_t *input, size_t input_size,
                                          uint64_t *gas, uint8_t **output,
                                          size_t *output_size, evm_fork_t fork)
{
    (void)fork;

    if (*gas < 50000)
        return EVM_OUT_OF_GAS;
    *gas -= 50000;

    // Input must be exactly 192 bytes
    if (input_size != 192)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    const uint8_t *versioned_hash = input;
    const uint8_t *z_bytes = input + 32;
    const uint8_t *y_bytes = input + 64;
    const uint8_t *commitment_bytes = input + 96;
    const uint8_t *proof_bytes = input + 144;

    // Validate z < BLS_MODULUS and y < BLS_MODULUS
    if (memcmp(z_bytes, BLS_MODULUS, 32) >= 0 ||
        memcmp(y_bytes, BLS_MODULUS, 32) >= 0)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Validate versioned_hash == 0x01 || SHA-256(commitment)[1:]
    uint8_t computed_hash[32];
    SHA256(commitment_bytes, 48, computed_hash);
    computed_hash[0] = 0x01;
    if (memcmp(versioned_hash, computed_hash, 32) != 0)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Decompress commitment and proof (G1 points)
    blst_p1_affine commitment_aff, proof_aff;
    if (blst_p1_uncompress(&commitment_aff, commitment_bytes) != BLST_SUCCESS ||
        blst_p1_uncompress(&proof_aff, proof_bytes) != BLST_SUCCESS)
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Subgroup check
    if (!blst_p1_affine_in_g1(&commitment_aff) ||
        !blst_p1_affine_in_g1(&proof_aff))
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Lazy-init [s]₂ from trusted setup
    static blst_p2_affine kzg_setup_g2_1_aff;
    static blst_p2 kzg_setup_g2_1;
    static bool kzg_g2_loaded = false;
    if (!kzg_g2_loaded)
    {
        blst_p2_uncompress(&kzg_setup_g2_1_aff, KZG_SETUP_G2_1_BYTES);
        blst_p2_from_affine(&kzg_setup_g2_1, &kzg_setup_g2_1_aff);
        kzg_g2_loaded = true;
    }

    // Pairing check: e(proof, [s]₂ - z·G2) · e(y·G1 - commitment, G2) == 1

    // Q1 = [s]₂ - z·G2
    blst_scalar z_scalar;
    blst_scalar_from_bendian(&z_scalar, z_bytes);
    uint8_t z_le[32];
    blst_lendian_from_scalar(z_le, &z_scalar);

    blst_p2 z_g2;
    blst_p2_mult(&z_g2, blst_p2_generator(), z_le, 256);
    blst_p2_cneg(&z_g2, 1);  // negate

    blst_p2 q1;
    blst_p2_add_or_double(&q1, &kzg_setup_g2_1, &z_g2);

    // P2 = y·G1 - commitment
    blst_scalar y_scalar;
    blst_scalar_from_bendian(&y_scalar, y_bytes);
    uint8_t y_le[32];
    blst_lendian_from_scalar(y_le, &y_scalar);

    blst_p1 y_g1;
    blst_p1_mult(&y_g1, blst_p1_generator(), y_le, 256);

    blst_p1 commitment_jac;
    blst_p1_from_affine(&commitment_jac, &commitment_aff);
    blst_p1_cneg(&commitment_jac, 1);

    blst_p1 p2;
    blst_p1_add_or_double(&p2, &y_g1, &commitment_jac);

    // Convert to affine for miller loop
    blst_p2_affine q1_aff;
    blst_p2_to_affine(&q1_aff, &q1);

    blst_p1_affine p2_aff;
    blst_p1_to_affine(&p2_aff, &p2);

    // Multi-pairing: e(proof, Q1) · e(P2, G2) == 1
    const blst_p1_affine *Ps[2] = { &proof_aff, &p2_aff };
    const blst_p2_affine *Qs[2] = { &q1_aff, &BLS12_381_G2 };

    blst_fp12 gt;
    blst_miller_loop_n(&gt, Qs, Ps, 2);

    blst_fp12 pairing_result;
    blst_final_exp(&pairing_result, &gt);

    if (!blst_fp12_is_one(&pairing_result))
    {
        *gas = 0;
        return EVM_REVERT;
    }

    // Success: output FIELD_ELEMENTS_PER_BLOB (4096) || BLS_MODULUS
    *output = calloc(64, 1);
    if (!*output)
        return EVM_INTERNAL_ERROR;

    (*output)[30] = 0x10;  // 4096 = 0x1000, big-endian
    (*output)[31] = 0x00;
    memcpy(*output + 32, BLS_MODULUS, 32);
    *output_size = 64;

    return EVM_SUCCESS;
}

//==============================================================================
// BLS12-381 Precompiles (0x0B–0x11) — EIP-2537
//==============================================================================

// BLS12-381 base field modulus p (48 bytes, big-endian)
static const uint8_t BLS12_381_P[48] = {
    0x1a, 0x01, 0x11, 0xea, 0x39, 0x7f, 0xe6, 0x9a,
    0x4b, 0x1b, 0xa7, 0xb6, 0x43, 0x4b, 0xac, 0xd7,
    0x64, 0x77, 0x4b, 0x84, 0xf3, 0x85, 0x12, 0xbf,
    0x67, 0x30, 0xd2, 0xa0, 0xf6, 0xb0, 0xf6, 0x24,
    0x1e, 0xab, 0xff, 0xfe, 0xb1, 0x53, 0xff, 0xff,
    0xb9, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xaa, 0xab
};

// G1 MSM discount table (128 entries, k=1..128)
static const uint16_t g1_msm_discount[128] = {
    1000, 949, 848, 797, 764, 750, 738, 728,
    719, 712, 705, 698, 692, 687, 682, 677,
    673, 669, 665, 661, 658, 654, 651, 648,
    645, 642, 640, 637, 635, 632, 630, 627,
    625, 623, 621, 619, 617, 615, 613, 611,
    609, 608, 606, 604, 603, 601, 599, 598,
    596, 595, 593, 592, 591, 589, 588, 586,
    585, 584, 582, 581, 580, 579, 577, 576,
    575, 574, 573, 572, 570, 569, 568, 567,
    566, 565, 564, 563, 562, 561, 560, 559,
    558, 557, 556, 555, 554, 553, 552, 551,
    550, 549, 548, 547, 547, 546, 545, 544,
    543, 542, 541, 540, 540, 539, 538, 537,
    536, 536, 535, 534, 533, 532, 532, 531,
    530, 529, 528, 528, 527, 526, 525, 525,
    524, 523, 522, 522, 521, 520, 520, 519
};

// G2 MSM discount table (128 entries, k=1..128)
static const uint16_t g2_msm_discount[128] = {
    1000, 1000, 923, 884, 855, 832, 812, 796,
    782, 770, 759, 749, 740, 732, 724, 717,
    711, 704, 699, 693, 688, 683, 679, 674,
    670, 666, 663, 659, 655, 652, 649, 646,
    643, 640, 637, 634, 632, 629, 627, 624,
    622, 620, 618, 615, 613, 611, 609, 607,
    606, 604, 602, 600, 598, 597, 595, 593,
    592, 590, 589, 587, 586, 584, 583, 582,
    580, 579, 578, 576, 575, 574, 573, 571,
    570, 569, 568, 567, 566, 565, 563, 562,
    561, 560, 559, 558, 557, 556, 555, 554,
    553, 552, 552, 551, 550, 549, 548, 547,
    546, 545, 545, 544, 543, 542, 541, 541,
    540, 539, 538, 537, 537, 536, 535, 535,
    534, 533, 532, 532, 531, 530, 530, 529,
    528, 528, 527, 526, 526, 525, 524, 524
};

//------------------------------------------------------------------------------
// BLS12-381 encoding helpers
//------------------------------------------------------------------------------

// Decode a 64-byte padded Fp element (top 16 bytes must be zero, value < p)
static int bls_decode_fp(blst_fp *out, const uint8_t input[64])
{
    for (int i = 0; i < 16; i++)
        if (input[i] != 0) return -1;
    if (memcmp(input + 16, BLS12_381_P, 48) >= 0) return -1;
    blst_fp_from_bendian(out, input + 16);
    return 0;
}

// Check if buffer is all zeros
static bool bls_is_zero(const uint8_t *data, size_t len)
{
    for (size_t i = 0; i < len; i++)
        if (data[i] != 0) return false;
    return true;
}

// Decode a 128-byte G1 point (two padded Fp elements, or all zeros = infinity)
static int bls_decode_g1(blst_p1_affine *out, const uint8_t input[128])
{
    if (bls_is_zero(input, 128))
    {
        memset(out, 0, sizeof(*out));
        return 0;
    }
    if (bls_decode_fp(&out->x, input) != 0) return -1;
    if (bls_decode_fp(&out->y, input + 64) != 0) return -1;
    if (!blst_p1_affine_on_curve(out)) return -1;
    return 0;
}

// Decode a 256-byte G2 point (four padded Fp elements, or all zeros = infinity)
static int bls_decode_g2(blst_p2_affine *out, const uint8_t input[256])
{
    if (bls_is_zero(input, 256))
    {
        memset(out, 0, sizeof(*out));
        return 0;
    }
    if (bls_decode_fp(&out->x.fp[0], input) != 0) return -1;
    if (bls_decode_fp(&out->x.fp[1], input + 64) != 0) return -1;
    if (bls_decode_fp(&out->y.fp[0], input + 128) != 0) return -1;
    if (bls_decode_fp(&out->y.fp[1], input + 192) != 0) return -1;
    if (!blst_p2_affine_on_curve(out)) return -1;
    return 0;
}

// Encode a G1 point (Jacobian) to 128 bytes
static void bls_encode_g1(uint8_t output[128], const blst_p1 *p)
{
    memset(output, 0, 128);
    if (blst_p1_is_inf(p)) return;
    blst_p1_affine aff;
    blst_p1_to_affine(&aff, p);
    blst_bendian_from_fp(output + 16, &aff.x);
    blst_bendian_from_fp(output + 80, &aff.y);
}

// Encode a G2 point (Jacobian) to 256 bytes
static void bls_encode_g2(uint8_t output[256], const blst_p2 *p)
{
    memset(output, 0, 256);
    if (blst_p2_is_inf(p)) return;
    blst_p2_affine aff;
    blst_p2_to_affine(&aff, p);
    blst_bendian_from_fp(output + 16, &aff.x.fp[0]);
    blst_bendian_from_fp(output + 80, &aff.x.fp[1]);
    blst_bendian_from_fp(output + 144, &aff.y.fp[0]);
    blst_bendian_from_fp(output + 208, &aff.y.fp[1]);
}

//------------------------------------------------------------------------------
// 0x0B — BLS12_G1ADD (375 gas)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_g1add(const uint8_t *input, size_t input_size,
                                          uint64_t *gas, uint8_t **output,
                                          size_t *output_size)
{
    if (*gas < 375) return EVM_OUT_OF_GAS;
    *gas -= 375;

    if (input_size != 256) { *gas = 0; return EVM_REVERT; }

    blst_p1_affine a, b;
    if (bls_decode_g1(&a, input) != 0 || bls_decode_g1(&b, input + 128) != 0)
    { *gas = 0; return EVM_REVERT; }

    blst_p1 aj, bj, r;
    blst_p1_from_affine(&aj, &a);
    blst_p1_from_affine(&bj, &b);
    blst_p1_add_or_double(&r, &aj, &bj);

    *output = malloc(128);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g1(*output, &r);
    *output_size = 128;
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x0C — BLS12_G1MSM (discount-table gas, subgroup check required)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_g1msm(const uint8_t *input, size_t input_size,
                                          uint64_t *gas, uint8_t **output,
                                          size_t *output_size)
{
    if (input_size == 0 || input_size % 160 != 0)
    { *gas = 0; return EVM_REVERT; }

    size_t k = input_size / 160;
    size_t idx = (k <= 128) ? k - 1 : 127;
    uint64_t cost = (uint64_t)k * 12000 * g1_msm_discount[idx] / 1000;
    if (*gas < cost) return EVM_OUT_OF_GAS;
    *gas -= cost;

    // Decode, validate, and compute MSM
    blst_p1 result;
    memset(&result, 0, sizeof(result));  // identity

    for (size_t i = 0; i < k; i++)
    {
        const uint8_t *pair = input + i * 160;
        blst_p1_affine pt;
        if (bls_decode_g1(&pt, pair) != 0)
        { *gas = 0; return EVM_REVERT; }

        // Subgroup check (infinity passes)
        if (!blst_p1_affine_is_inf(&pt) && !blst_p1_affine_in_g1(&pt))
        { *gas = 0; return EVM_REVERT; }

        // Scalar is 32 bytes big-endian, convert to little-endian
        uint8_t scalar_le[32];
        for (int j = 0; j < 32; j++)
            scalar_le[j] = pair[160 - 1 - j];  // bytes [128..159] reversed

        blst_p1 pj, scaled;
        blst_p1_from_affine(&pj, &pt);
        blst_p1_mult(&scaled, &pj, scalar_le, 256);
        blst_p1_add_or_double(&result, &result, &scaled);
    }

    *output = malloc(128);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g1(*output, &result);
    *output_size = 128;
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x0D — BLS12_G2ADD (600 gas)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_g2add(const uint8_t *input, size_t input_size,
                                          uint64_t *gas, uint8_t **output,
                                          size_t *output_size)
{
    if (*gas < 600) return EVM_OUT_OF_GAS;
    *gas -= 600;

    if (input_size != 512) { *gas = 0; return EVM_REVERT; }

    blst_p2_affine a, b;
    if (bls_decode_g2(&a, input) != 0 || bls_decode_g2(&b, input + 256) != 0)
    { *gas = 0; return EVM_REVERT; }

    blst_p2 aj, bj, r;
    blst_p2_from_affine(&aj, &a);
    blst_p2_from_affine(&bj, &b);
    blst_p2_add_or_double(&r, &aj, &bj);

    *output = malloc(256);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g2(*output, &r);
    *output_size = 256;
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x0E — BLS12_G2MSM (discount-table gas, subgroup check required)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_g2msm(const uint8_t *input, size_t input_size,
                                          uint64_t *gas, uint8_t **output,
                                          size_t *output_size)
{
    if (input_size == 0 || input_size % 288 != 0)
    { *gas = 0; return EVM_REVERT; }

    size_t k = input_size / 288;
    size_t idx = (k <= 128) ? k - 1 : 127;
    uint64_t cost = (uint64_t)k * 22500 * g2_msm_discount[idx] / 1000;
    if (*gas < cost) return EVM_OUT_OF_GAS;
    *gas -= cost;

    blst_p2 result;
    memset(&result, 0, sizeof(result));

    for (size_t i = 0; i < k; i++)
    {
        const uint8_t *pair = input + i * 288;
        blst_p2_affine pt;
        if (bls_decode_g2(&pt, pair) != 0)
        { *gas = 0; return EVM_REVERT; }

        if (!blst_p2_affine_is_inf(&pt) && !blst_p2_affine_in_g2(&pt))
        { *gas = 0; return EVM_REVERT; }

        uint8_t scalar_le[32];
        for (int j = 0; j < 32; j++)
            scalar_le[j] = pair[288 - 1 - j];

        blst_p2 pj, scaled;
        blst_p2_from_affine(&pj, &pt);
        blst_p2_mult(&scaled, &pj, scalar_le, 256);
        blst_p2_add_or_double(&result, &result, &scaled);
    }

    *output = malloc(256);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g2(*output, &result);
    *output_size = 256;
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x0F — BLS12_PAIRING (32600*k + 37700 gas, subgroup check required)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_pairing(const uint8_t *input, size_t input_size,
                                            uint64_t *gas, uint8_t **output,
                                            size_t *output_size)
{
    if (input_size == 0 || input_size % 384 != 0)
    { *gas = 0; return EVM_REVERT; }

    size_t k = input_size / 384;
    uint64_t cost = (uint64_t)k * 32600 + 37700;
    if (*gas < cost) return EVM_OUT_OF_GAS;
    *gas -= cost;

    // Decode and validate all points
    blst_p1_affine *g1s = malloc(k * sizeof(blst_p1_affine));
    blst_p2_affine *g2s = malloc(k * sizeof(blst_p2_affine));
    if (!g1s || !g2s) { free(g1s); free(g2s); return EVM_INTERNAL_ERROR; }

    for (size_t i = 0; i < k; i++)
    {
        const uint8_t *pair = input + i * 384;
        if (bls_decode_g1(&g1s[i], pair) != 0 ||
            bls_decode_g2(&g2s[i], pair + 128) != 0)
        { free(g1s); free(g2s); *gas = 0; return EVM_REVERT; }

        // Subgroup checks
        if (!blst_p1_affine_is_inf(&g1s[i]) && !blst_p1_affine_in_g1(&g1s[i]))
        { free(g1s); free(g2s); *gas = 0; return EVM_REVERT; }
        if (!blst_p2_affine_is_inf(&g2s[i]) && !blst_p2_affine_in_g2(&g2s[i]))
        { free(g1s); free(g2s); *gas = 0; return EVM_REVERT; }
    }

    // Multi-pairing — filter out pairs where either point is infinity
    const blst_p1_affine **Ps = malloc(k * sizeof(blst_p1_affine *));
    const blst_p2_affine **Qs = malloc(k * sizeof(blst_p2_affine *));
    size_t valid = 0;
    for (size_t i = 0; i < k; i++)
    {
        if (!blst_p1_affine_is_inf(&g1s[i]) && !blst_p2_affine_is_inf(&g2s[i]))
        {
            Ps[valid] = &g1s[i];
            Qs[valid] = &g2s[i];
            valid++;
        }
    }

    *output = calloc(32, 1);
    if (!*output) { free(g1s); free(g2s); free(Ps); free(Qs); return EVM_INTERNAL_ERROR; }

    if (valid == 0)
    {
        // All pairs had infinity — product of identities = 1
        (*output)[31] = 1;
    }
    else
    {
        blst_fp12 gt;
        blst_miller_loop_n(&gt, Qs, Ps, valid);
        blst_fp12 final;
        blst_final_exp(&final, &gt);
        if (blst_fp12_is_one(&final))
            (*output)[31] = 1;
    }
    *output_size = 32;

    free(g1s); free(g2s); free(Ps); free(Qs);
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x10 — BLS12_MAP_FP_TO_G1 (5500 gas)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_map_g1(const uint8_t *input, size_t input_size,
                                           uint64_t *gas, uint8_t **output,
                                           size_t *output_size)
{
    if (*gas < 5500) return EVM_OUT_OF_GAS;
    *gas -= 5500;

    if (input_size != 64) { *gas = 0; return EVM_REVERT; }

    blst_fp u;
    if (bls_decode_fp(&u, input) != 0)
    { *gas = 0; return EVM_REVERT; }

    blst_p1 r;
    blst_map_to_g1(&r, &u, NULL);

    *output = malloc(128);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g1(*output, &r);
    *output_size = 128;
    return EVM_SUCCESS;
}

//------------------------------------------------------------------------------
// 0x11 — BLS12_MAP_FP2_TO_G2 (23800 gas)
//------------------------------------------------------------------------------

static evm_status_t precompile_bls_map_g2(const uint8_t *input, size_t input_size,
                                           uint64_t *gas, uint8_t **output,
                                           size_t *output_size)
{
    if (*gas < 23800) return EVM_OUT_OF_GAS;
    *gas -= 23800;

    if (input_size != 128) { *gas = 0; return EVM_REVERT; }

    blst_fp2 u;
    if (bls_decode_fp(&u.fp[0], input) != 0 ||
        bls_decode_fp(&u.fp[1], input + 64) != 0)
    { *gas = 0; return EVM_REVERT; }

    blst_p2 r;
    blst_map_to_g2(&r, &u, NULL);

    *output = malloc(256);
    if (!*output) return EVM_INTERNAL_ERROR;
    bls_encode_g2(*output, &r);
    *output_size = 256;
    return EVM_SUCCESS;
}

//==============================================================================
// Stub for unimplemented precompiles
//==============================================================================

static evm_status_t precompile_stub(uint8_t idx, uint64_t *gas)
{
    (void)idx;
    *gas = 0;
    return EVM_REVERT;
}

//==============================================================================
// P256VERIFY Precompile (0x0100) — EIP-7212
//==============================================================================

// Gas: 6900 flat
// Input: 160 bytes — hash(32) | r(32) | s(32) | x(32) | y(32)
// Output: 32 bytes (uint256 = 1) on valid signature, empty on invalid
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/bn.h>
#include <openssl/obj_mac.h>

#define P256VERIFY_GAS 6900

static evm_status_t precompile_p256verify(const uint8_t *input, size_t input_size,
                                          uint64_t *gas,
                                          uint8_t **output, size_t *output_size)
{
    if (*gas < P256VERIFY_GAS)
        return EVM_OUT_OF_GAS;
    *gas -= P256VERIFY_GAS;

    // Input must be exactly 160 bytes
    if (input_size != 160)
    {
        *output = NULL;
        *output_size = 0;
        return EVM_SUCCESS;
    }

    const uint8_t *hash = input;
    const uint8_t *r_bytes = input + 32;
    const uint8_t *s_bytes = input + 64;
    const uint8_t *x_bytes = input + 96;
    const uint8_t *y_bytes = input + 128;

    int valid = 0;

    BIGNUM *r = BN_bin2bn(r_bytes, 32, NULL);
    BIGNUM *s = BN_bin2bn(s_bytes, 32, NULL);
    BIGNUM *x = BN_bin2bn(x_bytes, 32, NULL);
    BIGNUM *y = BN_bin2bn(y_bytes, 32, NULL);
    if (!r || !s || !x || !y)
        goto cleanup;

    // Check r, s are in [1, order-1]
    EC_GROUP *group = EC_GROUP_new_by_curve_name(NID_X9_62_prime256v1);
    if (!group)
        goto cleanup;

    BIGNUM *order = BN_new();
    if (!order || !EC_GROUP_get_order(group, order, NULL))
    {
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }

    if (BN_is_zero(r) || BN_is_zero(s) ||
        BN_cmp(r, order) >= 0 || BN_cmp(s, order) >= 0)
    {
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }

    // Construct the public key point
    EC_POINT *pub_point = EC_POINT_new(group);
    if (!pub_point ||
        !EC_POINT_set_affine_coordinates_GFp(group, pub_point, x, y, NULL) ||
        !EC_POINT_is_on_curve(group, pub_point, NULL))
    {
        EC_POINT_free(pub_point);
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }

    // Build EC_KEY from the point
    EC_KEY *ec_key = EC_KEY_new();
    if (!ec_key ||
        !EC_KEY_set_group(ec_key, group) ||
        !EC_KEY_set_public_key(ec_key, pub_point))
    {
        EC_KEY_free(ec_key);
        EC_POINT_free(pub_point);
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }

    // Build ECDSA_SIG from (r, s)
    ECDSA_SIG *sig = ECDSA_SIG_new();
    if (!sig)
    {
        EC_KEY_free(ec_key);
        EC_POINT_free(pub_point);
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }
    // ECDSA_SIG_set0 takes ownership of r_bn, s_bn — duplicate them
    BIGNUM *r_dup = BN_dup(r);
    BIGNUM *s_dup = BN_dup(s);
    if (!r_dup || !s_dup || !ECDSA_SIG_set0(sig, r_dup, s_dup))
    {
        if (r_dup) BN_free(r_dup);
        if (s_dup) BN_free(s_dup);
        ECDSA_SIG_free(sig);
        EC_KEY_free(ec_key);
        EC_POINT_free(pub_point);
        BN_free(order);
        EC_GROUP_free(group);
        goto cleanup;
    }

    // Verify: ECDSA_do_verify returns 1 on valid, 0 on invalid, -1 on error
    valid = (ECDSA_do_verify(hash, 32, sig, ec_key) == 1);

    ECDSA_SIG_free(sig);
    EC_KEY_free(ec_key);
    EC_POINT_free(pub_point);
    BN_free(order);
    EC_GROUP_free(group);

cleanup:
    BN_free(r);
    BN_free(s);
    BN_free(x);
    BN_free(y);

    if (valid)
    {
        // Return 32-byte big-endian uint256 = 1
        *output = calloc(32, 1);
        if (!*output)
            return EVM_INTERNAL_ERROR;
        (*output)[31] = 1;
        *output_size = 32;
    }
    else
    {
        *output = NULL;
        *output_size = 0;
    }

    return EVM_SUCCESS;
}

//==============================================================================
// precompile_execute
//==============================================================================

evm_status_t precompile_execute(const address_t *addr,
                                const uint8_t *input, size_t input_size,
                                uint64_t *gas,
                                uint8_t **output, size_t *output_size,
                                evm_fork_t fork)
{
    uint16_t idx = precompile_index(addr);

    *output = NULL;
    *output_size = 0;

    switch (idx)
    {
    case PRECOMPILE_ECRECOVER:
        return precompile_ecrecover(input, input_size, gas, output, output_size);
    case PRECOMPILE_SHA256:
        return precompile_sha256(input, input_size, gas, output, output_size, fork);
    case PRECOMPILE_RIPEMD160:
        return precompile_ripemd160(input, input_size, gas, output, output_size, fork);
    case PRECOMPILE_IDENTITY:
        return precompile_identity(input, input_size, gas, output, output_size, fork);

    case PRECOMPILE_MODEXP:
        return precompile_modexp(input, input_size, gas, output, output_size, fork);

    case PRECOMPILE_BN256_ADD:
        return precompile_bn256_add(input, input_size, gas, output, output_size, fork);
    case PRECOMPILE_BN256_MUL:
        return precompile_bn256_mul(input, input_size, gas, output, output_size, fork);
    case PRECOMPILE_BN256_PAIRING:
        return precompile_bn256_pairing(input, input_size, gas, output, output_size, fork);

    case PRECOMPILE_BLAKE2F:
        return precompile_blake2f(input, input_size, gas, output, output_size);
    case PRECOMPILE_POINT_EVAL:
        return precompile_point_eval(input, input_size, gas, output, output_size, fork);
    case PRECOMPILE_BLS_G1ADD:
        return precompile_bls_g1add(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_G1MSM:
        return precompile_bls_g1msm(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_G2ADD:
        return precompile_bls_g2add(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_G2MSM:
        return precompile_bls_g2msm(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_PAIRING:
        return precompile_bls_pairing(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_MAP_G1:
        return precompile_bls_map_g1(input, input_size, gas, output, output_size);
    case PRECOMPILE_BLS_MAP_G2:
        return precompile_bls_map_g2(input, input_size, gas, output, output_size);

    case PRECOMPILE_P256VERIFY:
        return precompile_p256verify(input, input_size, gas, output, output_size);

    default:
        return EVM_INTERNAL_ERROR;
    }
}
