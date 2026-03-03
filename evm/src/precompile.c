/**
 * Precompiled Contracts Implementation
 *
 * Implemented: ECRECOVER (0x01), SHA-256 (0x02), RIPEMD-160 (0x03), IDENTITY (0x04)
 * Stubbed: MODEXP, BN256, BLAKE2F, POINT_EVAL, BLS12-381
 */

#include "precompile.h"
#include <stdlib.h>
#include <string.h>
#include <openssl/sha.h>
#include <openssl/ripemd.h>
#include <secp256k1.h>
#include <secp256k1_recovery.h>
#include "keccak256.h"

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

static uint8_t precompile_index(const address_t *addr)
{
    for (int i = 0; i < 19; i++)
    {
        if (addr->bytes[i] != 0)
            return 0;
    }
    return addr->bytes[19];
}

//==============================================================================
// is_precompile
//==============================================================================

bool is_precompile(const address_t *addr, evm_fork_t fork)
{
    uint8_t idx = precompile_index(addr);
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

// Gas: 60 base + 12 per word (EIP-150+), 12 base + 6 per word (pre-EIP-150)
// Input: arbitrary bytes
// Output: 32 bytes — SHA-256 digest
static evm_status_t precompile_sha256(const uint8_t *input, size_t input_size,
                                      uint64_t *gas,
                                      uint8_t **output, size_t *output_size,
                                      evm_fork_t fork)
{
    uint64_t words = (input_size + 31) / 32;
    uint64_t cost = (fork >= FORK_TANGERINE_WHISTLE)
                    ? 60 + 12 * words
                    : 12 + 6 * words;

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

// Gas: 600 base + 120 per word (EIP-150+), 120 base + 12 per word (pre-EIP-150)
// Input: arbitrary bytes
// Output: 32 bytes — RIPEMD-160 digest left-padded to 32 bytes
static evm_status_t precompile_ripemd160(const uint8_t *input, size_t input_size,
                                         uint64_t *gas,
                                         uint8_t **output, size_t *output_size,
                                         evm_fork_t fork)
{
    uint64_t words = (input_size + 31) / 32;
    uint64_t cost = (fork >= FORK_TANGERINE_WHISTLE)
                    ? 600 + 120 * words
                    : 120 + 12 * words;

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
    uint64_t cost = (fork >= FORK_TANGERINE_WHISTLE)
                    ? 15 + 3 * words
                    : 3 + 1 * words;

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
// Stub for unimplemented precompiles
//==============================================================================

static evm_status_t precompile_stub(uint8_t idx, uint64_t *gas)
{
    (void)idx;
    *gas = 0;
    return EVM_REVERT;
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
    uint8_t idx = precompile_index(addr);

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
    case PRECOMPILE_BN256_ADD:
    case PRECOMPILE_BN256_MUL:
    case PRECOMPILE_BN256_PAIRING:
    case PRECOMPILE_BLAKE2F:
        return precompile_blake2f(input, input_size, gas, output, output_size);
    case PRECOMPILE_POINT_EVAL:
    case PRECOMPILE_BLS_G1ADD:
    case PRECOMPILE_BLS_G1MUL:
    case PRECOMPILE_BLS_G1MSM:
    case PRECOMPILE_BLS_G2ADD:
    case PRECOMPILE_BLS_G2MUL:
    case PRECOMPILE_BLS_G2MSM:
    case PRECOMPILE_BLS_PAIRING:
    case PRECOMPILE_BLS_MAP_G1:
    case PRECOMPILE_BLS_MAP_G2:
        return precompile_stub(idx, gas);

    default:
        return EVM_INTERNAL_ERROR;
    }
}
