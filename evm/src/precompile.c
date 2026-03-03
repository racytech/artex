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
