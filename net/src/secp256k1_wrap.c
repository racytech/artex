/*
 * secp256k1 wrapper — thin layer over bitcoin-core/secp256k1.
 *
 * Provides the subset needed by Discovery v5:
 *   - ECDH with raw X coordinate (not SHA256-hashed)
 *   - Recoverable ECDSA sign / recover
 *   - Public key compress / decompress
 */

#include "../include/secp256k1_wrap.h"
#include <secp256k1.h>
#include <secp256k1_ecdh.h>
#include <secp256k1_recovery.h>
#include <string.h>

/* =========================================================================
 * Global context
 * ========================================================================= */

static secp256k1_context *g_ctx = NULL;

bool secp256k1_wrap_init(void) {
    if (g_ctx) return true;
    g_ctx = secp256k1_context_create(SECP256K1_CONTEXT_NONE);
    return g_ctx != NULL;
}

void secp256k1_wrap_destroy(void) {
    if (g_ctx) {
        secp256k1_context_destroy(g_ctx);
        g_ctx = NULL;
    }
}

/* =========================================================================
 * Public key derivation
 * ========================================================================= */

bool secp256k1_wrap_pubkey_create(uint8_t pub[64], const uint8_t priv[32]) {
    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_create(g_ctx, &pk, priv))
        return false;

    /* Serialize uncompressed: 65 bytes (04 || x || y) */
    uint8_t buf[65];
    size_t len = 65;
    secp256k1_ec_pubkey_serialize(g_ctx, buf, &len,
                                  &pk, SECP256K1_EC_UNCOMPRESSED);
    /* Strip the 0x04 prefix */
    memcpy(pub, buf + 1, 64);
    return true;
}

/* =========================================================================
 * ECDH — raw X coordinate
 * ========================================================================= */

/* Custom hash function that returns the raw X coordinate (no SHA256) */
static int ecdh_hash_raw_x(unsigned char *output,
                            const unsigned char *x32,
                            const unsigned char *y32,
                            void *data) {
    (void)y32;
    (void)data;
    memcpy(output, x32, 32);
    return 1;
}

bool secp256k1_wrap_ecdh(uint8_t shared[32],
                         const uint8_t priv[32],
                         const uint8_t pub[64]) {
    /* Parse the uncompressed public key (need to prepend 0x04) */
    uint8_t buf[65];
    buf[0] = 0x04;
    memcpy(buf + 1, pub, 64);

    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_parse(g_ctx, &pk, buf, 65))
        return false;

    return secp256k1_ecdh(g_ctx, shared, &pk, priv, ecdh_hash_raw_x, NULL) == 1;
}

/* Custom hash function that returns the compressed point (02/03 prefix + X) */
static int ecdh_hash_compressed(unsigned char *output,
                                 const unsigned char *x32,
                                 const unsigned char *y32,
                                 void *data) {
    (void)data;
    /* Prefix: 0x02 if y is even, 0x03 if y is odd */
    output[0] = (y32[31] & 1) ? 0x03 : 0x02;
    memcpy(output + 1, x32, 32);
    return 1;
}

bool secp256k1_wrap_ecdh_compressed(uint8_t shared[33],
                                     const uint8_t priv[32],
                                     const uint8_t pub[64]) {
    uint8_t buf[65];
    buf[0] = 0x04;
    memcpy(buf + 1, pub, 64);

    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_parse(g_ctx, &pk, buf, 65))
        return false;

    return secp256k1_ecdh(g_ctx, shared, &pk, priv, ecdh_hash_compressed, NULL) == 1;
}

/* =========================================================================
 * ECDSA sign (recoverable)
 * ========================================================================= */

bool secp256k1_wrap_sign(uint8_t sig[64], int *recid,
                         const uint8_t hash[32],
                         const uint8_t priv[32]) {
    secp256k1_ecdsa_recoverable_signature rsig;
    if (!secp256k1_ecdsa_sign_recoverable(g_ctx, &rsig, hash, priv, NULL, NULL))
        return false;

    return secp256k1_ecdsa_recoverable_signature_serialize_compact(
        g_ctx, sig, recid, &rsig) == 1;
}

/* =========================================================================
 * ECDSA recover
 * ========================================================================= */

bool secp256k1_wrap_recover(uint8_t pub[64],
                            const uint8_t sig[64], int recid,
                            const uint8_t hash[32]) {
    secp256k1_ecdsa_recoverable_signature rsig;
    if (!secp256k1_ecdsa_recoverable_signature_parse_compact(g_ctx, &rsig, sig, recid))
        return false;

    secp256k1_pubkey pk;
    if (!secp256k1_ecdsa_recover(g_ctx, &pk, &rsig, hash))
        return false;

    uint8_t buf[65];
    size_t len = 65;
    secp256k1_ec_pubkey_serialize(g_ctx, buf, &len,
                                  &pk, SECP256K1_EC_UNCOMPRESSED);
    memcpy(pub, buf + 1, 64);
    return true;
}

/* =========================================================================
 * ECDSA verify
 * ========================================================================= */

bool secp256k1_wrap_verify(const uint8_t sig[64],
                            const uint8_t hash[32],
                            const uint8_t pub[64]) {
    secp256k1_ecdsa_signature esig;
    if (!secp256k1_ecdsa_signature_parse_compact(g_ctx, &esig, sig))
        return false;

    /* Normalize to low-S form (required by secp256k1_ecdsa_verify) */
    secp256k1_ecdsa_signature_normalize(g_ctx, &esig, &esig);

    uint8_t buf[65];
    buf[0] = 0x04;
    memcpy(buf + 1, pub, 64);

    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_parse(g_ctx, &pk, buf, 65))
        return false;

    return secp256k1_ecdsa_verify(g_ctx, &esig, hash, &pk) == 1;
}

/* =========================================================================
 * Compress / Decompress
 * ========================================================================= */

bool secp256k1_wrap_compress(uint8_t out[33], const uint8_t pub[64]) {
    uint8_t buf[65];
    buf[0] = 0x04;
    memcpy(buf + 1, pub, 64);

    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_parse(g_ctx, &pk, buf, 65))
        return false;

    size_t len = 33;
    secp256k1_ec_pubkey_serialize(g_ctx, out, &len,
                                  &pk, SECP256K1_EC_COMPRESSED);
    return true;
}

bool secp256k1_wrap_decompress(uint8_t pub[64], const uint8_t comp[33]) {
    secp256k1_pubkey pk;
    if (!secp256k1_ec_pubkey_parse(g_ctx, &pk, comp, 33))
        return false;

    uint8_t buf[65];
    size_t len = 65;
    secp256k1_ec_pubkey_serialize(g_ctx, buf, &len,
                                  &pk, SECP256K1_EC_UNCOMPRESSED);
    memcpy(pub, buf + 1, 64);
    return true;
}
