#ifndef ART_NET_SECP256K1_WRAP_H
#define ART_NET_SECP256K1_WRAP_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * secp256k1 wrapper — thin layer over bitcoin-core/secp256k1.
 *
 * Used by Discovery v5:
 *   - ECDH key agreement (raw X coordinate, NOT SHA256-hashed)
 *   - ECDSA sign/recover for ENR records
 *   - Public key serialization (compressed 33 bytes / uncompressed 65 bytes)
 *
 * Must call secp256k1_wrap_init() once before using any other function.
 */

/**
 * Initialize the secp256k1 context. Call once at startup.
 * @return true on success
 */
bool secp256k1_wrap_init(void);

/**
 * Destroy the secp256k1 context. Call at shutdown (optional).
 */
void secp256k1_wrap_destroy(void);

/**
 * Derive public key from private key.
 *
 * @param pub     Output: 64-byte uncompressed public key (x || y, no prefix)
 * @param priv    32-byte private key
 * @return        true on success
 */
bool secp256k1_wrap_pubkey_create(uint8_t pub[64], const uint8_t priv[32]);

/**
 * Compute ECDH shared secret (raw X coordinate).
 *
 * @param shared  Output: 32-byte shared secret (X coordinate)
 * @param priv    32-byte private key
 * @param pub     64-byte uncompressed public key (x || y, no prefix)
 * @return        true on success
 */
bool secp256k1_wrap_ecdh(uint8_t shared[32],
                         const uint8_t priv[32],
                         const uint8_t pub[64]);

/**
 * Compute ECDH shared secret (compressed point).
 *
 * Discv5 key derivation uses the 33-byte compressed representation
 * of the ECDH shared point as the HKDF salt.
 *
 * @param shared  Output: 33-byte compressed point (02/03 prefix + X)
 * @param priv    32-byte private key
 * @param pub     64-byte uncompressed public key (x || y, no prefix)
 * @return        true on success
 */
bool secp256k1_wrap_ecdh_compressed(uint8_t shared[33],
                                     const uint8_t priv[32],
                                     const uint8_t pub[64]);

/**
 * Sign a 32-byte message hash (recoverable ECDSA).
 *
 * @param sig     Output: 64-byte compact signature (r || s)
 * @param recid   Output: recovery ID (0-3)
 * @param hash    32-byte message hash
 * @param priv    32-byte private key
 * @return        true on success
 */
bool secp256k1_wrap_sign(uint8_t sig[64], int *recid,
                         const uint8_t hash[32],
                         const uint8_t priv[32]);

/**
 * Recover public key from signature.
 *
 * @param pub     Output: 64-byte uncompressed public key
 * @param sig     64-byte compact signature (r || s)
 * @param recid   Recovery ID (0-3)
 * @param hash    32-byte message hash
 * @return        true on success
 */
bool secp256k1_wrap_recover(uint8_t pub[64],
                            const uint8_t sig[64], int recid,
                            const uint8_t hash[32]);

/**
 * Verify an ECDSA signature against a known public key.
 *
 * @param sig     64-byte compact signature (r || s)
 * @param hash    32-byte message hash
 * @param pub     64-byte uncompressed public key (x || y, no prefix)
 * @return        true if signature is valid
 */
bool secp256k1_wrap_verify(const uint8_t sig[64],
                            const uint8_t hash[32],
                            const uint8_t pub[64]);

/**
 * Compress a public key (64 bytes → 33 bytes).
 *
 * @param out     Output: 33-byte compressed public key (02/03 prefix + x)
 * @param pub     64-byte uncompressed public key (x || y, no prefix)
 * @return        true on success
 */
bool secp256k1_wrap_compress(uint8_t out[33], const uint8_t pub[64]);

/**
 * Decompress a public key (33 bytes → 64 bytes).
 *
 * @param pub     Output: 64-byte uncompressed public key (x || y, no prefix)
 * @param comp    33-byte compressed public key (02/03 prefix + x)
 * @return        true on success
 */
bool secp256k1_wrap_decompress(uint8_t pub[64], const uint8_t comp[33]);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_SECP256K1_WRAP_H */
