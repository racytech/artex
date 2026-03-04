#ifndef ART_NET_HKDF_H
#define ART_NET_HKDF_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * HKDF-SHA256 — HMAC-based Key Derivation Function (RFC 5869)
 *
 * Used by Discovery v5 for session key derivation:
 *   1. Extract: PRK = HMAC-SHA256(salt, IKM)    — from ECDH shared secret
 *   2. Expand:  OKM = HKDF-Expand(PRK, info, L) — 32 bytes → 2x16 session keys
 *
 * Built on top of BLST's blst_sha256() for the underlying SHA-256 hash.
 */

/**
 * HMAC-SHA256: keyed hash message authentication code.
 *
 * out = HMAC-SHA256(key, msg)
 *
 * @param out     Output: 32-byte MAC
 * @param key     HMAC key (any length)
 * @param key_len Key length in bytes
 * @param msg     Message to authenticate (may be NULL if msg_len == 0)
 * @param msg_len Message length in bytes
 */
void hmac_sha256(uint8_t out[32],
                 const uint8_t *key, size_t key_len,
                 const uint8_t *msg, size_t msg_len);

/**
 * HKDF-Extract: derive pseudorandom key from input keying material.
 *
 * PRK = HMAC-SHA256(salt, IKM)
 *
 * @param prk      Output: 32-byte pseudorandom key
 * @param salt     Optional salt (may be NULL for zero salt)
 * @param salt_len Salt length in bytes
 * @param ikm      Input keying material
 * @param ikm_len  IKM length in bytes
 */
void hkdf_extract(uint8_t prk[32],
                  const uint8_t *salt, size_t salt_len,
                  const uint8_t *ikm, size_t ikm_len);

/**
 * HKDF-Expand: expand pseudorandom key to output keying material.
 *
 * OKM = HKDF-Expand(PRK, info, L)
 *
 * @param okm      Output keying material (okm_len bytes)
 * @param okm_len  Desired output length (max 255 * 32 = 8160 bytes)
 * @param prk      32-byte pseudorandom key (from hkdf_extract)
 * @param info     Optional context/application info (may be NULL)
 * @param info_len Info length in bytes
 */
void hkdf_expand(uint8_t *okm, size_t okm_len,
                 const uint8_t prk[32],
                 const uint8_t *info, size_t info_len);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_HKDF_H */
