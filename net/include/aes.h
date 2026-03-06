#ifndef ART_NET_AES_H
#define ART_NET_AES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * AES-128 — CTR mode and GCM mode.
 *
 * Used by Discovery v5:
 *   - AES-128-CTR: header masking (key = dest-id[:16], IV = masking-iv)
 *   - AES-128-GCM: message encryption (key = session key, nonce = 12 bytes)
 *
 * Implementation uses AES-NI + PCLMULQDQ intrinsics (x86-64).
 */

/**
 * AES-128-CTR encrypt/decrypt (symmetric — same operation).
 *
 * XORs plaintext with AES-CTR keystream. Counter is the full 128-bit IV
 * block, incremented as big-endian integer (NIST SP 800-38A).
 *
 * @param out  Output buffer (same size as input)
 * @param in   Input data
 * @param len  Data length in bytes
 * @param key  16-byte AES key
 * @param iv   16-byte initial counter value
 */
void aes128_ctr(uint8_t *out, const uint8_t *in, size_t len,
                const uint8_t key[16], const uint8_t iv[16]);

/**
 * AES-128-GCM encrypt.
 *
 * @param ct       Output ciphertext (same length as plaintext)
 * @param tag      Output 16-byte authentication tag
 * @param pt       Plaintext
 * @param pt_len   Plaintext length
 * @param aad      Additional authenticated data (may be NULL if aad_len == 0)
 * @param aad_len  AAD length
 * @param key      16-byte AES key
 * @param nonce    12-byte nonce
 */
void aes128_gcm_encrypt(uint8_t *ct, uint8_t tag[16],
                        const uint8_t *pt, size_t pt_len,
                        const uint8_t *aad, size_t aad_len,
                        const uint8_t key[16], const uint8_t nonce[12]);

/**
 * AES-128-GCM decrypt.
 *
 * @param pt       Output plaintext (same length as ciphertext)
 * @param ct       Ciphertext
 * @param ct_len   Ciphertext length
 * @param tag      16-byte authentication tag to verify
 * @param aad      Additional authenticated data (may be NULL if aad_len == 0)
 * @param aad_len  AAD length
 * @param key      16-byte AES key
 * @param nonce    12-byte nonce
 * @return         true if tag is valid, false if authentication failed
 */
bool aes128_gcm_decrypt(uint8_t *pt,
                        const uint8_t *ct, size_t ct_len,
                        const uint8_t tag[16],
                        const uint8_t *aad, size_t aad_len,
                        const uint8_t key[16], const uint8_t nonce[12]);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_AES_H */
