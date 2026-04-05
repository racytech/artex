/*
 * HKDF-SHA256 — RFC 5869 implementation.
 *
 * Uses BLST's blst_sha256() for the underlying SHA-256 hash.
 * HMAC-SHA256 is constructed per RFC 2104.
 *
 * HMAC(K, m) = H((K' xor opad) || H((K' xor ipad) || m))
 *   where K' = H(K) if |K| > block_size, else K zero-padded to block_size
 *   ipad = 0x36 repeated, opad = 0x5c repeated
 *   block_size = 64 bytes for SHA-256
 */

#include "hkdf.h"
#include <blst.h>
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Constants
 * ========================================================================= */

#define SHA256_HASH_SIZE  32
#define SHA256_BLOCK_SIZE 64

/* =========================================================================
 * HMAC-SHA256
 * ========================================================================= */

void hmac_sha256(uint8_t out[32],
                 const uint8_t *key, size_t key_len,
                 const uint8_t *msg, size_t msg_len) {

    uint8_t k_prime[SHA256_BLOCK_SIZE];

    /* Step 1: Derive K' (block-sized key) */
    if (key_len > SHA256_BLOCK_SIZE) {
        /* Key too long: K' = H(key), zero-padded */
        blst_sha256(k_prime, key, key_len);
        memset(k_prime + SHA256_HASH_SIZE, 0,
               SHA256_BLOCK_SIZE - SHA256_HASH_SIZE);
    } else {
        /* Key fits: zero-pad to block size */
        memcpy(k_prime, key, key_len);
        memset(k_prime + key_len, 0, SHA256_BLOCK_SIZE - key_len);
    }

    /* Step 2: Inner hash = H((K' xor ipad) || msg) */
    uint8_t inner_buf[SHA256_BLOCK_SIZE + SHA256_HASH_SIZE];
    /* We need to hash: (k_prime xor ipad) || msg
     * Since msg can be large, we concatenate into a temp buffer.
     * For simplicity, allocate on stack for small msgs, or use
     * a two-pass approach with a temp buffer for the padded key. */

    /* Build ipad key block */
    uint8_t ipad_key[SHA256_BLOCK_SIZE];
    for (size_t i = 0; i < SHA256_BLOCK_SIZE; i++)
        ipad_key[i] = k_prime[i] ^ 0x36;

    /* Inner hash: H(ipad_key || msg)
     * We need streaming SHA256 for this. Since blst_sha256 is single-shot,
     * we concatenate into a temporary buffer. */
    size_t inner_len = SHA256_BLOCK_SIZE + msg_len;
    uint8_t inner_stack[SHA256_BLOCK_SIZE + 256];
    uint8_t *inner_heap = NULL;
    uint8_t *inner;
    if (inner_len <= sizeof(inner_stack)) {
        inner = inner_stack;
    } else {
        inner_heap = (uint8_t *)malloc(inner_len);
        inner = inner_heap;
    }

    memcpy(inner, ipad_key, SHA256_BLOCK_SIZE);
    if (msg_len > 0 && msg)
        memcpy(inner + SHA256_BLOCK_SIZE, msg, msg_len);

    uint8_t inner_hash[SHA256_HASH_SIZE];
    blst_sha256(inner_hash, inner, inner_len);

    if (inner_heap) free(inner_heap);

    /* Step 3: Outer hash = H((K' xor opad) || inner_hash) */
    uint8_t opad_key[SHA256_BLOCK_SIZE];
    for (size_t i = 0; i < SHA256_BLOCK_SIZE; i++)
        opad_key[i] = k_prime[i] ^ 0x5c;

    memcpy(inner_buf, opad_key, SHA256_BLOCK_SIZE);
    memcpy(inner_buf + SHA256_BLOCK_SIZE, inner_hash, SHA256_HASH_SIZE);

    blst_sha256(out, inner_buf, SHA256_BLOCK_SIZE + SHA256_HASH_SIZE);
}

/* =========================================================================
 * HKDF-Extract (RFC 5869 Section 2.2)
 * ========================================================================= */

void hkdf_extract(uint8_t prk[32],
                  const uint8_t *salt, size_t salt_len,
                  const uint8_t *ikm, size_t ikm_len) {
    /* PRK = HMAC-SHA256(salt, IKM)
     * If salt is NULL, use a string of SHA256_HASH_SIZE zeros. */
    uint8_t zero_salt[SHA256_HASH_SIZE];
    if (!salt || salt_len == 0) {
        memset(zero_salt, 0, SHA256_HASH_SIZE);
        salt = zero_salt;
        salt_len = SHA256_HASH_SIZE;
    }

    hmac_sha256(prk, salt, salt_len, ikm, ikm_len);
}

/* =========================================================================
 * HKDF-Expand (RFC 5869 Section 2.3)
 * ========================================================================= */

void hkdf_expand(uint8_t *okm, size_t okm_len,
                 const uint8_t prk[32],
                 const uint8_t *info, size_t info_len) {
    /*
     * T(0) = empty
     * T(i) = HMAC-SHA256(PRK, T(i-1) || info || i)
     * OKM  = first okm_len bytes of T(1) || T(2) || ...
     *
     * Each T(i) is 32 bytes. Max iterations = ceil(okm_len / 32).
     */
    uint8_t t_prev[SHA256_HASH_SIZE];
    size_t t_prev_len = 0;  /* T(0) is empty */

    size_t offset = 0;
    uint8_t counter = 1;

    /* Buffer for HMAC input: T(i-1) || info || counter
     * Max size: 32 + info_len + 1 */
    size_t buf_cap = SHA256_HASH_SIZE + info_len + 1;
    uint8_t buf_stack[SHA256_HASH_SIZE + 256 + 1];
    uint8_t *buf = (buf_cap <= sizeof(buf_stack))
                   ? buf_stack : NULL;
    uint8_t *buf_heap = NULL;
    if (!buf) {
        buf_heap = (uint8_t *)malloc(buf_cap);
        buf = buf_heap;
    }

    while (offset < okm_len) {
        /* Build HMAC input: T(i-1) || info || counter */
        size_t pos = 0;
        if (t_prev_len > 0) {
            memcpy(buf, t_prev, t_prev_len);
            pos = t_prev_len;
        }
        if (info_len > 0 && info) {
            memcpy(buf + pos, info, info_len);
            pos += info_len;
        }
        buf[pos] = counter;
        pos++;

        /* T(i) = HMAC-SHA256(PRK, buf) */
        uint8_t t_cur[SHA256_HASH_SIZE];
        hmac_sha256(t_cur, prk, SHA256_HASH_SIZE, buf, pos);

        /* Copy to output */
        size_t remaining = okm_len - offset;
        size_t to_copy = remaining < SHA256_HASH_SIZE ? remaining : SHA256_HASH_SIZE;
        memcpy(okm + offset, t_cur, to_copy);

        /* Prepare for next iteration */
        memcpy(t_prev, t_cur, SHA256_HASH_SIZE);
        t_prev_len = SHA256_HASH_SIZE;
        offset += to_copy;
        counter++;
    }

    if (buf_heap) free(buf_heap);
}
