/*
 * AES-128 — CTR and GCM modes using AES-NI intrinsics.
 *
 * AES-128-CTR: NIST SP 800-38A, big-endian counter increment.
 * AES-128-GCM: NIST SP 800-38D, 12-byte nonce, portable GHASH.
 *
 * Requires: -maes (AES-NI for key expansion + block encrypt).
 */

#include "../include/aes.h"
#include <wmmintrin.h>   /* AES-NI: _mm_aesenc_si128, etc. */
#include <emmintrin.h>   /* SSE2: _mm_xor_si128, _mm_slli_si128, etc. */
#include <string.h>

/* =========================================================================
 * AES-128 Key Expansion (AES-NI)
 * ========================================================================= */

/* 11 round keys for AES-128 */
typedef struct {
    __m128i rk[11];
} aes128_key_t;

static inline __m128i aes128_key_assist(__m128i key, __m128i keygen) {
    keygen = _mm_shuffle_epi32(keygen, 0xff);
    key = _mm_xor_si128(key, _mm_slli_si128(key, 4));
    key = _mm_xor_si128(key, _mm_slli_si128(key, 4));
    key = _mm_xor_si128(key, _mm_slli_si128(key, 4));
    return _mm_xor_si128(key, keygen);
}

static void aes128_expand_key(aes128_key_t *ks, const uint8_t key[16]) {
    ks->rk[0]  = _mm_loadu_si128((const __m128i *)key);
    ks->rk[1]  = aes128_key_assist(ks->rk[0],  _mm_aeskeygenassist_si128(ks->rk[0],  0x01));
    ks->rk[2]  = aes128_key_assist(ks->rk[1],  _mm_aeskeygenassist_si128(ks->rk[1],  0x02));
    ks->rk[3]  = aes128_key_assist(ks->rk[2],  _mm_aeskeygenassist_si128(ks->rk[2],  0x04));
    ks->rk[4]  = aes128_key_assist(ks->rk[3],  _mm_aeskeygenassist_si128(ks->rk[3],  0x08));
    ks->rk[5]  = aes128_key_assist(ks->rk[4],  _mm_aeskeygenassist_si128(ks->rk[4],  0x10));
    ks->rk[6]  = aes128_key_assist(ks->rk[5],  _mm_aeskeygenassist_si128(ks->rk[5],  0x20));
    ks->rk[7]  = aes128_key_assist(ks->rk[6],  _mm_aeskeygenassist_si128(ks->rk[6],  0x40));
    ks->rk[8]  = aes128_key_assist(ks->rk[7],  _mm_aeskeygenassist_si128(ks->rk[7],  0x80));
    ks->rk[9]  = aes128_key_assist(ks->rk[8],  _mm_aeskeygenassist_si128(ks->rk[8],  0x1b));
    ks->rk[10] = aes128_key_assist(ks->rk[9],  _mm_aeskeygenassist_si128(ks->rk[9],  0x36));
}

/* Encrypt a single 128-bit block */
static inline __m128i aes128_encrypt_block(const aes128_key_t *ks, __m128i block) {
    block = _mm_xor_si128(block, ks->rk[0]);
    block = _mm_aesenc_si128(block, ks->rk[1]);
    block = _mm_aesenc_si128(block, ks->rk[2]);
    block = _mm_aesenc_si128(block, ks->rk[3]);
    block = _mm_aesenc_si128(block, ks->rk[4]);
    block = _mm_aesenc_si128(block, ks->rk[5]);
    block = _mm_aesenc_si128(block, ks->rk[6]);
    block = _mm_aesenc_si128(block, ks->rk[7]);
    block = _mm_aesenc_si128(block, ks->rk[8]);
    block = _mm_aesenc_si128(block, ks->rk[9]);
    block = _mm_aesenclast_si128(block, ks->rk[10]);
    return block;
}

/* =========================================================================
 * Counter Increment (big-endian, full 128-bit)
 * ========================================================================= */

/* Increment a 128-bit big-endian counter stored in __m128i.
 * Simple scalar approach: store, increment bytes right-to-left, reload. */
static inline __m128i ctr_inc(__m128i ctr) {
    uint8_t bytes[16];
    _mm_storeu_si128((__m128i *)bytes, ctr);
    for (int i = 15; i >= 0; i--) {
        if (++bytes[i] != 0) break;
    }
    return _mm_loadu_si128((const __m128i *)bytes);
}

/* =========================================================================
 * AES-128-CTR
 * ========================================================================= */

void aes128_ctr(uint8_t *out, const uint8_t *in, size_t len,
                const uint8_t key[16], const uint8_t iv[16]) {
    aes128_key_t ks;
    aes128_expand_key(&ks, key);

    __m128i ctr = _mm_loadu_si128((const __m128i *)iv);

    size_t i = 0;

    /* Full 16-byte blocks */
    for (; i + 16 <= len; i += 16) {
        __m128i keystream = aes128_encrypt_block(&ks, ctr);
        __m128i plaintext = _mm_loadu_si128((const __m128i *)(in + i));
        _mm_storeu_si128((__m128i *)(out + i), _mm_xor_si128(plaintext, keystream));
        ctr = ctr_inc(ctr);
    }

    /* Partial final block */
    if (i < len) {
        __m128i keystream = aes128_encrypt_block(&ks, ctr);
        uint8_t ks_bytes[16];
        _mm_storeu_si128((__m128i *)ks_bytes, keystream);
        for (size_t j = 0; j < len - i; j++)
            out[i + j] = in[i + j] ^ ks_bytes[j];
    }
}

/* =========================================================================
 * GHASH (GCM authentication)
 * ========================================================================= */

/*
 * GF(2^128) multiplication per NIST SP 800-38D, Section 6.3.
 * Reduction polynomial: x^128 + x^7 + x^2 + x + 1
 *
 * GCM convention: bit 0 = MSB of byte 0 (reflected ordering).
 * For each bit i of X (MSB-first within bytes): if set, Z ^= V.
 * Then V >>= 1 (byte-level right-shift); if shifted-out bit was 1, V ^= R
 * where R = 0xe1 || 0^120.
 */
static void gf128_mul_bytes(uint8_t z[16], const uint8_t x[16], const uint8_t h[16]) {
    uint8_t v[16];
    memcpy(v, h, 16);
    memset(z, 0, 16);

    for (int i = 0; i < 128; i++) {
        /* If bit i of X is set (MSB-first within each byte) */
        if ((x[i / 8] >> (7 - (i & 7))) & 1) {
            for (int j = 0; j < 16; j++)
                z[j] ^= v[j];
        }
        /* V = V >> 1 in GCM convention, with polynomial feedback */
        int lsb = v[15] & 1;
        for (int j = 15; j > 0; j--)
            v[j] = (v[j] >> 1) | (v[j - 1] << 7);
        v[0] >>= 1;
        if (lsb) v[0] ^= 0xe1;
    }
}

/* GHASH update: process data blocks.
 * ghash = (ghash XOR block_i) * H for each 16-byte block. */
static void ghash_update(uint8_t ghash[16], const uint8_t h[16],
                         const uint8_t *data, size_t len) {
    size_t i = 0;

    for (; i + 16 <= len; i += 16) {
        for (int j = 0; j < 16; j++)
            ghash[j] ^= data[i + j];
        uint8_t tmp[16];
        gf128_mul_bytes(tmp, ghash, h);
        memcpy(ghash, tmp, 16);
    }

    if (i < len) {
        uint8_t pad[16] = {0};
        memcpy(pad, data + i, len - i);
        for (int j = 0; j < 16; j++)
            ghash[j] ^= pad[j];
        uint8_t tmp[16];
        gf128_mul_bytes(tmp, ghash, h);
        memcpy(ghash, tmp, 16);
    }
}

/* =========================================================================
 * AES-128-GCM (NIST SP 800-38D, 12-byte nonce only)
 * ========================================================================= */

/*
 * GCM with 12-byte nonce:
 *   J0 = nonce || 0x00000001  (initial counter)
 *   Encrypt: CTR mode starting from inc32(J0)
 *   Tag: GHASH(H, AAD || CT || len_block) XOR E(K, J0)
 */

/* GCM counter increment: big-endian increment of the low 32 bits only. */
static inline void gcm_inc32(uint8_t ctr[16]) {
    uint32_t c = ((uint32_t)ctr[12] << 24) | ((uint32_t)ctr[13] << 16) |
                 ((uint32_t)ctr[14] << 8)  | (uint32_t)ctr[15];
    c++;
    ctr[12] = (uint8_t)(c >> 24);
    ctr[13] = (uint8_t)(c >> 16);
    ctr[14] = (uint8_t)(c >> 8);
    ctr[15] = (uint8_t)(c);
}

/* GCM CTR-mode encryption/decryption with AES-128. */
static void gcm_ctr_crypt(uint8_t *out, const uint8_t *in, size_t len,
                           const aes128_key_t *ks, uint8_t ctr[16]) {
    size_t i = 0;
    for (; i + 16 <= len; i += 16) {
        gcm_inc32(ctr);
        __m128i keystream = aes128_encrypt_block(ks,
            _mm_loadu_si128((const __m128i *)ctr));
        __m128i block = _mm_loadu_si128((const __m128i *)(in + i));
        _mm_storeu_si128((__m128i *)(out + i), _mm_xor_si128(block, keystream));
    }
    if (i < len) {
        gcm_inc32(ctr);
        __m128i keystream = aes128_encrypt_block(ks,
            _mm_loadu_si128((const __m128i *)ctr));
        uint8_t ks_bytes[16];
        _mm_storeu_si128((__m128i *)ks_bytes, keystream);
        for (size_t j = 0; j < len - i; j++)
            out[i + j] = in[i + j] ^ ks_bytes[j];
    }
}

/* Compute GCM authentication tag.
 * tag = GHASH(H, AAD, data) XOR E(K, J0) */
static void gcm_compute_tag(uint8_t tag[16],
                             const aes128_key_t *ks,
                             const uint8_t h[16],
                             const uint8_t j0[16],
                             const uint8_t *aad, size_t aad_len,
                             const uint8_t *data, size_t data_len) {
    uint8_t ghash[16] = {0};

    /* Hash AAD */
    ghash_update(ghash, h, aad, aad_len);

    /* Hash ciphertext/data */
    ghash_update(ghash, h, data, data_len);

    /* Length block: [aad_len_bits(64) || data_len_bits(64)] big-endian */
    uint64_t aad_bits  = (uint64_t)aad_len * 8;
    uint64_t data_bits = (uint64_t)data_len * 8;
    uint8_t len_block[16];
    len_block[0]  = (uint8_t)(aad_bits >> 56);
    len_block[1]  = (uint8_t)(aad_bits >> 48);
    len_block[2]  = (uint8_t)(aad_bits >> 40);
    len_block[3]  = (uint8_t)(aad_bits >> 32);
    len_block[4]  = (uint8_t)(aad_bits >> 24);
    len_block[5]  = (uint8_t)(aad_bits >> 16);
    len_block[6]  = (uint8_t)(aad_bits >> 8);
    len_block[7]  = (uint8_t)(aad_bits);
    len_block[8]  = (uint8_t)(data_bits >> 56);
    len_block[9]  = (uint8_t)(data_bits >> 48);
    len_block[10] = (uint8_t)(data_bits >> 40);
    len_block[11] = (uint8_t)(data_bits >> 32);
    len_block[12] = (uint8_t)(data_bits >> 24);
    len_block[13] = (uint8_t)(data_bits >> 16);
    len_block[14] = (uint8_t)(data_bits >> 8);
    len_block[15] = (uint8_t)(data_bits);

    for (int i = 0; i < 16; i++)
        ghash[i] ^= len_block[i];
    uint8_t tmp[16];
    gf128_mul_bytes(tmp, ghash, h);
    memcpy(ghash, tmp, 16);

    /* Tag = GHASH XOR E(K, J0) */
    __m128i e_j0 = aes128_encrypt_block(ks,
        _mm_loadu_si128((const __m128i *)j0));
    uint8_t e_j0_bytes[16];
    _mm_storeu_si128((__m128i *)e_j0_bytes, e_j0);
    for (int i = 0; i < 16; i++)
        tag[i] = ghash[i] ^ e_j0_bytes[i];
}

void aes128_gcm_encrypt(uint8_t *ct, uint8_t tag[16],
                        const uint8_t *pt, size_t pt_len,
                        const uint8_t *aad, size_t aad_len,
                        const uint8_t key[16], const uint8_t nonce[12]) {
    aes128_key_t ks;
    aes128_expand_key(&ks, key);

    /* H = E(K, 0^128) — GHASH hash key */
    uint8_t h[16];
    __m128i H = aes128_encrypt_block(&ks, _mm_setzero_si128());
    _mm_storeu_si128((__m128i *)h, H);

    /* J0 = nonce || 0x00000001 (for 12-byte nonce) */
    uint8_t j0[16] = {0};
    memcpy(j0, nonce, 12);
    j0[15] = 0x01;

    /* Encrypt: CTR mode starting from inc32(J0) */
    uint8_t ctr[16];
    memcpy(ctr, j0, 16);
    gcm_ctr_crypt(ct, pt, pt_len, &ks, ctr);

    /* Compute tag over AAD and ciphertext */
    gcm_compute_tag(tag, &ks, h, j0, aad, aad_len, ct, pt_len);
}

bool aes128_gcm_decrypt(uint8_t *pt,
                        const uint8_t *ct, size_t ct_len,
                        const uint8_t tag[16],
                        const uint8_t *aad, size_t aad_len,
                        const uint8_t key[16], const uint8_t nonce[12]) {
    aes128_key_t ks;
    aes128_expand_key(&ks, key);

    /* H = E(K, 0^128) */
    uint8_t h[16];
    __m128i H = aes128_encrypt_block(&ks, _mm_setzero_si128());
    _mm_storeu_si128((__m128i *)h, H);

    /* J0 = nonce || 0x00000001 */
    uint8_t j0[16] = {0};
    memcpy(j0, nonce, 12);
    j0[15] = 0x01;

    /* Verify tag BEFORE decryption (authenticate-then-decrypt) */
    uint8_t computed_tag[16];
    gcm_compute_tag(computed_tag, &ks, h, j0, aad, aad_len, ct, ct_len);

    /* Constant-time tag comparison */
    uint8_t diff = 0;
    for (int i = 0; i < 16; i++)
        diff |= computed_tag[i] ^ tag[i];

    if (diff != 0)
        return false;

    /* Decrypt: CTR mode starting from inc32(J0) */
    uint8_t ctr[16];
    memcpy(ctr, j0, 16);
    gcm_ctr_crypt(pt, ct, ct_len, &ks, ctr);

    return true;
}
