/*
 * AES-128 Tests
 *
 * Validates against:
 *   1. NIST SP 800-38A — AES-128-CTR test vectors (F.5.1)
 *   2. NIST GCM Spec — AES-128-GCM test cases 1-4
 *   3. Discv5 wire spec — AES-GCM encryption vector
 *   4. GCM decrypt + tag verification
 */

#include "../include/aes.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static int test_num = 0;
static int pass_count = 0;

#define RUN(fn) do { \
    test_num++; \
    printf("  [%2d] %-55s", test_num, #fn); \
    fn(); \
    pass_count++; \
    printf("OK\n"); \
} while (0)

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("FAIL\n      %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        exit(1); \
    } \
} while (0)

static void hex_to_bytes(const char *hex, uint8_t *out, size_t *out_len) {
    size_t len = strlen(hex);
    *out_len = len / 2;
    for (size_t i = 0; i < *out_len; i++) {
        unsigned int byte;
        sscanf(hex + 2 * i, "%02x", &byte);
        out[i] = (uint8_t)byte;
    }
}

static void assert_hex_equal(const uint8_t *got, size_t got_len,
                             const char *expected_hex, const char *label) {
    uint8_t expected[512];
    size_t expected_len;
    hex_to_bytes(expected_hex, expected, &expected_len);
    ASSERT(got_len == expected_len, "%s: length mismatch (%zu vs %zu)",
           label, got_len, expected_len);
    if (memcmp(got, expected, got_len) != 0) {
        printf("FAIL\n      %s: data mismatch\n", label);
        printf("      got:      ");
        for (size_t i = 0; i < got_len; i++) printf("%02x", got[i]);
        printf("\n      expected: %s\n", expected_hex);
        exit(1);
    }
}

/* =========================================================================
 * NIST SP 800-38A — AES-128-CTR (F.5.1)
 * ========================================================================= */

static void test_ctr_nist_f51(void) {
    uint8_t key[16], iv[16];
    size_t key_len, iv_len;
    hex_to_bytes("2b7e151628aed2a6abf7158809cf4f3c", key, &key_len);
    hex_to_bytes("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff", iv, &iv_len);

    uint8_t pt[64], ct[64];
    size_t pt_len;
    hex_to_bytes(
        "6bc1bee22e409f96e93d7e117393172a"
        "ae2d8a571e03ac9c9eb76fac45af8e51"
        "30c81c46a35ce411e5fbc1191a0a52ef"
        "f69f2445df4f9b17ad2b417be66c3e10",
        pt, &pt_len);

    aes128_ctr(ct, pt, 64, key, iv);
    assert_hex_equal(ct, 64,
        "874d6191b620e3261bef6864990db6ce"
        "9806f66b7970fdff8617187bb9fffdff"
        "5ae4df3edbd5d35e5b4f09020db03eab"
        "1e031dda2fbe03d1792170a0f30095ee",
        "CTR ciphertext");
}

/* CTR decrypt (same as encrypt — symmetric) */
static void test_ctr_nist_f52_decrypt(void) {
    uint8_t key[16], iv[16];
    size_t key_len, iv_len;
    hex_to_bytes("2b7e151628aed2a6abf7158809cf4f3c", key, &key_len);
    hex_to_bytes("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff", iv, &iv_len);

    uint8_t ct[64], pt[64];
    size_t ct_len;
    hex_to_bytes(
        "874d6191b620e3261bef6864990db6ce"
        "9806f66b7970fdff8617187bb9fffdff"
        "5ae4df3edbd5d35e5b4f09020db03eab"
        "1e031dda2fbe03d1792170a0f30095ee",
        ct, &ct_len);

    aes128_ctr(pt, ct, 64, key, iv);
    assert_hex_equal(pt, 64,
        "6bc1bee22e409f96e93d7e117393172a"
        "ae2d8a571e03ac9c9eb76fac45af8e51"
        "30c81c46a35ce411e5fbc1191a0a52ef"
        "f69f2445df4f9b17ad2b417be66c3e10",
        "CTR plaintext");
}

/* Partial block CTR (17 bytes — 1 full block + 1 byte) */
static void test_ctr_partial_block(void) {
    uint8_t key[16], iv[16];
    size_t key_len, iv_len;
    hex_to_bytes("2b7e151628aed2a6abf7158809cf4f3c", key, &key_len);
    hex_to_bytes("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff", iv, &iv_len);

    /* First 17 bytes of the NIST vector */
    uint8_t pt[17], ct[17];
    size_t pt_len;
    hex_to_bytes("6bc1bee22e409f96e93d7e117393172aae", pt, &pt_len);

    aes128_ctr(ct, pt, 17, key, iv);
    assert_hex_equal(ct, 17,
        "874d6191b620e3261bef6864990db6ce98",
        "CTR partial");
}

/* =========================================================================
 * NIST GCM Spec — Test Case 1 (Empty PT, Empty AAD)
 * ========================================================================= */

static void test_gcm_nist_case1(void) {
    uint8_t key[16], nonce[12];
    size_t key_len, nonce_len;
    hex_to_bytes("00000000000000000000000000000000", key, &key_len);
    hex_to_bytes("000000000000000000000000", nonce, &nonce_len);

    uint8_t tag[16];
    aes128_gcm_encrypt(NULL, tag, NULL, 0, NULL, 0, key, nonce);
    assert_hex_equal(tag, 16,
        "58e2fccefa7e3061367f1d57a4e7455a",
        "GCM TC1 tag");
}

/* =========================================================================
 * NIST GCM Spec — Test Case 2 (16-byte PT, Empty AAD)
 * ========================================================================= */

static void test_gcm_nist_case2(void) {
    uint8_t key[16], nonce[12], pt[16];
    size_t key_len, nonce_len, pt_len;
    hex_to_bytes("00000000000000000000000000000000", key, &key_len);
    hex_to_bytes("000000000000000000000000", nonce, &nonce_len);
    hex_to_bytes("00000000000000000000000000000000", pt, &pt_len);

    uint8_t ct[16], tag[16];
    aes128_gcm_encrypt(ct, tag, pt, 16, NULL, 0, key, nonce);
    assert_hex_equal(ct, 16,
        "0388dace60b6a392f328c2b971b2fe78",
        "GCM TC2 ct");
    assert_hex_equal(tag, 16,
        "ab6e47d42cec13bdf53a67b21257bddf",
        "GCM TC2 tag");
}

/* =========================================================================
 * NIST GCM Spec — Test Case 3 (60-byte PT, Empty AAD)
 * ========================================================================= */

static void test_gcm_nist_case3(void) {
    uint8_t key[16], nonce[12], pt[64];
    size_t key_len, nonce_len, pt_len;
    hex_to_bytes("feffe9928665731c6d6a8f9467308308", key, &key_len);
    hex_to_bytes("cafebabefacedbaddecaf888", nonce, &nonce_len);
    hex_to_bytes(
        "d9313225f88406e5a55909c5aff5269a"
        "86a7a9531534f7da2e4c303d8a318a72"
        "1c3c0c95956809532fcf0e2449a6b525"
        "b16aedf5aa0de657ba637b391aafd255",
        pt, &pt_len);

    uint8_t ct[64], tag[16];
    aes128_gcm_encrypt(ct, tag, pt, pt_len, NULL, 0, key, nonce);
    assert_hex_equal(ct, pt_len,
        "42831ec2217774244b7221b784d0d49c"
        "e3aa212f2c02a4e035c17e2329aca12e"
        "21d514b25466931c7d8f6a5aac84aa05"
        "1ba30b396a0aac973d58e091473f5985",
        "GCM TC3 ct");
    assert_hex_equal(tag, 16,
        "4d5c2af327cd64a62cf35abd2ba6fab4",
        "GCM TC3 tag");
}

/* =========================================================================
 * NIST GCM Spec — Test Case 4 (60-byte PT, 20-byte AAD)
 * ========================================================================= */

static void test_gcm_nist_case4(void) {
    uint8_t key[16], nonce[12], pt[64], aad[20];
    size_t key_len, nonce_len, pt_len, aad_len;
    hex_to_bytes("feffe9928665731c6d6a8f9467308308", key, &key_len);
    hex_to_bytes("cafebabefacedbaddecaf888", nonce, &nonce_len);
    hex_to_bytes(
        "d9313225f88406e5a55909c5aff5269a"
        "86a7a9531534f7da2e4c303d8a318a72"
        "1c3c0c95956809532fcf0e2449a6b525"
        "b16aedf5aa0de657ba637b39",
        pt, &pt_len);
    hex_to_bytes("feedfacedeadbeeffeedfacedeadbeefabaddad2", aad, &aad_len);

    uint8_t ct[64], tag[16];
    aes128_gcm_encrypt(ct, tag, pt, pt_len, aad, aad_len, key, nonce);
    assert_hex_equal(ct, pt_len,
        "42831ec2217774244b7221b784d0d49c"
        "e3aa212f2c02a4e035c17e2329aca12e"
        "21d514b25466931c7d8f6a5aac84aa05"
        "1ba30b396a0aac973d58e091",
        "GCM TC4 ct");
    assert_hex_equal(tag, 16,
        "5bc94fbc3221a5db94fae95ae7121a47",
        "GCM TC4 tag");
}

/* =========================================================================
 * Discv5 Wire Spec — AES-GCM Encryption Vector
 * ========================================================================= */

static void test_gcm_discv5(void) {
    uint8_t key[16], nonce[12], pt[4], aad[32];
    size_t key_len, nonce_len, pt_len, aad_len;
    hex_to_bytes("9f2d77db7004bf8a1a85107ac686990b", key, &key_len);
    hex_to_bytes("27b5af763c446acd2749fe8e", nonce, &nonce_len);
    hex_to_bytes("01c20101", pt, &pt_len);
    hex_to_bytes(
        "93a7400fa0d6a694ebc24d5cf570f65d"
        "04215b6ac00757875e3f3a5f42107903",
        aad, &aad_len);

    uint8_t ct[4], tag[16];
    aes128_gcm_encrypt(ct, tag, pt, pt_len, aad, aad_len, key, nonce);

    /* Expected: a5d12a2d (ct) + 94b8ccb3ba55558229867dc13bfa3648 (tag) */
    assert_hex_equal(ct, 4, "a5d12a2d", "Discv5 GCM ct");
    assert_hex_equal(tag, 16, "94b8ccb3ba55558229867dc13bfa3648", "Discv5 GCM tag");
}

/* =========================================================================
 * GCM Decrypt — Verify tag and recover plaintext
 * ========================================================================= */

static void test_gcm_decrypt_valid(void) {
    /* Decrypt the Discv5 vector */
    uint8_t key[16], nonce[12], ct[4], tag[16], aad[32];
    size_t key_len, nonce_len, ct_len, tag_len, aad_len;
    hex_to_bytes("9f2d77db7004bf8a1a85107ac686990b", key, &key_len);
    hex_to_bytes("27b5af763c446acd2749fe8e", nonce, &nonce_len);
    hex_to_bytes("a5d12a2d", ct, &ct_len);
    hex_to_bytes("94b8ccb3ba55558229867dc13bfa3648", tag, &tag_len);
    hex_to_bytes(
        "93a7400fa0d6a694ebc24d5cf570f65d"
        "04215b6ac00757875e3f3a5f42107903",
        aad, &aad_len);

    uint8_t pt[4];
    bool ok = aes128_gcm_decrypt(pt, ct, ct_len, tag, aad, aad_len, key, nonce);
    ASSERT(ok, "decrypt should succeed");
    assert_hex_equal(pt, 4, "01c20101", "Discv5 GCM decrypt");
}

static void test_gcm_decrypt_tampered(void) {
    /* Same as above but flip one bit in ciphertext */
    uint8_t key[16], nonce[12], ct[4], tag[16], aad[32];
    size_t key_len, nonce_len, ct_len, tag_len, aad_len;
    hex_to_bytes("9f2d77db7004bf8a1a85107ac686990b", key, &key_len);
    hex_to_bytes("27b5af763c446acd2749fe8e", nonce, &nonce_len);
    hex_to_bytes("a5d12a2e", ct, &ct_len);  /* last byte: 2d -> 2e */
    hex_to_bytes("94b8ccb3ba55558229867dc13bfa3648", tag, &tag_len);
    hex_to_bytes(
        "93a7400fa0d6a694ebc24d5cf570f65d"
        "04215b6ac00757875e3f3a5f42107903",
        aad, &aad_len);

    uint8_t pt[4];
    bool ok = aes128_gcm_decrypt(pt, ct, ct_len, tag, aad, aad_len, key, nonce);
    ASSERT(!ok, "decrypt should fail with tampered ciphertext");
}

static void test_gcm_decrypt_bad_tag(void) {
    /* Correct ciphertext but wrong tag */
    uint8_t key[16], nonce[12], ct[4], tag[16], aad[32];
    size_t key_len, nonce_len, ct_len, tag_len, aad_len;
    hex_to_bytes("9f2d77db7004bf8a1a85107ac686990b", key, &key_len);
    hex_to_bytes("27b5af763c446acd2749fe8e", nonce, &nonce_len);
    hex_to_bytes("a5d12a2d", ct, &ct_len);
    hex_to_bytes("00000000000000000000000000000000", tag, &tag_len);
    hex_to_bytes(
        "93a7400fa0d6a694ebc24d5cf570f65d"
        "04215b6ac00757875e3f3a5f42107903",
        aad, &aad_len);

    uint8_t pt[4];
    bool ok = aes128_gcm_decrypt(pt, ct, ct_len, tag, aad, aad_len, key, nonce);
    ASSERT(!ok, "decrypt should fail with wrong tag");
}

/* =========================================================================
 * GCM Encrypt+Decrypt Roundtrip
 * ========================================================================= */

static void test_gcm_roundtrip(void) {
    uint8_t key[16] = {0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
                       0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,0x10};
    uint8_t nonce[12] = {0xaa,0xbb,0xcc,0xdd,0xee,0xff,0x11,0x22,
                         0x33,0x44,0x55,0x66};
    const char *msg = "Hello, Discovery v5!";
    size_t msg_len = strlen(msg);
    uint8_t aad[] = {0xde, 0xad, 0xbe, 0xef};

    uint8_t ct[32], tag[16], pt[32];
    aes128_gcm_encrypt(ct, tag, (const uint8_t *)msg, msg_len,
                       aad, sizeof(aad), key, nonce);

    bool ok = aes128_gcm_decrypt(pt, ct, msg_len, tag,
                                  aad, sizeof(aad), key, nonce);
    ASSERT(ok, "roundtrip decrypt should succeed");
    ASSERT(memcmp(pt, msg, msg_len) == 0, "roundtrip plaintext mismatch");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== AES-128 Tests ===\n\n");

    printf("Phase 1: AES-128-CTR (NIST SP 800-38A)\n");
    RUN(test_ctr_nist_f51);
    RUN(test_ctr_nist_f52_decrypt);
    RUN(test_ctr_partial_block);

    printf("\nPhase 2: AES-128-GCM (NIST GCM Spec)\n");
    RUN(test_gcm_nist_case1);
    RUN(test_gcm_nist_case2);
    RUN(test_gcm_nist_case3);
    RUN(test_gcm_nist_case4);

    printf("\nPhase 3: AES-128-GCM (Discv5 Wire Spec)\n");
    RUN(test_gcm_discv5);

    printf("\nPhase 4: AES-128-GCM Decrypt\n");
    RUN(test_gcm_decrypt_valid);
    RUN(test_gcm_decrypt_tampered);
    RUN(test_gcm_decrypt_bad_tag);
    RUN(test_gcm_roundtrip);

    printf("\n=== Results: %d / %d passed ===\n", pass_count, test_num);
    return 0;
}
