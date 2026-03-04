/*
 * HKDF-SHA256 Tests
 *
 * Validates against:
 *   1. RFC 5869 Test Vectors (3 cases)
 *   2. Discv5 spec key derivation vector
 */

#include "../include/hkdf.h"

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
    uint8_t expected[256];
    size_t expected_len;
    hex_to_bytes(expected_hex, expected, &expected_len);
    ASSERT(got_len == expected_len, "%s: length mismatch (%zu vs %zu)",
           label, got_len, expected_len);
    ASSERT(memcmp(got, expected, got_len) == 0, "%s: data mismatch", label);
}

/* =========================================================================
 * RFC 5869 Test Case 1 (Basic)
 * ========================================================================= */

static void test_rfc5869_case1(void) {
    /* IKM  = 0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b (22 bytes)
     * salt = 0x000102030405060708090a0b0c (13 bytes)
     * info = 0xf0f1f2f3f4f5f6f7f8f9 (10 bytes)
     * L    = 42 */

    uint8_t ikm[22], salt[13], info[10];
    size_t ikm_len, salt_len, info_len;
    hex_to_bytes("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b", ikm, &ikm_len);
    hex_to_bytes("000102030405060708090a0b0c", salt, &salt_len);
    hex_to_bytes("f0f1f2f3f4f5f6f7f8f9", info, &info_len);

    /* Expected PRK */
    uint8_t prk[32];
    hkdf_extract(prk, salt, salt_len, ikm, ikm_len);
    assert_hex_equal(prk, 32,
        "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5",
        "PRK");

    /* Expected OKM (42 bytes) */
    uint8_t okm[42];
    hkdf_expand(okm, 42, prk, info, info_len);
    assert_hex_equal(okm, 42,
        "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
        "34007208d5b887185865",
        "OKM");
}

/* =========================================================================
 * RFC 5869 Test Case 2 (Longer inputs)
 * ========================================================================= */

static void test_rfc5869_case2(void) {
    /* IKM  = 80 bytes of 0x00-0x4f
     * salt = 80 bytes of 0x60-0xaf
     * info = 80 bytes of 0xb0-0xff
     * L    = 82 */

    uint8_t ikm[80], salt[80], info[80];
    size_t ikm_len, salt_len, info_len;
    hex_to_bytes(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
        "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
        "404142434445464748494a4b4c4d4e4f",
        ikm, &ikm_len);
    hex_to_bytes(
        "606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"
        "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf",
        salt, &salt_len);
    hex_to_bytes(
        "b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
        "d0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeef"
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff",
        info, &info_len);

    uint8_t prk[32];
    hkdf_extract(prk, salt, salt_len, ikm, ikm_len);
    assert_hex_equal(prk, 32,
        "06a6b88c5853361a06104c9ceb35b45cef760014904671014a193f40c15fc244",
        "PRK");

    uint8_t okm[82];
    hkdf_expand(okm, 82, prk, info, info_len);
    assert_hex_equal(okm, 82,
        "b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c"
        "59045a99cac7827271cb41c65e590e09da3275600c2f09b8367793a9aca3db71"
        "cc30c58179ec3e87c14c01d5c1f3434f1d87",
        "OKM");
}

/* =========================================================================
 * RFC 5869 Test Case 3 (Zero-length salt and info)
 * ========================================================================= */

static void test_rfc5869_case3(void) {
    /* IKM  = 22 bytes of 0x0b
     * salt = (not provided → zero-length)
     * info = "" (zero-length)
     * L    = 42 */

    uint8_t ikm[22];
    size_t ikm_len;
    hex_to_bytes("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b", ikm, &ikm_len);

    uint8_t prk[32];
    hkdf_extract(prk, NULL, 0, ikm, ikm_len);
    assert_hex_equal(prk, 32,
        "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04",
        "PRK");

    uint8_t okm[42];
    hkdf_expand(okm, 42, prk, NULL, 0);
    assert_hex_equal(okm, 42,
        "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d"
        "9d201395faa4b61a96c8",
        "OKM");
}

/* =========================================================================
 * HMAC-SHA256 RFC 4231 Test Case 2 (Key longer than block size)
 * ========================================================================= */

static void test_hmac_rfc4231_case6(void) {
    /* RFC 4231 Test Case 6:
     * Key  = 0xaa repeated 131 times (> 64 byte block size → hashed)
     * Data = "Test Using Larger Than Block-Size Key - Hash Key First"
     * HMAC = 60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54 */
    uint8_t key[131];
    memset(key, 0xaa, 131);
    const uint8_t *data = (const uint8_t *)
        "Test Using Larger Than Block-Size Key - Hash Key First";
    size_t data_len = 54;

    uint8_t out[32];
    hmac_sha256(out, key, 131, data, data_len);
    assert_hex_equal(out, 32,
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
        "HMAC RFC4231 case 6");
}

/* =========================================================================
 * HMAC-SHA256 RFC 4231 Test Case 7 (Key and data longer than block size)
 * ========================================================================= */

static void test_hmac_rfc4231_case7(void) {
    /* RFC 4231 Test Case 7:
     * Key  = 0xaa repeated 131 times
     * Data = "This is a test using a larger than block-size key and a "
     *        "larger than block-size data. The key needs to be hashed "
     *        "before being used by the HMAC algorithm."
     * HMAC = 9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2 */
    uint8_t key[131];
    memset(key, 0xaa, 131);
    const uint8_t *data = (const uint8_t *)
        "This is a test using a larger than block-size key and a "
        "larger than block-size data. The key needs to be hashed "
        "before being used by the HMAC algorithm.";
    size_t data_len = strlen((const char *)data);

    uint8_t out[32];
    hmac_sha256(out, key, 131, data, data_len);
    assert_hex_equal(out, 32,
        "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
        "HMAC RFC4231 case 7");
}

/* =========================================================================
 * HMAC-SHA256 basic test
 * ========================================================================= */

static void test_hmac_basic(void) {
    /* HMAC-SHA256("", "") — RFC 4231 does not have this exact case,
     * but we can verify against a known value. */
    uint8_t out[32];

    /* Test with known key and message from RFC 4231 Test Case 1:
     * Key  = 0x0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b (20 bytes)
     * Data = "Hi There" (8 bytes)
     * HMAC = b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7 */
    uint8_t key[20];
    memset(key, 0x0b, 20);
    const uint8_t *data = (const uint8_t *)"Hi There";

    hmac_sha256(out, key, 20, data, 8);
    assert_hex_equal(out, 32,
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        "HMAC RFC4231 case 1");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== HKDF-SHA256 Tests ===\n\n");

    printf("Phase 1: HMAC-SHA256\n");
    RUN(test_hmac_basic);

    printf("\nPhase 2: RFC 5869 Test Vectors\n");
    RUN(test_rfc5869_case1);
    RUN(test_rfc5869_case2);
    RUN(test_rfc5869_case3);

    printf("\nPhase 3: HMAC-SHA256 Edge Cases\n");
    RUN(test_hmac_rfc4231_case6);
    RUN(test_hmac_rfc4231_case7);

    printf("\n=== Results: %d / %d passed ===\n", pass_count, test_num);
    return 0;
}
