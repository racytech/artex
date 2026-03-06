/*
 * secp256k1 Wrapper Tests
 *
 * Validates:
 *   1. Key derivation (pubkey from privkey)
 *   2. Compress / decompress roundtrip
 *   3. ECDH (Discv5 test vector — raw X coordinate)
 *   4. ECDSA sign / recover roundtrip
 */

#include "../include/secp256k1_wrap.h"

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
    if (memcmp(got, expected, got_len) != 0) {
        printf("FAIL\n      %s: data mismatch\n", label);
        printf("      got:      ");
        for (size_t i = 0; i < got_len; i++) printf("%02x", got[i]);
        printf("\n      expected: %s\n", expected_hex);
        exit(1);
    }
}

/* =========================================================================
 * Pubkey from privkey
 * ========================================================================= */

static void test_pubkey_create(void) {
    /* Discv5 test vector: ephemeral key → ephemeral pubkey */
    uint8_t priv[32];
    size_t priv_len;
    hex_to_bytes(
        "0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6",
        priv, &priv_len);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(pub, priv), "pubkey_create failed");

    /* Compress and check against known compressed ephemeral-pubkey */
    uint8_t comp[33];
    ASSERT(secp256k1_wrap_compress(comp, pub), "compress failed");
    assert_hex_equal(comp, 33,
        "039a003ba6517b473fa0cd74aefe99dadfdb34627f90fec6362df85803908f53a5",
        "compressed pubkey");
}

/* =========================================================================
 * Compress / Decompress roundtrip
 * ========================================================================= */

static void test_compress_decompress(void) {
    uint8_t priv[32];
    size_t priv_len;
    hex_to_bytes(
        "0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6",
        priv, &priv_len);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(pub, priv), "pubkey_create failed");

    /* Compress */
    uint8_t comp[33];
    ASSERT(secp256k1_wrap_compress(comp, pub), "compress failed");

    /* Decompress and compare */
    uint8_t pub2[64];
    ASSERT(secp256k1_wrap_decompress(pub2, comp), "decompress failed");
    ASSERT(memcmp(pub, pub2, 64) == 0, "roundtrip mismatch");
}

/* =========================================================================
 * ECDH — Discv5 test vector
 * ========================================================================= */

static void test_ecdh_discv5(void) {
    /*
     * From Discv5 wire test vectors — ECDH section:
     * secret-key:     fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736
     * public-key:     039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231
     * shared-secret:  033b11a2a1f214567e1537ce5e509ffd9b21373247f2a3ff6841f4976f53165e7e
     *
     * shared-secret is a compressed point; raw X = last 32 bytes.
     */
    uint8_t priv[32];
    size_t priv_len;
    hex_to_bytes(
        "fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736",
        priv, &priv_len);

    uint8_t comp[33];
    size_t comp_len;
    hex_to_bytes(
        "039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231",
        comp, &comp_len);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_decompress(pub, comp), "decompress failed");

    uint8_t shared[32];
    ASSERT(secp256k1_wrap_ecdh(shared, priv, pub), "ECDH failed");

    assert_hex_equal(shared, 32,
        "3b11a2a1f214567e1537ce5e509ffd9b21373247f2a3ff6841f4976f53165e7e",
        "ECDH shared secret");
}

/* =========================================================================
 * ECDSA sign / recover roundtrip
 * ========================================================================= */

static void test_sign_recover(void) {
    uint8_t priv[32];
    size_t priv_len;
    hex_to_bytes(
        "0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6",
        priv, &priv_len);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(pub, priv), "pubkey_create failed");

    /* Sign a test hash */
    uint8_t hash[32];
    memset(hash, 0xaa, 32);

    uint8_t sig[64];
    int recid;
    ASSERT(secp256k1_wrap_sign(sig, &recid, hash, priv), "sign failed");
    ASSERT(recid >= 0 && recid <= 3, "invalid recid: %d", recid);

    /* Recover the public key */
    uint8_t recovered[64];
    ASSERT(secp256k1_wrap_recover(recovered, sig, recid, hash), "recover failed");
    ASSERT(memcmp(pub, recovered, 64) == 0, "recovered pubkey mismatch");
}

/* Different hash → different recovered key (sanity check) */
static void test_sign_wrong_hash(void) {
    uint8_t priv[32];
    size_t priv_len;
    hex_to_bytes(
        "fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736",
        priv, &priv_len);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(pub, priv), "pubkey_create failed");

    uint8_t hash1[32], hash2[32];
    memset(hash1, 0xaa, 32);
    memset(hash2, 0xbb, 32);

    uint8_t sig[64];
    int recid;
    ASSERT(secp256k1_wrap_sign(sig, &recid, hash1, priv), "sign failed");

    /* Recover with wrong hash — should give different pubkey */
    uint8_t recovered[64];
    ASSERT(secp256k1_wrap_recover(recovered, sig, recid, hash2), "recover failed");
    ASSERT(memcmp(pub, recovered, 64) != 0, "should not match with wrong hash");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== secp256k1 Wrapper Tests ===\n\n");

    ASSERT(secp256k1_wrap_init(), "init failed");

    printf("Phase 1: Key Operations\n");
    RUN(test_pubkey_create);
    RUN(test_compress_decompress);

    printf("\nPhase 2: ECDH\n");
    RUN(test_ecdh_discv5);

    printf("\nPhase 3: ECDSA Sign/Recover\n");
    RUN(test_sign_recover);
    RUN(test_sign_wrong_hash);

    secp256k1_wrap_destroy();

    printf("\n=== Results: %d / %d passed ===\n", pass_count, test_num);
    return 0;
}
