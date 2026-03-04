/*
 * Test: ENR — Ethereum Node Record (EIP-778)
 *
 * Tests:
 *  1. Init / set / get / sort — basic record operations
 *  2. Set v4 identity — derive pubkey + node_id from private key
 *  3. Node A test vector — node_id matches Discv5 spec
 *  4. Node B test vector — node_id matches Discv5 spec
 *  5. Sign + verify roundtrip
 *  6. Encode + decode roundtrip
 *  7. Decode preserves identity (pubkey + node_id)
 *  8. Verify fails with tampered seq
 *  9. Set IP4 — IP + UDP + TCP encoding
 * 10. Encode respects 300-byte max
 */

#include "../include/enr.h"
#include "../include/secp256k1_wrap.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    do { printf("  [%s] ", name); } while(0)

#define PASS() \
    do { printf("PASS\n"); tests_passed++; return; } while(0)

#define FAIL(msg) \
    do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)

#define ASSERT(cond, msg) \
    do { if (!(cond)) FAIL(msg); } while(0)

static void hex_to_bytes(const char *hex, uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned int b;
        sscanf(hex + 2 * i, "%02x", &b);
        out[i] = (uint8_t)b;
    }
}

static void bytes_to_hex_str(const uint8_t *data, size_t len, char *out) {
    for (size_t i = 0; i < len; i++)
        sprintf(out + 2 * i, "%02x", data[i]);
    out[2 * len] = '\0';
}

/* =========================================================================
 * Test 1: Basic set/get/sort
 * ========================================================================= */

static void test_basic_ops(void) {
    TEST("basic set/get/sort");

    enr_t enr;
    enr_init(&enr);

    /* Set some pairs (out of order) */
    const uint8_t val_v4[] = { 'v', '4' };
    ASSERT(enr_set(&enr, "id", val_v4, 2), "set id");

    uint8_t ip[] = { 127, 0, 0, 1 };
    ASSERT(enr_set(&enr, "ip", ip, 4), "set ip");

    uint8_t port[] = { 0x76, 0x5f };  /* 30303 big-endian */
    ASSERT(enr_set(&enr, "udp", port, 2), "set udp");

    /* Get */
    const uint8_t *val;
    size_t vlen;
    ASSERT(enr_get(&enr, "id", &val, &vlen), "get id");
    ASSERT(vlen == 2 && val[0] == 'v' && val[1] == '4', "id value");

    ASSERT(enr_get(&enr, "ip", &val, &vlen), "get ip");
    ASSERT(vlen == 4 && val[0] == 127, "ip value");

    ASSERT(!enr_get(&enr, "nonexistent", &val, &vlen), "nonexistent returns false");

    /* Replace */
    uint8_t ip2[] = { 10, 0, 0, 1 };
    ASSERT(enr_set(&enr, "ip", ip2, 4), "replace ip");
    ASSERT(enr_get(&enr, "ip", &val, &vlen), "get replaced ip");
    ASSERT(val[0] == 10, "replaced ip value");

    /* Sort */
    enr_sort(&enr);
    ASSERT(strcmp(enr.pairs[0].key, "id") == 0, "sorted[0] = id");
    ASSERT(strcmp(enr.pairs[1].key, "ip") == 0, "sorted[1] = ip");
    ASSERT(strcmp(enr.pairs[2].key, "udp") == 0, "sorted[2] = udp");

    PASS();
}

/* =========================================================================
 * Test 2: Set v4 identity
 * ========================================================================= */

static void test_set_v4_identity(void) {
    TEST("set v4 identity");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;

    /* Use Node A private key from Discv5 spec */
    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");
    ASSERT(enr.has_identity, "has identity flag");

    /* Check "id" = "v4" */
    const uint8_t *val;
    size_t vlen;
    ASSERT(enr_get(&enr, "id", &val, &vlen), "has id key");
    ASSERT(vlen == 2 && val[0] == 'v' && val[1] == '4', "id = v4");

    /* Check "secp256k1" = 33-byte compressed key */
    ASSERT(enr_get(&enr, "secp256k1", &val, &vlen), "has secp256k1 key");
    ASSERT(vlen == 33, "compressed key is 33 bytes");

    /* Check node_id is set */
    uint8_t nid[32];
    ASSERT(enr_node_id(&enr, nid), "node_id available");

    PASS();
}

/* =========================================================================
 * Test 3: Node A — Discv5 spec test vector
 * ========================================================================= */

static void test_node_a_vector(void) {
    TEST("Node A test vector");

    enr_t enr;
    enr_init(&enr);

    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");

    /* Expected node_id from Discv5 spec */
    uint8_t expected_nid[32];
    hex_to_bytes("aaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb", expected_nid, 32);

    uint8_t nid[32];
    ASSERT(enr_node_id(&enr, nid), "get node_id");

    if (memcmp(nid, expected_nid, 32) != 0) {
        char got[65], exp[65];
        bytes_to_hex_str(nid, 32, got);
        bytes_to_hex_str(expected_nid, 32, exp);
        printf("FAIL: node_id mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 4: Node B — Discv5 spec test vector
 * ========================================================================= */

static void test_node_b_vector(void) {
    TEST("Node B test vector");

    enr_t enr;
    enr_init(&enr);

    uint8_t priv[32];
    hex_to_bytes("66fb62bfbd66b9177a138c1e5cddbe4f7c30c343e94e68df8769459cb1cde628", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");

    /* Expected node_id from Discv5 spec */
    uint8_t expected_nid[32];
    hex_to_bytes("bbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9", expected_nid, 32);

    uint8_t nid[32];
    ASSERT(enr_node_id(&enr, nid), "get node_id");

    if (memcmp(nid, expected_nid, 32) != 0) {
        char got[65], exp[65];
        bytes_to_hex_str(nid, 32, got);
        bytes_to_hex_str(expected_nid, 32, exp);
        printf("FAIL: node_id mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 5: Sign + verify roundtrip
 * ========================================================================= */

static void test_sign_verify(void) {
    TEST("sign + verify");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;

    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");
    enr_set_ip4(&enr, 0x7f000001, 30303, 30303);  /* 127.0.0.1 */
    enr_sort(&enr);

    ASSERT(enr_sign(&enr, priv), "sign");
    ASSERT(enr.has_signature, "has signature");
    ASSERT(enr_verify(&enr), "verify");

    PASS();
}

/* =========================================================================
 * Test 6: Encode + decode roundtrip
 * ========================================================================= */

static void test_encode_decode(void) {
    TEST("encode + decode roundtrip");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 42;

    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");
    enr_set_ip4(&enr, 0xC0A80001, 9000, 9000);  /* 192.168.0.1 */
    enr_sort(&enr);
    ASSERT(enr_sign(&enr, priv), "sign");

    /* Encode */
    uint8_t buf[ENR_MAX_SIZE];
    size_t encoded_len;
    ASSERT(enr_encode(&enr, buf, &encoded_len), "encode");
    ASSERT(encoded_len > 0 && encoded_len <= ENR_MAX_SIZE, "encoded size valid");

    /* Decode */
    enr_t decoded;
    ASSERT(enr_decode(&decoded, buf, encoded_len), "decode");

    /* Compare */
    ASSERT(decoded.seq == 42, "seq matches");
    ASSERT(decoded.has_signature, "decoded has signature");
    ASSERT(memcmp(decoded.signature, enr.signature, 64) == 0, "signature matches");
    ASSERT(decoded.pair_count == enr.pair_count, "pair count matches");

    /* Check identity preserved */
    ASSERT(decoded.has_identity, "decoded has identity");
    ASSERT(memcmp(decoded.node_id, enr.node_id, 32) == 0, "node_id matches");
    ASSERT(memcmp(decoded.pubkey, enr.pubkey, 64) == 0, "pubkey matches");

    /* Check values */
    const uint8_t *val;
    size_t vlen;
    ASSERT(enr_get(&decoded, "ip", &val, &vlen), "decoded has ip");
    ASSERT(vlen == 4, "ip is 4 bytes");
    ASSERT(val[0] == 192 && val[1] == 168 && val[2] == 0 && val[3] == 1, "ip matches");

    PASS();
}

/* =========================================================================
 * Test 7: Decoded record can be verified
 * ========================================================================= */

static void test_decode_verify(void) {
    TEST("decode + verify");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;

    uint8_t priv[32];
    hex_to_bytes("66fb62bfbd66b9177a138c1e5cddbe4f7c30c343e94e68df8769459cb1cde628", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");
    enr_sort(&enr);
    ASSERT(enr_sign(&enr, priv), "sign");

    /* Encode → decode → verify */
    uint8_t buf[ENR_MAX_SIZE];
    size_t len;
    ASSERT(enr_encode(&enr, buf, &len), "encode");

    enr_t decoded;
    ASSERT(enr_decode(&decoded, buf, len), "decode");
    ASSERT(enr_verify(&decoded), "verify decoded");

    PASS();
}

/* =========================================================================
 * Test 8: Verify fails with tampered seq
 * ========================================================================= */

static void test_tamper_fails(void) {
    TEST("tampered record fails verify");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;

    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");
    enr_sort(&enr);
    ASSERT(enr_sign(&enr, priv), "sign");
    ASSERT(enr_verify(&enr), "verify before tamper");

    /* Tamper with seq */
    enr.seq = 999;
    ASSERT(!enr_verify(&enr), "verify fails after tamper");

    PASS();
}

/* =========================================================================
 * Test 9: Set IP4 encoding
 * ========================================================================= */

static void test_set_ip4(void) {
    TEST("set ip4");

    enr_t enr;
    enr_init(&enr);

    enr_set_ip4(&enr, 0x0A000001, 30303, 80);  /* 10.0.0.1, udp=30303, tcp=80 */

    const uint8_t *val;
    size_t vlen;

    /* IP: 4 bytes */
    ASSERT(enr_get(&enr, "ip", &val, &vlen), "has ip");
    ASSERT(vlen == 4, "ip len");
    ASSERT(val[0] == 10 && val[1] == 0 && val[2] == 0 && val[3] == 1, "ip bytes");

    /* UDP: 30303 = 0x765F → 2 bytes */
    ASSERT(enr_get(&enr, "udp", &val, &vlen), "has udp");
    ASSERT(vlen == 2, "udp len");
    ASSERT(val[0] == 0x76 && val[1] == 0x5F, "udp bytes");

    /* TCP: 80 = 0x50 → 1 byte */
    ASSERT(enr_get(&enr, "tcp", &val, &vlen), "has tcp");
    ASSERT(vlen == 1, "tcp len");
    ASSERT(val[0] == 80, "tcp byte");

    PASS();
}

/* =========================================================================
 * Test 10: Max size enforcement
 * ========================================================================= */

static void test_max_size(void) {
    TEST("max size enforcement");

    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;

    uint8_t priv[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", priv, 32);

    ASSERT(enr_set_v4_identity(&enr, priv), "set identity");

    /* Fill with large values to exceed 300 bytes */
    uint8_t big[256];
    memset(big, 0xAA, sizeof(big));
    enr_set(&enr, "bigdata1", big, 200);

    enr_sort(&enr);
    ASSERT(enr_sign(&enr, priv), "sign");

    uint8_t buf[512];
    size_t len;
    ASSERT(!enr_encode(&enr, buf, &len), "encode rejects oversized");

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    secp256k1_wrap_init();

    printf("ENR — Ethereum Node Record (EIP-778)\n");
    printf("=====================================\n");

    test_basic_ops();
    test_set_v4_identity();
    test_node_a_vector();
    test_node_b_vector();
    test_sign_verify();
    test_encode_decode();
    test_decode_verify();
    test_tamper_fails();
    test_set_ip4();
    test_max_size();

    printf("-------------------------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    secp256k1_wrap_destroy();
    return tests_failed > 0 ? 1 : 0;
}
