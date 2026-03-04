/*
 * Test: Discv5 Packet Codec — all 4 wire test vectors from ethereum/devp2p.
 *
 * Tests:
 *  1. Key derivation — ECDH + HKDF → session keys
 *  2. ID nonce signing — SHA256-based identity proof
 *  3. Ordinary packet (flag=0) — ping message
 *  4. WHOAREYOU packet (flag=1)
 *  5. Handshake without ENR (flag=2, enr-seq matches)
 *  6. Handshake with ENR (flag=2, enr-seq=0)
 *  7. Decode ordinary — unmask + parse + decrypt
 *  8. Decode WHOAREYOU — unmask + parse
 *  9. Decode handshake — unmask + parse + decrypt
 */

#include "../include/discv5_codec.h"
#include "../include/secp256k1_wrap.h"
#include "../include/enr.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

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
 * Shared test vector constants
 * ========================================================================= */

static const char *NODE_A_KEY = "eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f";
static const char *NODE_A_ID  = "aaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb";
static const char *NODE_B_KEY __attribute__((unused)) = "66fb62bfbd66b9177a138c1e5cddbe4f7c30c343e94e68df8769459cb1cde628";
static const char *NODE_B_ID  = "bbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9";

/* =========================================================================
 * Test 1: Key derivation
 * ========================================================================= */

static void test_ecdh_compressed(void) {
    TEST("ECDH compressed");

    /* ECDH primitive test vector: separate from key derivation */
    uint8_t priv[32];
    hex_to_bytes("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736", priv, 32);

    uint8_t pub_comp[33];
    hex_to_bytes("039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231", pub_comp, 33);

    uint8_t pub[64];
    ASSERT(secp256k1_wrap_decompress(pub, pub_comp), "decompress");

    uint8_t shared[33];
    ASSERT(secp256k1_wrap_ecdh_compressed(shared, priv, pub), "ecdh");

    uint8_t expected[33];
    hex_to_bytes("033b11a2a1f214567e1537ce5e509ffd9b21373247f2a3ff6841f4976f53165e7e", expected, 33);

    if (memcmp(shared, expected, 33) != 0) {
        char got[67], exp[67];
        bytes_to_hex_str(shared, 33, got);
        bytes_to_hex_str(expected, 33, exp);
        printf("FAIL: shared secret mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

static void test_key_derivation(void) {
    TEST("key derivation");

    /* Key derivation test vector uses eph-key with Node B's pubkey */
    uint8_t eph_key[32], dest_pub_comp[33];
    hex_to_bytes("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736", eph_key, 32);
    hex_to_bytes("0317931e6e0840220642f230037d285d122bc59063221ef3226b1f403ddc69ca91", dest_pub_comp, 33);

    uint8_t dest_pub[64];
    ASSERT(secp256k1_wrap_decompress(dest_pub, dest_pub_comp), "decompress");

    uint8_t node_id_a[32], node_id_b[32];
    hex_to_bytes(NODE_A_ID, node_id_a, 32);
    hex_to_bytes(NODE_B_ID, node_id_b, 32);

    uint8_t challenge_data[63];
    hex_to_bytes("000000000000000000000000000000006469736376350001010102030405060708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000", challenge_data, 63);

    /* Derive keys using full ECDH + HKDF */
    discv5_keys_t keys;
    ASSERT(discv5_derive_keys(&keys, eph_key, dest_pub,
                               challenge_data, 63,
                               node_id_a, node_id_b), "derive keys");

    uint8_t exp_init[16], exp_recv[16];
    hex_to_bytes("dccc82d81bd610f4f76d3ebe97a40571", exp_init, 16);
    hex_to_bytes("ac74bb8773749920b0d3a8881c173ec5", exp_recv, 16);

    if (memcmp(keys.initiator_key, exp_init, 16) != 0) {
        char got[33];
        bytes_to_hex_str(keys.initiator_key, 16, got);
        printf("FAIL: initiator_key mismatch: %s\n", got);
        tests_failed++;
        return;
    }

    ASSERT(memcmp(keys.recipient_key, exp_recv, 16) == 0, "recipient_key");

    PASS();
}

/* =========================================================================
 * Test 2: ID nonce signing
 * ========================================================================= */

static void test_id_nonce_signing(void) {
    TEST("ID nonce signing");

    uint8_t static_key[32];
    hex_to_bytes("fb757dc581730490a1d7a00deea65e9b1936924caaea8f44d476014856b68736", static_key, 32);

    uint8_t challenge_data[63];
    hex_to_bytes("000000000000000000000000000000006469736376350001010102030405060708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000", challenge_data, 63);

    uint8_t eph_pubkey[33];
    hex_to_bytes("039961e4c2356d61bedb83052c115d311acb3a96f5777296dcf297351130266231", eph_pubkey, 33);

    uint8_t node_id_b[32];
    hex_to_bytes(NODE_B_ID, node_id_b, 32);

    uint8_t sig[64];
    ASSERT(discv5_sign_id_nonce(sig, challenge_data, 63, eph_pubkey, node_id_b, static_key), "sign");

    uint8_t expected_sig[64];
    hex_to_bytes("94852a1e2318c4e5e9d422c98eaf19d1d90d876b29cd06ca7cb7546d0fff7b484fe86c09a064fe72bdbef73ba8e9c34df0cd2b53e9d65528c2c7f336d5dfc6e6", expected_sig, 64);

    if (memcmp(sig, expected_sig, 64) != 0) {
        char got[129], exp[129];
        bytes_to_hex_str(sig, 64, got);
        bytes_to_hex_str(expected_sig, 64, exp);
        printf("FAIL: sig mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 3: Ordinary packet encoding (flag=0)
 * ========================================================================= */

static void test_encode_ordinary(void) {
    TEST("encode ordinary packet");

    uint8_t masking_iv[16], src_id[32], dest_id[32], nonce[12], session_key[16];
    memset(masking_iv, 0, 16);
    hex_to_bytes(NODE_A_ID, src_id, 32);
    hex_to_bytes(NODE_B_ID, dest_id, 32);
    memset(nonce, 0xFF, 12);
    memset(session_key, 0, 16);

    /* PING message: type=0x01, RLP([req-id=0x00000001, enr-seq=2])
     * req-id = 0x00000001 (4 bytes) → RLP: 84 00 00 00 01
     * enr-seq = 2 → RLP: 02
     * List total = 5 + 1 = 6 → prefix: c6
     * plaintext = 01 c6 84 00 00 00 01 02 */
    uint8_t pt[] = { 0x01, 0xc6, 0x84, 0x00, 0x00, 0x00, 0x01, 0x02 };

    uint8_t pkt[512];
    size_t pkt_len = discv5_encode_ordinary(pkt, sizeof(pkt),
                                             masking_iv, src_id, dest_id,
                                             nonce, pt, sizeof(pt), session_key);
    ASSERT(pkt_len > 0, "encode succeeded");

    /* Expected encoded packet from spec */
    uint8_t expected[95];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d4342774649325f313964a39e55"
        "ea96c005ad52be8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
        "4c4f53245d08dab84102ed931f66d1492acb308fa1c6715b9d139b81acbdcc",
        expected, 95);

    ASSERT(pkt_len == 95, "packet length");

    if (memcmp(pkt, expected, 95) != 0) {
        /* Find first mismatch */
        for (size_t i = 0; i < 95; i++) {
            if (pkt[i] != expected[i]) {
                printf("FAIL: mismatch at byte %zu: got %02x, expected %02x\n",
                       i, pkt[i], expected[i]);
                tests_failed++;
                return;
            }
        }
    }

    PASS();
}

/* =========================================================================
 * Test 4: WHOAREYOU packet encoding (flag=1)
 * ========================================================================= */

static void test_encode_whoareyou(void) {
    TEST("encode WHOAREYOU packet");

    uint8_t masking_iv[16], dest_id[32], request_nonce[12], id_nonce[16];
    memset(masking_iv, 0, 16);
    hex_to_bytes(NODE_B_ID, dest_id, 32);
    hex_to_bytes("0102030405060708090a0b0c", request_nonce, 12);
    hex_to_bytes("0102030405060708090a0b0c0d0e0f10", id_nonce, 16);

    uint8_t pkt[512];
    size_t pkt_len = discv5_encode_whoareyou(pkt, sizeof(pkt),
                                              masking_iv, dest_id,
                                              request_nonce, id_nonce, 0);
    ASSERT(pkt_len > 0, "encode succeeded");

    uint8_t expected[63];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d434277464933a1ccc59f5967ad"
        "1d6035f15e528627dde75cd68292f9e6c27d6b66c8100a873fcbaed4e16b8d",
        expected, 63);

    ASSERT(pkt_len == 63, "packet length");

    if (memcmp(pkt, expected, 63) != 0) {
        for (size_t i = 0; i < 63; i++) {
            if (pkt[i] != expected[i]) {
                printf("FAIL: mismatch at byte %zu: got %02x, expected %02x\n",
                       i, pkt[i], expected[i]);
                tests_failed++;
                return;
            }
        }
    }

    PASS();
}

/* =========================================================================
 * Test 5: Handshake without ENR (flag=2)
 * ========================================================================= */

static void test_encode_handshake_no_enr(void) {
    TEST("encode handshake (no ENR)");

    uint8_t masking_iv[16], src_id[32], dest_id[32], nonce[12];
    memset(masking_iv, 0, 16);
    hex_to_bytes(NODE_A_ID, src_id, 32);
    hex_to_bytes(NODE_B_ID, dest_id, 32);
    memset(nonce, 0xFF, 12);

    /* Session key for this vector (whoareyou.enr-seq=1) */
    uint8_t session_key[16];
    hex_to_bytes("4f9fac6de7567d1e3b1241dffe90f662", session_key, 16);

    /* ID signature and ephemeral pubkey — need to compute these.
     * challenge-data for enr-seq=1: last 8 bytes are 0000000000000001 */
    uint8_t challenge_data[63];
    hex_to_bytes("000000000000000000000000000000006469736376350001010102030405060708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000001", challenge_data, 63);

    uint8_t eph_key[32];
    hex_to_bytes("0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6", eph_key, 32);

    /* Derive ephemeral public key */
    uint8_t eph_pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(eph_pub, eph_key), "eph pubkey");
    uint8_t eph_pubkey[33];
    ASSERT(secp256k1_wrap_compress(eph_pubkey, eph_pub), "eph compress");

    /* Verify eph-pubkey matches expected */
    uint8_t exp_eph[33];
    hex_to_bytes("039a003ba6517b473fa0cd74aefe99dadfdb34627f90fec6362df85803908f53a5", exp_eph, 33);
    ASSERT(memcmp(eph_pubkey, exp_eph, 33) == 0, "eph pubkey matches");

    /* Sign ID nonce with Node A's static key */
    uint8_t node_a_key[32], node_id_b[32];
    hex_to_bytes(NODE_A_KEY, node_a_key, 32);
    hex_to_bytes(NODE_B_ID, node_id_b, 32);

    uint8_t id_sig[64];
    ASSERT(discv5_sign_id_nonce(id_sig, challenge_data, 63, eph_pubkey,
                                 node_id_b, node_a_key), "sign id nonce");

    /* PING plaintext (same as ordinary test but enr-seq=1):
     * type=0x01, RLP([0x00000001, 1])
     * req-id = 0x00000001 → 84 00 00 00 01
     * enr-seq = 1 → 01
     * list = 5+1=6 → c6
     * plaintext = 01 c6 84 00 00 00 01 01 */
    uint8_t pt[] = { 0x01, 0xc6, 0x84, 0x00, 0x00, 0x00, 0x01, 0x01 };

    uint8_t pkt[512];
    size_t pkt_len = discv5_encode_handshake(pkt, sizeof(pkt),
                                              masking_iv, src_id, dest_id, nonce,
                                              id_sig, eph_pubkey,
                                              NULL, 0,  /* no ENR */
                                              pt, sizeof(pt), session_key);
    ASSERT(pkt_len > 0, "encode succeeded");

    /* Expected packet from spec (194 bytes) */
    uint8_t expected[194];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d4342774649305f313964a39e55"
        "ea96c005ad521d8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
        "4c4f53245d08da4bb252012b2cba3f4f374a90a75cff91f142fa9be3e0a5f3ef"
        "268ccb9065aeecfd67a999e7fdc137e062b2ec4a0eb92947f0d9a74bfbf44dfb"
        "a776b21301f8b65efd5796706adff216ab862a9186875f9494150c4ae06fa4d1"
        "f0396c93f215fa4ef524f1eadf5f0f4126b79336671cbcf7a885b1f8bd2a5d83"
        "9cf8",
        expected, 194);

    ASSERT(pkt_len == 194, "packet length");

    if (memcmp(pkt, expected, 194) != 0) {
        for (size_t i = 0; i < pkt_len; i++) {
            if (pkt[i] != expected[i]) {
                printf("FAIL: mismatch at byte %zu: got %02x, expected %02x\n",
                       i, pkt[i], expected[i]);
                tests_failed++;
                return;
            }
        }
    }

    PASS();
}

/* =========================================================================
 * Test 6: Handshake with ENR (flag=2)
 * ========================================================================= */

static void test_encode_handshake_with_enr(void) {
    TEST("encode handshake (with ENR)");

    uint8_t masking_iv[16], src_id[32], dest_id[32], nonce[12];
    memset(masking_iv, 0, 16);
    hex_to_bytes(NODE_A_ID, src_id, 32);
    hex_to_bytes(NODE_B_ID, dest_id, 32);
    memset(nonce, 0xFF, 12);

    /* Session key for this vector (whoareyou.enr-seq=0) */
    uint8_t session_key[16];
    hex_to_bytes("53b1c075f41876423154e157470c2f48", session_key, 16);

    /* Challenge data for enr-seq=0: last 8 bytes are 0000000000000000 */
    uint8_t challenge_data[63];
    hex_to_bytes("000000000000000000000000000000006469736376350001010102030405060708090a0b0c00180102030405060708090a0b0c0d0e0f100000000000000000", challenge_data, 63);

    uint8_t eph_key[32];
    hex_to_bytes("0288ef00023598499cb6c940146d050d2b1fb914198c327f76aad590bead68b6", eph_key, 32);

    uint8_t eph_pub[64];
    ASSERT(secp256k1_wrap_pubkey_create(eph_pub, eph_key), "eph pubkey");
    uint8_t eph_pubkey[33];
    ASSERT(secp256k1_wrap_compress(eph_pubkey, eph_pub), "eph compress");

    /* Sign ID nonce */
    uint8_t node_a_key[32], node_id_b[32];
    hex_to_bytes(NODE_A_KEY, node_a_key, 32);
    hex_to_bytes(NODE_B_ID, node_id_b, 32);

    uint8_t id_sig[64];
    ASSERT(discv5_sign_id_nonce(id_sig, challenge_data, 63, eph_pubkey,
                                 node_id_b, node_a_key), "sign id nonce");

    /* Build Node A ENR: seq=1, id=v4, ip=127.0.0.1, secp256k1 (no udp/tcp) */
    enr_t enr;
    enr_init(&enr);
    enr.seq = 1;
    ASSERT(enr_set_v4_identity(&enr, node_a_key), "set identity");
    uint8_t ip[] = { 0x7f, 0x00, 0x00, 0x01 };
    enr_set(&enr, "ip", ip, 4);
    enr_sort(&enr);
    ASSERT(enr_sign(&enr, node_a_key), "sign enr");

    uint8_t enr_buf[300];
    size_t enr_len;
    ASSERT(enr_encode(&enr, enr_buf, &enr_len), "encode enr");

    /* PING plaintext (enr-seq=1) */
    uint8_t pt[] = { 0x01, 0xc6, 0x84, 0x00, 0x00, 0x00, 0x01, 0x01 };

    uint8_t pkt[512];
    size_t pkt_len = discv5_encode_handshake(pkt, sizeof(pkt),
                                              masking_iv, src_id, dest_id, nonce,
                                              id_sig, eph_pubkey,
                                              enr_buf, enr_len,
                                              pt, sizeof(pt), session_key);
    ASSERT(pkt_len > 0, "encode succeeded");

    /* Expected packet from spec (321 bytes) */
    uint8_t expected[321];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d4342774649305f313964a39e55"
        "ea96c005ad539c8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
        "4c4f53245d08da4bb23698868350aaad22e3ab8dd034f548a1c43cd246be9856"
        "2fafa0a1fa86d8e7a3b95ae78cc2b988ded6a5b59eb83ad58097252188b902b2"
        "1481e30e5e285f19735796706adff216ab862a9186875f9494150c4ae06fa4d1"
        "f0396c93f215fa4ef524e0ed04c3c21e39b1868e1ca8105e585ec17315e755e6"
        "cfc4dd6cb7fd8e1a1f55e49b4b5eb024221482105346f3c82b15fdaae36a3bb1"
        "2a494683b4a3c7f2ae41306252fed84785e2bbff3b022812d0882f06978df84a"
        "80d443972213342d04b9048fc3b1d5fcb1df0f822152eced6da4d3f6df27e70e"
        "4539717307a0208cd208d65093ccab5aa596a34d7511401987662d8cf62b1394"
        "71",
        expected, 321);

    ASSERT(pkt_len == 321, "packet length");

    if (memcmp(pkt, expected, 321) != 0) {
        for (size_t i = 0; i < pkt_len; i++) {
            if (pkt[i] != expected[i]) {
                printf("FAIL: mismatch at byte %zu: got %02x, expected %02x\n",
                       i, pkt[i], expected[i]);
                tests_failed++;
                return;
            }
        }
    }

    PASS();
}

/* =========================================================================
 * Test 7: Decode ordinary packet
 * ========================================================================= */

static void test_decode_ordinary(void) {
    TEST("decode ordinary packet");

    uint8_t pkt[95];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d4342774649325f313964a39e55"
        "ea96c005ad52be8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
        "4c4f53245d08dab84102ed931f66d1492acb308fa1c6715b9d139b81acbdcc",
        pkt, 95);

    uint8_t local_id[32];
    hex_to_bytes(NODE_B_ID, local_id, 32);

    discv5_header_t hdr;
    uint8_t header_buf[256];
    ASSERT(discv5_decode_header(&hdr, header_buf, pkt, 95, local_id), "decode header");
    ASSERT(hdr.flag == DISCV5_FLAG_ORDINARY, "flag = 0");

    /* Check nonce */
    uint8_t exp_nonce[12];
    memset(exp_nonce, 0xFF, 12);
    ASSERT(memcmp(hdr.nonce, exp_nonce, 12) == 0, "nonce");

    /* Check src_id */
    uint8_t exp_src_id[32];
    hex_to_bytes(NODE_A_ID, exp_src_id, 32);
    ASSERT(memcmp(hdr.auth.ordinary.src_id, exp_src_id, 32) == 0, "src_id");

    /* Decrypt message */
    uint8_t pt[256];
    size_t pt_len;
    uint8_t session_key[16];
    memset(session_key, 0, 16);
    ASSERT(discv5_decrypt_message(pt, &pt_len, pkt, 95, &hdr, header_buf, session_key), "decrypt");
    ASSERT(pt_len == 8, "plaintext length");
    ASSERT(pt[0] == 0x01, "message type = PING");

    PASS();
}

/* =========================================================================
 * Test 8: Decode WHOAREYOU packet
 * ========================================================================= */

static void test_decode_whoareyou(void) {
    TEST("decode WHOAREYOU packet");

    uint8_t pkt[63];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d434277464933a1ccc59f5967ad"
        "1d6035f15e528627dde75cd68292f9e6c27d6b66c8100a873fcbaed4e16b8d",
        pkt, 63);

    uint8_t local_id[32];
    hex_to_bytes(NODE_B_ID, local_id, 32);

    discv5_header_t hdr;
    uint8_t header_buf[256];
    ASSERT(discv5_decode_header(&hdr, header_buf, pkt, 63, local_id), "decode header");
    ASSERT(hdr.flag == DISCV5_FLAG_WHOAREYOU, "flag = 1");

    /* Check request nonce */
    uint8_t exp_nonce[12];
    hex_to_bytes("0102030405060708090a0b0c", exp_nonce, 12);
    ASSERT(memcmp(hdr.nonce, exp_nonce, 12) == 0, "request nonce");

    /* Check id-nonce */
    uint8_t exp_id_nonce[16];
    hex_to_bytes("0102030405060708090a0b0c0d0e0f10", exp_id_nonce, 16);
    ASSERT(memcmp(hdr.auth.whoareyou.id_nonce, exp_id_nonce, 16) == 0, "id-nonce");

    /* Check enr-seq */
    ASSERT(hdr.auth.whoareyou.enr_seq == 0, "enr-seq = 0");

    PASS();
}

/* =========================================================================
 * Test 9: Decode handshake packet
 * ========================================================================= */

static void test_decode_handshake(void) {
    TEST("decode handshake packet");

    /* Handshake without ENR (test vector 3, 194 bytes) */
    uint8_t pkt[194];
    hex_to_bytes(
        "00000000000000000000000000000000088b3d4342774649305f313964a39e55"
        "ea96c005ad521d8c7560413a7008f16c9e6d2f43bbea8814a546b7409ce783d3"
        "4c4f53245d08da4bb252012b2cba3f4f374a90a75cff91f142fa9be3e0a5f3ef"
        "268ccb9065aeecfd67a999e7fdc137e062b2ec4a0eb92947f0d9a74bfbf44dfb"
        "a776b21301f8b65efd5796706adff216ab862a9186875f9494150c4ae06fa4d1"
        "f0396c93f215fa4ef524f1eadf5f0f4126b79336671cbcf7a885b1f8bd2a5d83"
        "9cf8",
        pkt, 194);

    uint8_t local_id[32];
    hex_to_bytes(NODE_B_ID, local_id, 32);

    discv5_header_t hdr;
    uint8_t header_buf[256];
    ASSERT(discv5_decode_header(&hdr, header_buf, pkt, 194, local_id), "decode header");
    ASSERT(hdr.flag == DISCV5_FLAG_HANDSHAKE, "flag = 2");
    ASSERT(hdr.auth.handshake.sig_size == 64, "sig_size = 64");
    ASSERT(hdr.auth.handshake.eph_key_size == 33, "eph_key_size = 33");
    ASSERT(hdr.auth.handshake.enr_len == 0, "no ENR");

    /* Check src_id */
    uint8_t exp_src_id[32];
    hex_to_bytes(NODE_A_ID, exp_src_id, 32);
    ASSERT(memcmp(hdr.auth.handshake.src_id, exp_src_id, 32) == 0, "src_id");

    /* Check eph_pubkey */
    uint8_t exp_eph[33];
    hex_to_bytes("039a003ba6517b473fa0cd74aefe99dadfdb34627f90fec6362df85803908f53a5", exp_eph, 33);
    ASSERT(memcmp(hdr.auth.handshake.eph_pubkey, exp_eph, 33) == 0, "eph_pubkey");

    /* Decrypt message */
    uint8_t pt[256];
    size_t pt_len;
    uint8_t session_key[16];
    hex_to_bytes("4f9fac6de7567d1e3b1241dffe90f662", session_key, 16);
    ASSERT(discv5_decrypt_message(pt, &pt_len, pkt, 194, &hdr, header_buf, session_key), "decrypt");
    ASSERT(pt_len == 8, "plaintext length");
    ASSERT(pt[0] == 0x01, "message type = PING");

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    secp256k1_wrap_init();

    printf("Discv5 Packet Codec\n");
    printf("====================\n");

    test_ecdh_compressed();
    test_key_derivation();
    test_id_nonce_signing();
    test_encode_ordinary();
    test_encode_whoareyou();
    test_encode_handshake_no_enr();
    test_encode_handshake_with_enr();
    test_decode_ordinary();
    test_decode_whoareyou();
    test_decode_handshake();

    printf("--------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    secp256k1_wrap_destroy();
    return tests_failed > 0 ? 1 : 0;
}
