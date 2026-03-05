/*
 * Portal Wire Protocol — test suite.
 *
 * Tests all 8 message types against official spec test vectors from:
 *   - portal-wire-test-vectors.md
 *   - ping-extensions/extensions/type-0.md
 *   - ping-extensions/extensions/type-1.md
 */

#include "../include/portal_wire.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static size_t hex_decode(uint8_t *out, size_t cap, const char *hex) {
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) hex += 2;
    size_t hlen = strlen(hex);
    if (hlen % 2 != 0 || hlen / 2 > cap) return 0;
    for (size_t i = 0; i < hlen / 2; i++) {
        int hi = hex_val(hex[2 * i]);
        int lo = hex_val(hex[2 * i + 1]);
        if (hi < 0 || lo < 0) return 0;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return hlen / 2;
}

static void assert_hex_eq(const uint8_t *data, size_t len, const char *expected_hex) {
    uint8_t expected[4096];
    size_t elen = hex_decode(expected, sizeof(expected), expected_hex);
    if (len != elen) {
        fprintf(stderr, "  length mismatch: got %zu, expected %zu\n", len, elen);
        fprintf(stderr, "  got:      ");
        for (size_t i = 0; i < len && i < 64; i++) fprintf(stderr, "%02x", data[i]);
        if (len > 64) fprintf(stderr, "...");
        fprintf(stderr, "\n  expected: ");
        for (size_t i = 0; i < elen && i < 64; i++) fprintf(stderr, "%02x", expected[i]);
        if (elen > 64) fprintf(stderr, "...");
        fprintf(stderr, "\n");
        assert(len == elen);
    }
    if (memcmp(data, expected, len) != 0) {
        fprintf(stderr, "  content mismatch at byte ");
        for (size_t i = 0; i < len; i++) {
            if (data[i] != expected[i]) {
                fprintf(stderr, "%zu: got 0x%02x, expected 0x%02x\n", i, data[i], expected[i]);
                break;
            }
        }
        assert(memcmp(data, expected, len) == 0);
    }
}

/* data_radius = 2^256 - 2 (LE) */
static const uint8_t RADIUS_MAX_MINUS_1[32] = {
    0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
};

/* ENR1 and ENR2 RLP bytes (used in NODES and CONTENT ENRs test vectors) */
static const char *ENR1_HEX =
    "f875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706"
    "596dc655448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4"
    "edaa53280182696482763489736563703235366b31a103ca634cae0d49acb401"
    "d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138";

static const char *ENR2_HEX =
    "f875b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7"
    "d5a67553417d719539d6ae4b445946de4d99e680eb8063f29485b555d45b7df1"
    "6a1850130182696482763489736563703235366b31a1030e2cb74241c0c4fc8e"
    "8166f1a79a05d5b0dd95813a74b094529f317d5c39d235";

/* =========================================================================
 * Test: Ping Type 0 with client info
 * ========================================================================= */

static void test_ping_type0_with_client(void) {
    printf("  test_ping_type0_with_client... ");

    /* Encode payload first */
    uint8_t payload[256];
    uint16_t caps[] = {0, 1, 65535};
    size_t plen = portal_encode_payload_type0(payload, sizeof(payload),
        "trin/v0.1.1-b61fdc5c/linux-x86_64/rustc1.81.0",
        RADIUS_MAX_MINUS_1, caps, 3);
    assert(plen > 0);

    /* Encode PING */
    uint8_t out[512];
    size_t len = portal_encode_ping(out, sizeof(out), 1, 0, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x00010000000000000000000e000000"
        "28000000feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        "550000007472696e2f76302e312e312d62363166646335632f6c696e75782d7838365f"
        "36342f7275737463312e38312e3000000100ffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Ping Type 0 empty client info
 * ========================================================================= */

static void test_ping_type0_empty_client(void) {
    printf("  test_ping_type0_empty_client... ");

    uint8_t payload[256];
    uint16_t caps[] = {0, 1, 65535};
    size_t plen = portal_encode_payload_type0(payload, sizeof(payload),
        "", RADIUS_MAX_MINUS_1, caps, 3);
    assert(plen > 0);

    uint8_t out[256];
    size_t len = portal_encode_ping(out, sizeof(out), 1, 0, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x00010000000000000000000e000000"
        "28000000feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        "2800000000000100ffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Pong Type 0 with client info
 * ========================================================================= */

static void test_pong_type0_with_client(void) {
    printf("  test_pong_type0_with_client... ");

    uint8_t payload[256];
    uint16_t caps[] = {0, 1, 65535};
    size_t plen = portal_encode_payload_type0(payload, sizeof(payload),
        "trin/v0.1.1-b61fdc5c/linux-x86_64/rustc1.81.0",
        RADIUS_MAX_MINUS_1, caps, 3);
    assert(plen > 0);

    uint8_t out[512];
    size_t len = portal_encode_pong(out, sizeof(out), 1, 0, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x01010000000000000000000e000000"
        "28000000feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        "550000007472696e2f76302e312e312d62363166646335632f6c696e75782d7838365f"
        "36342f7275737463312e38312e3000000100ffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Pong Type 0 empty client info
 * ========================================================================= */

static void test_pong_type0_empty_client(void) {
    printf("  test_pong_type0_empty_client... ");

    uint8_t payload[256];
    uint16_t caps[] = {0, 1, 65535};
    size_t plen = portal_encode_payload_type0(payload, sizeof(payload),
        "", RADIUS_MAX_MINUS_1, caps, 3);
    assert(plen > 0);

    uint8_t out[256];
    size_t len = portal_encode_pong(out, sizeof(out), 1, 0, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x01010000000000000000000e000000"
        "28000000feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        "2800000000000100ffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Ping Type 1
 * ========================================================================= */

static void test_ping_type1(void) {
    printf("  test_ping_type1... ");

    uint8_t payload[64];
    size_t plen = portal_encode_payload_type1(payload, sizeof(payload),
        RADIUS_MAX_MINUS_1);
    assert(plen == 32);

    uint8_t out[128];
    size_t len = portal_encode_ping(out, sizeof(out), 1, 1, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x00010000000000000001000e000000"
        "feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Pong Type 1
 * ========================================================================= */

static void test_pong_type1(void) {
    printf("  test_pong_type1... ");

    uint8_t payload[64];
    size_t plen = portal_encode_payload_type1(payload, sizeof(payload),
        RADIUS_MAX_MINUS_1);
    assert(plen == 32);

    uint8_t out[128];
    size_t len = portal_encode_pong(out, sizeof(out), 1, 1, payload, plen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x01010000000000000001000e000000"
        "feffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");

    printf("OK\n");
}

/* =========================================================================
 * Test: Find Nodes
 * ========================================================================= */

static void test_find_nodes(void) {
    printf("  test_find_nodes... ");

    uint16_t distances[] = {256, 255};
    uint8_t out[64];
    size_t len = portal_encode_find_nodes(out, sizeof(out), distances, 2);
    assert(len > 0);

    assert_hex_eq(out, len, "0x02040000000001ff00");

    printf("OK\n");
}

/* =========================================================================
 * Test: Nodes — empty
 * ========================================================================= */

static void test_nodes_empty(void) {
    printf("  test_nodes_empty... ");

    uint8_t out[64];
    size_t len = portal_encode_nodes(out, sizeof(out), 1, NULL, NULL, 0);
    assert(len > 0);

    assert_hex_eq(out, len, "0x030105000000");

    printf("OK\n");
}

/* =========================================================================
 * Test: Nodes — multiple ENRs
 * ========================================================================= */

static void test_nodes_with_enrs(void) {
    printf("  test_nodes_with_enrs... ");

    uint8_t enr1[256], enr2[256];
    size_t enr1_len = hex_decode(enr1, sizeof(enr1), ENR1_HEX);
    size_t enr2_len = hex_decode(enr2, sizeof(enr2), ENR2_HEX);
    assert(enr1_len > 0 && enr2_len > 0);

    const uint8_t *enrs[] = {enr1, enr2};
    size_t enr_lens[] = {enr1_len, enr2_len};

    uint8_t out[512];
    size_t len = portal_encode_nodes(out, sizeof(out), 1, enrs, enr_lens, 2);
    assert(len > 0);

    /* Build expected hex: msg_id + total + offset + list_offsets + enr_data */
    assert_hex_eq(out, len,
        "0x030105000000080000007f000000"
        "f875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706"
        "596dc655448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4"
        "edaa53280182696482763489736563703235366b31a103ca634cae0d49acb401"
        "d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138"
        "f875b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7"
        "d5a67553417d719539d6ae4b445946de4d99e680eb8063f29485b555d45b7df1"
        "6a1850130182696482763489736563703235366b31a1030e2cb74241c0c4fc8e"
        "8166f1a79a05d5b0dd95813a74b094529f317d5c39d235");

    printf("OK\n");
}

/* =========================================================================
 * Test: Find Content
 * ========================================================================= */

static void test_find_content(void) {
    printf("  test_find_content... ");

    uint8_t key[64];
    size_t klen = hex_decode(key, sizeof(key), "706f7274616c");

    uint8_t out[64];
    size_t len = portal_encode_find_content(out, sizeof(out), key, klen);
    assert(len > 0);

    assert_hex_eq(out, len, "0x0404000000706f7274616c");

    printf("OK\n");
}

/* =========================================================================
 * Test: Content — connection ID
 * ========================================================================= */

static void test_content_connid(void) {
    printf("  test_content_connid... ");

    uint8_t conn_id[] = {0x01, 0x02};
    uint8_t out[16];
    size_t len = portal_encode_content_connid(out, sizeof(out), conn_id);
    assert(len > 0);

    assert_hex_eq(out, len, "0x05000102");

    printf("OK\n");
}

/* =========================================================================
 * Test: Content — data payload
 * ========================================================================= */

static void test_content_data(void) {
    printf("  test_content_data... ");

    uint8_t data[64];
    size_t dlen = hex_decode(data, sizeof(data),
        "7468652063616b652069732061206c6965");

    uint8_t out[64];
    size_t len = portal_encode_content_data(out, sizeof(out), data, dlen);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x05017468652063616b652069732061206c6965");

    printf("OK\n");
}

/* =========================================================================
 * Test: Content — ENR list
 * ========================================================================= */

static void test_content_enrs(void) {
    printf("  test_content_enrs... ");

    uint8_t enr1[256], enr2[256];
    size_t enr1_len = hex_decode(enr1, sizeof(enr1), ENR1_HEX);
    size_t enr2_len = hex_decode(enr2, sizeof(enr2), ENR2_HEX);

    const uint8_t *enrs[] = {enr1, enr2};
    size_t enr_lens[] = {enr1_len, enr2_len};

    uint8_t out[512];
    size_t len = portal_encode_content_enrs(out, sizeof(out), enrs, enr_lens, 2);
    assert(len > 0);

    assert_hex_eq(out, len,
        "0x0502080000007f000000"
        "f875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706"
        "596dc655448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4"
        "edaa53280182696482763489736563703235366b31a103ca634cae0d49acb401"
        "d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138"
        "f875b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7"
        "d5a67553417d719539d6ae4b445946de4d99e680eb8063f29485b555d45b7df1"
        "6a1850130182696482763489736563703235366b31a1030e2cb74241c0c4fc8e"
        "8166f1a79a05d5b0dd95813a74b094529f317d5c39d235");

    printf("OK\n");
}

/* =========================================================================
 * Test: Offer
 * ========================================================================= */

static void test_offer(void) {
    printf("  test_offer... ");

    uint8_t key1[] = {0x01, 0x02, 0x03};
    const uint8_t *keys[] = {key1};
    size_t key_lens[] = {3};

    uint8_t out[64];
    size_t len = portal_encode_offer(out, sizeof(out), keys, key_lens, 1);
    assert(len > 0);

    assert_hex_eq(out, len, "0x060400000004000000010203");

    printf("OK\n");
}

/* =========================================================================
 * Test: Accept
 * ========================================================================= */

static void test_accept(void) {
    printf("  test_accept... ");

    uint8_t conn_id[] = {0x01, 0x02};
    uint8_t content_keys[] = {0, 1, 2, 3, 4, 5, 1, 1};

    uint8_t out[64];
    size_t len = portal_encode_accept(out, sizeof(out),
        conn_id, content_keys, 8);
    assert(len > 0);

    assert_hex_eq(out, len, "0x070102060000000001020304050101");

    printf("OK\n");
}

/* =========================================================================
 * Test: Decode roundtrip — all message types
 * ========================================================================= */

static void test_decode_roundtrip(void) {
    printf("  test_decode_roundtrip... ");

    portal_msg_t msg;
    uint8_t buf[512];
    size_t len;

    /* PING */
    {
        uint8_t payload[] = {0xAA, 0xBB, 0xCC};
        len = portal_encode_ping(buf, sizeof(buf), 42, 7, payload, 3);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_PING);
        assert(msg.ping.enr_seq == 42);
        assert(msg.ping.payload_type == 7);
        assert(msg.ping.payload_len == 3);
        assert(memcmp(msg.ping.payload, payload, 3) == 0);
    }

    /* PONG */
    {
        uint8_t payload[] = {0xDD};
        len = portal_encode_pong(buf, sizeof(buf), 100, 1, payload, 1);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_PONG);
        assert(msg.ping.enr_seq == 100);
        assert(msg.ping.payload_type == 1);
        assert(msg.ping.payload_len == 1);
        assert(msg.ping.payload[0] == 0xDD);
    }

    /* FIND_NODES */
    {
        uint16_t dists[] = {0, 128, 256};
        len = portal_encode_find_nodes(buf, sizeof(buf), dists, 3);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_FIND_NODES);
        assert(msg.find_nodes.count == 3);
        assert(msg.find_nodes.distances[0] == 0);
        assert(msg.find_nodes.distances[1] == 128);
        assert(msg.find_nodes.distances[2] == 256);
    }

    /* NODES empty */
    {
        len = portal_encode_nodes(buf, sizeof(buf), 1, NULL, NULL, 0);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_NODES);
        assert(msg.nodes.total == 1);
        assert(msg.nodes.enr_count == 0);
    }

    /* NODES with data */
    {
        uint8_t e1[] = {0x01, 0x02, 0x03};
        uint8_t e2[] = {0x04, 0x05};
        const uint8_t *enrs[] = {e1, e2};
        size_t enr_lens[] = {3, 2};
        len = portal_encode_nodes(buf, sizeof(buf), 2, enrs, enr_lens, 2);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_NODES);
        assert(msg.nodes.total == 2);
        assert(msg.nodes.enr_count == 2);
        assert(msg.nodes.enrs[0].len == 3);
        assert(memcmp(msg.nodes.enrs[0].data, e1, 3) == 0);
        assert(msg.nodes.enrs[1].len == 2);
        assert(memcmp(msg.nodes.enrs[1].data, e2, 2) == 0);
    }

    /* FIND_CONTENT */
    {
        uint8_t key[] = {0xAA, 0xBB};
        len = portal_encode_find_content(buf, sizeof(buf), key, 2);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_FIND_CONTENT);
        assert(msg.find_content.key_len == 2);
        assert(memcmp(msg.find_content.key, key, 2) == 0);
    }

    /* CONTENT — connection ID */
    {
        uint8_t cid[] = {0xAB, 0xCD};
        len = portal_encode_content_connid(buf, sizeof(buf), cid);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_CONTENT);
        assert(msg.content.selector == PORTAL_CONTENT_CONNID);
        assert(msg.content.conn_id[0] == 0xAB);
        assert(msg.content.conn_id[1] == 0xCD);
    }

    /* CONTENT — data */
    {
        uint8_t data[] = {0x11, 0x22, 0x33, 0x44};
        len = portal_encode_content_data(buf, sizeof(buf), data, 4);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_CONTENT);
        assert(msg.content.selector == PORTAL_CONTENT_DATA);
        assert(msg.content.payload.len == 4);
        assert(memcmp(msg.content.payload.data, data, 4) == 0);
    }

    /* CONTENT — ENRs */
    {
        uint8_t e1[] = {0x10, 0x20};
        uint8_t e2[] = {0x30};
        const uint8_t *enrs[] = {e1, e2};
        size_t enr_lens[] = {2, 1};
        len = portal_encode_content_enrs(buf, sizeof(buf), enrs, enr_lens, 2);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_CONTENT);
        assert(msg.content.selector == PORTAL_CONTENT_ENRS);
        assert(msg.content.enr_list.count == 2);
        assert(msg.content.enr_list.enrs[0].len == 2);
        assert(msg.content.enr_list.enrs[1].len == 1);
    }

    /* OFFER */
    {
        uint8_t k1[] = {0xAA};
        uint8_t k2[] = {0xBB, 0xCC};
        const uint8_t *keys[] = {k1, k2};
        size_t key_lens[] = {1, 2};
        len = portal_encode_offer(buf, sizeof(buf), keys, key_lens, 2);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_OFFER);
        assert(msg.offer.key_count == 2);
        assert(msg.offer.keys[0].len == 1);
        assert(msg.offer.keys[0].data[0] == 0xAA);
        assert(msg.offer.keys[1].len == 2);
    }

    /* ACCEPT */
    {
        uint8_t cid[] = {0x55, 0x66};
        uint8_t ck[] = {0, 1, 2};
        len = portal_encode_accept(buf, sizeof(buf), cid, ck, 3);
        assert(len > 0);
        assert(portal_decode(&msg, buf, len));
        assert(msg.msg_id == PORTAL_MSG_ACCEPT);
        assert(msg.accept.conn_id[0] == 0x55);
        assert(msg.accept.conn_id[1] == 0x66);
        assert(msg.accept.keys_len == 3);
        assert(msg.accept.content_keys[0] == 0);
        assert(msg.accept.content_keys[1] == 1);
        assert(msg.accept.content_keys[2] == 2);
    }

    printf("OK\n");
}

/* =========================================================================
 * Test: Decode spec test vectors from raw hex
 * ========================================================================= */

static void test_decode_spec_vectors(void) {
    printf("  test_decode_spec_vectors... ");

    portal_msg_t msg;
    uint8_t raw[512];
    size_t rlen;

    /* FIND_NODES */
    rlen = hex_decode(raw, sizeof(raw), "0x02040000000001ff00");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_FIND_NODES);
    assert(msg.find_nodes.count == 2);
    assert(msg.find_nodes.distances[0] == 256);
    assert(msg.find_nodes.distances[1] == 255);

    /* NODES empty */
    rlen = hex_decode(raw, sizeof(raw), "0x030105000000");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_NODES);
    assert(msg.nodes.total == 1);
    assert(msg.nodes.enr_count == 0);

    /* FIND_CONTENT */
    rlen = hex_decode(raw, sizeof(raw), "0x0404000000706f7274616c");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_FIND_CONTENT);
    assert(msg.find_content.key_len == 6);
    assert(memcmp(msg.find_content.key, "portal", 6) == 0);

    /* CONTENT — connection ID */
    rlen = hex_decode(raw, sizeof(raw), "0x05000102");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_CONTENT);
    assert(msg.content.selector == PORTAL_CONTENT_CONNID);
    assert(msg.content.conn_id[0] == 0x01);
    assert(msg.content.conn_id[1] == 0x02);

    /* CONTENT — data */
    rlen = hex_decode(raw, sizeof(raw),
        "0x05017468652063616b652069732061206c6965");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_CONTENT);
    assert(msg.content.selector == PORTAL_CONTENT_DATA);
    assert(msg.content.payload.len == 17);
    assert(memcmp(msg.content.payload.data, "the cake is a lie", 17) == 0);

    /* ACCEPT */
    rlen = hex_decode(raw, sizeof(raw),
        "0x070102060000000001020304050101");
    assert(portal_decode(&msg, raw, rlen));
    assert(msg.msg_id == PORTAL_MSG_ACCEPT);
    assert(msg.accept.conn_id[0] == 0x01);
    assert(msg.accept.conn_id[1] == 0x02);
    assert(msg.accept.keys_len == 8);
    assert(msg.accept.content_keys[0] == 0);  /* accept */
    assert(msg.accept.content_keys[1] == 1);  /* generic decline */
    assert(msg.accept.content_keys[2] == 2);  /* already stored */
    assert(msg.accept.content_keys[3] == 3);  /* not in radius */
    assert(msg.accept.content_keys[4] == 4);  /* rate limit */
    assert(msg.accept.content_keys[5] == 5);  /* inbound limit */

    printf("OK\n");
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("test_portal_wire (%d tests)\n", 16);

    test_ping_type0_with_client();     /* 1 */
    test_ping_type0_empty_client();    /* 2 */
    test_pong_type0_with_client();     /* 3 */
    test_pong_type0_empty_client();    /* 4 */
    test_ping_type1();                 /* 5 */
    test_pong_type1();                 /* 6 */
    test_find_nodes();                 /* 7 */
    test_nodes_empty();                /* 8 */
    test_nodes_with_enrs();            /* 9 */
    test_find_content();               /* 10 */
    test_content_connid();             /* 11 */
    test_content_data();               /* 12 */
    test_content_enrs();               /* 13 */
    test_offer();                      /* 14 */
    test_accept();                     /* 15 */
    test_decode_roundtrip();           /* 16 */
    test_decode_spec_vectors();        /* 17 */

    printf("\nAll 17 tests passed.\n");
    return 0;
}
