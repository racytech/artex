/*
 * Test: Discv5 Protocol Engine
 *
 * Tests:
 *  1. PING encode/decode roundtrip
 *  2. PONG encode/decode roundtrip
 *  3. FINDNODE encode/decode roundtrip
 *  4. NODES encode/decode roundtrip
 *  5. TALKREQ encode/decode roundtrip
 *  6. TALKRESP encode/decode roundtrip
 *  7. Engine create/destroy
 *  8. Engine add node
 */

#include "../include/discv5.h"
#include "../include/secp256k1_wrap.h"
#include <stdio.h>
#include <string.h>

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

/* =========================================================================
 * Test 1: PING encode/decode
 * ========================================================================= */

static void test_ping_roundtrip(void) {
    TEST("PING roundtrip");

    uint8_t buf[128];
    size_t len = discv5_msg_encode_ping(buf, sizeof(buf), 42, 100);
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x01, "type = PING");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x01, "decoded type");
    ASSERT(msg.req_id == 42, "req_id = 42");
    ASSERT(msg.body.ping.enr_seq == 100, "enr_seq = 100");

    PASS();
}

/* =========================================================================
 * Test 2: PONG encode/decode
 * ========================================================================= */

static void test_pong_roundtrip(void) {
    TEST("PONG roundtrip");

    uint8_t ip[] = { 192, 168, 1, 1 };
    uint8_t buf[128];
    size_t len = discv5_msg_encode_pong(buf, sizeof(buf), 7, 50, ip, 30303);
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x02, "type = PONG");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x02, "decoded type");
    ASSERT(msg.req_id == 7, "req_id = 7");
    ASSERT(msg.body.pong.enr_seq == 50, "enr_seq = 50");
    ASSERT(memcmp(msg.body.pong.ip, ip, 4) == 0, "ip matches");
    ASSERT(msg.body.pong.port == 30303, "port = 30303");

    PASS();
}

/* =========================================================================
 * Test 3: FINDNODE encode/decode
 * ========================================================================= */

static void test_findnode_roundtrip(void) {
    TEST("FINDNODE roundtrip");

    uint16_t dists[] = { 254, 255, 256 };
    uint8_t buf[128];
    size_t len = discv5_msg_encode_findnode(buf, sizeof(buf), 99, dists, 3);
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x03, "type = FINDNODE");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x03, "decoded type");
    ASSERT(msg.req_id == 99, "req_id = 99");
    ASSERT(msg.body.findnode.dist_count == 3, "3 distances");
    ASSERT(msg.body.findnode.distances[0] == 254, "dist[0] = 254");
    ASSERT(msg.body.findnode.distances[1] == 255, "dist[1] = 255");
    ASSERT(msg.body.findnode.distances[2] == 256, "dist[2] = 256");

    PASS();
}

/* =========================================================================
 * Test 4: NODES encode/decode
 * ========================================================================= */

static void test_nodes_roundtrip(void) {
    TEST("NODES roundtrip");

    uint8_t buf[256];
    size_t len = discv5_msg_encode_nodes(buf, sizeof(buf), 5, 2, NULL, 0);
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x04, "type = NODES");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x04, "decoded type");
    ASSERT(msg.req_id == 5, "req_id = 5");
    ASSERT(msg.body.nodes.total == 2, "total = 2");

    PASS();
}

/* =========================================================================
 * Test 5: TALKREQ encode/decode
 * ========================================================================= */

static void test_talkreq_roundtrip(void) {
    TEST("TALKREQ roundtrip");

    const char *proto = "portal";
    uint8_t data[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    uint8_t buf[256];
    size_t len = discv5_msg_encode_talkreq(buf, sizeof(buf), 123,
                                             (const uint8_t *)proto, strlen(proto),
                                             data, sizeof(data));
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x05, "type = TALKREQ");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x05, "decoded type");
    ASSERT(msg.req_id == 123, "req_id = 123");
    ASSERT(msg.body.talkreq.proto_len == 6, "protocol len = 6");
    ASSERT(memcmp(msg.body.talkreq.protocol, "portal", 6) == 0, "protocol = portal");
    ASSERT(msg.body.talkreq.data_len == 4, "data len = 4");
    ASSERT(memcmp(msg.body.talkreq.data, data, 4) == 0, "data matches");

    PASS();
}

/* =========================================================================
 * Test 6: TALKRESP encode/decode
 * ========================================================================= */

static void test_talkresp_roundtrip(void) {
    TEST("TALKRESP roundtrip");

    uint8_t data[] = { 0xCA, 0xFE };
    uint8_t buf[128];
    size_t len = discv5_msg_encode_talkresp(buf, sizeof(buf), 123, data, 2);
    ASSERT(len > 0, "encode");
    ASSERT(buf[0] == 0x06, "type = TALKRESP");

    discv5_msg_t msg;
    ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
    ASSERT(msg.type == 0x06, "decoded type");
    ASSERT(msg.req_id == 123, "req_id = 123");
    ASSERT(msg.body.talkresp.data_len == 2, "data len = 2");
    ASSERT(memcmp(msg.body.talkresp.data, data, 2) == 0, "data matches");

    PASS();
}

/* =========================================================================
 * Test 7: Engine create/destroy
 * ========================================================================= */

static void test_engine_create(void) {
    TEST("engine create/destroy");

    uint8_t privkey[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", privkey, 32);

    discv5_engine_t *e = discv5_engine_create(privkey, 0);
    ASSERT(e != NULL, "create");

    const uint8_t *id = discv5_engine_local_id(e);
    ASSERT(id != NULL, "has node_id");

    /* Verify node_id = keccak256(pubkey) */
    uint8_t expected_id[32];
    hex_to_bytes("aaaa8419e9f49d0083561b48287df592939a8d19947d8c0ef88f2a4856a69fbb", expected_id, 32);
    ASSERT(memcmp(id, expected_id, 32) == 0, "node_id matches");

    ASSERT(discv5_engine_enr_seq(e) == 1, "enr_seq = 1");

    discv5_engine_destroy(e);
    PASS();
}

/* =========================================================================
 * Test 8: Engine add node
 * ========================================================================= */

static void test_engine_add_node(void) {
    TEST("engine add node");

    uint8_t privkey[32];
    hex_to_bytes("eef77acb6c6a6eebc5b363a475ac583ec7eccdb42b6481424c60f59aa326547f", privkey, 32);

    discv5_engine_t *e = discv5_engine_create(privkey, 0);
    ASSERT(e != NULL, "create");

    /* Add Node B */
    uint8_t node_b_id[32], node_b_pub[33], ip[] = { 127, 0, 0, 1 };
    hex_to_bytes("bbbb9d047f0488c0b5a93c1c3f2d8bafc7c8ff337024a55434a0d0555de64db9", node_b_id, 32);
    hex_to_bytes("0317931e6e0840220642f230037d285d122bc59063221ef3226b1f403ddc69ca91", node_b_pub, 33);

    ASSERT(discv5_engine_add_node(e, node_b_id, node_b_pub, ip, 30303), "add node");

    discv5_engine_destroy(e);
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    secp256k1_wrap_init();

    printf("Discv5 Protocol Engine\n");
    printf("=======================\n");

    test_ping_roundtrip();
    test_pong_roundtrip();
    test_findnode_roundtrip();
    test_nodes_roundtrip();
    test_talkreq_roundtrip();
    test_talkresp_roundtrip();
    test_engine_create();
    test_engine_add_node();

    printf("-----------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    secp256k1_wrap_destroy();
    return tests_failed > 0 ? 1 : 0;
}
