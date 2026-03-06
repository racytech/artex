/*
 * Roundtrip tests — encode random valid data, decode, verify equality.
 *
 * Uses a deterministic xorshift64 PRNG so failures are reproducible.
 * Each codec runs N_ITER iterations with different random inputs.
 *
 * Codecs tested:
 *   1.  RLP strings (various lengths: 0, 1, 55, 56, 200)
 *   2.  RLP uint64 (edge values + random)
 *   3.  RLP nested lists
 *   4.  Portal Wire — PING/PONG
 *   5.  Portal Wire — FIND_NODES
 *   6.  Portal Wire — NODES
 *   7.  Portal Wire — FIND_CONTENT
 *   8.  Portal Wire — CONTENT (connection ID)
 *   9.  Portal Wire — CONTENT (raw data)
 *  10.  Portal Wire — OFFER
 *  11.  Portal Wire — ACCEPT
 *  12.  Discv5 — PING
 *  13.  Discv5 — PONG
 *  14.  Discv5 — FINDNODE
 *  15.  Discv5 — TALKREQ
 *  16.  Discv5 — TALKRESP
 *  17.  uTP — DATA packets (no ext)
 *  18.  uTP — DATA packets (with SACK)
 *  19.  History — content key
 *  20.  SSZ — BitList
 */

#include "../include/portal_wire.h"
#include "../include/discv5.h"
#include "../include/utp.h"
#include "../include/history.h"
#include "../include/ssz.h"
#include "../../common/include/rlp.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Test framework
 * ========================================================================= */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

/* =========================================================================
 * Deterministic PRNG (xorshift64)
 * ========================================================================= */

static uint64_t rng_state;

static void rng_seed(uint64_t seed) { rng_state = seed ? seed : 1; }

static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static uint8_t rng_u8(void)  { return (uint8_t)rng_next(); }
static uint16_t rng_u16(void) { return (uint16_t)rng_next(); }
static uint32_t rng_u32(void) { return (uint32_t)rng_next(); }
static uint64_t rng_u64(void) { return rng_next(); }

static void rng_bytes(uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++)
        out[i] = rng_u8();
}

#define N_ITER 100

/* =========================================================================
 * 1. RLP string roundtrip
 * ========================================================================= */

static void test_rlp_string_roundtrip(void) {
    TEST("RLP string roundtrip");
    rng_seed(0x1001);

    /* Test various string lengths including edge cases */
    size_t lengths[] = {0, 1, 2, 55, 56, 57, 100, 200};
    int n_lengths = sizeof(lengths) / sizeof(lengths[0]);

    for (int i = 0; i < N_ITER; i++) {
        size_t len = lengths[i % n_lengths];
        uint8_t data[256];
        rng_bytes(data, len);

        /* For single-byte strings, avoid [0x00,0x7f] colliding with
         * the single-byte RLP rule — test both ranges explicitly */
        rlp_item_t *item = rlp_string(data, len);
        ASSERT(item, "create");

        bytes_t encoded = rlp_encode(item);
        ASSERT(encoded.len > 0, "encode");

        rlp_item_t *decoded = rlp_decode(encoded.data, encoded.len);
        ASSERT(decoded, "decode");
        ASSERT(rlp_get_type(decoded) == RLP_TYPE_STRING, "type");

        const bytes_t *str = rlp_get_string(decoded);
        ASSERT(str, "get string");
        ASSERT(str->len == len, "length mismatch");
        if (len > 0)
            ASSERT(memcmp(str->data, data, len) == 0, "data mismatch");

        rlp_item_free(item);
        rlp_item_free(decoded);
        bytes_free(&encoded);
    }
    PASS();
}

/* =========================================================================
 * 2. RLP uint64 roundtrip
 * ========================================================================= */

static void test_rlp_uint64_roundtrip(void) {
    TEST("RLP uint64 roundtrip");
    rng_seed(0x1002);

    /* Edge values */
    uint64_t edges[] = {0, 1, 0x7f, 0x80, 0xff, 0x100, 0xffff,
                        0x10000, 0xffffff, 0xffffffff, UINT64_MAX};
    int n_edges = sizeof(edges) / sizeof(edges[0]);

    for (int i = 0; i < N_ITER; i++) {
        uint64_t val = (i < n_edges) ? edges[i] : rng_u64();

        rlp_item_t *item = rlp_uint64(val);
        ASSERT(item, "create");

        bytes_t encoded = rlp_encode(item);
        ASSERT(encoded.len > 0, "encode");

        rlp_item_t *decoded = rlp_decode(encoded.data, encoded.len);
        ASSERT(decoded, "decode");
        ASSERT(rlp_get_type(decoded) == RLP_TYPE_STRING, "type");

        /* Reconstruct uint64 from decoded bytes */
        const bytes_t *str = rlp_get_string(decoded);
        ASSERT(str, "get string");

        uint64_t got = 0;
        for (size_t j = 0; j < str->len; j++)
            got = (got << 8) | str->data[j];

        ASSERT(got == val, "value mismatch");

        rlp_item_free(item);
        rlp_item_free(decoded);
        bytes_free(&encoded);
    }
    PASS();
}

/* =========================================================================
 * 3. RLP nested list roundtrip
 * ========================================================================= */

static void test_rlp_list_roundtrip(void) {
    TEST("RLP nested list roundtrip");
    rng_seed(0x1003);

    for (int i = 0; i < N_ITER; i++) {
        /* Build a list with 1-5 random string items */
        rlp_item_t *list = rlp_list_new();
        ASSERT(list, "create list");

        int count = 1 + (rng_u8() % 5);
        uint8_t items_data[5][32];
        size_t items_len[5];

        for (int j = 0; j < count; j++) {
            items_len[j] = rng_u8() % 32;
            rng_bytes(items_data[j], items_len[j]);
            rlp_item_t *s = rlp_string(items_data[j], items_len[j]);
            ASSERT(s, "create string");
            rlp_list_append(list, s);
        }

        bytes_t encoded = rlp_encode(list);
        ASSERT(encoded.len > 0, "encode");

        rlp_item_t *decoded = rlp_decode(encoded.data, encoded.len);
        ASSERT(decoded, "decode");
        ASSERT(rlp_get_type(decoded) == RLP_TYPE_LIST, "type");
        ASSERT(rlp_get_list_count(decoded) == (size_t)count, "count");

        for (int j = 0; j < count; j++) {
            const rlp_item_t *child = rlp_get_list_item(decoded, j);
            ASSERT(child, "get child");
            const bytes_t *str = rlp_get_string(child);
            ASSERT(str, "get string");
            ASSERT(str->len == items_len[j], "child len");
            if (items_len[j] > 0)
                ASSERT(memcmp(str->data, items_data[j], items_len[j]) == 0,
                       "child data");
        }

        rlp_item_free(list);
        rlp_item_free(decoded);
        bytes_free(&encoded);
    }
    PASS();
}

/* =========================================================================
 * 4. Portal Wire — PING/PONG roundtrip
 * ========================================================================= */

static void test_portal_ping_roundtrip(void) {
    TEST("Portal PING roundtrip");
    rng_seed(0x2001);

    for (int i = 0; i < N_ITER; i++) {
        uint64_t enr_seq = rng_u64();
        uint8_t radius[32];
        rng_bytes(radius, 32);

        /* Encode Type 1 payload (radius only) */
        uint8_t payload[64];
        size_t plen = portal_encode_payload_type1(payload, sizeof(payload),
                                                   radius);
        ASSERT(plen > 0, "payload encode");

        uint8_t buf[256];
        size_t len = portal_encode_ping(buf, sizeof(buf), enr_seq,
                                         PORTAL_PAYLOAD_TYPE1, payload, plen);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_PING, "msg_id");
        ASSERT(msg.ping.enr_seq == enr_seq, "enr_seq");
        ASSERT(msg.ping.payload_type == PORTAL_PAYLOAD_TYPE1, "payload_type");
    }
    PASS();
}

/* =========================================================================
 * 5. Portal Wire — FIND_NODES roundtrip
 * ========================================================================= */

static void test_portal_find_nodes_roundtrip(void) {
    TEST("Portal FIND_NODES roundtrip");
    rng_seed(0x2002);

    for (int i = 0; i < N_ITER; i++) {
        size_t count = 1 + (rng_u8() % 8);
        uint16_t distances[8];
        for (size_t j = 0; j < count; j++)
            distances[j] = rng_u16() % 257;

        uint8_t buf[512];
        size_t len = portal_encode_find_nodes(buf, sizeof(buf),
                                               distances, count);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_FIND_NODES, "msg_id");
        ASSERT(msg.find_nodes.count == count, "count");
        for (size_t j = 0; j < count; j++)
            ASSERT(msg.find_nodes.distances[j] == distances[j], "distance");
    }
    PASS();
}

/* =========================================================================
 * 6. Portal Wire — NODES roundtrip
 * ========================================================================= */

static void test_portal_nodes_roundtrip(void) {
    TEST("Portal NODES roundtrip");
    rng_seed(0x2003);

    for (int i = 0; i < N_ITER; i++) {
        uint8_t total = 1 + (rng_u8() % 5);
        size_t enr_count = 1 + (rng_u8() % 3);
        uint8_t enr_data[3][64];
        size_t enr_lens[3];
        const uint8_t *enr_ptrs[3];

        for (size_t j = 0; j < enr_count; j++) {
            enr_lens[j] = 16 + (rng_u8() % 32);
            rng_bytes(enr_data[j], enr_lens[j]);
            enr_ptrs[j] = enr_data[j];
        }

        uint8_t buf[1024];
        size_t len = portal_encode_nodes(buf, sizeof(buf), total,
                                          enr_ptrs, enr_lens, enr_count);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_NODES, "msg_id");
        ASSERT(msg.nodes.total == total, "total");
        ASSERT(msg.nodes.enr_count == enr_count, "enr_count");
        for (size_t j = 0; j < enr_count; j++) {
            ASSERT(msg.nodes.enrs[j].len == enr_lens[j], "enr len");
            ASSERT(memcmp(msg.nodes.enrs[j].data, enr_data[j],
                          enr_lens[j]) == 0, "enr data");
        }
    }
    PASS();
}

/* =========================================================================
 * 7. Portal Wire — FIND_CONTENT roundtrip
 * ========================================================================= */

static void test_portal_find_content_roundtrip(void) {
    TEST("Portal FIND_CONTENT roundtrip");
    rng_seed(0x2004);

    for (int i = 0; i < N_ITER; i++) {
        size_t key_len = 1 + (rng_u8() % 32);
        uint8_t key[32];
        rng_bytes(key, key_len);

        uint8_t buf[256];
        size_t len = portal_encode_find_content(buf, sizeof(buf),
                                                 key, key_len);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_FIND_CONTENT, "msg_id");
        ASSERT(msg.find_content.key_len == key_len, "key_len");
        ASSERT(memcmp(msg.find_content.key, key, key_len) == 0, "key data");
    }
    PASS();
}

/* =========================================================================
 * 8. Portal Wire — CONTENT (connection ID) roundtrip
 * ========================================================================= */

static void test_portal_content_connid_roundtrip(void) {
    TEST("Portal CONTENT connid roundtrip");
    rng_seed(0x2005);

    for (int i = 0; i < N_ITER; i++) {
        uint8_t conn_id[2];
        rng_bytes(conn_id, 2);

        uint8_t buf[64];
        size_t len = portal_encode_content_connid(buf, sizeof(buf), conn_id);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_CONTENT, "msg_id");
        ASSERT(msg.content.selector == PORTAL_CONTENT_CONNID, "selector");
        ASSERT(msg.content.conn_id[0] == conn_id[0], "conn_id[0]");
        ASSERT(msg.content.conn_id[1] == conn_id[1], "conn_id[1]");
    }
    PASS();
}

/* =========================================================================
 * 9. Portal Wire — CONTENT (raw data) roundtrip
 * ========================================================================= */

static void test_portal_content_data_roundtrip(void) {
    TEST("Portal CONTENT data roundtrip");
    rng_seed(0x2006);

    for (int i = 0; i < N_ITER; i++) {
        size_t data_len = rng_u8() % 128;
        uint8_t data[128];
        rng_bytes(data, data_len);

        uint8_t buf[512];
        size_t len = portal_encode_content_data(buf, sizeof(buf),
                                                 data, data_len);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_CONTENT, "msg_id");
        ASSERT(msg.content.selector == PORTAL_CONTENT_DATA, "selector");
        ASSERT(msg.content.payload.len == data_len, "data_len");
        if (data_len > 0)
            ASSERT(memcmp(msg.content.payload.data, data, data_len) == 0,
                   "data");
    }
    PASS();
}

/* =========================================================================
 * 10. Portal Wire — OFFER roundtrip
 * ========================================================================= */

static void test_portal_offer_roundtrip(void) {
    TEST("Portal OFFER roundtrip");
    rng_seed(0x2007);

    for (int i = 0; i < N_ITER; i++) {
        size_t key_count = 1 + (rng_u8() % 4);
        uint8_t key_data[4][32];
        size_t key_lens[4];
        const uint8_t *key_ptrs[4];

        for (size_t j = 0; j < key_count; j++) {
            key_lens[j] = 9; /* history content key size */
            rng_bytes(key_data[j], key_lens[j]);
            key_ptrs[j] = key_data[j];
        }

        uint8_t buf[512];
        size_t len = portal_encode_offer(buf, sizeof(buf),
                                          key_ptrs, key_lens, key_count);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_OFFER, "msg_id");
        ASSERT(msg.offer.key_count == key_count, "key_count");
        for (size_t j = 0; j < key_count; j++) {
            ASSERT(msg.offer.keys[j].len == key_lens[j], "key len");
            ASSERT(memcmp(msg.offer.keys[j].data, key_data[j],
                          key_lens[j]) == 0, "key data");
        }
    }
    PASS();
}

/* =========================================================================
 * 11. Portal Wire — ACCEPT roundtrip
 * ========================================================================= */

static void test_portal_accept_roundtrip(void) {
    TEST("Portal ACCEPT roundtrip");
    rng_seed(0x2008);

    for (int i = 0; i < N_ITER; i++) {
        uint8_t conn_id[2];
        rng_bytes(conn_id, 2);

        /* Content key bitlist — 1-8 bytes */
        size_t keys_len = 1 + (rng_u8() % 8);
        uint8_t content_keys[8];
        rng_bytes(content_keys, keys_len);

        uint8_t buf[256];
        size_t len = portal_encode_accept(buf, sizeof(buf), conn_id,
                                           content_keys, keys_len);
        ASSERT(len > 0, "encode");

        portal_msg_t msg;
        ASSERT(portal_decode(&msg, buf, len), "decode");
        ASSERT(msg.msg_id == PORTAL_MSG_ACCEPT, "msg_id");
        ASSERT(msg.accept.conn_id[0] == conn_id[0], "conn_id[0]");
        ASSERT(msg.accept.conn_id[1] == conn_id[1], "conn_id[1]");
        ASSERT(msg.accept.keys_len == keys_len, "keys_len");
        ASSERT(memcmp(msg.accept.content_keys, content_keys, keys_len) == 0,
               "keys data");
    }
    PASS();
}

/* =========================================================================
 * 12. Discv5 — PING roundtrip
 * ========================================================================= */

static void test_discv5_ping_roundtrip(void) {
    TEST("Discv5 PING roundtrip");
    rng_seed(0x3001);

    for (int i = 0; i < N_ITER; i++) {
        uint32_t req_id = rng_u32() & 0xFFFF; /* keep small for RLP */
        uint64_t enr_seq = rng_u64();

        uint8_t buf[128];
        size_t len = discv5_msg_encode_ping(buf, sizeof(buf),
                                             req_id, enr_seq);
        ASSERT(len > 0, "encode");

        discv5_msg_t msg;
        ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
        ASSERT(msg.type == DISCV5_MSG_PING, "type");
        ASSERT(msg.req_id == req_id, "req_id");
        ASSERT(msg.body.ping.enr_seq == enr_seq, "enr_seq");
    }
    PASS();
}

/* =========================================================================
 * 13. Discv5 — PONG roundtrip
 * ========================================================================= */

static void test_discv5_pong_roundtrip(void) {
    TEST("Discv5 PONG roundtrip");
    rng_seed(0x3002);

    for (int i = 0; i < N_ITER; i++) {
        uint32_t req_id = rng_u32() & 0xFFFF;
        uint64_t enr_seq = rng_u64();
        uint8_t ip[4];
        rng_bytes(ip, 4);
        uint16_t port = rng_u16();

        uint8_t buf[128];
        size_t len = discv5_msg_encode_pong(buf, sizeof(buf),
                                             req_id, enr_seq, ip, port);
        ASSERT(len > 0, "encode");

        discv5_msg_t msg;
        ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
        ASSERT(msg.type == DISCV5_MSG_PONG, "type");
        ASSERT(msg.req_id == req_id, "req_id");
        ASSERT(msg.body.pong.enr_seq == enr_seq, "enr_seq");
        ASSERT(memcmp(msg.body.pong.ip, ip, 4) == 0, "ip");
        ASSERT(msg.body.pong.port == port, "port");
    }
    PASS();
}

/* =========================================================================
 * 14. Discv5 — FINDNODE roundtrip
 * ========================================================================= */

static void test_discv5_findnode_roundtrip(void) {
    TEST("Discv5 FINDNODE roundtrip");
    rng_seed(0x3003);

    for (int i = 0; i < N_ITER; i++) {
        uint32_t req_id = rng_u32() & 0xFFFF;
        size_t count = 1 + (rng_u8() % 8);
        uint16_t distances[8];
        for (size_t j = 0; j < count; j++)
            distances[j] = rng_u16() % 257;

        uint8_t buf[256];
        size_t len = discv5_msg_encode_findnode(buf, sizeof(buf),
                                                 req_id, distances, count);
        ASSERT(len > 0, "encode");

        discv5_msg_t msg;
        ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
        ASSERT(msg.type == DISCV5_MSG_FINDNODE, "type");
        ASSERT(msg.req_id == req_id, "req_id");
        ASSERT(msg.body.findnode.dist_count == count, "count");
        for (size_t j = 0; j < count; j++)
            ASSERT(msg.body.findnode.distances[j] == distances[j],
                   "distance");
    }
    PASS();
}

/* =========================================================================
 * 15. Discv5 — TALKREQ roundtrip
 * ========================================================================= */

static void test_discv5_talkreq_roundtrip(void) {
    TEST("Discv5 TALKREQ roundtrip");
    rng_seed(0x3004);

    for (int i = 0; i < N_ITER; i++) {
        uint32_t req_id = rng_u32() & 0xFFFF;
        size_t proto_len = 1 + (rng_u8() % 16);
        uint8_t protocol[16];
        rng_bytes(protocol, proto_len);

        size_t data_len = rng_u8() % 64;
        uint8_t data[64];
        rng_bytes(data, data_len);

        uint8_t buf[256];
        size_t len = discv5_msg_encode_talkreq(buf, sizeof(buf),
                                                req_id, protocol, proto_len,
                                                data, data_len);
        ASSERT(len > 0, "encode");

        discv5_msg_t msg;
        ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
        ASSERT(msg.type == DISCV5_MSG_TALKREQ, "type");
        ASSERT(msg.req_id == req_id, "req_id");
        ASSERT(msg.body.talkreq.proto_len == proto_len, "proto_len");
        ASSERT(memcmp(msg.body.talkreq.protocol, protocol, proto_len) == 0,
               "protocol");
        ASSERT(msg.body.talkreq.data_len == data_len, "data_len");
        if (data_len > 0)
            ASSERT(memcmp(msg.body.talkreq.data, data, data_len) == 0,
                   "data");
    }
    PASS();
}

/* =========================================================================
 * 16. Discv5 — TALKRESP roundtrip
 * ========================================================================= */

static void test_discv5_talkresp_roundtrip(void) {
    TEST("Discv5 TALKRESP roundtrip");
    rng_seed(0x3005);

    for (int i = 0; i < N_ITER; i++) {
        uint32_t req_id = rng_u32() & 0xFFFF;
        size_t data_len = rng_u8() % 64;
        uint8_t data[64];
        rng_bytes(data, data_len);

        uint8_t buf[256];
        size_t len = discv5_msg_encode_talkresp(buf, sizeof(buf),
                                                 req_id, data, data_len);
        ASSERT(len > 0, "encode");

        discv5_msg_t msg;
        ASSERT(discv5_msg_decode(&msg, buf, len), "decode");
        ASSERT(msg.type == DISCV5_MSG_TALKRESP, "type");
        ASSERT(msg.req_id == req_id, "req_id");
        ASSERT(msg.body.talkresp.data_len == data_len, "data_len");
        if (data_len > 0)
            ASSERT(memcmp(msg.body.talkresp.data, data, data_len) == 0,
                   "data");
    }
    PASS();
}

/* =========================================================================
 * 17. uTP — DATA packets (no extension) roundtrip
 * ========================================================================= */

static void test_utp_data_roundtrip(void) {
    TEST("uTP DATA roundtrip (no ext)");
    rng_seed(0x4001);

    for (int i = 0; i < N_ITER; i++) {
        utp_header_t hdr = {
            .type      = rng_u8() % 5,
            .version   = UTP_VERSION,
            .extension = UTP_EXT_NONE,
            .conn_id   = rng_u16(),
            .timestamp      = rng_u32(),
            .timestamp_diff = rng_u32(),
            .wnd_size  = rng_u32(),
            .seq_nr    = rng_u16(),
            .ack_nr    = rng_u16(),
        };

        size_t payload_len = rng_u8() % 64;
        uint8_t payload[64];
        rng_bytes(payload, payload_len);

        uint8_t buf[256];
        size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL,
                                payload, payload_len);
        ASSERT(len > 0, "encode");

        utp_packet_t pkt;
        ASSERT(utp_decode(&pkt, buf, len), "decode");
        ASSERT(pkt.hdr.type == hdr.type, "type");
        ASSERT(pkt.hdr.version == UTP_VERSION, "version");
        ASSERT(pkt.hdr.conn_id == hdr.conn_id, "conn_id");
        ASSERT(pkt.hdr.timestamp == hdr.timestamp, "timestamp");
        ASSERT(pkt.hdr.timestamp_diff == hdr.timestamp_diff, "ts_diff");
        ASSERT(pkt.hdr.wnd_size == hdr.wnd_size, "wnd_size");
        ASSERT(pkt.hdr.seq_nr == hdr.seq_nr, "seq_nr");
        ASSERT(pkt.hdr.ack_nr == hdr.ack_nr, "ack_nr");
        ASSERT(!pkt.has_sack, "no sack");
        ASSERT(pkt.payload_len == payload_len, "payload_len");
        if (payload_len > 0)
            ASSERT(memcmp(pkt.payload, payload, payload_len) == 0,
                   "payload");
    }
    PASS();
}

/* =========================================================================
 * 18. uTP — DATA packets (with SACK) roundtrip
 * ========================================================================= */

static void test_utp_sack_roundtrip(void) {
    TEST("uTP DATA roundtrip (with SACK)");
    rng_seed(0x4002);

    for (int i = 0; i < N_ITER; i++) {
        utp_header_t hdr = {
            .type      = UTP_ST_STATE,
            .version   = UTP_VERSION,
            .extension = UTP_EXT_SACK,
            .conn_id   = rng_u16(),
            .timestamp      = rng_u32(),
            .timestamp_diff = rng_u32(),
            .wnd_size  = rng_u32(),
            .seq_nr    = rng_u16(),
            .ack_nr    = rng_u16(),
        };

        /* SACK bitmask must be multiple of 4 bytes, 4-32 */
        utp_sack_t sack;
        sack.len = 4 * (1 + (rng_u8() % 8));
        if (sack.len > UTP_MAX_SACK_LEN) sack.len = UTP_MAX_SACK_LEN;
        rng_bytes(sack.bitmask, sack.len);

        uint8_t buf[256];
        size_t len = utp_encode(buf, sizeof(buf), &hdr, &sack, NULL, 0);
        ASSERT(len > 0, "encode");

        utp_packet_t pkt;
        ASSERT(utp_decode(&pkt, buf, len), "decode");
        ASSERT(pkt.hdr.conn_id == hdr.conn_id, "conn_id");
        ASSERT(pkt.has_sack, "has_sack");
        ASSERT(pkt.sack.len == sack.len, "sack_len");
        ASSERT(memcmp(pkt.sack.bitmask, sack.bitmask, sack.len) == 0,
               "sack data");
    }
    PASS();
}

/* =========================================================================
 * 19. History — content key roundtrip
 * ========================================================================= */

static void test_history_content_key_roundtrip(void) {
    TEST("History content key roundtrip");
    rng_seed(0x5001);

    /* Edge block numbers + random */
    uint64_t edges[] = {0, 1, 0xFFFF, 0x10000, 0x10001, 12345678, UINT64_MAX};
    int n_edges = sizeof(edges) / sizeof(edges[0]);

    for (int i = 0; i < N_ITER; i++) {
        uint64_t block = (i < n_edges) ? edges[i] : rng_u64();
        uint8_t sel = (rng_u8() & 1) ? HISTORY_SELECTOR_RECEIPTS
                                      : HISTORY_SELECTOR_BODY;

        uint8_t key[9];
        history_encode_content_key(key, sel, block);

        uint8_t dec_sel;
        uint64_t dec_block;
        ASSERT(history_decode_content_key(key, 9, &dec_sel, &dec_block),
               "decode");
        ASSERT(dec_sel == sel, "selector");
        ASSERT(dec_block == block, "block_number");

        /* Also verify content_id_from_key works without crashing */
        uint8_t id[32];
        ASSERT(history_content_id_from_key(id, key, 9), "content_id");
    }
    PASS();
}

/* =========================================================================
 * 20. SSZ — BitList roundtrip
 * ========================================================================= */

static void test_ssz_bitlist_roundtrip(void) {
    TEST("SSZ BitList roundtrip");
    rng_seed(0x6001);

    for (int i = 0; i < N_ITER; i++) {
        /* 0-64 bits */
        size_t count = rng_u8() % 65;
        bool bits[64];
        for (size_t j = 0; j < count; j++)
            bits[j] = (rng_u8() & 1);

        ssz_buf_t buf;
        ssz_buf_init(&buf, 32);
        ssz_encode_bitlist(&buf, bits, count);

        bool decoded[64];
        size_t dec_count = 0;
        ASSERT(ssz_decode_bitlist(buf.data, buf.len, decoded, &dec_count),
               "decode");
        ASSERT(dec_count == count, "count");
        for (size_t j = 0; j < count; j++)
            ASSERT(decoded[j] == bits[j], "bit mismatch");

        ssz_buf_free(&buf);
    }
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Roundtrip Tests ===\n");

    printf("\n--- RLP ---\n");
    test_rlp_string_roundtrip();
    test_rlp_uint64_roundtrip();
    test_rlp_list_roundtrip();

    printf("\n--- Portal Wire ---\n");
    test_portal_ping_roundtrip();
    test_portal_find_nodes_roundtrip();
    test_portal_nodes_roundtrip();
    test_portal_find_content_roundtrip();
    test_portal_content_connid_roundtrip();
    test_portal_content_data_roundtrip();
    test_portal_offer_roundtrip();
    test_portal_accept_roundtrip();

    printf("\n--- Discv5 Messages ---\n");
    test_discv5_ping_roundtrip();
    test_discv5_pong_roundtrip();
    test_discv5_findnode_roundtrip();
    test_discv5_talkreq_roundtrip();
    test_discv5_talkresp_roundtrip();

    printf("\n--- uTP ---\n");
    test_utp_data_roundtrip();
    test_utp_sack_roundtrip();

    printf("\n--- History ---\n");
    test_history_content_key_roundtrip();

    printf("\n--- SSZ ---\n");
    test_ssz_bitlist_roundtrip();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed ? 1 : 0;
}
