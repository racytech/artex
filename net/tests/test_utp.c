/*
 * Test: uTP (micro Transport Protocol)
 *
 * Tests:
 *  1. SYN encode — spec test vector
 *  2. ACK encode (no ext) — spec test vector
 *  3. ACK encode (with SACK) — spec test vector
 *  4. DATA encode — spec test vector
 *  5. FIN encode — spec test vector
 *  6. RESET encode — spec test vector
 *  7. Decode roundtrip — all packet types
 *  8. Connection lifecycle — SYN → STATE → DATA → FIN
 *  9. Loopback transfer — write on one side, read on other
 * 10. Out-of-order delivery + SACK reassembly
 */

#include "../include/utp.h"
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

static void bytes_to_hex(const uint8_t *data, size_t len, char *out) {
    for (size_t i = 0; i < len; i++)
        sprintf(out + 2 * i, "%02x", data[i]);
    out[2 * len] = '\0';
}

/* =========================================================================
 * Test 1: SYN packet encoding
 * ========================================================================= */

static void test_encode_syn(void) {
    TEST("SYN encode");

    utp_header_t hdr = {
        .type       = UTP_ST_SYN,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_NONE,
        .conn_id    = 10049,
        .timestamp  = 3384187322,
        .timestamp_diff = 0,
        .wnd_size   = 1048576,
        .seq_nr     = 11884,
        .ack_nr     = 0,
    };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL, NULL, 0);
    ASSERT(len == 20, "length = 20");

    uint8_t expected[20];
    hex_to_bytes("41002741c9b699ba00000000001000002e6c0000", expected, 20);

    if (memcmp(buf, expected, 20) != 0) {
        char got[41], exp[41];
        bytes_to_hex(buf, 20, got);
        bytes_to_hex(expected, 20, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 2: ACK packet (no extension)
 * ========================================================================= */

static void test_encode_ack(void) {
    TEST("ACK encode (no ext)");

    utp_header_t hdr = {
        .type       = UTP_ST_STATE,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_NONE,
        .conn_id    = 10049,
        .timestamp  = 6195294,
        .timestamp_diff = 916973699,
        .wnd_size   = 1048576,
        .seq_nr     = 16807,
        .ack_nr     = 11885,
    };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL, NULL, 0);
    ASSERT(len == 20, "length = 20");

    uint8_t expected[20];
    hex_to_bytes("21002741005e885e36a7e8830010000041a72e6d", expected, 20);

    if (memcmp(buf, expected, 20) != 0) {
        char got[41], exp[41];
        bytes_to_hex(buf, 20, got);
        bytes_to_hex(expected, 20, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 3: ACK packet (with selective ack extension)
 * ========================================================================= */

static void test_encode_ack_sack(void) {
    TEST("ACK encode (with SACK)");

    utp_header_t hdr = {
        .type       = UTP_ST_STATE,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_SACK,
        .conn_id    = 10049,
        .timestamp  = 6195294,
        .timestamp_diff = 916973699,
        .wnd_size   = 1048576,
        .seq_nr     = 16807,
        .ack_nr     = 11885,
    };

    utp_sack_t sack = {
        .bitmask = { 1, 0, 0, 128 },
        .len = 4,
    };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, &sack, NULL, 0);
    ASSERT(len == 26, "length = 26");

    uint8_t expected[26];
    hex_to_bytes("21012741005e885e36a7e8830010000041a72e6d000401000080", expected, 26);

    if (memcmp(buf, expected, 26) != 0) {
        char got[53], exp[53];
        bytes_to_hex(buf, 26, got);
        bytes_to_hex(expected, 26, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 4: DATA packet
 * ========================================================================= */

static void test_encode_data(void) {
    TEST("DATA encode");

    utp_header_t hdr = {
        .type       = UTP_ST_DATA,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_NONE,
        .conn_id    = 26237,
        .timestamp  = 252492495,
        .timestamp_diff = 242289855,
        .wnd_size   = 1048576,
        .seq_nr     = 8334,
        .ack_nr     = 16806,
    };

    uint8_t payload[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL, payload, 10);
    ASSERT(len == 30, "length = 30");

    uint8_t expected[30];
    hex_to_bytes("0100667d0f0cbacf0e710cbf00100000208e41a600010203040506070809", expected, 30);

    if (memcmp(buf, expected, 30) != 0) {
        char got[61], exp[61];
        bytes_to_hex(buf, 30, got);
        bytes_to_hex(expected, 30, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 5: FIN packet
 * ========================================================================= */

static void test_encode_fin(void) {
    TEST("FIN encode");

    utp_header_t hdr = {
        .type       = UTP_ST_FIN,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_NONE,
        .conn_id    = 19003,
        .timestamp  = 515227279,
        .timestamp_diff = 511481041,
        .wnd_size   = 1048576,
        .seq_nr     = 41050,
        .ack_nr     = 16806,
    };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL, NULL, 0);
    ASSERT(len == 20, "length = 20");

    uint8_t expected[20];
    hex_to_bytes("11004a3b1eb5be8f1e7c94d100100000a05a41a6", expected, 20);

    if (memcmp(buf, expected, 20) != 0) {
        char got[41], exp[41];
        bytes_to_hex(buf, 20, got);
        bytes_to_hex(expected, 20, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 6: RESET packet
 * ========================================================================= */

static void test_encode_reset(void) {
    TEST("RESET encode");

    utp_header_t hdr = {
        .type       = UTP_ST_RESET,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_NONE,
        .conn_id    = 62285,
        .timestamp  = 751226811,
        .timestamp_diff = 0,
        .wnd_size   = 0,
        .seq_nr     = 55413,
        .ack_nr     = 16807,
    };

    uint8_t buf[64];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, NULL, NULL, 0);
    ASSERT(len == 20, "length = 20");

    uint8_t expected[20];
    hex_to_bytes("3100f34d2cc6cfbb0000000000000000d87541a7", expected, 20);

    if (memcmp(buf, expected, 20) != 0) {
        char got[41], exp[41];
        bytes_to_hex(buf, 20, got);
        bytes_to_hex(expected, 20, exp);
        printf("FAIL: mismatch\n  got:    %s\n  expect: %s\n", got, exp);
        tests_failed++;
        return;
    }

    PASS();
}

/* =========================================================================
 * Test 7: Decode roundtrip
 * ========================================================================= */

static void test_decode_roundtrip(void) {
    TEST("decode roundtrip");

    /* Encode a DATA packet with SACK extension and payload */
    utp_header_t hdr = {
        .type       = UTP_ST_DATA,
        .version    = UTP_VERSION,
        .extension  = UTP_EXT_SACK,
        .conn_id    = 12345,
        .timestamp  = 99999,
        .timestamp_diff = 42,
        .wnd_size   = 65536,
        .seq_nr     = 100,
        .ack_nr     = 50,
    };

    utp_sack_t sack = { .bitmask = { 0xFF, 0x00, 0xAA, 0x55 }, .len = 4 };
    uint8_t payload[] = { 'H', 'e', 'l', 'l', 'o' };

    uint8_t buf[128];
    size_t len = utp_encode(buf, sizeof(buf), &hdr, &sack, payload, 5);
    ASSERT(len == 20 + 6 + 5, "encoded length = 31");

    /* Decode */
    utp_packet_t pkt;
    ASSERT(utp_decode(&pkt, buf, len), "decode");

    ASSERT(pkt.hdr.type == UTP_ST_DATA, "type");
    ASSERT(pkt.hdr.version == UTP_VERSION, "version");
    ASSERT(pkt.hdr.conn_id == 12345, "conn_id");
    ASSERT(pkt.hdr.timestamp == 99999, "timestamp");
    ASSERT(pkt.hdr.timestamp_diff == 42, "timestamp_diff");
    ASSERT(pkt.hdr.wnd_size == 65536, "wnd_size");
    ASSERT(pkt.hdr.seq_nr == 100, "seq_nr");
    ASSERT(pkt.hdr.ack_nr == 50, "ack_nr");

    ASSERT(pkt.has_sack, "has sack");
    ASSERT(pkt.sack.len == 4, "sack len");
    ASSERT(pkt.sack.bitmask[0] == 0xFF, "sack[0]");
    ASSERT(pkt.sack.bitmask[2] == 0xAA, "sack[2]");

    ASSERT(pkt.payload_len == 5, "payload len");
    ASSERT(memcmp(pkt.payload, "Hello", 5) == 0, "payload data");

    PASS();
}

/* =========================================================================
 * Test 8: Connection lifecycle (loopback)
 * ========================================================================= */

/* Capture sent packets for loopback testing */
#define MAX_CAPTURED 128

typedef struct {
    uint8_t peer_id[32];
    uint8_t data[2048];
    size_t  len;
} captured_pkt_t;

static captured_pkt_t captured_a[MAX_CAPTURED];
static int captured_a_count = 0;
static captured_pkt_t captured_b[MAX_CAPTURED];
static int captured_b_count = 0;

static void send_fn_a(const uint8_t peer_id[32],
                      const uint8_t *data, size_t len,
                      void *user_data) {
    (void)user_data;
    if (captured_a_count < MAX_CAPTURED) {
        memcpy(captured_a[captured_a_count].peer_id, peer_id, 32);
        memcpy(captured_a[captured_a_count].data, data,
               len > 2048 ? 2048 : len);
        captured_a[captured_a_count].len = len;
        captured_a_count++;
    }
}

static void send_fn_b(const uint8_t peer_id[32],
                      const uint8_t *data, size_t len,
                      void *user_data) {
    (void)user_data;
    if (captured_b_count < MAX_CAPTURED) {
        memcpy(captured_b[captured_b_count].peer_id, peer_id, 32);
        memcpy(captured_b[captured_b_count].data, data,
               len > 2048 ? 2048 : len);
        captured_b[captured_b_count].len = len;
        captured_b_count++;
    }
}

/* Data received callback */
static uint8_t received_data[65536];
static size_t  received_len = 0;
static uint16_t received_conn_id = 0;

static void on_data_cb(uint16_t conn_id,
                       const uint8_t *data, size_t len,
                       void *user_data) {
    (void)user_data;
    received_conn_id = conn_id;
    if (data && len > 0 && len <= sizeof(received_data)) {
        memcpy(received_data, data, len);
        received_len = len;
    }
}

static void test_connection_lifecycle(void) {
    TEST("connection lifecycle");

    captured_a_count = 0;
    captured_b_count = 0;
    received_len = 0;

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    uint16_t conn_id = 100;  /* out-of-band ID */

    /* A = initiator, B = responder */
    utp_ctx_t *ctx_a = utp_ctx_create(send_fn_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_fn_b, NULL);
    ASSERT(ctx_a && ctx_b, "create contexts");

    /* B listens */
    ASSERT(utp_listen(ctx_b, peer_a, conn_id, on_data_cb, NULL), "listen");

    /* A connects — sends SYN */
    utp_conn_t *conn_a = utp_connect(ctx_a, peer_b, conn_id);
    ASSERT(conn_a != NULL, "connect");
    ASSERT(utp_conn_state(conn_a) == UTP_STATE_SYN_SENT, "state = SYN_SENT");
    ASSERT(captured_a_count == 1, "SYN sent");

    /* Deliver SYN to B */
    utp_on_packet(ctx_b, peer_a, captured_a[0].data, captured_a[0].len);
    ASSERT(captured_b_count == 1, "STATE sent by B");

    /* Deliver STATE to A */
    utp_on_packet(ctx_a, peer_b, captured_b[0].data, captured_b[0].len);
    ASSERT(utp_conn_state(conn_a) == UTP_STATE_CONNECTED, "state = CONNECTED");

    /* A writes data */
    uint8_t test_data[] = "Hello, uTP!";
    ssize_t written = utp_write(conn_a, test_data, sizeof(test_data));
    ASSERT(written == (ssize_t)sizeof(test_data), "write accepted");
    ASSERT(captured_a_count == 2, "DATA sent");

    /* Deliver DATA to B */
    utp_on_packet(ctx_b, peer_a, captured_a[1].data, captured_a[1].len);
    ASSERT(captured_b_count == 2, "ACK sent by B");

    /* Deliver ACK to A */
    utp_on_packet(ctx_a, peer_b, captured_b[1].data, captured_b[1].len);

    /* A closes (sends FIN) */
    utp_close(conn_a);
    ASSERT(captured_a_count == 3, "FIN sent");

    /* Deliver FIN to B → triggers on_data callback */
    utp_on_packet(ctx_b, peer_a, captured_a[2].data, captured_a[2].len);
    ASSERT(received_len == sizeof(test_data), "received data length");
    ASSERT(memcmp(received_data, test_data, sizeof(test_data)) == 0, "data matches");

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);

    PASS();
}

/* =========================================================================
 * Test 9: Larger loopback transfer
 * ========================================================================= */

static void test_loopback_transfer(void) {
    TEST("loopback transfer");

    captured_a_count = 0;
    captured_b_count = 0;
    received_len = 0;

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    uint16_t conn_id = 200;

    utp_ctx_t *ctx_a = utp_ctx_create(send_fn_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_fn_b, NULL);
    ASSERT(ctx_a && ctx_b, "create");

    /* B listens */
    utp_listen(ctx_b, peer_a, conn_id, on_data_cb, NULL);

    /* A connects */
    utp_conn_t *conn_a = utp_connect(ctx_a, peer_b, conn_id);
    ASSERT(conn_a, "connect");

    /* SYN → B, STATE → A */
    utp_on_packet(ctx_b, peer_a, captured_a[0].data, captured_a[0].len);
    utp_on_packet(ctx_a, peer_b, captured_b[0].data, captured_b[0].len);
    ASSERT(utp_conn_state(conn_a) == UTP_STATE_CONNECTED, "connected");

    /* Write 5000 bytes (spans multiple packets) */
    uint8_t big_data[5000];
    for (int i = 0; i < 5000; i++)
        big_data[i] = (uint8_t)(i & 0xFF);

    ssize_t written = utp_write(conn_a, big_data, 5000);
    ASSERT(written == 5000, "write 5000 bytes");

    /* Deliver all data packets to B and ACKs back to A */
    int prev_a = 1;  /* skip SYN */
    int prev_b = 1;  /* skip initial STATE */

    for (int round = 0; round < 20; round++) {
        /* Deliver A→B packets */
        for (int i = prev_a; i < captured_a_count; i++) {
            utp_on_packet(ctx_b, peer_a, captured_a[i].data, captured_a[i].len);
        }
        prev_a = captured_a_count;

        /* Deliver B→A packets (ACKs) */
        for (int i = prev_b; i < captured_b_count; i++) {
            utp_on_packet(ctx_a, peer_b, captured_b[i].data, captured_b[i].len);
        }
        prev_b = captured_b_count;

        /* Try to flush more data */
        utp_tick(ctx_a, 0);

        if (captured_a_count == prev_a && captured_b_count == prev_b)
            break;
    }

    /* Close */
    utp_close(conn_a);

    /* Deliver FIN */
    for (int i = prev_a; i < captured_a_count; i++) {
        utp_on_packet(ctx_b, peer_a, captured_a[i].data, captured_a[i].len);
    }

    ASSERT(received_len == 5000, "received 5000 bytes");
    ASSERT(memcmp(received_data, big_data, 5000) == 0, "data integrity");

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);

    PASS();
}

/* =========================================================================
 * Test 10: Out-of-order delivery
 * ========================================================================= */

static void test_out_of_order(void) {
    TEST("out-of-order delivery");

    captured_a_count = 0;
    captured_b_count = 0;
    received_len = 0;

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    uint16_t conn_id = 300;

    utp_ctx_t *ctx_a = utp_ctx_create(send_fn_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_fn_b, NULL);

    /* B listens */
    utp_listen(ctx_b, peer_a, conn_id, on_data_cb, NULL);

    /* A connects */
    utp_conn_t *conn_a = utp_connect(ctx_a, peer_b, conn_id);

    /* Handshake */
    utp_on_packet(ctx_b, peer_a, captured_a[0].data, captured_a[0].len);
    utp_on_packet(ctx_a, peer_b, captured_b[0].data, captured_b[0].len);

    /* Write 3 chunks of data (will be 3 separate DATA packets since each < 1200) */
    uint8_t chunk1[100], chunk2[100], chunk3[100];
    memset(chunk1, 'A', 100);
    memset(chunk2, 'B', 100);
    memset(chunk3, 'C', 100);

    utp_write(conn_a, chunk1, 100);
    utp_write(conn_a, chunk2, 100);
    utp_write(conn_a, chunk3, 100);

    /* A should have sent: SYN(idx=0), DATA1(idx=1), DATA2(idx=2), DATA3(idx=3) */
    ASSERT(captured_a_count >= 4, "3 data packets sent");

    /* Deliver out of order: DATA3, DATA1, DATA2 */
    utp_on_packet(ctx_b, peer_a, captured_a[3].data, captured_a[3].len);
    utp_on_packet(ctx_b, peer_a, captured_a[1].data, captured_a[1].len);
    utp_on_packet(ctx_b, peer_a, captured_a[2].data, captured_a[2].len);

    /* Close and deliver FIN */
    utp_close(conn_a);
    /* Deliver ACKs back so FIN can be sent */
    int prev_b = 1;
    for (int i = prev_b; i < captured_b_count; i++) {
        utp_on_packet(ctx_a, peer_b, captured_b[i].data, captured_b[i].len);
    }
    utp_tick(ctx_a, 0);

    /* Deliver FIN */
    for (int i = 4; i < captured_a_count; i++) {
        utp_on_packet(ctx_b, peer_a, captured_a[i].data, captured_a[i].len);
    }

    ASSERT(received_len == 300, "received 300 bytes");

    /* Verify order: should be AAA...BBB...CCC... regardless of delivery order */
    bool order_ok = true;
    for (int i = 0; i < 100; i++) {
        if (received_data[i] != 'A') order_ok = false;
        if (received_data[100 + i] != 'B') order_ok = false;
        if (received_data[200 + i] != 'C') order_ok = false;
    }
    ASSERT(order_ok, "data in correct order");

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("uTP (micro Transport Protocol)\n");
    printf("===============================\n");

    test_encode_syn();
    test_encode_ack();
    test_encode_ack_sack();
    test_encode_data();
    test_encode_fin();
    test_encode_reset();
    test_decode_roundtrip();
    test_connection_lifecycle();
    test_loopback_transfer();
    test_out_of_order();

    printf("-------------------------------\n");
    printf("Results: %d passed, %d failed\n", tests_passed, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
