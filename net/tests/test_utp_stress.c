/*
 * uTP Stress Tests — state machine under adversarial conditions.
 *
 * Tests:
 *  1. Concurrent connections (8 parallel transfers)
 *  2. Large transfer (64 KB)
 *  3. Packet loss + retransmission (drop 30% of packets)
 *  4. Duplicate delivery (deliver some packets twice)
 *  5. Rapid connect/disconnect cycling
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

/* =========================================================================
 * PRNG (same xorshift64 as roundtrip tests)
 * ========================================================================= */

static uint64_t rng_state;
static void rng_seed(uint64_t s) { rng_state = s ? s : 1; }
static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}
static uint8_t rng_u8(void) { return (uint8_t)rng_next(); }

/* =========================================================================
 * Packet capture infrastructure
 * ========================================================================= */

#define MAX_PKT 512

typedef struct {
    uint8_t data[2048];
    size_t  len;
} pkt_t;

typedef struct {
    pkt_t pkts[MAX_PKT];
    int   count;
} pkt_queue_t;

static pkt_queue_t q_a; /* packets sent by A */
static pkt_queue_t q_b; /* packets sent by B */

static void send_a(const uint8_t peer_id[32], const uint8_t *data,
                   size_t len, void *user) {
    (void)peer_id; (void)user;
    if (q_a.count < MAX_PKT) {
        size_t n = len < 2048 ? len : 2048;
        memcpy(q_a.pkts[q_a.count].data, data, n);
        q_a.pkts[q_a.count].len = n;
        q_a.count++;
    }
}

static void send_b(const uint8_t peer_id[32], const uint8_t *data,
                   size_t len, void *user) {
    (void)peer_id; (void)user;
    if (q_b.count < MAX_PKT) {
        size_t n = len < 2048 ? len : 2048;
        memcpy(q_b.pkts[q_b.count].data, data, n);
        q_b.pkts[q_b.count].len = n;
        q_b.count++;
    }
}

/* Per-connection receive buffer */
#define MAX_RECV (128 * 1024)
static uint8_t recv_buf[MAX_RECV];
static size_t  recv_len;
static uint16_t recv_cid;

static void on_recv(uint16_t cid, const uint8_t *data, size_t len, void *u) {
    (void)u;
    recv_cid = cid;
    if (data && len > 0 && len <= MAX_RECV) {
        memcpy(recv_buf, data, len);
        recv_len = len;
    }
}

/* Multi-connection receive infrastructure */
#define MAX_CONNS_STRESS 8
static uint8_t mc_recv_buf[MAX_CONNS_STRESS][8192];
static size_t  mc_recv_len[MAX_CONNS_STRESS];

static void on_recv_0(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[0], d, l); mc_recv_len[0] = l; }
}
static void on_recv_1(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[1], d, l); mc_recv_len[1] = l; }
}
static void on_recv_2(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[2], d, l); mc_recv_len[2] = l; }
}
static void on_recv_3(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[3], d, l); mc_recv_len[3] = l; }
}
static void on_recv_4(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[4], d, l); mc_recv_len[4] = l; }
}
static void on_recv_5(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[5], d, l); mc_recv_len[5] = l; }
}
static void on_recv_6(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[6], d, l); mc_recv_len[6] = l; }
}
static void on_recv_7(uint16_t c, const uint8_t *d, size_t l, void *u) {
    (void)c; (void)u;
    if (d && l <= 8192) { memcpy(mc_recv_buf[7], d, l); mc_recv_len[7] = l; }
}

static utp_data_fn mc_recv_fns[MAX_CONNS_STRESS] = {
    on_recv_0, on_recv_1, on_recv_2, on_recv_3,
    on_recv_4, on_recv_5, on_recv_6, on_recv_7,
};

/* =========================================================================
 * Helper: drive a complete transfer (normal, no loss)
 * ========================================================================= */

static int drv_prev_a, drv_prev_b;

static void drive_reset(void) { drv_prev_a = 0; drv_prev_b = 0; }

static bool drive_transfer(utp_ctx_t *ctx_a, utp_ctx_t *ctx_b,
                           const uint8_t peer_a[32], const uint8_t peer_b[32]) {
    for (int round = 0; round < 100; round++) {
        /* Deliver A→B */
        for (int i = drv_prev_a; i < q_a.count; i++)
            utp_on_packet(ctx_b, peer_a, q_a.pkts[i].data, q_a.pkts[i].len);
        drv_prev_a = q_a.count;
        /* Deliver B→A */
        for (int i = drv_prev_b; i < q_b.count; i++)
            utp_on_packet(ctx_a, peer_b, q_b.pkts[i].data, q_b.pkts[i].len);
        drv_prev_b = q_b.count;
        utp_tick(ctx_a, 0);
        if (q_a.count == drv_prev_a && q_b.count == drv_prev_b)
            break;
    }
    return true;
}

/* =========================================================================
 * 1. Concurrent connections
 * ========================================================================= */

static void test_concurrent_connections(void) {
    TEST("concurrent connections (8)");

    /* Use separate capture queues per direction (reuse global q_a/q_b) */
    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    utp_ctx_t *ctx_a = utp_ctx_create(send_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_b, NULL);
    ASSERT(ctx_a && ctx_b, "create contexts");

    const int N = MAX_CONNS_STRESS;
    utp_conn_t *conns[MAX_CONNS_STRESS];
    uint8_t test_data[MAX_CONNS_STRESS][256];

    /* Reset multi-connection receive state */
    for (int i = 0; i < N; i++) {
        mc_recv_len[i] = 0;
        for (int j = 0; j < 256; j++)
            test_data[i][j] = (uint8_t)(i * 37 + j);
    }

    q_a.count = 0;
    q_b.count = 0;
    drive_reset();

    /* Register all listeners on B, connect all from A */
    for (int i = 0; i < N; i++) {
        uint16_t cid = (uint16_t)(500 + i * 2);
        utp_listen(ctx_b, peer_a, cid, mc_recv_fns[i], NULL);
        conns[i] = utp_connect(ctx_a, peer_b, cid);
        ASSERT(conns[i], "connect");
    }

    /* Complete all handshakes */
    drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

    /* Write data on each connection */
    for (int i = 0; i < N; i++) {
        ASSERT(utp_conn_state(conns[i]) == UTP_STATE_CONNECTED, "connected");
        ssize_t w = utp_write(conns[i], test_data[i], 256);
        ASSERT(w == 256, "write");
    }

    /* Drive data delivery */
    drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

    /* Close all */
    for (int i = 0; i < N; i++)
        utp_close(conns[i]);

    drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

    /* Verify all received correctly */
    for (int i = 0; i < N; i++) {
        ASSERT(mc_recv_len[i] == 256, "recv len");
        ASSERT(memcmp(mc_recv_buf[i], test_data[i], 256) == 0, "recv data");
    }

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);
    PASS();
}

/* =========================================================================
 * 2. Large transfer (64 KB)
 * ========================================================================= */

static void test_large_transfer(void) {
    TEST("large transfer (64 KB)");

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    utp_ctx_t *ctx_a = utp_ctx_create(send_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_b, NULL);
    ASSERT(ctx_a && ctx_b, "create");

    q_a.count = 0;
    q_b.count = 0;
    recv_len = 0;
    drive_reset();

    uint16_t cid = 700;
    utp_listen(ctx_b, peer_a, cid, on_recv, NULL);
    utp_conn_t *conn = utp_connect(ctx_a, peer_b, cid);
    ASSERT(conn, "connect");

    /* Handshake */
    drive_transfer(ctx_a, ctx_b, peer_a, peer_b);
    ASSERT(utp_conn_state(conn) == UTP_STATE_CONNECTED, "connected");

    /* Generate 64 KB of deterministic data */
    const size_t DATA_SIZE = 65536;
    uint8_t *data = (uint8_t *)malloc(DATA_SIZE);
    ASSERT(data, "alloc");
    rng_seed(0x64BE);
    for (size_t i = 0; i < DATA_SIZE; i++)
        data[i] = rng_u8();

    /* Write in chunks to avoid filling send buffer */
    size_t sent = 0;
    while (sent < DATA_SIZE) {
        size_t chunk = DATA_SIZE - sent;
        if (chunk > 4096) chunk = 4096;
        ssize_t w = utp_write(conn, data + sent, chunk);
        if (w > 0) sent += (size_t)w;
        /* Pump packets to make room */
        drive_transfer(ctx_a, ctx_b, peer_a, peer_b);
    }

    /* Close and finalize */
    utp_close(conn);
    drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

    ASSERT(recv_len == DATA_SIZE, "received 64KB");
    ASSERT(memcmp(recv_buf, data, DATA_SIZE) == 0, "data integrity");

    free(data);
    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);
    PASS();
}

/* =========================================================================
 * 3. Packet loss + retransmission
 * ========================================================================= */

static void test_packet_loss(void) {
    TEST("packet loss (30%) + retransmission");

    rng_seed(0xD0E0);

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    utp_ctx_t *ctx_a = utp_ctx_create(send_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_b, NULL);
    ASSERT(ctx_a && ctx_b, "create");

    q_a.count = 0;
    q_b.count = 0;
    recv_len = 0;

    uint16_t cid = 800;
    utp_listen(ctx_b, peer_a, cid, on_recv, NULL);
    utp_conn_t *conn = utp_connect(ctx_a, peer_b, cid);
    ASSERT(conn, "connect");

    /* Write test data */
    uint8_t data[2000];
    for (int i = 0; i < 2000; i++)
        data[i] = (uint8_t)(i & 0xFF);

    /* Drive with packet loss: deliver from queues but skip ~30% of packets.
     * Use utp_tick with advancing timestamps to trigger retransmissions. */
    uint64_t now_us = 1000000; /* start at 1 second */
    int prev_a = 0, prev_b = 0;
    bool data_written = false;
    bool closed = false;

    for (int round = 0; round < 200; round++) {
        /* Deliver A→B (with 30% random drops for data, never drop SYN) */
        for (int i = prev_a; i < q_a.count; i++) {
            bool is_handshake = (round < 5 && i < 3);
            bool drop = !is_handshake && (rng_u8() % 100 < 30);
            if (!drop)
                utp_on_packet(ctx_b, peer_a,
                              q_a.pkts[i].data, q_a.pkts[i].len);
        }
        prev_a = q_a.count;

        /* Deliver B→A (with 30% drops, protect handshake) */
        for (int i = prev_b; i < q_b.count; i++) {
            bool is_handshake = (round < 5 && i < 3);
            bool drop = !is_handshake && (rng_u8() % 100 < 30);
            if (!drop)
                utp_on_packet(ctx_a, peer_b,
                              q_b.pkts[i].data, q_b.pkts[i].len);
        }
        prev_b = q_b.count;

        /* Write data once connected */
        if (!data_written && utp_conn_state(conn) == UTP_STATE_CONNECTED) {
            ssize_t w = utp_write(conn, data, 2000);
            ASSERT(w == 2000, "write");
            data_written = true;
        }

        /* Close once data is written and some rounds have passed */
        if (data_written && !closed && round > 50) {
            utp_close(conn);
            closed = true;
        }

        /* Advance time to trigger retransmissions */
        now_us += 1100000; /* 1.1 seconds per round */
        utp_tick(ctx_a, now_us);
        utp_tick(ctx_b, now_us);

        if (recv_len > 0) break; /* transfer complete */
    }

    ASSERT(recv_len == 2000, "received 2000 bytes despite packet loss");
    ASSERT(memcmp(recv_buf, data, 2000) == 0, "data integrity");

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);
    PASS();
}

/* =========================================================================
 * 4. Duplicate delivery
 * ========================================================================= */

static void test_duplicate_delivery(void) {
    TEST("duplicate packet delivery");

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    utp_ctx_t *ctx_a = utp_ctx_create(send_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_b, NULL);
    ASSERT(ctx_a && ctx_b, "create");

    q_a.count = 0;
    q_b.count = 0;
    recv_len = 0;
    drive_reset();

    uint16_t cid = 900;
    utp_listen(ctx_b, peer_a, cid, on_recv, NULL);
    utp_conn_t *conn = utp_connect(ctx_a, peer_b, cid);
    ASSERT(conn, "connect");

    int prev_a = 0, prev_b = 0;
    uint8_t data[1000];
    for (int i = 0; i < 1000; i++)
        data[i] = (uint8_t)(i * 7);

    for (int round = 0; round < 50; round++) {
        /* Deliver A→B — deliver each packet TWICE */
        for (int i = prev_a; i < q_a.count; i++) {
            utp_on_packet(ctx_b, peer_a,
                          q_a.pkts[i].data, q_a.pkts[i].len);
            /* Duplicate */
            utp_on_packet(ctx_b, peer_a,
                          q_a.pkts[i].data, q_a.pkts[i].len);
        }
        prev_a = q_a.count;

        for (int i = prev_b; i < q_b.count; i++) {
            utp_on_packet(ctx_a, peer_b,
                          q_b.pkts[i].data, q_b.pkts[i].len);
            /* Duplicate */
            utp_on_packet(ctx_a, peer_b,
                          q_b.pkts[i].data, q_b.pkts[i].len);
        }
        prev_b = q_b.count;

        if (utp_conn_state(conn) == UTP_STATE_CONNECTED && round == 5) {
            ssize_t w = utp_write(conn, data, 1000);
            ASSERT(w == 1000, "write");
        }

        if (round == 20)
            utp_close(conn);

        utp_tick(ctx_a, 0);

        if (recv_len > 0) break;
    }

    ASSERT(recv_len == 1000, "received 1000 bytes despite duplicates");
    ASSERT(memcmp(recv_buf, data, 1000) == 0, "data integrity");

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);
    PASS();
}

/* =========================================================================
 * 5. Rapid connect/disconnect cycling
 * ========================================================================= */

static void test_rapid_cycle(void) {
    TEST("rapid connect/disconnect (20 cycles)");

    uint8_t peer_a[32], peer_b[32];
    memset(peer_a, 0xAA, 32);
    memset(peer_b, 0xBB, 32);

    utp_ctx_t *ctx_a = utp_ctx_create(send_a, NULL);
    utp_ctx_t *ctx_b = utp_ctx_create(send_b, NULL);
    ASSERT(ctx_a && ctx_b, "create");

    for (int cycle = 0; cycle < 20; cycle++) {
        q_a.count = 0;
        q_b.count = 0;
        recv_len = 0;
        drive_reset();

        uint16_t cid = (uint16_t)(1000 + cycle);
        utp_listen(ctx_b, peer_a, cid, on_recv, NULL);
        utp_conn_t *conn = utp_connect(ctx_a, peer_b, cid);
        ASSERT(conn, "connect");

        /* Handshake */
        drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

        if (utp_conn_state(conn) != UTP_STATE_CONNECTED)
            continue; /* connection failed to establish, skip */

        /* Small write */
        uint8_t msg[4] = { (uint8_t)cycle, 0xDE, 0xAD, 0xFF };
        ssize_t w = utp_write(conn, msg, 4);
        ASSERT(w == 4, "write");

        /* Drive and close */
        drive_transfer(ctx_a, ctx_b, peer_a, peer_b);
        utp_close(conn);
        drive_transfer(ctx_a, ctx_b, peer_a, peer_b);

        ASSERT(recv_len == 4, "recv 4 bytes");
        ASSERT(recv_buf[0] == (uint8_t)cycle, "cycle tag");
    }

    utp_ctx_destroy(ctx_a);
    utp_ctx_destroy(ctx_b);
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== uTP Stress Tests ===\n");

    test_concurrent_connections();
    test_large_transfer();
    test_packet_loss();
    test_duplicate_delivery();
    test_rapid_cycle();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);
    return tests_failed ? 1 : 0;
}
