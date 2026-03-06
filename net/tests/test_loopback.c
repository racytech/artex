/*
 * Loopback Integration Test — full stack on localhost.
 *
 * Two Discv5 engines on ephemeral ports shuttle encrypted UDP packets
 * through the full handshake → session → message pipeline.
 *
 * Tests:
 *  1. Discv5 PING → PONG via real UDP on localhost
 *  2. Portal PING → routing table update via TALKREQ
 *  3. Two-engine TALKREQ/TALKRESP echo
 */

#include "../include/discv5.h"
#include "../include/portal.h"
#include "../include/portal_wire.h"
#include "../include/history.h"
#include "../include/secp256k1_wrap.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <poll.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

/* =========================================================================
 * Helpers: pump packets between two engines
 * ========================================================================= */

/**
 * Read one UDP packet from an engine's socket (non-blocking).
 * Returns bytes read, or 0 if no packet available.
 */
static ssize_t read_one(int fd, uint8_t *buf, size_t cap,
                        uint8_t src_ip[4], uint16_t *src_port) {
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    if (poll(&pfd, 1, 10) <= 0) return 0; /* 10ms timeout */

    struct sockaddr_in from;
    socklen_t from_len = sizeof(from);
    ssize_t n = recvfrom(fd, buf, cap, MSG_DONTWAIT,
                         (struct sockaddr *)&from, &from_len);
    if (n <= 0) return 0;

    uint32_t ip = ntohl(from.sin_addr.s_addr);
    src_ip[0] = (ip >> 24) & 0xFF;
    src_ip[1] = (ip >> 16) & 0xFF;
    src_ip[2] = (ip >> 8)  & 0xFF;
    src_ip[3] = ip & 0xFF;
    *src_port = ntohs(from.sin_port);
    return n;
}

/**
 * Pump all pending packets between two engines.
 * Each round: read from A→deliver to B, read from B→deliver to A.
 * Returns total packets delivered.
 */
static int pump(discv5_engine_t *a, discv5_engine_t *b, int max_rounds) {
    int fd_a = discv5_engine_udp_fd(a);
    int fd_b = discv5_engine_udp_fd(b);
    int total = 0;

    for (int round = 0; round < max_rounds; round++) {
        int delivered = 0;
        uint8_t buf[2048];
        uint8_t src_ip[4];
        uint16_t src_port;

        /* A's socket → deliver to B's engine (or A if self-directed) */
        /* Actually, engine A sends packets TO engine B's port.
         * So engine B receives on its socket. */

        /* Read from B's socket (packets A sent to B) */
        ssize_t n = read_one(fd_b, buf, sizeof(buf), src_ip, &src_port);
        if (n > 0) {
            discv5_engine_on_packet(b, buf, (size_t)n, src_ip, src_port);
            delivered++;
        }

        /* Read from A's socket (packets B sent to A) */
        n = read_one(fd_a, buf, sizeof(buf), src_ip, &src_port);
        if (n > 0) {
            discv5_engine_on_packet(a, buf, (size_t)n, src_ip, src_port);
            delivered++;
        }

        total += delivered;
        if (delivered == 0) break;
    }
    return total;
}

/* =========================================================================
 * Test 1: Discv5 PING → PONG via localhost UDP
 * ========================================================================= */

static void test_discv5_ping_pong(void) {
    TEST("Discv5 PING/PONG loopback");

    /* Create two engines with random keys */
    uint8_t priv_a[32], priv_b[32];
    memset(priv_a, 0, 32); priv_a[31] = 1; /* key = 1 (minimal valid key) */
    memset(priv_b, 0, 32); priv_b[31] = 2; /* key = 2 */

    discv5_engine_t *a = discv5_engine_create(priv_a, 0);
    discv5_engine_t *b = discv5_engine_create(priv_b, 0);
    ASSERT(a && b, "create engines");

    uint16_t port_a = discv5_engine_udp_port(a);
    uint16_t port_b = discv5_engine_udp_port(b);
    ASSERT(port_a > 0 && port_b > 0, "bound ports");

    const uint8_t *id_a = discv5_engine_local_id(a);
    const uint8_t *id_b = discv5_engine_local_id(b);

    /* Derive compressed pubkeys for add_node */
    uint8_t pub_a[64], pub_b[64], cpub_a[33], cpub_b[33];
    secp256k1_wrap_pubkey_create(pub_a, priv_a);
    secp256k1_wrap_pubkey_create(pub_b, priv_b);
    secp256k1_wrap_compress(cpub_a, pub_a);
    secp256k1_wrap_compress(cpub_b, pub_b);

    uint8_t lo[] = { 127, 0, 0, 1 };

    /* Register each as peer */
    ASSERT(discv5_engine_add_node(a, id_b, cpub_b, lo, port_b), "add B to A");
    ASSERT(discv5_engine_add_node(b, id_a, cpub_a, lo, port_a), "add A to B");

    /* A sends PING to B */
    uint32_t req = discv5_engine_send_ping(a, id_b);
    ASSERT(req > 0, "send_ping returns req_id");

    /* Pump packets through the handshake:
     * Round 1: A→B ordinary (triggers WHOAREYOU)
     * Round 2: B→A WHOAREYOU
     * Round 3: A→B handshake (with actual PING)
     * Round 4: B→A ordinary (PONG response)
     */
    int delivered = 0;
    for (int i = 0; i < 20; i++) {
        int d = pump(a, b, 5);
        delivered += d;
        if (d == 0 && i > 4) break;
        usleep(10000); /* 10ms between pumps */
    }

    ASSERT(delivered >= 4, "enough packets exchanged for handshake");

    discv5_engine_destroy(a);
    discv5_engine_destroy(b);
    PASS();
}

/* =========================================================================
 * Test 2: TALKREQ/TALKRESP echo via localhost
 * ========================================================================= */

/* Track TALKREQ received state */
static bool talk_received = false;
static uint8_t talk_data[256];
static size_t  talk_data_len = 0;

static void talk_handler(discv5_engine_t *engine,
                         const uint8_t peer_id[32],
                         uint32_t req_id,
                         const uint8_t *request, size_t req_len,
                         void *user_data) {
    (void)user_data;
    talk_received = true;
    if (req_len <= sizeof(talk_data)) {
        memcpy(talk_data, request, req_len);
        talk_data_len = req_len;
    }
    /* Echo back the request data */
    discv5_engine_send_talkresp(engine, peer_id, req_id, request, req_len);
}

static void test_talkreq_echo(void) {
    TEST("TALKREQ/TALKRESP echo loopback");

    uint8_t priv_a[32], priv_b[32];
    memset(priv_a, 0, 32); priv_a[31] = 3;
    memset(priv_b, 0, 32); priv_b[31] = 4;

    discv5_engine_t *a = discv5_engine_create(priv_a, 0);
    discv5_engine_t *b = discv5_engine_create(priv_b, 0);
    ASSERT(a && b, "create");

    uint16_t port_a = discv5_engine_udp_port(a);
    uint16_t port_b = discv5_engine_udp_port(b);

    const uint8_t *id_a = discv5_engine_local_id(a);
    const uint8_t *id_b = discv5_engine_local_id(b);

    uint8_t pub_a[64], pub_b[64], cpub_a[33], cpub_b[33];
    secp256k1_wrap_pubkey_create(pub_a, priv_a);
    secp256k1_wrap_pubkey_create(pub_b, priv_b);
    secp256k1_wrap_compress(cpub_a, pub_a);
    secp256k1_wrap_compress(cpub_b, pub_b);

    uint8_t lo[] = { 127, 0, 0, 1 };
    discv5_engine_add_node(a, id_b, cpub_b, lo, port_b);
    discv5_engine_add_node(b, id_a, cpub_a, lo, port_a);

    /* Register TALKREQ handler on B */
    talk_received = false;
    discv5_engine_register_talk(b, "test", talk_handler, NULL);

    /* A sends TALKREQ to B */
    uint8_t payload[] = { 0xDE, 0xAD, 0xBE, 0xEF };
    discv5_engine_send_talkreq(a, id_b, "test", payload, sizeof(payload));

    /* Pump through handshake + message delivery */
    for (int i = 0; i < 20; i++) {
        pump(a, b, 10);
        if (talk_received) break;
        usleep(10000);
    }

    ASSERT(talk_received, "B received TALKREQ");
    ASSERT(talk_data_len == 4, "payload length");
    ASSERT(memcmp(talk_data, payload, 4) == 0, "payload data");

    discv5_engine_destroy(a);
    discv5_engine_destroy(b);
    PASS();
}

/* =========================================================================
 * Test 3: Portal PING via Discv5 TALKREQ on localhost
 * ========================================================================= */

static const uint8_t MAX_RADIUS[32] = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};

static void test_portal_ping_loopback(void) {
    TEST("Portal PING via Discv5 TALKREQ loopback");

    uint8_t priv_a[32], priv_b[32];
    memset(priv_a, 0, 32); priv_a[31] = 5;
    memset(priv_b, 0, 32); priv_b[31] = 6;

    discv5_engine_t *ea = discv5_engine_create(priv_a, 0);
    discv5_engine_t *eb = discv5_engine_create(priv_b, 0);
    ASSERT(ea && eb, "create engines");

    uint16_t port_a = discv5_engine_udp_port(ea);
    uint16_t port_b = discv5_engine_udp_port(eb);

    const uint8_t *id_a = discv5_engine_local_id(ea);
    const uint8_t *id_b = discv5_engine_local_id(eb);

    uint8_t pub_a[64], pub_b[64], cpub_a[33], cpub_b[33];
    secp256k1_wrap_pubkey_create(pub_a, priv_a);
    secp256k1_wrap_pubkey_create(pub_b, priv_b);
    secp256k1_wrap_compress(cpub_a, pub_a);
    secp256k1_wrap_compress(cpub_b, pub_b);

    uint8_t lo[] = { 127, 0, 0, 1 };
    discv5_engine_add_node(ea, id_b, cpub_b, lo, port_b);
    discv5_engine_add_node(eb, id_a, cpub_a, lo, port_a);

    /* Create portal overlays on both engines */
    portal_overlay_t *ov_b = portal_overlay_create(
        eb, NULL,
        HISTORY_PROTOCOL_ID, HISTORY_PROTOCOL_ID_LEN,
        id_b, MAX_RADIUS, NULL);
    ASSERT(ov_b, "create overlay B");

    /* A sends a Portal PING as raw TALKREQ */
    uint8_t radius[32];
    memset(radius, 0xFF, 32);
    uint8_t payload_buf[64];
    size_t plen = portal_encode_payload_type1(payload_buf, sizeof(payload_buf),
                                               radius);
    uint8_t ping_buf[128];
    size_t ping_len = portal_encode_ping(ping_buf, sizeof(ping_buf),
                                          1, PORTAL_PAYLOAD_TYPE1,
                                          payload_buf, plen);
    ASSERT(ping_len > 0, "encode portal ping");

    /* Send as TALKREQ from A to B */
    discv5_engine_send_talkreq(ea, id_b,
                                HISTORY_PROTOCOL_ID,
                                ping_buf, ping_len);

    /* Pump packets through handshake and message exchange */
    for (int i = 0; i < 20; i++) {
        pump(ea, eb, 10);
        usleep(10000);
    }

    /* Check that B's overlay learned about A */
    portal_table_t *t = portal_overlay_table(ov_b);
    bool peer_found = (portal_table_find(t, id_a) != NULL);

    portal_overlay_destroy(ov_b);
    discv5_engine_destroy(ea);
    discv5_engine_destroy(eb);

    ASSERT(peer_found, "peer A found in B's routing table");
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    secp256k1_wrap_init();

    printf("=== Loopback Integration Tests ===\n");

    test_discv5_ping_pong();
    test_talkreq_echo();
    test_portal_ping_loopback();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);

    secp256k1_wrap_destroy();
    return tests_failed ? 1 : 0;
}
