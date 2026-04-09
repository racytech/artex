/**
 * Portal Node — connects to the Portal History Network.
 *
 * Joins the mainnet Portal Network via known bootnodes,
 * discovers peers, and attempts to fetch historical block data.
 *
 * Usage: ./portal_node [--port <udp_port>]
 */

#include "discv5.h"
#include "portal.h"
#include "portal_wire.h"
#include "history.h"
#include "enr.h"
#include "secp256k1_wrap.h"
#include "keccak256.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

/* =========================================================================
 * Base64url decoder (for "enr:" records)
 * ========================================================================= */

static const int8_t b64url_table[256] = {
    [0 ... 255] = -1,
    ['A'] = 0,  ['B'] = 1,  ['C'] = 2,  ['D'] = 3,
    ['E'] = 4,  ['F'] = 5,  ['G'] = 6,  ['H'] = 7,
    ['I'] = 8,  ['J'] = 9,  ['K'] = 10, ['L'] = 11,
    ['M'] = 12, ['N'] = 13, ['O'] = 14, ['P'] = 15,
    ['Q'] = 16, ['R'] = 17, ['S'] = 18, ['T'] = 19,
    ['U'] = 20, ['V'] = 21, ['W'] = 22, ['X'] = 23,
    ['Y'] = 24, ['Z'] = 25,
    ['a'] = 26, ['b'] = 27, ['c'] = 28, ['d'] = 29,
    ['e'] = 30, ['f'] = 31, ['g'] = 32, ['h'] = 33,
    ['i'] = 34, ['j'] = 35, ['k'] = 36, ['l'] = 37,
    ['m'] = 38, ['n'] = 39, ['o'] = 40, ['p'] = 41,
    ['q'] = 42, ['r'] = 43, ['s'] = 44, ['t'] = 45,
    ['u'] = 46, ['v'] = 47, ['w'] = 48, ['x'] = 49,
    ['y'] = 50, ['z'] = 51,
    ['0'] = 52, ['1'] = 53, ['2'] = 54, ['3'] = 55,
    ['4'] = 56, ['5'] = 57, ['6'] = 58, ['7'] = 59,
    ['8'] = 60, ['9'] = 61,
    ['-'] = 62, ['_'] = 63,
};

static size_t base64url_decode(const char *in, size_t in_len,
                               uint8_t *out, size_t cap) {
    size_t o = 0;
    uint32_t acc = 0;
    int bits = 0;
    for (size_t i = 0; i < in_len; i++) {
        int8_t v = b64url_table[(unsigned char)in[i]];
        if (v < 0) continue;  /* skip padding/whitespace */
        acc = (acc << 6) | (uint32_t)v;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (o < cap) out[o++] = (uint8_t)(acc >> bits);
        }
    }
    return o;
}

/* Decode "enr:..." text into enr_t. Returns true on success. */
static bool enr_decode_text(enr_t *enr, const char *text) {
    if (strncmp(text, "enr:", 4) != 0) return false;
    const char *b64 = text + 4;
    size_t b64_len = strlen(b64);

    uint8_t buf[1024];
    size_t decoded = base64url_decode(b64, b64_len, buf, sizeof(buf));
    if (decoded == 0) return false;

    return enr_decode(enr, buf, decoded);
}

/* Extract IP and UDP port from ENR. Returns true if both found. */
static bool enr_get_endpoint(const enr_t *enr, uint8_t ip[4], uint16_t *port) {
    const uint8_t *val;
    size_t val_len;

    if (!enr_get(enr, "ip", &val, &val_len) || val_len != 4)
        return false;
    memcpy(ip, val, 4);

    if (!enr_get(enr, "udp", &val, &val_len) || val_len == 0 || val_len > 2)
        return false;
    *port = 0;
    for (size_t i = 0; i < val_len; i++)
        *port = (*port << 8) | val[i];

    return true;
}

/* Extract compressed pubkey (33 bytes) from ENR. */
static bool enr_get_pubkey(const enr_t *enr, uint8_t pubkey[33]) {
    const uint8_t *val;
    size_t val_len;
    if (!enr_get(enr, "secp256k1", &val, &val_len) || val_len != 33)
        return false;
    memcpy(pubkey, val, 33);
    return true;
}

/* =========================================================================
 * Portal History Network bootnodes (from trin/fluffy repos)
 * ========================================================================= */

static const char *BOOTNODES[] = {
    /* Trin bootnodes (from ethereum/trin repo, 2025) */
    "enr:-Jm4QBcjAoXU79kbUGNGfeDwW9OjiaknvaiKwZa81U91xC9ODSpQvzsEbNd_lww3CCHsqxgGHR8O18frKStu4A3F7sGEaBUIa2OJdCBhZmE1N2MxgmlkgnY0gmlwhKEjVaWCcHaCAAGJc2VjcDI1NmsxoQLSC_nhF1iRwsCw0n3J4jRjqoaRxtKgsEe5a-Dz7y0JloN1ZHCCIyg",
    "enr:-Jm4QAtSz2CaRwEceSHfSeI2g15cumv9oPqlKAVCHoi34-X6NZuQHYEieuZz-acnmww3yAPTDd4BZeFyv248apKSsKaEaBUIYWOJdCBhZmE1N2MxgmlkgnY0gmlwhJO2oc6CcHaCAAGJc2VjcDI1NmsxoQLMSGVlxXL62N3sPtaV-n_TbZFCEM5AR7RDyIwOadbQK4N1ZHCCIyg",
    "enr:-Jm4QNepJO38VOGGsuj0fBjeLHU2fsNBvYewhpCRDHyCFjgFI7EKdBptbi_jwCsGDQhrgh4X5TikBlqYcSUJyExbSMqEaBUIeGOJdCBhZmE1N2MxgmlkgnY0gmlwhJ31OTWCcHaCAAGJc2VjcDI1NmsxoQPC0eRkjRajDiETr_DRa5N5VJRm-ttCWDoO1QAMMCg5pIN1ZHCCIyg",
    /* Fluffy bootnodes */
    "enr:-I64QBKOAFoNByj98RU-UbHPk7zKl8EoIyqiI1hxgMpJQ4Snf--RWm_qBU9eSRfBv0hhZOdDwgMmYq74kNYMePaRr1uCG5xjZoJpZIJ2NIJpcITCISsggnB2ggABiXNlY3AyNTZrMaECedPKSKkarI7L5lEH2Br2lBU8X7BCz7KP-thSg6pcSNuDdWRwgiOM",
    "enr:-I64QIgwjKS4gH06AhSDUP7SY3C7KfKXmauGNs6QSYXztefacltV9neVVRIfGFI-ICHoeRSVX-NSoktGXETxcPC9ABSCGlJjZoJpZIJ2NIJpcITCISshgnB2ggABiXNlY3AyNTZrMaEDNpAR0-bP1IeILDYjnJUA7yyuJD6aELnIHt5rW7i7lBmDdWRwgiOM",
    "enr:-I64QPjnD7qcuc5Odzh4889v2X-zrjs1KFHqqJQhIaTxNNh6HOx5xokIX6uUYbbAzxt9tkg_MzYt8ZoeIZxiVwRRNTWCHcxjZoJpZIJ2NIJpcITCIStAgnB2ggABiXNlY3AyNTZrMaEDjt4BbmggMkCrHGojSwvR3y21cxXSHqxEhZThnBLilh-DdWRwgiOM",
    /* ethpandaops bootnodes */
    "enr:-LC4QMnoW2m4YYQRPjZhJ5hEpcA6a3V7iQs3slQ1TepzKBIVWQtjpcHsPINc0TcheMCbx6I2n5aax8M3AtUObt74ySUCY6p0IDVhYzI2NzViNGRmMjNhNmEwOWVjNDFkZTRlYTQ2ODQxNjk2ZTQ1YzSCaWSCdjSCaXCEQONKaYlzZWNwMjU2azGhAvZgYbpA9G8NQ6X4agu-R7Ymtu0hcX6xBQ--UEel_b6Pg3VkcIIjKA",
    NULL
};

/* =========================================================================
 * Content store (in-memory, for testing)
 * ========================================================================= */

static bool store_content_id(uint8_t out[32], const uint8_t *key, size_t key_len,
                             void *user_data) {
    (void)user_data;
    return history_content_id_from_key(out, key, key_len);
}

static size_t store_get(const uint8_t *key, size_t key_len,
                        uint8_t *out, size_t cap, void *user_data) {
    (void)key; (void)key_len; (void)out; (void)cap; (void)user_data;
    return 0;  /* we don't store anything yet */
}

static bool store_put(const uint8_t *key, size_t key_len,
                      const uint8_t *data, size_t data_len, void *user_data) {
    (void)key; (void)key_len; (void)data; (void)data_len; (void)user_data;
    return true;
}

/* =========================================================================
 * Signal handling
 * ========================================================================= */

static volatile sig_atomic_t g_stop = 0;
static discv5_engine_t *g_engine = NULL;

static void sig_handler(int sig) {
    (void)sig;
    g_stop = 1;
    if (g_engine) discv5_engine_stop(g_engine);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(int argc, char **argv) {
    uint16_t bind_port = 9009;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            bind_port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            fprintf(stderr, "Usage: %s [--port <udp_port>]\n", argv[0]);
            return 0;
        }
    }

    /* Initialize secp256k1 context */
    secp256k1_wrap_init();

    /* Generate random private key */
    uint8_t privkey[32];
    FILE *rng = fopen("/dev/urandom", "rb");
    if (!rng || fread(privkey, 32, 1, rng) != 1) {
        fprintf(stderr, "Failed to read /dev/urandom\n");
        if (rng) fclose(rng);
        return 1;
    }
    fclose(rng);

    /* Create discv5 engine */
    discv5_engine_t *engine = discv5_engine_create(privkey, bind_port);
    if (!engine) {
        fprintf(stderr, "Failed to create discv5 engine on port %d\n", bind_port);
        return 1;
    }
    g_engine = engine;

    const uint8_t *local_id = discv5_engine_local_id(engine);
    uint16_t actual_port = discv5_engine_udp_port(engine);
    fprintf(stderr, "Portal node started on UDP port %d\n", actual_port);
    fprintf(stderr, "Node ID: ");
    for (int i = 0; i < 32; i++) fprintf(stderr, "%02x", local_id[i]);
    fprintf(stderr, "\n");

    /* Add bootnodes */
    int added = 0;
    for (int i = 0; BOOTNODES[i] != NULL; i++) {
        enr_t enr;
        if (!enr_decode_text(&enr, BOOTNODES[i])) {
            fprintf(stderr, "  bootnode %d: failed to decode ENR\n", i);
            continue;
        }

        uint8_t ip[4], pubkey[33];
        uint16_t port;
        if (!enr_get_endpoint(&enr, ip, &port)) {
            fprintf(stderr, "  bootnode %d: no IP/port in ENR\n", i);
            continue;
        }
        if (!enr_get_pubkey(&enr, pubkey)) {
            fprintf(stderr, "  bootnode %d: no pubkey in ENR\n", i);
            continue;
        }

        if (discv5_engine_add_node(engine, enr.node_id, pubkey, ip, port)) {
            fprintf(stderr, "  bootnode %d: added %d.%d.%d.%d:%d\n",
                    i, ip[0], ip[1], ip[2], ip[3], port);
            added++;
        } else {
            fprintf(stderr, "  bootnode %d: failed to add\n", i);
        }
    }
    fprintf(stderr, "Added %d bootnodes\n\n", added);

    if (added == 0) {
        fprintf(stderr, "No bootnodes available — cannot join network\n");
        discv5_engine_destroy(engine);
        return 1;
    }

    /* Create portal history overlay */
    uint8_t data_radius[32];
    memset(data_radius, 0xFF, 32);  /* accept everything */

    portal_content_store_t store = {
        .content_id_fn = store_content_id,
        .get_fn = store_get,
        .put_fn = store_put,
        .user_data = NULL,
    };

    portal_overlay_t *overlay = portal_overlay_create(
        engine, NULL,
        HISTORY_PROTOCOL_ID, HISTORY_PROTOCOL_ID_LEN,
        local_id, data_radius, &store);

    if (!overlay) {
        fprintf(stderr, "Failed to create history overlay\n");
        discv5_engine_destroy(engine);
        return 1;
    }

    /* Install signal handler */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ping bootnodes to initiate handshake */
    fprintf(stderr, "Pinging bootnodes...\n");
    for (int i = 0; BOOTNODES[i] != NULL && !g_stop; i++) {
        enr_t enr;
        if (enr_decode_text(&enr, BOOTNODES[i])) {
            uint32_t rid = discv5_engine_send_ping(engine, enr.node_id);
            fprintf(stderr, "  PING → bootnode %d (req_id=%u)\n", i, rid);
        }
    }

    /* Run event loop */
    fprintf(stderr, "\nRunning... (Ctrl+C to stop)\n\n");
    discv5_engine_run(engine);

    /* Cleanup */
    fprintf(stderr, "\nShutting down...\n");
    portal_overlay_destroy(overlay);
    discv5_engine_destroy(engine);

    return 0;
}
