#ifndef ART_NET_DISCV5_H
#define ART_NET_DISCV5_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Discv5 Protocol Engine — ties codec + sessions + table + UDP socket.
 *
 * Implements: PING/PONG, FINDNODE/NODES, TALKREQ/TALKRESP.
 * Handles the full handshake state machine (ordinary → WHOAREYOU → handshake).
 * Single-threaded epoll-based event loop.
 */

/* =========================================================================
 * Message types
 * ========================================================================= */

#define DISCV5_MSG_PING     0x01
#define DISCV5_MSG_PONG     0x02
#define DISCV5_MSG_FINDNODE 0x03
#define DISCV5_MSG_NODES    0x04
#define DISCV5_MSG_TALKREQ  0x05
#define DISCV5_MSG_TALKRESP 0x06

/* =========================================================================
 * Message codec (RLP encode/decode)
 * ========================================================================= */

/** Encode PING message: type(1) || RLP([req-id, enr-seq]).
 *  Returns plaintext length, or 0 on error. */
size_t discv5_msg_encode_ping(uint8_t *out, size_t cap,
                                uint32_t req_id, uint64_t enr_seq);

/** Encode PONG message: type(1) || RLP([req-id, enr-seq, ip, port]).
 *  ip is 4 bytes (network order), port is host-order uint16. */
size_t discv5_msg_encode_pong(uint8_t *out, size_t cap,
                                uint32_t req_id, uint64_t enr_seq,
                                const uint8_t ip[4], uint16_t port);

/** Encode FINDNODE: type(1) || RLP([req-id, [dist1, dist2, ...]]).
 *  distances is an array of log-distance values. */
size_t discv5_msg_encode_findnode(uint8_t *out, size_t cap,
                                    uint32_t req_id,
                                    const uint16_t *distances, size_t dist_count);

/** Encode NODES: type(1) || RLP([req-id, total, [enr1, enr2, ...]]).
 *  enrs_raw points to concatenated RLP-encoded ENRs. */
size_t discv5_msg_encode_nodes(uint8_t *out, size_t cap,
                                 uint32_t req_id, uint8_t total,
                                 const uint8_t *enrs_raw, size_t enrs_len);

/** Encode TALKREQ: type(1) || RLP([req-id, protocol, request]). */
size_t discv5_msg_encode_talkreq(uint8_t *out, size_t cap,
                                   uint32_t req_id,
                                   const uint8_t *protocol, size_t proto_len,
                                   const uint8_t *request, size_t req_len);

/** Encode TALKRESP: type(1) || RLP([req-id, response]). */
size_t discv5_msg_encode_talkresp(uint8_t *out, size_t cap,
                                    uint32_t req_id,
                                    const uint8_t *response, size_t resp_len);

/** Decoded message (any type). All data is owned (copied). */
typedef struct {
    uint8_t  type;
    uint32_t req_id;
    union {
        struct { uint64_t enr_seq; } ping;
        struct { uint64_t enr_seq; uint8_t ip[4]; uint16_t port; } pong;
        struct { uint16_t distances[256]; size_t dist_count; } findnode;
        struct { uint8_t total; const uint8_t *enrs; size_t enrs_len; } nodes;
        struct { uint8_t protocol[64]; size_t proto_len;
                 uint8_t data[1024]; size_t data_len; } talkreq;
        struct { uint8_t data[1024]; size_t data_len; } talkresp;
    } body;
} discv5_msg_t;

/** Decode a message from plaintext (type byte + RLP body).
 *  Pointers in the decoded message reference the input buffer. */
bool discv5_msg_decode(discv5_msg_t *msg, const uint8_t *pt, size_t pt_len);

/* =========================================================================
 * Engine (forward declaration — opaque struct)
 * ========================================================================= */

typedef struct discv5_engine discv5_engine_t;

/** TALKREQ handler callback. */
typedef void (*discv5_talk_handler_t)(
    discv5_engine_t *engine,
    const uint8_t peer_id[32],
    uint32_t req_id,
    const uint8_t *request, size_t req_len,
    void *user_data);

/**
 * Create a Discv5 engine.
 *
 * @param privkey   32-byte static private key (secp256k1)
 * @param bind_port UDP port to bind to (0 for ephemeral)
 * @return          Engine instance or NULL on error
 */
discv5_engine_t *discv5_engine_create(const uint8_t privkey[32], uint16_t bind_port);

/** Destroy engine and release resources. */
void discv5_engine_destroy(discv5_engine_t *engine);

/** Get the local node ID (32 bytes). */
const uint8_t *discv5_engine_local_id(const discv5_engine_t *engine);

/** Get the local ENR sequence number. */
uint64_t discv5_engine_enr_seq(const discv5_engine_t *engine);

/**
 * Register a TALKREQ handler for a protocol.
 * The handler is called when a TALKREQ with matching protocol string arrives.
 */
void discv5_engine_register_talk(discv5_engine_t *engine,
                                   const char *protocol,
                                   discv5_talk_handler_t handler,
                                   void *user_data);

/** Add a bootstrap node (ENR or enode URL). */
bool discv5_engine_add_node(discv5_engine_t *engine,
                              const uint8_t node_id[32],
                              const uint8_t pubkey[33],
                              const uint8_t ip[4], uint16_t port);

/** Send PING to a node. Returns request ID. */
uint32_t discv5_engine_send_ping(discv5_engine_t *engine,
                                   const uint8_t node_id[32]);

/** Send FINDNODE to a node. Returns request ID. */
uint32_t discv5_engine_send_findnode(discv5_engine_t *engine,
                                       const uint8_t node_id[32],
                                       const uint16_t *distances, size_t count);

/** Send TALKREQ to a node. Returns request ID. */
uint32_t discv5_engine_send_talkreq(discv5_engine_t *engine,
                                      const uint8_t node_id[32],
                                      const char *protocol,
                                      const uint8_t *data, size_t data_len);

/** Send TALKRESP (reply to a TALKREQ). */
bool discv5_engine_send_talkresp(discv5_engine_t *engine,
                                   const uint8_t node_id[32],
                                   uint32_t req_id,
                                   const uint8_t *data, size_t data_len);

/**
 * Process a single incoming UDP packet.
 * Called by the event loop or by test code.
 */
void discv5_engine_on_packet(discv5_engine_t *engine,
                               const uint8_t *data, size_t len,
                               const uint8_t src_ip[4], uint16_t src_port);

/**
 * Run the event loop (blocking). Returns when discv5_engine_stop() is called.
 */
void discv5_engine_run(discv5_engine_t *engine);

/** Signal the event loop to stop. Thread-safe. */
void discv5_engine_stop(discv5_engine_t *engine);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_DISCV5_H */
