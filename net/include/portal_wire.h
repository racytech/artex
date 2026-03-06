#ifndef ART_NET_PORTAL_WIRE_H
#define ART_NET_PORTAL_WIRE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Portal Wire Protocol — message codec for Portal Network.
 *
 * All 8 message types are SSZ-encoded with a 1-byte message ID prefix.
 * Wire format: message_id (1 byte) || SSZ-encoded body.
 *
 * Rides over Discv5 TALKREQ/TALKRESP.
 */

/* =========================================================================
 * Constants
 * ========================================================================= */

/* Message IDs */
#define PORTAL_MSG_PING          0x00
#define PORTAL_MSG_PONG          0x01
#define PORTAL_MSG_FIND_NODES    0x02
#define PORTAL_MSG_NODES         0x03
#define PORTAL_MSG_FIND_CONTENT  0x04
#define PORTAL_MSG_CONTENT       0x05
#define PORTAL_MSG_OFFER         0x06
#define PORTAL_MSG_ACCEPT        0x07

/* PING/PONG custom payload types */
#define PORTAL_PAYLOAD_TYPE0     0   /* client_info + radius + capabilities */
#define PORTAL_PAYLOAD_TYPE1     1   /* basic radius only */

/* CONTENT union selectors */
#define PORTAL_CONTENT_CONNID    0
#define PORTAL_CONTENT_DATA      1
#define PORTAL_CONTENT_ENRS      2

/* ACCEPT content key response codes */
#define PORTAL_ACCEPT_OK              0
#define PORTAL_ACCEPT_DECLINE         1
#define PORTAL_ACCEPT_ALREADY_STORED  2
#define PORTAL_ACCEPT_NOT_IN_RADIUS   3
#define PORTAL_ACCEPT_RATE_LIMIT      4
#define PORTAL_ACCEPT_INBOUND_LIMIT   5
#define PORTAL_ACCEPT_NOT_VERIFIABLE  6

/* Limits */
#define PORTAL_MAX_ENRS          32
#define PORTAL_MAX_OFFER_KEYS    64
#define PORTAL_MAX_DISTANCES     256

/* =========================================================================
 * Encode functions
 * ========================================================================= */

/**
 * Encode a PING message.
 * @param payload  Pre-encoded custom payload (Type 0 or Type 1)
 * @return         Total encoded length, or 0 on error
 */
size_t portal_encode_ping(uint8_t *out, size_t cap,
                          uint64_t enr_seq, uint16_t payload_type,
                          const uint8_t *payload, size_t payload_len);

/** Encode a PONG message (same layout as PING). */
size_t portal_encode_pong(uint8_t *out, size_t cap,
                          uint64_t enr_seq, uint16_t payload_type,
                          const uint8_t *payload, size_t payload_len);

/** Encode a FIND_NODES message. */
size_t portal_encode_find_nodes(uint8_t *out, size_t cap,
                                const uint16_t *distances, size_t count);

/** Encode a NODES response. */
size_t portal_encode_nodes(uint8_t *out, size_t cap,
                           uint8_t total,
                           const uint8_t *const *enrs,
                           const size_t *enr_lens,
                           size_t enr_count);

/** Encode a FIND_CONTENT message. */
size_t portal_encode_find_content(uint8_t *out, size_t cap,
                                  const uint8_t *content_key, size_t key_len);

/** Encode CONTENT response — connection ID variant. */
size_t portal_encode_content_connid(uint8_t *out, size_t cap,
                                    const uint8_t conn_id[2]);

/** Encode CONTENT response — raw content data variant. */
size_t portal_encode_content_data(uint8_t *out, size_t cap,
                                  const uint8_t *data, size_t len);

/** Encode CONTENT response — ENR list variant. */
size_t portal_encode_content_enrs(uint8_t *out, size_t cap,
                                  const uint8_t *const *enrs,
                                  const size_t *enr_lens,
                                  size_t enr_count);

/** Encode an OFFER message. */
size_t portal_encode_offer(uint8_t *out, size_t cap,
                           const uint8_t *const *keys,
                           const size_t *key_lens,
                           size_t key_count);

/** Encode an ACCEPT response. */
size_t portal_encode_accept(uint8_t *out, size_t cap,
                            const uint8_t conn_id[2],
                            const uint8_t *content_keys, size_t keys_len);

/* =========================================================================
 * Payload helpers
 * ========================================================================= */

/**
 * Encode Type 0 payload: client_info + data_radius + capabilities.
 * @param client_info   UTF-8 string (may be NULL or empty)
 * @param data_radius   U256 (32 bytes, little-endian)
 * @param capabilities  Array of enabled extension type IDs
 * @param cap_count     Number of capabilities
 */
size_t portal_encode_payload_type0(uint8_t *out, size_t cap,
                                   const char *client_info,
                                   const uint8_t data_radius[32],
                                   const uint16_t *capabilities,
                                   size_t cap_count);

/**
 * Encode Type 1 payload: data_radius only.
 * @param data_radius  U256 (32 bytes, little-endian)
 */
size_t portal_encode_payload_type1(uint8_t *out, size_t cap,
                                   const uint8_t data_radius[32]);

/* =========================================================================
 * Decoded message
 * ========================================================================= */

/** A (pointer, length) pair into the input buffer. */
typedef struct {
    const uint8_t *data;
    size_t len;
} portal_bytes_t;

/**
 * Decoded Portal wire message.
 *
 * Zero-copy: byte pointers reference the input buffer.
 * PING and PONG both decode into the .ping member.
 */
typedef struct {
    uint8_t msg_id;
    union {
        /* PING (0x00) and PONG (0x01) */
        struct {
            uint64_t enr_seq;
            uint16_t payload_type;
            const uint8_t *payload;
            size_t payload_len;
        } ping;

        /* FIND_NODES (0x02) */
        struct {
            uint16_t distances[PORTAL_MAX_DISTANCES];
            size_t count;
        } find_nodes;

        /* NODES (0x03) */
        struct {
            uint8_t total;
            size_t enr_count;
            portal_bytes_t enrs[PORTAL_MAX_ENRS];
        } nodes;

        /* FIND_CONTENT (0x04) */
        struct {
            const uint8_t *key;
            size_t key_len;
        } find_content;

        /* CONTENT (0x05) — union */
        struct {
            uint8_t selector;
            union {
                uint8_t conn_id[2];            /* selector 0 */
                portal_bytes_t payload;        /* selector 1 */
                struct {
                    size_t count;
                    portal_bytes_t enrs[PORTAL_MAX_ENRS];
                } enr_list;                    /* selector 2 */
            };
        } content;

        /* OFFER (0x06) */
        struct {
            size_t key_count;
            portal_bytes_t keys[PORTAL_MAX_OFFER_KEYS];
        } offer;

        /* ACCEPT (0x07) */
        struct {
            uint8_t conn_id[2];
            const uint8_t *content_keys;
            size_t keys_len;
        } accept;
    };
} portal_msg_t;

/**
 * Decode any Portal wire message.
 * Zero-copy: payload pointers reference the input buffer.
 */
bool portal_decode(portal_msg_t *msg, const uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_PORTAL_WIRE_H */
