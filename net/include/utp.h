#ifndef ART_NET_UTP_H
#define ART_NET_UTP_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * uTP (micro Transport Protocol) — BEP29 over Discv5 TALKREQ.
 *
 * Provides reliable ordered byte-stream delivery over unreliable
 * UDP transport. Adapted for Portal Network: connection_id passed
 * out-of-band, connections keyed by (peer_id, conn_id).
 */

/* =========================================================================
 * Constants
 * ========================================================================= */

#define UTP_HEADER_SIZE   20
#define UTP_VERSION       1
#define UTP_MAX_SACK_LEN  32    /* max selective ACK bitmask bytes */

/* Packet types */
#define UTP_ST_DATA   0
#define UTP_ST_FIN    1
#define UTP_ST_STATE  2
#define UTP_ST_RESET  3
#define UTP_ST_SYN    4

/* Extension types */
#define UTP_EXT_NONE  0
#define UTP_EXT_SACK  1

/* Tuning */
#define UTP_MAX_CONNS       64
#define UTP_MAX_LISTENERS   64
#define UTP_SEND_BUF_SIZE   64      /* packets in send ring buffer */
#define UTP_RECV_BUF_MAX    (1024 * 1024)  /* 1MB max receive buffer */
#define UTP_MAX_PAYLOAD     1200    /* conservative for UDP */
#define UTP_DEFAULT_WND     (1024 * 1024)  /* 1MB advertised window */
#define UTP_INITIAL_RTO_US  1000000 /* 1 second initial RTO */
#define UTP_MIN_RTO_US      500000  /* 500ms minimum RTO */

/* =========================================================================
 * Packet codec
 * ========================================================================= */

/** uTP packet header (20 bytes on wire). */
typedef struct {
    uint8_t  type;       /* UTP_ST_DATA .. UTP_ST_SYN */
    uint8_t  version;    /* always UTP_VERSION (1) */
    uint8_t  extension;  /* 0=none, 1=selective ack */
    uint16_t conn_id;
    uint32_t timestamp;       /* microseconds */
    uint32_t timestamp_diff;  /* microseconds */
    uint32_t wnd_size;
    uint16_t seq_nr;
    uint16_t ack_nr;
} utp_header_t;

/** Selective ACK extension. */
typedef struct {
    uint8_t bitmask[UTP_MAX_SACK_LEN];
    size_t  len;  /* bytes, must be multiple of 4 */
} utp_sack_t;

/** Decoded packet (header + optional extension + payload pointer). */
typedef struct {
    utp_header_t  hdr;
    utp_sack_t    sack;
    bool          has_sack;
    const uint8_t *payload;      /* points into input buffer */
    size_t        payload_len;
} utp_packet_t;

/**
 * Encode a uTP packet.
 * @param sack    Selective ACK extension, or NULL for none
 * @return        Total encoded length, or 0 on error
 */
size_t utp_encode(uint8_t *out, size_t cap,
                  const utp_header_t *hdr,
                  const utp_sack_t *sack,
                  const uint8_t *payload, size_t payload_len);

/**
 * Decode a uTP packet from raw bytes.
 * Payload pointer references the input buffer (zero-copy).
 */
bool utp_decode(utp_packet_t *pkt, const uint8_t *data, size_t len);

/* =========================================================================
 * Connection
 * ========================================================================= */

/** Connection state. */
typedef enum {
    UTP_STATE_IDLE,
    UTP_STATE_SYN_SENT,
    UTP_STATE_SYN_RECV,
    UTP_STATE_CONNECTED,
    UTP_STATE_FIN_SENT,
    UTP_STATE_CLOSED,
    UTP_STATE_RESET,
} utp_state_t;

typedef struct utp_conn utp_conn_t;
typedef struct utp_ctx  utp_ctx_t;

/** Get connection state. */
utp_state_t utp_conn_state(const utp_conn_t *conn);

/** Get connection ID (our recv ID). */
uint16_t utp_conn_id(const utp_conn_t *conn);

/* =========================================================================
 * Context (connection manager)
 * ========================================================================= */

/**
 * Callback: send raw uTP packet bytes to a peer.
 * Called by the uTP engine when it needs to transmit.
 */
typedef void (*utp_send_fn)(const uint8_t peer_id[32],
                            const uint8_t *data, size_t len,
                            void *user_data);

/**
 * Callback: data received on a connection.
 * Called when FIN is received (complete transfer) or on RESET/error.
 * @param conn_id  The out-of-band connection ID
 * @param data     Received data (NULL on error/reset)
 * @param len      Data length (0 on error/reset)
 */
typedef void (*utp_data_fn)(uint16_t conn_id,
                            const uint8_t *data, size_t len,
                            void *user_data);

/** Create a uTP context with a send callback. */
utp_ctx_t *utp_ctx_create(utp_send_fn send_fn, void *send_data);

/** Destroy context and all connections. */
void utp_ctx_destroy(utp_ctx_t *ctx);

/**
 * Initiate an outgoing connection (send SYN).
 * @param conn_id  Out-of-band connection ID (our conn_id_recv)
 * @return         Connection handle or NULL on error
 */
utp_conn_t *utp_connect(utp_ctx_t *ctx,
                         const uint8_t peer_id[32],
                         uint16_t conn_id);

/**
 * Listen for an incoming connection from a specific peer.
 * @param conn_id   Out-of-band connection ID (responder's conn_id_send)
 * @param on_data   Called when transfer completes (FIN received)
 * @return          true if listener registered
 */
bool utp_listen(utp_ctx_t *ctx,
                const uint8_t peer_id[32],
                uint16_t conn_id,
                utp_data_fn on_data, void *user_data);

/**
 * Process an incoming uTP packet.
 * Called from the Discv5 TALKREQ handler.
 */
void utp_on_packet(utp_ctx_t *ctx,
                   const uint8_t peer_id[32],
                   const uint8_t *data, size_t len);

/**
 * Write data to an established connection.
 * Data is buffered internally and sent with flow control.
 * @return  Bytes accepted, or -1 on error
 */
ssize_t utp_write(utp_conn_t *conn,
                  const uint8_t *data, size_t len);

/**
 * Close a connection (sends FIN after all data is ACKed).
 */
void utp_close(utp_conn_t *conn);

/**
 * Timer tick — call periodically for retransmission and timeouts.
 * @param now_us  Current time in microseconds
 */
void utp_tick(utp_ctx_t *ctx, uint64_t now_us);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_UTP_H */
