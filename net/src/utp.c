/*
 * uTP (micro Transport Protocol) — BEP29 over Discv5 TALKREQ.
 *
 * Three layers:
 *   1. Packet codec — encode/decode 20-byte headers + extensions
 *   2. Connection   — per-connection state machine, send/recv buffers
 *   3. Context      — connection manager, routing, timer integration
 */

#include "../include/utp.h"
#include <string.h>
#include <stdlib.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void put_be16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)(v);
}

static void put_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v);
}

static uint16_t get_be16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | p[1];
}

static uint32_t get_be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  | p[3];
}

/* Sequence number comparison (handles wraparound). */
static int16_t seq_diff(uint16_t a, uint16_t b) {
    return (int16_t)(a - b);
}

/* =========================================================================
 * Layer 1: Packet Codec
 * ========================================================================= */

size_t utp_encode(uint8_t *out, size_t cap,
                  const utp_header_t *hdr,
                  const utp_sack_t *sack,
                  const uint8_t *payload, size_t payload_len) {
    size_t ext_len = 0;
    if (sack && sack->len > 0)
        ext_len = 2 + sack->len;  /* next_ext(1) + length(1) + bitmask */

    size_t total = UTP_HEADER_SIZE + ext_len + payload_len;
    if (total > cap) return 0;

    /* Byte 0: (type << 4) | version */
    out[0] = (uint8_t)((hdr->type << 4) | (hdr->version & 0x0F));

    /* Byte 1: extension */
    out[1] = (sack && sack->len > 0) ? UTP_EXT_SACK : UTP_EXT_NONE;

    /* Bytes 2-19: header fields */
    put_be16(out + 2,  hdr->conn_id);
    put_be32(out + 4,  hdr->timestamp);
    put_be32(out + 8,  hdr->timestamp_diff);
    put_be32(out + 12, hdr->wnd_size);
    put_be16(out + 16, hdr->seq_nr);
    put_be16(out + 18, hdr->ack_nr);

    /* Extension */
    size_t pos = UTP_HEADER_SIZE;
    if (sack && sack->len > 0) {
        out[pos++] = UTP_EXT_NONE;  /* next extension = none */
        out[pos++] = (uint8_t)sack->len;
        memcpy(out + pos, sack->bitmask, sack->len);
        pos += sack->len;
    }

    /* Payload */
    if (payload_len > 0 && payload)
        memcpy(out + pos, payload, payload_len);

    return total;
}

bool utp_decode(utp_packet_t *pkt, const uint8_t *data, size_t len) {
    if (len < UTP_HEADER_SIZE) return false;

    memset(pkt, 0, sizeof(*pkt));

    pkt->hdr.type    = (data[0] >> 4) & 0x0F;
    pkt->hdr.version = data[0] & 0x0F;
    pkt->hdr.extension = data[1];

    if (pkt->hdr.version != UTP_VERSION) return false;
    if (pkt->hdr.type > UTP_ST_SYN) return false;

    pkt->hdr.conn_id       = get_be16(data + 2);
    pkt->hdr.timestamp      = get_be32(data + 4);
    pkt->hdr.timestamp_diff = get_be32(data + 8);
    pkt->hdr.wnd_size       = get_be32(data + 12);
    pkt->hdr.seq_nr         = get_be16(data + 16);
    pkt->hdr.ack_nr         = get_be16(data + 18);

    /* Parse extensions */
    size_t pos = UTP_HEADER_SIZE;
    uint8_t ext_type = pkt->hdr.extension;

    while (ext_type != UTP_EXT_NONE) {
        if (pos + 2 > len) return false;
        uint8_t next_ext = data[pos];
        uint8_t ext_len  = data[pos + 1];
        pos += 2;

        if (pos + ext_len > len) return false;

        if (ext_type == UTP_EXT_SACK && ext_len > 0) {
            pkt->has_sack = true;
            pkt->sack.len = ext_len > UTP_MAX_SACK_LEN ? UTP_MAX_SACK_LEN : ext_len;
            memcpy(pkt->sack.bitmask, data + pos, pkt->sack.len);
        }

        pos += ext_len;
        ext_type = next_ext;
    }

    /* Remaining bytes = payload */
    pkt->payload = (pos < len) ? data + pos : NULL;
    pkt->payload_len = (pos < len) ? len - pos : 0;

    return true;
}

/* =========================================================================
 * Layer 2: Connection internals
 * ========================================================================= */

/* Send buffer entry */
typedef struct {
    uint16_t seq_nr;
    uint8_t  data[UTP_MAX_PAYLOAD];
    size_t   data_len;
    uint64_t sent_at;   /* microseconds */
    bool     acked;
    uint8_t  type;      /* packet type (ST_DATA, ST_FIN, ST_SYN) */
} utp_send_entry_t;

/* Out-of-order received packet tracking */
#define UTP_OOO_MAX 64

typedef struct {
    uint16_t seq_nr;
    uint8_t  data[UTP_MAX_PAYLOAD];
    size_t   data_len;
    bool     used;
} utp_ooo_entry_t;

/* User write buffer (accumulated before sending) */
typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
    size_t   pos;   /* next byte to send */
} utp_write_buf_t;

/* Pending listener */
typedef struct {
    uint8_t     peer_id[32];
    uint16_t    conn_id;
    utp_data_fn on_data;
    void       *user_data;
    bool        in_use;
} utp_listener_t;

struct utp_conn {
    utp_state_t state;
    utp_ctx_t  *ctx;

    /* Peer identity */
    uint8_t  peer_id[32];
    uint16_t conn_id_recv;   /* packets we receive have this ID */
    uint16_t conn_id_send;   /* packets we send have this ID */

    /* Sequence numbers */
    uint16_t seq_nr;         /* next seq to send */
    uint16_t ack_nr;         /* last in-order seq received */

    /* Flow control */
    uint32_t peer_wnd;       /* peer's advertised window */
    uint32_t our_wnd;        /* our advertised window */
    uint32_t cwnd;           /* congestion window (bytes) */
    uint32_t ssthresh;       /* slow start threshold */
    uint32_t bytes_in_flight;

    /* Timestamps */
    uint32_t last_recv_timestamp;
    uint32_t last_timestamp_diff;

    /* RTT estimation */
    int64_t  srtt;           /* smoothed RTT (microseconds), -1 = unset */
    int64_t  rttvar;         /* RTT variance */
    uint64_t rto;            /* retransmission timeout */

    /* Send buffer (ring) */
    utp_send_entry_t send_buf[UTP_SEND_BUF_SIZE];
    uint16_t send_start;     /* oldest unACKed index */
    uint16_t send_count;     /* number of entries in buffer */

    /* Receive buffer */
    uint8_t *recv_buf;
    size_t   recv_len;
    size_t   recv_cap;

    /* Out-of-order tracking */
    utp_ooo_entry_t ooo[UTP_OOO_MAX];

    /* Pending close */
    bool     fin_pending;    /* user called utp_close() */
    bool     fin_sent;       /* FIN packet queued in send buffer */

    /* Callback for received data */
    utp_data_fn on_data;
    void       *on_data_ud;

    bool in_use;
};

/* Context definition (after utp_conn so it can embed the array) */
struct utp_ctx {
    utp_send_fn  send_fn;
    void         *send_data;

    utp_conn_t   conns[UTP_MAX_CONNS];
    utp_listener_t listeners[UTP_MAX_LISTENERS];
    utp_write_buf_t write_bufs[UTP_MAX_CONNS];

    uint64_t     last_tick;
};

/* =========================================================================
 * Timestamp helpers
 * ========================================================================= */

static uint32_t utp_timestamp_us(uint64_t now_us) {
    return (uint32_t)(now_us & 0xFFFFFFFF);
}

/* =========================================================================
 * Connection: send a raw packet
 * ========================================================================= */

static void conn_send_packet(utp_conn_t *c, uint8_t type,
                              const uint8_t *payload, size_t payload_len,
                              uint64_t now_us) {
    utp_header_t hdr = {
        .type          = type,
        .version       = UTP_VERSION,
        .extension     = UTP_EXT_NONE,
        .conn_id       = c->conn_id_send,
        .timestamp     = utp_timestamp_us(now_us),
        .timestamp_diff = c->last_timestamp_diff,
        .wnd_size      = c->our_wnd,
        .seq_nr        = (type == UTP_ST_STATE) ? c->seq_nr : c->seq_nr,
        .ack_nr        = c->ack_nr,
    };

    /* SYN uses conn_id_recv (special case per BEP29) */
    if (type == UTP_ST_SYN)
        hdr.conn_id = c->conn_id_recv;

    /* ST_STATE doesn't consume a sequence number */
    if (type != UTP_ST_STATE) {
        hdr.seq_nr = c->seq_nr;
    }

    /* Build selective ACK if we have out-of-order packets */
    utp_sack_t sack;
    utp_sack_t *sack_ptr = NULL;
    memset(&sack, 0, sizeof(sack));

    int has_ooo = 0;
    for (int i = 0; i < UTP_OOO_MAX; i++) {
        if (c->ooo[i].used) { has_ooo = 1; break; }
    }

    if (has_ooo && type == UTP_ST_STATE) {
        sack.len = 4;  /* minimum 4 bytes */
        for (int i = 0; i < UTP_OOO_MAX; i++) {
            if (!c->ooo[i].used) continue;
            int16_t diff = seq_diff(c->ooo[i].seq_nr, c->ack_nr);
            if (diff >= 2 && diff < 34) {
                int bit_pos = diff - 2;
                int byte_idx = bit_pos / 8;
                int bit_idx  = bit_pos % 8;
                if (byte_idx < (int)sack.len)
                    sack.bitmask[byte_idx] |= (1 << bit_idx);
            }
        }
        hdr.extension = UTP_EXT_SACK;
        sack_ptr = &sack;
    }

    uint8_t buf[UTP_HEADER_SIZE + 2 + UTP_MAX_SACK_LEN + UTP_MAX_PAYLOAD];
    size_t pkt_len = utp_encode(buf, sizeof(buf), &hdr, sack_ptr,
                                 payload, payload_len);
    if (pkt_len == 0) return;

    c->ctx->send_fn(c->peer_id, buf, pkt_len, c->ctx->send_data);
}

/* Buffer a data/FIN/SYN packet in the send ring for retransmission. */
static bool conn_buffer_send(utp_conn_t *c, uint8_t type,
                              const uint8_t *data, size_t data_len,
                              uint64_t now_us) {
    if (c->send_count >= UTP_SEND_BUF_SIZE)
        return false;

    uint16_t idx = (c->send_start + c->send_count) % UTP_SEND_BUF_SIZE;
    utp_send_entry_t *e = &c->send_buf[idx];

    e->seq_nr   = c->seq_nr;
    e->type     = type;
    e->data_len = data_len;
    e->sent_at  = now_us;
    e->acked    = false;
    if (data_len > 0 && data)
        memcpy(e->data, data, data_len);

    c->send_count++;
    c->seq_nr++;
    c->bytes_in_flight += data_len;

    /* Actually transmit */
    conn_send_packet(c, type, data, data_len, now_us);
    return true;
}

/* =========================================================================
 * Connection: process ACKs
 * ========================================================================= */

static void conn_process_ack(utp_conn_t *c, uint16_t ack_nr,
                              const utp_sack_t *sack, uint64_t now_us) {
    /* ACK all packets up to and including ack_nr */
    while (c->send_count > 0) {
        utp_send_entry_t *e = &c->send_buf[c->send_start];
        if (seq_diff(ack_nr, e->seq_nr) < 0)
            break;  /* not yet ACKed */

        if (!e->acked) {
            e->acked = true;
            c->bytes_in_flight -= e->data_len;

            /* Update RTT */
            if (e->sent_at > 0 && now_us > e->sent_at) {
                int64_t rtt = (int64_t)(now_us - e->sent_at);
                if (c->srtt < 0) {
                    c->srtt = rtt;
                    c->rttvar = rtt / 2;
                } else {
                    int64_t delta = rtt - c->srtt;
                    if (delta < 0) delta = -delta;
                    c->rttvar += (delta - c->rttvar) / 4;
                    c->srtt += (rtt - c->srtt) / 8;
                }
                c->rto = (uint64_t)(c->srtt + 4 * c->rttvar);
                if (c->rto < UTP_MIN_RTO_US) c->rto = UTP_MIN_RTO_US;
            }

            /* Congestion window growth */
            if (c->cwnd < c->ssthresh) {
                /* Slow start: increase by one MSS per ACK */
                c->cwnd += UTP_MAX_PAYLOAD;
            } else {
                /* Congestion avoidance: linear increase */
                c->cwnd += UTP_MAX_PAYLOAD * UTP_MAX_PAYLOAD / c->cwnd;
            }
        }

        c->send_start = (c->send_start + 1) % UTP_SEND_BUF_SIZE;
        c->send_count--;
    }

    /* Process selective ACK bitmask */
    if (sack && sack->len > 0) {
        for (size_t byte_i = 0; byte_i < sack->len; byte_i++) {
            for (int bit_i = 0; bit_i < 8; bit_i++) {
                if (!(sack->bitmask[byte_i] & (1 << bit_i)))
                    continue;
                uint16_t sacked_nr = ack_nr + 2 + (uint16_t)(byte_i * 8 + bit_i);

                /* Find and mark in send buffer */
                for (uint16_t j = 0; j < c->send_count; j++) {
                    uint16_t idx = (c->send_start + j) % UTP_SEND_BUF_SIZE;
                    if (c->send_buf[idx].seq_nr == sacked_nr &&
                        !c->send_buf[idx].acked) {
                        c->send_buf[idx].acked = true;
                        c->bytes_in_flight -= c->send_buf[idx].data_len;
                    }
                }
            }
        }
    }

    /* Check if FIN was ACKed */
    if (c->state == UTP_STATE_FIN_SENT && c->send_count == 0) {
        c->state = UTP_STATE_CLOSED;
    }
}

/* =========================================================================
 * Connection: receive data
 * ========================================================================= */

static bool conn_recv_data(utp_conn_t *c, uint16_t seq_nr,
                            const uint8_t *data, size_t data_len) {
    if (data_len == 0) return true;

    int16_t diff = seq_diff(seq_nr, c->ack_nr);

    if (diff == 1) {
        /* Next expected packet — append to receive buffer */
        if (c->recv_len + data_len > UTP_RECV_BUF_MAX)
            return false;
        if (c->recv_len + data_len > c->recv_cap) {
            size_t new_cap = c->recv_cap ? c->recv_cap * 2 : 4096;
            while (new_cap < c->recv_len + data_len) new_cap *= 2;
            if (new_cap > UTP_RECV_BUF_MAX) new_cap = UTP_RECV_BUF_MAX;
            uint8_t *nb = realloc(c->recv_buf, new_cap);
            if (!nb) return false;
            c->recv_buf = nb;
            c->recv_cap = new_cap;
        }
        memcpy(c->recv_buf + c->recv_len, data, data_len);
        c->recv_len += data_len;
        c->ack_nr = seq_nr;

        /* Check for buffered out-of-order packets that are now in order */
        bool found;
        do {
            found = false;
            for (int i = 0; i < UTP_OOO_MAX; i++) {
                if (!c->ooo[i].used) continue;
                if (seq_diff(c->ooo[i].seq_nr, c->ack_nr) == 1) {
                    /* This is the next expected packet */
                    if (c->recv_len + c->ooo[i].data_len <= UTP_RECV_BUF_MAX) {
                        if (c->recv_len + c->ooo[i].data_len > c->recv_cap) {
                            size_t new_cap = c->recv_cap * 2;
                            while (new_cap < c->recv_len + c->ooo[i].data_len)
                                new_cap *= 2;
                            if (new_cap > UTP_RECV_BUF_MAX) new_cap = UTP_RECV_BUF_MAX;
                            uint8_t *nb = realloc(c->recv_buf, new_cap);
                            if (!nb) break;
                            c->recv_buf = nb;
                            c->recv_cap = new_cap;
                        }
                        memcpy(c->recv_buf + c->recv_len,
                               c->ooo[i].data, c->ooo[i].data_len);
                        c->recv_len += c->ooo[i].data_len;
                        c->ack_nr = c->ooo[i].seq_nr;
                    }
                    c->ooo[i].used = false;
                    found = true;
                }
            }
        } while (found);

        return true;
    }

    if (diff > 1) {
        /* Out of order — buffer for later */
        for (int i = 0; i < UTP_OOO_MAX; i++) {
            if (c->ooo[i].used && c->ooo[i].seq_nr == seq_nr)
                return true;  /* duplicate */
        }
        for (int i = 0; i < UTP_OOO_MAX; i++) {
            if (!c->ooo[i].used) {
                c->ooo[i].seq_nr = seq_nr;
                c->ooo[i].data_len = data_len > UTP_MAX_PAYLOAD ? UTP_MAX_PAYLOAD : data_len;
                memcpy(c->ooo[i].data, data, c->ooo[i].data_len);
                c->ooo[i].used = true;
                return true;
            }
        }
        return false;  /* OOO buffer full */
    }

    /* diff <= 0: duplicate or old packet, ignore */
    return true;
}

/* =========================================================================
 * Connection: handle incoming packet
 * ========================================================================= */

static void conn_on_packet(utp_conn_t *c, const utp_packet_t *pkt,
                            uint64_t now_us) {
    /* Update timestamp tracking */
    c->last_recv_timestamp = pkt->hdr.timestamp;
    c->last_timestamp_diff = utp_timestamp_us(now_us) - pkt->hdr.timestamp;
    c->peer_wnd = pkt->hdr.wnd_size;

    switch (c->state) {
    case UTP_STATE_SYN_SENT:
        /* Expecting ST_STATE in reply to our SYN */
        if (pkt->hdr.type == UTP_ST_STATE) {
            conn_process_ack(c, pkt->hdr.ack_nr,
                              pkt->has_sack ? &pkt->sack : NULL, now_us);
            /* Portal deviation: ack_nr = pkt.seq_nr - 1 */
            c->ack_nr = pkt->hdr.seq_nr - 1;
            c->state = UTP_STATE_CONNECTED;
        } else if (pkt->hdr.type == UTP_ST_RESET) {
            c->state = UTP_STATE_RESET;
        }
        break;

    case UTP_STATE_SYN_RECV:
    case UTP_STATE_CONNECTED:
        if (pkt->hdr.type == UTP_ST_DATA) {
            conn_recv_data(c, pkt->hdr.seq_nr,
                           pkt->payload, pkt->payload_len);
            conn_process_ack(c, pkt->hdr.ack_nr,
                              pkt->has_sack ? &pkt->sack : NULL, now_us);
            /* Send ACK */
            conn_send_packet(c, UTP_ST_STATE, NULL, 0, now_us);
            if (c->state == UTP_STATE_SYN_RECV)
                c->state = UTP_STATE_CONNECTED;
        } else if (pkt->hdr.type == UTP_ST_STATE) {
            conn_process_ack(c, pkt->hdr.ack_nr,
                              pkt->has_sack ? &pkt->sack : NULL, now_us);
        } else if (pkt->hdr.type == UTP_ST_FIN) {
            conn_recv_data(c, pkt->hdr.seq_nr,
                           pkt->payload, pkt->payload_len);
            conn_process_ack(c, pkt->hdr.ack_nr,
                              pkt->has_sack ? &pkt->sack : NULL, now_us);
            c->ack_nr = pkt->hdr.seq_nr;
            /* Send ACK for FIN */
            conn_send_packet(c, UTP_ST_STATE, NULL, 0, now_us);
            c->state = UTP_STATE_CLOSED;
            /* Deliver received data */
            if (c->on_data) {
                c->on_data(c->conn_id_recv, c->recv_buf, c->recv_len,
                           c->on_data_ud);
            }
        } else if (pkt->hdr.type == UTP_ST_RESET) {
            c->state = UTP_STATE_RESET;
            if (c->on_data)
                c->on_data(c->conn_id_recv, NULL, 0, c->on_data_ud);
        }
        break;

    case UTP_STATE_FIN_SENT:
        if (pkt->hdr.type == UTP_ST_STATE) {
            conn_process_ack(c, pkt->hdr.ack_nr,
                              pkt->has_sack ? &pkt->sack : NULL, now_us);
            /* conn_process_ack sets CLOSED when send_count hits 0 */
        } else if (pkt->hdr.type == UTP_ST_DATA) {
            conn_recv_data(c, pkt->hdr.seq_nr,
                           pkt->payload, pkt->payload_len);
            conn_send_packet(c, UTP_ST_STATE, NULL, 0, now_us);
        } else if (pkt->hdr.type == UTP_ST_RESET) {
            c->state = UTP_STATE_RESET;
        }
        break;

    default:
        break;
    }
}

/* =========================================================================
 * Connection: flush pending writes
 * ========================================================================= */

static void conn_flush(utp_conn_t *c, uint64_t now_us);

/* =========================================================================
 * Layer 3: Context
 * ========================================================================= */

utp_ctx_t *utp_ctx_create(utp_send_fn send_fn, void *send_data) {
    utp_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->send_fn = send_fn;
    ctx->send_data = send_data;
    return ctx;
}

void utp_ctx_destroy(utp_ctx_t *ctx) {
    if (!ctx) return;
    for (int i = 0; i < UTP_MAX_CONNS; i++) {
        if (ctx->conns[i].in_use) {
            free(ctx->conns[i].recv_buf);
        }
        free(ctx->write_bufs[i].data);
    }
    free(ctx);
}

static utp_conn_t *ctx_alloc_conn(utp_ctx_t *ctx) {
    for (int i = 0; i < UTP_MAX_CONNS; i++) {
        if (!ctx->conns[i].in_use) {
            memset(&ctx->conns[i], 0, sizeof(utp_conn_t));
            ctx->conns[i].in_use = true;
            ctx->conns[i].ctx = ctx;
            ctx->conns[i].srtt = -1;
            ctx->conns[i].rto = UTP_INITIAL_RTO_US;
            ctx->conns[i].our_wnd = UTP_DEFAULT_WND;
            ctx->conns[i].peer_wnd = UTP_DEFAULT_WND;
            ctx->conns[i].cwnd = 2 * UTP_MAX_PAYLOAD;
            ctx->conns[i].ssthresh = UTP_DEFAULT_WND;
            return &ctx->conns[i];
        }
    }
    return NULL;
}

static utp_conn_t *ctx_find_conn(utp_ctx_t *ctx, const uint8_t peer_id[32],
                                  uint16_t conn_id) {
    for (int i = 0; i < UTP_MAX_CONNS; i++) {
        if (!ctx->conns[i].in_use) continue;
        if (ctx->conns[i].conn_id_recv == conn_id &&
            memcmp(ctx->conns[i].peer_id, peer_id, 32) == 0)
            return &ctx->conns[i];
    }
    return NULL;
}

static utp_listener_t *ctx_find_listener(utp_ctx_t *ctx,
                                          const uint8_t peer_id[32],
                                          uint16_t conn_id) {
    for (int i = 0; i < UTP_MAX_LISTENERS; i++) {
        if (!ctx->listeners[i].in_use) continue;
        if (ctx->listeners[i].conn_id == conn_id &&
            memcmp(ctx->listeners[i].peer_id, peer_id, 32) == 0)
            return &ctx->listeners[i];
    }
    return NULL;
}

static int ctx_conn_index(utp_ctx_t *ctx, utp_conn_t *conn) {
    return (int)(conn - ctx->conns);
}

/* =========================================================================
 * Public API: connect, listen, on_packet, write, close, tick
 * ========================================================================= */

utp_conn_t *utp_connect(utp_ctx_t *ctx, const uint8_t peer_id[32],
                         uint16_t conn_id) {
    utp_conn_t *c = ctx_alloc_conn(ctx);
    if (!c) return NULL;

    memcpy(c->peer_id, peer_id, 32);
    c->conn_id_recv = conn_id;
    c->conn_id_send = conn_id + 1;
    c->seq_nr = 1;
    c->ack_nr = 0;
    c->state = UTP_STATE_SYN_SENT;

    /* Send SYN (buffered for retransmission) */
    uint64_t now = 0;  /* caller should provide via tick */
    conn_buffer_send(c, UTP_ST_SYN, NULL, 0, now);

    return c;
}

bool utp_listen(utp_ctx_t *ctx, const uint8_t peer_id[32],
                uint16_t conn_id,
                utp_data_fn on_data, void *user_data) {
    for (int i = 0; i < UTP_MAX_LISTENERS; i++) {
        if (!ctx->listeners[i].in_use) {
            memcpy(ctx->listeners[i].peer_id, peer_id, 32);
            ctx->listeners[i].conn_id = conn_id;
            ctx->listeners[i].on_data = on_data;
            ctx->listeners[i].user_data = user_data;
            ctx->listeners[i].in_use = true;
            return true;
        }
    }
    return false;
}

void utp_on_packet(utp_ctx_t *ctx, const uint8_t peer_id[32],
                   const uint8_t *data, size_t len) {
    utp_packet_t pkt;
    if (!utp_decode(&pkt, data, len))
        return;

    uint64_t now_us = 0;  /* timestamp from packet or system */

    /* Route to existing connection */
    /* For the initiator: incoming packets have conn_id = our conn_id_recv */
    /* For the responder: incoming packets have conn_id = our conn_id_recv */
    utp_conn_t *c = ctx_find_conn(ctx, peer_id, pkt.hdr.conn_id);

    if (c) {
        conn_on_packet(c, &pkt, now_us);
        return;
    }

    /* No connection found — check if it's a SYN for a listener */
    if (pkt.hdr.type == UTP_ST_SYN) {
        /* The SYN has connection_id = initiator's conn_id_recv = X
         * The responder's out-of-band ID = X (responder's conn_id_send)
         * Responder: conn_id_recv = X + 1, conn_id_send = X */
        utp_listener_t *l = ctx_find_listener(ctx, peer_id, pkt.hdr.conn_id);
        if (!l) return;

        c = ctx_alloc_conn(ctx);
        if (!c) return;

        memcpy(c->peer_id, peer_id, 32);
        c->conn_id_recv = pkt.hdr.conn_id + 1;
        c->conn_id_send = pkt.hdr.conn_id;
        c->seq_nr = 1;
        /* Portal deviation: ack_nr = pkt.seq_nr - 1 */
        c->ack_nr = pkt.hdr.seq_nr;
        c->state = UTP_STATE_SYN_RECV;
        c->on_data = l->on_data;
        c->on_data_ud = l->user_data;
        c->peer_wnd = pkt.hdr.wnd_size;
        c->last_recv_timestamp = pkt.hdr.timestamp;
        c->last_timestamp_diff = utp_timestamp_us(now_us) - pkt.hdr.timestamp;

        /* Remove listener */
        l->in_use = false;

        /* Send STATE (ACK for SYN) */
        conn_send_packet(c, UTP_ST_STATE, NULL, 0, now_us);
        c->state = UTP_STATE_CONNECTED;
    }
}

ssize_t utp_write(utp_conn_t *conn, const uint8_t *data, size_t len) {
    if (!conn || conn->state != UTP_STATE_CONNECTED)
        return -1;

    int idx = ctx_conn_index(conn->ctx, conn);
    utp_write_buf_t *wb = &conn->ctx->write_bufs[idx];

    /* Append to write buffer */
    if (wb->len + len > wb->cap) {
        size_t new_cap = wb->cap ? wb->cap * 2 : 4096;
        while (new_cap < wb->len + len) new_cap *= 2;
        uint8_t *nb = realloc(wb->data, new_cap);
        if (!nb) return -1;
        wb->data = nb;
        wb->cap = new_cap;
    }
    memcpy(wb->data + wb->len, data, len);
    wb->len += len;

    /* Try to flush immediately */
    conn_flush(conn, 0);

    return (ssize_t)len;
}

static void conn_flush(utp_conn_t *c, uint64_t now_us) {
    int idx = ctx_conn_index(c->ctx, c);
    utp_write_buf_t *wb = &c->ctx->write_bufs[idx];

    while (wb->pos < wb->len) {
        /* Check window */
        if (c->bytes_in_flight >= c->peer_wnd ||
            c->bytes_in_flight >= c->cwnd)
            break;
        if (c->send_count >= UTP_SEND_BUF_SIZE)
            break;

        size_t remaining = wb->len - wb->pos;
        size_t chunk = remaining > UTP_MAX_PAYLOAD ? UTP_MAX_PAYLOAD : remaining;

        conn_buffer_send(c, UTP_ST_DATA, wb->data + wb->pos, chunk, now_us);
        wb->pos += chunk;
    }

    /* If all data flushed and FIN pending, send FIN */
    if (wb->pos >= wb->len && c->fin_pending && !c->fin_sent) {
        conn_buffer_send(c, UTP_ST_FIN, NULL, 0, now_us);
        c->fin_sent = true;
        c->state = UTP_STATE_FIN_SENT;
    }
}

void utp_close(utp_conn_t *conn) {
    if (!conn) return;

    if (conn->state == UTP_STATE_CONNECTED) {
        conn->fin_pending = true;
        conn_flush(conn, 0);
    } else if (conn->state == UTP_STATE_SYN_SENT ||
               conn->state == UTP_STATE_SYN_RECV) {
        /* Send RESET */
        conn_send_packet(conn, UTP_ST_RESET, NULL, 0, 0);
        conn->state = UTP_STATE_CLOSED;
    }
}

void utp_tick(utp_ctx_t *ctx, uint64_t now_us) {
    for (int i = 0; i < UTP_MAX_CONNS; i++) {
        utp_conn_t *c = &ctx->conns[i];
        if (!c->in_use) continue;
        if (c->state == UTP_STATE_CLOSED || c->state == UTP_STATE_RESET)
            continue;

        /* Retransmit timed-out packets */
        if (c->send_count > 0) {
            utp_send_entry_t *e = &c->send_buf[c->send_start];
            if (!e->acked && now_us - e->sent_at >= c->rto) {
                /* Timeout — retransmit oldest unACKed */
                conn_send_packet(c, e->type, e->data, e->data_len, now_us);
                e->sent_at = now_us;

                /* Multiplicative decrease */
                c->ssthresh = c->cwnd / 2;
                if (c->ssthresh < 2 * UTP_MAX_PAYLOAD)
                    c->ssthresh = 2 * UTP_MAX_PAYLOAD;
                c->cwnd = UTP_MAX_PAYLOAD;

                /* Double RTO (exponential backoff) */
                c->rto *= 2;
                if (c->rto > 60000000)  /* cap at 60s */
                    c->rto = 60000000;
            }
        }

        /* Try to flush pending writes */
        conn_flush(c, now_us);
    }

    ctx->last_tick = now_us;
}

/* =========================================================================
 * Connection accessors
 * ========================================================================= */

utp_state_t utp_conn_state(const utp_conn_t *conn) {
    return conn ? conn->state : UTP_STATE_IDLE;
}

uint16_t utp_conn_id(const utp_conn_t *conn) {
    return conn ? conn->conn_id_recv : 0;
}
