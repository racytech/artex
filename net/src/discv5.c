/*
 * Discv5 Protocol Engine — ties codec + sessions + table + UDP socket.
 *
 * Handles PING/PONG, FINDNODE/NODES, TALKREQ/TALKRESP.
 * Full handshake state machine: ordinary → WHOAREYOU → handshake.
 */

#include "../include/discv5.h"
#include "../include/discv5_codec.h"
#include "../include/discv5_session.h"
#include "../include/discv5_table.h"
#include "../include/enr.h"
#include "../include/secp256k1_wrap.h"
#include "../../common/include/rlp.h"
#include "../../common/include/hash.h"

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

/* =========================================================================
 * Message codec — RLP encode/decode
 * ========================================================================= */

/* Encode uint32 as minimal big-endian bytes */
static rlp_item_t *rlp_req_id(uint32_t req_id) {
    return rlp_uint64((uint64_t)req_id);
}

size_t discv5_msg_encode_ping(uint8_t *out, size_t cap,
                                uint32_t req_id, uint64_t enr_seq) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));
    rlp_list_append(list, rlp_uint64(enr_seq));

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_PING;
    memcpy(out + 1, enc.data, enc.len);
    size_t total = 1 + enc.len;
    bytes_free(&enc);
    return total;
}

size_t discv5_msg_encode_pong(uint8_t *out, size_t cap,
                                uint32_t req_id, uint64_t enr_seq,
                                const uint8_t ip[4], uint16_t port) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));
    rlp_list_append(list, rlp_uint64(enr_seq));
    rlp_list_append(list, rlp_string(ip, 4));
    rlp_list_append(list, rlp_uint64(port));

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_PONG;
    memcpy(out + 1, enc.data, enc.len);
    size_t total = 1 + enc.len;
    bytes_free(&enc);
    return total;
}

size_t discv5_msg_encode_findnode(uint8_t *out, size_t cap,
                                    uint32_t req_id,
                                    const uint16_t *distances, size_t dist_count) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));

    rlp_item_t *dists = rlp_list_new();
    for (size_t i = 0; i < dist_count; i++)
        rlp_list_append(dists, rlp_uint64(distances[i]));
    rlp_list_append(list, dists);

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_FINDNODE;
    memcpy(out + 1, enc.data, enc.len);
    size_t total = 1 + enc.len;
    bytes_free(&enc);
    return total;
}

size_t discv5_msg_encode_nodes(uint8_t *out, size_t cap,
                                 uint32_t req_id, uint8_t total,
                                 const uint8_t *enrs_raw, size_t enrs_len) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));
    rlp_list_append(list, rlp_uint64(total));

    /* enrs_raw is already a concatenation of RLP-encoded ENRs.
     * We wrap them in an RLP list by treating them as raw bytes
     * inside a list. Actually each ENR is its own RLP item, so
     * we need to decode and re-wrap. For simplicity, encode the
     * ENR bytes directly as a list. */
    if (enrs_raw && enrs_len > 0) {
        /* Decode the concatenated ENRs as a sequence of RLP items */
        rlp_item_t *enr_list = rlp_list_new();
        size_t offset = 0;
        while (offset < enrs_len) {
            rlp_item_t *enr_item = rlp_decode(enrs_raw + offset, enrs_len - offset);
            if (!enr_item) break;
            /* Get the encoded size by re-encoding */
            bytes_t re = rlp_encode(enr_item);
            offset += re.len;
            bytes_free(&re);
            rlp_list_append(enr_list, enr_item);
        }
        rlp_list_append(list, enr_list);
    } else {
        rlp_list_append(list, rlp_list_new());  /* empty list */
    }

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_NODES;
    memcpy(out + 1, enc.data, enc.len);
    size_t total_len = 1 + enc.len;
    bytes_free(&enc);
    return total_len;
}

size_t discv5_msg_encode_talkreq(uint8_t *out, size_t cap,
                                   uint32_t req_id,
                                   const uint8_t *protocol, size_t proto_len,
                                   const uint8_t *request, size_t req_len) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));
    rlp_list_append(list, rlp_string(protocol, proto_len));
    rlp_list_append(list, rlp_string(request, req_len));

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_TALKREQ;
    memcpy(out + 1, enc.data, enc.len);
    size_t total = 1 + enc.len;
    bytes_free(&enc);
    return total;
}

size_t discv5_msg_encode_talkresp(uint8_t *out, size_t cap,
                                    uint32_t req_id,
                                    const uint8_t *response, size_t resp_len) {
    rlp_item_t *list = rlp_list_new();
    if (!list) return 0;
    rlp_list_append(list, rlp_req_id(req_id));
    rlp_list_append(list, rlp_string(response, resp_len));

    bytes_t enc = rlp_encode(list);
    rlp_item_free(list);

    if (1 + enc.len > cap) { bytes_free(&enc); return 0; }
    out[0] = DISCV5_MSG_TALKRESP;
    memcpy(out + 1, enc.data, enc.len);
    size_t total = 1 + enc.len;
    bytes_free(&enc);
    return total;
}

/* =========================================================================
 * Message decode
 * ========================================================================= */

/* Extract uint64 from RLP string item (big-endian) */
static uint64_t rlp_get_uint64(const rlp_item_t *item) {
    const bytes_t *s = rlp_get_string(item);
    if (!s) return 0;
    uint64_t v = 0;
    for (size_t i = 0; i < s->len && i < 8; i++)
        v = (v << 8) | s->data[i];
    return v;
}

bool discv5_msg_decode(discv5_msg_t *msg, const uint8_t *pt, size_t pt_len) {
    if (pt_len < 2) return false;

    msg->type = pt[0];
    rlp_item_t *list = rlp_decode(pt + 1, pt_len - 1);
    if (!list || rlp_get_type(list) != RLP_TYPE_LIST) {
        if (list) rlp_item_free(list);
        return false;
    }

    size_t count = rlp_get_list_count(list);
    if (count < 1) { rlp_item_free(list); return false; }

    msg->req_id = (uint32_t)rlp_get_uint64(rlp_get_list_item(list, 0));

    switch (msg->type) {
    case DISCV5_MSG_PING:
        if (count < 2) { rlp_item_free(list); return false; }
        msg->body.ping.enr_seq = rlp_get_uint64(rlp_get_list_item(list, 1));
        break;

    case DISCV5_MSG_PONG:
        if (count < 4) { rlp_item_free(list); return false; }
        msg->body.pong.enr_seq = rlp_get_uint64(rlp_get_list_item(list, 1));
        {
            const bytes_t *ip = rlp_get_string(rlp_get_list_item(list, 2));
            if (ip && ip->len == 4)
                memcpy(msg->body.pong.ip, ip->data, 4);
        }
        msg->body.pong.port = (uint16_t)rlp_get_uint64(rlp_get_list_item(list, 3));
        break;

    case DISCV5_MSG_FINDNODE:
        if (count < 2) { rlp_item_free(list); return false; }
        {
            const rlp_item_t *dists = rlp_get_list_item(list, 1);
            if (rlp_get_type(dists) == RLP_TYPE_LIST) {
                msg->body.findnode.dist_count = rlp_get_list_count(dists);
                if (msg->body.findnode.dist_count > 256)
                    msg->body.findnode.dist_count = 256;
                for (size_t i = 0; i < msg->body.findnode.dist_count; i++)
                    msg->body.findnode.distances[i] =
                        (uint16_t)rlp_get_uint64(rlp_get_list_item(dists, i));
            }
        }
        break;

    case DISCV5_MSG_NODES:
        if (count < 3) { rlp_item_free(list); return false; }
        msg->body.nodes.total = (uint8_t)rlp_get_uint64(rlp_get_list_item(list, 1));
        /* ENR list — for now just store count; full ENR decode by caller */
        msg->body.nodes.enrs = NULL;
        msg->body.nodes.enrs_len = 0;
        break;

    case DISCV5_MSG_TALKREQ:
        if (count < 3) { rlp_item_free(list); return false; }
        {
            const bytes_t *proto = rlp_get_string(rlp_get_list_item(list, 1));
            const bytes_t *data = rlp_get_string(rlp_get_list_item(list, 2));
            if (proto && proto->len <= sizeof(msg->body.talkreq.protocol)) {
                memcpy(msg->body.talkreq.protocol, proto->data, proto->len);
                msg->body.talkreq.proto_len = proto->len;
            } else {
                msg->body.talkreq.proto_len = 0;
            }
            if (data && data->len <= sizeof(msg->body.talkreq.data)) {
                memcpy(msg->body.talkreq.data, data->data, data->len);
                msg->body.talkreq.data_len = data->len;
            } else {
                msg->body.talkreq.data_len = 0;
            }
        }
        break;

    case DISCV5_MSG_TALKRESP:
        if (count < 2) { rlp_item_free(list); return false; }
        {
            const bytes_t *data = rlp_get_string(rlp_get_list_item(list, 1));
            if (data && data->len <= sizeof(msg->body.talkresp.data)) {
                memcpy(msg->body.talkresp.data, data->data, data->len);
                msg->body.talkresp.data_len = data->len;
            } else {
                msg->body.talkresp.data_len = 0;
            }
        }
        break;

    default:
        rlp_item_free(list);
        return false;
    }

    rlp_item_free(list);
    return true;
}

/* =========================================================================
 * Engine internals
 * ========================================================================= */

#define DISCV5_MAX_PENDING  256
#define DISCV5_MAX_TALK_HANDLERS 16

typedef struct {
    uint8_t  node_id[32];
    uint32_t req_id;
    uint8_t  msg_type;
    uint64_t timestamp;
    /* Queued message to re-send after handshake completes */
    uint8_t  queued_pt[512];
    size_t   queued_pt_len;
} discv5_pending_t;

typedef struct {
    char protocol[32];
    discv5_talk_handler_t handler;
    void *user_data;
} discv5_talk_entry_t;

/* Address book entry for known nodes (IP + port) */
typedef struct {
    uint8_t  node_id[32];
    uint8_t  ip[4];
    uint16_t port;
    uint8_t  pubkey_comp[33];   /* compressed pubkey for ECDH */
    uint8_t  pubkey[64];        /* uncompressed pubkey */
    bool     has_pubkey;
} discv5_addr_t;

#define DISCV5_MAX_ADDRS 1024

struct discv5_engine {
    /* Local identity */
    uint8_t privkey[32];
    uint8_t pubkey[64];
    uint8_t pubkey_comp[33];
    uint8_t node_id[32];
    enr_t   local_enr;

    /* Components */
    discv5_session_cache_t sessions;
    discv5_table_t table;

    /* Pending requests */
    discv5_pending_t pending[DISCV5_MAX_PENDING];
    size_t pending_count;
    uint32_t next_req_id;

    /* TALKREQ handlers */
    discv5_talk_entry_t talk_handlers[DISCV5_MAX_TALK_HANDLERS];
    size_t talk_handler_count;

    /* Address book (node_id → ip:port) */
    discv5_addr_t addrs[DISCV5_MAX_ADDRS];
    size_t addr_count;

    /* Network */
    int udp_fd;
    int epoll_fd;
    int timer_fd;
    bool running;
};

/* =========================================================================
 * Address book helpers
 * ========================================================================= */

static discv5_addr_t *find_addr(discv5_engine_t *e, const uint8_t node_id[32]) {
    for (size_t i = 0; i < e->addr_count; i++) {
        if (memcmp(e->addrs[i].node_id, node_id, 32) == 0)
            return &e->addrs[i];
    }
    return NULL;
}

static discv5_addr_t *get_or_create_addr(discv5_engine_t *e,
                                           const uint8_t node_id[32]) {
    discv5_addr_t *a = find_addr(e, node_id);
    if (a) return a;
    if (e->addr_count >= DISCV5_MAX_ADDRS) return NULL;
    a = &e->addrs[e->addr_count++];
    memset(a, 0, sizeof(*a));
    memcpy(a->node_id, node_id, 32);
    return a;
}

/* =========================================================================
 * Random bytes (from /dev/urandom)
 * ========================================================================= */

static void random_bytes(uint8_t *out, size_t len) {
    int fd = open("/dev/urandom", 0);
    if (fd >= 0) {
        read(fd, out, len);
        close(fd);
    }
}

/* =========================================================================
 * Send raw UDP packet
 * ========================================================================= */

static void send_udp(int fd, const uint8_t *data, size_t len,
                      const uint8_t ip[4], uint16_t port) {
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    memcpy(&addr.sin_addr.s_addr, ip, 4);
    addr.sin_port = htons(port);
    sendto(fd, data, len, 0, (struct sockaddr *)&addr, sizeof(addr));
}

/* =========================================================================
 * Send encrypted ordinary message
 * ========================================================================= */

static bool send_ordinary_msg(discv5_engine_t *e,
                                const uint8_t dest_id[32],
                                const uint8_t *pt, size_t pt_len) {
    discv5_addr_t *addr = find_addr(e, dest_id);
    if (!addr) return false;

    discv5_session_t *sess = discv5_session_find(&e->sessions, dest_id);
    if (!sess || sess->state != DISCV5_SESS_ESTABLISHED)
        return false;

    uint8_t masking_iv[16], nonce[12];
    random_bytes(masking_iv, 16);
    random_bytes(nonce, 12);

    const uint8_t *key = discv5_session_encrypt_key(sess);

    uint8_t pkt[2048];
    size_t pkt_len = discv5_encode_ordinary(pkt, sizeof(pkt),
                                              masking_iv, e->node_id, dest_id,
                                              nonce, pt, pt_len, key);
    if (pkt_len == 0) return false;

    send_udp(e->udp_fd, pkt, pkt_len, addr->ip, addr->port);
    return true;
}

/* =========================================================================
 * Initiate handshake (send random ordinary to trigger WHOAREYOU)
 * ========================================================================= */

static void initiate_handshake(discv5_engine_t *e,
                                const uint8_t dest_id[32],
                                const uint8_t *queued_pt, size_t queued_len) {
    discv5_addr_t *addr = find_addr(e, dest_id);
    if (!addr) return;

    /* Send an ordinary packet with a random session key (will fail to decrypt) */
    uint8_t masking_iv[16], nonce[12], random_key[16];
    random_bytes(masking_iv, 16);
    random_bytes(nonce, 12);
    random_bytes(random_key, 16);

    /* Minimal plaintext — will not decrypt on receiver, triggering WHOAREYOU */
    uint8_t dummy_pt[1] = { 0x00 };

    uint8_t pkt[2048];
    size_t pkt_len = discv5_encode_ordinary(pkt, sizeof(pkt),
                                              masking_iv, e->node_id, dest_id,
                                              nonce, dummy_pt, 1, random_key);
    if (pkt_len == 0) return;

    /* Store pending with queued message */
    if (e->pending_count < DISCV5_MAX_PENDING) {
        discv5_pending_t *p = &e->pending[e->pending_count++];
        memcpy(p->node_id, dest_id, 32);
        p->req_id = 0;
        p->msg_type = 0xFF;  /* handshake-in-progress marker */
        p->timestamp = 0;
        if (queued_pt && queued_len <= sizeof(p->queued_pt)) {
            memcpy(p->queued_pt, queued_pt, queued_len);
            p->queued_pt_len = queued_len;
        } else {
            p->queued_pt_len = 0;
        }
    }

    send_udp(e->udp_fd, pkt, pkt_len, addr->ip, addr->port);
}

/* =========================================================================
 * Send or queue message (initiates handshake if no session)
 * ========================================================================= */

static bool send_or_queue(discv5_engine_t *e,
                            const uint8_t dest_id[32],
                            const uint8_t *pt, size_t pt_len) {
    discv5_session_t *sess = discv5_session_find(&e->sessions, dest_id);
    if (sess && sess->state == DISCV5_SESS_ESTABLISHED)
        return send_ordinary_msg(e, dest_id, pt, pt_len);

    /* No session → initiate handshake, queue message */
    initiate_handshake(e, dest_id, pt, pt_len);
    return true;
}

/* =========================================================================
 * Handle WHOAREYOU (we got challenged)
 * ========================================================================= */

static void handle_whoareyou(discv5_engine_t *e,
                               const discv5_header_t *hdr,
                               const uint8_t *header_buf,
                               const uint8_t src_ip[4], uint16_t src_port) {
    /* We need to know who sent this — find pending handshake by matching
     * the WHOAREYOU nonce (which echoes our original packet nonce) */

    /* For now, use the src IP to find the node. In practice, we'd match
     * by the nonce of our original packet. */
    discv5_addr_t *addr = NULL;
    for (size_t i = 0; i < e->addr_count; i++) {
        if (memcmp(e->addrs[i].ip, src_ip, 4) == 0 &&
            e->addrs[i].port == src_port) {
            addr = &e->addrs[i];
            break;
        }
    }
    if (!addr || !addr->has_pubkey) return;

    /* Build challenge_data = masking-iv || unmasked-header */
    uint8_t challenge_data[128];
    size_t cd_len = DISCV5_MASKING_IV_SIZE + hdr->header_len;
    if (cd_len > sizeof(challenge_data)) return;
    memcpy(challenge_data, hdr->masking_iv, DISCV5_MASKING_IV_SIZE);
    memcpy(challenge_data + DISCV5_MASKING_IV_SIZE, header_buf, hdr->header_len);

    /* Generate ephemeral key pair */
    uint8_t eph_priv[32], eph_pub[64], eph_pubkey[33];
    random_bytes(eph_priv, 32);
    if (!secp256k1_wrap_pubkey_create(eph_pub, eph_priv)) return;
    if (!secp256k1_wrap_compress(eph_pubkey, eph_pub)) return;

    /* Derive session keys */
    discv5_keys_t keys;
    if (!discv5_derive_keys(&keys, eph_priv, addr->pubkey,
                             challenge_data, cd_len,
                             e->node_id, addr->node_id))
        return;

    /* Sign ID nonce */
    uint8_t id_sig[64];
    if (!discv5_sign_id_nonce(id_sig, challenge_data, cd_len,
                               eph_pubkey, addr->node_id, e->privkey))
        return;

    /* Build ENR if needed */
    uint8_t *enr_buf = NULL;
    size_t enr_len = 0;
    uint8_t enr_tmp[300];
    if (hdr->auth.whoareyou.enr_seq < e->local_enr.seq) {
        size_t elen;
        if (enr_encode(&e->local_enr, enr_tmp, &elen)) {
            enr_buf = enr_tmp;
            enr_len = elen;
        }
    }

    /* Find queued message for this node */
    uint8_t *queued_pt = NULL;
    size_t queued_len = 0;
    for (size_t i = 0; i < e->pending_count; i++) {
        if (memcmp(e->pending[i].node_id, addr->node_id, 32) == 0 &&
            e->pending[i].msg_type == 0xFF) {
            queued_pt = e->pending[i].queued_pt;
            queued_len = e->pending[i].queued_pt_len;
            /* Remove from pending */
            e->pending[i] = e->pending[--e->pending_count];
            break;
        }
    }

    /* Use queued message or empty plaintext */
    uint8_t empty_pt[1] = { 0x00 };
    if (!queued_pt || queued_len == 0) {
        queued_pt = empty_pt;
        queued_len = 1;
    }

    /* Encode handshake packet */
    uint8_t masking_iv[16], nonce[12];
    random_bytes(masking_iv, 16);
    random_bytes(nonce, 12);

    uint8_t pkt[2048];
    size_t pkt_len = discv5_encode_handshake(pkt, sizeof(pkt),
                                               masking_iv, e->node_id, addr->node_id,
                                               nonce, id_sig, eph_pubkey,
                                               enr_buf, enr_len,
                                               queued_pt, queued_len,
                                               keys.initiator_key);
    if (pkt_len == 0) return;

    /* Establish session (we are the initiator) */
    discv5_session_t *sess = discv5_session_get_or_create(&e->sessions, addr->node_id);
    discv5_session_establish(sess, &keys, true);

    send_udp(e->udp_fd, pkt, pkt_len, addr->ip, addr->port);
}

/* =========================================================================
 * Handle ordinary message (established session)
 * ========================================================================= */

static void handle_message(discv5_engine_t *e,
                             const uint8_t *pt, size_t pt_len,
                             const uint8_t src_id[32],
                             const uint8_t src_ip[4], uint16_t src_port) {
    discv5_msg_t msg;
    if (!discv5_msg_decode(&msg, pt, pt_len)) return;

    switch (msg.type) {
    case DISCV5_MSG_PING:
        /* Auto-respond with PONG */
        {
            uint8_t pong_pt[64];
            size_t pong_len = discv5_msg_encode_pong(pong_pt, sizeof(pong_pt),
                                                       msg.req_id, e->local_enr.seq,
                                                       src_ip, src_port);
            if (pong_len > 0)
                send_ordinary_msg(e, src_id, pong_pt, pong_len);
        }
        discv5_table_mark_alive((discv5_table_t *)&e->table, src_id);
        break;

    case DISCV5_MSG_PONG:
        discv5_table_mark_alive((discv5_table_t *)&e->table, src_id);
        break;

    case DISCV5_MSG_FINDNODE:
        /* Respond with NODES */
        {
            uint8_t nodes_pt[1280];
            /* Collect ENRs from requested distances */
            /* For now, send empty NODES response */
            size_t nodes_len = discv5_msg_encode_nodes(nodes_pt, sizeof(nodes_pt),
                                                         msg.req_id, 1,
                                                         NULL, 0);
            if (nodes_len > 0)
                send_ordinary_msg(e, src_id, nodes_pt, nodes_len);
        }
        break;

    case DISCV5_MSG_NODES:
        /* TODO: process received ENRs, add to table */
        break;

    case DISCV5_MSG_TALKREQ:
        /* Dispatch to registered handler */
        for (size_t i = 0; i < e->talk_handler_count; i++) {
            size_t plen = strlen(e->talk_handlers[i].protocol);
            if (plen == msg.body.talkreq.proto_len &&
                memcmp(e->talk_handlers[i].protocol,
                       msg.body.talkreq.protocol, plen) == 0) {
                e->talk_handlers[i].handler(e, src_id, msg.req_id,
                                              msg.body.talkreq.data,
                                              msg.body.talkreq.data_len,
                                              e->talk_handlers[i].user_data);
                break;
            }
        }
        break;

    case DISCV5_MSG_TALKRESP:
        /* TODO: match to pending TALKREQ */
        break;
    }
}

/* =========================================================================
 * Handle incoming ordinary packet
 * ========================================================================= */

static void handle_ordinary(discv5_engine_t *e,
                              const discv5_header_t *hdr,
                              const uint8_t *header_buf,
                              const uint8_t *pkt, size_t pkt_len,
                              const uint8_t src_ip[4], uint16_t src_port) {
    const uint8_t *src_id = hdr->auth.ordinary.src_id;

    /* Update address book */
    discv5_addr_t *addr = get_or_create_addr(e, src_id);
    if (addr) {
        memcpy(addr->ip, src_ip, 4);
        addr->port = src_port;
    }

    /* Try to decrypt with session key */
    discv5_session_t *sess = discv5_session_find(&e->sessions, src_id);
    if (sess && sess->state == DISCV5_SESS_ESTABLISHED) {
        const uint8_t *key = discv5_session_decrypt_key(sess);
        uint8_t pt[1280];
        size_t pt_len;
        if (discv5_decrypt_message(pt, &pt_len, pkt, pkt_len,
                                     hdr, header_buf, key)) {
            handle_message(e, pt, pt_len, src_id, src_ip, src_port);
            return;
        }
    }

    /* Decryption failed or no session → send WHOAREYOU */
    uint8_t masking_iv[16], id_nonce[16];
    random_bytes(masking_iv, 16);
    random_bytes(id_nonce, 16);

    /* Look up remote's enr-seq (0 = we don't know) */
    uint64_t enr_seq = 0;

    uint8_t whoareyou[128];
    size_t way_len = discv5_encode_whoareyou(whoareyou, sizeof(whoareyou),
                                               masking_iv, src_id,
                                               hdr->nonce, id_nonce, enr_seq);
    if (way_len == 0) return;

    /* Store challenge data for verifying handshake response.
     * challenge_data = masking-iv || unmasked header of WHOAREYOU */
    uint8_t cd[128];
    /* We need the unmasked header. Build it again. */
    uint8_t way_hdr[64];
    size_t way_hdr_len = 23 + 24;  /* static_hdr + authdata(id_nonce+enr_seq) */
    /* The unmasked header can be reconstructed from our params */
    memcpy(cd, masking_iv, 16);
    /* Unmask from our own encoded WHOAREYOU */
    discv5_header_t way_parsed;
    discv5_decode_header(&way_parsed, way_hdr, whoareyou, way_len, src_id);
    memcpy(cd + 16, way_hdr, way_hdr_len);
    size_t cd_len = 16 + way_hdr_len;

    discv5_session_t *new_sess = discv5_session_get_or_create(&e->sessions, src_id);
    discv5_session_set_whoareyou(new_sess, cd, cd_len);

    send_udp(e->udp_fd, whoareyou, way_len, src_ip, src_port);
}

/* =========================================================================
 * Handle incoming handshake packet
 * ========================================================================= */

static void handle_handshake(discv5_engine_t *e,
                               const discv5_header_t *hdr,
                               const uint8_t *header_buf,
                               const uint8_t *pkt, size_t pkt_len,
                               const uint8_t src_ip[4], uint16_t src_port) {
    const uint8_t *src_id = hdr->auth.handshake.src_id;

    /* We must have sent WHOAREYOU to this node */
    discv5_session_t *sess = discv5_session_find(&e->sessions, src_id);
    if (!sess || sess->state != DISCV5_SESS_WHOAREYOU_SENT)
        return;

    /* Get remote's public key from ENR in handshake or from address book */
    uint8_t remote_pub[64];
    bool have_pub = false;

    if (hdr->auth.handshake.enr_len > 0) {
        /* Decode ENR from handshake */
        enr_t remote_enr;
        if (enr_decode(&remote_enr, hdr->auth.handshake.enr_data,
                        hdr->auth.handshake.enr_len)) {
            memcpy(remote_pub, remote_enr.pubkey, 64);
            have_pub = true;

            /* Update address book */
            discv5_addr_t *addr = get_or_create_addr(e, src_id);
            if (addr) {
                memcpy(addr->ip, src_ip, 4);
                addr->port = src_port;
                memcpy(addr->pubkey, remote_pub, 64);
                /* Compress for reference */
                secp256k1_wrap_compress(addr->pubkey_comp, remote_pub);
                addr->has_pubkey = true;
            }
        }
    }

    if (!have_pub) {
        discv5_addr_t *addr = find_addr(e, src_id);
        if (addr && addr->has_pubkey) {
            memcpy(remote_pub, addr->pubkey, 64);
            have_pub = true;
        }
    }

    if (!have_pub) return;

    /* Verify ID nonce signature */
    if (!discv5_verify_id_nonce(hdr->auth.handshake.id_sig,
                                  sess->challenge_data, sess->challenge_data_len,
                                  hdr->auth.handshake.eph_pubkey,
                                  e->node_id, remote_pub))
        return;

    /* Derive keys (we are the recipient: remote is node_id_a, we are node_id_b) */
    uint8_t eph_pub[64];
    if (!secp256k1_wrap_decompress(eph_pub, hdr->auth.handshake.eph_pubkey))
        return;

    /* ECDH with our static key + their ephemeral pubkey */
    uint8_t shared[33];
    if (!secp256k1_wrap_ecdh_compressed(shared, e->privkey, eph_pub))
        return;

    discv5_keys_t keys;
    if (!discv5_derive_keys_from_secret(&keys, shared,
                                          sess->challenge_data, sess->challenge_data_len,
                                          src_id, e->node_id))
        return;

    /* Establish session (we are the recipient) */
    discv5_session_establish(sess, &keys, false);

    /* Decrypt message */
    uint8_t pt[1280];
    size_t pt_len;
    if (discv5_decrypt_message(pt, &pt_len, pkt, pkt_len,
                                 hdr, header_buf, keys.initiator_key)) {
        handle_message(e, pt, pt_len, src_id, src_ip, src_port);
    }
}

/* =========================================================================
 * Public API: on_packet
 * ========================================================================= */

void discv5_engine_on_packet(discv5_engine_t *e,
                               const uint8_t *data, size_t len,
                               const uint8_t src_ip[4], uint16_t src_port) {
    discv5_header_t hdr;
    uint8_t header_buf[1024];

    if (!discv5_decode_header(&hdr, header_buf, data, len, e->node_id))
        return;

    switch (hdr.flag) {
    case DISCV5_FLAG_ORDINARY:
        handle_ordinary(e, &hdr, header_buf, data, len, src_ip, src_port);
        break;
    case DISCV5_FLAG_WHOAREYOU:
        handle_whoareyou(e, &hdr, header_buf, src_ip, src_port);
        break;
    case DISCV5_FLAG_HANDSHAKE:
        handle_handshake(e, &hdr, header_buf, data, len, src_ip, src_port);
        break;
    }
}

/* =========================================================================
 * Public API: create/destroy
 * ========================================================================= */

discv5_engine_t *discv5_engine_create(const uint8_t privkey[32], uint16_t bind_port) {
    discv5_engine_t *e = calloc(1, sizeof(discv5_engine_t));
    if (!e) return NULL;

    memcpy(e->privkey, privkey, 32);
    if (!secp256k1_wrap_pubkey_create(e->pubkey, privkey)) {
        free(e);
        return NULL;
    }
    secp256k1_wrap_compress(e->pubkey_comp, e->pubkey);

    /* node_id = keccak256(uncompressed_pubkey) */
    {
        hash_t h = hash_keccak256(e->pubkey, 64);
        memcpy(e->node_id, h.bytes, 32);
    }

    /* Initialize ENR */
    enr_init(&e->local_enr);
    e->local_enr.seq = 1;
    enr_set_v4_identity(&e->local_enr, privkey);

    /* Initialize components */
    discv5_session_cache_init(&e->sessions, 1000);
    discv5_table_init(&e->table, e->node_id);

    e->next_req_id = 1;
    e->udp_fd = -1;
    e->epoll_fd = -1;
    e->timer_fd = -1;

    /* Create UDP socket */
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) { discv5_engine_destroy(e); return NULL; }

    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = INADDR_ANY;
    bind_addr.sin_port = htons(bind_port);

    if (bind(fd, (struct sockaddr *)&bind_addr, sizeof(bind_addr)) < 0) {
        close(fd);
        discv5_engine_destroy(e);
        return NULL;
    }

    e->udp_fd = fd;

    return e;
}

void discv5_engine_destroy(discv5_engine_t *e) {
    if (!e) return;
    if (e->udp_fd >= 0) close(e->udp_fd);
    if (e->epoll_fd >= 0) close(e->epoll_fd);
    if (e->timer_fd >= 0) close(e->timer_fd);
    discv5_session_cache_destroy(&e->sessions);
    free(e);
}

const uint8_t *discv5_engine_local_id(const discv5_engine_t *e) {
    return e->node_id;
}

uint64_t discv5_engine_enr_seq(const discv5_engine_t *e) {
    return e->local_enr.seq;
}

/* =========================================================================
 * Public API: add node, register handler
 * ========================================================================= */

bool discv5_engine_add_node(discv5_engine_t *e,
                              const uint8_t node_id[32],
                              const uint8_t pubkey_comp[33],
                              const uint8_t ip[4], uint16_t port) {
    discv5_addr_t *addr = get_or_create_addr(e, node_id);
    if (!addr) return false;

    memcpy(addr->ip, ip, 4);
    addr->port = port;
    memcpy(addr->pubkey_comp, pubkey_comp, 33);
    if (secp256k1_wrap_decompress(addr->pubkey, pubkey_comp))
        addr->has_pubkey = true;

    /* Add to routing table */
    discv5_node_t node;
    memset(&node, 0, sizeof(node));
    memcpy(node.node_id, node_id, 32);
    memcpy(node.pubkey, pubkey_comp, 33);
    memcpy(node.ip4, ip, 4);
    node.udp_port = port;
    discv5_table_insert(&e->table, &node);

    return true;
}

void discv5_engine_register_talk(discv5_engine_t *e,
                                   const char *protocol,
                                   discv5_talk_handler_t handler,
                                   void *user_data) {
    if (e->talk_handler_count >= DISCV5_MAX_TALK_HANDLERS) return;
    discv5_talk_entry_t *entry = &e->talk_handlers[e->talk_handler_count++];
    strncpy(entry->protocol, protocol, sizeof(entry->protocol) - 1);
    entry->handler = handler;
    entry->user_data = user_data;
}

/* =========================================================================
 * Public API: send messages
 * ========================================================================= */

uint32_t discv5_engine_send_ping(discv5_engine_t *e,
                                   const uint8_t node_id[32]) {
    uint32_t req_id = e->next_req_id++;
    uint8_t pt[64];
    size_t pt_len = discv5_msg_encode_ping(pt, sizeof(pt), req_id, e->local_enr.seq);
    if (pt_len == 0) return 0;

    send_or_queue(e, node_id, pt, pt_len);
    return req_id;
}

uint32_t discv5_engine_send_findnode(discv5_engine_t *e,
                                       const uint8_t node_id[32],
                                       const uint16_t *distances, size_t count) {
    uint32_t req_id = e->next_req_id++;
    uint8_t pt[512];
    size_t pt_len = discv5_msg_encode_findnode(pt, sizeof(pt), req_id, distances, count);
    if (pt_len == 0) return 0;

    send_or_queue(e, node_id, pt, pt_len);
    return req_id;
}

uint32_t discv5_engine_send_talkreq(discv5_engine_t *e,
                                      const uint8_t node_id[32],
                                      const char *protocol,
                                      const uint8_t *data, size_t data_len) {
    uint32_t req_id = e->next_req_id++;
    uint8_t pt[1280];
    size_t pt_len = discv5_msg_encode_talkreq(pt, sizeof(pt), req_id,
                                                (const uint8_t *)protocol,
                                                strlen(protocol),
                                                data, data_len);
    if (pt_len == 0) return 0;

    send_or_queue(e, node_id, pt, pt_len);
    return req_id;
}

bool discv5_engine_send_talkresp(discv5_engine_t *e,
                                   const uint8_t node_id[32],
                                   uint32_t req_id,
                                   const uint8_t *data, size_t data_len) {
    uint8_t pt[1280];
    size_t pt_len = discv5_msg_encode_talkresp(pt, sizeof(pt), req_id, data, data_len);
    if (pt_len == 0) return false;

    return send_ordinary_msg(e, node_id, pt, pt_len);
}

/* =========================================================================
 * Event loop
 * ========================================================================= */

void discv5_engine_run(discv5_engine_t *e) {
    e->epoll_fd = epoll_create1(0);
    if (e->epoll_fd < 0) return;

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = e->udp_fd;
    epoll_ctl(e->epoll_fd, EPOLL_CTL_ADD, e->udp_fd, &ev);

    e->running = true;
    struct epoll_event events[16];

    while (e->running) {
        int n = epoll_wait(e->epoll_fd, events, 16, 1000 /* 1s timeout */);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == e->udp_fd) {
                uint8_t buf[2048];
                struct sockaddr_in from;
                socklen_t from_len = sizeof(from);
                ssize_t r = recvfrom(e->udp_fd, buf, sizeof(buf), 0,
                                      (struct sockaddr *)&from, &from_len);
                if (r > 0) {
                    uint8_t ip[4];
                    memcpy(ip, &from.sin_addr.s_addr, 4);
                    uint16_t port = ntohs(from.sin_port);
                    discv5_engine_on_packet(e, buf, (size_t)r, ip, port);
                }
            }
        }
    }

    close(e->epoll_fd);
    e->epoll_fd = -1;
}

void discv5_engine_stop(discv5_engine_t *e) {
    e->running = false;
}
