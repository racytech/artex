/*
 * Portal Overlay Handler — generic integration layer for Portal sub-protocols.
 *
 * Wires together: Discv5 (TALKREQ transport), portal_wire (message codec),
 * portal_table (routing), and uTP (streaming large content).
 */

#include "../include/portal.h"
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Internal struct
 * ========================================================================= */

struct portal_overlay {
    discv5_engine_t      *engine;
    utp_ctx_t            *utp;
    char                  protocol_id[16];
    size_t                proto_len;
    portal_table_t        table;
    portal_content_store_t store;
    uint64_t              enr_seq;  /* our ENR sequence number */
};

/* =========================================================================
 * TALKREQ callback — bridge from Discv5 to overlay
 * ========================================================================= */

static void talkreq_handler(discv5_engine_t *engine,
                             const uint8_t peer_id[32],
                             uint32_t req_id,
                             const uint8_t *request, size_t req_len,
                             void *user_data) {
    (void)engine;
    portal_overlay_t *ov = (portal_overlay_t *)user_data;
    portal_overlay_on_request(ov, peer_id, req_id, request, req_len);
}

/* =========================================================================
 * Create / Destroy
 * ========================================================================= */

portal_overlay_t *portal_overlay_create(
    discv5_engine_t *engine,
    utp_ctx_t *utp,
    const char *protocol_id,
    size_t proto_len,
    const uint8_t local_id[32],
    const uint8_t data_radius[32],
    const portal_content_store_t *store) {

    portal_overlay_t *ov = calloc(1, sizeof(*ov));
    if (!ov) return NULL;

    ov->engine = engine;
    ov->utp = utp;

    if (proto_len > sizeof(ov->protocol_id))
        proto_len = sizeof(ov->protocol_id);
    memcpy(ov->protocol_id, protocol_id, proto_len);
    ov->proto_len = proto_len;

    portal_table_init(&ov->table, local_id, data_radius);

    if (store)
        ov->store = *store;

    if (engine) {
        ov->enr_seq = discv5_engine_enr_seq(engine);
        discv5_engine_register_talk(engine, protocol_id,
                                    talkreq_handler, ov);
    }

    return ov;
}

void portal_overlay_destroy(portal_overlay_t *overlay) {
    if (overlay)
        free(overlay);
}

portal_table_t *portal_overlay_table(portal_overlay_t *overlay) {
    return &overlay->table;
}

/* =========================================================================
 * Response helper — encode and send TALKRESP
 * ========================================================================= */

static void send_response(portal_overlay_t *ov,
                           const uint8_t peer_id[32],
                           uint32_t req_id,
                           const uint8_t *resp, size_t resp_len) {
    if (ov->engine && resp_len > 0) {
        discv5_engine_send_talkresp(ov->engine, peer_id, req_id,
                                    resp, resp_len);
    }
}

/* =========================================================================
 * PING handler → reply with PONG
 * ========================================================================= */

static void handle_ping(portal_overlay_t *ov,
                         const uint8_t peer_id[32],
                         uint32_t req_id,
                         const portal_msg_t *msg) {
    /* Extract radius from the payload if it's Type 1 (basic radius) */
    if (msg->ping.payload_type == PORTAL_PAYLOAD_TYPE1 &&
        msg->ping.payload_len >= 32) {
        /* Type 1 payload is just data_radius (32 bytes) */
        portal_table_update_radius(&ov->table, peer_id, msg->ping.payload);
    }

    /* Insert/update peer in routing table */
    portal_node_t node;
    memset(&node, 0, sizeof(node));
    memcpy(node.node_id, peer_id, 32);
    /* Set max radius by default — will be updated from payload */
    memset(node.data_radius, 0xFF, 32);

    if (msg->ping.payload_type == PORTAL_PAYLOAD_TYPE1 &&
        msg->ping.payload_len >= 32) {
        memcpy(node.data_radius, msg->ping.payload, 32);
    }

    portal_table_insert(&ov->table, &node);

    /* Encode PONG response with Type 1 payload (basic radius) */
    uint8_t payload[32];
    memcpy(payload, ov->table.local_radius, 32);

    uint8_t resp[128];
    size_t resp_len = portal_encode_pong(resp, sizeof(resp),
                                         ov->enr_seq,
                                         PORTAL_PAYLOAD_TYPE1,
                                         payload, 32);

    send_response(ov, peer_id, req_id, resp, resp_len);
}

/* =========================================================================
 * FIND_NODES handler → reply with NODES
 * ========================================================================= */

static void handle_find_nodes(portal_overlay_t *ov,
                               const uint8_t peer_id[32],
                               uint32_t req_id,
                               const portal_msg_t *msg) {
    (void)peer_id;

    /* Collect nodes at requested distances */
    portal_node_t nodes[PORTAL_BUCKET_SIZE];
    size_t total = 0;

    for (size_t d = 0; d < msg->find_nodes.count && total < PORTAL_MAX_ENRS; d++) {
        size_t n = portal_table_nodes_at_distance(&ov->table,
                                                   msg->find_nodes.distances[d],
                                                   nodes + total);
        total += n;
        if (total > PORTAL_MAX_ENRS) total = PORTAL_MAX_ENRS;
    }

    /* For now, respond with an empty NODES (we don't have ENR encoding
       integrated into portal_node_t yet — nodes are tracked by node_id) */
    uint8_t resp[64];
    size_t resp_len = portal_encode_nodes(resp, sizeof(resp), 1, NULL, NULL, 0);

    send_response(ov, peer_id, req_id, resp, resp_len);
}

/* =========================================================================
 * FIND_CONTENT handler → reply with CONTENT
 * ========================================================================= */

static void handle_find_content(portal_overlay_t *ov,
                                 const uint8_t peer_id[32],
                                 uint32_t req_id,
                                 const portal_msg_t *msg) {
    uint8_t resp[PORTAL_RESP_BUF_SIZE];
    size_t resp_len = 0;

    /* Try to retrieve content from store */
    if (ov->store.get_fn) {
        uint8_t content_buf[PORTAL_RESP_BUF_SIZE - 16];
        size_t content_len = ov->store.get_fn(msg->find_content.key,
                                               msg->find_content.key_len,
                                               content_buf,
                                               sizeof(content_buf),
                                               ov->store.user_data);

        if (content_len > 0) {
            /* Content found — return inline */
            resp_len = portal_encode_content_data(resp, sizeof(resp),
                                                   content_buf, content_len);
            send_response(ov, peer_id, req_id, resp, resp_len);
            return;
        }
    }

    /* Content not found — return closest nodes as ENR list.
       For now return an empty ENR list since we don't serialize ENRs
       from portal_node_t. */
    resp_len = portal_encode_content_enrs(resp, sizeof(resp),
                                           NULL, NULL, 0);
    send_response(ov, peer_id, req_id, resp, resp_len);
}

/* =========================================================================
 * OFFER handler → reply with ACCEPT
 * ========================================================================= */

static void handle_offer(portal_overlay_t *ov,
                          const uint8_t peer_id[32],
                          uint32_t req_id,
                          const portal_msg_t *msg) {
    (void)peer_id;

    /* Build content_keys bitfield — one byte per offered key.
       Accept keys that are in our radius and not already stored. */
    uint8_t accept_keys[PORTAL_MAX_OFFER_KEYS];
    size_t accept_count = msg->offer.key_count;
    if (accept_count > PORTAL_MAX_OFFER_KEYS)
        accept_count = PORTAL_MAX_OFFER_KEYS;

    bool any_accepted = false;

    for (size_t i = 0; i < accept_count; i++) {
        accept_keys[i] = PORTAL_ACCEPT_DECLINE;  /* default: decline */

        if (!ov->store.content_id_fn || !ov->store.get_fn)
            continue;

        /* Derive content_id from content_key */
        uint8_t content_id[32];
        if (!ov->store.content_id_fn(content_id,
                                      msg->offer.keys[i].data,
                                      msg->offer.keys[i].len,
                                      ov->store.user_data))
            continue;

        /* Check if in our radius */
        if (!portal_table_content_in_radius(&ov->table, content_id)) {
            accept_keys[i] = PORTAL_ACCEPT_NOT_IN_RADIUS;
            continue;
        }

        /* Check if already stored */
        size_t existing = ov->store.get_fn(msg->offer.keys[i].data,
                                            msg->offer.keys[i].len,
                                            NULL, 0,
                                            ov->store.user_data);
        if (existing > 0) {
            accept_keys[i] = PORTAL_ACCEPT_ALREADY_STORED;
            continue;
        }

        /* Accept this key */
        accept_keys[i] = PORTAL_ACCEPT_OK;
        any_accepted = true;
    }

    /* Generate a connection ID for uTP transfer if any keys accepted */
    uint8_t conn_id[2] = {0, 0};
    if (any_accepted) {
        /* Simple connection ID from clock */
        uint16_t cid = (uint16_t)(ov->table.clock & 0xFFFF);
        conn_id[0] = (uint8_t)(cid & 0xFF);
        conn_id[1] = (uint8_t)(cid >> 8);
    }

    uint8_t resp[PORTAL_RESP_BUF_SIZE];
    size_t resp_len = portal_encode_accept(resp, sizeof(resp),
                                            conn_id,
                                            accept_keys, accept_count);

    send_response(ov, peer_id, req_id, resp, resp_len);
}

/* =========================================================================
 * Incoming message dispatch
 * ========================================================================= */

void portal_overlay_on_request(portal_overlay_t *ov,
                               const uint8_t peer_id[32],
                               uint32_t req_id,
                               const uint8_t *data, size_t len) {
    portal_msg_t msg;
    if (!portal_decode(&msg, data, len))
        return;

    switch (msg.msg_id) {
    case PORTAL_MSG_PING:
        handle_ping(ov, peer_id, req_id, &msg);
        break;
    case PORTAL_MSG_FIND_NODES:
        handle_find_nodes(ov, peer_id, req_id, &msg);
        break;
    case PORTAL_MSG_FIND_CONTENT:
        handle_find_content(ov, peer_id, req_id, &msg);
        break;
    case PORTAL_MSG_OFFER:
        handle_offer(ov, peer_id, req_id, &msg);
        break;
    default:
        /* PONG, NODES, CONTENT, ACCEPT are responses — not handled here */
        break;
    }
}

/* =========================================================================
 * Outgoing requests
 * ========================================================================= */

void portal_overlay_ping(portal_overlay_t *ov,
                         const uint8_t peer_id[32]) {
    /* Encode Type 1 payload (basic radius) */
    uint8_t payload[32];
    memcpy(payload, ov->table.local_radius, 32);

    uint8_t buf[128];
    size_t len = portal_encode_ping(buf, sizeof(buf),
                                     ov->enr_seq,
                                     PORTAL_PAYLOAD_TYPE1,
                                     payload, 32);

    if (len > 0 && ov->engine) {
        discv5_engine_send_talkreq(ov->engine, peer_id,
                                    ov->protocol_id,
                                    buf, len);
    }
}

void portal_overlay_find_nodes(portal_overlay_t *ov,
                               const uint8_t peer_id[32],
                               const uint16_t *distances, size_t count) {
    uint8_t buf[1024];
    size_t len = portal_encode_find_nodes(buf, sizeof(buf),
                                           distances, count);

    if (len > 0 && ov->engine) {
        discv5_engine_send_talkreq(ov->engine, peer_id,
                                    ov->protocol_id,
                                    buf, len);
    }
}

void portal_overlay_find_content(portal_overlay_t *ov,
                                 const uint8_t peer_id[32],
                                 const uint8_t *key, size_t key_len) {
    uint8_t buf[512];
    size_t len = portal_encode_find_content(buf, sizeof(buf), key, key_len);

    if (len > 0 && ov->engine) {
        discv5_engine_send_talkreq(ov->engine, peer_id,
                                    ov->protocol_id,
                                    buf, len);
    }
}

void portal_overlay_offer(portal_overlay_t *ov,
                          const uint8_t peer_id[32],
                          const uint8_t *const *keys,
                          const size_t *key_lens,
                          size_t key_count) {
    uint8_t buf[2048];
    size_t len = portal_encode_offer(buf, sizeof(buf),
                                      keys, key_lens, key_count);

    if (len > 0 && ov->engine) {
        discv5_engine_send_talkreq(ov->engine, peer_id,
                                    ov->protocol_id,
                                    buf, len);
    }
}
