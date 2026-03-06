/*
 * Portal Wire Protocol — message codec.
 *
 * Encodes/decodes all 8 Portal wire message types using SSZ.
 * Wire format: message_id (1 byte) || SSZ body.
 */

#include "../include/portal_wire.h"
#include "../include/ssz.h"
#include <string.h>

/* =========================================================================
 * Internal helpers
 * ========================================================================= */

/** Copy buf to output, free buf, return length (or 0 if too large). */
static size_t flush_buf(ssz_buf_t *buf, uint8_t *out, size_t cap) {
    size_t result = 0;
    if (buf->len <= cap) {
        memcpy(out, buf->data, buf->len);
        result = buf->len;
    }
    ssz_buf_free(buf);
    return result;
}

/**
 * Encode a List[ByteList] — offset table followed by concatenated items.
 * Each offset is relative to the list start.
 */
static void encode_bytelist_list(ssz_buf_t *buf,
                                  const uint8_t *const *items,
                                  const size_t *item_lens,
                                  size_t count) {
    if (count == 0) return;

    size_t list_start = buf->len;

    /* Reserve 4-byte offset per item */
    size_t off_pos[PORTAL_MAX_OFFER_KEYS]; /* max(64, 32) */
    for (size_t i = 0; i < count; i++)
        off_pos[i] = ssz_container_reserve_offset(buf);

    /* Write item data and patch offsets */
    for (size_t i = 0; i < count; i++) {
        ssz_container_patch_offset(buf, off_pos[i], list_start);
        if (item_lens[i] > 0)
            ssz_buf_append(buf, items[i], item_lens[i]);
    }
}

/**
 * Decode a List[ByteList] from raw bytes.
 * Items are determined by offset table.
 */
static bool decode_bytelist_list(const uint8_t *data, size_t len,
                                  portal_bytes_t *out, size_t max_items,
                                  size_t *count) {
    if (len == 0) {
        *count = 0;
        return true;
    }

    ssz_dec_t dec;
    ssz_dec_init(&dec, data, len);

    /* First offset tells us where offset table ends → item count */
    uint32_t first_off = ssz_dec_u32(&dec);
    if (ssz_dec_error(&dec) || first_off > len || first_off % 4 != 0)
        return false;

    size_t n = first_off / 4;
    if (n > max_items) return false;

    uint32_t offsets[PORTAL_MAX_OFFER_KEYS];
    offsets[0] = first_off;
    for (size_t i = 1; i < n; i++) {
        offsets[i] = ssz_dec_u32(&dec);
        if (ssz_dec_error(&dec) || offsets[i] > len || offsets[i] < offsets[i - 1])
            return false;
    }

    for (size_t i = 0; i < n; i++) {
        size_t end = (i + 1 < n) ? offsets[i + 1] : len;
        out[i].data = data + offsets[i];
        out[i].len = end - offsets[i];
    }

    *count = n;
    return true;
}

/* =========================================================================
 * PING / PONG  (identical layout)
 * Container(enr_seq: u64, payload_type: u16, payload: ByteList[1100])
 * ========================================================================= */

static size_t encode_ping_pong(uint8_t *out, size_t cap, uint8_t msg_id,
                                uint64_t enr_seq, uint16_t payload_type,
                                const uint8_t *payload, size_t payload_len) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 64 + payload_len);

    ssz_buf_append_u8(&buf, msg_id);

    size_t cs = buf.len; /* container start */
    ssz_buf_append_u64(&buf, enr_seq);
    ssz_buf_append_u16(&buf, payload_type);
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    if (payload_len > 0)
        ssz_buf_append(&buf, payload, payload_len);

    return flush_buf(&buf, out, cap);
}

size_t portal_encode_ping(uint8_t *out, size_t cap,
                          uint64_t enr_seq, uint16_t payload_type,
                          const uint8_t *payload, size_t payload_len) {
    return encode_ping_pong(out, cap, PORTAL_MSG_PING,
                            enr_seq, payload_type, payload, payload_len);
}

size_t portal_encode_pong(uint8_t *out, size_t cap,
                          uint64_t enr_seq, uint16_t payload_type,
                          const uint8_t *payload, size_t payload_len) {
    return encode_ping_pong(out, cap, PORTAL_MSG_PONG,
                            enr_seq, payload_type, payload, payload_len);
}

/* =========================================================================
 * FIND_NODES
 * Container(distances: List[u16, 256])
 * ========================================================================= */

size_t portal_encode_find_nodes(uint8_t *out, size_t cap,
                                const uint16_t *distances, size_t count) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 8 + count * 2);

    ssz_buf_append_u8(&buf, PORTAL_MSG_FIND_NODES);

    size_t cs = buf.len;
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    for (size_t i = 0; i < count; i++)
        ssz_buf_append_u16(&buf, distances[i]);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * NODES
 * Container(total: u8, enrs: List[ByteList[2048], 32])
 * ========================================================================= */

size_t portal_encode_nodes(uint8_t *out, size_t cap,
                           uint8_t total,
                           const uint8_t *const *enrs,
                           const size_t *enr_lens,
                           size_t enr_count) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 512);

    ssz_buf_append_u8(&buf, PORTAL_MSG_NODES);

    size_t cs = buf.len;
    ssz_buf_append_u8(&buf, total);
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    encode_bytelist_list(&buf, enrs, enr_lens, enr_count);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * FIND_CONTENT
 * Container(content_key: ByteList[2048])
 * ========================================================================= */

size_t portal_encode_find_content(uint8_t *out, size_t cap,
                                  const uint8_t *content_key, size_t key_len) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 8 + key_len);

    ssz_buf_append_u8(&buf, PORTAL_MSG_FIND_CONTENT);

    size_t cs = buf.len;
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    ssz_buf_append(&buf, content_key, key_len);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * CONTENT (Union)
 * Union[Bytes2, ByteList[2048], List[ByteList[2048], 32]]
 * ========================================================================= */

size_t portal_encode_content_connid(uint8_t *out, size_t cap,
                                    const uint8_t conn_id[2]) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 8);

    ssz_buf_append_u8(&buf, PORTAL_MSG_CONTENT);
    ssz_encode_union_selector(&buf, PORTAL_CONTENT_CONNID);
    ssz_buf_append(&buf, conn_id, 2);

    return flush_buf(&buf, out, cap);
}

size_t portal_encode_content_data(uint8_t *out, size_t cap,
                                  const uint8_t *data, size_t len) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 8 + len);

    ssz_buf_append_u8(&buf, PORTAL_MSG_CONTENT);
    ssz_encode_union_selector(&buf, PORTAL_CONTENT_DATA);
    if (len > 0)
        ssz_buf_append(&buf, data, len);

    return flush_buf(&buf, out, cap);
}

size_t portal_encode_content_enrs(uint8_t *out, size_t cap,
                                  const uint8_t *const *enrs,
                                  const size_t *enr_lens,
                                  size_t enr_count) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 512);

    ssz_buf_append_u8(&buf, PORTAL_MSG_CONTENT);
    ssz_encode_union_selector(&buf, PORTAL_CONTENT_ENRS);
    encode_bytelist_list(&buf, enrs, enr_lens, enr_count);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * OFFER
 * Container(content_keys: List[ByteList[2048], 64])
 * ========================================================================= */

size_t portal_encode_offer(uint8_t *out, size_t cap,
                           const uint8_t *const *keys,
                           const size_t *key_lens,
                           size_t key_count) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 256);

    ssz_buf_append_u8(&buf, PORTAL_MSG_OFFER);

    size_t cs = buf.len;
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    encode_bytelist_list(&buf, keys, key_lens, key_count);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * ACCEPT
 * Container(connection_id: Bytes2, content_keys: ByteList[64])
 * ========================================================================= */

size_t portal_encode_accept(uint8_t *out, size_t cap,
                            const uint8_t conn_id[2],
                            const uint8_t *content_keys, size_t keys_len) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16 + keys_len);

    ssz_buf_append_u8(&buf, PORTAL_MSG_ACCEPT);

    size_t cs = buf.len;
    ssz_buf_append(&buf, conn_id, 2);
    size_t off = ssz_container_reserve_offset(&buf);

    ssz_container_patch_offset(&buf, off, cs);
    if (keys_len > 0)
        ssz_buf_append(&buf, content_keys, keys_len);

    return flush_buf(&buf, out, cap);
}

/* =========================================================================
 * Payload helpers
 * ========================================================================= */

size_t portal_encode_payload_type0(uint8_t *out, size_t cap,
                                   const char *client_info,
                                   const uint8_t data_radius[32],
                                   const uint16_t *capabilities,
                                   size_t cap_count) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 128);

    /*
     * Container(client_info: ByteList[200], data_radius: U256,
     *           capabilities: List[u16, 400])
     *
     * Fixed part: offset(client_info) + data_radius(32) + offset(capabilities)
     *           = 4 + 32 + 4 = 40 bytes
     */
    size_t cs = buf.len;
    size_t off_ci = ssz_container_reserve_offset(&buf);
    ssz_buf_append(&buf, data_radius, 32);
    size_t off_caps = ssz_container_reserve_offset(&buf);

    /* Variable: client_info */
    ssz_container_patch_offset(&buf, off_ci, cs);
    size_t ci_len = client_info ? strlen(client_info) : 0;
    if (ci_len > 0)
        ssz_buf_append(&buf, (const uint8_t *)client_info, ci_len);

    /* Variable: capabilities */
    ssz_container_patch_offset(&buf, off_caps, cs);
    for (size_t i = 0; i < cap_count; i++)
        ssz_buf_append_u16(&buf, capabilities[i]);

    return flush_buf(&buf, out, cap);
}

size_t portal_encode_payload_type1(uint8_t *out, size_t cap,
                                   const uint8_t data_radius[32]) {
    /* Container(data_radius: U256) — fixed only, no offsets */
    if (cap < 32) return 0;
    memcpy(out, data_radius, 32);
    return 32;
}

/* =========================================================================
 * Decoder
 * ========================================================================= */

bool portal_decode(portal_msg_t *msg, const uint8_t *data, size_t len) {
    if (len < 1) return false;

    memset(msg, 0, sizeof(*msg));
    msg->msg_id = data[0];

    const uint8_t *body = data + 1;
    size_t body_len = len - 1;

    ssz_dec_t dec;
    ssz_dec_init(&dec, body, body_len);

    switch (msg->msg_id) {

    case PORTAL_MSG_PING:
    case PORTAL_MSG_PONG: {
        msg->ping.enr_seq = ssz_dec_u64(&dec);
        msg->ping.payload_type = ssz_dec_u16(&dec);
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || off > body_len) return false;
        msg->ping.payload = body + off;
        msg->ping.payload_len = body_len - off;
        return true;
    }

    case PORTAL_MSG_FIND_NODES: {
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || off > body_len) return false;
        size_t dist_bytes = body_len - off;
        if (dist_bytes % 2 != 0) return false;
        msg->find_nodes.count = dist_bytes / 2;
        if (msg->find_nodes.count > PORTAL_MAX_DISTANCES) return false;

        ssz_dec_t d2;
        ssz_dec_init(&d2, body + off, dist_bytes);
        for (size_t i = 0; i < msg->find_nodes.count; i++)
            msg->find_nodes.distances[i] = ssz_dec_u16(&d2);
        return !ssz_dec_error(&d2);
    }

    case PORTAL_MSG_NODES: {
        msg->nodes.total = ssz_dec_u8(&dec);
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || off > body_len) return false;
        return decode_bytelist_list(body + off, body_len - off,
                                    msg->nodes.enrs, PORTAL_MAX_ENRS,
                                    &msg->nodes.enr_count);
    }

    case PORTAL_MSG_FIND_CONTENT: {
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || off > body_len) return false;
        msg->find_content.key = body + off;
        msg->find_content.key_len = body_len - off;
        return true;
    }

    case PORTAL_MSG_CONTENT: {
        msg->content.selector = ssz_decode_union_selector(&dec);
        if (ssz_dec_error(&dec)) return false;
        size_t remaining = ssz_dec_remaining(&dec);
        const uint8_t *rest = body + dec.pos;

        switch (msg->content.selector) {
        case PORTAL_CONTENT_CONNID:
            if (remaining < 2) return false;
            msg->content.conn_id[0] = rest[0];
            msg->content.conn_id[1] = rest[1];
            return true;
        case PORTAL_CONTENT_DATA:
            msg->content.payload.data = rest;
            msg->content.payload.len = remaining;
            return true;
        case PORTAL_CONTENT_ENRS:
            return decode_bytelist_list(rest, remaining,
                                        msg->content.enr_list.enrs,
                                        PORTAL_MAX_ENRS,
                                        &msg->content.enr_list.count);
        default:
            return false;
        }
    }

    case PORTAL_MSG_OFFER: {
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || off > body_len) return false;
        return decode_bytelist_list(body + off, body_len - off,
                                    msg->offer.keys, PORTAL_MAX_OFFER_KEYS,
                                    &msg->offer.key_count);
    }

    case PORTAL_MSG_ACCEPT: {
        const uint8_t *cid = ssz_dec_bytes(&dec, 2);
        uint32_t off = ssz_dec_offset(&dec);
        if (ssz_dec_error(&dec) || !cid || off > body_len) return false;
        msg->accept.conn_id[0] = cid[0];
        msg->accept.conn_id[1] = cid[1];
        msg->accept.content_keys = body + off;
        msg->accept.keys_len = body_len - off;
        return true;
    }

    default:
        return false;
    }
}
