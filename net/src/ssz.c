/*
 * SSZ — Simple Serialize (Portal Network subset).
 *
 * Encode/decode for Portal wire protocol types:
 *   - Fixed: uint8, uint16, uint64, BytesN
 *   - Variable: ByteList, List, BitList, Container, Union
 *
 * All multi-byte integers are little-endian.
 * Variable fields in containers/lists use 4-byte (uint32) offsets.
 */

#include "../include/ssz.h"
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Buffer
 * ========================================================================= */

void ssz_buf_init(ssz_buf_t *buf, size_t initial_cap) {
    buf->data = (uint8_t *)malloc(initial_cap > 0 ? initial_cap : 64);
    buf->len = 0;
    buf->cap = initial_cap > 0 ? initial_cap : 64;
}

void ssz_buf_free(ssz_buf_t *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

void ssz_buf_reset(ssz_buf_t *buf) {
    buf->len = 0;
}

static void ssz_buf_grow(ssz_buf_t *buf, size_t need) {
    if (buf->len + need <= buf->cap) return;
    size_t new_cap = buf->cap * 2;
    if (new_cap < buf->len + need)
        new_cap = buf->len + need;
    buf->data = (uint8_t *)realloc(buf->data, new_cap);
    buf->cap = new_cap;
}

void ssz_buf_append(ssz_buf_t *buf, const void *data, size_t len) {
    ssz_buf_grow(buf, len);
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
}

void ssz_buf_append_u8(ssz_buf_t *buf, uint8_t v) {
    ssz_buf_grow(buf, 1);
    buf->data[buf->len++] = v;
}

void ssz_buf_append_u16(ssz_buf_t *buf, uint16_t v) {
    uint8_t b[2] = { (uint8_t)(v), (uint8_t)(v >> 8) };
    ssz_buf_append(buf, b, 2);
}

void ssz_buf_append_u32(ssz_buf_t *buf, uint32_t v) {
    uint8_t b[4] = { (uint8_t)(v), (uint8_t)(v >> 8),
                     (uint8_t)(v >> 16), (uint8_t)(v >> 24) };
    ssz_buf_append(buf, b, 4);
}

void ssz_buf_append_u64(ssz_buf_t *buf, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (8 * i));
    ssz_buf_append(buf, b, 8);
}

void ssz_buf_patch_u32(ssz_buf_t *buf, size_t offset, uint32_t v) {
    buf->data[offset]     = (uint8_t)(v);
    buf->data[offset + 1] = (uint8_t)(v >> 8);
    buf->data[offset + 2] = (uint8_t)(v >> 16);
    buf->data[offset + 3] = (uint8_t)(v >> 24);
}

/* =========================================================================
 * Decoder
 * ========================================================================= */

void ssz_dec_init(ssz_dec_t *dec, const uint8_t *data, size_t len) {
    dec->data = data;
    dec->len = len;
    dec->pos = 0;
    dec->error = false;
}

uint8_t ssz_dec_u8(ssz_dec_t *dec) {
    if (dec->pos + 1 > dec->len) { dec->error = true; return 0; }
    return dec->data[dec->pos++];
}

uint16_t ssz_dec_u16(ssz_dec_t *dec) {
    if (dec->pos + 2 > dec->len) { dec->error = true; return 0; }
    uint16_t v = (uint16_t)dec->data[dec->pos]
               | ((uint16_t)dec->data[dec->pos + 1] << 8);
    dec->pos += 2;
    return v;
}

uint32_t ssz_dec_u32(ssz_dec_t *dec) {
    if (dec->pos + 4 > dec->len) { dec->error = true; return 0; }
    uint32_t v = (uint32_t)dec->data[dec->pos]
               | ((uint32_t)dec->data[dec->pos + 1] << 8)
               | ((uint32_t)dec->data[dec->pos + 2] << 16)
               | ((uint32_t)dec->data[dec->pos + 3] << 24);
    dec->pos += 4;
    return v;
}

uint64_t ssz_dec_u64(ssz_dec_t *dec) {
    if (dec->pos + 8 > dec->len) { dec->error = true; return 0; }
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
        v |= (uint64_t)dec->data[dec->pos + i] << (8 * i);
    dec->pos += 8;
    return v;
}

const uint8_t *ssz_dec_bytes(ssz_dec_t *dec, size_t n) {
    if (dec->pos + n > dec->len) { dec->error = true; return NULL; }
    const uint8_t *p = dec->data + dec->pos;
    dec->pos += n;
    return p;
}

size_t ssz_dec_remaining(const ssz_dec_t *dec) {
    if (dec->pos >= dec->len) return 0;
    return dec->len - dec->pos;
}

bool ssz_dec_error(const ssz_dec_t *dec) {
    return dec->error;
}

/* =========================================================================
 * Container helpers
 * ========================================================================= */

size_t ssz_container_reserve_offset(ssz_buf_t *buf) {
    size_t pos = buf->len;
    ssz_buf_append_u32(buf, 0);  /* placeholder */
    return pos;
}

void ssz_container_patch_offset(ssz_buf_t *buf, size_t placeholder_pos,
                                 size_t container_start) {
    uint32_t offset = (uint32_t)(buf->len - container_start);
    ssz_buf_patch_u32(buf, placeholder_pos, offset);
}

uint32_t ssz_dec_offset(ssz_dec_t *dec) {
    return ssz_dec_u32(dec);
}

/* =========================================================================
 * BitList
 * ========================================================================= */

void ssz_encode_bitlist(ssz_buf_t *buf, const bool *bits, size_t count) {
    /* Pack bits + sentinel into bytes.
     * Total bits = count + 1 (sentinel).
     * Number of bytes = ceil((count + 1) / 8). */
    size_t total_bits = count + 1;
    size_t byte_count = (total_bits + 7) / 8;

    for (size_t i = 0; i < byte_count; i++) {
        uint8_t b = 0;
        for (int bit = 0; bit < 8; bit++) {
            size_t idx = i * 8 + bit;
            if (idx < count) {
                if (bits[idx]) b |= (1 << bit);
            } else if (idx == count) {
                /* Sentinel bit */
                b |= (1 << bit);
            }
        }
        ssz_buf_append_u8(buf, b);
    }
}

bool ssz_decode_bitlist(const uint8_t *data, size_t byte_len,
                        bool *bits, size_t *count) {
    if (byte_len == 0) return false;

    /* Find the sentinel bit — highest set bit in the last byte */
    uint8_t last = data[byte_len - 1];
    if (last == 0) return false;  /* no sentinel */

    int sentinel_bit = 7;
    while (sentinel_bit >= 0 && !(last & (1 << sentinel_bit)))
        sentinel_bit--;

    /* Total logical bits = (byte_len - 1) * 8 + sentinel_bit */
    *count = (byte_len - 1) * 8 + sentinel_bit;

    for (size_t i = 0; i < *count; i++) {
        size_t byte_idx = i / 8;
        int bit_idx = i % 8;
        bits[i] = (data[byte_idx] >> bit_idx) & 1;
    }

    return true;
}

/* =========================================================================
 * Union
 * ========================================================================= */

void ssz_encode_union_selector(ssz_buf_t *buf, uint8_t selector) {
    ssz_buf_append_u8(buf, selector);
}

uint8_t ssz_decode_union_selector(ssz_dec_t *dec) {
    return ssz_dec_u8(dec);
}
