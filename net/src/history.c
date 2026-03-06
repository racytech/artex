/*
 * Portal History Network — content key codec and content ID derivation.
 *
 * Content ID algorithm (from spec):
 *   CYCLE_BITS = 16, OFFSET_BITS = 240
 *   cycle_bits   = block_number mod 2^16
 *   offset_bits  = block_number / 2^16
 *   Reverse offset_bits as a 240-bit number
 *   content_id   = (cycle_bits << 240) | reversed_offset | content_type
 */

#include "../include/history.h"
#include <string.h>

/* =========================================================================
 * Content key encode/decode
 * ========================================================================= */

void history_encode_content_key(uint8_t out[9],
                                uint8_t selector,
                                uint64_t block_number) {
    out[0] = selector;
    /* uint64 little-endian */
    for (int i = 0; i < 8; i++)
        out[1 + i] = (uint8_t)(block_number >> (i * 8));
}

bool history_decode_content_key(const uint8_t *key, size_t len,
                                uint8_t *selector,
                                uint64_t *block_number) {
    if (len != HISTORY_CONTENT_KEY_LEN)
        return false;

    uint8_t sel = key[0];
    if (sel != HISTORY_SELECTOR_BODY && sel != HISTORY_SELECTOR_RECEIPTS)
        return false;

    *selector = sel;

    uint64_t bn = 0;
    for (int i = 0; i < 8; i++)
        bn |= (uint64_t)key[1 + i] << (i * 8);
    *block_number = bn;

    return true;
}

/* =========================================================================
 * Content ID derivation — bit-reversal algorithm
 * ========================================================================= */

void history_content_id(uint8_t out[32],
                        uint64_t block_number,
                        uint8_t content_type) {
    memset(out, 0, 32);

    uint16_t cycle = (uint16_t)(block_number & 0xFFFF);
    uint64_t offset = block_number >> 16;  /* high 48 bits */

    /* Place cycle_bits as top 2 bytes (big-endian) */
    out[0] = (uint8_t)(cycle >> 8);
    out[1] = (uint8_t)(cycle & 0xFF);

    /*
     * Bit-reverse offset as a 240-bit value.
     *
     * The Python spec reverses the 240-char binary string representation.
     * In the reversed 240-bit number, bit (239 - b) = original bit b.
     *
     * For big-endian byte layout, bit (239 - b) of the reversed value
     * goes into byte b/8 at shift (7 - b%8). Since these 30 bytes
     * occupy out[2..31], byte_idx = 2 + b/8.
     */
    for (int b = 0; b < 48; b++) {
        if ((offset >> b) & 1) {
            int byte_idx = 2 + (b / 8);
            int bit_pos = 7 - (b % 8);
            out[byte_idx] |= (1 << bit_pos);
        }
    }

    /* OR content_type into the last byte */
    out[31] |= content_type;
}

bool history_content_id_from_key(uint8_t out[32],
                                 const uint8_t *key, size_t len) {
    uint8_t selector;
    uint64_t block_number;

    if (!history_decode_content_key(key, len, &selector, &block_number))
        return false;

    history_content_id(out, block_number, selector);
    return true;
}
