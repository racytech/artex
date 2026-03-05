#ifndef ART_NET_HISTORY_H
#define ART_NET_HISTORY_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Portal History Network — content key codec and content ID derivation.
 *
 * Content types:
 *   0x00  Block body  (transactions + ommers + withdrawals)
 *   0x01  Receipts
 *
 * Content key format: selector (1 byte) || uint64_le(block_number) → 9 bytes.
 *
 * Content ID uses a custom bit-reversal algorithm (NOT SHA-256) that spreads
 * consecutive blocks across the 256-bit domain while keeping blocks within
 * a 65536-block cycle somewhat close together.
 */

#define HISTORY_CONTENT_KEY_LEN   9
#define HISTORY_SELECTOR_BODY     0x00
#define HISTORY_SELECTOR_RECEIPTS 0x01

/* Protocol identifier for TALKREQ: 0x5000 */
#define HISTORY_PROTOCOL_ID       "\x50\x00"
#define HISTORY_PROTOCOL_ID_LEN   2

/* =========================================================================
 * Content key encode/decode
 * ========================================================================= */

/** Encode content key: selector + uint64_le(block_number) → 9 bytes. */
void history_encode_content_key(uint8_t out[9],
                                uint8_t selector,
                                uint64_t block_number);

/** Decode content key. Returns false if len != 9 or unknown selector. */
bool history_decode_content_key(const uint8_t *key, size_t len,
                                uint8_t *selector,
                                uint64_t *block_number);

/* =========================================================================
 * Content ID derivation
 * ========================================================================= */

/**
 * Compute content_id using the bit-reversal algorithm.
 *
 * Algorithm:
 *   cycle_bits   = block_number mod 2^16
 *   offset_bits  = block_number / 2^16
 *   reverse offset_bits as a 240-bit number
 *   content_id   = (cycle_bits << 240) | reversed_offset | content_type
 *
 * @param out           32-byte output (big-endian uint256)
 * @param block_number  Block number
 * @param content_type  Selector (0x00 = body, 0x01 = receipts)
 */
void history_content_id(uint8_t out[32],
                        uint64_t block_number,
                        uint8_t content_type);

/**
 * Compute content_id from an encoded content_key.
 * Decodes the key, then calls history_content_id().
 */
bool history_content_id_from_key(uint8_t out[32],
                                 const uint8_t *key, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_HISTORY_H */
