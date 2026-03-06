#ifndef ART_NET_SSZ_H
#define ART_NET_SSZ_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * SSZ — Simple Serialize (Portal Network subset).
 *
 * Implements encode/decode for the types needed by Portal wire protocol:
 *   - Fixed: uint8, uint16, uint64, BytesN (fixed-size byte vector)
 *   - Variable: ByteList, List, BitList, Container, Union
 *
 * No Merkleization — only wire serialization/deserialization.
 *
 * Encoding rules (NIST little-endian):
 *   - Fixed types are encoded inline
 *   - Variable types use 4-byte (uint32) offsets in containers/lists
 *   - BitList uses a sentinel bit for length
 *   - Union uses a 1-byte selector prefix
 */

/* =========================================================================
 * Buffer — output accumulator for encoding
 * ========================================================================= */

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} ssz_buf_t;

/** Initialize a buffer with pre-allocated capacity. */
void ssz_buf_init(ssz_buf_t *buf, size_t initial_cap);

/** Free buffer memory. */
void ssz_buf_free(ssz_buf_t *buf);

/** Reset buffer length to 0 (keeps allocation). */
void ssz_buf_reset(ssz_buf_t *buf);

/** Append raw bytes. */
void ssz_buf_append(ssz_buf_t *buf, const void *data, size_t len);

/** Append a single byte. */
void ssz_buf_append_u8(ssz_buf_t *buf, uint8_t v);

/** Append a uint16 (little-endian). */
void ssz_buf_append_u16(ssz_buf_t *buf, uint16_t v);

/** Append a uint32 (little-endian). */
void ssz_buf_append_u32(ssz_buf_t *buf, uint32_t v);

/** Append a uint64 (little-endian). */
void ssz_buf_append_u64(ssz_buf_t *buf, uint64_t v);

/** Write a uint32 at a specific offset (for back-patching offsets). */
void ssz_buf_patch_u32(ssz_buf_t *buf, size_t offset, uint32_t v);

/* =========================================================================
 * Decoder — cursor into encoded data
 * ========================================================================= */

typedef struct {
    const uint8_t *data;
    size_t         len;
    size_t         pos;
    bool           error;
} ssz_dec_t;

/** Initialize decoder over a byte range. */
void ssz_dec_init(ssz_dec_t *dec, const uint8_t *data, size_t len);

/** Read a uint8. */
uint8_t ssz_dec_u8(ssz_dec_t *dec);

/** Read a uint16 (little-endian). */
uint16_t ssz_dec_u16(ssz_dec_t *dec);

/** Read a uint32 (little-endian). */
uint32_t ssz_dec_u32(ssz_dec_t *dec);

/** Read a uint64 (little-endian). */
uint64_t ssz_dec_u64(ssz_dec_t *dec);

/** Read N raw bytes. Returns pointer into the decoder's buffer (zero-copy). */
const uint8_t *ssz_dec_bytes(ssz_dec_t *dec, size_t n);

/** Get remaining bytes count. */
size_t ssz_dec_remaining(const ssz_dec_t *dec);

/** Check if decoder has encountered an error (underflow). */
bool ssz_dec_error(const ssz_dec_t *dec);

/* =========================================================================
 * Container helpers — offset-based encoding
 * ========================================================================= */

/**
 * Container encoding pattern:
 *
 * 1. Call ssz_buf_append_* for each fixed field
 * 2. For each variable field, call ssz_container_reserve_offset() which
 *    writes a placeholder 4-byte offset and returns the position to patch
 * 3. After all fixed/offset fields, write each variable field's data and
 *    call ssz_container_patch_offset() to fill in the offset
 *
 * Example:
 *   ssz_buf_append_u64(&buf, enr_seq);            // fixed field
 *   size_t off = ssz_container_reserve_offset(&buf);  // variable field placeholder
 *   // ... write variable data ...
 *   ssz_container_patch_offset(&buf, off);         // back-patch
 */

/** Reserve a 4-byte offset placeholder. Returns the byte position of the placeholder. */
size_t ssz_container_reserve_offset(ssz_buf_t *buf);

/** Patch the offset at the given position to point to the current end of buffer.
 * The offset is relative to container_start. */
void ssz_container_patch_offset(ssz_buf_t *buf, size_t placeholder_pos,
                                 size_t container_start);

/* =========================================================================
 * Container decoding helpers
 * ========================================================================= */

/**
 * Read a uint32 offset from the decoder. Returns the absolute byte position
 * within the container (offset + container_start).
 */
uint32_t ssz_dec_offset(ssz_dec_t *dec);

/* =========================================================================
 * BitList encode/decode
 * ========================================================================= */

/**
 * Encode a BitList: pack bits with a trailing sentinel bit.
 *
 * @param buf    Output buffer
 * @param bits   Array of bool values (true = 1, false = 0)
 * @param count  Number of bits
 */
void ssz_encode_bitlist(ssz_buf_t *buf, const bool *bits, size_t count);

/**
 * Decode a BitList from raw bytes.
 *
 * @param data     Encoded BitList bytes
 * @param byte_len Number of bytes
 * @param bits     Output array (must be large enough for max_count)
 * @param count    Output: number of bits decoded
 * @return         true on success, false on invalid encoding
 */
bool ssz_decode_bitlist(const uint8_t *data, size_t byte_len,
                        bool *bits, size_t *count);

/* =========================================================================
 * Union encode/decode
 * ========================================================================= */

/**
 * Encode a Union: selector byte followed by the value.
 * The caller writes the value to buf after this call.
 *
 * @param buf      Output buffer
 * @param selector Union variant index (0, 1, 2, ...)
 */
void ssz_encode_union_selector(ssz_buf_t *buf, uint8_t selector);

/**
 * Decode a Union selector byte.
 *
 * @param dec      Decoder
 * @return         Selector value (0, 1, 2, ...)
 */
uint8_t ssz_decode_union_selector(ssz_dec_t *dec);

#ifdef __cplusplus
}
#endif

#endif /* ART_NET_SSZ_H */
