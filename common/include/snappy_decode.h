#ifndef ART_COMMON_SNAPPY_DECODE_H
#define ART_COMMON_SNAPPY_DECODE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Decode snappy-framed (streaming) data.
 *
 * The snappy framing format wraps raw snappy blocks with a stream header
 * and per-chunk headers. This function handles the framing layer and
 * calls libsnappy for raw block decompression.
 *
 * @param input      Snappy-framed input data
 * @param input_len  Length of input data
 * @param output     Output buffer for decompressed data
 * @param output_cap Capacity of output buffer
 * @return Decompressed size, or 0 on error
 */
size_t snappy_frame_decode(const uint8_t *input, size_t input_len,
                           uint8_t *output, size_t output_cap);

#ifdef __cplusplus
}
#endif

#endif /* ART_COMMON_SNAPPY_DECODE_H */
