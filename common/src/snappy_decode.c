#include "snappy_decode.h"
#include <snappy-c.h>
#include <string.h>

/*
 * Snappy framing format (RFC-like):
 *
 * Stream     := StreamHeader Chunk*
 * StreamHeader := 0xFF 0x06 0x00 0x00 "sNaPpY"
 * Chunk      := ChunkType(1) Length(3 LE) Data(Length-4)
 *
 * ChunkType 0x00 = compressed   (4-byte CRC + snappy-compressed payload)
 * ChunkType 0x01 = uncompressed (4-byte CRC + raw payload)
 * ChunkType 0xFE-0xFF = padding/stream header (skip)
 */

#define STREAM_HEADER_LEN 10  /* 0xFF + 3-byte len + "sNaPpY" */
#define CHUNK_HEADER_LEN   4  /* type(1) + length(3 LE) */
#define CRC_LEN            4  /* checksum (skipped) */

static inline uint32_t read_le24(const uint8_t *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

size_t snappy_frame_decode(const uint8_t *input, size_t input_len,
                           uint8_t *output, size_t output_cap) {
    if (!input || input_len < STREAM_HEADER_LEN || !output || output_cap == 0)
        return 0;

    /* Verify stream header: 0xFF followed by length, then "sNaPpY" */
    if (input[0] != 0xFF)
        return 0;

    static const uint8_t magic[] = "sNaPpY";
    if (memcmp(input + 4, magic, 6) != 0)
        return 0;

    size_t pos = STREAM_HEADER_LEN;
    size_t out_pos = 0;

    while (pos + CHUNK_HEADER_LEN <= input_len) {
        uint8_t chunk_type = input[pos];
        uint32_t chunk_len = read_le24(input + pos + 1);

        pos += CHUNK_HEADER_LEN;
        if (pos + chunk_len > input_len)
            return 0;  /* truncated */

        if (chunk_type == 0x00) {
            /* Compressed chunk: 4-byte CRC + snappy block */
            if (chunk_len < CRC_LEN)
                return 0;

            const uint8_t *compressed = input + pos + CRC_LEN;
            size_t compressed_len = chunk_len - CRC_LEN;

            size_t uncompressed_len;
            if (snappy_uncompressed_length((const char *)compressed,
                    compressed_len, &uncompressed_len) != SNAPPY_OK)
                return 0;

            if (out_pos + uncompressed_len > output_cap)
                return 0;  /* output too small */

            if (snappy_uncompress((const char *)compressed, compressed_len,
                    (char *)(output + out_pos), &uncompressed_len) != SNAPPY_OK)
                return 0;

            out_pos += uncompressed_len;
        } else if (chunk_type == 0x01) {
            /* Uncompressed chunk: 4-byte CRC + raw data */
            if (chunk_len < CRC_LEN)
                return 0;

            size_t data_len = chunk_len - CRC_LEN;
            if (out_pos + data_len > output_cap)
                return 0;

            memcpy(output + out_pos, input + pos + CRC_LEN, data_len);
            out_pos += data_len;
        } else if (chunk_type >= 0x80) {
            /* Padding or reserved — skip */
        } else {
            /* Unknown chunk type in range 0x02-0x7F — skip */
        }

        pos += chunk_len;
    }

    return out_pos;
}
