/*
 * Fuzz target: Discv5 packet header decoder.
 *
 * MEDIUM priority — AES unmasking + flag-dependent parsing (3 packet types).
 */

#include "../include/discv5_codec.h"
#include <stdint.h>
#include <stddef.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    discv5_header_t hdr;
    /* header_buf must be large enough for the unmasked header.
       Per API: "Must be large enough (packet length - 16)". */
    uint8_t header_buf[4096];
    uint8_t local_id[32];
    memset(local_id, 0x01, 32);

    if (size > sizeof(header_buf) + DISCV5_MASKING_IV_SIZE)
        return 0; /* skip inputs too large for our buffer */

    discv5_decode_header(&hdr, header_buf, data, size, local_id);
    return 0;
}
