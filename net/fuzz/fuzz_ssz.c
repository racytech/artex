/*
 * Fuzz target: SSZ decoder primitives.
 *
 * LOW priority — well-bounded with error flags. Test all primitive decoders.
 */

#include "../include/ssz.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    ssz_dec_t dec;

    /* Exercise all primitive decoders */
    ssz_dec_init(&dec, data, size);
    (void)ssz_dec_u8(&dec);
    (void)ssz_dec_u16(&dec);
    (void)ssz_dec_u32(&dec);
    (void)ssz_dec_u64(&dec);
    (void)ssz_dec_bytes(&dec, 4);
    (void)ssz_dec_remaining(&dec);
    (void)ssz_dec_error(&dec);

    /* Re-init and try offset-based decoding */
    ssz_dec_init(&dec, data, size);
    if (size >= 4) {
        uint32_t off = ssz_dec_offset(&dec);
        (void)off;
    }

    /* Re-init and try reading a large fixed-size field */
    ssz_dec_init(&dec, data, size);
    (void)ssz_dec_bytes(&dec, 32);

    /* Re-init and try union selector */
    ssz_dec_init(&dec, data, size);
    if (size >= 1) {
        uint8_t sel = ssz_decode_union_selector(&dec);
        (void)sel;
    }

    return 0;
}
