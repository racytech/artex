/*
 * Fuzz target: History Network content key decoder + content ID derivation.
 *
 * LOW priority — simple 9-byte format, but good for completeness.
 */

#include "../include/history.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    uint8_t selector;
    uint64_t block_number;

    /* Fuzz the content key decoder */
    history_decode_content_key(data, size, &selector, &block_number);

    /* Fuzz content_id_from_key (calls decode + content_id) */
    uint8_t id[32];
    history_content_id_from_key(id, data, size);

    /* If we have at least 9 bytes, also try direct content_id */
    if (size >= 9) {
        uint64_t bn = 0;
        for (int i = 0; i < 8; i++)
            bn |= (uint64_t)data[1 + i] << (i * 8);
        history_content_id(id, bn, data[0]);
    }

    return 0;
}
