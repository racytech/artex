/*
 * Fuzz target: RLP decoder.
 *
 * CRITICAL priority — recursive with no depth limit, unbounded list growth.
 * This is the most likely place to find crashes.
 */

#include "../../common/include/rlp.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    rlp_item_t *item = rlp_decode(data, size);
    if (item)
        rlp_item_free(item);
    return 0;
}
