/*
 * Fuzz target: Portal wire protocol decoder.
 *
 * HIGH priority — 8 message types, SSZ with offset tables, bytelist lists.
 */

#include "../include/portal_wire.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    portal_msg_t msg;
    portal_decode(&msg, data, size);
    return 0;
}
