/*
 * Fuzz target: Discv5 message decoder.
 *
 * HIGH priority — RLP-based message parsing with 6 message types
 * (PING, PONG, FINDNODE, NODES, TALKREQ, TALKRESP).
 */

#include "../include/discv5.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    discv5_msg_t msg;
    discv5_msg_decode(&msg, data, size);
    return 0;
}
