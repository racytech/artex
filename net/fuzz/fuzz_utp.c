/*
 * Fuzz target: uTP packet decoder.
 *
 * MEDIUM priority — extension chain parsing, fixed header + variable extensions.
 */

#include "../include/utp.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    utp_packet_t pkt;
    utp_decode(&pkt, data, size);
    return 0;
}
