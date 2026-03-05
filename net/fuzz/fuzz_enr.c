/*
 * Fuzz target: ENR (Ethereum Node Record) decoder.
 *
 * HIGH priority — nested RLP, variable key-value pairs, secp256k1 decompression.
 */

#include "../include/enr.h"
#include <stdint.h>
#include <stddef.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    enr_t enr;
    enr_decode(&enr, data, size);
    return 0;
}
