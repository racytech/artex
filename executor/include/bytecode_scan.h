#ifndef ART_EXECUTOR_BYTECODE_SCAN_H
#define ART_EXECUTOR_BYTECODE_SCAN_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Check if bytecode contains any external call opcodes.
 *
 * Scans for: CREATE, CALL, CALLCODE, DELEGATECALL, CREATE2, STATICCALL.
 * Uses AVX2 SIMD for fast rejection, scalar fallback for verification.
 *
 * Returns false if the contract is guaranteed to not make external calls.
 * This means it only touches {sender, contract_address, contract_storage}.
 *
 * @param code  Contract bytecode
 * @param len   Bytecode length
 * @return true if bytecode may make external calls
 */
bool bytecode_has_calls(const uint8_t *code, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BYTECODE_SCAN_H */
