/**
 * Precompiled Contracts — Ethereum built-in functions at fixed addresses
 *
 * Precompiles are executed directly (no EVM bytecode interpretation).
 * Each precompile has a fixed address and fork-dependent availability:
 *
 *   0x01  ECRECOVER       Frontier+
 *   0x02  SHA-256          Frontier+
 *   0x03  RIPEMD-160       Frontier+
 *   0x04  IDENTITY         Frontier+
 *   0x05  MODEXP           Byzantium+    (EIP-198)
 *   0x06  BN256_ADD        Byzantium+    (EIP-196)
 *   0x07  BN256_MUL        Byzantium+    (EIP-196)
 *   0x08  BN256_PAIRING    Byzantium+    (EIP-197)
 *   0x09  BLAKE2F          Istanbul+     (EIP-152)
 *   0x0A  POINT_EVAL       Cancun+       (EIP-4844)
 *   0x0B–0x11  BLS12-381   Prague+       (EIP-2537)
 *   0x0100     P256VERIFY  Osaka+        (EIP-7212)
 */

#ifndef ART_EVM_PRECOMPILE_H
#define ART_EVM_PRECOMPILE_H

#include "evm.h"
#include "fork.h"
#include "address.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

//==============================================================================
// Precompile Addresses
//==============================================================================

#define PRECOMPILE_ECRECOVER       0x01
#define PRECOMPILE_SHA256          0x02
#define PRECOMPILE_RIPEMD160       0x03
#define PRECOMPILE_IDENTITY        0x04
#define PRECOMPILE_MODEXP          0x05
#define PRECOMPILE_BN256_ADD       0x06
#define PRECOMPILE_BN256_MUL       0x07
#define PRECOMPILE_BN256_PAIRING   0x08
#define PRECOMPILE_BLAKE2F         0x09
#define PRECOMPILE_POINT_EVAL      0x0A

// BLS12-381 precompiles (Prague+, EIP-2537) — 7 precompiles at 0x0B-0x11
#define PRECOMPILE_BLS_G1ADD       0x0B
#define PRECOMPILE_BLS_G1MSM       0x0C
#define PRECOMPILE_BLS_G2ADD       0x0D
#define PRECOMPILE_BLS_G2MSM       0x0E
#define PRECOMPILE_BLS_PAIRING     0x0F
#define PRECOMPILE_BLS_MAP_G1      0x10
#define PRECOMPILE_BLS_MAP_G2      0x11

// P256VERIFY precompile (Osaka+, EIP-7212) — secp256r1 signature verification
#define PRECOMPILE_P256VERIFY      0x0100

//==============================================================================
// Precompile Interface
//==============================================================================

/**
 * Check if an address is a precompile for the given fork.
 *
 * @param addr  Address to check (only low 20 bytes used)
 * @param fork  Active fork
 * @return true if addr is an active precompile
 */
bool is_precompile(const address_t *addr, evm_fork_t fork);

/**
 * Execute a precompile.
 *
 * Caller must have already verified is_precompile() == true.
 * On success, *output and *output_size are set (caller frees *output).
 * On failure (OOG or invalid input), returns non-success status.
 *
 * @param addr        Precompile address
 * @param input       Input data (calldata)
 * @param input_size  Input data length
 * @param gas         Gas available (updated: gas remaining on return)
 * @param output      Output buffer (heap-allocated on success, caller frees)
 * @param output_size Output length
 * @param fork        Active fork (affects gas costs)
 * @return EVM_SUCCESS on success, EVM_OUT_OF_GAS or EVM_REVERT on failure
 */
evm_status_t precompile_execute(const address_t *addr,
                                const uint8_t *input, size_t input_size,
                                uint64_t *gas,
                                uint8_t **output, size_t *output_size,
                                evm_fork_t fork);

#ifdef __cplusplus
}
#endif

#endif /* ART_EVM_PRECOMPILE_H */
