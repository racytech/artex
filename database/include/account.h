#ifndef ACCOUNT_H
#define ACCOUNT_H

#include <stdint.h>
#include <stdbool.h>
#include "../../common/include/uint256.h"
#include "../../common/include/hash.h"

/**
 * Account — Compact encoding for Ethereum accounts.
 *
 * No storage_root — derived by MPT hash tree from flat storage slots.
 *
 * Wire format:
 *   [1B flags] [1B balance_len] [nonce bytes] [balance bytes] [code_hash]
 *
 *   flags byte:
 *     bit 0:    has_code (code_hash follows at end, 32 bytes)
 *     bits 1-4: nonce_len (0 = nonce is zero, 1-8 = N bytes follow)
 *     bits 5-7: reserved
 *
 *   balance_len byte: 0-32 (0 = balance is zero)
 *
 * Max encoded size: 2 + 8 + 32 + 32 = 74 bytes (theoretical)
 * Max realistic:    2 + 8 + 12 + 32 = 54 bytes (fits in 62-byte slot)
 */

#define ACCOUNT_MAX_ENCODED  74

typedef struct {
    uint64_t  nonce;
    uint256_t balance;
    hash_t    code_hash;
    bool      has_code;
} account_t;

/**
 * Return a default empty account (nonce=0, balance=0, no code).
 */
account_t account_empty(void);

/**
 * Encode account into compact wire format.
 * out must be >= ACCOUNT_MAX_ENCODED bytes.
 * Returns encoded length, or 0 on error.
 */
uint16_t account_encode(const account_t *acct, uint8_t *out);

/**
 * Decode compact wire format into account_t.
 * Returns true on success, false on malformed data.
 */
bool account_decode(const uint8_t *buf, uint16_t len, account_t *out);

#endif // ACCOUNT_H
