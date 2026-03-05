#ifndef ART_EXECUTOR_TX_DECODER_H
#define ART_EXECUTOR_TX_DECODER_H

#include "transaction.h"
#include "rlp.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Decode a transaction from an RLP item (as found in a block body).
 *
 * Handles all transaction types:
 *   - Legacy (type 0): RLP list directly
 *   - EIP-2930 (type 1): RLP string starting with 0x01
 *   - EIP-1559 (type 2): RLP string starting with 0x02
 *   - EIP-4844 (type 3): RLP string starting with 0x03
 *
 * Recovers the sender address from the signature using secp256k1 ecrecover.
 *
 * @param tx        Output transaction (caller must call tx_decoded_free() after)
 * @param item      RLP item from block body transaction list
 * @param chain_id  Chain ID for EIP-155 replay protection
 * @return true on success
 */
bool tx_decode_rlp(transaction_t *tx,
                   const rlp_item_t *item,
                   uint64_t chain_id);

/**
 * Decode a transaction from raw bytes (type prefix + RLP, or bare RLP for legacy).
 *
 * @param tx        Output transaction
 * @param data      Raw encoded transaction bytes
 * @param len       Length of data
 * @param chain_id  Chain ID
 * @return true on success
 */
bool tx_decode_raw(transaction_t *tx,
                   const uint8_t *data, size_t len,
                   uint64_t chain_id);

/**
 * Free any heap-allocated memory inside a decoded transaction.
 * (access list entries, etc.)
 */
void tx_decoded_free(transaction_t *tx);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_TX_DECODER_H */
