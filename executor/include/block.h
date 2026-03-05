#ifndef ART_EXECUTOR_BLOCK_H
#define ART_EXECUTOR_BLOCK_H

#include "hash.h"
#include "address.h"
#include "uint256.h"
#include "rlp.h"
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Ethereum block header — decoded from RLP.
 *
 * Field count varies by fork:
 *   Pre-London:  15 fields
 *   London+:     16 (+ baseFeePerGas)
 *   Shanghai+:   17 (+ withdrawalsRoot)
 *   Cancun+:     20 (+ blobGasUsed, excessBlobGas, parentBeaconBlockRoot)
 */
typedef struct {
    hash_t    parent_hash;
    hash_t    uncle_hash;
    address_t coinbase;
    hash_t    state_root;
    hash_t    tx_root;
    hash_t    receipt_root;
    uint8_t   logs_bloom[256];
    uint256_t difficulty;
    uint64_t  number;
    uint64_t  gas_limit;
    uint64_t  gas_used;
    uint64_t  timestamp;
    uint8_t   extra_data[32];
    size_t    extra_data_len;
    hash_t    mix_hash;         // prev_randao post-merge
    uint64_t  nonce;

    /* EIP-1559 (London+) */
    bool      has_base_fee;
    uint256_t base_fee;

    /* Shanghai+ */
    bool      has_withdrawals_root;
    hash_t    withdrawals_root;

    /* Cancun+ */
    bool      has_blob_gas;
    uint64_t  blob_gas_used;
    uint64_t  excess_blob_gas;

    bool      has_parent_beacon_root;
    hash_t    parent_beacon_root;
} block_header_t;

/**
 * Decoded block body — holds transaction list.
 *
 * Body RLP: [transactions_list, uncles_list, withdrawals_list?]
 * We only need the transactions for execution.
 */
typedef struct {
    rlp_item_t *_rlp;      /* internal: keep RLP alive for tx pointers */
    size_t      tx_count;
} block_body_t;

/**
 * Decode a block header from RLP bytes.
 *
 * @param hdr   Output header struct
 * @param data  RLP-encoded header bytes
 * @param len   Length of data
 * @return true on success
 */
bool block_header_decode_rlp(block_header_t *hdr,
                             const uint8_t *data, size_t len);

/**
 * Decode a block body from RLP bytes.
 *
 * After decoding, use block_body_tx() to access individual transactions.
 * Call block_body_free() when done.
 *
 * @param body  Output body struct
 * @param data  RLP-encoded body bytes
 * @param len   Length of data
 * @return true on success
 */
bool block_body_decode_rlp(block_body_t *body,
                           const uint8_t *data, size_t len);

/**
 * Get the RLP item for a transaction at the given index.
 *
 * For legacy transactions: returns an RLP list item.
 * For typed transactions: returns an RLP string item (type || rlp_payload).
 *
 * @param body  Decoded body
 * @param index Transaction index (0-based)
 * @return RLP item, or NULL if out of range
 */
const rlp_item_t *block_body_tx(const block_body_t *body, size_t index);

/**
 * Free block body resources.
 */
void block_body_free(block_body_t *body);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_H */
