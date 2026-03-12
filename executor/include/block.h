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

    /* Prague+ (EIP-7685) */
    bool      has_requests_hash;
    hash_t    requests_hash;
} block_header_t;

/**
 * EIP-4895 withdrawal (Shanghai+).
 */
typedef struct {
    uint64_t  index;
    uint64_t  validator_index;
    address_t address;
    uint64_t  amount_gwei;  /* amount in Gwei (multiply by 1e9 for Wei) */
} withdrawal_t;

/**
 * Decoded block body — holds transaction list.
 *
 * Body RLP: [transactions_list, uncles_list, withdrawals_list?]
 * We only need the transactions for execution.
 */
typedef struct {
    rlp_item_t *_rlp;      /* internal: keep RLP alive for tx pointers */
    size_t      _tx_list_idx; /* index of tx list in _rlp (0=body-only, 1=full block) */
    size_t      tx_count;
    /* EIP-4895 withdrawals (Shanghai+) */
    withdrawal_t *withdrawals;
    size_t        withdrawal_count;
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
 * Decode a full block RLP [header, transactions, uncles] into
 * both a block_header_t and a block_body_t.
 *
 * @param data  Full block RLP bytes
 * @param len   Length of data
 * @param hdr   Output header struct
 * @param body  Output body struct (call block_body_free() when done)
 * @return true on success
 */
bool block_decode_full_rlp(const uint8_t *data, size_t len,
                           block_header_t *hdr, block_body_t *body);

/**
 * Get the number of uncle headers in the block body.
 */
size_t block_body_uncle_count(const block_body_t *body);

/**
 * Decode the uncle header at the given index.
 *
 * @param body   Decoded body
 * @param index  Uncle index (0-based)
 * @param hdr    Output uncle header
 * @return true on success
 */
bool block_body_get_uncle(const block_body_t *body, size_t index,
                          block_header_t *hdr);

/**
 * Free block body resources.
 */
void block_body_free(block_body_t *body);

/**
 * Compute the block hash from full block RLP bytes.
 * Hash = keccak256(RLP(header_list)).
 *
 * @param data  Full block RLP bytes
 * @param len   Length of data
 * @return Block hash, or zero hash on error
 */
hash_t block_hash_from_rlp(const uint8_t *data, size_t len);

/**
 * Encode a block header to RLP bytes.
 * Reverse of block_header_decode_rlp.
 *
 * @param hdr  Block header struct
 * @return RLP-encoded bytes (caller must free .data), or {NULL,0} on error
 */
bytes_t block_header_encode_rlp(const block_header_t *hdr);

/**
 * Compute the block hash from a decoded block header struct.
 * Hash = keccak256(block_header_encode_rlp(hdr)).
 *
 * @param hdr  Block header struct
 * @return Block hash, or zero hash on error
 */
hash_t block_header_hash(const block_header_t *hdr);

/**
 * Compute the transaction trie root from a block body.
 * Keys are RLP-encoded tx indices, values are raw tx RLP bytes.
 *
 * @param body  Decoded block body
 * @return Transaction root hash, or empty trie root if no transactions
 */
hash_t block_compute_tx_root(const block_body_t *body);

/**
 * Compute the withdrawals trie root from a withdrawal list.
 * Keys are RLP-encoded withdrawal indices, values are RLP-encoded withdrawals.
 * Each withdrawal is encoded as RLP([index, validator_index, address, amount]).
 *
 * @param withdrawals  Array of withdrawals
 * @param count        Number of withdrawals
 * @return Withdrawals root hash, or empty trie root if count is 0
 */
hash_t block_compute_withdrawals_root(const withdrawal_t *withdrawals,
                                       size_t count);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_H */
