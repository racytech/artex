#ifndef ART_EXECUTOR_BLOCK_EXECUTOR_H
#define ART_EXECUTOR_BLOCK_EXECUTOR_H

#include "block.h"
#include "transaction.h"
#include "evm.h"
#include "evm_state.h"
#include "block_diff.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef ENABLE_HISTORY
#include "state_history.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif

/**
 * Per-transaction receipt.
 *
 * Contains all fields needed for receipt RLP encoding:
 *   type || RLP([status, cumulative_gas, bloom, logs])
 */
typedef struct {
    bool      success;          /* true if tx executed without fatal error */
    uint64_t  gas_used;         /* gas consumed by this tx */
    uint64_t  cumulative_gas;   /* cumulative gas after this tx */
    address_t contract_addr;    /* created contract address (if applicable) */
    bool      contract_created;
    uint8_t   tx_type;          /* transaction type (0=legacy, 1=2930, 2=1559, 3=4844, 4=7702) */
    uint8_t   status_code;      /* 0=fail, 1=success (post-Byzantium) */
    uint8_t   logs_bloom[256];  /* per-tx 2048-bit bloom filter */
    evm_log_t *logs;            /* log entries from this tx (caller must free) */
    size_t    log_count;        /* number of log entries */
} tx_receipt_t;

/**
 * Block execution result.
 */
typedef struct {
    hash_t      state_root;      /* post-execution state root */
    uint64_t    gas_used;        /* total gas used in the block */
    size_t      tx_count;        /* number of transactions executed */
    bool        success;         /* true if all txs processed without fatal error */

    tx_receipt_t *receipts;      /* per-tx receipts (caller must free) */
    size_t       receipt_count;

    /* Debugging: index of the first failed tx (-1 if all ok) */
    int          first_failure;

    hash_t      receipt_root;    /* MPT root of RLP-encoded receipts */
    uint8_t     logs_bloom[256]; /* aggregate bloom = OR of all per-tx blooms */

    /* EIP-7685: accumulated execution requests (Prague+)
     * Each request: type_byte || request_data */
    uint8_t   **requests;        /* array of request byte arrays */
    size_t     *request_lengths; /* length of each request */
    size_t      request_count;   /* number of requests */

    uint32_t    transfer_count;  /* simple ETH transfers (no code, no calldata) */
    uint32_t    call_count;      /* contract calls / creates */
    double      root_ms;         /* time spent in state root computation */

    bool        deposit_layout_invalid;  /* EIP-6110: invalid deposit event ABI layout */

    /* Block diff for undo/reorg (always populated by block_execute).
     * Caller must free via block_diff_free if non-zero group_count. */
    block_diff_t diff;
} block_result_t;

/**
 * Execute all transactions in a block.
 *
 * The EVM must be initialized with the correct state and chain config
 * before calling this function. The state should contain the pre-state
 * from the previous block.
 *
 * @param evm          EVM instance (with state and chain config)
 * @param header       Decoded block header
 * @param body         Decoded block body (transaction list)
 * @param block_hashes Optional: 256 recent block hashes for BLOCKHASH opcode
 *                     (indexed by block_number % 256). NULL = all zeros.
 * @param history      Optional: state history tracker (NULL = no capture)
 * @return Block execution result
 */
block_result_t block_execute(evm_t *evm,
                             const block_header_t *header,
                             const block_body_t *body,
                             const hash_t *block_hashes
#ifdef ENABLE_HISTORY
                             , state_history_t *history
#endif
                             );

/**
 * Free block result resources.
 */
void block_result_free(block_result_t *result);

/* =========================================================================
 * Block Production
 * ========================================================================= */

/**
 * Block environment parameters for block production.
 * Caller specifies the block context; the engine fills in the rest.
 */
typedef struct {
    address_t coinbase;
    uint64_t  timestamp;
    uint64_t  gas_limit;
    uint64_t  block_number;       /* 0 = last_block + 1 */
    hash_t    parent_hash;
    hash_t    prev_randao;        /* post-merge: PREVRANDAO */
    uint256_t base_fee;
    bool      has_base_fee;       /* false for pre-London */

    /* Cancun+ */
    uint64_t  excess_blob_gas;
    bool      has_blob_gas;
    hash_t    parent_beacon_root;
    bool      has_parent_beacon_root;

    /* Withdrawals (Shanghai+) */
    withdrawal_t *withdrawals;
    size_t        withdrawal_count;
} block_produce_params_t;

/**
 * Block production result.
 */
typedef struct {
    bool        ok;
    hash_t      state_root;
    hash_t      receipt_root;
    uint8_t     logs_bloom[256];
    uint64_t    gas_used;
    size_t      tx_count;         /* successfully executed */
    size_t      tx_rejected;      /* skipped (bad nonce, insufficient balance, etc.) */

    tx_receipt_t *receipts;       /* per-tx receipts (caller must free) */
    size_t        receipt_count;

    /* EIP-7685 requests (Prague+) */
    uint8_t   **requests;
    size_t     *request_lengths;
    size_t      request_count;

    double      exec_ms;
    double      root_ms;
} block_produce_result_t;

/**
 * Execute transactions and produce a block.
 *
 * Takes raw RLP-encoded transactions. Invalid transactions (bad nonce,
 * insufficient balance, gas exceeds remaining) are skipped.
 * Returns the assembled block result with state root, receipts, bloom.
 *
 * @param evm          EVM instance (with state and chain config)
 * @param params       Block environment parameters
 * @param txs_rlp      Array of pointers to raw RLP-encoded transactions
 * @param txs_len      Array of lengths for each transaction
 * @param tx_count     Number of transactions to attempt
 * @param block_hashes Optional: 256 recent block hashes for BLOCKHASH opcode
 * @return Block production result
 */
block_produce_result_t block_produce(evm_t *evm,
                                     const block_produce_params_t *params,
                                     const uint8_t *const *txs_rlp,
                                     const size_t *txs_len,
                                     size_t tx_count,
                                     const hash_t *block_hashes);

/**
 * Free block produce result resources.
 */
void block_produce_result_free(block_produce_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_EXECUTOR_H */
