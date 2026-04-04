#ifndef ART_EXECUTOR_BLOCK_EXECUTOR_H
#define ART_EXECUTOR_BLOCK_EXECUTOR_H

#include "block.h"
#include "transaction.h"
#include "evm.h"
#include "evm_state.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef ENABLE_HISTORY
typedef struct state_history state_history_t;
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

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_EXECUTOR_H */
