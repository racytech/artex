#ifndef ART_EXECUTOR_BLOCK_EXECUTOR_H
#define ART_EXECUTOR_BLOCK_EXECUTOR_H

#include "block.h"
#include "transaction.h"
#include "evm.h"
#include "evm_state.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Per-transaction receipt (simplified).
 */
typedef struct {
    bool      success;       /* true if tx succeeded (even if EVM reverted) */
    uint64_t  gas_used;      /* gas consumed by this tx */
    uint64_t  cumulative_gas; /* cumulative gas after this tx */
    address_t contract_addr; /* created contract address (if applicable) */
    bool      contract_created;
} tx_receipt_t;

/**
 * Block execution result.
 */
typedef struct {
    hash_t      state_root;    /* post-execution state root */
    uint64_t    gas_used;      /* total gas used in the block */
    size_t      tx_count;      /* number of transactions executed */
    bool        success;       /* true if all txs processed without fatal error */

    tx_receipt_t *receipts;    /* per-tx receipts (caller must free) */
    size_t       receipt_count;

    /* Debugging: index of the first failed tx (-1 if all ok) */
    int          first_failure;
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
 * @return Block execution result
 */
block_result_t block_execute(evm_t *evm,
                             const block_header_t *header,
                             const block_body_t *body,
                             const hash_t *block_hashes);

/**
 * Free block result resources.
 */
void block_result_free(block_result_t *result);

#ifdef __cplusplus
}
#endif

#endif /* ART_EXECUTOR_BLOCK_EXECUTOR_H */
