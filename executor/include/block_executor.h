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
 *
 * TODO: Extend for full Ethereum receipt (needed for receiptsRoot, logsBloom):
 *
 *   1. Add tx_type (uint8_t) — needed for typed receipt RLP envelope
 *      (EIP-2718: type || RLP[status, cumGas, bloom, logs])
 *
 *   2. Add status_code (uint8_t) — 0=fail, 1=success
 *      (post-Byzantium; pre-Byzantium uses intermediate state root)
 *
 *   3. Add logs_bloom[256] — per-tx 2048-bit bloom filter
 *      Computed from tx logs: for each log, set 3 bit-pairs from
 *      keccak256(address) and keccak256(each topic)
 *
 *   4. Add log_t *logs + log_count — actual log entries from EVM
 *      (depends on log capture in LOG0-LOG4 opcodes, see evm/src/opcodes/logging.c)
 *
 *   5. Add receipt_encode_rlp(receipt) → bytes — RLP encode for trie insertion
 *
 * Once complete, block_execute() can:
 *   - Compute per-tx bloom from logs after each tx
 *   - OR all per-tx blooms → block logs_bloom (for header validation)
 *   - Build receipt trie via mpt_compute_root_batch() → receiptsRoot
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
 *
 * TODO: Add computed roots for Engine API validation:
 *
 *   1. receipt_root (hash_t) — MPT root of RLP-encoded receipts
 *      Build trie: key=RLP(tx_index), value=receipt_encode_rlp(receipt)
 *      Use mpt_compute_root_batch() from state/include/mpt.h
 *
 *   2. logs_bloom[256] — aggregate bloom = OR of all per-tx blooms
 *      Engine API provides logsBloom in payload; must match
 *
 *   3. tx_root (hash_t) — MPT root of RLP-encoded transactions
 *      Build trie: key=RLP(tx_index), value=raw tx RLP bytes
 *      Can compute from block_body transactions directly
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
