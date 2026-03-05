#include "block_executor.h"
#include "tx_decoder.h"
#include "fork.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* =========================================================================
 * Build block_env_t from block_header_t
 * ========================================================================= */

static void header_to_block_env(const block_header_t *hdr, block_env_t *env) {
    memset(env, 0, sizeof(*env));
    address_copy(&env->coinbase, &hdr->coinbase);
    env->block_number = hdr->number;
    env->timestamp = hdr->timestamp;
    env->gas_limit = hdr->gas_limit;
    uint256_copy(&env->difficulty, &hdr->difficulty);

    if (hdr->has_base_fee)
        uint256_copy(&env->base_fee, &hdr->base_fee);

    /* Post-merge: mix_hash is PREVRANDAO */
    hash_copy(&env->prev_randao, &hdr->mix_hash);

    if (hdr->has_blob_gas)
        env->excess_blob_gas = uint256_from_uint64(hdr->excess_blob_gas);
}

/* =========================================================================
 * Build evm_block_env_t from block_header_t (for EVM context)
 * ========================================================================= */

static void header_to_evm_block_env(const block_header_t *hdr,
                                    evm_block_env_t *env,
                                    const chain_config_t *config) {
    memset(env, 0, sizeof(*env));
    env->number = hdr->number;
    env->timestamp = hdr->timestamp;
    env->gas_limit = hdr->gas_limit;
    uint256_copy(&env->difficulty, &hdr->difficulty);
    address_copy(&env->coinbase, &hdr->coinbase);

    if (hdr->has_base_fee)
        uint256_copy(&env->base_fee, &hdr->base_fee);

    if (config)
        env->chain_id = uint256_from_uint64(config->chain_id);

    if (hdr->has_blob_gas) {
        env->excess_blob_gas = uint256_from_uint64(hdr->excess_blob_gas);
        /* Compute blob base fee from excess blob gas */
        env->blob_base_fee = calc_blob_gas_price(&env->excess_blob_gas);
    }

    /* block_hash[256] would need to be populated from a block hash oracle.
     * For now, leave zeroed — BLOCKHASH opcode will return 0. */
}

/* =========================================================================
 * Block execution
 * ========================================================================= */

block_result_t block_execute(evm_t *evm,
                             const block_header_t *header,
                             const block_body_t *body) {
    block_result_t result;
    memset(&result, 0, sizeof(result));
    result.first_failure = -1;

    if (!evm || !header || !body) {
        result.success = false;
        return result;
    }

    size_t tx_count = body->tx_count;
    result.tx_count = tx_count;

    /* Allocate receipts */
    if (tx_count > 0) {
        result.receipts = calloc(tx_count, sizeof(tx_receipt_t));
        if (!result.receipts) {
            result.success = false;
            return result;
        }
        result.receipt_count = tx_count;
    }

    /* Set up block environment on the EVM */
    evm_block_env_t evm_env;
    header_to_evm_block_env(header, &evm_env, evm->chain_config);
    evm_set_block_env(evm, &evm_env);

    /* Build block_env_t for transaction_execute */
    block_env_t tx_env;
    header_to_block_env(header, &tx_env);

    /* Determine chain ID */
    uint64_t chain_id = evm->chain_config ? evm->chain_config->chain_id : 1;

    /* Commit state before executing transactions (EIP-2200 original values) */
    evm_state_commit(evm->state);

    uint64_t cumulative_gas = 0;
    result.success = true;

    for (size_t i = 0; i < tx_count; i++) {
        const rlp_item_t *tx_item = block_body_tx(body, i);
        if (!tx_item) {
            fprintf(stderr, "block_execute: failed to get tx %zu\n", i);
            result.success = false;
            if (result.first_failure < 0) result.first_failure = (int)i;
            break;
        }

        /* Decode transaction */
        transaction_t tx;
        if (!tx_decode_rlp(&tx, tx_item, chain_id)) {
            fprintf(stderr, "block_execute: failed to decode tx %zu\n", i);
            result.success = false;
            if (result.first_failure < 0) result.first_failure = (int)i;
            break;
        }

        /* Execute transaction */
        transaction_result_t tx_result;
        bool ok = transaction_execute(evm, &tx, &tx_env, &tx_result);

        /* Fill receipt */
        result.receipts[i].success = ok;
        result.receipts[i].gas_used = ok ? tx_result.gas_used : 0;
        cumulative_gas += result.receipts[i].gas_used;
        result.receipts[i].cumulative_gas = cumulative_gas;
        if (ok && tx_result.contract_created) {
            result.receipts[i].contract_created = true;
            address_copy(&result.receipts[i].contract_addr,
                         &tx_result.contract_address);
        }

        if (ok) {
            transaction_result_free(&tx_result);
        } else {
            if (result.first_failure < 0) result.first_failure = (int)i;
            /* Continue executing remaining txs — a single tx failure
             * doesn't abort the block (the tx is still included,
             * it just consumes all its gas). */
        }

        tx_decoded_free(&tx);
    }

    /* Finalize state: flush dirty accounts/storage to state_db */
    evm_state_finalize(evm->state);

    /* Compute state root (trie-agnostic — MPT or Verkle) */
    result.state_root = evm_state_compute_state_root(evm->state);
    result.gas_used = cumulative_gas;

    return result;
}

void block_result_free(block_result_t *result) {
    if (result) {
        free(result->receipts);
        result->receipts = NULL;
        result->receipt_count = 0;
    }
}
