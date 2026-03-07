#include "block_executor.h"
#include "tx_decoder.h"
#include "fork.h"
#include "transaction.h"
#include "verkle_key.h"
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
        evm_fork_t fork = config ? fork_get_active(hdr->number, hdr->timestamp, config) : FORK_CANCUN;
        env->blob_base_fee = calc_blob_gas_price(&env->excess_blob_gas, fork);
    }

    /* block_hash[256] would need to be populated from a block hash oracle.
     * For now, leave zeroed — BLOCKHASH opcode will return 0. */
}

/* =========================================================================
 * System contract call helper (Prague+)
 * ========================================================================= */

static void system_call(evm_t *evm, const uint8_t addr_bytes[20],
                        const uint8_t *calldata, size_t calldata_len) {
    address_t contract_addr, system_addr;
    memcpy(contract_addr.bytes, addr_bytes, 20);
    memset(system_addr.bytes, 0xff, 20);
    system_addr.bytes[19] = 0xfe;  /* SYSTEM_ADDRESS */

    /* Only call if contract has code */
    uint32_t code_len = 0;
    evm_state_get_code_ptr(evm->state, &contract_addr, &code_len);
    if (code_len == 0) return;

    /* Set tx context for system call */
    evm_tx_context_t sys_tx;
    memset(&sys_tx, 0, sizeof(sys_tx));
    address_copy(&sys_tx.origin, &system_addr);

    evm_set_tx_context(evm, &sys_tx);

    /* EIP-4762: record witness access for the target contract's basic_data.
     * System calls don't charge gas, but the access events must be recorded
     * so subsequent transactions see the address as warm. */
    if (evm->fork >= FORK_VERKLE) {
        uint8_t vk[32];
        verkle_account_basic_data_key(vk, addr_bytes);
        evm_state_witness_gas_access(evm->state, vk, false, false);
    }

    /* Execute: caller=SYSTEM_ADDRESS, generous gas, depth=0 */
    uint256_t zero = UINT256_ZERO;
    evm_message_t msg = evm_message_call(
        &system_addr, &contract_addr, &zero,
        calldata, calldata_len, 30000000, 0);
    evm_result_t result;
    evm_execute(evm, &msg, &result);
    evm_result_free(&result);

    /* Commit system call state changes (reset access lists, commit originals).
     * NOTE: witness gas is NOT reset here — it persists per-block (EIP-4762). */
    evm_state_commit_tx(evm->state);
}

/* =========================================================================
 * Block reward by fork (PoW era only)
 * ========================================================================= */

static uint256_t get_block_reward(evm_fork_t fork) {
    if (fork >= FORK_PARIS)
        return UINT256_ZERO;  /* No PoW reward after The Merge */
    if (fork >= FORK_CONSTANTINOPLE)
        return uint256_from_uint64(2000000000000000000ULL);  /* 2 ETH */
    if (fork >= FORK_BYZANTIUM)
        return uint256_from_uint64(3000000000000000000ULL);  /* 3 ETH */
    return uint256_from_uint64(5000000000000000000ULL);      /* 5 ETH */
}

/* =========================================================================
 * Block execution
 * ========================================================================= */

block_result_t block_execute(evm_t *evm,
                             const block_header_t *header,
                             const block_body_t *body,
                             const hash_t *block_hashes) {
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
    if (block_hashes)
        memcpy(evm_env.block_hash, block_hashes, sizeof(evm_env.block_hash));
    evm_set_block_env(evm, &evm_env);

    /* Build block_env_t for transaction_execute */
    block_env_t tx_env;
    header_to_block_env(header, &tx_env);

    /* Determine chain ID */
    uint64_t chain_id = evm->chain_config ? evm->chain_config->chain_id : 1;

    /* Commit state before executing transactions (EIP-2200 original values) */
    evm_state_commit(evm->state);

    /* Reset per-block witness gas state (EIP-4762) */
    evm_state_begin_block(evm->state);

    /* EIP-2935/EIP-7709: Store parent block hash in history contract (Prague+).
     * The contract at 0xff..fe is read-only (SLOAD), so we write storage
     * directly at the state level. */
    if (evm->fork >= FORK_PRAGUE) {
        static const uint8_t HISTORY_ADDR[20] = {
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe
        };
        #define BLOCKHASH_SERVE_WINDOW 8192

        address_t hist_addr;
        memcpy(hist_addr.bytes, HISTORY_ADDR, 20);

        /* Only write storage if the contract has code deployed */
        uint32_t hist_code_len = 0;
        evm_state_get_code_ptr(evm->state, &hist_addr, &hist_code_len);
        if (hist_code_len > 0) {
            /* Store parent hash at slot (block.number - 1) % SERVE_WINDOW */
            uint64_t slot_idx = (header->number - 1) % BLOCKHASH_SERVE_WINDOW;
            uint256_t slot = uint256_from_uint64(slot_idx);
            uint256_t parent_hash_val = uint256_from_bytes(header->parent_hash.bytes, 32);
            evm_state_set_storage(evm->state, &hist_addr, &slot, &parent_hash_val);
        }

        #undef BLOCKHASH_SERVE_WINDOW
    }

    /* EIP-4788: Store parent beacon block root (Cancun+) */
    if (evm->fork >= FORK_CANCUN && header->has_parent_beacon_root) {
        static const uint8_t BEACON_ROOT_ADDR[20] = {
            0x00, 0x0F, 0x3d, 0xf6, 0xD7, 0x32, 0x80, 0x7E, 0xf1, 0x31,
            0x9f, 0xB7, 0xB8, 0xbB, 0x85, 0x22, 0xd0, 0xBe, 0xac, 0x02
        };
        #define HISTORY_BUFFER_LENGTH 8191

        address_t beacon_addr;
        memcpy(beacon_addr.bytes, BEACON_ROOT_ADDR, 20);

        /* Only write storage if the contract has code deployed */
        uint32_t beacon_code_len = 0;
        evm_state_get_code_ptr(evm->state, &beacon_addr, &beacon_code_len);
        if (beacon_code_len > 0) {
            uint64_t ts_idx = header->timestamp % HISTORY_BUFFER_LENGTH;
            uint256_t ts_slot = uint256_from_uint64(ts_idx);
            uint256_t ts_val = uint256_from_uint64(header->timestamp);
            evm_state_set_storage(evm->state, &beacon_addr, &ts_slot, &ts_val);

            uint256_t root_slot = uint256_from_uint64(ts_idx + HISTORY_BUFFER_LENGTH);
            uint256_t root_val = uint256_from_bytes(header->parent_beacon_root.bytes, 32);
            evm_state_set_storage(evm->state, &beacon_addr, &root_slot, &root_val);
        }

        #undef HISTORY_BUFFER_LENGTH
    }

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

        /* Per-transaction commit: process self-destructs, reset access
         * lists and transient storage, commit storage originals.
         * Must happen after each tx so the next tx sees clean state. */
        evm_state_commit_tx(evm->state);
    }

    /* Pay block reward (PoW only — zero after The Merge) */
    uint256_t base_reward = get_block_reward(evm->fork);
    if (!uint256_is_zero(&base_reward)) {
        uint256_t miner_reward = base_reward;

        /* Uncle inclusion bonuses */
        size_t uncle_count = block_body_uncle_count(body);
        for (size_t u = 0; u < uncle_count; u++) {
            /* Miner gets base_reward/32 per uncle included */
            uint256_t thirty_two = uint256_from_uint64(32);
            uint256_t uncle_bonus = uint256_div(&base_reward, &thirty_two);
            miner_reward = uint256_add(&miner_reward, &uncle_bonus);

            /* Uncle miner gets base_reward * (uncle_num + 8 - block_num) / 8 */
            block_header_t uncle_hdr;
            if (block_body_get_uncle(body, u, &uncle_hdr)) {
                uint64_t depth = uncle_hdr.number + 8 - header->number;
                uint256_t depth_u = uint256_from_uint64(depth);
                uint256_t eight = uint256_from_uint64(8);
                uint256_t uncle_miner_reward = uint256_mul(&base_reward, &depth_u);
                uncle_miner_reward = uint256_div(&uncle_miner_reward, &eight);
                evm_state_add_balance(evm->state, &uncle_hdr.coinbase,
                                      &uncle_miner_reward);
            }
        }

        evm_state_add_balance(evm->state, &header->coinbase, &miner_reward);
    }

    /* Process EIP-4895 withdrawals (Shanghai+) */
    for (size_t w = 0; w < body->withdrawal_count; w++) {
        /* Withdrawal amount is in Gwei — convert to Wei (* 1e9) */
        uint256_t gwei = uint256_from_uint64(body->withdrawals[w].amount_gwei);
        uint256_t multiplier = uint256_from_uint64(1000000000ULL);
        uint256_t amount_wei = uint256_mul(&gwei, &multiplier);
        evm_state_add_balance(evm->state, &body->withdrawals[w].address,
                              &amount_wei);
    }

    /* EIP-7002: Dequeue withdrawal requests (Prague+) */
    if (evm->fork >= FORK_PRAGUE) {
        static const uint8_t WITHDRAWAL_REQ_ADDR[20] = {
            0x00, 0x00, 0x09, 0x61, 0xef, 0x48, 0x0e, 0xb5, 0x5e, 0x80,
            0xd1, 0x9a, 0xd8, 0x35, 0x79, 0xa6, 0x4c, 0x00, 0x70, 0x02
        };
        system_call(evm, WITHDRAWAL_REQ_ADDR, NULL, 0);
    }

    /* EIP-7251: Dequeue consolidation requests (Prague+) */
    if (evm->fork >= FORK_PRAGUE) {
        static const uint8_t CONSOLIDATION_REQ_ADDR[20] = {
            0x00, 0x00, 0xbb, 0xdd, 0xc7, 0xce, 0x48, 0x86, 0x42, 0xfb,
            0x57, 0x9f, 0x8b, 0x00, 0xf3, 0xa5, 0x90, 0x00, 0x72, 0x51
        };
        system_call(evm, CONSOLIDATION_REQ_ADDR, NULL, 0);
    }

    /* Finalize state: flush dirty accounts/storage to state_db */
    evm_state_finalize(evm->state);

    /* Compute state root — prune empty accounts post-Spurious Dragon (EIP-161) */
    bool prune_empty = (evm->fork >= FORK_SPURIOUS_DRAGON);
    result.state_root = evm_state_compute_state_root_ex(evm->state, prune_empty);
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
