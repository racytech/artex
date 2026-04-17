#include "mem_mpt.h"
#include "block_executor.h"
#ifdef ENABLE_HISTORY
#include "state_history.h"
#endif
#include "tx_pipeline.h"
#include "dao_fork.h"
#include "tx_decoder.h"
#include "fork.h"
#include "transaction.h"

#include "rlp.h"
#include "hash.h"
#include "evm_state.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#ifdef ENABLE_DEBUG
extern bool g_trace_calls __attribute__((weak));
#else
static const bool g_trace_calls = false;
#endif

/* =========================================================================
 * Bloom Filter (2048-bit / 256-byte Ethereum logs bloom)
 * ========================================================================= */

/** Add a single item (address or topic) to the bloom filter.
 *  Ethereum bloom is big-endian: bit position b maps to byte 255 - b/8. */
static void bloom_add(uint8_t bloom[256], const uint8_t *data, size_t len) {
    hash_t h = hash_keccak256(data, len);
    for (int i = 0; i < 6; i += 2) {
        uint16_t bit = ((uint16_t)h.bytes[i] << 8 | h.bytes[i + 1]) & 0x7FF;
        bloom[255 - bit / 8] |= (uint8_t)(1 << (bit % 8));
    }
}

/** Compute bloom filter from a set of log entries. */
static void bloom_from_logs(uint8_t bloom[256],
                            const evm_log_t *logs, size_t log_count) {
    memset(bloom, 0, 256);
    for (size_t i = 0; i < log_count; i++) {
        bloom_add(bloom, logs[i].address.bytes, 20);
        for (uint8_t t = 0; t < logs[i].topic_count; t++)
            bloom_add(bloom, logs[i].topics[t].bytes, 32);
    }
}

/** OR src bloom into dst bloom. */
static void bloom_or(uint8_t dst[256], const uint8_t src[256]) {
    for (int i = 0; i < 256; i++) dst[i] |= src[i];
}

/* =========================================================================
 * Receipt RLP Encoding
 * ========================================================================= */

/**
 * Encode a transaction receipt as RLP bytes.
 *
 * Post-Byzantium: RLP([status, cumulative_gas, bloom, logs_list])
 * For typed txs (type > 0): prepend type byte (EIP-2718 envelope).
 *
 * Caller must free returned bytes_t.data.
 */
static bytes_t receipt_encode_rlp(uint8_t tx_type, uint8_t status,
                                   uint64_t cumulative_gas,
                                   const uint8_t bloom[256],
                                   const evm_log_t *logs, size_t log_count) {
    rlp_item_t *receipt = rlp_list_new();

    /* Status: 0 → empty byte string (0x80), 1 → single byte 0x01 */
    if (status == 0)
        rlp_list_append(receipt, rlp_string(NULL, 0));
    else
        rlp_list_append(receipt, rlp_string(&status, 1));

    rlp_list_append(receipt, rlp_uint64(cumulative_gas));
    rlp_list_append(receipt, rlp_string(bloom, 256));

    rlp_item_t *logs_list = rlp_list_new();
    for (size_t i = 0; i < log_count; i++) {
        rlp_item_t *log_item = rlp_list_new();
        rlp_list_append(log_item, rlp_string(logs[i].address.bytes, 20));

        rlp_item_t *topics = rlp_list_new();
        for (uint8_t t = 0; t < logs[i].topic_count; t++)
            rlp_list_append(topics, rlp_string(logs[i].topics[t].bytes, 32));
        rlp_list_append(log_item, topics);

        rlp_list_append(log_item, rlp_string(logs[i].data, logs[i].data_len));
        rlp_list_append(logs_list, log_item);
    }
    rlp_list_append(receipt, logs_list);

    bytes_t rlp_bytes = rlp_encode(receipt);
    rlp_item_free(receipt);

    /* For typed tx (type > 0): prepend type byte (EIP-2718 envelope) */
    if (tx_type > 0) {
        uint8_t *typed = malloc(1 + rlp_bytes.len);
        if (typed) {
            typed[0] = tx_type;
            memcpy(typed + 1, rlp_bytes.data, rlp_bytes.len);
            free(rlp_bytes.data);
            rlp_bytes.data = typed;
            rlp_bytes.len += 1;
        }
    }
    return rlp_bytes;
}

/**
 * Compute receipt trie root from receipts array.
 * Keys = RLP(index), values = receipt RLP bytes.
 * Uses unsecured MPT (raw keys, not hashed).
 */
static hash_t compute_receipt_root(const tx_receipt_t *receipts, size_t count) {
    hash_t root;
    if (count == 0) {
        /* Empty trie root = keccak256(0x80) */
        const uint8_t empty_rlp[] = {0x80};
        root = hash_keccak256(empty_rlp, 1);
        return root;
    }

    mpt_unsecured_entry_t *entries = calloc(count, sizeof(*entries));
    bytes_t *keys = calloc(count, sizeof(bytes_t));
    bytes_t *values = calloc(count, sizeof(bytes_t));

    for (size_t i = 0; i < count; i++) {
        keys[i] = rlp_encode_uint64_direct(i);
        entries[i].key = keys[i].data;
        entries[i].key_len = keys[i].len;

        values[i] = receipt_encode_rlp(
            receipts[i].tx_type,
            receipts[i].status_code,
            receipts[i].cumulative_gas,
            receipts[i].logs_bloom,
            receipts[i].logs,
            receipts[i].log_count);
        entries[i].value = values[i].data;
        entries[i].value_len = values[i].len;
    }

    mpt_compute_root_unsecured(entries, count, &root);

    for (size_t i = 0; i < count; i++) {
        free(keys[i].data);
        free(values[i].data);
    }
    free(entries);
    free(keys);
    free(values);

    return root;
}

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
    address_copy(&env->coinbase, &hdr->coinbase);

    /* Post-merge (EIP-4399): DIFFICULTY opcode returns PREVRANDAO.
     * Pre-merge: difficulty from block header as usual. */
    if (uint256_is_zero(&hdr->difficulty))
        memcpy(&env->difficulty, hdr->mix_hash.bytes, 32);  /* prevRandao → difficulty */
    else
        uint256_copy(&env->difficulty, &hdr->difficulty);

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

/**
 * Execute a system call and optionally capture return data.
 * If out_data/out_len are non-NULL, the caller takes ownership of the output.
 */
static void system_call(evm_t *evm, const uint8_t addr_bytes[20],
                        const uint8_t *calldata, size_t calldata_len,
                        uint8_t **out_data, size_t *out_len) {
    if (out_data) *out_data = NULL;
    if (out_len) *out_len = 0;

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

    /* Execute: caller=SYSTEM_ADDRESS, generous gas, depth=0 */
    uint256_t zero = UINT256_ZERO;
    evm_message_t msg = evm_message_call(
        &system_addr, &contract_addr, &zero,
        calldata, calldata_len, 30000000, 0);
    evm_result_t result;
    evm_execute(evm, &msg, &result);

    /* Capture return data if requested */
    if (out_data && result.output_data && result.output_size > 0) {
        *out_data = result.output_data;
        *out_len = result.output_size;
        result.output_data = NULL;  /* transfer ownership */
        result.output_size = 0;
    }

    evm_result_free(&result);

    /* Commit system call state changes (reset access lists, commit originals).
     * NOTE: geth's Prepare() creates a fresh access list before each tx anyway,
     * so system call warmth does NOT persist into user transactions. */
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
 * Block execution helpers — standalone functions matching block_execute chunks.
 * Standalone functions matching block_execute chunks for reuse.
 * ========================================================================= */

/**
 * Set up EVM and transaction environments from a block header.
 * Applies block hashes to the EVM env if provided.
 */
static void block_setup_env(evm_t *evm,
                            const block_header_t *header,
                            const hash_t *block_hashes,
                            evm_block_env_t *evm_env,
                            block_env_t *tx_env) {
    header_to_evm_block_env(header, evm_env, evm->chain_config);
    if (block_hashes)
        memcpy(evm_env->block_hash, block_hashes, sizeof(evm_env->block_hash));
    evm_set_block_env(evm, evm_env);
    header_to_block_env(header, tx_env);
}

/**
 * Pre-block initialization: prune mode, commit originals, begin block,
 * DAO fork, EIP-4788 beacon root, EIP-2935 history.
 */
static void block_pre_init(evm_t *evm, const block_header_t *header) {
    uint64_t chain_id = evm->chain_config ? evm->chain_config->chain_id : 1;

    evm_state_set_prune_empty(evm->state, evm->fork >= FORK_SPURIOUS_DRAGON);
    evm_state_commit(evm->state);
    evm_state_begin_block(evm->state, header->number);

    /* DAO fork: drain 116 accounts into refund contract */
    if (header->number == DAO_FORK_BLOCK && chain_id == 1)
        apply_dao_fork(evm->state);

    /* EIP-4788: Store parent beacon block root (Cancun+) */
    if (evm->fork >= FORK_CANCUN && header->has_parent_beacon_root) {
        static const uint8_t BEACON_ROOT_ADDR[20] = {
            0x00, 0x0F, 0x3d, 0xf6, 0xD7, 0x32, 0x80, 0x7E, 0xf1, 0x31,
            0x9f, 0xB7, 0xB8, 0xbB, 0x85, 0x22, 0xd0, 0xBe, 0xac, 0x02
        };
        system_call(evm, BEACON_ROOT_ADDR,
                    header->parent_beacon_root.bytes, 32, NULL, NULL);
    }

    /* EIP-2935: Store parent block hash in history contract (Prague+) */
    if (evm->fork >= FORK_PRAGUE) {
        static const uint8_t HISTORY_ADDR[20] = {
            0x00, 0x00, 0xf9, 0x08, 0x27, 0xf1, 0xc5, 0x3a, 0x10, 0xcb,
            0x7a, 0x02, 0x33, 0x5b, 0x17, 0x53, 0x20, 0x00, 0x29, 0x35
        };
        system_call(evm, HISTORY_ADDR,
                    header->parent_hash.bytes, 32, NULL, NULL);
    }
}

/**
 * Pay block rewards (PoW only — zero after The Merge).
 * Includes uncle inclusion bonuses.
 */
static void block_pay_rewards(evm_t *evm, const block_header_t *header,
                              const block_body_t *body) {
    uint256_t base_reward = get_block_reward(evm->fork);
    if (uint256_is_zero(&base_reward)) return;

    uint256_t miner_reward = base_reward;
    size_t uncle_count = block_body_uncle_count(body);
    for (size_t u = 0; u < uncle_count; u++) {
        uint256_t thirty_two = uint256_from_uint64(32);
        uint256_t uncle_bonus = uint256_div(&base_reward, &thirty_two);
        miner_reward = uint256_add(&miner_reward, &uncle_bonus);

        block_header_t uncle_hdr;
        if (block_body_get_uncle(body, u, &uncle_hdr)) {
            if (uncle_hdr.number >= header->number ||
                header->number - uncle_hdr.number > 7)
                continue;
            uint64_t depth = uncle_hdr.number + 8 - header->number;
            if (g_trace_calls) {
                fprintf(stderr, "UNCLE[%zu] number=%lu depth=%lu coinbase=%02x%02x..%02x%02x\n",
                        u, uncle_hdr.number, depth,
                        uncle_hdr.coinbase.bytes[0], uncle_hdr.coinbase.bytes[1],
                        uncle_hdr.coinbase.bytes[18], uncle_hdr.coinbase.bytes[19]);
            }
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

/**
 * Process EIP-4895 withdrawals (Shanghai+).
 */
static void block_process_withdrawals(evm_t *evm, const block_body_t *body) {
    for (size_t w = 0; w < body->withdrawal_count; w++) {
        uint256_t gwei = uint256_from_uint64(body->withdrawals[w].amount_gwei);
        uint256_t multiplier = uint256_from_uint64(1000000000ULL);
        uint256_t amount_wei = uint256_mul(&gwei, &multiplier);
        evm_state_add_balance(evm->state, &body->withdrawals[w].address,
                              &amount_wei);
    }
}

/**
 * Aggregate per-tx blooms into block bloom, compute receipt root.
 */
static void block_aggregate_receipts(tx_receipt_t *receipts, size_t count,
                                     uint8_t block_bloom[256],
                                     hash_t *receipt_root) {
    memset(block_bloom, 0, 256);
    for (size_t i = 0; i < count; i++)
        bloom_or(block_bloom, receipts[i].logs_bloom);
    *receipt_root = compute_receipt_root(receipts, count);
}

/**
 * Finalize block state: flush dirty, capture diff, finalize_block,
 * compute root, reset per-block caches.
 * Returns state root.
 */
static hash_t block_finalize_state(evm_t *evm,
#ifdef ENABLE_HISTORY
                                   state_history_t *history,
                                   block_diff_t *diff,
                                   uint64_t block_number,
#endif
                                   double *root_ms_out) {
    evm_state_finalize(evm->state);

#ifdef ENABLE_HISTORY
    if (diff) {
        memset(diff, 0, sizeof(*diff));
        diff->block_number = block_number;
        evm_state_collect_block_diff(evm->state, diff);
    }
    if (history && diff) {
        block_diff_t hist_diff;
        block_diff_clone(diff, &hist_diff);
        state_history_push(history, &hist_diff);
    }
#endif

    bool prune_empty = (evm->fork >= FORK_SPURIOUS_DRAGON);
    evm_state_finalize_block(evm->state, prune_empty);

    struct timespec _rt0, _rt1;
    clock_gettime(CLOCK_MONOTONIC, &_rt0);
    hash_t root = {0};
    if (!evm->skip_root_hash)
        root = evm_state_compute_state_root_ex(evm->state, prune_empty);
    clock_gettime(CLOCK_MONOTONIC, &_rt1);

    if (root_ms_out)
        *root_ms_out = (_rt1.tv_sec - _rt0.tv_sec) * 1000.0 +
                       (_rt1.tv_nsec - _rt0.tv_nsec) / 1e6;

    /* Clear undo log unless caller wants to keep it for revert */
    if (!evm->keep_undo)
        evm_state_reset_block(evm->state);

    return root;
}

/* =========================================================================
 * Block execution
 * ========================================================================= */

block_result_t block_execute(evm_t *evm,
                             const block_header_t *header,
                             const block_body_t *body,
                             const hash_t *block_hashes
#ifdef ENABLE_HISTORY
                             , state_history_t *history
#endif
                             ) {
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

    /* Set up block + tx environments */
    evm_block_env_t evm_env;
    block_env_t tx_env;
    block_setup_env(evm, header, block_hashes, &evm_env, &tx_env);

    /* Pre-block init: prune mode, commit, begin_block, DAO fork, system calls */
    block_pre_init(evm, header);

    /* Determine chain ID */
    uint64_t chain_id = evm->chain_config ? evm->chain_config->chain_id : 1;

    uint64_t cumulative_gas = 0;
    result.success = true;

    if (g_trace_calls) {
        fprintf(stderr, "BLOCK number=%lu gas_limit=%lu timestamp=%lu difficulty_lo=%lu difficulty_hi=%lu\n",
                header->number, header->gas_limit, header->timestamp,
                (unsigned long)header->difficulty.low, (unsigned long)header->difficulty.high);
    }

    /* Batch-decode all transactions in parallel (RLP + ecrecover).
     * Use 4 threads for blocks with 16+ txs, serial for small blocks. */
    #define BATCH_DECODE_THREADS  4
    #define BATCH_DECODE_THRESH  16
    prepared_tx_t *decoded = NULL;
    if (tx_count > 0) {
        decoded = calloc(tx_count, sizeof(prepared_tx_t));
        if (!decoded) { result.success = false; return result; }
        int nthreads = (tx_count >= BATCH_DECODE_THRESH) ? BATCH_DECODE_THREADS : 1;
        tx_batch_decode(body, tx_count, chain_id, decoded, nthreads);
    }

    size_t last_tx = tx_count;  /* track how far we got for cleanup */
    for (size_t i = 0; i < tx_count; i++) {
        prepared_tx_t *ptx = &decoded[i];

        transaction_t tx;
        if (!ptx->valid) {
            fprintf(stderr, "block_execute: failed to decode tx %zu\n", i);
            result.success = false;
            if (result.first_failure < 0) result.first_failure = (int)i;
            last_tx = i;
            break;
        }
        tx = ptx->tx;

        /* Trace transaction details when debugging */
        if (g_trace_calls) {
            fprintf(stderr, "TX[%zu] sender=%02x%02x..%02x%02x to=%02x%02x..%02x%02x "
                    "is_create=%d nonce=%lu gas_limit=%lu data_len=%zu\n",
                    i,
                    tx.sender.bytes[0], tx.sender.bytes[1],
                    tx.sender.bytes[18], tx.sender.bytes[19],
                    tx.to.bytes[0], tx.to.bytes[1],
                    tx.to.bytes[18], tx.to.bytes[19],
                    tx.is_create, tx.nonce, tx.gas_limit, tx.data_size);
        }

        /* Classify: simple transfer vs contract interaction */
        if (!tx.is_create && tx.data_size == 0)
            result.transfer_count++;
        else
            result.call_count++;

        /* Execute transaction */
        transaction_result_t tx_result;
        bool ok = transaction_execute(evm, &tx, &tx_env, &tx_result);

        /* Fill receipt */
        result.receipts[i].success = ok;
        result.receipts[i].gas_used = ok ? tx_result.gas_used : 0;
        cumulative_gas += result.receipts[i].gas_used;

        result.receipts[i].cumulative_gas = cumulative_gas;
        result.receipts[i].tx_type = (uint8_t)tx.type;
        result.receipts[i].status_code = (ok && tx_result.status == EVM_SUCCESS) ? 1 : 0;

        if (ok && tx_result.contract_created) {
            result.receipts[i].contract_created = true;
            address_copy(&result.receipts[i].contract_addr,
                         &tx_result.contract_address);
        }

        /* Transfer logs from tx result to receipt (move, not copy) */
        if (ok) {
            result.receipts[i].logs = tx_result.logs;
            result.receipts[i].log_count = tx_result.log_count;
            tx_result.logs = NULL;
            tx_result.log_count = 0;
            transaction_result_free(&tx_result);
        } else {
            result.receipts[i].logs = NULL;
            result.receipts[i].log_count = 0;
            if (result.first_failure < 0) result.first_failure = (int)i;
            /* Continue executing remaining txs — a single tx failure
             * doesn't abort the block (the tx is still included,
             * it just consumes all its gas). */
        }

        /* Compute per-tx bloom from logs */
        bloom_from_logs(result.receipts[i].logs_bloom,
                        result.receipts[i].logs,
                        result.receipts[i].log_count);

        tx_decoded_free(&tx);

        /* Per-transaction commit: process self-destructs, reset access
         * lists and transient storage, commit storage originals.
         * Must happen after each tx so the next tx sees clean state. */
        evm_state_commit_tx(evm->state);
    }

    /* Aggregate per-tx blooms + compute receipt root */
    block_aggregate_receipts(result.receipts, tx_count,
                             result.logs_bloom, &result.receipt_root);

    /* Free remaining decoded txs (if execution broke early) */
    if (decoded) {
        for (size_t j = last_tx; j < tx_count; j++) {
            if (decoded[j].valid) tx_decoded_free(&decoded[j].tx);
        }
        free(decoded);
    }

    /* Block rewards (PoW only) + withdrawals (Shanghai+) */
    block_pay_rewards(evm, header, body);
    block_process_withdrawals(evm, body);

    /* EIP-7685: Accumulate execution requests (Prague+)
     * Three request types collected in order:
     *   0x00 = deposits (EIP-6110, extracted from receipt logs)
     *   0x01 = withdrawal requests (EIP-7002, from system call return data)
     *   0x02 = consolidation requests (EIP-7251, from system call return data)
     */
    if (evm->fork >= FORK_PRAGUE) {
        /* Temporary storage for up to 3 request types */
        uint8_t *req_bufs[3] = {NULL, NULL, NULL};
        size_t   req_lens[3] = {0, 0, 0};

        /* --- EIP-6110: Extract deposit requests from transaction logs --- */
        static const uint8_t DEPOSIT_ADDR[20] = {
            0x00, 0x00, 0x00, 0x00, 0x21, 0x9a, 0xb5, 0x40, 0x35, 0x6c,
            0xbb, 0x83, 0x9c, 0xbe, 0x05, 0x30, 0x3d, 0x77, 0x05, 0xfa
        };
        /* DepositEvent(bytes,bytes,bytes,bytes,bytes) topic0 */
        static const uint8_t DEPOSIT_TOPIC[32] = {
            0x64, 0x9b, 0xbc, 0x62, 0xd0, 0xe3, 0x13, 0x42,
            0xaf, 0xea, 0x4e, 0x5c, 0xd8, 0x2d, 0x40, 0x49,
            0xe7, 0xe1, 0xee, 0x91, 0x2f, 0xc0, 0x88, 0x9a,
            0xa7, 0x90, 0x80, 0x3b, 0xe3, 0x90, 0x38, 0xc5
        };
        {
            /* Count deposit logs — must match address AND topic0 */
            size_t deposit_count = 0;
            for (size_t r = 0; r < result.receipt_count; r++) {
                for (size_t l = 0; l < result.receipts[r].log_count; l++) {
                    evm_log_t *log = &result.receipts[r].logs[l];
                    if (memcmp(log->address.bytes, DEPOSIT_ADDR, 20) == 0 &&
                        log->topic_count >= 1 &&
                        memcmp(log->topics[0].bytes, DEPOSIT_TOPIC, 32) == 0)
                        deposit_count++;
                }
            }
            if (deposit_count > 0) {
                /* Each deposit: type(1) + pubkey(48) + creds(32) + amount(8) + sig(96) + index(8) = 193 bytes */
                size_t deposit_data_len = deposit_count * 192;  /* without type prefix */
                req_bufs[0] = malloc(1 + deposit_data_len);
                if (req_bufs[0]) {
                    req_bufs[0][0] = 0x00;  /* type byte */
                    size_t off = 1;
                    for (size_t r = 0; r < result.receipt_count; r++) {
                        for (size_t l = 0; l < result.receipts[r].log_count; l++) {
                            evm_log_t *log = &result.receipts[r].logs[l];
                            if (memcmp(log->address.bytes, DEPOSIT_ADDR, 20) != 0 ||
                                log->topic_count < 1 ||
                                memcmp(log->topics[0].bytes, DEPOSIT_TOPIC, 32) != 0)
                                continue;
                            /* ABI-decoded deposit log data (576 bytes):
                             * 5 offsets (160B) + 5 length-prefixed fields.
                             * Validate layout per EIP-6110 before extracting.
                             * Invalid layout → mark block invalid. */
                            if (log->data_len < 576) {
                                result.deposit_layout_invalid = true;
                                continue;
                            }
                            /* Validate ABI offsets and field lengths */
                            {
                                const uint8_t *d = log->data;
                                /* Read uint64 from last 8 bytes of 32-byte word */
                                #define W64(off) ( \
                                    ((uint64_t)d[(off)+24]<<56) | ((uint64_t)d[(off)+25]<<48) | \
                                    ((uint64_t)d[(off)+26]<<40) | ((uint64_t)d[(off)+27]<<32) | \
                                    ((uint64_t)d[(off)+28]<<24) | ((uint64_t)d[(off)+29]<<16) | \
                                    ((uint64_t)d[(off)+30]<<8)  |  (uint64_t)d[(off)+31])
                                /* Check first 24 bytes of word are zero */
                                #define HI0(off) ( \
                                    d[(off)]==0 && d[(off)+1]==0 && d[(off)+2]==0 && d[(off)+3]==0 && \
                                    d[(off)+4]==0 && d[(off)+5]==0 && d[(off)+6]==0 && d[(off)+7]==0 && \
                                    d[(off)+8]==0 && d[(off)+9]==0 && d[(off)+10]==0 && d[(off)+11]==0 && \
                                    d[(off)+12]==0 && d[(off)+13]==0 && d[(off)+14]==0 && d[(off)+15]==0 && \
                                    d[(off)+16]==0 && d[(off)+17]==0 && d[(off)+18]==0 && d[(off)+19]==0 && \
                                    d[(off)+20]==0 && d[(off)+21]==0 && d[(off)+22]==0 && d[(off)+23]==0)
                                bool valid =
                                    HI0(0)   && W64(0)   == 0xa0  &&  /* pubkey offset */
                                    HI0(32)  && W64(32)  == 0x100 &&  /* withdrawal_creds offset */
                                    HI0(64)  && W64(64)  == 0x140 &&  /* amount offset */
                                    HI0(96)  && W64(96)  == 0x180 &&  /* signature offset */
                                    HI0(128) && W64(128) == 0x200 &&  /* index offset */
                                    HI0(160) && W64(160) == 48  &&    /* pubkey length */
                                    HI0(256) && W64(256) == 32  &&    /* withdrawal_creds length */
                                    HI0(320) && W64(320) == 8   &&    /* amount length */
                                    HI0(384) && W64(384) == 96  &&    /* signature length */
                                    HI0(512) && W64(512) == 8;        /* index length */
                                #undef W64
                                #undef HI0
                                if (!valid) {
                                    result.deposit_layout_invalid = true;
                                    continue;
                                }
                            }
                            memcpy(req_bufs[0] + off,       log->data + 192, 48);  /* pubkey */
                            memcpy(req_bufs[0] + off + 48,  log->data + 288, 32);  /* withdrawal_credentials */
                            memcpy(req_bufs[0] + off + 80,  log->data + 352, 8);   /* amount */
                            memcpy(req_bufs[0] + off + 88,  log->data + 416, 96);  /* signature */
                            memcpy(req_bufs[0] + off + 184, log->data + 544, 8);   /* index */
                            off += 192;
                        }
                    }
                    req_lens[0] = off;
                }
            }
        }

        /* --- EIP-7002: Dequeue withdrawal requests --- */
        {
            static const uint8_t WITHDRAWAL_REQ_ADDR[20] = {
                0x00, 0x00, 0x09, 0x61, 0xef, 0x48, 0x0e, 0xb5, 0x5e, 0x80,
                0xd1, 0x9a, 0xd8, 0x35, 0x79, 0xa6, 0x4c, 0x00, 0x70, 0x02
            };
            uint8_t *wd_data = NULL;
            size_t wd_len = 0;
            system_call(evm, WITHDRAWAL_REQ_ADDR, NULL, 0, &wd_data, &wd_len);
            if (wd_data && wd_len > 0) {
                req_bufs[1] = malloc(1 + wd_len);
                if (req_bufs[1]) {
                    req_bufs[1][0] = 0x01;  /* type byte */
                    memcpy(req_bufs[1] + 1, wd_data, wd_len);
                    req_lens[1] = 1 + wd_len;
                }
                free(wd_data);
            }
        }

        /* --- EIP-7251: Dequeue consolidation requests --- */
        {
            static const uint8_t CONSOLIDATION_REQ_ADDR[20] = {
                0x00, 0x00, 0xbb, 0xdd, 0xc7, 0xce, 0x48, 0x86, 0x42, 0xfb,
                0x57, 0x9f, 0x8b, 0x00, 0xf3, 0xa5, 0x90, 0x00, 0x72, 0x51
            };
            uint8_t *cons_data = NULL;
            size_t cons_len = 0;
            system_call(evm, CONSOLIDATION_REQ_ADDR, NULL, 0, &cons_data, &cons_len);
            if (cons_data && cons_len > 0) {
                req_bufs[2] = malloc(1 + cons_len);
                if (req_bufs[2]) {
                    req_bufs[2][0] = 0x02;  /* type byte */
                    memcpy(req_bufs[2] + 1, cons_data, cons_len);
                    req_lens[2] = 1 + cons_len;
                }
                free(cons_data);
            }
        }

        /* Collect non-empty requests into result */
        size_t total_reqs = 0;
        for (int i = 0; i < 3; i++)
            if (req_lens[i] > 0) total_reqs++;

        if (total_reqs > 0) {
            result.requests = calloc(total_reqs, sizeof(uint8_t *));
            result.request_lengths = calloc(total_reqs, sizeof(size_t));
            result.request_count = total_reqs;
            size_t idx = 0;
            for (int i = 0; i < 3; i++) {
                if (req_lens[i] > 0) {
                    result.requests[idx] = req_bufs[i];
                    result.request_lengths[idx] = req_lens[i];
                    idx++;
                }
            }
        } else {
            /* Free any allocated but empty buffers */
            for (int i = 0; i < 3; i++) free(req_bufs[i]);
        }
    }

    /* Finalize state, compute root, reset per-block caches */
    result.state_root = block_finalize_state(evm,
#ifdef ENABLE_HISTORY
                                             history, &result.diff, header->number,
#endif
                                             &result.root_ms);
    result.gas_used = cumulative_gas;

    return result;
}

void block_result_free(block_result_t *result) {
    if (result) {
        for (size_t i = 0; i < result->receipt_count; i++) {
            for (size_t j = 0; j < result->receipts[i].log_count; j++)
                evm_log_free(&result->receipts[i].logs[j]);
            free(result->receipts[i].logs);
        }
        free(result->receipts);
        result->receipts = NULL;
        result->receipt_count = 0;

        for (size_t i = 0; i < result->request_count; i++)
            free(result->requests[i]);
        free(result->requests);
        free(result->request_lengths);
        result->requests = NULL;
        result->request_lengths = NULL;
        result->request_count = 0;

#ifdef ENABLE_HISTORY
        block_diff_free(&result->diff);
#endif
    }
}
