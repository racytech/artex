/**
 * Execute block 1 after snapshot via BOTH paths and compare:
 *   Path A: block_execute directly (same as C chain_replay / sync)
 *   Path B: rx_execute_block (API path, same as Python)
 *
 * If roots differ, the conversion layer is the problem.
 * If roots match, the Python EP parsing is the problem.
 */
#include "artex.h"
#include "evm.h"
#include "evm_state.h"
#include "block.h"
#include "block_executor.h"
#include "era.h"
#include "fork.h"
#include "hash.h"
#include "code_store.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <state.bin> <era_dir>\n", argv[0]);
        return 1;
    }
    const char *state_path = argv[1];
    const char *era_dir = argv[2];

    /* Get data_dir from state path */
    char data_dir[512];
    snprintf(data_dir, sizeof(data_dir), "%s", state_path);
    char *slash = strrchr(data_dir, '/');
    if (slash) *slash = '\0';

    /* ================================================================
     * Path A: Direct block_execute (same as C chain_replay)
     * ================================================================ */
    printf("=== Path A: Direct block_execute ===\n");

    /* Create state + evm manually (like sync does) */
    char cs_path[1024];
    snprintf(cs_path, sizeof(cs_path), "%s/chain_replay_code", data_dir);
    code_store_t *cs = code_store_open(cs_path);
    printf("code_store: %s\n", cs ? "opened" : "FAILED");

    evm_state_t *es_a = evm_state_create(cs);
    const chain_config_t *chain_cfg = chain_config_mainnet();
    evm_t *evm_a = evm_create(es_a, chain_cfg);

    /* Load state */
    state_t *st_a = evm_state_get_state(es_a);
    hash_t loaded_root;
    state_load(st_a, state_path, &loaded_root);
    uint64_t block_num = state_get_block(st_a);
    printf("Loaded state at block %lu\n", block_num);

    /* Find the next block in era files */
    uint64_t target = block_num + 1;
    block_header_t hdr_a;
    block_body_t body_a;
    uint8_t hash_a[32];
    bool found = false;

    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "ls %s/mainnet-*.era 2>/dev/null | sort", era_dir);
    FILE *pipe = popen(cmd, "r");
    char era_path[512];
    while (fgets(era_path, sizeof(era_path), pipe)) {
        era_path[strcspn(era_path, "\n")] = '\0';
        era_t era;
        if (!era_open(&era, era_path)) continue;
        era_iter_t it = era_iter(&era);
        uint64_t slot;
        while (era_iter_next(&it, &hdr_a, &body_a, hash_a, &slot)) {
            if (hdr_a.number == target) { found = true; break; }
            block_body_free(&body_a);
            if (hdr_a.number > target) break;
        }
        era_close(&era);
        if (found) break;
    }
    pclose(pipe);

    if (!found) {
        fprintf(stderr, "Block %lu not found\n", target);
        return 1;
    }

    printf("Block %lu: %zu txs, gas_used=%lu\n",
           hdr_a.number, body_a.tx_count, hdr_a.gas_used);

    /* Set fork */
    evm_a->fork = fork_get_active(hdr_a.number, hdr_a.timestamp, chain_cfg);

    /* Execute via direct path */
    hash_t block_hashes_a[256] = {0};
    block_result_t br_a = block_execute(evm_a, &hdr_a, &body_a, block_hashes_a
#ifdef ENABLE_HISTORY
        , NULL
#endif
    );

    printf("Path A: gas=%lu, state_root=", br_a.gas_used);
    for (int i = 0; i < 32; i++) printf("%02x", br_a.state_root.bytes[i]);
    printf("\n");

    block_result_free(&br_a);
    block_body_free(&body_a);
    evm_destroy(evm_a);
    evm_state_destroy(es_a);
    if (cs) code_store_destroy(cs);

    /* ================================================================
     * Path B: rx_execute_block (API path)
     * ================================================================ */
    printf("\n=== Path B: rx_execute_block ===\n");

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET, .data_dir = data_dir };
    rx_engine_t *engine = rx_engine_create(&config);
    if (!engine) { fprintf(stderr, "FAIL: rx_engine_create\n"); return 1; }
    if (!rx_engine_load_state(engine, state_path)) {
        fprintf(stderr, "FAIL: rx_engine_load_state\n"); return 1;
    }
    printf("Loaded state at block %lu\n", rx_get_block_number(engine));

    /* Find the block again (state was loaded fresh) */
    found = false;
    pipe = popen(cmd, "r");
    block_header_t hdr_b;
    block_body_t body_b;
    uint8_t hash_b[32];
    while (fgets(era_path, sizeof(era_path), pipe)) {
        era_path[strcspn(era_path, "\n")] = '\0';
        era_t era;
        if (!era_open(&era, era_path)) continue;
        era_iter_t it = era_iter(&era);
        uint64_t slot;
        while (era_iter_next(&it, &hdr_b, &body_b, hash_b, &slot)) {
            if (hdr_b.number == target) { found = true; break; }
            block_body_free(&body_b);
            if (hdr_b.number > target) break;
        }
        era_close(&era);
        if (found) break;
    }
    pclose(pipe);

    /* Convert C types → rx types (same as convert_header/convert_body) */
    rx_block_header_t rx_hdr;
    memset(&rx_hdr, 0, sizeof(rx_hdr));
    memcpy(rx_hdr.parent_hash.bytes, hdr_b.parent_hash.bytes, 32);
    memcpy(rx_hdr.uncle_hash.bytes, hdr_b.uncle_hash.bytes, 32);
    memcpy(rx_hdr.coinbase.bytes, hdr_b.coinbase.bytes, 20);
    memcpy(rx_hdr.state_root.bytes, hdr_b.state_root.bytes, 32);
    memcpy(rx_hdr.tx_root.bytes, hdr_b.tx_root.bytes, 32);
    memcpy(rx_hdr.receipt_root.bytes, hdr_b.receipt_root.bytes, 32);
    memcpy(rx_hdr.logs_bloom, hdr_b.logs_bloom, 256);
    uint256_to_bytes(&hdr_b.difficulty, rx_hdr.difficulty.bytes);
    rx_hdr.number = hdr_b.number;
    rx_hdr.gas_limit = hdr_b.gas_limit;
    rx_hdr.gas_used = hdr_b.gas_used;
    rx_hdr.timestamp = hdr_b.timestamp;
    memcpy(rx_hdr.extra_data, hdr_b.extra_data, 32);
    rx_hdr.extra_data_len = hdr_b.extra_data_len;
    memcpy(rx_hdr.mix_hash.bytes, hdr_b.mix_hash.bytes, 32);
    rx_hdr.nonce = hdr_b.nonce;
    rx_hdr.has_base_fee = hdr_b.has_base_fee;
    if (hdr_b.has_base_fee) uint256_to_bytes(&hdr_b.base_fee, rx_hdr.base_fee.bytes);
    rx_hdr.has_withdrawals_root = hdr_b.has_withdrawals_root;
    if (hdr_b.has_withdrawals_root)
        memcpy(rx_hdr.withdrawals_root.bytes, hdr_b.withdrawals_root.bytes, 32);
    rx_hdr.has_blob_gas = hdr_b.has_blob_gas;
    rx_hdr.blob_gas_used = hdr_b.blob_gas_used;
    rx_hdr.excess_blob_gas = hdr_b.excess_blob_gas;
    rx_hdr.has_parent_beacon_root = hdr_b.has_parent_beacon_root;
    if (hdr_b.has_parent_beacon_root)
        memcpy(rx_hdr.parent_beacon_root.bytes, hdr_b.parent_beacon_root.bytes, 32);

    /* Build rx_block_body_t from C body */
    rx_block_body_t rx_body;
    memset(&rx_body, 0, sizeof(rx_body));
    rx_body.tx_count = body_b.tx_count;
    const uint8_t **tx_ptrs = calloc(body_b.tx_count, sizeof(uint8_t *));
    size_t *tx_lens = calloc(body_b.tx_count, sizeof(size_t));
    for (size_t i = 0; i < body_b.tx_count; i++) {
        const rlp_item_t *tx = block_body_tx(&body_b, i);
        if (tx && rlp_get_type(tx) == RLP_TYPE_STRING) {
            const bytes_t *b = rlp_get_string(tx);
            tx_ptrs[i] = b->data;
            tx_lens[i] = b->len;
        }
    }
    rx_body.transactions = tx_ptrs;
    rx_body.tx_lengths = tx_lens;
    if (body_b.withdrawal_count > 0) {
        rx_withdrawal_t *wds = calloc(body_b.withdrawal_count, sizeof(rx_withdrawal_t));
        for (size_t i = 0; i < body_b.withdrawal_count; i++) {
            wds[i].index = body_b.withdrawals[i].index;
            wds[i].validator_index = body_b.withdrawals[i].validator_index;
            memcpy(wds[i].address.bytes, body_b.withdrawals[i].address.bytes, 20);
            wds[i].amount_gwei = body_b.withdrawals[i].amount_gwei;
        }
        rx_body.withdrawals = wds;
        rx_body.withdrawal_count = body_b.withdrawal_count;
    }

    rx_hash_t bh;
    memcpy(bh.bytes, hash_b, 32);

    rx_block_result_t result;
    rx_execute_block(engine, &rx_hdr, &rx_body, &bh, &result);

    printf("Path B: gas=%lu, state_root=", result.gas_used);
    for (int i = 0; i < 32; i++) printf("%02x", result.state_root.bytes[i]);
    printf("\n");

    /* Compare */
    printf("\n=== Comparison ===\n");
    printf("Gas match: %s\n", br_a.gas_used == result.gas_used ? "YES" : "NO");
    printf("  A: %lu  B: %lu\n", br_a.gas_used, result.gas_used);

    rx_block_result_free(&result);
    block_body_free(&body_b);
    free(tx_ptrs);
    free(tx_lens);
    rx_engine_destroy(engine);

    return 0;
}
