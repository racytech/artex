/**
 * Reproduce the DUP1 London state test end-to-end.
 * Setup prestate, execute tx, dump post-state, compare root.
 */
#include "state.h"
#include "evm_state.h"
#include "evm.h"
#include "transaction.h"
#include "fork.h"
#include "keccak256.h"
#include "uint256.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static void hex_to_bytes(const char *hex, uint8_t *out, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned int b; sscanf(hex + i*2, "%02x", &b); out[i] = (uint8_t)b;
    }
}

static void print_hash(const char *label, const uint8_t *h) {
    printf("%s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h[i]);
    printf("\n");
}

int main(void) {
    evm_state_t *es = evm_state_create(NULL);

    /* Pre-state addresses */
    address_t sender, contract, coinbase;
    hex_to_bytes("965fbaafe14938f0d39b584bb1c4a4010b1f0ed9", sender.bytes, 20);
    hex_to_bytes("01f523d3286fb17dd7ac27e08a26487eaf8820c6", contract.bytes, 20);
    hex_to_bytes("2adc25665018aa1fe0e6bc666dac8fc2697ff9ba", coinbase.bytes, 20);

    /* Setup sender: nonce=0, balance=0x3635c9adc5dea00000 (9 bytes) */
    evm_state_set_nonce(es, &sender, 0);
    uint256_t sender_bal;
    {
        uint8_t b[9];
        hex_to_bytes("3635c9adc5dea00000", b, 9);
        sender_bal = uint256_from_bytes(b, 9);
    }
    evm_state_set_balance(es, &sender, &sender_bal);
    evm_state_mark_existed(es, &sender);

    /* Setup contract: nonce=1, balance=0, code */
    evm_state_set_nonce(es, &contract, 1);
    /* Code: push 0..16, DUP1, then SSTORE 17 times */
    const char *code_hex = "6000600160026003600460056006600760086009600a600b600c600d600e600f601080600055600155600255600355600455600555600655600755600855600955600a55600b55600c55600d55600e55600f55601055";
    size_t code_len = strlen(code_hex) / 2;
    uint8_t *code = malloc(code_len);
    hex_to_bytes(code_hex, code, code_len);
    evm_state_set_code(es, &contract, code, (uint32_t)code_len);
    evm_state_mark_existed(es, &contract);

    /* Commit prestate + compute genesis root + clear dirty */
    evm_state_commit(es);
    hash_t genesis = evm_state_compute_mpt_root(es, false);
    print_hash("Genesis root", genesis.bytes);
    evm_state_clear_prestate_dirty(es);

    /* Setup EVM with London fork (all forks active at block 0) */
    chain_config_t london_cfg = {
        .chain_id = 1,
        .fork_blocks = {0}, /* all zeros = all forks active at genesis */
    };
    evm_t *evm = evm_create(es, &london_cfg);

    /* Set block env */
    evm_block_env_t block = {
        .number = 1,
        .timestamp = 0x3e8,
        .gas_limit = 0x07270e00,
        .coinbase = coinbase,
        .base_fee = uint256_from_uint64(7),
        .difficulty = uint256_from_uint64(0x020000),
    };
    evm_set_block_env(evm, &block);

    /* Build and execute tx */
    transaction_t tx = {
        .type = TX_TYPE_LEGACY,
        .nonce = 0,
        .sender = sender,
        .to = contract,
        .value = UINT256_ZERO,
        .gas_limit = 0x07a120,
        .gas_price = uint256_from_uint64(10),
        .data = NULL,
        .data_size = 0,
        .is_create = false,
    };

    block_env_t benv = {
        .coinbase = coinbase,
        .block_number = 1,
        .timestamp = 0x3e8,
        .gas_limit = 0x07270e00,
        .base_fee = uint256_from_uint64(7),
        .difficulty = uint256_from_uint64(0x020000),
    };

    transaction_result_t tx_result;
    bool ok = transaction_execute(evm, &tx, &benv, &tx_result);
    printf("tx_executed=%d status=%d gas_used=%lu\n", ok, ok ? tx_result.status : -1, ok ? tx_result.gas_used : 0UL);

    /* Commit tx + compute root */
    evm_state_set_prune_empty(es, true); /* London = post-Spurious Dragon */
    evm_state_commit_tx(es);
    hash_t post_root = evm_state_compute_mpt_root(es, true);

    /* Dump post-state */
    printf("\nPost-state:\n");
    printf("  sender:   nonce=%lu bal=", evm_state_get_nonce(es, &sender));
    { uint256_t b = evm_state_get_balance(es, &sender); uint8_t be[32]; uint256_to_bytes(&b, be);
      for(int i=0;i<32;i++) printf("%02x",be[i]);
      printf("\n"); }

    printf("  contract: nonce=%lu bal=", evm_state_get_nonce(es, &contract));
    { uint256_t b = evm_state_get_balance(es, &contract); uint8_t be[32]; uint256_to_bytes(&b, be);
      for(int i=0;i<32;i++) printf("%02x",be[i]); printf("\n"); }

    printf("  coinbase: nonce=%lu bal=", evm_state_get_nonce(es, &coinbase));
    { uint256_t b = evm_state_get_balance(es, &coinbase); uint8_t be[32]; uint256_to_bytes(&b, be);
      for(int i=0;i<32;i++) printf("%02x",be[i]); printf("\n"); }

    /* Check storage slots 0x00-0x10 */
    printf("  contract storage:\n");
    for (int slot = 0; slot <= 0x10; slot++) {
        uint256_t key = uint256_from_uint64(slot);
        uint256_t val = evm_state_get_storage(es, &contract, &key);
        if (!uint256_is_zero(&val)) {
            printf("    slot 0x%02x = 0x%lx\n", slot, uint256_to_uint64(&val));
        }
    }

    print_hash("\nPost root", post_root.bytes);
    printf("Expected:  ae6f46106d4fc638970f7a4d680bed7f768469e3752dc3e460841a710cc6aff3\n");

    /* Expected balances */
    printf("\nExpected sender bal:   0x3635c9adc5de6373ce\n");
    printf("Expected coinbase bal: 0x122a0f\n");

    if (ok) transaction_result_free(&tx_result);
    free(code);
    evm_destroy(evm);
    evm_state_destroy(es);
    return 0;
}
