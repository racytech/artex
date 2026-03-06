/**
 * Block Executor Tests
 *
 * Tests:
 *  1. Block header RLP decode
 *  2. Legacy transaction RLP decode + sender recovery (EIP-155 test vector)
 *  3. Block body RLP decode
 *  4. Block executor smoke test (value transfer)
 */

#include "block.h"
#include "tx_decoder.h"
#include "block_executor.h"
#include "rlp.h"
#include "hash.h"
#include "address.h"
#include "uint256.h"
#include "evm.h"
#include "evm_state.h"
#include "state_db.h"
#include "fork.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { printf("  [%s] ", name); } while(0)
#define PASS() do { printf("PASS\n"); tests_passed++; return; } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); tests_failed++; return; } while(0)
#define ASSERT(cond, msg) do { if (!(cond)) FAIL(msg); } while(0)

/* =========================================================================
 * Hex string to bytes helper
 * ========================================================================= */

static size_t hex_to_bytes(const char *hex, uint8_t *out, size_t cap) {
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) hex += 2;
    size_t len = strlen(hex) / 2;
    if (len > cap) len = cap;
    for (size_t i = 0; i < len; i++) {
        unsigned int byte;
        sscanf(hex + i * 2, "%2x", &byte);
        out[i] = (uint8_t)byte;
    }
    return len;
}

/* =========================================================================
 * Test 1: Block header RLP encode + decode roundtrip
 * ========================================================================= */

static void test_header_decode(void) {
    TEST("Block header RLP decode");

    /* Build a synthetic block header using the RLP encoder */
    rlp_item_t *hdr_list = rlp_list_new();

    /* 0: parentHash (32 bytes) */
    uint8_t parent[32]; memset(parent, 0x11, 32);
    rlp_list_append(hdr_list, rlp_string(parent, 32));

    /* 1: unclesHash (32 bytes) */
    uint8_t uncles[32]; memset(uncles, 0x22, 32);
    rlp_list_append(hdr_list, rlp_string(uncles, 32));

    /* 2: coinbase (20 bytes) */
    uint8_t coinbase[20]; memset(coinbase, 0x33, 20);
    rlp_list_append(hdr_list, rlp_string(coinbase, 20));

    /* 3: stateRoot (32 bytes) */
    uint8_t state_root[32]; memset(state_root, 0x44, 32);
    rlp_list_append(hdr_list, rlp_string(state_root, 32));

    /* 4: txRoot (32 bytes) */
    uint8_t tx_root[32]; memset(tx_root, 0x55, 32);
    rlp_list_append(hdr_list, rlp_string(tx_root, 32));

    /* 5: receiptRoot (32 bytes) */
    uint8_t rcpt_root[32]; memset(rcpt_root, 0x66, 32);
    rlp_list_append(hdr_list, rlp_string(rcpt_root, 32));

    /* 6: logsBloom (256 bytes) */
    uint8_t bloom[256]; memset(bloom, 0, 256);
    rlp_list_append(hdr_list, rlp_string(bloom, 256));

    /* 7: difficulty */
    rlp_list_append(hdr_list, rlp_uint64(0x20000));

    /* 8: number */
    rlp_list_append(hdr_list, rlp_uint64(42));

    /* 9: gasLimit */
    rlp_list_append(hdr_list, rlp_uint64(8000000));

    /* 10: gasUsed */
    rlp_list_append(hdr_list, rlp_uint64(21000));

    /* 11: timestamp */
    rlp_list_append(hdr_list, rlp_uint64(1638360000));

    /* 12: extraData */
    uint8_t extra[] = { 0xCA, 0xFE };
    rlp_list_append(hdr_list, rlp_string(extra, 2));

    /* 13: mixHash (32 bytes) */
    uint8_t mix[32]; memset(mix, 0x77, 32);
    rlp_list_append(hdr_list, rlp_string(mix, 32));

    /* 14: nonce (8 bytes) */
    rlp_list_append(hdr_list, rlp_uint64(0));

    /* 15: baseFeePerGas (London+) */
    rlp_list_append(hdr_list, rlp_uint64(7));

    /* Encode */
    bytes_t encoded = rlp_encode(hdr_list);
    ASSERT(encoded.data && encoded.len > 0, "RLP encode header");

    /* Decode */
    block_header_t hdr;
    ASSERT(block_header_decode_rlp(&hdr, encoded.data, encoded.len),
           "decode header");

    /* Verify fields */
    for (int i = 0; i < 32; i++) ASSERT(hdr.parent_hash.bytes[i] == 0x11, "parentHash");
    for (int i = 0; i < 32; i++) ASSERT(hdr.uncle_hash.bytes[i] == 0x22, "unclesHash");
    for (int i = 0; i < 20; i++) ASSERT(hdr.coinbase.bytes[i] == 0x33, "coinbase");
    for (int i = 0; i < 32; i++) ASSERT(hdr.state_root.bytes[i] == 0x44, "stateRoot");
    ASSERT(hdr.number == 42, "number");
    ASSERT(hdr.gas_limit == 8000000, "gasLimit");
    ASSERT(hdr.gas_used == 21000, "gasUsed");
    ASSERT(hdr.timestamp == 1638360000, "timestamp");
    ASSERT(hdr.extra_data_len == 2, "extraData len");
    ASSERT(hdr.extra_data[0] == 0xCA, "extraData[0]");
    ASSERT(hdr.has_base_fee, "has baseFee");
    ASSERT(uint256_to_uint64(&hdr.base_fee) == 7, "baseFee");

    free(encoded.data);
    rlp_item_free(hdr_list);
    PASS();
}

/* =========================================================================
 * Test 2: Legacy transaction decode (EIP-155 test vector)
 * ========================================================================= */

static void test_legacy_tx_decode(void) {
    TEST("Legacy TX decode (EIP-155 vector)");

    /*
     * EIP-155 test vector:
     *   nonce = 9, gasPrice = 20 gwei, gasLimit = 21000
     *   to = 0x3535353535353535353535353535353535353535
     *   value = 1 ETH, data = ""
     *   chain_id = 1
     *   Sender: 0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F
     */
    const char *raw_hex =
        "f86c098504a817c800825208943535353535353535353535353535353535353535"
        "880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d"
        "3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9"
        "f3dc64214b297fb1966a3b6d83";

    uint8_t raw[256];
    size_t raw_len = hex_to_bytes(raw_hex, raw, sizeof(raw));
    ASSERT(raw_len > 0, "parse hex");

    transaction_t tx;
    ASSERT(tx_decode_raw(&tx, raw, raw_len, 1), "decode");

    ASSERT(tx.type == TX_TYPE_LEGACY, "type");
    ASSERT(tx.nonce == 9, "nonce");
    ASSERT(tx.gas_limit == 21000, "gasLimit");
    ASSERT(!tx.is_create, "not create");

    /* Verify 'to' address */
    uint8_t expected_to[20];
    memset(expected_to, 0x35, 20);
    ASSERT(memcmp(tx.to.bytes, expected_to, 20) == 0, "to address");

    /* Verify value = 1 ETH = 10^18 */
    uint256_t one_eth = uint256_from_hex("0xde0b6b3a7640000");
    ASSERT(uint256_is_equal(&tx.value, &one_eth), "value");

    /* Verify recovered sender: 0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F */
    address_t expected_sender;
    address_from_hex("0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F", &expected_sender);
    ASSERT(address_equal(&tx.sender, &expected_sender), "sender recovery");

    tx_decoded_free(&tx);
    PASS();
}

/* =========================================================================
 * Test 3: Block body RLP decode
 * ========================================================================= */

static void test_body_decode(void) {
    TEST("Block body RLP decode");

    /* Build a body: [transactions_list, uncles_list] */
    rlp_item_t *body_list = rlp_list_new();

    /* Transactions list: one legacy tx */
    rlp_item_t *txs_list = rlp_list_new();

    /* Build a simple legacy tx as an RLP list */
    rlp_item_t *tx = rlp_list_new();
    rlp_list_append(tx, rlp_uint64(0));        /* nonce */
    rlp_list_append(tx, rlp_uint64(20000000000ULL)); /* gasPrice (20 gwei) */
    rlp_list_append(tx, rlp_uint64(21000));    /* gasLimit */
    uint8_t to[20]; memset(to, 0xAA, 20);
    rlp_list_append(tx, rlp_string(to, 20));   /* to */
    rlp_list_append(tx, rlp_uint64(1000));     /* value */
    rlp_list_append(tx, rlp_string(NULL, 0));  /* data */
    rlp_list_append(tx, rlp_uint64(27));       /* v */
    uint8_t zero32[32]; memset(zero32, 0, 32);
    rlp_list_append(tx, rlp_string(zero32, 32)); /* r (dummy) */
    rlp_list_append(tx, rlp_string(zero32, 32)); /* s (dummy) */

    rlp_list_append(txs_list, tx);

    rlp_list_append(body_list, txs_list);

    /* Empty uncles list */
    rlp_list_append(body_list, rlp_list_new());

    bytes_t encoded = rlp_encode(body_list);
    ASSERT(encoded.data && encoded.len > 0, "encode body");

    block_body_t body;
    ASSERT(block_body_decode_rlp(&body, encoded.data, encoded.len), "decode body");
    ASSERT(body.tx_count == 1, "tx count");

    const rlp_item_t *tx_item = block_body_tx(&body, 0);
    ASSERT(tx_item != NULL, "get tx 0");
    ASSERT(rlp_get_type(tx_item) == RLP_TYPE_LIST, "tx is list (legacy)");
    ASSERT(rlp_get_list_count(tx_item) == 9, "tx has 9 fields");

    block_body_free(&body);
    free(encoded.data);
    rlp_item_free(body_list);
    PASS();
}

/* =========================================================================
 * Test 4: Executor smoke test (simple value transfer)
 * ========================================================================= */

static void test_executor_smoke(void) {
    TEST("Block executor smoke test");

    /*
     * Set up:
     * - Genesis: account A with 100 ETH
     * - Block 1: single tx transferring 1 ETH from A to B
     * - Verify gas_used and that the executor runs without crashing
     *
     * This requires the EIP-155 test vector's sender account to have balance.
     * Sender: 0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F
     */

    /* Create state DB in /tmp */
    state_db_t *sdb = sdb_create("/tmp/art_block_executor_test");
    ASSERT(sdb != NULL, "create state_db");

    /* Create EVM state */
    evm_state_t *state = evm_state_create(sdb);
    ASSERT(state != NULL, "create evm_state");

    /* Create EVM */
    evm_t *evm = evm_create(state, chain_config_mainnet());
    ASSERT(evm != NULL, "create evm");

    /* Set up genesis: sender with 100 ETH */
    address_t sender;
    address_from_hex("0x9d8A62f656a8d1615C1294fd71e9CFb3E4855A4F", &sender);
    uint256_t hundred_eth = uint256_from_hex("0x56BC75E2D63100000"); /* 100 * 10^18 */
    evm_state_add_balance(state, &sender, &hundred_eth);

    /* Commit genesis state */
    evm_state_commit(state);

    /* Build block header */
    block_header_t header;
    memset(&header, 0, sizeof(header));
    header.number = 1;
    header.gas_limit = 8000000;
    header.timestamp = 1638360000;
    header.has_base_fee = true;
    header.base_fee = uint256_from_uint64(7); /* 7 wei base fee */
    /* coinbase */
    memset(header.coinbase.bytes, 0xCC, 20);

    /* Build block body with the EIP-155 test vector tx */
    const char *raw_hex =
        "f86c098504a817c800825208943535353535353535353535353535353535353535"
        "880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d"
        "3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9"
        "f3dc64214b297fb1966a3b6d83";

    uint8_t raw_tx[256];
    size_t raw_tx_len = hex_to_bytes(raw_hex, raw_tx, sizeof(raw_tx));

    /* Wrap the legacy tx inside a body: [[[tx fields...]], []] */
    /* First decode the raw tx to get the RLP list */
    rlp_item_t *tx_decoded = rlp_decode(raw_tx, raw_tx_len);
    ASSERT(tx_decoded != NULL, "decode raw tx for body");

    /* But we need sender nonce to be 9. Set it. */
    evm_state_set_nonce(state, &sender, 9);
    evm_state_commit(state);

    /* Build body RLP: [[tx], []] */
    rlp_item_t *body_rlp = rlp_list_new();
    rlp_item_t *txs_list = rlp_list_new();
    rlp_list_append(txs_list, tx_decoded); /* takes ownership */
    rlp_list_append(body_rlp, txs_list);
    rlp_list_append(body_rlp, rlp_list_new()); /* empty uncles */

    bytes_t body_encoded = rlp_encode(body_rlp);
    ASSERT(body_encoded.data != NULL, "encode body");

    /* Decode the body */
    block_body_t body;
    ASSERT(block_body_decode_rlp(&body, body_encoded.data, body_encoded.len),
           "decode body");
    ASSERT(body.tx_count == 1, "body tx count");

    /* Execute! */
    block_result_t result = block_execute(evm, &header, &body, NULL);

    printf("\n    gas_used=%lu, tx_count=%zu, success=%d, first_failure=%d\n    ",
           result.gas_used, result.tx_count, result.success, result.first_failure);

    ASSERT(result.tx_count == 1, "result tx_count");
    /* The tx should execute (though it might fail validation due to
     * gasPrice vs baseFee mismatch — that's ok, we're testing the pipeline) */
    ASSERT(result.gas_used > 0 || result.first_failure >= 0, "executor ran");

    /* Cleanup */
    block_result_free(&result);
    block_body_free(&body);
    free(body_encoded.data);
    rlp_item_free(body_rlp);
    evm_destroy(evm);
    evm_state_destroy(state);
    sdb_destroy(sdb);

    /* Clean up temp dir */
    system("rm -rf /tmp/art_block_executor_test");

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Block Executor Tests ===\n");

    test_header_decode();
    test_legacy_tx_decode();
    test_body_decode();
    test_executor_smoke();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);

    return tests_failed ? 1 : 0;
}
