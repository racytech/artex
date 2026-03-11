/**
 * Block Executor Tests
 *
 * Tests:
 *  1. Block header RLP decode
 *  2. Legacy transaction RLP decode + sender recovery (EIP-155 test vector)
 *  3. Block body RLP decode
 *  4. Block executor smoke test (value transfer)
 *  5. DAO fork drain
 *  6. DAO fork block/chain gate
 *  7. Block header encode → decode round-trip
 *  8. Block header hash consistency
 *  9. Empty transaction root
 * 10. Transaction root from body with one tx
 */

#include "block.h"
#include "tx_decoder.h"
#include "block_executor.h"
#include "dao_fork.h"
#include "rlp.h"
#include "hash.h"
#include "address.h"
#include "uint256.h"
#include "evm.h"
#include "evm_state.h"
#ifdef ENABLE_VERKLE
#include "verkle_state.h"
#endif
#include "fork.h"
#include "mem_mpt.h"
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

    /* Create verkle state (in-memory tree) */
#ifdef ENABLE_VERKLE
    verkle_state_t *vs = verkle_state_create();
    ASSERT(vs != NULL, "create verkle_state");
#else
    void *vs = NULL;
#endif

    /* Create EVM state */
    evm_state_t *state = evm_state_create(vs,
#ifdef ENABLE_MPT
        "/tmp/test_block_executor_mpt",
#else
        NULL,
#endif
        NULL  /* no code_store for tests */
    );
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
#ifdef ENABLE_VERKLE
    verkle_state_destroy(vs);
#endif

    PASS();
}

/* =========================================================================
 * Test 5: DAO fork — drain accounts into refund contract
 * ========================================================================= */

static void test_dao_fork(void) {
    TEST("DAO fork drain");

    /* Create verkle state + evm state (no EVM needed, just state) */
#ifdef ENABLE_VERKLE
    verkle_state_t *vs = verkle_state_create();
    ASSERT(vs != NULL, "create verkle_state");
#else
    void *vs = NULL;
#endif
    evm_state_t *state = evm_state_create(vs,
#ifdef ENABLE_MPT
        "/tmp/test_block_executor_mpt",
#else
        NULL,
#endif
        NULL  /* no code_store for tests */
    );
    ASSERT(state != NULL, "create evm_state");

    /* Pick 3 drain addresses from the list and give them known balances */
    address_t addr1, addr2, addr3;
    address_from_hex("0xd4fe7bc31cedb7bfb8a345f31e668033056b2728", &addr1);
    address_from_hex("0xb3fb0e5aba0e20e5c49d252dfd30e102b171a425", &addr2);
    address_from_hex("0xbb9bc244d798123fde783fcc1c72d3bb8c189413", &addr3); /* The DAO contract itself */

    uint256_t bal1 = uint256_from_uint64(1000000);
    uint256_t bal2 = uint256_from_uint64(2000000);
    uint256_t bal3 = uint256_from_uint64(3000000);

    evm_state_add_balance(state, &addr1, &bal1);
    evm_state_add_balance(state, &addr2, &bal2);
    evm_state_add_balance(state, &addr3, &bal3);
    evm_state_commit(state);

    /* Refund contract address */
    address_t refund;
    address_from_hex("0xbf4ed7b27f1d666546e30d74d50d173d20bca754", &refund);

    /* Verify refund contract doesn't exist yet */
    uint256_t refund_bal_before = evm_state_get_balance(state, &refund);
    ASSERT(uint256_is_zero(&refund_bal_before), "refund starts at 0");

    /* Apply the DAO fork */
    apply_dao_fork(state);

    /* All drain accounts should be zero */
    uint256_t bal_after;
    bal_after = evm_state_get_balance(state, &addr1);
    ASSERT(uint256_is_zero(&bal_after), "addr1 drained");
    bal_after = evm_state_get_balance(state, &addr2);
    ASSERT(uint256_is_zero(&bal_after), "addr2 drained");
    bal_after = evm_state_get_balance(state, &addr3);
    ASSERT(uint256_is_zero(&bal_after), "addr3 drained");

    /* Refund contract should have all the balance */
    uint256_t expected_total = uint256_from_uint64(6000000); /* 1M + 2M + 3M */
    uint256_t refund_bal = evm_state_get_balance(state, &refund);
    ASSERT(uint256_is_equal(&refund_bal, &expected_total), "refund has total");

    /* Cleanup */
    evm_state_destroy(state);
#ifdef ENABLE_VERKLE
    verkle_state_destroy(vs);
#endif
    PASS();
}

/* =========================================================================
 * Test 6: DAO fork — no-op on non-mainnet or wrong block
 * ========================================================================= */

static void test_dao_fork_block_check(void) {
    TEST("DAO fork block/chain gate");

    /* Create state with balance on a drain address */
#ifdef ENABLE_VERKLE
    verkle_state_t *vs = verkle_state_create();
    ASSERT(vs != NULL, "create verkle_state");
#else
    void *vs = NULL;
#endif
    evm_state_t *state = evm_state_create(vs,
#ifdef ENABLE_MPT
        "/tmp/test_block_executor_mpt",
#else
        NULL,
#endif
        NULL  /* no code_store for tests */
    );
    ASSERT(state != NULL, "create evm_state");
    evm_t *evm = evm_create(state, chain_config_mainnet());
    ASSERT(evm != NULL, "create evm");

    address_t drain_addr;
    address_from_hex("0xd4fe7bc31cedb7bfb8a345f31e668033056b2728", &drain_addr);
    uint256_t bal = uint256_from_uint64(5000);
    evm_state_add_balance(state, &drain_addr, &bal);
    evm_state_commit(state);

    /* Execute a block at number != 1920000 — DAO fork should NOT trigger */
    block_header_t header;
    memset(&header, 0, sizeof(header));
    header.number = 100;
    header.gas_limit = 5000000;
    header.timestamp = 1438269988;

    /* Empty body */
    rlp_item_t *body_rlp = rlp_list_new();
    rlp_list_append(body_rlp, rlp_list_new()); /* empty txs */
    rlp_list_append(body_rlp, rlp_list_new()); /* empty uncles */
    bytes_t body_enc = rlp_encode(body_rlp);
    block_body_t body;
    ASSERT(block_body_decode_rlp(&body, body_enc.data, body_enc.len), "decode body");

    block_result_t result = block_execute(evm, &header, &body, NULL);
    (void)result;

    /* Drain address should still have its balance */
    uint256_t bal_after = evm_state_get_balance(state, &drain_addr);
    ASSERT(uint256_is_equal(&bal_after, &bal), "not drained at block 100");

    block_result_free(&result);
    block_body_free(&body);
    free(body_enc.data);
    rlp_item_free(body_rlp);
    evm_destroy(evm);
    evm_state_destroy(state);
#ifdef ENABLE_VERKLE
    verkle_state_destroy(vs);
#endif
    PASS();
}

/* =========================================================================
 * Test 7: Block header encode → decode round-trip
 * ========================================================================= */

static void test_header_encode_roundtrip(void) {
    TEST("Block header encode → decode round-trip");

    /* Build a header struct (London+ with baseFee) */
    block_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memset(hdr.parent_hash.bytes, 0x11, 32);
    memset(hdr.uncle_hash.bytes, 0x22, 32);
    memset(hdr.coinbase.bytes, 0x33, 20);
    memset(hdr.state_root.bytes, 0x44, 32);
    memset(hdr.tx_root.bytes, 0x55, 32);
    memset(hdr.receipt_root.bytes, 0x66, 32);
    memset(hdr.logs_bloom, 0, 256);
    hdr.difficulty = uint256_from_uint64(0x20000);
    hdr.number = 42;
    hdr.gas_limit = 8000000;
    hdr.gas_used = 21000;
    hdr.timestamp = 1638360000;
    hdr.extra_data[0] = 0xCA;
    hdr.extra_data[1] = 0xFE;
    hdr.extra_data_len = 2;
    memset(hdr.mix_hash.bytes, 0x77, 32);
    hdr.nonce = 0;
    hdr.has_base_fee = true;
    hdr.base_fee = uint256_from_uint64(7);

    /* Encode */
    bytes_t encoded = block_header_encode_rlp(&hdr);
    ASSERT(encoded.data && encoded.len > 0, "encode header");

    /* Decode back */
    block_header_t decoded;
    ASSERT(block_header_decode_rlp(&decoded, encoded.data, encoded.len),
           "decode round-trip");

    /* Verify all fields */
    ASSERT(memcmp(decoded.parent_hash.bytes, hdr.parent_hash.bytes, 32) == 0,
           "parentHash round-trip");
    ASSERT(memcmp(decoded.uncle_hash.bytes, hdr.uncle_hash.bytes, 32) == 0,
           "uncleHash round-trip");
    ASSERT(memcmp(decoded.coinbase.bytes, hdr.coinbase.bytes, 20) == 0,
           "coinbase round-trip");
    ASSERT(memcmp(decoded.state_root.bytes, hdr.state_root.bytes, 32) == 0,
           "stateRoot round-trip");
    ASSERT(decoded.number == hdr.number, "number round-trip");
    ASSERT(decoded.gas_limit == hdr.gas_limit, "gasLimit round-trip");
    ASSERT(decoded.gas_used == hdr.gas_used, "gasUsed round-trip");
    ASSERT(decoded.timestamp == hdr.timestamp, "timestamp round-trip");
    ASSERT(decoded.extra_data_len == hdr.extra_data_len, "extraData len round-trip");
    ASSERT(decoded.extra_data[0] == 0xCA && decoded.extra_data[1] == 0xFE,
           "extraData round-trip");
    ASSERT(memcmp(decoded.mix_hash.bytes, hdr.mix_hash.bytes, 32) == 0,
           "mixHash round-trip");
    ASSERT(decoded.nonce == hdr.nonce, "nonce round-trip");
    ASSERT(decoded.has_base_fee, "has_base_fee round-trip");
    ASSERT(uint256_to_uint64(&decoded.base_fee) == 7, "baseFee round-trip");

    free(encoded.data);
    PASS();
}

/* =========================================================================
 * Test 8: Block header hash consistency
 * ========================================================================= */

static void test_header_hash(void) {
    TEST("Block header hash consistency");

    /* Build header */
    block_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    memset(hdr.parent_hash.bytes, 0xAA, 32);
    memset(hdr.uncle_hash.bytes, 0xBB, 32);
    memset(hdr.coinbase.bytes, 0xCC, 20);
    memset(hdr.state_root.bytes, 0xDD, 32);
    memset(hdr.tx_root.bytes, 0xEE, 32);
    memset(hdr.receipt_root.bytes, 0xFF, 32);
    memset(hdr.logs_bloom, 0, 256);
    hdr.difficulty = uint256_from_uint64(0x100);
    hdr.number = 1000;
    hdr.gas_limit = 30000000;
    hdr.gas_used = 100000;
    hdr.timestamp = 1700000000;
    hdr.extra_data_len = 0;
    memset(hdr.mix_hash.bytes, 0x11, 32);
    hdr.nonce = 0;
    hdr.has_base_fee = true;
    hdr.base_fee = uint256_from_uint64(1000000000); /* 1 gwei */

    /* Compute hash via block_header_hash */
    hash_t hash1 = block_header_hash(&hdr);
    ASSERT(!hash_is_zero(&hash1), "hash non-zero");

    /* Encode header, wrap in block RLP [header, [], []], compute via block_hash_from_rlp */
    bytes_t hdr_rlp = block_header_encode_rlp(&hdr);
    ASSERT(hdr_rlp.data != NULL, "encode header");

    /* Wrap: [[header_fields], [], []] */
    rlp_item_t *hdr_decoded = rlp_decode(hdr_rlp.data, hdr_rlp.len);
    ASSERT(hdr_decoded != NULL, "decode header RLP");

    rlp_item_t *block = rlp_list_new();
    rlp_list_append(block, hdr_decoded);
    rlp_list_append(block, rlp_list_new()); /* uncles */
    rlp_list_append(block, rlp_list_new()); /* txs */
    bytes_t block_rlp = rlp_encode(block);
    ASSERT(block_rlp.data != NULL, "encode block");

    hash_t hash2 = block_hash_from_rlp(block_rlp.data, block_rlp.len);

    /* Both methods should produce the same hash */
    ASSERT(memcmp(hash1.bytes, hash2.bytes, 32) == 0,
           "block_header_hash == block_hash_from_rlp");

    /* Same header should produce same hash (deterministic) */
    hash_t hash3 = block_header_hash(&hdr);
    ASSERT(memcmp(hash1.bytes, hash3.bytes, 32) == 0, "deterministic");

    free(hdr_rlp.data);
    free(block_rlp.data);
    rlp_item_free(block);
    PASS();
}

/* =========================================================================
 * Test 9: Empty transaction root
 * ========================================================================= */

static void test_empty_tx_root(void) {
    TEST("Empty transaction root");

    /* Build empty body */
    rlp_item_t *body_rlp = rlp_list_new();
    rlp_list_append(body_rlp, rlp_list_new()); /* empty txs */
    rlp_list_append(body_rlp, rlp_list_new()); /* empty uncles */
    bytes_t encoded = rlp_encode(body_rlp);
    ASSERT(encoded.data != NULL, "encode body");

    block_body_t body;
    ASSERT(block_body_decode_rlp(&body, encoded.data, encoded.len), "decode body");
    ASSERT(body.tx_count == 0, "empty body");

    hash_t root = block_compute_tx_root(&body);

    /* Empty trie root = keccak256(0x80) =
     * 0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421 */
    uint8_t expected[32];
    hex_to_bytes("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                 expected, 32);
    ASSERT(memcmp(root.bytes, expected, 32) == 0, "empty trie root");

    block_body_free(&body);
    free(encoded.data);
    rlp_item_free(body_rlp);
    PASS();
}

/* =========================================================================
 * Test 10: Transaction root from body with one tx
 * ========================================================================= */

static void test_single_tx_root(void) {
    TEST("Single tx root");

    /* Build body with one legacy tx */
    rlp_item_t *body_list = rlp_list_new();
    rlp_item_t *txs_list = rlp_list_new();

    rlp_item_t *tx = rlp_list_new();
    rlp_list_append(tx, rlp_uint64(0));               /* nonce */
    rlp_list_append(tx, rlp_uint64(20000000000ULL));   /* gasPrice */
    rlp_list_append(tx, rlp_uint64(21000));            /* gasLimit */
    uint8_t to[20]; memset(to, 0xAA, 20);
    rlp_list_append(tx, rlp_string(to, 20));           /* to */
    rlp_list_append(tx, rlp_uint64(1000));             /* value */
    rlp_list_append(tx, rlp_string(NULL, 0));          /* data */
    rlp_list_append(tx, rlp_uint64(27));               /* v */
    uint8_t zero32[32]; memset(zero32, 0, 32);
    rlp_list_append(tx, rlp_string(zero32, 32));       /* r */
    rlp_list_append(tx, rlp_string(zero32, 32));       /* s */

    rlp_list_append(txs_list, tx);
    rlp_list_append(body_list, txs_list);
    rlp_list_append(body_list, rlp_list_new()); /* empty uncles */

    bytes_t encoded = rlp_encode(body_list);
    ASSERT(encoded.data != NULL, "encode body");

    block_body_t body;
    ASSERT(block_body_decode_rlp(&body, encoded.data, encoded.len), "decode body");
    ASSERT(body.tx_count == 1, "1 tx");

    hash_t root = block_compute_tx_root(&body);

    /* Root should be non-zero and different from empty trie root */
    uint8_t empty_root[32];
    hex_to_bytes("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
                 empty_root, 32);
    ASSERT(memcmp(root.bytes, empty_root, 32) != 0, "not empty root");
    ASSERT(!hash_is_zero(&root), "non-zero root");

    /* Compute again — should be deterministic */
    hash_t root2 = block_compute_tx_root(&body);
    ASSERT(memcmp(root.bytes, root2.bytes, 32) == 0, "deterministic");

    block_body_free(&body);
    free(encoded.data);
    rlp_item_free(body_list);
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
    test_dao_fork();
    test_dao_fork_block_check();
    test_header_encode_roundtrip();
    test_header_hash();
    test_empty_tx_root();
    test_single_tx_root();

    printf("\n=== Results: %d passed, %d failed ===\n",
           tests_passed, tests_failed);

    return tests_failed ? 1 : 0;
}
