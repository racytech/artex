/**
 * Test the public rx_ API end-to-end.
 *
 * Tests:
 *   1. Engine create/destroy
 *   2. Genesis loading + state queries
 *   3. State save/load round-trip
 *   4. Compute state root
 *   5. Logger callback
 */
#include "artex.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define TEST_GENESIS "/tmp/test_artex_genesis.json"
#define TEST_STATE   "/tmp/test_artex_state.bin"

static int g_log_count = 0;
static void test_logger(rx_log_level_t level, const char *msg, void *ud) {
    (void)ud;
    const char *names[] = {"ERROR", "WARN", "INFO", "DEBUG"};
    printf("  [%s] %s\n", names[level], msg);
    g_log_count++;
}

static void print_hash(const char *label, const rx_hash_t *h) {
    printf("  %s: ", label);
    for (int i = 0; i < 32; i++) printf("%02x", h->bytes[i]);
    printf("\n");
}

static int test_create_destroy(void) {
    printf("test_create_destroy:\n");

    /* NULL config fails */
    if (rx_engine_create(NULL) != NULL) {
        printf("  FAIL: NULL config should fail\n");
        return 1;
    }

    /* Bad chain_id fails */
    rx_config_t bad = { .chain_id = 999 };
    if (rx_engine_create(&bad) != NULL) {
        printf("  FAIL: bad chain_id should fail\n");
        return 1;
    }

    /* Mainnet succeeds */
    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) {
        printf("  FAIL: create returned NULL\n");
        return 1;
    }

    /* Version */
    const char *v = rx_version();
    printf("  version: %s\n", v);
    if (!v || strlen(v) == 0) {
        printf("  FAIL: empty version\n");
        rx_engine_destroy(e);
        return 1;
    }

    /* Block number starts at 0 */
    if (rx_get_block_number(e) != 0) {
        printf("  FAIL: initial block number should be 0\n");
        rx_engine_destroy(e);
        return 1;
    }

    rx_engine_destroy(e);
    printf("  OK\n");
    return 0;
}

static int test_genesis_and_queries(void) {
    printf("\ntest_genesis_and_queries:\n");

    /* Write a small test genesis */
    FILE *f = fopen(TEST_GENESIS, "w");
    if (!f) { printf("  FAIL: can't write genesis\n"); return 1; }
    fprintf(f,
        "{\n"
        "  \"0x0000000000000000000000000000000000000001\": {\n"
        "    \"balance\": \"0xde0b6b3a7640000\"\n"
        "  },\n"
        "  \"0x0000000000000000000000000000000000000002\": {\n"
        "    \"balance\": \"0x1bc16d674ec80000\"\n"
        "  },\n"
        "  \"0x0000000000000000000000000000000000000003\": {\n"
        "    \"balance\": \"0x0\"\n"
        "  }\n"
        "}\n");
    fclose(f);

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }

    /* Set logger */
    rx_set_logger(e, test_logger, NULL);

    /* Load genesis */
    if (!rx_engine_load_genesis(e, TEST_GENESIS, NULL)) {
        printf("  FAIL: load_genesis\n");
        rx_engine_destroy(e);
        return 1;
    }

    /* Can't load genesis twice */
    if (rx_engine_load_genesis(e, TEST_GENESIS, NULL)) {
        printf("  FAIL: double genesis should fail\n");
        rx_engine_destroy(e);
        return 1;
    }

    /* Query state */
    rx_state_t *state = rx_engine_get_state(e);
    if (!state) { printf("  FAIL: get_state\n"); rx_engine_destroy(e); return 1; }

    int errors = 0;

    /* Account 1: balance = 0xde0b6b3a7640000 = 1 ETH */
    rx_address_t addr1 = {0};
    addr1.bytes[19] = 0x01;

    if (!rx_account_exists(state, &addr1)) {
        printf("  FAIL: addr1 should exist\n"); errors++;
    }

    rx_uint256_t bal1 = rx_get_balance(state, &addr1);
    /* 1 ETH = 0x0de0b6b3a7640000 — check last 8 bytes */
    uint64_t bal1_lo = 0;
    for (int i = 24; i < 32; i++)
        bal1_lo = (bal1_lo << 8) | bal1.bytes[i];
    printf("  addr1 balance: %lu (expected: %lu)\n", bal1_lo, 0xde0b6b3a7640000UL);
    if (bal1_lo != 0xde0b6b3a7640000UL) {
        printf("  FAIL: addr1 balance\n"); errors++;
    }

    /* Account 2: balance = 0x1bc16d674ec80000 = 2 ETH */
    rx_address_t addr2 = {0};
    addr2.bytes[19] = 0x02;

    rx_uint256_t bal2 = rx_get_balance(state, &addr2);
    uint64_t bal2_lo = 0;
    for (int i = 24; i < 32; i++)
        bal2_lo = (bal2_lo << 8) | bal2.bytes[i];
    printf("  addr2 balance: %lu (expected: %lu)\n", bal2_lo, 0x1bc16d674ec80000UL);
    if (bal2_lo != 0x1bc16d674ec80000UL) {
        printf("  FAIL: addr2 balance\n"); errors++;
    }

    /* Account 3: exists but zero balance */
    rx_address_t addr3 = {0};
    addr3.bytes[19] = 0x03;
    if (!rx_account_exists(state, &addr3)) {
        printf("  FAIL: addr3 should exist\n"); errors++;
    }

    /* Nonce should be 0 for all */
    if (rx_get_nonce(state, &addr1) != 0) {
        printf("  FAIL: addr1 nonce should be 0\n"); errors++;
    }

    /* Non-existent account */
    rx_address_t noaddr = {0};
    noaddr.bytes[19] = 0xFF;
    if (rx_account_exists(state, &noaddr)) {
        printf("  FAIL: 0xFF should not exist\n"); errors++;
    }
    if (rx_get_nonce(state, &noaddr) != 0) {
        printf("  FAIL: non-existent nonce should be 0\n"); errors++;
    }

    /* Code queries on EOA */
    if (rx_get_code_size(state, &addr1) != 0) {
        printf("  FAIL: EOA code_size should be 0\n"); errors++;
    }
    if (rx_get_code(state, &addr1, NULL, 0) != 0) {
        printf("  FAIL: EOA get_code should return 0\n"); errors++;
    }

    /* Compute state root */
    rx_hash_t root = rx_compute_state_root(e);
    print_hash("state_root", &root);
    /* Root should be non-zero (we have accounts) */
    bool root_zero = true;
    for (int i = 0; i < 32; i++) if (root.bytes[i] != 0) { root_zero = false; break; }
    if (root_zero) {
        printf("  FAIL: state root is all zeros\n"); errors++;
    }

    rx_engine_destroy(e);
    unlink(TEST_GENESIS);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_save_load(void) {
    printf("\ntest_save_load:\n");

    /* Write genesis */
    FILE *f = fopen(TEST_GENESIS, "w");
    fprintf(f,
        "{\n"
        "  \"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\": {\n"
        "    \"balance\": \"0x3635c9adc5dea00000\"\n"
        "  }\n"
        "}\n");
    fclose(f);

    /* Create + load genesis */
    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    rx_engine_load_genesis(e, TEST_GENESIS, NULL);

    /* Save state */
    if (!rx_engine_save_state(e, TEST_STATE)) {
        printf("  FAIL: save_state\n");
        rx_engine_destroy(e);
        return 1;
    }
    printf("  saved state\n");

    /* Get original balance */
    rx_address_t addr = {0};
    memset(addr.bytes, 0xaa, 20);
    rx_state_t *state = rx_engine_get_state(e);
    rx_uint256_t orig_bal = rx_get_balance(state, &addr);
    rx_engine_destroy(e);

    /* Load into fresh engine */
    rx_engine_t *e2 = rx_engine_create(&config);
    if (!rx_engine_load_state(e2, TEST_STATE)) {
        printf("  FAIL: load_state\n");
        rx_engine_destroy(e2);
        return 1;
    }
    printf("  loaded state\n");

    int errors = 0;

    /* Verify balance matches */
    rx_state_t *state2 = rx_engine_get_state(e2);
    rx_uint256_t loaded_bal = rx_get_balance(state2, &addr);
    if (memcmp(orig_bal.bytes, loaded_bal.bytes, 32) != 0) {
        printf("  FAIL: balance mismatch after load\n");
        errors++;
    } else {
        printf("  OK: balance matches after round-trip\n");
    }

    /* Verify account exists */
    if (!rx_account_exists(state2, &addr)) {
        printf("  FAIL: account should exist after load\n");
        errors++;
    }

    rx_engine_destroy(e2);
    unlink(TEST_STATE);
    char hashes_path[256];
    snprintf(hashes_path, sizeof(hashes_path), "%s.hashes", TEST_STATE);
    unlink(hashes_path);
    unlink(TEST_GENESIS);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_logger_callback(void) {
    printf("\ntest_logger_callback:\n");

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);

    /* No logger set — should be silent */
    rx_engine_load_state(e, "/nonexistent/path.bin");  /* should fail silently */

    /* Set logger */
    g_log_count = 0;
    rx_set_logger(e, test_logger, NULL);
    rx_engine_load_state(e, "/nonexistent/path.bin");  /* should log error */

    rx_engine_destroy(e);

    if (g_log_count > 0) {
        printf("  OK: logger received %d messages\n", g_log_count);
        return 0;
    } else {
        printf("  FAIL: logger received no messages\n");
        return 1;
    }
}

static int test_error_codes(void) {
    printf("\ntest_error_codes:\n");
    int errors = 0;

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }

    /* Fresh engine — no error */
    if (rx_engine_last_error(e) != RX_OK) {
        printf("  FAIL: initial error should be RX_OK\n"); errors++;
    }

    /* Load bad path — should set FILE_IO error */
    if (rx_engine_load_state(e, "/nonexistent/path.bin")) {
        printf("  FAIL: load_state should fail\n"); errors++;
    }
    if (rx_engine_last_error(e) != RX_ERR_FILE_IO) {
        printf("  FAIL: expected RX_ERR_FILE_IO, got %d\n", rx_engine_last_error(e));
        errors++;
    }
    const char *msg = rx_engine_last_error_msg(e);
    if (!msg || strlen(msg) == 0) {
        printf("  FAIL: error message should not be empty\n"); errors++;
    } else {
        printf("  error msg: \"%s\"\n", msg);
    }

    /* Load genesis — clears error on success */
    FILE *f = fopen(TEST_GENESIS, "w");
    fprintf(f, "{ \"0x0000000000000000000000000000000000000001\": { \"balance\": \"0x1\" } }\n");
    fclose(f);

    if (!rx_engine_load_genesis(e, TEST_GENESIS, NULL)) {
        printf("  FAIL: load_genesis\n"); errors++;
    }
    if (rx_engine_last_error(e) != RX_OK) {
        printf("  FAIL: error should be cleared after success\n"); errors++;
    }

    /* Double genesis — should set ALREADY_INIT */
    if (rx_engine_load_genesis(e, TEST_GENESIS, NULL)) {
        printf("  FAIL: double genesis should fail\n"); errors++;
    }
    if (rx_engine_last_error(e) != RX_ERR_ALREADY_INIT) {
        printf("  FAIL: expected RX_ERR_ALREADY_INIT, got %d\n", rx_engine_last_error(e));
        errors++;
    }

    /* Error string */
    const char *s = rx_error_string(RX_ERR_DECODE);
    printf("  RX_ERR_DECODE = \"%s\"\n", s);
    if (!s || strlen(s) == 0) {
        printf("  FAIL: error string empty\n"); errors++;
    }

    /* NULL engine */
    if (rx_engine_last_error(NULL) != RX_ERR_NULL_ARG) {
        printf("  FAIL: NULL engine should return RX_ERR_NULL_ARG\n"); errors++;
    }

    rx_engine_destroy(e);
    unlink(TEST_GENESIS);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_block_hash_query(void) {
    printf("\ntest_block_hash_query:\n");
    int errors = 0;

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }

    /* No blocks executed — query should fail */
    rx_hash_t out;
    if (rx_get_block_hash(e, 1, &out)) {
        printf("  FAIL: should fail with no blocks\n"); errors++;
    }

    /* Block 0 always returns false (genesis hash not queryable this way) */
    if (rx_get_block_hash(e, 0, &out)) {
        printf("  FAIL: block 0 should return false\n"); errors++;
    }

    /* NULL args */
    if (rx_get_block_hash(NULL, 1, &out)) {
        printf("  FAIL: NULL engine should fail\n"); errors++;
    }
    if (rx_get_block_hash(e, 1, NULL)) {
        printf("  FAIL: NULL out should fail\n"); errors++;
    }

    rx_engine_destroy(e);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_commit_revert(void) {
    printf("\ntest_commit_revert:\n");
    int errors = 0;

    /* NULL engine */
    if (rx_commit_block(NULL)) {
        printf("  FAIL: commit NULL should fail\n"); errors++;
    }
    if (rx_revert_block(NULL)) {
        printf("  FAIL: revert NULL should fail\n"); errors++;
    }

    /* Create engine + load genesis */
    FILE *f = fopen(TEST_GENESIS, "w");
    fprintf(f,
        "{\n"
        "  \"0x0000000000000000000000000000000000000001\": {\n"
        "    \"balance\": \"0xde0b6b3a7640000\"\n"
        "  }\n"
        "}\n");
    fclose(f);

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }
    if (!rx_engine_load_genesis(e, TEST_GENESIS, NULL)) {
        printf("  FAIL: load_genesis\n"); rx_engine_destroy(e); return 1;
    }

    /* Get balance before */
    rx_address_t addr = {0};
    addr.bytes[19] = 0x01;
    rx_state_t *state = rx_engine_get_state(e);
    rx_uint256_t bal_before = rx_get_balance(state, &addr);

    /* Commit with no pending block — should succeed (no-op) */
    if (!rx_commit_block(e)) {
        printf("  FAIL: commit should succeed\n"); errors++;
    }

    /* Revert with no pending block — should succeed (no-op) */
    if (!rx_revert_block(e)) {
        printf("  FAIL: revert should succeed\n"); errors++;
    }

    /* Balance should be unchanged */
    rx_uint256_t bal_after = rx_get_balance(state, &addr);
    if (memcmp(bal_before.bytes, bal_after.bytes, 32) != 0) {
        printf("  FAIL: balance changed after no-op commit/revert\n"); errors++;
    }

    /* State root should be consistent */
    rx_hash_t root1 = rx_compute_state_root(e);
    rx_hash_t root2 = rx_compute_state_root(e);
    if (memcmp(root1.bytes, root2.bytes, 32) != 0) {
        printf("  FAIL: state root not stable\n"); errors++;
    }

    rx_engine_destroy(e);
    unlink(TEST_GENESIS);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_genesis_alloc(void) {
    printf("\ntest_genesis_alloc:\n");
    int errors = 0;

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }

    /* Build genesis alloc: 2 accounts */
    rx_genesis_account_t accounts[2];
    memset(accounts, 0, sizeof(accounts));

    /* Account 0: address 0x01, balance 1 ETH */
    accounts[0].address.bytes[19] = 0x01;
    /* 1 ETH = 0xde0b6b3a7640000 in big-endian */
    accounts[0].balance.bytes[24] = 0x0d;
    accounts[0].balance.bytes[25] = 0xe0;
    accounts[0].balance.bytes[26] = 0xb6;
    accounts[0].balance.bytes[27] = 0xb3;
    accounts[0].balance.bytes[28] = 0xa7;
    accounts[0].balance.bytes[29] = 0x64;
    accounts[0].balance.bytes[30] = 0x00;
    accounts[0].balance.bytes[31] = 0x00;

    /* Account 1: address 0x02, balance 2 ETH, nonce 5 */
    accounts[1].address.bytes[19] = 0x02;
    accounts[1].balance.bytes[24] = 0x1b;
    accounts[1].balance.bytes[25] = 0xc1;
    accounts[1].balance.bytes[26] = 0x6d;
    accounts[1].balance.bytes[27] = 0x67;
    accounts[1].balance.bytes[28] = 0x4e;
    accounts[1].balance.bytes[29] = 0xc8;
    accounts[1].balance.bytes[30] = 0x00;
    accounts[1].balance.bytes[31] = 0x00;
    accounts[1].nonce = 5;

    if (!rx_engine_load_genesis_alloc(e, accounts, 2, NULL)) {
        printf("  FAIL: load_genesis_alloc\n");
        rx_engine_destroy(e);
        return 1;
    }

    /* Query state */
    rx_state_t *state = rx_engine_get_state(e);

    /* Check account 0 */
    rx_address_t addr1 = {0};
    addr1.bytes[19] = 0x01;
    if (!rx_account_exists(state, &addr1)) {
        printf("  FAIL: addr1 should exist\n"); errors++;
    }
    rx_uint256_t bal1 = rx_get_balance(state, &addr1);
    uint64_t bal1_lo = 0;
    for (int i = 24; i < 32; i++)
        bal1_lo = (bal1_lo << 8) | bal1.bytes[i];
    if (bal1_lo != 0xde0b6b3a7640000UL) {
        printf("  FAIL: addr1 balance %lu != %lu\n", bal1_lo, 0xde0b6b3a7640000UL);
        errors++;
    }

    /* Check account 1 nonce */
    rx_address_t addr2 = {0};
    addr2.bytes[19] = 0x02;
    if (rx_get_nonce(state, &addr2) != 5) {
        printf("  FAIL: addr2 nonce should be 5, got %lu\n",
               rx_get_nonce(state, &addr2));
        errors++;
    }

    /* State root should match JSON-based genesis with same data */
    rx_hash_t root = rx_compute_state_root(e);
    bool root_zero = true;
    for (int i = 0; i < 32; i++) if (root.bytes[i] != 0) { root_zero = false; break; }
    if (root_zero) {
        printf("  FAIL: state root is all zeros\n"); errors++;
    }

    /* Can't load genesis twice */
    if (rx_engine_load_genesis_alloc(e, accounts, 2, NULL)) {
        printf("  FAIL: double genesis should fail\n"); errors++;
    }
    if (rx_engine_last_error(e) != RX_ERR_ALREADY_INIT) {
        printf("  FAIL: expected ALREADY_INIT\n"); errors++;
    }

    /* NULL accounts with count=0 is valid (empty genesis) */
    rx_engine_t *e2 = rx_engine_create(&config);
    if (!rx_engine_load_genesis_alloc(e2, NULL, 0, NULL)) {
        printf("  FAIL: empty genesis should succeed\n"); errors++;
    }
    rx_engine_destroy(e2);

    rx_engine_destroy(e);

    if (errors == 0) printf("  OK\n");
    return errors;
}

static int test_call(void) {
    printf("\ntest_call:\n");
    int errors = 0;

    rx_config_t config = { .chain_id = RX_CHAIN_MAINNET };
    rx_engine_t *e = rx_engine_create(&config);
    if (!e) { printf("  FAIL: create\n"); return 1; }

    /* Deploy a contract that returns 0x42 when called:
     * PUSH1 0x42   (60 42)
     * PUSH1 0x00   (60 00)
     * MSTORE       (52)
     * PUSH1 0x20   (60 20)
     * PUSH1 0x00   (60 00)
     * RETURN       (f3)
     * = 604260005260206000f3 (10 bytes)
     */
    uint8_t code[] = { 0x60, 0x42, 0x60, 0x00, 0x52,
                       0x60, 0x20, 0x60, 0x00, 0xf3 };

    rx_genesis_account_t accounts[2];
    memset(accounts, 0, sizeof(accounts));

    /* Contract at 0xCC..CC */
    memset(accounts[0].address.bytes, 0xCC, 20);
    accounts[0].code = code;
    accounts[0].code_len = sizeof(code);

    /* Caller at 0xAA..AA with some balance */
    memset(accounts[1].address.bytes, 0xAA, 20);
    accounts[1].balance.bytes[31] = 0x01; /* 1 wei */

    if (!rx_engine_load_genesis_alloc(e, accounts, 2, NULL)) {
        printf("  FAIL: genesis alloc\n");
        rx_engine_destroy(e);
        return 1;
    }

    /* Call the contract */
    rx_call_msg_t msg;
    memset(&msg, 0, sizeof(msg));
    memset(msg.from.bytes, 0xAA, 20);
    memset(msg.to.bytes, 0xCC, 20);
    msg.gas = 100000;

    rx_call_result_t result;
    if (!rx_call(e, &msg, &result)) {
        printf("  FAIL: rx_call returned false: %s\n",
               rx_engine_last_error_msg(e));
        errors++;
    } else {
        printf("  success=%d gas_used=%lu output_len=%zu\n",
               result.success, result.gas_used, result.output_len);

        if (!result.success) {
            printf("  FAIL: call should succeed\n"); errors++;
        }

        /* Output should be 32 bytes with 0x42 at position 31 */
        if (result.output_len != 32) {
            printf("  FAIL: expected 32 bytes output, got %zu\n",
                   result.output_len); errors++;
        } else if (result.output[31] != 0x42) {
            printf("  FAIL: expected 0x42 at byte 31, got 0x%02x\n",
                   result.output[31]); errors++;
        }

        rx_call_result_free(&result);
    }

    /* Verify state is unchanged after call */
    rx_state_t *state = rx_engine_get_state(e);
    rx_address_t contract_addr;
    memset(contract_addr.bytes, 0xCC, 20);
    if (!rx_account_exists(state, &contract_addr)) {
        printf("  FAIL: contract should still exist\n"); errors++;
    }

    /* NULL args */
    if (rx_call(NULL, &msg, &result)) {
        printf("  FAIL: NULL engine should fail\n"); errors++;
    }

    rx_engine_destroy(e);

    if (errors == 0) printf("  OK\n");
    return errors;
}

int main(void) {
    int errors = 0;
    errors += test_create_destroy();
    errors += test_genesis_and_queries();
    errors += test_save_load();
    errors += test_logger_callback();
    errors += test_error_codes();
    errors += test_block_hash_query();
    errors += test_commit_revert();
    errors += test_genesis_alloc();
    errors += test_call();

    printf("\n=== %s (%d errors) ===\n", errors ? "FAIL" : "PASS", errors);
    return errors ? 1 : 0;
}
