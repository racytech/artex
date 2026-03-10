/*
 * Test: Engine RPC dispatcher + Engine types + Engine store.
 *
 * Tests:
 *   1. JSON-RPC dispatch to registered method
 *   2. Method not found error
 *   3. Parse error for invalid JSON
 *   4. Invalid request (missing jsonrpc)
 *   5. RPC handler returning error
 *   6. Execution payload JSON round-trip (V4)
 *   7. Forkchoice state JSON parsing
 *   8. Payload status JSON output
 *   9. ExecutionPayload V1 parse (no withdrawals/blobGas)
 *  10. ExecutionPayload V2 parse (with withdrawals)
 *  11. PayloadAttributes V1 parse (3 fields only)
 *  12. Engine store put/get/has
 *  13. Engine store fork choice + prune
 *  14. Engine store blockhash ring buffer
 */

#include "engine_rpc.h"
#include "engine_types.h"
#include "engine_store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cjson/cJSON.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%d] %-55s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

/* =========================================================================
 * Test RPC Handlers
 * ========================================================================= */

static cJSON *echo_handler(const cJSON *params, const cJSON *id,
                           void *ctx, int *err_code, const char **err_msg) {
    (void)id; (void)ctx; (void)err_code; (void)err_msg;
    return cJSON_Duplicate(params, 1);
}

static cJSON *error_handler(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)params; (void)id; (void)ctx;
    *err_code = -32099;
    *err_msg = "custom error";
    return NULL;
}

/* =========================================================================
 * Tests
 * ========================================================================= */

static void test_rpc_dispatch_success(void) {
    TEST("RPC dispatch to registered method");

    engine_rpc_t *rpc = engine_rpc_create(NULL);
    engine_rpc_register(rpc, "test_echo", echo_handler);

    const char *req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test_echo\","
                      "\"params\":[42]}";
    size_t resp_len = 0;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); engine_rpc_destroy(rpc); return; }
    if (!strstr(resp, "\"result\"")) {
        printf("FAIL: no result in response: %s\n", resp);
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }
    if (!strstr(resp, "42")) {
        FAIL("expected 42 in result");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }

    free(resp);
    engine_rpc_destroy(rpc);
    PASS();
}

static void test_rpc_method_not_found(void) {
    TEST("RPC method not found");

    engine_rpc_t *rpc = engine_rpc_create(NULL);
    const char *req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"nonexistent\"}";
    size_t resp_len = 0;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); engine_rpc_destroy(rpc); return; }
    if (!strstr(resp, "-32601")) {
        FAIL("expected -32601 error code");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }

    free(resp);
    engine_rpc_destroy(rpc);
    PASS();
}

static void test_rpc_parse_error(void) {
    TEST("RPC parse error for invalid JSON");

    engine_rpc_t *rpc = engine_rpc_create(NULL);
    const char *req = "not json at all {{{";
    size_t resp_len = 0;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); engine_rpc_destroy(rpc); return; }
    if (!strstr(resp, "-32700")) {
        FAIL("expected -32700 error code");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }

    free(resp);
    engine_rpc_destroy(rpc);
    PASS();
}

static void test_rpc_invalid_request(void) {
    TEST("RPC invalid request (missing jsonrpc)");

    engine_rpc_t *rpc = engine_rpc_create(NULL);
    const char *req = "{\"id\":1,\"method\":\"test\"}";
    size_t resp_len = 0;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); engine_rpc_destroy(rpc); return; }
    if (!strstr(resp, "-32600")) {
        FAIL("expected -32600 error code");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }

    free(resp);
    engine_rpc_destroy(rpc);
    PASS();
}

static void test_rpc_handler_error(void) {
    TEST("RPC handler returning error");

    engine_rpc_t *rpc = engine_rpc_create(NULL);
    engine_rpc_register(rpc, "test_error", error_handler);

    const char *req = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test_error\"}";
    size_t resp_len = 0;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); engine_rpc_destroy(rpc); return; }
    if (!strstr(resp, "-32099")) {
        FAIL("expected -32099 error code");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }
    if (!strstr(resp, "custom error")) {
        FAIL("expected custom error message");
        free(resp);
        engine_rpc_destroy(rpc);
        return;
    }

    free(resp);
    engine_rpc_destroy(rpc);
    PASS();
}

/* =========================================================================
 * Type Tests
 * ========================================================================= */

static void test_payload_json_roundtrip(void) {
    TEST("ExecutionPayload JSON round-trip");

    const char *json_str =
        "{"
        "\"parentHash\":\"0x0000000000000000000000000000000000000000000000000000000000000001\","
        "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\","
        "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\","
        "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\","
        "\"logsBloom\":\"0x"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000\","
        "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\","
        "\"blockNumber\":\"0x1\","
        "\"gasLimit\":\"0x1c9c380\","
        "\"gasUsed\":\"0x5208\","
        "\"timestamp\":\"0x6789abcd\","
        "\"extraData\":\"0x\","
        "\"baseFeePerGas\":\"0x7\","
        "\"blockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000005\","
        "\"transactions\":[],"
        "\"withdrawals\":[],"
        "\"blobGasUsed\":\"0x0\","
        "\"excessBlobGas\":\"0x0\","
        "\"executionRequests\":[]"
        "}";

    cJSON *json = cJSON_Parse(json_str);
    if (!json) { FAIL("failed to parse JSON"); return; }

    execution_payload_t payload;
    if (!execution_payload_from_json(json, &payload)) {
        cJSON_Delete(json);
        FAIL("from_json failed");
        return;
    }
    cJSON_Delete(json);

    if (payload.block_number != 1) { FAIL("block_number != 1"); return; }
    if (payload.gas_limit != 0x1c9c380) { FAIL("gas_limit mismatch"); return; }
    if (payload.gas_used != 0x5208) { FAIL("gas_used mismatch"); return; }
    if (payload.timestamp != 0x6789abcd) { FAIL("timestamp mismatch"); return; }
    if (payload.parent_hash[31] != 0x01) { FAIL("parent_hash mismatch"); return; }
    if (payload.block_hash[31] != 0x05) { FAIL("block_hash mismatch"); return; }
    if (payload.fee_recipient[19] != 0x42) { FAIL("fee_recipient mismatch"); return; }

    /* Convert back to JSON */
    cJSON *out_json = execution_payload_to_json(&payload);
    if (!out_json) {
        execution_payload_free(&payload);
        FAIL("to_json failed");
        return;
    }

    /* Verify round-trip fields */
    cJSON *bn = cJSON_GetObjectItem(out_json, "blockNumber");
    if (!bn || !cJSON_IsString(bn) || strcmp(bn->valuestring, "0x1") != 0) {
        printf("FAIL: blockNumber round-trip: %s\n",
               bn ? bn->valuestring : "null");
        cJSON_Delete(out_json);
        execution_payload_free(&payload);
        return;
    }

    cJSON_Delete(out_json);
    execution_payload_free(&payload);
    PASS();
}

static void test_forkchoice_state_json(void) {
    TEST("ForkchoiceState JSON parsing");

    const char *json_str =
        "{"
        "\"headBlockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000001\","
        "\"safeBlockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000002\","
        "\"finalizedBlockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000003\""
        "}";

    cJSON *json = cJSON_Parse(json_str);
    if (!json) { FAIL("parse failed"); return; }

    forkchoice_state_t fc;
    if (!forkchoice_state_from_json(json, &fc)) {
        cJSON_Delete(json);
        FAIL("from_json failed");
        return;
    }
    cJSON_Delete(json);

    if (fc.head_block_hash[31] != 1) { FAIL("head hash mismatch"); return; }
    if (fc.safe_block_hash[31] != 2) { FAIL("safe hash mismatch"); return; }
    if (fc.finalized_block_hash[31] != 3) { FAIL("finalized hash mismatch"); return; }
    PASS();
}

static void test_payload_status_json(void) {
    TEST("PayloadStatus JSON output");

    payload_status_t ps = {
        .status = PAYLOAD_VALID,
        .has_latest_valid_hash = true,
        .validation_error = NULL,
    };
    memset(ps.latest_valid_hash, 0, 32);
    ps.latest_valid_hash[31] = 0xAB;

    cJSON *json = payload_status_to_json(&ps);
    if (!json) { FAIL("to_json failed"); return; }

    char *str = cJSON_PrintUnformatted(json);
    if (!strstr(str, "VALID")) {
        printf("FAIL: missing VALID in output: %s\n", str);
        free(str);
        cJSON_Delete(json);
        return;
    }
    if (!strstr(str, "ab")) {
        printf("FAIL: missing hash in output: %s\n", str);
        free(str);
        cJSON_Delete(json);
        return;
    }

    free(str);
    cJSON_Delete(json);
    PASS();
}

/* =========================================================================
 * Versioned Type Tests
 * ========================================================================= */

static void test_payload_v1_parse(void) {
    TEST("ExecutionPayload V1 parse (no withdrawals/blobGas)");

    /* V1 payload: 14 core fields only, no withdrawals/blobGas/requests */
    const char *json_str =
        "{"
        "\"parentHash\":\"0x0000000000000000000000000000000000000000000000000000000000000001\","
        "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\","
        "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\","
        "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\","
        "\"logsBloom\":\"0x"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000\","
        "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\","
        "\"blockNumber\":\"0xa\","
        "\"gasLimit\":\"0x1c9c380\","
        "\"gasUsed\":\"0x0\","
        "\"timestamp\":\"0x60000000\","
        "\"extraData\":\"0x\","
        "\"baseFeePerGas\":\"0x3b9aca00\","
        "\"blockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000005\","
        "\"transactions\":[]"
        "}";

    cJSON *json = cJSON_Parse(json_str);
    if (!json) { FAIL("parse JSON failed"); return; }

    execution_payload_t payload;
    memset(&payload, 0, sizeof(payload));
    if (!execution_payload_from_json_v(json, &payload, ENGINE_V1)) {
        cJSON_Delete(json);
        FAIL("from_json_v(V1) failed");
        return;
    }
    cJSON_Delete(json);

    if (payload.block_number != 10) {
        FAIL("block_number != 10"); execution_payload_free(&payload); return;
    }
    if (payload.withdrawal_count != 0) {
        FAIL("V1 should have no withdrawals"); execution_payload_free(&payload); return;
    }
    if (payload.blob_gas_used != 0 || payload.excess_blob_gas != 0) {
        FAIL("V1 should have no blob gas fields"); execution_payload_free(&payload); return;
    }

    /* Round-trip: V1 to_json should NOT emit withdrawals or blobGas */
    cJSON *out = execution_payload_to_json_v(&payload, ENGINE_V1);
    if (!out) {
        FAIL("to_json_v(V1) failed"); execution_payload_free(&payload); return;
    }
    if (cJSON_GetObjectItem(out, "withdrawals")) {
        FAIL("V1 JSON should not have withdrawals");
        cJSON_Delete(out); execution_payload_free(&payload); return;
    }
    if (cJSON_GetObjectItem(out, "blobGasUsed")) {
        FAIL("V1 JSON should not have blobGasUsed");
        cJSON_Delete(out); execution_payload_free(&payload); return;
    }

    cJSON_Delete(out);
    execution_payload_free(&payload);
    PASS();
}

static void test_payload_v2_parse(void) {
    TEST("ExecutionPayload V2 parse (with withdrawals)");

    /* V2: core + withdrawals, no blobGas */
    const char *json_str =
        "{"
        "\"parentHash\":\"0x0000000000000000000000000000000000000000000000000000000000000001\","
        "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\","
        "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\","
        "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\","
        "\"logsBloom\":\"0x"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000000000000000000000000000000000\","
        "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\","
        "\"blockNumber\":\"0x14\","
        "\"gasLimit\":\"0x1c9c380\","
        "\"gasUsed\":\"0x5208\","
        "\"timestamp\":\"0x60000001\","
        "\"extraData\":\"0x\","
        "\"baseFeePerGas\":\"0x3b9aca00\","
        "\"blockHash\":\"0x0000000000000000000000000000000000000000000000000000000000000005\","
        "\"transactions\":[],"
        "\"withdrawals\":["
        "  {\"index\":\"0x0\",\"validatorIndex\":\"0x1\","
        "   \"address\":\"0x0000000000000000000000000000000000000099\","
        "   \"amount\":\"0x64\"}"
        "]"
        "}";

    cJSON *json = cJSON_Parse(json_str);
    if (!json) { FAIL("parse JSON failed"); return; }

    execution_payload_t payload;
    memset(&payload, 0, sizeof(payload));
    if (!execution_payload_from_json_v(json, &payload, ENGINE_V2)) {
        cJSON_Delete(json);
        FAIL("from_json_v(V2) failed");
        return;
    }
    cJSON_Delete(json);

    if (payload.block_number != 20) {
        FAIL("block_number != 20"); execution_payload_free(&payload); return;
    }
    if (payload.withdrawal_count != 1) {
        FAIL("V2 should have 1 withdrawal"); execution_payload_free(&payload); return;
    }
    if (payload.withdrawals[0].validator_index != 1) {
        FAIL("withdrawal validator_index != 1"); execution_payload_free(&payload); return;
    }
    if (payload.withdrawals[0].amount != 100) {
        FAIL("withdrawal amount != 100"); execution_payload_free(&payload); return;
    }
    if (payload.withdrawals[0].address[19] != 0x99) {
        FAIL("withdrawal address mismatch"); execution_payload_free(&payload); return;
    }

    /* V2 to_json should have withdrawals but NOT blobGasUsed */
    cJSON *out = execution_payload_to_json_v(&payload, ENGINE_V2);
    if (!out) {
        FAIL("to_json_v(V2) failed"); execution_payload_free(&payload); return;
    }
    if (!cJSON_GetObjectItem(out, "withdrawals")) {
        FAIL("V2 JSON should have withdrawals");
        cJSON_Delete(out); execution_payload_free(&payload); return;
    }
    if (cJSON_GetObjectItem(out, "blobGasUsed")) {
        FAIL("V2 JSON should not have blobGasUsed");
        cJSON_Delete(out); execution_payload_free(&payload); return;
    }

    cJSON_Delete(out);
    execution_payload_free(&payload);
    PASS();
}

static void test_payload_attrs_v1_parse(void) {
    TEST("PayloadAttributes V1 parse (3 fields only)");

    /* V1: only timestamp, prevRandao, suggestedFeeRecipient */
    const char *json_str =
        "{"
        "\"timestamp\":\"0x60000000\","
        "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000001\","
        "\"suggestedFeeRecipient\":\"0x0000000000000000000000000000000000000042\""
        "}";

    cJSON *json = cJSON_Parse(json_str);
    if (!json) { FAIL("parse JSON failed"); return; }

    payload_attributes_t attrs;
    memset(&attrs, 0, sizeof(attrs));
    if (!payload_attributes_from_json_v(json, &attrs, ENGINE_V1)) {
        cJSON_Delete(json);
        FAIL("from_json_v(V1) failed");
        return;
    }
    cJSON_Delete(json);

    if (attrs.timestamp != 0x60000000) {
        FAIL("timestamp mismatch"); payload_attributes_free(&attrs); return;
    }
    if (attrs.prev_randao[31] != 1) {
        FAIL("prevRandao mismatch"); payload_attributes_free(&attrs); return;
    }
    if (attrs.suggested_fee_recipient[19] != 0x42) {
        FAIL("suggestedFeeRecipient mismatch"); payload_attributes_free(&attrs); return;
    }
    if (attrs.withdrawal_count != 0) {
        FAIL("V1 should have no withdrawals"); payload_attributes_free(&attrs); return;
    }

    payload_attributes_free(&attrs);
    PASS();
}

/* =========================================================================
 * Store Tests
 * ========================================================================= */

static void test_store_put_get(void) {
    TEST("Store put/get/has");

    engine_store_t *store = engine_store_create();

    execution_payload_t payload;
    memset(&payload, 0, sizeof(payload));
    payload.block_number = 100;
    memset(payload.block_hash, 0xAA, 32);

    if (!engine_store_put(store, &payload, true)) {
        FAIL("put failed");
        engine_store_destroy(store);
        return;
    }

    if (!engine_store_has(store, payload.block_hash)) {
        FAIL("has returned false");
        engine_store_destroy(store);
        return;
    }

    const engine_stored_block_t *got = engine_store_get(store, payload.block_hash);
    if (!got) {
        FAIL("get returned NULL");
        engine_store_destroy(store);
        return;
    }
    if (got->payload.block_number != 100) {
        FAIL("block_number mismatch");
        engine_store_destroy(store);
        return;
    }
    if (!got->valid) {
        FAIL("valid flag mismatch");
        engine_store_destroy(store);
        return;
    }

    /* Unknown hash */
    uint8_t unknown[32];
    memset(unknown, 0xBB, 32);
    if (engine_store_has(store, unknown)) {
        FAIL("should not have unknown hash");
        engine_store_destroy(store);
        return;
    }

    engine_store_destroy(store);
    PASS();
}

static void test_store_forkchoice_prune(void) {
    TEST("Store fork choice + prune");

    engine_store_t *store = engine_store_create();

    /* Add blocks 10, 20, 30 */
    for (int i = 1; i <= 3; i++) {
        execution_payload_t p;
        memset(&p, 0, sizeof(p));
        p.block_number = (uint64_t)(i * 10);
        memset(p.block_hash, (uint8_t)i, 32);
        engine_store_put(store, &p, true);
    }

    /* Set forkchoice: head=30, safe=20, finalized=20 */
    uint8_t head[32], safe[32], fin[32];
    memset(head, 3, 32);
    memset(safe, 2, 32);
    memset(fin, 2, 32);
    engine_store_set_forkchoice(store, head, safe, fin);
    engine_store_prune(store);

    /* Block 10 should be pruned (< finalized block 20) */
    uint8_t hash10[32];
    memset(hash10, 1, 32);
    if (engine_store_has(store, hash10)) {
        FAIL("block 10 should have been pruned");
        engine_store_destroy(store);
        return;
    }

    /* Block 20 (finalized) should still exist */
    if (!engine_store_has(store, safe)) {
        FAIL("block 20 should still exist");
        engine_store_destroy(store);
        return;
    }

    engine_store_destroy(store);
    PASS();
}

static void test_store_blockhash_ring(void) {
    TEST("Store blockhash ring buffer");

    engine_store_t *store = engine_store_create();

    /* Record blocks 100-110 */
    for (uint64_t i = 100; i <= 110; i++) {
        uint8_t hash[32];
        memset(hash, 0, 32);
        hash[31] = (uint8_t)i;
        engine_store_record_blockhash(store, i, hash);
    }

    /* Look up block 105 */
    uint8_t out[32];
    if (!engine_store_get_blockhash(store, 105, out)) {
        FAIL("get_blockhash failed for 105");
        engine_store_destroy(store);
        return;
    }
    if (out[31] != 105) {
        FAIL("hash mismatch for block 105");
        engine_store_destroy(store);
        return;
    }

    /* Block 0 should not be in range */
    if (engine_store_get_blockhash(store, 0, out)) {
        FAIL("should not find block 0");
        engine_store_destroy(store);
        return;
    }

    engine_store_destroy(store);
    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Engine RPC + Types + Store Tests ===\n\n");

    /* RPC tests */
    test_rpc_dispatch_success();
    test_rpc_method_not_found();
    test_rpc_parse_error();
    test_rpc_invalid_request();
    test_rpc_handler_error();

    /* Type tests */
    test_payload_json_roundtrip();
    test_forkchoice_state_json();
    test_payload_status_json();

    /* Versioned type tests */
    test_payload_v1_parse();
    test_payload_v2_parse();
    test_payload_attrs_v1_parse();

    /* Store tests */
    test_store_put_get();
    test_store_forkchoice_prune();
    test_store_blockhash_ring();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
