/*
 * Test: Engine Handlers — All versions (Paris V1 → Prague V4).
 *
 * Tests:
 *   1. engine_exchangeCapabilities — lists all 12 methods
 *   2. engine_newPayloadV1 — Paris payload (no withdrawals)
 *   3. engine_newPayloadV2 — Shanghai payload (with withdrawals)
 *   4. engine_newPayloadV3 — Cancun payload (3 params)
 *   5. engine_newPayloadV4 — Prague payload (4 params)
 *   6. engine_newPayloadV1 — syncing (unknown parent)
 *   7. engine_forkchoiceUpdatedV1 — update head
 *   8. engine_forkchoiceUpdatedV3 — syncing (unknown head)
 *   9. engine_getPayloadV1 — unknown payload error
 *  10. Full flow: V1 newPayload → V1 forkchoice → verify store
 */

#include "engine_rpc.h"
#include "engine_handlers.h"
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

/* Minimal V1 payload JSON (Paris — no withdrawals, no blob gas) */
#define V1_PAYLOAD_FMT \
    "{" \
    "\"parentHash\":\"%s\"," \
    "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\"," \
    "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\"," \
    "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\"," \
    "\"logsBloom\":\"0x" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000\"," \
    "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\"," \
    "\"blockNumber\":\"%s\"," \
    "\"gasLimit\":\"0x1c9c380\"," \
    "\"gasUsed\":\"0x0\"," \
    "\"timestamp\":\"0x6789abcd\"," \
    "\"extraData\":\"0x\"," \
    "\"baseFeePerGas\":\"0x7\"," \
    "\"blockHash\":\"%s\"," \
    "\"transactions\":[]" \
    "}"

/* V2 payload adds withdrawals */
#define V2_PAYLOAD_FMT \
    "{" \
    "\"parentHash\":\"%s\"," \
    "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\"," \
    "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\"," \
    "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\"," \
    "\"logsBloom\":\"0x" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000\"," \
    "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\"," \
    "\"blockNumber\":\"%s\"," \
    "\"gasLimit\":\"0x1c9c380\"," \
    "\"gasUsed\":\"0x0\"," \
    "\"timestamp\":\"0x6789abcd\"," \
    "\"extraData\":\"0x\"," \
    "\"baseFeePerGas\":\"0x7\"," \
    "\"blockHash\":\"%s\"," \
    "\"transactions\":[]," \
    "\"withdrawals\":[{\"index\":\"0x0\",\"validatorIndex\":\"0x1\"," \
    "\"address\":\"0x0000000000000000000000000000000000000042\",\"amount\":\"0x1\"}]" \
    "}"

/* V3 payload adds blob gas fields */
#define V3_PAYLOAD_FMT \
    "{" \
    "\"parentHash\":\"%s\"," \
    "\"feeRecipient\":\"0x0000000000000000000000000000000000000042\"," \
    "\"stateRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000002\"," \
    "\"receiptsRoot\":\"0x0000000000000000000000000000000000000000000000000000000000000003\"," \
    "\"logsBloom\":\"0x" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000" \
    "0000000000000000000000000000000000000000000000000000000000000000\"," \
    "\"prevRandao\":\"0x0000000000000000000000000000000000000000000000000000000000000004\"," \
    "\"blockNumber\":\"%s\"," \
    "\"gasLimit\":\"0x1c9c380\"," \
    "\"gasUsed\":\"0x0\"," \
    "\"timestamp\":\"0x6789abcd\"," \
    "\"extraData\":\"0x\"," \
    "\"baseFeePerGas\":\"0x7\"," \
    "\"blockHash\":\"%s\"," \
    "\"transactions\":[]," \
    "\"withdrawals\":[]," \
    "\"blobGasUsed\":\"0x20000\"," \
    "\"excessBlobGas\":\"0x0\"" \
    "}"

static const char *ZERO_HASH = "0x0000000000000000000000000000000000000000000000000000000000000000";

static char *make_rpc_request(const char *method, const char *params) {
    char *buf = malloc(8192);
    snprintf(buf, 8192,
             "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"%s\",\"params\":%s}",
             method, params);
    return buf;
}

/* =========================================================================
 * Tests
 * ========================================================================= */

static void test_exchange_capabilities(engine_rpc_t *rpc) {
    TEST("exchangeCapabilities — lists all 12 methods");

    char *req = make_rpc_request("engine_exchangeCapabilities", "[[]]");
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp) { FAIL("null response"); free(req); return; }
    /* Check for all 12 methods */
    const char *methods[] = {
        "engine_newPayloadV1", "engine_newPayloadV2",
        "engine_newPayloadV3", "engine_newPayloadV4",
        "engine_forkchoiceUpdatedV1", "engine_forkchoiceUpdatedV2",
        "engine_forkchoiceUpdatedV3",
        "engine_getPayloadV1", "engine_getPayloadV2",
        "engine_getPayloadV3", "engine_getPayloadV4",
        "engine_exchangeCapabilities"
    };
    for (int i = 0; i < 12; i++) {
        if (!strstr(resp, methods[i])) {
            printf("FAIL: missing %s\n", methods[i]);
            free(resp); free(req);
            return;
        }
    }

    free(resp);
    free(req);
    PASS();
}

static void test_new_payload_v1(engine_rpc_t *rpc) {
    TEST("engine_newPayloadV1 — Paris (no withdrawals)");

    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V1_PAYLOAD_FMT,
             ZERO_HASH, "0x1",
             "0x00000000000000000000000000000000000000000000000000000000000000a1");

    char params[4096 + 64];
    snprintf(params, sizeof(params), "[%s]", payload_json);

    char *req = make_rpc_request("engine_newPayloadV1", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: expected VALID: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_new_payload_v2(engine_rpc_t *rpc) {
    TEST("engine_newPayloadV2 — Shanghai (with withdrawals)");

    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V2_PAYLOAD_FMT,
             ZERO_HASH, "0x2",
             "0x00000000000000000000000000000000000000000000000000000000000000a2");

    char params[4096 + 64];
    snprintf(params, sizeof(params), "[%s]", payload_json);

    char *req = make_rpc_request("engine_newPayloadV2", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: expected VALID: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_new_payload_v3(engine_rpc_t *rpc) {
    TEST("engine_newPayloadV3 — Cancun (3 params)");

    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V3_PAYLOAD_FMT,
             ZERO_HASH, "0x3",
             "0x00000000000000000000000000000000000000000000000000000000000000a3");

    char params[4096 + 256];
    snprintf(params, sizeof(params), "[%s,[],\"%s\"]",
             payload_json, ZERO_HASH);

    char *req = make_rpc_request("engine_newPayloadV3", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: expected VALID: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_new_payload_v4(engine_rpc_t *rpc) {
    TEST("engine_newPayloadV4 — Prague (4 params)");

    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V3_PAYLOAD_FMT,
             ZERO_HASH, "0x4",
             "0x00000000000000000000000000000000000000000000000000000000000000a4");

    char params[4096 + 256];
    snprintf(params, sizeof(params), "[%s,[],\"%s\",[]]",
             payload_json, ZERO_HASH);

    char *req = make_rpc_request("engine_newPayloadV4", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: expected VALID: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_new_payload_syncing(engine_rpc_t *rpc) {
    TEST("engine_newPayloadV1 — syncing (unknown parent)");

    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V1_PAYLOAD_FMT,
             "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
             "0x99",
             "0x00000000000000000000000000000000000000000000000000000000000000bb");

    char params[4096 + 64];
    snprintf(params, sizeof(params), "[%s]", payload_json);

    char *req = make_rpc_request("engine_newPayloadV1", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "SYNCING")) {
        printf("FAIL: expected SYNCING: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_forkchoice_v1(engine_rpc_t *rpc) {
    TEST("engine_forkchoiceUpdatedV1 — update head");

    /* Block 0xa1 was stored as VALID from test_new_payload_v1 */
    const char *params =
        "[{\"headBlockHash\":\"0x00000000000000000000000000000000000000000000000000000000000000a1\","
        "\"safeBlockHash\":\"0x00000000000000000000000000000000000000000000000000000000000000a1\","
        "\"finalizedBlockHash\":\"0x00000000000000000000000000000000000000000000000000000000000000a1\""
        "},null]";

    char *req = make_rpc_request("engine_forkchoiceUpdatedV1", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: expected VALID: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }
    if (!strstr(resp, "payloadStatus")) {
        FAIL("missing payloadStatus");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_forkchoice_syncing(engine_rpc_t *rpc) {
    TEST("engine_forkchoiceUpdatedV3 — syncing (unknown head)");

    const char *params =
        "[{\"headBlockHash\":\"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\","
        "\"safeBlockHash\":\"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\","
        "\"finalizedBlockHash\":\"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff\""
        "},null]";

    char *req = make_rpc_request("engine_forkchoiceUpdatedV3", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "SYNCING")) {
        printf("FAIL: expected SYNCING: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_get_payload_unknown(engine_rpc_t *rpc) {
    TEST("engine_getPayloadV1 — unknown payload error");

    const char *params = "[\"0x0000000000000000\"]";
    char *req = make_rpc_request("engine_getPayloadV1", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);

    if (!resp || !strstr(resp, "-38001")) {
        printf("FAIL: expected -38001: %s\n", resp ? resp : "null");
        free(resp); free(req);
        return;
    }

    free(resp);
    free(req);
    PASS();
}

static void test_full_flow_v1(engine_rpc_t *rpc, engine_store_t *store) {
    TEST("Full flow V1: newPayload → forkchoice → verify");

    /* Submit block 10 via V1 */
    char payload_json[4096];
    snprintf(payload_json, sizeof(payload_json), V1_PAYLOAD_FMT,
             ZERO_HASH, "0xa",
             "0x2222222222222222222222222222222222222222222222222222222222222222");

    char params[4096 + 64];
    snprintf(params, sizeof(params), "[%s]", payload_json);

    char *req = make_rpc_request("engine_newPayloadV1", params);
    size_t resp_len;
    char *resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);
    free(req);
    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: newPayload: %s\n", resp ? resp : "null");
        free(resp);
        return;
    }
    free(resp);

    /* Set forkchoice via V1 */
    const char *fc_params =
        "[{\"headBlockHash\":\"0x2222222222222222222222222222222222222222222222222222222222222222\","
        "\"safeBlockHash\":\"0x2222222222222222222222222222222222222222222222222222222222222222\","
        "\"finalizedBlockHash\":\"0x2222222222222222222222222222222222222222222222222222222222222222\""
        "},null]";

    req = make_rpc_request("engine_forkchoiceUpdatedV1", fc_params);
    resp = engine_rpc_dispatch(rpc, req, strlen(req), &resp_len);
    free(req);
    if (!resp || !strstr(resp, "VALID")) {
        printf("FAIL: forkchoice: %s\n", resp ? resp : "null");
        free(resp);
        return;
    }
    free(resp);

    /* Verify store */
    uint8_t expected[32];
    memset(expected, 0x22, 32);
    if (!engine_store_has(store, expected)) {
        FAIL("store doesn't have block");
        return;
    }
    if (!store->has_head || memcmp(store->head_hash, expected, 32) != 0) {
        FAIL("head hash not updated");
        return;
    }

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Engine Handler Tests (All Versions) ===\n\n");

    engine_store_t *store = engine_store_create();
    engine_handler_ctx_t *ctx = engine_handler_ctx_create(store, NULL, NULL);
    engine_rpc_t *rpc = engine_rpc_create(ctx);
    engine_register_handlers(rpc);

    test_exchange_capabilities(rpc);
    test_new_payload_v1(rpc);
    test_new_payload_v2(rpc);
    test_new_payload_v3(rpc);
    test_new_payload_v4(rpc);
    test_new_payload_syncing(rpc);
    test_forkchoice_v1(rpc);
    test_forkchoice_syncing(rpc);
    test_get_payload_unknown(rpc);
    test_full_flow_v1(rpc, store);

    engine_rpc_destroy(rpc);
    engine_handler_ctx_destroy(ctx);
    engine_store_destroy(store);

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
