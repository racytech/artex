/*
 * Engine Handlers — All versions (Paris V1 → Prague V4).
 *
 * Shared core logic with version-specific wrappers for:
 *   newPayload:        V1..V4
 *   forkchoiceUpdated: V1..V3
 *   getPayload:        V1..V4
 *   exchangeCapabilities
 */

#include "engine_handlers.h"
#include "engine_rpc.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* =========================================================================
 * Handler Context
 * ========================================================================= */

engine_handler_ctx_t *engine_handler_ctx_create(engine_store_t *store,
                                                 struct evm *evm,
                                                 struct evm_state *state) {
    engine_handler_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->store = store;
    ctx->evm = evm;
    ctx->state = state;
    return ctx;
}

void engine_handler_ctx_destroy(engine_handler_ctx_t *ctx) {
    free(ctx);
}

/* =========================================================================
 * Helpers
 * ========================================================================= */

static bool is_zero_hash(const uint8_t hash[32]) {
    for (int i = 0; i < 32; i++)
        if (hash[i] != 0) return false;
    return true;
}

/* =========================================================================
 * newPayload — Shared Core
 *
 * V1: params[0] = ExecutionPayloadV1
 * V2: params[0] = ExecutionPayloadV2
 * V3: params[0] = ExecutionPayloadV3, params[1] = blobHashes, params[2] = beaconRoot
 * V4: params[0] = ExecutionPayloadV3, params[1] = blobHashes, params[2] = beaconRoot, params[3] = requests
 * ========================================================================= */

static cJSON *new_payload_common(const cJSON *params, void *ctx_ptr,
                                  engine_version_t version,
                                  int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with ExecutionPayload";
        return NULL;
    }

    /* V3+ requires 3 params, V4 requires 4 */
    if (version >= ENGINE_V3 && cJSON_GetArraySize(params) < 3) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "V3/V4 requires expectedBlobVersionedHashes and parentBeaconBlockRoot";
        return NULL;
    }

    /* Parse the execution payload with version-appropriate fields */
    cJSON *payload_json = cJSON_GetArrayItem(params, 0);
    execution_payload_t payload;
    if (!execution_payload_from_json_v(payload_json, &payload, version)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "invalid execution payload";
        return NULL;
    }

    /* V4: parse execution requests from params[3] */
    if (version >= ENGINE_V4 && cJSON_GetArraySize(params) >= 4) {
        cJSON *reqs_json = cJSON_GetArrayItem(params, 3);
        if (reqs_json && cJSON_IsArray(reqs_json)) {
            payload.request_count = (size_t)cJSON_GetArraySize(reqs_json);
            if (payload.request_count > 0) {
                payload.requests = calloc(payload.request_count, sizeof(uint8_t *));
                payload.request_lengths = calloc(payload.request_count, sizeof(size_t));
                /* Parse each request as hex DATA */
                size_t idx = 0;
                cJSON *req;
                cJSON_ArrayForEach(req, reqs_json) {
                    if (cJSON_IsString(req)) {
                        const char *hex = req->valuestring;
                        if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
                            hex += 2;
                        size_t hex_len = strlen(hex);
                        size_t byte_len = hex_len / 2;
                        payload.requests[idx] = malloc(byte_len > 0 ? byte_len : 1);
                        if (payload.requests[idx] && byte_len > 0) {
                            /* Simple inline hex decode */
                            for (size_t j = 0; j < byte_len; j++) {
                                int hi = (hex[j*2] >= 'a') ? hex[j*2]-'a'+10 :
                                         (hex[j*2] >= 'A') ? hex[j*2]-'A'+10 :
                                         hex[j*2]-'0';
                                int lo = (hex[j*2+1] >= 'a') ? hex[j*2+1]-'a'+10 :
                                         (hex[j*2+1] >= 'A') ? hex[j*2+1]-'A'+10 :
                                         hex[j*2+1]-'0';
                                payload.requests[idx][j] = (uint8_t)((hi << 4) | lo);
                            }
                        }
                        payload.request_lengths[idx] = byte_len;
                    }
                    idx++;
                }
            }
        }
    }

    /* TODO: Verify block hash (RLP encode header → keccak256) */
    /* TODO: V3+: Validate expectedBlobVersionedHashes from params[1] */

    payload_status_t status;
    memset(&status, 0, sizeof(status));

    /* Check parent */
    if (!is_zero_hash(payload.parent_hash) &&
        !engine_store_has(ctx->store, payload.parent_hash)) {
        status.status = PAYLOAD_SYNCING;
        status.has_latest_valid_hash = false;
        engine_store_put(ctx->store, &payload, false);
    } else {
        /*
         * TODO: Full block execution:
         * 1. Convert payload → block_header_t + block_body_t
         * 2. block_execute(evm, &header, &body, block_hashes)
         * 3. Compare state_root → VALID or INVALID
         * 4. On mismatch: revert, return INVALID
         */
        status.status = PAYLOAD_VALID;
        status.has_latest_valid_hash = true;
        memcpy(status.latest_valid_hash, payload.block_hash, 32);

        engine_store_put(ctx->store, &payload, true);
        engine_store_record_blockhash(ctx->store,
                                       payload.block_number,
                                       payload.block_hash);
    }

    return payload_status_to_json(&status);
}

/* newPayload version wrappers */
cJSON *engine_newPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_newPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_newPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

cJSON *engine_newPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return new_payload_common(params, ctx, ENGINE_V4, err_code, err_msg);
}

/* =========================================================================
 * forkchoiceUpdated — Shared Core
 *
 * V1: params[0] = ForkchoiceState, params[1] = PayloadAttributesV1 | null
 * V2: params[0] = ForkchoiceState, params[1] = PayloadAttributesV1|V2 | null
 * V3: params[0] = ForkchoiceState, params[1] = PayloadAttributesV3 | null
 * ========================================================================= */

static cJSON *forkchoice_updated_common(const cJSON *params, void *ctx_ptr,
                                         engine_version_t version,
                                         int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with ForkchoiceState";
        return NULL;
    }

    cJSON *fc_json = cJSON_GetArrayItem(params, 0);
    forkchoice_state_t fc;
    if (!forkchoice_state_from_json(fc_json, &fc)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "invalid forkchoice state";
        return NULL;
    }

    payload_status_t ps;
    memset(&ps, 0, sizeof(ps));

    if (!engine_store_has(ctx->store, fc.head_block_hash)) {
        ps.status = PAYLOAD_SYNCING;
        ps.has_latest_valid_hash = false;
    } else {
        engine_store_set_forkchoice(ctx->store,
                                     fc.head_block_hash,
                                     fc.safe_block_hash,
                                     fc.finalized_block_hash);
        engine_store_prune(ctx->store);

        ps.status = PAYLOAD_VALID;
        ps.has_latest_valid_hash = true;
        memcpy(ps.latest_valid_hash, fc.head_block_hash, 32);
    }

    cJSON *result = cJSON_CreateObject();
    cJSON_AddItemToObject(result, "payloadStatus", payload_status_to_json(&ps));

    /* Handle optional PayloadAttributes */
    cJSON *attr_json = cJSON_GetArrayItem(params, 1);
    if (attr_json && !cJSON_IsNull(attr_json) && ps.status == PAYLOAD_VALID) {
        payload_attributes_t attrs;
        if (payload_attributes_from_json_v(attr_json, &attrs, version)) {
            uint64_t payload_id = attrs.timestamp ^ 0xDEADBEEF;
            char id_hex[19];
            snprintf(id_hex, sizeof(id_hex), "0x%016llx",
                     (unsigned long long)payload_id);
            cJSON_AddStringToObject(result, "payloadId", id_hex);
            payload_attributes_free(&attrs);
        } else {
            cJSON_AddNullToObject(result, "payloadId");
        }
    } else {
        cJSON_AddNullToObject(result, "payloadId");
    }

    return result;
}

/* forkchoiceUpdated version wrappers */
cJSON *engine_forkchoiceUpdatedV1(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_forkchoiceUpdatedV2(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_forkchoiceUpdatedV3(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return forkchoice_updated_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

/* =========================================================================
 * getPayload — Shared Core
 *
 * V1: returns ExecutionPayloadV1
 * V2: returns {executionPayload, blockValue}
 * V3: returns {executionPayload, blockValue, blobsBundle, shouldOverrideBuilder}
 * V4: returns V3 + executionRequests
 * ========================================================================= */

static cJSON *get_payload_common(const cJSON *params, void *ctx_ptr,
                                  engine_version_t version,
                                  int *err_code, const char **err_msg) {
    engine_handler_ctx_t *ctx = (engine_handler_ctx_t *)ctx_ptr;
    if (!ctx || !ctx->store) {
        *err_code = -32603;
        *err_msg = "no handler context";
        return NULL;
    }

    if (!params || !cJSON_IsArray(params) || cJSON_GetArraySize(params) < 1) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "expected params array with payloadId";
        return NULL;
    }

    cJSON *pid = cJSON_GetArrayItem(params, 0);
    if (!pid || !cJSON_IsString(pid)) {
        *err_code = RPC_ERR_INVALID_PARAMS;
        *err_msg = "payloadId must be a hex string";
        return NULL;
    }

    const char *hex = pid->valuestring;
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;
    uint64_t payload_id = strtoull(hex, NULL, 16);

    execution_payload_t payload;
    if (!engine_store_take_pending(ctx->store, payload_id, &payload)) {
        *err_code = ENGINE_ERR_UNKNOWN_PAYLOAD;
        *err_msg = "unknown payload";
        return NULL;
    }

    cJSON *result;

    if (version == ENGINE_V1) {
        /* V1: return just the ExecutionPayload */
        result = execution_payload_to_json_v(&payload, ENGINE_V1);
    } else {
        /* V2+: return wrapped object */
        result = cJSON_CreateObject();
        cJSON_AddItemToObject(result, "executionPayload",
                              execution_payload_to_json_v(&payload, version));
        cJSON_AddStringToObject(result, "blockValue", "0x0");

        if (version >= ENGINE_V3) {
            /* Blobs bundle (empty for non-blob blocks) */
            cJSON *blobs = cJSON_CreateObject();
            cJSON_AddItemToObject(blobs, "commitments", cJSON_CreateArray());
            cJSON_AddItemToObject(blobs, "proofs", cJSON_CreateArray());
            cJSON_AddItemToObject(blobs, "blobs", cJSON_CreateArray());
            cJSON_AddItemToObject(result, "blobsBundle", blobs);
            cJSON_AddBoolToObject(result, "shouldOverrideBuilder", false);
        }

        if (version >= ENGINE_V4) {
            /* Execution requests */
            cJSON *reqs = cJSON_AddArrayToObject(result, "executionRequests");
            (void)reqs; /* empty for now */
        }
    }

    execution_payload_free(&payload);
    return result;
}

/* getPayload version wrappers */
cJSON *engine_getPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V1, err_code, err_msg);
}

cJSON *engine_getPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V2, err_code, err_msg);
}

cJSON *engine_getPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V3, err_code, err_msg);
}

cJSON *engine_getPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg) {
    (void)id;
    return get_payload_common(params, ctx, ENGINE_V4, err_code, err_msg);
}

/* =========================================================================
 * exchangeCapabilities
 * ========================================================================= */

cJSON *engine_exchangeCapabilities(const cJSON *params, const cJSON *id,
                                    void *ctx_ptr, int *err_code,
                                    const char **err_msg) {
    (void)params; (void)id; (void)ctx_ptr;
    (void)err_code; (void)err_msg;

    cJSON *result = cJSON_CreateArray();
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_newPayloadV4"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_forkchoiceUpdatedV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV1"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV2"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV3"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_getPayloadV4"));
    cJSON_AddItemToArray(result, cJSON_CreateString("engine_exchangeCapabilities"));
    return result;
}

/* =========================================================================
 * Registration — All Methods
 * ========================================================================= */

void engine_register_handlers(engine_rpc_t *rpc) {
    engine_rpc_register(rpc, "engine_newPayloadV1", engine_newPayloadV1);
    engine_rpc_register(rpc, "engine_newPayloadV2", engine_newPayloadV2);
    engine_rpc_register(rpc, "engine_newPayloadV3", engine_newPayloadV3);
    engine_rpc_register(rpc, "engine_newPayloadV4", engine_newPayloadV4);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV1", engine_forkchoiceUpdatedV1);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV2", engine_forkchoiceUpdatedV2);
    engine_rpc_register(rpc, "engine_forkchoiceUpdatedV3", engine_forkchoiceUpdatedV3);
    engine_rpc_register(rpc, "engine_getPayloadV1", engine_getPayloadV1);
    engine_rpc_register(rpc, "engine_getPayloadV2", engine_getPayloadV2);
    engine_rpc_register(rpc, "engine_getPayloadV3", engine_getPayloadV3);
    engine_rpc_register(rpc, "engine_getPayloadV4", engine_getPayloadV4);
    engine_rpc_register(rpc, "engine_exchangeCapabilities", engine_exchangeCapabilities);
}
