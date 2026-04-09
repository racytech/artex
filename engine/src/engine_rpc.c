/*
 * Engine RPC — JSON-RPC 2.0 dispatcher for Engine API.
 *
 * Uses cJSON for JSON parsing and generation.
 */

#include "engine_rpc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

engine_rpc_t *engine_rpc_create(void *ctx) {
    engine_rpc_t *rpc = calloc(1, sizeof(*rpc));
    if (!rpc) return NULL;
    rpc->ctx = ctx;
    return rpc;
}

bool engine_rpc_register(engine_rpc_t *rpc, const char *method,
                         engine_rpc_handler_fn handler) {
    if (!rpc || !method || !handler) return false;
    if (rpc->method_count >= ENGINE_RPC_MAX_METHODS) return false;

    rpc->methods[rpc->method_count].name = method;
    rpc->methods[rpc->method_count].handler = handler;
    rpc->method_count++;
    return true;
}

void engine_rpc_destroy(engine_rpc_t *rpc) {
    free(rpc);
}

/* =========================================================================
 * Response Builders
 * ========================================================================= */

static char *build_error_response(const cJSON *id, int code, const char *msg,
                                  size_t *resp_len) {
    cJSON *resp = cJSON_CreateObject();
    cJSON_AddStringToObject(resp, "jsonrpc", "2.0");

    if (id && !cJSON_IsNull(id))
        cJSON_AddItemToObject(resp, "id", cJSON_Duplicate(id, 1));
    else
        cJSON_AddNullToObject(resp, "id");

    cJSON *err = cJSON_CreateObject();
    cJSON_AddNumberToObject(err, "code", code);
    cJSON_AddStringToObject(err, "message", msg);
    cJSON_AddItemToObject(resp, "error", err);

    char *str = cJSON_PrintUnformatted(resp);
    if (resp_len) *resp_len = str ? strlen(str) : 0;
    cJSON_Delete(resp);
    return str;
}

static char *build_result_response(const cJSON *id, cJSON *result,
                                   size_t *resp_len) {
    cJSON *resp = cJSON_CreateObject();
    cJSON_AddStringToObject(resp, "jsonrpc", "2.0");

    if (id && !cJSON_IsNull(id))
        cJSON_AddItemToObject(resp, "id", cJSON_Duplicate(id, 1));
    else
        cJSON_AddNullToObject(resp, "id");

    cJSON_AddItemToObject(resp, "result", result);

    char *str = cJSON_PrintUnformatted(resp);
    if (resp_len) *resp_len = str ? strlen(str) : 0;
    cJSON_Delete(resp);
    return str;
}

/* =========================================================================
 * Dispatch
 * ========================================================================= */

char *engine_rpc_dispatch(engine_rpc_t *rpc,
                          const char *request, size_t request_len,
                          size_t *resp_len) {
    (void)request_len;

    if (!rpc || !request) {
        return build_error_response(NULL, RPC_ERR_PARSE_ERROR,
                                    "null argument", resp_len);
    }

    /* Parse JSON */
    cJSON *root = cJSON_Parse(request);
    if (!root) {
        return build_error_response(NULL, RPC_ERR_PARSE_ERROR,
                                    "parse error", resp_len);
    }

    /* Validate JSON-RPC 2.0 */
    cJSON *jsonrpc = cJSON_GetObjectItem(root, "jsonrpc");
    if (!jsonrpc || !cJSON_IsString(jsonrpc) ||
        strcmp(jsonrpc->valuestring, "2.0") != 0) {
        char *r = build_error_response(NULL, RPC_ERR_INVALID_REQUEST,
                                       "missing jsonrpc 2.0", resp_len);
        cJSON_Delete(root);
        return r;
    }

    cJSON *id = cJSON_GetObjectItem(root, "id");
    /* id can be number, string, or null */

    cJSON *method = cJSON_GetObjectItem(root, "method");
    if (!method || !cJSON_IsString(method)) {
        char *r = build_error_response(id, RPC_ERR_INVALID_REQUEST,
                                       "missing method", resp_len);
        cJSON_Delete(root);
        return r;
    }

    cJSON *params = cJSON_GetObjectItem(root, "params");

    /* Look up handler */
    engine_rpc_handler_fn handler = NULL;
    for (int i = 0; i < rpc->method_count; i++) {
        if (strcmp(rpc->methods[i].name, method->valuestring) == 0) {
            handler = rpc->methods[i].handler;
            break;
        }
    }

    if (!handler) {
        fprintf(stderr, "ENGINE RPC: unknown method '%s'\n", method->valuestring);
        char *r = build_error_response(id, RPC_ERR_METHOD_NOT_FOUND,
                                       "method not found", resp_len);
        cJSON_Delete(root);
        return r;
    }

    /* Call handler */
    int err_code = 0;
    const char *err_msg = NULL;
    cJSON *result = handler(params, id, rpc->ctx, &err_code, &err_msg);

    char *r;
    if (result) {
        r = build_result_response(id, result, resp_len);
    } else {
        if (!err_msg) err_msg = "internal error";
        if (err_code == 0) err_code = -32603;
        r = build_error_response(id, err_code, err_msg, resp_len);
    }

    cJSON_Delete(root);
    return r;
}
