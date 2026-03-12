#ifndef ENGINE_HANDLERS_H
#define ENGINE_HANDLERS_H

#include "engine_rpc.h"
#include "engine_store.h"
#include "engine_types.h"
#include <cjson/cJSON.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine Handlers — Implementation of Engine API methods.
 *
 * All versions from Paris (V1) through Prague (V4):
 *   engine_newPayloadV1..V4
 *   engine_forkchoiceUpdatedV1..V3
 *   engine_getPayloadV1..V4
 *   engine_exchangeCapabilities
 *
 * Each handler is an engine_rpc_handler_fn compatible function.
 * The ctx pointer is an engine_handler_ctx_t.
 */

/* Forward declarations for EVM/executor types */
struct evm;
struct evm_state;

typedef struct {
    engine_store_t *store;
    struct evm     *evm;        /* for block execution */
    struct evm_state *state;    /* EVM state handle */
} engine_handler_ctx_t;

/** Create handler context. Does NOT take ownership of store/evm/state. */
engine_handler_ctx_t *engine_handler_ctx_create(engine_store_t *store,
                                                 struct evm *evm,
                                                 struct evm_state *state);

/** Destroy handler context. */
void engine_handler_ctx_destroy(engine_handler_ctx_t *ctx);

/** Register all Engine API handlers (V1-V4) on the given RPC dispatcher. */
void engine_register_handlers(engine_rpc_t *rpc);

/* =========================================================================
 * newPayload — V1 (Paris), V2 (Shanghai), V3 (Cancun), V4 (Prague)
 * ========================================================================= */

cJSON *engine_newPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_newPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_newPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_newPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);

/* =========================================================================
 * forkchoiceUpdated — V1 (Paris), V2 (Shanghai), V3 (Cancun+Prague)
 * ========================================================================= */

cJSON *engine_forkchoiceUpdatedV1(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg);
cJSON *engine_forkchoiceUpdatedV2(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg);
cJSON *engine_forkchoiceUpdatedV3(const cJSON *params, const cJSON *id,
                                   void *ctx, int *err_code, const char **err_msg);

/* =========================================================================
 * getPayload — V1 (Paris), V2 (Shanghai), V3 (Cancun), V4 (Prague)
 * ========================================================================= */

cJSON *engine_getPayloadV1(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_getPayloadV2(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_getPayloadV3(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);
cJSON *engine_getPayloadV4(const cJSON *params, const cJSON *id,
                            void *ctx, int *err_code, const char **err_msg);

/* =========================================================================
 * exchangeCapabilities
 * ========================================================================= */

cJSON *engine_exchangeCapabilities(const cJSON *params, const cJSON *id,
                                    void *ctx, int *err_code, const char **err_msg);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_HANDLERS_H */
