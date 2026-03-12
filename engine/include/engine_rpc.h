#ifndef ENGINE_RPC_H
#define ENGINE_RPC_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <cjson/cJSON.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine RPC — JSON-RPC 2.0 dispatcher for Engine API.
 *
 * Parses incoming JSON-RPC requests, dispatches to registered handlers,
 * and formats JSON-RPC responses (result or error).
 *
 * Standard JSON-RPC error codes:
 *   -32700  Parse error
 *   -32600  Invalid request
 *   -32601  Method not found
 *   -32602  Invalid params
 *
 * Engine-specific error codes:
 *   -38001  Unknown payload
 *   -38002  Invalid forkchoice state
 *   -38003  Too large request
 *   -38004  Unsupported fork
 *   -38005  Request too large
 */

#define RPC_ERR_PARSE_ERROR     -32700
#define RPC_ERR_INVALID_REQUEST -32600
#define RPC_ERR_METHOD_NOT_FOUND -32601
#define RPC_ERR_INVALID_PARAMS  -32602

#define ENGINE_ERR_UNKNOWN_PAYLOAD   -38001
#define ENGINE_ERR_INVALID_FORKCHOICE -38002
#define ENGINE_ERR_TOO_LARGE         -38003
#define ENGINE_ERR_UNSUPPORTED_FORK  -38004

/**
 * RPC method handler.
 *
 * @param params  JSON params array (may be NULL)
 * @param id      JSON-RPC request ID (number or string)
 * @param ctx     User context pointer
 * @return        cJSON result object (caller takes ownership), or NULL for error
 *                On error, set *err_code and *err_msg
 */
typedef cJSON *(*engine_rpc_handler_fn)(const cJSON *params,
                                         const cJSON *id,
                                         void *ctx,
                                         int *err_code,
                                         const char **err_msg);

#define ENGINE_RPC_MAX_METHODS 16

typedef struct {
    const char            *name;
    engine_rpc_handler_fn  handler;
} engine_rpc_method_t;

typedef struct {
    engine_rpc_method_t methods[ENGINE_RPC_MAX_METHODS];
    int                 method_count;
    void               *ctx;    /* passed to handlers */
} engine_rpc_t;

/** Create a new RPC dispatcher. ctx is passed to all handlers. */
engine_rpc_t *engine_rpc_create(void *ctx);

/** Register a method handler. Returns false if table full. */
bool engine_rpc_register(engine_rpc_t *rpc, const char *method,
                         engine_rpc_handler_fn handler);

/**
 * Dispatch a raw JSON-RPC request string.
 *
 * Parses the JSON, validates JSON-RPC 2.0 format, dispatches to handler.
 * Returns a newly allocated JSON-RPC response string (caller must free).
 * Also sets *resp_len to the response length.
 */
char *engine_rpc_dispatch(engine_rpc_t *rpc,
                          const char *request, size_t request_len,
                          size_t *resp_len);

/** Destroy the RPC dispatcher. */
void engine_rpc_destroy(engine_rpc_t *rpc);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_RPC_H */
