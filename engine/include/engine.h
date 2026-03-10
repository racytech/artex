#ifndef ENGINE_H
#define ENGINE_H

#include "engine_http.h"
#include "engine_jwt.h"
#include "engine_rpc.h"
#include "engine_store.h"
#include "engine_handlers.h"
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine — Top-level Engine API orchestrator.
 *
 * Wires together: HTTP server → JWT auth → JSON-RPC dispatch → handlers.
 * Manages lifecycle of all sub-components.
 *
 * Usage:
 *   engine_t *eng = engine_create(&cfg);
 *   engine_run(eng);    // blocking
 *   engine_destroy(eng);
 */

typedef struct {
    const char *host;           /* Listen address (default "127.0.0.1") */
    uint16_t    port;           /* Listen port (default 8551) */
    const char *jwt_secret_path;/* Path to hex-encoded JWT secret file */
    /* EVM/state handles (optional, for block execution) */
    void       *evm;
    void       *evm_state;
} engine_config_t;

typedef struct engine engine_t;

/** Create engine with the given configuration. */
engine_t *engine_create(const engine_config_t *config);

/** Run the engine (blocking accept loop). */
int engine_run(engine_t *eng);

/** Signal the engine to stop. */
void engine_stop(engine_t *eng);

/** Get the actual port (useful when config port=0). */
uint16_t engine_port(const engine_t *eng);

/** Get the engine store (for testing). */
engine_store_t *engine_get_store(engine_t *eng);

/** Destroy and free all resources. */
void engine_destroy(engine_t *eng);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_H */
