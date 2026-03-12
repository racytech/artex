#ifndef ENGINE_HTTP_H
#define ENGINE_HTTP_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine HTTP — Minimal TCP HTTP server for Engine API.
 *
 * Only handles POST / requests. Extracts:
 *   - Authorization: Bearer <token>
 *   - Content-Length header
 *   - Request body (JSON)
 *
 * Single-threaded, sequential request handling.
 * The consensus client sends one request at a time.
 */

/** Incoming HTTP request (parsed). */
typedef struct {
    const char *body;           /* Request body (null-terminated) */
    size_t      body_len;       /* Body length */
    const char *bearer_token;   /* Bearer token (null if missing) */
    size_t      token_len;      /* Token length */
} engine_http_request_t;

/** HTTP response to send back. */
typedef struct {
    int         status_code;    /* 200, 400, 401, 405 */
    const char *body;           /* Response body (JSON) */
    size_t      body_len;       /* Body length */
} engine_http_response_t;

/**
 * Request handler callback.
 * Called for each valid POST / request.
 * Must fill resp with status code and body.
 */
typedef void (*engine_http_handler_fn)(const engine_http_request_t *req,
                                       engine_http_response_t *resp,
                                       void *userdata);

typedef struct engine_http engine_http_t;

/** Create HTTP server bound to host:port. Returns NULL on failure. */
engine_http_t *engine_http_create(const char *host, uint16_t port);

/** Set the request handler callback. */
void engine_http_set_handler(engine_http_t *http,
                             engine_http_handler_fn handler,
                             void *userdata);

/** Run the accept loop (blocking). Returns when stop is called or on error. */
int engine_http_run(engine_http_t *http);

/** Signal the server to stop (safe to call from signal handler). */
void engine_http_stop(engine_http_t *http);

/** Get the actual port the server is listening on (useful when port=0). */
uint16_t engine_http_port(const engine_http_t *http);

/** Destroy and free all resources. */
void engine_http_destroy(engine_http_t *http);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_HTTP_H */
