/*
 * Engine — Top-level Engine API orchestrator.
 *
 * Wires HTTP → JWT → RPC → handlers.
 * The HTTP handler callback: validate JWT, then dispatch RPC.
 */

#include "engine.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

struct engine {
    engine_http_t        *http;
    engine_jwt_t         *jwt;
    engine_rpc_t         *rpc;
    engine_store_t       *store;
    engine_handler_ctx_t *handler_ctx;
};

/* =========================================================================
 * HTTP Handler: JWT verify → RPC dispatch
 * ========================================================================= */

static void engine_http_handler(const engine_http_request_t *req,
                                 engine_http_response_t *resp,
                                 void *userdata) {
    engine_t *eng = (engine_t *)userdata;

    /* JWT authentication */
    if (eng->jwt) {
        if (!req->bearer_token) {
            resp->status_code = 401;
            resp->body = "{\"error\":\"missing Authorization header\"}";
            resp->body_len = strlen(resp->body);
            return;
        }

        /* Copy token to null-terminated string */
        char token_buf[1024];
        if (req->token_len >= sizeof(token_buf)) {
            resp->status_code = 401;
            resp->body = "{\"error\":\"token too long\"}";
            resp->body_len = strlen(resp->body);
            return;
        }
        memcpy(token_buf, req->bearer_token, req->token_len);
        token_buf[req->token_len] = '\0';

        const char *err = NULL;
        if (!engine_jwt_validate(eng->jwt, token_buf, &err)) {
            resp->status_code = 403;
            resp->body = "{\"error\":\"JWT authentication failed\"}";
            resp->body_len = strlen(resp->body);
            return;
        }
    }

    /* RPC dispatch */
    if (!req->body || req->body_len == 0) {
        resp->status_code = 400;
        resp->body = "{\"error\":\"empty body\"}";
        resp->body_len = strlen(resp->body);
        return;
    }

    size_t rpc_len = 0;
    char *rpc_resp = engine_rpc_dispatch(eng->rpc,
                                          req->body, req->body_len,
                                          &rpc_len);
    if (rpc_resp) {
        resp->status_code = 200;
        resp->body = rpc_resp;  /* Note: caller doesn't free this currently */
        resp->body_len = rpc_len;
    } else {
        resp->status_code = 500;
        resp->body = "{\"error\":\"internal error\"}";
        resp->body_len = strlen(resp->body);
    }
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

engine_t *engine_create(const engine_config_t *config) {
    if (!config) return NULL;

    engine_t *eng = calloc(1, sizeof(*eng));
    if (!eng) return NULL;

    /* JWT */
    if (config->jwt_secret_path) {
        eng->jwt = engine_jwt_load(config->jwt_secret_path);
        if (!eng->jwt) {
            fprintf(stderr, "engine: failed to load JWT secret from %s\n",
                    config->jwt_secret_path);
            free(eng);
            return NULL;
        }
    }

    /* Store */
    eng->store = engine_store_create();
    if (!eng->store) goto fail;

    /* Handler context */
    eng->handler_ctx = engine_handler_ctx_create(
        eng->store,
        (struct evm *)config->evm,
        (struct evm_state *)config->evm_state);
    if (!eng->handler_ctx) goto fail;

    /* If sync engine is provided, enable live mode execution */
    if (config->sync)
        engine_handler_ctx_set_sync(eng->handler_ctx, config->sync);

    /* RPC */
    eng->rpc = engine_rpc_create(eng->handler_ctx);
    if (!eng->rpc) goto fail;
    engine_register_handlers(eng->rpc);

    /* HTTP */
    const char *host = config->host ? config->host : "127.0.0.1";
    uint16_t port = config->port ? config->port : 8551;

    eng->http = engine_http_create(host, port);
    if (!eng->http) {
        fprintf(stderr, "engine: failed to bind %s:%d\n", host, port);
        goto fail;
    }
    engine_http_set_handler(eng->http, engine_http_handler, eng);

    return eng;

fail:
    engine_destroy(eng);
    return NULL;
}

int engine_run(engine_t *eng) {
    if (!eng || !eng->http) return -1;
    printf("Engine API listening on port %d\n", engine_http_port(eng->http));
    return engine_http_run(eng->http);
}

void engine_stop(engine_t *eng) {
    if (eng && eng->http)
        engine_http_stop(eng->http);
}

uint16_t engine_port(const engine_t *eng) {
    return eng && eng->http ? engine_http_port(eng->http) : 0;
}

engine_store_t *engine_get_store(engine_t *eng) {
    return eng ? eng->store : NULL;
}

void engine_destroy(engine_t *eng) {
    if (!eng) return;
    if (eng->http) engine_http_destroy(eng->http);
    if (eng->rpc) engine_rpc_destroy(eng->rpc);
    if (eng->handler_ctx) engine_handler_ctx_destroy(eng->handler_ctx);
    if (eng->store) engine_store_destroy(eng->store);
    if (eng->jwt) engine_jwt_destroy(eng->jwt);
    free(eng);
}
