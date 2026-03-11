/*
 * Standalone Engine API server for testing.
 *
 * Usage:
 *   ./engine_server --jwt-secret /tmp/jwt.hex [--port 8551]
 *
 * Starts the Engine API HTTP server without EVM — all newPayload calls
 * return SYNCING (no parent block). Useful for testing the mock CL
 * communication stack (HTTP + JWT + JSON-RPC).
 */

#include "engine.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

static engine_t *g_engine = NULL;

static void sigint_handler(int sig) {
    (void)sig;
    if (g_engine)
        engine_stop(g_engine);
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s --jwt-secret <path> [--port <port>] [--host <addr>]\n",
            prog);
    exit(1);
}

int main(int argc, char **argv) {
    engine_config_t config = {0};
    config.host = "127.0.0.1";
    config.port = 8551;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--jwt-secret") == 0 && i + 1 < argc) {
            config.jwt_secret_path = argv[++i];
        } else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            config.port = (uint16_t)atoi(argv[++i]);
        } else if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            config.host = argv[++i];
        } else {
            usage(argv[0]);
        }
    }

    if (!config.jwt_secret_path) {
        fprintf(stderr, "Error: --jwt-secret is required\n");
        usage(argv[0]);
    }

    g_engine = engine_create(&config);
    if (!g_engine) {
        fprintf(stderr, "Failed to create engine\n");
        return 1;
    }

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    printf("Engine API server starting...\n");
    int rc = engine_run(g_engine);

    engine_destroy(g_engine);
    return rc;
}
