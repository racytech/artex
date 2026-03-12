/*
 * Engine HTTP — Minimal TCP HTTP server for Engine API.
 *
 * POST-only, single-threaded, one-request-at-a-time.
 * Parses Content-Length, Authorization: Bearer, reads body.
 */

#include "engine_http.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAX_HEADER_SIZE  8192
#define MAX_BODY_SIZE    (4 * 1024 * 1024)  /* 4 MB max body */

struct engine_http {
    int                     listen_fd;
    uint16_t                port;
    volatile int            stop_flag;
    engine_http_handler_fn  handler;
    void                   *userdata;

    /* Worker thread for non-blocking request dispatch */
    pthread_t       worker;
    pthread_mutex_t mtx;
    pthread_cond_t  cond_req;       /* "request available" */
    pthread_cond_t  cond_done;      /* "response ready" */
    char           *req_body;
    size_t          req_len;
    const char     *req_token;
    size_t          req_token_len;
    char           *resp_body;
    size_t          resp_len;
    int             resp_status;
    bool            has_request;
    bool            has_response;
    bool            worker_running;
};

/* Forward declarations */
static void worker_stop(engine_http_t *http);
static bool dispatch_to_worker(engine_http_t *http,
                                char *body, size_t body_len,
                                const char *token, size_t token_len,
                                int *out_status, const char **out_body,
                                size_t *out_body_len);

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

engine_http_t *engine_http_create(const char *host, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return NULL;

    /* Allow port reuse */
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (!host || strcmp(host, "0.0.0.0") == 0) {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
            close(fd);
            return NULL;
        }
    }

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return NULL;
    }

    if (listen(fd, 8) < 0) {
        close(fd);
        return NULL;
    }

    /* Get actual port (useful when port=0) */
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);

    engine_http_t *http = calloc(1, sizeof(*http));
    if (!http) { close(fd); return NULL; }

    http->listen_fd = fd;
    http->port = ntohs(bound.sin_port);
    http->stop_flag = 0;
    http->handler = NULL;
    http->userdata = NULL;

    return http;
}

void engine_http_set_handler(engine_http_t *http,
                             engine_http_handler_fn handler,
                             void *userdata) {
    if (!http) return;
    http->handler = handler;
    http->userdata = userdata;
}

uint16_t engine_http_port(const engine_http_t *http) {
    return http ? http->port : 0;
}

void engine_http_stop(engine_http_t *http) {
    if (http) http->stop_flag = 1;
}

void engine_http_destroy(engine_http_t *http) {
    if (!http) return;
    worker_stop(http);
    if (http->listen_fd >= 0) close(http->listen_fd);
    free(http);
}

/* =========================================================================
 * HTTP Parsing Helpers
 * ========================================================================= */

/* Read exactly n bytes from fd. Returns false on error/short read. */
static bool read_exact(int fd, void *buf, size_t n) {
    size_t total = 0;
    while (total < n) {
        ssize_t r = read(fd, (char *)buf + total, n - total);
        if (r <= 0) return false;
        total += (size_t)r;
    }
    return true;
}

/* Case-insensitive prefix match */
static bool starts_with_ci(const char *line, const char *prefix) {
    while (*prefix) {
        char a = *line, b = *prefix;
        if (a >= 'A' && a <= 'Z') a += 32;
        if (b >= 'A' && b <= 'Z') b += 32;
        if (a != b) return false;
        line++;
        prefix++;
    }
    return true;
}

/* Write all bytes to fd, retrying on short writes. Returns false on error. */
static bool write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t w = write(fd, p, remaining);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false; /* EPIPE, ECONNRESET, etc. */
        }
        p += w;
        remaining -= (size_t)w;
    }
    return true;
}

/* Send HTTP response */
static void send_response(int fd, int status, const char *body, size_t body_len) {
    const char *reason;
    switch (status) {
        case 200: reason = "OK"; break;
        case 400: reason = "Bad Request"; break;
        case 401: reason = "Unauthorized"; break;
        case 403: reason = "Forbidden"; break;
        case 405: reason = "Method Not Allowed"; break;
        case 500: reason = "Internal Server Error"; break;
        case 503: reason = "Service Unavailable"; break;
        default:  reason = "Unknown"; break;
    }

    char header[512];
    int hlen = snprintf(header, sizeof(header),
        "HTTP/1.1 %d %s\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n",
        status, reason, body_len);

    if (!write_all(fd, header, (size_t)hlen)) return;
    if (body && body_len > 0)
        write_all(fd, body, body_len);
}

/* =========================================================================
 * Request Handling
 * ========================================================================= */

static void handle_connection(engine_http_t *http, int client_fd) {
    /* Read headers (up to MAX_HEADER_SIZE) */
    char header_buf[MAX_HEADER_SIZE];
    size_t header_len = 0;
    bool found_end = false;

    while (header_len < sizeof(header_buf) - 1) {
        ssize_t r = read(client_fd, header_buf + header_len, 1);
        if (r <= 0) goto done;
        header_len++;

        /* Check for \r\n\r\n */
        if (header_len >= 4 &&
            header_buf[header_len - 4] == '\r' &&
            header_buf[header_len - 3] == '\n' &&
            header_buf[header_len - 2] == '\r' &&
            header_buf[header_len - 1] == '\n') {
            found_end = true;
            break;
        }
    }

    if (!found_end) {
        send_response(client_fd, 400, "{\"error\":\"headers too large\"}", 28);
        goto done;
    }
    header_buf[header_len] = '\0';

    /* Parse request line: METHOD PATH HTTP/version */
    char method[16], path[256];
    if (sscanf(header_buf, "%15s %255s", method, path) != 2) {
        send_response(client_fd, 400, "{\"error\":\"bad request line\"}", 27);
        goto done;
    }

    /* Only POST allowed */
    if (strcmp(method, "POST") != 0) {
        send_response(client_fd, 405, "{\"error\":\"method not allowed\"}", 30);
        goto done;
    }

    /* Parse headers: Content-Length and Authorization */
    size_t content_length = 0;
    char bearer_token[512];
    bearer_token[0] = '\0';
    size_t token_len = 0;

    const char *line = strchr(header_buf, '\n');
    while (line && *line) {
        line++; /* skip \n */
        if (*line == '\r' || *line == '\n') break;

        if (starts_with_ci(line, "content-length:")) {
            const char *val = line + 15;
            while (*val == ' ') val++;
            content_length = (size_t)strtoul(val, NULL, 10);
        } else if (starts_with_ci(line, "authorization:")) {
            const char *val = line + 14;
            while (*val == ' ') val++;
            if (starts_with_ci(val, "bearer ")) {
                val += 7;
                while (*val == ' ') val++;
                const char *end = val;
                while (*end && *end != '\r' && *end != '\n') end++;
                token_len = (size_t)(end - val);
                if (token_len < sizeof(bearer_token)) {
                    memcpy(bearer_token, val, token_len);
                    bearer_token[token_len] = '\0';
                }
            }
        }

        line = strchr(line, '\n');
    }

    /* Read body */
    if (content_length > MAX_BODY_SIZE) {
        send_response(client_fd, 400, "{\"error\":\"body too large\"}", 25);
        goto done;
    }

    char *body = NULL;
    if (content_length > 0) {
        body = malloc(content_length + 1);
        if (!body) {
            send_response(client_fd, 500, "{\"error\":\"out of memory\"}", 24);
            goto done;
        }
        if (!read_exact(client_fd, body, content_length)) {
            free(body);
            goto done;
        }
        body[content_length] = '\0';
    }

    /* Dispatch to worker thread */
    if (http->handler && http->worker_running) {
        int status;
        const char *resp_body;
        size_t resp_len;

        if (dispatch_to_worker(http, body, content_length,
                                token_len > 0 ? bearer_token : NULL, token_len,
                                &status, &resp_body, &resp_len)) {
            send_response(client_fd, status, resp_body, resp_len);
        } else {
            /* Worker timed out */
            send_response(client_fd, 503,
                          "{\"error\":\"execution timeout\"}", 28);
        }
    } else if (http->handler) {
        /* Fallback: direct call if worker not running */
        engine_http_request_t req = {
            .body = body ? body : "",
            .body_len = content_length,
            .bearer_token = token_len > 0 ? bearer_token : NULL,
            .token_len = token_len,
        };
        engine_http_response_t resp = {
            .status_code = 200,
            .body = NULL,
            .body_len = 0,
        };

        http->handler(&req, &resp, http->userdata);
        send_response(client_fd, resp.status_code,
                      resp.body, resp.body_len);
    } else {
        send_response(client_fd, 500,
                      "{\"error\":\"no handler\"}", 21);
    }

    free(body);
done:
    close(client_fd);
}

/* =========================================================================
 * Worker Thread
 * ========================================================================= */

static void *worker_thread(void *arg) {
    engine_http_t *http = (engine_http_t *)arg;

    pthread_mutex_lock(&http->mtx);
    while (!http->stop_flag) {
        /* Wait for a request */
        while (!http->has_request && !http->stop_flag)
            pthread_cond_wait(&http->cond_req, &http->mtx);

        if (http->stop_flag) break;

        /* Build request/response structs and call handler */
        engine_http_request_t req = {
            .body       = http->req_body ? http->req_body : "",
            .body_len   = http->req_len,
            .bearer_token = http->req_token,
            .token_len  = http->req_token_len,
        };
        engine_http_response_t resp = {
            .status_code = 200,
            .body = NULL,
            .body_len = 0,
        };

        /* Release lock during handler execution so stop can be signalled */
        pthread_mutex_unlock(&http->mtx);

        if (http->handler)
            http->handler(&req, &resp, http->userdata);

        pthread_mutex_lock(&http->mtx);

        /* Post response */
        http->resp_body   = (char *)resp.body;
        http->resp_len    = resp.body_len;
        http->resp_status = resp.status_code;
        http->has_request  = false;
        http->has_response = true;
        pthread_cond_signal(&http->cond_done);
    }
    pthread_mutex_unlock(&http->mtx);
    return NULL;
}

static bool worker_start(engine_http_t *http) {
    pthread_mutex_init(&http->mtx, NULL);
    pthread_cond_init(&http->cond_req, NULL);
    pthread_cond_init(&http->cond_done, NULL);
    http->has_request  = false;
    http->has_response = false;

    if (pthread_create(&http->worker, NULL, worker_thread, http) != 0)
        return false;
    http->worker_running = true;
    return true;
}

static void worker_stop(engine_http_t *http) {
    if (!http->worker_running) return;

    pthread_mutex_lock(&http->mtx);
    http->stop_flag = 1;
    pthread_cond_signal(&http->cond_req);
    pthread_mutex_unlock(&http->mtx);

    pthread_join(http->worker, NULL);
    http->worker_running = false;

    pthread_mutex_destroy(&http->mtx);
    pthread_cond_destroy(&http->cond_req);
    pthread_cond_destroy(&http->cond_done);
}

/* Dispatch request to worker thread with timeout.
 * Returns true if worker responded in time, false on timeout/error. */
static bool dispatch_to_worker(engine_http_t *http,
                                char *body, size_t body_len,
                                const char *token, size_t token_len,
                                int *out_status, const char **out_body,
                                size_t *out_body_len) {
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 30; /* 30-second timeout */

    pthread_mutex_lock(&http->mtx);

    http->req_body      = body;
    http->req_len       = body_len;
    http->req_token     = token;
    http->req_token_len = token_len;
    http->has_request   = true;
    http->has_response  = false;
    pthread_cond_signal(&http->cond_req);

    /* Wait for response or timeout */
    while (!http->has_response && !http->stop_flag) {
        int rc = pthread_cond_timedwait(&http->cond_done, &http->mtx, &deadline);
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&http->mtx);
            return false;
        }
    }

    *out_status   = http->resp_status;
    *out_body     = http->resp_body;
    *out_body_len = http->resp_len;

    pthread_mutex_unlock(&http->mtx);
    return true;
}

/* =========================================================================
 * Accept Loop
 * ========================================================================= */

int engine_http_run(engine_http_t *http) {
    if (!http) return -1;

    /* Ignore SIGPIPE so broken client connections don't kill us */
    signal(SIGPIPE, SIG_IGN);

    /* Start worker thread */
    if (!worker_start(http)) {
        fprintf(stderr, "engine_http: failed to start worker thread\n");
        return -1;
    }

    while (!http->stop_flag) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(http->listen_fd,
                               (struct sockaddr *)&client_addr,
                               &client_len);
        if (client_fd < 0) {
            if (http->stop_flag) break;
            if (errno == EINTR) continue;
            worker_stop(http);
            return -1;
        }

        /* Set socket timeouts to prevent zombie connections */
        struct timeval tv = { .tv_sec = 30 };
        setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        handle_connection(http, client_fd);
    }

    worker_stop(http);
    return 0;
}
