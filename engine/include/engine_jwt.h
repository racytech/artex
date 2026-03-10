#ifndef ENGINE_JWT_H
#define ENGINE_JWT_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Engine JWT — HS256 authentication for the Engine API.
 *
 * The consensus client authenticates each HTTP request with a JWT
 * (JSON Web Token) using HMAC-SHA256. The shared secret is a 32-byte
 * key loaded from a hex-encoded file.
 *
 * Validation rules (execution-apis spec):
 *   - Algorithm must be HS256
 *   - Signature must verify against the shared secret
 *   - |now - iat| <= 60 seconds
 */

typedef struct {
    uint8_t secret[32];
} engine_jwt_t;

/** Load JWT secret from a hex-encoded file (64 hex chars, optional 0x prefix). */
engine_jwt_t *engine_jwt_load(const char *path);

/** Create JWT handle from raw 32-byte secret. */
engine_jwt_t *engine_jwt_create(const uint8_t secret[32]);

/** Free JWT handle. */
void engine_jwt_destroy(engine_jwt_t *jwt);

/**
 * Validate a JWT token string.
 *
 * Checks:
 *   1. Format: header.payload.signature (3 dot-separated base64url parts)
 *   2. Header: {"alg":"HS256","typ":"JWT"}
 *   3. Signature: HMAC-SHA256(secret, header.payload) matches
 *   4. Payload: |now - iat| <= 60 seconds
 *
 * Returns true if valid. On failure, sets *err_msg (static string, do not free).
 */
bool engine_jwt_validate(const engine_jwt_t *jwt, const char *token,
                         const char **err_msg);

/**
 * Generate a JWT token string for the given iat (issued-at) time.
 * Writes to buf (must be >= 256 bytes). Returns length, or 0 on failure.
 */
int engine_jwt_generate(const engine_jwt_t *jwt, time_t iat,
                        char *buf, size_t buf_size);

/* Base64url utilities (no padding) */
int base64url_encode(char *out, size_t out_size,
                     const uint8_t *data, size_t data_len);
int base64url_decode(uint8_t *out, size_t out_size,
                     const char *str, size_t str_len);

#ifdef __cplusplus
}
#endif

#endif /* ENGINE_JWT_H */
