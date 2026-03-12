/*
 * Engine JWT — HS256 authentication for the Engine API.
 *
 * Uses hmac_sha256() from net/src/hkdf.c for HMAC computation.
 * Base64url encode/decode implemented inline (no padding, URL-safe alphabet).
 */

#include "engine_jwt.h"
#include "hkdf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

/* =========================================================================
 * Base64url (RFC 4648 §5, no padding)
 * ========================================================================= */

static const char B64URL_TABLE[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int base64url_encode(char *out, size_t out_size,
                     const uint8_t *data, size_t data_len) {
    size_t out_len = ((data_len + 2) / 3) * 4;
    /* Strip padding characters from length estimate */
    size_t padding = (3 - data_len % 3) % 3;
    out_len -= padding;

    if (out_len + 1 > out_size) return -1;

    size_t i = 0, j = 0;
    while (i + 2 < data_len) {
        uint32_t v = ((uint32_t)data[i] << 16) |
                     ((uint32_t)data[i+1] << 8) |
                     (uint32_t)data[i+2];
        out[j++] = B64URL_TABLE[(v >> 18) & 0x3F];
        out[j++] = B64URL_TABLE[(v >> 12) & 0x3F];
        out[j++] = B64URL_TABLE[(v >> 6)  & 0x3F];
        out[j++] = B64URL_TABLE[v & 0x3F];
        i += 3;
    }

    if (data_len - i == 2) {
        uint32_t v = ((uint32_t)data[i] << 16) | ((uint32_t)data[i+1] << 8);
        out[j++] = B64URL_TABLE[(v >> 18) & 0x3F];
        out[j++] = B64URL_TABLE[(v >> 12) & 0x3F];
        out[j++] = B64URL_TABLE[(v >> 6)  & 0x3F];
    } else if (data_len - i == 1) {
        uint32_t v = (uint32_t)data[i] << 16;
        out[j++] = B64URL_TABLE[(v >> 18) & 0x3F];
        out[j++] = B64URL_TABLE[(v >> 12) & 0x3F];
    }

    out[j] = '\0';
    return (int)j;
}

static int b64url_char_val(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '-') return 62;
    if (c == '_') return 63;
    return -1;
}

int base64url_decode(uint8_t *out, size_t out_size,
                     const char *str, size_t str_len) {
    /* Strip any trailing padding */
    while (str_len > 0 && str[str_len - 1] == '=')
        str_len--;

    /* Calculate output length */
    size_t out_len = (str_len * 3) / 4;
    if (out_len > out_size) return -1;

    size_t i = 0, j = 0;
    while (i + 3 < str_len) {
        int a = b64url_char_val(str[i]);
        int b = b64url_char_val(str[i+1]);
        int c = b64url_char_val(str[i+2]);
        int d = b64url_char_val(str[i+3]);
        if (a < 0 || b < 0 || c < 0 || d < 0) return -1;

        uint32_t v = ((uint32_t)a << 18) | ((uint32_t)b << 12) |
                     ((uint32_t)c << 6) | (uint32_t)d;
        out[j++] = (v >> 16) & 0xFF;
        out[j++] = (v >> 8)  & 0xFF;
        out[j++] = v & 0xFF;
        i += 4;
    }

    size_t rem = str_len - i;
    if (rem == 3) {
        int a = b64url_char_val(str[i]);
        int b = b64url_char_val(str[i+1]);
        int c = b64url_char_val(str[i+2]);
        if (a < 0 || b < 0 || c < 0) return -1;
        uint32_t v = ((uint32_t)a << 18) | ((uint32_t)b << 12) | ((uint32_t)c << 6);
        out[j++] = (v >> 16) & 0xFF;
        out[j++] = (v >> 8)  & 0xFF;
    } else if (rem == 2) {
        int a = b64url_char_val(str[i]);
        int b = b64url_char_val(str[i+1]);
        if (a < 0 || b < 0) return -1;
        uint32_t v = ((uint32_t)a << 18) | ((uint32_t)b << 12);
        out[j++] = (v >> 16) & 0xFF;
    } else if (rem == 1) {
        return -1; /* Invalid base64url length */
    }

    return (int)j;
}

/* =========================================================================
 * Hex Parsing
 * ========================================================================= */

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static bool parse_hex32(const char *hex, size_t len, uint8_t out[32]) {
    /* Skip optional 0x prefix */
    if (len >= 2 && hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X')) {
        hex += 2;
        len -= 2;
    }
    if (len != 64) return false;

    for (int i = 0; i < 32; i++) {
        int hi = hex_val(hex[i * 2]);
        int lo = hex_val(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0) return false;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

/* =========================================================================
 * Lifecycle
 * ========================================================================= */

engine_jwt_t *engine_jwt_load(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;

    char line[256];
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        return NULL;
    }
    fclose(f);

    /* Strip trailing whitespace */
    size_t len = strlen(line);
    while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r' ||
                        line[len-1] == ' '  || line[len-1] == '\t'))
        line[--len] = '\0';

    uint8_t secret[32];
    if (!parse_hex32(line, len, secret))
        return NULL;

    return engine_jwt_create(secret);
}

engine_jwt_t *engine_jwt_create(const uint8_t secret[32]) {
    engine_jwt_t *jwt = calloc(1, sizeof(*jwt));
    if (!jwt) return NULL;
    memcpy(jwt->secret, secret, 32);
    return jwt;
}

void engine_jwt_destroy(engine_jwt_t *jwt) {
    if (!jwt) return;
    /* Wipe secret before freeing */
    memset(jwt->secret, 0, 32);
    free(jwt);
}

/* =========================================================================
 * JWT Validation
 * ========================================================================= */

/* Find next '.' in token, return pointer to it or NULL */
static const char *find_dot(const char *s, const char *end) {
    while (s < end) {
        if (*s == '.') return s;
        s++;
    }
    return NULL;
}

/*
 * C99-portable JSON key finder.
 * Scans for "key" at the top level, skipping over nested strings.
 * Returns pointer to the character after the colon, or NULL.
 */
static const char *json_find_key(const char *json, size_t json_len,
                                  const char *key) {
    const char *end = json + json_len;
    size_t key_len = strlen(key);
    const char *p = json;

    while (p < end) {
        if (*p == '"') {
            /* Start of a string — check if it matches our key */
            p++;
            const char *str_start = p;
            /* Scan to end of string, handling escapes */
            while (p < end && *p != '"') {
                if (*p == '\\' && p + 1 < end) p++; /* skip escaped char */
                p++;
            }
            size_t str_len = (size_t)(p - str_start);
            if (p < end) p++; /* skip closing quote */

            /* Check if this string matches the key and is followed by ':' */
            if (str_len == key_len && memcmp(str_start, key, key_len) == 0) {
                /* Skip whitespace after closing quote */
                while (p < end && (*p == ' ' || *p == '\t')) p++;
                if (p < end && *p == ':') {
                    p++;
                    while (p < end && (*p == ' ' || *p == '\t')) p++;
                    return p;
                }
            }
        } else {
            p++;
        }
    }
    return NULL;
}

/* Extract integer value for "key" from JSON. */
static bool json_get_int64(const char *json, size_t json_len,
                           const char *key, int64_t *out) {
    const char *val = json_find_key(json, json_len, key);
    if (!val) return false;
    char *endp;
    *out = strtoll(val, &endp, 10);
    return endp != val;
}

/* Check if "key" has string value "expected" in JSON. */
static bool json_match_string(const char *json, size_t json_len,
                              const char *key, const char *expected) {
    const char *val = json_find_key(json, json_len, key);
    if (!val || *val != '"') return false;
    val++; /* skip opening quote */
    size_t exp_len = strlen(expected);
    const char *end = json + json_len;
    if (val + exp_len >= end) return false;
    return memcmp(val, expected, exp_len) == 0 && val[exp_len] == '"';
}

bool engine_jwt_validate(const engine_jwt_t *jwt, const char *token,
                         const char **err_msg) {
    if (!jwt || !token) {
        if (err_msg) *err_msg = "null argument";
        return false;
    }

    const char *end = token + strlen(token);

    /* Split: header.payload.signature */
    const char *dot1 = find_dot(token, end);
    if (!dot1) {
        if (err_msg) *err_msg = "missing first dot";
        return false;
    }
    const char *dot2 = find_dot(dot1 + 1, end);
    if (!dot2) {
        if (err_msg) *err_msg = "missing second dot";
        return false;
    }
    /* No more dots allowed */
    if (find_dot(dot2 + 1, end)) {
        if (err_msg) *err_msg = "extra dots in token";
        return false;
    }

    const char *hdr_b64 = token;
    size_t hdr_b64_len = (size_t)(dot1 - token);
    const char *pay_b64 = dot1 + 1;
    size_t pay_b64_len = (size_t)(dot2 - dot1 - 1);
    const char *sig_b64 = dot2 + 1;
    size_t sig_b64_len = (size_t)(end - dot2 - 1);

    /* 1. Verify signature: HMAC-SHA256(secret, header_b64.payload_b64) */
    size_t signing_input_len = hdr_b64_len + 1 + pay_b64_len;
    uint8_t expected_sig[32];
    hmac_sha256(expected_sig, jwt->secret, 32,
                (const uint8_t *)token, signing_input_len);

    uint8_t actual_sig[32];
    int sig_len = base64url_decode(actual_sig, sizeof(actual_sig),
                                   sig_b64, sig_b64_len);
    if (sig_len != 32) {
        if (err_msg) *err_msg = "invalid signature encoding";
        return false;
    }

    /* Constant-time comparison */
    uint8_t diff = 0;
    for (int i = 0; i < 32; i++)
        diff |= expected_sig[i] ^ actual_sig[i];
    if (diff != 0) {
        if (err_msg) *err_msg = "signature mismatch";
        return false;
    }

    /* 2. Decode header and check alg */
    uint8_t hdr_json[512];
    int hdr_len = base64url_decode(hdr_json, sizeof(hdr_json) - 1,
                                   hdr_b64, hdr_b64_len);
    if (hdr_len < 0) {
        if (err_msg) *err_msg = "invalid header encoding";
        return false;
    }
    hdr_json[hdr_len] = '\0';

    if (!json_match_string((const char *)hdr_json, (size_t)hdr_len,
                           "alg", "HS256")) {
        if (err_msg) *err_msg = "unsupported algorithm (expected HS256)";
        return false;
    }

    /* 3. Decode payload and check iat */
    uint8_t pay_json[1024];
    int pay_len = base64url_decode(pay_json, sizeof(pay_json) - 1,
                                   pay_b64, pay_b64_len);
    if (pay_len < 0) {
        if (err_msg) *err_msg = "invalid payload encoding";
        return false;
    }
    pay_json[pay_len] = '\0';

    int64_t iat;
    if (!json_get_int64((const char *)pay_json, (size_t)pay_len, "iat", &iat)) {
        if (err_msg) *err_msg = "missing iat claim";
        return false;
    }

    time_t now = time(NULL);
    int64_t delta = (int64_t)now - iat;
    if (delta < 0) delta = -delta;
    if (delta > 60) {
        if (err_msg) *err_msg = "iat too far from current time (>60s)";
        return false;
    }

    if (err_msg) *err_msg = NULL;
    return true;
}

/* =========================================================================
 * JWT Generation (for testing + CL simulation)
 * ========================================================================= */

int engine_jwt_generate(const engine_jwt_t *jwt, time_t iat,
                        char *buf, size_t buf_size) {
    if (!jwt || !buf || buf_size < 256) return 0;

    /* Fixed header: {"alg":"HS256","typ":"JWT"} */
    static const char header_json[] = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    /* Payload: {"iat":<timestamp>} */
    char payload_json[64];
    int plen = snprintf(payload_json, sizeof(payload_json),
                        "{\"iat\":%lld}", (long long)iat);
    if (plen < 0) return 0;

    /* Base64url encode header */
    char hdr_b64[64];
    int hdr_len = base64url_encode(hdr_b64, sizeof(hdr_b64),
                                   (const uint8_t *)header_json,
                                   strlen(header_json));
    if (hdr_len < 0) return 0;

    /* Base64url encode payload */
    char pay_b64[128];
    int pay_len = base64url_encode(pay_b64, sizeof(pay_b64),
                                   (const uint8_t *)payload_json,
                                   (size_t)plen);
    if (pay_len < 0) return 0;

    /* Signing input: header_b64.payload_b64 */
    char signing_input[256];
    int si_len = snprintf(signing_input, sizeof(signing_input),
                          "%s.%s", hdr_b64, pay_b64);
    if (si_len < 0) return 0;

    /* HMAC-SHA256 signature */
    uint8_t sig[32];
    hmac_sha256(sig, jwt->secret, 32,
                (const uint8_t *)signing_input, (size_t)si_len);

    /* Base64url encode signature */
    char sig_b64[64];
    int sig_len = base64url_encode(sig_b64, sizeof(sig_b64), sig, 32);
    if (sig_len < 0) return 0;

    /* Assemble: header.payload.signature */
    int total = snprintf(buf, buf_size, "%s.%s.%s", hdr_b64, pay_b64, sig_b64);
    if (total < 0 || (size_t)total >= buf_size) return 0;

    return total;
}
