/*
 * Test: Engine JWT authentication (HS256)
 *
 * Tests:
 *   1. Load secret from hex file (with and without 0x prefix)
 *   2. Generate + validate round-trip
 *   3. Reject tampered signature
 *   4. Reject expired iat (>60s)
 *   5. Accept iat within 60s window
 *   6. Reject malformed tokens
 *   7. Base64url encode/decode round-trip
 *   8. Reject wrong secret
 */

#include "engine_jwt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%d] %-50s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

/* Known 32-byte secret (hex) */
static const char *HEX_SECRET =
    "7365637265745f6b65795f666f725f746573740000000000000000000000000a";

static const uint8_t RAW_SECRET[32] = {
    0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x5f, 0x6b,
    0x65, 0x79, 0x5f, 0x66, 0x6f, 0x72, 0x5f, 0x74,
    0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a,
};

/* Write a temp file and return its path */
static char tmp_path[256];
static const char *write_tmp_file(const char *content) {
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/test_jwt_%d.hex", getpid());
    FILE *f = fopen(tmp_path, "w");
    if (!f) return NULL;
    fputs(content, f);
    fclose(f);
    return tmp_path;
}

static void cleanup_tmp(void) {
    unlink(tmp_path);
}

/* =========================================================================
 * Tests
 * ========================================================================= */

static void test_load_hex_file(void) {
    TEST("load secret from hex file");

    const char *path = write_tmp_file(HEX_SECRET);
    engine_jwt_t *jwt = engine_jwt_load(path);
    cleanup_tmp();

    if (!jwt) { FAIL("load returned NULL"); return; }
    if (memcmp(jwt->secret, RAW_SECRET, 32) != 0) {
        engine_jwt_destroy(jwt);
        FAIL("secret mismatch");
        return;
    }
    engine_jwt_destroy(jwt);
    PASS();
}

static void test_load_hex_file_0x_prefix(void) {
    TEST("load secret with 0x prefix");

    char prefixed[128];
    snprintf(prefixed, sizeof(prefixed), "0x%s\n", HEX_SECRET);
    const char *path = write_tmp_file(prefixed);
    engine_jwt_t *jwt = engine_jwt_load(path);
    cleanup_tmp();

    if (!jwt) { FAIL("load returned NULL"); return; }
    if (memcmp(jwt->secret, RAW_SECRET, 32) != 0) {
        engine_jwt_destroy(jwt);
        FAIL("secret mismatch");
        return;
    }
    engine_jwt_destroy(jwt);
    PASS();
}

static void test_generate_validate_roundtrip(void) {
    TEST("generate + validate round-trip");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);
    if (!jwt) { FAIL("create failed"); return; }

    time_t now = time(NULL);
    char token[512];
    int len = engine_jwt_generate(jwt, now, token, sizeof(token));
    if (len <= 0) {
        engine_jwt_destroy(jwt);
        FAIL("generate failed");
        return;
    }

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt, token, &err);
    if (!valid) {
        printf("FAIL: validation failed: %s\n", err ? err : "(null)");
        engine_jwt_destroy(jwt);
        return;
    }

    engine_jwt_destroy(jwt);
    PASS();
}

static void test_reject_tampered_signature(void) {
    TEST("reject tampered signature");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);
    time_t now = time(NULL);
    char token[512];
    engine_jwt_generate(jwt, now, token, sizeof(token));

    /* Tamper with a middle character of signature (avoid last char padding bits) */
    size_t tlen = strlen(token);
    /* Find the last dot to locate the signature portion */
    char *last_dot = strrchr(token, '.');
    size_t sig_start = (size_t)(last_dot - token) + 1;
    size_t sig_mid = sig_start + (tlen - sig_start) / 2;
    token[sig_mid] = (token[sig_mid] == 'A') ? 'Z' : 'A';

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt, token, &err);
    engine_jwt_destroy(jwt);

    if (valid) { FAIL("should have rejected tampered token"); return; }
    if (!err || !strstr(err, "signature")) {
        FAIL("expected signature error");
        return;
    }
    PASS();
}

static void test_reject_expired_iat(void) {
    TEST("reject expired iat (>60s)");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);
    time_t old = time(NULL) - 120; /* 2 minutes ago */
    char token[512];
    engine_jwt_generate(jwt, old, token, sizeof(token));

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt, token, &err);
    engine_jwt_destroy(jwt);

    if (valid) { FAIL("should have rejected expired token"); return; }
    if (!err || !strstr(err, "iat")) {
        FAIL("expected iat error");
        return;
    }
    PASS();
}

static void test_reject_future_iat(void) {
    TEST("reject future iat (>60s ahead)");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);
    time_t future = time(NULL) + 120; /* 2 minutes ahead */
    char token[512];
    engine_jwt_generate(jwt, future, token, sizeof(token));

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt, token, &err);
    engine_jwt_destroy(jwt);

    if (valid) { FAIL("should have rejected future token"); return; }
    PASS();
}

static void test_accept_within_window(void) {
    TEST("accept iat within 60s window");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);

    /* 30 seconds ago — well within window */
    time_t recent = time(NULL) - 30;
    char token[512];
    engine_jwt_generate(jwt, recent, token, sizeof(token));

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt, token, &err);
    engine_jwt_destroy(jwt);

    if (!valid) {
        printf("FAIL: should have accepted: %s\n", err ? err : "(null)");
        return;
    }
    PASS();
}

static void test_reject_malformed(void) {
    TEST("reject malformed tokens");

    engine_jwt_t *jwt = engine_jwt_create(RAW_SECRET);
    const char *err = NULL;

    /* No dots */
    if (engine_jwt_validate(jwt, "nodots", &err)) {
        engine_jwt_destroy(jwt);
        FAIL("accepted no-dot token");
        return;
    }

    /* One dot */
    if (engine_jwt_validate(jwt, "one.dot", &err)) {
        engine_jwt_destroy(jwt);
        FAIL("accepted one-dot token");
        return;
    }

    /* Three dots */
    if (engine_jwt_validate(jwt, "too.many.dots.here", &err)) {
        engine_jwt_destroy(jwt);
        FAIL("accepted three-dot token");
        return;
    }

    /* Empty token */
    if (engine_jwt_validate(jwt, "", &err)) {
        engine_jwt_destroy(jwt);
        FAIL("accepted empty token");
        return;
    }

    engine_jwt_destroy(jwt);
    PASS();
}

static void test_wrong_secret(void) {
    TEST("reject token signed with wrong secret");

    engine_jwt_t *jwt1 = engine_jwt_create(RAW_SECRET);

    /* Different secret */
    uint8_t other_secret[32];
    memset(other_secret, 0xFF, 32);
    engine_jwt_t *jwt2 = engine_jwt_create(other_secret);

    time_t now = time(NULL);
    char token[512];
    engine_jwt_generate(jwt1, now, token, sizeof(token));

    const char *err = NULL;
    bool valid = engine_jwt_validate(jwt2, token, &err);

    engine_jwt_destroy(jwt1);
    engine_jwt_destroy(jwt2);

    if (valid) { FAIL("should have rejected wrong secret"); return; }
    PASS();
}

static void test_base64url_roundtrip(void) {
    TEST("base64url encode/decode round-trip");

    uint8_t data[] = {0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x80, 0x7F};
    char encoded[32];
    int elen = base64url_encode(encoded, sizeof(encoded), data, sizeof(data));
    if (elen < 0) { FAIL("encode failed"); return; }

    uint8_t decoded[16];
    int dlen = base64url_decode(decoded, sizeof(decoded), encoded, (size_t)elen);
    if (dlen != (int)sizeof(data)) {
        FAIL("decode length mismatch");
        return;
    }
    if (memcmp(decoded, data, sizeof(data)) != 0) {
        FAIL("decode data mismatch");
        return;
    }
    PASS();
}

static void test_base64url_empty(void) {
    TEST("base64url empty input");

    char encoded[8];
    int elen = base64url_encode(encoded, sizeof(encoded), NULL, 0);
    if (elen != 0) { FAIL("expected 0 length for empty encode"); return; }
    if (encoded[0] != '\0') { FAIL("expected empty string"); return; }

    PASS();
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== Engine JWT Tests ===\n\n");

    test_load_hex_file();
    test_load_hex_file_0x_prefix();
    test_generate_validate_roundtrip();
    test_reject_tampered_signature();
    test_reject_expired_iat();
    test_reject_future_iat();
    test_accept_within_window();
    test_reject_malformed();
    test_wrong_secret();
    test_base64url_roundtrip();
    test_base64url_empty();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
