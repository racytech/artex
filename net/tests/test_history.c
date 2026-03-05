/*
 * Tests for Portal History Network content key codec and content ID derivation.
 *
 * Test vectors from the spec for block 12,345,678:
 *   Block body content_key:  0x00 4E 61 BC 00 00 00 00 00
 *   Block body content_id:   0x614E 3D 00..00 00  (32 bytes)
 *   Receipts content_key:    0x01 4E 61 BC 00 00 00 00 00
 *   Receipts content_id:     0x614E 3D 00..00 01  (32 bytes)
 */

#include "../include/history.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do { \
    tests_run++; \
    printf("  [%d] %-50s", tests_run, #fn); \
    fn(); \
    tests_passed++; \
    printf(" OK\n"); \
} while(0)

/* =========================================================================
 * Helpers
 * ========================================================================= */

static void print_hex(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++)
        printf("%02x", data[i]);
}

static void assert_hex_eq(const uint8_t *actual, const uint8_t *expected,
                           size_t len, const char *label) {
    if (memcmp(actual, expected, len) != 0) {
        printf(" FAIL (%s)\n  expected: ", label);
        print_hex(expected, len);
        printf("\n  actual:   ");
        print_hex(actual, len);
        printf("\n");
        fflush(stdout);
        assert(0);
    }
}

/* =========================================================================
 * Test: Content key encode (block body)
 * ========================================================================= */

static void test_content_key_encode_body(void) {
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 12345678);

    /* 12345678 = 0xBC614E → LE: 4E 61 BC 00 00 00 00 00 */
    uint8_t expected[] = { 0x00, 0x4E, 0x61, 0xBC, 0x00, 0x00, 0x00, 0x00, 0x00 };
    assert_hex_eq(key, expected, 9, "body content_key");
}

/* =========================================================================
 * Test: Content key encode (receipts)
 * ========================================================================= */

static void test_content_key_encode_receipts(void) {
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_RECEIPTS, 12345678);

    uint8_t expected[] = { 0x01, 0x4E, 0x61, 0xBC, 0x00, 0x00, 0x00, 0x00, 0x00 };
    assert_hex_eq(key, expected, 9, "receipts content_key");
}

/* =========================================================================
 * Test: Content key decode roundtrip
 * ========================================================================= */

static void test_content_key_decode_roundtrip(void) {
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 12345678);

    uint8_t selector;
    uint64_t block_number;
    assert(history_decode_content_key(key, 9, &selector, &block_number));
    assert(selector == HISTORY_SELECTOR_BODY);
    assert(block_number == 12345678);

    history_encode_content_key(key, HISTORY_SELECTOR_RECEIPTS, 9999999);
    assert(history_decode_content_key(key, 9, &selector, &block_number));
    assert(selector == HISTORY_SELECTOR_RECEIPTS);
    assert(block_number == 9999999);
}

/* =========================================================================
 * Test: Content key decode invalid
 * ========================================================================= */

static void test_content_key_decode_invalid(void) {
    uint8_t selector;
    uint64_t block_number;

    /* Wrong length */
    uint8_t short_key[] = { 0x00, 0x01 };
    assert(!history_decode_content_key(short_key, 2, &selector, &block_number));

    /* Unknown selector */
    uint8_t bad_sel[] = { 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    assert(!history_decode_content_key(bad_sel, 9, &selector, &block_number));
}

/* =========================================================================
 * Test: Content ID for block 12,345,678 — block body
 *
 * block_number = 12345678 = 0xBC614E
 * cycle_bits   = 12345678 % 65536 = 24910 = 0x614E
 * offset_bits  = 12345678 / 65536 = 188   = 0xBC = 0b10111100
 *
 * offset reversed as 240-bit:
 *   Original bits (LSB first): 0,0,1,1,1,1,0,1 (= 0xBC reversed)
 *   In 240-bit reversed: these go to positions 239,238,...,232
 *   Byte 0 of reversed (MSB): 00111101 = 0x3D
 *   Bytes 1..29: all zeros
 *
 * content_id = 0x614E 3D 000000...00 00
 * ========================================================================= */

static void test_content_id_body_12345678(void) {
    uint8_t id[32];
    history_content_id(id, 12345678, HISTORY_SELECTOR_BODY);

    uint8_t expected[32] = {0};
    expected[0] = 0x61;  /* cycle high byte */
    expected[1] = 0x4E;  /* cycle low byte */
    expected[2] = 0x3D;  /* reversed offset */
    /* bytes 3..30 = 0 */
    expected[31] = 0x00; /* content_type = body */

    assert_hex_eq(id, expected, 32, "body content_id block 12345678");
}

/* =========================================================================
 * Test: Content ID for block 12,345,678 — receipts
 * ========================================================================= */

static void test_content_id_receipts_12345678(void) {
    uint8_t id[32];
    history_content_id(id, 12345678, HISTORY_SELECTOR_RECEIPTS);

    uint8_t expected[32] = {0};
    expected[0] = 0x61;
    expected[1] = 0x4E;
    expected[2] = 0x3D;
    expected[31] = 0x01; /* content_type = receipts */

    assert_hex_eq(id, expected, 32, "receipts content_id block 12345678");
}

/* =========================================================================
 * Test: Content ID from key convenience function
 * ========================================================================= */

static void test_content_id_from_key(void) {
    uint8_t key[9];
    history_encode_content_key(key, HISTORY_SELECTOR_BODY, 12345678);

    uint8_t id[32];
    assert(history_content_id_from_key(id, key, 9));

    /* Should match the direct call */
    uint8_t expected[32];
    history_content_id(expected, 12345678, HISTORY_SELECTOR_BODY);
    assert_hex_eq(id, expected, 32, "content_id_from_key");
}

/* =========================================================================
 * Test: Content ID for block 0 (edge case)
 *
 * cycle = 0, offset = 0
 * content_id = all zeros (+ content_type in last byte)
 * ========================================================================= */

static void test_content_id_block_0(void) {
    uint8_t id[32];
    history_content_id(id, 0, HISTORY_SELECTOR_BODY);

    uint8_t expected[32] = {0};
    assert_hex_eq(id, expected, 32, "content_id block 0 body");

    history_content_id(id, 0, HISTORY_SELECTOR_RECEIPTS);
    expected[31] = 0x01;
    assert_hex_eq(id, expected, 32, "content_id block 0 receipts");
}

/* =========================================================================
 * Test: Content ID for block 65535 (last block in cycle 0)
 *
 * cycle = 65535 = 0xFFFF, offset = 0
 * content_id bytes 0-1 = 0xFF 0xFF, rest = 0
 * ========================================================================= */

static void test_content_id_block_65535(void) {
    uint8_t id[32];
    history_content_id(id, 65535, HISTORY_SELECTOR_BODY);

    uint8_t expected[32] = {0};
    expected[0] = 0xFF;
    expected[1] = 0xFF;
    assert_hex_eq(id, expected, 32, "content_id block 65535");
}

/* =========================================================================
 * Test: Content ID for block 65536 (first block in cycle 1)
 *
 * block_number = 65536 = 0x10000
 * cycle = 0, offset = 1
 *
 * offset = 1 = bit 0 set
 * Reversed as 240-bit: bit 239 set → byte 0 of reversed, bit 7 → 0x80
 * But byte 0 of reversed is byte [2] of output.
 *
 * content_id: 0x0000 80 00..00 00
 * ========================================================================= */

static void test_content_id_block_65536(void) {
    uint8_t id[32];
    history_content_id(id, 65536, HISTORY_SELECTOR_BODY);

    uint8_t expected[32] = {0};
    expected[0] = 0x00;  /* cycle = 0 */
    expected[1] = 0x00;
    expected[2] = 0x80;  /* reversed offset bit 239 */
    assert_hex_eq(id, expected, 32, "content_id block 65536");
}

/* =========================================================================
 * Test: Content ID for block 131072 (offset = 2)
 *
 * block_number = 131072 = 2 * 65536
 * cycle = 0, offset = 2 = 0b10
 *
 * offset bit 1 set → reversed bit 238 → big-endian byte 2, bit 6 → 0x40
 *
 * content_id: 0x0000 40 00..00 00
 * ========================================================================= */

static void test_content_id_block_131072(void) {
    uint8_t id[32];
    history_content_id(id, 131072, HISTORY_SELECTOR_BODY);

    uint8_t expected[32] = {0};
    expected[2] = 0x40;  /* reversed offset: bit 1 → byte 2, bit 6 */
    assert_hex_eq(id, expected, 32, "content_id block 131072 body");

    /* For receipts, last byte has content_type = 0x01 */
    history_content_id(id, 131072, HISTORY_SELECTOR_RECEIPTS);
    expected[31] = 0x01;
    assert_hex_eq(id, expected, 32, "content_id block 131072 receipts");
}

/* =========================================================================
 * Test: Body vs receipts differ only in last byte
 * ========================================================================= */

static void test_content_id_body_vs_receipts(void) {
    uint8_t id_body[32], id_receipts[32];

    history_content_id(id_body, 12345678, HISTORY_SELECTOR_BODY);
    history_content_id(id_receipts, 12345678, HISTORY_SELECTOR_RECEIPTS);

    /* First 31 bytes should be identical */
    assert(memcmp(id_body, id_receipts, 31) == 0);
    /* Last byte differs by content_type */
    assert(id_body[31] == 0x00);
    assert(id_receipts[31] == 0x01);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== test_history ===\n");

    RUN_TEST(test_content_key_encode_body);
    RUN_TEST(test_content_key_encode_receipts);
    RUN_TEST(test_content_key_decode_roundtrip);
    RUN_TEST(test_content_key_decode_invalid);
    RUN_TEST(test_content_id_body_12345678);
    RUN_TEST(test_content_id_receipts_12345678);
    RUN_TEST(test_content_id_from_key);
    RUN_TEST(test_content_id_block_0);
    RUN_TEST(test_content_id_block_65535);
    RUN_TEST(test_content_id_block_65536);
    RUN_TEST(test_content_id_block_131072);
    RUN_TEST(test_content_id_body_vs_receipts);

    printf("\n  %d/%d tests passed\n\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
