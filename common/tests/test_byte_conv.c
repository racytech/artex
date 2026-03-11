/*
 * Byte conversion regression tests.
 * Validates bswap64-optimized bytes↔uint256 conversions and
 * to_words/from_words roundtrips against known byte patterns.
 */
#include <stdio.h>
#include <string.h>
#include "uint256.h"

static int passed = 0, failed = 0;

/* ================================================================
 * Helpers
 * ================================================================ */
static void check_bytes_equal(const char *label,
                              const uint8_t *got, const uint8_t *expected, size_t len) {
    if (memcmp(got, expected, len) == 0) {
        passed++;
    } else {
        failed++;
        printf("FAIL %s:\n  expected:", label);
        for (size_t i = 0; i < len; i++) printf(" %02x", expected[i]);
        printf("\n  got:     ");
        for (size_t i = 0; i < len; i++) printf(" %02x", got[i]);
        printf("\n");
    }
}

static void check_uint256_equal(const char *label,
                                const uint256_t *got, const uint256_t *expected) {
    if (uint256_is_equal(got, expected)) {
        passed++;
    } else {
        failed++;
        printf("FAIL %s:\n  expected=", label);
        uint256_print(expected);
        printf("\n  got=     ");
        uint256_print(got);
        printf("\n");
    }
}

/* ================================================================
 * to_bytes / from_bytes roundtrip (big-endian)
 * ================================================================ */
static void test_roundtrip_be(void) {
    /* Zero */
    {
        uint256_t x = UINT256_ZERO;
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(0)", &y, &x);
    }

    /* One (LSB set) */
    {
        uint256_t x = uint256_from_uint64(1);
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(1)", &y, &x);
    }

    /* MSB set: 0x80000...0 */
    {
        uint256_t x = UINT256_ZERO;
        x.high = (uint128_t)1 << 127;
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(MSB)", &y, &x);
    }

    /* MAX */
    {
        uint256_t x;
        x.high = UINT128_MAX;
        x.low = UINT128_MAX;
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(MAX)", &y, &x);
    }

    /* Cross-word pattern: 0x0102030405060708090a0b0c0d0e0f10... */
    {
        uint256_t x = uint256_from_hex("0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20");
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(sequential)", &y, &x);
    }

    /* DEADBEEF pattern spanning all 4 words */
    {
        uint256_t x = uint256_from_hex("DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF");
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint256_t y = uint256_from_bytes(buf, 32);
        check_uint256_equal("roundtrip_be(DEADBEEF)", &y, &x);
    }
}

/* ================================================================
 * Known byte layout for to_bytes (big-endian output)
 * ================================================================ */
static void test_to_bytes_known(void) {
    /* Value 1 → 31 zero bytes then 0x01 */
    {
        uint256_t x = uint256_from_uint64(1);
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint8_t expected[32] = {0};
        expected[31] = 1;
        check_bytes_equal("to_bytes(1)", buf, expected, 32);
    }

    /* Value 0x0102 → ...00 01 02 */
    {
        uint256_t x = uint256_from_uint64(0x0102);
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint8_t expected[32] = {0};
        expected[30] = 0x01;
        expected[31] = 0x02;
        check_bytes_equal("to_bytes(0x0102)", buf, expected, 32);
    }

    /* Sequential bytes 0x00..0x1F — the value whose big-endian repr is 0x00,0x01,...,0x1F */
    {
        uint8_t expected[32];
        for (int i = 0; i < 32; i++) expected[i] = (uint8_t)i;
        uint256_t x = uint256_from_bytes(expected, 32);
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        check_bytes_equal("to_bytes(0x00..0x1F)", buf, expected, 32);
    }

    /* Value with MSB set: 0xFF000...000 */
    {
        uint256_t x = UINT256_ZERO;
        x.high = (uint128_t)0xFF << 120;
        uint8_t buf[32];
        uint256_to_bytes(&x, buf);
        uint8_t expected[32] = {0};
        expected[0] = 0xFF;
        check_bytes_equal("to_bytes(0xFF<<248)", buf, expected, 32);
    }
}

/* ================================================================
 * from_bytes with short input (< 32 bytes, zero-padded)
 * ================================================================ */
static void test_from_bytes_short(void) {
    /* 1 byte: 0x42 → value 0x42 */
    {
        uint8_t data[] = {0x42};
        uint256_t got = uint256_from_bytes(data, 1);
        uint256_t expected = uint256_from_uint64(0x42);
        check_uint256_equal("from_bytes(1 byte)", &got, &expected);
    }

    /* 2 bytes: 0x01 0x02 → value 0x0102 */
    {
        uint8_t data[] = {0x01, 0x02};
        uint256_t got = uint256_from_bytes(data, 2);
        uint256_t expected = uint256_from_uint64(0x0102);
        check_uint256_equal("from_bytes(2 bytes)", &got, &expected);
    }

    /* 8 bytes: one full word */
    {
        uint8_t data[] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF};
        uint256_t got = uint256_from_bytes(data, 8);
        uint256_t expected = uint256_from_hex("0123456789ABCDEF");
        check_uint256_equal("from_bytes(8 bytes)", &got, &expected);
    }

    /* 16 bytes: two words */
    {
        uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
                          0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
        uint256_t got = uint256_from_bytes(data, 16);
        uint256_t expected = uint256_from_hex("DEADBEEFCAFEBABE0102030405060708");
        check_uint256_equal("from_bytes(16 bytes)", &got, &expected);
    }

    /* 20 bytes: address-sized */
    {
        uint8_t data[20];
        for (int i = 0; i < 20; i++) data[i] = (uint8_t)(0xA0 + i);
        uint256_t got = uint256_from_bytes(data, 20);
        uint256_t expected = uint256_from_hex("A0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3");
        check_uint256_equal("from_bytes(20 bytes)", &got, &expected);
    }

    /* 0 bytes → zero */
    {
        uint256_t got = uint256_from_bytes(NULL, 0);
        uint256_t expected = UINT256_ZERO;
        check_uint256_equal("from_bytes(0 bytes)", &got, &expected);
    }
}

/* ================================================================
 * to_bytes_le / from_bytes_le roundtrip (little-endian)
 * ================================================================ */
static void test_roundtrip_le(void) {
    /* Zero */
    {
        uint256_t x = UINT256_ZERO;
        uint8_t buf[32];
        uint256_to_bytes_le(&x, buf);
        uint256_t y = uint256_from_bytes_le(buf, 32);
        check_uint256_equal("roundtrip_le(0)", &y, &x);
    }

    /* One: LE byte[0] = 0x01 */
    {
        uint256_t x = uint256_from_uint64(1);
        uint8_t buf[32];
        uint256_to_bytes_le(&x, buf);

        /* Verify LE layout: first byte is 0x01 */
        uint8_t expected_le[32] = {0};
        expected_le[0] = 1;
        check_bytes_equal("to_bytes_le(1) layout", buf, expected_le, 32);

        uint256_t y = uint256_from_bytes_le(buf, 32);
        check_uint256_equal("roundtrip_le(1)", &y, &x);
    }

    /* MAX */
    {
        uint256_t x;
        x.high = UINT128_MAX;
        x.low = UINT128_MAX;
        uint8_t buf[32];
        uint256_to_bytes_le(&x, buf);
        uint256_t y = uint256_from_bytes_le(buf, 32);
        check_uint256_equal("roundtrip_le(MAX)", &y, &x);
    }

    /* Cross-word pattern */
    {
        uint256_t x = uint256_from_hex("0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20");
        uint8_t buf[32];
        uint256_to_bytes_le(&x, buf);
        uint256_t y = uint256_from_bytes_le(buf, 32);
        check_uint256_equal("roundtrip_le(sequential)", &y, &x);
    }

    /* Verify LE is reverse of BE */
    {
        uint256_t x = uint256_from_hex("DEADBEEFCAFEBABE0102030405060708090A0B0C0D0E0F101112131415161718");
        uint8_t be[32], le[32];
        uint256_to_bytes(&x, be);
        uint256_to_bytes_le(&x, le);

        /* LE should be byte-reversed BE */
        int reversed = 1;
        for (int i = 0; i < 32; i++) {
            if (le[i] != be[31 - i]) { reversed = 0; break; }
        }
        if (reversed) passed++;
        else {
            failed++;
            printf("FAIL LE is not byte-reverse of BE\n");
        }
    }
}

/* ================================================================
 * to_words / from_words roundtrip
 * ================================================================ */
static void test_words_roundtrip(void) {
    /* Zero */
    {
        uint256_t x = UINT256_ZERO;
        uint64_t words[4];
        uint256_to_words(&x, words);
        uint256_t y = uint256_from_words(words);
        check_uint256_equal("words_roundtrip(0)", &y, &x);
    }

    /* MAX */
    {
        uint256_t x;
        x.high = UINT128_MAX;
        x.low = UINT128_MAX;
        uint64_t words[4];
        uint256_to_words(&x, words);
        uint256_t y = uint256_from_words(words);
        check_uint256_equal("words_roundtrip(MAX)", &y, &x);
    }

    /* Sequential pattern */
    {
        uint256_t x = uint256_from_hex("0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20");
        uint64_t words[4];
        uint256_to_words(&x, words);
        uint256_t y = uint256_from_words(words);
        check_uint256_equal("words_roundtrip(sequential)", &y, &x);
    }

    /* Verify word ordering: words[0] = low 64 bits of low */
    {
        uint256_t x = uint256_from_hex("1111111111111111222222222222222233333333333333334444444444444444");
        uint64_t words[4];
        uint256_to_words(&x, words);

        int ok = 1;
        if (words[0] != 0x4444444444444444ULL) ok = 0;
        if (words[1] != 0x3333333333333333ULL) ok = 0;
        if (words[2] != 0x2222222222222222ULL) ok = 0;
        if (words[3] != 0x1111111111111111ULL) ok = 0;
        if (ok) passed++;
        else {
            failed++;
            printf("FAIL word ordering: w[0]=%016llx w[1]=%016llx w[2]=%016llx w[3]=%016llx\n",
                   (unsigned long long)words[0], (unsigned long long)words[1],
                   (unsigned long long)words[2], (unsigned long long)words[3]);
        }
    }

    /* Verify words match byte layout: to_words then to_bytes should be consistent */
    {
        uint256_t x = uint256_from_hex("AABBCCDD11223344EEFF00995566778899AABBCCDDEEFF0011223344AABBCCDD");
        uint64_t words[4];
        uint256_to_words(&x, words);

        /* Reconstruct from words and compare */
        uint256_t y = uint256_from_words(words);
        uint8_t be_x[32], be_y[32];
        uint256_to_bytes(&x, be_x);
        uint256_to_bytes(&y, be_y);
        check_bytes_equal("words↔bytes consistency", be_y, be_x, 32);
    }
}

/* ================================================================
 * BE vs LE endianness cross-check
 * ================================================================ */
static void test_endianness(void) {
    /* Value 0x0102...1F20: BE bytes should be 01 02 ... 1F 20,
       LE bytes should be 20 1F ... 02 01 */
    {
        uint256_t x = uint256_from_hex("0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20");
        uint8_t be[32];
        uint256_to_bytes(&x, be);

        /* Verify big-endian: first byte should be 0x01 */
        if (be[0] == 0x01 && be[31] == 0x20) passed++;
        else {
            failed++;
            printf("FAIL BE endianness: be[0]=%02x be[31]=%02x\n", be[0], be[31]);
        }

        uint8_t le[32];
        uint256_to_bytes_le(&x, le);

        /* Verify little-endian: first byte should be 0x20 (lowest byte of value) */
        if (le[0] == 0x20 && le[31] == 0x01) passed++;
        else {
            failed++;
            printf("FAIL LE endianness: le[0]=%02x le[31]=%02x\n", le[0], le[31]);
        }
    }

    /* from_bytes(BE) and from_bytes_le(LE) of same value should match */
    {
        uint256_t x = uint256_from_hex("CAFEBABE00000000DEADBEEF00000000CAFEBABE00000000DEADBEEF00000000");
        uint8_t be[32], le[32];
        uint256_to_bytes(&x, be);
        uint256_to_bytes_le(&x, le);

        uint256_t from_be = uint256_from_bytes(be, 32);
        uint256_t from_le = uint256_from_bytes_le(le, 32);
        check_uint256_equal("from_be == from_le", &from_be, &from_le);
    }
}

/* ================================================================
 * MAIN
 * ================================================================ */
int main(void) {
    printf("=== byte conversion regression tests ===\n\n");

    test_roundtrip_be();
    test_to_bytes_known();
    test_from_bytes_short();
    test_roundtrip_le();
    test_words_roundtrip();
    test_endianness();

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
