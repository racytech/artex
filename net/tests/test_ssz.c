/*
 * SSZ Tests
 *
 * Validates the Portal Network SSZ subset:
 *   1. Basic type encode/decode (uint8, uint16, uint64)
 *   2. Container encoding with fixed + variable fields
 *   3. BitList encode/decode
 *   4. Union encode/decode
 *   5. PING/PONG message encoding (worked example)
 *   6. ACCEPT message encoding (worked example)
 *   7. List of variable-size elements (nested offsets)
 */

#include "../include/ssz.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* =========================================================================
 * Helpers
 * ========================================================================= */

static int test_num = 0;
static int pass_count = 0;

#define RUN(fn) do { \
    test_num++; \
    printf("  [%2d] %-55s", test_num, #fn); \
    fn(); \
    pass_count++; \
    printf("OK\n"); \
} while (0)

#define ASSERT(cond, fmt, ...) do { \
    if (!(cond)) { \
        printf("FAIL\n      %s:%d: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        exit(1); \
    } \
} while (0)

static void assert_bytes_equal(const uint8_t *got, size_t got_len,
                                const uint8_t *expected, size_t expected_len,
                                const char *label) {
    ASSERT(got_len == expected_len, "%s: length mismatch (%zu vs %zu)",
           label, got_len, expected_len);
    if (memcmp(got, expected, got_len) != 0) {
        printf("FAIL\n      %s: data mismatch\n", label);
        printf("      got:      ");
        for (size_t i = 0; i < got_len; i++) printf("%02x", got[i]);
        printf("\n      expected: ");
        for (size_t i = 0; i < expected_len; i++) printf("%02x", expected[i]);
        printf("\n");
        exit(1);
    }
}

/* =========================================================================
 * Basic type encode/decode
 * ========================================================================= */

static void test_uint8_roundtrip(void) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    ssz_buf_append_u8(&buf, 0x00);
    ssz_buf_append_u8(&buf, 0x42);
    ssz_buf_append_u8(&buf, 0xff);

    ASSERT(buf.len == 3, "expected 3 bytes, got %zu", buf.len);

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    ASSERT(ssz_dec_u8(&dec) == 0x00, "u8[0]");
    ASSERT(ssz_dec_u8(&dec) == 0x42, "u8[1]");
    ASSERT(ssz_dec_u8(&dec) == 0xff, "u8[2]");
    ASSERT(!ssz_dec_error(&dec), "no error");

    ssz_buf_free(&buf);
}

static void test_uint16_roundtrip(void) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    ssz_buf_append_u16(&buf, 0x0000);
    ssz_buf_append_u16(&buf, 0x1234);
    ssz_buf_append_u16(&buf, 0xffff);

    ASSERT(buf.len == 6, "expected 6 bytes");

    /* Verify little-endian encoding */
    ASSERT(buf.data[2] == 0x34 && buf.data[3] == 0x12, "LE u16");

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    ASSERT(ssz_dec_u16(&dec) == 0x0000, "u16[0]");
    ASSERT(ssz_dec_u16(&dec) == 0x1234, "u16[1]");
    ASSERT(ssz_dec_u16(&dec) == 0xffff, "u16[2]");

    ssz_buf_free(&buf);
}

static void test_uint64_roundtrip(void) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    ssz_buf_append_u64(&buf, 1);
    ssz_buf_append_u64(&buf, 0xdeadbeefcafe1234ULL);

    ASSERT(buf.len == 16, "expected 16 bytes");

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    ASSERT(ssz_dec_u64(&dec) == 1, "u64[0]");
    ASSERT(ssz_dec_u64(&dec) == 0xdeadbeefcafe1234ULL, "u64[1]");

    ssz_buf_free(&buf);
}

static void test_decoder_underflow(void) {
    uint8_t data[2] = {0x42, 0x00};
    ssz_dec_t dec;
    ssz_dec_init(&dec, data, 2);

    ssz_dec_u8(&dec);   /* OK */
    ssz_dec_u16(&dec);  /* needs 2 bytes, only 1 left → error */
    ASSERT(ssz_dec_error(&dec), "should be error");
}

/* =========================================================================
 * Container encoding — PING message
 * ========================================================================= */

static void test_container_ping(void) {
    /*
     * PING { enr_seq: uint64, custom_payload: ByteList }
     * enr_seq = 1, custom_payload = [0x01, 0x02]
     *
     * Expected encoding (no message ID prefix, just the container):
     *   01 00 00 00 00 00 00 00    enr_seq = 1 (LE u64)
     *   0c 00 00 00                offset = 12 (0x0c) for custom_payload
     *   01 02                      custom_payload data
     */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 64);

    size_t start = buf.len;

    /* Fixed field: enr_seq */
    ssz_buf_append_u64(&buf, 1);

    /* Variable field: custom_payload offset placeholder */
    size_t off = ssz_container_reserve_offset(&buf);

    /* Patch offset and write variable data */
    ssz_container_patch_offset(&buf, off, start);
    uint8_t payload[] = {0x01, 0x02};
    ssz_buf_append(&buf, payload, 2);

    uint8_t expected[] = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  /* enr_seq */
        0x0c, 0x00, 0x00, 0x00,                            /* offset */
        0x01, 0x02                                          /* payload */
    };
    assert_bytes_equal(buf.data, buf.len, expected, sizeof(expected), "PING");

    ssz_buf_free(&buf);
}

/* Decode the PING */
static void test_container_ping_decode(void) {
    uint8_t data[] = {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x0c, 0x00, 0x00, 0x00,
        0x01, 0x02
    };

    ssz_dec_t dec;
    ssz_dec_init(&dec, data, sizeof(data));

    uint64_t enr_seq = ssz_dec_u64(&dec);
    ASSERT(enr_seq == 1, "enr_seq = %llu", (unsigned long long)enr_seq);

    uint32_t payload_offset = ssz_dec_offset(&dec);
    ASSERT(payload_offset == 12, "offset = %u", payload_offset);

    /* Payload is from offset to end */
    size_t payload_len = sizeof(data) - payload_offset;
    ASSERT(payload_len == 2, "payload_len = %zu", payload_len);

    const uint8_t *payload = data + payload_offset;
    ASSERT(payload[0] == 0x01 && payload[1] == 0x02, "payload data");

    ASSERT(!ssz_dec_error(&dec), "no error");
}

/* =========================================================================
 * Container encoding — ACCEPT message (with BitList)
 * ========================================================================= */

static void test_container_accept(void) {
    /*
     * ACCEPT { connection_id: Bytes2, content_keys: BitList[64] }
     * connection_id = [0xAB, 0xCD], content_keys = [1, 0, 1]
     *
     * Expected:
     *   AB CD                      connection_id (fixed, 2 bytes)
     *   06 00 00 00                offset = 6 for content_keys
     *   0D                         BitList: bits 1,0,1 + sentinel = 0x0D
     */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 64);
    size_t start = buf.len;

    /* Fixed field: connection_id */
    uint8_t conn_id[2] = {0xAB, 0xCD};
    ssz_buf_append(&buf, conn_id, 2);

    /* Variable field: content_keys offset */
    size_t off = ssz_container_reserve_offset(&buf);

    /* Write BitList */
    ssz_container_patch_offset(&buf, off, start);
    bool bits[3] = {true, false, true};
    ssz_encode_bitlist(&buf, bits, 3);

    uint8_t expected[] = {
        0xAB, 0xCD,                     /* connection_id */
        0x06, 0x00, 0x00, 0x00,         /* offset = 6 */
        0x0D                            /* BitList: 1,0,1,sentinel = 00001101 */
    };
    assert_bytes_equal(buf.data, buf.len, expected, sizeof(expected), "ACCEPT");

    ssz_buf_free(&buf);
}

/* =========================================================================
 * BitList encode/decode
 * ========================================================================= */

static void test_bitlist_empty(void) {
    ssz_buf_t buf;
    ssz_buf_init(&buf, 8);

    /* Empty BitList: just the sentinel bit */
    ssz_encode_bitlist(&buf, NULL, 0);
    ASSERT(buf.len == 1, "empty bitlist = 1 byte");
    ASSERT(buf.data[0] == 0x01, "sentinel only = 0x01");

    bool bits[64];
    size_t count;
    ASSERT(ssz_decode_bitlist(buf.data, buf.len, bits, &count), "decode ok");
    ASSERT(count == 0, "empty count");

    ssz_buf_free(&buf);
}

static void test_bitlist_3bits(void) {
    /* [1, 0, 1] → bits: 1 0 1 sentinel → 00001101 = 0x0D */
    bool input[3] = {true, false, true};

    ssz_buf_t buf;
    ssz_buf_init(&buf, 8);
    ssz_encode_bitlist(&buf, input, 3);

    ASSERT(buf.len == 1, "3-bit bitlist = 1 byte");
    ASSERT(buf.data[0] == 0x0D, "expected 0x0D, got 0x%02x", buf.data[0]);

    bool output[64];
    size_t count;
    ASSERT(ssz_decode_bitlist(buf.data, buf.len, output, &count), "decode ok");
    ASSERT(count == 3, "count = %zu", count);
    ASSERT(output[0] == true, "bit 0");
    ASSERT(output[1] == false, "bit 1");
    ASSERT(output[2] == true, "bit 2");

    ssz_buf_free(&buf);
}

static void test_bitlist_8bits(void) {
    /* 8 bits: all set → bits + sentinel = 9 bits → 2 bytes */
    bool input[8] = {1,1,1,1,1,1,1,1};

    ssz_buf_t buf;
    ssz_buf_init(&buf, 8);
    ssz_encode_bitlist(&buf, input, 8);

    ASSERT(buf.len == 2, "8-bit bitlist = 2 bytes");
    ASSERT(buf.data[0] == 0xFF, "byte 0 = 0xFF");
    ASSERT(buf.data[1] == 0x01, "byte 1 = 0x01 (sentinel)");

    bool output[64];
    size_t count;
    ASSERT(ssz_decode_bitlist(buf.data, buf.len, output, &count), "decode ok");
    ASSERT(count == 8, "count = %zu", count);
    for (int i = 0; i < 8; i++)
        ASSERT(output[i] == true, "bit %d", i);

    ssz_buf_free(&buf);
}

static void test_bitlist_7bits(void) {
    /* 7 bits: [1,0,1,0,1,0,1] + sentinel = 8 bits = 1 byte */
    bool input[7] = {1,0,1,0,1,0,1};

    ssz_buf_t buf;
    ssz_buf_init(&buf, 8);
    ssz_encode_bitlist(&buf, input, 7);

    ASSERT(buf.len == 1, "7-bit bitlist = 1 byte");
    /* bits: 1 0 1 0 1 0 1 sentinel(1) = bit7..bit0 = 10101011 → but LSB-first packing!
     * bit0=1, bit1=0, bit2=1, bit3=0, bit4=1, bit5=0, bit6=1, bit7=1(sentinel)
     * = 0xD5 */
    ASSERT(buf.data[0] == 0xD5, "expected 0xD5, got 0x%02x", buf.data[0]);

    bool output[64];
    size_t count;
    ASSERT(ssz_decode_bitlist(buf.data, buf.len, output, &count), "decode ok");
    ASSERT(count == 7, "count = %zu", count);

    ssz_buf_free(&buf);
}

/* =========================================================================
 * Union encode/decode
 * ========================================================================= */

static void test_union_selector0(void) {
    /* Union variant 0: Bytes2 (connection_id) */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    ssz_encode_union_selector(&buf, 0);
    uint8_t conn_id[2] = {0x12, 0x34};
    ssz_buf_append(&buf, conn_id, 2);

    ASSERT(buf.len == 3, "union(0, Bytes2) = 3 bytes");
    ASSERT(buf.data[0] == 0x00, "selector = 0");
    ASSERT(buf.data[1] == 0x12, "data[0]");
    ASSERT(buf.data[2] == 0x34, "data[1]");

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    uint8_t sel = ssz_decode_union_selector(&dec);
    ASSERT(sel == 0, "decoded selector");
    const uint8_t *p = ssz_dec_bytes(&dec, 2);
    ASSERT(p[0] == 0x12 && p[1] == 0x34, "decoded data");

    ssz_buf_free(&buf);
}

static void test_union_selector1(void) {
    /* Union variant 1: ByteList (content) */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    ssz_encode_union_selector(&buf, 1);
    uint8_t content[] = {0xDE, 0xAD, 0xBE, 0xEF};
    ssz_buf_append(&buf, content, 4);

    ASSERT(buf.len == 5, "union(1, 4 bytes) = 5 bytes");
    ASSERT(buf.data[0] == 0x01, "selector = 1");

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    uint8_t sel = ssz_decode_union_selector(&dec);
    ASSERT(sel == 1, "decoded selector");
    size_t remaining = ssz_dec_remaining(&dec);
    ASSERT(remaining == 4, "4 bytes remaining");

    ssz_buf_free(&buf);
}

/* =========================================================================
 * List of fixed-size elements
 * ========================================================================= */

static void test_list_fixed_uint16(void) {
    /* List[uint16, 256]: distances for FIND_NODES */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 16);

    uint16_t distances[] = {256, 255, 254};
    for (int i = 0; i < 3; i++)
        ssz_buf_append_u16(&buf, distances[i]);

    ASSERT(buf.len == 6, "3 x uint16 = 6 bytes");

    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    size_t count = ssz_dec_remaining(&dec) / 2;
    ASSERT(count == 3, "3 elements");
    ASSERT(ssz_dec_u16(&dec) == 256, "dist[0]");
    ASSERT(ssz_dec_u16(&dec) == 255, "dist[1]");
    ASSERT(ssz_dec_u16(&dec) == 254, "dist[2]");

    ssz_buf_free(&buf);
}

/* =========================================================================
 * List of variable-size elements (with offsets)
 * ========================================================================= */

static void test_list_variable_bytelist(void) {
    /*
     * List[ByteList[300], 32]: ENR records in NODES
     * Two records: [0xAA, 0xBB] and [0xCC, 0xDD, 0xEE]
     *
     * Encoding:
     *   08 00 00 00    offset to record 0 data (= 2 offsets * 4 = 8)
     *   0a 00 00 00    offset to record 1 data (= 8 + 2 = 10)
     *   AA BB          record 0
     *   CC DD EE       record 1
     */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 32);

    size_t start = buf.len;

    /* Reserve offsets for 2 elements */
    size_t off0 = ssz_container_reserve_offset(&buf);
    size_t off1 = ssz_container_reserve_offset(&buf);

    /* Write element 0 */
    ssz_container_patch_offset(&buf, off0, start);
    uint8_t r0[] = {0xAA, 0xBB};
    ssz_buf_append(&buf, r0, 2);

    /* Write element 1 */
    ssz_container_patch_offset(&buf, off1, start);
    uint8_t r1[] = {0xCC, 0xDD, 0xEE};
    ssz_buf_append(&buf, r1, 3);

    uint8_t expected[] = {
        0x08, 0x00, 0x00, 0x00,   /* offset 0 = 8 */
        0x0a, 0x00, 0x00, 0x00,   /* offset 1 = 10 */
        0xAA, 0xBB,               /* record 0 */
        0xCC, 0xDD, 0xEE          /* record 1 */
    };
    assert_bytes_equal(buf.data, buf.len, expected, sizeof(expected),
                       "List[ByteList]");

    /* Decode */
    ssz_dec_t dec;
    ssz_dec_init(&dec, buf.data, buf.len);
    uint32_t o0 = ssz_dec_offset(&dec);
    uint32_t o1 = ssz_dec_offset(&dec);
    ASSERT(o0 == 8 && o1 == 10, "offsets");

    /* Record 0: bytes [o0, o1) */
    size_t len0 = o1 - o0;
    ASSERT(len0 == 2, "record 0 len");
    ASSERT(buf.data[o0] == 0xAA && buf.data[o0 + 1] == 0xBB, "record 0 data");

    /* Record 1: bytes [o1, end) */
    size_t len1 = buf.len - o1;
    ASSERT(len1 == 3, "record 1 len");
    ASSERT(buf.data[o1] == 0xCC, "record 1 data");

    ssz_buf_free(&buf);
}

/* =========================================================================
 * FIND_NODES container (List inside Container)
 * ========================================================================= */

static void test_container_find_nodes(void) {
    /*
     * FIND_NODES { distances: List[uint16, 256] }
     * distances = [256, 255]
     *
     * Container has 1 variable field:
     *   04 00 00 00    offset = 4 (just the offset itself)
     *   00 01          256 as LE uint16
     *   FF 00          255 as LE uint16
     */
    ssz_buf_t buf;
    ssz_buf_init(&buf, 32);
    size_t start = buf.len;

    size_t off = ssz_container_reserve_offset(&buf);
    ssz_container_patch_offset(&buf, off, start);

    ssz_buf_append_u16(&buf, 256);
    ssz_buf_append_u16(&buf, 255);

    uint8_t expected[] = {
        0x04, 0x00, 0x00, 0x00,
        0x00, 0x01,
        0xFF, 0x00
    };
    assert_bytes_equal(buf.data, buf.len, expected, sizeof(expected),
                       "FIND_NODES");

    ssz_buf_free(&buf);
}

/* =========================================================================
 * Main
 * ========================================================================= */

int main(void) {
    printf("=== SSZ Tests ===\n\n");

    printf("Phase 1: Basic Types\n");
    RUN(test_uint8_roundtrip);
    RUN(test_uint16_roundtrip);
    RUN(test_uint64_roundtrip);
    RUN(test_decoder_underflow);

    printf("\nPhase 2: Containers\n");
    RUN(test_container_ping);
    RUN(test_container_ping_decode);
    RUN(test_container_accept);
    RUN(test_container_find_nodes);

    printf("\nPhase 3: BitList\n");
    RUN(test_bitlist_empty);
    RUN(test_bitlist_3bits);
    RUN(test_bitlist_8bits);
    RUN(test_bitlist_7bits);

    printf("\nPhase 4: Union\n");
    RUN(test_union_selector0);
    RUN(test_union_selector1);

    printf("\nPhase 5: Variable-Size Lists\n");
    RUN(test_list_fixed_uint16);
    RUN(test_list_variable_bytelist);

    printf("\n=== Results: %d / %d passed ===\n", pass_count, test_num);
    return 0;
}
