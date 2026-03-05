#include "hash.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

static void test_hash_zero(void) {
    printf("Testing hash_zero...\n");
    hash_t h = hash_zero();
    assert(hash_is_zero(&h));
    
    for (size_t i = 0; i < HASH_SIZE; i++) {
        assert(h.bytes[i] == 0);
    }
    
    printf("✅ hash_zero passed\n");
}

static void test_hash_from_bytes(void) {
    printf("Testing hash_from_bytes...\n");
    uint8_t data[HASH_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    
    hash_t h = hash_from_bytes(data);
    assert(memcmp(h.bytes, data, HASH_SIZE) == 0);
    assert(!hash_is_zero(&h));
    
    printf("✅ hash_from_bytes passed\n");
}

static void test_hash_to_hex(void) {
    printf("Testing hash_to_hex...\n");
    uint8_t data[HASH_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    
    hash_t h = hash_from_bytes(data);
    char hex[67];
    hash_to_hex(&h, hex);
    
    const char* expected = "0x123456789abcdef0112233445566778899aabbccddeeff000102030405060708";
    assert(strcmp(hex, expected) == 0);
    
    printf("  Result: %s\n", hex);
    printf("✅ hash_to_hex passed\n");
}

static void test_hash_from_hex(void) {
    printf("Testing hash_from_hex...\n");
    
    // Test with 0x prefix
    const char* hex1 = "0x123456789abcdef0112233445566778899aabbccddeeff000102030405060708";
    hash_t h1;
    assert(hash_from_hex(hex1, &h1));
    
    char result1[67];
    hash_to_hex(&h1, result1);
    assert(strcmp(result1, hex1) == 0);
    
    // Test without 0x prefix
    const char* hex2 = "123456789abcdef0112233445566778899aabbccddeeff000102030405060708";
    hash_t h2;
    assert(hash_from_hex(hex2, &h2));
    assert(hash_equal(&h1, &h2));
    
    // Test with uppercase
    const char* hex3 = "0x123456789ABCDEF0112233445566778899AABBCCDDEEFF000102030405060708";
    hash_t h3;
    assert(hash_from_hex(hex3, &h3));
    assert(hash_equal(&h1, &h3));
    
    // Test invalid hex (wrong length)
    hash_t h_invalid;
    assert(!hash_from_hex("0x1234", &h_invalid));
    
    // Test invalid hex (bad characters)
    assert(!hash_from_hex("0x123456789abcdef0112233445566778899aabbccddeeff00010203040506070g", &h_invalid));
    
    printf("✅ hash_from_hex passed\n");
}

static void test_hash_equal(void) {
    printf("Testing hash_equal...\n");
    
    uint8_t data1[HASH_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    
    uint8_t data2[HASH_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    
    uint8_t data3[HASH_SIZE] = {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
    };
    
    hash_t h1 = hash_from_bytes(data1);
    hash_t h2 = hash_from_bytes(data2);
    hash_t h3 = hash_from_bytes(data3);
    
    assert(hash_equal(&h1, &h2));
    assert(!hash_equal(&h1, &h3));
    
    printf("✅ hash_equal passed\n");
}

static void test_hash_copy(void) {
    printf("Testing hash_copy...\n");
    
    uint8_t data[HASH_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08
    };
    
    hash_t h1 = hash_from_bytes(data);
    hash_t h2;
    hash_copy(&h2, &h1);
    
    assert(hash_equal(&h1, &h2));
    
    printf("✅ hash_copy passed\n");
}

static void test_real_ethereum_hashes(void) {
    printf("Testing real Ethereum hashes...\n");
    
    // Genesis block hash (mainnet)
    const char* genesis = "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3";
    hash_t h;
    assert(hash_from_hex(genesis, &h));
    
    char hex[67];
    hash_to_hex(&h, hex);
    
    // Compare case-insensitively
    for (size_t i = 0; i < strlen(genesis); i++) {
        char c1 = tolower(genesis[i]);
        char c2 = tolower(hex[i]);
        assert(c1 == c2);
    }
    
    printf("  Genesis hash: %s\n", hex);
    printf("✅ real Ethereum hashes passed\n");
}

static void test_hash_keccak256(void) {
    printf("Testing hash_keccak256...\n");

    // keccak256("hello") = 0x1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8
    const char* test_data = "hello";
    hash_t h = hash_keccak256((const uint8_t*)test_data, strlen(test_data));

    hash_t expected;
    assert(hash_from_hex("0x1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8", &expected));
    assert(hash_equal(&h, &expected));

    // keccak256("") = 0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
    hash_t empty = hash_keccak256(NULL, 0);
    hash_t expected_empty;
    assert(hash_from_hex("0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470", &expected_empty));
    assert(hash_equal(&empty, &expected_empty));

    char hex[67];
    hash_to_hex(&h, hex);
    printf("  keccak256(\"hello\") = %s\n", hex);
    printf("  keccak256(\"\") verified\n");
    printf("  hash_keccak256 passed\n");
}

int main(void) {
    printf("Running hash tests...\n\n");
    
    test_hash_zero();
    test_hash_from_bytes();
    test_hash_to_hex();
    test_hash_from_hex();
    test_hash_equal();
    test_hash_copy();
    test_real_ethereum_hashes();
    test_hash_keccak256();
    
    printf("\nAll hash tests passed!\n");
    return 0;
}
