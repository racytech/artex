#include "address.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

static void test_address_zero(void) {
    printf("Testing address_zero...\n");
    address_t addr = address_zero();
    assert(address_is_zero(&addr));
    
    for (size_t i = 0; i < ADDRESS_SIZE; i++) {
        assert(addr.bytes[i] == 0);
    }
    
    printf("✅ address_zero passed\n");
}

static void test_address_from_bytes(void) {
    printf("Testing address_from_bytes...\n");
    uint8_t data[ADDRESS_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc
    };
    
    address_t addr = address_from_bytes(data);
    assert(memcmp(addr.bytes, data, ADDRESS_SIZE) == 0);
    assert(!address_is_zero(&addr));
    
    printf("✅ address_from_bytes passed\n");
}

static void test_address_to_hex(void) {
    printf("Testing address_to_hex...\n");
    uint8_t data[ADDRESS_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc
    };
    
    address_t addr = address_from_bytes(data);
    char hex[43];
    address_to_hex(&addr, hex);
    
    const char* expected = "0x123456789abcdef0112233445566778899aabbcc";
    assert(strcmp(hex, expected) == 0);
    
    printf("  Result: %s\n", hex);
    printf("✅ address_to_hex passed\n");
}

static void test_address_from_hex(void) {
    printf("Testing address_from_hex...\n");
    
    // Test with 0x prefix
    const char* hex1 = "0x123456789abcdef0112233445566778899aabbcc";
    address_t addr1;
    assert(address_from_hex(hex1, &addr1));
    
    char result1[43];
    address_to_hex(&addr1, result1);
    assert(strcmp(result1, hex1) == 0);
    
    // Test without 0x prefix
    const char* hex2 = "123456789abcdef0112233445566778899aabbcc";
    address_t addr2;
    assert(address_from_hex(hex2, &addr2));
    assert(address_equal(&addr1, &addr2));
    
    // Test with uppercase
    const char* hex3 = "0x123456789ABCDEF0112233445566778899AABBCC";
    address_t addr3;
    assert(address_from_hex(hex3, &addr3));
    assert(address_equal(&addr1, &addr3));
    
    // Test invalid hex (wrong length)
    address_t addr_invalid;
    assert(!address_from_hex("0x1234", &addr_invalid));
    
    // Test invalid hex (bad characters)
    assert(!address_from_hex("0x123456789abcdef0112233445566778899aabbcg", &addr_invalid));
    
    printf("✅ address_from_hex passed\n");
}

static void test_address_equal(void) {
    printf("Testing address_equal...\n");
    
    uint8_t data1[ADDRESS_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc
    };
    
    uint8_t data2[ADDRESS_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc
    };
    
    uint8_t data3[ADDRESS_SIZE] = {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff
    };
    
    address_t addr1 = address_from_bytes(data1);
    address_t addr2 = address_from_bytes(data2);
    address_t addr3 = address_from_bytes(data3);
    
    assert(address_equal(&addr1, &addr2));
    assert(!address_equal(&addr1, &addr3));
    
    printf("✅ address_equal passed\n");
}

static void test_address_copy(void) {
    printf("Testing address_copy...\n");
    
    uint8_t data[ADDRESS_SIZE] = {
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc
    };
    
    address_t addr1 = address_from_bytes(data);
    address_t addr2;
    address_copy(&addr2, &addr1);
    
    assert(address_equal(&addr1, &addr2));
    
    printf("✅ address_copy passed\n");
}

static void test_real_ethereum_addresses(void) {
    printf("Testing real Ethereum addresses...\n");
    
    // Vitalik's address
    const char* vitalik = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    address_t addr;
    assert(address_from_hex(vitalik, &addr));
    
    char hex[43];
    address_to_hex(&addr, hex);
    
    // Compare case-insensitively
    for (size_t i = 0; i < strlen(vitalik); i++) {
        char c1 = tolower(vitalik[i]);
        char c2 = tolower(hex[i]);
        assert(c1 == c2);
    }
    
    printf("  Vitalik's address: %s\n", hex);
    printf("✅ real Ethereum addresses passed\n");
}

int main(void) {
    printf("Running address tests...\n\n");
    
    test_address_zero();
    test_address_from_bytes();
    test_address_to_hex();
    test_address_from_hex();
    test_address_equal();
    test_address_copy();
    test_real_ethereum_addresses();
    
    printf("\n✅ All address tests passed!\n");
    return 0;
}
