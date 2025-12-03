#include "bytes.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

static void test_bytes_new(void) {
    printf("Testing bytes_new...\n");
    bytes_t b = bytes_new();
    
    assert(b.data == NULL);
    assert(b.len == 0);
    assert(b.capacity == 0);
    assert(bytes_is_empty(&b));
    
    printf("✅ bytes_new passed\n");
}

static void test_bytes_with_capacity(void) {
    printf("Testing bytes_with_capacity...\n");
    bytes_t b = bytes_with_capacity(100);
    
    assert(b.data != NULL);
    assert(b.len == 0);
    assert(b.capacity == 100);
    assert(bytes_is_empty(&b));
    
    bytes_free(&b);
    assert(b.data == NULL);
    
    printf("✅ bytes_with_capacity passed\n");
}

static void test_bytes_from_data(void) {
    printf("Testing bytes_from_data...\n");
    uint8_t data[] = {0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0};
    bytes_t b = bytes_from_data(data, sizeof(data));
    
    assert(b.data != NULL);
    assert(b.len == sizeof(data));
    assert(memcmp(b.data, data, sizeof(data)) == 0);
    assert(!bytes_is_empty(&b));
    
    bytes_free(&b);
    printf("✅ bytes_from_data passed\n");
}

static void test_bytes_to_hex(void) {
    printf("Testing bytes_to_hex...\n");
    uint8_t data[] = {0x12, 0x34, 0x56, 0x78};
    bytes_t b = bytes_from_data(data, sizeof(data));
    
    char* hex = bytes_to_hex(&b);
    assert(hex != NULL);
    assert(strcmp(hex, "0x12345678") == 0);
    
    printf("  Result: %s\n", hex);
    free(hex);
    bytes_free(&b);
    
    // Test empty bytes
    bytes_t empty = bytes_new();
    char* empty_hex = bytes_to_hex(&empty);
    assert(strcmp(empty_hex, "0x") == 0);
    free(empty_hex);
    
    printf("✅ bytes_to_hex passed\n");
}

static void test_bytes_from_hex(void) {
    printf("Testing bytes_from_hex...\n");
    
    // Test with 0x prefix
    printf("  Test 1: 0x12345678\n");
    bytes_t b1 = bytes_new();
    assert(bytes_from_hex("0x12345678", &b1));
    assert(b1.len == 4);
    assert(b1.data[0] == 0x12);
    assert(b1.data[3] == 0x78);
    bytes_free(&b1);
    printf("  Test 1 passed\n");
    
    // Test without 0x prefix
    printf("  Test 2: abcdef\n");
    bytes_t b2 = bytes_new();
    assert(bytes_from_hex("abcdef", &b2));
    assert(b2.len == 3);
    assert(b2.data[0] == 0xab);
    assert(b2.data[2] == 0xef);
    bytes_free(&b2);
    printf("  Test 2 passed\n");
    
    // Test empty
    printf("  Test 3: 0x (empty)\n");
    bytes_t b3 = bytes_new();
    assert(bytes_from_hex("0x", &b3));
    assert(b3.len == 0);
    bytes_free(&b3);
    printf("  Test 3 passed\n");
    
    // Test invalid (odd length)
    printf("  Test 4: 0x123 (invalid)\n");
    bytes_t b_invalid = bytes_new();
    assert(!bytes_from_hex("0x123", &b_invalid));
    printf("  Test 4 passed\n");
    
    printf("✅ bytes_from_hex passed\n");
}

static void test_bytes_append(void) {
    printf("Testing bytes_append...\n");
    bytes_t b = bytes_new();
    
    uint8_t data1[] = {0x11, 0x22};
    assert(bytes_append(&b, data1, sizeof(data1)));
    assert(b.len == 2);
    
    uint8_t data2[] = {0x33, 0x44, 0x55};
    assert(bytes_append(&b, data2, sizeof(data2)));
    assert(b.len == 5);
    assert(b.data[0] == 0x11);
    assert(b.data[4] == 0x55);
    
    bytes_free(&b);
    printf("✅ bytes_append passed\n");
}

static void test_bytes_push(void) {
    printf("Testing bytes_push...\n");
    bytes_t b = bytes_new();
    
    assert(bytes_push(&b, 0xaa));
    assert(bytes_push(&b, 0xbb));
    assert(bytes_push(&b, 0xcc));
    
    assert(b.len == 3);
    assert(b.data[0] == 0xaa);
    assert(b.data[1] == 0xbb);
    assert(b.data[2] == 0xcc);
    
    bytes_free(&b);
    printf("✅ bytes_push passed\n");
}

static void test_bytes_resize(void) {
    printf("Testing bytes_resize...\n");
    bytes_t b = bytes_with_capacity(10);
    
    // Resize larger
    assert(bytes_resize(&b, 5));
    assert(b.len == 5);
    for (size_t i = 0; i < 5; i++) {
        assert(b.data[i] == 0); // Should be zero-initialized
    }
    
    // Resize smaller
    b.data[0] = 0x11;
    b.data[4] = 0x55;
    assert(bytes_resize(&b, 3));
    assert(b.len == 3);
    assert(b.data[0] == 0x11);
    
    bytes_free(&b);
    printf("✅ bytes_resize passed\n");
}

static void test_bytes_clear(void) {
    printf("Testing bytes_clear...\n");
    uint8_t data[] = {0x11, 0x22, 0x33};
    bytes_t b = bytes_from_data(data, sizeof(data));
    
    size_t old_capacity = b.capacity;
    bytes_clear(&b);
    
    assert(b.len == 0);
    assert(b.capacity == old_capacity); // Capacity preserved
    assert(bytes_is_empty(&b));
    
    bytes_free(&b);
    printf("✅ bytes_clear passed\n");
}

static void test_bytes_clone(void) {
    printf("Testing bytes_clone...\n");
    uint8_t data[] = {0xaa, 0xbb, 0xcc, 0xdd};
    bytes_t b1 = bytes_from_data(data, sizeof(data));
    bytes_t b2 = bytes_clone(&b1);
    
    assert(bytes_equal(&b1, &b2));
    assert(b1.data != b2.data); // Different pointers
    
    // Modify b2, b1 should be unchanged
    b2.data[0] = 0xff;
    assert(!bytes_equal(&b1, &b2));
    assert(b1.data[0] == 0xaa);
    
    bytes_free(&b1);
    bytes_free(&b2);
    printf("✅ bytes_clone passed\n");
}

static void test_bytes_equal(void) {
    printf("Testing bytes_equal...\n");
    uint8_t data1[] = {0x11, 0x22, 0x33};
    uint8_t data2[] = {0x11, 0x22, 0x33};
    uint8_t data3[] = {0x11, 0x22, 0x44};
    
    bytes_t b1 = bytes_from_data(data1, sizeof(data1));
    bytes_t b2 = bytes_from_data(data2, sizeof(data2));
    bytes_t b3 = bytes_from_data(data3, sizeof(data3));
    
    assert(bytes_equal(&b1, &b2));
    assert(!bytes_equal(&b1, &b3));
    
    bytes_free(&b1);
    bytes_free(&b2);
    bytes_free(&b3);
    printf("✅ bytes_equal passed\n");
}

static void test_bytes_real_world(void) {
    printf("Testing real-world usage...\n");
    
    // Build ERC20 transfer calldata: function selector + address + amount
    bytes_t calldata = bytes_new();
    
    // Function selector for transfer(address,uint256)
    uint8_t selector[] = {0xa9, 0x05, 0x9c, 0xbb};
    bytes_append(&calldata, selector, sizeof(selector));
    
    // Padded address (20 bytes address + 12 bytes padding)
    uint8_t padding[12] = {0};
    bytes_append(&calldata, padding, sizeof(padding));
    uint8_t recipient[] = {
        0xd8, 0xdA, 0x6B, 0xF2, 0x69, 0x64, 0xaF, 0x9D,
        0x7e, 0xEd, 0x9e, 0x03, 0xE5, 0x34, 0x15, 0xD3,
        0x7a, 0xA9, 0x60, 0x45
    };
    bytes_append(&calldata, recipient, sizeof(recipient));
    
    // Amount (1000 tokens with 18 decimals)
    uint8_t amount[32] = {0};
    amount[31] = 0xe8; // Low byte of 1000
    amount[30] = 0x03;
    bytes_append(&calldata, amount, sizeof(amount));
    
    assert(calldata.len == 4 + 32 + 32); // selector + address + amount
    
    char* hex = bytes_to_hex(&calldata);
    printf("  Transfer calldata: %s\n", hex);
    
    free(hex);
    bytes_free(&calldata);
    printf("✅ real-world usage passed\n");
}

int main(void) {
    printf("Running bytes tests...\n\n");
    
    test_bytes_new();
    test_bytes_with_capacity();
    test_bytes_from_data();
    test_bytes_to_hex();
    test_bytes_from_hex();
    test_bytes_append();
    test_bytes_push();
    test_bytes_resize();
    test_bytes_clear();
    test_bytes_clone();
    test_bytes_equal();
    test_bytes_real_world();
    
    printf("\n✅ All bytes tests passed!\n");
    return 0;
}
