#include "../include/rlp.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

// Test RLP string encoding
static void test_rlp_string(void) {
    printf("Testing RLP string encoding...\n");
    
    // Empty string
    rlp_item_t* empty = rlp_string(NULL, 0);
    bytes_t encoded = rlp_encode(empty);
    assert(encoded.len == 1);
    assert(encoded.data[0] == 0x80);
    bytes_free(&encoded);
    rlp_item_free(empty);
    
    // Single byte < 0x80
    uint8_t byte1 = 0x42;
    rlp_item_t* single = rlp_string(&byte1, 1);
    encoded = rlp_encode(single);
    assert(encoded.len == 1);
    assert(encoded.data[0] == 0x42);
    bytes_free(&encoded);
    rlp_item_free(single);
    
    // Single byte >= 0x80
    uint8_t byte2 = 0x80;
    rlp_item_t* single2 = rlp_string(&byte2, 1);
    encoded = rlp_encode(single2);
    assert(encoded.len == 2);
    assert(encoded.data[0] == 0x81);
    assert(encoded.data[1] == 0x80);
    bytes_free(&encoded);
    rlp_item_free(single2);
    
    // Short string (< 56 bytes)
    uint8_t dog[] = {'d', 'o', 'g'};
    rlp_item_t* str = rlp_string(dog, 3);
    encoded = rlp_encode(str);
    assert(encoded.len == 4);
    assert(encoded.data[0] == 0x83);  // 0x80 + 3
    assert(memcmp(encoded.data + 1, dog, 3) == 0);
    bytes_free(&encoded);
    rlp_item_free(str);
    
    printf("✅ RLP string encoding passed\n");
}

// Test RLP uint64 encoding
static void test_rlp_uint64(void) {
    printf("Testing RLP uint64 encoding...\n");
    
    // Zero
    rlp_item_t* zero = rlp_uint64(0);
    bytes_t encoded = rlp_encode(zero);
    assert(encoded.len == 1);
    assert(encoded.data[0] == 0x80);  // Empty string
    bytes_free(&encoded);
    rlp_item_free(zero);
    
    // Small number (1 byte)
    rlp_item_t* small = rlp_uint64(15);
    encoded = rlp_encode(small);
    assert(encoded.len == 1);
    assert(encoded.data[0] == 0x0f);
    bytes_free(&encoded);
    rlp_item_free(small);
    
    // Number requiring 2 bytes
    rlp_item_t* med = rlp_uint64(1024);
    encoded = rlp_encode(med);
    assert(encoded.len == 3);
    assert(encoded.data[0] == 0x82);  // 0x80 + 2
    assert(encoded.data[1] == 0x04);
    assert(encoded.data[2] == 0x00);
    bytes_free(&encoded);
    rlp_item_free(med);
    
    printf("✅ RLP uint64 encoding passed\n");
}

// Test RLP list encoding
static void test_rlp_list(void) {
    printf("Testing RLP list encoding...\n");
    
    // Empty list
    rlp_item_t* empty_list = rlp_list_new();
    bytes_t encoded = rlp_encode(empty_list);
    assert(encoded.len == 1);
    assert(encoded.data[0] == 0xc0);
    bytes_free(&encoded);
    rlp_item_free(empty_list);
    
    // List with strings: ["cat", "dog"]
    rlp_item_t* list = rlp_list_new();
    uint8_t cat[] = {'c', 'a', 't'};
    uint8_t dog[] = {'d', 'o', 'g'};
    rlp_list_append(list, rlp_string(cat, 3));
    rlp_list_append(list, rlp_string(dog, 3));
    
    encoded = rlp_encode(list);
    // Expected: 0xc8 (0xc0 + 8), 0x83 cat, 0x83 dog
    assert(encoded.len == 9);
    assert(encoded.data[0] == 0xc8);  // 0xc0 + 8
    assert(encoded.data[1] == 0x83);
    assert(memcmp(encoded.data + 2, cat, 3) == 0);
    assert(encoded.data[5] == 0x83);
    assert(memcmp(encoded.data + 6, dog, 3) == 0);
    bytes_free(&encoded);
    rlp_item_free(list);
    
    printf("✅ RLP list encoding passed\n");
}

// Test nested lists
static void test_rlp_nested_list(void) {
    printf("Testing RLP nested lists...\n");
    
    // [ [], [[]], [ [], [[]] ] ]
    rlp_item_t* inner1 = rlp_list_new();
    
    rlp_item_t* inner2 = rlp_list_new();
    rlp_list_append(inner2, rlp_list_new());
    
    rlp_item_t* inner3 = rlp_list_new();
    rlp_list_append(inner3, rlp_list_new());
    rlp_item_t* inner3_2 = rlp_list_new();
    rlp_list_append(inner3_2, rlp_list_new());
    rlp_list_append(inner3, inner3_2);
    
    rlp_item_t* outer = rlp_list_new();
    rlp_list_append(outer, inner1);
    rlp_list_append(outer, inner2);
    rlp_list_append(outer, inner3);
    
    bytes_t encoded = rlp_encode(outer);
    // Expected: 0xc7 (header), 0xc0 (inner1), 0xc1 0xc0 (inner2), 0xc3 0xc0 0xc1 0xc0 (inner3)
    assert(encoded.len == 8);
    assert(encoded.data[0] == 0xc7);
    bytes_free(&encoded);
    rlp_item_free(outer);
    
    printf("✅ RLP nested lists passed\n");
}

// Test RLP decoding
static void test_rlp_decode(void) {
    printf("Testing RLP decoding...\n");
    
    // Decode empty string
    uint8_t empty_encoded[] = {0x80};
    rlp_item_t* empty = rlp_decode(empty_encoded, 1);
    assert(empty != NULL);
    assert(empty->type == RLP_TYPE_STRING);
    assert(empty->data.string.len == 0);
    rlp_item_free(empty);
    
    // Decode single byte
    uint8_t byte_encoded[] = {0x42};
    rlp_item_t* byte_item = rlp_decode(byte_encoded, 1);
    assert(byte_item != NULL);
    assert(byte_item->type == RLP_TYPE_STRING);
    assert(byte_item->data.string.len == 1);
    assert(byte_item->data.string.data[0] == 0x42);
    rlp_item_free(byte_item);
    
    // Decode string
    uint8_t dog_encoded[] = {0x83, 'd', 'o', 'g'};
    rlp_item_t* dog = rlp_decode(dog_encoded, 4);
    assert(dog != NULL);
    assert(dog->type == RLP_TYPE_STRING);
    assert(dog->data.string.len == 3);
    assert(memcmp(dog->data.string.data, "dog", 3) == 0);
    rlp_item_free(dog);
    
    // Decode list
    uint8_t list_encoded[] = {0xc8, 0x83, 'c', 'a', 't', 0x83, 'd', 'o', 'g'};
    rlp_item_t* list = rlp_decode(list_encoded, 9);
    assert(list != NULL);
    assert(list->type == RLP_TYPE_LIST);
    assert(list->data.list.count == 2);
    
    const rlp_item_t* item0 = rlp_get_list_item(list, 0);
    assert(item0->type == RLP_TYPE_STRING);
    assert(memcmp(item0->data.string.data, "cat", 3) == 0);
    
    const rlp_item_t* item1 = rlp_get_list_item(list, 1);
    assert(item1->type == RLP_TYPE_STRING);
    assert(memcmp(item1->data.string.data, "dog", 3) == 0);
    
    rlp_item_free(list);
    
    printf("✅ RLP decoding passed\n");
}

// Test encode/decode roundtrip
static void test_rlp_roundtrip(void) {
    printf("Testing RLP encode/decode roundtrip...\n");
    
    // Create complex structure
    rlp_item_t* list = rlp_list_new();
    rlp_list_append(list, rlp_uint64(0));
    rlp_list_append(list, rlp_uint64(255));
    rlp_list_append(list, rlp_uint64(65535));
    
    uint8_t data[] = {'h', 'e', 'l', 'l', 'o'};
    rlp_list_append(list, rlp_string(data, 5));
    
    rlp_item_t* nested = rlp_list_new();
    rlp_list_append(nested, rlp_uint64(42));
    rlp_list_append(list, nested);
    
    // Encode
    bytes_t encoded = rlp_encode(list);
    
    // Decode
    rlp_item_t* decoded = rlp_decode(encoded.data, encoded.len);
    assert(decoded != NULL);
    assert(decoded->type == RLP_TYPE_LIST);
    assert(decoded->data.list.count == 5);
    
    // Verify structure
    const rlp_item_t* item0 = rlp_get_list_item(decoded, 0);
    assert(item0->data.string.len == 0);  // Zero encoded as empty
    
    const rlp_item_t* item1 = rlp_get_list_item(decoded, 1);
    assert(item1->data.string.len == 1);
    assert(item1->data.string.data[0] == 0xff);
    
    const rlp_item_t* item3 = rlp_get_list_item(decoded, 3);
    assert(item3->data.string.len == 5);
    assert(memcmp(item3->data.string.data, "hello", 5) == 0);
    
    const rlp_item_t* item4 = rlp_get_list_item(decoded, 4);
    assert(item4->type == RLP_TYPE_LIST);
    assert(item4->data.list.count == 1);
    
    bytes_free(&encoded);
    rlp_item_free(list);
    rlp_item_free(decoded);
    
    printf("✅ RLP roundtrip passed\n");
}

// Test helper functions
static void test_rlp_helpers(void) {
    printf("Testing RLP helper functions...\n");
    
    // Test rlp_encode_empty_string
    bytes_t empty = rlp_encode_empty_string();
    assert(empty.len == 1);
    assert(empty.data[0] == 0x80);
    bytes_free(&empty);
    
    // Test rlp_encode_byte
    bytes_t byte_low = rlp_encode_byte(0x42);
    assert(byte_low.len == 1);
    assert(byte_low.data[0] == 0x42);
    bytes_free(&byte_low);
    
    bytes_t byte_high = rlp_encode_byte(0x80);
    assert(byte_high.len == 2);
    assert(byte_high.data[0] == 0x81);
    assert(byte_high.data[1] == 0x80);
    bytes_free(&byte_high);
    
    // Test rlp_encode_bytes
    uint8_t data[] = {'t', 'e', 's', 't'};
    bytes_t encoded = rlp_encode_bytes(data, 4);
    assert(encoded.len == 5);
    assert(encoded.data[0] == 0x84);
    assert(memcmp(encoded.data + 1, data, 4) == 0);
    bytes_free(&encoded);
    
    // Test rlp_encode_uint64_direct
    bytes_t num = rlp_encode_uint64_direct(1024);
    assert(num.len == 3);
    assert(num.data[0] == 0x82);
    bytes_free(&num);
    
    printf("✅ RLP helper functions passed\n");
}

// Test real-world Ethereum data
static void test_rlp_ethereum_examples(void) {
    printf("Testing Ethereum RLP examples...\n");
    
    // Example: Transaction data structure
    rlp_item_t* tx = rlp_list_new();
    
    // nonce = 0
    rlp_list_append(tx, rlp_uint64(0));
    
    // gasPrice = 20000000000 (20 gwei)
    rlp_list_append(tx, rlp_uint64(20000000000ULL));
    
    // gasLimit = 21000
    rlp_list_append(tx, rlp_uint64(21000));
    
    // to address (20 bytes)
    uint8_t to[20] = {0};
    rlp_list_append(tx, rlp_string(to, 20));
    
    // value = 1 ether (in wei)
    rlp_list_append(tx, rlp_uint64(1000000000000000000ULL));
    
    // data (empty)
    rlp_list_append(tx, rlp_string(NULL, 0));
    
    bytes_t encoded = rlp_encode(tx);
    assert(encoded.len > 0);
    
    // Decode and verify
    rlp_item_t* decoded = rlp_decode(encoded.data, encoded.len);
    assert(decoded != NULL);
    assert(decoded->type == RLP_TYPE_LIST);
    assert(decoded->data.list.count == 6);
    
    bytes_free(&encoded);
    rlp_item_free(tx);
    rlp_item_free(decoded);
    
    printf("✅ Ethereum RLP examples passed\n");
}

int main(void) {
    printf("Running RLP tests...\n\n");
    
    test_rlp_string();
    test_rlp_uint64();
    test_rlp_list();
    test_rlp_nested_list();
    test_rlp_decode();
    test_rlp_roundtrip();
    test_rlp_helpers();
    test_rlp_ethereum_examples();
    
    printf("\n✅ All RLP tests passed!\n");
    return 0;
}
