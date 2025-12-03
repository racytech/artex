#include "mpt.h"
#include "logger.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

/**
 * Test MPT state root determinism
 * Same keys inserted in different order should produce same root hash
 */

static void print_hash(const char *label, const mpt_hash_t *hash) {
    printf("%s: ", label);
    for (int i = 0; i < MPT_HASH_SIZE; i++) {
        printf("%02x", hash->bytes[i]);
    }
    printf("\n");
}

static void test_deterministic_root_simple(void) {
    printf("Test: Deterministic root (simple)... ");
    
    mpt_state_t state1 = {0};
    mpt_state_t state2 = {0};
    
    assert(mpt_init(&state1));
    assert(mpt_init(&state2));
    
    const uint8_t key1[] = {0x01, 0x02, 0x03};
    const uint8_t value1[] = {0xAA, 0xBB};
    const uint8_t key2[] = {0x04, 0x05, 0x06};
    const uint8_t value2[] = {0xCC, 0xDD};
    
    // Insert in order 1-2
    assert(mpt_insert(&state1, key1, sizeof(key1), value1, sizeof(value1)));
    assert(mpt_insert(&state1, key2, sizeof(key2), value2, sizeof(value2)));
    
    // Insert in order 2-1
    assert(mpt_insert(&state2, key2, sizeof(key2), value2, sizeof(value2)));
    assert(mpt_insert(&state2, key1, sizeof(key1), value1, sizeof(value1)));
    
    // Root hashes should be identical
    const mpt_hash_t *root1 = mpt_root(&state1);
    const mpt_hash_t *root2 = mpt_root(&state2);
    
    assert(root1 && root2);
    
    if (memcmp(root1->bytes, root2->bytes, MPT_HASH_SIZE) != 0) {
        printf("\nFAILED - roots differ:\n");
        print_hash("State1", root1);
        print_hash("State2", root2);
        assert(0);
    }
    
    mpt_destroy(&state1);
    mpt_destroy(&state2);
    printf("PASSED\n");
}

static void test_deterministic_root_complex(void) {
    printf("Test: Deterministic root (complex with common prefixes)... ");
    
    mpt_state_t state1 = {0};
    mpt_state_t state2 = {0};
    
    assert(mpt_init(&state1));
    assert(mpt_init(&state2));
    
    // Keys with common prefixes to test branch/extension nodes
    const uint8_t key1[] = {0x12, 0x34, 0x56, 0x01};
    const uint8_t key2[] = {0x12, 0x34, 0x56, 0x02};
    const uint8_t key3[] = {0x12, 0x34, 0x78, 0x01};
    const uint8_t key4[] = {0xAB, 0xCD, 0xEF, 0x01};
    
    const uint8_t value1[] = {0x01};
    const uint8_t value2[] = {0x02};
    const uint8_t value3[] = {0x03};
    const uint8_t value4[] = {0x04};
    
    // Insert in one order
    assert(mpt_insert(&state1, key1, sizeof(key1), value1, sizeof(value1)));
    assert(mpt_insert(&state1, key2, sizeof(key2), value2, sizeof(value2)));
    assert(mpt_insert(&state1, key3, sizeof(key3), value3, sizeof(value3)));
    assert(mpt_insert(&state1, key4, sizeof(key4), value4, sizeof(value4)));
    
    // Insert in reverse order
    assert(mpt_insert(&state2, key4, sizeof(key4), value4, sizeof(value4)));
    assert(mpt_insert(&state2, key3, sizeof(key3), value3, sizeof(value3)));
    assert(mpt_insert(&state2, key2, sizeof(key2), value2, sizeof(value2)));
    assert(mpt_insert(&state2, key1, sizeof(key1), value1, sizeof(value1)));
    
    const mpt_hash_t *root1 = mpt_root(&state1);
    const mpt_hash_t *root2 = mpt_root(&state2);
    
    assert(root1 && root2);
    
    if (memcmp(root1->bytes, root2->bytes, MPT_HASH_SIZE) != 0) {
        printf("\nFAILED - roots differ:\n");
        print_hash("State1", root1);
        print_hash("State2", root2);
        assert(0);
    }
    
    mpt_destroy(&state1);
    mpt_destroy(&state2);
    printf("PASSED\n");
}

static void test_root_after_delete(void) {
    printf("Test: Root hash after delete... ");
    
    mpt_state_t state1 = {0};
    mpt_state_t state2 = {0};
    
    assert(mpt_init(&state1));
    assert(mpt_init(&state2));
    
    const uint8_t key1[] = {0x01};
    const uint8_t key2[] = {0x02};
    const uint8_t key3[] = {0x03};
    const uint8_t value[] = {0xFF};
    
    // State1: insert 1,2,3 then delete 2
    assert(mpt_insert(&state1, key1, sizeof(key1), value, sizeof(value)));
    assert(mpt_insert(&state1, key2, sizeof(key2), value, sizeof(value)));
    assert(mpt_insert(&state1, key3, sizeof(key3), value, sizeof(value)));
    assert(mpt_delete(&state1, key2, sizeof(key2)));
    
    // State2: insert only 1,3
    assert(mpt_insert(&state2, key1, sizeof(key1), value, sizeof(value)));
    assert(mpt_insert(&state2, key3, sizeof(key3), value, sizeof(value)));
    
    const mpt_hash_t *root1 = mpt_root(&state1);
    const mpt_hash_t *root2 = mpt_root(&state2);
    
    assert(root1 && root2);
    
    if (memcmp(root1->bytes, root2->bytes, MPT_HASH_SIZE) != 0) {
        printf("\nFAILED - roots differ after delete:\n");
        print_hash("Delete path", root1);
        print_hash("Direct insert", root2);
        assert(0);
    }
    
    mpt_destroy(&state1);
    mpt_destroy(&state2);
    printf("PASSED\n");
}

static void test_empty_root(void) {
    printf("Test: Empty tree has correct Ethereum empty root... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    const mpt_hash_t *root = mpt_root(&state);
    assert(root);
    
    // Ethereum empty trie root is Keccak256 of empty byte array
    // Expected: c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
    const uint8_t expected_empty[32] = {
        0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c,
        0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
        0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b,
        0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70
    };
    
    // Compare the actual root with expected empty root
    bool matches = (memcmp(root->bytes, expected_empty, MPT_HASH_SIZE) == 0);
    assert(matches);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

int main(void) {
    printf("=== MPT Determinism Test Suite ===\n\n");
    
    log_init(LOG_LEVEL_INFO, stdout);
    
    test_empty_root();
    test_deterministic_root_simple();
    test_deterministic_root_complex();
    test_root_after_delete();
    
    printf("\n=== All determinism tests passed! ===\n");
    return 0;
}
