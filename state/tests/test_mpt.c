#include "mpt.h"
#include "logger.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

/**
 * Test suite for Hash-only MPT
 */

static void test_mpt_init_destroy(void) {
    printf("Test: MPT init/destroy... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    assert(mpt_is_empty(&state));
    assert(mpt_size(&state) == 0);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_insert_get(void) {
    printf("Test: MPT insert/get... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    // Insert some key-value pairs
    const uint8_t key1[] = {0x01, 0x02, 0x03, 0x04};
    const uint8_t value1[] = {0xAA, 0xBB, 0xCC, 0xDD};
    
    const uint8_t key2[] = {0x01, 0x02, 0xFF, 0xFF};
    const uint8_t value2[] = {0x11, 0x22, 0x33, 0x44, 0x55};
    
    assert(mpt_insert(&state, key1, sizeof(key1), value1, sizeof(value1)));
    assert(mpt_size(&state) == 1);
    assert(!mpt_is_empty(&state));
    
    assert(mpt_insert(&state, key2, sizeof(key2), value2, sizeof(value2)));
    assert(mpt_size(&state) == 2);
    
    // Retrieve values
    size_t val_len;
    const void *val = mpt_get(&state, key1, sizeof(key1), &val_len);
    assert(val != NULL);
    assert(val_len == sizeof(value1));
    assert(memcmp(val, value1, val_len) == 0);
    
    val = mpt_get(&state, key2, sizeof(key2), &val_len);
    assert(val != NULL);
    assert(val_len == sizeof(value2));
    assert(memcmp(val, value2, val_len) == 0);
    
    // Non-existent key
    const uint8_t key3[] = {0xFF, 0xFF, 0xFF, 0xFF};
    val = mpt_get(&state, key3, sizeof(key3), &val_len);
    assert(val == NULL);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_update(void) {
    printf("Test: MPT update existing key... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    const uint8_t key[] = {0xAA, 0xBB, 0xCC};
    const uint8_t value1[] = {0x01, 0x02};
    const uint8_t value2[] = {0xFF, 0xEE, 0xDD};
    
    // Insert
    assert(mpt_insert(&state, key, sizeof(key), value1, sizeof(value1)));
    assert(mpt_size(&state) == 1);
    
    // Update
    assert(mpt_insert(&state, key, sizeof(key), value2, sizeof(value2)));
    assert(mpt_size(&state) == 1); // Size shouldn't change
    
    // Verify updated value
    size_t val_len;
    const void *val = mpt_get(&state, key, sizeof(key), &val_len);
    assert(val != NULL);
    assert(val_len == sizeof(value2));
    assert(memcmp(val, value2, val_len) == 0);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_delete(void) {
    printf("Test: MPT delete... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    const uint8_t key1[] = {0x01, 0x02};
    const uint8_t value1[] = {0xAA};
    const uint8_t key2[] = {0x03, 0x04};
    const uint8_t value2[] = {0xBB};
    
    assert(mpt_insert(&state, key1, sizeof(key1), value1, sizeof(value1)));
    assert(mpt_insert(&state, key2, sizeof(key2), value2, sizeof(value2)));
    assert(mpt_size(&state) == 2);
    
    // Delete first key
    assert(mpt_delete(&state, key1, sizeof(key1)));
    assert(mpt_size(&state) == 1);
    assert(mpt_get(&state, key1, sizeof(key1), NULL) == NULL);
    
    // Second key should still exist
    size_t val_len;
    const void *val = mpt_get(&state, key2, sizeof(key2), &val_len);
    assert(val != NULL);
    assert(memcmp(val, value2, val_len) == 0);
    
    // Delete second key
    assert(mpt_delete(&state, key2, sizeof(key2)));
    assert(mpt_size(&state) == 0);
    assert(mpt_is_empty(&state));
    
    // Delete non-existent key
    assert(!mpt_delete(&state, key1, sizeof(key1)));
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_ethereum_address(void) {
    printf("Test: MPT with Ethereum addresses... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    // Simulate Ethereum addresses (20 bytes)
    uint8_t addr1[20] = {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                         0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                         0x99, 0xAA, 0xBB, 0xCC};
    uint8_t addr2[20] = {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
                         0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
                         0x99, 0xAA, 0xBB, 0xDD}; // Differs in last byte
    
    // Account balances (32 bytes)
    uint8_t balance1[32] = {0};
    balance1[31] = 100; // 100 wei
    uint8_t balance2[32] = {0};
    balance2[31] = 200; // 200 wei
    
    assert(mpt_insert(&state, addr1, 20, balance1, 32));
    assert(mpt_insert(&state, addr2, 20, balance2, 32));
    assert(mpt_size(&state) == 2);
    
    // Verify retrieval
    size_t val_len;
    const void *val = mpt_get(&state, addr1, 20, &val_len);
    assert(val != NULL);
    assert(val_len == 32);
    assert(((const uint8_t *)val)[31] == 100);
    
    val = mpt_get(&state, addr2, 20, &val_len);
    assert(val != NULL);
    assert(val_len == 32);
    assert(((const uint8_t *)val)[31] == 200);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_state_root(void) {
    printf("Test: MPT state root... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    // Empty state should have zero root
    const mpt_hash_t *root = mpt_root(&state);
    assert(root != NULL);
    bool all_zero = true;
    for (int i = 0; i < MPT_HASH_SIZE; i++) {
        if (root->bytes[i] != 0) {
            all_zero = false;
            break;
        }
    }
    assert(all_zero);
    
    // Insert data and verify root changes
    const uint8_t key[] = {0x01, 0x02, 0x03};
    const uint8_t value[] = {0xAA, 0xBB};
    assert(mpt_insert(&state, key, sizeof(key), value, sizeof(value)));
    
    root = mpt_root(&state);
    all_zero = true;
    for (int i = 0; i < MPT_HASH_SIZE; i++) {
        if (root->bytes[i] != 0) {
            all_zero = false;
            break;
        }
    }
    assert(!all_zero); // Root should be non-zero now
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_proof_generation(void) {
    printf("Test: MPT proof generation... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    const uint8_t key[] = {0x12, 0x34, 0x56};
    const uint8_t value[] = {0xAA, 0xBB, 0xCC};
    
    assert(mpt_insert(&state, key, sizeof(key), value, sizeof(value)));
    
    // Generate proof
    mpt_proof_t *proof = NULL;
    assert(mpt_prove(&state, key, sizeof(key), &proof));
    assert(proof != NULL);
    assert(proof->key_len == sizeof(key));
    assert(proof->value_len == sizeof(value));
    assert(memcmp(proof->key, key, sizeof(key)) == 0);
    assert(memcmp(proof->value, value, sizeof(value)) == 0);
    
    // Verify proof
    assert(mpt_verify_proof(proof));
    
    mpt_proof_free(proof);
    mpt_destroy(&state);
    printf("PASSED\n");
}

static void test_mpt_proof_nonexistent(void) {
    printf("Test: MPT proof for non-existent key... ");
    
    mpt_state_t state = {0};
    assert(mpt_init(&state));
    
    const uint8_t key[] = {0xFF, 0xFF};
    
    mpt_proof_t *proof = NULL;
    assert(!mpt_prove(&state, key, sizeof(key), &proof));
    assert(proof == NULL);
    
    mpt_destroy(&state);
    printf("PASSED\n");
}

int main(void) {
    printf("=== Hash-only MPT Test Suite ===\n\n");
    
    log_init(LOG_LEVEL_INFO, stdout);
    
    test_mpt_init_destroy();
    test_mpt_insert_get();
    test_mpt_update();
    test_mpt_delete();
    test_mpt_ethereum_address();
    test_mpt_state_root();
    test_mpt_proof_generation();
    test_mpt_proof_nonexistent();
    
    printf("\n=== All tests passed! ===\n");
    return 0;
}
