/*
 * Transaction Buffer Tests - Atomic Multi-Key Updates
 *
 * Tests the transaction buffer that enables true atomicity for
 * multi-key updates without requiring MVCC infrastructure.
 */

#include "data_art.h"
#include "txn_buffer.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define TEST_DIR "/tmp/test_txn_buffer"
#define KEY_SIZE 32

// Test helper macros
#define TEST_ASSERT(cond, msg)                                                     \
    do                                                                             \
    {                                                                              \
        if (!(cond))                                                               \
        {                                                                          \
            fprintf(stderr, "FAILED: %s\n   %s:%d\n", msg, __FILE__, __LINE__); \
            return false;                                                          \
        }                                                                          \
    } while (0)

#define PRINT_TEST_HEADER(name) \
    printf("\nRunning: %s\n", name)

// Test helper functions
static void cleanup_test_dir()
{
    system("rm -rf " TEST_DIR);
    sync();
    usleep(10000);
}

// ============================================================================
// Test 1: Basic Transaction Commit
// ============================================================================

static bool test_basic_commit()
{
    PRINT_TEST_HEADER("test_basic_commit");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Begin transaction
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin transaction");
    TEST_ASSERT(txn_id > 0, "Invalid transaction ID");
    TEST_ASSERT(tree->txn_buffer != NULL, "Transaction buffer not created");

    // Insert multiple keys (buffered)
    uint8_t key1[KEY_SIZE] = "key1";
    uint8_t key2[KEY_SIZE] = "key2";
    uint8_t key3[KEY_SIZE] = "key3";

    TEST_ASSERT(data_art_insert(tree, key1, KEY_SIZE, "value1", 6),
                "Failed to insert key1");
    TEST_ASSERT(data_art_insert(tree, key2, KEY_SIZE, "value2", 6),
                "Failed to insert key2");
    TEST_ASSERT(data_art_insert(tree, key3, KEY_SIZE, "value3", 6),
                "Failed to insert key3");

    // Keys should NOT be in tree yet (buffered)
    TEST_ASSERT(data_art_size(tree) == 0, "Keys should not be in tree yet");
    TEST_ASSERT(!data_art_contains(tree, key1, KEY_SIZE), "key1 should not exist yet");

    // Commit transaction
    TEST_ASSERT(data_art_commit_txn(tree), "Failed to commit transaction");
    TEST_ASSERT(tree->txn_buffer == NULL, "Transaction buffer not cleaned up");
    TEST_ASSERT(tree->current_txn_id == 0, "Transaction ID not reset");

    // NOW keys should be in tree
    TEST_ASSERT(data_art_size(tree) == 3, "Tree should have 3 keys after commit");
    TEST_ASSERT(data_art_contains(tree, key1, KEY_SIZE), "key1 should exist");
    TEST_ASSERT(data_art_contains(tree, key2, KEY_SIZE), "key2 should exist");
    TEST_ASSERT(data_art_contains(tree, key3, KEY_SIZE), "key3 should exist");

    // Verify values
    size_t val_len;
    const void *val1 = data_art_get(tree, key1, KEY_SIZE, &val_len);
    TEST_ASSERT(val1 != NULL && val_len == 6, "Failed to get key1");
    TEST_ASSERT(memcmp(val1, "value1", 6) == 0, "key1 value mismatch");

    // Cleanup
    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 2: Transaction Abort (Rollback)
// ============================================================================

static bool test_abort_rollback()
{
    PRINT_TEST_HEADER("test_abort_rollback");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    // Insert some initial data (no transaction)
    uint8_t existing_key[KEY_SIZE] = "existing";
    TEST_ASSERT(data_art_insert(tree, existing_key, KEY_SIZE, "exists", 6),
                "Failed to insert existing key");
    TEST_ASSERT(data_art_size(tree) == 1, "Tree should have 1 key");

    // Begin transaction
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin transaction");

    // Insert multiple keys (buffered)
    uint8_t key1[KEY_SIZE] = "new1";
    uint8_t key2[KEY_SIZE] = "new2";
    uint8_t key3[KEY_SIZE] = "new3";

    TEST_ASSERT(data_art_insert(tree, key1, KEY_SIZE, "value1", 6),
                "Failed to insert key1");
    TEST_ASSERT(data_art_insert(tree, key2, KEY_SIZE, "value2", 6),
                "Failed to insert key2");
    TEST_ASSERT(data_art_insert(tree, key3, KEY_SIZE, "value3", 6),
                "Failed to insert key3");

    // Tree should still have only 1 key (buffered)
    TEST_ASSERT(data_art_size(tree) == 1, "Tree should still have 1 key");

    // ABORT transaction
    TEST_ASSERT(data_art_abort_txn(tree), "Failed to abort transaction");
    TEST_ASSERT(tree->txn_buffer == NULL, "Transaction buffer not cleaned up");

    // Tree should STILL have only 1 key (abort worked)
    TEST_ASSERT(data_art_size(tree) == 1, "Tree should still have 1 key after abort");
    TEST_ASSERT(data_art_contains(tree, existing_key, KEY_SIZE),
                "Existing key should still exist");
    TEST_ASSERT(!data_art_contains(tree, key1, KEY_SIZE),
                "key1 should NOT exist (rolled back)");
    TEST_ASSERT(!data_art_contains(tree, key2, KEY_SIZE),
                "key2 should NOT exist (rolled back)");
    TEST_ASSERT(!data_art_contains(tree, key3, KEY_SIZE),
                "key3 should NOT exist (rolled back)");

    // Cleanup
    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 3: Mixed Insert/Delete in Transaction
// ============================================================================

static bool test_mixed_operations()
{
    PRINT_TEST_HEADER("test_mixed_operations");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    // Insert initial data
    uint8_t key1[KEY_SIZE] = "key1";
    uint8_t key2[KEY_SIZE] = "key2";
    uint8_t key3[KEY_SIZE] = "key3";

    TEST_ASSERT(data_art_insert(tree, key1, KEY_SIZE, "value1", 6),
                "Failed to insert key1");
    TEST_ASSERT(data_art_insert(tree, key2, KEY_SIZE, "value2", 6),
                "Failed to insert key2");
    TEST_ASSERT(data_art_size(tree) == 2, "Tree should have 2 keys");

    // Begin transaction with mixed operations
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin transaction");

    // Delete key1, insert key3
    TEST_ASSERT(data_art_delete(tree, key1, KEY_SIZE), "Failed to delete key1");
    TEST_ASSERT(data_art_insert(tree, key3, KEY_SIZE, "value3", 6),
                "Failed to insert key3");

    // Tree should still have 2 keys (operations buffered)
    TEST_ASSERT(data_art_size(tree) == 2, "Tree should still have 2 keys");
    TEST_ASSERT(data_art_contains(tree, key1, KEY_SIZE), "key1 should still exist");

    // Commit
    TEST_ASSERT(data_art_commit_txn(tree), "Failed to commit");

    // Now changes should be applied
    TEST_ASSERT(data_art_size(tree) == 2, "Tree should have 2 keys after commit");
    TEST_ASSERT(!data_art_contains(tree, key1, KEY_SIZE), "key1 should be deleted");
    TEST_ASSERT(data_art_contains(tree, key2, KEY_SIZE), "key2 should exist");
    TEST_ASSERT(data_art_contains(tree, key3, KEY_SIZE), "key3 should exist");

    // Cleanup
    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 4: Large Transaction (Capacity Growth)
// ============================================================================

static bool test_large_transaction()
{
    PRINT_TEST_HEADER("test_large_transaction");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin transaction");

    // Insert 100 keys (will cause buffer growth)
    uint8_t key[KEY_SIZE];
    char value[32];

    for (int i = 0; i < 100; i++)
    {
        snprintf((char *)key, KEY_SIZE, "key_%05d", i);
        snprintf(value, sizeof(value), "value_%05d", i);

        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, value, strlen(value) + 1),
                    "Failed to insert key");
    }

    // Tree should be empty (all buffered)
    TEST_ASSERT(data_art_size(tree) == 0, "Tree should be empty");

    // Commit
    TEST_ASSERT(data_art_commit_txn(tree), "Failed to commit");

    // All 100 keys should now be in tree
    TEST_ASSERT(data_art_size(tree) == 100, "Tree should have 100 keys");

    // Verify a few
    snprintf((char *)key, KEY_SIZE, "key_%05d", 0);
    TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "key_00000 should exist");

    snprintf((char *)key, KEY_SIZE, "key_%05d", 50);
    TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "key_00050 should exist");

    snprintf((char *)key, KEY_SIZE, "key_%05d", 99);
    TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "key_00099 should exist");

    // Cleanup
    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 5: Nested Transaction Rejection
// ============================================================================

static bool test_nested_transaction_rejection()
{
    PRINT_TEST_HEADER("test_nested_transaction_rejection");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    uint64_t txn_id1, txn_id2;

    // Begin first transaction
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id1), "Failed to begin first transaction");

    // Try to begin nested transaction (should fail)
    TEST_ASSERT(!data_art_begin_txn(tree, &txn_id2),
                "Should not allow nested transactions");

    // First transaction should still be active
    TEST_ASSERT(tree->current_txn_id == txn_id1, "First transaction should still be active");

    // Commit first transaction
    TEST_ASSERT(data_art_commit_txn(tree), "Failed to commit");

    // Now we should be able to start a new transaction
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id2),
                "Should allow new transaction after commit");
    TEST_ASSERT(txn_id2 > txn_id1, "New transaction should have higher ID");

    TEST_ASSERT(data_art_abort_txn(tree), "Failed to abort");

    // Cleanup
    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 6: Batch Insert Basic
// ============================================================================

static bool test_batch_insert_basic()
{
    PRINT_TEST_HEADER("test_batch_insert_basic");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Prepare 100 keys
    const int COUNT = 100;
    uint8_t key_data[COUNT][KEY_SIZE];
    uint8_t value_data[COUNT][32];
    const uint8_t *keys[COUNT];
    size_t key_lens[COUNT];
    const void *values[COUNT];
    size_t value_lens[COUNT];

    for (int i = 0; i < COUNT; i++) {
        memset(key_data[i], 0, KEY_SIZE);
        snprintf((char *)key_data[i], KEY_SIZE, "batch_key_%04d", i);
        snprintf((char *)value_data[i], 32, "val_%04d", i);
        keys[i] = key_data[i];
        key_lens[i] = KEY_SIZE;
        values[i] = value_data[i];
        value_lens[i] = strlen((char *)value_data[i]) + 1;
    }

    // Batch insert
    TEST_ASSERT(data_art_insert_batch(tree, keys, key_lens, values, value_lens, COUNT),
                "Batch insert failed");

    // Verify all 100 keys present
    for (int i = 0; i < COUNT; i++) {
        size_t vlen;
        const void *val = data_art_get(tree, key_data[i], KEY_SIZE, &vlen);
        TEST_ASSERT(val != NULL, "Key not found after batch insert");
        TEST_ASSERT(memcmp(val, value_data[i], vlen) == 0, "Value mismatch");
    }

    TEST_ASSERT(tree->size == (size_t)COUNT, "Tree size should be 100");

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 7: Batch Insert Atomicity (Failure Rolls Back)
// ============================================================================

static bool test_batch_insert_atomicity()
{
    PRINT_TEST_HEADER("test_batch_insert_atomicity");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Prepare 51 keys -- last one has NULL value (should fail)
    const int COUNT = 51;
    uint8_t key_data[COUNT][KEY_SIZE];
    uint8_t value_data[COUNT][32];
    const uint8_t *keys[COUNT];
    size_t key_lens[COUNT];
    const void *values[COUNT];
    size_t value_lens[COUNT];

    for (int i = 0; i < COUNT; i++) {
        memset(key_data[i], 0, KEY_SIZE);
        snprintf((char *)key_data[i], KEY_SIZE, "atom_key_%04d", i);
        snprintf((char *)value_data[i], 32, "val_%04d", i);
        keys[i] = key_data[i];
        key_lens[i] = KEY_SIZE;
        values[i] = value_data[i];
        value_lens[i] = strlen((char *)value_data[i]) + 1;
    }

    // Make the last entry invalid (NULL value)
    values[COUNT - 1] = NULL;
    value_lens[COUNT - 1] = 0;

    // Batch should fail
    bool result = data_art_insert_batch(tree, keys, key_lens, values, value_lens, COUNT);
    TEST_ASSERT(!result, "Batch with NULL value should fail");

    // Tree should be empty -- all operations rolled back
    TEST_ASSERT(tree->size == 0, "Tree should be empty after failed batch");

    // Verify no keys are present
    for (int i = 0; i < COUNT - 1; i++) {
        size_t vlen;
        const void *val = data_art_get(tree, key_data[i], KEY_SIZE, &vlen);
        TEST_ASSERT(val == NULL, "Key should not exist after rollback");
    }

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 8: Batch Mixed Operations (Insert + Delete)
// ============================================================================

static bool test_batch_mixed_ops()
{
    PRINT_TEST_HEADER("test_batch_mixed_ops");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Insert 50 keys individually first
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_key_%04d", i);
        char val[32];
        snprintf(val, 32, "old_val_%04d", i);
        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, val, strlen(val) + 1),
                    "Pre-insert failed");
    }
    TEST_ASSERT(tree->size == 50, "Should have 50 keys after pre-insert");

    // Build batch: delete keys 10-29 (20 deletes) + insert keys 50-79 (30 inserts)
    const int NUM_OPS = 50;
    data_art_batch_op_t ops[NUM_OPS];
    uint8_t op_keys[NUM_OPS][KEY_SIZE];

    // 20 deletes (keys 10-29)
    for (int i = 0; i < 20; i++) {
        memset(op_keys[i], 0, KEY_SIZE);
        snprintf((char *)op_keys[i], KEY_SIZE, "mixed_key_%04d", i + 10);
        ops[i].type = BATCH_OP_DELETE;
        ops[i].key = op_keys[i];
        ops[i].key_len = KEY_SIZE;
        ops[i].value = NULL;
        ops[i].value_len = 0;
    }

    // 30 inserts (keys 50-79)
    uint8_t new_values[30][32];
    for (int i = 0; i < 30; i++) {
        memset(op_keys[20 + i], 0, KEY_SIZE);
        snprintf((char *)op_keys[20 + i], KEY_SIZE, "mixed_key_%04d", i + 50);
        snprintf((char *)new_values[i], 32, "new_val_%04d", i + 50);
        ops[20 + i].type = BATCH_OP_INSERT;
        ops[20 + i].key = op_keys[20 + i];
        ops[20 + i].key_len = KEY_SIZE;
        ops[20 + i].value = new_values[i];
        ops[20 + i].value_len = strlen((char *)new_values[i]) + 1;
    }

    // Execute batch
    TEST_ASSERT(data_art_batch(tree, ops, NUM_OPS), "Mixed batch failed");

    // Verify: 50 - 20 + 30 = 60 keys
    TEST_ASSERT(tree->size == 60, "Tree should have 60 keys");

    // Keys 0-9 should exist (original, not deleted)
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_key_%04d", i);
        size_t vlen;
        TEST_ASSERT(data_art_get(tree, key, KEY_SIZE, &vlen) != NULL,
                    "Key 0-9 should still exist");
    }

    // Keys 10-29 should NOT exist (deleted)
    for (int i = 10; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_key_%04d", i);
        size_t vlen;
        TEST_ASSERT(data_art_get(tree, key, KEY_SIZE, &vlen) == NULL,
                    "Deleted key should not exist");
    }

    // Keys 30-79 should exist (30-49 original, 50-79 new)
    for (int i = 30; i < 80; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_key_%04d", i);
        size_t vlen;
        TEST_ASSERT(data_art_get(tree, key, KEY_SIZE, &vlen) != NULL,
                    "Key 30-79 should exist");
    }

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 9: Batch Insert Persistence (mmap)
// ============================================================================

static bool test_batch_persistence()
{
    PRINT_TEST_HEADER("test_batch_persistence");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    // Phase 1: batch insert 100 keys, then close
    {
        data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
        TEST_ASSERT(tree != NULL, "Failed to create tree");

        const int COUNT = 100;
        uint8_t key_data[COUNT][KEY_SIZE];
        uint8_t value_data[COUNT][32];
        const uint8_t *keys[COUNT];
        size_t key_lens[COUNT];
        const void *values[COUNT];
        size_t value_lens[COUNT];

        for (int i = 0; i < COUNT; i++) {
            memset(key_data[i], 0, KEY_SIZE);
            snprintf((char *)key_data[i], KEY_SIZE, "recov_key_%04d", i);
            snprintf((char *)value_data[i], 32, "recov_val_%04d", i);
            keys[i] = key_data[i];
            key_lens[i] = KEY_SIZE;
            values[i] = value_data[i];
            value_lens[i] = strlen((char *)value_data[i]) + 1;
        }

        TEST_ASSERT(data_art_insert_batch(tree, keys, key_lens, values, value_lens, COUNT),
                    "Batch insert failed");
        TEST_ASSERT(tree->size == (size_t)COUNT, "Tree should have 100 keys");

        data_art_destroy(tree);
    }

    // Phase 2: reopen and verify all keys persisted via mmap
    {
        data_art_tree_t *tree = data_art_open(TEST_DIR "/art.dat", KEY_SIZE);
        TEST_ASSERT(tree != NULL, "Failed to reopen tree");

        // Verify all 100 keys recovered
        const int COUNT = 100;
        for (int i = 0; i < COUNT; i++) {
            uint8_t key[KEY_SIZE];
            memset(key, 0, KEY_SIZE);
            snprintf((char *)key, KEY_SIZE, "recov_key_%04d", i);
            size_t vlen;
            const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
            TEST_ASSERT(val != NULL, "Recovered key not found");

            char expected[32];
            snprintf(expected, 32, "recov_val_%04d", i);
            TEST_ASSERT(memcmp(val, expected, strlen(expected) + 1) == 0,
                        "Recovered value mismatch");
        }

        TEST_ASSERT(tree->size == (size_t)COUNT, "Recovered tree should have 100 keys");

        data_art_destroy(tree);
    }

    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 10: Empty Batch (No-op)
// ============================================================================

static bool test_batch_empty()
{
    PRINT_TEST_HEADER("test_batch_empty");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Empty insert batch
    TEST_ASSERT(data_art_insert_batch(tree, NULL, NULL, NULL, NULL, 0),
                "Empty insert batch should succeed");
    TEST_ASSERT(tree->size == 0, "Tree should be empty");

    // Empty mixed batch
    TEST_ASSERT(data_art_batch(tree, NULL, 0),
                "Empty mixed batch should succeed");
    TEST_ASSERT(tree->size == 0, "Tree should still be empty");

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 11: Optimized Commit -- 1000 Keys (No Deadlock)
// ============================================================================

static bool test_optimized_commit_1000_keys()
{
    PRINT_TEST_HEADER("test_optimized_commit_1000_keys");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Begin explicit transaction and buffer 1000 keys
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin txn");

    for (int i = 0; i < 1000; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "opt_key_%06d", i);
        char value[32];
        snprintf(value, sizeof(value), "val_%06d", i);

        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, value, strlen(value) + 1),
                    "Failed to buffer insert");
    }

    // Nothing committed yet
    TEST_ASSERT(data_art_size(tree) == 0, "Tree should be empty before commit");

    // Commit -- this is where the optimized path runs.
    // If internal functions tried to re-acquire write_lock, this would deadlock.
    TEST_ASSERT(data_art_commit_txn(tree), "Commit of 1000 keys failed (possible deadlock)");

    TEST_ASSERT(data_art_size(tree) == 1000, "Tree should have 1000 keys");

    // Spot-check a few keys
    for (int i = 0; i < 1000; i += 100) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "opt_key_%06d", i);
        size_t vlen;
        const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
        TEST_ASSERT(val != NULL, "Key not found after optimized commit");

        char expected[32];
        snprintf(expected, sizeof(expected), "val_%06d", i);
        TEST_ASSERT(memcmp(val, expected, strlen(expected) + 1) == 0, "Value mismatch");
    }

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 12: Optimized Commit -- Root Published Once
// ============================================================================

static bool test_optimized_commit_root_once()
{
    PRINT_TEST_HEADER("test_optimized_commit_root_once");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);
    TEST_ASSERT(tree != NULL, "Failed to create tree");

    // Record root before commit
    uint64_t root_before = atomic_load(&tree->committed_root_page_id);
    TEST_ASSERT(root_before == 0, "Root should be 0 (empty tree)");

    // Begin txn, insert 50 keys
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin txn");

    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "root_test_%04d", i);
        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, "val", 4),
                    "Failed to buffer insert");
    }

    // Root should still be 0 (buffered, not committed)
    uint64_t root_during = atomic_load(&tree->committed_root_page_id);
    TEST_ASSERT(root_during == 0, "Root should not change during buffering");

    // Commit
    TEST_ASSERT(data_art_commit_txn(tree), "Commit failed");

    // Root should now be non-zero (published once at end)
    uint64_t root_after = atomic_load(&tree->committed_root_page_id);
    TEST_ASSERT(root_after != 0, "Root should be published after commit");
    TEST_ASSERT(root_after == tree->root.page_id,
                "Published root should match tree root");

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 13: Optimized Commit -- Persistence After Explicit Txn
// ============================================================================

static bool test_optimized_commit_persistence()
{
    PRINT_TEST_HEADER("test_optimized_commit_persistence");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    // Phase 1: insert 200 keys via explicit txn, close
    {
        data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

        uint64_t txn_id;
        TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin txn");

        for (int i = 0; i < 200; i++) {
            uint8_t key[KEY_SIZE];
            memset(key, 0, KEY_SIZE);
            snprintf((char *)key, KEY_SIZE, "wal_recov_%05d", i);
            char value[32];
            snprintf(value, sizeof(value), "v_%05d", i);
            TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, value, strlen(value) + 1),
                        "Failed to buffer insert");
        }

        TEST_ASSERT(data_art_commit_txn(tree), "Commit failed");
        TEST_ASSERT(data_art_size(tree) == 200, "Should have 200 keys");

        data_art_destroy(tree);
    }

    // Phase 2: reopen and verify all keys persisted via mmap
    {
        data_art_tree_t *tree = data_art_open(TEST_DIR "/art.dat", KEY_SIZE);
        TEST_ASSERT(tree != NULL, "Failed to reopen tree");

        TEST_ASSERT(data_art_size(tree) == 200, "Reopened tree should have 200 keys");

        // Verify all keys and values
        for (int i = 0; i < 200; i++) {
            uint8_t key[KEY_SIZE];
            memset(key, 0, KEY_SIZE);
            snprintf((char *)key, KEY_SIZE, "wal_recov_%05d", i);
            size_t vlen;
            const void *val = data_art_get(tree, key, KEY_SIZE, &vlen);
            TEST_ASSERT(val != NULL, "Recovered key not found");

            char expected[32];
            snprintf(expected, sizeof(expected), "v_%05d", i);
            TEST_ASSERT(memcmp(val, expected, strlen(expected) + 1) == 0,
                        "Recovered value mismatch");
        }

        data_art_destroy(tree);
    }

    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 14: Optimized Commit -- Mixed Insert+Delete via Explicit Txn
// ============================================================================

static bool test_optimized_commit_mixed()
{
    PRINT_TEST_HEADER("test_optimized_commit_mixed");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    // Pre-insert 50 keys (auto-commit)
    for (int i = 0; i < 50; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        char val[32];
        snprintf(val, sizeof(val), "old_%05d", i);
        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, val, strlen(val) + 1),
                    "Pre-insert failed");
    }
    TEST_ASSERT(data_art_size(tree) == 50, "Should have 50 keys");

    // Begin explicit txn: delete keys 10-29 (20 deletes) + insert keys 50-79 (30 inserts)
    uint64_t txn_id;
    TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin txn");

    for (int i = 10; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        TEST_ASSERT(data_art_delete(tree, key, KEY_SIZE), "Failed to buffer delete");
    }

    for (int i = 50; i < 80; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        char val[32];
        snprintf(val, sizeof(val), "new_%05d", i);
        TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, val, strlen(val) + 1),
                    "Failed to buffer insert");
    }

    // Tree should still show 50 keys (changes buffered)
    TEST_ASSERT(data_art_size(tree) == 50, "Size should not change during buffering");

    // Commit
    TEST_ASSERT(data_art_commit_txn(tree), "Mixed commit failed");

    // Verify: 50 - 20 + 30 = 60 keys
    TEST_ASSERT(data_art_size(tree) == 60, "Should have 60 keys after mixed commit");

    // Keys 0-9: present (original)
    for (int i = 0; i < 10; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "Key 0-9 should exist");
    }

    // Keys 10-29: deleted
    for (int i = 10; i < 30; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        TEST_ASSERT(!data_art_contains(tree, key, KEY_SIZE), "Key 10-29 should be deleted");
    }

    // Keys 30-79: present (30-49 original, 50-79 new)
    for (int i = 30; i < 80; i++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "mixed_%05d", i);
        TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "Key 30-79 should exist");
    }

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Test 15: Optimized Commit -- Consecutive Transactions
// ============================================================================

static bool test_optimized_commit_consecutive_txns()
{
    PRINT_TEST_HEADER("test_optimized_commit_consecutive_txns");

    cleanup_test_dir();
    system("mkdir -p " TEST_DIR);

    data_art_tree_t *tree = data_art_create(TEST_DIR "/art.dat", KEY_SIZE);

    // Run 10 consecutive transactions, each inserting 50 keys
    for (int t = 0; t < 10; t++) {
        uint64_t txn_id;
        TEST_ASSERT(data_art_begin_txn(tree, &txn_id), "Failed to begin txn");

        for (int i = 0; i < 50; i++) {
            uint8_t key[KEY_SIZE];
            memset(key, 0, KEY_SIZE);
            snprintf((char *)key, KEY_SIZE, "consec_%02d_%04d", t, i);
            char val[32];
            snprintf(val, sizeof(val), "v_%02d_%04d", t, i);
            TEST_ASSERT(data_art_insert(tree, key, KEY_SIZE, val, strlen(val) + 1),
                        "Failed to buffer insert");
        }

        TEST_ASSERT(data_art_commit_txn(tree), "Commit failed");
        TEST_ASSERT(data_art_size(tree) == (size_t)(50 * (t + 1)),
                    "Size mismatch after commit");
    }

    TEST_ASSERT(data_art_size(tree) == 500, "Final tree should have 500 keys");

    // Spot-check keys from different transactions
    for (int t = 0; t < 10; t++) {
        uint8_t key[KEY_SIZE];
        memset(key, 0, KEY_SIZE);
        snprintf((char *)key, KEY_SIZE, "consec_%02d_%04d", t, 25);
        TEST_ASSERT(data_art_contains(tree, key, KEY_SIZE), "Key from txn should exist");
    }

    data_art_destroy(tree);
    cleanup_test_dir();

    printf("   PASSED\n");
    return true;
}

// ============================================================================
// Main Test Runner
// ============================================================================

int main()
{
    printf("\n");
    printf("========================================\n");
    printf(" Transaction Buffer Tests\n");
    printf("========================================\n");

    int passed = 0;
    int total = 0;

#define RUN_TEST(test_func)                           \
    do                                                \
    {                                                 \
        total++;                                      \
        if (test_func())                              \
        {                                             \
            passed++;                                 \
        }                                             \
        else                                          \
        {                                             \
            printf("   FAILED: %s\n", #test_func);   \
        }                                             \
    } while (0)

    RUN_TEST(test_basic_commit);
    RUN_TEST(test_abort_rollback);
    RUN_TEST(test_mixed_operations);
    RUN_TEST(test_large_transaction);
    RUN_TEST(test_nested_transaction_rejection);
    RUN_TEST(test_batch_insert_basic);
    RUN_TEST(test_batch_insert_atomicity);
    RUN_TEST(test_batch_mixed_ops);
    RUN_TEST(test_batch_persistence);
    RUN_TEST(test_batch_empty);
    RUN_TEST(test_optimized_commit_1000_keys);
    RUN_TEST(test_optimized_commit_root_once);
    RUN_TEST(test_optimized_commit_persistence);
    RUN_TEST(test_optimized_commit_mixed);
    RUN_TEST(test_optimized_commit_consecutive_txns);

    printf("\n");
    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n");
    printf("\n");

    return (passed == total) ? 0 : 1;
}
