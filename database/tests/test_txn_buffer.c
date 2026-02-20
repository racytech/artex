/*
 * Transaction Buffer Tests - Atomic Multi-Key Updates
 *
 * Tests the transaction buffer that enables true atomicity for
 * multi-key updates without requiring MVCC infrastructure.
 */

#include "data_art.h"
#include "txn_buffer.h"
#include "page_manager.h"
#include "buffer_pool.h"
#include "wal.h"
#include "logger.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#define TEST_DB_PATH "test_txn_buffer.db"
#define TEST_WAL_PATH "test_txn_buffer_wal"
#define KEY_SIZE 32

// Test helper macros
#define TEST_ASSERT(cond, msg)                                                     \
    do                                                                             \
    {                                                                              \
        if (!(cond))                                                               \
        {                                                                          \
            fprintf(stderr, "❌ FAILED: %s\n   %s:%d\n", msg, __FILE__, __LINE__); \
            return false;                                                          \
        }                                                                          \
    } while (0)

#define PRINT_TEST_HEADER(name) \
    printf("\n🧪 Running: %s\n", name)

// Test helper functions
static void cleanup_test_files()
{
    unlink(TEST_DB_PATH);
    unlink(TEST_WAL_PATH);
    // Clean up WAL segments
    char seg_path[512];
    for (int i = 0; i < 10; i++)
    {
        snprintf(seg_path, sizeof(seg_path), "%s.%04d", TEST_WAL_PATH, i);
        unlink(seg_path);
    }
}

// ============================================================================
// Test 1: Basic Transaction Commit
// ============================================================================

static bool test_basic_commit()
{
    PRINT_TEST_HEADER("test_basic_commit");

    cleanup_test_files();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    TEST_ASSERT(pm != NULL, "Failed to create page manager");

    buffer_pool_t *bp = buffer_pool_create(&(buffer_pool_config_t){.capacity = 16}, pm);
    TEST_ASSERT(bp != NULL, "Failed to create buffer pool");

    wal_t *wal = wal_open(TEST_WAL_PATH, &(wal_config_t){.segment_size = 1024 * 1024}); // 1MB segments
    TEST_ASSERT(wal != NULL, "Failed to create WAL");

    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);
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
    buffer_pool_destroy(bp);
    wal_close(wal);
    page_manager_destroy(pm);
    cleanup_test_files();

    printf("   ✅ PASSED\n");
    return true;
}

// ============================================================================
// Test 2: Transaction Abort (Rollback)
// ============================================================================

static bool test_abort_rollback()
{
    PRINT_TEST_HEADER("test_abort_rollback");

    cleanup_test_files();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    buffer_pool_t *bp = buffer_pool_create(&(buffer_pool_config_t){.capacity = 16}, pm);
    wal_t *wal = wal_open(TEST_WAL_PATH, &(wal_config_t){.segment_size = 1024 * 1024});
    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);

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
    buffer_pool_destroy(bp);
    wal_close(wal);
    page_manager_destroy(pm);
    cleanup_test_files();

    printf("   ✅ PASSED\n");
    return true;
}

// ============================================================================
// Test 3: Mixed Insert/Delete in Transaction
// ============================================================================

static bool test_mixed_operations()
{
    PRINT_TEST_HEADER("test_mixed_operations");

    cleanup_test_files();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    buffer_pool_t *bp = buffer_pool_create(&(buffer_pool_config_t){.capacity = 16}, pm);
    wal_t *wal = wal_open(TEST_WAL_PATH, &(wal_config_t){.segment_size = 1024 * 1024});
    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);

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
    buffer_pool_destroy(bp);
    wal_close(wal);
    page_manager_destroy(pm);
    cleanup_test_files();

    printf("   ✅ PASSED\n");
    return true;
}

// ============================================================================
// Test 4: Large Transaction (Capacity Growth)
// ============================================================================

static bool test_large_transaction()
{
    PRINT_TEST_HEADER("test_large_transaction");

    cleanup_test_files();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    buffer_pool_t *bp = buffer_pool_create(&(buffer_pool_config_t){.capacity = 64}, pm);
    wal_t *wal = wal_open(TEST_WAL_PATH, &(wal_config_t){.segment_size = 1024 * 1024});
    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);

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
    buffer_pool_destroy(bp);
    wal_close(wal);
    page_manager_destroy(pm);
    cleanup_test_files();

    printf("   ✅ PASSED\n");
    return true;
}

// ============================================================================
// Test 5: Nested Transaction Rejection
// ============================================================================

static bool test_nested_transaction_rejection()
{
    PRINT_TEST_HEADER("test_nested_transaction_rejection");

    cleanup_test_files();

    page_manager_t *pm = page_manager_create(TEST_DB_PATH, false);
    buffer_pool_t *bp = buffer_pool_create(&(buffer_pool_config_t){.capacity = 16}, pm);
    wal_t *wal = wal_open(TEST_WAL_PATH, &(wal_config_t){.segment_size = 1024 * 1024});
    data_art_tree_t *tree = data_art_create(pm, bp, wal, KEY_SIZE);

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
    buffer_pool_destroy(bp);
    wal_close(wal);
    page_manager_destroy(pm);
    cleanup_test_files();

    printf("   ✅ PASSED\n");
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
            printf("   ❌ FAILED: %s\n", #test_func); \
        }                                             \
    } while (0)

    RUN_TEST(test_basic_commit);
    RUN_TEST(test_abort_rollback);
    RUN_TEST(test_mixed_operations);
    RUN_TEST(test_large_transaction);
    RUN_TEST(test_nested_transaction_rejection);

    printf("\n");
    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n");
    printf("\n");

    return (passed == total) ? 0 : 1;
}
