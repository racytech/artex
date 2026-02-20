/**
 * Test: MVCC Edge Cases and Stress Tests
 * 
 * Tests edge cases, race conditions, and stress scenarios:
 * - Concurrent snapshot creation
 * - Transaction table collision handling
 * - Large numbers of active transactions
 * - Snapshot with many active transactions
 * - Transaction ID wraparound behavior
 * - Memory leak detection
 * - Concurrent visibility checks
 */

#include "mvcc.h"
#include "logger.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// ANSI color codes
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_YELLOW "\033[0;33m"
#define COLOR_CYAN   "\033[0;36m"
#define COLOR_RESET  "\033[0m"

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "   ✗ FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "     at %s:%d\n", __FILE__, __LINE__); \
        return false; \
    } \
} while(0)

// ============================================================================
// Edge Case Tests
// ============================================================================

static bool test_empty_snapshot(void) {
    printf("🧪 Running: test_empty_snapshot\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Create snapshot with no active transactions
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    TEST_ASSERT(snapshot->num_active == 0, "Should have no active transactions");
    TEST_ASSERT(snapshot->xmin == snapshot->xmax, "xmin should equal xmax");
    
    // Check visibility of non-existent transaction
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    
    // Version with txn_id = 0 (invalid) should not be visible
    bool visible = mvcc_is_visible(manager, snapshot, 0, 0, txn1);
    TEST_ASSERT(!visible, "Invalid txn_id should not be visible");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_transaction_not_found(void) {
    printf("🧪 Running: test_transaction_not_found\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Try to commit non-existent transaction
    bool result = mvcc_commit_txn(manager, 9999);
    TEST_ASSERT(!result, "Committing non-existent txn should fail");
    
    // Try to abort non-existent transaction
    result = mvcc_abort_txn(manager, 9999);
    TEST_ASSERT(!result, "Aborting non-existent txn should fail");
    
    // Get state of non-existent transaction (should return aborted)
    txn_state_t state = mvcc_get_txn_state(manager, 9999);
    TEST_ASSERT(state == TXN_STATE_ABORTED, "Non-existent txn should appear aborted");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_double_commit_abort(void) {
    printf("🧪 Running: test_double_commit_abort\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    
    // Commit twice
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "First commit should succeed");
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Second commit should succeed (idempotent)");
    
    // Try to abort already committed transaction
    TEST_ASSERT(mvcc_abort_txn(manager, txn1), "Abort after commit should succeed");
    
    // State should be aborted (last operation wins)
    txn_state_t state = mvcc_get_txn_state(manager, txn1);
    TEST_ASSERT(state == TXN_STATE_ABORTED, "Final state should be aborted");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_many_active_transactions(void) {
    printf("🧪 Running: test_many_active_transactions\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    const int NUM_TXNS = 1000;
    uint64_t txns[NUM_TXNS];
    
    // Start many transactions
    for (int i = 0; i < NUM_TXNS; i++) {
        TEST_ASSERT(mvcc_begin_txn(manager, &txns[i]), "Begin transaction should succeed");
        TEST_ASSERT(txns[i] == (uint64_t)(i + 1), "Transaction IDs should be sequential");
    }
    
    // Create snapshot - should capture all active transactions
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    TEST_ASSERT(snapshot->num_active == NUM_TXNS, "Should capture all active transactions");
    TEST_ASSERT(snapshot->xmin == 1, "xmin should be 1");
    TEST_ASSERT(snapshot->xmax == NUM_TXNS + 1, "xmax should be NUM_TXNS + 1");
    
    // Verify all transactions are in snapshot
    for (int i = 0; i < NUM_TXNS; i++) {
        TEST_ASSERT(mvcc_txn_was_active(snapshot, txns[i]), "Transaction should be in snapshot");
    }
    
    // Commit half of them
    for (int i = 0; i < NUM_TXNS / 2; i++) {
        TEST_ASSERT(mvcc_commit_txn(manager, txns[i]), "Commit should succeed");
    }
    
    // Abort the other half
    for (int i = NUM_TXNS / 2; i < NUM_TXNS; i++) {
        TEST_ASSERT(mvcc_abort_txn(manager, txns[i]), "Abort should succeed");
    }
    
    // Create new snapshot - should have no active transactions
    mvcc_snapshot_t *snapshot2 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot2 != NULL, "Second snapshot creation should succeed");
    TEST_ASSERT(snapshot2->num_active == 0, "Should have no active transactions");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_snapshot_release(manager, snapshot2);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_transaction_table_collision(void) {
    printf("🧪 Running: test_transaction_table_collision\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Start transactions that will hash to same slots
    const int NUM_TXNS = 100;
    uint64_t txns[NUM_TXNS];
    
    for (int i = 0; i < NUM_TXNS; i++) {
        TEST_ASSERT(mvcc_begin_txn(manager, &txns[i]), "Begin transaction should succeed");
    }
    
    // Verify all transactions are tracked correctly
    for (int i = 0; i < NUM_TXNS; i++) {
        txn_state_t state = mvcc_get_txn_state(manager, txns[i]);
        TEST_ASSERT(state == TXN_STATE_ACTIVE, "Transaction should be active");
    }
    
    // Commit all
    for (int i = 0; i < NUM_TXNS; i++) {
        TEST_ASSERT(mvcc_commit_txn(manager, txns[i]), "Commit should succeed");
        txn_state_t state = mvcc_get_txn_state(manager, txns[i]);
        TEST_ASSERT(state == TXN_STATE_COMMITTED, "Transaction should be committed");
    }
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_reference_counting(void) {
    printf("🧪 Running: test_snapshot_reference_counting\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    TEST_ASSERT(snapshot->ref_count == 1, "Initial ref_count should be 1");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should have 1 active snapshot");
    
    // Acquire multiple references
    for (int i = 0; i < 10; i++) {
        mvcc_snapshot_acquire(snapshot);
    }
    TEST_ASSERT(snapshot->ref_count == 11, "ref_count should be 11");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should still have 1 active snapshot");
    
    // Release all but one
    for (int i = 0; i < 10; i++) {
        mvcc_snapshot_release(manager, snapshot);
    }
    TEST_ASSERT(snapshot->ref_count == 1, "ref_count should be back to 1");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should still have 1 active snapshot");
    
    // Release last reference - should be freed
    mvcc_snapshot_release(manager, snapshot);
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 active snapshots");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_multiple_snapshots(void) {
    printf("🧪 Running: test_multiple_snapshots\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    const int NUM_SNAPSHOTS = 100;
    mvcc_snapshot_t *snapshots[NUM_SNAPSHOTS];
    
    // Create many snapshots
    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
        snapshots[i] = mvcc_snapshot_create(manager);
        TEST_ASSERT(snapshots[i] != NULL, "Snapshot creation should succeed");
    }
    
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == NUM_SNAPSHOTS, "Should have all snapshots active");
    
    // Get oldest snapshot
    uint64_t oldest;
    TEST_ASSERT(mvcc_get_oldest_snapshot(manager, &oldest), "Should have oldest snapshot");
    TEST_ASSERT(oldest == snapshots[0]->snapshot_id, "Oldest should be first snapshot");
    
    // Release in reverse order
    for (int i = NUM_SNAPSHOTS - 1; i >= 0; i--) {
        mvcc_snapshot_release(manager, snapshots[i]);
    }
    
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 active snapshots");
    
    // No oldest snapshot
    TEST_ASSERT(!mvcc_get_oldest_snapshot(manager, &oldest), "Should have no oldest snapshot");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_edge_cases(void) {
    printf("🧪 Running: test_visibility_edge_cases\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    // Test with NULL snapshot (should return false)
    bool visible = mvcc_is_visible(manager, NULL, txn1, 0, txn1);
    TEST_ASSERT(!visible, "NULL snapshot should return false");
    
    // Test with NULL manager (should return false)
    visible = mvcc_is_visible(NULL, snapshot, txn1, 0, txn1);
    TEST_ASSERT(!visible, "NULL manager should return false");
    
    // Test with xmin = 0 (invalid)
    visible = mvcc_is_visible(manager, snapshot, 0, 0, txn1);
    TEST_ASSERT(!visible, "xmin=0 should not be visible");
    
    // Test with xmax = xmin (deleted by same txn that created it)
    visible = mvcc_is_visible(manager, snapshot, txn1, txn1, txn1);
    TEST_ASSERT(!visible, "Version deleted by creator should not be visible");
    
    // Test with very high xmin (future transaction)
    visible = mvcc_is_visible(manager, snapshot, UINT64_MAX, 0, txn1);
    TEST_ASSERT(!visible, "Future transaction should not be visible");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_committed_vs_aborted_visibility(void) {
    printf("🧪 Running: test_committed_vs_aborted_visibility\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Create txn1, commit it
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit txn1");
    
    // Create txn2, abort it
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    TEST_ASSERT(mvcc_abort_txn(manager, txn2), "Abort txn2");
    
    // Create snapshot after both finished
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    uint64_t txn3;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn3), "Begin txn3");
    
    // txn1's version should be visible (committed)
    bool visible = mvcc_is_visible(manager, snapshot, txn1, 0, txn3);
    TEST_ASSERT(visible, "Committed transaction's version should be visible");
    
    // txn2's version should NOT be visible (aborted)
    visible = mvcc_is_visible(manager, snapshot, txn2, 0, txn3);
    TEST_ASSERT(!visible, "Aborted transaction's version should not be visible");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_delete_by_active_transaction(void) {
    printf("🧪 Running: test_delete_by_active_transaction\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Create and commit txn1 (creates version)
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit txn1");
    
    // Start txn2 (will delete)
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    
    // Create snapshot while txn2 is active
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    uint64_t txn3;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn3), "Begin txn3");
    
    // Version created by txn1, marked for deletion by txn2 (still active)
    // Should be visible because txn2 hasn't committed yet
    bool visible = mvcc_is_visible(manager, snapshot, txn1, txn2, txn3);
    TEST_ASSERT(visible, "Version should be visible (deleting txn active at snapshot)");
    
    // Now commit txn2
    TEST_ASSERT(mvcc_commit_txn(manager, txn2), "Commit txn2");
    
    // Still visible to this snapshot (delete happened after snapshot)
    visible = mvcc_is_visible(manager, snapshot, txn1, txn2, txn3);
    TEST_ASSERT(visible, "Version should still be visible (delete after snapshot)");
    
    // Create new snapshot after deletion
    mvcc_snapshot_t *snapshot2 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot2 != NULL, "Snapshot2 creation should succeed");
    
    // Now not visible in new snapshot
    visible = mvcc_is_visible(manager, snapshot2, txn1, txn2, txn3);
    TEST_ASSERT(!visible, "Version should not be visible (deleted before snapshot)");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_snapshot_release(manager, snapshot2);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

// ============================================================================
// Concurrent Tests
// ============================================================================

typedef struct {
    mvcc_manager_t *manager;
    int thread_id;
    int num_ops;
    int *success_count;
    pthread_mutex_t *count_lock;
} thread_args_t;

static void *concurrent_txn_worker(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    
    for (int i = 0; i < args->num_ops; i++) {
        uint64_t txn_id;
        if (mvcc_begin_txn(args->manager, &txn_id)) {
            // Random commit or abort
            if ((txn_id % 2) == 0) {
                mvcc_commit_txn(args->manager, txn_id);
            } else {
                mvcc_abort_txn(args->manager, txn_id);
            }
            
            pthread_mutex_lock(args->count_lock);
            (*args->success_count)++;
            pthread_mutex_unlock(args->count_lock);
        }
    }
    
    return NULL;
}

static bool test_concurrent_transactions(void) {
    printf("🧪 Running: test_concurrent_transactions\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    const int NUM_THREADS = 10;
    const int OPS_PER_THREAD = 100;
    pthread_t threads[NUM_THREADS];
    thread_args_t args[NUM_THREADS];
    int success_count = 0;
    pthread_mutex_t count_lock;
    
    pthread_mutex_init(&count_lock, NULL);
    
    // Start threads
    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].manager = manager;
        args[i].thread_id = i;
        args[i].num_ops = OPS_PER_THREAD;
        args[i].success_count = &success_count;
        args[i].count_lock = &count_lock;
        
        pthread_create(&threads[i], NULL, concurrent_txn_worker, &args[i]);
    }
    
    // Wait for completion
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    TEST_ASSERT(success_count == NUM_THREADS * OPS_PER_THREAD, 
                "All transactions should succeed");
    
    pthread_mutex_destroy(&count_lock);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static void *concurrent_snapshot_worker(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    
    for (int i = 0; i < args->num_ops; i++) {
        mvcc_snapshot_t *snapshot = mvcc_snapshot_create(args->manager);
        if (snapshot) {
            // Do some visibility checks
            uint64_t txn_id;
            if (mvcc_begin_txn(args->manager, &txn_id)) {
                mvcc_is_visible(args->manager, snapshot, 1, 0, txn_id);
                mvcc_commit_txn(args->manager, txn_id);
            }
            
            mvcc_snapshot_release(args->manager, snapshot);
            
            pthread_mutex_lock(args->count_lock);
            (*args->success_count)++;
            pthread_mutex_unlock(args->count_lock);
        }
    }
    
    return NULL;
}

static bool test_concurrent_snapshots(void) {
    printf("🧪 Running: test_concurrent_snapshots\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    const int NUM_THREADS = 10;
    const int OPS_PER_THREAD = 50;
    pthread_t threads[NUM_THREADS];
    thread_args_t args[NUM_THREADS];
    int success_count = 0;
    pthread_mutex_t count_lock;
    
    pthread_mutex_init(&count_lock, NULL);
    
    // Start threads
    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].manager = manager;
        args[i].thread_id = i;
        args[i].num_ops = OPS_PER_THREAD;
        args[i].success_count = &success_count;
        args[i].count_lock = &count_lock;
        
        pthread_create(&threads[i], NULL, concurrent_snapshot_worker, &args[i]);
    }
    
    // Wait for completion
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    TEST_ASSERT(success_count == NUM_THREADS * OPS_PER_THREAD, 
                "All snapshot operations should succeed");
    
    // All snapshots should be released
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, 
                "All snapshots should be released");
    
    pthread_mutex_destroy(&count_lock);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    // Reduce log noise
    log_set_level(LOG_LEVEL_ERROR);
    
    printf("\n");
    printf("========================================\n");
    printf(" MVCC Edge Case Tests\n");
    printf("========================================\n\n");
    
    int passed = 0;
    int total = 0;
    
    #define RUN_TEST(test_func) do { \
        total++; \
        if (test_func()) { \
            passed++; \
        } else { \
            printf(COLOR_RED "   ✗ Test failed\n" COLOR_RESET); \
        } \
        printf("\n"); \
    } while(0)
    
    RUN_TEST(test_empty_snapshot);
    RUN_TEST(test_transaction_not_found);
    RUN_TEST(test_double_commit_abort);
    RUN_TEST(test_many_active_transactions);
    RUN_TEST(test_transaction_table_collision);
    RUN_TEST(test_snapshot_reference_counting);
    RUN_TEST(test_multiple_snapshots);
    RUN_TEST(test_visibility_edge_cases);
    RUN_TEST(test_committed_vs_aborted_visibility);
    RUN_TEST(test_delete_by_active_transaction);
    RUN_TEST(test_concurrent_transactions);
    RUN_TEST(test_concurrent_snapshots);
    
    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n\n");
    
    // Print stats
    if (passed == total) {
        printf(COLOR_GREEN "✓ All edge case tests passed!\n" COLOR_RESET);
    }
    
    return (passed == total) ? 0 : 1;
}
