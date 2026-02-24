/**
 * Test: MVCC (Multi-Version Concurrency Control)
 * 
 * Tests the core MVCC functionality:
 * - Transaction ID allocation
 * - Transaction state tracking (active/committed/aborted)
 * - Snapshot creation and management
 * - Visibility checks (snapshot isolation semantics)
 */

#include "mvcc.h"
#include "logger.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <time.h>

// ANSI color codes
#define COLOR_GREEN  "\033[0;32m"
#define COLOR_RED    "\033[0;31m"
#define COLOR_CYAN   "\033[0;36m"
#define COLOR_RESET  "\033[0m"

#define TEST_ASSERT(cond, msg) do { \
    if (!(cond)) { \
        fprintf(stderr, COLOR_RED "   ✗ FAILED: %s\n" COLOR_RESET, msg); \
        fprintf(stderr, "     at %s:%d\n", __FILE__, __LINE__); \
        return false; \
    } \
} while(0)

static bool test_manager_creation(void) {
    printf("🧪 Running: test_manager_creation\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Check initial state
    TEST_ASSERT(manager->next_txn_id == 1, "Initial txn_id should be 1");
    TEST_ASSERT(manager->snapshots == NULL, "Should have no snapshots initially");
    TEST_ASSERT(manager->snapshots_created == 0, "Snapshots created count should be 0");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_transaction_lifecycle(void) {
    printf("🧪 Running: test_transaction_lifecycle\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Begin transaction
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn should succeed");
    TEST_ASSERT(txn1 == 1, "First txn_id should be 1");
    
    // Check state
    txn_state_t state = mvcc_get_txn_state(manager, txn1);
    TEST_ASSERT(state == TXN_STATE_ACTIVE, "Transaction should be active");
    
    // Begin second transaction
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin second txn should succeed");
    TEST_ASSERT(txn2 == 2, "Second txn_id should be 2");
    
    // Commit first transaction (entry is removed from map and retired to GC)
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit should succeed");
    state = mvcc_get_txn_state(manager, txn1);
    TEST_ASSERT(state == TXN_STATE_COMMITTED, "Committed txn should report COMMITTED");

    // Abort second transaction (entry is removed from map and retired to GC)
    TEST_ASSERT(mvcc_abort_txn(manager, txn2), "Abort should succeed");
    // After removal, not-found returns COMMITTED (safe: aborted ops are never in tree)
    state = mvcc_get_txn_state(manager, txn2);
    TEST_ASSERT(state == TXN_STATE_COMMITTED, "Removed txn should report COMMITTED");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_creation(void) {
    printf("🧪 Running: test_snapshot_creation\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Start some transactions
    uint64_t txn1, txn2, txn3;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    TEST_ASSERT(mvcc_begin_txn(manager, &txn3), "Begin txn3");
    
    // Create snapshot - should capture all 3 active transactions
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    TEST_ASSERT(snapshot->ref_count == 1, "Initial ref_count should be 1");
    TEST_ASSERT(snapshot->num_active == 3, "Should capture 3 active transactions");
    TEST_ASSERT(snapshot->xmin == 1, "xmin should be 1 (oldest active)");
    TEST_ASSERT(snapshot->xmax == 4, "xmax should be 4 (next txn_id)");
    
    // Check active transactions were captured
    TEST_ASSERT(mvcc_txn_was_active(snapshot, txn1), "txn1 should be in snapshot");
    TEST_ASSERT(mvcc_txn_was_active(snapshot, txn2), "txn2 should be in snapshot");
    TEST_ASSERT(mvcc_txn_was_active(snapshot, txn3), "txn3 should be in snapshot");
    
    // Commit one transaction
    mvcc_commit_txn(manager, txn1);
    
    // Create new snapshot - should capture only 2 active transactions
    mvcc_snapshot_t *snapshot2 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot2 != NULL, "Second snapshot creation should succeed");
    TEST_ASSERT(snapshot2->num_active == 2, "Should capture 2 active transactions");
    TEST_ASSERT(!mvcc_txn_was_active(snapshot2, txn1), "txn1 should not be active anymore");
    TEST_ASSERT(mvcc_txn_was_active(snapshot2, txn2), "txn2 should still be active");
    TEST_ASSERT(mvcc_txn_was_active(snapshot2, txn3), "txn3 should still be active");
    
    // Test reference counting
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 2, "Should have 2 active snapshots");
    
    mvcc_snapshot_acquire(snapshot);
    TEST_ASSERT(snapshot->ref_count == 2, "ref_count should be 2 after acquire");
    
    mvcc_snapshot_release(manager, snapshot);
    TEST_ASSERT(snapshot->ref_count == 1, "ref_count should be 1 after release");
    
    mvcc_snapshot_release(manager, snapshot);
    // snapshot is now freed
    
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should have 1 active snapshot");
    
    mvcc_snapshot_release(manager, snapshot2);
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 active snapshots");
    
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_own_transaction(void) {
    printf("🧪 Running: test_visibility_own_transaction\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    // Version created by our own transaction should be visible
    bool visible = mvcc_is_visible(manager, snapshot, txn1, 0, txn1);
    TEST_ASSERT(visible, "Own version should be visible");
    
    // Version created and deleted by our own transaction should NOT be visible
    visible = mvcc_is_visible(manager, snapshot, txn1, txn1, txn1);
    TEST_ASSERT(!visible, "Own deleted version should not be visible");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_committed_before_snapshot(void) {
    printf("🧪 Running: test_visibility_committed_before_snapshot\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Create and commit a transaction
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit txn1");
    
    // Create snapshot AFTER commit
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    // Start new transaction
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    
    // Version created by txn1 (committed before snapshot) should be visible to txn2
    bool visible = mvcc_is_visible(manager, snapshot, txn1, 0, txn2);
    TEST_ASSERT(visible, "Committed version before snapshot should be visible");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_active_at_snapshot(void) {
    printf("🧪 Running: test_visibility_active_at_snapshot\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Start txn1
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    
    // Create snapshot while txn1 is active
    mvcc_snapshot_t *snapshot = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot != NULL, "Snapshot creation should succeed");
    
    // Start txn2
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    
    // Version created by txn1 (active at snapshot time) should NOT be visible to txn2
    bool visible = mvcc_is_visible(manager, snapshot, txn1, 0, txn2);
    TEST_ASSERT(!visible, "Version from active txn should not be visible");
    
    // Now commit txn1
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit txn1");
    
    // Even after commit, still not visible because txn was active at snapshot time
    visible = mvcc_is_visible(manager, snapshot, txn1, 0, txn2);
    TEST_ASSERT(!visible, "Version from txn active at snapshot time should not be visible");
    
    // Create new snapshot AFTER commit
    mvcc_snapshot_t *snapshot2 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot2 != NULL, "Second snapshot creation should succeed");
    
    // Now it should be visible in new snapshot
    visible = mvcc_is_visible(manager, snapshot2, txn1, 0, txn2);
    TEST_ASSERT(visible, "Committed version should be visible in new snapshot");
    
    mvcc_snapshot_release(manager, snapshot);
    mvcc_snapshot_release(manager, snapshot2);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_deleted_version(void) {
    printf("🧪 Running: test_visibility_deleted_version\n");
    
    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");
    
    // Create and commit txn1 (creates a version)
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_commit_txn(manager, txn1), "Commit txn1");
    
    // Create snapshot - should see txn1's version
    mvcc_snapshot_t *snapshot1 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot1 != NULL, "Snapshot1 creation should succeed");
    
    // Create and commit txn2 (deletes the version)
    uint64_t txn2;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn2), "Begin txn2");
    TEST_ASSERT(mvcc_commit_txn(manager, txn2), "Commit txn2");
    
    // Create new snapshot after deletion
    mvcc_snapshot_t *snapshot2 = mvcc_snapshot_create(manager);
    TEST_ASSERT(snapshot2 != NULL, "Snapshot2 creation should succeed");
    
    uint64_t txn3;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn3), "Begin txn3");
    
    // snapshot1: Deletion happened after snapshot, version should be visible
    bool visible = mvcc_is_visible(manager, snapshot1, txn1, txn2, txn3);
    TEST_ASSERT(visible, "Version should be visible (deleted after snapshot1)");
    
    // snapshot2: Deletion happened before snapshot, version should NOT be visible
    visible = mvcc_is_visible(manager, snapshot2, txn1, txn2, txn3);
    TEST_ASSERT(!visible, "Version should not be visible (deleted before snapshot2)");
    
    mvcc_snapshot_release(manager, snapshot1);
    mvcc_snapshot_release(manager, snapshot2);
    mvcc_manager_destroy(manager);
    
    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_visibility_aborted_transaction(void) {
    printf("🧪 Running: test_visibility_aborted_transaction\n");

    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");

    // Create and abort txn1
    uint64_t txn1;
    TEST_ASSERT(mvcc_begin_txn(manager, &txn1), "Begin txn1");
    TEST_ASSERT(mvcc_abort_txn(manager, txn1), "Abort txn1");

    // Verify aborted txn is removed from map
    // (not-found returns COMMITTED, which is safe because aborted ops
    //  are discarded and no leaf ever references an aborted txn's xmin)
    txn_state_t state = mvcc_get_txn_state(manager, txn1);
    TEST_ASSERT(state == TXN_STATE_COMMITTED, "Removed txn reports COMMITTED");

    // In practice, no leaf will have xmin == aborted txn_id, because
    // aborted transactions discard their ops without modifying the tree.
    // The visibility system relies on this invariant.

    mvcc_manager_destroy(manager);

    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_timeout_basic(void) {
    printf("🧪 Running: test_snapshot_timeout_basic\n");

    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");

    // Set a very short timeout (50ms)
    mvcc_set_snapshot_timeout(manager, 50);

    // Create a snapshot
    mvcc_snapshot_t *snap = mvcc_snapshot_create(manager);
    TEST_ASSERT(snap != NULL, "Snapshot creation should succeed");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should have 1 active snapshot");

    // Immediately expire — snapshot is fresh, should survive
    size_t expired = mvcc_expire_snapshots(manager);
    TEST_ASSERT(expired == 0, "Fresh snapshot should not expire");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Should still have 1 snapshot");

    // Wait for timeout to elapse
    struct timespec sleep_time = {0, 60 * 1000000}; // 60ms
    nanosleep(&sleep_time, NULL);

    // Now expire — should remove it
    expired = mvcc_expire_snapshots(manager);
    TEST_ASSERT(expired == 1, "Expired snapshot should be removed");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 snapshots");

    mvcc_manager_destroy(manager);

    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_timeout_selective(void) {
    printf("🧪 Running: test_snapshot_timeout_selective\n");

    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");

    // Set 100ms timeout
    mvcc_set_snapshot_timeout(manager, 100);

    // Create first batch of snapshots
    mvcc_snapshot_t *old_snaps[5];
    for (int i = 0; i < 5; i++) {
        old_snaps[i] = mvcc_snapshot_create(manager);
        TEST_ASSERT(old_snaps[i] != NULL, "Snapshot creation should succeed");
    }
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 5, "Should have 5 snapshots");

    // Wait for them to age past timeout
    struct timespec sleep_time = {0, 120 * 1000000}; // 120ms
    nanosleep(&sleep_time, NULL);

    // Create fresh snapshots
    mvcc_snapshot_t *new_snaps[3];
    for (int i = 0; i < 3; i++) {
        new_snaps[i] = mvcc_snapshot_create(manager);
        TEST_ASSERT(new_snaps[i] != NULL, "Snapshot creation should succeed");
    }
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 8, "Should have 8 snapshots");

    // Expire — should remove only the 5 old ones
    size_t expired = mvcc_expire_snapshots(manager);
    TEST_ASSERT(expired == 5, "Should expire exactly 5 old snapshots");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 3, "Should have 3 remaining");

    // Clean up remaining snapshots
    for (int i = 0; i < 3; i++) {
        mvcc_snapshot_release(manager, new_snaps[i]);
    }
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 snapshots");

    mvcc_manager_destroy(manager);

    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_timeout_disabled(void) {
    printf("🧪 Running: test_snapshot_timeout_disabled\n");

    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");

    // Default: no timeout (0)
    mvcc_snapshot_t *snap = mvcc_snapshot_create(manager);
    TEST_ASSERT(snap != NULL, "Snapshot creation should succeed");

    // Wait a bit
    struct timespec sleep_time = {0, 10 * 1000000}; // 10ms
    nanosleep(&sleep_time, NULL);

    // Expire should do nothing when timeout is 0
    size_t expired = mvcc_expire_snapshots(manager);
    TEST_ASSERT(expired == 0, "No snapshots should expire with timeout=0");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 1, "Snapshot should survive");

    mvcc_snapshot_release(manager, snap);
    mvcc_manager_destroy(manager);

    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

static bool test_snapshot_timeout_many(void) {
    printf("🧪 Running: test_snapshot_timeout_many\n");

    mvcc_manager_t *manager = mvcc_manager_create();
    TEST_ASSERT(manager != NULL, "Manager creation should succeed");

    // Set 30ms timeout
    mvcc_set_snapshot_timeout(manager, 30);

    // Create 100 snapshots
    for (int i = 0; i < 100; i++) {
        mvcc_snapshot_t *snap = mvcc_snapshot_create(manager);
        TEST_ASSERT(snap != NULL, "Snapshot creation should succeed");
    }
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 100, "Should have 100 snapshots");

    // Wait for timeout
    struct timespec sleep_time = {0, 50 * 1000000}; // 50ms
    nanosleep(&sleep_time, NULL);

    // All 100 should expire
    size_t expired = mvcc_expire_snapshots(manager);
    TEST_ASSERT(expired == 100, "All 100 snapshots should expire");
    TEST_ASSERT(mvcc_active_snapshot_count(manager) == 0, "Should have 0 snapshots");

    mvcc_manager_destroy(manager);

    printf(COLOR_GREEN "   ✅ PASSED\n" COLOR_RESET);
    return true;
}

int main(void) {
    // Reduce log noise
    log_set_level(LOG_LEVEL_ERROR);
    
    printf("\n");
    printf("========================================\n");
    printf(" MVCC Tests\n");
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
    
    RUN_TEST(test_manager_creation);
    RUN_TEST(test_transaction_lifecycle);
    RUN_TEST(test_snapshot_creation);
    RUN_TEST(test_visibility_own_transaction);
    RUN_TEST(test_visibility_committed_before_snapshot);
    RUN_TEST(test_visibility_active_at_snapshot);
    RUN_TEST(test_visibility_deleted_version);
    RUN_TEST(test_visibility_aborted_transaction);
    RUN_TEST(test_snapshot_timeout_basic);
    RUN_TEST(test_snapshot_timeout_selective);
    RUN_TEST(test_snapshot_timeout_disabled);
    RUN_TEST(test_snapshot_timeout_many);

    printf("========================================\n");
    printf(" Results: %d/%d tests passed\n", passed, total);
    printf("========================================\n\n");
    
    return (passed == total) ? 0 : 1;
}
