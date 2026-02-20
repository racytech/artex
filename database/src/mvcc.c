/**
 * MVCC (Multi-Version Concurrency Control) Implementation
 * 
 * Provides snapshot isolation for concurrent access to the persistent ART.
 * Based on PostgreSQL's MVCC design with transaction state tracking and
 * visibility checks using xmin/xmax.
 */

#include "mvcc.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Transaction table parameters
#define TXN_TABLE_INITIAL_SIZE 1024
#define TXN_TABLE_MAX_SIZE (1024 * 1024)  // 1M transactions

// Helper: Hash function for transaction IDs
static inline size_t txn_hash(uint64_t txn_id, size_t table_size) {
    return (size_t)(txn_id % table_size);
}

// ============================================================================
// MVCC Manager Creation/Destruction
// ============================================================================

mvcc_manager_t *mvcc_manager_create(void) {
    mvcc_manager_t *manager = calloc(1, sizeof(mvcc_manager_t));
    if (!manager) {
        LOG_ERROR("Failed to allocate MVCC manager");
        return NULL;
    }
    
    // Initialize transaction ID counter (start at 1, 0 is reserved)
    manager->next_txn_id = 1;
    
    // Initialize transaction table
    manager->txn_table_size = TXN_TABLE_INITIAL_SIZE;
    manager->txn_table = calloc(manager->txn_table_size, sizeof(txn_info_t));
    if (!manager->txn_table) {
        LOG_ERROR("Failed to allocate transaction table");
        free(manager);
        return NULL;
    }
    
    // Initialize locks
    if (pthread_mutex_init(&manager->txn_id_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize txn_id_lock");
        free(manager->txn_table);
        free(manager);
        return NULL;
    }
    
    if (pthread_rwlock_init(&manager->txn_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize txn_lock");
        pthread_mutex_destroy(&manager->txn_id_lock);
        free(manager->txn_table);
        free(manager);
        return NULL;
    }
    
    if (pthread_rwlock_init(&manager->snapshot_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize snapshot_lock");
        pthread_rwlock_destroy(&manager->txn_lock);
        pthread_mutex_destroy(&manager->txn_id_lock);
        free(manager->txn_table);
        free(manager);
        return NULL;
    }
    
    // No snapshots initially
    manager->snapshots = NULL;
    
    // Initialize statistics
    manager->snapshots_created = 0;
    manager->snapshots_released = 0;
    manager->visibility_checks = 0;
    
    LOG_INFO("MVCC manager created (txn_table_size=%zu)", manager->txn_table_size);
    return manager;
}

void mvcc_manager_destroy(mvcc_manager_t *manager) {
    if (!manager) return;
    
    // Free all snapshots
    pthread_rwlock_wrlock(&manager->snapshot_lock);
    mvcc_snapshot_t *snapshot = manager->snapshots;
    while (snapshot) {
        mvcc_snapshot_t *next = snapshot->next;
        free(snapshot->active_txns);
        free(snapshot);
        snapshot = next;
    }
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    // Destroy locks
    pthread_rwlock_destroy(&manager->snapshot_lock);
    pthread_rwlock_destroy(&manager->txn_lock);
    pthread_mutex_destroy(&manager->txn_id_lock);
    
    // Free transaction table
    free(manager->txn_table);
    
    LOG_INFO("MVCC manager destroyed (snapshots_created=%lu, snapshots_released=%lu, visibility_checks=%lu)",
             manager->snapshots_created, manager->snapshots_released, manager->visibility_checks);
    
    free(manager);
}

// ============================================================================
// Transaction Management
// ============================================================================

bool mvcc_begin_txn(mvcc_manager_t *manager, uint64_t *txn_id_out) {
    if (!manager || !txn_id_out) return false;
    
    // Allocate new transaction ID
    pthread_mutex_lock(&manager->txn_id_lock);
    uint64_t txn_id = manager->next_txn_id++;
    pthread_mutex_unlock(&manager->txn_id_lock);
    
    // Add to transaction table
    pthread_rwlock_wrlock(&manager->txn_lock);
    
    size_t index = txn_hash(txn_id, manager->txn_table_size);
    txn_info_t *slot = &manager->txn_table[index];
    
    // Linear probing for collision resolution
    size_t probes = 0;
    while (slot->txn_id != 0 && slot->txn_id != txn_id && probes < manager->txn_table_size) {
        index = (index + 1) % manager->txn_table_size;
        slot = &manager->txn_table[index];
        probes++;
    }
    
    if (probes >= manager->txn_table_size) {
        pthread_rwlock_unlock(&manager->txn_lock);
        LOG_ERROR("Transaction table full (size=%zu)", manager->txn_table_size);
        return false;
    }
    
    // Initialize transaction info
    slot->txn_id = txn_id;
    slot->state = TXN_STATE_ACTIVE;
    slot->commit_ts = 0;
    
    pthread_rwlock_unlock(&manager->txn_lock);
    
    *txn_id_out = txn_id;
    LOG_DEBUG("Transaction %lu started", txn_id);
    return true;
}

bool mvcc_commit_txn(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return false;
    
    pthread_rwlock_wrlock(&manager->txn_lock);
    
    size_t index = txn_hash(txn_id, manager->txn_table_size);
    txn_info_t *slot = &manager->txn_table[index];
    
    // Find transaction
    size_t probes = 0;
    while (slot->txn_id != txn_id && probes < manager->txn_table_size) {
        if (slot->txn_id == 0) {
            // Not found
            pthread_rwlock_unlock(&manager->txn_lock);
            LOG_ERROR("Transaction %lu not found for commit", txn_id);
            return false;
        }
        index = (index + 1) % manager->txn_table_size;
        slot = &manager->txn_table[index];
        probes++;
    }
    
    if (slot->txn_id != txn_id) {
        pthread_rwlock_unlock(&manager->txn_lock);
        LOG_ERROR("Transaction %lu not found after %zu probes", txn_id, probes);
        return false;
    }
    
    // Mark as committed
    slot->state = TXN_STATE_COMMITTED;
    
    // Assign commit timestamp (use current next_txn_id as logical timestamp)
    pthread_mutex_lock(&manager->txn_id_lock);
    slot->commit_ts = manager->next_txn_id;
    pthread_mutex_unlock(&manager->txn_id_lock);
    
    pthread_rwlock_unlock(&manager->txn_lock);
    
    LOG_DEBUG("Transaction %lu committed (commit_ts=%lu)", txn_id, slot->commit_ts);
    return true;
}

bool mvcc_abort_txn(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return false;
    
    pthread_rwlock_wrlock(&manager->txn_lock);
    
    size_t index = txn_hash(txn_id, manager->txn_table_size);
    txn_info_t *slot = &manager->txn_table[index];
    
    // Find transaction
    size_t probes = 0;
    while (slot->txn_id != txn_id && probes < manager->txn_table_size) {
        if (slot->txn_id == 0) {
            pthread_rwlock_unlock(&manager->txn_lock);
            LOG_ERROR("Transaction %lu not found for abort", txn_id);
            return false;
        }
        index = (index + 1) % manager->txn_table_size;
        slot = &manager->txn_table[index];
        probes++;
    }
    
    if (slot->txn_id != txn_id) {
        pthread_rwlock_unlock(&manager->txn_lock);
        LOG_ERROR("Transaction %lu not found after %zu probes", txn_id, probes);
        return false;
    }
    
    // Mark as aborted
    slot->state = TXN_STATE_ABORTED;
    slot->commit_ts = 0;
    
    pthread_rwlock_unlock(&manager->txn_lock);
    
    LOG_DEBUG("Transaction %lu aborted", txn_id);
    return true;
}

txn_state_t mvcc_get_txn_state(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return TXN_STATE_ABORTED;
    
    pthread_rwlock_rdlock(&manager->txn_lock);
    
    size_t index = txn_hash(txn_id, manager->txn_table_size);
    txn_info_t *slot = &manager->txn_table[index];
    
    // Find transaction
    size_t probes = 0;
    while (slot->txn_id != txn_id && probes < manager->txn_table_size) {
        if (slot->txn_id == 0) {
            // Not found - assume aborted or very old
            pthread_rwlock_unlock(&manager->txn_lock);
            return TXN_STATE_ABORTED;
        }
        index = (index + 1) % manager->txn_table_size;
        slot = &manager->txn_table[index];
        probes++;
    }
    
    txn_state_t state = (slot->txn_id == txn_id) ? slot->state : TXN_STATE_ABORTED;
    
    pthread_rwlock_unlock(&manager->txn_lock);
    return state;
}

// ============================================================================
// Snapshot Management
// ============================================================================

mvcc_snapshot_t *mvcc_snapshot_create(mvcc_manager_t *manager) {
    if (!manager) return NULL;
    
    mvcc_snapshot_t *snapshot = calloc(1, sizeof(mvcc_snapshot_t));
    if (!snapshot) {
        LOG_ERROR("Failed to allocate snapshot");
        return NULL;
    }
    
    // Get current transaction ID as snapshot ID
    pthread_mutex_lock(&manager->txn_id_lock);
    snapshot->snapshot_id = manager->next_txn_id;
    snapshot->xmax = manager->next_txn_id;
    pthread_mutex_unlock(&manager->txn_id_lock);
    
    // Collect all active transactions
    pthread_rwlock_rdlock(&manager->txn_lock);
    
    // Initial allocation for active transactions
    snapshot->active_capacity = 256;
    snapshot->active_txns = malloc(snapshot->active_capacity * sizeof(uint64_t));
    if (!snapshot->active_txns) {
        pthread_rwlock_unlock(&manager->txn_lock);
        free(snapshot);
        LOG_ERROR("Failed to allocate active_txns array");
        return NULL;
    }
    
    snapshot->num_active = 0;
    snapshot->xmin = snapshot->xmax;  // Will be updated if we find active txns
    
    // Scan transaction table for active transactions
    for (size_t i = 0; i < manager->txn_table_size; i++) {
        txn_info_t *slot = &manager->txn_table[i];
        if (slot->txn_id != 0 && slot->state == TXN_STATE_ACTIVE) {
            // Grow array if needed
            if (snapshot->num_active >= snapshot->active_capacity) {
                size_t new_capacity = snapshot->active_capacity * 2;
                uint64_t *new_array = realloc(snapshot->active_txns,
                                             new_capacity * sizeof(uint64_t));
                if (!new_array) {
                    pthread_rwlock_unlock(&manager->txn_lock);
                    free(snapshot->active_txns);
                    free(snapshot);
                    LOG_ERROR("Failed to grow active_txns array");
                    return NULL;
                }
                snapshot->active_txns = new_array;
                snapshot->active_capacity = new_capacity;
            }
            
            snapshot->active_txns[snapshot->num_active++] = slot->txn_id;
            
            // Update xmin to oldest active transaction
            if (slot->txn_id < snapshot->xmin) {
                snapshot->xmin = slot->txn_id;
            }
        }
    }
    
    pthread_rwlock_unlock(&manager->txn_lock);
    
    // Initialize reference count
    snapshot->ref_count = 1;
    snapshot->next = NULL;
    
    // Add to snapshot list
    pthread_rwlock_wrlock(&manager->snapshot_lock);
    snapshot->next = manager->snapshots;
    manager->snapshots = snapshot;
    manager->snapshots_created++;
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    LOG_DEBUG("Snapshot created: snapshot_id=%lu, xmin=%lu, xmax=%lu, num_active=%zu",
              snapshot->snapshot_id, snapshot->xmin, snapshot->xmax, snapshot->num_active);
    
    return snapshot;
}

void mvcc_snapshot_acquire(mvcc_snapshot_t *snapshot) {
    if (!snapshot) return;
    __atomic_fetch_add(&snapshot->ref_count, 1, __ATOMIC_SEQ_CST);
}

void mvcc_snapshot_release(mvcc_manager_t *manager, mvcc_snapshot_t *snapshot) {
    if (!manager || !snapshot) return;
    
    uint32_t old_count = __atomic_fetch_sub(&snapshot->ref_count, 1, __ATOMIC_SEQ_CST);
    
    if (old_count == 1) {
        // Last reference - remove from list and free
        pthread_rwlock_wrlock(&manager->snapshot_lock);
        
        // Remove from linked list
        mvcc_snapshot_t **ptr = &manager->snapshots;
        while (*ptr) {
            if (*ptr == snapshot) {
                *ptr = snapshot->next;
                break;
            }
            ptr = &(*ptr)->next;
        }
        
        manager->snapshots_released++;
        pthread_rwlock_unlock(&manager->snapshot_lock);
        
        LOG_DEBUG("Snapshot %lu freed", snapshot->snapshot_id);
        free(snapshot->active_txns);
        free(snapshot);
    }
}

// ============================================================================
// Visibility Checks
// ============================================================================

bool mvcc_txn_was_active(const mvcc_snapshot_t *snapshot, uint64_t txn_id) {
    if (!snapshot || txn_id == 0) return false;
    
    // Binary search in active transactions list
    // Note: For better performance, we could sort the active_txns array
    // For now, use linear search
    for (size_t i = 0; i < snapshot->num_active; i++) {
        if (snapshot->active_txns[i] == txn_id) {
            return true;
        }
    }
    
    return false;
}

bool mvcc_is_visible(mvcc_manager_t *manager,
                     const mvcc_snapshot_t *snapshot,
                     uint64_t xmin,
                     uint64_t xmax,
                     uint64_t current_txn_id) {
    if (!manager || !snapshot) return false;
    
    __atomic_fetch_add(&manager->visibility_checks, 1, __ATOMIC_RELAXED);
    
    // Rule 1: Version created by our own transaction? Always visible
    if (xmin == current_txn_id) {
        // Check if we deleted it
        if (xmax != 0 && xmax == current_txn_id) {
            return false;  // We deleted it
        }
        return true;  // We created it and haven't deleted it
    }
    
    // Rule 2: Version created after our snapshot? Not visible
    if (xmin >= snapshot->xmax) {
        return false;
    }
    
    // Rule 3: Version created by transaction active at snapshot time? Not visible
    if (mvcc_txn_was_active(snapshot, xmin)) {
        return false;
    }
    
    // Rule 4: Check if creating transaction committed
    txn_state_t xmin_state = mvcc_get_txn_state(manager, xmin);
    if (xmin_state != TXN_STATE_COMMITTED) {
        return false;  // Creating transaction aborted or still active
    }
    
    // Version was created by a committed transaction before our snapshot
    // Now check if it was deleted
    
    if (xmax == 0) {
        return true;  // Not deleted, visible
    }
    
    // Rule 5: Deleted by our own transaction?
    if (xmax == current_txn_id) {
        return false;  // We deleted it
    }
    
    // Rule 6: Deleted after our snapshot? Still visible to us
    if (xmax >= snapshot->xmax) {
        return true;  // Delete happened after snapshot
    }
    
    // Rule 7: Deleted by transaction active at snapshot time? Still visible
    if (mvcc_txn_was_active(snapshot, xmax)) {
        return true;  // Deleting transaction not committed at snapshot time
    }
    
    // Rule 8: Check if deleting transaction committed
    txn_state_t xmax_state = mvcc_get_txn_state(manager, xmax);
    if (xmax_state != TXN_STATE_COMMITTED) {
        return true;  // Deleting transaction aborted, so deletion didn't happen
    }
    
    // Version was deleted by committed transaction before our snapshot
    return false;
}

// ============================================================================
// Statistics & Utilities
// ============================================================================

size_t mvcc_active_snapshot_count(mvcc_manager_t *manager) {
    if (!manager) return 0;
    
    size_t count = 0;
    
    pthread_rwlock_rdlock(&manager->snapshot_lock);
    mvcc_snapshot_t *snapshot = manager->snapshots;
    while (snapshot) {
        if (snapshot->ref_count > 0) {
            count++;
        }
        snapshot = snapshot->next;
    }
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    return count;
}

bool mvcc_get_oldest_snapshot(mvcc_manager_t *manager, uint64_t *oldest_snapshot_out) {
    if (!manager || !oldest_snapshot_out) return false;
    
    uint64_t oldest = UINT64_MAX;
    bool found = false;
    
    pthread_rwlock_rdlock(&manager->snapshot_lock);
    mvcc_snapshot_t *snapshot = manager->snapshots;
    while (snapshot) {
        if (snapshot->ref_count > 0) {
            if (snapshot->snapshot_id < oldest) {
                oldest = snapshot->snapshot_id;
                found = true;
            }
        }
        snapshot = snapshot->next;
    }
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    if (found) {
        *oldest_snapshot_out = oldest;
    }
    
    return found;
}

void mvcc_print_stats(const mvcc_manager_t *manager) {
    if (!manager) return;
    
    printf("\n");
    printf("================================================================\n");
    printf("  MVCC Manager Statistics\n");
    printf("================================================================\n");
    printf("  Next transaction ID: %lu\n", manager->next_txn_id);
    printf("  Transaction table size: %zu\n", manager->txn_table_size);
    printf("  Snapshots created: %lu\n", manager->snapshots_created);
    printf("  Snapshots released: %lu\n", manager->snapshots_released);
    printf("  Active snapshots: %zu\n", mvcc_active_snapshot_count((mvcc_manager_t *)manager));
    printf("  Visibility checks: %lu\n", manager->visibility_checks);
    printf("================================================================\n");
}
