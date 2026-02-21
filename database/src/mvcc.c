/**
 * MVCC (Multi-Version Concurrency Control) Implementation
 * 
 * Provides snapshot isolation for concurrent access to the persistent ART.
 * Based on PostgreSQL's MVCC design with transaction state tracking and
 * visibility checks using xmin/xmax.
 * 
 * Uses epoch-based garbage collection and dynamic hash map for scalability.
 */

#include "mvcc.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Hash map parameters
#define TXN_MAP_INITIAL_BUCKETS 1024
#define TXN_MAP_LOAD_FACTOR 0.75

// Helper: Hash function for transaction IDs
static inline size_t txn_hash(uint64_t txn_id, size_t bucket_count) {
    // Simple multiplicative hash
    return (size_t)((txn_id * 2654435761ULL) % bucket_count);
}

// Helper: Check if number is power of 2
static inline bool is_power_of_2(size_t n) {
    return n > 0 && (n & (n - 1)) == 0;
}

// ============================================================================
// Transaction Hash Map
// ============================================================================

/**
 * Create a new transaction hash map
 */
static bool txn_map_create(txn_map_t *map, size_t initial_buckets) {
    if (!map || !is_power_of_2(initial_buckets)) return false;
    
    map->buckets = calloc(initial_buckets, sizeof(txn_info_t*));
    if (!map->buckets) {
        LOG_ERROR("Failed to allocate hash map buckets");
        return false;
    }
    
    map->bucket_count = initial_buckets;
    map->active_count = 0;
    map->threshold = (size_t)(initial_buckets * TXN_MAP_LOAD_FACTOR);
    
    if (pthread_rwlock_init(&map->resize_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize resize_lock");
        free(map->buckets);
        return false;
    }
    
    LOG_DEBUG("Transaction map created (buckets=%zu, threshold=%zu)", 
              map->bucket_count, map->threshold);
    return true;
}

/**
 * Destroy transaction hash map and free all entries
 */
static void txn_map_destroy(txn_map_t *map) {
    if (!map || !map->buckets) return;
    
    // Free all entries in all buckets
    for (size_t i = 0; i < map->bucket_count; i++) {
        txn_info_t *entry = map->buckets[i];
        while (entry) {
            txn_info_t *next = entry->next;
            free(entry);
            entry = next;
        }
    }
    
    free(map->buckets);
    pthread_rwlock_destroy(&map->resize_lock);
    
    LOG_DEBUG("Transaction map destroyed (final_count=%zu)", map->active_count);
}

/**
 * Insert a new transaction entry into the map
 * Caller must hold write lock on resize_lock
 */
static bool txn_map_insert(txn_map_t *map, txn_info_t *entry) {
    if (!map || !entry) return false;
    
    size_t bucket = txn_hash(entry->txn_id, map->bucket_count);
    
    // Check for duplicates (shouldn't happen, but be defensive)
    txn_info_t *curr = map->buckets[bucket];
    while (curr) {
        if (curr->txn_id == entry->txn_id) {
            LOG_ERROR("Duplicate transaction ID %lu in map", entry->txn_id);
            return false;
        }
        curr = curr->next;
    }
    
    // Insert at head of chain
    entry->next = map->buckets[bucket];
    map->buckets[bucket] = entry;
    map->active_count++;
    
    return true;
}

/**
 * Lookup a transaction entry by ID
 * Caller must hold at least read lock on resize_lock
 */
static txn_info_t *txn_map_lookup(txn_map_t *map, uint64_t txn_id) {
    if (!map || txn_id == 0) return NULL;
    
    size_t bucket = txn_hash(txn_id, map->bucket_count);
    txn_info_t *entry = map->buckets[bucket];
    
    while (entry) {
        if (entry->txn_id == txn_id) {
            return entry;
        }
        entry = entry->next;
    }
    
    return NULL;
}

/**
 * Remove and return a transaction entry from the map
 * Caller must hold write lock on resize_lock
 */
static txn_info_t *txn_map_remove(txn_map_t *map, uint64_t txn_id) {
    if (!map || txn_id == 0) return NULL;
    
    size_t bucket = txn_hash(txn_id, map->bucket_count);
    txn_info_t **prev_ptr = &map->buckets[bucket];
    txn_info_t *entry = map->buckets[bucket];
    
    while (entry) {
        if (entry->txn_id == txn_id) {
            // Unlink from chain
            *prev_ptr = entry->next;
            entry->next = NULL;
            map->active_count--;
            return entry;
        }
        prev_ptr = &entry->next;
        entry = entry->next;
    }
    
    return NULL;
}

/**
 * Resize the hash map to a larger size
 * Caller must hold write lock on resize_lock
 */
static bool txn_map_resize(txn_map_t *map, size_t new_bucket_count) {
    if (!map || !is_power_of_2(new_bucket_count)) return false;
    
    LOG_INFO("Resizing transaction map: %zu -> %zu buckets", 
             map->bucket_count, new_bucket_count);
    
    // Allocate new bucket array
    txn_info_t **new_buckets = calloc(new_bucket_count, sizeof(txn_info_t*));
    if (!new_buckets) {
        LOG_ERROR("Failed to allocate new buckets for resize");
        return false;
    }
    
    // Rehash all entries into new buckets
    for (size_t i = 0; i < map->bucket_count; i++) {
        txn_info_t *entry = map->buckets[i];
        while (entry) {
            txn_info_t *next = entry->next;
            
            // Compute new bucket
            size_t new_bucket = txn_hash(entry->txn_id, new_bucket_count);
            
            // Insert at head of new chain
            entry->next = new_buckets[new_bucket];
            new_buckets[new_bucket] = entry;
            
            entry = next;
        }
    }
    
    // Swap in new buckets
    free(map->buckets);
    map->buckets = new_buckets;
    map->bucket_count = new_bucket_count;
    map->threshold = (size_t)(new_bucket_count * TXN_MAP_LOAD_FACTOR);
    
    LOG_INFO("Transaction map resized (new_threshold=%zu)", map->threshold);
    return true;
}

// ============================================================================
// Epoch-Based Garbage Collection
// ============================================================================

/**
 * Initialize epoch GC system
 */
static bool epoch_gc_init(epoch_gc_t *gc) {
    if (!gc) return false;
    
    gc->global_epoch = 1;
    gc->retired_queue = NULL;
    gc->retired_count = 0;
    
    if (pthread_mutex_init(&gc->epoch_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize epoch_lock");
        return false;
    }
    
    LOG_DEBUG("Epoch GC initialized");
    return true;
}

/**
 * Destroy epoch GC system and free all retired entries
 */
static void epoch_gc_destroy(epoch_gc_t *gc) {
    if (!gc) return;
    
    // Free all retired entries
    retired_txn_t *retired = gc->retired_queue;
    size_t freed = 0;
    while (retired) {
        retired_txn_t *next = retired->next;
        free(retired->entry);
        free(retired);
        retired = next;
        freed++;
    }
    
    pthread_mutex_destroy(&gc->epoch_lock);
    
    LOG_DEBUG("Epoch GC destroyed (freed %zu retired entries)", freed);
}

/**
 * Advance the global epoch
 * Called periodically to move time forward
 */
static void epoch_gc_advance(epoch_gc_t *gc) {
    if (!gc) return;
    
    pthread_mutex_lock(&gc->epoch_lock);
    gc->global_epoch++;
    pthread_mutex_unlock(&gc->epoch_lock);
}

/**
 * Retire a transaction entry for later garbage collection
 * Entry will be freed once all readers from its epoch have finished
 */
static void epoch_gc_retire(epoch_gc_t *gc, txn_info_t *entry) {
    if (!gc || !entry) return;
    
    pthread_mutex_lock(&gc->epoch_lock);
    
    // Create retired entry
    retired_txn_t *retired = malloc(sizeof(retired_txn_t));
    if (!retired) {
        LOG_ERROR("Failed to allocate retired entry, freeing immediately");
        pthread_mutex_unlock(&gc->epoch_lock);
        free(entry);
        return;
    }
    
    retired->entry = entry;
    retired->retire_epoch = gc->global_epoch;
    
    // Add to head of retirement queue
    retired->next = gc->retired_queue;
    gc->retired_queue = retired;
    gc->retired_count++;
    
    pthread_mutex_unlock(&gc->epoch_lock);
}

/**
 * Reclaim retired entries that are safe to free
 * An entry is safe to free if all readers from its epoch have finished
 * 
 * @param gc GC system
 * @param min_safe_epoch Minimum epoch that is known to be safe (no active readers)
 */
static void epoch_gc_reclaim(epoch_gc_t *gc, uint64_t min_safe_epoch) {
    if (!gc) return;
    
    pthread_mutex_lock(&gc->epoch_lock);
    
    retired_txn_t **prev_ptr = &gc->retired_queue;
    retired_txn_t *retired = gc->retired_queue;
    size_t reclaimed = 0;
    
    while (retired) {
        if (retired->retire_epoch < min_safe_epoch) {
            // Safe to free
            *prev_ptr = retired->next;
            retired_txn_t *to_free = retired;
            retired = retired->next;
            
            free(to_free->entry);
            free(to_free);
            gc->retired_count--;
            reclaimed++;
        } else {
            // Keep in queue
            prev_ptr = &retired->next;
            retired = retired->next;
        }
    }
    
    pthread_mutex_unlock(&gc->epoch_lock);
    
    if (reclaimed > 0) {
        LOG_DEBUG("Reclaimed %zu entries (min_safe_epoch=%lu, retired_count=%zu)",
                  reclaimed, min_safe_epoch, gc->retired_count);
    }
}

/**
 * Compute minimum safe epoch for garbage collection
 * This is the minimum epoch across all active snapshots
 * Entries retired before this epoch can be safely freed
 */
static uint64_t epoch_gc_compute_min_safe_epoch(mvcc_manager_t *manager) {
    if (!manager) return 0;
    
    pthread_rwlock_rdlock(&manager->snapshot_lock);
    
    // If no active snapshots, all epochs before current are safe
    if (!manager->snapshots) {
        pthread_rwlock_unlock(&manager->snapshot_lock);
        pthread_mutex_lock(&manager->gc.epoch_lock);
        uint64_t current = manager->gc.global_epoch;
        pthread_mutex_unlock(&manager->gc.epoch_lock);
        return current;
    }
    
    // Find minimum epoch across all active snapshots
    uint64_t min_epoch = UINT64_MAX;
    mvcc_snapshot_t *snapshot = manager->snapshots;
    while (snapshot) {
        // Snapshots store their creation epoch in xmin (reusing field)
        if (snapshot->xmin < min_epoch) {
            min_epoch = snapshot->xmin;
        }
        snapshot = snapshot->next;
    }
    
    pthread_rwlock_unlock(&manager->snapshot_lock);
    return min_epoch;
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
    
    // Initialize transaction hash map
    if (!txn_map_create(&manager->txn_map, TXN_MAP_INITIAL_BUCKETS)) {
        LOG_ERROR("Failed to create transaction map");
        free(manager);
        return NULL;
    }
    
    // Initialize epoch-based GC
    if (!epoch_gc_init(&manager->gc)) {
        LOG_ERROR("Failed to initialize epoch GC");
        txn_map_destroy(&manager->txn_map);
        free(manager);
        return NULL;
    }
    
    // Initialize locks
    if (pthread_mutex_init(&manager->txn_id_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize txn_id_lock");
        epoch_gc_destroy(&manager->gc);
        txn_map_destroy(&manager->txn_map);
        free(manager);
        return NULL;
    }
    
    if (pthread_rwlock_init(&manager->snapshot_lock, NULL) != 0) {
        LOG_ERROR("Failed to initialize snapshot_lock");
        pthread_mutex_destroy(&manager->txn_id_lock);
        epoch_gc_destroy(&manager->gc);
        txn_map_destroy(&manager->txn_map);
        free(manager);
        return NULL;
    }
    
    // No snapshots initially
    manager->snapshots = NULL;
    
    // Initialize statistics
    manager->snapshots_created = 0;
    manager->snapshots_released = 0;
    manager->visibility_checks = 0;
    
    LOG_INFO("MVCC manager created (buckets=%zu)", manager->txn_map.bucket_count);
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
    
    // Destroy snapshot lock
    pthread_rwlock_destroy(&manager->snapshot_lock);
    pthread_mutex_destroy(&manager->txn_id_lock);
    
    // Destroy epoch GC (frees retired entries)
    epoch_gc_destroy(&manager->gc);
    
    // Destroy transaction map (frees active entries)
    txn_map_destroy(&manager->txn_map);
    
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
    
    // Create transaction entry
    txn_info_t *entry = malloc(sizeof(txn_info_t));
    if (!entry) {
        LOG_ERROR("Failed to allocate transaction entry");
        return false;
    }
    
    entry->txn_id = txn_id;
    entry->state = TXN_STATE_ACTIVE;
    entry->commit_ts = 0;
    entry->next = NULL;
    
    // Get current epoch for this transaction
    pthread_mutex_lock(&manager->gc.epoch_lock);
    entry->epoch = manager->gc.global_epoch;
    pthread_mutex_unlock(&manager->gc.epoch_lock);
    
    // Insert into hash map
    pthread_rwlock_wrlock(&manager->txn_map.resize_lock);
    
    if (!txn_map_insert(&manager->txn_map, entry)) {
        pthread_rwlock_unlock(&manager->txn_map.resize_lock);
        free(entry);
        LOG_ERROR("Failed to insert transaction %lu into map", txn_id);
        return false;
    }
    
    // Check if resize is needed
    if (manager->txn_map.active_count > manager->txn_map.threshold) {
        size_t new_size = manager->txn_map.bucket_count * 2;
        if (!txn_map_resize(&manager->txn_map, new_size)) {
            LOG_WARN("Failed to resize transaction map");
        }
    }
    
    pthread_rwlock_unlock(&manager->txn_map.resize_lock);
    
    *txn_id_out = txn_id;
    LOG_DEBUG("Transaction %lu started (epoch=%lu)", txn_id, entry->epoch);
    return true;
}

bool mvcc_commit_txn(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return false;
    
    pthread_rwlock_rdlock(&manager->txn_map.resize_lock);
    txn_info_t *entry = txn_map_lookup(&manager->txn_map, txn_id);
    
    if (!entry) {
        pthread_rwlock_unlock(&manager->txn_map.resize_lock);
        LOG_ERROR("Transaction %lu not found for commit", txn_id);
        return false;
    }
    
    // Mark as committed
    entry->state = TXN_STATE_COMMITTED;
    
    // Assign commit timestamp
    pthread_mutex_lock(&manager->txn_id_lock);
    entry->commit_ts = manager->next_txn_id;
    pthread_mutex_unlock(&manager->txn_id_lock);
    
    pthread_rwlock_unlock(&manager->txn_map.resize_lock);
    
    LOG_DEBUG("Transaction %lu committed (commit_ts=%lu)", txn_id, entry->commit_ts);
    
    // Periodically run GC (every 100 commits)
    if (txn_id % 100 == 0) {
        uint64_t min_safe_epoch = epoch_gc_compute_min_safe_epoch(manager);
        epoch_gc_reclaim(&manager->gc, min_safe_epoch);
        epoch_gc_advance(&manager->gc);
    }
    
    return true;
}

bool mvcc_abort_txn(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return false;
    
    pthread_rwlock_wrlock(&manager->txn_map.resize_lock);
    txn_info_t *entry = txn_map_remove(&manager->txn_map, txn_id);
    pthread_rwlock_unlock(&manager->txn_map.resize_lock);
    
    if (!entry) {
        LOG_ERROR("Transaction %lu not found for abort", txn_id);
        return false;
    }
    
    // Mark as aborted and retire for GC
    entry->state = TXN_STATE_ABORTED;
    entry->commit_ts = 0;
    epoch_gc_retire(&manager->gc, entry);
    
    LOG_DEBUG("Transaction %lu aborted", txn_id);
    return true;
}

txn_state_t mvcc_get_txn_state(mvcc_manager_t *manager, uint64_t txn_id) {
    if (!manager || txn_id == 0) return TXN_STATE_ABORTED;
    
    pthread_rwlock_rdlock(&manager->txn_map.resize_lock);
    txn_info_t *entry = txn_map_lookup(&manager->txn_map, txn_id);
    txn_state_t state = entry ? entry->state : TXN_STATE_ABORTED;
    pthread_rwlock_unlock(&manager->txn_map.resize_lock);
    
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
    
    // Register with epoch system (store epoch in xmin field temporarily)
    pthread_mutex_lock(&manager->gc.epoch_lock);
    uint64_t current_epoch = manager->gc.global_epoch;
    pthread_mutex_unlock(&manager->gc.epoch_lock);
    
    // Collect all active transactions from hash map
    pthread_rwlock_rdlock(&manager->txn_map.resize_lock);
    
    // Initial allocation for active transactions
    snapshot->active_capacity = 256;
    snapshot->active_txns = malloc(snapshot->active_capacity * sizeof(uint64_t));
    if (!snapshot->active_txns) {
        pthread_rwlock_unlock(&manager->txn_map.resize_lock);
        free(snapshot);
        LOG_ERROR("Failed to allocate active_txns array");
        return NULL;
    }
    
    snapshot->num_active = 0;
    uint64_t min_txn_id = snapshot->xmax;  // Will be updated if we find active txns
    
    // Scan all hash map buckets for active transactions
    for (size_t i = 0; i < manager->txn_map.bucket_count; i++) {
        txn_info_t *entry = manager->txn_map.buckets[i];
        while (entry) {
            if (entry->state == TXN_STATE_ACTIVE) {
                // Grow array if needed
                if (snapshot->num_active >= snapshot->active_capacity) {
                    size_t new_capacity = snapshot->active_capacity * 2;
                    uint64_t *new_array = realloc(snapshot->active_txns,
                                                 new_capacity * sizeof(uint64_t));
                    if (!new_array) {
                        pthread_rwlock_unlock(&manager->txn_map.resize_lock);
                        free(snapshot->active_txns);
                        free(snapshot);
                        LOG_ERROR("Failed to grow active_txns array");
                        return NULL;
                    }
                    snapshot->active_txns = new_array;
                    snapshot->active_capacity = new_capacity;
                }
                
                snapshot->active_txns[snapshot->num_active++] = entry->txn_id;
                
                // Update min to oldest active transaction
                if (entry->txn_id < min_txn_id) {
                    min_txn_id = entry->txn_id;
                }
            }
            entry = entry->next;
        }
    }
    
    pthread_rwlock_unlock(&manager->txn_map.resize_lock);
    
    // Store epoch in xmin field for GC tracking
    snapshot->xmin = current_epoch;
    
    // Initialize reference count
    snapshot->ref_count = 1;
    snapshot->next = NULL;
    
    // Add to snapshot list
    pthread_rwlock_wrlock(&manager->snapshot_lock);
    snapshot->next = manager->snapshots;
    manager->snapshots = snapshot;
    manager->snapshots_created++;
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    LOG_DEBUG("Snapshot created: snapshot_id=%lu, epoch=%lu, xmax=%lu, num_active=%zu",
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
        
        // Trigger GC after snapshot release
        uint64_t min_safe_epoch = epoch_gc_compute_min_safe_epoch(manager);
        epoch_gc_reclaim(&manager->gc, min_safe_epoch);
    }
}

bool mvcc_has_active_snapshots(mvcc_manager_t *manager) {
    if (!manager) return false;
    
    pthread_rwlock_rdlock(&manager->snapshot_lock);
    bool has_snapshots = (manager->snapshots != NULL);
    pthread_rwlock_unlock(&manager->snapshot_lock);
    
    return has_snapshots;
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
    printf("  Transaction map buckets: %zu\n", manager->txn_map.bucket_count);
    printf("  Transaction map active: %zu\n", manager->txn_map.active_count);
    printf("  GC retired entries: %zu\n", manager->gc.retired_count);
    printf("  GC current epoch: %lu\n", manager->gc.global_epoch);
    printf("  Snapshots created: %lu\n", manager->snapshots_created);
    printf("  Snapshots released: %lu\n", manager->snapshots_released);
    printf("  Active snapshots: %zu\n", mvcc_active_snapshot_count((mvcc_manager_t *)manager));
    printf("  Visibility checks: %lu\n", manager->visibility_checks);
    printf("================================================================\n");
}
