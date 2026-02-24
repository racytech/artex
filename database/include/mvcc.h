/**
 * MVCC (Multi-Version Concurrency Control) for Persistent ART
 * 
 * Provides snapshot isolation and concurrent read access through versioning.
 * 
 * Key concepts:
 * - Transaction ID (txn_id): Monotonically increasing counter
 * - Snapshot: Point-in-time view of database at specific txn_id
 * - Visibility: Rules for determining which version a transaction can see
 * - Version chains: Multiple versions of same key linked together
 * 
 * Isolation Level: Snapshot Isolation (SI)
 * - Readers never block writers
 * - Writers never block readers
 * - Writers block other writers (single writer at a time)
 * - Each transaction sees consistent snapshot at begin time
 * 
 * Based on PostgreSQL's MVCC design with xmin/xmax visibility.
 */

#ifndef MVCC_H
#define MVCC_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>

// ============================================================================
// Transaction State
// ============================================================================

/**
 * Transaction state
 */
typedef enum {
    TXN_STATE_ACTIVE    = 0,  // Transaction in progress
    TXN_STATE_COMMITTED = 1,  // Transaction committed successfully
    TXN_STATE_ABORTED   = 2   // Transaction aborted/rolled back
} txn_state_t;

/**
 * Transaction information
 * 
 * Tracks state of a transaction for visibility checks.
 * Forms linked list for hash map chaining.
 */
typedef struct txn_info {
    uint64_t txn_id;           // Transaction ID
    txn_state_t state;         // Current state
    uint64_t commit_ts;        // Commit timestamp (0 if not committed)
    uint64_t epoch;            // Epoch when entry was created
    struct txn_info *next;     // Next in hash chain
} txn_info_t;

// ============================================================================
// Snapshot
// ============================================================================

/**
 * Snapshot represents a point-in-time view of the database
 * 
 * A snapshot captures:
 * - The snapshot timestamp (when snapshot was taken)
 * - List of active transactions at snapshot time
 * - Used for visibility checks: which versions can this snapshot see?
 */
typedef struct mvcc_snapshot {
    uint64_t snapshot_id;          // Snapshot ID (same as txn_id at snapshot time)
    uint64_t xmin;                 // Minimum active txn_id at snapshot time
    uint64_t xmax;                 // Next txn_id to be assigned at snapshot time
    
    // Active transactions at snapshot time (for visibility checks)
    uint64_t *active_txns;         // Array of active txn_ids
    size_t num_active;             // Number of active transactions
    size_t active_capacity;        // Allocated capacity
    
    // Reference counting for garbage collection
    uint32_t ref_count;            // Number of references to this snapshot

    // Lifetime tracking
    struct timespec created_at;    // When this snapshot was created (CLOCK_MONOTONIC)

    // Linked list for snapshot tracking
    struct mvcc_snapshot *next;
} mvcc_snapshot_t;

// ============================================================================
// MVCC Manager
// ============================================================================

/**
 * Retired transaction entry for epoch-based garbage collection
 */
typedef struct retired_txn {
    txn_info_t *entry;              // Transaction entry to be freed
    uint64_t retire_epoch;          // Epoch when entry was retired
    struct retired_txn *next;       // Next in retirement queue
} retired_txn_t;

/**
 * Transaction hash map for dynamic growth
 */
typedef struct {
    txn_info_t **buckets;           // Array of hash buckets
    size_t bucket_count;            // Number of buckets (power of 2)
    size_t active_count;            // Number of active entries
    size_t threshold;               // Resize threshold (75% of bucket_count)
    pthread_rwlock_t resize_lock;   // Lock for concurrent resizing
} txn_map_t;

/**
 * Epoch-based garbage collection system
 */
typedef struct {
    uint64_t global_epoch;          // Current global epoch
    pthread_mutex_t epoch_lock;     // Lock for epoch advancement
    retired_txn_t *retired_queue;   // Queue of retired entries
    size_t retired_count;           // Number of retired entries
} epoch_gc_t;

/**
 * MVCC Manager - coordinates transaction state and snapshots
 * 
 * Central coordinator for all MVCC operations:
 * - Assigns transaction IDs
 * - Tracks transaction states (via dynamic hash map)
 * - Creates and manages snapshots
 * - Provides visibility checks
 * - Garbage collects old transaction entries
 */
typedef struct mvcc_manager {
    // Transaction ID allocation
    uint64_t next_txn_id;          // Next transaction ID to assign
    pthread_mutex_t txn_id_lock;   // Lock for txn_id allocation
    
    // Transaction state tracking (dynamic hash map)
    txn_map_t txn_map;             // Dynamic hash map for transaction states
    
    // Epoch-based garbage collection
    epoch_gc_t gc;                 // GC system for safe reclamation
    
    // Active snapshots
    mvcc_snapshot_t *snapshots;    // Linked list of active snapshots
    pthread_rwlock_t snapshot_lock; // RW lock for snapshot list
    uint64_t snapshot_timeout_ms;  // Max snapshot lifetime in ms (0 = no timeout)

    // Statistics
    uint64_t snapshots_created;
    uint64_t snapshots_released;
    uint64_t visibility_checks;
} mvcc_manager_t;

// ============================================================================
// MVCC Manager API
// ============================================================================

/**
 * Create a new MVCC manager
 * 
 * @return Pointer to manager, or NULL on failure
 */
mvcc_manager_t *mvcc_manager_create(void);

/**
 * Destroy MVCC manager and free resources
 * 
 * @param manager Manager to destroy
 */
void mvcc_manager_destroy(mvcc_manager_t *manager);

/**
 * Begin a new transaction and assign transaction ID
 * 
 * @param manager MVCC manager
 * @param txn_id_out Output parameter for assigned transaction ID
 * @return true on success, false on failure
 */
bool mvcc_begin_txn(mvcc_manager_t *manager, uint64_t *txn_id_out);

/**
 * Commit a transaction
 * 
 * Marks transaction as committed and records commit timestamp.
 * 
 * @param manager MVCC manager
 * @param txn_id Transaction ID to commit
 * @return true on success, false on failure
 */
bool mvcc_commit_txn(mvcc_manager_t *manager, uint64_t txn_id);

/**
 * Abort a transaction
 * 
 * Marks transaction as aborted. All versions created by this transaction
 * become invisible.
 * 
 * @param manager MVCC manager
 * @param txn_id Transaction ID to abort
 * @return true on success, false on failure
 */
bool mvcc_abort_txn(mvcc_manager_t *manager, uint64_t txn_id);

/**
 * Create a snapshot for read operations
 * 
 * Captures current transaction state and creates a consistent snapshot.
 * The snapshot must be released with mvcc_snapshot_release() when done.
 * 
 * @param manager MVCC manager
 * @return Snapshot pointer, or NULL on failure
 */
mvcc_snapshot_t *mvcc_snapshot_create(mvcc_manager_t *manager);

/**
 * Acquire reference to snapshot (increment ref count)
 * 
 * @param snapshot Snapshot to acquire
 */
void mvcc_snapshot_acquire(mvcc_snapshot_t *snapshot);

/**
 * Release snapshot (decrement ref count, free if zero)
 *
 * @param manager MVCC manager
 * @param snapshot Snapshot to release
 */
void mvcc_snapshot_release(mvcc_manager_t *manager, mvcc_snapshot_t *snapshot);

/**
 * Set snapshot timeout (max lifetime before force-expiration)
 *
 * @param manager MVCC manager
 * @param timeout_ms Timeout in milliseconds (0 = no timeout, default)
 */
void mvcc_set_snapshot_timeout(mvcc_manager_t *manager, uint64_t timeout_ms);

/**
 * Expire snapshots that have exceeded the timeout
 *
 * Force-releases snapshots whose age exceeds snapshot_timeout_ms.
 * Called automatically during periodic GC, or can be called manually.
 *
 * @param manager MVCC manager
 * @return Number of snapshots expired
 */
size_t mvcc_expire_snapshots(mvcc_manager_t *manager);

/**
 * Check if there are any active snapshots
 * 
 * Used to determine if logical deletes are needed (to preserve versions
 * for concurrent readers).
 * 
 * @param manager MVCC manager
 * @return true if there are active snapshots, false otherwise
 */
bool mvcc_has_active_snapshots(mvcc_manager_t *manager);

// ============================================================================
// Visibility Checks
// ============================================================================

/**
 * Check if a version is visible to a snapshot
 * 
 * Visibility rules (Snapshot Isolation):
 * 1. Version created by our own transaction? Always visible
 * 2. Version created after snapshot? Not visible
 * 3. Version created by active transaction at snapshot time? Not visible
 * 4. Version created by committed transaction before snapshot? Visible
 * 5. Version deleted by transaction after snapshot? Visible (delete not seen)
 * 6. Version deleted by committed transaction before snapshot? Not visible
 * 
 * @param manager MVCC manager
 * @param snapshot Snapshot doing the check
 * @param xmin Transaction that created this version
 * @param xmax Transaction that deleted this version (0 if not deleted)
 * @param current_txn_id Transaction ID of current transaction (for "our own" check)
 * @return true if version is visible to snapshot, false otherwise
 */
bool mvcc_is_visible(mvcc_manager_t *manager,
                     const mvcc_snapshot_t *snapshot,
                     uint64_t xmin,
                     uint64_t xmax,
                     uint64_t current_txn_id);

/**
 * Check if transaction was active at snapshot time
 * 
 * @param snapshot Snapshot to check against
 * @param txn_id Transaction ID to check
 * @return true if transaction was active, false otherwise
 */
bool mvcc_txn_was_active(const mvcc_snapshot_t *snapshot, uint64_t txn_id);

/**
 * Get transaction state
 * 
 * @param manager MVCC manager
 * @param txn_id Transaction ID
 * @return Transaction state, or TXN_STATE_ABORTED if not found
 */
txn_state_t mvcc_get_txn_state(mvcc_manager_t *manager, uint64_t txn_id);

// ============================================================================
// Statistics & Debugging
// ============================================================================

/**
 * Get number of active snapshots
 * 
 * @param manager MVCC manager
 * @return Number of snapshots with ref_count > 0
 */
size_t mvcc_active_snapshot_count(mvcc_manager_t *manager);

/**
 * Get oldest active snapshot ID
 * 
 * Used for garbage collection: versions older than this are potentially
 * eligible for cleanup.
 * 
 * @param manager MVCC manager
 * @param oldest_snapshot_out Output parameter for oldest snapshot ID
 * @return true if there are active snapshots, false otherwise
 */
bool mvcc_get_oldest_snapshot(mvcc_manager_t *manager, uint64_t *oldest_snapshot_out);

/**
 * Print MVCC manager statistics
 * 
 * @param manager MVCC manager
 */
void mvcc_print_stats(const mvcc_manager_t *manager);

#endif // MVCC_H
