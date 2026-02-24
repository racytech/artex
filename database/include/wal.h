/**
 * Write-Ahead Log (WAL) for Persistent ART Database
 * 
 * Provides durability guarantees via write-ahead logging:
 * - All modifications logged BEFORE applying to tree
 * - Log entries synced to disk (fsync) BEFORE transaction commit
 * - Crash recovery replays log from last checkpoint
 * - Periodic checkpoints allow log truncation
 * 
 * Design Philosophy:
 * - Durability First: Always fsync() before commit (no compromises)
 * - Simple Format: Fixed-size headers + variable payloads
 * - Sequential Writes: Append-only for maximum throughput
 * - Bounded Recovery: Checkpoint every 256MB to limit replay time
 * 
 * Log Format:
 *   [WAL Header] [Entry 1] [Entry 2] ... [Entry N]
 * 
 * Each entry:
 *   [Entry Header (32 bytes)] [Payload (variable)]
 * 
 * References:
 * - TODO.md Section 4: Checkpoint Mechanism (256MB/10min/50k pages triggers)
 * - TODO.md Section 8: Error Handling & Durability (WAL fsync protocol)
 * - ERROR_HANDLING.md: Crash recovery and fsync retry logic
 */

#ifndef WAL_H
#define WAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Constants & Configuration
// ============================================================================

#define WAL_MAGIC 0x57414C0A  // "WAL\n" - for format validation
#define WAL_VERSION 1         // Schema version

#define WAL_MAX_ENTRY_SIZE (4 * 1024 * 1024)  // 4MB max entry (for large values)
#define WAL_SEGMENT_SIZE (64 * 1024 * 1024)   // 64MB per segment file
#define WAL_BUFFER_SIZE (256 * 1024)          // 256KB write buffer

// Checkpoint triggers (from TODO.md Section 4)
#define WAL_CHECKPOINT_SIZE_THRESHOLD (256 * 1024 * 1024)  // 256MB of WAL
#define WAL_CHECKPOINT_TIME_THRESHOLD 600                   // 10 minutes
#define WAL_CHECKPOINT_PAGES_THRESHOLD 50000                // 50k dirty pages

// ============================================================================
// WAL Entry Types
// ============================================================================

/**
 * WAL entry types
 * 
 * Each modification to the tree is logged as one of these entry types.
 * During recovery, entries are replayed in order to reconstruct tree state.
 */
typedef enum {
    WAL_ENTRY_INSERT       = 1,   // Insert or update key-value pair
    WAL_ENTRY_DELETE       = 2,   // Delete key
    WAL_ENTRY_BEGIN_TXN    = 3,   // Transaction begin
    WAL_ENTRY_COMMIT_TXN   = 4,   // Transaction commit
    WAL_ENTRY_ABORT_TXN    = 5,   // Transaction abort (rollback)
    WAL_ENTRY_CHECKPOINT   = 6,   // Checkpoint marker
    WAL_ENTRY_NOOP         = 7,   // No-op (for testing)
} wal_entry_type_t;

// ============================================================================
// WAL Entry Format
// ============================================================================

/**
 * WAL entry header (fixed 32 bytes)
 * 
 * Layout designed for:
 * - Fast validation (magic + checksum)
 * - Sequential scan (prev_lsn linkage)
 * - Variable payload support (payload_len)
 */
typedef struct {
    uint32_t magic;           // WAL_MAGIC (validation)
    uint32_t entry_type;      // wal_entry_type_t
    uint64_t lsn;             // Log Sequence Number (monotonic)
    uint64_t txn_id;          // Transaction ID (0 = no transaction)
    uint32_t payload_len;     // Length of payload following this header
    uint32_t checksum;        // CRC32 of (header + payload)
    uint64_t timestamp;       // Unix timestamp (seconds since epoch)
} __attribute__((packed)) wal_entry_header_t;

/**
 * Insert/Update entry payload
 * 
 * Variable size: sizeof(wal_insert_payload_t) + key_len + value_len
 */
typedef struct {
    uint32_t key_len;
    uint32_t value_len;
    uint8_t data[];           // key followed by value
} __attribute__((packed)) wal_insert_payload_t;

/**
 * Delete entry payload
 * 
 * Variable size: sizeof(wal_delete_payload_t) + key_len
 */
typedef struct {
    uint32_t key_len;
    uint8_t key[];
} __attribute__((packed)) wal_delete_payload_t;

/**
 * Transaction entry payload (begin/commit/abort)
 * 
 * Fixed size: 16 bytes
 */
typedef struct {
    uint64_t txn_id;
    uint64_t timestamp;
} __attribute__((packed)) wal_txn_payload_t;

/**
 * Checkpoint entry payload
 * 
 * Fixed size: 32 bytes
 * 
 * Records tree state at checkpoint time:
 * - root_page_id/root_offset: Tree root location
 * - tree_size: Number of entries in tree
 * - next_page_id: Next available page ID
 */
typedef struct {
    uint64_t root_page_id;
    uint32_t root_offset;
    uint32_t padding1;
    uint64_t tree_size;
    uint64_t next_page_id;
    uint64_t timestamp;
} __attribute__((packed)) wal_checkpoint_payload_t;

// ============================================================================
// WAL File Format
// ============================================================================

/**
 * WAL file header (first 512 bytes of each segment)
 * 
 * Each WAL segment file starts with this header:
 * - Validates file format (magic + version)
 * - Tracks segment number for ordering
 * - Contains integrity checksum
 */
typedef struct {
    uint32_t magic;           // WAL_MAGIC
    uint32_t version;         // WAL_VERSION
    uint64_t segment_id;      // Segment number (0, 1, 2, ...)
    uint64_t first_lsn;       // LSN of first entry in this segment
    uint64_t last_lsn;        // LSN of last entry (updated on close)
    uint64_t created_at;      // Timestamp when segment created
    uint32_t entry_count;     // Number of entries in segment
    uint32_t checksum;        // CRC32 of this header
    uint8_t padding[472];     // Reserved for future use (total 512 bytes)
} __attribute__((packed)) wal_segment_header_t;

// ============================================================================
// WAL Manager
// ============================================================================

/**
 * WAL configuration
 */
typedef struct {
    // Checkpoint triggers
    uint64_t checkpoint_size_threshold;   // Bytes of WAL (default: 256MB)
    uint64_t checkpoint_time_threshold;   // Seconds (default: 600 = 10 min)
    uint64_t checkpoint_pages_threshold;  // Dirty pages (default: 50k)
    
    // I/O behavior
    bool fsync_on_commit;                 // Always true (durability)
    uint32_t fsync_retry_max;             // Max fsync retries (default: 3)
    uint32_t fsync_retry_delay_us;        // Initial backoff (default: 100µs)
    
    // Segment management
    uint64_t segment_size;                // Bytes per segment (default: 64MB)
    bool auto_truncate;                   // Auto-delete old segments (default: true)
    uint32_t keep_segments;               // Min segments to keep (default: 2)
} wal_config_t;

/**
 * WAL statistics
 */
typedef struct {
    // Write stats
    uint64_t total_entries;               // Total entries written
    uint64_t total_bytes;                 // Total bytes written
    uint64_t inserts;                     // Insert entries
    uint64_t deletes;                     // Delete entries
    uint64_t commits;                     // Commit entries
    uint64_t aborts;                      // Abort entries
    uint64_t checkpoints;                 // Checkpoint entries
    
    // Sync stats
    uint64_t fsync_calls;                 // Total fsync() calls
    uint64_t fsync_retries;               // Total fsync retries
    uint64_t fsync_failures;              // Permanent fsync failures
    uint64_t fsync_total_time_us;         // Total time in fsync (microseconds)
    
    // Segment stats
    uint32_t current_segment_id;          // Active segment ID
    uint32_t total_segments;              // Total segments created
    uint32_t active_segments;             // Segments not truncated
    
    // Recovery stats
    uint64_t last_recovery_entries;       // Entries replayed in last recovery
    uint64_t last_recovery_time_ms;       // Recovery duration (milliseconds)
    
    // Checkpoint trigger stats (from CHECKPOINT.md)
    uint64_t checkpoint_wal_triggered;    // Checkpoints triggered by WAL size
    uint64_t checkpoint_time_triggered;   // Checkpoints triggered by time
    uint64_t checkpoint_pages_triggered;  // Checkpoints triggered by dirty pages
} wal_stats_t;

/**
 * WAL manager instance
 * 
 * Manages WAL segments, buffers writes, handles fsync, and coordinates
 * with checkpoint system.
 */
typedef struct {
    // Configuration
    wal_config_t config;
    char *wal_dir;                        // Directory for WAL segments
    
    // Current segment
    int segment_fd;                       // File descriptor for active segment
    uint64_t segment_id;                  // Current segment number
    uint64_t segment_offset;              // Write position in current segment
    wal_segment_header_t *segment_header; // Active segment header
    
    // Write buffer (for batching small writes)
    uint8_t *write_buffer;
    size_t buffer_size;
    size_t buffer_offset;                 // Current position in buffer
    
    // LSN tracking
    uint64_t next_lsn;                    // Next LSN to assign
    uint64_t last_fsynced_lsn;            // Last LSN that was fsynced
    uint64_t last_checkpoint_lsn;         // LSN of last checkpoint
    
    // Statistics
    wal_stats_t stats;
    
    // Synchronization
    pthread_rwlock_t lock;                // Protects WAL state
    
    // Checkpoint coordination (from CHECKPOINT.md)
    uint64_t bytes_since_checkpoint;      // Bytes written since last checkpoint
    uint64_t last_checkpoint_time;        // Unix timestamp of last checkpoint
    bool checkpoint_in_progress;          // Checkpoint currently running
    
    // Dirty page tracking (for checkpoint trigger)
    uint64_t dirty_pages_count;           // Current dirty pages in buffer pool
} wal_t;

// ============================================================================
// WAL Operations - Lifecycle
// ============================================================================

/**
 * Get default WAL configuration
 * 
 * Returns sensible defaults based on TODO.md design:
 * - 256MB checkpoint size threshold
 * - 10 minute checkpoint time threshold
 * - 50k pages checkpoint threshold
 * - fsync always enabled
 * - 3 fsync retries with exponential backoff
 * 
 * @return Default configuration
 */
wal_config_t wal_default_config(void);

/**
 * Create and open a new WAL
 * 
 * Opens or creates WAL segments in the specified directory.
 * If segments exist, validates format and prepares for append.
 * 
 * @param wal_dir Directory to store WAL segments
 * @param config WAL configuration (NULL for defaults)
 * @return WAL instance, or NULL on failure
 */
wal_t *wal_open(const char *wal_dir, const wal_config_t *config);

/**
 * Close WAL and flush all pending writes
 * 
 * Flushes write buffer, fsyncs current segment, updates segment header.
 * Does NOT truncate segments - use wal_truncate() first if desired.
 * 
 * @param wal WAL instance
 */
void wal_close(wal_t *wal);

/**
 * Get current LSN (last assigned)
 * 
 * Returns the LSN of the most recently written entry.
 * Thread-safe.
 * 
 * @param wal WAL instance
 * @return Current LSN
 */
uint64_t wal_current_lsn(const wal_t *wal);

// ============================================================================
// WAL Operations - Write Entries
// ============================================================================

/**
 * Append an insert/update entry to WAL
 * 
 * Logs a key-value pair insertion or update.
 * Does NOT fsync - use wal_fsync() or wal_commit_txn() to ensure durability.
 * 
 * Thread-safe: Multiple writers will be serialized by internal lock.
 * 
 * @param wal WAL instance
 * @param txn_id Transaction ID (0 if not in transaction)
 * @param key Key bytes
 * @param key_len Key length
 * @param value Value bytes
 * @param value_len Value length
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success, false on I/O error
 */
bool wal_log_insert(wal_t *wal, uint64_t txn_id,
                    const uint8_t *key, uint32_t key_len,
                    const uint8_t *value, uint32_t value_len,
                    uint64_t *lsn_out);

/**
 * Append a delete entry to WAL
 * 
 * Logs a key deletion.
 * Does NOT fsync - use wal_fsync() or wal_commit_txn() to ensure durability.
 * 
 * @param wal WAL instance
 * @param txn_id Transaction ID (0 if not in transaction)
 * @param key Key bytes
 * @param key_len Key length
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success, false on I/O error
 */
bool wal_log_delete(wal_t *wal, uint64_t txn_id,
                    const uint8_t *key, uint32_t key_len,
                    uint64_t *lsn_out);

/**
 * Append a transaction begin entry
 * 
 * Marks the start of a transaction.
 * Logs a begin-transaction entry with the given transaction ID (allocated by
 * the MVCC manager) which will be used for all subsequent operations until
 * commit or abort.
 * 
 * @param wal WAL instance
 * @param txn_id Transaction ID (allocated by MVCC manager)
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success, false on I/O error
 */
bool wal_log_begin_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out);

/**
 * Append a transaction commit entry
 * 
 * Marks the successful completion of a transaction.
 * This entry WILL be fsynced to ensure durability before returning.
 * 
 * CRITICAL: This function implements the durability guarantee.
 * If fsync fails after retries, returns false and transaction is NOT committed.
 * 
 * @param wal WAL instance
 * @param txn_id Transaction ID to commit
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success and fsync, false on fsync failure
 */
bool wal_log_commit_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out);

/**
 * Append a transaction abort entry
 * 
 * Marks transaction rollback. Operations in this transaction will be ignored
 * during recovery.
 * 
 * @param wal WAL instance
 * @param txn_id Transaction ID to abort
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success, false on I/O error
 */
bool wal_log_abort_txn(wal_t *wal, uint64_t txn_id, uint64_t *lsn_out);

/**
 * Append a checkpoint entry
 * 
 * Records current tree state. Allows truncation of WAL entries before this point.
 * This entry WILL be fsynced to ensure checkpoint is durable.
 * 
 * @param wal WAL instance
 * @param root_page_id Tree root page ID
 * @param root_offset Tree root offset within page
 * @param tree_size Number of entries in tree
 * @param next_page_id Next available page ID
 * @param lsn_out Output: Assigned LSN for this entry
 * @return true on success and fsync, false on fsync failure
 */
bool wal_log_checkpoint(wal_t *wal,
                        uint64_t root_page_id, uint32_t root_offset,
                        uint64_t tree_size, uint64_t next_page_id,
                        uint64_t *lsn_out);

// ============================================================================
// WAL Operations - Durability
// ============================================================================

/**
 * Flush write buffer and fsync WAL to disk
 * 
 * Guarantees all entries up to current LSN are durable.
 * Implements retry logic with exponential backoff per ERROR_HANDLING.md.
 * 
 * CRITICAL: This is the durability barrier. If this fails, data may be lost
 * on crash. Caller should abort transaction on failure.
 * 
 * @param wal WAL instance
 * @return true on success, false after max retries exhausted
 */
bool wal_fsync(wal_t *wal);

/**
 * Check if checkpoint should be triggered
 * 
 * Returns true if any checkpoint threshold is exceeded:
 * - WAL size >= 256MB (PRIMARY - from CHECKPOINT.md)
 * - Time since last checkpoint >= 10 minutes (BACKUP)
 * - Dirty pages >= 50k pages (SAFETY - requires dirty_pages_count update)
 * 
 * This implements the hybrid trigger strategy from CHECKPOINT.md.
 * 
 * @param wal WAL instance
 * @param trigger_out Output: Which trigger fired (optional, can be NULL)
 *                    0 = none, 1 = WAL size, 2 = time, 3 = dirty pages
 * @return true if checkpoint is recommended
 */
bool wal_should_checkpoint(const wal_t *wal, uint32_t *trigger_out);

/**
 * Update dirty pages count for checkpoint trigger
 * 
 * Should be called by buffer pool when dirty page count changes.
 * Used for the 50k dirty pages checkpoint trigger.
 * 
 * @param wal WAL instance
 * @param dirty_pages_count Current number of dirty pages in buffer pool
 */
void wal_update_dirty_pages(wal_t *wal, uint64_t dirty_pages_count);

/**
 * Notify WAL that checkpoint completed
 * 
 * Resets checkpoint tracking counters (bytes_since_checkpoint, time).
 * Should be called after successful checkpoint.
 * 
 * @param wal WAL instance
 * @param checkpoint_lsn LSN of the checkpoint
 */
void wal_checkpoint_completed(wal_t *wal, uint64_t checkpoint_lsn);

/**
 * Truncate WAL entries before checkpoint LSN
 * 
 * Deletes old segment files that contain only entries before checkpoint_lsn.
 * Keeps at least config.keep_segments segments for safety.
 * 
 * Should be called after successful checkpoint.
 * 
 * @param wal WAL instance
 * @param checkpoint_lsn LSN of checkpoint (entries before this can be deleted)
 * @return Number of segments truncated
 */
uint32_t wal_truncate(wal_t *wal, uint64_t checkpoint_lsn);

// ============================================================================
// WAL Operations - Recovery
// ============================================================================

/**
 * Replay WAL entries to recover tree state
 * 
 * Reads entries from start_lsn to end_lsn and calls apply_fn for each entry.
 * The apply function should reconstruct tree state from the log entries.
 * 
 * Used during startup to recover from crashes:
 * 1. Load last checkpoint (get checkpoint_lsn and tree state)
 * 2. Replay from checkpoint_lsn to end of log
 * 3. Tree is now consistent with last committed transaction
 * 
 * @param wal WAL instance
 * @param start_lsn First LSN to replay (usually checkpoint_lsn)
 * @param end_lsn Last LSN to replay (usually current_lsn)
 * @param context User context passed to apply_fn
 * @param apply_fn Callback to apply each entry to tree
 * @return Number of entries replayed, or -1 on error
 */
int64_t wal_replay(wal_t *wal, uint64_t start_lsn, uint64_t end_lsn,
                   void *context,
                   bool (*apply_fn)(void *context, 
                                   const wal_entry_header_t *header,
                                   const void *payload));

/**
 * Scan WAL to find last valid LSN
 * 
 * Reads all segments to find the last entry with valid checksum.
 * Used during recovery to determine end_lsn for replay.
 * 
 * @param wal WAL instance
 * @return Last valid LSN, or 0 if WAL is empty
 */
uint64_t wal_find_last_lsn(const wal_t *wal);

/**
 * Validate WAL segment integrity
 * 
 * Checks segment header and scans all entries for:
 * - Valid magic numbers
 * - Valid checksums
 * - LSN ordering
 * 
 * @param wal WAL instance
 * @param segment_id Segment ID to validate
 * @param errors_out Output: Error count (optional, can be NULL)
 * @return true if segment is valid, false if corrupted
 */
bool wal_validate_segment(const wal_t *wal, uint64_t segment_id, 
                          uint32_t *errors_out);

// ============================================================================
// WAL Operations - Statistics & Debugging
// ============================================================================

/**
 * Get WAL statistics
 * 
 * Returns current statistics including entry counts, fsync stats, and segment info.
 * Thread-safe - returns snapshot of stats at call time.
 * 
 * @param wal WAL instance
 * @param stats_out Output: Statistics structure
 */
void wal_get_stats(const wal_t *wal, wal_stats_t *stats_out);

/**
 * Reset statistics counters
 * 
 * Resets all counters to zero. Does not affect LSN or segment tracking.
 * Useful for benchmarking.
 * 
 * @param wal WAL instance
 */
void wal_reset_stats(wal_t *wal);

/**
 * Print WAL statistics (for debugging)
 * 
 * Logs statistics to stdout in human-readable format.
 * 
 * @param wal WAL instance
 */
void wal_print_stats(const wal_t *wal);

/**
 * List all WAL segments in directory
 * 
 * Returns array of segment IDs sorted in ascending order.
 * Caller must free the returned array.
 * 
 * @param wal_dir Directory containing WAL segments
 * @param count_out Output: Number of segments found
 * @return Array of segment IDs, or NULL if none found
 */
uint64_t *wal_list_segments(const char *wal_dir, uint32_t *count_out);

#ifdef __cplusplus
}
#endif

#endif // WAL_H
