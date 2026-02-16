# Error Handling & Durability

## Overview

The persistent ART database must handle failures gracefully while maintaining data integrity and durability guarantees. This document describes the error handling strategy, durability protocol, and recovery mechanisms.

## Core Principles

1. **Durability First** - WAL fsync() before commit (no compromises)
2. **Fail-Fast on Corruption** - Don't continue with bad data
3. **Graceful Degradation** - Handle transient errors with retries
4. **Clear Error Reporting** - Return detailed error codes, not generic -1
5. **Crash Recovery Always Works** - WAL replay must be robust

## Error Categories

### 1. Disk Full (ENOSPC)

**Nature**: Resource exhaustion, recoverable

**Strategy**: Abort transaction cleanly, return error to caller

```c
typedef enum {
    WRITE_SUCCESS = 0,
    WRITE_DISK_FULL = -1,
    WRITE_IO_ERROR = -2,
} write_result_t;

write_result_t page_manager_write(page_manager_t *pm, uint64_t page_id, 
                                   const uint8_t *data, size_t size) {
    int fd = open_data_file(pm, page_id);
    
    ssize_t written = write(fd, data, size);
    if (written == -1) {
        if (errno == ENOSPC) {
            LOG_ERROR("Disk full writing page %lu", page_id);
            close(fd);
            return WRITE_DISK_FULL;
        }
        
        LOG_ERROR("Write failed for page %lu: %s", page_id, strerror(errno));
        close(fd);
        return WRITE_IO_ERROR;
    }
    
    close(fd);
    return WRITE_SUCCESS;
}

// Caller handles disk full
int data_art_commit_write(data_art_txn_t *txn) {
    // ... write operations ...
    
    write_result_t result = page_manager_write(txn->tree->pm, page_id, data, size);
    if (result == WRITE_DISK_FULL) {
        LOG_ERROR("Transaction aborted: disk full");
        data_art_abort_write(txn);
        return ERROR_DISK_FULL;
    }
    
    // ... continue ...
}
```

**User Action**:
- Free up disk space
- Retry transaction
- Database remains consistent (transaction never committed)

**Database State**: Healthy (no data lost, can continue)

---

### 2. I/O Errors (EIO, etc.)

**Nature**: Transient or hardware failure

**Strategy**: Retry with exponential backoff, then fail-stop

```c
#define MAX_IO_RETRIES 3

typedef struct {
    int retry_count;
    uint64_t retry_delay_us;  // Exponential backoff
} retry_state_t;

int fsync_with_retry(int fd, const char *context) {
    retry_state_t retry = {
        .retry_count = 0,
        .retry_delay_us = 100,  // Start with 100µs
    };
    
    while (retry.retry_count < MAX_IO_RETRIES) {
        if (fsync(fd) == 0) {
            if (retry.retry_count > 0) {
                LOG_WARN("fsync succeeded after %d retries (%s)",
                         retry.retry_count, context);
            }
            return 0;  // Success
        }
        
        // Check error type
        if (errno == EINTR) {
            // Interrupted by signal - retry immediately
            continue;
        }
        
        if (errno == EAGAIN || errno == EIO) {
            // Transient error - retry with backoff
            LOG_WARN("fsync retry %d/%d: %s (%s)",
                     retry.retry_count + 1, MAX_IO_RETRIES,
                     strerror(errno), context);
            
            usleep(retry.retry_delay_us);
            retry.retry_delay_us *= 2;  // Exponential backoff
            retry.retry_count++;
            continue;
        }
        
        // Unrecoverable error
        break;
    }
    
    // Failed after retries or unrecoverable error
    LOG_CRITICAL("fsync failed after %d retries: %s (%s)",
                 retry.retry_count, strerror(errno), context);
    return -1;
}
```

**User Action**:
- Check hardware (disk failure?)
- Check system logs (kernel errors?)
- Restore from backup if hardware failure

**Database State**: Unhealthy (may recover if transient), Corrupted (if persistent hardware failure)

---

### 3. Corrupted Page (Checksum Mismatch)

**Nature**: Data integrity violation

**Strategy**: Fail-stop, do not propagate corruption

```c
page_t *page_manager_load_safe(page_manager_t *pm, uint64_t page_id) {
    // Read page from disk
    page_t *page = read_page_from_disk(pm, page_id);
    if (!page) {
        return NULL;
    }
    
    // Verify checksum
    uint32_t computed_checksum = crc32(page->data, 
                                       PAGE_SIZE - sizeof(page_header_t));
    
    if (page->header.checksum != computed_checksum) {
        LOG_CRITICAL("Corruption detected: page %lu checksum mismatch", 
                     page_id);
        LOG_CRITICAL("  Expected: 0x%08x", page->header.checksum);
        LOG_CRITICAL("  Computed: 0x%08x", computed_checksum);
        LOG_CRITICAL("  Page version: %lu", page->header.version);
        LOG_CRITICAL("  Last modified: %lu", page->header.last_access_time);
        
        // Mark database as corrupted
        mark_database_corrupted(pm->tree);
        
        // Free page, return NULL
        free(page);
        return NULL;
    }
    
    return page;
}

// Caller must check for NULL
data_art_node_t *load_node(data_art_tree_t *tree, node_ref_t ref) {
    page_t *page = page_manager_load_safe(tree->pm, ref.page_id);
    if (!page) {
        // Corruption detected or I/O error
        LOG_ERROR("Failed to load page %lu", ref.page_id);
        return NULL;
    }
    
    data_art_node_t *node = (data_art_node_t*)(page->data + ref.offset);
    return node;
}
```

**User Action**:
- **STOP IMMEDIATELY** - do not write more data
- Restore from backup
- Investigate cause (disk failure, software bug, cosmic rays?)

**Database State**: Corrupted (cannot continue safely)

---

### 4. WAL Sync Failure

**Nature**: Cannot guarantee durability

**Strategy**: Abort transaction, do not commit

```c
int wal_sync(wal_t *wal) {
    // Flush write buffer
    if (fflush(wal->file) != 0) {
        LOG_ERROR("WAL fflush failed: %s", strerror(errno));
        return -1;
    }
    
    // Sync to disk
    int result = fsync_with_retry(fileno(wal->file), "WAL");
    if (result != 0) {
        LOG_CRITICAL("WAL fsync failed - cannot guarantee durability");
        return -1;
    }
    
    return 0;
}

int data_art_commit_write(data_art_txn_t *txn) {
    // Write WAL entries (buffered)
    wal_write_entries(txn->wal_txn, txn->operations);
    wal_write_commit(txn->wal_txn);
    
    // CRITICAL: Sync WAL before updating tree
    if (wal_sync(txn->tree->wal) != 0) {
        LOG_ERROR("Transaction aborted: WAL sync failed");
        data_art_abort_write(txn);
        return ERROR_WAL_SYNC_FAILED;
    }
    
    // *** DURABILITY POINT ***
    // After this line, transaction is durable even if crash occurs
    
    // Update in-memory state
    txn->tree->current_root = txn->new_root;
    txn->tree->current_version = txn->new_version;
    
    // Update metadata
    update_metadata(&txn->tree->metadata, txn->new_root);
    
    // Release locks
    pthread_rwlock_unlock(&txn->tree->write_lock);
    
    LOG_DEBUG("Transaction committed: version %lu", txn->new_version);
    return SUCCESS;
}
```

**User Action**:
- Retry transaction
- Check disk health

**Database State**: Healthy (transaction aborted cleanly, can retry)

---

## Durability Protocol

### ACID Compliance

**Atomicity**: All operations in transaction commit or abort together (via WAL)

**Consistency**: Tree invariants maintained (via explicit transactions)

**Isolation**: Readers see consistent snapshots (via MVCC)

**Durability**: Committed transactions survive crashes (via WAL fsync)

### Commit Protocol

```c
int data_art_commit_write(data_art_txn_t *txn) {
    // ========== PHASE 1: Prepare ==========
    // Process each operation in transaction
    // Only create NEW nodes along modified paths (Copy-on-Write)
    // Allocate page IDs from free list (no disk writes)
    // Build new tree structure in buffer pool (memory only)
    // Most of tree SHARED with previous version (unchanged pages reused)
    
    for (size_t i = 0; i < txn->num_operations; i++) {
        operation_t *op = &txn->operations[i];
        
        // Traverse from root to modification point
        // Create new nodes along path (typically 5-10 nodes per operation)
        // NOT the entire tree - structural sharing keeps most pages unchanged
        
        switch (op->type) {
            case OP_INSERT:
                // CoW path: root → ... → parent → new leaf
                node_ref_t new_leaf = allocate_new_page();  // Just page ID
                page_t *leaf_page = buffer_pool_get_new(cache, new_leaf.page_id);
                // Build leaf in memory (buffer pool)
                break;
            case OP_UPDATE:
                // CoW path to existing leaf
                break;
            case OP_DELETE:
                // CoW path, possibly merge nodes
                break;
        }
    }
    
    // At this point:
    // - New root reference created (txn->new_root)
    // - Modified pages in buffer pool (memory, marked dirty)
    // ========== PHASE 4: Update In-Memory State ==========
    // Now safe to update tree root (points to new version)
    txn->tree->current_root = txn->new_root;
    txn->tree->current_version = txn->new_version;
    
    // Update metadata (double-buffered, crash-safe)
    txn->tree->metadata.current_root = txn->new_root;
    txn->tree->metadata.current_version_id = txn->new_version;
    update_metadata(&txn->tree->metadata, txn->new_root);
    
    // New root now points to:
    // - NEW pages (modified path - created in Phase 1)
    // - OLD pages (unchanged subtrees - shared with version N)
    // This is structural sharing (CoW efficiency)
    
    // ========== PHASE 5: Mark Dirty Pages ==========
    // Only modified pages need flushing at checkpoint
    // Unchanged pages already on disk (shared from previous version)
    for (each modified page in txn) {
        mark_page_dirty(txn->tree->cache, page->page_id);
    }
    // Typical: 5-10 dirty pages per operation (tree depth)
    // NOT entire tree!           wal_write_update(txn->wal_txn, op->key, op->value);
                break;
            case OP_DELETE:
                wal_write_delete(txn->wal_txn, op->key);
                break;
        }
    }
    wal_write_commit(txn->wal_txn);
    
    // ========== PHASE 3: DURABILITY POINT ==========
    // Force WAL to disk
    if (wal_sync(txn->tree->wal) != 0) {
        data_art_abort_write(txn);
        return ERROR_WAL_SYNC_FAILED;
    }
    
    // *** CRASH BEFORE HERE: Transaction lost (not durable) ***
    // *** CRASH AFTER HERE: Transaction recoverable (durable) ***
    
    // ========== PHASE 4: Update In-Memory State ==========
    // Now safe to update tree root
    txn->tree->current_root = txn->new_root;
    txn->tree->current_version = txn->new_version;
    
    // Update metadata (double-buffered, crash-safe)
    txn->tree->metadata.current_root = txn->new_root;
    txn->tree->metadata.current_version_id = txn->new_version;
    update_metadata(&txn->tree->metadata, txn->new_root);
    
    // ========== PHASE 5: Mark Dirty Pages ==========
    // Pages will be flushed at next checkpoint
    for (each modified page in txn) {
        mark_page_dirty(txn->tree->cache, page->page_id);
    }
    
    // ========== PHASE 6: Release Locks ==========
    pthread_rwlock_unlock(&txn->tree->write_lock);
    pthread_rwlock_unlock(&txn->tree->checkpoint_lock);
    
    LOG_INFO("Transaction committed: version %lu (%zu operations)",
             txn->new_version, txn->num_operations);
    
    return SUCCESS;
}
```

### Crash Recovery

```c
typedef struct {
    uint64_t start_lsn;
    uint64_t end_lsn;
    uint64_t transactions_replayed;
    uint64_t operations_replayed;
    uint64_t duration_ms;
} recovery_stats_t;

recovery_stats_t recover_database(data_art_tree_t *tree) {
    recovery_stats_t stats = {0};
    uint64_t start_time = get_time_ms();
    
    LOG_INFO("=== Starting crash recovery ===");
    
    // ========== PHASE 1: Load Metadata ==========
    database_header_t *meta = load_metadata(tree->db_path);
    if (!meta) {
        LOG_CRITICAL("Cannot load metadata - database unrecoverable");
        LOG_CRITICAL("Possible causes:");
        LOG_CRITICAL("  - Both metadata files corrupted");
        LOG_CRITICAL("  - Disk failure");
        LOG_CRITICAL("  - File permissions issue");
        exit(1);  // Cannot continue
    }
    
    LOG_INFO("Loaded metadata:");
    LOG_INFO("  Version: %lu", meta->current_version_id);
    LOG_INFO("  Root: page=%lu, offset=%u", 
             meta->current_root.page_id, 
             meta->current_root.offset);
    LOG_INFO("  Checkpoint LSN: %lu", meta->checkpoint_lsn);
    
    // ========== PHASE 2: Restore Tree State ==========
    tree->current_root = meta->current_root;
    tree->current_version = meta->current_version_id;
    stats.start_lsn = meta->checkpoint_lsn;
    
    // ========== PHASE 3: Determine WAL Replay Range ==========
    stats.end_lsn = wal_get_last_lsn(tree->wal);
    
    if (stats.end_lsn <= stats.start_lsn) {
        LOG_INFO("No WAL entries to replay - clean shutdown detected");
        stats.duration_ms = get_time_ms() - start_time;
        LOG_INFO("=== Recovery complete: %lu ms ===", stats.duration_ms);
        return stats;
    }
    
    uint64_t entries_to_replay = stats.end_lsn - stats.start_lsn;
    LOG_INFO("Replaying WAL: LSN %lu to %lu (%lu entries)",
             stats.start_lsn, stats.end_lsn, entries_to_replay);
    
    // ========== PHASE 4: Replay WAL ==========
    int result = wal_replay(tree->wal, stats.start_lsn, stats.end_lsn, 
                           tree, &stats);
    
    if (result != 0) {
        LOG_CRITICAL("WAL replay failed - database may be inconsistent");
        mark_database_unhealthy(tree);
        // Continue in read-only mode
        tree->read_only = true;
    }
    
    // ========== PHASE 5: Verify Integrity ==========
    LOG_INFO("Verifying tree integrity...");
    if (!verify_tree_integrity(tree)) {
        LOG_CRITICAL("Tree integrity check failed after recovery");
        mark_database_corrupted(tree);
        tree->read_only = true;
    }
    
    stats.duration_ms = get_time_ms() - start_time;
    
    LOG_INFO("=== Recovery complete ===");
    LOG_INFO("  Transactions replayed: %lu", stats.transactions_replayed);
    LOG_INFO("  Operations replayed: %lu", stats.operations_replayed);
    LOG_INFO("  Final version: %lu", tree->current_version);
    LOG_INFO("  Duration: %lu ms", stats.duration_ms);
    
    return stats;
}

int wal_replay(wal_t *wal, uint64_t start_lsn, uint64_t end_lsn,
               data_art_tree_t *tree, recovery_stats_t *stats) {
    
    for (uint64_t lsn = start_lsn; lsn <= end_lsn; lsn++) {
        wal_entry_t *entry = wal_read_entry(wal, lsn);
        if (!entry) {
            LOG_ERROR("Failed to read WAL entry %lu", lsn);
            return -1;
        }
        
        switch (entry->type) {
            case WAL_INSERT:
                // Re-apply insert: creates new path (CoW)
                // Only ~5-10 pages created per insert (tree depth)
                data_art_insert(tree, entry->key, entry->key_len,
                               entry->value, entry->value_len);
                stats->operations_replayed++;
                break;
                
            case WAL_UPDATE:
                // Re-apply update: CoW path to existing leaf
                data_art_update(tree, entry->key, entry->key_len,
                               entry->value, entry->value_len);
                stats->operations_replayed++;
                break;
                
            case WAL_DELETE:
                // Re-apply delete: CoW path, possibly merge nodes
                data_art_delete(tree, entry->key, entry->key_len);
                stats->operations_replayed++;
                break;
                
            case WAL_COMMIT:
                // Transaction committed - increment version
                stats->transactions_replayed++;
                break;
                
            case WAL_ABORT:
                // Transaction aborted - discard pending operations
                break;
        }
        
        free(entry);
        
        // Progress logging every 10000 entries
        if ((lsn - start_lsn) % 10000 == 0) {
            LOG_INFO("  Replayed %lu / %lu entries...", 
                     lsn - start_lsn, end_lsn - start_lsn);
        }
    }
    
    // After replay: tree rebuilt with structural sharing
    // - Each replayed operation created modified path only
    // - Unchanged subtrees shared between versions
    // - Final tree consistent with all committed transactions
    
    return 0;
}
```

---

## Error Code Design

```c
typedef enum {
    // Success
    SUCCESS = 0,
    
    // Recoverable errors (user can retry)
    ERROR_DISK_FULL = -1,
    ERROR_WOULD_BLOCK = -2,
    ERROR_RETRY = -3,
    ERROR_NOT_FOUND = -4,
    ERROR_EXISTS = -5,
    
    // Fatal errors (abort transaction)
    ERROR_IO_FAILURE = -10,
    ERROR_WAL_SYNC_FAILED = -11,
    ERROR_CORRUPTION = -12,
    ERROR_INVALID_ARGUMENT = -13,
    ERROR_OUT_OF_MEMORY = -14,
    
    // Database health
    ERROR_DB_UNHEALTHY = -20,
    ERROR_DB_CORRUPTED = -21,
    ERROR_DB_READ_ONLY = -22,
    ERROR_DB_CLOSED = -23,
} db_error_t;

const char *db_error_string(db_error_t error) {
    switch (error) {
        case SUCCESS: return "Success";
        case ERROR_DISK_FULL: return "Disk full";
        case ERROR_IO_FAILURE: return "I/O failure";
        case ERROR_WAL_SYNC_FAILED: return "WAL sync failed";
        case ERROR_CORRUPTION: return "Data corruption detected";
        case ERROR_DB_CORRUPTED: return "Database corrupted";
        // ... etc
        default: return "Unknown error";
    }
}
```

---

## Health Monitoring

```c
typedef enum {
    DB_HEALTHY,      // All systems operational
    DB_UNHEALTHY,    // I/O errors detected, may recover
    DB_CORRUPTED,    // Corruption detected, need restore
    DB_READ_ONLY,    // Operating in degraded mode
    DB_CLOSED,       // Database closed
} db_health_status_t;

typedef struct {
    db_health_status_t status;
    
    // Error counters
    uint64_t io_errors;
    uint64_t checksum_failures;
    uint64_t wal_sync_failures;
    uint64_t allocation_failures;
    
    // Timing
    uint64_t last_error_time;
    uint64_t last_healthy_time;
    
    // Details
    char last_error_msg[256];
    int last_errno;
} db_health_t;

void mark_database_unhealthy(data_art_tree_t *tree) {
    tree->health.status = DB_UNHEALTHY;
    tree->health.last_error_time = time(NULL);
    LOG_WARN("Database marked UNHEALTHY");
}

void mark_database_corrupted(data_art_tree_t *tree) {
    tree->health.status = DB_CORRUPTED;
    tree->health.last_error_time = time(NULL);
    tree->read_only = true;
    LOG_CRITICAL("Database marked CORRUPTED - entering read-only mode");
}

bool is_database_healthy(data_art_tree_t *tree) {
    return tree->health.status == DB_HEALTHY;
}
```

---

## Best Practices

### DO ✅

1. **Always fsync() WAL before commit**
   ```c
   wal_write_commit(txn);
   if (wal_sync(wal) != 0) {
       abort_transaction();
   }
   ```

2. **Check return values and errno**
   ```c
   if (write(fd, data, size) == -1) {
       if (errno == ENOSPC) {
           // Handle disk full
       } else {
           // Handle other errors
       }
   }
   ```

3. **Verify checksums on page load**
   ```c
   if (!verify_checksum(page)) {
       mark_database_corrupted();
       return NULL;
   }
   ```

4. **Retry transient errors**
   ```c
   for (int retry = 0; retry < 3; retry++) {
       if (fsync(fd) == 0) break;
       if (errno == EINTR || errno == EAGAIN) continue;
       break;  // Unrecoverable
   }
   ```

5. **Log all errors with context**
   ```c
   LOG_ERROR("Failed to write page %lu: %s", page_id, strerror(errno));
   ```

### DON'T ❌

1. **Never skip fsync() for performance**
   ```c
   // BAD - defeats durability guarantee
   if (config->fast_mode) {
       // skip fsync
   }
   ```

2. **Never continue with corrupted data**
   ```c
   // BAD - propagates corruption
   if (!verify_checksum(page)) {
       LOG_WARN("Checksum failed, continuing anyway");  // NO!
   }
   ```

3. **Never ignore error codes**
   ```c
   // BAD - silent failure
   write(fd, data, size);  // Didn't check return value
   ```

4. **Never retry indefinitely**
   ```c
   // BAD - infinite loop if hardware failure
   while (fsync(fd) != 0) {
       sleep(1);  // Keep trying forever
   }
   ```

5. **Never return generic errors**
   ```c
   // BAD - caller can't distinguish errors
   return -1;  // What failed? Disk full? Corruption?
   
   // GOOD
   return ERROR_DISK_FULL;
   ```

---

## Testing Strategy

### Error Injection

```c
#ifdef DEBUG_ERROR_INJECTION
typedef struct {
    bool inject_disk_full;
    bool inject_io_error;
    bool inject_corruption;
    float io_error_rate;  // 0.0-1.0
} error_injection_t;

int write_with_injection(int fd, const void *buf, size_t size, 
                         error_injection_t *inject) {
    if (inject->inject_disk_full) {
        errno = ENOSPC;
        return -1;
    }
    
    if (inject->inject_io_error || 
        (rand() / (float)RAND_MAX) < inject->io_error_rate) {
        errno = EIO;
        return -1;
    }
    
    return write(fd, buf, size);
}
#endif
```

### Crash Simulation

```bash
#!/bin/bash
# Crash test script

# Start database
./test_art &
PID=$!

# Let it run for 5 seconds
sleep 5

# Kill brutally (simulates power loss)
kill -9 $PID

# Restart and verify recovery
./test_art --recover
```

### Corruption Detection

```c
void test_corruption_detection() {
    // Write page
    page_t *page = create_test_page();
    page_manager_write(pm, 1, page);
    
    // Corrupt page on disk
    corrupt_page_on_disk(1);  // Modify bytes randomly
    
    // Try to load - should detect corruption
    page_t *loaded = page_manager_load_safe(pm, 1);
    assert(loaded == NULL);  // Should return NULL
    assert(pm->tree->health.status == DB_CORRUPTED);
}
```

---

## Summary

**Chosen Strategy**: Defense in depth with fail-safe defaults

**Key Decisions**:
- ✅ WAL fsync() before commit (durability guarantee)
- ✅ Retry transient errors with limit (graceful degradation)
- ✅ Fail-stop on corruption (don't propagate bad data)
- ✅ Detailed error codes (clear reporting)
- ✅ Health monitoring (track database state)

**For Ethereum**: This ensures state integrity even with power loss, disk failures, or software crashes. Recovery is always possible from WAL + last checkpoint.
