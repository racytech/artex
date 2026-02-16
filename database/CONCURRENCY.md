# Concurrency & Lock Granularity

## Overview

The persistent ART database uses **coarse-grained locking with MVCC (Multi-Version Concurrency Control)** to provide concurrent read access while maintaining correctness. This document describes the locking strategy, concurrency guarantees, and rationale.

## Design Philosophy

**Key Insight**: For a persistent database, **disk I/O is the bottleneck, not locking**. Complex fine-grained locking adds overhead without improving throughput when operations spend milliseconds waiting for disk.

**Strategy**: Use simple coarse-grained locks + MVCC snapshots to achieve:
- ✅ **Correctness** - No data races, no corruption
- ✅ **Simplicity** - Easy to reason about, no deadlocks
- ✅ **Concurrency** - Unlimited readers, one writer
- ✅ **Ethereum-optimized** - Matches serial block processing model

## Lock Structure

```c
typedef struct {
    // Global write lock - only ONE writer at a time
    pthread_rwlock_t write_lock;
    
    // Buffer pool lock - protects page cache
    pthread_rwlock_t cache_lock;
    
    // Version list lock - protects version metadata
    pthread_rwlock_t version_lock;
    
    // Checkpoint lock - for checkpoint operations
    pthread_rwlock_t checkpoint_lock;
} data_art_tree_t;
```

### Lock Hierarchy (Always Acquire in This Order)

```
1. write_lock       (transaction-level)
2. cache_lock       (buffer pool access)
3. version_lock     (snapshot management)
4. checkpoint_lock  (checkpoint coordination)
```

**Rule**: Always acquire locks in this order to prevent deadlocks.

## Lock Purposes

### 1. write_lock - Transaction Serialization

**Purpose**: Ensures only one writer modifies the tree at a time.

**Acquired by**:
- **Writers (WRITE mode)**: Hold for entire transaction
- **Readers (READ mode)**: Hold briefly to create snapshot

```c
// Writer transaction
data_art_txn_t *data_art_begin_write(data_art_tree_t *tree) {
    pthread_rwlock_wrlock(&tree->write_lock);  // Exclusive access
    
    // Create new version, get CoW root
    txn->new_version = tree->current_version + 1;
    txn->new_root = tree->current_root;
    
    return txn;
}

void data_art_commit_write(data_art_txn_t *txn) {
    // Update tree root to new version
    txn->tree->current_root = txn->new_root;
    txn->tree->current_version = txn->new_version;
    
    // Log to WAL
    wal_commit(txn->wal_txn);
    
    pthread_rwlock_unlock(&txn->tree->write_lock);
}

// Reader snapshot
data_art_snapshot_t *data_art_begin_read(data_art_tree_t *tree) {
    pthread_rwlock_rdlock(&tree->write_lock);  // Shared access
    
    // Create snapshot at current version
    snapshot->version_id = tree->current_version;
    snapshot->root = tree->current_root;
    
    // Increment ref count
    increment_version_refcount(tree, snapshot->version_id);
    
    pthread_rwlock_unlock(&tree->write_lock);
    return snapshot;
}
```

**Why single writer?**
- **Ethereum requirement**: Blocks processed serially (deterministic state root)
- **Simplicity**: No conflict resolution, no merge logic
- **Sufficient**: Disk I/O is bottleneck, not CPU

**Duration held**:
- Writer: Entire transaction (~milliseconds)
- Reader: Brief (~microseconds, just to capture version)

### 2. cache_lock - Buffer Pool Access

**Purpose**: Protects the in-memory page cache (implemented as ART).

**Acquired by**:
- **Page lookup (READ mode)**: Check if page in cache
- **Page insert (WRITE mode)**: Add page to cache
- **Page eviction (WRITE mode)**: Remove page from cache

```c
// Page lookup (read)
cached_page_t *buffer_pool_lookup(buffer_pool_t *pool, uint64_t page_id) {
    pthread_rwlock_rdlock(&pool->cache_lock);
    
    // Search in-memory ART
    cached_page_t *page = art_get(pool->cache_art, &page_id, sizeof(page_id));
    
    if (page) {
        page->access_count++;
        page->last_access = time(NULL);
    }
    
    pthread_rwlock_unlock(&pool->cache_lock);
    return page;
}

// Page insertion (write)
void buffer_pool_insert(buffer_pool_t *pool, cached_page_t *page) {
    pthread_rwlock_wrlock(&pool->cache_lock);
    
    // Check if cache is full
    if (pool->num_pages >= pool->max_pages) {
        evict_lru_page(pool);  // Make room
    }
    
    // Insert into cache ART
    art_insert(pool->cache_art, &page->page_id, sizeof(page->page_id), page);
    pool->num_pages++;
    
    pthread_rwlock_unlock(&pool->cache_lock);
}
```

**Why separate from write_lock?**
- **Concurrency**: Readers can use buffer pool while writer is working on tree
- **Independent resource**: Cache is orthogonal to tree structure
- **Short critical section**: Lookup takes microseconds

**Duration held**: Microseconds (just for ART lookup/insert)

### 3. version_lock - Version Metadata

**Purpose**: Protects the list of active versions and their metadata.

**Acquired by**:
- **Snapshot creation**: Increment version refcount
- **Snapshot destruction**: Decrement version refcount
- **GC**: Scan versions, free eligible ones

```c
// Snapshot creation
void increment_version_refcount(data_art_tree_t *tree, uint64_t version_id) {
    pthread_rwlock_wrlock(&tree->version_lock);
    
    version_metadata_t *version = find_version(tree->versions, version_id);
    version->ref_count++;
    
    pthread_rwlock_unlock(&tree->version_lock);
}

// Snapshot destruction
void decrement_version_refcount(data_art_tree_t *tree, uint64_t version_id) {
    pthread_rwlock_wrlock(&tree->version_lock);
    
    version_metadata_t *version = find_version(tree->versions, version_id);
    version->ref_count--;
    
    pthread_rwlock_unlock(&tree->version_lock);
}

// GC
void gc_old_versions(data_art_tree_t *tree) {
    pthread_rwlock_wrlock(&tree->version_lock);
    
    for (each version in tree->versions) {
        if (is_eligible_for_gc(version)) {
            free_version_pages(tree->pm, version->version_id);
            remove_version(tree->versions, version);
        }
    }
    
    pthread_rwlock_unlock(&tree->version_lock);
}
```

**Why separate from write_lock?**
- **Independence**: Version metadata operations don't affect tree structure
- **Frequent access**: Many snapshots created/destroyed, don't block writes
- **GC isolation**: GC can run without blocking transactions

**Duration held**: Microseconds (increment/decrement), milliseconds (GC scan)

### 4. checkpoint_lock - Checkpoint Coordination

**Purpose**: Coordinates checkpoint with ongoing transactions.

**Acquired by**:
- **Checkpoint (WRITE mode)**: Blocks new write transactions
- **Write transactions (READ mode)**: Allowed during normal operation

```c
// Checkpoint
void checkpoint_full(data_art_tree_t *tree) {
    pthread_rwlock_wrlock(&tree->checkpoint_lock);
    
    // Now no new write transactions can start
    // Existing readers continue with their snapshots
    
    // Flush all dirty pages
    buffer_pool_flush_all(tree->cache);
    
    // Update metadata
    tree->metadata.checkpoint_lsn = wal_current_lsn(tree->wal);
    update_metadata(&tree->metadata);
    
    // Truncate WAL
    wal_truncate(tree->wal, tree->metadata.checkpoint_lsn);
    
    pthread_rwlock_unlock(&tree->checkpoint_lock);
}

// Write transaction
data_art_txn_t *data_art_begin_write(data_art_tree_t *tree) {
    pthread_rwlock_rdlock(&tree->checkpoint_lock);  // Check if checkpointing
    pthread_rwlock_wrlock(&tree->write_lock);
    
    // ... create transaction ...
    
    return txn;
}

void data_art_commit_write(data_art_txn_t *txn) {
    // ... commit transaction ...
    
    pthread_rwlock_unlock(&txn->tree->write_lock);
    pthread_rwlock_unlock(&txn->tree->checkpoint_lock);
}
```

**Why separate from write_lock?**
- **Different semantics**: Checkpoint is system operation, not user transaction
- **Independent timing**: Checkpoint triggered by background thread
- **Clear separation**: Transaction logic vs checkpoint logic

**Duration held**: 
- Write transaction: Entire transaction (~milliseconds)
- Checkpoint: Entire checkpoint (~seconds)

## Concurrency Scenarios

### Scenario 1: Multiple Readers, No Writer

```
Timeline:
T0: Reader 1 calls data_art_begin_read()
    - Acquires read lock on write_lock (shared)
    - Creates snapshot at version 100
    - Releases write_lock
    - Reads data from version 100

T1: Reader 2 calls data_art_begin_read()
    - Acquires read lock on write_lock (shared, doesn't block)
    - Creates snapshot at version 100
    - Releases write_lock
    - Reads data from version 100

T2: Reader 3 calls data_art_begin_read()
    - Acquires read lock on write_lock (shared, doesn't block)
    - Creates snapshot at version 100
    - Releases write_lock
    - Reads data from version 100

All readers:
- Access buffer pool concurrently (shared cache_lock)
- No blocking, linear scalability
```

**Result**: Perfect scalability for read-heavy workloads.

### Scenario 2: Multiple Readers + One Writer

```
Timeline:
T0: Reader 1 has snapshot at version 100 (reading)
T1: Reader 2 has snapshot at version 100 (reading)

T2: Writer calls data_art_begin_write()
    - Tries to acquire write lock (exclusive)
    - Must wait for brief moment (readers release immediately after snapshot)
    - Acquires write lock
    - Creates version 101

T3: Writer modifies tree
    - Inserts/updates/deletes use CoW (copy-on-write)
    - Creates NEW pages for version 101
    - Doesn't modify version 100 pages

T4: Writer calls data_art_commit_write()
    - Updates current_version = 101
    - Logs to WAL
    - Releases write lock

T5: Reader 3 calls data_art_begin_read()
    - Acquires read lock
    - Creates snapshot at version 101 (NEW version)
    - Releases write_lock
    - Reads data from version 101

Meanwhile:
- Readers 1 & 2 still reading version 100 (their snapshots)
- No blocking, no conflicts
```

**Key**: MVCC allows readers and writer to work on different versions simultaneously.

### Scenario 3: Checkpoint During Transactions

```
Timeline:
T0: Reader 1 has snapshot at version 100
T1: Writer working on version 101 (holding write_lock)

T2: Checkpoint triggered
    - Tries to acquire checkpoint_lock (exclusive)
    - Blocks waiting for writer to finish

T3: Writer commits version 101
    - Releases write_lock
    - Releases checkpoint_lock
    - Checkpoint can now proceed

T4: Checkpoint acquires checkpoint_lock
    - Blocks NEW write transactions
    - Reader 1 continues (snapshot isolation)
    - Flushes dirty pages
    - Updates metadata
    - Releases checkpoint_lock

T5: Writer 2 can now start (checkpoint done)
```

**Key**: Checkpoint waits for active writer, then blocks new writers while flushing.

### Scenario 4: Long-Running Reader + GC

```
Timeline:
T0: Reader 1 starts, snapshot at version 100 (refcount = 1)

T1: Writer commits version 101

T2: Writer commits version 102

T3: Writer commits version 103

T4: GC runs
    - Checks version 100: ref_count = 1 (Reader 1 using it)
    - SKIP version 100 (protected by refcount)
    - Checks version 101: ref_count = 0, age < 1 hour
    - SKIP version 101 (time-based retention)

T5: Reader 1 finishes
    - Decrements version 100 refcount (now 0)

T6: GC runs again
    - Checks version 100: ref_count = 0, age > 1 hour, not in last 100
    - GC FREES version 100

T7: 100 more versions committed (now at version 203)

T8: GC runs
    - Checks version 101: ref_count = 0, age > 1 hour, version 203 - 101 > 100
    - GC FREES version 101
```

**Key**: Refcount + time + count hybrid protects versions appropriately.

## Deadlock Prevention

### Lock Ordering Rule

**ALWAYS acquire locks in this order**:
1. checkpoint_lock
2. write_lock
3. cache_lock
4. version_lock

**Example (transaction with checkpoint protection)**:
```c
// CORRECT
pthread_rwlock_rdlock(&tree->checkpoint_lock);  // 1
pthread_rwlock_wrlock(&tree->write_lock);       // 2
pthread_rwlock_rdlock(&tree->cache_lock);       // 3
// ... do work ...
pthread_rwlock_unlock(&tree->cache_lock);
pthread_rwlock_unlock(&tree->write_lock);
pthread_rwlock_unlock(&tree->checkpoint_lock);

// INCORRECT - DEADLOCK RISK
pthread_rwlock_rdlock(&tree->cache_lock);       // 3 first!
pthread_rwlock_wrlock(&tree->write_lock);       // 2 second - WRONG ORDER
```

### Why This Order Works

- **checkpoint_lock first**: Ensures transactions check for checkpoint before starting
- **write_lock second**: Serializes write transactions
- **cache_lock third**: Short-lived, released quickly
- **version_lock last**: Independent, rarely held with others

**Result**: No circular dependencies, no deadlocks.

## Performance Characteristics

### Lock Contention Analysis

| Lock | Hold Time | Contention | Impact |
|------|-----------|------------|--------|
| write_lock (writer) | ~1-10ms | Low (single writer) | Acceptable |
| write_lock (reader) | ~1-10µs | Very low | Negligible |
| cache_lock | ~1-5µs | Medium (many lookups) | Low |
| version_lock | ~1-10µs | Low (infrequent) | Negligible |
| checkpoint_lock | ~1-5s | Very low (periodic) | Acceptable |

### Bottleneck Analysis

For persistent database:
- **Disk I/O**: 1-10ms per page read/write
- **Lock acquire/release**: ~100ns
- **ART lookup**: ~1-5µs

**Conclusion**: Disk I/O dominates (10,000x slower than locking). Lock overhead is negligible.

### Scalability

**Read scalability**: Linear with CPU cores
- Readers don't block each other
- Buffer pool shared efficiently
- Only bottleneck is disk I/O (for cache misses)

**Write scalability**: Single writer (by design)
- Matches Ethereum's serial block processing
- Not a limitation for target use case

## When to Optimize?

Consider fine-grained locking (per-page locks, lock-free structures) ONLY if profiling shows:

1. **Cache lock contention** > 5% of CPU time
2. **Write lock blocking** readers for > 10ms regularly
3. **Multiple writers** required (not Ethereum use case)

**Current design is sufficient for Phase 1-5**. Defer optimization to Phase 6 if needed.

## Alternative Designs (Not Chosen)

### Fine-Grained Page Locks

```c
// Lock every page during traversal
void art_insert_with_page_locks(tree, key, value) {
    page_t *page = load_root_page();
    lock_page(page);
    
    while (not_leaf) {
        page_t *child = load_child_page(page);
        lock_page(child);
        unlock_page(page);  // Release parent
        page = child;
    }
    
    // ... insert ...
    unlock_page(page);
}
```

**Problems**:
- Complex lock ordering (avoid deadlocks)
- High overhead (lock/unlock per node)
- MVCC already provides concurrency
- No benefit when disk I/O is bottleneck

**Verdict**: Not worth the complexity.

### Lock-Free Structures

```c
// Use atomic operations, no locks
atomic_compare_exchange(&tree->root, old_root, new_root);
```

**Problems**:
- Extremely complex (especially for tree structures)
- ABA problem, memory reclamation issues
- Doesn't help with disk I/O (still need fsync)
- Not applicable to persistent storage

**Verdict**: Overkill for this use case.

### Multiple Writers

```c
// Allow concurrent writes to different subtrees
// Requires conflict detection and resolution
```

**Problems**:
- Complex conflict resolution
- Not needed for Ethereum (serial block processing)
- Harder to reason about correctness

**Verdict**: YAGNI (You Ain't Gonna Need It).

## Testing Strategy

### Correctness Tests

1. **Race condition tests**: Multiple readers + writer
2. **Deadlock tests**: All lock combinations
3. **Snapshot isolation**: Verify readers see consistent view
4. **Stress tests**: 1000+ concurrent readers

### Performance Tests

1. **Lock contention**: Measure time spent waiting for locks
2. **Throughput**: Reads/sec with 1, 10, 100 readers
3. **Latency**: P50, P99, P999 for operations
4. **Scalability**: Linear scaling with CPU cores?

### Tools

```bash
# Thread sanitizer (detects data races)
gcc -fsanitize=thread test_concurrency.c

# Helgrind (detects lock issues)
valgrind --tool=helgrind ./test_art

# Lock profiling
perf record -e lock:contention_begin ./test_art
```

## Summary

**Chosen Strategy**: Coarse-grained locking (4 rwlocks) + MVCC

**Rationale**:
- ✅ **Simple**: Easy to understand, no deadlocks
- ✅ **Correct**: No data races, MVCC guarantees isolation
- ✅ **Concurrent**: Unlimited readers, no blocking
- ✅ **Sufficient**: Disk I/O is bottleneck, not locks
- ✅ **Ethereum-optimized**: Single writer matches serial block processing

**When to revisit**: Only if profiling shows lock contention > 5% of CPU time (unlikely for persistent DB).
