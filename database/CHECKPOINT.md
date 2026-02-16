# Checkpoint Mechanism

## Overview

Checkpointing is the process of periodically flushing dirty pages from the buffer pool to disk and truncating the write-ahead log (WAL). This ensures bounded recovery time after crashes and prevents unbounded resource usage.

## The Problem

As transactions execute:
1. **Changes are logged to WAL** (fast, append-only)
2. **Pages are modified in buffer pool** (in-memory, dirty)
3. **Disk pages remain stale** (not immediately synced)

Without checkpointing:
- ⚠️ **WAL grows unbounded** → disk space exhaustion
- ⚠️ **Dirty pages accumulate** → memory exhaustion
- ⚠️ **Recovery time grows** → minutes/hours to replay entire WAL

**Checkpointing solves this** by periodically creating a consistent snapshot on disk.

## Checkpoint Strategy: Hybrid (Multi-Trigger)

**DECISION: Hybrid approach with WAL size as primary trigger** ✅

Checkpoint when **ANY** of these conditions is met:

```c
typedef struct {
    // Primary trigger
    uint64_t wal_threshold;        // Default: 256 MB
    
    // Backup triggers
    uint64_t time_threshold;       // Default: 600 seconds (10 minutes)
    uint64_t dirty_pages_threshold; // Default: 50000 pages (~200 MB)
    
    // Performance
    uint32_t checkpoint_timeout_ms; // Default: 30000 (30 seconds max)
    bool incremental;              // Default: false (full checkpoint)
} checkpoint_config_t;

// Trigger checkpoint when any condition met
bool should_checkpoint(checkpoint_state_t *state) {
    return (wal_size(state->wal) > state->config.wal_threshold) ||
           (time_since_checkpoint(state) > state->config.time_threshold) ||
           (dirty_pages_count(state->buffer_pool) > state->config.dirty_pages_threshold);
}
```

### Why This Strategy?

#### 1. WAL Size (256 MB) - PRIMARY TRIGGER ✅

**Most important for recovery time and disk space:**

- **Recovery time**: 256 MB WAL replays in ~2-5 seconds (acceptable)
- **Disk space**: Bounded WAL prevents disk exhaustion
- **Workload-responsive**: Checkpoints more frequently during heavy writes

**Example**: Ethereum block processing
```
Block 1: 200 transactions → 50 MB WAL
Block 2: 180 transactions → 45 MB WAL
Block 3: 220 transactions → 55 MB WAL
Block 4: 190 transactions → 48 MB WAL
Block 5: 210 transactions → 52 MB WAL
Total: 250 MB → Checkpoint triggered
```

**Why 256 MB?**
- Small enough: Recovery takes 2-5 seconds (acceptable downtime)
- Large enough: Not too frequent (checkpoint overhead ~1-2 seconds)
- Ethereum-specific: ~5-10 blocks worth of state changes

#### 2. Time-Based (10 minutes) - BACKUP TRIGGER ✅

**Prevents indefinite stalls during light load:**

- If writes are sparse, WAL grows slowly
- Still need periodic checkpoints to:
  - Free dirty pages from buffer pool
  - Keep recovery time bounded
  - Update disk state regularly

**Example**: Synced node with light load
```
0:00 - Last checkpoint
0:05 - Only 10 MB WAL growth (light writes)
0:10 - Time trigger fires → Checkpoint anyway
```

**Why 10 minutes?**
- Not too aggressive: Allows batching of writes
- Not too lazy: Recovery still bounded (10 min of WAL = fast replay)
- Ethereum-specific: ~50-100 blocks on mainnet

#### 3. Dirty Pages (50k pages = ~200 MB) - SAFETY TRIGGER ✅

**Prevents memory pressure:**

- Many small updates to different pages → high memory usage
- Buffer pool could run out of space
- Forces checkpoint before OOM

**Example**: State expansion with many accounts
```
1M small updates to different accounts
Each update touches new page
50k pages × 4 KB = 200 MB dirty memory
Checkpoint triggered to free memory
```

**Why 50k pages?**
- 50k × 4 KB = 200 MB dirty data (manageable memory usage)
- Leaves headroom in buffer pool (typically 1-2 GB total)
- Prevents OOM scenarios

## Checkpoint Process

### Full Checkpoint (Default)

```c
typedef struct {
    uint64_t checkpoint_lsn;       // LSN at checkpoint start
    uint64_t pages_flushed;        // Statistics
    uint64_t bytes_written;
    uint64_t duration_ms;
} checkpoint_result_t;

checkpoint_result_t checkpoint_full(data_art_tree_t *tree) {
    checkpoint_result_t result = {0};
    uint64_t start_time = get_time_ms();
    
    // 1. Block write transactions (readers continue via MVCC snapshots)
    pthread_rwlock_wrlock(&tree->checkpoint_lock);
    LOG_INFO("Checkpoint started");
    
    // 2. Record current WAL position
    result.checkpoint_lsn = wal_current_lsn(tree->wal);
    
    // 3. Flush all dirty pages in buffer pool to disk
    dirty_page_list_t *dirty_pages = buffer_pool_get_dirty_pages(tree->cache);
    
    for (size_t i = 0; i < dirty_pages->count; i++) {
        cached_page_t *page = dirty_pages->pages[i];
        
        // Write page to disk with current compression tier
        page_manager_write(tree->pm, page->page_id, page->data, 
                          page->compression_type);
        
        // Mark page as clean
        page->dirty = false;
        
        result.pages_flushed++;
        result.bytes_written += 4096; // Uncompressed size
        
        // Timeout safety
        if (get_time_ms() - start_time > tree->checkpoint_config.checkpoint_timeout_ms) {
            LOG_WARN("Checkpoint timeout reached, aborting");
            pthread_rwlock_unlock(&tree->checkpoint_lock);
            return result; // Partial checkpoint
        }
    }
    
    // 4. Sync all data files to disk
    page_manager_sync_all(tree->pm);
    
    // 5. Update metadata with new checkpoint LSN
    tree->metadata.checkpoint_lsn = result.checkpoint_lsn;
    tree->metadata.last_checkpoint_time = time(NULL);
    update_metadata(&tree->metadata);
    
    // 6. Truncate WAL (delete entries before checkpoint_lsn)
    wal_truncate(tree->wal, result.checkpoint_lsn);
    LOG_INFO("WAL truncated to LSN %lu", result.checkpoint_lsn);
    
    // 7. Release lock, resume write transactions
    pthread_rwlock_unlock(&tree->checkpoint_lock);
    
    result.duration_ms = get_time_ms() - start_time;
    LOG_INFO("Checkpoint completed: %lu pages, %lu bytes, %lu ms",
             result.pages_flushed, result.bytes_written, result.duration_ms);
    
    return result;
}
```

### Recovery Using Checkpoint

On database startup after crash:

```c
void recover_database(data_art_tree_t *tree) {
    // 1. Load metadata from disk (double-buffered, crash-safe)
    database_header_t *meta = load_metadata(tree->db_path);
    
    LOG_INFO("Recovering from checkpoint LSN %lu", meta->checkpoint_lsn);
    
    // 2. Load root pointer and tree structure
    tree->root = meta->current_root;
    tree->version = meta->current_version_id;
    
    // 3. Replay WAL from checkpoint_lsn to end
    uint64_t replay_start = meta->checkpoint_lsn;
    uint64_t replay_end = wal_current_lsn(tree->wal);
    
    LOG_INFO("Replaying WAL: %lu entries", replay_end - replay_start);
    
    wal_replay(tree->wal, replay_start, replay_end, tree);
    
    LOG_INFO("Recovery complete");
}

void wal_replay(wal_t *wal, uint64_t start_lsn, uint64_t end_lsn, 
                data_art_tree_t *tree) {
    for (uint64_t lsn = start_lsn; lsn <= end_lsn; lsn++) {
        wal_entry_t *entry = wal_read_entry(wal, lsn);
        
        switch (entry->type) {
            case WAL_INSERT:
                data_art_insert(tree, entry->key, entry->value);
                break;
            case WAL_UPDATE:
                data_art_update(tree, entry->key, entry->value);
                break;
            case WAL_DELETE:
                data_art_delete(tree, entry->key);
                break;
            case WAL_COMMIT:
                // Transaction boundary marker
                break;
        }
        
        free(entry);
    }
}
```

## Background Checkpoint Thread

Run checkpointing in background thread to avoid blocking main application:

```c
typedef struct {
    data_art_tree_t *tree;
    pthread_t thread;
    bool running;
    uint64_t last_checkpoint_time;
    uint64_t checkpoint_count;
} checkpoint_manager_t;

void *checkpoint_thread(void *arg) {
    checkpoint_manager_t *mgr = (checkpoint_manager_t *)arg;
    
    while (mgr->running) {
        // Check if checkpoint needed
        if (should_checkpoint(&mgr->tree->checkpoint_state)) {
            checkpoint_result_t result = checkpoint_full(mgr->tree);
            
            mgr->last_checkpoint_time = time(NULL);
            mgr->checkpoint_count++;
            
            LOG_INFO("Checkpoint #%lu completed", mgr->checkpoint_count);
        }
        
        // Sleep for 1 second before checking again
        sleep(1);
    }
    
    return NULL;
}

checkpoint_manager_t *checkpoint_manager_start(data_art_tree_t *tree) {
    checkpoint_manager_t *mgr = calloc(1, sizeof(checkpoint_manager_t));
    mgr->tree = tree;
    mgr->running = true;
    mgr->last_checkpoint_time = time(NULL);
    
    pthread_create(&mgr->thread, NULL, checkpoint_thread, mgr);
    
    return mgr;
}

void checkpoint_manager_stop(checkpoint_manager_t *mgr) {
    mgr->running = false;
    pthread_join(mgr->thread, NULL);
    free(mgr);
}
```

## Incremental Checkpoint (Future Optimization)

For Phase 2, consider incremental checkpoints to reduce pause time:

```c
checkpoint_result_t checkpoint_incremental(data_art_tree_t *tree, 
                                          uint32_t pages_to_flush) {
    checkpoint_result_t result = {0};
    
    // 1. Get oldest N dirty pages (by modification time)
    dirty_page_list_t *oldest = buffer_pool_get_oldest_dirty(tree->cache, 
                                                              pages_to_flush);
    
    // 2. Flush only these pages
    for (size_t i = 0; i < oldest->count; i++) {
        flush_page_to_disk(oldest->pages[i]);
        result.pages_flushed++;
    }
    
    // 3. Update checkpoint LSN to oldest remaining dirty page
    //    (Can only truncate WAL up to this point)
    cached_page_t *oldest_remaining = buffer_pool_get_oldest_dirty_page(tree->cache);
    result.checkpoint_lsn = oldest_remaining->first_modified_lsn;
    
    // 4. Truncate WAL to checkpoint_lsn
    wal_truncate(tree->wal, result.checkpoint_lsn);
    
    return result;
}
```

**Pros**: Shorter pauses (flush 5k pages instead of 50k)
**Cons**: More complex, WAL truncation limited, more checkpoints needed

## Performance Characteristics

### Checkpoint Duration

| Scenario | Pages | Duration | Notes |
|----------|-------|----------|-------|
| Light load | 1,000 | ~100ms | Quick flush |
| Moderate load | 10,000 | ~1s | Acceptable |
| Heavy load | 50,000 | ~5s | Still reasonable |
| Extreme load | 100,000 | ~10s | May hit timeout |

### Recovery Time

| WAL Size | Entries | Replay Time | Notes |
|----------|---------|-------------|-------|
| 64 MB | ~1M | ~1s | Fast |
| 256 MB | ~4M | ~2-5s | Target |
| 1 GB | ~16M | ~10-20s | Too long |
| 4 GB | ~64M | ~1-2 min | Unacceptable |

**Target**: Keep WAL under 256 MB for sub-5-second recovery.

## Configuration Tuning

### For Ethereum Archive Node (Heavy Writes)
```c
checkpoint_config_t archive_config = {
    .wal_threshold = 128 * 1024 * 1024,  // 128 MB (checkpoint more often)
    .time_threshold = 300,                // 5 minutes
    .dirty_pages_threshold = 30000,       // 30k pages
    .checkpoint_timeout_ms = 60000,       // 60 seconds
};
```

### For Ethereum RPC Node (Light Writes)
```c
checkpoint_config_t rpc_config = {
    .wal_threshold = 512 * 1024 * 1024,  // 512 MB (checkpoint less often)
    .time_threshold = 1800,               // 30 minutes
    .dirty_pages_threshold = 100000,      // 100k pages
    .checkpoint_timeout_ms = 30000,       // 30 seconds
};
```

## Implementation Phases

### Phase 1: Basic Full Checkpoint
- [ ] Implement checkpoint trigger logic (3 conditions)
- [ ] Implement full checkpoint (flush all dirty pages)
- [ ] Integrate with WAL truncation
- [ ] Update metadata with checkpoint LSN
- [ ] Test recovery from checkpoint

### Phase 2: Background Thread
- [ ] Implement checkpoint manager thread
- [ ] Add checkpoint statistics and monitoring
- [ ] Add configurable thresholds
- [ ] Test concurrent checkpoints and transactions

### Phase 3: Incremental Checkpoint (Optional)
- [ ] Implement oldest-first page flushing
- [ ] Track per-page modification LSN
- [ ] Partial WAL truncation
- [ ] Performance benchmarks vs full checkpoint

### Phase 4: Optimization
- [ ] Parallel page flushing
- [ ] Adaptive threshold tuning based on workload
- [ ] Checkpoint compression statistics
- [ ] Integration with monitoring/metrics

## Monitoring & Debugging

```c
typedef struct {
    uint64_t total_checkpoints;
    uint64_t total_pages_flushed;
    uint64_t total_bytes_written;
    uint64_t total_duration_ms;
    
    uint64_t wal_triggered_count;
    uint64_t time_triggered_count;
    uint64_t memory_triggered_count;
    
    uint64_t avg_checkpoint_duration_ms;
    uint64_t max_checkpoint_duration_ms;
    
    uint64_t last_checkpoint_time;
    uint64_t last_checkpoint_lsn;
} checkpoint_stats_t;

void log_checkpoint_stats(checkpoint_stats_t *stats) {
    LOG_INFO("Checkpoint Statistics:");
    LOG_INFO("  Total checkpoints: %lu", stats->total_checkpoints);
    LOG_INFO("  Pages flushed: %lu (%.2f GB)", 
             stats->total_pages_flushed,
             stats->total_bytes_written / (1024.0 * 1024.0 * 1024.0));
    LOG_INFO("  Triggers: WAL=%lu, Time=%lu, Memory=%lu",
             stats->wal_triggered_count,
             stats->time_triggered_count,
             stats->memory_triggered_count);
    LOG_INFO("  Duration: avg=%lu ms, max=%lu ms",
             stats->avg_checkpoint_duration_ms,
             stats->max_checkpoint_duration_ms);
}
```

## Summary

**Chosen Strategy**: Hybrid checkpoint with WAL size (256 MB) as primary trigger, backed by time (10 min) and memory (50k pages) safety limits.

**Benefits**:
- ✅ Bounded recovery time (~2-5 seconds)
- ✅ Bounded memory usage (dirty pages capped)
- ✅ Responsive to workload (more checkpoints during heavy writes)
- ✅ Simple to implement and understand
- ✅ Battle-tested approach (PostgreSQL, SQLite use similar strategies)

**For Ethereum**: This strategy handles bursty block processing well while keeping recovery fast and memory usage predictable.
