# Implementation TODO - Critical Design Decisions

This document outlines open design questions and missing pieces that need to be resolved before implementing the persistent ART database. Items are prioritized by urgency.

## Status Legend
- 🔴 **CRITICAL** - Must resolve before Phase 1 implementation (✅ ALL COMPLETE)
- 🟡 **IMPORTANT** - Should design before Phase 2 (✅ ALL COMPLETE)
- 🟢 **NICE-TO-HAVE** - Can defer or decide during implementation (Sections 10-12)

## Progress Summary

**Phase 1 Ready** ✅ - All critical design decisions resolved:
- ✅ Section 1: Page Allocation (free list with size classes)
- ✅ Section 2: Root Pointer Persistence (double-buffered metadata)
- ✅ Section 3: Compression Strategy (variable-size storage with page index)
- ✅ Section 4: Checkpoint Mechanism (hybrid 256MB/10min/50k pages)

**Phase 2 Ready** ✅ - All important design decisions resolved:
- ✅ Section 5: Buffer Pool Eviction (LRU with dirty page priority)
- ✅ Section 6: Version Garbage Collection (hybrid refcount+time+count)
- ✅ Section 7: Concurrency Control (coarse-grained 4 rwlocks + MVCC)
- ✅ Section 8: Error Handling & Durability (defense in depth, WAL fsync)
- ✅ Section 9: Iterator Support (separate persistent iterator with pinning)

**Deferred to Later** 🟢 - Polish and integration (Sections 10-12):
- 🟢 Section 10: Configuration & Initialization API
- 🟢 Section 11: Monitoring & Statistics
- 🟢 Section 12: Multi-File Management & Ethereum Integration

**Status**: Ready to begin Phase 1 implementation! 🚀

---

## 🔴 1. Page Allocation Strategy [CRITICAL] ✅

### DECISION: Free List with Size Classes

**Reference**: `PAGE_ALLOCATION.md`

**Problem**: How do we efficiently find pages with free space and allocate new pages?

**Questions**:
- How to track which pages have free space?
- How to allocate space within a page for variable-size nodes?
- What happens when a page fills up and node needs to grow?

**Options**:

### Option A: Free Space Bitmap
```c
typedef struct {
    uint64_t *bitmap;      // 1 bit per page (has free space?)
    size_t num_pages;
    uint64_t next_page_id;
} page_allocator_t;
```
**Pros**: Fast scan for free pages
**Cons**: Doesn't track *how much* free space

### Option B: Free List per Size Class
```c
typedef struct {
    // Pages grouped by available free space
    uint64_t *pages_0_512_bytes;    // 0-512 bytes free
    uint64_t *pages_512_1024_bytes; // 512-1024 bytes free
    uint64_t *pages_1024_2048_bytes; // 1024-2048 bytes free
    uint64_t *pages_2048_plus;      // 2048+ bytes free
} free_space_tracker_t;
```
**Pros**: Fast allocation for specific sizes
**Cons**: More complex bookkeeping

### Option C: Append-Only with Compaction
```c
// Always allocate at end of file (like log-structured)
// Periodically compact to reclaim space
uint64_t alloc_page() {
    return next_page_id++;  // Simple!
}
```
**Pros**: Simplest allocation
**Cons**: Wastes space, needs background compaction

**Decision Needed**: Which strategy to use? Hybrid?

**Within-Page Allocation**:
```c
typedef struct {
    uint32_t free_offset;  // Next free byte in page
    uint32_t num_nodes;    // Number of nodes in page
    // Nodes packed from offset 64 (after header)
    // Free space at end
} page_layout_t;
```

**Node Growth Problem**:
- NODE_4 (48 bytes) → NODE_16 (144 bytes): May not fit in same page
- Solution: Copy to new page, update parent, mark old space as garbage

---

## 🔴 2. Root Pointer Persistence [CRITICAL] ✅

### DECISION: Double-Buffered Metadata Files

**Reference**: `ROOT_POINTER_PERSISTENCE.md`

**Problem**: Where is the tree root stored between restarts?

**Solution**: Metadata file with database header

### Metadata File Structure
```c
#define DB_MAGIC 0x4152544442  // "ARTDB"
#define DB_VERSION 1

typedef struct {
    uint64_t magic;              // DB_MAGIC (for validation)
    uint64_t version;            // Schema version
    
    // Current tree state
    node_ref_t current_root;     // Root of latest committed version
    uint64_t current_version_id; // Latest version number
    size_t tree_size;            // Total number of entries
    
    // WAL state
    uint64_t checkpoint_lsn;     // Last checkpointed WAL entry
    uint64_t wal_segment_id;     // Current WAL segment
    
    // File management
    uint64_t next_page_id;       // Next available page ID
    uint32_t num_data_files;     // Number of pages_XXXXX.dat files
    
    // Configuration
    compression_config_t compression;
    
    // Integrity
    uint64_t last_modified;      // Timestamp
    uint32_t checksum;           // CRC32 of this header
    
    uint8_t padding[384];        // Reserved for future use (total 512 bytes)
} database_header_t;
```

### Metadata Operations
```c
// On startup
database_header_t *load_metadata(const char *db_path) {
    // Read from {db_path}/metadata.dat
    // Verify magic and checksum
    // Return header
}

// On commit
void update_metadata(database_header_t *meta, node_ref_t new_root) {
    meta->current_root = new_root;
    meta->current_version_id++;
    meta->last_modified = time(NULL);
    meta->checksum = crc32(meta, offsetof(database_header_t, checksum));
    // Write to disk
    fsync(metadata_fd);
}
```

**Questions**:
- Single metadata file or versioned? (metadata.dat vs metadata.1.dat, metadata.2.dat)
- Atomic update strategy? (write temp, rename?)
- Backup old metadata on update?

---

## 🔴 3. Node Structure Conversion (In-Memory vs On-Disk) [CRITICAL] ✅

### DECISION: Separate Node Types (mem_art vs data_art)

**Reference**: This section (implementation in `data_art.h` / `data_art.c`)

The persistent ART will use **completely separate node structures** from the in-memory version. This provides clear separation of concerns and avoids contaminating the simple in-memory implementation.

### Rationale

1. **Clean separation**: `mem_art.h` stays simple (pointers only), `data_art.h` uses page references
2. **No conditional compilation**: Easier to maintain, test, and understand
3. **Different optimization goals**: In-memory optimized for pointer chasing, persistent optimized for disk layout
4. **Type safety**: Compiler catches mixing of pointer-based and page-based nodes

### In-Memory Nodes (mem_art.h) - BASELINE

```c
typedef struct art_node {
    art_node_type_t type;
    uint32_t num_children;
    uint32_t partial_len;
    uint8_t partial[10];
    union {
        art_node4_t node4;   // Contains art_node_t *children[4]
        art_node16_t node16; // Contains art_node_t *children[16]
        art_node48_t node48; // Contains art_node_t *children[48]
        art_node256_t node256; // Contains art_node_t *children[256]
    };
} art_node_t;

typedef struct {
    uint8_t keys[4];
    art_node_t *children[4];  // Direct memory pointers
} art_node4_t;
```

### Persistent Nodes (data_art.h) - NEW IMPLEMENTATION

```c
// Page reference instead of pointer
typedef struct {
    uint64_t page_id;    // Which page?
    uint32_t offset;     // Offset within page (0-4095)
} node_ref_t;

#define NULL_NODE_REF ((node_ref_t){.page_id = 0, .offset = 0})

typedef struct data_art_node {
    art_node_type_t type;
    uint32_t num_children;
    uint32_t partial_len;
    uint8_t partial[10];
    union {
        data_art_node4_t node4;
        data_art_node16_t node16;
        data_art_node48_t node48;
        data_art_node256_t node256;
    };
} data_art_node_t;

typedef struct {
    uint8_t keys[4];
    node_ref_t children[4];  // Page references, not pointers
} data_art_node4_t;

typedef struct {
    uint8_t keys[16];
    node_ref_t children[16];
} data_art_node16_t;

typedef struct {
    uint8_t keys[256];       // Index: key → child slot (255 = empty)
    node_ref_t children[48];
} data_art_node48_t;

typedef struct {
    node_ref_t children[256]; // Direct mapping
} data_art_node256_t;

typedef struct {
    uint32_t key_len;
    uint32_t value_len;
    uint8_t data[];  // key followed by value
} data_art_leaf_t;
```

### On-Disk Serialization Format

Nodes are serialized with **fixed structure** for easy deserialization:

```c
// NODE_4 on disk layout
typedef struct {
    uint8_t type;               // 1 byte: NODE_4
    uint8_t num_children;       // 1 byte: 0-4
    uint8_t partial_len;        // 1 byte
    uint8_t padding1;           // 1 byte (alignment)
    uint8_t partial[10];        // 10 bytes
    uint8_t keys[4];            // 4 bytes
    
    // Children as page references
    uint64_t child_page_ids[4];   // 32 bytes
    uint32_t child_offsets[4];    // 16 bytes
    
    // Total: 70 bytes
} __attribute__((packed)) node4_disk_t;

// NODE_16 on disk layout
typedef struct {
    uint8_t type;               // 1 byte
    uint8_t num_children;       // 1 byte
    uint8_t partial_len;        // 1 byte
    uint8_t padding1;           // 1 byte
    uint8_t partial[10];        // 10 bytes
    uint8_t keys[16];           // 16 bytes
    uint8_t padding2[2];        // 2 bytes (alignment)
    
    uint64_t child_page_ids[16];  // 128 bytes
    uint32_t child_offsets[16];   // 64 bytes
    
    // Total: 240 bytes
} __attribute__((packed)) node16_disk_t;

// NODE_48 on disk layout
typedef struct {
    uint8_t type;
    uint8_t num_children;
    uint8_t partial_len;
    uint8_t padding1;
    uint8_t partial[10];
    uint8_t keys[256];          // Index array
    uint8_t padding2[2];
    
    uint64_t child_page_ids[48];  // 384 bytes
    uint32_t child_offsets[48];   // 192 bytes
    
    // Total: 658 bytes
} __attribute__((packed)) node48_disk_t;

// NODE_256 on disk layout
typedef struct {
    uint8_t type;
    uint8_t num_children;       // Always 256 when full
    uint8_t partial_len;
    uint8_t padding1;
    uint8_t partial[10];
    uint8_t padding2[2];
    
    uint64_t child_page_ids[256];   // 2048 bytes
    uint32_t child_offsets[256];    // 1024 bytes
    
    // Total: 3088 bytes (~3KB)
} __attribute__((packed)) node256_disk_t;

// Leaf on disk layout
typedef struct {
    uint8_t type;               // LEAF
    uint8_t padding[3];
    uint32_t key_len;
    uint32_t value_len;
    uint8_t data[];             // key + value (variable size)
} __attribute__((packed)) leaf_disk_t;
```

### Size Analysis

| Node Type | On-Disk Size | Notes |
|-----------|--------------|-------|
| NODE_4    | 70 bytes     | Fixed size |
| NODE_16   | 240 bytes    | Fixed size |
| NODE_48   | 658 bytes    | Fixed size |
| NODE_256  | 3088 bytes   | Fixed size (~75% of page) |
| Leaf      | 12 + key_len + value_len | Variable size |

**Benefit of fixed sizes**: Easy to allocate and load. NODE_256 fits in one page with room for header.

### Serialization/Deserialization

```c
// Serialize in-memory node to disk format
void serialize_node(const data_art_node_t *node, uint8_t *disk_buffer) {
    switch (node->type) {
        case NODE_4: {
            node4_disk_t *disk = (node4_disk_t*)disk_buffer;
            disk->type = NODE_4;
            disk->num_children = node->num_children;
            disk->partial_len = node->partial_len;
            memcpy(disk->partial, node->partial, 10);
            memcpy(disk->keys, node->node4.keys, 4);
            
            for (int i = 0; i < 4; i++) {
                disk->child_page_ids[i] = node->node4.children[i].page_id;
                disk->child_offsets[i] = node->node4.children[i].offset;
            }
            break;
        }
        // Similar for NODE_16, NODE_48, NODE_256
    }
}

// Deserialize disk format to in-memory node
data_art_node_t *deserialize_node(const uint8_t *disk_buffer) {
    uint8_t type = disk_buffer[0];
    data_art_node_t *node = alloc_node(type);
    
    switch (type) {
        case NODE_4: {
            node4_disk_t *disk = (node4_disk_t*)disk_buffer;
            node->type = NODE_4;
            node->num_children = disk->num_children;
            node->partial_len = disk->partial_len;
            memcpy(node->partial, disk->partial, 10);
            memcpy(node->node4.keys, disk->keys, 4);
            
            for (int i = 0; i < 4; i++) {
                node->node4.children[i].page_id = disk->child_page_ids[i];
                node->node4.children[i].offset = disk->child_offsets[i];
            }
            break;
        }
        // Similar for other types
    }
    
    return node;
}
```

### Design Decisions

✅ **DECIDED: Separate structs** - No conditional compilation, clear separation
✅ **DECIDED: Fixed-size serialization** - Easy allocation, predictable sizes
✅ **DECIDED: Packed structs** - Minimize disk space, explicit padding for alignment
🟡 **TODO: Endianness** - Store in little-endian for portability? Or native for speed?
🟡 **TODO: Versioning** - Add version byte to disk format for future compatibility?



---

## 🔴 4. Checkpoint Mechanism [CRITICAL] ✅

### DECISION: Hybrid Strategy (WAL-size-primary with time and memory backups)

**Reference**: `CHECKPOINT.md`

### Rationale

Checkpoint when **ANY** of these conditions is met:

1. **WAL Size > 256 MB** (PRIMARY) - Bounds recovery time and disk space
2. **Time > 10 minutes** (BACKUP) - Prevents indefinite stalls during light load
3. **Dirty Pages > 50k** (SAFETY) - Prevents memory exhaustion

### Configuration

```c
typedef struct {
    // Primary trigger
    uint64_t wal_threshold;        // Default: 256 MB
    
    // Backup triggers
    uint64_t time_threshold;       // Default: 600 seconds (10 minutes)
    uint64_t dirty_pages_threshold; // Default: 50000 pages (~200 MB)
    
    // Performance
    uint32_t checkpoint_timeout_ms; // Default: 30000 (30 seconds max)
} checkpoint_config_t;
```

### Why This Strategy?

- **WAL-size-primary**: Ethereum has bursty writes (block processing), WAL grows fast
  - 256 MB = ~2-5 seconds recovery time (acceptable)
  - Triggers after ~5-10 blocks on mainnet
- **Time-backup**: Prevents stale checkpoints during idle periods
  - 10 minutes = reasonable for light load (~50-100 blocks)
- **Memory-safety**: Prevents OOM from many small updates to different pages
  - 50k pages × 4 KB = 200 MB dirty memory

### Checkpoint Process

```c
checkpoint_result_t checkpoint_full(data_art_tree_t *tree) {
    // 1. Block write transactions (readers continue via MVCC)
    pthread_rwlock_wrlock(&tree->checkpoint_lock);
    
    // 2. Flush all dirty pages in buffer pool
    dirty_page_list_t *dirty = buffer_pool_get_dirty_pages(tree->cache);
    for (each page) {
        page_manager_write(tree->pm, page);
        mark_clean(page);
    }
    page_manager_sync_all(tree->pm);
    
    // 3. Update metadata with checkpoint LSN
    tree->metadata.checkpoint_lsn = wal_current_lsn(tree->wal);
    update_metadata(&tree->metadata);
    
    // 4. Truncate WAL (delete entries before checkpoint)
    wal_truncate(tree->wal, tree->metadata.checkpoint_lsn);
    
    // 5. Release lock
    pthread_rwlock_unlock(&tree->checkpoint_lock);
}
```

### Design Decisions

✅ **DECIDED: Hybrid triggers** - WAL size (256MB) + time (10min) + memory (50k pages)
✅ **DECIDED: Full checkpoint** - Flush all dirty pages (simple, Phase 1)
✅ **DECIDED: Block writes during checkpoint** - Ensures consistency
✅ **DECIDED: Background thread** - Non-blocking checkpoint manager
🟡 **TODO (Phase 2): Incremental checkpoint** - Flush oldest pages only, reduce pause time
🟡 **TODO: Checkpoint compression** - Track compression statistics during checkpoint



---

## 🟡 5. Transaction Boundaries & Atomicity [IMPORTANT] ✅

### DECISION: Explicit Multi-Operation Transactions

**Reference**: This section (implementation in `data_art_txn.c`)

**Problem**: What's the scope of a transaction? Single operation or multi-key batch?

**DECISION: Explicit Transactions (User Controls Boundaries)** ✅

### Chosen Approach: Explicit Transactions
```c
// User controls transaction boundaries
txn = data_art_begin_write(tree);
data_art_insert_txn(txn, key1, value1);
data_art_insert_txn(txn, key2, value2);
data_art_delete_txn(txn, key3);
data_art_commit_write(txn);  // All or nothing
```
**Pros**: 
- Multi-key atomicity (essential for Ethereum)
- User controls transaction scope
- Batch operations for better performance

**Cons**: 
- More complex implementation
- Need rollback on abort
- Must track allocated pages for cleanup

### Alternative (NOT Chosen): Single-Operation Transactions
```c
// Each insert/delete is its own transaction
data_art_insert(tree, key, value);  // Atomic
```
**Why not chosen**: 
- Can't group operations atomically
- Ethereum block processing requires updating multiple accounts atomically
- No way to rollback partial updates

### Rollback Implementation
```c
typedef struct {
    data_art_tree_t *tree;
    uint64_t new_version;
    node_ref_t new_root;
    
    // Track allocated pages for rollback
    uint64_t *allocated_pages;  // Pages allocated during txn
    size_t num_allocated;
    
    wal_txn_t *wal_txn;
} data_art_txn_t;

void data_art_abort_write(data_art_txn_t *txn) {
    // 1. Free all pages allocated during transaction
    for (size_t i = 0; i < txn->num_allocated; i++) {
        page_manager_free(txn->tree->pm, txn->allocated_pages[i]);
    }
    
    // 2. Discard WAL entries
    wal_abort(txn->wal_txn);
    
    // 3. Don't update tree root
    // tree->root stays at old version
    
    free(txn->allocated_pages);
    free(txn);
}
```

**Rationale for Explicit Transactions**:
- **Ethereum Requirement**: Block processing needs multi-key transactions (update many accounts atomically)
- **Atomicity**: All updates in a transaction succeed or all fail
- **Consistency**: State is never left in partial update
- **Performance**: Batch operations reduce WAL syncs

---

## 🟡 6. Version History Management [IMPORTANT] ✅

### DECISION: Hybrid Strategy (Reference Counting + Time Window + Count Limit)

**Reference**: This section (implementation in `version_gc.c`)

**Problem**: How many old versions do we keep? When does garbage collection run?

### Strategy

Keep a version if **ANY** of these conditions is true:

1. **Reference counting** (ref_count > 0) - Active snapshots using this version
2. **Time-based retention** (age < 1 hour) - Recent versions for historical queries  
3. **Count-based limit** (within last 100 versions) - Bounded memory usage

```c
typedef struct {
    uint64_t version_id;
    uint64_t timestamp;
    uint32_t ref_count;          // Number of active snapshots
    node_ref_t root;             // Root pointer for this version
} version_metadata_t;

typedef struct {
    uint64_t retention_time_sec;  // Default: 3600 (1 hour)
    uint32_t max_versions;        // Default: 100
    uint64_t gc_interval_sec;     // Default: 60 (run every minute)
} gc_config_t;
```

### Rationale

**Why Reference Counting (Primary)**:
- **Safety**: Never delete versions with active snapshots (prevents crashes)
- **MVCC correctness**: Readers hold snapshots, version pinned until released
- **Example**: Long-running RPC query holds snapshot for 30 seconds → version protected

**Why Time-Based Retention**:
- **Historical queries**: Support queries on recent blocks (e.g., `eth_call` at block N-10)
- **Typical pattern**: RPC queries access last few minutes of state
- **Example**: 1 hour retention = ~300 blocks on Ethereum mainnet

**Why Count-Based Limit**:
- **Bounded memory**: Prevents unbounded growth during heavy sync
- **Safety net**: Even if time retention is generous, cap total versions
- **Example**: 100 versions × 500MB each = 50GB max (predictable)

### GC Decision Logic

```c
bool is_version_eligible_for_gc(version_metadata_t *version, 
                                 uint64_t current_version_id,
                                 gc_config_t *config) {
    uint64_t now = time(NULL);
    
    // Keep if ANY condition is true
    if (version->ref_count > 0) {
        return false;  // SAFETY: Active snapshots using it
    }
    
    if (now - version->timestamp < config->retention_time_sec) {
        return false;  // RECENT: Within time window (1 hour)
    }
    
    if (current_version_id - version->version_id < config->max_versions) {
        return false;  // BOUNDED: Within count window (100 versions)
    }
    
    // All conditions failed → eligible for GC
    return true;
}
```

### GC Implementation

```c
void gc_old_versions(data_art_tree_t *tree) {
    pthread_rwlock_wrlock(&tree->version_lock);
    
    uint64_t current_version = tree->current_version_id;
    version_list_t *versions = tree->versions;
    
    // Scan all versions and collect eligible ones
    for (size_t i = 0; i < versions->count; i++) {
        version_metadata_t *version = &versions->entries[i];
        
        if (is_version_eligible_for_gc(version, current_version, &tree->gc_config)) {
            // Free all pages belonging to this version
            free_version_pages(tree->pm, version->version_id);
            
            // Remove from version list
            remove_version(versions, i);
            i--;  // Adjust index after removal
            
            LOG_INFO("GC: Freed version %lu", version->version_id);
        }
    }
    
    pthread_rwlock_unlock(&tree->version_lock);
}
```

### GC Triggers

**Background thread** (primary) + **checkpoint-triggered** (opportunistic):

```c
typedef struct {
    data_art_tree_t *tree;
    pthread_t thread;
    bool running;
    uint64_t gc_runs;
    uint64_t versions_freed;
} gc_manager_t;

void *gc_thread(void *arg) {
    gc_manager_t *gc = (gc_manager_t *)arg;
    
    while (gc->running) {
        // Run every minute
        sleep(gc->tree->gc_config.gc_interval_sec);
        
        uint64_t before = gc->tree->versions->count;
        gc_old_versions(gc->tree);
        uint64_t after = gc->tree->versions->count;
        
        gc->gc_runs++;
        gc->versions_freed += (before - after);
        
        LOG_DEBUG("GC run #%lu: freed %lu versions", 
                  gc->gc_runs, before - after);
    }
    
    return NULL;
}

// Also run during checkpoint
void checkpoint_full(data_art_tree_t *tree) {
    // ... flush pages ...
    
    // Opportunistic GC
    gc_old_versions(tree);
}
```

### Configuration Examples

**Archive Node** (keep more history):
```c
gc_config_t archive_config = {
    .retention_time_sec = 86400,  // 24 hours
    .max_versions = 1000,         // 1000 versions
    .gc_interval_sec = 300,       // 5 minutes
};
```

**RPC Node** (less history):
```c
gc_config_t rpc_config = {
    .retention_time_sec = 3600,   // 1 hour
    .max_versions = 100,          // 100 versions
    .gc_interval_sec = 60,        // 1 minute
};
```

**Light Node** (minimal history):
```c
gc_config_t light_config = {
    .retention_time_sec = 300,    // 5 minutes
    .max_versions = 10,           // 10 versions
    .gc_interval_sec = 30,        // 30 seconds
};
```

### Design Decisions

✅ **DECIDED: Hybrid (refcount + time + count)** - Covers all scenarios
✅ **DECIDED: Background GC thread** - Runs every minute
✅ **DECIDED: Checkpoint-triggered GC** - Opportunistic cleanup
✅ **DECIDED: 1 hour default retention** - Balance history vs memory
✅ **DECIDED: 100 versions default limit** - Bounded memory (~50GB)
🟡 **TODO: Incremental GC** - Process N versions per run to avoid spikes
🟡 **TODO: GC metrics** - Track freed versions, memory reclaimed, GC duration



---

---

## 🟡 7. Concurrency & Lock Granularity [IMPORTANT] ✅

### DECISION: Coarse-Grained Locking (4 rwlocks) + MVCC

**Reference**: `CONCURRENCY.md`

**DECISION: Coarse-Grained Locking (4 rwlocks) + MVCC** ✅

**See [CONCURRENCY.md](CONCURRENCY.md) for complete design.**

### Rationale

For a persistent database, **disk I/O is the bottleneck** (1-10ms per page), not locking (~100ns). Complex fine-grained locking adds overhead without improving throughput.

**Strategy**: Simple coarse-grained locks + MVCC snapshots provide:
- ✅ Correctness (no data races)
- ✅ Simplicity (no deadlocks)
- ✅ Concurrency (unlimited readers, one writer)
- ✅ Ethereum-optimized (matches serial block processing)

### Lock Structure

```c
typedef struct {
    // Global write lock - only ONE writer at a time
    pthread_rwlock_t write_lock;
    
    // Buffer pool lock - protects page cache ART
    pthread_rwlock_t cache_lock;
    
    // Version list lock - protects version metadata
    pthread_rwlock_t version_lock;
    
    // Checkpoint lock - for checkpoint operations
    pthread_rwlock_t checkpoint_lock;
} data_art_tree_t;
```

### Lock Hierarchy (Deadlock Prevention)

**Always acquire in this order**:
1. `checkpoint_lock` - Checkpoint coordination
2. `write_lock` - Transaction serialization
3. `cache_lock` - Buffer pool access
4. `version_lock` - Snapshot management

**Rule**: Never acquire locks out of order to prevent deadlocks.

### Lock Purposes

| Lock | Purpose | Acquired By | Hold Time |
|------|---------|-------------|-----------|
| write_lock | Serialize writes | Writer (WRITE), Reader (READ) | Writer: ~1-10ms, Reader: ~1-10µs |
| cache_lock | Protect buffer pool | Page lookup/insert/evict | ~1-5µs |
| version_lock | Protect version list | Snapshot create/destroy, GC | ~1-10µs |
| checkpoint_lock | Coordinate checkpoint | Checkpoint (WRITE), Writer (READ) | Checkpoint: ~1-5s |

### Concurrency Guarantees

**Multiple Readers**:
```c
// All readers acquire read lock (shared) - no blocking
Thread 1: data_art_begin_read() → Snapshot at version 100
Thread 2: data_art_begin_read() → Snapshot at version 100
Thread 3: data_art_begin_read() → Snapshot at version 100

// Linear scalability, all use buffer pool concurrently
```

**Readers + Writer**:
```c
// MVCC allows concurrent access to different versions
Reader 1: Reading version 100 (snapshot held)
Reader 2: Reading version 100 (snapshot held)
Writer:   Creating version 101 (CoW, new pages)

// Writer commits → new readers see version 101
// Old readers continue with version 100 (snapshot isolation)
```

**Single Writer**:
- Only one writer can modify tree at a time (exclusive write_lock)
- Matches Ethereum's serial block processing (deterministic state root)
- Sufficient for target use case

### Why Single Writer?

**For Ethereum**: ✅ Blocks processed serially (one at a time)
- State transitions must be sequential
- Even with parallel execution, final state update is serial
- No need for complex multi-writer conflict resolution

**For other use cases**: Could optimize later if needed

### Performance Characteristics

| Operation | Time | Bottleneck |
|-----------|------|------------|
| Disk I/O | 1-10ms | **PRIMARY** |
| Lock acquire/release | ~100ns | Negligible |
| ART lookup | ~1-5µs | Minor |

**Conclusion**: Lock overhead is ~0.001% of total operation time. Disk I/O dominates.

### When to Optimize?

Consider fine-grained locking ONLY if profiling shows:
- Cache lock contention > 5% of CPU time
- Write lock blocking readers > 10ms regularly
- Multiple writers required (not Ethereum use case)

**Current design is sufficient for Phase 1-5**. Defer to Phase 6 if needed.

### Design Decisions

✅ **DECIDED: Coarse-grained (4 rwlocks)** - Simple, correct, sufficient
✅ **DECIDED: Single writer** - Matches Ethereum block processing
✅ **DECIDED: MVCC for readers** - Unlimited concurrent readers
✅ **DECIDED: Lock hierarchy** - Prevents deadlocks
✅ **DECIDED: No per-page locks** - Unnecessary complexity, defer to Phase 6
🟡 **TODO: Lock profiling** - Measure contention in production
🟡 **TODO: Reader starvation monitoring** - Track if writers block too long



---

## 🟡 8. Error Handling & Durability

**Problem**: What happens when operations fail? How do we guarantee durability?

### Error Categories

**1. Disk Full**
```c
if (page_manager_write() == -1 && errno == ENOSPC) {
    // Disk full - abort transaction
    data_art_abort_write(txn);
    return ERROR_DISK_FULL;
}
```

**2. I/O Error**
```c
if (fsync(fd) == -1) {
    // Failed to sync - data may be lost
    log_error("fsync failed: %s", strerror(errno));
    // Mark database as potentially corrupted?
    tree->health_status = DB_UNHEALTHY;
    return ERROR_IO_FAILURE;
}
```

**3. Corrupted Page**
```c
if (verify_checksum(page) == false) {
    // Page corrupted on disk
    log_error("Corrupted page %lu detected", page_id);
    // Try to recover from WAL?
    return ERROR_CORRUPTION;
}
```

### DECISION: Defense in Depth with Fail-Safe Defaults ✅

**Reference**: `ERROR_HANDLING.md`

**Rationale**:
- Durability first - WAL fsync() before commit (no compromises)
- Fail-fast on corruption - don't propagate bad data
- Graceful degradation - retry transient errors with limit
- Clear error reporting - detailed error codes, not generic -1
- Crash recovery always works - WAL replay is robust

### Error Categories & Handling

**1. Disk Full (ENOSPC)**:
- Strategy: Abort transaction cleanly, return ERROR_DISK_FULL
- User Action: Free up space, retry transaction
- Database State: Healthy (no data lost)

**2. I/O Errors (EIO)**:
- Strategy: Retry with exponential backoff (max 3 attempts), then fail-stop
- User Action: Check hardware, restore from backup if needed
- Database State: Unhealthy (may recover) or Corrupted (if persistent)

**3. Corrupted Page (Checksum Mismatch)**:
- Strategy: Fail-stop immediately, mark database corrupted
- User Action: **STOP** - restore from backup
- Database State: Corrupted (cannot continue safely)

**4. WAL Sync Failure**:
- Strategy: Abort transaction, cannot guarantee durability
- User Action: Retry transaction, check disk health
- Database State: Healthy (transaction aborted cleanly)

### Durability Protocol

```c
// ACID Compliance:
// - Atomicity: All operations commit/abort together (via WAL)
// - Consistency: Tree invariants maintained
// - Isolation: Readers see consistent snapshots (MVCC)
// - Durability: Committed transactions survive crashes (WAL fsync)

int data_art_commit_write(data_art_txn_t *txn) {
    // Phase 1: Prepare (build tree in memory)
    
    // Phase 2: Log to WAL (buffered writes)
    wal_write_entries(txn->wal_txn, txn->operations);
    wal_write_commit(txn->wal_txn);
    
    // Phase 3: DURABILITY POINT
    if (wal_sync(txn->tree->wal) != 0) {
        data_art_abort_write(txn);
        return ERROR_WAL_SYNC_FAILED;
    }
    
    // *** CRASH BEFORE: Transaction lost (not durable) ***
    // *** CRASH AFTER: Transaction recoverable (durable) ***
    
    // Phase 4: Update in-memory state (safe now)
    txn->tree->current_root = txn->new_root;
    txn->tree->current_version = txn->new_version;
    
    // Phase 5: Mark dirty pages (flush at checkpoint)
    // Phase 6: Release locks
    
    return SUCCESS;
}
```

### Crash Recovery

```c
recovery_stats_t recover_database(data_art_tree_t *tree) {
    // Phase 1: Load metadata (double-buffered)
    database_header_t *meta = load_metadata(tree->db_path);
    
    // Phase 2: Restore tree state
    tree->current_root = meta->current_root;
    tree->current_version = meta->current_version_id;
    
    // Phase 3: Determine WAL replay range
    uint64_t start_lsn = meta->checkpoint_lsn;
    uint64_t end_lsn = wal_get_last_lsn(tree->wal);
    
    // Phase 4: Replay WAL (re-apply operations)
    wal_replay(tree->wal, start_lsn, end_lsn, tree, &stats);
    
    // Phase 5: Verify integrity
    verify_tree_integrity(tree);
    
    return stats;
}
```

**Recovery Guarantees**:
- All committed transactions recovered (WAL fsync before commit)
- Tree integrity verified (checksum validation)
- Uncommitted transactions discarded (WAL abort entries)
- Recovery time: O(WAL_size) - bounded by checkpoint frequency

### Error Code Design

```c
typedef enum {
    // Success
    SUCCESS = 0,
    
    // Recoverable (user can retry)
    ERROR_DISK_FULL = -1,
    ERROR_WOULD_BLOCK = -2,
    ERROR_NOT_FOUND = -4,
    
    // Fatal (abort transaction)
    ERROR_IO_FAILURE = -10,
    ERROR_WAL_SYNC_FAILED = -11,
    ERROR_CORRUPTION = -12,
    
    // Database health
    ERROR_DB_UNHEALTHY = -20,
    ERROR_DB_CORRUPTED = -21,
    ERROR_DB_READ_ONLY = -22,
} db_error_t;
```

### Health Monitoring

```c
typedef enum {
    DB_HEALTHY,      // All systems operational
    DB_UNHEALTHY,    // I/O errors detected, may recover
    DB_CORRUPTED,    // Corruption detected, need restore
    DB_READ_ONLY,    // Operating in degraded mode
} db_health_status_t;

typedef struct {
    db_health_status_t status;
    uint64_t io_errors;
    uint64_t checksum_failures;
    uint64_t wal_sync_failures;
    char last_error_msg[256];
} db_health_t;
```

### Best Practices

**DO** ✅:
- Always fsync() WAL before commit
- Check return values and errno
- Verify checksums on page load
- Retry transient errors (max 3 times)
- Log all errors with context

**DON'T** ❌:
- Never skip fsync() for performance
- Never continue with corrupted data
- Never ignore error codes
- Never retry indefinitely
- Never return generic -1 errors

### DECIDED ✅
- ✅ WAL fsync() before commit (durability guarantee)
- ✅ Retry I/O errors 3 times with exponential backoff
- ✅ Fail-stop on corruption (checksum mismatch)
- ✅ Detailed error codes (not generic -1)
- ✅ Health monitoring (track database state)
- ✅ Crash recovery via WAL replay from checkpoint_lsn

---

## 🟡 9. Iterator Support for Persistent Tree [IMPORTANT] ✅

### DECISION: Separate Persistent Iterator Logic

**Rationale**:
- In-memory iterator (art.c) uses raw pointers - simple, fast
- Persistent iterator (data_art.c) needs page references + buffer pool - complex
- Keep separate implementations (don't mix concerns)
- Persistent iterator operates on snapshots (MVCC isolation)

### Current Iterator (art.c) - Memory Pointers
```c
typedef struct {
    struct {
        art_node_t *node;     // Direct memory pointer
        int child_idx;
    } stack[64];
    int depth;
} art_iterator_state_t;

// Simple: no page loading, no pinning, no I/O
```

### Persistent Iterator (data_art.c) - Page References
```c
typedef struct {
    struct {
        node_ref_t node_ref;       // (page_id, offset) - disk reference
        data_art_node_t *node_ptr; // Cached pointer (pinned in buffer pool)
        int child_idx;
    } stack[64];
    int depth;
    
    data_art_snapshot_t *snapshot;  // Associated snapshot (MVCC)
    buffer_pool_t *cache;           // For loading pages
    
    uint64_t creation_time;         // For timeout detection
    uint64_t pages_pinned;          // Track pinned page count
} data_art_iterator_state_t;
```

### Operations
```c
// Create iterator for specific snapshot version
data_art_iterator_t *data_art_iterator_create(data_art_snapshot_t *snapshot) {
    data_art_iterator_t *iter = malloc(sizeof(*iter));
    iter->snapshot = snapshot;
    iter->cache = snapshot->tree->cache;
    iter->creation_time = time(NULL);
    iter->pages_pinned = 0;
    
    // Start at root of snapshot version
    iter->stack[0].node_ref = snapshot->root;
    iter->stack[0].node_ptr = NULL;  // Load on demand
    iter->depth = 1;
    
    return iter;
}

// Advance iterator, load pages as needed
bool data_art_iterator_next(data_art_iterator_t *iter, 
                            uint8_t **key_out, size_t *key_len_out,
                            uint8_t **value_out, size_t *value_len_out) {
    while (iter->depth > 0) {
        // Get current stack frame
        stack_frame_t *frame = &iter->stack[iter->depth - 1];
        
        // Load page if not cached
        if (frame->node_ptr == NULL) {
            page_t *page = buffer_pool_get(iter->cache, frame->node_ref.page_id);
            if (!page) {
                LOG_ERROR("Failed to load page %lu for iterator", 
                         frame->node_ref.page_id);
                return false;
            }
            
            // Pin page (prevent eviction during iteration)
            buffer_pool_pin(iter->cache, page);
            iter->pages_pinned++;
            
            frame->node_ptr = (data_art_node_t*)(page->data + frame->node_ref.offset);
        }
        
        // Traverse tree (same logic as art.c iterator, but with page loading)
        // ... find next leaf, push children to stack, etc ...
        
        if (found_leaf) {
            *key_out = leaf->key;
            *key_len_out = leaf->key_len;
            *value_out = leaf->value;
            *value_len_out = leaf->value_len;
            return true;
        }
    }
    
    return false;  // No more entries
}

// Clean up iterator, unpin all pages
void data_art_iterator_destroy(data_art_iterator_t *iter) {
    // Unpin all pages in stack
    for (int i = 0; i < iter->depth; i++) {
        if (iter->stack[i].node_ptr != NULL) {
            page_t *page = get_page_for_node(iter->cache, iter->stack[i].node_ptr);
            buffer_pool_unpin(iter->cache, page);
        }
    }
    
    // Release snapshot
    data_art_snapshot_release(iter->snapshot);
    
    free(iter);
}
```

### Page Pinning Strategy

**Decision**: Pin pages during traversal

```c
// Why pin?
// - Iterator may be long-lived (scanning millions of keys)
// - If pages evicted mid-iteration, iterator breaks
// - Pinning ensures consistency

// Pinned pages:
// - Root to current leaf path (~8-10 pages for typical tree depth)
// - Additional pages for children being explored
// - Total: ~20-30 pages pinned per iterator

// Trade-off:
// - Safety: Iterator always works (pages can't disappear)
// - Cost: Reduces effective buffer pool size
// - Limit: Max 100 concurrent iterators (configurable)
```

### Iterator Timeout

**Decision**: 5-minute timeout for iterators

```c
bool data_art_iterator_next(data_art_iterator_t *iter, ...) {
    // Check if iterator too old
    uint64_t elapsed = time(NULL) - iter->creation_time;
    if (elapsed > 300) {  // 5 minutes
        LOG_WARN("Iterator timed out after %lu seconds (%lu pages pinned)",
                 elapsed, iter->pages_pinned);
        return false;  // Force iterator close
    }
    
    // ... normal iteration ...
}
```

**Rationale**:
- Prevents iterators from holding pages indefinitely
- Ethereum use case: State iteration typically completes in seconds
- Long-lived iterators should use range queries instead

### Reverse Iteration

**Decision**: Not supported initially (forward-only)

**Rationale**:
- Ethereum doesn't need reverse iteration (state tree scans are forward)
- Adds complexity (prev pointer logic, boundary cases)
- Can be added in Phase 4 if needed

### DECIDED ✅
- ✅ Separate persistent iterator implementation (data_art_iterator.c)
- ✅ Use page references (node_ref_t) + buffer pool loading
- ✅ Pin pages during traversal (prevent eviction)
- ✅ Iterator operates on snapshot (MVCC isolation)
- ✅ 5-minute timeout (prevent resource hogging)
- ✅ Track pinned page count (monitoring)
- ✅ Forward iteration only (no reverse initially)

---

## 🟢 10. Configuration & Initialization API

**Problem**: How does user configure and open the database?

### Configuration Structure
```c
typedef struct {
    // Buffer pool
    size_t cache_size_mb;           // Default: 1024 (1 GB)
    
    // Compression
    uint64_t hot_threshold_sec;     // Default: 600 (10 min)
    uint64_t warm_threshold_sec;    // Default: 3600 (1 hour)
    uint64_t cold_threshold_sec;    // Default: 86400 (24 hours)
    bool enable_compression;        // Default: true
    
    // Checkpoints
    uint64_t checkpoint_interval_sec;  // Default: 300 (5 min)
    size_t checkpoint_wal_size_mb;     // Default: 256 MB
    
    // Versioning
    uint32_t max_versions;          // Default: 100
    uint64_t version_retention_sec; // Default: 3600 (1 hour)
    bool enable_gc;                 // Default: true
    
    // Concurrency
    uint32_t max_readers;           // Default: 128
    
    // I/O
    bool use_direct_io;             // Default: false
    size_t write_buffer_size;       // Default: 4 MB
    
} data_art_config_t;
```

### Initialization API
```c
// Open existing database or create new
data_art_tree_t *data_art_open(const char *db_path, 
                                const data_art_config_t *config) {
    // 1. Check if database exists
    if (db_exists(db_path)) {
        // Load metadata
        // Initialize page manager, buffer pool, WAL
        // Recover from crash if needed
    } else {
        // Create new database
        // Initialize empty tree
        // Write initial metadata
    }
    
    // 2. Start background threads (compactor, GC, checkpointer)
    
    return tree;
}

// Close database (wait for checkpoint)
void data_art_close(data_art_tree_t *tree) {
    // 1. Stop background threads
    // 2. Final checkpoint
    // 3. Close files
    // 4. Free memory
}

// Get default configuration
data_art_config_t data_art_default_config(void) {
    return (data_art_config_t){
        .cache_size_mb = 1024,
        .checkpoint_interval_sec = 300,
        // ... all defaults
    };
}
```

**Questions**:
- Support hot reconfiguration? (change cache size without restart)
- Validate config on open? (e.g., cache_size_mb > 0)
- Config file format? (TOML? JSON? Or just C struct?)

---

## 🟢 11. Monitoring & Observability

**Problem**: How do we monitor database health and performance?

### Statistics Structure
```c
typedef struct {
    // Buffer pool stats
    uint64_t cache_hits;
    uint64_t cache_misses;
    double cache_hit_rate;
    size_t pages_in_cache;
    size_t dirty_pages;
    
    // Compression stats
    uint64_t pages_compressed;
    uint64_t bytes_saved;
    double avg_compression_ratio;
    
    // Version stats
    uint32_t num_versions;
    uint64_t oldest_version;
    uint32_t active_snapshots;
    
    // I/O stats
    uint64_t pages_read;
    uint64_t pages_written;
    uint64_t wal_bytes_written;
    
    // Transaction stats
    uint64_t commits;
    uint64_t aborts;
    
    // Checkpoints
    uint64_t checkpoints;
    uint64_t last_checkpoint_duration_ms;
    
    // GC stats
    uint64_t gc_runs;
    uint64_t pages_reclaimed;
    
} data_art_stats_t;
```

### API
```c
// Get current statistics
data_art_stats_t data_art_get_stats(const data_art_tree_t *tree);

// Reset statistics counters
void data_art_reset_stats(data_art_tree_t *tree);

// Health check
typedef enum {
    DB_HEALTHY,
    DB_DEGRADED,      // Some issues but functional
    DB_UNHEALTHY,     // Critical issues, may need repair
    DB_CORRUPTED      // Data corruption detected
} db_health_status_t;

db_health_status_t data_art_health_check(data_art_tree_t *tree);
```

### Logging
```c
// Use existing logger.h infrastructure
LOG_DB_INFO("Checkpoint completed in %lu ms", duration);
LOG_DB_WARN("Cache hit rate low: %.2f%%", hit_rate);
LOG_DB_ERROR("Page corruption detected: page_id=%lu", page_id);
```

**Questions**:
- Export metrics to Prometheus? (for production monitoring)
- Periodic stats dump to log file?
- Debug mode with verbose logging?

---

## 🟢 12. Multi-File Management

**Problem**: How to split data across multiple files when > 1GB?

### File Organization
```
db_path/
├── metadata.dat                  # Database header
├── data/
│   ├── pages_00000000.dat       # First 128K pages (512 MB @ 4KB/page)
│   ├── pages_00000001.dat       # Next 128K pages
│   └── pages_00000002.dat       # ...
└── wal/
    ├── wal_00000000.log         # First WAL segment (256 MB)
    └── wal_00000001.log         # Next segment
```

### Page ID Encoding

**Option A: Flat Numbering**
```c
// page_id is just a sequential number
// File number = page_id / PAGES_PER_FILE
// Offset = (page_id % PAGES_PER_FILE) * 4096

#define PAGES_PER_FILE 131072  // 512 MB / 4096 bytes

uint32_t get_file_num(uint64_t page_id) {
    return page_id / PAGES_PER_FILE;
}

uint64_t get_file_offset(uint64_t page_id) {
    return (page_id % PAGES_PER_FILE) * 4096;
}
```

**Option B: Embedded File Number**
```c
// Upper 32 bits = file number
// Lower 32 bits = page number within file

uint64_t make_page_id(uint32_t file_num, uint32_t page_num) {
    return ((uint64_t)file_num << 32) | page_num;
}

uint32_t get_file_num(uint64_t page_id) {
    return page_id >> 32;
}

uint32_t get_page_num(uint64_t page_id) {
    return page_id & 0xFFFFFFFF;
}
```

### File Growth Strategy

**Pre-allocation**:
```c
// Allocate entire 512 MB file upfront
posix_fallocate(fd, 0, 512 * 1024 * 1024);
```
**Pros**: No fragmentation, predictable performance
**Cons**: Wastes space if not fully used

**On-Demand Growth**:
```c
// Extend file as needed
ftruncate(fd, new_size);
```
**Pros**: Only uses needed space
**Cons**: May cause fragmentation, slower

**Questions**:
- File size limit? (512 MB, 1 GB, 4 GB?)
- Pre-allocate or grow on demand?
- Maximum number of files? (cleanup old files?)

---

## 🟢 13. Ethereum-Specific Integration

**Problem**: How does persistent ART integrate with Ethereum execution?

### State Trie Replacement

**Current**: Merkle Patricia Trie (MPT)
```
Block → State Root Hash → MPT → Account/Storage
```

**With ART**: Replace MPT with ART
```
Block → State Root Ref → ART → Account/Storage
```

**Questions**:
- Do we still compute Merkle root? (needed for consensus)
  - Option: Compute root hash on demand (traverse ART, hash nodes)
  - Option: Maintain separate Merkle tree in parallel (inefficient)
  - Option: Store hashes in ART nodes (bloat)
- How to handle block reorgs? (need to revert to previous version)
  - MVCC snapshots help! Just restore old version

### Transaction Boundaries

**Ethereum Block Processing**:
```c
// Process entire block as one transaction
txn = data_art_begin_write(state_tree);

for (tx in block.transactions) {
    // Update sender account (nonce, balance)
    data_art_insert_txn(txn, sender_key, updated_sender);
    
    // Update receiver account (balance)
    data_art_insert_txn(txn, receiver_key, updated_receiver);
    
    // Update contract storage slots
    for (slot in modified_slots) {
        data_art_insert_txn(txn, slot_key, slot_value);
    }
}

// Apply block rewards, uncle rewards, etc.
data_art_insert_txn(txn, miner_key, updated_miner);

// Commit entire block atomically
data_art_commit_write(txn);  // → New version = block number
```

**Benefits**:
- One version per block (easy to query historical state)
- Atomic block application
- Snapshot = specific block state

### Account Storage Layout

**Account Key**: 20-byte address
```c
uint8_t account_key[20] = {0x12, 0x34, ..., 0xab, 0xcd};
```

**Account Value**: RLP-encoded account data
```c
struct account {
    uint64_t nonce;
    uint256_t balance;
    uint256_t storage_root;  // Root of storage trie (or ref to ART)
    uint256_t code_hash;
};
// RLP encode → insert into ART
```

**Storage Slot Key**: address (20 bytes) + slot (32 bytes) = 52 bytes
```c
uint8_t storage_key[52];
memcpy(storage_key, address, 20);
memcpy(storage_key + 20, slot, 32);
```

**Storage Slot Value**: 32-byte value
```c
uint8_t storage_value[32] = {...};
```

**Questions**:
- Separate ART for accounts vs storage? (one global ART)
- How to handle contract code? (separate key-value store?)
- Prestate/poststate for debugging? (snapshots!)

---

## Summary

### Must Resolve Before Implementation (🔴)
1. ✅ Page allocation strategy → **Decision needed**
2. ✅ Root pointer persistence → **Design metadata file**
3. ✅ Node structure conversion → **Define on-disk layout**
4. ✅ Checkpoint mechanism → **Pick strategy (hybrid recommended)**

### Should Design Soon (🟡)
5. ✅ **Transaction atomicity → EXPLICIT TRANSACTIONS (decided)**
6. ✅ Version GC policy → **Hybrid: ref counting + time-based**
7. ✅ Concurrency model → **Current design sufficient**
8. ✅ Error handling → **Define error codes and recovery**
9. ✅ Persistent iterator → **Adapt current iterator**

### Can Defer (🟢)
10. ✅ Configuration API → **Design when needed**
11. ✅ Monitoring → **Add incrementally**
12. ✅ Multi-file management → **Implement when files grow large**
13. ✅ Ethereum integration → **After core implementation**

## Next Steps

**Immediate**:
- Review and decide on page allocation strategy (Item 1)
- Finalize metadata file format (Item 2)
- Design persistent node structures (Item 3)

**Near-term**:
- Start Phase 1 implementation (page manager)
- Prototype checkpoint mechanism (Item 4)
- Define transaction API (Item 5)

**Long-term**:
- Integrate with Ethereum execution layer (Item 13)
- Optimize based on real workload patterns
- Add production monitoring (Item 11)
