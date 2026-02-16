# ART Persistence Layer Design

## Overview

Hybrid page-based storage with Write-Ahead Logging (WAL) for disk persistence while maintaining in-memory performance for hot data.

## Two-Layer Architecture

### In-Memory ART (`art.h` / `art.c`)
- **Purpose**: Fast, simple, malloc-based implementation
- **Status**: Production-ready, 627 tests passing, proven performance
- **Usage**: 
  - Buffer pool page cache (page_id → cached_page_t*)
  - Small datasets that fit in RAM
  - Development and testing baseline
- **Characteristics**:
  - Direct memory pointers
  - No persistence
  - Zero overhead for hot data
  - Single-threaded (no locks)

### Persistent ART (`data_art.h` / `data_art.c`)
- **Purpose**: Disk-backed, scalable, multi-reader implementation
- **Status**: New implementation based on in-memory version
- **Usage**:
  - Large datasets (TB-scale Ethereum state)
  - Requires durability and crash recovery
  - Production blockchain nodes with concurrent RPC workers
- **Characteristics**:
  - Page-based references (page_id, offset)
  - WAL for crash recovery
  - Buffer pool caching for performance
  - **MVCC snapshots for concurrent readers**
  - Copy-on-write for non-blocking reads

### Layer Interaction
```
User Application (Multiple RPC Workers)
    ↓
┌─────────────────────────────────┐
│ data_art.h API                  │  ← Persistent API with MVCC
│ Reader 1: snapshot at version N │
│ Reader 2: snapshot at version N │
│ Writer: creates version N+1     │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Buffer Pool (uses art.h)        │  ← Thread-safe in-memory ART
│ page_cache: ART[page_id → page] │
│ Shared by all readers/writers   │
└─────────────────────────────────┘
    ↓
┌─────────────────────────────────┐
│ Page Manager + WAL              │  ← Disk I/O + versioning
│ Copy-on-write for MVCC          │
└─────────────────────────────────┘
```

## Architecture

### 1. Page-Based Storage

**Page Structure** (4KB fixed size):
```
Page Header (64 bytes):
- page_id: uint64_t
- version: uint64_t         # For MVCC
- node_count: uint32_t
- free_space_offset: uint32_t
- checksum: uint32_t
- flags: uint32_t
- prev_version: uint64_t    # Link to previous version (CoW)

Node Data (3968 bytes):
- Variable-size nodes packed sequentially
- Free space at end
```

**Node Layout**:
- Replace memory pointers with `(page_id, offset)` tuples
- Each node stores: type, partial_len, partial[10], num_children, child references
- Leaf nodes: type, key_len, value_len, data[]
- **Pages are immutable within a version** (copy-on-write for modifications)

### 2. Buffer Pool Cache

**LRU Cache** (1-2 GB configurable):
- Hot pages remain in memory
- Dirty pages marked for flush
- Eviction policy: Least Recently Used
- Page pinning for active operations
- **Uses in-memory ART for O(log n) page lookups by page_id**
- **Thread-safe with read-write locks for concurrent access**

**Cache Structure**:
```c
typedef struct cached_page {
    uint64_t page_id;
    uint64_t version;           // Page version for MVCC
    void *data;                 // 4KB page data
    bool dirty;                 // Modified since load
    uint32_t pin_count;         // Reference count
    uint64_t last_access;       // LRU timestamp
    
    // LRU doubly-linked list pointers
    struct cached_page *lru_prev;
    struct cached_page *lru_next;
} cached_page_t;

typedef struct {
    art_tree_t page_index;      // In-memory ART: page_id → cached_page_t*
    pthread_rwlock_t index_lock; // Protects page_index for concurrent access
    
    size_t capacity;            // Max pages in cache
    size_t size;                // Current page count
    uint64_t clock;             // LRU clock
    
    // LRU list for O(1) eviction
    cached_page_t *lru_head;
    cached_page_t *lru_tail;
    pthread_mutex_t lru_lock;   // Protects LRU list
    
    page_manager_t *pm;         // For page I/O
} buffer_pool_t;
```

**Why ART for Page Cache?**
- Page IDs are sequential uint64_t → excellent prefix compression
- Benchmarked faster than hash tables (uthash)
- Ordered iteration useful for sequential prefetch and checkpointing
- No hash collisions, no resize overhead
- Memory efficient, incremental growth
- **Read-write locks allow concurrent readers, single writer**

### 3. Write-Ahead Log (WAL)

**Log Entry Format**:
```
Entry Header:
- lsn: uint64_t (Log Sequence Number)
- type: uint8_t (INSERT/UPDATE/DELETE/CHECKPOINT)
- page_id: uint64_t
- data_len: uint32_t

Entry Data:
- Old value (for UNDO)
- New value (for REDO)
```

**WAL Operations**:
1. Write operation → Append to WAL first
2. Modify page in cache (mark dirty)
3. Periodic flush: WAL → Disk → Pages → Disk
4. Checkpoint: Sync all dirty pages, truncate WAL

### 4. Crash Recovery

**Recovery Process**:
1. Read last checkpoint LSN from metadata
2. Replay WAL entries from checkpoint forward
3. REDO all committed operations
4. Rebuild cache state

## 5. Compression

**Purpose**: Reduce disk space and I/O bandwidth while maintaining performance

**Strategy**: Tiered compression based on data access patterns

**Compression Levels**:
- **Hot Pages**: Uncompressed (recently accessed, in buffer pool)
- **Warm Pages**: LZ4 compression (occasional access, fast decompress ~2-4 GB/s)
- **Cold Pages**: Zstd level 5 (rarely accessed, high ratio ~3-5x)
- **Archival**: Zstd level 19 (old versions, maximum compression)

**Implementation Location**: Page Manager layer (transparent to ART logic)

**Benefits**:
- 2-3x disk space reduction for Ethereum state
- Reduced SSD wear
- Faster checkpoints (less data to write)
- Zero overhead for hot data (uncompressed in cache)

See [COMPRESSION.md](./COMPRESSION.md) for detailed design.

## 6. MVCC (Multi-Version Concurrency Control)

**Purpose**: Allow multiple concurrent readers without blocking writers

**Snapshot Isolation**:
- Each read transaction sees consistent snapshot at specific version
- Writers create new versions via copy-on-write
- Old versions garbage collected when no readers reference them

**Version Management**:
```c
typedef struct {
    uint64_t version_id;          // Current version number
    node_ref_t root_ref;          // Root at this version
    uint64_t commit_timestamp;    // When version was committed
} version_info_t;

typedef struct {
    version_info_t *versions;     // Array of historical versions
    size_t num_versions;
    uint64_t min_active_snapshot; // For garbage collection
    pthread_rwlock_t version_lock;
} version_manager_t;
```

**Copy-on-Write Process**:
1. Reader holds reference to root at version N
2. Writer modifies node → allocates new page → writes modified data
3. Writer updates parent to point to new page (new version)
4. Old page remains until no readers reference version N
5. Garbage collector reclaims old versions

**Snapshot Lifecycle**:
```c
// Reader: Begin snapshot
data_art_snapshot_t *snapshot = data_art_begin_read(tree);
// → Captures current version, increments reference count
// → Returns root_ref at captured version

// Reader: Access data (no locks needed during traversal)
const void *value = data_art_get_snapshot(snapshot, key, key_len);
// → Pages are immutable within snapshot
// → Safe concurrent access

// Reader: End snapshot
data_art_end_read(snapshot);
// → Decrements reference count
// → Allows GC of old versions
```

**Writer Isolation**:
```c
// Writer: Begin write transaction
data_art_txn_t *txn = data_art_begin_write(tree);

// Writer: Modifications create new pages (CoW)
data_art_insert_txn(txn, key, key_len, value, value_len);
// → Modified nodes allocated on new pages
// → Old pages remain unchanged

// Writer: Commit atomically updates version
data_art_commit_write(txn);
// → WAL sync
// → Atomic CAS: tree->current_version = new_version
// → New readers see new version, old readers still see old version
```

**Garbage Collection**:
```c
// Background GC thread
void gc_old_versions(data_art_tree_t *tree) {
    uint64_t min_snapshot = get_min_active_snapshot(tree);
    
    // Free pages from versions older than min_snapshot
    for (version_id < min_snapshot) {
        free_version_pages(tree, version_id);
    }
}
```

## File Layout

```
database/
├── include/
│   ├── art.h              # In-memory ART (unchanged)
│   ├── data_art.h         # Persistent ART API
│   ├── buffer_pool.h      # Page cache (uses art.h)
│   ├── page_manager.h     # Page allocation, I/O
│   └── wal.h              # Write-ahead log
├── src/
│   ├── art.c              # In-memory implementation (unchanged)
│   ├── data_art.c         # Persistent ART operations
│   ├── buffer_pool.c      # Buffer pool with ART cache
│   ├── page_manager.c     # Page management
│   └── wal.c              # WAL implementation
└── tests/
    ├── test_art.c         # In-memory tests (unchanged, 627 passing)
    ├── test_data_art.c    # Persistent ART tests
    ├── test_buffer_pool.c # Cache tests
    └── test_page_manager.c

art_database_files/
├── data/
│   ├── pages_00000.dat  # Page files (1GB each)
│   ├── pages_00001.dat
│   └── ...
├── wal/
│   ├── wal_00000.log     # WAL segments (256MB each)
│   ├── wal_00001.log
│   └── ...
└── metadata.dat          # Root page, checkpoint info
```

## Key Data Structures

### In-Memory ART (art.h) - Unchanged
```c
typedef struct art_tree {
    art_node_t *root;     // Memory pointer
    size_t size;
} art_tree_t;

// API remains unchanged
bool art_insert(art_tree_t *tree, const uint8_t *key, size_t key_len,
                const void *value, size_t value_len);
const void *art_get(const art_tree_t *tree, const uint8_t *key, 
                    size_t key_len, size_t *value_len);
bool art_delete(art_tree_t *tree, const uint8_t *key, size_t key_len);
```

### Node Reference (replaces pointers in data_art.h)
```c
typedef struct {
    uint64_t page_id;
    uint32_t offset;      // Offset within page
} node_ref_t;

#define NULL_REF ((node_ref_t){0, 0})
```

### Persistent Tree Root (data_art.h)
```c
typedef struct {
    node_ref_t root;          // Root node reference
    size_t size;              // Total entries
    uint64_t next_page_id;
    
    // MVCC version management
    uint64_t current_version;      // Latest committed version
    version_manager_t *versions;   // Historical versions
    
    // Dependencies
    buffer_pool_t *cache;     // Uses in-memory ART (thread-safe)
    page_manager_t *pm;
    wal_t *wal;
    
    // Concurrency control
    pthread_rwlock_t write_lock;   // Only one writer at a time
} data_art_tree_t;

// Reader snapshot (read-only view)
typedef struct {
    node_ref_t root_ref;      // Root at snapshot version
    uint64_t snapshot_version;
    data_art_tree_t *tree;    // Back-reference for cache access
} data_art_snapshot_t;

// Write transaction
typedef struct {
    data_art_tree_t *tree;
    uint64_t new_version;     // Version being created
    node_ref_t new_root;      // New root after modifications
    wal_txn_t *wal_txn;       // Associated WAL transaction
} data_art_txn_t;

// Persistent API
// Read operations (snapshot-based, non-blocking)
data_art_snapshot_t *data_art_begin_read(data_art_tree_t *tree);
const void *data_art_get_snapshot(const data_art_snapshot_t *snapshot,
                                  const uint8_t *key, size_t key_len,
                                  size_t *value_len);
void data_art_end_read(data_art_snapshot_t *snapshot);

// Write operations (transactional, single writer)
data_art_txn_t *data_art_begin_write(data_art_tree_t *tree);
bool data_art_insert_txn(data_art_txn_t *txn, const uint8_t *key,
                         size_t key_len, const void *value, size_t value_len);
bool data_art_delete_txn(data_art_txn_t *txn, const uint8_t *key,
                         size_t key_len);
bool data_art_commit_write(data_art_txn_t *txn);
void data_art_abort_write(data_art_txn_t *txn);

// Convenience wrappers (single-operation transactions)
bool data_art_insert(data_art_tree_t *tree, const uint8_t *key,
                     size_t key_len, const void *value, size_t value_len);
const void *data_art_get(const data_art_tree_t *tree, const uint8_t *key,
                         size_t key_len, size_t *value_len);
bool data_art_delete(data_art_tree_t *tree, const uint8_t *key,
                     size_t key_len);
```

### Buffer Pool (uses art.h internally)
```c
typedef struct {
    art_tree_t page_index;        // In-memory ART for lookups
    pthread_rwlock_t index_lock;  // Read-write lock for concurrent access
    
    size_t capacity;              // Max pages in cache
    size_t size;                  // Current pages
    uint64_t clock;               // LRU clock
    
    // LRU list for eviction (doubly-linked)
    cached_page_t *lru_head;
    cached_page_t *lru_tail;
    pthread_mutex_t lru_lock;     // Protects LRU list
    
    page_manager_t *pm;           // For page I/O
} buffer_pool_t;

// Buffer pool API (thread-safe)
cached_page_t *buffer_pool_get(buffer_pool_t *pool, uint64_t page_id,
                                uint64_t version);
void buffer_pool_mark_dirty(buffer_pool_t *pool, cached_page_t *page);
void buffer_pool_flush(buffer_pool_t *pool);
void buffer_pool_unpin(buffer_pool_t *pool, cached_page_t *page);
```

### Page ID Encoding for ART Cache
```c
// Encode page_id as big-endian 8 bytes for ART key
// Ensures numeric order = byte order
static inline void encode_page_id(uint64_t page_id, uint8_t key[8]) {
    key[0] = (page_id >> 56) & 0xFF;
    key[1] = (page_id >> 48) & 0xFF;
    key[2] = (page_id >> 40) & 0xFF;
    key[3] = (page_id >> 32) & 0xFF;
    key[4] = (page_id >> 24) & 0xFF;
    key[5] = (page_id >> 16) & 0xFF;
    key[6] = (page_id >> 8) & 0xFF;
    key[7] = page_id & 0xFF;
}
```

## Core Operations

### Node Allocation (data_art.c)
```c
// In-memory (art.c) - unchanged:
art_node_t *alloc_node(art_node_type_t type) {
    return calloc(1, sizeof(art_node_t));
}

// Persistent (data_art.c) - new:
node_ref_t alloc_node_persistent(data_art_tree_t *tree, art_node_type_t type) {
    // 1. Find page with enough free space (via page_manager)
    // 2. Allocate space in page
    // 3. Return (page_id, offset)
    uint64_t page_id = page_manager_alloc(tree->pm, sizeof(art_node_t));
    uint32_t offset = page_get_free_offset(page_id);
    return (node_ref_t){page_id, offset};
}
```

### Node Access (data_art.c)
```c
// In-memory (art.c) - direct pointer access:
node->node4.children[i]

// Persistent (data_art.c) - via buffer pool:
art_node_t *get_node(data_art_tree_t *tree, node_ref_t ref) {
    // 1. Encode page_id as ART key
    uint8_t key[8];
    encode_page_id(ref.page_id, key);
    
    // 2. Lookup in buffer pool (uses in-memory ART)
    size_t val_len;
    cached_page_t **page_ptr = art_get(&tree->cache->page_index, 
                                        key, 8, &val_len);
    
    cached_page_t *page;
    if (!page_ptr) {
        // 3. Cache miss - load from disk
        page = page_manager_load(tree->pm, ref.page_id);
        
        // 4. Insert into cache
        art_insert(&tree->cache->page_index, key, 8, 
                   &page, sizeof(page));
    } else {
        page = *page_ptr;
    }
    
    // 5. Update LRU
    lru_touch(tree->cache, page);
    
    // 6. Return pointer to node within page
    return (art_node_t *)(page->data + ref.offset);
}
```

### Insert with WAL (data_art.c)
```c
bool data_art_insert(data_art_tree_t *tree, const uint8_t *key, 
                     size_t key_len, const void *value, size_t value_len) {
    // 1. Write WAL entry: INSERT(key, value)
    wal_append(tree->wal, WAL_INSERT, key, key_len, value, value_len);
    
    // 2. Traverse tree, load pages via buffer pool (uses in-memory ART)
    // 3. Modify nodes in cache (mark dirty)
    node_ref_t new_root = insert_recursive_persistent(
        tree, tree->root, key, key_len, 0, value, value_len);
    
    tree->root = new_root;
    tree->size++;
    
    // 4. Commit: fsync(wal_fd)
    wal_sync(tree->wal);
    
    return true;
}
```

### Buffer Pool Lookup (buffer_pool.c)
```c
cached_page_t *buffer_pool_get(buffer_pool_t *pool, uint64_t page_id,
                                uint64_t version) {
    // Encode page_id for ART key
    uint8_t key[8];
    encode_page_id(page_id, key);
    
    // Read lock for lookup (allows concurrent readers)
    pthread_rwlock_rdlock(&pool->index_lock);
    
    size_t val_len;
    cached_page_t **page_ptr = art_get(&pool->page_index, key, 8, &val_len);
    
    if (page_ptr) {
        cached_page_t *page = *page_ptr;
        
        // Check if page version matches requested version
        if (page->version == version) {
            // Cache hit
            page->pin_count++;  // Atomic increment
            pthread_rwlock_unlock(&pool->index_lock);
            
            // Update LRU (separate lock)
            pthread_mutex_lock(&pool->lru_lock);
            lru_touch(pool, page);
            pthread_mutex_unlock(&pool->lru_lock);
            
            return page;
        }
    }
    
    pthread_rwlock_unlock(&pool->index_lock);
    
    // Cache miss or version mismatch - need to load
    // Acquire write lock to insert
    pthread_rwlock_wrlock(&pool->index_lock);
    
    // Check again (another thread may have loaded it)
    page_ptr = art_get(&pool->page_index, key, 8, &val_len);
    if (page_ptr && (*page_ptr)->version == version) {
        cached_page_t *page = *page_ptr;
        page->pin_count++;
        pthread_rwlock_unlock(&pool->index_lock);
        return page;
    }
    
    // Evict if full
    if (pool->size >= pool->capacity) {
        buffer_pool_evict_lru(pool);
    }
    
    // Load from disk
    cached_page_t *page = page_manager_load(pool->pm, page_id, version);
    page->pin_count = 1;
    
    // Insert into cache (in-memory ART)
    art_insert(&pool->page_index, key, 8, &page, sizeof(page));
    
    pthread_mutex_lock(&pool->lru_lock);
    lru_add_head(pool, page);
    pthread_mutex_unlock(&pool->lru_lock);
    
    pool->size++;
    
    pthread_rwlock_unlock(&pool->index_lock);
    
    return page;
}
```

## Configuration

```c
typedef struct {
    size_t cache_size_mb;      // Default: 1024 (1GB)
    size_t wal_segment_size_mb; // Default: 256
    uint32_t checkpoint_interval_sec; // Default: 300 (5 min)
    const char *data_dir;      // Database directory
} art_config_t;
```

## Implementation Phases

### Phase 0: Preserve In-Memory Implementation ✅
- [x] Keep `art.h` / `art.c` unchanged and fully functional
- [x] All 627 tests continue passing
- [x] Serves as baseline for benchmarking

### Phase 1: Core Infrastructure
- [ ] Create `data_art.h` with persistent API
- [ ] Implement `page_manager.h` / `page_manager.c` (page allocator, I/O)
- [ ] Implement `buffer_pool.h` / `buffer_pool.c` (using in-memory ART)
- [ ] Design node serialization format
- [ ] Add page ID encoding utilities

**Milestone**: Can allocate pages, cache them, and evict LRU

### Phase 2: WAL System
- [ ] Implement `wal.h` / `wal.c` (write-ahead log)
- [ ] WAL writer and reader
- [ ] Log entry encoding/decoding (INSERT/UPDATE/DELETE)
- [ ] Checkpoint mechanism
- [ ] Recovery procedure (replay from checkpoint)
- [ ] Version tracking in WAL entries

**Milestone**: Can write operations to WAL and recover from crash

### Phase 3: MVCC & Concurrency
- [ ] Implement version manager (version_manager.h/c)
- [ ] Copy-on-write page allocation
- [ ] Snapshot creation and lifecycle
- [ ] Reference counting for versions
- [ ] Garbage collection for old versions
- [ ] Thread-safe buffer pool operations
- [ ] Read-write locks for concurrent access

**Milestone**: Multiple readers can access different snapshots concurrently

### Phase 4: Persistent ART Integration
- [ ] Implement `data_art.c` based on `art.c` structure
- [ ] Convert `alloc_node()` → `alloc_node_persistent()` with CoW
- [ ] Convert pointer-based traversal → `node_ref_t` with buffer pool
- [ ] Implement `data_art_begin_read()` / `data_art_end_read()`
- [ ] Implement `data_art_begin_write()` / `data_art_commit_write()`
- [ ] Implement `data_art_get_snapshot()` (read from specific version)
- [ ] Implement `data_art_insert_txn()` with CoW and WAL
- [ ] Implement `data_art_delete_txn()` with CoW and WAL

**Milestone**: Can insert/get/delete with full persistence, crash safety, and concurrent reads

### Phase 5: Testing & Validation
- [ ] Create `test_page_manager.c`
- [ ] Create `test_buffer_pool.c` (verify ART-based cache, concurrent access)
- [ ] Create `test_version_manager.c` (MVCC operations)
- [ ] Create `test_data_art.c` (persistent operations)
- [ ] Add concurrent reader tests (multiple snapshots)
- [ ] Add crash/recovery tests (kill mid-write, verify state)
- [ ] Add version GC tests (verify old versions cleaned up)
- [ ] Port existing `test_art.c` test cases to persistent version
- [ ] Benchmark: in-memory vs persistent (measure overhead)
- [ ] Benchmark: concurrent reads (scalability with multiple threads)

**Milestone**: All tests passing, crash recovery verified, concurrent reads working

### Phase 6: Optimization
- [ ] Prefetching for sequential access patterns
- [ ] Page compression for cold data
- [ ] Background writer thread for dirty pages
- [ ] Background GC thread for old versions
- [ ] Adaptive cache sizing based on hit rate
- [ ] Batch commits (group multiple operations)
- [ ] Node packing within pages (reduce fragmentation)
- [ ] Fine-grained locking (per-page locks vs global locks)

**Milestone**: Production-ready performance (<5% overhead on cache hits, linear scaling with readers)

## Performance Targets

- **Hot path (cache hit)**: <5% overhead vs in-memory ART
- **Cold read (disk load)**: ~10-20µs per page (SSD)
- **Write amplification**: <3x (WAL + page writes)
- **Cache efficiency**: >95% hit rate for typical workload
- **Buffer pool lookup**: O(log n) via in-memory ART (proven faster than hash table)
- **Sustained throughput**: >100k ops/sec with 90% cache hits
- **Concurrent read scalability**: Near-linear scaling (8 readers → ~8x throughput)
- **MVCC overhead**: <1µs for snapshot creation, <10µs for version GC
- **Reader latency**: Zero blocking (readers never wait for writers)

## Design Advantages

### Using In-Memory ART for Buffer Pool
1. **Proven Performance**: Benchmarked faster than uthash
2. **Sequential Page IDs**: Natural fit for ART's prefix compression
3. **Zero Hash Overhead**: No hash function computation
4. **Ordered Iteration**: Useful for sequential prefetch
5. **No Resize Spikes**: Incremental growth vs hash table rehashing
6. **Self-Hosting**: ART managing ART is elegant and efficient
7. **Thread-Safe**: Read-write locks allow concurrent readers

### MVCC Snapshot Isolation
1. **Non-Blocking Reads**: Readers never block writers or other readers
2. **Consistent Snapshots**: Each reader sees stable view at specific version
3. **Perfect for Ethereum**: RPC workers can query historical state while sync continues
4. **Copy-on-Write**: Natural fit with page-based storage
5. **Garbage Collection**: Old versions cleaned up when no longer needed
6. **Scalable**: Linear scaling with number of reader threads

### Separation of Concerns
1. **In-Memory ART**: Simple, fast, battle-tested (627 tests)
2. **Persistent ART**: Complex but isolated, can iterate independently
3. **Clear Interface**: Users choose based on needs (speed vs durability)
4. **Gradual Migration**: Can develop persistent version without touching working code
5. **Reusability**: In-memory ART serves as cache infrastructure

## Use Cases

### Ethereum Node Scenarios

**Multiple RPC Workers**:
```c
// Worker 1: eth_getBalance at block 1000
snapshot_1 = data_art_begin_read(state_tree);  // Version 1000
balance_1 = data_art_get_snapshot(snapshot_1, addr1);

// Worker 2: eth_getStorageAt at block 1001 (concurrent)
snapshot_2 = data_art_begin_read(state_tree);  // Version 1001
storage_2 = data_art_get_snapshot(snapshot_2, slot);

// Worker 3: eth_call at block 999 (concurrent, historical)
snapshot_3 = data_art_begin_read_at_version(state_tree, 999);
result_3 = execute_call(snapshot_3);

// Sync worker: Processing block 1002 (concurrent)
txn = data_art_begin_write(state_tree);
data_art_insert_txn(txn, new_account, ...);
data_art_commit_write(txn);  // Creates version 1002
```

All operations happen **simultaneously** without blocking!

## Testing Strategy

### 1. In-Memory Baseline (art.c)
- **Status**: ✅ 627 tests passing
- **Purpose**: Performance baseline and correctness reference
- **Tests**: `test_art.c` (unchanged)

### 2. Buffer Pool Tests (test_buffer_pool.c)
- ART-based page cache lookup performance
- LRU eviction correctness
- Page pinning during operations
- Cache hit/miss statistics
- Concurrent access (if multi-threaded)

### 3. Page Manager Tests (test_page_manager.c)
- Page allocation and freeing
- Page I/O (write/read/verify)
- Multi-file management (when > 1GB)
- Free space tracking

### 4. WAL Tests (test_wal.c)
- Append log entries
- Replay from checkpoint
- Truncate after checkpoint
- Corruption detection (checksums)

### 5. Persistent ART Tests (test_data_art.c)
- Port all `test_art.c` test cases to persistent version
- Insert/get/delete with persistence
- Node growth/shrink with page allocation
- Iteration over persistent tree
- Large dataset tests (millions of entries)
- **Concurrent reader tests**:
  - Multiple snapshots at same version
  - Multiple snapshots at different versions
  - Reader while writer is committing
  - Verify snapshot isolation (readers don't see uncommitted writes)

### 6. MVCC & Concurrency Tests (test_mvcc.c)
- Snapshot creation and destruction
- Version reference counting
- Copy-on-write verification
- Garbage collection of old versions
- Concurrent readers (10+ threads reading simultaneously)
- Reader-writer isolation (readers don't block writers)
- Version visibility (readers see consistent snapshot)
- Stress test: rapid snapshot creation/destruction

### 6. Crash Recovery Tests
- Kill process mid-insert → verify recovery
- Kill process mid-delete → verify recovery
- Corrupt page → verify detection
- Incomplete WAL entry → verify handling
- Power loss simulation (sync points)
- **Verify snapshots survive crash** (old versions recoverable)

### 7. Performance Benchmarks
```c
// Compare in-memory vs persistent
benchmark_insert_10k(art_tree);      // Baseline
benchmark_insert_10k(data_art_tree); // Measure overhead

// Cache efficiency
benchmark_sequential_access();  // Should have high hit rate
benchmark_random_access();      // Stress test eviction
benchmark_working_set();        // Typical Ethereum access pattern

// Concurrent read scalability
benchmark_concurrent_readers(1);   // Single reader baseline
benchmark_concurrent_readers(4);   // 4 readers (should be ~4x throughput)
benchmark_concurrent_readers(8);   // 8 readers (should be ~8x throughput)
benchmark_concurrent_readers(16);  // 16 readers (test lock contention)

// MVCC overhead
benchmark_snapshot_create_destroy(); // Snapshot lifecycle cost
benchmark_version_gc();              // GC performance
```

### 8. Scale Tests
- Load 1M entries → measure memory usage
- Load 10M entries → verify cache behavior
- Load 100M entries → test multi-file handling
- Measure sustained insert/get/delete throughput
