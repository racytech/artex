# Storage Architecture: Evolution and Design

## Current Architecture (db-dev-2/3): mmap Storage

Single memory-mapped file with shadow-paging crash safety.

```
insert(key, value)
  → direct pointer write to mmap'd page     O(1), zero-copy
  → mark page dirty in bitmap

read(key)
  → atomic load committed_root               lock-free
  → traverse ART via pointer dereference     zero-copy, no syscall

checkpoint
  → msync dirty pages → fdatasync
  → write new root to shadow header slot
  → fdatasync header page → flip active slot

crash recovery
  → open file, pick header slot with highest valid checkpoint_num
  → tree is immediately consistent (shadow paging)
```

**Strengths:**
- Zero-copy reads (pointer dereference, no `pread` syscall)
- No compression/decompression overhead
- No buffer pool lock contention
- O(1) page access (base + page_id * 4096)
- Simple crash safety via shadow paging

**Weakness:**
- Kernel controls page eviction (LRU) — no way to prioritize hot pages
- When DB > RAM, random ART traversal causes ~12 page faults per insert
- Throughput collapses from 80 Kkeys/s to 15-20 Kkeys/s

### Key files

| File | Role |
|------|------|
| `include/mmap_storage.h` | Storage API, page access, checkpoint |
| `src/mmap_storage.c` | mmap lifecycle, growth, shadow paging |
| `src/data_art_core.c` | Tree ops, slot allocator, mlock |
| `src/data_art_compact.c` | Online compaction (DFS relocation + truncate) |

---

## Previous Architecture (db-dev-0): Page Manager + Buffer Pool + WAL

Explicit I/O with write-ahead logging.

```
insert(key, value)
  → wal_log_insert(txn_id, key, value)       append to WAL segment
  → buffer_pool_get_pinned(page_id)           LRU lookup or pread()
  → modify page in buffer pool                mark dirty
  → wal_log_commit_txn(txn_id)               append + FSYNC (blocks!)

read(key)
  → buffer_pool_get(page_id)                  LRU hit? return cached
  → on miss: page_manager_read(page_id)       pread + decompress
  → add to buffer pool, evict LRU tail

checkpoint (background thread, every 256 MB WAL or 10 min)
  → buffer_pool_flush_all()                   write all dirty pages
  → page_manager_sync()                       fsync all data files
  → wal_log_checkpoint(root, size)            record tree state + fsync
  → wal_truncate()                            delete old segments

crash recovery
  → wal_replay(start_lsn, end_lsn)           two-pass:
    1. scan for BEGIN/COMMIT → find uncommitted txns
    2. replay committed ops, skip aborted
```

### Components

**Buffer Pool** (`buffer_pool.c`)
- Fixed 10,000 frames = 40 MB (way too small)
- uthash for O(1) lookup by page_id
- Doubly-linked LRU list for eviction
- Pin/unpin reference counting
- Single pthread_rwlock for entire pool

**Page Manager** (`page_manager.c`, 1,661 lines)
- Append-only: pages written to end of current data file
- Multi-file: 512 MB segments, auto-creates new files
- LZ4/ZSTD compression on every write (CPU overhead)
- Binary search page index: page_id → (file_idx, offset)
- Dead page marking for deferred GC

**WAL** (`wal.c`)
- 64 MB segments, 256 KB write buffer
- Entry types: INSERT, DELETE, BEGIN_TXN, COMMIT_TXN, ABORT_TXN, CHECKPOINT
- fsync-on-commit: every commit blocks until WAL entry is durable
- Two-pass replay: identifies uncommitted txns, skips their operations
- Checkpoint triggers: 256 MB WAL size, 600s elapsed, or 50K dirty pages

**Checkpoint Manager** (`checkpoint_manager.c`)
- Background thread polling every 1 second
- Coordinates: flush buffer pool → sync pages → WAL checkpoint → truncate

### Why it was slow

| Bottleneck | Impact | Root cause |
|-----------|--------|------------|
| 40 MB buffer pool | Constant eviction thrashing | 10K frames for GB-scale data |
| LZ4 on every write | CPU-bound hot path | Compression before page flush |
| fsync per commit | Serialized commits | No group commit |
| Single RWlock | All ops serialize | Buffer pool lock contention |
| Binary search index | O(log n) per read | vs mmap's O(1) dereference |
| pread + decompress | Syscall + CPU per miss | vs mmap's zero-copy |

### Key insight

The buffer pool was 100x too small. At 40 MB, any dataset beyond trivial
size thrashes the LRU. A 4-8 GB buffer pool with no compression on the
hot path would have performed much better.

---

## Future Architecture: Blockchain-is-Your-WAL

For Ethereum state storage, we can eliminate the WAL entirely because
the blockchain provides the same guarantees.

### Why a WAL exists

A WAL ensures that committed transactions survive crashes. The sequence:
1. Append operation to WAL → fsync (durable)
2. Apply to in-memory pages (fast, volatile)
3. Periodically checkpoint: flush pages → fsync → truncate WAL

If we crash between steps 2 and 3, replay the WAL to reconstruct state.

### Why Ethereum doesn't need one

Ethereum blocks are **deterministic state transitions**:
- Block N + state S → execute → state S'
- Same input always produces same output
- The blockchain is a permanent, immutable, ordered log of all transitions
- Any node can re-derive state by re-executing blocks from genesis (or checkpoint)

The blockchain IS a WAL — it's an append-only, ordered, immutable log of
every state change, stored and replicated across thousands of nodes.

### Architecture

```
Block N arrives from consensus
  ↓
Apply to in-memory write buffer (hash map or small ART)
  ↓  ← writes are memory-speed, never touch disk
  ↓
Every K blocks (e.g., 100 = ~20 min of chain):
  ↓
  Sorted merge: buffer → persistent ART → checkpoint
  ↓  ← sequential I/O, amortized over K blocks
  ↓
On crash:
  ↓
  Recover from last checkpoint (shadow paging, instant)
  Re-execute K blocks from chain (deterministic, seconds)
  Done.
```

### What this eliminates

| Traditional DB | Ethereum state DB |
|---------------|-------------------|
| WAL append per write | Not needed — blockchain is the log |
| fsync per commit | Not needed — buffer is volatile by design |
| WAL replay on crash | Re-execute blocks from chain instead |
| WAL segment management | Not needed |
| Two-phase commit | Not needed — single writer, deterministic |

### ACID properties

**Atomicity**: Each block's state changes are applied atomically to the
buffer (pointer flip). Merge to disk uses shadow paging (atomic root swap).

**Consistency**: Buffer version takes precedence over disk. Readers see
either all of a block's changes or none.

**Isolation**: Single writer (block executor). Readers see committed state.
No MVCC needed — the committed_root atomic gives point-in-time consistency.

**Durability**: At checkpoint granularity. Between checkpoints, state lives
in volatile buffer. Crash = re-execute K blocks from chain.

Recovery cost at K=100 blocks:
- ~20 minutes of chain time
- Re-execution takes seconds (state is in page cache from previous run)
- Acceptable for all production use cases

### Comparison: WAL vs blockchain-as-WAL

```
Traditional (db-dev-0):
  write → WAL append (fsync!) → buffer pool → checkpoint → disk
  Latency: ~1ms per commit (fsync dominates)

Blockchain-as-WAL:
  write → memory buffer → (nothing until checkpoint)
  Latency: ~100ns per write (memory only)
  10,000x faster per-operation
```

### Implementation plan

This architecture maps directly to Option A in WRITE_SCALABILITY.md:

1. **Write buffer**: In-memory hash map, key[32] → (value, len, tombstone)
2. **Read path**: Check buffer first, fall through to persistent ART
3. **Merge**: Every K blocks, sort buffer keys, walk persistent ART in order,
   apply inserts/deletes sequentially (good page locality)
4. **Checkpoint**: After merge, shadow-page flip + fsync. Clear buffer.
5. **Crash recovery**: Open last checkpoint. Ask consensus layer for blocks
   since checkpoint. Re-execute them. Done.

No WAL. No fsync per commit. No compression on hot path. No buffer pool
eviction. Writes are always memory-speed.

---

## Summary: Architecture Evolution

```
db-dev-0 (explicit I/O)     db-dev-2/3 (mmap)        Future (write buffer)
┌─────────────────┐         ┌─────────────────┐      ┌─────────────────┐
│   WAL (fsync!)  │         │                 │      │  Write Buffer   │
│   Buffer Pool   │         │   mmap file     │      │  (in-memory)    │
│   Page Manager  │         │   shadow paging │      ├─────────────────┤
│   Compression   │         │   zero-copy     │      │  Persistent ART │
│   Checkpoint Mgr│         │                 │      │  (mmap, as-is)  │
└─────────────────┘         └─────────────────┘      └─────────────────┘
Slow: fsync, LZ4,           Fast in RAM,              Fast always:
40MB buffer, locks           slow when DB > RAM        writes = memory
```
