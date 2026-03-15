# MPT Checkpoint Optimization

## Current Performance (block ~4.27M, 256-block checkpoint)

```
storage_roots: 2143 ms, 22K dirty slots, 16K dirty accts
mpt_root:      storage=2143 ms, account=214 ms, total=2358 ms
flush:         3551 ms
evict:         1 ms
------
total:         ~7.5s per checkpoint
```

- Storage root computation: **91%** of root time (2.1s / 2.4s)
- Account root computation: **9%** (214ms) — fast, not a bottleneck
- Flush (disk writes + fsync): **3.6s** — proportional to dirty nodes
- Cache hit rate: 83% (17% disk misses)

## Current Sequential Flow

```
[execute 256 blocks] → [compute root] → [flush+fsync] → [evict cache]
       ~10s                 ~2.4s            ~3.6s          ~1ms
```

Total checkpoint wall time: ~16s per 256 blocks (10s exec + 5.5s checkpoint + overhead).

## Part 1: Root Computation Improvements

### Root Cause

`compute_all_storage_roots()` processes **16K accounts individually**:

```
for each dirty account (16K):
    mpt_store_set_root(storage_mpt, account.storage_root)
    mpt_store_begin_batch(storage_mpt)
    for each dirty slot in this account (~1.4 avg):
        mpt_store_update(storage_mpt, slot_hash, value)
    mpt_store_commit_batch(storage_mpt)   // <-- qsort + trie walk + rehash
    mpt_store_root(storage_mpt, &account.storage_root)
```

Each `commit_batch` does: qsort dirty entries, dedup, recursive `update_subtrie`
(loads nodes from cache/disk, rehashes every ancestor up to root).
With 16K cycles, even small per-cycle overhead compounds.

### 1. Multi-account batch processing (30-40% speedup, high effort)

Eliminate 16K separate `begin_batch`/`commit_batch` cycles. Instead:
- Accept all (account, slot_hash, value) tuples at once
- Group by account internally
- Process each account's trie in sequence but with shared setup/teardown
- Amortize sort, arena allocation, batch state management

Key change: new `mpt_store_commit_multi()` that takes pre-grouped entries
and iterates account roots internally, avoiding 16K begin/commit cycles.

Files: `mpt_store.c`, `evm_state.c:compute_all_storage_roots()`

### 2. Skip qsort for pre-sorted entries (10-15% speedup, medium effort)

`commit_batch` always qsorts the dirty array. But in `compute_all_storage_roots`,
entries within each account group are often already in order (slot hashes are
keccak256 outputs, inserted sequentially). Add an `is_sorted` flag or check
to skip the sort when entries are already ordered.

Alternatively, maintain sorted insertion during `mpt_store_update()` via
binary search insert — O(N) total for small N per account.

Files: `mpt_store.c:commit_batch()` (line ~2304)

### 3. Deferred buffer scaling (5-10% speedup, medium effort)

The deferred write buffer uses 4096 hash buckets but can grow to 100K+
entries during a checkpoint. Average chain length ~24 degrades lookup.
Bump to 65536 buckets (power of 2, use mask instead of modulo).

Files: `mpt_store.c` — `DEFERRED_BUCKETS` (line ~40), `def_bucket()` (line ~932)

### 4. Per-batch node path cache (8-12% speedup, medium effort)

During `update_subtrie`, the same intermediate nodes get loaded multiple
times (descend to find insertion point, then re-read siblings for rehashing).
A temporary map of (hash -> decoded node) within a single `commit_batch`
would eliminate redundant `load_from_ref` + `decode_node` calls.

Files: `mpt_store.c:update_subtrie()`, `load_from_ref()`

### 5. Extension/prefix comparison vectorization (5-10% speedup, low effort)

`merge_extension` and `build_fresh` do O(N*M) nibble-by-nibble comparisons.
Use SSE to compare 16 nibbles at a time. Already compiling with `-msse4.2`.

Files: `mpt_store.c:merge_extension()` (line ~2077), `build_fresh()` (line ~1793)

## Part 2: Flush Optimization

### Current flush path
```
mpt_store_flush():
    1. Sort deferred entries by disk offset (sequential I/O)
    2. pwrite each entry to data file
    3. disk_hash_put for each index entry
    4. Process pending deletes (refcount decrement)
    5. fsync data file + disk_hash_sync index
```

### Potential improvements
- **Batch pwrite**: coalesce adjacent writes into single pwrite calls
- **Async fsync**: fsync on a background thread (risk: crash before sync)
- **Write-ahead buffer**: accumulate writes in a large buffer, single write
- **Reduce dirty nodes**: fewer rehashed nodes = fewer nodes to flush

## Part 3: Pipeline Root+Flush on a Separate Thread

### Goal

Overlap root+flush with the next batch of block execution:

```
Thread 1 (executor):  [exec batch N]──────[exec batch N+1]──────[exec batch N+2]
Thread 2 (root+flush):          [root+flush N]──────[root+flush N+1]
```

Effective speedup: root+flush (5.5s) runs in parallel with execution (10s),
so checkpoint overhead drops from 5.5s to near zero — just snapshot copy time.

### Data Conflict Analysis

Both threads access the same `mpt_store` instances:

```
                       Executor (next batch)          Root+Flush (prev batch)
                       ─────────────────────          ──────────────────────
account_mpt:           mpt_store_get (read-through)   begin_batch/update/commit
storage_mpt:           set_root + mpt_store_get       set_root/update/commit
dirty_accounts:        appending new entries           iterating + clearing
dirty_slots:           appending new entries           iterating + clearing
cached accounts:       read/write nonce,balance,etc    read nonce,balance,storage_root
evict_cache:           —                               destroys mem_art trees!
```

The executor calls `mpt_store_get` during normal execution on EVM cache miss
(ensure_account line ~311, ensure_slot line ~368). This conflicts with the
root thread which calls `mpt_store_update`/`commit_batch` on the same stores.

### Approach A1: Snapshot + Skip Eviction + Rare-Miss Mutex (recommended)

**Key insight**: if we don't evict the cache, the executor almost never needs
`mpt_store_get` because everything stays cached. At 95%+ hit rate, the few
remaining misses can simply wait on a mutex until the root thread finishes.

#### Checkpoint flow

```
checkpoint boundary:
    1. Wait for previous root+flush to complete (if still running)
    2. Snapshot dirty_accounts + dirty_slots values → root_job_t
       - For each dirty slot: copy (addr, slot_hash, current_value)
       - For each dirty account: copy (addr_hash, nonce, balance, code_hash,
         storage_root, flags, existed, created, self_destructed)
    3. Swap dirty lists — give old lists to root thread, init fresh ones
    4. Signal root thread with root_job
    5. Executor continues immediately with next 256 blocks

root thread (loop):
    1. Wait for root_job signal
    2. compute_all_storage_roots() from snapshot (owns mpt_store exclusively)
    3. compute account root from snapshot
    4. mpt_store_flush() — pwrite + fsync
    5. Do NOT evict cache
    6. Signal "done" (release mpt_store access)

executor on cache miss (rare):
    1. Try mpt_store_get
    2. If root thread is active → acquire mutex → block until root finishes
    3. Proceed with read-through
```

#### Why this works

- `mpt_store` is only accessed by the root thread during root+flush. The
  executor avoids it because the EVM cache (mem_art) holds everything hot.
- No eviction means the cache grows, but at block 4.27M it's only 13MB for
  20K accounts + 24K slots. Even at 1M+ accounts the overhead is manageable
  (RSS grows ~100-200MB, well within 6.4GB envelope).
- The rare cache miss (new account/slot never seen before) can block briefly.
  With 95%+ hit rate this happens <5% of reads, and only when the root thread
  is active (~2.4s out of every 16s window).
- The snapshot copy at checkpoint is O(dirty) — 22K slots * ~84 bytes + 16K
  accounts * ~120 bytes ≈ 3.8MB. A memcpy, <1ms.

#### Tricky parts

1. **storage_root feedback**: root computation produces new `storage_root`
   values for each account. The executor doesn't read `storage_root` during
   normal execution — it's only used at the NEXT checkpoint's root computation.
   So the root thread can write new roots back into cached accounts after
   completing, or store them in the snapshot and apply at the start of the
   next checkpoint (before the executor needs them).

2. **mpt_dirty / block_dirty flag management**: root computation clears
   `mpt_dirty` on processed entries. But the executor may have already
   re-dirtied those accounts in the new batch. Solution: the snapshot captures
   the dirty state; the root thread processes the snapshot without touching
   live flags. New dirty flags from the executor accumulate independently.

3. **No eviction → memory growth**: monitor RSS. If it becomes a problem,
   evict on the root thread AFTER flush completes, but use a reader-writer
   lock — executor holds read lock (allows concurrent cache reads), root
   thread acquires write lock only during eviction (brief, ~1ms).

4. **Crash safety**: if we crash during root+flush, the executor has already
   moved on to the next batch. On restart, we resume from the PREVIOUS
   checkpoint (which was fully flushed). The in-flight root+flush data is lost
   but that's fine — it hadn't been committed yet.

### Approach B: Double-Buffer MPT Stores (rejected)

Maintain two sets of `mpt_store` — executor reads from "front", root thread
writes to "back", swap after flush. Clean separation but doubles MPT memory
(~6GB per set) and disk handle count. Too expensive.

### Approach C: Queue-Based (viable alternative)

Same pattern as state_history — SPSC ring of `root_job_t`. Functionally
equivalent to A1 but with a ring buffer instead of a single-slot handoff.
Unnecessary complexity since we can only have one root+flush in flight
(they share `mpt_store`), so A1's single-slot design is sufficient.

## Priority Order

### Phase 1: Root computation improvements (no architectural change)
1. Multi-account batching (#1) — biggest single win
2. Skip redundant qsort (#2) — easy, compounds with #1
3. Deferred buffer scaling (#3) — simple change, good ROI
4. Path cache (#4) — reduces disk I/O during trie walk
5. Vectorized comparison (#5) — small gain, easy to do

### Phase 2: Pipeline root+flush on separate thread
6. Implement Approach A1 — snapshot + skip eviction + mutex

Phase 1 reduces root+flush time (currently 5.5s → target 2-3s).
Phase 2 hides the remaining cost by overlapping with execution.
Combined effect: checkpoint overhead goes from 5.5s to near zero.
