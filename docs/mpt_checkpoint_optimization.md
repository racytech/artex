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
- Account root computation: **9%** (214ms) - fast, not a bottleneck
- Flush (disk writes + fsync): **3.6s** - proportional to dirty nodes
- Cache hit rate: 83% (17% disk misses)

## Root Cause

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

## Optimization Plan

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

## Flush Optimization (separate from root computation)

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

## Priority Order

1. Multi-account batching (#1) — biggest single win
2. Skip redundant qsort (#2) — easy, compounds with #1
3. Deferred buffer scaling (#3) — simple change, good ROI
4. Path cache (#4) — reduces disk I/O during trie walk
5. Vectorized comparison (#5) — small gain, easy to do
