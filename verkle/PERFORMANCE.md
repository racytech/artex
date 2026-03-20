# Verkle Backend Performance Notes

## Current Bottlenecks

### 1. Blocking msync in `verkle_flat_sync()`

`verkle_flat_sync()` calls `disk_hash_sync()` on all 4 stores sequentially.
Each `disk_hash_sync()` does `msync(MS_SYNC)` — blocks until all dirty pages
are physically written. Total cost: 100-600ms per sync call.

```
verkle_flat_sync()
  → disk_hash_sync(value_store)      // MS_SYNC — blocks
  → vcs_sync(commit_store)
      → disk_hash_sync(leaf_store)   // MS_SYNC — blocks
      → disk_hash_sync(internal_store) // MS_SYNC — blocks
  → disk_hash_sync(slot_store)       // MS_SYNC — blocks
```

**Fix**: Remove msync entirely. The MPT backend already skips sync in
`mpt_store_flush()` and relies on OS page cache writeback. Verkle should
do the same. Crash recovery uses `.meta` checkpoint — no need for durable
writes on every flush.

### 2. pthread_rwlock on every disk_hash operation

Every `disk_hash_get()` takes a read lock, every `disk_hash_put()` takes a
write lock. Chain replay is single-threaded — the lock overhead is pure waste.

**Fix**: Add a lock-free mode (`disk_hash_set_single_threaded(dh)`) that
bypasses rwlock acquisition. Or compile-time flag to disable locks.

### 3. Sequential store syncs (vcs_sync)

`vcs_sync()` syncs leaf_store then internal_store in series.
If sync is needed at all, these could run in parallel (separate files).

**Fix**: If we keep sync, use pthreads to sync stores in parallel.

### 4. Random page access during commit_block

`verkle_flat_commit_block()` does hash-based lookups across all 4 stores
per stem change. Bucket IDs are random → scattered page access → TLB misses.

At scale (100M+ entries), the mmap'd region spans hundreds of GB.
Working set exceeds TLB coverage, causing kernel page table walks.

**Fix**: Batch operations by bucket locality. Use `madvise(MADV_SEQUENTIAL)`
during bulk operations. Consider explicit prefetch for known access patterns.

## Optimization Priority

1. **Remove msync** — zero code change in verkle_flat, just remove sync calls
   or make `disk_hash_sync` a no-op. Biggest win for least effort.
2. **Remove rwlock for single-threaded use** — moderate effort, good perf win.
3. **Parallel store sync** — only matters if we keep sync at all.
4. **Page locality** — harder, profile-driven optimization for scale.

## Reference

- MPT backend already skips sync: `mpt_store_flush()` writes to mmap, no msync.
- `sync_flush_and_evict()` in `sync/src/sync.c` is where flush is triggered.
- `verkle_state_sync()` is called from `sync_flush_and_evict()` after
  `evm_state_flush_verkle()`.
