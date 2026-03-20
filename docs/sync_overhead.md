# Synchronous I/O and Lock Overhead in Chain Replay

All blocking sync calls and lock overhead identified in the hot path.
None of these are needed — the OS page cache handles writeback, and
chain_replay is single-threaded.

## Blocking calls every 256 blocks (checkpoint)

### 1. code_store_flush → code_store_sync

```
sync_flush_and_evict()
  → code_store_flush()          [sync/src/sync.c:653]
    → code_store_sync()         [database/src/code_store.c:623]
      → msync(MS_SYNC)          [database/src/code_store.c:615]  ← BLOCKS
      → disk_hash_sync()        [database/src/code_store.c:616]
        → msync(MS_SYNC)        [database/src/disk_hash_mmap.c:758]  ← BLOCKS
```

Fix: make `code_store_flush` just call `write_data_header`, skip sync.

### 2. write_free_overflow → fsync (x2, account + storage MPT)

```
sync_flush_and_evict()
  → evm_state_flush()           [sync/src/sync.c:655]
    → mpt_store_flush()         [evm/src/evm_state.c]
      → write_header_dat()      [database/src/mpt_store.c:1505]
        → write_free_overflow() [database/src/mpt_store.c:429]
          → fsync(fd)           [database/src/mpt_store.c:351]  ← BLOCKS
```

Called for both account_mpt and storage_mpt = 2x fsync per checkpoint.

Fix: remove `fsync(fd)` from `write_free_overflow`. The `close(fd)` flushes
to page cache, and the OS writes to disk in the background.

## Lock overhead on every disk_hash operation

### 3. pthread_rwlock on every get/put

```
disk_hash_get()   → pthread_rwlock_rdlock  [disk_hash_mmap.c:558]
disk_hash_put()   → pthread_rwlock_wrlock  [disk_hash_mmap.c:566]
disk_hash_delete()→ pthread_rwlock_wrlock  [disk_hash_mmap.c:574]
disk_hash_contains()→ pthread_rwlock_rdlock [disk_hash_mmap.c:582]
disk_hash_batch_get()→ pthread_rwlock_rdlock [disk_hash_mmap.c:613]
disk_hash_batch_put()→ pthread_rwlock_wrlock [disk_hash_mmap.c:680]
disk_hash_sync()  → pthread_rwlock_wrlock  [disk_hash_mmap.c:755]
```

Each acquire/release is ~20-40ns. During a 256-block window, millions of
disk_hash ops fire (MPT trie walks = 5-10 gets per state access). At 20ns
per lock, 10M ops = ~200ms of pure lock overhead per checkpoint window.

Fix: add a single-threaded mode flag to disk_hash that bypasses rwlock.
Chain_replay is single-threaded — the locks protect against concurrent
access that never happens.

```c
// Proposed API:
void disk_hash_set_single_threaded(disk_hash_t *dh, bool single);
```

When set, get/put/delete skip pthread_rwlock entirely.

## Not blocking (confirmed safe)

- `mpt_store_flush` — no msync, writes to mmap only [mpt_store.c:1504]
- `flat_state_sync` — flat_state is disabled (NULL) in chain_replay
- `state_history fdatasync` — runs in background thread, not main thread
- `disk_hash recover msync` — one-time at open, not hot path
- `verkle_state_sync` — verkle disabled in current config

## Cleanup TODO

- Remove flat_state from codebase entirely (disabled, stale, confusing)
- Remove state_history background thread fdatasync calls (not needed)
- Consider removing disk_hash_sync() function entirely — if no caller
  needs durable writes, the function is dead code
