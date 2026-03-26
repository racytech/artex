# madvise Page Cache Optimization

## Problem

During chain_replay, MPT root computation degrades from ~0.4s to ~28s
over time. After restarting the process, performance returns to 0.4s.

## Observed at Block 4,748,800

```
Before restart (cold page cache):
  root: stor=18596.6 ms  acct=10160.5 ms  total=28757.0 ms
  stor: load=3119.0 ms (cache=53098 disk=26358)
  acct: load=10056.7 ms (cache=434 disk=88772)
  RSS: 76203 MB

After restart (warm page cache):
  root: stor=261.5 ms  acct=142.6 ms  total=404.1 ms
  stor: load=144.2 ms (cache=77865 disk=1262)
  acct: load=46.1 ms (cache=483 disk=96161 — served from OS page cache)
```

70x improvement just from restarting the process.

## Root Cause

At 76GB RSS, OS page cache is under memory pressure. Over time, stale
mmap pages from old checkpoint regions accumulate. The kernel evicts
useful pages to keep stale ones, causing real disk I/O on subsequent
node loads.

After restart, mmap regions are fresh. Only the current working set
is in page cache. No stale pages competing for RAM.

## Solution

After each checkpoint commit, tell the kernel to drop pages we won't
need again:

```c
#include <sys/mman.h>

// After checkpoint commit, release stale mmap regions
void release_stale_pages(mpt_store_t *store) {
    // Option 1: Drop all pages, let OS re-fault on demand
    madvise(store->mmap_ptr, store->file_size, MADV_DONTNEED);

    // Option 2: More targeted — only drop pages before current offset
    // (keeping recently written nodes hot)
    madvise(store->mmap_ptr, store->checkpoint_offset, MADV_DONTNEED);
}
```

Alternative: `posix_fadvise(fd, 0, offset, POSIX_FADV_DONTNEED)` for
file-descriptor based approach (works without mmap).

## Where to Apply

- `database/src/mpt_store.c` — after `mpt_store_commit()` or at
  checkpoint boundaries
- `database/src/disk_table.c` — after index rebuilds or compaction
- Both account and storage stores

## Expected Impact

Keep root computation closer to 0.4s steady-state instead of
degrading to 28s. Effectively simulates the restart benefit
continuously without stopping the process.

## Risks

- `MADV_DONTNEED` drops dirty pages on private mappings (not an
  issue for our read-only mmap or MAP_SHARED with explicit writes)
- If we DONTNEED pages we'll read again soon, we cause unnecessary
  re-faults. Solution: only DONTNEED regions older than the current
  checkpoint window.
- On systems with plenty of RAM, this may not help (kernel already
  manages pages well). Only matters under memory pressure.

## Testing

1. Run chain_replay normally, observe root times degrading
2. Add madvise after checkpoint, observe if root times stay low
3. Compare RSS before/after (should decrease after DONTNEED calls)
