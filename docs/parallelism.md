# Parallelism Opportunities

Current chain replay pipeline is single-threaded:

```
[decode block] -> [execute txs] -> ... 256 blocks ... -> [compute MPT root] -> [flush] -> [evict] -> repeat
```

Baseline: 6161 blk/s at 1M blocks (single-threaded, no compaction).

## Dependency Graph

```
#1 Pipeline Block Decoding (independent — can be done first or in parallel)

#2 Background Flush (foundation — threading model, double-buffered deferred writes)
 └→ #3 Parallel Storage Root (needs #2's thread pool + concurrent mpt_store patterns)
      └→ #4 io_uring Async I/O (replaces blocking pwrite/pread in #2 and #3)

#5 Speculative Tx Execution (independent, very hard, research-stage)
```

## 1. Pipeline Block Decoding

**Difficulty:** Easy | **Impact:** Moderate

Read and RLP-decode the next era1 block in a producer thread while the main
thread executes the current block. Removes I/O latency from the hot path.

- Simple bounded queue (1-2 blocks ahead)
- No shared mutable state — producer only reads era1 files
- Main thread consumes decoded block structs

## 2. Background Flush (Option A — Double-Buffered Deferred Writes)

**Difficulty:** Medium | **Impact:** High

Eliminate the flush stall by double-buffering the deferred write path.
After MPT root computation, hand off current `def_entries` to a background
thread and give the main thread fresh empty buffers to continue executing.

### Current flow (blocking)

```
Batch N: [execute 256 blocks] → [compute root] → [flush (STALL)] → [evict] → next batch
```

### Proposed flow (pipelined)

```
Batch N:   [execute] → [compute root] → [rotate buffers + spawn flush thread]
                                              ↓                        ↓
                                    [main: execute batch N+1]   [bg: flush old entries]
                                              ↓                        ↓
Batch N+1: [execute] → [wait for bg flush] → [compute root] → [rotate + spawn] → ...
```

### Implementation

**Step 1 — Rotate deferred buffers** (`mpt_store_rotate_deferred`):
- Move current `def_entries`, `def_deletes`, `def_ht` into a `deferred_snapshot_t`
- Reset main store to fresh empty buffers and clean hash table
- The snapshot is immutable — safe for concurrent reads without locks

**Step 2 — Background thread flushes snapshot:**
- `pwrite` each snapshot entry's RLP to `.dat` at its allocated offset
- `disk_hash_put` each entry's hash → record into `.idx`
- Apply pending deletes
- `fsync` + `disk_hash_sync` + `write_header`
- Signal completion via condition variable

**Step 3 — Main thread reads through both buffers:**
- `load_node_rlp` lookup order: (1) new def_entries → (2) old snapshot → (3) disk
- Old snapshot is read-only, no lock needed
- After background flush completes, snapshot is freed

### Concurrency concerns

**`disk_hash` concurrent access:** The background thread calls `disk_hash_put`
while the main thread may call `disk_hash_get`/`disk_hash_contains`. Options:
- (a) Lock around disk_hash operations (simple, some contention)
- (b) Background thread only does `pwrite` to `.dat`, defers `disk_hash_put`
  until join time (zero contention, slightly more complex)
- (c) Use a separate shadow index for the background thread, merge on join

Option (b) is preferred — during the overlap window, main thread finds nodes
via the snapshot buffer (step 3 above), so it never needs the index entries
that haven't been written yet.

**File descriptor sharing:** `pwrite`/`pread` on the same fd from different
threads is safe on Linux (POSIX guarantees atomic offset-based I/O). No lock
needed for `.dat` file access.

### Synchronization

- One `pthread_mutex_t` + `pthread_cond_t` per flush job
- Before next `compute_mpt_root`, `pthread_cond_wait` until bg flush done
- After join: free snapshot, apply deferred `disk_hash_put` if using option (b)

### Expected gain

Flush takes ~5-15ms per checkpoint (256 blocks). At 6K blk/s, a batch takes
~42ms. Hiding the flush saves ~10-25% of checkpoint overhead. The gain
compounds with compaction (which also does I/O at checkpoint time).

## 3. Parallel Storage Root Computation (Forked Batch Contexts)

**Difficulty:** Medium | **Impact:** High (especially DoS-era blocks)

**Depends on:** #2 (same snapshot + thread-local buffer + merge pattern)

Each account's storage root is computed independently via
`set_root()` → `begin_batch()` → insert dirty slots → `commit_batch()`.
Currently these run sequentially on the shared `storage_mpt`. The expensive
part is `commit_batch` — trie walking and keccak hashing (CPU-bound).

### Current flow (sequential)

```
for each dirty account:
    storage_mpt.set_root(account.storage_root)
    storage_mpt.begin_batch()
    for each dirty slot: storage_mpt.update(slot_hash, value_rlp)
    storage_mpt.commit_batch()   ← CPU-intensive: trie walk + keccak
    account.storage_root = storage_mpt.root()
```

### Proposed flow (parallel)

Give each thread a lightweight "fork" of the store — shared read path
(disk + parent `def_entries`), thread-local write path (`def_entries`).

```
                    ┌─ Thread 0: accounts[0..N/4]     → local_def_entries_0
shared storage_mpt ─┼─ Thread 1: accounts[N/4..N/2]   → local_def_entries_1
  (read-only)       ├─ Thread 2: accounts[N/2..3N/4]  → local_def_entries_2
                    └─ Thread 3: accounts[3N/4..N]    → local_def_entries_3

After all threads join:
    merge local_def_entries_* → main def_entries
```

### Implementation

**Forked batch context** — lightweight per-thread view of the store:

```c
typedef struct {
    mpt_store_t *parent;          // shared store (read-only)
    deferred_entry_t *def_entries; // thread-local write buffer
    size_t def_count, def_cap;
    int def_ht[DEFERRED_BUCKETS]; // thread-local hash table
    uint8_t root_hash[32];        // current account's storage root
    uint64_t data_size;           // local allocation cursor
    free_list_t free_lists[NUM_SIZE_CLASSES]; // thread-local
} mpt_batch_ctx_t;
```

**load_node_rlp** in forked context checks:
1. Own `def_entries` (thread-local, just-written nodes)
2. Parent's `def_entries` (read-only, from previous batches)
3. Parent's disk (pread — safe, POSIX guarantees offset-based I/O)

**write_node** goes to thread-local `def_entries` only.

**Per-thread worker:**

```c
void *storage_root_worker(void *arg) {
    worker_job_t *job = arg;
    mpt_batch_ctx_t ctx;
    init_batch_ctx(&ctx, job->parent_store);

    for (size_t i = job->start; i < job->end; i++) {
        account_group_t *grp = &job->groups[i];
        memcpy(ctx.root_hash, grp->ca->storage_root.bytes, 32);
        batch_begin(&ctx);
        for (size_t j = grp->slot_start; j < grp->slot_end; j++)
            batch_update(&ctx, slots[j].hash, slots[j].rlp, slots[j].len);
        batch_commit(&ctx);  // trie walk + hash — CPU intensive
        memcpy(grp->ca->storage_root.bytes, ctx.root_hash, 32);
    }
    job->result_entries = ctx.def_entries;
    job->result_count = ctx.def_count;
}
```

**Merge phase** (single-threaded, after all workers join):

```c
for (int t = 0; t < N_THREADS; t++) {
    for (size_t i = 0; i < results[t].count; i++) {
        def_append(storage_mpt, &results[t].entries[i]);
    }
}
```

### Why this is safe

- **Reads are immutable** — parent's `def_entries` and disk don't change during
  computation (no concurrent writers to the parent store)
- **Writes are thread-local** — no locks needed
- **Node dedup** — `write_node` checks parent for existing hash before writing
- **Account groups are disjoint** — sorted by address, no two threads touch
  the same storage trie subtrees
- **pread is thread-safe** — POSIX guarantees atomic offset-based I/O on same fd

### Slot allocation concern

Each thread allocates data slots (offsets into `.dat`) for new nodes. These
must not overlap across threads. Options:
- (a) Pre-partition the data_size space: thread T gets range `[base + T*chunk, base + (T+1)*chunk)`
- (b) Use atomic fetch-add on shared `data_size` counter
- (c) Thread-local cursors, reconcile at merge time (relocate offsets)

Option (c) is simplest — each thread tracks its own `data_size` starting at 0.
During merge, remap offsets: `real_offset = main.data_size + thread_local_offset`.

### Relation to #2 and #4

- **#2 establishes the pattern**: snapshot + thread-local buffers + merge.
  `mpt_batch_ctx_t` is the same concept as #2's `deferred_snapshot_t`.
- **#4 amplifies gains**: multiple threads doing trie walks means many concurrent
  `pread` calls for cache misses. `io_uring` can batch these across threads
  into a single submission ring, reducing syscall overhead.

## 4. Async Disk I/O (io_uring)

**Difficulty:** Hard | **Impact:** High

**Depends on:** #2 (flush batching), #3 (parallel trie walk reads)

Replace synchronous `pread`/`pwrite` with io_uring for batched async I/O.
Linux-specific (already Linux-only codebase). Requires kernel 5.1+.

### Current I/O points

| Location | Syscall | When | Frequency |
|----------|---------|------|-----------|
| `load_node_rlp` (mpt_store.c:982) | `pread` | Trie walk cache miss | ~1000s/checkpoint |
| `mpt_store_flush` (mpt_store.c:1312) | `pwrite` | Flush def_entries | ~1000s/checkpoint |
| `disk_hash read_bucket` (disk_hash.c:112) | `pread` | Index lookup | ~1000s/checkpoint |
| `disk_hash write_bucket` (disk_hash.c:127) | `pwrite` | Index insert | ~1000s/checkpoint |

### Phase 1: Batched Flush Writes (easiest — pairs with #2)

During `mpt_store_flush`, all `def_entries` are independent writes to
different `.dat` offsets. Currently a loop of `pwrite` syscalls.

```c
// Current: N syscalls
for (size_t i = 0; i < count; i++)
    pwrite(fd, entries[i].rlp, entries[i].len, PAGE_SIZE + entries[i].offset);

// io_uring: 1 syscall for N writes
for (size_t i = 0; i < count; i++) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
    io_uring_prep_write(sqe, fd, entries[i].rlp, entries[i].len,
                        PAGE_SIZE + entries[i].offset);
}
io_uring_submit(&ring);       // single syscall
io_uring_wait_cqe_nr(&ring, cqes, count);  // wait for all
```

Same pattern for `disk_hash` bucket writes during flush. Combined with #2's
background flush thread, this makes the entire flush path async and batched.

**Expected gain:** Reduces ~2000 syscalls to ~2 (one submit, one wait).
NVMe drives handle batched writes much better than sequential ones.

### Phase 2: Prefetch Reads During Trie Walk (pairs with #3)

`load_node_rlp` is synchronous — the trie walk blocks on each node read.
Can't fully async this because each trie level depends on the previous.

**But:** when we decode a branch node, we know all 16 child hashes before
visiting them. We can prefetch children while processing siblings.

```c
// Decode branch node → get 16 child refs
for (int i = 0; i < 16; i++) {
    if (child[i].type == REF_HASH && !in_cache(child[i].hash)) {
        // Submit async pread for child's disk_hash lookup + .dat read
        prefetch_submit(&ring, child[i].hash);
    }
}
// By the time we recurse into child[0], children[1..15] are in flight
for (int i = 0; i < 16; i++) {
    if (child[i].type != REF_HASH) continue;
    prefetch_await(&ring, child[i].hash, buf);  // instant if prefetch done
    // recurse...
}
```

This is harder because it requires:
- A prefetch buffer (hash → pending I/O mapping)
- Two-stage index lookup: first `pread` the disk_hash bucket, extract offset,
  then `pread` the .dat node. Could submit both as a chain (io_uring linked SQEs).
- Integration with the node cache (don't prefetch what's already cached)

**With #3 (parallel workers):** Multiple threads doing trie walks means many
concurrent `pread` calls. A shared io_uring ring (or per-thread rings) can
batch these across threads. The NVMe queue depth (typically 32-128) is
utilized much better than sequential pread.

### Phase 3: Batched Index Lookups (hardest)

`disk_hash_get` does a `pread` per bucket probe. For chained buckets
(hash collisions), this is multiple sequential reads. io_uring could:

- Submit the first bucket read
- On completion, check if entry found or need next bucket
- Submit next bucket read if chained

This is a **dependent I/O chain** — each read depends on the previous result.
io_uring supports linked SQEs but only for a fixed chain length. For variable-
length hash chains, need a completion callback loop.

**Alternative:** Memory-map the index file (`mmap`) instead of `pread`. Let the
kernel's page cache handle readahead. Simpler than io_uring for random reads,
and the OS can batch page faults. Downside: less control over memory usage.

### Architecture

```
┌─────────────────────────────┐
│  io_uring instance          │
│  ┌─────────┐ ┌───────────┐ │
│  │   SQ    │ │    CQ     │ │
│  │(submits)│ │(completions│ │
│  └────┬────┘ └─────┬─────┘ │
│       │             │       │
│  ┌────▼─────────────▼────┐  │
│  │   NVMe driver         │  │
│  └───────────────────────┘  │
└─────────────────────────────┘
        ▲           ▲
        │           │
   flush thread  trie walk threads (#3)
   (Phase 1)     (Phase 2)
```

### Implementation order

1. **Phase 1** (pairs with #2): Batch flush writes. Simple, self-contained.
   Replace the pwrite loop in `mpt_store_flush` with io_uring submit+wait.
   Also batch `disk_hash` bucket writes.

2. **Phase 2** (pairs with #3): Prefetch during trie walk. Requires
   restructuring `commit_batch` → `compact_walk` to decode-then-prefetch.
   Most impactful for parallel storage root computation where multiple
   threads generate many concurrent read requests.

3. **Phase 3** (optional): Async index lookups or switch to mmap for
   disk_hash. Only worth it if Phase 2 shows index reads as bottleneck.

### Dependencies

- **liburing** for the userspace API (header-only, no runtime dependency)
- Kernel 5.1+ for basic io_uring, 5.6+ for linked SQEs
- `io_uring_register_files` and `io_uring_register_buffers` for reduced
  per-I/O overhead (avoids fd table lookup and page pinning per call)

## 5. Speculative Transaction Execution

**Difficulty:** Very Hard | **Impact:** Uncertain

Execute non-conflicting transactions in parallel within a block.
Requires read/write set tracking and rollback on conflict.

- Complex conflict detection (address + storage slot granularity)
- Diminishing returns for early mainnet (few txs per block)
- More relevant for post-merge blocks with high tx density
- Research: Block-STM (Aptos), parallel EVM (Monad/Sei)
