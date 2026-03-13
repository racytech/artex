# MPT State Root Pipeline: Data Flow & Optimization Analysis

## Overview

This document traces the complete data flow from EVM state modifications through
MPT root computation, identifies bottlenecks, and proposes optimizations for
Ethereum-scale throughput.

---

## 1. Dirty Tracking

**File:** `evm/src/evm_state.c`

### Account Dirty Marking

Every state-modifying operation marks the account `mpt_dirty = true` and appends
the address to `dirty_accounts` vec:

- `evm_state_set_nonce()`
- `evm_state_add_balance()`
- `evm_state_set_code()`
- `evm_state_create_account()`
- `evm_state_self_destruct()`
- Storage root changes

```c
mark_account_mpt_dirty(evm_state_t *es, cached_account_t *ca):
    ca->mpt_dirty = true
    dirty_account_push(&es->dirty_accounts, ca->addr.bytes)
```

### Storage Dirty Marking

Each modified slot is tracked with a 52-byte composite key (`addr[20] || slot[32]`):

```c
mark_slot_mpt_dirty(evm_state_t *es, cached_slot_t *cs):
    cs->mpt_dirty = true
    dirty_slot_push(&es->dirty_slots, cs->key)
```

The `cached_slot_t` caches `keccak256(slot)` as `slot_hash` on first use to avoid
rehashing during commit.

---

## 2. Block Execution Lifecycle

**File:** `executor/src/block_executor.c`

```
block_execute():
  1. evm_state_commit()          — set original = current (EIP-2200)
  2. For each tx:
       transaction_execute()     — modifies state, marks dirty
       evm_state_commit_tx()     — process selfdestructs, reset journal
  3. evm_state_finalize()        — flush dirty state
  4. evm_state_compute_state_root_ex()  — compute root
```

Dirty vecs accumulate across all txs in the block. A single account touched by
10 txs appears once in the dirty vec (deduped at commit time).

---

## 3. MPT Root Computation

**File:** `evm/src/evm_state.c` — `evm_state_compute_mpt_root()`

### Phase 1: Storage Roots (serial, per-account)

```
compute_all_storage_roots():
  1. Collect dirty slots from dirty_slots vec
  2. qsort by address → group slots per account
  3. For each account group:
       mpt_store_set_root(storage_mpt, ca->storage_root)   // switch context
       mpt_store_begin_batch(storage_mpt)
       for each slot:
           if value == 0: mpt_store_delete(slot_hash)
           else:          mpt_store_update(slot_hash, rlp(value))
       mpt_store_commit_batch(storage_mpt)                  // walk + hash
       mpt_store_root(storage_mpt, &ca->storage_root)       // read new root
```

**Key design:** All per-account storage tries share a single `storage_mpt` on disk.
`mpt_store_set_root()` switches which trie root the store operates on.

### Phase 2: Account Trie

```
mpt_store_begin_batch(account_mpt)
for each dirty account:
    rlp = encode(nonce, balance, storage_root, code_hash)
    if empty && prune: mpt_store_delete(addr_hash)
    else:              mpt_store_update(addr_hash, rlp)
mpt_store_commit_batch(account_mpt)
mpt_store_root(account_mpt, &root)
```

**Ordering constraint:** Storage roots must complete before account trie commit,
because account RLP embeds `storage_root`.

---

## 4. Inside `mpt_store_commit_batch()`

**File:** `database/src/mpt_store.c`

```
mpt_store_commit_batch():
  1. qsort dirty entries by nibble path    O(D log D)
  2. Deduplicate (last-write-wins)         O(D)
  3. update_subtrie(root, dirty, 0, n, 0)  O(D × depth)
     ├─ Load node: cache lookup → disk pread if miss
     ├─ Decode RLP → mpt_node_t (branch/leaf/extension)
     ├─ Merge dirty entries into subtree (recurse)
     ├─ Rebuild RLP bottom-up
     ├─ If RLP ≥ 32 bytes: keccak → write_node()
     └─ Return new ref to parent
  4. Update root hash
  5. Reset dirty state
```

### Node Write Path (deferred)

```
write_node(rlp, &out_hash):
  1. keccak(rlp) → hash
  2. Skip if hash already in index (dedup)
  3. Allocate slot from free list or append (size classes: 64/128/256/512/1024)
  4. Buffer in deferred write list (no pwrite yet)
  5. Insert into LRU cache
```

Writes are batched and flushed to disk at checkpoint time via `mpt_store_flush()`.

### Node Cache (2 GB LRU)

```
ncache_entry_t:
  hash[32], rlp[1024], rlp_len, depth, lru links, ht chain

Policy:
  - ~1.9M entries at 1070 bytes each
  - Top 4 depth levels pinned (never evicted) — ~70K nodes
  - LRU eviction for deeper nodes
  - Eliminates redundant pread for hot paths
```

---

## 5. Checkpoint & Flush

**File:** `sync/src/sync.c` — `sync_checkpoint()`

```
sync_checkpoint():
  1. evm_state_compute_mpt_root()     — compute root (Phases 1+2 above)
  2. evm_state_flush()                — pwrite all deferred nodes
     ├─ mpt_store_flush(account_mpt)  — write .idx + .dat, fsync
     └─ mpt_store_flush(storage_mpt)  — write .idx + .dat, fsync
  3. code_store_flush()               — sync bytecode store
  4. evm_state_evict_cache()          — drop all cached accounts/slots
  5. Every 32 checkpoints: mpt_store_compact_roots() on storage_mpt
```

---

## 6. Two Operating Modes

MPT root computation behaves very differently depending on the operating mode:

### Replay Mode (era1 sync)

- `batch_mode = ON`, `checkpoint_interval = 256`
- MPT root computed once per 256 blocks
- Dirty set accumulates across 256 blocks: **D=1000+ accounts, S=10000+ slots**
- Cost amortized: ~200 ms / 256 = **~0.8 ms per block**
- Cache evicted at each checkpoint → cold starts
- **Bottleneck: disk I/O** (cold cache, deep trie traversals)

### Tip-of-Chain Mode (engine API)

- `batch_mode = OFF` (not set by engine handlers)
- MPT root computed **every block** within `newPayload` handler
- Dirty set is 1 block's worth: **D=100-300 accounts, S=1000-5000 slots**
- Cost: **~50-200 ms per block** (not amortized)
- Cache stays warm (recent blocks touch similar accounts)
- Time budget: must complete within **~4 seconds** (attestation deadline)
- **Bottleneck: CPU** (keccak hashing, RLP encode — cache hits eliminate most I/O)

### Why the bottleneck shifts

| Factor | Replay | Tip-of-Chain |
|--------|--------|-------------|
| Cache state | Cold (evicted every 256 blocks) | Warm (no eviction) |
| ncache hit rate | ~60-70% (cold start) | ~95%+ (recent blocks) |
| Disk I/O fraction | ~70% of time | ~20% of time |
| CPU fraction | ~20% of time | ~60% of time |
| Dominant cost | pread latency | keccak + RLP |

At tip of chain, the cache is warm because consecutive blocks touch overlapping
account sets (same DEX contracts, same MEV bots, same hot wallets). Most trie
nodes are in ncache. Disk I/O drops to the minority. CPU (keccak hashing per
rebuilt node) becomes the primary bottleneck.

---

## 7. Profiling Results

### Benchmark Setup

Standalone benchmark (`tools/profile_mpt_root.c`) that creates account and storage
`mpt_store` instances, populates them, then runs dirty update rounds measuring each
phase with `clock_gettime`. Uses Zipf-like access pattern (biased toward recent
accounts) to simulate realistic workloads.

### 500K Accounts, 2000 Dirty/Round, 5 Slots/Account

```
Node cache: 512 MB per store, 500K entry capacity
Storage trie: 3.3M nodes (cache full — 500K/500K)
Account trie: 665K nodes (cache full — 500K/500K)

 round  total_ms   stor_ms  stage_ms  commt_ms  s_hit%  a_hit%  d_accts  d_slots
------  --------  --------  --------  --------  ------  ------  -------  -------
     0     25.3      13.2       0.5      11.6    28.6    95.5     2000     3993
     5     30.6      15.8       0.5      14.3    27.6    95.7     2000     4032
    10     33.6      17.8       0.5      15.3    26.9    95.7     2000     4007
    15     38.1      20.5       0.5      17.1    26.1    95.6     2000     3974
    19     48.4      25.4       0.5      22.4    28.4    95.8     2000     3989
```

### Phase Breakdown (average across 20 rounds)

| Phase | Time | % of Total | Notes |
|-------|------|-----------|-------|
| Storage trie commits (`stor_ms`) | ~17 ms | **55%** | Per-account set_root + batch commit |
| Account staging (`stage_ms`) | ~0.5 ms | **1.5%** | RLP encode + mpt_store_update |
| Account trie commit (`commt_ms`) | ~14 ms | **43%** | Sort + dedup + trie walk + hash |

### Cache Behavior

| Store | Hit Rate | Interpretation |
|-------|----------|---------------|
| Storage ncache | **~28%** | Cache full, most dirty accounts' storage nodes evicted between visits |
| Account ncache | **~96%** | Account trie is shallower, top nodes stay pinned/cached |

The storage cache is the critical bottleneck. With 3.3M storage nodes competing for
500K cache slots, 72% of node accesses go to disk. The account trie's shallower
structure (665K nodes) and depth-based pinning keep its top levels cached.

### Degradation Over Rounds

Times drift upward (25 ms → 48 ms) as rounds progress. Each round creates new trie
nodes via splits and extensions, growing the trie beyond the cache's capacity. This
mirrors real-world behavior: as the state grows, cache pressure increases and more
traversals hit disk.

### 50K Accounts (Warm Cache Baseline)

With 50K accounts, everything fits in cache (100% hit rate both stores):

```
total_ms ~2.0   stor_ms ~0.9   stage_ms ~0.0   commt_ms ~0.9
```

This represents the **CPU-only floor**: ~2 ms total when all nodes are cached.
The ratio holds: storage commits ≈ account commits, staging is negligible.
This is the performance profile at tip-of-chain with a warm cache.

### Key Takeaways

1. **Storage tries are the primary bottleneck (55%)** — and they're embarrassingly
   parallel (each account is independent). 4 workers → ~4x speedup on this phase.
2. **Cache hit rate is the performance multiplier** — 100% hits = 2 ms, 28% hits = 30+ ms.
   Larger cache or smarter eviction (LFU, depth-weighted) would help significantly.
3. **Account staging is negligible (1.5%)** — RLP encoding + key lookup is fast, not
   worth optimizing.
4. **Account trie commit at 43%** is the second target — batched keccak (SIMD) would
   reduce hashing time here since commit is dominated by keccak calls during bottom-up
   node rebuild.

---

## 8. Bottleneck Analysis

### Replay Mode (checkpoint boundary, cold cache)

For a checkpoint with **D=1000 dirty accounts**, **S=10000 dirty storage
slots**, trie depth ~10:

| Phase | Est. Time | Bottleneck | Notes |
|-------|-----------|-----------|-------|
| Sort dirty slots | ~1 ms | CPU | qsort O(S log S) |
| Storage trie traversal | ~80 ms | **Disk I/O** | S × depth pread calls |
| Storage trie hashing | ~20 ms | CPU | keccak per rebuilt node |
| Sort dirty accounts | ~0.5 ms | CPU | qsort O(D log D) |
| Account trie traversal | ~40 ms | **Disk I/O** | D × depth pread calls |
| Account trie hashing | ~10 ms | CPU | keccak per rebuilt node |
| Deferred flush | ~50 ms | Disk I/O | Batch pwrite + fsync |
| **Total** | **~200 ms** | | |

Breakdown: **~70% disk I/O**, ~20% CPU (hash + RLP), ~10% other.

### Tip-of-Chain (single block, warm cache)

For a typical mainnet block with **D=200 dirty accounts**, **S=2000 dirty
storage slots**, trie depth ~12-14, ncache hit rate ~95%:

| Phase | Est. Time | Bottleneck | Notes |
|-------|-----------|-----------|-------|
| Sort dirty slots | ~0.2 ms | CPU | qsort O(S log S) |
| Storage trie traversal | ~5 ms | Cache hits | S × depth, ~95% ncache hit |
| Storage trie hashing | ~15 ms | **CPU** | keccak per rebuilt node |
| Storage root switching | ~2 ms | CPU | mpt_store_set_root × 200 accounts |
| Sort dirty accounts | ~0.1 ms | CPU | qsort O(D log D) |
| Account trie traversal | ~3 ms | Cache hits | D × depth, ~95% ncache hit |
| Account trie hashing | ~8 ms | **CPU** | keccak per rebuilt node |
| Deferred flush | ~5 ms | Disk I/O | Smaller batch than replay |
| **Total** | **~40-80 ms** | | |

Breakdown: **~60% CPU** (keccak + RLP), ~25% cache traversal, ~15% I/O.

The per-account overhead matters more at tip: each dirty account requires a
`mpt_store_set_root()` call to switch the shared storage trie context. With
200 dirty accounts, that's 200 root switches even if each account has only
a few dirty slots.

---

## 9. Optimization Opportunities

### 9.1 Parallel Storage Trie Commits (highest impact)

**Current:** Storage tries committed serially, one account at a time.

**Proposed:** Dispatch N account groups to N worker threads.

```
Thread pool (N workers):
  Each worker gets: account group + read-only fd to storage_mpt files
  Workers do: set_root → begin_batch → update slots → commit_batch
  Workers return: (address, new_storage_root, deferred_writes[])
  Main thread: collect results, apply deferred writes, continue to account trie
```

**Feasibility:**
- `pread()` is thread-safe — concurrent reads from same fd work
- Each account group touches disjoint trie paths (different roots)
- Deferred writes can use per-thread buffers, merged after join
- Node cache needs either per-thread caches or a concurrent LRU

**Expected speedup:** Storage phase from ~100 ms → ~100/N ms.
With 4 threads: ~25 ms. With 8: ~12 ms.

**Complexity:** Medium. Main challenge is cache coherence and write merging.

### 9.2 Path Prefetching

**Current:** Nodes loaded on-demand during trie traversal (cache miss → pread).

**Proposed:** After sorting dirty keys, predict which node offsets will be needed
and issue `posix_fadvise(POSIX_FADV_WILLNEED)` or `readahead()` before traversal.

```
For each dirty key:
  Walk index to find root → child → grandchild node offsets
  Batch readahead(fd, offset, len) for all predicted nodes
Then: traverse as normal (nodes already in page cache)
```

**Expected speedup:** Depends on cache hit rate. For cold starts (after eviction),
could eliminate ~50% of blocking pread latency.

**Complexity:** Low. A prefetch pass before `update_subtrie()` using the sorted
dirty keys and the disk_hash index.

### 9.3 Batched Keccak (SIMD)

**Current:** `keccak256()` called once per node, scalar.

**Proposed:** Batch 4 independent node hashes using AVX2 4-way keccak.

```
During bottom-up rebuild:
  Collect leaf/branch RLP buffers that are ready to hash
  When 4 buffers accumulated: keccak_4x(buf[4], hash[4])
  Flush remaining with scalar keccak
```

**Expected speedup:** ~2-3x on hashing phase. Hashing is ~15-20% of total,
so overall ~10% improvement.

**Complexity:** Medium. Requires a 4-way keccak implementation (existing open-source
implementations: XKCP). Ryzen (Zen3+) has good AVX2 throughput.

### 9.4 io_uring for Flush

**Current:** `mpt_store_flush()` does sequential `pwrite()` calls + `fsync()`.

**Proposed:** Submit all deferred writes as a single io_uring submission queue batch.

```
struct io_uring ring;
io_uring_queue_init(256, &ring, 0);
for each deferred entry:
    io_uring_prep_write(sqe, fd, rlp, len, offset);
io_uring_submit(&ring);
// wait for completions
io_uring_prep_fsync(sqe, fd, 0);
```

**Expected speedup:** Reduces syscall overhead from N pwrite calls to 1 submit.
Most impactful when N is large (thousands of nodes per checkpoint).

**Complexity:** Low-medium. Linux 5.1+ only (available on 6.17 kernel).

### 9.5 Dirty-Path Coalescing Verification

**Current:** Sorted dirty keys are merged into the trie via `update_subtrie()`,
which naturally coalesces entries sharing branch nodes.

**Verify:** That when 100 storage slots share the same first 4 nibbles, the branch
node at depth 4 is loaded **once** (not 100 times). The recursive structure should
handle this, but worth confirming with instrumentation.

### 9.6 Incremental Root (skip clean subtrees)

**Current behavior (already optimal):** `update_subtrie()` only traverses paths
containing dirty keys. Clean subtrees are represented by their existing hash
reference and never loaded from disk.

This is already `O(dirty × depth)` not `O(total_accounts)`. No change needed.

---

## 10. Priority Ranking

Priorities differ between the two operating modes:

### Replay Mode (era1 sync)

| # | Optimization | Impact | Effort | Risk |
|---|-------------|--------|--------|------|
| 1 | Parallel storage tries | **High** (~4x on storage phase) | Medium | Low |
| 2 | Path prefetching | Medium (~30% on I/O) | Low | None |
| 3 | io_uring flush | Medium (~2x on flush) | Low | None |
| 4 | Batched keccak (SIMD) | Low-Medium (~10% overall) | Medium | None |
| 5 | Verify coalescing | Diagnostic | Low | None |

### Tip-of-Chain (engine API)

| # | Optimization | Impact | Effort | Risk |
|---|-------------|--------|--------|------|
| 1 | Parallel storage tries | **High** (~4x on storage phase) | Medium | Low |
| 2 | Batched keccak (SIMD) | **High** (~2x on hashing, ~30% overall) | Medium | None |
| 3 | Skip flush between blocks | Medium (defer to idle) | Low | Low |
| 4 | Path prefetching | Low (cache is warm) | Low | None |
| 5 | io_uring flush | Low (small batches) | Low | None |

At tip of chain, **batched keccak jumps to #2** because CPU is the bottleneck,
not I/O. Path prefetching drops to #4 because the warm cache eliminates most
disk reads.

A new optimization appears at #3: **skip flush between blocks**. At tip of chain,
there's no need to fsync after every block. Deferred writes can accumulate across
blocks and flush during idle time (between slots) or on finalization. This saves
~5-10 ms per block of unnecessary fsync.

### Ethereum Mainnet Scale Estimates

At block ~19M: ~250M accounts, ~1B storage slots, trie depth ~12-14.

Typical block: 100-300 dirty accounts, 1000-5000 dirty storage slots.

#### Replay Mode (per-checkpoint)

| Configuration | Est. root compute time |
|---------------|----------------------|
| Current (serial, no prefetch) | ~200 ms |
| + Parallel storage (4 threads) | ~120 ms |
| + Path prefetch | ~90 ms |
| + io_uring flush | ~70 ms |
| + Batched keccak | ~60 ms |

Target: <100 ms per checkpoint root on NVMe + Ryzen.

#### Tip-of-Chain (per-block)

| Configuration | Est. root compute time |
|---------------|----------------------|
| Current (serial, warm cache) | ~50-80 ms |
| + Parallel storage (4 threads) | ~30-50 ms |
| + Batched keccak (4-way AVX2) | ~20-35 ms |
| + Skip inter-block flush | ~15-30 ms |

Target: <50 ms per block root at tip of chain on NVMe + Ryzen.
Well within the ~4 second attestation window.

---

## 11. File Reference

| Component | File | Key Functions |
|-----------|------|---------------|
| Dirty tracking | `evm/src/evm_state.c:213-226` | `mark_account_mpt_dirty`, `mark_slot_mpt_dirty` |
| Storage root compute | `evm/src/evm_state.c:2163-2248` | `compute_all_storage_roots` |
| Account root compute | `evm/src/evm_state.c:2406-2496` | `evm_state_compute_mpt_root` |
| Block execution | `executor/src/block_executor.c:270-555` | `block_execute` |
| Checkpoint | `sync/src/sync.c:597-647` | `sync_checkpoint` |
| Trie commit | `database/src/mpt_store.c:2242-2303` | `mpt_store_commit_batch` |
| Trie traversal | `database/src/mpt_store.c:2192-2221` | `update_subtrie` |
| Node write (deferred) | `database/src/mpt_store.c:1165-1223` | `write_node` |
| Node cache (LRU) | `database/src/mpt_store.c:148-398` | `ncache_get`, `ncache_put` |
| Flush to disk | `database/src/mpt_store.c` | `mpt_store_flush` |
| Root profiler | `tools/profile_mpt_root.c` | Standalone benchmark for phase timing |
