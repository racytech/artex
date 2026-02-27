# Write Scalability: Data > RAM

## Problem

When the mmap'd DB file exceeds physical RAM, write throughput collapses due
to page cache thrashing. Both ART and LMDB (B+tree) exhibit this — it's
fundamental to mmap-based storage.

Benchmark at 32M keys / 32 GB RAM / NVMe SSD:

| Metric          | ART       | LMDB      |
|-----------------|-----------|-----------|
| Write speed     | 17 Kkeys/s | 30 Kkeys/s |
| DB size         | 44.9 GB   | 37.4 GB   |
| Peak (1M keys)  | 86 Kkeys/s | 183 Kkeys/s |
| Degradation     | ~5x       | ~6x       |

Root cause: each ART insert traverses 8-12 random pages. When pages are
evicted by the kernel, each hop costs ~0.5ms (SSD random read). At 12 hops
that's 6ms/insert = 166 ops/sec worst case.

## Option C: mlock Hot Pages (implemented)

**Complexity**: Low — additive change, no architectural changes.

Pin the top levels of the ART (inner nodes) in RAM using `mlock()`. These
pages are accessed on every single operation but the kernel doesn't know
that — it evicts them alongside cold leaf pages under memory pressure.

- Walk the tree from root, BFS to depth N, mlock each page
- Call after checkpoint/compaction (pages may have moved)
- Budget: ~2-4 GB for inner nodes (configurable)
- Expected improvement: reduce page faults from ~12 to ~2-3 per insert
- Estimated throughput at scale: 40-50 Kkeys/s (vs 15-20 without)

Limitations: leaf page writes still cause page faults. Doesn't solve the
fundamental problem, but significantly reduces the constant factor.

## Option A: Write Buffer with Sorted Merge

**Complexity**: Medium — new layer on top, persistent ART unchanged.

Accumulate writes in an in-memory hash map / small ART. Reads check buffer
first, then disk tree. Periodically merge buffer into persistent ART using
sorted iteration (sequential page access instead of random).

Architecture:
```
┌──────────────────────────────────┐
│       Write Buffer (RAM)         │
│  hash map: key[32] → (val, len)  │
│  + delete tombstones             │
│  Size: 2-4 GB configurable       │
├──────────────────────────────────┤
│     Persistent ART (mmap)        │
│     Current code, unchanged      │
└──────────────────────────────────┘
```

Write path:
1. `buffer_put(key, value)` — O(1), always in RAM
2. Every K blocks: sort buffer by key, merge into persistent ART
3. Merge walks both buffer and tree in sorted order → sequential I/O

Read path:
1. Check buffer (tombstone = deleted)
2. Fall through to persistent ART

Crash safety:
- The blockchain itself is the WAL — blocks are deterministic and replayable
- Checkpoint = merge buffer → tree → shadow page flip → fsync
- On crash: recover from last checkpoint, re-execute K blocks from chain
- No separate WAL needed for Ethereum use case

ACID:
- Atomicity: buffer commit is pointer flip; disk merge uses shadow paging
- Consistency: buffer version always takes precedence over disk
- Isolation: readers see committed buffer + disk; no MVCC needed
- Durability: at checkpoint granularity (acceptable for Ethereum)

Expected throughput: 200-500 Kkeys/s sustained (writes never touch disk
directly). Merge cost amortized over K blocks.

## Option B: LSM-Style Two ARTs

**Complexity**: High — significant architectural change.

Two-level LSM using ART at both levels:
- Level 0: in-memory ART (always fast, fixed 4 GB budget)
- Level 1: persistent ART on disk (current code)
- When L0 exceeds threshold, freeze + sorted merge into L1
- Reads: check L0, then L1

Advantages over Option A:
- L0 supports range queries natively (ART is sorted)
- Merge can be done incrementally (partial subtree merge)
- Natural fit for Ethereum's trie structure

Disadvantages:
- More complex read path (merge iterators)
- L0 rebuild on crash requires re-executing blocks
- Memory management for two ART instances

Expected throughput: similar to Option A for writes; better for range queries.

## Recommended Path

1. **Now**: Option C (mlock) — immediate improvement, needed regardless
2. **Next**: Option A (write buffer) — stable writes, moderate effort
3. **Later**: Option B (LSM) — only if range query performance matters

Options A and B both benefit from Option C — inner nodes of the persistent
ART should always be pinned regardless of the write strategy.
