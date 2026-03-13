# Pipeline Interactions: TX Pipeline × MPT Pipeline

## Overview

Two proposed optimizations operate on different phases of block processing:

- **TX Pipeline** — prep thread decodes + ecrecovers transactions ahead of execution
- **MPT Pipeline** — parallel storage trie commits during state root computation

This document analyzes their interactions, confirms they don't conflict, and
identifies a cross-block pipelining opportunity.

---

## Block Lifecycle — Two Modes

### Replay Mode (era1 sync)

```
chain_replay main loop:
  for each block:
    ┌──────────────────────────────────────────────────────────────┐
    │ sync_execute_block()                                        │
    │   block_execute()                                           │
    │     for each tx:                                            │
    │       decode + ecrecover + execute     ← TX PHASE           │
    │     finalize state                                          │
    │                                                             │
    │   if checkpoint_interval reached:      (every 256 blocks)   │
    │     evm_state_compute_mpt_root()       ← MPT PHASE         │
    │       compute_all_storage_roots()        (storage tries)    │
    │       commit account trie                (account trie)     │
    │     evm_state_flush()                  ← FLUSH PHASE        │
    │     evm_state_evict_cache()            ← EVICT              │
    └──────────────────────────────────────────────────────────────┘
```

MPT root is deferred to checkpoint boundaries (every 256 blocks). Between
checkpoints, dirty flags accumulate. The two pipelines never overlap.

### Tip-of-Chain Mode (engine API)

```
engine newPayload handler:
  ┌──────────────────────────────────────────────────────────────┐
  │ block_execute()                                              │
  │   for each tx:                                               │
  │     decode + ecrecover + execute         ← TX PHASE          │
  │   finalize state                                             │
  │                                                              │
  │ evm_state_compute_mpt_root()             ← MPT PHASE        │
  │   compute_all_storage_roots()              (storage tries)   │
  │   commit account trie                      (account trie)    │
  │                                                              │
  │ return PAYLOAD_VALID                     (NO flush — deferred)│
  └──────────────────────────────────────────────────────────────┘
```

MPT root is computed **every block**. No flush after each block — deferred
writes stay in memory until finalization or idle time. No cache eviction.

**Key difference:** At tip, both pipelines run every block, back to back.
They still don't overlap — TX phase completes before MPT phase starts.

---

## Phase Separation

```
Replay (checkpoint block):
  [───── TX PHASE ─────][── MPT PHASE ──][─ FLUSH ─][EVICT]

Tip-of-chain (every block):
  [───── TX PHASE ─────][── MPT PHASE ──]
                                          (no flush, no evict)

TX Pipeline active here ─┘
                          MPT Pipeline active here ─┘
```

The two pipelines operate on **strictly different phases**. They never overlap
within the same block. This is the fundamental reason they don't conflict.

---

## Detailed Interaction Analysis

### 1. Within a single block — NO CONFLICT

| Resource | TX Pipeline (during TX phase) | MPT Pipeline (during MPT phase) |
|----------|-------------------------------|----------------------------------|
| `evm_state` (accounts, storage) | Exec thread: read/write | Main thread: read dirty vecs, commit batches |
| `mpt_store` account trie | Exec thread: pread (account loads) | Main thread: begin/update/commit batch |
| `mpt_store` storage trie | Exec thread: pread (storage loads) | Parallel threads: begin/update/commit per-account |
| Prep thread | Active: decode + ecrecover | **Not running** |
| MPT worker threads | **Not running** | Active: parallel storage commits |
| Ring buffer | Active: producer/consumer | **Not in use** |

**No overlap. No shared mutable state between the two pipelines.**

### 2. Across checkpoint boundaries — NO CONFLICT

The checkpoint flow is:

```
Block 255: [TX phase] → (no MPT)
Block 256: [TX phase] → [MPT root] → [flush] → [evict]
Block 257: [TX phase] → (no MPT)
```

The MPT phase at block 256 processes all dirty state accumulated from blocks
1-256. The TX pipeline for block 257 doesn't start until block 256 is fully
complete (`sync_execute_block` returns, then next iteration starts).

### 3. mpt_store file descriptors — SAFE

Both pipelines may pread from the same mpt_store `.dat` and `.idx` files:

| Scenario | Reader | Writer | Safe? |
|----------|--------|--------|-------|
| TX prep thread + exec thread (same block) | Both pread | Exec: deferred writes only | Yes — pread is thread-safe, writes are buffered |
| MPT parallel storage threads | All pread | Each: deferred writes to own buffer | Yes — pread is thread-safe |
| Flush phase | Nobody | Main thread: pwrite batch | Yes — no concurrent readers during flush |

Writes to `.dat` and `.idx` only happen during `mpt_store_flush()`, which
runs after MPT root computation and after all threads have joined. No
concurrent readers during flush.

### 4. Node cache (ncache) — NEEDS CARE

The 2GB LRU cache inside `mpt_store` is **not thread-safe**:

| Pipeline | Cache access |
|----------|-------------|
| TX prep thread speculative pread | Does NOT touch ncache (raw fd pread only) |
| TX exec thread account loads | Reads/writes ncache via `mpt_store_get` |
| MPT parallel storage threads | Each calls `update_subtrie` → `ncache_get/put` |

**TX Pipeline:** Safe. The prep thread never touches ncache — it only does raw
pread on fds to warm the OS page cache. The exec thread is the sole ncache user
during the TX phase.

**MPT Pipeline:** Needs either:
- **Per-thread ncache** — each parallel storage worker has its own cache (simplest)
- **Shared ncache with mutex** — adds lock contention on every node access
- **Lock-free concurrent cache** — complex, probably not worth it

Recommendation: Per-thread cache for storage workers. The account trie commit
runs single-threaded after storage commits join, so it can use the main cache
without contention.

### 5. Dirty tracking vectors — NO CONFLICT

| Vector | TX Pipeline writes to it | MPT Pipeline reads from it |
|--------|--------------------------|---------------------------|
| `dirty_accounts` | Exec thread appends during execution | Main thread iterates during MPT phase |
| `dirty_slots` | Exec thread appends during execution | Main thread iterates during MPT phase |

These are append-only during the TX phase, then read-only during the MPT phase.
No concurrent access.

### 6. `evm_state_evict_cache()` — timing matters

Cache eviction happens AFTER flush, at the end of a checkpoint. If a TX prep
thread for the next block were already running and trying to speculatively read
account state, it would read from a cache that's about to be evicted.

This is fine because:
- The prep thread's speculative pread targets the **disk files**, not the cache
- The evicted cache entries are reloaded on demand when the exec thread needs them
- The prep thread's pread warms the OS page cache, which survives eviction

---

## Cross-Block Pipelining

### Replay Mode — prep next block during MPT phase

```
Block N:   [───── execute ─────][── MPT root ──][flush]
Block N+1: [decode+ecrecover...]                       [───── execute ─────]
                                ↑                       ↑
                        prep thread runs          exec thread starts
                        while MPT computes        after flush completes
```

During a checkpoint block, the MPT phase takes ~200 ms. Block N+1 has ~200 txs
× 200 μs = ~40 ms of decode+ecrecover work. This is completely hidden behind
the MPT phase. When the exec thread finishes flush and starts block N+1, all
transactions are already in the ring buffer.

```c
// In chain_replay main loop:
for each block:
    sync_execute_block(sync, ...);  // pops from ring, executes

    // After execution, start prep for NEXT block immediately
    // (overlaps with this block's potential MPT/flush phase)
    if (next_block_available) {
        prep_ctx.body = next_body;
        prep_ctx.tx_count = next_tx_count;
        pthread_create(&prep_tid, NULL, prep_thread_fn, &prep_ctx);
    }
```

### Tip-of-Chain — no cross-block prep (blocks arrive on demand)

At tip, blocks arrive from the CL via `newPayload` every ~12 seconds. The
next block isn't available until the CL sends it. Cross-block pipelining
is **not possible** because there's nothing to prep ahead of time.

```
                     12 second slot
├────────────────────────────────────────────────────┤
Block N arrives:   [execute ~80ms][MPT ~60ms]
                                          ↑
                                  respond VALID
                                          │
                              ~11.8 seconds idle
                                          │
Block N+1 arrives:                        [execute ~80ms][MPT ~60ms]
```

However, within each block the TX pipeline still provides value: the prep
thread finishes all decode+ecrecover (~40 ms for 200 txs) before the exec
thread finishes executing all txs (~80 ms). So the exec thread never waits
for decode.

### Tip-of-Chain — where cross-block prep COULD help

If the node is a **block builder** (not just a validator), it knows the
pending transaction pool. The prep thread could speculatively decode and
recover senders from mempool transactions between slots. When the CL calls
`forkchoiceUpdated` with `payloadAttributes`, the builder already has
prepared transactions ready for block assembly.

This is a future optimization that depends on the txpool module (not yet
implemented — see `engine/TODO.md`).

### Constraint (both modes)

The exec thread for block N+1 **cannot start** until block N's state is fully
committed (including MPT root). The prep thread only does state-independent
work (decode + ecrecover), so it can run anytime.

---

## Combined Architecture

```
Thread 0 (Main / Exec):
  for each block:
    pop prepared txs from ring → execute serially
    evm_state_commit_tx() after each tx
    compute_mpt_root()       ← spawns MPT worker threads internally
    if replay checkpoint:
      flush()
      evict_cache()

Thread 1 (Prep):
  decode + ecrecover txs for current/next block
  speculative pread on mpt_store fds (page cache warming)
  push to ring buffer

Threads 2..N (MPT Workers, transient):
  spawned by compute_all_storage_roots()
  each commits one account's storage trie batch
  joined before account trie commit begins
```

### Thread count by mode

#### Replay Mode

| Phase | Thread 0 | Thread 1 | Threads 2..N |
|-------|----------|----------|-------------|
| TX execution | Execute txs | Decode ahead | — |
| MPT root (checkpoint) | Commit account trie | Decode next block | Storage trie workers |
| Flush + evict | pwrite batch | Decode next block | — |
| Between checkpoints | Execute txs | Decode ahead | — |

Steady state: **2 threads**. At checkpoints: **2 + N** (N=4 workers).
MPT workers exist ~200 ms every 256 blocks.

#### Tip-of-Chain

| Phase | Thread 0 | Thread 1 | Threads 2..N |
|-------|----------|----------|-------------|
| TX execution | Execute txs | Decode ahead (same block) | — |
| MPT root | Commit account trie | Idle (done decoding) | Storage trie workers |
| Between blocks | Idle (waiting for CL) | Idle | — |

Steady state: **2 threads** during block processing, **1 + N** during MPT root.
Between blocks (~11.8 seconds idle): **0 active threads**.

MPT workers are more impactful at tip — they run every block instead of every
256 blocks. With 4 workers, the storage trie phase drops from ~25 ms to ~8 ms
per block (warm cache).

### Resource contention

| Resource | Max concurrent accessors | Contention? |
|----------|--------------------------|-------------|
| CPU cores | 2 steady, 6 during MPT | No (Ryzen has 8+ cores) |
| mpt_store `.dat` fd | 1 (exec) + 1 (prep pread) = 2 | No (pread is independent) |
| mpt_store `.idx` fd | Same | No |
| OS page cache | All threads benefit | Positive (prep warms for exec) |
| L3 cache | Prep + exec share | Minimal (prep touches different data) |
| Memory (2GB ncache) | 1 writer at a time | No (per-thread for MPT workers) |

At tip-of-chain, the 12-second gap between blocks means **zero sustained
resource pressure**. Both pipelines complete in <150 ms, then everything
goes idle. No thermal throttling, no cache pollution between blocks.

---

## Invariants to Maintain

1. **Exec thread is sole writer to `evm_state`** — prep thread never touches it
2. **Prep thread is sole writer to ring buffer head** — SPSC guarantee
3. **MPT workers only exist during MPT phase** — no overlap with TX execution
4. **`mpt_store_flush()` runs single-threaded** — no concurrent readers during pwrite
5. **Block N+1 execution starts only after block N fully completes** — state consistency
6. **Prep thread for block N+1 can start before block N's MPT phase** — decode is state-independent
7. **Cancel flag for cleanup** — exec thread can stop prep thread on error

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Race in ncache | High if shared | Per-thread cache for MPT workers; prep thread doesn't use ncache |
| Stale speculative pread | None | Page cache warming is best-effort, stale data is harmless |
| Ring buffer deadlock | Medium | Cancel flag + timeout; ring size > max txs per block |
| Memory ownership (tx data) | Low | Clear handoff: prep allocates, exec frees |
| Thread creation overhead | Low | pthread_create is ~50 μs; amortized over 200+ txs |
| TSAN false positives | Low | Ring buffer atomics are standard; run TSAN early |

---

## Tip-of-Chain: End-to-End Latency Budget

Complete `newPayload` response time with all optimizations:

```
newPayload arrives from CL
  │
  ├─ [TX Phase: ~70 ms]
  │    Prep thread: decode + ecrecover all txs       (~40 ms, hidden)
  │    Exec thread: serial EVM execution             (~70 ms)
  │
  ├─ [MPT Phase: ~25 ms]
  │    Parallel storage trie commits (4 workers)     (~8 ms)
  │    Account trie commit (single-threaded)          (~10 ms)
  │    Keccak hashing (batched SIMD)                  (~7 ms, overlaps)
  │
  ├─ [Validation: <1 ms]
  │    Compare computed root vs payload state_root
  │
  └─ respond PAYLOAD_VALID
      Total: ~95 ms

Without optimizations: ~150 ms
With all optimizations: ~95 ms
Attestation deadline:   ~4000 ms
Margin:                 ~3900 ms
```

The combined pipelines put `newPayload` response at **<100 ms**, leaving
~97% of the attestation window as margin. Even under heavy blocks (500+ txs,
complex DeFi), the response stays well under 500 ms.

### What matters most at tip

| Optimization | Replay impact (amortized) | Tip impact (per-block) |
|-------------|--------------------------|----------------------|
| TX pipeline (ecrecover) | ~20 ms/block | ~30 ms/block |
| Parallel storage tries | ~0.3 ms/block | **~17 ms/block** |
| Batched keccak | ~0.04 ms/block | **~8 ms/block** |
| Cross-block prep | ~40 ms/checkpoint | Not applicable |
| Skip inter-block flush | N/A | ~5 ms/block |

At tip, **parallel storage tries** and **batched keccak** together save ~25 ms
per block — more than the TX pipeline. During replay, the TX pipeline dominates
because MPT root is amortized.

---

## File Reference

| Document | Content |
|----------|---------|
| `docs/tx_pipeline.md` | TX pipeline design, ring buffer, prep thread |
| `docs/mpt_root_pipeline.md` | MPT root pipeline, parallel storage tries |
| `docs/pipeline_interactions.md` | This document — interaction analysis |
| `sync/src/sync.c:505-591` | `sync_execute_block` — block lifecycle |
| `sync/src/sync.c:597-647` | `sync_checkpoint` — MPT root + flush |
| `executor/src/block_executor.c:353-462` | Current 1-tx lookahead (to be replaced) |
| `evm/src/evm_state.c:2406-2496` | `evm_state_compute_mpt_root` |
| `database/src/mpt_store.c:148-398` | Node cache (ncache) — thread safety concern |
