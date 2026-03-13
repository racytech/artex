# N-Deep Transaction Pipeline: Design & Implementation

## Overview

Pipelined 2-thread EVM execution where a prep thread decodes and recovers
senders for all transactions ahead of the execution thread. The prep thread
races N transactions ahead, hiding decode + ecrecover + speculative I/O
latency behind serial EVM execution.

```
                    ┌─────────────────┐
Block body ──────►  │  PREP THREAD    │
(raw RLP txs)       │                 │
                    │  for i=0..N:    │
                    │    decode tx[i] │──► ring buffer ──► EXEC THREAD
                    │    ecrecover    │    (prepared_tx)    serial execution
                    │    prefetch I/O │
                    └─────────────────┘
```

---

## Motivation

Transactions within a block are strictly serial — tx N+1 can read state that
tx N wrote. But prep work for tx N+1 is independent of tx N's execution:

| Operation | Cost per tx | State-dependent? |
|-----------|-------------|------------------|
| RLP decode | ~100 μs | No |
| ecrecover (secp256k1 ECDSA) | 50-100 μs | No |
| Signing hash (keccak) | 10-20 μs | No |
| Access list parsing | ~10 μs | No |
| EIP-7702 auth recovery | 50-100 μs each | No |
| Intrinsic gas calc | ~10 μs | No |
| Account load (cold) | 1-10 ms | Yes (speculative) |
| Nonce/balance check | <1 μs | Yes |
| EVM execution | 10 μs - 10 ms | Yes |

The N-deep pipeline moves all state-independent work off the critical path.

---

## Current Implementation (1-tx Lookahead)

**File:** `executor/src/block_executor.c:353-402`

The current code decodes tx[i+1] while tx[i] executes, and calls
`evm_state_prefetch_account()` (which is just `mem_art_prefetch` — a CPU
cache line prefetch on the in-memory ART tree, not disk I/O).

Limitations:
- Only 1 tx ahead — prep work for tx[i+1] may not finish before tx[i] does
- No disk I/O prefetch — only warms CPU cache for the ART index
- ecrecover still on the critical path (done inline during decode)

---

## Proposed Design

### Data Structures

```c
#define TX_RING_CAP 32  // power of 2, must exceed max pipeline depth

typedef struct {
    transaction_t tx;       // decoded, sender recovered
    bool          valid;    // decode + ecrecover succeeded
    bool          done;     // sentinel: no more txs
} prepared_tx_t;

typedef struct {
    prepared_tx_t  slots[TX_RING_CAP];
    _Atomic size_t head;    // prep thread writes, monotonically increasing
    _Atomic size_t tail;    // exec thread reads, monotonically increasing
} tx_ring_t;
```

Lock-free SPSC (single-producer, single-consumer) ring buffer. No mutexes.

```c
// Producer (prep thread)
static inline void ring_push(tx_ring_t *r, const prepared_tx_t *pt) {
    size_t h = atomic_load_explicit(&r->head, memory_order_relaxed);
    // Spin until slot available (backpressure when exec thread is slow)
    while (h - atomic_load_explicit(&r->tail, memory_order_acquire) >= TX_RING_CAP)
        ;  // spin (or sched_yield)
    r->slots[h & (TX_RING_CAP - 1)] = *pt;
    atomic_store_explicit(&r->head, h + 1, memory_order_release);
}

// Consumer (exec thread)
static inline prepared_tx_t *ring_pop(tx_ring_t *r) {
    size_t t = atomic_load_explicit(&r->tail, memory_order_relaxed);
    // Spin until data available
    while (atomic_load_explicit(&r->head, memory_order_acquire) == t)
        ;  // spin (or sched_yield)
    prepared_tx_t *pt = &r->slots[t & (TX_RING_CAP - 1)];
    return pt;  // caller calls ring_consume() after processing
}

static inline void ring_consume(tx_ring_t *r) {
    size_t t = atomic_load_explicit(&r->tail, memory_order_relaxed);
    atomic_store_explicit(&r->tail, t + 1, memory_order_release);
}
```

### Prep Thread

```c
typedef struct {
    tx_ring_t          *ring;
    const block_body_t *body;
    size_t              tx_count;
    uint64_t            chain_id;
    int                 mpt_dat_fd;   // read-only fd for speculative I/O
    int                 mpt_idx_fd;   // read-only fd for speculative I/O
} prep_ctx_t;

static void *prep_thread_fn(void *arg) {
    prep_ctx_t *ctx = arg;

    for (size_t i = 0; i < ctx->tx_count; i++) {
        prepared_tx_t pt = { .valid = false, .done = false };

        const rlp_item_t *item = block_body_tx(ctx->body, i);
        if (item && tx_decode_rlp(&pt.tx, item, ctx->chain_id)) {
            pt.valid = true;

            // Speculative I/O: warm OS page cache for sender/recipient
            // accounts. These pread calls touch mpt_store files without
            // modifying any shared state.
            prefetch_account_io(ctx->mpt_dat_fd, ctx->mpt_idx_fd,
                                &pt.tx.sender);
            if (!pt.tx.is_create)
                prefetch_account_io(ctx->mpt_dat_fd, ctx->mpt_idx_fd,
                                    &pt.tx.to);
        }

        ring_push(ctx->ring, &pt);
    }

    // Sentinel
    prepared_tx_t sentinel = { .valid = false, .done = true };
    ring_push(ctx->ring, &sentinel);
    return NULL;
}
```

### Exec Thread (modified block_execute loop)

```c
// Setup
tx_ring_t ring = {0};
prep_ctx_t prep_ctx = {
    .ring     = &ring,
    .body     = body,
    .tx_count = tx_count,
    .chain_id = chain_id,
    .mpt_dat_fd = evm_state_get_mpt_dat_fd(evm->state),  // new accessor
    .mpt_idx_fd = evm_state_get_mpt_idx_fd(evm->state),   // new accessor
};

pthread_t prep_tid;
pthread_create(&prep_tid, NULL, prep_thread_fn, &prep_ctx);

for (size_t i = 0; i < tx_count; i++) {
    prepared_tx_t *pt = ring_pop(&ring);

    if (!pt->valid) {
        // decode failed — handle error
        ring_consume(&ring);
        result.success = false;
        break;
    }

    // Execute using pre-decoded, pre-recovered tx
    transaction_result_t tx_result;
    bool ok = transaction_execute(evm, &pt->tx, &tx_env, &tx_result);

    // ... fill receipt, bloom, etc. (unchanged) ...

    tx_decoded_free(&pt->tx);
    ring_consume(&ring);
    evm_state_commit_tx(evm->state);
}

pthread_join(prep_tid, NULL);
```

---

## Speculative I/O Prefetch

The prep thread cannot touch `evm_state` (not thread-safe). Three options
for warming disk pages, in order of complexity:

### Option A: posix_fadvise (trivial, ~5 lines)

```c
void prefetch_account_io(int dat_fd, int idx_fd,
                         const address_t *addr) {
    // Advise kernel to read-ahead from start of both files.
    // Coarse but effective when accounts are clustered on disk.
    posix_fadvise(dat_fd, 0, 0, POSIX_FADV_WILLNEED);
    posix_fadvise(idx_fd, 0, 0, POSIX_FADV_WILLNEED);
}
```

Effectiveness: Low precision, but free. Good for sequential replay where
data files fit in RAM.

### Option B: Targeted pread into throwaway buffer (~30 lines)

```c
void prefetch_account_io(int dat_fd, int idx_fd,
                         const address_t *addr) {
    // Hash address to get trie key
    hash_t key = hash_keccak256(addr->bytes, 20);

    // Look up node offset in disk_hash index (read-only pread)
    // This is a simplified version — real impl follows disk_hash bucket chain
    uint8_t throwaway[1024];
    uint64_t bucket = hash_to_bucket(key.bytes, idx_capacity);
    pread(idx_fd, throwaway, PAGE_SIZE, bucket * PAGE_SIZE);

    // The pread brings the index page into OS page cache.
    // When exec thread does the real lookup, it hits page cache instead of disk.
}
```

Effectiveness: High. Warms exactly the index pages needed. The data pages
get warmed on the real lookup (which is now fast because the index lookup
doesn't block on I/O).

### Option C: Concurrent prefetch cache (100+ lines)

A dedicated hash map where the prep thread stores fully-loaded account data.
The exec thread checks this map before going to mpt_store. Requires a
concurrent hash map or mutex. High reward but real complexity.

**Recommendation:** Start with Option B. It gives most of the benefit with
minimal code and zero thread-safety concerns (pread is thread-safe on the
same fd).

---

## Thread Safety Analysis

| Resource | Prep Thread | Exec Thread | Safe? |
|----------|-------------|-------------|-------|
| `block_body` RLP data | Read-only | Not accessed | Yes |
| `secp256k1_context` | Read-only (verify) | Read-only (verify) | Yes (after init) |
| Ring buffer | Write head | Read tail | Yes (atomics) |
| `transaction_t` in ring | Write (produce) | Read (consume) | Yes (sequential) |
| `evm_state` | **Never touched** | Read/write | Yes (no sharing) |
| mpt_store `.dat` fd | pread (read-only) | pread + deferred pwrite | Yes (pread is atomic) |
| mpt_store `.idx` fd | pread (read-only) | pread + disk_hash_put | See note below |

**Note on idx fd:** The exec thread may call `disk_hash_put` (via `write_node`)
which modifies the index. However, deferred writes are buffered in memory and
only flushed at checkpoint time. During block execution, the index file is
read-only on disk. The exec thread's writes go to the deferred buffer, not the
fd. So concurrent pread from the prep thread is safe.

**secp256k1 thread safety:** `get_secp_ctx()` returns a static context created
once. `secp256k1_ecdsa_recover` with `SECP256K1_CONTEXT_VERIFY` is documented
as thread-safe for concurrent verification calls.

---

## Error Handling

| Scenario | Prep Thread | Exec Thread |
|----------|-------------|-------------|
| Decode failure | Push `{valid=false}` | Break loop, log error |
| ecrecover failure | Push `{valid=false}` | Break loop, log error |
| Exec thread breaks early | Stops consuming | Prep thread spins on full ring |
| Prep thread crashes | Never pushes sentinel | Exec thread spins forever |

To handle the crash/hang case, add a shared `_Atomic bool cancel` flag:
- Exec thread sets `cancel = true` on error or early exit
- Prep thread checks `cancel` before each iteration
- Exec thread drains remaining ring entries after join

```c
// In prep thread loop:
if (atomic_load_explicit(&ctx->cancel, memory_order_relaxed))
    break;

// In exec thread on error:
atomic_store_explicit(&prep_ctx.cancel, true, memory_order_relaxed);
pthread_join(prep_tid, NULL);
// Drain and free any remaining decoded txs in the ring
```

---

## Performance Estimate

### Per-tx savings (moved off critical path)

| Operation | Time | Notes |
|-----------|------|-------|
| RLP decode | ~100 μs | Parsing + allocation |
| ecrecover | 50-100 μs | secp256k1 ECDSA recovery |
| Signing hash | 10-20 μs | keccak256 |
| EIP-7702 auth recovery | 50-100 μs × N | Per authorization tuple |
| Speculative pread | 0-500 μs | Depends on page cache state |
| **Total per tx** | **~160-720 μs** | |

### Per-block savings

| Block era | Txs/block | ecrecover saved | I/O prefetch | Total saved |
|-----------|-----------|-----------------|--------------|-------------|
| 0-1M | 1-10 | 0.5-1 ms | minimal | ~1 ms |
| 1M-4M | 10-30 | 1.5-3 ms | 1-5 ms | ~3-8 ms |
| 4M-8M | 50-100 | 5-10 ms | 5-15 ms | ~10-25 ms |
| 8M-15M | 100-200 | 10-20 ms | 10-30 ms | ~20-50 ms |
| 15M+ | 150-300 | 15-30 ms | 15-40 ms | ~30-70 ms |

The savings scale linearly with transaction count. At modern mainnet blocks
(200+ txs), **30-70 ms per block** is a significant fraction of total block
processing time.

### Backpressure behavior

With `TX_RING_CAP = 32`, the prep thread can race up to 32 txs ahead. At
~200 μs per tx decode+ecrecover, the prep thread fills the ring in ~6.4 ms.
If the exec thread averages >200 μs per tx execution (typical for non-trivial
contracts), the prep thread stays ahead and never blocks.

If the exec thread is faster (simple transfers, ~50 μs), the ring stays
nearly full and prep thread occasionally spins on backpressure. This is fine —
the spin cost is negligible compared to the savings.

---

## Implementation Plan

### Phase 1: Core pipeline (ecrecover only)

1. Add `tx_ring_t` and `prepared_tx_t` to a new header (e.g., `executor/include/tx_pipeline.h`)
2. Write `prep_thread_fn` — decode + ecrecover loop, push to ring
3. Modify `block_execute()` — spawn prep thread, pop from ring, execute
4. Remove existing 1-tx lookahead code (subsumed by pipeline)
5. Add `cancel` flag for error handling
6. Test: verify identical roots and receipts on mainnet blocks

~200 lines. No thread-safety concerns beyond the ring buffer atomics.

### Phase 2: Speculative I/O prefetch

1. Expose read-only fds from `mpt_store_t` (new accessor, ~5 lines)
2. Implement `prefetch_account_io()` using targeted pread (Option B)
3. Call from prep thread after decode
4. Test: verify no corruption, measure I/O cache hit rate improvement

~40 lines on top of Phase 1.

### Phase 3: Validation & tuning

1. Run with TSAN (ThreadSanitizer) on full chain replay
2. Benchmark before/after on blocks 0-1M, 4M-8M, 15M+
3. Tune `TX_RING_CAP` based on measured prep/exec throughput ratio
4. Consider `sched_yield()` vs spin in ring buffer wait loops

---

## Tip-of-Chain Considerations

At tip of chain (engine API), the TX pipeline behaves differently than during
historical replay:

### Different timing profile

| Factor | Replay | Tip-of-Chain |
|--------|--------|-------------|
| Blocks per second | 50-200 | 1 per 12 seconds |
| Time budget per block | ~5-20 ms target | ~4 seconds (attestation) |
| MPT root frequency | Every 256 blocks | **Every block** |
| TX pipeline value | High (amortized over many blocks) | Medium (one block at a time) |

During replay, the TX pipeline saves ~20-30 ms per block across hundreds of
blocks per second — a major throughput win. At tip of chain, you process one
block per slot (12 seconds), so raw throughput matters less than **latency** —
how fast you can validate and respond to `newPayload`.

### Where the time goes at tip

For a typical mainnet block at tip (~200 txs):

```
newPayload handler:
  [block_execute: ~50-100 ms] [compute_mpt_root: ~50-80 ms] → respond VALID
   ├─ TX decode+ecrecover: ~30 ms (hidden by pipeline)
   └─ EVM execution: ~50-70 ms

Total without pipeline: ~130-180 ms
Total with pipeline:    ~100-150 ms  (ecrecover hidden)
MPT root:               ~50-80 ms    (see mpt_root_pipeline.md)
```

The TX pipeline saves **~30 ms** off the critical path. The MPT root costs
**~50-80 ms** (warm cache). Combined, total `newPayload` latency is ~100-150 ms,
well within the attestation window.

### Cross-block prep at tip

At tip, blocks arrive every 12 seconds. The prep thread for block N+1 can't
start until block N+1's payload arrives from the CL. There's no cross-block
pipelining opportunity because blocks aren't available in advance.

However, the prep thread still provides value **within** a block: while the
exec thread processes tx[0], the prep thread has already decoded and recovered
txs [1..N]. With 200 txs, the prep thread finishes all decode+ecrecover in
~40 ms, well before the exec thread finishes all 200 txs (~100 ms).

### Engine API integration

The `newPayload` handler in `engine_handlers.c` calls `block_execute()`, which
is where the TX pipeline lives. No engine-level changes are needed — the
pipeline is entirely within `block_execute()`.

```
engine_newPayloadV4():
  block_execute(evm, header, body, ...)   ← TX pipeline runs here
  evm_state_compute_mpt_root(...)         ← MPT pipeline runs here
  return PAYLOAD_VALID
```

### Flush strategy at tip

During replay, `mpt_store_flush()` runs at checkpoint boundaries (every 256
blocks). At tip, the engine doesn't call `sync_checkpoint()` — it computes the
root per-block but doesn't flush to disk after each block.

This is intentional: flushing after every block would add ~5-10 ms of fsync
latency per block for no benefit. Deferred writes accumulate in memory. Flush
should happen:
- On finalization (CL signals `forkchoiceUpdated` with new finalized hash)
- On graceful shutdown
- Periodically during idle time (between slots)

---

## Testing Strategy

| Test | Purpose |
|------|---------|
| Chain replay (blocks 0-100K) | Correctness: same state roots |
| Chain replay (blocks 4M-4.01M) | Correctness under heavy DeFi txs |
| TSAN chain replay | Race condition detection |
| Single-tx block | Edge case: ring with 1 item |
| Empty block | Edge case: 0 txs, prep thread exits immediately |
| Decode failure mid-block | Error propagation: exec thread stops cleanly |
| Deliberately slow exec | Backpressure: ring full, prep thread spins |

---

## File Reference

| Component | Current file | Notes |
|-----------|-------------|-------|
| Block executor loop | `executor/src/block_executor.c:367-462` | Replace with ring consumer |
| TX decode + ecrecover | `executor/src/tx_decoder.c:220-600` | Called by prep thread |
| Prefetch (current) | `evm/src/evm_state.c:649-652` | CPU cache only, replace |
| transaction_t struct | `evm/include/transaction.h:63-97` | Passed through ring |
| secp256k1 context | `executor/src/tx_decoder.c:220-225` | Thread-safe after init |
| mpt_store fds | `database/src/mpt_store.c` | Need read-only accessors |
