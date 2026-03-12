# Single-Threaded Block Processing Flow

## Overview

All block processing, state mutation, and disk I/O happen on a single thread.
MPT root computation remains parallel (read-only tree walking, no disk mutation).

Fixed window size: **8192 blocks** (matches era1 file size).

## Why Single-Threaded Flush

The background flush (`flush_bg`) introduced a race condition between the bg
thread writing/deleting nodes on disk and the main thread reading them via
`load_from_ref` during `commit_batch`. This caused non-deterministic node loss:
the same codebase would fail at different blocks on different runs. The
`build_fresh` fallback in `update_subtrie` silently dropped entire subtrees
when `load_from_ref` failed, corrupting the trie without any error.

Synchronous flush eliminates this class of bug entirely. The I/O overlap
from bg_flush saved ~10-15% of checkpoint overhead — not worth the
complexity and correctness risk.

## Window Lifecycle

```
for each window (8192 blocks):

    1. BEGIN WINDOW
       - evm_state_begin_block() for first block

    2. EXECUTE BLOCKS
       for each block in window:
           - decode block (header + transactions)
           - execute all transactions
           - evm_state_commit_tx() after each transaction
           - evm_state_begin_block() for next block
           - accumulate block hashes

    3. COMPUTE MPT ROOT (parallel OK)
       - collect dirty accounts + storage from cache
       - sort dirty entries by key (sequential trie access)
       - account trie: mpt_store_commit_batch()
       - storage tries: parallel commit_batch per dirty account (fork/join)
       - compute root hash

    4. VALIDATE
       - compare computed root against expected (block header)
       - on mismatch: discard window, do NOT flush

    5. FLUSH (synchronous, single-threaded)
       - mpt_store_flush() for account trie
       - mpt_store_flush() for storage trie
       - code_store_flush()
       - all deferred writes go to disk
       - all deferred deletes applied
       - fsync data + index files

    6. CHECKPOINT
       - save checkpoint marker (block number, block hashes, gas total)
       - data is fully on disk before checkpoint is written

    7. EVICT CACHE
       - evm_state_evict_cache()
       - drops all in-memory account + storage entries
       - read-through cache reloads from disk on demand
```

## Memory Budget (per window)

| Component              | Early chain  | At tip (~20M)  |
|------------------------|-------------|----------------|
| Cached accounts        | ~100 MB     | ~1 GB          |
| Cached storage slots   | ~50 MB      | ~3 GB          |
| Deferred trie nodes    | ~200 MB     | ~2-4 GB        |
| Journal                | ~100 MB     | ~1-2 GB        |
| **Total**              | **~500 MB** | **~7-10 GB**   |

Peak memory fits comfortably on any full-node machine (32 GB+).
Hot working set (interpreter + frequent accounts) fits in 64 MB L3 cache.

## Read-Through Chain (simplified)

```
load_node_rlp(hash):
    1. ncache (in-memory LRU)         → hit: return
    2. deferred buffer (not yet flushed) → hit: return
    3. disk (disk_hash_get + pread)   → hit: populate cache, return
    4. not found                      → return 0
```

No bg_flush snapshot layer. No parent deferred layer (except for fork contexts
during parallel storage root computation).

## Flush Path

```
mpt_store_flush(ms):
    1. Write all deferred entries to .dat (pwrite)
    2. Update .idx (disk_hash_put) for each written entry
    3. Process deletes:
       - disk_hash_delete() for each deferred delete
       - Return freed slots to size-class free lists
    4. Clear deferred buffers
    5. fsync(.dat)
    6. disk_hash_sync(.idx)
    7. Write root hash + free list heads to .dat header
```

All synchronous. No threads, no locks, no races.

## Parallel Storage Root (kept)

Storage trie commit uses `mpt_store_fork()` + parallel `commit_batch` per
dirty account. Each fork has its own deferred buffer. Reads go through:
own deferred → parent deferred → disk. Writes go to thread-local buffer only.

This is safe because:
- Parent store is not mutated during fork computation
- Each fork operates on a disjoint storage trie (different root)
- pread on shared fd is POSIX-safe
- Merge is single-threaded after all forks join

## Error Handling

- If any block in the window fails: discard entire window, do NOT flush
- `evm_state_discard_pending()` prevents partial writes
- On restart: resume from previous checkpoint (at most 8192 blocks re-executed)
- Acceptable cost: 8192 blocks at tip ~ 1-2 minutes of re-execution
- Integrity check on resume: `evm_state_mpt_integrity_check()` walks all
  nodes from root, aborts if any node is missing from disk

## Current Hot Path Analysis

### Interpreter (evm/src/interpreter.c) — Well Optimized

Already has: computed goto dispatch, inline PUSH/DUP/SWAP, bswap64 MLOAD/MSTORE,
O(1) JUMPDEST bitmap, pre-allocated 32KB stack (fits L1), `__builtin_expect` on
gas checks. Single translation unit for full inlining. Not much to improve
without JIT/AOT.

### State Access (evm/src/evm_state.c) — Partially Optimized

**`cached_account_t` layout (DONE — reordered for cache-line alignment):**
```
Offset  Field            Size   Access Frequency
  0     nonce             8B    HOT (every tx)
  8     [pad]             8B    alignment
 16     balance          32B    HOT (every tx)
 48     dirty             1B    HOT (every mutation)
 49     block_dirty       1B    HOT
 50     existed           1B    warm
 51     mpt_dirty         1B    warm
 52     storage_dirty     1B    warm
 53     has_code          1B    warm (CALL)
 54     created           1B    warm
 55     self_destructed   1B    cold
 56     code_dirty        1B    cold
 57     block_code_dirty  1B    cold
 58     *code             8B    cold
 66     code_size         4B    warm (EXTCODESIZE)
--- cache line 2+ (cold) ---
 76     addr             20B    rare (key only)
 96     code_hash        32B    cold (CREATE only)
128     storage_root     32B    cold (compute_mpt_root only)
160     addr_hash        32B    cold (MPT key only)
```
All hot fields (nonce, balance, flags) now fit in the first 64-byte cache
line. Every state mutation touches 1 cache line instead of 3.

**`ensure_account` (DONE — uses `mem_art_upsert`):**
- Single ART traversal: `mem_art_upsert()` combines insert + get_mut.
  Eliminates the previous double lookup (`mem_art_insert` + `mem_art_get_mut`).

**`ensure_slot` (DONE — uses `mem_art_upsert`):**
- Same pattern applied to slot cache lookups.

**`ensure_slot` always builds full 52-byte key:**
- `make_slot_key(addr, slot, skey)` runs before cache lookup.
  Could check cache with addr prefix first, build full key only on miss.

**Dirty tracking is good:**
- Idempotent push to dirty vectors, O(dirty) not O(total) at
  compute_mpt_root time.

### MPT Store (database/src/mpt_store.c) — Several Bottlenecks

**`def_del_cancel` (DONE — hash-indexed O(1) lookup):**
- Was O(N) linear scan, causing O(N²) total in commit_batch.
  Now uses bucket-chained hash table (`def_del_ht[]`) matching the
  `def_ht` pattern. Cancel is O(1) with swap-and-shrink compaction.

**Individual malloc per deferred node RLP:**
- `def_append` calls `malloc(rlp_len)` + `memcpy` for each node.
  1000 nodes per commit = 1000 malloc calls. Arena bump allocator
  would eliminate this — one bulk free at flush time.

**Individual pwrite per node in flush (lines 1304-1340):**
- Each deferred entry gets its own `pwrite()` syscall.
  Could batch with `writev()` or io_uring.

**`dirty_cmp` (DONE — bswap64 prefix comparison):**
- Was `memcmp(a->nibbles, b->nibbles, 64)` for every comparison.
  Now compares first 8 bytes as `uint64_t` via `__builtin_bswap64` (single
  compare instruction), falls back to `memcmp(+8, 56)` only on prefix tie.
  Most comparisons resolve in the fast path.

**Free list heads capped at 502 per size class:**
- `write_header` packs at most 502 free offsets into the .dat header.
  Beyond that, slots are silently lost. Becomes padding waste until
  compaction reclaims.

## Concrete Improvements (by impact)

| Priority | Change | Impact | Complexity | Status |
|----------|--------|--------|------------|--------|
| 1 | `def_del_cancel` → hash set | Eliminates O(N²) in commit_batch | Low | **DONE** |
| 2 | Arena for deferred RLP | Eliminates ~1000 malloc/free per flush | Medium | TODO |
| 3 | Reorder `cached_account_t` | Pack hot fields in 1 cache line | Low | **DONE** |
| 4 | Insert-or-get in ensure_account | Eliminates double ART lookup on miss | Low | **DONE** |
| 5 | Batch pwrite in flush | `writev` or io_uring, ~1000→1 syscalls | Medium | TODO |
| 6 | Lazy slot key build | Skip 52-byte concat on cache hit | Low | TODO |
| 7 | Faster dirty_cmp | uint64 prefix compare before full memcmp | Low | **DONE** |

## Chain Replay Metrics

`chain_replay` reports **windowed** blk/s and tps (measured over each
CHECKPOINT_INTERVAL window, not cumulative). This gives accurate throughput
for the current chain region rather than averaging over the entire run.

## Potential Optimizations

### Prefetching

- **Account prefetch**: when decoding a transaction, issue `__builtin_prefetch`
  for sender and receiver account trie paths before execution starts. The trie
  walk touches 8-10 cache lines per lookup — prefetching hides DRAM latency
  (~60-80ns per miss on Zen3+).
- **Storage prefetch**: SLOAD targets are often predictable from contract ABI
  (first 4 bytes of calldata map to known storage layouts). For top contracts
  (Uniswap, USDT, WETH), the slot patterns are well-known.
- **Block-level prefetch**: scan all transactions in a block, collect unique
  sender/receiver addresses, batch-prefetch their trie paths before executing
  any transaction.

### Memory Layout

- **Cache-line alignment**: pack `cached_account_t` fields accessed together
  (nonce, balance, flags) into the same 64-byte cache line. Move cold fields
  (code hash, storage root, journal pointers) to a separate region.
- **Arena allocation**: per-window arena for trie nodes, journal entries, and
  deferred buffers. One bulk free at evict time instead of thousands of
  individual malloc/free calls. Reduces allocator overhead and fragmentation.
- **SoA vs AoS**: for batch operations (commit_batch, flush), structure-of-arrays
  layout may be better than array-of-structures — allows vectorized processing
  of hashes, offsets, and lengths separately.

### Interpreter

- Computed gotos already implemented — further gains require:
- **Opcode fusion**: common sequences like PUSH1+ADD, PUSH1+MSTORE, DUP1+PUSH1+EQ
  can be fused into single "super-instructions" during jump analysis pass.
- **Stack caching**: keep top 2-3 stack values in registers instead of memory.
  Most opcodes only touch the top few stack elements.

### I/O

- **io_uring for flush**: batch all pwrite calls into a single io_uring
  submission. Reduces ~2000 syscalls to ~2 per flush. Already partially
  implemented (`USE_IO_URING` flag). See `docs/parallelism.md` for details.
- **mmap for read-through**: mmap the .dat file read-only for trie node lookups.
  Lets the kernel manage page cache, avoids pread syscall per node.
  Trade-off: less control over eviction, but simpler code.
- **Direct I/O for flush**: bypass page cache on writes (O_DIRECT) since
  flushed data is immediately evicted from our cache anyway. Avoids polluting
  the kernel page cache with write-once trie nodes.

### Hashing

- **Batch keccak**: collect multiple nodes to hash, process them in a pipeline.
  Keccak's internal state fits in registers — batching amortizes setup cost.
- **SIMD permutation**: Zen3+ AVX2 can parallelize parts of the keccak-f
  permutation. The fused permutation with precomputed round constants
  (already implemented) is a good baseline to build on.
