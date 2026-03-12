# Single-Threaded Block Processing Flow

## Overview

All block processing, state mutation, and disk I/O happen on a single thread.
MPT root computation remains parallel (read-only tree walking, no disk mutation).

Fixed window size: **8192 blocks** (matches era1 file size).

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

    3. COMPUTE MPT ROOT (parallel)
       - collect dirty accounts + storage from cache
       - sort dirty entries by key (sequential trie access)
       - account trie: mpt_store_commit_batch()
       - storage tries: parallel commit_batch per dirty account (fork/join)
       - compute root hash

    4. VALIDATE
       - compare computed root against expected (block header)
       - on mismatch: discard window, report error

    5. FLUSH (synchronous, single-threaded)
       - mpt_store_flush() for account trie
       - mpt_store_flush() for storage trie
       - code_store_flush()
       - all deferred writes go to disk
       - all deferred deletes applied
       - fsync data + index files

    6. CHECKPOINT
       - save checkpoint marker (block number, block hashes, gas total)

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

## What Changes from Current Design

| Current                          | New                              |
|----------------------------------|----------------------------------|
| `flush_bg` (background thread)   | `flush` (synchronous)            |
| Checkpoint interval: 256 blocks  | Window size: 8192 blocks         |
| Read-through: cache -> deferred -> bg_snapshot -> disk | Read-through: cache -> deferred -> disk |
| `flush_snapshot_t`, `snap_find`, `snap_del_contains` | Removed |
| Race between bg_flush and commit_batch | No races |

## Removed Concepts

- `flush_snapshot_t` — no background flush, no snapshot
- `bg_flush` pointer in `mpt_store_t` — always NULL
- `snap_find` / `snap_del_contains` — dead code
- `flush_thread_fn` / `pthread_create` for flush — not needed
- Parent deferred read-through (`ms->parent->bg_flush`) — simplified

## Kept Parallel

- **Storage trie commit**: `mpt_store_fork()` + parallel `commit_batch` per dirty account.
  Each fork has its own deferred buffer, writes merged at join time. Pure computation,
  no disk I/O during commit. This is safe because forks are read-only against disk.

- **MPT root hash computation**: read-only walk of the trie structure. No mutation.

## Flush Path (simplified)

```
mpt_store_flush(ms):
    1. Write all deferred entries to .dat (pwrite)
    2. Update .idx (disk_hash_put) for each entry
    3. Process deletes:
       - disk_hash_delete() for each deferred delete
       - Return freed slots to free lists
    4. Clear deferred buffers
    5. fsync(.dat)
    6. disk_hash_sync(.idx)
    7. Write root hash + free list heads to .dat header
```

## Error Handling

- If any block in the window fails: discard entire window, do NOT flush
- `evm_state_discard_pending()` prevents partial writes
- On restart: resume from previous checkpoint (at most 8192 blocks re-executed)
- Acceptable cost: 8192 blocks at tip ~ 1-2 minutes of re-execution

## Integrity Check

- `evm_state_mpt_integrity_check()` walks all nodes from root
- Run on startup when resuming from checkpoint
- Optional: run after each flush during development/testing

## Potential Optimizations

### Prefetching

- **Account prefetch**: when decoding a transaction, issue prefetch for sender and
  receiver account trie paths before execution starts. The trie walk touches 8-10
  cache lines per lookup — prefetching hides the DRAM latency (~60-80ns per miss).
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

- **Computed gotos**: replace switch/case opcode dispatch with computed goto table.
  Eliminates branch predictor pollution from the single indirect branch.
- **Opcode fusion**: common sequences like PUSH1+ADD, PUSH1+MSTORE, DUP1+PUSH1+EQ
  can be fused into single "super-instructions" during jump analysis.
- **Stack caching**: keep top 2-3 stack values in registers instead of memory.
  Most opcodes only touch the top few stack elements.

### I/O

- **io_uring for flush**: batch all pwrite calls into a single io_uring
  submission. Already partially implemented (`USE_IO_URING` flag).
  Reduces syscall overhead during flush.
- **mmap for read-through**: mmap the .dat file read-only for trie node lookups.
  Lets the kernel manage page cache, avoids pread syscall per node.
  Trade-off: less control over eviction, but simpler code.
- **Direct I/O for flush**: bypass page cache on writes (O_DIRECT) since
  flushed data is immediately evicted from our cache anyway.
  Avoids polluting the page cache with write-once trie nodes.

### Hashing

- **Batch keccak**: collect multiple nodes to hash, process them in a
  pipeline. Keccak's internal state fits in registers — batching amortizes
  the setup cost.
- **Hardware SHA extensions**: Zen3+ has SHA-NI. While keccak != SHA, the
  general approach of using hardware acceleration for the permutation
  rounds may apply via custom intrinsics.
