# Claude Code Project Context

## Current Work: MPT Store Integration into chain_replay

Branch: `evm-verkle-dev-3`

### Status
- Incremental MPT root computation via `mpt_store` — DONE (account trie + per-account storage tries)
- Read-through caching, cache eviction, deferred writes — DONE
- 500K+ mainnet blocks validated, 44,039 state tests + 47,589 blockchain tests pass

---

## TODO: Deferred Writes for Verkle Value Store (disk_hash)

The MPT path already has deferred writes in `mpt_store.c` — nodes are buffered in memory and flushed to disk at checkpoint time, avoiding per-update pwrite() syscalls. The Verkle path (`verkle_state`) writes to `disk_hash` immediately on every state change. It needs the same deferred write pattern:

- Buffer disk_hash puts in memory, flush at checkpoint boundaries
- This also enables non-per-block root computation: accumulate dirty state across multiple blocks, compute the Verkle root once per batch (e.g., every 256 blocks) instead of every block
- Currently `compute_state_root_ex` is called every block from `block_executor.c` — for Verkle this flushes all dirty state and computes the root. Should be restructured to match the MPT pattern where root computation frequency is decoupled from block execution

## Nice to Have: Pipelined Root Computation

### Idea
Compute state roots in a background thread while executing the next batch of blocks. Instead of blocking execution on root computation every 256 blocks, pipeline them:

1. Execute 256 blocks → dirty state accumulates in cache
2. Copy dirty entries into a standalone buffer (O(dirty) — cheap)
3. Clear dirty flags, hand buffer to background thread
4. Main thread starts executing next 256 blocks immediately
5. Background thread: computes storage roots, updates mpt_store, gets root hash
6. When done: validate root, evict clean cache entries, save checkpoint

### Prerequisites
- Steps 1-3 complete (DONE)
- Batch root computation every 256 blocks — dirty flags already accumulate naturally

---

## Build
```bash
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```
Always use Release mode -- default build has no optimizations, 4x slower.
