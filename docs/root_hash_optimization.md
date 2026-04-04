# Root Hash Optimization Plan

## Two Modes

### Per-block (chain tip)
- Validates every block for live consensus
- Small dirty set (~100-1000 accounts per block)
- `state_compute_root()` — current code, full fidelity
- Latency matters: must complete before next block arrives (~12s)

### Interval (sync/replay)
- Validates every N blocks (default 256) during historical replay
- `state_compute_root_ex(compute_hash=true)` on checkpoint blocks
- `state_finalize_block()` on non-checkpoint blocks (prune only, skip hash)
- Throughput matters: blocks/second during catchup

---

## Current Bugs (interval mode)

### Stale storage roots
`state_finalize_block` clears `ACCT_STORAGE_DIRTY` and `blk_dirty` every block.
At checkpoint, accounts modified in non-checkpoint blocks have stale `storage_root`
values. `acct_trie_encode` reads stale roots → wrong account trie hash.

**Fix**: `state_finalize_block` must NOT clear `ACCT_STORAGE_DIRTY` or `blk_dirty`.
Let them accumulate until the checkpoint block processes the full dirty set.
Only clear after `state_compute_root_ex(compute_hash=true)`.

### Dead account pruning across intervals
Phantoms/destructed/pruned lists accumulate correctly (not cleared per block).
But `blk_dirty`-based pruning (step 2 of compute_root) only sees the last block.
Accounts deleted in non-checkpoint blocks may not get pruned from the trie.

**Fix**: same as above — accumulate `blk_dirty` across the interval.

---

## Optimizations — Per-block Path

### 1. Single-pass dirty processing (low impact)
Current: 3 passes over `blk_dirty` (storage roots, trie update, flag clear),
each calling `find_account` (keccak + hart_get).
With small dirty sets (~1000), this is <1ms. Not worth the complexity.

### 2. Cache addr_hash in account struct (medium impact)
`keccak256(addr)` is computed on every `find_account`, `mark_dirty`, `ensure_account`.
Caching the 32-byte addr_hash in `account_t` eliminates repeated hashing.
Cost: +32 bytes per account (80→112 bytes). With 30M accounts: +960MB.
**Verdict**: too expensive in memory. Keep computing on demand.

### 3. Pre-compute addr_hash in dirty list (low impact)
Store `addr_hash` alongside `addr` in `blk_dirty`. Avoids recomputing during
`state_compute_root`. Cost: +32 bytes per dirty entry, ~32KB per block.
**Verdict**: marginal benefit for per-block mode. More useful for interval mode.

---

## Optimizations — Interval Path

### 4. Fix dirty accumulation (critical, correctness)
See bugs section above. Must fix before any performance work.

### 5. Deduplicate blk_dirty across blocks (medium impact)
With 256 blocks accumulated, an account modified 100 times appears 100 times
in `blk_dirty`. The processing loop calls `find_account` + checks flags for
each duplicate. Deduplicate on insert (use a set/bitmap) or sort+dedup before
processing.
**Verdict**: worth doing once dirty accumulation is fixed.

### 6. Parallel storage root computation (high impact)
The most expensive part of `state_compute_root` is step 1: computing storage
roots for dirty accounts. Each `hart_root_hash(r->storage, ...)` is independent.
With 256 blocks of dirty accounts, there could be thousands of storage tries
to rehash. These can be computed in parallel across threads.
**Verdict**: significant win, especially post-DoS with many storage accounts.

### 7. Batch keccak for address hashing (medium impact)
`SAFE_DELETE_IDX` and dirty processing both call `hash_keccak256(addr, 20)`.
These are independent single hashes — batch 8 at a time with AVX-512.
**Verdict**: moderate win for large dirty/dead sets.

---

## Optimizations — hart_root_hash Itself

### 8. Account RLP caching (high impact)
`acct_trie_encode` rebuilds [nonce, balance, storage_root, code_hash] RLP from
scratch every time a leaf is visited. For the account trie with 30M+ entries,
this is called for every dirty leaf.
Cache the encoded RLP in the account struct or resource struct. Only rebuild when
the account actually changes (nonce, balance, storage_root, code_hash modified).
**Verdict**: saves RLP construction + potentially avoids leaf re-hash if the
encoded value hasn't changed (same leaf hash as before).

### 9. Reduce hart tree depth for MPT (structural)
Hart uses one byte per level = 32 levels for 32-byte keys. Each level splits
into 2 MPT nibble levels (hi/lo). So the MPT hash computation traverses 64
logical levels. With path compression in the MPT (single-child nodes become
extensions), many levels are collapsed, but the hart traversal still visits
each byte level.
No easy fix — this is inherent to the ART→MPT mapping.

### 10. Lazy storage root computation (high impact)
Currently `state_compute_root` computes ALL dirty storage roots in step 1,
then hashes the account trie in step 3. But `acct_trie_encode` only reads
`storage_root` when hart_root_hash visits a dirty leaf.
Instead: compute storage root lazily inside `acct_trie_encode` when needed.
This avoids computing storage roots for accounts that end up getting pruned
or deleted before the trie walk reaches them.
**Verdict**: saves work for accounts that are created+destroyed within the
256-block window (common during DoS blocks).

### 11. SIMD keccak for independent hashes (low impact for tree hashing)
AVX-512 8-way keccak tested: ~5% slower for tree hashing due to sequential
dependency chain. Useful only for truly parallel workloads (batch address
hashing, parallel storage roots).

### 12. Scalar keccak unrolling (no impact)
Tested: compiler with -O3 -flto already optimizes the 24-round loop.
Manual unrolling increased icache pressure, was slower.

---

## Priority Order

1. **Fix dirty accumulation bug** (correctness, interval mode)
2. **Parallel storage root computation** (high impact, interval mode)
3. **Account RLP caching** (high impact, both modes)
4. **Lazy storage root in acct_trie_encode** (high impact, interval mode)
5. **Deduplicate blk_dirty** (medium impact, interval mode)
6. **Batch keccak for address hashing** (medium impact, interval mode)
