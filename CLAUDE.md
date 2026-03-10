# Claude Code Project Context

## Current Work: MPT Store Integration into chain_replay

Branch: `evm-verkle-dev-2` (based on `evm-verkle-dev-1`)

### Goal
Replace the O(total_accounts) full MPT trie rebuild per block with O(dirty * trie_depth) incremental updates using `mpt_store`. This is critical for scaling past ~50K blocks where the full rebuild becomes the bottleneck.

### Architecture
Ethereum MPT has two trie levels:
1. **Account trie**: `keccak256(address)` -> `RLP(nonce, balance, storage_root, code_hash)`
2. **Per-account storage tries**: `keccak256(slot)` -> `RLP(value)` (one trie per account)

`mpt_store` is a persistent disk-backed MPT with incremental batch updates. It uses disk_hash for O(1) node indexing, size-class slot allocation (no garbage), and a 2GB LRU cache.

---

## Step 1: mpt_store for Account Trie -- DONE

- `mpt_store_t *account_mpt` in `evm_state` — incremental account trie updates
- `mpt_dirty` flag on `cached_account_t` — survives commit_tx, cleared after compute_mpt_root
- `storage_dirty` flag on `cached_account_t` — set when any storage slot changes

## Step 2: Cache Storage Roots Per Account -- DONE

- `hash_t storage_root` in `cached_account_t` — cached across blocks
- Only recomputed for accounts with `storage_dirty == true`
- Self-destruct resets to `HASH_EMPTY_STORAGE`

## Step 3: Per-Account Storage Tries via mpt_store -- DONE

- `mpt_store_t *storage_mpt` — shared store for ALL per-account storage tries
- `mpt_store_set_root()` switches between tries per account
- `mpt_store_set_shared()` disables node deletion (shared nodes across tries)
- `mpt_dirty` flag on `cached_slot_t` — tracks which slots need mpt_store update
- `compute_all_storage_roots()` collects dirty slots, groups by account, applies incremental updates
- Validated: 250K+ mainnet blocks, 0 failures, ~315 blk/s

### Key bugs fixed in Step 3
- **build_fresh double-free**: compaction must NULL moved entries' value pointers
- **merge_extension diverge path**: must collapse degenerate branches (0 or 1 children) and merge path prefixes canonically — otherwise deletes of non-existent keys create non-canonical tree structures

### Files changed (Steps 1-3)
- `evm/src/evm_state.c` — core logic (account/storage tries, flags, incremental updates)
- `evm/include/evm_state.h` — evm_state_create signature (mpt_path param)
- `database/src/mpt_store.c` — set_root, set_shared, build_fresh fix, merge_extension fix
- `database/include/mpt_store.h` — new API declarations
- `CMakeLists.txt` — link mpt_store to evm
- `tools/chain_replay.c` — pass mpt_path, clean storage_mpt files on --clean
- `executor/tests/test_block_executor.c` — NULL mpt_path
- `integration_tests/src/test_runner_core.c` — NULL mpt_path
- `evm/tests/test_evm_state_audit.c` — NULL mpt_path

---

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
