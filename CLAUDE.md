# Claude Code Project Context

## Current Work: MPT Store Integration into chain_replay

Branch: `evm-verkle-dev-2` (based on `evm-verkle-dev-1`)

### Goal
Replace the O(total_accounts) full MPT trie rebuild per block with O(dirty_accounts * trie_depth) incremental updates using `mpt_store`. This is critical for scaling past ~50K blocks where the full rebuild becomes the bottleneck.

### Architecture
Ethereum MPT has two trie levels:
1. **Account trie**: `keccak256(address)` -> `RLP(nonce, balance, storage_root, code_hash)`
2. **Per-account storage tries**: `keccak256(slot)` -> `RLP(value)` (one trie per account)

`mpt_store` is a persistent disk-backed MPT with incremental batch updates. It uses disk_hash for O(1) node indexing, size-class slot allocation (no garbage), and a 2GB LRU cache.

---

## Step 1: mpt_store for Account Trie -- DONE (on this branch)

### What was done
- Added `mpt_store_t *account_mpt` to `evm_state` struct
- Added `mpt_dirty` flag to `cached_account_t` -- survives `commit_tx` and `clear_block_dirty`, only cleared after `compute_mpt_root`
- Added `storage_dirty` flag to `cached_account_t` -- set when any storage slot changes
- `evm_state_create(vs, mpt_path)` -- new signature, opens/creates mpt_store when path is non-NULL
- `evm_state_compute_mpt_root()` -- incremental path when `account_mpt != NULL`:
  1. `compute_all_storage_roots()` -- single pass over all storage (shared helper)
  2. `mpt_store_begin_batch()`
  3. Iterate accounts, for each `mpt_dirty` one: RLP-encode, `mpt_store_update()` or `mpt_store_delete()`
  4. `mpt_store_commit_batch()` -> root from `mpt_store_root()`
  5. Clear `mpt_dirty` flags
  - Falls back to full batch rebuild when `account_mpt == NULL`
- All callers updated (`chain_replay.c` passes `"/tmp/chain_replay_mpt"`, tests pass `NULL`)
- All tests pass (16/16 evm_state_audit, 6/6 block_executor)

### Files changed (Step 1)
- `evm/src/evm_state.c` -- core logic
- `evm/include/evm_state.h` -- new create signature
- `CMakeLists.txt` -- link `mpt_store` to `evm`
- `tools/chain_replay.c` -- pass mpt_path
- `executor/tests/test_block_executor.c` -- NULL mpt_path
- `integration_tests/src/test_runner_core.c` -- NULL mpt_path
- `evm/tests/test_evm_state_audit.c` -- NULL mpt_path

### Remaining for Step 1
- **Run `chain_replay` on real blocks to verify roots match**
- Need: `genesis.json` (mainnet genesis alloc, flat `{ "addr": { "balance": "0x..." } }` format)
- Need: era1 files in `data/era1/` (first file already downloaded: `mainnet-00000-5ec1ffb8.era1`)
- era1 source: `https://era1.ethportal.net/mainnet-NNNNN-HASH.era1`
- Run: `./chain_replay data/era1 genesis.json 0 8191`

---

## Step 2: Cache Storage Roots Per Account -- TODO

### Problem
Even with incremental account trie updates, `compute_all_storage_roots()` still scans ALL cached storage slots every block -- O(total_slots). For accounts with unchanged storage, we're recomputing the same storage root repeatedly.

### Plan
- Add `hash_t storage_root` to `cached_account_t`
- Initialize to `HASH_EMPTY_STORAGE` on account load
- Only recompute storage_root for accounts with `storage_dirty == true`
- Cache the computed storage_root in the account struct across blocks
- Handle self-destruct: reset storage_root to `HASH_EMPTY_STORAGE`
- Once done, enable every-block root checking in chain_replay (currently skips empty blocks because full rebuild was too slow)

### Files to modify
- `evm/src/evm_state.c`:
  - Add `hash_t storage_root` field to `cached_account_t`
  - In `compute_mpt_root` incremental path: only call `compute_all_storage_roots()` for dirty-storage accounts, use cached root for others
  - In `commit_tx_account_cb`: if self-destructed, reset `storage_root` to empty
- `tools/chain_replay.c`: change `check_root` to always true (O(dirty) is fast enough)

---

## Step 3: Per-Account Storage Tries via mpt_store -- TODO (future)

### Problem
Step 2 still rebuilds storage roots from scratch (batch) for dirty accounts. An account with 10K storage slots that changes 1 slot still rebuilds the entire 10K-slot trie. At scale (contracts like Uniswap with millions of slots), this becomes the new bottleneck.

### Plan
- Design multi-trie mpt_store API: `trie_id + key -> value`
- Each account address maps to a `trie_id`
- Storage updates go to per-account trie in a shared backing mpt_store
- Incremental `storage_root` computation per account: O(dirty_slots * depth)
- Evict cold accounts' storage tries from memory (scales to billions of slots)

### Files to modify
- `mpt/src/mpt_store.c` + `mpt/include/mpt_store.h`: multi-trie support
- `database/src/disk_hash.c`: composite keys or separate namespace for per-account tries
- `evm/src/evm_state.c`: use per-account mpt_store instead of batch rebuild

---

## Build
```bash
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```
Always use Release mode -- default build has no optimizations, 4x slower.
