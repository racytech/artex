# Pre-Relaunch TODO

Changes to make before the next fresh chain_replay from genesis.
Grouped by priority. All changes require a clean restart (state rebuilt).

## Must Do (correctness + architecture cleanup)

### 1. Remove all synchronous flush/sync calls
- `code_store_flush` → skip `msync(MS_SYNC)` and `disk_hash_sync` [code_store.c:615-616]
- `write_free_overflow` → remove `fsync(fd)` [mpt_store.c:351]
- `state_history` → remove `fdatasync` calls [state_history.c:372-373, 425-426, 579, 747-748]
- Let OS page cache handle all writeback
- See `docs/sync_overhead.md` for full call chains

### 2. Remove flat_state from codebase
- Disabled in chain_replay (`cfg.flat_state_path = NULL`)
- Dead code, causes confusion
- Remove: `database/src/flat_state.c`, `database/include/flat_state.h`
- Remove all references in `sync.c`, `evm_state.c`, `CMakeLists.txt`

### 3. Add code archive to history directory
- Separate `code_archive.dat` + `code_archive.idx` alongside state_history
- Stores `code_hash[32] → bytecode` (deduplicated, append-only)
- Written during chain_replay when new code_hash appears in block diff
- Makes state_reconstruct self-contained (no dependency on live code_store)

### 4. Skip rwlock in disk_hash for single-threaded mode
- Add `disk_hash_set_single_threaded(dh, true)` flag
- Bypasses `pthread_rwlock_rdlock/wrlock` on every get/put
- Chain_replay is single-threaded — millions of wasted lock ops per checkpoint
- ~20-40ns per op × millions = ~200ms overhead per 256-block window

## Should Do (performance)

### 5. Merge warm check into ensure_slot
- SLOAD/SSTORE do two ART lookups with same 52-byte key (warm check + storage access)
- Add `bool warm` to `cached_slot_t`, check on first access
- One ART lookup instead of two — 5-10% on storage-heavy blocks
- See `docs/evm_optimizations.md` item #1

### 6. Cache JUMPDEST bitmap by code_hash
- Every `evm_interpret` call does `calloc` + linear scan + `free` for bitmap
- Nested CALLs to same contract rebuild the same bitmap repeatedly
- Add `jumpdest_cache` (hash map: code_hash → bitmap) to evm_t
- 3-8% on blocks with many nested calls
- See `docs/evm_optimizations.md` item #2

### 7. Short-circuit CALL to EOA (code_size == 0)
- When CALL targets an account with no code, skip `evm_interpret` entirely
- Currently allocates jumpdest bitmap, sets up context, returns immediately
- Just do value transfer and return success
- 2-5% on transfer-heavy blocks
- See `docs/evm_optimizations.md` item #3

### 8. Partial cache eviction
- Currently `evm_state_evict_cache` destroys both ART trees every 256 blocks
- Next 256 blocks start fully cold — every account reloaded from MPT
- Keep hot accounts/slots across checkpoints (top K by access count)
- 10-20% improvement — the biggest single optimization
- See `docs/evm_optimizations.md` item #7

### 9. Resolve fork gas constants once per block
- SLOAD/SSTORE evaluate 5-branch if-else on `evm->fork` every execution
- Fork is constant for entire block — resolve once in `evm_set_block_env`
- 1-2% on storage-heavy blocks
- See `docs/evm_optimizations.md` item #5

## Nice to Have (future)

### 10. Remove verkle branch from DISPATCH when verkle disabled
- `DISPATCH()` macro tests `verkle_chunk_mode` on every instruction fetch
- Compile two interpreter versions or `#ifdef` it out
- 1-3% overall
- See `docs/evm_optimizations.md` item #4

### 11. Hot node cache in mpt_store
- LRU for top trie nodes (root + top 2-3 levels)
- Would eliminate ~60% of disk_hash lookups
- 5-10% improvement
- See `docs/evm_optimizations.md` item #9

### 12. AVX-512 Keccak (batched)
- Single-message keccak is already fast (211 ns scalar)
- AVX-512 wins only for 4-way batched hashing
- Needs batched hashing API for MPT node hashing
- See `docs/evm_optimizations.md` items #6, #12
