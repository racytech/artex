# MPT Store Optimization TODO

## P0: Dirty entry list (eliminates O(total_cached) scan)

Currently `mpt_incr_update_cb` and `mpt_collect_dirty_slot_cb` iterate ALL cached
accounts/slots via `mem_art_foreach` to find the few dirty ones. At scale (200K+
cached accounts, 500K+ cached slots), this scan costs 50-70ms/block even though
only ~10-100 entries are dirty.

**Fix:** Maintain a dirty list. When `mpt_dirty` is set on an account or slot,
push it onto an intrusive linked list (or a separate vector). At `compute_mpt_root`
time, iterate only the dirty list.

**Files:** `evm/src/evm_state.c` (cached_account_t, cached_slot_t, set_* functions,
compute_all_storage_roots, mpt_incr_update_cb)

## P1: Skip disk_hash_contains when LRU cache hit

`write_node` in mpt_store always calls `disk_hash_contains` (a pread syscall) to
check if a node already exists on disk. If the node hash is in the LRU cache, it
must exist — the disk check can be skipped.

~800 nodes rewritten per block = 160-800us of syscall overhead saved.

**Files:** `database/src/mpt_store.c` (write_node)

## P1: Cache keccak256(addr) and keccak256(slot)

`keccak256(address)` is computed in both `ensure_account` (MPT read-through) and
`mpt_incr_update_cb` (MPT write). Same for `keccak256(slot)` in `ensure_slot` and
`mpt_collect_dirty_slot_cb`. Cache the result on the struct.

**Files:** `evm/src/evm_state.c` (cached_account_t: add addr_hash field,
cached_slot_t: add slot_hash field)

## P2: Shared-mode storage node compaction

Storage MPT runs in shared mode (`mpt_store_set_shared(true)`), so old trie nodes
are never deleted. Disk grows unbounded and hash bucket chains get longer over time,
degrading lookup performance.

**Fix:** Periodic `mpt_store_compact()` or reference-counted node deletion for
shared mode.

**Files:** `database/src/mpt_store.c` (delete_ref, compact logic)

## P3: Minor optimizations

- **Arena allocator for dirty values:** `mpt_store_update` mallocs per value,
  commit_batch frees all. Use arena for bulk alloc/free. (~5-10us/block)
- **Stack-allocate merge_leaf temp array:** `merge_leaf` mallocs 2-3 entries in
  hot recursive path. Use stack array with heap fallback. (marginal)
- **Hash set for def_del_cancel:** Linear scan of pending deletes. Use hash set
  for O(1) cancel. (rare worst case only)
