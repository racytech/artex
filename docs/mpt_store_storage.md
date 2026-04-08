# Storage Backend: Hart → mpt_store Migration Plan

## Why

Hart (in-memory ART trie) per account has scaling issues:
- 50K hot accounts × 400KB avg = 20GB RAM just for storage
- Whale contracts (Uniswap: 2.5M slots, 248MB arena) dominate
- Eviction/reload thrashing for large accounts
- `hart_root_hash` walks entire trie = 82ms/block at 12.5M slots

mpt_store (disk-backed persistent MPT):
- 6.9ms/block at same scale (12x faster)
- Near-zero RAM (OS page cache manages hot pages)
- No eviction, no LRU, no eviction file
- Nodes persist — no rebuild on restart

## Architecture

### Single shared mpt_store for ALL storage accounts

```
state_t {
    mpt_store_t *storage_mpt;    // one shared node store
    // ... no more per-account hart_t pointers
}

resource_t {
    hash_t storage_root;         // points into storage_mpt (already exists)
    // hart_t *storage → REMOVED
    // evict_count, evict_offset → REMOVED
    // lru_prev, lru_next → REMOVED
}
```

Flow:
```
SLOAD(addr, slot):
  root = resource[addr].storage_root
  mpt_store_set_root(storage_mpt, root)
  value = mpt_store_get(storage_mpt, keccak(slot))

SSTORE(addr, slot, value):
  → buffer (addr, slot_hash, rlp_value) in dirty_slots list

End of block (compute_root_ex):
  for each dirty account:
    mpt_store_set_root(storage_mpt, old_root)
    mpt_store_begin_batch()
    for each dirty slot of this account:
      mpt_store_update(slot_hash, rlp_value)
    mpt_store_commit_batch()
    mpt_store_root(storage_mpt, new_root)
    resource.storage_root = new_root
```

### Dirty slot buffer

During block execution, SSTOREs accumulate in a per-block buffer:

```c
typedef struct {
    address_t addr;
    uint8_t   slot_hash[32];
    uint8_t   value_rlp[34];  // RLP-encoded storage value
    uint8_t   rlp_len;
} dirty_slot_entry_t;

// In state_t:
dirty_slot_entry_t *dirty_slots;
uint32_t dirty_slot_count;
uint32_t dirty_slot_cap;
```

At `state_compute_root_ex` time:
1. Sort dirty_slots by address
2. Group consecutive entries by address
3. For each group: set_root → begin_batch → updates → commit → get new root

### SLOAD path (read)

Current: `hart_get(r->storage, slot_hash)` → direct memory access

New: `mpt_store_set_root + mpt_store_get` → walks trie nodes via mmap'd
page cache. Hot nodes in RAM, cold nodes paged in from NVMe.

Cost: ~2-10μs per SLOAD (page cache hit) vs ~200ns (direct hart_get).
At 2000 SLOADs/block: 4-20ms vs 0.4ms. The 4-20ms overhead is absorbed
by the 75ms savings in root computation (82ms → 7ms).

### SSTORE path (write)

Current: `hart_insert` → immediate in-memory write + dirty path marking

New: buffer the `(addr, slot_hash, value)` triple. No disk I/O during
SSTORE — all writes are deferred to batch commit.

### Root computation

Current: `hart_root_hash` per dirty account — walks entire trie, rehashes
dirty subtrees. At 10K slots: ~3ms. At 2.5M slots: ~100ms+.

New: `mpt_store_commit_batch` — only processes dirty paths. At 5 dirty
slots in a 2.5M-slot account: ~50μs (walks 5 paths × 8 levels deep).

This is where the 12x speedup comes from.

### Transaction revert (REVERT opcode)

Current: per-tx journal records old values, reverts hart_insert/delete.

New: per-tx journal records old values. On revert, remove the dirty_slot
entries for the reverted tx. No mpt_store interaction during revert —
the batch hasn't been committed yet.

### Block-level undo (reorg)

Current: block_diff records old/new values, revert writes raw values.

New: Same block_diff approach. Revert writes old storage_root values
back to resource_t. The old trie nodes still exist in mpt_store (shared
mode doesn't delete old nodes). So reverting is just swapping root hashes.

This is actually simpler than the hart approach — no need to undo
individual slot changes. Just restore the old root.

## Files on disk

Location: `<data_dir>/storage_mpt.idx` and `storage_mpt.dat`

Sizing for 3B total nodes (all storage accounts combined):
- `.idx`: 200GB sparse file (actual ~30-50GB at 70% load)
- `.dat`: ~60-80GB of RLP-encoded trie node data

Total: ~100-130GB actual disk usage.

With sparse file, the idx doesn't consume 200GB upfront.

## What gets removed

- `hart_t *storage` from resource_t
- `evict_count`, `evict_offset` from resource_t
- `lru_prev`, `lru_next`, `acct_idx` from resource_t
- `storage_evict.dat` file
- `evict_fd`, `evict_file_size`, `evict_threshold`, `evict_budget` from state_t
- `lru_head`, `lru_tail`, `lru_count`, `lru_capacity` from state_t
- `stor_arena_total`, `stor_in_memory`, `stor_evicted` from state_t
- All eviction functions (evict_one, state_reload_storage, etc.)
- All LRU functions (lru_touch, lru_push_front, lru_evict_tail)
- `state_trim_storage`, `state_compact_evict_file`
- `--storage-budget`, `--storage-cache` CLI flags

## What stays

- `resource_t.storage_root` (hash_t) — the trie root per account
- `blk_dirty` / `blk_orig_stor` — for block diff tracking
- `state_compute_root_ex` — but calls mpt_store instead of hart_root_hash
- `state_save` / `state_load` — walks mpt_store per account to serialize

## state_save / state_load

### state_save

For each account with storage:
```
mpt_store_set_root(storage_mpt, r->storage_root)
walk all leaves via mpt_store_walk_leaves()
write (slot_hash[32], value[32]) pairs to save file
```

Format unchanged — backward compatible with existing snapshots.

### state_load

For each account with storage:
```
mpt_store_set_root(storage_mpt, EMPTY_ROOT)
mpt_store_begin_batch()
for each (slot_hash, value) pair:
    mpt_store_update(slot_hash, rlp_encode(value))
mpt_store_commit_batch()
mpt_store_root(storage_mpt, new_root)
r->storage_root = new_root
```

This is the slow path (130s for 12.5M slots in the benchmark). But it
only happens once per snapshot load. Could be optimized with bulk
insertion APIs later.

## Implementation order

1. Add `mpt_store_t *storage_mpt` to state_t, open/create in state_create
2. Add dirty_slot buffer to state_t
3. Replace `storage_read` → mpt_store_get
4. Replace `state_set_storage` → buffer dirty slot
5. Replace storage root computation in `state_compute_root_ex` →
   group dirty slots by account, batch commit each
6. Update `state_save` → walk mpt_store leaves
7. Update `state_load` → batch insert into mpt_store
8. Remove hart_t *storage, eviction, LRU from resource_t and state_t
9. Update chain_replay (remove --storage-budget, --storage-cache)
10. Update tests

## Risks

1. **state_load speed**: 130s for 12.5M slots. Full chain at 16M storage
   accounts could take 30+ minutes to load. Acceptable for a one-time
   operation, but could be optimized.

2. **SLOAD latency**: 2-10μs vs 200ns. For read-heavy blocks (many
   SLOADs), this could add 20ms/block. Offset by root computation savings.

3. **disk_table sizing**: Need to estimate total node count upfront.
   Undersizing requires online resize (supported but expensive).
   Oversizing wastes address space (but sparse file = no disk waste).

4. **Shared mode compaction**: Old nodes from previous roots accumulate.
   Need periodic `mpt_store_compact_roots` with all live storage roots.
   This is O(all nodes) and could take minutes.

5. **Journal revert interaction**: The dirty_slot buffer must be cleared
   on tx revert. Need to track which entries belong to the current tx.
