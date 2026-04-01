# Per-Account Storage — Phase 5 Design

## Status: Draft

## Problem

Storage slots currently live in a single shared `flat_store` with 64-byte composite
keys (`addr_hash[32] || slot_hash[32]`). This causes:

1. **O(n) delete_all_storage** — self-destruct/CREATE iterates and deletes per-slot
2. **Subtree walk for storage root** — `art_mpt_subtree_hash` navigates 32-byte prefix, then hashes remaining 32 bytes
3. **32-byte addr_hash duplication** in every storage key
4. **Mixed-size keys** — accounts use 32-byte keys, storage uses 64-byte keys, requiring separate flat_stores

## Goal

Each account owns a per-account compact_art for its storage:

```
account_record {
    nonce, balance, code_hash, storage_root,
    compact_art *storage   // NULL if no storage
}
```

- `storage_root = art_mpt_hash(account.storage)` — direct hash, no subtree navigation
- `self_destruct = compact_art_destroy(storage)` — O(1)
- `delete_all_storage = destroy + recreate` — O(1)
- Storage keys become 32 bytes (just `slot_hash`)
- No filtered walks, no sentinel keys

## Blocker: compact_art Virtual Memory Reservation

Each `compact_art` instance reserves via `mmap(MAP_NORESERVE)`:
- Node pool: **16 GB** virtual address space
- Leaf pool: **32 GB** virtual address space (for 16-byte compact leaves)

Total: **48 GB per instance**. With 10K accounts having storage in a checkpoint
window, that's 480 TB of virtual address space — obviously infeasible.

Physical memory commitment is tiny (starts at 8 bytes), but the virtual address
space reservation exceeds any practical limit.

### Solution: Small-Pool Mode for compact_art

Add a `compact_art_create_small()` variant with configurable reserve sizes:

```c
compact_art_t *compact_art_create_small(
    uint32_t key_size,
    uint32_t value_size,
    size_t   node_reserve,   // e.g., 4 MB
    size_t   leaf_reserve,   // e.g., 8 MB
    bool     compact_leaves
);
```

For per-account storage:
- **Node reserve: 4 MB** — enough for ~250K internal nodes (16 bytes each)
- **Leaf reserve: 8 MB** — enough for ~500K leaves (16 bytes each)
- Most accounts have < 100 slots; a few (Uniswap, large contracts) have millions
- If an account exceeds the reserve, `mremap()` to grow (rare path)

Virtual cost for 10K active accounts: 10K * 12 MB = **120 GB** — feasible on 64-bit.
Physical cost: proportional to actual slot count.

**Alternative: Arena-backed pools**
Instead of per-instance mmap, allocate from a shared arena:
- One large mmap for all storage nodes (e.g., 64 GB reserve)
- Per-account compact_art gets a slice (offset + length)
- Requires a free-list allocator — more complex but minimal VA waste

**Recommendation**: Start with small-pool mmap per instance. Simple, correct,
and the VA usage is manageable. Switch to arena if VA pressure becomes an issue.

## Current Architecture (for reference)

```
state_overlay
  ├── acct_meta_pool: cached_account_t[]  (sequential index)
  ├── slot_meta_pool: cached_slot_t[]     (sequential index, skey[52] = addr[20]||slot_be[32])
  ├── acct_index: mem_art (addr[20] → meta index)
  ├── slot_index: mem_art (skey[52] → meta index)
  └── flat_state
        ├── flat_store (accounts): key[32]=addr_hash → account record
        └── flat_store (storage):  key[64]=addr_hash||slot_hash → slot value (compressed)

storage_trie_root(addr_hash):
  → art_mpt_subtree_hash(storage_flat_store.compact_art, addr_hash, 32, out)
  → walks 32-byte prefix, hashes remaining 32-byte slot_hash portion
```

## Target Architecture

```
state_overlay
  ├── acct_meta_pool: cached_account_t[]  (now with compact_art *storage)
  ├── slot_meta_pool: cached_slot_t[]     (key shrinks: just slot_be[32])
  ├── acct_index: mem_art (addr[20] → meta index)
  ├── slot_index: mem_art (addr[20]||slot_be[32] → meta index)  [unchanged for now]
  └── flat_state
        └── flat_store (accounts): key[32]=addr_hash → account record

Per-account storage: cached_account_t.storage = compact_art (32-byte keys)
  → created lazily on first SSTORE or when loaded from disk with non-empty storage_root
  → destroyed on self-destruct or eviction
  → persisted to disk as individual .art files or packed into a single storage file

storage_trie_root(account):
  → art_mpt_hash(account.storage)   // direct hash, no subtree walk
```

## Implementation Plan

### Step 1: compact_art Small-Pool Mode

Add `compact_art_init_small()` that takes explicit reserve sizes instead of the
giant defaults. The pool_init already takes a reserve parameter — we just need
a creation path that passes smaller values.

- Modify `pool_init()` to accept caller-specified reserve (it already does via parameter)
- Add `compact_art_create_ex()` with explicit node_reserve, leaf_reserve
- Default `compact_art_create()` uses existing large reserves (backwards compatible)
- Test: create 10K small compact_arts, insert/lookup/delete, verify no VA exhaustion

### Step 2: Per-Account Storage Art in cached_account_t

Add `compact_art_t *storage_art` field to `cached_account_t`:
- NULL when account has no storage (most accounts)
- Created lazily on first `ensure_slot()` for this account
- Key size: 32 bytes (slot_hash only — no addr_hash prefix)
- Value size: 32 bytes (raw slot value, big-endian)
- Compact leaves: yes (8-byte key hash + 8-byte value/pointer)

Lifecycle:
- `ensure_slot()`: if `ca->storage_art == NULL`, create it
- `self_destruct()`: destroy storage_art, set NULL
- `evict()`: if storage_art has no dirty entries, destroy; if dirty, flush first
- `create_account()` with `storage_cleared`: destroy and recreate

### Step 3: Route SLOAD/SSTORE Through Per-Account Art

**SLOAD** (`ensure_slot`):
1. `ensure_account()` as before
2. If `ca->storage_art == NULL` and account has storage_root != empty:
   - Load from disk: create small compact_art, populate from storage flat_store
   - (Or: lazy-load individual slots from disk into the per-account art)
3. `compact_art_get(ca->storage_art, slot_hash, 32)` to read value
4. Populate `cached_slot_t` as before

**SSTORE** (`set_storage`):
1. Same ensure_slot path
2. Update `cs->current`
3. `compact_art_insert(ca->storage_art, slot_hash, 32, value, 32)` — sync immediately
4. Mark dirty flags as before

### Step 4: Storage Root via Direct Hash

Replace `storage_trie_root()` subtree walk:

```c
// Old:
art_mpt_subtree_hash(st->mpt, addr_hash, 32, out);

// New:
art_mpt_hash(ca->storage_art, out);
```

This requires `art_mpt_hash` to work with a standalone compact_art (not one
inside a flat_store). The encode callback reads values directly from the
per-account art's leaves.

### Step 5: Eliminate Storage flat_store

Once per-account arts own all storage:
- Remove `flat_store (storage)` entirely
- Remove 64-byte composite keys
- Remove `flat_state_get_storage` / `flat_state_put_storage` / `flat_state_delete_storage`
- `flat_state_delete_all_storage` becomes `compact_art_destroy(ca->storage_art)`

### Step 6: Disk Persistence

Per-account storage arts need disk persistence across restarts:

**Option A: Packed storage file**
- Single file with all storage arts serialized sequentially
- Each account's storage section: `[slot_count][slot_hash, value]...`
- On load: deserialize into per-account compact_art
- On flush: serialize dirty accounts' storage arts

**Option B: Embed in account flat_store**
- Account record includes a pointer/offset to its storage data
- Storage data stored inline or in a companion file
- Simpler addressing but larger account records

**Option C: Keep storage flat_store for persistence only**
- In-memory: per-account compact_arts (fast, clean API)
- On disk: still use the shared storage flat_store for persistence
- On load: populate per-account art from flat_store
- On flush: sync per-account art back to flat_store
- Easiest migration path — disk format unchanged

**Recommendation**: Option C for initial implementation. Keeps disk format stable,
isolates the change to in-memory structures. Optimize disk format later.

## Risk Assessment

| Risk | Mitigation |
|------|------------|
| VA exhaustion with many accounts | Small-pool mode (12 MB/instance), monitor with /proc/self/maps |
| mremap overhead for large contracts | Pre-size reserves based on loaded slot count |
| Memory fragmentation from many small mmaps | Arena allocator as fallback (Phase 2) |
| Performance regression on storage root | Direct hash should be faster; benchmark against subtree walk |
| Disk format change | Option C avoids it entirely |

## Open Questions

1. **Slot meta pool**: Keep as-is (global array with 52-byte keys) or move meta
   into per-account art values? Moving would eliminate slot_index mem_art but
   changes the art value format.

2. **Warm storage during eviction**: If we evict an account but its storage_art
   has warm slots from the current tx, we need to handle that. Probably: never
   evict accounts with dirty/warm state.

3. **compact_art iteration**: `compute_mpt_root` currently iterates `dirty_slots`
   vector. With per-account arts, we'd iterate dirty accounts → each account's
   art. The art_mpt_hash already handles this.

4. **Memory budget**: How much physical memory do per-account arts actually use?
   Need to measure: 100 slots ≈ how many art nodes? Likely < 4 KB per account.
