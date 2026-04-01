# Per-Account Storage v2 — Clean Design

## Status: Design

## Core Idea

Each account owns its storage as a per-account compact_art. All compact_arts
share a single arena (one large mmap). No shared storage flat_store. No
slot_meta_pool. No slot_index. The per-account art IS the storage.

## Architecture

```
account compact_art (existing, one global index)
  └── leaf → account_record {
        nonce: u64
        balance: u256
        code_hash: hash
        storage_ref: arena_ref   ← points into shared storage arena
      }

shared storage arena (one mmap, bump allocator)
  ├── [account A's compact_art pools: nodes + leaves]
  ├── [account B's compact_art pools: nodes + leaves]
  └── [free space ...]

per-account compact_art (backed by arena slice):
  key = slot_hash[32]
  value = slot_value_be[32]
  → art_mpt_root_hash = storage root (always fresh, never stale)
```

## What Gets Eliminated

- Storage flat_store (chain_replay_flat_stor.art) — entirely
- slot_meta_pool — entirely
- slot_index mem_art — entirely
- storage_trie — already removed
- dirty_slots vector — per-account dirty tracking instead
- tx_dirty_slots vector — per-account
- sync_slot_to_overlay — no shared flat_store to sync to
- 64-byte composite keys — gone

## What Remains

- account flat_store (chain_replay_flat_acct.art) — accounts + storage_ref
- account compact_art + account_trie (for account MPT root)
- acct_meta_pool + acct_index (account-level metadata + lookup)
- journal (stores slot changes for revert)
- warm_slots mem_art (ephemeral per-tx, stays)
- transient mem_art (EIP-1153, stays)

## Per-Account Art Value Layout

Two options for what the compact_art leaf stores:

### Option A: Just current value (32 bytes)
```
leaf value = current_value_be[32]
```
- SLOAD: `compact_art_get(ca->storage, slot_hash)` → 32-byte value
- SSTORE: `compact_art_insert(ca->storage, slot_hash, value_be)`
- Original values: stored separately in journal or a per-tx map
- Pro: minimal leaf size, direct RLP encode
- Con: need separate structure for EIP-2200 originals

### Option B: Current + original (64 bytes)
```
leaf value = current_be[32] + original_be[32]
```
- SLOAD: read first 32 bytes
- SSTORE: update first 32 bytes, keep original
- commit_tx: copy current → original
- Pro: self-contained, no separate structure
- Con: larger leaves, encode callback must skip original

**Recommendation: Option A.** Keep per-tx originals in a small hash map
(similar to how warm_slots works). The art stays minimal and the encode
callback is trivial.

## SLOAD/SSTORE Flow

### SLOAD
```
1. Check warm_slots → was_warm
2. compact_art_get(ca->storage, slot_hash)
   → found: return value
   → not found AND ca->storage exists: slot is zero
   → ca->storage is NULL: account has no storage, return zero
3. Return value + was_warm
```

### SSTORE
```
1. Read current value (SLOAD path)
2. Read original value (from originals map, or current if first write)
3. Journal: save {slot_hash, old_current, old_in_originals_map}
4. If new value == 0:
     compact_art_delete(ca->storage, slot_hash)
   Else:
     compact_art_insert(ca->storage, slot_hash, value_be)
5. If first write to this slot this tx:
     store current as original in per-tx map
6. Mark ca->storage_dirty = true
```

### commit_tx
```
For each dirty account:
  Clear per-tx originals → they become the new originals
  (or: per-tx originals are just dropped, next tx re-reads from art)
```

### Revert
```
For each journal entry (reverse order):
  compact_art_insert or delete to restore old value
  Remove from originals map if it was added this snapshot
```

## Storage Root Computation

```
compute_mpt_root Step 4:
  For each dirty account with storage_dirty:
    art_mpt_root_hash(ca->storage_mpt, ca->storage_root.bytes)
  // That's it. No sync to flat_store. No subtree walk.
```

## Persistence (Disk)

### On eviction (flush to disk):
For each dirty account that has storage:
  1. Iterate ca->storage compact_art
  2. Write all slot_hash → value pairs to a packed storage file
  3. Record the file offset + slot count in the account record
  4. Destroy the per-account art (free arena slice)

### On reload (after eviction):
When ensure_slot needs a slot from an evicted account:
  1. Read account record → get storage file offset + count
  2. Bulk-load all slots into a new per-account compact_art
  3. Cache the art in ca->storage

### Storage file format:
```
Per-account section:
  slot_count: u32
  [slot_hash[32] + value_be[32]] × slot_count
```

Simple, sequential, bulk-readable. No index needed — the per-account
art IS the index once loaded.

## Arena Design

### Bump allocator with checkpoint reset:
```
arena {
  base: void*       (one large mmap, e.g., 64 GB virtual)
  offset: size_t    (bump pointer)
  committed: size_t (physical pages committed)
}

alloc(size):
  ptr = base + offset
  offset += align(size, 8)
  return ptr

reset():
  offset = 0
  madvise(base, committed, MADV_DONTNEED)  // release physical pages
```

No free-list, no fragmentation. All per-account arts allocate forward.
On eviction, reset to zero. Next checkpoint window starts fresh.

Pro: dead simple, zero fragmentation, excellent locality
Con: can't selectively keep hot accounts across evictions

### Alternative: size-class arena
If selective retention matters later, use size classes:
- Small: 4 KB (< 64 slots)
- Medium: 64 KB (< 1024 slots)
- Large: 1 MB (< 16K slots)
- Huge: direct mmap (Uniswap-scale)

But bump+reset is sufficient for now.

## Migration Path

1. Implement arena + arena-backed compact_art
2. Wire SLOAD/SSTORE through per-account art (already partially done)
3. Eliminate slot_meta_pool and slot_index
4. Add disk persistence (packed storage file)
5. Remove shared storage flat_store

Each step is independently testable.

## Open Questions

1. **Per-tx originals**: hash map vs parallel array vs compact_art with 64-byte values?
   → Lean toward small hash map (most txs touch < 100 unique slots)

2. **Bulk load on reload**: read all slots at once or lazy-load per-slot?
   → Bulk load — sequential disk read is fast, avoids repeated seeks

3. **Large contracts**: Uniswap has millions of slots. Loading all into a
   per-account art on first access is expensive.
   → Lazy approach: only load slots that are actually accessed.
   → But then art_mpt_root_hash can't hash slots that aren't loaded.
   → Solution: keep the old storage_root from disk for unmodified accounts.
   → Only recompute storage_root for storage_dirty accounts.
   → This is what we already do — non-dirty accounts use cached storage_root.

4. **Hot account retention**: with bump+reset, Uniswap's art is destroyed
   every checkpoint window and rebuilt. Is that a problem?
   → At 256-block intervals, Uniswap might have ~1000 dirty slots.
   → Loading 1000 slots from mmap'd file: ~64 KB sequential read = microseconds.
   → Rebuilding art from 1000 inserts: < 1ms.
   → Not a problem.
