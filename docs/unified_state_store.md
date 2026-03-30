# Unified State Store — Eliminating mem_art

## Problem

Current architecture has two redundant layers:

```
EVM execution
    ↓
mem_art cache (accounts + storage)     ← in-memory ART, per-entry flags, journal
    ↓
flat_store (accounts + storage)        ← compact_art index + mmap'd data + deferred buffer
    ↓
disk (mmap'd .art files)
```

This causes:
- **Double memory** for hot entries (mem_art arena + flat_store deferred buffer)
- **Complex flush dance** (flush_slot_cb, flush_account_cb, evict_cache)
- **Stale data bugs** (cached slots surviving CREATE, evict writing back stale entries)
- **Flag management complexity** (mpt_dirty, block_dirty, storage_cleared across two layers)

## Proposed Architecture

Merge mem_art into flat_store. One data structure per domain:

```
EVM execution
    ↓
flat_store (accounts)     ← compact_art index + mmap'd data + overlay + journal
flat_store (storage)      ← compact_art index + mmap'd data + overlay + journal
    ↓
disk (mmap'd .art files)
```

### compact_art leaf values

Currently: `uint64_t offset` (file offset or DEFERRED_BIT | buffer_offset)

Proposed: `uint64_t` encoding three states:
- `FILE_BIT`:     offset into mmap'd data file (cold, on disk)
- `OVERLAY_BIT`:  index into overlay array (hot, in memory)
- `DELETED_BIT`:  tombstone (entry deleted, pending disk cleanup)

### Overlay Entry

Replaces `cached_account_t` / `cached_slot_t` and the deferred buffer:

```c
typedef struct {
    uint8_t  *data;          // serialized record (same format as disk)
    uint32_t  data_len;
    uint32_t  lru_prev;      // LRU doubly-linked list
    uint32_t  lru_next;

    // Journal support
    uint8_t  *original_data; // pre-tx value (for EIP-2200 / revert)

    // Flags (replace cached_account_t flags)
    uint16_t  flags;         // DIRTY, EXISTED, CREATED, SELF_DESTRUCTED,
                             // STORAGE_CLEARED, STORAGE_DIRTY, HAS_CODE, etc.
} overlay_entry_t;
```

### Operations

**Read (ensure_account / ensure_slot)**:
1. `compact_art_get(key)` → leaf value
2. If OVERLAY_BIT: return overlay entry (hot path, no copy)
3. If FILE_BIT: read from mmap, create overlay entry, update leaf to OVERLAY_BIT
4. If not found: create empty overlay entry

**Write (set_balance, set_storage, etc.)**:
1. Ensure entry is in overlay (step above)
2. Journal old value if snapshot active
3. Update data in overlay entry
4. Set DIRTY flag

**Snapshot / Revert**:
- Snapshot: record journal position
- Revert: walk journal backwards, restore original_data for each entry

**Checkpoint (compute_mpt_root)**:
1. Iterate dirty overlay entries (dirty list, not full scan)
2. Compute storage roots for storage_dirty accounts
3. Flush dirty entries to disk (write to mmap, update leaf to FILE_BIT)
4. Compute account trie root from compact_art
5. Clear dirty flags

**LRU Eviction**:
- Only evict CLEAN overlay entries (dirty must stay until checkpoint)
- Update leaf from OVERLAY_BIT back to FILE_BIT
- Free overlay entry memory
- Can happen anytime under memory pressure

### What This Eliminates

| Current                          | Proposed                         |
|----------------------------------|----------------------------------|
| mem_art (2 instances)            | Gone                             |
| cached_account_t struct          | overlay_entry_t (generic)        |
| cached_slot_t struct             | overlay_entry_t (generic)        |
| flush_slot_cb / flush_account_cb | Direct dirty-list flush          |
| evict_cache (destroy + rebuild)  | LRU eviction (granular)          |
| Deferred buffer                  | Overlay IS the deferred buffer   |
| flat_store_flush_deferred        | Part of checkpoint flush         |
| Stale slot bugs                  | Impossible (one source of truth) |

### What This Keeps

- compact_art as the unified index (already exists)
- mmap'd data files for cold storage (already exists)
- art_mpt for hash computation (walks compact_art, unchanged)
- account_trie / storage_trie (unchanged)

### EIP-2200 Original Values

SSTORE gas calculation needs the "original" storage value (at transaction start).
Two options:

**Option A**: Store `original_data` in overlay entry. Set at commit_tx time
(original = current). On revert, restore from journal.

**Option B**: On first SLOAD/SSTORE per-tx, read from disk (the committed value).
Cache it as original. This avoids storing originals for unmodified slots.

Option A matches current code. Option B saves memory but adds a disk read.

### Per-Account Storage Trie (Future)

This design naturally extends to per-account storage compact_arts (Option A from
the earlier discussion). Each account's overlay entry could hold a pointer to
its storage compact_art. Benefits:

- Storage root = `art_mpt_hash(account.storage_art)` — no subtree walk
- Self-destruct = destroy per-account compact_art — O(1)
- No 32-byte addr_hash prefix duplication in storage keys

### Migration Path

1. Add overlay + journal to flat_store (new fields, backward compatible)
2. Move flag management from cached_account_t to overlay_entry_t
3. Redirect evm_state read/write functions to flat_store overlay
4. Remove mem_art usage from evm_state
5. Remove evict_cache flush dance
6. Add LRU eviction

Steps 1-3 can be done incrementally. Step 4 is the breaking change.

### Performance Expectations

- **Hot path (SLOAD/SSTORE)**: Same or faster. Currently: mem_art lookup (O(key_len)).
  Proposed: compact_art lookup (O(key_len)) + overlay dereference. The compact_art
  is the same ART structure, just with 4-byte refs instead of pointer-sized.

- **Checkpoint**: Faster. Currently: scan ALL cached entries (flush_slot_cb,
  flush_account_cb). Proposed: iterate dirty list only.

- **Memory**: Lower. No duplication between mem_art and deferred buffer.
  Clean overlay entries can be evicted under pressure.

- **Eviction**: Granular. Currently: destroy entire cache every 256 blocks.
  Proposed: LRU eviction of individual cold entries anytime.

## Future: Per-Account Storage (Option A)

Once the unified overlay is stable, replace the single 64-byte-key storage
flat_store with per-account storage compact_arts embedded in account entries.

### Current

```
flat_store (accounts):  32-byte keys → account_record
flat_store (storage):   64-byte keys (addr_hash[32] || slot_hash[32]) → value[32]
```

Two separate stores. Storage keys duplicate the 32-byte addr_hash prefix.
delete_all_storage walks a subtree. Storage root uses art_mpt_subtree_hash.

### Proposed

```
flat_store (accounts):  32-byte keys → { account_record, compact_art *storage }
```

Each account overlay entry owns a per-account compact_art for its storage.
Storage keys are 32 bytes (slot_hash only).

### Why per-account (not unified 64-byte key)

A single 64-byte-key compact_art was considered and rejected:

| Concern                | Unified 64B key           | Per-account 32B key      |
|------------------------|---------------------------|--------------------------|
| Account lookup speed   | 64-byte (2x slower)       | 32-byte (same as now)    |
| Storage root           | Filtered subtree walk     | `art_mpt_hash(art)` — trivial |
| Self-destruct          | Walk subtree + skip sentinel | `compact_art_destroy` — O(1) |
| delete_all_storage     | Walk subtree              | Destroy + recreate — O(1) |
| Mixed record sizes     | 104B accounts + 32B slots | Separate — no waste      |
| MPT computation        | Needs filtered walks      | Natural — matches Ethereum model |

Two sequential 32-byte lookups (account → per-account slot) equals the same
total key bytes as one 64-byte lookup, but the account lookup is shared across
BALANCE, EXTCODESIZE, CALL, etc.

### Per-Account Storage Design

```c
typedef struct {
    overlay_entry_t base;        // account data, flags, LRU
    compact_art_t  *storage;     // NULL until first SLOAD/SSTORE
    art_mpt_t      *storage_mpt; // NULL until storage root needed
} account_overlay_entry_t;
```

**SLOAD**: `compact_art_get(account.storage, slot_hash)` — 32-byte lookup.
If storage is NULL, account has no storage (return 0).

**SSTORE**: ensure storage compact_art exists (lazy create), insert slot.

**Storage root**: `art_mpt_hash(account.storage_mpt)` — hash the per-account
compact_art directly. No subtree prefix walk needed.

**Self-destruct / CREATE**: `compact_art_destroy(account.storage)` — O(1).
Set storage = NULL. No walking, no orphan cleanup.

**Persistence**: Per-account storage compact_arts serialize to a shared arena
file (single mmap). Account record stores an offset/handle to its storage
arena region. On load, deserialize lazily on first SLOAD.

### Memory at Scale

At 250M accounts, ~5M have storage. Per-account compact_art overhead:
- Empty accounts: storage = NULL, 0 bytes
- Accounts with storage: ~64 bytes struct + ART nodes for their slots
- Total: ~5M * 64B = ~320MB overhead for structs
- ART nodes: same total as current 64-byte key ART minus the addr_hash
  prefix layer (~200-500MB saved)
- Net: roughly break-even on base memory, but with granular eviction
  (evict cold accounts' storage independently)

### Implementation Order

1. Unified overlay (this document, phase 1)
2. Per-account storage compact_art (replace 64-byte key storage)
3. Lazy storage loading (deserialize from arena on first access)
4. Per-account LRU eviction (drop cold accounts' storage trees)
