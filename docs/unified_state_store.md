# Unified State Store

## Status: Phase 2 Complete (overlay-dev-0 branch)

### What's Done

**Phase 1: flat_store overlay pool** (replaces deferred buffer)
- Per-entry overlay with dirty tracking and LRU links
- `OVERLAY_BIT` encoding in compact_art leaf values
- `flat_store_ensure_overlay` / `overlay_record` / `mark_dirty` API
- No duplicates by design (one entry per key)
- `flat_store_evict_clean` for granular LRU eviction

**Phase 2: state_overlay without mem_art** (replaces evm_state.c internals)
- `state_overlay.c` (~1400 lines): full EVM state API
- `evm_state.c` (~300 lines): thin forwarder (public API unchanged)
- Meta sidecar arrays (`account_meta_pool`, `slot_meta_pool`) for typed access
- Deferred sync: mutations update meta only, flat_store synced at checkpoint
- compute_mpt_root simplified: promote → delete orphans → sync dirty → hash
- No full-flush (old Steps 3+5 eliminated)

### Current Architecture

```
EVM execution
    ↓ typed access (nonce, balance, flags)
state_overlay meta arrays (account_meta_pool, slot_meta_pool)
    ↓ sync dirty entries at checkpoint
flat_store overlay (compressed records, in-memory)
    ↓ flush to disk at checkpoint/evict
flat_store data file (mmap'd .art files)
    ↓ art_mpt walks compact_art directly
account_trie / storage_trie → MPT root hash
```

### Remaining mem_art Usage

| Structure | Purpose | Can be eliminated? |
|-----------|---------|-------------------|
| `acct_index` | addr[20] → meta index | Yes: use flat_store overlay with phantom flag |
| `slot_index` | skey[52] → meta index | Yes: same approach |
| `warm_addrs` | EIP-2929 warm addresses | No: ephemeral per-tx, not persisted |
| `warm_slots` | EIP-2929 warm slots | No: ephemeral per-tx |
| `transient` | EIP-1153 transient storage | No: ephemeral per-tx |

### Test Status

~1873 failures across state_tests (0 on mpt-arena-dev-0 baseline):
- Homestead, Byzantium, Istanbul: **pass fully**
- Constantinople: 7 (CREATE2+REVERT edge case)
- London: 5 (EIP-1559 valid tx)
- Shanghai: 3 (stack overflow)
- Berlin: 590, Cancun: 480, Frontier (Berlin+ variants): 788

Root cause: sync flow between meta and flat_store. Accounts written to
flat_state before `existed` promotion pollute the trie. Need systematic
debugging of the sync → promote → delete → trie-hash flow.

## Remaining Work

### Phase 3: Eliminate meta duplication
- Store typed fields directly in overlay entry (not separate meta array)
- One allocation per account instead of two (meta + overlay)
- Encode callback serializes from typed struct on-the-fly

### Phase 4: Eliminate acct_index / slot_index mem_arts
- Add phantom flag to flat_store overlay entries
- Phantom entries: in overlay (for typed access) but not in compact_art (not in trie)
- art_mpt skips phantoms during hash computation
- Removes last non-ephemeral mem_art usage

### Phase 5: Per-account storage (Option A)
Each account owns a per-account compact_art for its storage:

```
flat_store (accounts): 32-byte keys → { account_record, compact_art *storage }
```

Benefits:
- Storage root = `art_mpt_hash(account.storage_art)` — trivial
- Self-destruct = `compact_art_destroy` — O(1)
- delete_all_storage = destroy + recreate — O(1)
- No 32-byte addr_hash prefix duplication in storage keys
- No sentinel keys, no mixed sizes, no filtered walks

### Phase 6: LRU eviction policy
- Evict cold overlay entries under memory pressure
- Only clean entries (dirty must stay until checkpoint)
- Keeps hot set in memory, cold on disk
- At 250M accounts: ~100K hot vs 250M total — 2500x reduction

### Phase 7: Eliminate full-flush in evict_cache
- Currently evict flushes ALL cached entries to disk
- With deferred sync, only dirty entries need flushing
- Clean entries already backed by disk — just drop meta

## Key Bugs Found During Development

### flat_store
- Deferred buffer duplicates: same key written multiple times created orphaned file slots
- Fix: free old file slot in `put()` when overwriting disk entry
- Fix: `flush_deferred` collects from compact_art iterator (dedup), not buffer scan

### EIP-161 (Spurious Dragon)
- Mass prune wrong: Ethereum doesn't mass-prune at SD, only prunes touched accounts
- Fix: per-tx pruning in `commit_tx` with `prune_empty` flag from block_executor
- Checkpoint window spanning SD boundary: pre-SD touched accounts must not be pruned
- Fix: `commit_tx` uses per-block `prune_empty` value (set before each block)

### State management
- `evm_state_exists` must check `dirty || created || block_dirty` (not just `existed`)
- Stale slots after CREATE: cached slots from before CREATE survive in cache
- Fix: `flush_slot_cb` skips `storage_cleared && !mpt_dirty` slots
- JOURNAL_TRANSIENT_STORAGE must store slot key in storage union member
- `sync_account_to_overlay` must skip empty non-existed accounts (precompile touches)
- `flat_store_ensure_overlay` for NOT FOUND must NOT insert into compact_art

## SD Boundary (block 2,675,200)

Still failing on mpt-arena-dev-0 with root `0xcba578...`. The SD mismatch
predates the overlay refactor. Analysis suggests it's in the interaction
between flush ordering and EIP-161 at the SD checkpoint boundary.
Will revisit after overlay is stable and producing correct roots.
