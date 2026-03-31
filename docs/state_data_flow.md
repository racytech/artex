# State Data Flow

## Architecture

```
┌─────────────────────────────────────────────┐
│  EVM execution                              │
│  (set_balance, set_nonce, set_storage, ...) │
└──────────────┬──────────────────────────────┘
               │ typed read/write
               ▼
┌─────────────────────────────────────────────┐
│  Meta Pool (cached_account_t / cached_slot_t)│
│  - Single source of truth during execution   │
│  - Indexed by sequential next_acct_idx       │
│  - Lookup via mem_art: addr[20] → meta_idx   │
│  - Journal tracks old values for revert      │
└──────────────┬──────────────────────────────┘
               │ bulk flush (only at compute_mpt_root)
               ▼
┌─────────────────────────────────────────────┐
│  flat_store overlay (compressed records)     │
│  - Written by sync_account/slot_to_overlay   │
│  - LRU eviction for clean entries            │
│  - compact_art leaf = OVERLAY_BIT | idx      │
└──────────────┬──────────────────────────────┘
               │ flush_deferred (at evict)
               ▼
┌─────────────────────────────────────────────┐
│  flat_store disk (mmap'd .art files)         │
│  - compact_art leaf = file offset            │
│  - art_mpt walks compact_art for trie hash   │
│  - encode callback reads from here           │
└─────────────────────────────────────────────┘
```

## Per-Mutation Flow (e.g. set_balance)

```
1. ensure_account(addr)
   → check mem_art acct_index for cached meta
   → if miss: allocate meta[next_acct_idx++]
   → load from flat_state if on disk (decode compressed → typed)
   → register in acct_index mem_art

2. Save old value to journal (for snapshot/revert)

3. Update typed field: ca->balance = new_value

4. Mark dirty: ca->dirty=true, ca->block_dirty=true

5. Mark mpt_dirty: ca->mpt_dirty=true
   → push addr to dirty_accounts vector (once per account)

6. NO sync to flat_store (removed in Phase 3)
```

## Snapshot / Revert Flow

```
snapshot():
  return journal_len (position marker)

revert(snap_id):
  walk journal backwards from current to snap_id:
    JOURNAL_NONCE:   restore ca->nonce, dirty flags
    JOURNAL_BALANCE: restore ca->balance, dirty flags
    JOURNAL_CODE:    restore ca->code, code_hash, has_code
    JOURNAL_STORAGE: restore cs->current, mpt_dirty
    JOURNAL_ACCOUNT_CREATE: restore full account snapshot
    JOURNAL_SELF_DESTRUCT:  restore self_destructed flag
    JOURNAL_WARM_*:  remove from warm_addrs/warm_slots
    JOURNAL_TRANSIENT: restore old transient value

  NO sync to flat_store on revert (removed in Phase 3)
  Meta is the only copy — reverting meta is sufficient
```

## Commit TX Flow (per transaction)

```
commit_tx():
  for each tx_dirty_account:
    if self_destructed:
      → zero balance/nonce/code, existed=false
      → mark storage_cleared, storage_dirty
    else:
      → promote existed=true if non-empty
      → EIP-161: if prune_empty && empty && touched → existed=false
    → clear dirty, created, code_dirty flags

  for each tx_dirty_slot:
    if belongs to self-destructed account:
      → zero current/original
    else:
      → cs->original = cs->current (commit for EIP-2200)
      → clear dirty flag

  clear tx_dirty vectors
  reset journal
  clear warm_addrs, warm_slots, transient
```

## Commit (per block) Flow

```
commit():
  for all cached slots:
    cs->original = cs->current
    cs->dirty = false

  for all cached accounts:
    promote existed=true if non-empty
    clear created, dirty, code_dirty, self_destructed
```

## Compute MPT Root Flow (at checkpoint)

```
compute_mpt_root(prune_empty):

  Step 1. Promote existed on dirty accounts
    for each dirty_accounts entry:
      if block_dirty and not self_destructed:
        existed = true (unless empty + not-created + prune_empty)

  Step 2. Delete orphaned storage + dead accounts
    for each dirty_accounts entry:
      if !existed or storage_cleared:
        flat_state_delete_all_storage(addr_hash)
      if !existed:
        flat_state_delete_account(addr_hash)

  Step 3. Bulk flush dirty slots to flat_state  ← ONLY SYNC POINT FOR SLOTS
    for each dirty_slots entry:
      if mpt_dirty and account existed:
        sync_slot_to_overlay(ca, cs)
          → flat_state_put_storage or flat_state_delete_storage
          → flat_store_put → compact_art_insert (marks trie dirty)

  Step 4. Compute storage roots
    for each dirty_accounts entry:
      if storage_dirty and existed:
        storage_trie_root(addr_hash) → ca->storage_root
          → art_mpt subtree hash reads from flat_store

  Step 5. Bulk flush dirty accounts to flat_state  ← ONLY SYNC POINT FOR ACCOUNTS
    for each dirty_accounts entry:
      if mpt_dirty and existed:
        sync_account_to_overlay(ca)
          → encode {nonce, balance, code_hash, storage_root} → compressed
          → flat_state_put_account → flat_store_put
          → compact_art_insert (marks trie dirty)

  Step 6. Compute account trie root
    account_trie_root()
      → art_mpt walks compact_art
      → encode callback: flat_store_read_leaf_record → decode → RLP
      → incremental: only re-hashes dirty paths

  Step 7. Clear dirty flags
    mpt_dirty = false, block_dirty = false, storage_dirty = false
    (dirty_accounts vector NOT cleared — accumulates across blocks)
```

## Evict Flow (at checkpoint boundary, after compute_mpt_root)

```
evict():
  1. flat_store_flush_deferred (accounts + storage)
     → writes dirty overlay entries to disk
     → compact_art_insert(key, file_offset) replaces OVERLAY_BIT entries
     → marks overlay entries clean

  2. flat_store_evict_clean (accounts + storage)
     → drops clean overlay entries (LRU tail first)
     → frees overlay data buffers

  3. Free code pointers in meta

  4. Zero meta arrays

  5. Reset next_acct_idx / next_slot_idx to 0

  6. Destroy + reinit acct_index / slot_index mem_arts

  7. Clear dirty_accounts / dirty_slots vectors

  8. Reset journal
```

## After Evict: Next Block

```
ensure_account(addr):
  → mem_art miss (index was rebuilt empty)
  → allocate fresh meta entry
  → flat_state_get_account(addr_hash)
    → flat_store_get → compact_art lookup → file offset → read from disk
    → decode compressed → fill typed fields
  → ca->existed = true (was on disk)
  → register in acct_index mem_art
```

## Key Invariants

1. **Meta is the single source of truth** during execution (between evictions)
2. **flat_store is the single source of truth** on disk (between sessions)
3. **Sync happens only at compute_mpt_root** (Steps 3 and 5) — not per-mutation
4. **Encode callback reads from flat_store** (overlay or disk) — never from meta
5. **Eviction is safe** only after compute_mpt_root has flushed all dirty meta
6. **dirty_accounts accumulates** across blocks — NOT cleared until evict

## Known Issue: Eviction Bug

With validate-every-256 (eviction every 256 blocks), chain_replay fails at
block 46848 (second checkpoint). With validate-every-1 (eviction still every
256 blocks), it passes 520K+ blocks. The per-block validation forces
compute_mpt_root every block (flushing dirty meta). The 256-block batch flush
produces a different root than expected.

The data flow is correct for the steady-state case. The bug is likely in how
the incremental art_mpt cache interacts with the bulk flush — specifically,
compact_art dirty flags set by flat_store_put in Step 5 may interfere with
the subsequent account_trie_root in Step 6, or flush_deferred in evict may
corrupt the trie cache state for the next checkpoint window.
