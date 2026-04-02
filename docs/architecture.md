# State Backend Architecture

Living document. Updated incrementally.

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EVM (interpreter)                              │
│  SLOAD/SSTORE ──► evm_state_get/set_storage()                         │
│  CREATE/CALL  ──► evm_state_create_account(), snapshot(), revert()     │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                    evm_state_t  │  thin wrapper (evm/src/evm_state.c)
                                 │  delegates everything to state_overlay
                                 │
┌────────────────────────────────▼────────────────────────────────────────┐
│                        state_overlay_t                                  │
│                   (database/src/state_overlay.c)                        │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  Account Layer                                                   │    │
│  │                                                                   │    │
│  │  acct_index ─── mem_art: addr[20] ──► uint32_t idx               │    │
│  │       │                                                           │    │
│  │       ▼                                                           │    │
│  │  acct_meta ─── array of cached_account_t (by idx)                │    │
│  │       │          ├ nonce, balance, code_hash, storage_root        │    │
│  │       │          ├ storage_art ──► compact_art_t *  (or NULL)     │    │
│  │       │          ├ storage_mpt ──► art_mpt_t *      (or NULL)    │    │
│  │       │          ├ flags: dirty, existed, created, storage_cleared│    │
│  │       │          └ addr, addr_hash                                │    │
│  │       │                                                           │    │
│  │       ▼                                                           │    │
│  │  flat_state ── disk-backed O(1) account lookups                  │    │
│  │       │          flat_store (compact_art + disk_table)            │    │
│  │       │          class-based record sizes (12B/44B/104B)         │    │
│  │       │          loaded into acct_meta on ensure_account()       │    │
│  │       │                                                           │    │
│  └───────┼──────────────────────────────────────────────────────────┘    │
│          │                                                               │
│  ┌───────┼──────────────────────────────────────────────────────────┐    │
│  │  Storage Layer (per-account)                                      │    │
│  │       │                                                           │    │
│  │       ▼                                                           │    │
│  │  storage_arena ─── 128 GB virtual mmap (bump allocator)          │    │
│  │       │              demand-paged, physical = pages touched       │    │
│  │       │                                                           │    │
│  │       ├──► Account A: compact_art_t                               │    │
│  │       │      nodes pool [256 KB] ◄── arena_alloc                 │    │
│  │       │      leaves pool [512 KB] ◄── arena_alloc                │    │
│  │       │      key: slot_hash[32] = keccak(slot_be)                │    │
│  │       │      val: slot_value_be[32]                               │    │
│  │       │      art_mpt_t wraps this for root computation           │    │
│  │       │                                                           │    │
│  │       ├──► Account B: compact_art_t                               │    │
│  │       │      nodes pool [256 KB]                                  │    │
│  │       │      leaves pool [512 KB]                                 │    │
│  │       │      ...                                                  │    │
│  │       │                                                           │    │
│  │       └──► (arena offset grows monotonically, never freed)       │    │
│  │                                                                   │    │
│  │  storage_file ── packed binary persistence                       │    │
│  │       │            written at checkpoint (compute_mpt_root)      │    │
│  │       │            read by bulk_load on cold account access      │    │
│  │       │                                                           │    │
│  │  stor_index ─── mem_art: addr_hash[32] ──► {offset, count}      │    │
│  │                   maps account to its storage_file section       │    │
│  │                                                                   │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  Transaction State                                                │    │
│  │                                                                   │    │
│  │  journal[] ─── undo log (nonce/balance/code/storage/create)      │    │
│  │                 snapshot() returns journal_len                    │    │
│  │                 revert() walks backwards, restores old values    │    │
│  │                                                                   │    │
│  │  originals ─── mem_art: addr[20]+slot_be[32] ──► uint256_t      │    │
│  │                 value at tx start (EIP-2200 gas metering)        │    │
│  │                 cleared on commit_tx()                            │    │
│  │                                                                   │    │
│  │  warm_addrs ── mem_art: addr[20] (EIP-2929)                     │    │
│  │  warm_slots ── mem_art: addr[20]+slot_be[32] (EIP-2929)         │    │
│  │  transient ─── mem_art: addr[20]+slot_be[32] (EIP-1153)         │    │
│  │                 all three cleared on commit_tx()                  │    │
│  │                                                                   │    │
│  │  dirty_accounts ──── vec of addr[20] (dirty this block)          │    │
│  │  tx_dirty_accounts ─ vec of addr[20] (dirty this tx)             │    │
│  │                                                                   │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  MPT Root Computation (at checkpoint, every 256 blocks)           │    │
│  │                                                                   │    │
│  │  For each dirty account:                                          │    │
│  │    1. art_mpt_root_hash(ca->storage_mpt) ──► storage_root       │    │
│  │       walks compact_art, hashes dirty subtrees, caches clean     │    │
│  │    2. Write storage to storage_file, update stor_index           │    │
│  │    3. Sync account record to flat_state                          │    │
│  │                                                                   │    │
│  │  account_trie ── art_mpt over flat_state account compact_art    │    │
│  │    RLP-encode each dirty account (nonce,bal,storage_root,code_h) │    │
│  │    art_mpt_root_hash() ──► block state root                     │    │
│  │                                                                   │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐    │
│  │  Code Store                                                       │    │
│  │                                                                   │    │
│  │  code_store ── code_hash[32] ──► bytecode                       │    │
│  │                 separate from account data                       │    │
│  │                 loaded on demand (load_code in ensure_account)   │    │
│  │                 freed on evict, reloaded on next access          │    │
│  │                                                                   │    │
│  └───────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

Data flow: SSTORE(addr, slot, value)
─────────────────────────────────────
  1. state_overlay_sstore_lookup(addr, slot)
     ├ ensure_account(addr) ──► cached_account_t
     ├ get_storage(addr, slot) ──► current value
     │   └ storage_read(ca, keccak(slot))
     │       ├ bulk_load from storage_file if storage_art is NULL
     │       └ compact_art_get(ca->storage_art, slot_hash)
     └ get_committed_storage(addr, slot) ──► original value
         └ check originals map, fallback to storage_read

  2. EVM calculates gas from current/original/new (EIP-2200)

  3. state_overlay_set_storage(addr, slot, value)
     ├ ensure_account(addr)
     ├ storage_read for old value (journal)
     ├ journal_push(JOURNAL_STORAGE, old_value)
     ├ save to originals if first write this tx
     └ compact_art_insert(ca->storage_art, slot_hash, value_be)

Data flow: checkpoint (every 256 blocks)
────────────────────────────────────────
  compute_mpt_root():
    for each dirty account:
      1. storage root = art_mpt_root_hash(ca->storage_mpt)
      2. storage_file_write(slots) ──► stor_index update
      3. sync_account_to_overlay(ca) ──► flat_state
      4. account_trie_update(addr_hash, rlp(account))
    final: art_mpt_root_hash(account_trie) ──► state root
    clear dirty flags
```

## mem_art as Storage Backend — Benchmarks

Benchmark: `database/tests/bench_mem_art_storage.c`
Key = keccak256(slot_be[32]) (32 bytes), Value = slot_value_be[32] (32 bytes).

### Per-slot memory cost

~100 bytes per slot, stable from 1 to 50,000 slots:

```
  slots  | arena used | per_slot
---------|------------|----------
       1 |        96  |  96.0 B
      10 |       976  |  97.6 B
     100 |    11,052  | 110.5 B
    1000 |   103,296  | 103.3 B
   10000 | 1,020,752  | 102.1 B
   50000 | 5,056,288  | 101.1 B
```

### init_cap tuning (100 slots/account = 11 KB used)

mem_art uses a realloc-doubling arena internally. Initial cap determines waste:

```
  init_cap | waste% | cap/1M accts
-----------|--------|-------------
    4 KB   |   33%  |   15 GB
   12 KB   |   10%  |   11.5 GB    ◄── sweet spot
   16 KB   |   33%  |   15 GB
   64 KB   |   83%  |   62 GB
```

12 KB is optimal because 100 slots uses ~11 KB — fits without doubling.
Power-of-two caps (4K, 16K) always overshoot then double, wasting 33%+.

### Comparison: mem_art vs compact_art (current)

```
                     | compact_art (current) | mem_art (12KB init)
---------------------|-----------------------|--------------------
reserved per account |     768 KB            |    12 KB (grows)
used per 100 slots   |     768 KB            |    11 KB
waste per account    |     757 KB (98%)      |     1 KB (10%)
max accounts (128GB) |     170K              |    11M+
pool overflow        |     silent drop       |    realloc (grows)
eviction reclaim     |     no (arena)        |    yes (free)
growth model         |     fixed slab        |    realloc doubles
```

At 1M accounts x 100 slots: mem_art uses ~11 GB live, compact_art would need 768 TB reserved.

### Cache capacity (all trees alive, init_cap=12KB)

Benchmark: `database/tests/bench_mem_art_cache.c`

Fixed 100 slots/acct:
```
  accounts |  used  |   RSS  | per acct
-----------|--------|--------|----------
     10K   | 105 MB | 117 MB |   11 KB
    100K   |   1 GB | 1.2 GB |   11 KB
    500K   | 5.3 GB | 5.9 GB |   11 KB
      1M   |  11 GB |  12 GB |   11 KB
```

Realistic mix (90% x 10 slots, 9% x 100, 1% x 1000):
```
  accounts |  used  |  RSS   | per acct
-----------|--------|--------|----------
     10K   |  27 MB |  64 MB |    3 KB
    100K   | 277 MB | 646 MB |    3 KB
    500K   | 1.4 GB | 3.2 GB |    3 KB
```

With 16 GB RAM budget: ~2.5M accounts cached (realistic mix).
With LRU eviction of cold accounts, sustainable indefinitely.

### cached_account_t without storage

208 bytes per entry (nonce 8B, balance 32B, 11 bools 16B with padding,
code ptr+size 12B, addr 20B, 3 hashes 96B, 2 pointers 16B).

At 10M accounts: ~2 GB. Dominated by the three 32-byte hashes
(code_hash, storage_root, addr_hash = 96B, 46% of struct).
Bools could pack into uint16_t flags (saves 14B), but marginal.
Account structs are not the bottleneck — storage arts are.

## Per-Block Checkpoint

### Background

The 256-block checkpoint interval was introduced on disk-table-dev-0 for the
disk-based `mpt_store` — batching amortized expensive disk I/O (B-tree updates,
fsync). When state_overlay replaced mpt_store on overlay-dev-0, benchmarks showed
**faster execution with per-block checkpoint** because state_overlay has no disk
I/O on the hot path. The 256-block batch was carried forward by inertia.

### Why per-block is better

**Smaller dirty set.** A single block touches hundreds to low thousands of accounts.
256-block batches accumulate tens of thousands of dirty accounts — more work per
checkpoint, longer pauses.

**No spike amortization needed.** With incremental dirty tracking, per-block root
computation is proportional to that block's dirty set. No burst of 256 blocks
of accumulated changes.

**Simpler state management.** No distinction between "dirty this block" vs
"dirty since last checkpoint". No accumulated dirty vectors. No "what if crash
mid-window" recovery logic.

**Simpler eviction.** Evict after any block, not only at checkpoint boundaries.
Every block has a fully materialized state root.

### Impact on mem_art migration

Per-block checkpoint makes even full-rebuild MPT (Option A) viable for most blocks:
- Typical block: ~500 dirty accounts, most with <100 dirty slots
- Full rebuild of 500 accounts × 100 slots × 100 B/slot = ~5 MB of data, milliseconds
- Only extreme blocks (DEX arb storms, mass mints) push 5-10K dirty accounts

Incremental dirty tracking (Option B: `mem_art_mpt`) is still better long-term,
but per-block checkpoint removes the urgency. You can ship mem_art with Option A,
then add dirty flags later as an optimization — not a correctness requirement.

### Cost

One root computation per block instead of one per 256. But each is ~256x cheaper
(256x fewer dirty accounts). Net: roughly the same total work, spread evenly
instead of in bursts. Better latency, same throughput.

## LRU Eviction and Storage Persistence

### Problem

Without eviction, every account stays in memory forever. At 2.3M blocks
(DoS era), 8M+ cached accounts consume 23 GB RSS. Most are empty or
cold — touched once and never again.

### Budget-based eviction

Not timer-based (evict every N blocks) — budget-based:
- Set max cached accounts target (e.g. 500K)
- After each block, if cached > target, evict coldest accounts to 80% of target
- `last_access_block` on `cached_account_t` identifies coldest
- Avoids thrashing: only evict accounts cold for 1000+ blocks

### What eviction does

For each evicted account:
1. If storage_mem has slots → write to storage cache file
2. Destroy mem_art + art_mpt (memory returned to OS)
3. Account record already in flat_state (synced at checkpoint)
4. Remove from acct_index + acct_meta (or keep shell for fast relookup)

### Storage cache file

Single append-only file. Deleted on startup (it's a cache, not a database).
In-memory index (mem_art: addr_hash → offset+count). No fsync needed.

```
File format:
  [addr_hash(32)] [slot_count(4)] [slot_hash(32)+value(32)] × slot_count
  [addr_hash(32)] [slot_count(4)] [slot_hash(32)+value(32)] × slot_count
  ...

On evict:
  1. append section to file
  2. index[addr_hash] = {file_offset, slot_count}
  3. mem_art_destroy(storage_mem)

On reload (cold account accessed):
  1. pread at index[addr_hash].offset
  2. populate fresh mem_art from section data
  3. remove index entry (data now in mem_art)

On re-evict (same account evicted again after modification):
  1. append NEW section (old section becomes dead space)
  2. update index to new offset
```

### Why no crash recovery needed

The storage cache file is ephemeral — a session-local optimization.
- On crash: replay from last era1 checkpoint (source of truth)
- On restart: delete file, start fresh, populate mem_arts from execution
- No fsync, no write-ahead log, no durability guarantees

### Compaction

Dead space accumulates when accounts are evicted, reloaded, modified,
then evicted again (old section orphaned, new section appended).

Compaction: rewrite file with only live sections (those in the index).
Trigger when dead_space > 50% of file_size. Run during idle time.
Not needed for initial implementation — dead space is bounded by
eviction churn rate which is low (most evicted accounts stay cold).

### Integration with existing flat_store LRU

The account flat_store already has a doubly-linked LRU overlay with
flush_deferred + evict_clean. Account records (nonce, balance, code_hash,
storage_root) are handled by this existing mechanism.

The storage cache file extends this: when flat_store evicts an account's
overlay entry, state_overlay also destroys the account's mem_art and
writes its storage to the cache file.

Single eviction decision per account — both account record and storage
are evicted together. Reload brings both back: account from flat_store
disk, storage from cache file.

## Target Architecture (post-refactor)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          EVM (interpreter)                              │
│  SLOAD/SSTORE ──► evm_state_get/set_storage()                         │
│  CREATE/CALL  ──► evm_state_create_account(), snapshot(), revert()     │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                    evm_state_t  │  thin wrapper
                                 │
┌────────────────────────────────▼────────────────────────────────────────┐
│                        state_overlay_t                                  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Account Cache (in memory, LRU-managed)                           │  │
│  │                                                                   │  │
│  │  acct_index ─── mem_art: addr[20] ──► uint32_t idx               │  │
│  │       │                                                           │  │
│  │       ▼                                                           │  │
│  │  acct_meta ─── array of cached_account_t (by idx)                │  │
│  │       │          ├ nonce, balance, code_hash, storage_root        │  │
│  │       │          ├ storage ──► mem_art_t * (NULL if evicted)      │  │
│  │       │          ├ storage_ctx ──► art_iface_mem_ctx_t            │  │
│  │       │          ├ storage_mpt ──► art_mpt_t * (NULL if evicted) │  │
│  │       │          ├ last_access_block (for LRU eviction)          │  │
│  │       │          ├ flags: dirty, existed, created, etc            │  │
│  │       │          └ addr, addr_hash                                │  │
│  │       │                                                           │  │
│  │  Per-account storage: mem_art (malloc-backed, growable)           │  │
│  │       key: slot_hash[32] = keccak(slot_be)                       │  │
│  │       val: slot_value_be[32]                                      │  │
│  │       init_cap: 12 KB, grows via realloc, ~100 B/slot            │  │
│  │       eviction: mem_art_destroy() → memory returned to OS        │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Cold Storage (disk, eviction-only)                               │  │
│  │                                                                   │  │
│  │  storage_file ── append-only packed sections                     │  │
│  │       written ONLY on LRU eviction (not on checkpoint)           │  │
│  │       read ONLY on cold account reload                           │  │
│  │                                                                   │  │
│  │  stor_index ─── mem_art: addr_hash[32] ──► {offset, count}      │  │
│  │                                                                   │  │
│  │  flat_state ── disk-backed account records                       │  │
│  │       nonce, balance, code_hash, storage_root                    │  │
│  │       class-based record sizes (12B/44B/104B)                    │  │
│  │       synced on eviction, loaded on ensure_account()             │  │
│  │                                                                   │  │
│  │  code_store ── code_hash[32] ──► bytecode (LRU cached)          │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Transaction State (per-tx, cleared on commit_tx)                 │  │
│  │                                                                   │  │
│  │  journal[] ─── undo log (snapshot/revert)                        │  │
│  │  originals ─── mem_art (EIP-2200 committed values)               │  │
│  │  warm_addrs ── mem_art (EIP-2929)                                │  │
│  │  warm_slots ── mem_art (EIP-2929)                                │  │
│  │  transient ─── mem_art (EIP-1153)                                │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  MPT Root (per-block, incremental)                                │  │
│  │                                                                   │  │
│  │  For each dirty account:                                          │  │
│  │    1. art_mpt_root_hash(storage_mpt) ──► storage_root            │  │
│  │       via art_iface_mem vtable, dirty flags on mem_art nodes     │  │
│  │    2. NO disk write (storage_file only on eviction)              │  │
│  │    3. Sync account record to flat_state                          │  │
│  │                                                                   │  │
│  │  account_trie root ──► block state root                          │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  LRU Eviction (after each block)                                  │  │
│  │                                                                   │  │
│  │  For accounts with last_access_block < current_block - N:        │  │
│  │    1. Serialize mem_art ──► storage_file (append)                │  │
│  │    2. Update stor_index                                           │  │
│  │    3. Sync account to flat_state                                  │  │
│  │    4. mem_art_destroy() + art_mpt_destroy()                      │  │
│  │    5. Free code pointer                                           │  │
│  │    6. Remove from acct_index + acct_meta                         │  │
│  │       (or keep acct_meta shell — 200 bytes — for fast relookup)  │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

Data flow: SSTORE(addr, slot, value)
─────────────────────────────────────
  1. ensure_account(addr)
     ├ acct_index lookup → cached_account_t
     └ if not found: load from flat_state, reload storage from storage_file
  2. touch last_access_block = current_block
  3. sstore_lookup → mem_art_get for current value
  4. journal_push(JOURNAL_STORAGE, old_value)
  5. save to originals if first write this tx
  6. mem_art_insert(storage, slot_hash, value_be)

Data flow: per-block checkpoint
────────────────────────────────
  for each dirty account:
    1. storage_root = art_mpt_root_hash(storage_mpt)  [incremental, via iface]
    2. sync account record to flat_state
    3. account_trie_update(addr_hash, rlp(account))
  state_root = art_mpt_root_hash(account_trie)
  clear dirty flags
  (no storage_file writes — that's eviction only)

Data flow: LRU eviction (after checkpoint)
──────────────────────────────────────────
  scan acct_meta for last_access_block < threshold:
    1. iterate mem_art → write slots to storage_file
    2. stor_index[addr_hash] = {offset, count}
    3. mem_art_destroy(storage), art_mpt_destroy(storage_mpt)
    4. free(code), remove from acct_index
    5. (optionally keep acct_meta entry for fast re-lookup)
```
