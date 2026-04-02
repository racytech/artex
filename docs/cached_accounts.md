# State Layer v2 — Design

## Core Idea

```
vector of cached_accounts [N] indexed by mem_art

simple accounts -         SA
accounts with resources - AR

[SA, SA, SA, SA, SA, SA, SA, AR, SA, SA, SA, SA]
                              |
                         has mem_art for storage
```

Single flat vector. No disk on hot path. No flat_state. No flat_store.

## Data Structures

```
acct_index:  mem_art  addr[20] → uint32_t idx
accounts:    cached_account_t[N]   (flat vector, realloc-growable)

Per account (accounts[idx]):
  addr            [20]    address
  addr_hash       [32]    keccak(addr) — precomputed, trie key
  nonce           u64
  balance         uint256
  code_hash       [32]    keccak(code) or EMPTY_CODE_HASH
  storage_root    [32]    computed at checkpoint, EMPTY_STORAGE if no slots
  has_code        bool
  flags           u16     (dirty, existed, created, self_destructed, etc)
  last_access_blk u64     for LRU eviction
  code            *u8     heap-alloc'd bytecode (NULL if not loaded)
  code_size       u32
  storage_mem     *mem_art  (NULL for SA — no storage)
  storage_ctx     *art_iface_mem_ctx_t  (heap, bridges to art_mpt)
  storage_mpt     *art_mpt_t  (heap, incremental root)

Account trie (for MPT state root):
  compact_art or mem_art keyed by addr_hash[32]
  leaf value = uint32_t idx into accounts[]
  wrapped by art_mpt for incremental root computation
  account_value_encode reads accounts[idx] directly — one path, no disk
```

## No Disk on Hot Path

- No flat_state
- No flat_store
- No storage_file
- No mmap'd files during execution
- Checkpoint = compute MPT root from in-memory data only

## Persistence (graceful shutdown only)

Serialize to a single file on clean shutdown:

```
File:
  header: magic(4) + version(4) + account_count(8) + block_number(8)

  per account:
    addr(20) + nonce(8) + balance(32) + code_hash(32) + storage_root(32)
    + flags(2) + storage_count(4)
    + [slot_hash(32) + value(32)] × storage_count

  code section:
    code_count(4)
    per code: code_hash(32) + code_len(4) + code_bytes(code_len)
```

On restart: read file → populate accounts[] + acct_index + mem_arts.
No file (first run): start from genesis.
Crash (no clean shutdown): replay from era1.

## Checkpoint (per-block)

```
for each dirty account:
  1. if storage_dirty: art_mpt_root_hash(storage_mpt) → storage_root
  2. encode account RLP (nonce, balance, storage_root, code_hash)
  3. insert/update account trie entry

account_trie root → state_root
clear dirty flags
```

No sync to disk. Pure in-memory computation.

## Account Trie

Keyed by addr_hash[32]. Leaf value = account idx (uint32_t).

account_value_encode(idx):
  ca = accounts[idx]
  return RLP(ca.nonce, ca.balance, ca.storage_root, ca.code_hash)

Single code path. No overlay bit check. No disk fallback.

On account create: insert into trie.
On account delete (EIP-161): delete from trie.

## Journal (snapshot/revert)

Same undo log: (type, addr, old_value) entries.
snapshot() = journal_len. revert() walks backwards.

Per-tx ephemeral state (cleared on commit_tx):
  - originals (EIP-2200)
  - warm_addrs, warm_slots (EIP-2929)
  - transient (EIP-1153)
  - tx_dirty_accounts

## LRU Eviction (future)

When memory exceeds budget:
  - Scan resource_list (accounts with storage_mem)
  - Evict cold: serialize storage to cache file, destroy mem_art
  - Keep account shell in vector (storage_root preserved)
  - Reload from cache file on next access

## What Gets Removed

- flat_state.c / flat_state.h
- flat_store.c / flat_store.h
- storage_file.c / storage_file.h (already gone)
- arena.c / arena.h (already gone from storage)
- state_overlay.c sync/flush/overlay logic
- overlay pool, size classes, deferred buffer
- disk path in account_value_encode
- All stale storage_root bugs (single source of truth — memory only)
