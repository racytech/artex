# MPT Root Computation — Execution Flow

## Chain Replay Main Loop

```
chain_replay main loop (per block)
│
├─ block_execute(evm, header, body, hashes)
│   │
│   ├─ evm_state_begin_block(state, block_num)
│   │
│   ├─ for each tx:
│   │   ├─ EVM execution (opcodes modify state via evm_state_*)
│   │   │   ├─ evm_state_set_nonce()      → sets mpt_dirty, block_dirty
│   │   │   ├─ evm_state_set_balance()    → sets mpt_dirty, block_dirty
│   │   │   ├─ evm_state_set_code()       → sets mpt_dirty, block_dirty
│   │   │   ├─ evm_state_set_storage()    → sets mpt_dirty, storage_dirty, block_dirty
│   │   │   ├─ evm_state_create_account() → sets mpt_dirty, resets storage_root
│   │   │   │                               journal saves old state for revert
│   │   │   └─ evm_state_snapshot/revert  → journal undo for failed CALLs/CREATEs
│   │   │
│   │   └─ evm_state_commit_tx()
│   │       ├─ apply self-destructs (zero storage, mark deleted)
│   │       ├─ clear journal
│   │       ├─ clear warm sets, transient storage
│   │       └─ (mpt_dirty & block_dirty SURVIVE commit_tx)
│   │
│   ├─ coinbase rewards (set_balance → mpt_dirty)
│   └─ uncle rewards
│
├─ check_root? (every 256 blocks, or if txs/uncles present)
│   │
│   └─ evm_state_compute_mpt_root(state, prune_empty)
│       │
│       ├─ 1. compute_all_storage_roots(es)     ← STORAGE TRIES
│       │   │
│       │   ├─ collect mpt_dirty slots          (scan es->storage)
│       │   ├─ sort by address                  (group per account)
│       │   ├─ for each dirty account:
│       │   │   ├─ mpt_store_set_root(storage_mpt, ca->storage_root)
│       │   │   ├─ mpt_store_begin_batch(storage_mpt)
│       │   │   ├─ update/delete dirty slots
│       │   │   ├─ mpt_store_commit_batch(storage_mpt)
│       │   │   └─ mpt_store_root() → ca->storage_root   (cached!)
│       │   │
│       │   └─ clear storage_dirty on ALL accounts
│       │
│       ├─ 2. mpt_store_begin_batch(account_mpt)  ← ACCOUNT TRIE
│       │
│       ├─ 3. for each mpt_dirty account:
│       │   ├─ if empty & prune → mpt_store_delete
│       │   └─ else → RLP(nonce, balance, storage_root, code_hash)
│       │            → mpt_store_update
│       │
│       ├─ 4. mpt_store_commit_batch(account_mpt)
│       │      └─ walks dirty paths, recomputes hashes bottom-up
│       │         writes new nodes to .dat, updates .idx
│       │
│       ├─ 5. mpt_store_root(account_mpt) → state_root
│       │
│       └─ 6. clear mpt_dirty on ALL accounts
│
├─ verify: state_root == header.state_root
│
└─ checkpoint (every 256 blocks if clean)
```

## Disk Files

```
account_mpt:  /tmp/chain_replay_mpt.idx + .dat           (one trie)
storage_mpt:  /tmp/chain_replay_mpt_storage.idx + .dat    (shared, many tries)
```

Both use disk_hash (.idx) for O(1) node lookup by hash, and slot-allocated
flat file (.dat) for RLP-encoded trie node data.

Storage mpt runs in shared mode (node deletion disabled) because multiple
accounts' tries may share identical subtree nodes via dedup.

## Dirty Flag Lifecycle

```
set_storage() ──→ mpt_dirty=true, storage_dirty=true, block_dirty=true
set_nonce()   ──→ mpt_dirty=true, block_dirty=true
set_balance() ──→ mpt_dirty=true, block_dirty=true
set_code()    ──→ mpt_dirty=true, block_dirty=true
create_acct() ──→ mpt_dirty=true, storage_dirty=true (reset storage_root)
                    │
commit_tx()         │  (flags survive)
                    │
compute_mpt_root()  ├─ storage_dirty  → cleared after storage roots computed
                    ├─ mpt_dirty      → cleared after account trie committed
                    └─ block_dirty    → (not cleared here, used by verkle path)
```

| Flag | Set by | Cleared by | Survives commit_tx? |
|------|--------|------------|---------------------|
| `mpt_dirty` | any account field change | `clear_mpt_dirty_cb` (after account trie commit) | Yes |
| `storage_dirty` | `set_storage()`, `create_account()` | `clear_storage_dirty_cb` (after storage roots) | Yes |
| `block_dirty` | any field change | `clear_block_dirty_*_cb` (verkle path) | Yes |

## Complexity

All operations are incremental — O(dirty) not O(total):

| Step | Cost |
|------|------|
| Collect dirty slots | O(total_slots) scan, O(dirty_slots) collected |
| Storage trie updates | O(dirty_slots * trie_depth) per account |
| Account trie updates | O(dirty_accounts * trie_depth) |
| Clear flags | O(total_accounts) scan |

Typical block: 1-5 dirty accounts, 0-20 dirty slots.
At 250K blocks: ~8K accounts, ~50K slots in cache.

## Verkle Flow (separate path, not used in MPT validation)

```
evm_state_compute_state_root_ex(es, prune_empty)
│
├─ 1. flush block_dirty accounts → verkle_state
├─ 2. flush block_dirty slots → verkle_state
├─ 3. verkle_state_commit_block()
├─ 4. verkle_state_root_hash() → root hash
└─ 5. clear block_dirty flags
```

Currently chain_replay only calls the MPT path.
