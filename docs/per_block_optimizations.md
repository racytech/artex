# Per-Block Validation Optimizations

## Status: Notes (not implemented)

## Observation

Running chain_replay with `--validate-every 1` is faster than `--validate-every 256`
in the DoS block range (2.2M-2.7M). Smaller dirty sets per root computation outweigh
the cost of computing the root more often.

## Optimizations (ordered by expected impact)

### 1. Decouple eviction from validation
Currently `--validate-every 1` also evicts every block (evict_interval defaults to
checkpoint_interval). This destroys the entire cache after each block — the next block
reloads the same hot accounts from flat_state.

Fix: `--validate-every 1 --evict-every 256`. Validate per-block, evict per-window.
Cache stays hot. Per-account storage arts survive across blocks within the window.

### 2. Remove sync_account_to_overlay from compute_mpt_root
Currently Step 5 writes every dirty account to flat_store so the account_trie encode
callback can read it. But when OVERLAY_BIT is set, the encode callback already reads
from meta_pool directly. The flat_store write is only needed for disk persistence.

Fix: skip sync in compute_mpt_root, only flush at eviction time. The encode callback
reads from meta (fast, in-memory). The account_trie_root hash is the same.

This is the single biggest per-block speedup — eliminates O(dirty_accounts) flat_store
writes per block.

### 3. Collapse dirty flag levels
Per-block mode doesn't need three levels (tx / block / checkpoint). Collapse
`dirty`, `block_dirty`, `mpt_dirty` into a single `dirty` flag. Simplifies journal
entries and revert paths.

### 4. Batch account trie dirty marking
Each set_balance/set_nonce individually calls compact_art_insert on the account
flat_store, marking paths dirty. With per-block, batch: mark dirty once per account
at root computation time instead of on every mutation.

### 5. Lazy storage root computation
Only compute storage_root for accounts whose storage changed AND whose account
record needs trie update. Skip for read-only accounts.

### 6. Skip compute_mpt_root entirely for blocks with no state changes
Some blocks (empty blocks, blocks with only rejected txs) have no dirty accounts.
Skip the entire root computation — the expected root equals the previous block's root.
(Not quite true — coinbase always gets the block reward. But post-merge with
zero-reward blocks this matters.)

## Implementation Order

1. Decouple evict interval (trivial config change)
2. Skip sync in compute_mpt_root (biggest win, small change)
3. Collapse dirty flags (larger refactor, do after stabilization)
