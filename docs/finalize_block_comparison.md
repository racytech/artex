# state_finalize_block / compute_root: Old vs New Comparison

## Old Code (commit 5f2cbb9 — passed 3M+ with --validate-every 256)

### block_executor flow
```
finalize() → collect_diff() → compute_root_ex2(prune, !skip_root_hash)
```

Single call. `compute_root_ex` decides internally what to do.

### compute_root_ex(prune, compute_hash=false) — non-checkpoint blocks
Delegates to `state_finalize_block`:
```
for each blk_dirty:
    find_account (via keccak)
    promote/demote EXISTED
    mark storage_roots_stale if STORAGE_DIRTY
    clear flags (MPT_DIRTY, BLOCK_DIRTY, STORAGE_DIRTY, STORAGE_CLEARED)
    *** NO hart_delete ***
    *** NO SAFE_DELETE_IDX ***
swap blk_dirty → last_dirty
clear blk_orig_acct, blk_orig_stor
clear addr_hash_cache
```

### compute_root_ex(prune, compute_hash=true) — checkpoint blocks
Does everything in one function, does NOT call finalize_block:
```
for each blk_dirty:                          ← only THIS block's dirty entries
    find_account_h (via keccak)
    promote/demote EXISTED
    compute storage_root if STORAGE_DIRTY    ← per-account, targeted
    hart_delete if !EXISTED || (empty&&prune)  ← YES, hart_delete
    clear flags
SAFE_DELETE_IDX for phantoms/destructed/pruned  ← accumulated across all blocks
recompute stale storage roots (all resources)   ← fallback for prior finalize_block blocks
hart_root_hash
swap blk_dirty → last_dirty
clear blk_orig_acct, blk_orig_stor
clear addr_hash_cache
```

### Key properties
- hart_delete ONLY at checkpoint, ONLY for:
  - Current block's dirty dead accounts (from dirty loop)
  - Accumulated dead lists (phantoms/destructed/pruned from ALL blocks since last checkpoint)
- Between checkpoints: dead accounts stay in acct_index (EXISTED cleared but not deleted)
- Re-touch between checkpoints: find_account_h finds OLD slot (still in acct_index)
- Storage roots: computed per-dirty-account at checkpoint + stale fallback for prior blocks

---

## New Code (current — fails at 2.1M+ with --validate-every 1)

### block_executor flow
```
finalize() → collect_diff() → finalize_block(prune) → compute_root(prune) → reset_block()
```

Three explicit calls. Separation of concerns.

### state_finalize_block — every block
```
for each blk_dirty:
    find_account_h (via addr_hash_cached)
    promote/demote EXISTED
    mark storage_roots_stale if STORAGE_DIRTY
    hart_delete if !EXISTED || (empty&&prune)  ← YES, every block
    clear flags
SAFE_DELETE_IDX for phantoms/destructed/pruned  ← drained every block
swap blk_dirty → last_dirty
```

### state_compute_root — checkpoint only
```
recompute stale storage roots (all resources)
hart_root_hash
```

### state_reset_block — every block
```
clear blk_orig_acct, blk_orig_stor
clear addr_hash_cache
```

### Key properties
- hart_delete EVERY block for:
  - Current block's dirty dead accounts (from dirty loop)
  - Current block's dead lists (phantoms/destructed/pruned)
- Dead lists drained every block (counts reset to 0)
- Re-touch in later block: find_account_h returns NULL → ensure_account_h creates NEW slot
- Storage roots: always marked stale, recomputed for ALL resources at checkpoint

---

## Differences Summary

| Aspect | Old (passing) | New (failing) |
|--------|--------------|---------------|
| hart_delete timing | Checkpoint only | Every block |
| hart_delete scope | Current block dirty + accumulated dead lists | Current block dirty + current block dead lists |
| Dead list lifetime | Accumulate across blocks, drained at checkpoint | Drained every block |
| Re-touch after death | Reuses old slot (still in acct_index) | Creates new slot (old orphaned) |
| Storage root compute | Per-dirty-account at checkpoint | Mark stale, recompute all at checkpoint |
| addr_hash | hash_keccak256 | addr_hash_cached |
| Cache reset timing | After hash (same function) | Separate reset_block call |

## Root Cause of Failure

The per-block hart_delete causes the "duplicate account" problem:
1. Block N: account A dies → hart_delete removes from acct_index
2. Block N+K: A re-touched → ensure_account_h creates NEW slot (old slot orphaned)
3. New slot may have different state than old slot would have (missing resource, storage)

In the old code, A stays in acct_index between checkpoints. Re-touch reuses old slot.
At checkpoint, hart_delete + hash are atomic — no more blocks execute between them.
