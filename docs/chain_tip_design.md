# Chain Tip Design — Reorg-Safe State Management

## Problem

At the chain tip, the CL sends blocks via Engine API. Blocks can be
reorged (1-2 blocks typically, up to 64 in theory). The EL needs to:

1. Execute blocks speculatively (may be reverted)
2. Serve RPC reads against latest/safe/finalized state
3. Roll back state cleanly on reorg
4. Commit state permanently only after finalization

Currently artex executes blocks directly on the canonical state with
no rollback mechanism. This works for chain replay but breaks at the
chain tip.

## Block Tags

| Tag | Meaning | Latency | Can reorg? |
|-----|---------|---------|------------|
| `finalized` | 2 epochs (~13 min, ~64 blocks) | ~64 blocks behind | Never |
| `safe` | 1 epoch (~6 min, ~32 blocks) | ~32 blocks behind | Extremely rare |
| `latest` | Most recent executed block | 0 blocks behind | Yes (1-2 blocks) |
| `pending` | Mempool speculation | Before inclusion | N/A |
| `earliest` | Genesis | N/A | Never |

## Architecture: Diff-Based Overlay

Canonical state (mpt_store) is committed only at the finalized block.
Non-finalized blocks are stored as lightweight diffs in memory.

```
mpt_store (canonical at finalized block)
  │
  ├── committed to disk
  ├── mmap'd, read-only safe
  ├── serves "finalized" RPC reads directly
  │
  ├── pending_diffs[N-64 .. N]
  │     in-memory block_diff_t overlay
  │     serves "latest" / "safe" reads
  │     discarded on reorg
  │
  └── exec_cache[block_hash → {diff, root}]
        results from newPayload
        multiple heads can coexist
        committed or discarded by forkchoiceUpdated
```

## Engine API Flow

### newPayload(block)

1. Check if parent is known (in exec_cache or canonical)
2. Build temporary state: canonical + parent's pending diffs
3. Execute block on temporary state
4. Capture diff (block_diff_t) and state root
5. Store in exec_cache keyed by block hash
6. Return VALID/INVALID + state root
7. Do NOT modify canonical state

### forkchoiceUpdated(head, safe, finalized)

1. Walk from head back to finalized in exec_cache
2. If any block is missing → return SYNCING
3. Commit finalized blocks to canonical mpt_store:
   - For each block from old_finalized+1 to new_finalized:
     - Apply diff to mpt_store (batch update + commit)
     - Remove from exec_cache
4. Update pending_diffs: blocks from finalized+1 to head
5. Prune exec_cache: remove blocks not on the head chain
6. Store head/safe/finalized block numbers for RPC

### Reorg Handling

```
Before: head = A₃ → A₂ → A₁ → finalized
After:  head = B₃ → B₂ → A₁ → finalized

1. forkchoiceUpdated(head=B₃, finalized=same)
2. Walk B₃ → B₂ → A₁ — all in exec_cache? Yes
3. New pending_diffs = [A₁_diff, B₂_diff, B₃_diff]
4. Old A₂, A₃ diffs discarded (not on new chain)
5. Canonical state unchanged (finalized didn't move)
```

No state rollback needed. Canonical state only moves forward at
finalization. Non-finalized state is always reconstructed from diffs.

## RPC Reads

### eth_getBalance(addr, "finalized")

Read directly from canonical mpt_store. Walk trie from finalized root.
No overlay needed. Fast, consistent.

### eth_getBalance(addr, "latest")

1. Read from canonical mpt_store (finalized state)
2. Apply pending_diffs as overlay
3. If addr was modified in any pending diff, return that value
4. Otherwise return canonical value

Pending diffs are small (~64 blocks × ~200 changes = ~12K entries).
Hash map lookup per address = O(1).

### eth_call(tx, "latest")

1. Build overlay state: canonical + pending_diffs
2. Create temporary evm_state on the overlay
3. Execute transaction (read-only, no commit)
4. Return result

### eth_getBalance(addr, "0x5F5E100") — historical block

Only archive nodes. Use state_reconstruct from history diffs.
Not supported on execution-only nodes.

## Data Structures

### exec_cache

```c
typedef struct {
    uint8_t      block_hash[32];
    uint8_t      parent_hash[32];
    uint64_t     block_number;
    block_diff_t diff;           /* state changes this block made */
    uint8_t      state_root[32]; /* computed root after this block */
    bool         occupied;
} exec_cache_entry_t;

#define EXEC_CACHE_SIZE 128  /* enough for 64 finalized + some forks */
```

### pending_diffs

```c
typedef struct {
    block_diff_t *diffs;     /* array, ordered by block number */
    uint64_t      count;
    uint64_t      base_block; /* finalized block number */

    /* Fast lookup: address → latest value across all diffs */
    mem_art_t     overlay;   /* addr(20) → {nonce, balance, code_hash} */
    mem_art_t     storage;   /* addr(20)+slot(32) → value(32) */
} pending_overlay_t;
```

## Memory Usage

At chain tip with 64 non-finalized blocks:

- exec_cache: 128 entries × ~2KB diff = ~256KB
- pending_diffs: 64 diffs × ~2KB = ~128KB
- overlay ART: ~12K entries × ~100B = ~1.2MB

Total: ~2MB. Negligible.

## Commitment Strategy

When forkchoiceUpdated moves finalized forward:

```c
for (block = old_finalized + 1; block <= new_finalized; block++) {
    exec_cache_entry_t *e = find_on_chain(block, head_chain);

    evm_state_begin_block(es, block);
    evm_state_apply_diff_bulk(es, &e->diff);
    evm_state_commit(es);
    evm_state_compute_mpt_root(es, prune_empty);
    evm_state_flush(es);

    // Optional: write to state_history for archive
    state_history_push(history, &e->diff);

    exec_cache_remove(e);
}
```

This reuses the existing diff application code from state_reconstruct.

## Open Questions

- [ ] Max exec_cache size? 128 entries covers 64 finalized + forks
- [ ] Should pending_overlay eagerly compute or lazily apply diffs?
- [ ] How to handle eth_call that needs full EVM? Fork evm_state or COW?
- [ ] Should newPayload execute on a thread pool? (parallel block validation)
- [ ] Verkle state: same overlay approach? Diffs apply to verkle_state too
