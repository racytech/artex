# Block Undo Log — Reorg-Safe State Management

## Problem

Three issues share the same root cause:

1. **Chain reorgs**: At the chain tip, the CL can signal a fork switch.
   We need to revert to a previous block's state and replay a different chain.
   Currently impossible — `commit_tx` and `compute_root_ex` are permanent.

2. **76 Prague test failures**: Tests expect invalid blocks to be rejected
   and state reverted. Our `evm_state_revert` can't undo committed blocks.

3. **Engine API `forkchoiceUpdated`**: When head changes to a different
   fork, we need to unwind and re-execute.

## Solution: Extend State History with Old Values

We already have `state_history` (`executor/include/state_history.h`) that
captures per-block diffs with **new values** for forward replay. Extending
it with **old values** enables reverse replay (undo) using the same system.

One diff format. Two storage tiers:
- **In-memory ring** (`diff_ring_t`, 512 slots) — hot, for reorgs (last 64 blocks)
- **Disk files** (`state_history.dat/.idx`) — cold, for deep reconstruction

### Current diff structures

```c
/* executor/include/state_history.h */

typedef struct {
    uint256_t slot;
    uint256_t value;        /* new value */
} slot_diff_t;

typedef struct {
    address_t   addr;
    uint8_t     flags;       /* ACCT_DIFF_CREATED | ACCT_DIFF_DESTRUCTED */
    uint8_t     field_mask;  /* FIELD_NONCE | FIELD_BALANCE | FIELD_CODE_HASH */
    uint64_t    nonce;       /* new nonce */
    uint256_t   balance;     /* new balance */
    hash_t      code_hash;   /* new code_hash */
    slot_diff_t *slots;
    uint16_t    slot_count;
} addr_diff_t;

typedef struct block_diff_t {
    uint64_t     block_number;
    addr_diff_t *groups;
    uint16_t     group_count;
} block_diff_t;
```

### Changes: add old values

```c
typedef struct {
    uint256_t slot;
    uint256_t value;        /* new value (forward replay) */
    uint256_t old_value;    /* previous value (reverse replay) */
} slot_diff_t;

typedef struct {
    address_t   addr;
    uint8_t     flags;       /* ACCT_DIFF_CREATED | ACCT_DIFF_DESTRUCTED */
    uint8_t     field_mask;  /* FIELD_NONCE | FIELD_BALANCE | FIELD_CODE_HASH */
    uint64_t    nonce;       /* new nonce */
    uint64_t    old_nonce;   /* previous nonce */
    uint256_t   balance;     /* new balance */
    uint256_t   old_balance; /* previous balance */
    hash_t      code_hash;   /* new code_hash */
    hash_t      old_code_hash; /* previous code_hash */
    slot_diff_t *slots;
    uint16_t    slot_count;
} addr_diff_t;
```

The `old_*` fields are populated during `state_history_capture` — the
capture function already reads current state values before block
execution modifies them. We just need to save them.

### Memory cost

Per slot_diff: +32 bytes (old_value). Per addr_diff: +8 + 32 + 32 = +72 bytes.

Typical block: ~500 account changes + ~2000 storage writes
- Old: 500 × 120 + 2000 × 64 = 188KB
- New: 500 × 192 + 2000 × 96 = 288KB

64 blocks × 288KB = **~18MB**. Negligible.

### Existing functions — what changes

| Function | Current | Change |
|----------|---------|--------|
| `state_history_capture` | Reads new values from dirty accounts | Also reads old values from `originals` map or pre-block snapshot |
| `state_history_apply_diff` | Writes new values (forward) | No change |
| `state_history_push` | Saves to disk + ring | Saves old values too |
| **`state_history_revert_diff`** | **Does not exist** | **New: writes old values (reverse)** |
| **`state_history_revert_to`** | **Does not exist** | **New: revert N blocks in sequence** |

### New API

```c
/**
 * Revert a single block diff (reverse replay).
 * Writes old_nonce, old_balance, old_code_hash, old_value for each entry.
 * Handles ACCT_DIFF_CREATED (revert = delete) and ACCT_DIFF_DESTRUCTED
 * (revert = restore from old values).
 */
void state_history_revert_diff(evm_state_t *es, const block_diff_t *diff);

/**
 * Revert state from current_block back to target_block.
 * Reads diffs from the in-memory ring (must be within 64 blocks).
 * Returns number of blocks reverted (0 on failure).
 */
uint64_t state_history_revert_to(state_history_t *sh,
                                  evm_state_t *es,
                                  uint64_t target_block);

/**
 * Discard diffs for blocks > last_block (after reorg, before re-execution).
 * Existing function — no change needed.
 */
void state_history_truncate(state_history_t *sh, uint64_t last_block);
```

### Revert operation

```c
void state_history_revert_diff(evm_state_t *es, const block_diff_t *diff) {
    /* Process groups in reverse order */
    for (int g = diff->group_count - 1; g >= 0; g--) {
        addr_diff_t *grp = &diff->groups[g];

        if (grp->flags & ACCT_DIFF_CREATED) {
            /* Account was created in this block — delete it */
            evm_state_delete_account(es, &grp->addr);
            continue;
        }

        if (grp->flags & ACCT_DIFF_DESTRUCTED) {
            /* Account was destructed — restore from old values */
            evm_state_set_nonce(es, &grp->addr, grp->old_nonce);
            evm_state_set_balance(es, &grp->addr, &grp->old_balance);
            /* code_hash restored via set_code if needed */
        }

        /* Restore account fields */
        if (grp->field_mask & FIELD_NONCE)
            evm_state_set_nonce(es, &grp->addr, grp->old_nonce);
        if (grp->field_mask & FIELD_BALANCE)
            evm_state_set_balance(es, &grp->addr, &grp->old_balance);
        if (grp->field_mask & FIELD_CODE_HASH)
            /* Restore code_hash + code_size directly */
            evm_state_set_code_hash(es, &grp->addr, &grp->old_code_hash);

        /* Restore storage slots in reverse */
        for (int si = grp->slot_count - 1; si >= 0; si--) {
            evm_state_set_storage(es, &grp->addr,
                                  &grp->slots[si].slot,
                                  &grp->slots[si].old_value);
        }
    }
}
```

After revert:
1. Mark reverted accounts as dirty (ACCT_MPT_DIRTY, ACCT_STORAGE_DIRTY)
2. Call `state_invalidate_all()` to clear cached hashes
3. Next `state_compute_root_ex(true)` recomputes everything

### Chain reorg flow

```
Current chain: ... → A → B → C → D (head)
New chain:     ... → A → B' → C' (new head)

1. CL sends forkchoiceUpdated(head=C', finalized=...)

2. Find common ancestor:
   - Walk back from D using diff ring: D→C→B→A
   - Walk back from C' using CL parent hashes: C'→B'→A
   - Common ancestor = A

3. Revert to A:
   state_history_revert_diff(es, diff_D);  // undo D
   state_history_revert_diff(es, diff_C);  // undo C
   state_history_revert_diff(es, diff_B);  // undo B
   // State is now at block A

4. Truncate history:
   state_history_truncate(sh, A);

5. Re-execute new chain:
   block_execute(B') → capture diff → push to ring
   block_execute(C') → capture diff → push to ring

6. Head is now C'
```

### Finalization

When CL signals finalization:

```c
void state_history_finalize(state_history_t *sh, uint64_t finalized_block) {
    /* Diffs for blocks <= finalized can never be needed for reorg.
     * They remain on disk for deep reconstruction but can be evicted
     * from the in-memory ring. */
    /* The ring naturally evicts as new blocks push old ones out
     * (512 slots >> 64 finality depth). No explicit action needed. */
}
```

The in-memory ring has 512 slots. Finality depth is 64. The ring
automatically overwrites entries older than 512 blocks. Since finalized
blocks are always < 512 blocks behind head, no explicit cleanup needed.

### Disk format change

The disk serialization of `block_diff_t` needs to include old values.
Bump the history file version:

```
Per slot:  slot[32] + value[32] + old_value[32] = 96 bytes (was 64)
Per group: addr[20] + flags[1] + mask[1] + nonce[8] + old_nonce[8] +
           balance[32] + old_balance[32] + code_hash[32] + old_code_hash[32] +
           slot_count[2] + slots
```

Disk cost: ~50% larger diffs. At ~5KB/block → ~7.5KB/block.
For 15M blocks: ~112GB → ~168GB. Acceptable for an archive node.

For non-archive nodes: don't write old values to disk (only keep them
in the in-memory ring). Save disk space, still support reorgs.

### Integration with Engine API

```c
/* engine_handlers.c — forkchoiceUpdated */
cJSON *engine_forkchoiceUpdatedV3(const cJSON *params, ...) {
    hash_t head_hash = parse_hash(params, "headBlockHash");
    hash_t finalized_hash = parse_hash(params, "finalizedBlockHash");

    /* Check if head is on our canonical chain */
    if (!is_canonical(ctx->sync, &head_hash)) {
        /* Reorg needed */
        uint64_t ancestor = find_common_ancestor(ctx->sync, &head_hash);
        state_history_revert_to(ctx->sync->history, ctx->state, ancestor);
        /* Re-execute blocks from ancestor+1 to head */
        replay_chain(ctx->sync, ancestor + 1, &head_hash);
    }

    /* Update finalized */
    state_history_finalize(ctx->sync->history, finalized_block);

    return payload_status_valid();
}
```

### Fixes Prague test failures

The test runner can use `state_history_revert_diff` instead of the
broken `evm_state_revert(snapshot)`:

```c
/* After block_execute for a block with validation_error */
if (payload->validation_error != NULL) {
    state_history_revert_diff(runner->state, &block_result.diff);
    /* State is now restored to pre-block */
    continue;
}
```

This requires `block_execute` to also produce a `block_diff_t` with
old values, which it will once the capture includes them.

### Implementation order

1. Add `old_value`/`old_nonce`/`old_balance`/`old_code_hash` to diff structs
2. Update `state_history_capture` to record old values
3. Implement `state_history_revert_diff`
4. Implement `state_history_revert_to`
5. Wire into `sync_execute_block_live`
6. Wire into engine `forkchoiceUpdated`
7. Fix test_runner_engine to use revert_diff
8. Update disk serialization format (optional, for archive nodes)
