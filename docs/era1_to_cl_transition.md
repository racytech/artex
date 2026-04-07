# Era1 → CL Transition

## Three Execution Stages

```
Stage 1: Pre-merge (genesis → 15,537,393)
  Source: era1 files
  Mode:   batch — skip_root_hash, validate at snapshot boundaries
  Speed:  200-400 blk/s

Stage 2: Post-merge catch-up (15,537,393 → head-64)
  Source: CL via Engine API (newPayload)
  Mode:   catch-up — skip_root_hash for finalized blocks, validate every 1M
  Speed:  100-200 blk/s (estimate)

Stage 3: Chain tip (head-64 → live)
  Source: CL via Engine API (newPayload)
  Mode:   live — per-block root validation, VALID/INVALID responses
  Speed:  1 block / 12s (Ethereum slot time)
```

## Root Validation Strategy

Per-block root computation is expensive (~100-300ms at scale). During
catch-up, all blocks below the CL's finalized checkpoint are guaranteed
canonical — no need for per-block roots.

```
forkchoiceUpdated(head=22000100, safe=22000080, finalized=22000050)

our_head = 15537393 (Paris snapshot)

Blocks 15537394 → 22000050:  catch-up mode (skip root hash)
Blocks 22000051 → 22000100:  live mode (per-block root hash)
```

The switch is driven by `forkchoiceUpdated`:
```c
void on_forkchoice_updated(uint64_t head, uint64_t finalized) {
    if (our_head < finalized) {
        // Catch-up: skip root hash for blocks ≤ finalized
        sync->evm->skip_root_hash = true;
    } else {
        // Live: per-block root hash
        sync->evm->skip_root_hash = false;
    }
}
```

As we catch up, the CL keeps sending updated finalized blocks. The gap
shrinks until we're within 64 blocks of head → switch to live mode.

Periodic validation during catch-up (every 1M blocks):
- Compute full root with invalidate_all
- Compare against the block header's state_root
- Save snapshot for crash recovery

## Current State (artex_node.c)

The transition from era1 replay (Phase 1) to Engine API (Phase 2) at
line 516-518 is:
```c
sync_set_live_mode(sync, true);  // switches batch → live mode
engine_create(&eng_cfg);         // starts Engine API server
engine_run(eng);                 // blocks until shutdown
```

## Issues to Fix

### 1. Storage eviction not configured
`artex_node.c` doesn't set up eviction. At Paris (block 15.5M),
storage will consume 45GB+ RAM without budget-based eviction.

**Fix**: Add after sync_create:
```c
evm_state_set_evict_path(sync_get_state(sync), args.data_dir);
evm_state_set_evict_budget(sync_get_state(sync), 32ULL * 1024 * 1024 * 1024);
```

### 2. No --load-state support
Can't skip era1 replay by loading a Paris snapshot. Must replay from
genesis every time (~6+ hours).

**Fix**: Add `--load-state <path>` argument. If provided:
- Load state from snapshot
- Skip era1 replay
- Go directly to Engine API (Phase 2)
- Start in catch-up mode (skip root hash)

### 3. ENABLE_HISTORY off by default
The block undo log (required for reorgs) depends on ENABLE_HISTORY.
Without it, forkchoiceUpdated can't revert blocks.

**Fix**: Enable by default for artex_node builds, or make the undo
log independent of ENABLE_HISTORY.

### 4. Catch-up mode for post-merge blocks
`sync_execute_block_live` currently validates root at every checkpoint
interval. During catch-up (blocks ≤ finalized), this is wasteful.

**Fix**: `sync_execute_block_live` checks if the block is ≤ the last
known finalized block. If so, skip_root_hash = true. The finalized
block is updated by forkchoiceUpdated handler.

```c
bool sync_execute_block_live(sync_t *sync, ...) {
    if (header->number <= sync->finalized_block)
        sync->evm->skip_root_hash = true;   // catch-up
    else
        sync->evm->skip_root_hash = false;  // live
    ...
}
```

### 5. No periodic snapshots during catch-up
If the process crashes during post-merge catch-up, progress is lost.

**Fix**: Save snapshot every 1M blocks during catch-up, same as
era1 replay. Stop saving once in live mode (fork() for background
save if needed).

### 6. State root verification at transition points
After era1 replay completes, verify the state root matches Paris block.
After catch-up completes, verify root matches the finalized block before
switching to live per-block validation.

### 7. Engine API forkchoiceUpdated doesn't implement reorg
The handler exists but revert_to isn't wired.

**Fix**: Wire state_history_revert_to into forkchoiceUpdated handler.
Only needed in live mode — catch-up blocks are finalized, can't reorg.

## Minimal Path to Test Chain Tip

1. Add `--load-state` to artex_node
2. Wire storage eviction
3. Build with `ENABLE_HISTORY=ON`
4. Load Paris snapshot
5. Start Engine API
6. Connect Lighthouse with checkpoint sync:
   ```
   lighthouse bn --network mainnet \
     --checkpoint-sync-url https://beaconstate.info \
     --execution-endpoint http://localhost:8551 \
     --execution-jwt /path/to/jwt.hex
   ```
7. Lighthouse sends newPayload for recent blocks
8. We execute and return VALID/INVALID
9. Once caught up (within 64 blocks), we're at chain tip

For initial testing, skip steps 4-6 complexity:
- Just start Engine API without era1 replay
- Let Lighthouse checkpoint sync provide recent blocks
- Respond SYNCING until we have state for those blocks
- This tests the Engine API plumbing without full state

## Post-Merge Block Source

Post-merge blocks come exclusively from the CL via Engine API.
There are no era2 files on mainnet yet. The CL client (Lighthouse,
Prysm, etc.) syncs the beacon chain and sends execution payloads.

For bulk replay of historical post-merge blocks, options:
- Run a CL client in archive mode → sends all blocks
- Fetch execution payloads from a beacon API
- Wait for era2 file format standardization
