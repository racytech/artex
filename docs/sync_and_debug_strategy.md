# Sync and Debug Strategy

## Sync Modes

### 1. Era1 Replay (genesis → merge, blocks 0–15,537,393)

Historical blocks from static era1 files. Trusted data — no network peers,
no consensus validation needed.

**Strategy**: execute all blocks with no root validation. Compute root once
at the final era1 block and compare against the known state root from the
block header.

**Snapshots**: auto-save state every 1M blocks during replay. Cost: ~30s
per snapshot (write 2-3GB), ~30GB total disk for 15 snapshots. Negligible
compared to hours of replay time.

**Performance**: without root computation, the hot path is pure EVM
execution + secp256k1 signature recovery. Root hash (currently ~50% of
CPU) drops to near zero.

### 2. CL Optimistic Sync (merge → head)

Post-merge blocks received from beacon node via Engine API. CL uses
optimistic sync while we catch up — blocks are "optimistically accepted"
without requiring immediate state root validation.

**Strategy**: same as era1 — execute blocks without root validation.
Respond to CL with optimistic acceptance. Validate state root periodically
(every N blocks) or when approaching the head.

**Snapshots**: continue auto-saving every 1M blocks or at epoch boundaries.

**Transition to chain tip**: when our head is within ~64 blocks of the CL
head, switch to per-block validation mode. From this point, every
`newPayload` gets a full root computation and definitive VALID/INVALID
response.

### 3. CL Chain Tip (live consensus)

At the head of the chain. Every block must be fully validated — state root,
receipts root, gas used — before responding to the CL.

**Strategy**: per-block root computation (incremental, ~20ms at 200M entries).
No skipping. The 12-second slot time provides ample margin.

**No snapshots needed**: state is in memory, if anything goes wrong we
have the exact block and can dump/trace immediately.

---

## Debug Strategy

### Era1 replay: final root mismatch

The most likely failure mode during historical sync.

**Prerequisites**:
- State snapshots saved every 1M blocks during the replay
- Code store .dat files intact (code served on demand from disk)
- Era1 files available for any range

**Steps**:

```
1. Full run completes, final root != expected
   ├── We know: block 0 is correct (genesis)
   └── We know: block N has wrong root

2. Binary search via snapshots
   ├── Load state_8000000.bin, compute root, compare with era1 header
   │   ├── Correct → bug is in 8M..15M
   │   └── Wrong → bug is in 0..8M
   ├── Narrow to 1M range (1 more snapshot load + root check)
   └── Total: ~4 iterations to find the 1M range

3. Re-run the 1M range with --validate-every 256
   ├── Load snapshot, run ~1M blocks with periodic validation
   ├── Find the 256-block window where root first diverges
   └── Takes ~5-10 minutes

4. Re-run the 256-block window with --validate-every 1
   ├── Find the exact block
   └── Takes seconds

5. Debug the specific block
   ├── --dump-prestate N: extract pre-state for the failing block
   ├── Run through evm_t8n, compare with geth output
   ├── Diff account-by-account to find which value diverges
   └── Trace back to the root cause (EVM bug, state bug, trie bug)
```

**Time cost**: ~15-30 minutes from "root mismatch detected" to "exact
block identified". Compare with hours if re-running from genesis.

### CL sync: root mismatch during optimistic sync

Same as era1 debug, but with post-merge blocks. Snapshots provide the
same binary search capability.

**Additional tool**: if we have per-block state diffs (ENABLE_HISTORY),
we can reconstruct any intermediate state from a snapshot + diffs without
re-executing blocks.

### Chain tip: root mismatch on live block

Immediate — we have the state in memory and know the exact block.

**Steps**:

```
1. newPayload returns INVALID (our root != CL's expected root)

2. State is still in memory — dump it
   ├── state_save() to capture full state
   ├── Extract the block's transactions
   └── Run each tx through evm_t8n with tracing

3. Compare with reference client (geth)
   ├── Same block, same pre-state → diff the execution trace
   └── Find the diverging opcode/state access

4. If state was already wrong before this block
   ├── Load previous snapshot, validate forward
   └── Same binary search as era1 debug
```

---

## Snapshot File Management

### Naming convention

```
~/.artex/snapshots/
  state_1000000.bin    (block 1M, ~1.5GB)
  state_2000000.bin    (block 2M, ~2.0GB)
  state_3000000.bin    (block 3M, ~4.0GB, post-DoS peak)
  ...
  state_15000000.bin   (block 15M, ~3.0GB)
```

### What's in a snapshot

```
Header: magic("ART1") + block_number(8) + state_root(32) + account_count(4)
Per account:
  addr(20) + nonce(8) + balance(32) + code_hash(32) + storage_root(32) +
  code_size(4) + storage_count(4)
  [storage entries: key(32) + value(32)]
```

Code bytes NOT included — served by code_store .dat files on disk.
On load, hart indexes rebuilt from scratch (accounts vector + storage tries).

### Snapshot verification

On load, optionally compute root and compare with header's state_root.
One-time cost, confirms snapshot integrity before proceeding.

---

## Implementation Checklist

- [x] `state_save` / `state_load` — binary snapshot read/write
- [x] `--save-state N` — save snapshot after block N
- [x] `--load-state P` — load and resume from snapshot
- [x] `--validate-every N` — periodic root validation
- [x] `skip_root_hash` — skip hash on non-checkpoint blocks
- [x] `state_finalize_block` — prune without hashing, cursor-based
- [ ] `--no-validate` — skip all root checks, compute at end only
- [ ] `--snapshot-every N` — auto-save snapshots during run
- [ ] Snapshot directory management (cleanup, listing)
- [ ] Root verification on snapshot load
- [ ] CL optimistic sync integration with Engine API
- [ ] Chain tip mode transition (optimistic → full validation)
