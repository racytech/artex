# Fast Sync: State Transfer to Execution Node

## Context

Two-node architecture:
- **Full node** (current) — does all heavy lifting: block execution, MPT construction, state history. Must never be blocked or put at risk of corruption.
- **Execution node** (future) — receives state, does not re-execute historical blocks. Needs to bootstrap quickly and stay current.

The full node's state integrity is the top priority. No operation on behalf of the execution node may interfere with block execution, checkpointing, or MPT writes.

## Problem

The execution node needs complete state (all accounts + storage) at block N without replaying blocks 1..N. The full node has this state in its MPT store, but:
- MPT files are huge (~33GB+) and contain dead/unreferenced nodes
- Copying live MPT files while the full node writes to them risks corruption
- Walking the live trie while the full node mutates it is unsafe
- Any I/O contention (mmap, disk bandwidth) could slow the full node

## Constraints

1. **Zero interference** — the full node must not be blocked, slowed, or put at risk
2. **Consistency** — the snapshot must represent a single consistent block boundary
3. **Verifiable** — the execution node must be able to verify the snapshot (state root)
4. **Incremental catch-up** — after initial snapshot, the execution node catches up via diffs, not re-execution

## Available Tools

| Tool | What it provides |
|------|-----------------|
| MPT store (.dat/.idx) | Full merkle trie — all nodes including dead ones |
| Code store | Content-addressed contract bytecode |
| State history (.dat/.idx) | Per-block diffs (new values only), blocks 1..N |
| Checkpoint file | Block number + serialized cache state |

## Options

### Option A: Offline Copy at Checkpoint

**How:** Stop the full node (or pause at a checkpoint boundary), copy MPT + code store files, restart.

**Pros:**
- Simplest — no new code
- Consistent by definition (no concurrent writes)
- Execution node gets full MPT, can compute roots immediately

**Cons:**
- Requires downtime (seconds to minutes depending on file sizes)
- Copies dead nodes too (wasted space + bandwidth)
- MPT files are not portable — execution node must use same disk_hash format

**Risk:** Low. The full node is stopped, so no corruption possible. But downtime is undesirable if the full node is syncing live.

### Option B: Background Snapshot from History Diffs

**How:** A separate process reads the history files (which are append-only and safe to read concurrently) and reconstructs full state by replaying all diffs from genesis.

```
state = load_genesis()
for block 1..N:
    diff = history.get_diff(block)
    apply(diff, state)
serialize(state) → snapshot file
```

**Pros:**
- Zero interference with the full node — reads only append-only history files
- No mmap contention, no shared locks
- Produces a clean flat snapshot (no dead nodes)
- Can run on a completely separate machine if history files are copied/mounted

**Cons:**
- Requires complete history from block 1 (we have this: 8GB, no gaps)
- Replay time: O(N) — reading 4M+ diffs, applying each. CPU-bound, not disk-bound
- History stores only new values — genesis provides the base, diffs provide all subsequent changes
- Must store old values OR have genesis to reconstruct from scratch

**Risk:** None to the full node. History files are read-only from this process's perspective.

### Option C: Snapshot from MPT Trie Walk (Offline Tool)

**How:** A tool opens the MPT store files read-only and walks the live trie (starting from the root hash) to extract all accounts + storage.

```
root = known_state_root_at_block_N
walk(root) → for each leaf:
    extract account (nonce, balance, code_hash, storage_root)
    walk storage trie → extract all (slot, value) pairs
serialize → snapshot file
```

**Pros:**
- Produces a minimal snapshot (only live data, no dead nodes)
- Can open files read-only with separate mmap (no interference if full node is stopped)
- Output is verifiable: re-compute MPT root from extracted data

**Cons:**
- UNSAFE while full node is running — both processes mmap the same files, full node may be writing/remapping
- Trie walk is random-access I/O intensive — competes for disk bandwidth and page cache
- Must know the root hash for the target block

**Risk:** HIGH if run concurrently. Safe only when the full node is stopped.

### Option D: COW Filesystem Snapshot + Trie Walk

**How:** Use filesystem-level copy-on-write (btrfs snapshot, ZFS snapshot, LVM snapshot) to create an instant read-only copy of the MPT files, then run the trie walk tool against the copy.

```
1. Signal full node to complete current checkpoint (flush + fsync)
2. Create filesystem snapshot (instant, <1ms)
3. Full node resumes immediately
4. Run trie walk tool against the snapshot (takes minutes, no interference)
5. Delete filesystem snapshot when done
```

**Pros:**
- Near-zero downtime (~1ms for snapshot creation)
- Full node resumes immediately — snapshot is COW, writes go to new blocks
- Trie walk runs against a frozen consistent view
- No new code needed in the full node itself

**Cons:**
- Requires COW filesystem (btrfs, ZFS, XFS reflinks) — NVMe is ext4 by default
- Filesystem snapshot includes dead nodes (but trie walk filters them)
- Need to coordinate snapshot timing with checkpoint boundary

**Risk:** Very low. Filesystem snapshot is atomic. Full node is paused only for the checkpoint flush it already does.

### Option E: Dual-Write to Flat Export During Execution

**How:** The full node, during normal block execution, writes state changes to both the MPT store AND a flat export file. The flat file accumulates the latest value for each (addr, field) and (addr, slot) pair.

**Pros:**
- Always up to date — no separate snapshot step
- Flat file is directly loadable by the execution node

**Cons:**
- Adds write I/O to every block execution — violates "zero interference"
- Flat file grows large and needs compaction (duplicate entries for same account)
- Complexity in the hot path
- If the flat file corrupts, debugging is harder

**Risk:** Medium. Extra writes in the hot path could slow execution. Must be carefully benchmarked.

## Recommendation

**Option B (history replay) for initial implementation.** Reasons:

1. Zero risk to the full node — reads only append-only files
2. We already have complete history (blocks 1..4.3M, no gaps, 8GB)
3. Can run on a separate machine entirely
4. The output (flat snapshot) is portable and format-independent
5. No filesystem requirements

**Option D (COW snapshot) as the production path.** Once we move to btrfs/ZFS for the data volume:
1. Instant snapshot at checkpoint boundary
2. Trie walk produces a verified snapshot
3. Full node downtime: <1ms

## Snapshot File Format (Proposed)

Flat binary format, sorted by address for efficient loading:

```
Header:
  magic(4) "SNAP"
  version(4)
  block_number(8)
  state_root(32)
  account_count(8)

Per account (sorted by address):
  addr(20)
  nonce(8)
  balance(32)
  code_hash(32)
  slot_count(4)
  [slot(32) + value(32)] × slot_count    (sorted by slot)

Footer:
  crc32c(4) over entire file
```

The execution node:
1. Loads snapshot → populates its state store
2. Computes state root → verifies against `state_root` in header
3. Applies history diffs from `block_number+1` to tip
4. Starts executing new blocks

## Catch-Up via History Diffs

After loading a snapshot at block N, the execution node applies diffs N+1..tip:

```
for block in (N+1)..tip:
    diff = history.get_diff(block)
    for group in diff.groups:
        if FIELD_NONCE:    set_nonce(group.addr, group.nonce)
        if FIELD_BALANCE:  set_balance(group.addr, group.balance)
        if FIELD_CODE_HASH: set_code_hash(group.addr, group.code_hash)
        for (slot, value) in group.slots:
            set_storage(group.addr, slot, value)
```

This is fast — no EVM execution, no gas computation, just direct state writes. Catching up 10K blocks should take seconds.

## Ongoing Sync

Once caught up, the execution node has two options:
1. **Execute new blocks normally** — receives blocks from CL, runs EVM
2. **Continue applying diffs** — if the full node streams diffs (via history files or network), the execution node can stay in "diff follower" mode

Option 1 is the target — the execution node becomes a full participant. Option 2 is useful during initial sync or if the execution node falls behind.

## Open Questions

- [ ] How large is a full state snapshot at block 4.3M? (estimate: 1-3GB)
- [ ] History replay performance: how long to replay 4.3M diffs?
- [ ] Should we add old values to history diffs (v4) for reverse reconstruction?
- [ ] Network transfer: stream snapshot + diffs over TCP, or just copy files?
- [ ] Should the snapshot include contract bytecode, or just code_hash with separate code store copy?
