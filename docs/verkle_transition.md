# Verkle Transition — Background State Building via SPSC Diffs

## Goal

Build a fully up-to-date Verkle state tree in the background so that when
the Verkle fork activates, the executor can switch from MPT to Verkle with
zero downtime — no offline migration, no "stop the world" conversion.

## Current Architecture (Pre-Verkle)

```
                         checkpoint boundary
                               |
executor ──> mem_art ──> compute_mpt_root ──> MPT disk_hash
               |                                 (state root)
               |
          flat_state ← populated with dirty accounts/slots
               |
          (O(1) reads after cache eviction)
```

State root is computed from the MPT. flat_state provides O(1) reads but
has no trie structure — it cannot produce a state root.

## Transition Architecture

Reuse the existing state_history SPSC ring buffer. The consumer thread
currently writes diffs to disk. Extend it to also update a live Verkle tree:

```
executor ──> block_diff_t ──> SPSC ring ──> consumer thread
                                               |
                                               |──> verkle_state_update()
                                               |      (Pedersen commitments)
                                               |
                                               |──> state_history.dat (optional)
                                               |      (append-only diff log)
```

The executor stays on MPT for state root validation. The consumer
builds the Verkle tree in the background. Both run concurrently.

## What the Consumer Receives

Each `block_diff_t` contains:

```c
account_diff_t:
    addr, old_nonce, new_nonce, old_balance, new_balance,
    old_code_hash, new_code_hash, flags (created/destructed)

storage_diff_t:
    addr, slot, old_value, new_value
```

## Mapping Diffs to Verkle Tree Keys (EIP-6800)

Account fields map to fixed tree keys under the account header stem:

| Field       | Tree index | Bytes              |
|-------------|------------|--------------------|
| nonce       | 0          | LE uint64 → 32B    |
| balance     | 1          | LE uint256 → 32B   |
| code_hash   | 3          | 32B                |
| code_size   | 2          | LE uint64 → 32B    |

Storage slots map to storage stems derived from (address, slot_index).

Code is chunked into 31-byte pieces at code-specific tree keys.

### Data Not in Diffs

- **Code bytes**: diffs only contain `code_hash`. When `new_code_hash != old_code_hash`
  (contract creation), read the actual bytecode from `code_store` using the hash.
  Chunk into 31-byte pieces and insert at the appropriate Verkle tree keys.
- **Code size**: not stored in diffs or flat_state. Retrieve from `code_store_get_size()`.

The consumer thread needs read access to `code_store` (already thread-safe:
mmap reads are lock-free, only writes are mutex-protected).

## Fork Transition Sequence

### Phase 1: Background Building (blocks 0 → fork_block - 1)

Consumer thread processes every block diff and updates the Verkle tree.
The executor validates state roots against MPT as usual.

```
executor:  MPT root validation (primary)
consumer:  Verkle tree building (background)
```

At any point: `verkle_root(block N)` should match what the Verkle spec
expects for the same state — verifiable against test vectors.

### Phase 2: Fork Activation (fork_block)

1. Consumer must be fully caught up (Verkle tree at block `fork_block - 1`)
2. Executor switches state root source: MPT → Verkle
3. MPT is no longer needed for root computation

```c
if (header->number >= VERKLE_FORK_BLOCK) {
    root = verkle_state_root(verkle);  // from background-built tree
} else {
    root = evm_state_compute_mpt_root(es, prune_empty);
}
```

### Phase 3: Post-Fork (fork_block+)

Options:
- **Keep the SPSC path**: diffs still flow through the ring buffer, consumer
  updates Verkle. Executor reads from flat_state + mem_art, never touches
  the Verkle tree directly. Clean separation.
- **Inline Verkle**: executor updates Verkle directly (like MPT today).
  Removes the SPSC indirection. Simpler but couples Verkle to the hot path.

Either way, MPT files can be deleted. flat_state remains useful for O(1)
reads regardless of the trie backend.

## Catching Up: What If the Consumer Falls Behind?

The SPSC ring has 512 slots. If the consumer is slower than the executor
(Pedersen commitments are ~10x more expensive than MPT hashing), the ring
fills up and diffs are dropped.

### Mitigations

1. **Increase ring size**: 4096 or 8192 slots buys more headroom.
2. **Backpressure**: if approaching fork_block, slow the executor to let
   the consumer catch up. Only needed in the final stretch.
3. **Replay from flat_state**: if the consumer falls behind, it can
   reconstruct missed blocks by replaying from state_history.dat
   (the diffs are still written to disk).
4. **Checkpoint Verkle tree**: periodically serialize the Verkle tree to
   disk. On restart, resume from the last checkpoint instead of replaying
   from genesis.

### Critical Invariant

At `fork_block`, the consumer **must** be caught up. If not, the executor
cannot produce a valid Verkle state root. Options:
- Block the executor until the consumer catches up (simple, brief stall)
- Require sufficient lead time (e.g. activate building 10K blocks early)

## Memory Considerations

On a 96GB system with ~300GB mmap'd MPT data:

- MPT pages are only actively touched at checkpoint boundaries (root
  computation). Between checkpoints, flat_state handles reads.
- Use `madvise(MADV_DONTNEED)` on MPT pages after each checkpoint to
  release them from page cache, freeing RAM for Verkle.
- Verkle tree working set is smaller than MPT (flatter structure, ~2
  levels vs 7-10 for MPT hex trie).
- Post-fork: MPT files deleted entirely, freeing ~300GB disk + all
  associated page cache pressure.

### Memory Timeline

```
pre-fork:   [mem_art] + [flat_state pages] + [MPT pages (cold)] + [Verkle pages (growing)]
at fork:    [mem_art] + [flat_state pages] + [Verkle pages]
post-fork:  [mem_art] + [flat_state pages] + [Verkle pages]  ← MPT deleted, ~300GB freed
```

## Implementation Steps

### Step 1: Verkle Tree Library

Implement the core Verkle tree (Pedersen commitments, IPA proofs,
tree structure per EIP-6800). This is the largest piece of work —
independent of the SPSC integration.

### Step 2: Extend Consumer Thread

In `state_history.c`, add Verkle tree update logic to the consumer:

```c
// Consumer loop (existing)
while (pop_diff(&ring, &diff)) {
    // Existing: write to disk
    write_diff_to_disk(sh, &diff);

    // New: update Verkle tree
    if (sh->verkle) {
        apply_diff_to_verkle(sh->verkle, &diff, sh->code_store);
    }
}
```

### Step 3: Code Handling

When `new_code_hash != old_code_hash` and the account was created:
1. Read bytecode from code_store via code_hash
2. Chunk into 31-byte pieces
3. Insert chunks at Verkle code tree keys
4. Insert code_size at tree index 2

### Step 4: Fork Switch in Executor

Add fork-aware root computation in `block_executor.c` or `sync.c`:
- Before fork: `compute_mpt_root()`
- At/after fork: `verkle_state_root()`
- Transition block: verify both roots match expected values

### Step 5: MPT Teardown

After fork activation + sufficient confirmation depth:
- Stop writing to MPT (no more `mpt_store_update` calls)
- Delete MPT files (reclaim ~300GB disk)
- Remove MPT from checkpoint cycle

### Step 6: madvise for Transition Period

Between steps 4 and 5 (MPT still on disk but not primary):
- `madvise(MADV_DONTNEED)` on MPT mmap regions after checkpoint
- Keeps MPT cold, maximizes page cache for Verkle

## Verification

1. **Per-block Verkle root**: compare against reference implementation
   (e.g. go-verkle) for the same block diffs
2. **Full state equivalence**: at any block N, the Verkle tree should
   contain the same account/storage values as MPT + flat_state
3. **Transition block**: verify Verkle root matches the expected fork
   block root from the consensus spec
4. **Stress test**: run consumer at 2x executor speed to verify it
   keeps up; run at 0.5x to test backpressure / catch-up

## Disk Usage Summary (at fork transition)

| Component           | Pre-fork        | Post-fork     |
|---------------------|-----------------|---------------|
| MPT .idx (account)  | ~29 GB          | deleted       |
| MPT .idx (storage)  | ~228 GB         | deleted       |
| MPT .dat (both)     | ~50-80 GB       | deleted       |
| flat_state          | ~30 GB          | ~30 GB        |
| Verkle tree         | ~30-50 GB       | ~30-50 GB     |
| code_store          | ~1 GB           | ~1 GB         |
| state_history       | optional        | optional      |
| **Total**           | **~370-420 GB** | **~60-80 GB** |

Post-fork disk usage drops dramatically once MPT is removed.
