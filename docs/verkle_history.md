# Verkle Historical State — Design (Option B)

## Goal

Serve verkle proofs for any historical block N without replaying the chain.
Built on periodic verkle tree snapshots + an append-only diff log.

## Architecture

```
Block Executor (producer)          Verkle History Tracker (consumer)
        |                                    |
        |--- SPSC ring buffer -------------->|
        |    (block_diff per block)           |
        |                                    |---> diff log (append-only)
        |                                    |---> periodic tree snapshots
```

Single-producer single-consumer. Executor never blocks on history tracking.

## SPSC Message: block_diff

Per-block diff emitted by the executor after each block:

```c
typedef struct {
    uint64_t block_number;

    // Account diffs
    uint32_t       account_count;
    account_diff_t *accounts;   // (address, old/new nonce, balance, code_hash)

    // Storage diffs
    uint32_t       storage_count;
    storage_diff_t *storage;    // (address, slot, old_value, new_value)

    // Self-destructed addresses
    uint32_t    destruct_count;
    address_t  *destructed;

    // Created addresses
    uint32_t    created_count;
    address_t  *created;
} block_diff_t;
```

The executor already tracks all of this via journal + dirty lists.
After `commit_tx`, serialize the dirty set into a `block_diff_t` and
push into the SPSC ring buffer.

## Consumer: Verkle History Tracker

The consumer thread maintains:

1. **A live verkle tree** — updated block-by-block from diffs
2. **An append-only diff log** — persisted to disk, indexed by block number
3. **Periodic tree snapshots** — full verkle tree serialized every K blocks

### Workflow per block_diff:

```
1. Pop block_diff from ring buffer
2. Apply diff to live verkle tree (pedersen commitments)
3. Record verkle_root for this block
4. Append diff to diff log on disk
5. Every K blocks: snapshot the full verkle tree
```

### Snapshot interval (K)

Use the same checkpoint interval as chain_replay (e.g. 256 or 1024).
Tradeoff:
- Smaller K = faster proof reconstruction, more disk usage
- Larger K = less disk, slower proof serving (more diffs to replay)

## Serving a proof at block N

```
1. Find nearest snapshot S <= N
2. Load verkle tree from snapshot S
3. Replay diffs S+1 .. N from the diff log
4. Generate verkle proof from the reconstructed tree at block N
```

Worst case: replay K-1 diffs from a snapshot. With K=256 and ~cheap
verkle updates, this is sub-second.

## Gas rules

The verkle tree structure is independent from the gas schedule.
During historical replay (pre-transition), use MPT gas rules (EIP-2929).
Verkle witness gas (EIP-4762) only applies after the transition fork.
This ensures correct state (matching MPT-validated balances) while
building the verkle tree.

## Why SPSC

- T_exec >> T_verkle_update — consumer keeps up naturally
- No locks, no contention — just a ring buffer
- If consumer falls behind, diffs queue up; no backpressure unless
  the buffer fills (size it for worst case: a few hundred blocks)
- Clean separation of concerns: executor knows nothing about verkle history

## Storage estimates

Per block diff: ~1-10 KB (depends on touched accounts/slots)
Diff log: ~5-50 GB for full mainnet history
Snapshots at K=256: ~number_of_blocks/256 * tree_size
  - Tree size grows with state, ~1-4 GB at scale
  - ~200K snapshots for 50M blocks = needs pruning or larger K for full mainnet

## Future considerations

- **Snapshot pruning**: keep only recent N snapshots + a few archival ones
- **Diff log compaction**: merge sequential diffs for old ranges
- **Proof caching**: LRU cache of recently served proofs
- **Parallel proof generation**: multiple proof requests can share a snapshot
