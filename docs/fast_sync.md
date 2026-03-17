# Fast Sync: State Distribution via Portal Network

## Network Architecture

```
artex-h (heavy)          artex-p (portal)          artex-e (execution)
┌──────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ Full replay   │    │ DHT storage layer   │    │ Bootstrap from   │
│ from genesis  │───>│ Many nodes sharing  │<───│ portal, then     │
│ Builds state  │    │ chunks across DHT   │    │ execute forward  │
│ One-time feed │    │ Serves on demand    │    │ Many instances   │
└──────────────┘    └─────────────────────┘    └──────────────────┘
     (single)            (many nodes)              (many nodes)
```

- **artex-h** (heavy) — single node that does all the heavy lifting. Executes every block from genesis, builds MPT, produces state history. Once complete, feeds data into the portal network. Must never be blocked or put at risk of corruption.
- **artex-p** (portal) — data availability layer. Based on portal protocol (or modified version). Multiple nodes storing and serving state chunks across a DHT. No block execution. Provides data on demand to execution nodes.
- **artex-e** (execution) — lightweight execution nodes. Pull state from artex-p to bootstrap, then execute new blocks going forward. Do not re-execute historical blocks. Many can exist.

## Problem

artex-e needs complete state at block N without replaying 1..N. artex-h has this state but:
- MPT files are huge (~33GB+) with dead/unreferenced nodes
- artex-h must not be disturbed during execution
- State must be distributable across many artex-p nodes (not one giant blob)
- artex-e must be able to verify data from untrusted artex-p nodes

## Constraints

1. **Zero interference with artex-h** — no blocking, no I/O contention, no corruption risk
2. **Portal-friendly** — data must be chunked for DHT distribution, not monolithic
3. **Verifiable** — artex-e can verify every chunk against known state roots without trusting artex-p
4. **Incremental** — after bootstrap, artex-e catches up via diffs, not re-execution

## Data That Needs Distribution

| Data | Source | Size (at 4.3M blocks) | Nature |
|------|--------|-----------------------|--------|
| State snapshot | MPT or history replay | ~1-3 GB | Point-in-time, chunked by address range |
| History diffs | state_history.dat/idx | ~8 GB | Append-only, per-block |
| Contract bytecode | code_store | ~4.3 GB | Content-addressed, immutable once written |
| Block headers/bodies | era1 files | ~50 GB | Already portal-compatible format |

## How artex-h Feeds artex-p

artex-h produces data, artex-p stores and distributes it. artex-h never serves artex-e directly.

### Phase 1: State Snapshot Production

artex-h produces a state snapshot offline (after completing execution or at a checkpoint). Two approaches:

**Approach A: History Replay (safest)**

A separate process (can run on same or different machine) reads history files and reconstructs state:

```
state = load_genesis()
for block 1..N:
    diff = history_read(block)
    apply(diff, state)
chunk_and_feed(state) → artex-p
```

- Zero interference — reads only append-only history files
- Can run on a completely separate machine
- We have complete history: blocks 1..4.3M, no gaps, 8GB

**Approach B: COW Filesystem Snapshot (production)**

```
1. artex-h completes checkpoint (flush + fsync)
2. Filesystem snapshot (btrfs/ZFS, instant, <1ms)
3. artex-h resumes immediately
4. Trie walk tool runs against frozen snapshot → produces chunks
5. Feed chunks → artex-p
6. Delete filesystem snapshot
```

- Near-zero artex-h downtime
- Requires COW filesystem

### Phase 2: Ongoing Diff Streaming

After initial snapshot, artex-h continues producing history diffs. These are streamed to artex-p:

```
artex-h checkpoint → new diffs in state_history.dat
                   → feed diffs to artex-p
                   → artex-p stores per-block diffs
                   → artex-e pulls diffs to catch up
```

### Phase 3: Code Store Distribution

Contract bytecode is content-addressed (keyed by code_hash). Feed to artex-p as:
```
content_id = code_hash
content    = bytecode
```

artex-e requests code by hash. artex-p serves it. Verification is trivial: `keccak256(bytecode) == code_hash`.

## Portal Content Types

Data stored in artex-p DHT, each with a content ID for addressing:

### 1. State Chunk

State is divided into chunks by address range for parallel retrieval:

```
Content ID: hash(snapshot_block || chunk_index)

Chunk:
  magic(4) "SCHK"
  version(4)
  snapshot_block(8)        — block number this snapshot represents
  chunk_index(2)           — which chunk (0..N-1)
  chunk_count(2)           — total chunks in this snapshot
  account_count(4)
  state_root(32)           — full state root (same in every chunk, for verification)

  Per account (sorted by address):
    addr(20)
    nonce(8)
    balance(32)
    code_hash(32)
    slot_count(4)
    [slot(32) + value(32)] × slot_count

  chunk_proof              — merkle proof linking this chunk's accounts to state_root
  crc32c(4)
```

**Chunk sizing:** Target ~1-4 MB per chunk. At 4.3M blocks with ~2-3M accounts, that's ~500-1000 chunks. Each chunk covers a contiguous address range.

**Verification:** Each chunk includes a merkle proof (set of MPT nodes) that proves the included accounts belong to the claimed state_root. artex-e verifies the proof against the known state root from a trusted block header.

### 2. History Diff

Per-block diffs, same format as state_history.dat records:

```
Content ID: hash("diff" || block_number)

Body: serialized block_diff_t (existing v3 format)
  header(16) + addr_groups(variable) + crc32c(4)
```

artex-e requests diffs by block number to catch up from snapshot to tip.

### 3. Contract Code

```
Content ID: code_hash

Body: raw bytecode
```

Verification: `keccak256(body) == content_id`.

### 4. Block Header (for root verification)

artex-e needs trusted block headers to verify state roots. These can come from:
- Portal network (existing portal protocol already handles this)
- Hardcoded checkpoint hashes in the client
- CL beacon chain (post-merge)

## How artex-e Bootstraps

```
1. Get trusted block header for block N (from CL or hardcoded checkpoint)
   → extract state_root

2. Request all state chunks for block N from artex-p
   → verify each chunk's merkle proof against state_root
   → load accounts + storage into local state store

3. Request contract bytecode for all code_hashes seen in step 2
   → verify keccak256(code) == code_hash
   → populate local code store

4. Request history diffs for blocks N+1..tip from artex-p
   → apply diffs to local state (no EVM execution)

5. Start executing new blocks from CL
   → artex-e is now a full participant
```

Total bootstrap time estimate:
- Step 2: download ~1-3 GB of state chunks, limited by network bandwidth
- Step 3: download ~4 GB of code (can be lazy — fetch on first use)
- Step 4: apply diffs, seconds per 10K blocks
- No block re-execution at any point

## Chunk Verification Deep Dive

The key trust question: how does artex-e know the data from artex-p is correct?

**State chunks:** Each chunk includes a merkle proof (intermediate MPT nodes) that connects the leaf accounts to the state root. artex-e:
1. Knows the state root from a trusted header
2. Receives chunk with accounts + proof
3. Reconstructs the MPT path from each account leaf to the root
4. Verifies it matches the known state root

This is the same mechanism geth's snap sync uses. The proof size is ~log(N) nodes per account, but can be batched — one proof covers a contiguous range of accounts.

**Storage:** Each account has a `storage_root` in the MPT. Storage slots within a chunk can be verified against the account's `storage_root` the same way.

**Diffs:** Harder to verify individually (a diff is just "what changed", not a proof). Options:
- Apply diff, recompute state root, compare against next block's header → requires full MPT, expensive
- Trust diffs if they came from a source that also provided a valid snapshot → chain of trust
- Batch verify: apply N diffs, check state root at block N+k matches header

## Lazy Code Fetching

Contract code is ~4GB total but artex-e doesn't need all of it upfront. Alternative:

1. Bootstrap with state only (accounts + storage, no code)
2. When EVM needs code for an address, check local code store
3. Cache miss → request from artex-p by code_hash
4. Verify and cache locally

This cuts bootstrap download from ~5-7GB to ~1-3GB. Code is fetched on demand and cached permanently. Popular contracts get cached quickly.

## Ongoing Sync After Bootstrap

Once bootstrapped, artex-e has two modes:

**Normal execution (target):** Receives new blocks from CL, executes them with EVM, updates state. artex-e is a full execution node.

**Diff follower (fallback):** If artex-e falls behind (downtime, slow hardware), it can catch up by pulling diffs from artex-p instead of re-executing blocks. This is much faster but requires artex-h to keep feeding diffs.

```
if (blocks_behind < THRESHOLD)
    execute blocks normally
else
    pull diffs from artex-p, apply without execution
    resume normal execution at tip
```

## File Formats Summary

| Format | Producer | Consumer | Chunked | Verifiable |
|--------|----------|----------|---------|------------|
| State snapshot chunks | artex-h (offline tool) | artex-p → artex-e | Yes (~1-4MB) | Merkle proof |
| History diffs | artex-h (state_history) | artex-p → artex-e | Per-block | Via state root at checkpoints |
| Contract code | artex-h (code_store) | artex-p → artex-e | Per-contract | keccak256 |
| Block headers | CL / portal | artex-e | Per-block | Consensus |

## Implementation Phases

### Phase 0: Tools (current)
- [x] State history recording (blocks 1..4.3M, complete)
- [x] history_read.py for querying diffs
- [ ] State snapshot export tool (history replay → flat binary)
- [ ] State snapshot import tool (flat binary → local state)

### Phase 1: Local Fast Sync
- [ ] Export snapshot from artex-h (offline, history replay)
- [ ] Import snapshot on artex-e (load + verify state root)
- [ ] Diff catch-up (apply history diffs without execution)
- [ ] End-to-end test: export at block N, import on fresh node, verify root

### Phase 2: Portal Integration
- [ ] Chunk snapshot into portal-friendly pieces with merkle proofs
- [ ] Feed chunks + diffs + code to artex-p
- [ ] artex-e pulls and verifies from artex-p
- [ ] Lazy code fetching

### Phase 3: Production
- [ ] COW snapshot for zero-downtime export
- [ ] Continuous diff streaming to artex-p
- [ ] Diff follower mode for artex-e catch-up
- [ ] Multiple artex-e nodes bootstrapping in parallel

## Open Questions

- [ ] How large is a full state snapshot at block 4.3M? (estimate: 1-3GB)
- [ ] History replay performance: how long to replay 4.3M diffs into a snapshot?
- [ ] Optimal chunk size for portal DHT? (tradeoff: proof overhead vs parallelism)
- [ ] Should diffs include old values (v4 format) for reverse reconstruction?
- [ ] Portal protocol modifications needed? (content types, routing, discovery)
- [ ] How to handle artex-e that falls very far behind? (re-snapshot vs diff replay)
- [ ] Lazy code fetch latency: acceptable for block execution deadlines?
