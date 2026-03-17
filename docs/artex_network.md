# Artex Network Architecture

## Core Principle

**artex-p IS the network.** It holds all the data. artex-e nodes connect to artex-p and pull whatever they need — they don't know or care where the data originally came from. artex-h is just a feeder that populates artex-p. Once the data is in the portal network, artex-h could vanish and everything keeps working (until new blocks need execution).

## Node Types

```
artex-h (heavy)              artex-p (portal)              artex-e (execution)
┌──────────────────┐    ┌─────────────────────────┐    ┌──────────────────┐
│ Full execution    │    │ PRIMARY DATA STORE       │    │ Connects to      │
│ Feeds data into   │══> │ Has everything artex-e   │ <══│ artex-p network  │
│ the portal net    │    │ will ever need           │    │ Pulls state/code │
│ Always online     │    │ Snapshots, diffs, code   │    │ Executes forward │
│ Few instances     │    │ Cross-validates sources  │    │ Many instances   │
│                   │    │ Many instances (DHT)     │    │ Doesn't know     │
│                   │    │                          │    │ artex-h exists   │
└──────────────────┘    └─────────────────────────┘    └──────────────────┘
      (feeder)              (the network)                  (consumers)
```

### artex-h (heavy)

- Runs full block execution from genesis
- Continuously produces per-block state diffs
- Feeds data into artex-p — that's its only job toward the network
- Always online to keep feeding new blocks
- Few instances (2-3) for redundancy
- Expensive to run but essential — only node type that executes
- Never serves artex-e directly — artex-e doesn't even know it exists

### artex-p (portal)

- **The primary data store** — not a cache, not a proxy
- Already has everything artex-e needs before artex-e even connects
- Receives diff streams from one or more artex-h nodes
- Accumulates state from diffs over time
- Produces its own state snapshots at intervals (computed from diffs)
- Serves snapshots, diffs, and code to artex-e on demand
- Cross-validates: if multiple artex-h feed it, compares diffs for agreement
- Verifies diffs by computing state roots at checkpoints against known headers
- Many instances, DHT-based, sharing storage load
- No EVM execution

### artex-e (execution)

- Lightweight execution node
- Bootstraps state from artex-p (snapshot + diffs)
- After bootstrap, executes new blocks from CL
- Can fall back to diff follower mode if behind
- Many instances — this is what most users run
- Never depends on artex-h directly

## Data Flow

```
                    continuous diff stream
artex-h ──────────────────────────────────────> artex-p
artex-h ──────────────────────────────────────> artex-p
  (2-3)                                          (many)
                                                   │
                                                   │ snapshots + diffs + code
                                                   │ on demand
                                                   v
                                                artex-e
                                                artex-e
                                                artex-e
                                                 (many)
```

## What artex-h Produces

Per block, continuously:
- **Block diff** — which accounts/slots changed, new values (existing state_history v3 format)
- **Contract code** — new bytecode deployed (content-addressed by code_hash)

Periodically (at checkpoint boundaries):
- **State root** — for verification

artex-h does NOT produce snapshots. It just streams diffs. artex-p builds snapshots from the diff stream.

## What artex-p Does With the Data

**Accumulates:**
- Maintains running state by applying diffs: `state[addr] = latest_value`
- Stores full diff history for catch-up requests

**Produces:**
- Periodic state snapshots (chunked, with merkle proofs)
- Computed from accumulated state, not from artex-h

**Validates:**
- Computes state root at checkpoint intervals
- Compares against known block headers (from CL / consensus)
- If multiple artex-h nodes feed it, compares their diffs — must agree
- Detects and rejects bad data from rogue artex-h

**Serves:**
- State snapshot chunks (for artex-e bootstrap)
- History diffs (for artex-e catch-up)
- Contract code (on demand, by code_hash)

## How artex-e Bootstraps

```
1. Connect to artex-p network
2. Get trusted block header (from CL / hardcoded checkpoint)
3. Pull state snapshot chunks for that block
   → verify each chunk via merkle proof against state_root
4. Pull diffs from snapshot_block+1 to tip
   → apply without EVM execution
5. Start executing new blocks from CL
```

If artex-e falls behind later:
```
if blocks_behind < THRESHOLD:
    execute blocks normally (has full state)
else:
    pull diffs from artex-p, apply without execution
    resume normal execution at tip
```

## Redundancy and Failure Modes

### artex-h goes down

- Other artex-h instances keep streaming
- artex-p continues serving from accumulated data
- If ALL artex-h go down: artex-p still serves existing data, but no new diffs
- artex-e nodes already bootstrapped continue executing independently
- New artex-e nodes can still bootstrap from existing snapshots

### artex-p node goes down

- DHT redistributes its data to other artex-p nodes
- Clients retry with different artex-p nodes
- Standard DHT resilience

### artex-e falls behind

- Pulls diffs from artex-p (fast, no re-execution)
- If too far behind, pulls fresh snapshot + diffs

## Cross-Validation

Multiple artex-h nodes provide redundancy AND verification:

```
artex-h₁ produces diff for block N: {addr_A: balance=100, addr_B: nonce=5}
artex-h₂ produces diff for block N: {addr_A: balance=100, addr_B: nonce=5}

artex-p: diffs match ✓ → accept and store
```

If they disagree:
```
artex-h₁ produces diff for block N: {addr_A: balance=100}
artex-h₂ produces diff for block N: {addr_A: balance=200}

artex-p: CONFLICT → check state root against known header
         → the one whose cumulative state produces correct root wins
         → flag the other artex-h as suspect
```

This gives Byzantine fault tolerance without artex-p needing to execute anything.

## Diff Streaming Protocol (Sketch)

artex-h → artex-p connection:

```
1. artex-h connects to artex-p, identifies itself
2. artex-p tells artex-h its latest known block
3. artex-h streams diffs from that point forward:
   for each new block:
     send: block_number, block_hash, state_root, serialized diff, new code[]
4. artex-p acknowledges receipt + validation result
5. On disconnect: artex-h reconnects, resumes from artex-p's latest block
```

Simple TCP or libp2p stream. Diffs are small (~2KB avg), so bandwidth is trivial.

## Storage Distribution Across artex-p Nodes

Not every artex-p node stores everything. DHT splits by content ID:

- **State chunks:** split by address range, each artex-p stores a subset
- **History diffs:** split by block range, each artex-p stores a subset
- **Contract code:** split by code_hash, each artex-p stores a subset

Replication factor: 3-5 copies per content piece (standard DHT).

Total storage per artex-p node (at mainnet scale):
- State: ~1-3 GB / replication_factor
- History: ~8 GB / replication_factor (grows ~2 GB/M blocks)
- Code: ~4 GB / replication_factor
- Manageable on consumer hardware

## Open Questions

- [ ] Diff streaming protocol: TCP, libp2p, or portal protocol extension?
- [ ] artex-p snapshot frequency: every N blocks? on demand?
- [ ] How does artex-p compute merkle proofs for chunks without full MPT?
- [ ] Portal protocol modifications: new content types, or overlay network?
- [ ] artex-h discovery: how does artex-p find artex-h nodes?
- [ ] Authentication: should artex-h sign diffs? (prevents impersonation)
- [ ] Economics: incentives for running artex-p? (storage costs money)
- [ ] Can artex-p be a modified portal node, or does it need to be a new thing?
- [ ] State snapshot size at mainnet tip (~20M accounts)?
- [ ] Should artex-e also contribute to artex-p DHT after bootstrap? (give back storage)
