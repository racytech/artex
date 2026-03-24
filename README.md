# artex — Ethereum Execution Engine

A from-scratch Ethereum execution layer implementation in C. Designed for
maximum throughput on modern hardware with minimal dependencies.

## What is artex?

artex is an Ethereum execution engine that processes blocks, executes
transactions via the EVM, and maintains the world state. It validates
the entire Ethereum chain from genesis through pre-merge (era1 files)
and post-merge (via Engine API from a Consensus Layer client).

## Architecture

```
artex/
├── evm/             # EVM interpreter (computed goto dispatch, all opcodes)
├── executor/        # Block executor, era1 reader, state history
├── database/        # disk_hash, disk_table (mmap), mpt_store, code_store, mem_art (ART)
├── sync/            # Sync engine (checkpoint, validation, resume)
├── common/          # uint256, keccak256, RLP, hash, address, logger
├── verkle/          # Verkle trie backend (Pedersen/IPA, banderwagon)
├── engine/          # Engine API (JSON-RPC for CL integration)
├── net/             # P2P networking (Portal Network, discv5)
├── tui/             # Terminal UI for chain_replay progress
├── integration_tests/ # State test + blockchain test runner
├── tools/           # chain_replay, state_reconstruct, verkle_reconstruct, evm_t8n
└── third_party/     # blst, secp256k1, cJSON
```

## Key Components

**EVM** — Full Ethereum Virtual Machine with computed goto dispatch,
all opcodes through Prague, precompiles (ecrecover, SHA-256, RIPEMD-160,
modexp, BN256, BLAKE2, point evaluation, BLS12-381), and EIP-2929
warm/cold access tracking.

**State** — Two-tier cache (in-memory ART with two-generation eviction)
backed by persistent MPT store (mmap'd disk_table + slot-allocated .dat).
Incremental Merkle Patricia Trie with per-account storage roots.
Two-list LRU node cache pins upper-branch trie nodes (depth 0-4) while
evicting leaves under memory budget — O(1) eviction, no scanning.

**Sync** — Era1-based pre-merge chain replay with 256-block checkpoint
validation. Supports `--resume` from checkpoint, per-block state root
validation (`--validate-every`), and pre-state dumping for debugging.

**History** — Per-block state diffs written to append-only files via
background thread. A compact changelog of the entire Ethereum state:
every account and storage mutation, block by block. Enables state
reconstruction at any block without re-execution, historical balance/nonce
queries, tracking when accounts were created or destroyed, and building
any state backend (MPT, Verkle) from the same diffs.

**Verkle** — Verkle trie backend with Banderwagon curve, Pedersen
commitments, and IPA proofs. Incremental updates (O(1) per value change).
Backed by disk_table — an mmap'd hash table optimized for pre-hashed keys
(no hash function, power-of-2 bitmask, fingerprint pre-filter).
`verkle_reconstruct` rebuilds the full Verkle state from history diffs
without EVM execution.

## State History: Execute Once, Reconstruct Anywhere

artex records per-block state diffs during chain execution — every
account and storage change is written to append-only history files via
a background thread. This is a one-time cost: full chain replay from
genesis (~2-3 months) produces a complete history of all state changes.

Once the history exists, `state_reconstruct` can rebuild the full world
state at any block — without EVM execution, without transaction decoding,
without sender recovery. Just apply diffs sequentially. Hours instead of
months.

The same history diffs can reconstruct **any state backend**:
- MPT via `state_reconstruct` (validates against era1 state roots)
- Verkle via `verkle_reconstruct` (Pedersen commitments, code chunking)
- Both simultaneously from the same diffs

```
chain_replay (one-time)          state_reconstruct / verkle_reconstruct
┌─────────────────────┐          ┌─────────────────────────┐
│ EVM execution       │          │ Read diff               │
│ Gas validation      │ ──────►  │ set_nonce / set_balance  │
│ Sender recovery     │  diffs   │ set_storage / set_code   │
│ State root checks   │          │ compute_root            │
│ ~2-3 months         │          │ ~hours                  │
└─────────────────────┘          └─────────────────────────┘
```

## Fast Sync via State Snapshots

For nodes that don't need history diffs, artex supports distributing
pre-built state snapshots. Only the `.dat` files (trie node data) need
to be transferred — the `.idx` files (hash table indexes) can be rebuilt
locally using `rebuild_idx` in seconds.

```
Producer (periodic):
  state_reconstruct → .dat files at block N
  Distribute .dat + .meta over network (~8 GB for 4.5M blocks)

Consumer:
  Download .dat + .meta
  rebuild_idx --capacity 500000000    (~30 seconds)
  chain_replay --resume               (continues from block N)
```

The `.dat` files contain only the raw trie node data (append-only, no
hash table overhead). The `.idx` files are 10-50x larger (sparse hash
tables) but are fully derivable from `.dat`. This makes snapshots compact
for distribution while keeping local reads fast via the rebuilt index.

## Building

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

Configuration is in `config.cmake`:

```cmake
set(ENABLE_MPT    ON)    # Merkle Patricia Trie backend
set(ENABLE_VERKLE OFF)   # Verkle as active state backend (post-fork)
set(ENABLE_VERKLE_BUILD OFF) # Build verkle libraries (verkle_reconstruct, benchmarks)
set(ENABLE_HISTORY ON)   # Per-block state diff history
```

### Dependencies

- CMake 3.15+
- C11 compiler (GCC/Clang)
- libsnappy, libcjson, libssl

```bash
# Ubuntu/Debian
sudo apt-get install cmake build-essential libsnappy-dev libcjson-dev libssl-dev
```

## Tools

### chain_replay — Full chain execution from era1 files

```bash
# Fresh run from genesis
./build/chain_replay data/era1 data/mainnet_genesis.json

# Resume from checkpoint
./build/chain_replay --resume data/era1 data/mainnet_genesis.json

# Per-block root validation (debugging)
./build/chain_replay --resume --validate-every 1 data/era1 data/mainnet_genesis.json

# Custom data directory
./build/chain_replay --data-dir /path/to/state data/era1 data/mainnet_genesis.json
```

### state_reconstruct — Rebuild MPT state from history diffs

```bash
./build/state_reconstruct <history_dir> <genesis.json> <era1_dir> [target_block]
```

Reads per-block diffs from state history files, applies them to a fresh
MPT, and validates against era1 block headers. No EVM execution needed.

### verkle_reconstruct — Rebuild Verkle state from history diffs

```bash
./build/verkle_reconstruct <history_dir> <genesis.json> [target_block]
./build/verkle_reconstruct <history_dir> <genesis.json> 10000000 --resume
```

Same diffs, Verkle backend. Derives Pedersen tree keys, chunks code into
31-byte EIP-6800 segments, tracks per-account storage slots for
SELFDESTRUCT clearing. Supports `--resume` with slot tracker rebuild.

### evm_t8n — Transition tool for testing

```bash
./build/evm_t8n --input.alloc alloc.json --input.txs txs.json \
    --input.env env.json --state.fork Cancun --output.result result.json
```

Compatible with geth's `evm t8n` format for differential testing.

### rebuild_idx — Rebuild disk_hash index from data file

```bash
# Tight fit (verification/backup)
./build/rebuild_idx /path/to/mpt_store

# With capacity for continued use
./build/rebuild_idx /path/to/mpt_store --capacity 500000000
```

### verify_mpt — Verify MPT integrity

```bash
./build/verify_mpt /path/to/mpt_store
```

Walks all trie nodes from root and recomputes keccak256 hashes.

## Testing

```bash
# Internal test suites (94K+ tests)
./build/test_runner_batch integration_tests/fixtures/state_tests
./build/test_runner_batch integration_tests/fixtures/blockchain_tests

# Ethereum reference tests
./build/test_runner_batch ~/ethereum-tests/GeneralStateTests
./build/test_runner_batch ~/ethereum-tests/BlockchainTests
```

## State Files

Default data directory: `~/.artex/`

| File | Purpose |
|------|---------|
| `chain_replay_mpt.dat` | Account trie node data |
| `chain_replay_mpt.idx` | Account trie hash index |
| `chain_replay_mpt_storage.dat` | Storage trie node data |
| `chain_replay_mpt_storage.idx` | Storage trie hash index |
| `chain_replay_mpt.meta` | Checkpoint marker (block + root) |
| `chain_replay_mpt.free` | Free slot lists |
| `chain_replay_code.dat` | Contract bytecode store |
| `chain_replay_code.idx` | Code hash index |
| `chain_replay_history/` | Per-block state diffs |

## Design Principles

- **Single-threaded execution** — EVM is inherently serial. No locks in
  the hot path. Background thread only for history I/O.
- **mmap everything** — OS page cache manages all disk I/O. No explicit
  buffer pool, no fsync in the hot path.
- **Arena allocation** — ART state cache uses bump allocator. O(1) destroy
  on checkpoint eviction. No per-entry malloc.
- **Minimal dependencies** — C with no runtime overhead. No garbage
  collector, no JIT, no virtual dispatch.

## License

TBD
