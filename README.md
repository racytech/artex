# artex — Ethereum Execution Engine

A from-scratch Ethereum execution layer in C. Designed for maximum
throughput on modern hardware with minimal dependencies.

## What is artex?

artex processes blocks, executes transactions via the EVM, and
maintains the world state. It validates the entire Ethereum chain
from genesis — pre-merge from `.era1` archives, post-merge from
`.era` beacon archives (or live via Engine API to a Consensus Layer
client).

## Architecture

```
artex/
├── evm/               # EVM interpreter (computed goto dispatch, all opcodes)
├── executor/          # Block executor, era1 reader, tx decoding
├── database/          # hart_pool, storage_hart2, hashed_art, code_store, disk_table
├── state/             # in-memory state (accounts, resources, storage tries)
├── sync/              # era1 + era replay, checkpoints, validation
├── common/            # uint256, keccak256, RLP, hash, address, logger
├── lib/               # libartex — shared library with the public C API (rx_*)
├── examples/python/   # Python bindings + CLI example
├── integration_tests/ # state-test / blockchain-test / engine-test runner
├── tools/             # chain_replay, evm_t8n, py_chain_replay, make_hashes
└── third_party/       # blst, secp256k1, gmp, cJSON (pre-built static libs)
```

## Key components

**EVM** — Full Ethereum Virtual Machine with computed goto dispatch, all
opcodes through Osaka (Fusaka), precompiles (ecrecover, SHA-256,
RIPEMD-160, modexp, BN256, BLAKE2, KZG point evaluation, BLS12-381,
P256VERIFY), EIP-2929 warm/cold access tracking, EIP-7702 delegated
authorities.

**State** — Fully in-memory, two flat vectors (accounts, resources) plus
an account-index ART (`hashed_art`) and per-account storage ARTs
(`storage_hart2`) backed by a single anonymous mmap pool (`hart_pool`)
with per-account slab chains. No relocation-on-grow, bounded
fragmentation. See `docs/storage_hart_pool_design.md`.

**Sync** — Pre-merge replay from `.era1` RLP archives, post-merge replay
from `.era` beacon archives (SSZ beacon blocks → ExecutionPayload
extraction). Periodic state-root validation + `--snapshot-every N` for
checkpoint snapshots. Gas is validated every block.

**Snapshots** — `state_save(path)` writes a compact binary snapshot of
the full state (accounts, resources, storage entries) with MPT root.
`state_load(path)` rebuilds state in ~90 s for a ~140 GB mainnet
snapshot. Snapshots combined with the `chain_replay_code.{dat,idx}`
code store form a portable "package" that can resume replay anywhere.

**Public C API** — `include/artex.h` exposes ~30 `rx_*` functions:
engine lifecycle, block execution (RLP or decoded), state queries,
commit/revert, state save/load. Safe for FFI from any language — used
by the Python bindings in `examples/python/`.

## Building

### Dependencies

System packages (Debian/Ubuntu):

```bash
sudo apt install build-essential cmake pkg-config \
    libsnappy-dev libcjson-dev libssl-dev
```

Bundled as pre-built static libraries in `third_party/`:
- `blst` — BLS12-381 (KZG, EIP-4844)
- `secp256k1` — ECDSA (signature verification)
- `gmp` — arbitrary-precision arithmetic
- `cjson` — JSON parsing (genesis files)

### Full build

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

> **Always use `Release` mode**: debug builds are ~4× slower.

### Minimal build (just `libartex.so`)

If you only need the shared library (e.g., for the Python examples):

```bash
cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cmake --build build --target artex_shared -j
```

## Running chain_replay

Download era files first:

```bash
./data/download_era1.sh <target_block>    # pre-merge .era1 files
./data/download_era.sh <target_slot>      # post-merge .era files
```

Fresh run from genesis:

```bash
./build/chain_replay --genesis data/mainnet_genesis.json \
    --era-dir data/era --snapshot-every 800000
```

Resume from a snapshot:

```bash
./build/chain_replay --load-state ~/.artex/state_24864361.bin \
    --era-dir data/era --snapshot-every 800000
```

Per-block validation (debug slow path):

```bash
./build/chain_replay --load-state ~/.artex/state_N.bin \
    --era-dir data/era --validate-every 1
```

## Python examples

See [`examples/python/README.md`](examples/python/README.md) for setup.

```bash
# interactive REPL: load state, query accounts, execute blocks
python3 examples/python/artex_cli.py --state ~/.artex/state_24864361.bin

# one-shot scriptable query
python3 examples/python/example_query.py \
    --state ~/.artex/state_24864361.bin \
    0xdAC17F958D2ee523a2206206994597C13D831ec7
```

## State files

Default data directory: `~/.artex/`. A "snapshot package" is three files
that travel together:

| file | purpose |
|---|---|
| `state_NNNNNNNN.bin` | State snapshot — accounts, resources, storage entries, MPT root |
| `state_NNNNNNNN.bin.hashes` | Last 256 block hashes for BLOCKHASH opcode (optional; build with `tools/make_hashes.py`) |
| `chain_replay_code.dat` | Content-addressed contract bytecode blobs |
| `chain_replay_code.idx` | Code hash index (rebuildable from `.dat`) |

## Testing

```bash
# Internal integration suites (~133K tests, all passing)
./build/test_runner_batch integration_tests/fixtures/state_tests
./build/test_runner_batch integration_tests/fixtures/blockchain_tests
./build/test_runner_batch integration_tests/fixtures/blockchain_tests_engine

# Ethereum reference tests (clone ethereum/tests)
./build/test_runner_batch ~/ethereum-tests/GeneralStateTests
./build/test_runner_batch ~/ethereum-tests/BlockchainTests
```

Other test / benchmark executables in `build/`:
- `test_hart_pool` — slab allocator unit tests
- `bench_hart_*` — micro-benchmarks for the ART index
- `bench_storage_hart_scale` — pool fragmentation benchmark

## Tools

- **`chain_replay`** — era1/era replay with checkpoint snapshots
- **`evm_t8n`** — geth-compatible transition tool for differential testing
- **`evm_statetest`** — run individual state test fixtures
- **`tools/py_chain_replay.py`** — Python replay using libartex (leverages the public API)
- **`tools/make_hashes.py`** — build the `.hashes` sidecar for a snapshot

## Design principles

- **Single-threaded execution** — EVM is inherently serial. No locks in
  the hot path.
- **Anonymous mmap for state** — all state lives in `MAP_ANONYMOUS |
  MAP_PRIVATE`. No explicit buffer pool, OS handles paging.
- **Slab allocation** — per-account slab chains replace arena
  relocation. Zero memcpy on growth, bounded fragmentation.
- **Minimal dependencies** — C with no runtime overhead. No garbage
  collector, no JIT, no virtual dispatch.
- **Public C API** — everything the library needs is exposed via
  `rx_*` functions for FFI-based integrations.

## License

TBD
